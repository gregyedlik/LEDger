"""OTA firmware updater with automatic rollback.

Update flow (OTA class):
  1. fetch <base_url>/manifest        -> {"version": N, "files": [...]}
  2. if N > local version, download each file from <base_url>/files/<name>
     to <name>.new (all-or-nothing)
  3. atomically-ish activate: rename current files to .bak, .new to current
  4. record the new version with boot_attempts=0 and reboot

Boot flow (module-level helpers, called from boot.py):
  - increment_boot_attempts() counts boots of an unverified version
  - needs_rollback() is True after MAX failed boots -> rollback() restores
    the .bak files
  - the application calls mark_boot_ok() once it has proven itself
    (e.g. first successful publish or first ACKed heartbeat), which
    verifies the version and removes the .bak files

Manifest file names may use one subdirectory level (e.g. "holdfast/net.py");
the directory is created on the device as needed. "config.py" is never
touched — per-device configuration survives every update.

No external dependencies — raw socket + ssl for HTTP(S), HTTP/1.0 so the
server closes the connection and no chunked encoding is involved.
"""

import os
import json
import socket
import ssl
import machine
import time

_STATE_FILE = "ota_state.json"
_MAX_BOOT_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# State persistence (module-level: boot.py needs these without any config)
# ---------------------------------------------------------------------------

def read_state():
    """Load OTA state from flash. Returns defaults if missing."""
    try:
        with open(_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"version": 0, "boot_attempts": 0, "version_verified": -1}


def _write_state(state):
    with open(_STATE_FILE, "w") as f:
        json.dump(state, f)


def local_version():
    return read_state().get("version", 0)


# ---------------------------------------------------------------------------
# File-name validation and filesystem helpers
# ---------------------------------------------------------------------------

def valid_fname(fname):
    """Allow "name.ext" or "dir/name.ext" — nothing deeper, no dotfiles,
    no traversal, conservative character set."""
    if not fname or "\\" in fname:
        return False
    parts = fname.split("/")
    if len(parts) > 2:
        return False
    for part in parts:
        if not part or part.startswith("."):
            return False
        for ch in part:
            if not (ch.isalpha() or ch.isdigit() or ch in "._-"):
                return False
    return True


def _ensure_parent_dir(fname):
    if "/" in fname:
        d = fname.split("/")[0]
        try:
            os.mkdir(d)
        except OSError:
            pass  # already exists


def _is_dir(path):
    try:
        return os.stat(path)[0] & 0x4000 != 0
    except OSError:
        return False


def _iter_bak_files():
    """Yield .bak file paths in the cwd (the flash root on a device,
    where boot.py runs) and one directory level deep."""
    for entry in os.listdir():
        if _is_dir(entry):
            for sub in os.listdir(entry):
                if sub.endswith(".bak"):
                    yield entry + "/" + sub
        elif entry.endswith(".bak"):
            yield entry


# ---------------------------------------------------------------------------
# Boot-time helpers (rollback protection), called from boot.py
# ---------------------------------------------------------------------------

def increment_boot_attempts():
    """Count this boot if the current version has never proven itself.
    Verified versions always return 0 — their reboots are harmless."""
    state = read_state()
    current_ver = state.get("version", 0)
    verified_ver = state.get("version_verified", -1)

    if current_ver == verified_ver:
        if state.get("boot_attempts", 0) != 0:
            state["boot_attempts"] = 0
            _write_state(state)
        return 0

    state["boot_attempts"] = state.get("boot_attempts", 0) + 1
    _write_state(state)
    return state["boot_attempts"]


def needs_rollback():
    return read_state().get("boot_attempts", 0) >= _MAX_BOOT_ATTEMPTS


def rollback(wdt=None):
    """Restore .bak files and reset the OTA state."""
    print("[ota] ROLLBACK — restoring previous firmware")
    state = read_state()

    for bak in _iter_bak_files():
        original = bak[:-4]
        try:
            try:
                os.remove(original)
            except OSError:
                pass
            os.rename(bak, original)
            print("[ota]   restored %s" % original)
            if wdt:
                wdt.feed()
        except Exception as exc:
            print("[ota]   failed to restore %s: %s" % (original, exc))

    state["version"] = max(0, state.get("version", 1) - 1)
    state["boot_attempts"] = 0
    _write_state(state)
    print("[ota] rollback complete (now v%d)" % state["version"])


def mark_boot_ok():
    """Call once the application has proven itself (first successful
    publish, first ACKed heartbeat, ...). Verifies the current version
    so future reboots never trigger rollback, and removes .bak files."""
    state = read_state()
    current_ver = state.get("version", 0)
    if (state.get("boot_attempts", 0) == 0
            and state.get("version_verified", -1) == current_ver):
        return  # already verified

    state["boot_attempts"] = 0
    state["version_verified"] = current_ver
    _write_state(state)
    print("[ota] boot marked OK — v%d verified" % current_ver)
    for bak in _iter_bak_files():
        try:
            os.remove(bak)
        except OSError:
            pass


def boot_check():
    """The entire boot.py duty: count the attempt, roll back if needed.
    Never raises — a broken OTA module must not brick the device."""
    try:
        attempts = increment_boot_attempts()
        if attempts == 0:
            print("[boot] verified firmware — clean boot")
        else:
            print("[boot] unverified firmware — attempt #%d" % attempts)
        if needs_rollback():
            print("[boot] too many failed boots — rolling back!")
            rollback()
    except Exception as exc:
        print("[boot] ota check skipped:", exc)


# ---------------------------------------------------------------------------
# Updater
# ---------------------------------------------------------------------------

class OTA:
    """Checks <base_url>/manifest and installs newer firmware.

    base_url example: "https://example.org/api/ota/mydevice"
    The server must expose:
        <base_url>/manifest        JSON {"version": N, "files": [names]}
        <base_url>/files/<name>    raw file content
    """

    def __init__(self, base_url, wdt=None):
        self._base_url = base_url.rstrip("/")
        self._wdt = wdt

    def _feed(self):
        if self._wdt:
            self._wdt.feed()

    # -- minimal HTTP(S) GET, HTTP/1.0, no chunked encoding -----------------

    def _http_get(self, url):
        if url.startswith("https://"):
            rest = url[8:]
            tls = True
            port = 443
        elif url.startswith("http://"):
            rest = url[7:]
            tls = False
            port = 80
        else:
            raise ValueError("unsupported url scheme")

        slash = rest.find("/")
        if slash < 0:
            host, path = rest, "/"
        else:
            host, path = rest[:slash], rest[slash:]
        if ":" in host:
            host, port_s = host.rsplit(":", 1)
            port = int(port_s)

        self._feed()
        addr = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)[0][-1]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(15)
        try:
            sock.connect(addr)
            if tls:
                sock = ssl.wrap_socket(sock, server_hostname=host)
            self._feed()

            req = "GET %s HTTP/1.0\r\nHost: %s\r\nConnection: close\r\n\r\n" % (path, host)
            sock.write(req.encode())
            self._feed()

            header_buf = b""
            while b"\r\n\r\n" not in header_buf:
                chunk = sock.read(256)
                if not chunk:
                    break
                header_buf += chunk
                self._feed()

            header_end = header_buf.find(b"\r\n\r\n")
            if header_end < 0:
                raise OSError("bad HTTP response")

            headers = header_buf[:header_end].decode()
            status_line = headers.split("\r\n")[0]
            if " 200 " not in status_line:
                raise OSError("HTTP %s" % status_line)

            body = bytearray(header_buf[header_end + 4:])
            while True:
                chunk = sock.read(512)
                if not chunk:
                    break
                body.extend(chunk)
                self._feed()
            return bytes(body)
        finally:
            sock.close()

    def fetch_manifest(self):
        return json.loads(self._http_get(self._base_url + "/manifest"))

    def fetch_file(self, fname):
        return self._http_get(self._base_url + "/files/" + fname)

    # -- update -------------------------------------------------------------

    def check_and_update(self):
        """Install a newer firmware version if the manifest offers one.
        Reboots on success; returns False if up to date or on any error
        (errors never leave partial state behind)."""
        state = read_state()
        current = state.get("version", 0)
        print("[ota] checking for updates (current v%d)" % current)

        try:
            manifest = self.fetch_manifest()
        except Exception as exc:
            print("[ota] manifest fetch failed:", exc)
            return False

        remote = manifest.get("version", 0)
        files = [f for f in manifest.get("files", []) if f != "config.py"]

        if remote <= current:
            print("[ota] up to date (server v%d)" % remote)
            return False

        for fname in files:
            if not valid_fname(fname):
                print("[ota] rejecting manifest with bad file name: %r" % fname)
                return False

        print("[ota] update available: v%d -> v%d (%d files)"
              % (current, remote, len(files)))

        # Phase 1: download everything as .new — all or nothing.
        for fname in files:
            print("[ota] downloading %s" % fname)
            try:
                data = self.fetch_file(fname)
                _ensure_parent_dir(fname)
                with open(fname + ".new", "wb") as f:
                    f.write(data)
                print("[ota]   %d bytes OK" % len(data))
            except Exception as exc:
                print("[ota]   FAILED:", exc)
                self._cleanup_new(files)
                return False

        # Phase 2: back up current files and activate the new ones.
        print("[ota] activating update")
        for fname in files:
            try:
                try:
                    os.rename(fname, fname + ".bak")
                except OSError:
                    pass  # file didn't exist before this version
                os.rename(fname + ".new", fname)
            except Exception as exc:
                print("[ota] activation failed for %s: %s" % (fname, exc))
                self._undo_activation(files)
                return False

        # Phase 3: record the new (unverified) version and reboot.
        state["version"] = remote
        state["boot_attempts"] = 0
        _write_state(state)
        print("[ota] update complete — rebooting in 3s")
        time.sleep(3)
        machine.reset()
        return True  # unreachable, for clarity

    async def checker_task(self, interval_s, wifi=None, initial_delay_s=60):
        """Periodically check for updates (when the network is up)."""
        import uasyncio as asyncio
        await asyncio.sleep(initial_delay_s)
        while True:
            self._feed()
            if wifi is None or wifi.isconnected():
                try:
                    self.check_and_update()
                except Exception as exc:
                    print("[ota] check failed:", exc)
            await asyncio.sleep(interval_s)

    # -- helpers -------------------------------------------------------------

    def _cleanup_new(self, files):
        for fname in files:
            try:
                os.remove(fname + ".new")
            except OSError:
                pass

    def _undo_activation(self, files):
        for fname in files:
            try:
                os.rename(fname + ".bak", fname)
            except OSError:
                pass
            try:
                os.remove(fname + ".new")
            except OSError:
                pass

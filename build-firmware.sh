#!/bin/sh
# Assemble OTA-servable meter firmware from the client dir + the
# holdfast submodule, and write its version.json manifest.
#
# Usage: ./build-firmware.sh <version>
#
# Output goes to firmware/meter/ (committed, served by server.js).
# config.py is never included: it is per-device and survives OTA.
set -eu

VERSION="${1:?usage: ./build-firmware.sh <version>}"

cd "$(dirname "$0")"
OUT="firmware/meter"
rm -rf "$OUT"
mkdir -p "$OUT/holdfast"

cp client-meter-esp32/boot.py client-meter-esp32/main.py "$OUT/"
cp holdfast/holdfast/*.py "$OUT/holdfast/"

python3 - "$OUT" "$VERSION" <<'EOF'
import json, os, sys

out, version = sys.argv[1], int(sys.argv[2])
files = []
for root, dirs, names in os.walk(out):
    dirs[:] = [d for d in dirs if d != "__pycache__"]
    for name in sorted(names):
        if name.endswith(".pyc") or name == "version.json":
            continue
        rel = os.path.relpath(os.path.join(root, name), out)
        files.append(rel.replace(os.sep, "/"))
files.sort()
with open(os.path.join(out, "version.json"), "w") as f:
    json.dump({"version": version, "files": files}, f, indent=2)
    f.write("\n")
print("built %s v%d: %d files" % (out, version, len(files)))
EOF

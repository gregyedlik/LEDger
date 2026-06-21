"""
LEDger Electricity Meter — ESP32-C3 Client (MicroPython)

Watches the electricity meter's impulse LED through a TSL2561 light
sensor (1 flash = 1 Wh) and publishes a cumulative pulse counter over
MQTT. The counter is retained and idempotent: the server computes
deltas, so re-delivery never double-counts and pulses accumulated
during an outage are caught up by the next publish.

Connection management (WiFi radio cycling, MQTT backoff/keepalive),
OTA updates with rollback, and task supervision come from the holdfast
library (git submodule, shipped under holdfast/ by the firmware build).

Configuration (WiFi credentials, MQTT host/port/auth, OTA URL) lives in
config.py, which is NOT checked into the repo and is never touched by
OTA. Copy config.py.example and fill in your values.

Wiring:
  TSL2561 SDA -> GPIO8, SCL -> GPIO9, VCC -> 3.3V, GND -> GND
  (The INT pin is unused: polling proved more reliable than the
  TSL2561 level interrupt, which re-fires on every clear. The C3
  Super Mini's onboard LED shares GPIO8 with SDA, so no status LED.)
"""

import json
import time

import machine
import network
import ubinascii
import uasyncio as asyncio
from esp32 import NVS

import config
from config import WIFI_SSID, WIFI_PASSWORD, MQTT_HOST, MQTT_PORT

from holdfast import ota, supervisor
from holdfast.mqtt import AckHeartbeat, MqttLink
from holdfast.net import WifiManager

# ── Sensor tuning ────────────────────────────────────────────
# These live here (not config.py) on purpose: they are OTA-updatable,
# so thresholds can be retuned without opening the meter cabinet.

SDA_PIN = 8
SCL_PIN = 9
TSL2561_ADDR = 0x39  # ADDR pin floating

# Pulse detection — ambient dark ~0-10, LED flash ~100-150.
THRESH_HIGH = 30   # flash detected above this
THRESH_LOW = 10    # flash over below this

# Cabinet door detection — when ambient light exceeds this, the door is
# open and pulse counting is unreliable (room light triggers false pulses).
DOOR_OPEN_THRESH = 250   # above this = door open, stop counting
DOOR_CLOSE_THRESH = 200  # below this = door closed, resume counting

# At 3600 W the meter flashes once per second, so 200 ms is safe to ~18 kW.
PULSE_DEBOUNCE_MS = 200

# Must be shorter than the LED flash duration to catch every pulse.
POLL_INTERVAL_MS = 30

PUBLISH_INTERVAL_S = 60
HEARTBEAT_INTERVAL_S = 10
HEARTBEAT_ACK_TIMEOUT_S = 25

# ── Identity & topics ────────────────────────────────────────

CLIENT_ID = ubinascii.hexlify(machine.unique_id()).decode()
print("Client ID: %s" % CLIENT_ID)

HEARTBEAT_TOPIC = b"meter/heartbeat"
HEARTBEAT_ACK_TOPIC = b"meter/heartbeat/ack/" + CLIENT_ID.encode()

# ── TSL2561 ──────────────────────────────────────────────────

_CMD = 0x80
_WORD = 0x20
_REG_CONTROL = 0x00
_REG_TIMING = 0x01
_REG_ID = 0x0A
_REG_DATA0 = 0x0C


class TSL2561:
    def __init__(self, i2c, addr=TSL2561_ADDR):
        self._i2c = i2c
        self._addr = addr

    def init(self):
        self._i2c.writeto_mem(self._addr, _CMD | _REG_CONTROL, b"\x03")  # power on
        time.sleep_ms(50)
        chip_id = self._i2c.readfrom_mem(self._addr, _CMD | _REG_ID, 1)[0]
        print("TSL2561 ID: 0x%02X (expected 0x5x)" % chip_id)
        # Fastest integration time (13.7 ms), low gain — the LED is bright.
        self._i2c.writeto_mem(self._addr, _CMD | _REG_TIMING, b"\x00")

    def light(self):
        raw = self._i2c.readfrom_mem(self._addr, _CMD | _WORD | _REG_DATA0, 2)
        return raw[0] | (raw[1] << 8)


i2c = machine.I2C(0, sda=machine.Pin(SDA_PIN), scl=machine.Pin(SCL_PIN))
sensor = TSL2561(i2c)
try:
    sensor.init()
except OSError as exc:
    # Boot anyway: the device must come online for OTA and diagnostics
    # even with a dead sensor — sensor_task keeps retrying, and a reboot
    # wouldn't power-cycle the sensor's 3.3V rail anyway.
    print("TSL2561 init failed: %s" % exc)

# ── Persistent counter state (NVS) ───────────────────────────
# `total` is the cumulative pulse count since first install; it only
# resets if the flash is erased. `session` increments on every boot so
# the server can tell a reboot (catch up the delta) from a wiped or
# replaced device (re-baseline).

_nvs = NVS("meter")


def _nvs_get(key, default=0):
    try:
        return _nvs.get_i32(key)
    except OSError:
        return default


total = _nvs_get("total")
persisted_total = total
session = _nvs_get("session") + 1
_nvs.set_i32("session", session)
_nvs.commit()
print("Boot session %d, total %d pulses" % (session, total))

door_open = False
light_now = 0

# Hardware watchdog: if the event loop hangs for >8 s the ESP32 reboots.
wdt = machine.WDT(timeout=8000)

# ── Connectivity (holdfast) ──────────────────────────────────
# Mains-powered, so disable WiFi modem power-save where supported —
# the default power-save mode is a common source of dropped packets.

wifi = WifiManager(WIFI_SSID, WIFI_PASSWORD, hostname="ledger-meter", wdt=wdt,
                   pm=getattr(network.WLAN, "PM_NONE", None))
link = MqttLink(MQTT_HOST, MQTT_PORT, client_id=CLIENT_ID,
                user=getattr(config, "MQTT_USER", None),
                password=getattr(config, "MQTT_PASSWORD", None),
                topic_prefix="meter/" + CLIENT_ID, wdt=wdt)

# OTA is optional: a device whose config.py predates OTA_BASE just
# skips self-updating until its config is updated.
_OTA_BASE = getattr(config, "OTA_BASE", None)
_OTA_INTERVAL_S = getattr(config, "OTA_CHECK_INTERVAL", 21600)
updater = ota.OTA(_OTA_BASE, wdt=wdt) if _OTA_BASE else None

link.set_meta("_version", ota.local_version())

# ── Pulse detection ──────────────────────────────────────────


async def sensor_task():
    """Poll the light level and detect complete flash cycles: light
    rises above THRESH_HIGH, count one pulse when it falls back below
    THRESH_LOW. Counting pauses while the cabinet door is open."""
    global total, door_open, light_now
    in_pulse = False
    pulse_peak = 0
    last_pulse_ms = time.ticks_add(time.ticks_ms(), -PULSE_DEBOUNCE_MS)
    read_failures = 0

    while True:
        await asyncio.sleep_ms(POLL_INTERVAL_MS)
        try:
            ch0 = sensor.light()
        except OSError as exc:
            read_failures += 1
            light_now = -1  # signals "sensor unresponsive" on the dashboard
            in_pulse = False
            # After ~3 s of continuous I2C failure, re-init and back off to
            # a 30 s retry cadence. Never give up and never reboot over it:
            # the device must stay reachable for OTA.
            if read_failures % 100 == 0:
                print("Sensor unresponsive (x%d): %s — re-initialising"
                      % (read_failures, exc))
                try:
                    sensor.init()
                except OSError:
                    pass
                await asyncio.sleep(30)
            continue
        read_failures = 0
        light_now = ch0

        # Cabinet door detection (piggybacks on the same read)
        if not door_open and ch0 >= DOOR_OPEN_THRESH:
            door_open = True
            in_pulse = False
            print("Door OPEN (light=%d) — pausing count" % ch0)
        elif door_open and ch0 < DOOR_CLOSE_THRESH:
            door_open = False
            in_pulse = False
            print("Door CLOSED (light=%d) — resuming count" % ch0)
        if door_open:
            continue

        if not in_pulse and ch0 >= THRESH_HIGH:
            # Rising edge — light just went above threshold
            in_pulse = True
            pulse_peak = ch0
        elif in_pulse:
            if ch0 > pulse_peak:
                pulse_peak = ch0
            if ch0 < THRESH_LOW:
                # Falling edge — flash is over, count it (with debounce)
                in_pulse = False
                now = time.ticks_ms()
                if time.ticks_diff(now, last_pulse_ms) >= PULSE_DEBOUNCE_MS:
                    total += 1
                    last_pulse_ms = now
                    print("PULSE total=%d (peak=%d)" % (total, pulse_peak))


# ── Publishing ───────────────────────────────────────────────


async def publish_task():
    """Publish the retained state once a minute and persist the counter
    to NVS. Publishing the unchanged total doubles as a data heartbeat;
    a failed publish needs no special handling — the next successful one
    carries the same cumulative counter."""
    global persisted_total
    while True:
        link.update({"state": json.dumps({
            "session": session,
            "total": total,
            "light": light_now,
            "door": door_open,
        })})
        await link.publish_latest()

        # Persist even when offline so an outage + power loss doesn't
        # forget pulses that were counted but never published.
        if total != persisted_total:
            _nvs.set_i32("total", total)
            _nvs.commit()
            persisted_total = total

        print("light=%d | door=%s | session=%d | total=%d | mqtt=%s"
              % (light_now, "OPEN" if door_open else "closed",
                 session, total, "up" if link.connected else "down"))
        await asyncio.sleep(PUBLISH_INTERVAL_S)


# ── Heartbeat ────────────────────────────────────────────────


def heartbeat_payload(seq):
    return json.dumps({
        "clientId": CLIENT_ID,
        "seq": seq,
        "uptime_s": time.ticks_ms() // 1000,
    })


# The first ACKed heartbeat proves the whole pipeline (WiFi, broker,
# server, subscription) — only then is an OTA-updated firmware verified.
heartbeat = AckHeartbeat(
    link,
    HEARTBEAT_TOPIC,
    HEARTBEAT_ACK_TOPIC,
    heartbeat_payload,
    interval_s=HEARTBEAT_INTERVAL_S,
    timeout_s=HEARTBEAT_ACK_TIMEOUT_S,
    on_first_ack=ota.mark_boot_ok,
)


# ── Main ─────────────────────────────────────────────────────


def main():
    print("LEDger meter — firmware v%d" % ota.local_version())

    tasks = [
        ("manager", link.manager_task(wifi)),
        ("pump", link.pump_task()),
        ("keepalive", link.keepalive_task()),
        ("heartbeat", heartbeat.run()),
        ("sensor", sensor_task()),
        ("publish", publish_task()),
    ]
    if updater:
        tasks.append(("ota", updater.checker_task(_OTA_INTERVAL_S, wifi=wifi)))

    supervisor.run(tasks)


main()

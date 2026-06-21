# LEDger

**Turn a "dumb" electricity meter into a smart one — by counting the blinks of its pulse LED.**

ESP32-C3-SUPERMIN + TSL2561-M light sensor -> MQTT -> Node.js server -> live web dashboard.

The ESP32 runs MicroPython and watches the electricity meter LED, counting
flashes (`1 flash = 1 Wh`). It publishes a **cumulative pulse counter** over
MQTT to the server's embedded broker; the server turns counter deltas into
pulse rows. Because the counter is cumulative and retained, re-delivery never
double-counts and pulses buffered through an outage are caught up by the next
publish. Connection management, OTA updates with rollback, and task
supervision come from the [holdfast](https://github.com/gregyedlik/micropython-holdfast)
library (git submodule).

## Meter Compatibility

Works with any electricity meter that has a metrology/pulse **LED flashing once
per watt-hour** — the common `1000 imp/kWh` standard on modern residential
meters. There's no electrical connection to the meter: the light sensor simply
watches the LED through the cabinet. Meters with a different pulse rate (e.g.
`800` or `10000 imp/kWh`) work too, but the watt-hours-per-flash constant in the
firmware has to be adjusted to match.

Tested with a SX330 D2B32-RENNMN.

## Project Layout

- `client-meter-esp32/` - MicroPython firmware (boot.py, main.py, config.py.example)
- `config.example.json` - server configuration template (copy to `config.json`)
- `holdfast/` - WiFi/MQTT/OTA/supervision library (git submodule)
- `build-firmware.sh` - assembles OTA-servable firmware into `firmware/meter/`
- `firmware/meter/` - built firmware + version.json manifest (committed, served by the server)
- `server/server.js` - Express server, embedded aedes MQTT broker, API
- `server/readings.js` - durable JSON store for manual meter readings
- `server/test-mqtt-e2e.js` - end-to-end test of the MQTT ingest path
- `server/public/index.html` - dashboard
- `server/public/readings.html` - manual readings and drift history
- `server/public/admin.html` - pulse admin and outage view
- `monitor/check-services.sh` - Supervisor service monitor for cron

Runtime data is intentionally not committed:

- `server/meter.db` stores pulse rows, outages, and per-device counter state.
- `server/readings.json` stores manual meter readings.
- `server/backups/` stores daily database backups if used in production.

## Wiring

```text
TSL2561        ESP32-C3-SUPERMIN
-------        -----------------
SDA     ->     GPIO8  (default SDA)
SCL     ->     GPIO9  (default SCL)
VCC     ->     3.3V
GND     ->     GND
```

The firmware polls the TSL2561 directly over I2C every 30 ms. The INT pin is
unused: polling proved more reliable than the TSL2561 level interrupt, which
re-fires on every clear.

## ESP32 Setup

1. Flash MicroPython (ESP32_GENERIC_C3 build) onto the board:

   ```bash
   pip install esptool mpremote
   esptool.py --chip esp32c3 erase_flash
   esptool.py --chip esp32c3 write_flash -z 0x0 ESP32_GENERIC_C3-<version>.bin
   ```

2. Get the firmware files (holdfast is a submodule):

   ```bash
   git clone --recurse-submodules https://github.com/gregyedlik/LEDger.git
   # or, in an existing clone: git submodule update --init
   ```

3. Install the MQTT driver onto the device:

   ```bash
   mpremote mip install umqtt.simple
   ```

4. Create and edit the device config:

   ```bash
   cd client-meter-esp32
   cp config.py.example config.py   # gitignored, never touched by OTA
   # fill in WiFi credentials, MQTT host/port/password, OTA URL
   ```

5. Copy everything to the device:

   ```bash
   mpremote cp boot.py main.py config.py :
   mpremote cp -r ../holdfast/holdfast :
   ```

6. Watch it run:

   ```bash
   mpremote   # opens the REPL; Ctrl-D soft-reboots into main.py
   ```

### Calibrating Thresholds

The sensor thresholds live in `client-meter-esp32/main.py` (not config.py) on
purpose: they are OTA-updatable, so the meter cabinet never needs to be opened
to retune them.

1. Position the TSL2561 over the meter LED.
2. Watch the light level — either the `light=...` status line on the REPL, or
   remotely via the `light` field in `GET /meter/api/summary` (updated every
   minute, works with the cabinet closed).
3. Note ambient light with the LED off and peak light during a flash.
4. Set `THRESH_LOW` just above ambient, `THRESH_HIGH` between ambient and the
   flash peak.
5. Check door-open readings and adjust `DOOR_OPEN_THRESH` / `DOOR_CLOSE_THRESH`
   so room light pauses counting.
6. Ship the change over OTA (next section) — no USB cable needed.

## OTA Releases

The device checks `OTA_BASE/manifest` every 6 hours, installs newer versions
all-or-nothing, and rolls back automatically if a new version fails to boot 3
times. A version is only verified once the server ACKs a heartbeat.

```bash
# after editing client-meter-esp32/main.py or updating the holdfast submodule:
./build-firmware.sh <new-version-number>
git add firmware/ && git commit && git push   # deployed by GitHub Actions
```

`config.py` is never part of a build: per-device settings survive every update.

## MQTT Protocol

- `meter/<clientId>/state` (retained, QoS 1, every 60 s):
  `{"session": N, "total": N, "light": N, "door": bool}` — `total` is the
  cumulative pulse counter (persisted in NVS on the device), `session` a boot
  counter, `light` the current sensor reading (`-1` = sensor unresponsive). The server stores the last `(session, total)` per client and
  inserts the delta as a pulses row. A regression in both means a wiped or
  replaced device and re-baselines without inserting.
- `meter/<clientId>/_version` (retained): firmware version.
- `meter/heartbeat` (every 10 s): `{"clientId", "seq", "uptime_s"}` — the
  server ACKs on `meter/heartbeat/ack/<clientId>`. A missed ACK makes the
  device rebuild its connection; the first ACK after an OTA update marks the
  firmware as good.

## Server Setup

All configuration lives in `config.json` at the repo root (gitignored — no
environment variables):

```bash
cp config.example.json config.json   # then fill in ports and secrets
cd server
npm install
npm start
```

`config.json` sections:

- `web.port` - HTTP port, default `3003` (the app is served under `/meter`)
- `mqtt.port` - embedded MQTT broker port, default `1885`
- `mqtt.password` - if set, MQTT clients must present it as their connect
  password (set the same value in the device's `config.py`); omit for an
  open broker
- `auth.apiKey` - required for admin actions and the legacy pulse POST
- `auth.manualReadingPassword` - for submitting manual meter readings
- `paths.db` / `paths.readings` / `paths.backups` - data locations,
  relative to the config file; default `server/meter.db`,
  `server/readings.json`, `server/backups`

The server also accepts an alternate config path as its first argument
(`node server.js /path/to/config.json`), which the e2e test uses. To
exercise the MQTT ingest path end-to-end (deltas, idempotency,
re-baselining, heartbeat ACK):

```bash
node server.js &
node test-mqtt-e2e.js          # both read ../config.json by default
```

### Server one-time setup

```bash
# Open a public TCP port for the MQTT broker (e.g. 46524) on your host,
# put that number in config.json (mqtt.port) and the device's config.py,
# create ~/ledger/config.json from config.example.json,
# then restart the service: supervisorctl restart ledger
```

The web backend proxies `/meter` to the HTTP port; the MQTT port is reached
directly at `your-server.example.com:<port>`.

## GitHub Actions Deploy

Pushes to `main` that change `server/**`, `monitor/**`, `firmware/**`, or the
deploy workflow run `.github/workflows/deploy.yml`. The workflow SSHes into
the server, runs `git pull --ff-only`, installs server dependencies with
`npm ci --omit=dev`, then restarts the supervised service. (The server only
needs the built `firmware/meter/` files, not the holdfast submodule.)

The workflow deploys over SSH to `your-ssh-user@your-server.example.com`.
Configure this GitHub repository secret:

- `DEPLOY_SSH_KEY` - private SSH key allowed to log in as `your-ssh-user`

**Note:** `holdfast/` is a submodule pointing at GitHub — push
`micropython-holdfast` before pushing this repo, or the submodule commit
won't resolve for other clones.

## Manual Meter Readings

Manual readings are the ground-truth fixed points. Pulse data estimates usage
between those readings, and drift is recomputed from the pulse history whenever
the API is read.

Manual readings use a separate manual-reading password. Submit a
reading in kWh:

```bash
curl -X POST https://your-domain.example.com/meter/api/reading \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_MANUAL_READING_PASSWORD" \
  -d '{"readingKwh": 12345.0}'
```

The dashboard marks the estimated meter tile in red when the last manual
reading is more than 60 days old, because the pulse-only estimate may have
drifted too far from the real meter.

## API

- `GET /meter/api/summary` - lightweight dashboard tile data (includes ESP
  online status, cabinet door state, light level, firmware version)
- `GET /meter/api/data` - dashboard data, readings, drift, ESP status, outages
- `GET /meter/api/pulses/recent` - recent pulse rows for the power chart
- `POST /meter/api/reading` - submit a manual reading in kWh, manual reading password required
- `GET /meter/api/ota/manifest` - firmware manifest for device OTA
- `GET /meter/api/ota/files/...` - firmware files for device OTA
- `POST /meter/api/pulses` - legacy pulse batch endpoint (pre-MQTT Arduino
  firmware), API key required
- `GET /meter/api/admin/pulses` - list pulse rows, API key required
- `DELETE /meter/api/admin/pulses/:id` - delete one pulse row, API key required
- `DELETE /meter/api/admin/pulses` - delete multiple pulse rows, API key required

## Web Pages

- `https://your-domain.example.com/meter/` - main dashboard
- `https://your-domain.example.com/meter/readings.html` - submit readings and inspect drift
- `https://your-domain.example.com/meter/admin.html` - inspect/delete pulse rows and view outages

/**
 * Electricity Meter Monitor — Node.js Server
 *
 * Data model:
 *   - readings.json: manual meter readings (the ground truth fixed points).
 *     Tiny, infrequent, irreplaceable — kept in a plain JSON file with
 *     fsync-on-write so it can't share fate with the pulse blob.
 *   - `pulses` table (meter.db): LED flash counts from the ESP32.  High
 *     volume, self-healing — losing some is recoverable because drift is
 *     recomputed from the next manual reading.
 *   - Current meter estimate = last reading + pulses counted since that reading
 *   - Drift = (curr.reading_wh − prev.reading_wh) − pulses between them.
 *     Always recomputed on read, never stored.
 *
 * MQTT (the primary ingest path — the ESP32 runs MicroPython + holdfast):
 *   - Embedded aedes broker on MQTT_PORT.
 *   - meter/<clientId>/state (retained): {"session", "total", "light",
 *     "door"} — `total` is a cumulative pulse counter, `session` a boot
 *     counter. The server stores the last (session, total) per client in
 *     meter_state and inserts the DELTA as a pulses row, so re-delivery
 *     is idempotent and pulses buffered through an outage are caught up
 *     by the next publish.
 *   - meter/heartbeat: ACKed on meter/heartbeat/ack/<clientId> — drives
 *     online/offline status and verifies OTA-updated firmware.
 *
 * API:
 *   POST /meter/api/pulses           — receive pulse batch (legacy Arduino firmware)
 *   GET  /meter/api/data             — aggregated consumption + readings
 *   POST /meter/api/reading          — submit a manual meter reading (kWh)
 *   GET  /meter/api/ota/...          — firmware manifest + files for device OTA
 *   GET  /meter/                     — serve the webpage
 */

const express = require("express");
const initSqlJs = require("sql.js");
const fs = require("fs");
const net = require("net");
const path = require("path");
const { Aedes } = require("aedes");
const mqtt = require("mqtt");
const { ReadingsStore } = require("./readings");

const app = express();
app.use(express.json());

// ── Configuration ──────────────────────────────────────────
// Everything lives in config.json at the repo root (gitignored — copy
// config.example.json), no environment variables. An alternate config
// path can be passed as the first CLI argument (used by the e2e test).
// Relative paths in the file resolve against the config file's directory.
const CONFIG_PATH = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(__dirname, "..", "config.json");

function loadConfig() {
  let raw = {};
  try {
    raw = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
    console.log(`Config loaded from ${CONFIG_PATH}`);
  } catch (err) {
    console.warn(`No config at ${CONFIG_PATH} (${err.message}) — using defaults`);
  }
  const web = raw.web || {};
  const mqttCfg = raw.mqtt || {};
  const auth = raw.auth || {};
  const paths = raw.paths || {};
  const resolvePath = (p, fallback) =>
    p ? path.resolve(path.dirname(CONFIG_PATH), p) : fallback;
  return {
    webPort: Number(web.port) || 3003,
    mqttPort: Number(mqttCfg.port) || 1885,
    // If set, MQTT clients must present it as their connect password.
    mqttPassword: mqttCfg.password || null,
    apiKey: auth.apiKey || "CHANGE_ME_TO_A_SECRET",
    manualReadingPassword: auth.manualReadingPassword || "CHANGE_ME_MANUAL_READING_PASSWORD",
    dbPath: resolvePath(paths.db, path.join(__dirname, "meter.db")),
    readingsPath: resolvePath(paths.readings, path.join(__dirname, "readings.json")),
    backupDir: resolvePath(paths.backups, path.join(__dirname, "backups")),
  };
}

const CONFIG = loadConfig();
const PORT = CONFIG.webPort;
const MQTT_PORT = CONFIG.mqttPort;
const MQTT_PASSWORD = CONFIG.mqttPassword;
const API_KEY = CONFIG.apiKey;
const MANUAL_READING_PASSWORD = CONFIG.manualReadingPassword;
const DB_PATH = CONFIG.dbPath;
const READINGS_PATH = CONFIG.readingsPath;
const SAVE_INTERVAL = 10000;
const OFFLINE_THRESHOLD = 5 * 60 * 1000; // 5 minutes in ms
const HEARTBEAT_CHECK_INTERVAL = 30000;   // check every 30s
const MAX_PULSE_TIMESTAMP_SKEW_MS = 10 * 60 * 1000; // tolerate 10 min clock drift
const TIMESTAMP_WARNING_WINDOW_MS = 24 * 60 * 60 * 1000; // show recent corrections
const BACKUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 hours
const BACKUP_DIR = CONFIG.backupDir;
const ROLLUP_INTERVAL = 6 * 60 * 60 * 1000; // check every 6 hours
const ROLLUP_AGE_DAYS = 10;

// Ensure the server uses the same timezone as the ESP32 (Hungarian CET/CEST)
process.env.TZ = "Europe/Budapest";

let db;
let readingsStore;            // JSON-backed store for manual meter readings
let mqttClient = null;        // internal client on the embedded broker
let lastHeartbeat = null;  // timestamp (ms) of last device contact (MQTT or POST)
let currentOutageStart = null;  // if ESP32 is currently offline, when it started
let espDoorOpen = null;    // latest cabinet-door state reported over MQTT
let espLight = null;       // latest light level reported over MQTT
let espVersion = null;     // firmware version reported over MQTT

// ── Helpers ────────────────────────────────────────────────

function queryAll(sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function queryOne(sql, params = []) {
  return queryAll(sql, params)[0] || null;
}

function run(sql, params = []) {
  db.run(sql, params);
}

function ensurePulseColumn(columns, name, definition, backfillSql = null) {
  if (columns.has(name)) return false;
  run(`ALTER TABLE pulses ADD COLUMN ${name} ${definition}`);
  if (backfillSql) run(backfillSql);
  columns.add(name);
  return true;
}

function saveDb() {
  try {
    const data = db.export();
    const tmpPath = DB_PATH + ".tmp";
    fs.writeFileSync(tmpPath, Buffer.from(data));
    fs.renameSync(tmpPath, DB_PATH);
  } catch (err) {
    console.error("Save failed:", err.message);
  }
}

function backupDb() {
  try {
    if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });

    const now = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}`;
    const dest = path.join(BACKUP_DIR, `meter.db.${stamp}`);

    fs.copyFileSync(DB_PATH, dest);
    console.log(`Backup saved: ${dest}`);

    // Prune backups older than 30 days
    const cutoff = Date.now() - 30 * 86400000;
    for (const f of fs.readdirSync(BACKUP_DIR)) {
      if (!f.startsWith("meter.db.")) continue;
      const fpath = path.join(BACKUP_DIR, f);
      if (fs.statSync(fpath).mtimeMs < cutoff) {
        fs.unlinkSync(fpath);
        console.log(`Pruned old backup: ${f}`);
      }
    }
  } catch (err) {
    console.error("Backup failed:", err.message);
  }
}

function rollupOldPulses() {
  try {
    const cutoff = localDaysAgo(ROLLUP_AGE_DAYS) + "T00:00:00";

    // Aggregate old minute-level pulses into hourly rows.
    //
    // NOTE: Historically this had `HAVING COUNT(*) > 1` to skip hours that
    // already had a single row. That was a bug: the DELETE below removes
    // ALL rows matching the cutoff, so hours filtered out by HAVING were
    // deleted without being re-inserted. Because rollup's own output is
    // single-row-per-hour, every subsequent rollup pass then wiped the
    // PRIOR rollup's rows — silently eating historical pulses over time.
    // The fix is to aggregate unconditionally: re-inserting a single-row
    // hour is idempotent (its sum equals itself), so no data is lost.
    const hourly = queryAll(
      `SELECT SUM(count) as total,
              strftime('%Y-%m-%dT', timestamp) || printf('%02d', CAST(strftime('%H', timestamp) AS INTEGER)) || ':00:00' as hour_ts,
              MIN(received) as first_received
       FROM pulses
       WHERE timestamp < ?
       GROUP BY date(timestamp), strftime('%H', timestamp)`,
      [cutoff]
    );

    if (hourly.length === 0) return;

    // Atomic swap: delete originals and insert summaries in one transaction
    run("BEGIN");
    try {
      run("DELETE FROM pulses WHERE timestamp < ?", [cutoff]);
      for (const row of hourly) {
        run(
          "INSERT INTO pulses (count, timestamp, received) VALUES (?, ?, ?)",
          [row.total, row.hour_ts, row.first_received]
        );
      }
      run("COMMIT");
    } catch (err) {
      run("ROLLBACK");
      throw err;
    }

    console.log(`Rollup: consolidated pulses older than ${cutoff} into ${hourly.length} hourly rows`);
    saveDb();
  } catch (err) {
    console.error("Rollup failed:", err.message);
  }
}

// ── Auth middleware ─────────────────────────────────────────
function requireApiKey(req, res, next) {
  if (req.headers["x-api-key"] !== API_KEY) {
    return res.status(401).json({ error: "Invalid API key" });
  }
  next();
}

function requireManualReadingPassword(req, res, next) {
  if (req.headers["x-api-key"] !== MANUAL_READING_PASSWORD) {
    return res.status(401).json({ error: "Invalid manual reading password" });
  }
  next();
}

function parseClientTimestampMs(value) {
  if (typeof value !== "string") return NaN;
  const trimmed = value.trim();
  const localMatch = trimmed.match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/
  );

  if (localMatch) {
    const [, y, mo, d, h, mi, s] = localMatch.map(Number);
    const parsed = new Date(y, mo - 1, d, h, mi, s);
    if (
      parsed.getFullYear() === y &&
      parsed.getMonth() === mo - 1 &&
      parsed.getDate() === d &&
      parsed.getHours() === h &&
      parsed.getMinutes() === mi &&
      parsed.getSeconds() === s
    ) {
      return parsed.getTime();
    }
    return NaN;
  }

  return Date.parse(trimmed);
}

function validatePulseTimestamp(clientTimestamp, receivedMs) {
  const receivedAt = localFromMs(receivedMs);
  const rawTimestamp =
    typeof clientTimestamp === "string" ? clientTimestamp.trim() : null;

  if (!rawTimestamp) {
    return {
      timestamp: receivedAt,
      received: receivedAt,
      clientTimestamp: rawTimestamp,
      status: "missing",
      skewSeconds: null,
    };
  }

  const parsedMs = parseClientTimestampMs(rawTimestamp);
  if (!Number.isFinite(parsedMs)) {
    return {
      timestamp: receivedAt,
      received: receivedAt,
      clientTimestamp: rawTimestamp,
      status: "unparseable",
      skewSeconds: null,
    };
  }

  const skewMs = parsedMs - receivedMs;
  if (Math.abs(skewMs) > MAX_PULSE_TIMESTAMP_SKEW_MS) {
    return {
      timestamp: receivedAt,
      received: receivedAt,
      clientTimestamp: rawTimestamp,
      status: "skew",
      skewSeconds: Math.round(skewMs / 1000),
    };
  }

  return {
    timestamp: localFromMs(parsedMs),
    received: receivedAt,
    clientTimestamp: rawTimestamp,
    status: "ok",
    skewSeconds: Math.round(skewMs / 1000),
  };
}

function getPulseTimestampWarning() {
  const cutoff = localFromMs(Date.now() - TIMESTAMP_WARNING_WINDOW_MS);
  const row = queryOne(
    `SELECT count, timestamp, received, client_timestamp, timestamp_status
     FROM pulses
     WHERE timestamp_status != 'ok' AND received >= ?
     ORDER BY received DESC, id DESC
     LIMIT 1`,
    [cutoff]
  );

  if (!row) return null;
  return {
    status: row.timestamp_status,
    count: row.count,
    received: row.received,
    usedTimestamp: row.timestamp,
    clientTimestamp: row.client_timestamp,
  };
}

// Record a sign of life from the device (MQTT message or legacy POST) —
// closes a running outage and refreshes the online-status timestamp.
function recordDeviceActivity(now = Date.now()) {
  if (currentOutageStart) {
    const durationMs = now - currentOutageStart;
    run(
      "INSERT INTO outages (start_time, end_time, duration_sec) VALUES (?, ?, ?)",
      [localFromMs(currentOutageStart), localNow(), Math.round(durationMs / 1000)]
    );
    console.log(`Outage ended: ${Math.round(durationMs / 1000)}s`);
    currentOutageStart = null;
  }
  lastHeartbeat = now;
}

// ── API: Receive pulses from ESP32 ─────────────────────────
app.post("/meter/api/pulses", requireApiKey, (req, res) => {
  const { count, timestamp } = req.body;
  if (!count || count < 0) {
    return res.status(400).json({ error: "Invalid count" });
  }

  const now = Date.now();
  const ts = validatePulseTimestamp(timestamp, now);
  run(
    `INSERT INTO pulses
       (count, timestamp, received, client_timestamp, timestamp_status)
     VALUES (?, ?, ?, ?, ?)`,
    [count, ts.timestamp, ts.received, ts.clientTimestamp, ts.status]
  );

  if (ts.status !== "ok") {
    const skew =
      ts.skewSeconds === null ? "" : `, skew=${ts.skewSeconds}s`;
    console.warn(
      `Invalid ESP pulse timestamp (${ts.status}${skew}): ` +
        `client=${ts.clientTimestamp || "none"}, ` +
        `using_server_time=${ts.timestamp}, count=${count}`
    );
  }

  recordDeviceActivity(now);

  const total = queryOne(
    "SELECT COALESCE(SUM(count), 0) as total FROM pulses"
  ).total;

  console.log(
    `Received ${count} pulses (ts: ${ts.timestamp}, ` +
      `client_ts: ${ts.clientTimestamp || "none"}, ` +
      `timestamp_status: ${ts.status}). Total: ${total} Wh`
  );
  res.json({ ok: true, total, timestamp: ts.timestamp, timestampStatus: ts.status });
});

// ── API: Submit a manual meter reading ─────────────────────
//
// Readings now live in readings.json, not the sql.js blob.  drift_wh is
// computed for the response but never stored — it's always derivable
// from the current pulse state, and storing it just creates a way for
// the two to disagree (see the comment in the data handler).
app.post("/meter/api/reading", requireManualReadingPassword, (req, res) => {
  const { readingKwh } = req.body;
  if (typeof readingKwh !== "number" || readingKwh < 0) {
    return res.status(400).json({ error: "readingKwh must be a positive number" });
  }

  const readingWh = Math.round(readingKwh * 1000);
  const ts = localNow();

  // Compute drift against the previous reading (for the response only).
  const lastReading = readingsStore.last();
  let driftWh = null;
  if (lastReading) {
    const pulsesSinceLast = queryOne(
      "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp > ?",
      [lastReading.timestamp]
    ).total;
    const estimated = lastReading.reading_wh + pulsesSinceLast;
    driftWh = readingWh - estimated;
  }

  // Append + durable write.  If the disk write fails this throws,
  // Express turns it into a 500, and the client sees the failure
  // instead of getting a false "ok" while the data sits in RAM.
  try {
    readingsStore.append(readingWh, ts);
  } catch (err) {
    console.error("Failed to persist reading:", err);
    return res.status(500).json({ error: "Failed to persist reading" });
  }

  console.log(
    `Manual reading: ${readingKwh} kWh (${readingWh} Wh), drift: ${driftWh !== null ? driftWh + ' Wh' : 'first reading'}`
  );

  res.json({
    ok: true,
    readingKwh,
    readingWh,
    driftWh,
    timestamp: ts,
  });
});

// ── Helpers: local time strings (Europe/Budapest) ─────────
// All timestamps from the ESP32 are in local Hungarian time,
// so we must compare against local time, not UTC.
function localNow() {
  // With TZ=Europe/Budapest, toLocaleString gives local time
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function localToday() {
  return localNow().slice(0, 10);
}

function localDaysAgo(n) {
  const d = new Date(Date.now() - n * 86400000);
  const pad = (v) => String(v).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function localFromMs(ms) {
  const d = new Date(ms);
  const pad = (v) => String(v).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

// Array of 'YYYY-MM-DD' strings from startDay..endDay, inclusive on both ends.
function datesBetween(startDay, endDay) {
  const result = [];
  const s = new Date(startDay + "T00:00:00");
  const e = new Date(endDay + "T00:00:00");
  const pad = (v) => String(v).padStart(2, "0");
  for (let d = new Date(s); d <= e; d.setDate(d.getDate() + 1)) {
    result.push(`${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`);
  }
  return result;
}

// Monday-of-week (ISO-ish) for a given 'YYYY-MM-DD' day.
function weekStartOf(dayStr) {
  const d = new Date(dayStr + "T12:00:00");
  const dow = d.getDay(); // 0=Sun..6=Sat
  const offset = dow === 0 ? -6 : 1 - dow;
  d.setDate(d.getDate() + offset);
  const pad = (v) => String(v).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function getSummaryData() {
  const readingsAll = readingsStore.all();
  const firstReading = readingsAll[0] || null;
  const lastReading = readingsAll[readingsAll.length - 1] || null;

  const pulsesSinceReading = lastReading
    ? queryOne(
        "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp > ?",
        [lastReading.timestamp]
      ).total
    : queryOne("SELECT COALESCE(SUM(count), 0) as total FROM pulses").total;

  const meterReading = lastReading
    ? lastReading.reading_wh + pulsesSinceReading
    : pulsesSinceReading;

  const MARCH_START_DAY = '2026-03-01';
  const firstReadingDay = firstReading ? firstReading.timestamp.slice(0, 10) : null;
  const sinceStartDay =
    firstReadingDay && firstReadingDay > MARCH_START_DAY
      ? firstReadingDay
      : MARCH_START_DAY;
  const sinceStartTs = sinceStartDay + "T00:00:00";
  const pulseStartTs =
    firstReading && firstReading.timestamp > sinceStartTs
      ? firstReading.timestamp
      : sinceStartTs;

  let sinceWh = queryOne(
    "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp >= ?",
    [pulseStartTs]
  ).total;

  let lastReadingDriftWh = null;
  for (let i = 1; i < readingsAll.length; i++) {
    const prev = readingsAll[i - 1];
    const curr = readingsAll[i];
    const currentPulsesBetween = queryOne(
      "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp > ? AND timestamp <= ?",
      [prev.timestamp, curr.timestamp]
    ).total;
    const effectiveDrift =
      curr.reading_wh - prev.reading_wh - currentPulsesBetween;

    if (i === readingsAll.length - 1) {
      lastReadingDriftWh = effectiveDrift;
    }

    if (effectiveDrift === 0) continue;

    const days = datesBetween(
      prev.timestamp.slice(0, 10),
      curr.timestamp.slice(0, 10)
    );
    const includedDays = days.filter((day) => day >= sinceStartDay).length;
    if (includedDays > 0) {
      sinceWh += (effectiveDrift / days.length) * includedDays;
    }
  }
  sinceWh = Math.max(0, Math.round(sinceWh));

  const fiveMinAgo = (() => {
    const d = new Date(Date.now() - 5 * 60 * 1000);
    const pad = (v) => String(v).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  })();
  const recentWh = queryOne(
    "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp >= ?",
    [fiveMinAgo]
  ).total;
  const currentWatts = Math.round((recentWh / 5) * 60);

  const todayWh = queryOne(
    "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp >= ?",
    [localToday() + "T00:00:00"]
  ).total;

  const now = Date.now();
  const espOnline = lastHeartbeat !== null && (now - lastHeartbeat) < OFFLINE_THRESHOLD;
  const lastSeen = lastHeartbeat ? localFromMs(lastHeartbeat) : null;

  return {
    meterReading,
    pulsesSinceReading,
    lastReading: lastReading
      ? {
          readingWh: lastReading.reading_wh,
          driftWh: lastReadingDriftWh,
          timestamp: lastReading.timestamp,
        }
      : null,
    sinceWh,
    sinceStartDay,
    currentWatts,
    todayWh,
    esp: {
      online: espOnline,
      lastSeen,
      doorOpen: espDoorOpen,
      light: espLight,
      firmwareVersion: espVersion,
      currentOutageSince: currentOutageStart ? localFromMs(currentOutageStart) : null,
      timestampWarning: getPulseTimestampWarning(),
    },
  };
}

app.get("/meter/api/summary", (req, res) => {
  res.json(getSummaryData());
});

// ── API: Get all data ──────────────────────────────────────
app.get("/meter/api/data", (req, res) => {
  // All readings, ascending.  Pulled from the JSON store, not SQL.
  const readingsAll = readingsStore.all();
  const firstReading = readingsAll[0] || null;
  const lastReading = readingsAll[readingsAll.length - 1] || null;

  // Pulses since last reading (= consumption since last fixed point)
  const pulsesSinceReading = lastReading
    ? queryOne(
        "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp > ?",
        [lastReading.timestamp]
      ).total
    : queryOne("SELECT COALESCE(SUM(count), 0) as total FROM pulses").total;

  // Estimated current meter reading
  const meterReading = lastReading
    ? lastReading.reading_wh + pulsesSinceReading
    : pulsesSinceReading;

  // ── Per-day energy (pulses + distributed drift) ──────────
  // Build a per-day energy map combining:
  //   1. raw pulses grouped by day, and
  //   2. drift from manual readings, distributed evenly across the days
  //      between consecutive readings. This makes outage energy (which was
  //      not captured as pulses but WAS captured as drift when the user
  //      entered the next meter reading) show up in the daily/weekly/monthly
  //      stats — and also drives the "Since 1 March" total, so that the
  //      tile and the monthly chart always agree.

  // Pulses per day — but ONLY from the first manual reading onwards.
  // Pulses recorded before any manual reading have no anchor to balance
  // them against (the first reading's drift is null by definition), so
  // they can't satisfy the invariant
  //   sum(whByDay in interval) == curr.reading_wh − prev.reading_wh
  // If included, they silently inflate every downstream stat.
  const pulsesByDay = new Map();
  const pulseRows = firstReading
    ? queryAll(
        "SELECT date(timestamp) as day, SUM(count) as wh FROM pulses WHERE timestamp >= ? GROUP BY date(timestamp)",
        [firstReading.timestamp]
      )
    : queryAll(
        "SELECT date(timestamp) as day, SUM(count) as wh FROM pulses GROUP BY date(timestamp)"
      );
  for (const row of pulseRows) {
    pulsesByDay.set(row.day, row.wh);
  }

  // Walk consecutive readings to compute (a) per-reading drift for the
  // history table and (b) drift distributed across days for the charts.
  // Drift is always recomputed from the current pulse state — we never
  // persisted it, precisely so it can't go stale relative to pulses that
  // were lost or rolled up after the reading was entered.
  const readingsWithDrift = readingsAll.map((r) => ({ ...r, drift_wh: null }));
  const driftByDay = new Map();
  for (let i = 1; i < readingsAll.length; i++) {
    const prev = readingsAll[i - 1];
    const curr = readingsAll[i];
    const currentPulsesBetween = queryOne(
      "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp > ? AND timestamp <= ?",
      [prev.timestamp, curr.timestamp]
    ).total;
    const effectiveDrift =
      curr.reading_wh - prev.reading_wh - currentPulsesBetween;
    readingsWithDrift[i].drift_wh = effectiveDrift;
    if (effectiveDrift === 0) continue;
    const days = datesBetween(
      prev.timestamp.slice(0, 10),
      curr.timestamp.slice(0, 10)
    );
    const perDay = effectiveDrift / days.length;
    for (const d of days) {
      driftByDay.set(d, (driftByDay.get(d) || 0) + perDay);
    }
  }

  const whByDay = new Map();
  for (const [d, wh] of pulsesByDay) whByDay.set(d, (whByDay.get(d) || 0) + wh);
  for (const [d, wh] of driftByDay) whByDay.set(d, (whByDay.get(d) || 0) + wh);

  // Consumption "since X" — derived from whByDay so it's always consistent
  // with the monthly/weekly/daily totals. The start date is 1 March if a
  // reading exists on/before that date; otherwise it's the date of the first
  // manual reading (since we have no ground truth for anything earlier).
  const MARCH_START_DAY = '2026-03-01';
  const firstReadingDay = firstReading ? firstReading.timestamp.slice(0, 10) : null;
  const sinceStartDay =
    firstReadingDay && firstReadingDay > MARCH_START_DAY
      ? firstReadingDay
      : MARCH_START_DAY;
  let sinceWh = 0;
  for (const [day, wh] of whByDay) {
    if (day >= sinceStartDay) sinceWh += wh;
  }
  sinceWh = Math.max(0, Math.round(sinceWh));

  // Daily consumption (last 31 days)
  const cutoff31 = localDaysAgo(31);
  const daily = [...whByDay.entries()]
    .filter(([day]) => day >= cutoff31)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([day, wh]) => ({ day, wh: Math.round(wh) }));

  // Weekly consumption (last 12 weeks) — keyed by Monday-of-week
  const cutoff84 = localDaysAgo(84);
  const weeklyMap = new Map();
  for (const [day, wh] of whByDay) {
    if (day < cutoff84) continue;
    const wstart = weekStartOf(day);
    weeklyMap.set(wstart, (weeklyMap.get(wstart) || 0) + wh);
  }
  const weekly = [...weeklyMap.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([week_start, wh]) => ({
      week: week_start,
      week_start,
      wh: Math.round(wh),
    }));

  // Monthly consumption (all time)
  const monthlyMap = new Map();
  for (const [day, wh] of whByDay) {
    const month = day.slice(0, 7);
    monthlyMap.set(month, (monthlyMap.get(month) || 0) + wh);
  }
  const monthly = [...monthlyMap.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([month, wh]) => ({ month, wh: Math.round(wh) }));

  // Current power estimate (pulses in last 5 minutes → W)
  const fiveMinAgo = (() => {
    const d = new Date(Date.now() - 5 * 60 * 1000);
    const pad = (v) => String(v).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  })();
  const recentWh = queryOne(
    "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp >= ?",
    [fiveMinAgo]
  ).total;
  const currentWatts = Math.round((recentWh / 5) * 60);

  // Today's total
  const todayWh = queryOne(
    "SELECT COALESCE(SUM(count), 0) as total FROM pulses WHERE timestamp >= ?",
    [localToday() + "T00:00:00"]
  ).total;

  // All manual readings (for history display), newest first.
  const readings = [...readingsWithDrift].reverse();

  // ESP32 online status
  const now = Date.now();
  const espOnline = lastHeartbeat !== null && (now - lastHeartbeat) < OFFLINE_THRESHOLD;
  const lastSeen = lastHeartbeat ? localFromMs(lastHeartbeat) : null;

  // Recent outages (last 30 days)
  const outages = queryAll(
    `SELECT start_time, end_time, duration_sec
     FROM outages
     WHERE start_time >= ?
     ORDER BY start_time DESC`,
    [localDaysAgo(30)]
  );

  res.json({
    meterReading,
    pulsesSinceReading,
    lastReading: lastReading
      ? {
          readingWh: lastReading.reading_wh,
          driftWh:
            readingsWithDrift[readingsWithDrift.length - 1].drift_wh,
          timestamp: lastReading.timestamp,
        }
      : null,
    sinceWh,
    sinceStartDay,
    currentWatts,
    todayWh,
    daily,
    weekly,
    monthly,
    readings,
    esp: {
      online: espOnline,
      lastSeen,
      doorOpen: espDoorOpen,
      light: espLight,
      firmwareVersion: espVersion,
      currentOutageSince: currentOutageStart ? localFromMs(currentOutageStart) : null,
      timestampWarning: getPulseTimestampWarning(),
    },
    outages,
  });
});

// ── API: Recent pulses (last 24 h) for power chart ─────────
app.get("/meter/api/pulses/recent", (req, res) => {
  const cutoff = (() => {
    const d = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const pad = (v) => String(v).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  })();

  const rows = queryAll(
    "SELECT count, timestamp FROM pulses WHERE timestamp >= ? ORDER BY timestamp ASC",
    [cutoff]
  );

  res.json({ pulses: rows });
});

// ── Admin API: list and delete pulse records ───────────────
app.get("/meter/api/admin/pulses", requireApiKey, (req, res) => {
  const page = parseInt(req.query.page) || 0;
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  const offset = page * limit;

  const total = queryOne("SELECT COUNT(*) as n FROM pulses").n;
  const rows = queryAll(
    "SELECT id, count, timestamp FROM pulses ORDER BY timestamp DESC LIMIT ? OFFSET ?",
    [limit, offset]
  );

  res.json({ total, page, limit, rows });
});

app.delete("/meter/api/admin/pulses/:id", requireApiKey, (req, res) => {
  const id = parseInt(req.params.id);
  if (isNaN(id)) return res.status(400).json({ error: "Invalid id" });

  const existing = queryOne("SELECT id FROM pulses WHERE id = ?", [id]);
  if (!existing) return res.status(404).json({ error: "Not found" });

  run("DELETE FROM pulses WHERE id = ?", [id]);
  saveDb();
  console.log(`Admin: deleted pulse record #${id}`);
  res.json({ ok: true, deletedId: id });
});

app.delete("/meter/api/admin/pulses", requireApiKey, (req, res) => {
  const { ids } = req.body;
  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ error: "ids must be a non-empty array" });
  }

  const placeholders = ids.map(() => "?").join(",");
  run(`DELETE FROM pulses WHERE id IN (${placeholders})`, ids);
  saveDb();
  console.log(`Admin: deleted ${ids.length} pulse records`);
  res.json({ ok: true, deleted: ids.length });
});

// ── OTA firmware updates for the meter ESP32 ───────────────
// Serves ./build-firmware.sh output from firmware/meter/. No auth: the
// device is not a browser client, and config.py (the only secret-bearing
// file) is never part of a build and is rejected here as a second line
// of defense. The holdfast/ subdir is the only directory served.

const OTA_FIRMWARE_DIR = path.join(__dirname, "..", "firmware", "meter");

function badOtaName(filename) {
  return (
    filename === "config.py" ||
    filename.includes("/") ||
    filename.includes("\\") ||
    filename.includes("..")
  );
}

// subdir is '' or a fixed, route-supplied directory name — never user input.
function sendOtaFile(subdir, req, res) {
  const filename = req.params.filename;
  if (badOtaName(filename)) {
    return res.status(403).json({ error: "forbidden" });
  }

  const filePath = path.join(OTA_FIRMWARE_DIR, subdir, filename);
  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: "not found" });
  }

  console.log(`[ota] serving ${path.join(subdir, filename)} to ${req.ip}`);
  res.sendFile(filePath);
}

app.get("/meter/api/ota/manifest", (req, res) => {
  const manifestPath = path.join(OTA_FIRMWARE_DIR, "version.json");
  if (!fs.existsSync(manifestPath)) {
    return res.status(404).json({ error: "no firmware manifest" });
  }
  res.sendFile(manifestPath);
});

app.get("/meter/api/ota/files/:filename", (req, res) => {
  sendOtaFile("", req, res);
});

app.get("/meter/api/ota/files/holdfast/:filename", (req, res) => {
  sendOtaFile("holdfast", req, res);
});

// ── MQTT: embedded broker + ingest ─────────────────────────

// Apply a cumulative-counter report. `total` only ever grows within a
// device lifetime (it survives reboots in NVS); `session` increments on
// every boot. Anything else — session going backwards, total shrinking —
// means a wiped or replaced device: re-baseline without inserting, so a
// reset can never double-count. Deltas are inserted as ordinary pulses
// rows, timestamped on arrival (the device clock plays no role).
function handleMeterState(clientId, data) {
  const { session, total } = data;
  if (!Number.isInteger(session) || !Number.isInteger(total) ||
      session < 0 || total < 0) {
    console.warn(`Ignoring malformed meter state from ${clientId}:`, data);
    return;
  }

  const last = queryOne(
    "SELECT session, total FROM meter_state WHERE client_id = ?",
    [clientId]
  );

  let delta = 0;
  if (last && session >= last.session && total >= last.total) {
    delta = total - last.total;
  } else if (last) {
    console.warn(
      `Meter counter re-baselined for ${clientId}: ` +
        `stored session=${last.session} total=${last.total}, ` +
        `reported session=${session} total=${total}`
    );
  } else {
    console.log(`New meter client ${clientId}: baseline total=${total}`);
  }

  run(
    `INSERT INTO meter_state (client_id, session, total) VALUES (?, ?, ?)
     ON CONFLICT(client_id) DO UPDATE SET session = ?, total = ?`,
    [clientId, session, total, session, total]
  );

  if (delta > 0) {
    const ts = localNow();
    run(
      `INSERT INTO pulses
         (count, timestamp, received, client_timestamp, timestamp_status)
       VALUES (?, ?, ?, ?, ?)`,
      [delta, ts, ts, null, "ok"]
    );
    console.log(`MQTT: +${delta} pulses from ${clientId} (total ${total})`);
  }

  recordDeviceActivity();
  espDoorOpen = typeof data.door === "boolean" ? data.door : espDoorOpen;
  espLight = Number.isFinite(data.light) ? data.light : espLight;
}

function handleHeartbeat(data) {
  const clientId = data && data.clientId;
  if (!clientId) {
    console.warn("Heartbeat received without clientId, ignoring");
    return;
  }
  recordDeviceActivity();

  if (!mqttClient || !mqttClient.connected) return;
  const ack = {
    clientId,
    seq: Number.isInteger(data.seq) ? data.seq : null,
    serverTime: new Date().toISOString(),
  };
  mqttClient.publish(`meter/heartbeat/ack/${clientId}`, JSON.stringify(ack), { qos: 0 }, (err) => {
    if (err) {
      console.warn(`Failed to publish heartbeat ACK for ${clientId}: ${err.message}`);
    }
  });
}

async function setupMQTTBroker() {
  const aedes = await Aedes.createBroker();

  // The broker port is open to the internet; when
  // MQTT_PASSWORD is set, every client must present it to connect.
  aedes.authenticate = (client, username, password, done) => {
    if (!MQTT_PASSWORD) return done(null, true);
    const ok = password && password.toString() === MQTT_PASSWORD;
    if (!ok) {
      console.warn(`MQTT auth rejected for ${client?.id || "unknown"}`);
      const err = new Error("bad credentials");
      err.returnCode = 4; // bad username or password
      return done(err, false);
    }
    done(null, true);
  };

  const mqttServer = net.createServer(aedes.handle);

  return new Promise((resolve, reject) => {
    mqttServer.listen(MQTT_PORT, () => {
      console.log(`MQTT broker listening on port ${MQTT_PORT}`);

      mqttClient = mqtt.connect(`mqtt://localhost:${MQTT_PORT}`, {
        clientId: "meter-server-internal",
        password: MQTT_PASSWORD || undefined,
        username: MQTT_PASSWORD ? "server" : undefined,
      });

      mqttClient.on("connect", () => {
        console.log("Internal MQTT client connected");
        mqttClient.subscribe(["meter/heartbeat", "meter/+/state", "meter/+/_version"]);
        resolve();
      });

      mqttClient.on("error", (err) => {
        console.error("Internal MQTT client error:", err);
        reject(err);
      });

      mqttClient.on("message", (topic, payload) => {
        try {
          if (topic === "meter/heartbeat") {
            handleHeartbeat(JSON.parse(payload.toString()));
            return;
          }
          const m = topic.match(/^meter\/([^/]+)\/(state|_version)$/);
          if (!m) return;
          if (m[2] === "state") {
            handleMeterState(m[1], JSON.parse(payload.toString()));
          } else {
            const v = Number(payload.toString());
            espVersion = Number.isFinite(v) ? v : null; // version 0 is valid
          }
        } catch (err) {
          console.warn(`Bad MQTT message on ${topic}: ${err.message}`);
        }
      });
    });

    aedes.on("client", (client) => {
      console.log(`MQTT client connected: ${client.id}`);
    });
    aedes.on("clientDisconnect", (client) => {
      console.log(`MQTT client disconnected: ${client.id}`);
    });
    aedes.on("clientError", (client, err) => {
      console.error("MQTT client error:", client?.id || "unknown", err.message);
    });
    aedes.on("connectionError", (client, err) => {
      console.error("MQTT connection error:", client?.id || "unknown", err.message);
    });
    mqttServer.on("error", (err) => {
      console.error("MQTT server error:", err);
      reject(err);
    });
  });
}

// ── Serve static files ─────────────────────────────────────
app.use("/meter", express.static(path.join(__dirname, "public")));

// ── Start ──────────────────────────────────────────────────
async function main() {
  // Load readings BEFORE touching the SQL DB or starting the listener.
  // If the JSON file is empty-but-existing or malformed, ReadingsStore
  // throws — we want that to abort startup loudly rather than silently
  // come up with an empty store and then overwrite a recoverable file.
  readingsStore = new ReadingsStore(READINGS_PATH);
  console.log(
    `Readings store loaded (${readingsStore.count()} entries) from ${READINGS_PATH}`
  );

  const SQL = await initSqlJs();

  if (fs.existsSync(DB_PATH)) {
    db = new SQL.Database(fs.readFileSync(DB_PATH));
    console.log("Loaded existing database");
  } else {
    db = new SQL.Database();
    console.log("Created new database");
  }

  // Manual readings live in readings.json — only pulses and outages
  // are stored in meter.db.
  db.run(`
    CREATE TABLE IF NOT EXISTS pulses (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      count     INTEGER NOT NULL,
      timestamp TEXT    NOT NULL DEFAULT (datetime('now')),
      received  TEXT    NOT NULL DEFAULT (datetime('now')),
      client_timestamp TEXT,
      timestamp_status TEXT NOT NULL DEFAULT 'ok'
    )
  `);

  const pulseColumns = new Set(
    queryAll("PRAGMA table_info(pulses)").map((row) => row.name)
  );
  const migratedPulseColumns = [
    ensurePulseColumn(
      pulseColumns,
      "received",
      "TEXT",
      "UPDATE pulses SET received = timestamp WHERE received IS NULL"
    ),
    ensurePulseColumn(pulseColumns, "client_timestamp", "TEXT"),
    ensurePulseColumn(
      pulseColumns,
      "timestamp_status",
      "TEXT NOT NULL DEFAULT 'ok'"
    ),
  ].some(Boolean);

  if (migratedPulseColumns) {
    console.log("Migrated pulses table timestamp diagnostics columns");
    saveDb();
  }

  db.run(`
    CREATE TABLE IF NOT EXISTS outages (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      start_time   TEXT NOT NULL,
      end_time     TEXT NOT NULL,
      duration_sec INTEGER NOT NULL
    )
  `);

  // Last seen (session, total) per MQTT meter client — the baseline for
  // turning cumulative counter reports into pulse deltas.
  db.run(`
    CREATE TABLE IF NOT EXISTS meter_state (
      client_id TEXT PRIMARY KEY,
      session   INTEGER NOT NULL,
      total     INTEGER NOT NULL
    )
  `);

  db.run("CREATE INDEX IF NOT EXISTS idx_pulses_ts ON pulses(timestamp)");

  // Periodic heartbeat check — detect when ESP32 goes offline
  setInterval(() => {
    if (lastHeartbeat && !currentOutageStart) {
      const silenceMs = Date.now() - lastHeartbeat;
      if (silenceMs >= OFFLINE_THRESHOLD) {
        currentOutageStart = lastHeartbeat + 60000; // outage began ~1 min after last heartbeat
        console.log(`ESP32 offline since ${localFromMs(currentOutageStart)}`);
      }
    }
  }, HEARTBEAT_CHECK_INTERVAL);

  // Periodic save, backup & rollup
  setInterval(saveDb, SAVE_INTERVAL);
  setInterval(backupDb, BACKUP_INTERVAL);
  setInterval(rollupOldPulses, ROLLUP_INTERVAL);
  backupDb(); // initial backup on startup
  rollupOldPulses(); // initial rollup on startup
  process.on("SIGINT", () => { saveDb(); process.exit(0); });
  process.on("SIGTERM", () => { saveDb(); process.exit(0); });

  await setupMQTTBroker();

  app.listen(PORT, () => {
    console.log(`Meter monitor running at http://localhost:${PORT}/meter/`);
  });
}

// Exit non-zero on failed startup (e.g. MQTT port already in use) so the
// process supervisor restarts us instead of a half-alive server lingering.
main().catch((err) => {
  console.error(err);
  process.exit(1);
});

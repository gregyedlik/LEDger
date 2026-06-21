/**
 * Durable JSON store for manual meter readings.
 *
 * Why a separate file instead of the sql.js blob:
 *   - Readings are the irreplaceable ground truth (a few entries per year)
 *     while pulses are high-volume and self-healing via drift redistribution.
 *     Putting them in the same blob means the cheap data and the expensive
 *     data share fate — when meter.db gets corrupted, both vanish.
 *   - This file does an actually-durable write: fsync the tmp file, rename,
 *     then fsync the parent directory.  That makes a "successful save" mean
 *     "on disk" rather than "in the page cache, fingers crossed".
 *   - On load, we refuse to start on an empty-but-existing file.  That's
 *     the specific failure mode that silently wiped the SQLite readings
 *     table: a bad load returning an empty in-memory DB which the next
 *     periodic save then wrote back over the good file.  Loudly crashing
 *     is much better than silently overwriting.
 *
 * File shape:
 *   {
 *     "version": 1,
 *     "readings": [
 *       { "timestamp": "2026-03-01T10:30:00", "reading_wh": 12345000 },
 *       ...
 *     ]
 *   }
 *
 * Timestamps are local-time strings (Europe/Budapest) to match the rest
 * of the codebase.  reading_wh is an integer (Wh).
 */

const fs = require("fs");
const path = require("path");

class ReadingsStore {
  constructor(filePath) {
    this.path = filePath;
    this.readings = []; // always kept sorted ascending by timestamp
    this._load();
  }

  _load() {
    if (!fs.existsSync(this.path)) {
      // First run — empty store is fine if no file has ever been written.
      this.readings = [];
      return;
    }

    const stat = fs.statSync(this.path);
    if (stat.size === 0) {
      throw new Error(
        `Readings file exists but is empty: ${this.path}\n` +
          `Refusing to start so we don't overwrite a recoverable backup.\n` +
          `Inspect or remove the file manually, or restore from backup.`
      );
    }

    const raw = fs.readFileSync(this.path, "utf8");
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      throw new Error(
        `Readings file is not valid JSON: ${this.path}\n${err.message}\n` +
          `Refusing to start.  Fix the file or restore from backup.`
      );
    }

    if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.readings)) {
      throw new Error(
        `Readings file has unexpected shape: ${this.path}\n` +
          `Expected { version, readings: [...] }.  Refusing to start.`
      );
    }

    for (const r of parsed.readings) {
      if (
        !r ||
        typeof r.timestamp !== "string" ||
        typeof r.reading_wh !== "number" ||
        !Number.isFinite(r.reading_wh)
      ) {
        throw new Error(
          `Invalid reading entry in ${this.path}: ${JSON.stringify(r)}`
        );
      }
    }

    this.readings = [...parsed.readings].sort((a, b) =>
      a.timestamp.localeCompare(b.timestamp)
    );
  }

  /**
   * Append a new reading.  Throws if the on-disk write fails — callers
   * should let that propagate so a 500 is returned to the client rather
   * than the silent swallowing the old sql.js path used to do.
   */
  append(reading_wh, timestamp) {
    if (typeof reading_wh !== "number" || !Number.isFinite(reading_wh) || reading_wh < 0) {
      throw new Error(`Invalid reading_wh: ${reading_wh}`);
    }
    if (typeof timestamp !== "string" || timestamp.length === 0) {
      throw new Error(`Invalid timestamp: ${timestamp}`);
    }

    this.readings.push({ timestamp, reading_wh });
    this.readings.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
    this._save();
  }

  _save() {
    // Durable atomic-write recipe:
    //   1. write payload to tmp
    //   2. fsync the tmp file so its data hits the platter
    //   3. rename tmp → final (atomic on POSIX)
    //   4. fsync the parent directory so the rename itself is durable
    // Skipping any of these means the on-disk file can revert to an old
    // or zero-byte state after a crash — which is exactly the failure
    // mode the old saveDb() path was vulnerable to.
    const tmp = this.path + ".tmp";
    const payload =
      JSON.stringify({ version: 1, readings: this.readings }, null, 2) + "\n";

    fs.writeFileSync(tmp, payload);

    let fd = fs.openSync(tmp, "r+");
    try {
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }

    fs.renameSync(tmp, this.path);

    const dirFd = fs.openSync(path.dirname(this.path), "r");
    try {
      fs.fsyncSync(dirFd);
    } finally {
      fs.closeSync(dirFd);
    }
  }

  /** All readings, ascending by timestamp.  Returns a shallow copy. */
  all() {
    return this.readings.map((r) => ({ ...r }));
  }

  /** Newest reading, or null. */
  last() {
    return this.readings.length > 0
      ? { ...this.readings[this.readings.length - 1] }
      : null;
  }

  /** Oldest reading, or null. */
  first() {
    return this.readings.length > 0 ? { ...this.readings[0] } : null;
  }

  /** How many readings are stored. */
  count() {
    return this.readings.length;
  }
}

module.exports = { ReadingsStore };

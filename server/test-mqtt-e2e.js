/**
 * End-to-end exercise of the MQTT ingest path against a locally running
 * server (MQTT_PORT=1885). Simulates the ESP32's publishes and checks:
 *   - first contact baselines without inserting pulses
 *   - in-session counter growth inserts the delta
 *   - re-delivery of the same total inserts nothing (idempotent)
 *   - reboot (new session, total preserved) catches up the offline delta
 *   - wiped device (session+total regress) re-baselines without inserting
 *   - heartbeat is ACKed on the per-client topic
 *
 * Run: node test-mqtt-e2e.js [config.json]  (then check the exit code)
 * Reads broker port/password from the same config file as the server
 * (../config.json by default, or the path given as the first argument).
 */

const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");

const CONFIG_PATH = process.argv[2] || path.join(__dirname, "..", "config.json");
let cfg = {};
try {
  cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
} catch (err) {
  console.warn(`No config at ${CONFIG_PATH} — using defaults`);
}
const MQTT_PORT = Number(cfg.mqtt?.port) || 1885;
const PASSWORD = cfg.mqtt?.password || undefined;
const WEB_PORT = Number(cfg.web?.port) || 3003;

const CLIENT_ID = "e2etest01";
const STATE_TOPIC = `meter/${CLIENT_ID}/state`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function summary() {
  const res = await fetch(`http://localhost:${WEB_PORT}/meter/api/summary`);
  return res.json();
}

async function main() {
  const before = await summary();
  const startWh = before.todayWh;

  const client = mqtt.connect(`mqtt://localhost:${MQTT_PORT}`, {
    clientId: CLIENT_ID,
    username: PASSWORD ? "meter" : undefined,
    password: PASSWORD,
  });
  await new Promise((resolve, reject) => {
    client.on("connect", resolve);
    client.on("error", reject);
  });
  console.log("connected to broker");

  const pub = (obj) =>
    new Promise((resolve, reject) =>
      client.publish(STATE_TOPIC, JSON.stringify(obj), { qos: 1, retain: true },
        (err) => (err ? reject(err) : resolve()))
    );

  // 1. first contact — baseline, no pulses
  await pub({ session: 5, total: 1000, light: 4, door: false });
  await sleep(300);
  let s = await summary();
  if (s.todayWh !== startWh) throw new Error(`baseline inserted pulses: ${s.todayWh - startWh}`);
  console.log("PASS baseline inserts nothing");

  // 2. in-session growth — delta of 7
  await pub({ session: 5, total: 1007, light: 4, door: false });
  await sleep(300);
  s = await summary();
  if (s.todayWh !== startWh + 7) throw new Error(`expected +7, got +${s.todayWh - startWh}`);
  console.log("PASS in-session delta");

  // 3. re-delivery of the same total — nothing
  await pub({ session: 5, total: 1007, light: 4, door: false });
  await sleep(300);
  s = await summary();
  if (s.todayWh !== startWh + 7) throw new Error("re-delivery double-counted");
  console.log("PASS idempotent re-delivery");

  // 4. reboot: new session, counter kept growing offline — delta of 13
  await pub({ session: 6, total: 1020, light: 9, door: true });
  await sleep(300);
  s = await summary();
  if (s.todayWh !== startWh + 20) throw new Error(`expected +20, got +${s.todayWh - startWh}`);
  if (s.esp.doorOpen !== true || s.esp.light !== 9) throw new Error("door/light state not exposed");
  console.log("PASS reboot catch-up + door/light state");

  // 5. wiped device: session and total regress — re-baseline, nothing inserted
  await pub({ session: 1, total: 3, light: 4, door: false });
  await sleep(300);
  s = await summary();
  if (s.todayWh !== startWh + 20) throw new Error("re-baseline inserted pulses");
  console.log("PASS wipe re-baselines");

  // 6. growth after re-baseline — delta of 2
  await pub({ session: 1, total: 5, light: 4, door: false });
  await sleep(300);
  s = await summary();
  if (s.todayWh !== startWh + 22) throw new Error(`expected +22, got +${s.todayWh - startWh}`);
  console.log("PASS post-rebaseline delta");

  // 7. heartbeat → ACK on the per-client topic, online status flips
  const ackTopic = `meter/heartbeat/ack/${CLIENT_ID}`;
  const ack = new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error("no heartbeat ACK")), 3000);
    client.subscribe(ackTopic, (err) => err && reject(err));
    client.on("message", (topic, payload) => {
      if (topic !== ackTopic) return;
      clearTimeout(t);
      resolve(JSON.parse(payload.toString()));
    });
  });
  client.publish("meter/heartbeat", JSON.stringify({ clientId: CLIENT_ID, seq: 42 }));
  const ackMsg = await ack;
  if (ackMsg.seq !== 42) throw new Error(`ACK seq mismatch: ${ackMsg.seq}`);
  s = await summary();
  if (!s.esp.online) throw new Error("device not marked online");
  console.log("PASS heartbeat ACK + online status");

  // 8. OTA manifest route
  const manifest = await (await fetch(`http://localhost:${WEB_PORT}/meter/api/ota/manifest`)).json();
  if (!manifest.version || !manifest.files.includes("main.py")) {
    throw new Error("OTA manifest not served correctly");
  }
  console.log("PASS OTA manifest served");

  client.end();
  console.log("\nALL E2E CHECKS PASSED");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("E2E FAILED:", err.message);
  process.exit(1);
});

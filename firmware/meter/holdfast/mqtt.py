"""Resilient MQTT link for unattended MicroPython devices.

Wraps umqtt.simple with explicit connection management:

- single-attempt connect(), async ensure() with exponential backoff
- keepalive_task() pings the broker to detect dead connections early
- pump_task() polls for incoming messages and dispatches callbacks
- retained latest-value publishing (publish_latest), including clearing
  retained topics by updating a value to None
- AckHeartbeat: application-level liveness requiring a server ACK

umqtt.simple is used instead of umqtt.robust on purpose: robust's
auto-reconnect blocks in an unbounded retry loop and fights any external
connection manager.

Watchdog note: pump_task() feeds the watchdog on every iteration. Run it
(or feed the watchdog from your own always-running task) so long backoff
sleeps elsewhere cannot starve the WDT.
"""

import json
import time

import uasyncio as asyncio
from umqtt.simple import MQTTClient


class MqttLink:
    def __init__(self, host, port, client_id, user=None, password=None,
                 keepalive=60, topic_prefix="", wdt=None,
                 backoff_min_s=2, backoff_max_s=60):
        self._client = MQTTClient(client_id, host, port=port, user=user,
                                  password=password, keepalive=keepalive)
        self._client.set_callback(self._dispatch)
        self._host = host
        self._prefix = topic_prefix
        self._wdt = wdt
        self._backoff_min = backoff_min_s
        self._backoff_max = backoff_max_s
        self.backoff_s = backoff_min_s
        self.connected = False
        self._fail_count = 0
        self._subs = {}        # topic (bytes) -> callback(topic, msg)
        self._on_connect = []  # callbacks run after every (re)connect
        self._latest = {}      # topic suffix (str) -> payload (str)
        self._meta = {}        # suffix (str) -> payload (str)
        self._pending_clear = set()
        self._cleared = set()

    def _feed(self):
        if self._wdt:
            self._wdt.feed()

    # -- connection ---------------------------------------------------------

    def connect(self):
        """Single connection attempt. Raises on failure."""
        self._feed()
        self._client.connect()
        for topic in self._subs:
            self._client.subscribe(topic)
        self.connected = True
        self.backoff_s = self._backoff_min
        self._fail_count = 0
        print("[mqtt] connected to", self._host)
        for cb in self._on_connect:
            try:
                cb()
            except Exception as exc:
                print("[mqtt] on_connect error:", exc)

    def disconnect(self):
        try:
            self._client.disconnect()
        except Exception:
            pass
        self.connected = False

    async def ensure(self):
        """Reconnect if needed. Sleeps the (exponential) backoff after a
        failed attempt. Returns True when connected."""
        if self.connected:
            return True
        self.disconnect()
        await asyncio.sleep(1)
        try:
            self.connect()
            return True
        except Exception as exc:
            self._fail_count += 1
            print("[mqtt] reconnect failed (#%d): %s — next try in %ds"
                  % (self._fail_count, exc, self.backoff_s))
            await asyncio.sleep(self.backoff_s)
            self.backoff_s = min(self.backoff_s * 2, self._backoff_max)
            return False

    # -- subscriptions ------------------------------------------------------

    def subscribe(self, topic, callback):
        """Register callback(topic, msg) for an exact topic. Survives
        reconnects (re-subscribed automatically)."""
        if isinstance(topic, str):
            topic = topic.encode()
        self._subs[topic] = callback
        if self.connected:
            try:
                self._client.subscribe(topic)
            except Exception as exc:
                print("[mqtt] subscribe failed:", exc)
                self.connected = False

    def on_connect(self, callback):
        """Register a callback to run after every successful (re)connect."""
        self._on_connect.append(callback)

    def _dispatch(self, topic, msg):
        cb = self._subs.get(topic)
        if cb is None:
            return
        try:
            cb(topic, msg)
        except Exception as exc:
            print("[mqtt] handler error on %s: %s" % (topic, exc))

    # -- publishing ---------------------------------------------------------

    def publish(self, topic, payload, retain=False, qos=0):
        """Direct publish. Marks the link dead on failure. Returns success."""
        if not self.connected:
            return False
        try:
            self._client.publish(topic, payload, retain=retain, qos=qos)
            return True
        except Exception as exc:
            print("[mqtt] publish error:", exc)
            self.connected = False
            return False

    def update(self, values):
        """Store latest values to publish: {topic_suffix: value}.
        A value of None clears the retained topic on the broker."""
        for suffix, value in values.items():
            if value is None:
                if suffix in self._latest:
                    del self._latest[suffix]
                if suffix not in self._cleared:
                    self._pending_clear.add(suffix)
                continue
            self._latest[suffix] = str(value)
            self._pending_clear.discard(suffix)
            self._cleared.discard(suffix)

    def set_meta(self, suffix, value):
        """Set a meta value published alongside readings, e.g. ("_version", 3)."""
        self._meta[suffix] = str(value)

    def has_payloads(self):
        return bool(self._latest or self._meta or self._pending_clear)

    def _topic(self, suffix):
        return (self._prefix + "/" + suffix) if self._prefix else suffix

    async def publish_latest(self):
        """Publish all stored values (retained, qos=1), yielding between
        messages so other tasks keep running. Returns True if all went out."""
        if not self.connected:
            return False
        for suffix, payload in list(self._latest.items()):
            if not self.publish(self._topic(suffix), payload, retain=True, qos=1):
                return False
            await asyncio.sleep_ms(0)
        for suffix in list(self._pending_clear):
            if not self.publish(self._topic(suffix), b"", retain=True, qos=1):
                return False
            self._pending_clear.discard(suffix)
            self._cleared.add(suffix)
            await asyncio.sleep_ms(0)
        for suffix, payload in list(self._meta.items()):
            if not self.publish(self._topic(suffix), payload, retain=True, qos=1):
                return False
            await asyncio.sleep_ms(0)
        return True

    # -- tasks --------------------------------------------------------------

    async def manager_task(self, wifi, interval_s=5):
        """Keep WiFi and the broker connection alive. `wifi` is a
        holdfast.net.WifiManager."""
        while True:
            self._feed()
            if await wifi.ensure():
                await self.ensure()
            await asyncio.sleep(interval_s)

    async def pump_task(self, interval_ms=100):
        """Poll for incoming messages and dispatch subscription callbacks.
        Feeds the watchdog every iteration."""
        while True:
            self._feed()
            if self.connected:
                try:
                    self._client.check_msg()
                except Exception as exc:
                    print("[mqtt] receive error:", exc)
                    self.connected = False
            await asyncio.sleep_ms(interval_ms)

    async def keepalive_task(self, interval_s=30):
        """Ping the broker periodically to detect a dead connection early,
        rather than waiting for the next publish to fail."""
        while True:
            await asyncio.sleep(interval_s)
            self._feed()
            if self.connected:
                try:
                    self._client.ping()
                except Exception as exc:
                    print("[mqtt] ping failed — connection lost:", exc)
                    self.connected = False


class AckHeartbeat:
    """Application-level liveness: publish a heartbeat, require a server ACK.

    Detects failures that transport keepalive cannot: a half-open
    subscribe socket, or a broker that is up while the server behind it
    is down. On ACK timeout the link is marked dead so the manager
    reconnects.

    payload_fn(seq) must return the heartbeat payload (str or bytes).
    The ACK message must be JSON containing {"seq": <same seq>}.
    on_first_ack fires once, on the first ACK ever received — a strong
    "the whole pipeline works" signal (e.g. wire it to ota.mark_boot_ok).
    """

    def __init__(self, link, topic, ack_topic, payload_fn,
                 interval_s=10, timeout_s=25, on_first_ack=None):
        self._link = link
        self._topic = topic
        self._payload_fn = payload_fn
        self._interval_s = interval_s
        self._timeout_s = timeout_s
        self._on_first_ack = on_first_ack
        self._seq = 0
        self._awaiting_seq = None
        self._awaiting_since = 0
        self._acked_once = False
        link.subscribe(ack_topic, self._on_ack)
        link.on_connect(self._reset)

    def _reset(self):
        # After a reconnect the in-flight heartbeat can never be ACKed;
        # forget it so the next one goes out immediately.
        self._awaiting_seq = None

    def _on_ack(self, topic, msg):
        try:
            seq = json.loads(msg).get("seq")
        except Exception as exc:
            print("[hb] bad ack: %s" % exc)
            return
        if seq != self._awaiting_seq:
            print("[hb] ignoring ack seq %s (awaiting %s)" % (seq, self._awaiting_seq))
            return
        self._awaiting_seq = None
        if not self._acked_once:
            self._acked_once = True
            if self._on_first_ack:
                try:
                    self._on_first_ack()
                except Exception as exc:
                    print("[hb] on_first_ack error:", exc)

    async def run(self):
        # Backdate so the first heartbeat goes out immediately.
        last_sent = time.ticks_add(time.ticks_ms(), -self._interval_s * 1000)
        while True:
            now = time.ticks_ms()
            if self._awaiting_seq is not None:
                if time.ticks_diff(now, self._awaiting_since) > self._timeout_s * 1000:
                    print("[hb] ACK timeout for seq %d — marking link dead"
                          % self._awaiting_seq)
                    self._awaiting_seq = None
                    self._link.disconnect()
            elif (self._link.connected
                    and time.ticks_diff(now, last_sent) >= self._interval_s * 1000):
                self._seq += 1
                if self._link.publish(self._topic, self._payload_fn(self._seq)):
                    self._awaiting_seq = self._seq
                    self._awaiting_since = now
                    last_sent = now
            await asyncio.sleep_ms(500)

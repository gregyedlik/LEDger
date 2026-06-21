"""WiFi connection manager for unattended MicroPython devices.

The WiFi chip (CYW43 on the Pico W boards, the radio on ESP32) can wedge:
connect() never associates no matter how long you wait, and retrying
through the same stuck firmware is useless. WifiManager therefore
power-cycles the radio (active(False) -> active(True)) after every failed
attempt, which clears the chip's internal state without rebooting the
board.

All waits are async so the rest of the application keeps running while
the network is down.
"""

import time

import network
import uasyncio as asyncio


class WifiManager:
    def __init__(self, ssid, password, hostname=None, led=None, wdt=None,
                 attempt_timeout_s=20, pm=None):
        """pm: optional WiFi power-management mode applied after every
        radio activation (e.g. network.WLAN.PM_NONE on mains-powered
        devices — the default modem power-save is a common source of
        dropped packets, especially on ESP32). Silently ignored on ports
        without pm support."""
        self._ssid = ssid
        self._password = password
        self._hostname = hostname
        self._led = led
        self._wdt = wdt
        self._timeout_s = attempt_timeout_s
        self._pm = pm
        self.wlan = network.WLAN(network.STA_IF)

    def _feed(self):
        if self._wdt:
            self._wdt.feed()

    def _apply_pm(self):
        if self._pm is None:
            return
        try:
            self.wlan.config(pm=self._pm)
        except Exception:
            pass  # port or driver without pm support

    def _radio_off(self):
        # disconnect() raises on an inactive interface on some ports
        # (ESP32); the goal is just a cold radio, so ignore it.
        try:
            self.wlan.disconnect()
        except Exception:
            pass
        self.wlan.active(False)

    def isconnected(self):
        return self.wlan.isconnected()

    def ip(self):
        return self.wlan.ifconfig()[0] if self.wlan.isconnected() else None

    async def connect(self):
        """One connection attempt with timeout.

        Returns True if connected. On timeout the radio is power-cycled
        so the next attempt starts from a clean chip state.
        """
        self._feed()
        if self._hostname:
            network.hostname(self._hostname)
        self.wlan.active(True)
        self._apply_pm()
        if self.wlan.isconnected():
            if self._led:
                self._led.on()
            return True

        print("[wifi] connecting to", self._ssid)
        self.wlan.connect(self._ssid, self._password)
        deadline = time.ticks_add(time.ticks_ms(), self._timeout_s * 1000)
        while not self.wlan.isconnected():
            self._feed()
            if self._led:
                self._led.value(not self._led.value())
            await asyncio.sleep_ms(500)
            if time.ticks_diff(deadline, time.ticks_ms()) <= 0:
                break

        if self.wlan.isconnected():
            if self._led:
                self._led.on()
            print("[wifi] connected:", self.wlan.ifconfig()[0])
            return True

        print("[wifi] timed out after %ds — cycling radio" % self._timeout_s)
        self._radio_off()
        if self._led:
            self._led.off()
        await asyncio.sleep(2)
        return False

    async def ensure(self):
        """Reconnect if the connection dropped. Returns True when connected."""
        if self.wlan.isconnected():
            return True
        print("[wifi] lost — reconnecting")
        if self._led:
            self._led.off()
        self._radio_off()
        await asyncio.sleep(2)
        return await self.connect()

    async def wait_connected(self):
        """Retry until connected. Only returns once online."""
        while not await self.connect():
            pass

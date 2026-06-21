"""holdfast — resilient WiFi + MQTT + OTA scaffolding for unattended
MicroPython devices (Raspberry Pi Pico W / Pico 2 W and friends).

Modules (import what you need; nothing is imported eagerly to save RAM):

    holdfast.net         WifiManager — connect/ensure with radio cycling
    holdfast.mqtt        MqttLink, AckHeartbeat — managed broker connection
    holdfast.ota         OTA + boot-time rollback helpers
    holdfast.supervisor  run() — reboot if any task dies
"""

__version__ = "0.1.0"

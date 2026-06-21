# boot.py — runs before main.py on every ESP32 startup.
# OTA rollback protection: if an updated firmware fails to boot 3 times
# in a row, holdfast restores the previous version.

import gc
gc.collect()

try:
    from holdfast.ota import boot_check
    boot_check()
except Exception as e:
    # If the OTA code itself is broken or missing, just continue booting.
    print("[boot] ota check skipped:", e)

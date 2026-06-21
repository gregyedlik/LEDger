"""Task supervisor: run asyncio tasks and reboot if any of them dies.

A hardware watchdog only catches a wedged event loop — if a single task
dies from an unhandled exception while the others keep feeding the WDT,
the device stays up half-broken forever. The supervisor closes that gap:
any task ending (return or exception) logs the reason and resets the
machine, restoring crash-only semantics.
"""

import sys

import machine
import uasyncio as asyncio

try:
    _print_exception = sys.print_exception  # MicroPython
except AttributeError:  # CPython (tests)
    import traceback

    def _print_exception(exc):
        traceback.print_exception(type(exc), exc, exc.__traceback__)


def run(tasks, reboot_delay_s=3):
    """Run (name, coroutine) pairs forever; reset the machine if any ends.

    Example:
        supervisor.run([
            ("manager", link.manager_task(wifi)),
            ("pump", link.pump_task()),
            ("app", app_task()),
        ])
    """

    async def _main():
        died = asyncio.Event()
        reason = {}

        async def guard(name, coro):
            try:
                await coro
                reason["text"] = "task '%s' exited" % name
            except Exception as exc:
                _print_exception(exc)
                reason["text"] = "task '%s' crashed: %s" % (name, exc)
            died.set()

        for name, coro in tasks:
            asyncio.create_task(guard(name, coro))

        await died.wait()
        print("[supervisor] %s — rebooting in %ds"
              % (reason.get("text", "?"), reboot_delay_s))
        await asyncio.sleep(reboot_delay_s)
        machine.reset()

    asyncio.run(_main())

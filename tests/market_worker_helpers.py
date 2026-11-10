"""What the market worker transport tests serve from a real worker process."""

import asyncio
import os
import threading


class Retry(Exception):
    def __init__(self, message, retry_after):
        super().__init__(message)
        self.retry_after = retry_after

    def __reduce__(self):
        return (Retry, (self.args[0], self.retry_after))


class Served:
    def __init__(self, settings, name):
        self.settings, self.name = settings, name
        self.calls = 0
        self.active = 0
        self.overlapped = False
        self.cancelled = []

    async def echo(self, value):
        self.calls += 1
        return {"value": value, "pid": os.getpid(), "calls": self.calls, "worker": self.name}

    async def setting(self, name):
        return self.settings.load_config(name)

    async def slow(self, value, seconds):
        self.active += 1
        self.overlapped |= self.active > 1
        try:
            await asyncio.sleep(seconds)
        finally:
            self.active -= 1
        return {"value": value, "overlapped": self.overlapped}

    async def stubborn(self, value, seconds):
        """Notices its cancellation only after the given time, like a read that finishes
        the page it is on."""
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.cancelled.append(value)
            await asyncio.sleep(seconds)
            raise

    async def seen(self):
        return {"cancelled": self.cancelled, "calls": self.calls, "overlapped": self.overlapped}

    async def retry(self, seconds):
        raise Retry("try later", seconds)

    async def unportable(self):
        raise RuntimeError("held", threading.Lock())

    async def unportable_result(self):
        return threading.Lock()

    async def die(self):
        os._exit(7)


async def open_served(settings, name):
    return Served(settings, name)

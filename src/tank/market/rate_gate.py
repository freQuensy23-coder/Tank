"""Priority admission: baseline capture outranks repair and optional REST confirmation."""

import asyncio
import heapq
from contextlib import asynccontextmanager
from itertools import count


class PriorityGate:
    """At most `capacity` holders at once; waiters are admitted lowest priority number first."""

    def __init__(self, capacity=1):
        self.capacity = capacity
        self.active = 0
        self.waiters = []
        self.sequence = count()

    def release(self):
        while self.waiters:
            _, _, future = heapq.heappop(self.waiters)
            if not future.done():
                future.set_result(True)  # the slot passes to this waiter
                return
        self.active -= 1

    @asynccontextmanager
    async def enter(self, priority=0):
        if self.active >= self.capacity:
            future = asyncio.get_running_loop().create_future()
            heapq.heappush(self.waiters, (priority, next(self.sequence), future))
            try:
                await future
            except BaseException:
                if future.done() and not future.cancelled():
                    self.release()
                else:
                    future.cancel()
                raise
        else:
            self.active += 1
        try:
            yield
        finally:
            self.release()

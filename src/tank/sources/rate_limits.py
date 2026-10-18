"""Process-shared rolling HTTP budgets; only configured capacities are enforced."""

from __future__ import annotations

import asyncio
import time
from collections import deque


class SharedRequestBudgets:
    def __init__(self):
        self.windows = {}
        self.locks = {}

    async def acquire(self, key, capacity, window_s, *, weight=1):
        if capacity is None:
            return
        capacity = float(capacity)
        weight = float(weight)
        if capacity <= 0 or weight <= 0 or weight > capacity:
            raise ValueError("Invalid configured request budget")
        # Different limits for one provider are separate constraints over the same traffic.
        bucket = (key, float(window_s), capacity)
        lock = self.locks.setdefault(bucket, asyncio.Lock())
        history = self.windows.setdefault(bucket, deque())
        while True:
            async with lock:
                now = time.monotonic()
                while history and now - history[0][0] >= window_s:
                    history.popleft()
                if sum(row[1] for row in history) + weight <= capacity:
                    history.append((now, weight))
                    return
                delay = max(0.001, window_s - (now - history[0][0]))
            # Admission and accounting stay atomic. A request that cannot fit
            # must release the bucket so smaller requests can use free capacity.
            await asyncio.sleep(delay)


shared_request_budgets = SharedRequestBudgets()

"""Process-shared rolling HTTP budgets; only configured capacities are enforced."""

from __future__ import annotations

import asyncio
import time
from collections import deque


class SharedRequestBudgets:
    def __init__(self):
        self.windows = {}
        self.locks = {}
        self.urgent = {}  # bucket -> requests with a negative priority waiting for it

    async def acquire(self, key, capacity, window_s, *, weight=1, priority=0):
        """Wait until `weight` fits the rolling window. While a request with a negative
        priority waits for a bucket, requests of priority >= 0 leave its capacity to it."""
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
        urgent = priority < 0
        if urgent:
            self.urgent[bucket] = self.urgent.get(bucket, 0) + 1
        try:
            while True:
                async with lock:
                    now = time.monotonic()
                    while history and now - history[0][0] >= window_s:
                        history.popleft()
                    yielding = not urgent and self.urgent.get(bucket, 0) > 0
                    if not yielding and sum(row[1] for row in history) + weight <= capacity:
                        history.append((now, weight))
                        return
                    delay = (
                        min(0.05, window_s)
                        if yielding or not history
                        else max(0.001, window_s - (now - history[0][0]))
                    )
                # Admission and accounting stay atomic. A request that cannot fit
                # must release the bucket so smaller requests can use free capacity.
                await asyncio.sleep(delay)
        finally:
            if urgent:
                self.urgent[bucket] -= 1


shared_request_budgets = SharedRequestBudgets()

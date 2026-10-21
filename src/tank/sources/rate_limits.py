"""Process-shared rolling HTTP budgets; only configured capacities are enforced."""

from __future__ import annotations

import asyncio
import time
from collections import deque

# A shared admission answers with the wait until capacity frees; re-ask at least this often.
LEDGER_MAX_WAIT_S = 5.0


class RequestLedger:
    """ops.request_budget_ledger (migration 046): every process on this IP admits from the
    same budgets, tank-capture included. One autocommit connection outside the pool: each
    admission is its own short transaction and no pooled connection waits on a budget."""

    def __init__(self, database_url, owner):
        self.database_url, self.owner = database_url, owner
        self.connection = None
        self.closed = False
        self.lock = asyncio.Lock()

    async def _execute(self, query, args):
        from psycopg import AsyncConnection, OperationalError

        async with self.lock:
            for attempt in range(2):
                if self.closed:
                    raise RuntimeError("request ledger closed")
                if self.connection is None or self.connection.closed:
                    self.connection = await AsyncConnection.connect(self.database_url, autocommit=True)
                try:
                    return await (await self.connection.execute(query, args)).fetchone()
                except OperationalError:
                    await self.close()
                    if attempt:
                        raise

    async def call(self, query, args):
        # A cancelled caller leaves the statement to finish: the connection stays usable.
        return await asyncio.shield(self._execute(query, args))

    async def wait_s(self, bucket, capacity, window_s, weight, host):
        row = await self.call(
            "SELECT ops.request_budget_acquire(%s,%s::numeric,%s::float8,%s::numeric,%s,%s)",
            (bucket, capacity, window_s, weight, self.owner, host),
        )
        return float(row[0])

    async def block(self, host, until, reason):
        await self.call("SELECT ops.request_budget_block(%s,%s,%s,%s)", (host, until, reason, self.owner))

    async def close(self):
        connection, self.connection = self.connection, None
        if connection is not None:
            await connection.close()

    async def shutdown(self):
        self.closed = True
        async with self.lock:
            await self.close()


class SharedRequestBudgets:
    def __init__(self):
        self.windows = {}
        self.locks = {}
        self.urgent = {}  # bucket -> requests with a negative priority waiting for it
        self.ledger = None

    def configure(self, repo, settings, *, owner="tank-core"):
        """Admit from the PostgreSQL ledger shared with tank-capture; an in-memory repository
        (tests) keeps budgets in memory."""
        from tank.storage import PostgresRepository

        if not isinstance(repo, PostgresRepository):
            return None
        self.ledger = RequestLedger(repo.database_url, owner)
        return self.ledger

    async def release(self, ledger):
        if ledger is None:
            return
        if self.ledger is ledger:
            self.ledger = None
        await ledger.shutdown()

    async def block(self, host, until, reason):
        """Close a provider host for every process until `until` (ledger mode only)."""
        ledger = self.ledger
        if ledger is not None:
            await ledger.block(host, until, reason)

    async def acquire(self, key, capacity, window_s, *, weight=1, priority=0, host=None):
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
        ledger = self.ledger
        if ledger is not None:
            return await self._acquire_shared(ledger, bucket, weight, priority, host)
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

    async def _acquire_shared(self, ledger, bucket, weight, priority, host):
        key, window_s, capacity = bucket
        # tank-capture names buckets the same way: "<key>|<window_s>|<capacity>" as floats.
        name = f"{key}|{window_s!r}|{capacity!r}"
        urgent = priority < 0
        if urgent:
            self.urgent[bucket] = self.urgent.get(bucket, 0) + 1
        try:
            while True:
                if not urgent and self.urgent.get(bucket, 0) > 0:
                    await asyncio.sleep(min(0.05, window_s))
                    continue
                wait = await ledger.wait_s(name, capacity, window_s, weight, host)
                if wait <= 0:
                    return
                await asyncio.sleep(min(wait, LEDGER_MAX_WAIT_S))
        finally:
            if urgent:
                self.urgent[bucket] -= 1


shared_request_budgets = SharedRequestBudgets()

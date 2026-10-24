"""Process-shared rolling HTTP budgets; only configured capacities are enforced."""

import asyncio
import time
from collections import deque
from collections.abc import Sequence
from datetime import datetime

from psycopg import AsyncConnection, OperationalError
from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.settings import Settings
from tank.storage import Repository

LEDGER_MAX_WAIT_S = 5.0


class BudgetBucket(DomainModel):
    key: str
    window_s: float = Field(gt=0, allow_inf_nan=False)
    capacity: float = Field(gt=0, allow_inf_nan=False)


class BudgetAdmission(DomainModel):
    at: float
    weight: float = Field(gt=0, allow_inf_nan=False)


class RequestLedger:
    """ops.request_budget_ledger (migration 046): every process on this IP admits from the
    same budgets, tank-capture included. One autocommit connection outside the pool: each
    admission is its own short transaction and no pooled connection waits on a budget."""

    def __init__(self, database_url: str, owner: str) -> None:
        self.database_url, self.owner = database_url, owner
        self.connection: AsyncConnection[tuple[JsonValue, ...]] | None = None
        self.closed = False
        self.lock = asyncio.Lock()

    async def _execute(
        self, query: str, args: Sequence[str | float | int | datetime | None]
    ) -> tuple[JsonValue, ...] | None:

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

        raise RuntimeError("Request ledger exhausted its reconnect attempts")

    async def call(
        self, query: str, args: Sequence[str | float | int | datetime | None]
    ) -> tuple[JsonValue, ...] | None:

        return await asyncio.shield(self._execute(query, args))

    async def wait_s(
        self, bucket: str, capacity: float, window_s: float, weight: float, host: str | None
    ) -> float:
        row = await self.call(
            "SELECT ops.request_budget_acquire(%s,%s::numeric,%s::float8,%s::numeric,%s,%s)",
            (bucket, capacity, window_s, weight, self.owner, host),
        )
        if row is None or len(row) != 1 or isinstance(row[0], bool) or not isinstance(row[0], (float, int)):
            raise ValueError("Request ledger returned no numeric admission delay")
        return float(row[0])

    async def block(self, host: str, until: datetime, reason: str) -> None:
        await self.call("SELECT ops.request_budget_block(%s,%s,%s,%s)", (host, until, reason, self.owner))

    async def close(self) -> None:
        connection, self.connection = self.connection, None
        if connection is not None:
            await connection.close()

    async def shutdown(self) -> None:
        self.closed = True
        async with self.lock:
            await self.close()


class SharedRequestBudgets:
    def __init__(self) -> None:
        self.windows: dict[BudgetBucket, deque[BudgetAdmission]] = {}
        self.locks: dict[BudgetBucket, asyncio.Lock] = {}
        self.urgent: dict[BudgetBucket, int] = {}
        self.ledger: RequestLedger | None = None

    def configure(
        self, repo: Repository, settings: Settings, *, owner: str = "tank-core"
    ) -> RequestLedger | None:
        """Admit from the PostgreSQL ledger shared with tank-capture; an in-memory repository
        (tests) keeps budgets in memory."""
        from tank.storage import PostgresRepository

        if not isinstance(repo, PostgresRepository):
            return None
        self.ledger = RequestLedger(repo.database_url, owner)
        return self.ledger

    async def release(self, ledger: RequestLedger | None) -> None:
        if ledger is None:
            return
        if self.ledger is ledger:
            self.ledger = None
        await ledger.shutdown()

    async def block(self, host: str, until: datetime, reason: str) -> None:
        """Close a provider host for every process until `until` (ledger mode only)."""
        ledger = self.ledger
        if ledger is not None:
            await ledger.block(host, until, reason)

    async def acquire(
        self,
        key: str,
        capacity: float | None,
        window_s: float,
        *,
        weight: float = 1,
        priority: int = 0,
        host: str | None = None,
    ) -> None:
        """Wait until `weight` fits the rolling window. While a request with a negative
        priority waits for a bucket, requests of priority >= 0 leave its capacity to it."""
        if capacity is None:
            return
        capacity = float(capacity)
        weight = float(weight)
        if capacity <= 0 or weight <= 0 or weight > capacity:
            raise ValueError("Invalid configured request budget")

        bucket = BudgetBucket(key=key, window_s=float(window_s), capacity=capacity)
        BudgetAdmission(at=time.monotonic(), weight=weight)
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
                    while history and now - history[0].at >= window_s:
                        history.popleft()
                    yielding = not urgent and self.urgent.get(bucket, 0) > 0
                    if not yielding and sum(row.weight for row in history) + weight <= capacity:
                        history.append(BudgetAdmission(at=now, weight=weight))
                        return
                    delay = (
                        min(0.05, window_s)
                        if yielding or not history
                        else max(0.001, window_s - (now - history[0].at))
                    )

                await asyncio.sleep(delay)
        finally:
            if urgent:
                self.urgent[bucket] -= 1

    async def _acquire_shared(
        self, ledger: RequestLedger, bucket: BudgetBucket, weight: float, priority: int, host: str | None
    ) -> None:
        key, window_s, capacity = bucket.key, bucket.window_s, bucket.capacity

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

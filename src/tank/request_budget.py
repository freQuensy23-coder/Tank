"""Process-shared rolling HTTP budgets; only configured capacities are enforced."""

import asyncio
import math
import time
from collections import deque
from collections.abc import Sequence
from contextlib import AsyncExitStack
from datetime import datetime
from typing import Self

from psycopg import AsyncConnection, OperationalError
from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.storage import Repository

LEDGER_MAX_WAIT_S = 5.0


class BudgetBucket(DomainModel):
    key: str = Field(min_length=1)
    window_s: float = Field(gt=0, allow_inf_nan=False)
    capacity: float = Field(gt=0, allow_inf_nan=False)


class BudgetAdmission(DomainModel):
    at: float = Field(allow_inf_nan=False)
    weight: float = Field(gt=0, allow_inf_nan=False)


class BudgetConstraint(BudgetBucket):
    weight: float = Field(gt=0, allow_inf_nan=False)
    host: str | None = None

    @model_validator(mode="after")
    def usable_weight(self) -> Self:
        if self.weight > self.capacity:
            raise ValueError("Request weight exceeds its configured capacity")
        return self

    def bucket(self) -> BudgetBucket:
        return BudgetBucket(key=self.key, window_s=self.window_s, capacity=self.capacity)


class _BudgetBlocked(Exception):
    def __init__(self, delay: float) -> None:
        self.delay = delay
        super().__init__("Configured request budget is not currently available")


def ledger_key(bucket: BudgetBucket) -> str:
    return f"{bucket.key}|{bucket.window_s!r}|{bucket.capacity!r}"


def normalized_constraints(values: Sequence[BudgetConstraint]) -> tuple[BudgetConstraint, ...]:
    if not values:
        raise ValueError("An admission group requires at least one configured constraint")
    constraints = tuple(BudgetConstraint.model_validate(value) for value in values)
    keys = [ledger_key(value) for value in constraints]
    if len(set(keys)) != len(keys):
        raise ValueError("An admission group cannot repeat a bucket identity")
    return tuple(sorted(constraints, key=ledger_key))


def admission_delay(row: tuple[JsonValue, ...] | None) -> float:
    if row is None or len(row) != 1 or isinstance(row[0], bool) or not isinstance(row[0], (float, int)):
        raise ValueError("Request ledger returned no numeric admission delay")
    value = float(row[0])
    if not math.isfinite(value) or value < 0:
        raise ValueError("Request ledger returned an invalid admission delay")
    return value


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
        return await self.wait_many(
            [BudgetConstraint(key=bucket, capacity=capacity, window_s=window_s, weight=weight, host=host)],
            serialized_names=True,
        )

    async def wait_many(
        self, constraints: Sequence[BudgetConstraint], *, serialized_names: bool = False
    ) -> float:
        values = normalized_constraints(constraints)
        return await asyncio.shield(self._wait_many(values, serialized_names=serialized_names))

    async def _wait_many(self, constraints: Sequence[BudgetConstraint], *, serialized_names: bool) -> float:
        async with self.lock:
            for attempt in range(2):
                if self.closed:
                    raise RuntimeError("request ledger closed")
                if self.connection is None or self.connection.closed:
                    self.connection = await AsyncConnection.connect(self.database_url, autocommit=True)
                connection = self.connection
                try:
                    async with connection.transaction():
                        delay = 0.0
                        for constraint in constraints:
                            name = constraint.key if serialized_names else ledger_key(constraint)
                            row = await (
                                await connection.execute(
                                    "SELECT ops.request_budget_acquire(%s,%s::numeric,%s::float8,%s::numeric,%s,%s)",
                                    (
                                        name,
                                        constraint.capacity,
                                        constraint.window_s,
                                        constraint.weight,
                                        self.owner,
                                        constraint.host,
                                    ),
                                )
                            ).fetchone()
                            delay = max(delay, admission_delay(row))
                        if delay > 0:
                            raise _BudgetBlocked(delay)
                    return 0.0
                except _BudgetBlocked as blocked:
                    return blocked.delay
                except OperationalError:
                    await self.close()
                    if attempt:
                        raise
        raise RuntimeError("Request ledger exhausted its grouped reconnect attempts")

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

    def configure(self, repo: Repository, *, owner: str) -> RequestLedger | None:
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
        await self.acquire_many(
            [BudgetConstraint(key=key, capacity=capacity, window_s=window_s, weight=weight, host=host)],
            priority=priority,
        )

    async def acquire_many(self, constraints: Sequence[BudgetConstraint], *, priority: int = 0) -> None:
        values = normalized_constraints(constraints)
        buckets = [value.bucket() for value in values]
        urgent = priority < 0
        if urgent:
            for bucket in buckets:
                self.urgent[bucket] = self.urgent.get(bucket, 0) + 1
        try:
            while True:
                if not urgent and any(self.urgent.get(bucket, 0) > 0 for bucket in buckets):
                    await asyncio.sleep(min(0.05, *(bucket.window_s for bucket in buckets)))
                    continue
                if self.ledger is not None:
                    if len(values) == 1:
                        constraint = values[0]
                        wait = await self.ledger.wait_s(
                            ledger_key(constraint),
                            constraint.capacity,
                            constraint.window_s,
                            constraint.weight,
                            constraint.host,
                        )
                    else:
                        wait = await self.ledger.wait_many(values)
                    if wait <= 0:
                        return
                    await asyncio.sleep(min(wait, LEDGER_MAX_WAIT_S))
                    continue
                async with AsyncExitStack() as stack:
                    for bucket in buckets:
                        await stack.enter_async_context(self.locks.setdefault(bucket, asyncio.Lock()))
                    now = time.monotonic()
                    yielding = not urgent and any(self.urgent.get(bucket, 0) > 0 for bucket in buckets)
                    delay = min(0.05, *(bucket.window_s for bucket in buckets)) if yielding else 0.0
                    for constraint, bucket in zip(values, buckets, strict=True):
                        history = self.windows.setdefault(bucket, deque())
                        while history and now - history[0].at >= bucket.window_s:
                            history.popleft()
                        if sum(row.weight for row in history) + constraint.weight > bucket.capacity:
                            delay = max(delay, max(0.001, bucket.window_s - (now - history[0].at)))
                    if delay <= 0:
                        for constraint, bucket in zip(values, buckets, strict=True):
                            self.windows[bucket].append(BudgetAdmission(at=now, weight=constraint.weight))
                        return
                await asyncio.sleep(delay)
        finally:
            if urgent:
                for bucket in buckets:
                    self.urgent[bucket] -= 1


shared_request_budgets = SharedRequestBudgets()

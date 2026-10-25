"""Process-shared rolling HTTP budgets; only configured capacities are enforced."""

import asyncio
import math
import time
from collections import deque
from collections.abc import Callable, Sequence
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from typing import Protocol, Self

from psycopg import AsyncConnection, OperationalError
from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.util import parse_iso_time

LEDGER_MAX_WAIT_S = 5.0


class BudgetClock(Protocol):
    def monotonic(self) -> float: ...
    def now(self) -> datetime: ...
    async def sleep(self, delay: float) -> None: ...


class SystemBudgetClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, delay: float) -> None:
        await asyncio.sleep(delay)


class BudgetBucket(DomainModel):
    key: str = Field(min_length=1)
    window_s: float = Field(gt=0, allow_inf_nan=False)
    capacity: float = Field(gt=0, allow_inf_nan=False)


class BudgetAdmission(DomainModel):
    at: float = Field(allow_inf_nan=False)
    weight: float = Field(gt=0, allow_inf_nan=False)


class BudgetConstraint(BudgetBucket):
    weight: float = Field(gt=0, allow_inf_nan=False)
    host: str | None = Field(default=None, min_length=1)

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
        if not database_url.strip() or not owner.strip():
            raise ValueError("PostgreSQL admission requires a database and explicit owner")
        self.database_url, self.owner = database_url, owner
        self.clock = SystemBudgetClock()
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

    async def wait_many(self, constraints: Sequence[BudgetConstraint]) -> float:
        values = normalized_constraints(constraints)
        return await asyncio.shield(self._wait_many(values))

    async def _wait_many(self, constraints: Sequence[BudgetConstraint]) -> float:
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
                            name = ledger_key(constraint)
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

    async def host_delay(self, host: str) -> float:
        if not host:
            raise ValueError("Host waits require an actual destination")
        row = await self.call(
            "SELECT coalesce((SELECT greatest(0.0, extract(epoch FROM until - clock_timestamp())) "
            "FROM ops.request_budget_blocks WHERE host = %s), 0.0)::float8",
            (host,),
        )
        return admission_delay(row)

    async def block(self, host: str, until: datetime, reason: str) -> None:
        if not host or not reason:
            raise ValueError("Host blocks require a host and reason")
        until = parse_iso_time(until)
        await self.call("SELECT ops.request_budget_block(%s,%s,%s,%s)", (host, until, reason, self.owner))

    async def close(self) -> None:
        connection, self.connection = self.connection, None
        if connection is not None:
            await connection.close()

    async def shutdown(self) -> None:
        self.closed = True
        async with self.lock:
            await self.close()


class MemoryBudgetBackend:
    """Explicit process-local backend; never selected from a missing database capability."""

    def __init__(self, clock: BudgetClock) -> None:
        self.clock = clock
        self.windows: dict[BudgetBucket, deque[BudgetAdmission]] = {}
        self.locks: dict[BudgetBucket, asyncio.Lock] = {}
        self.blocked_hosts: dict[str, float] = {}
        self.closed = False

    async def wait_many(self, constraints: Sequence[BudgetConstraint], *, ready: Callable[[], bool]) -> float:
        if self.closed:
            raise RuntimeError("Memory request budget backend is closed")
        values = normalized_constraints(constraints)
        buckets = [value.bucket() for value in values]
        async with AsyncExitStack() as stack:
            for bucket in buckets:
                await stack.enter_async_context(self.locks.setdefault(bucket, asyncio.Lock()))
            if self.closed:
                raise RuntimeError("Memory request budget backend is closed")
            if not ready():
                return min(0.05, *(bucket.window_s for bucket in buckets))
            now = self.clock.monotonic()
            if not math.isfinite(now):
                raise ValueError("Memory admission requires a finite operational clock")
            delay = 0.0
            for constraint, bucket in zip(values, buckets, strict=True):
                if constraint.host is not None:
                    blocked_until = self.blocked_hosts.get(constraint.host)
                    if blocked_until is not None:
                        delay = max(delay, blocked_until - now)
                history = self.windows.setdefault(bucket, deque())
                while history and now - history[0].at >= bucket.window_s:
                    history.popleft()
                if sum(row.weight for row in history) + constraint.weight > bucket.capacity:
                    delay = max(delay, max(0.001, bucket.window_s - (now - history[0].at)))
            if delay <= 0:
                for constraint, bucket in zip(values, buckets, strict=True):
                    self.windows[bucket].append(BudgetAdmission(at=now, weight=constraint.weight))
            return max(delay, 0.0)

    async def host_delay(self, host: str) -> float:
        if self.closed:
            raise RuntimeError("Memory request budget backend is closed")
        if not host:
            raise ValueError("Host waits require an actual destination")
        now = self.clock.monotonic()
        if not math.isfinite(now):
            raise ValueError("Memory admission requires a finite operational clock")
        deadline = self.blocked_hosts.get(host)
        return 0.0 if deadline is None else max(0.0, deadline - now)

    async def block(self, host: str, until: datetime, reason: str) -> None:
        if self.closed:
            raise RuntimeError("Memory request budget backend is closed")
        if not host or not reason:
            raise ValueError("Host blocks require a host and reason")
        duration = max(0.0, (parse_iso_time(until) - parse_iso_time(self.clock.now())).total_seconds())
        now = self.clock.monotonic()
        if not math.isfinite(now):
            raise ValueError("Memory admission requires a finite operational clock")
        deadline = now + duration
        previous = self.blocked_hosts.get(host)
        self.blocked_hosts[host] = deadline if previous is None else max(previous, deadline)

    async def shutdown(self) -> None:
        self.closed = True


type BudgetBackend = MemoryBudgetBackend | RequestLedger


class RequestBudgets(Protocol):
    async def wait_host(self, host: str) -> None: ...
    async def acquire_many(self, constraints: Sequence[BudgetConstraint], *, priority: int = 0) -> None: ...
    async def block(self, host: str, until: datetime, reason: str) -> None: ...


class SharedRequestBudgets:
    """Fair admission against one explicitly chosen backend."""

    def __init__(self, backend: BudgetBackend) -> None:
        if not isinstance(backend, (MemoryBudgetBackend, RequestLedger)):
            raise TypeError("Request admission requires an explicit supported backend")
        self.backend = backend
        self.urgent: dict[BudgetBucket, int] = {}

    async def wait_host(self, host: str) -> None:
        while True:
            delay = await self.backend.host_delay(host)
            if delay <= 0:
                return
            await self.backend.clock.sleep(min(delay, LEDGER_MAX_WAIT_S))

    async def block(self, host: str, until: datetime, reason: str) -> None:
        await self.backend.block(host, until, reason)

    async def acquire(
        self,
        key: str,
        capacity: float,
        window_s: float,
        *,
        weight: float = 1,
        priority: int = 0,
        host: str | None = None,
    ) -> None:
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
                    await self.backend.clock.sleep(min(0.05, *(bucket.window_s for bucket in buckets)))
                    continue
                if isinstance(self.backend, MemoryBudgetBackend):
                    delay = await self.backend.wait_many(
                        values,
                        ready=lambda: urgent or not any(self.urgent.get(bucket, 0) > 0 for bucket in buckets),
                    )
                else:
                    delay = await self.backend.wait_many(values)
                if delay <= 0:
                    return
                await self.backend.clock.sleep(min(delay, LEDGER_MAX_WAIT_S))
        finally:
            if urgent:
                for bucket in buckets:
                    self.urgent[bucket] -= 1

    async def shutdown(self) -> None:
        await self.backend.shutdown()


class RequestBudgetScope:
    """Runtime lifetime binding; an absent binding always refuses admission."""

    def __init__(self) -> None:
        self._active: SharedRequestBudgets | None = None

    def bind(self, backend: BudgetBackend) -> SharedRequestBudgets:
        engine = SharedRequestBudgets(backend)
        self._active = engine
        return engine

    def require_binding(self) -> SharedRequestBudgets:
        if self._active is None:
            raise RuntimeError("Request admission backend has not been bound")
        return self._active

    async def release(self, binding: SharedRequestBudgets) -> None:
        if self._active is binding:
            self._active = None
        await binding.shutdown()

    async def wait_host(self, host: str) -> None:
        await self.require_binding().wait_host(host)

    async def acquire_many(self, constraints: Sequence[BudgetConstraint], *, priority: int = 0) -> None:
        await self.require_binding().acquire_many(constraints, priority=priority)

    async def block(self, host: str, until: datetime, reason: str) -> None:
        await self.require_binding().block(host, until, reason)


shared_request_budgets = RequestBudgetScope()

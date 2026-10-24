"""Transactional PostgreSQL storage. MemoryRepository is an explicit test double only."""

import asyncio
import copy
import hashlib
import logging
import random
import time
import weakref
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from contextvars import ContextVar
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, Protocol, Self

from psycopg import AsyncConnection
from psycopg.abc import PQGen
from psycopg_pool import AsyncConnectionPool
from pydantic import JsonValue

from tank.clickhouse_market import ClickHouse
from tank.contracts import DomainModel
from tank.contracts.jobs import JobEnvelope, JobRetryState
from tank.market_codec import StoredRow, StoredValue
from tank.market_codec import decode as decode_market
from tank.market_schema import MARKET_COLUMNS
from tank.market_storage import (
    iter_market_projection,
    latest_market_records,
    market_dependency_head,
    market_series_counts,
    put_market_batch,
)
from tank.models import (
    APPEND_ONLY,
    SERIAL_TABLES,
    TABLE_COLUMNS,
    TIMESTAMP_PRECISION_FIELDS,
    Record,
)
from tank.time_precision import Timestamp, collection_clock, offset_schedule, propagate_timestamp
from tank.util import JsonInput, TimeInput, iso, jsonable, new_id, parse_time, utcnow


class TaskOwner(Protocol):
    def done(self) -> bool: ...


class AdvisoryLockResult(DomainModel):
    ok: bool


class MigrationAdoption(DomainModel):
    adopt: bool


class HeldCaps(DomainModel):
    held: list[str]


class ClaimResult(DomainModel):
    document: Record | None
    analysis_busy: bool
    fresh_event: bool
    deferred: int


class ImmutableDocument(DomainModel):
    document: Record
    immutable: bool


class ImmutableRecordError(ValueError):
    pass


class Repository(Protocol):
    async def open(self): ...
    async def close(self): ...
    async def migrate(self): ...
    async def put(self, collection: str, key: str, record: Record, *, immutable: bool = False) -> Record: ...
    async def put_market_batch(self, collection: str, records, *, keys=None) -> list[Record]: ...
    async def latest_market_records(
        self,
        collection: str,
        *,
        group_by=("instrument_id",),
        time_field="ts",
        filters=None,
        cutoff=None,
        assembled_at=None,
        start=None,
    ) -> list[Record]: ...
    async def market_dependency_head(
        self,
        collection,
        semantic_field,
        cutoff,
        *,
        start=None,
        filters=None,
        instrument_ids=None,
        assembled_at=None,
        order="availability",
    ) -> Record | None: ...
    def iter_market_projection(
        self,
        collection,
        fields,
        time_field,
        *,
        start=None,
        end=None,
        filters=None,
        instrument_ids=None,
        assembled_at=None,
        batch_size=1000,
    ): ...
    async def market_series_counts(
        self, instrument_ids, start, cutoff, assembled_at=None
    ) -> dict[str, int]: ...
    async def get(self, collection: str, key: str) -> Record | None: ...
    async def list(
        self, collection: str, *, filters: dict | None = None, limit: int = 1000
    ) -> list[Record]: ...
    async def delete(self, collection: str, key: str): ...
    def transaction(self): ...
    def lock(self, key: str): ...
    async def enqueue(
        self, kind, payload, idem_key, *, priority=1, run_after=None, expires_at=None, max_attempts=3
    ): ...
    async def claim_job(self): ...
    async def finish_job(self, job_id): ...
    async def fail_job(self, job_id, error, *, retry_after=1): ...
    async def recover_jobs(self): ...
    async def prune_market(self, before, *, book_before=None, trades_before=None): ...
    async def prune_jobs(self, before): ...
    async def preserved_market_windows(self): ...
    async def latest_before(self, collection, time_field, cutoff, *, filters=None): ...
    def iter_all(self, collection, *, filters=None, batch_size=1000): ...
    def iter_metadata(self, collection, fields, *, filters=None, batch_size=1000): ...
    def iter_late_arrivals(
        self,
        collection,
        arrival_field,
        market_field,
        *,
        since=None,
        end=None,
        market_end,
        filters=None,
        batch_size=1000,
    ): ...
    async def list_latest(self, collection, time_field, *, filters=None, limit=100, start=None, end=None): ...
    async def list_range(
        self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
    ): ...
    def iter_range(self, collection, time_field, *, start=None, end=None, filters=None, batch_size=10000): ...


log = logging.getLogger(__name__)

BASELINE = "001_schema.sql"

RUNNING_CAPS = {"REACTION_CAPTURE": 1}


EARLIEST_EXPIRY_FIRST = ("REACTION_CAPTURE",)
CLAIM_SLOW_S, DELIVER_LATE_S = 0.5, 1.0


YIELDS_TO_ANALYSIS = ("REACTION_CAPTURE",)
ANALYSIS_TYPES = ("RUN_MARKET_TRUTH", "FILTER_ITEM", "ROUTE_ITEM", "AGENT_MESSAGE")
FRESH_EVENT_S, EXPIRY_SAFETY_S = 600, 1200
DEFERRAL_LOG_S = 60


def _time_field(record: Mapping[str, StoredValue], field: str) -> datetime:
    value = record[field]
    if isinstance(value, bool) or not isinstance(value, (str, int, float, datetime, Decimal)):
        raise ValueError(f"{field} requires a timestamp")
    return parse_time(value)


def _text_field(record: Mapping[str, StoredValue], field: str) -> str:
    value = record[field]
    if not isinstance(value, str):
        raise ValueError(f"{field} requires text")
    return value


def _analysis_gate_memory(data: dict[str, dict[str, Record]], now: datetime) -> tuple[bool, bool]:
    """Return whether captures yield to pending analysis or fresh uncarded events."""
    jobs = (JobEnvelope.model_validate(row) for row in data.get("ops.jobs", {}).values())
    busy = any(
        job.type in ANALYSIS_TYPES
        and (
            job.status == "RUNNING"
            or job.status == "QUEUED"
            and parse_time(job.run_after) <= now
            and (job.expires_at is None or parse_time(job.expires_at) > now)
        )
        for job in jobs
    )
    carded = {
        _text_field(alert, "event_id")
        for alert in data.get("out.alerts", {}).values()
        if alert.get("kind") == "FULL"
    }
    fresh = any(
        event.get("known_to_us_at") is not None
        and _time_field(event, "known_to_us_at") > now - timedelta(seconds=FRESH_EVENT_S)
        and _text_field(event, "event_id") not in carded
        for event in data.get("evt.events", {}).values()
    )
    return busy, fresh


def _table(collection):
    if collection not in TABLE_COLUMNS:
        raise ValueError(f"Unknown collection: {collection}")
    return collection


def _queue_anchor(value: TimeInput | Timestamp | None, field: str) -> Timestamp:
    if isinstance(value, Timestamp):
        return value
    return propagate_timestamp({field: value}, field)


def _stamp_job_clock_fields(record: Record) -> Record:
    """Explicit lifecycle clock policy for the in-memory test double, mirroring SQL026."""
    created = (
        collection_clock() if record.get("created_at") is None else propagate_timestamp(record, "created_at")
    )
    record.update(created.fields("created_at"))
    if record["status"] in ("DONE", "FAILED", "EXPIRED"):
        finished = (
            collection_clock()
            if record.get("finished_at") is None
            else propagate_timestamp(record, "finished_at")
        )
    else:
        finished = Timestamp(None, None, "PENDING")
    record.update(finished.fields("finished_at"))
    return record


def _job(
    kind: str,
    payload: Mapping[str, JsonInput],
    idem_key: str,
    priority: int,
    run_after: TimeInput | Timestamp | None,
    expires_at: TimeInput | Timestamp | None,
    max_attempts: int,
) -> Record:
    if not 0 <= priority <= 4 or max_attempts < 1:
        raise ValueError("priority must be0..4 and max_attempts positive")
    default_expiry = {
        "FILTER_ITEM": 21600,
        "ROUTE_ITEM": 21600,
        "AGENT_MESSAGE": 86400,
        "RUN_MARKET_TRUTH": 1800,
        "DELIVER": 86400,
        "EVAL_CONDITIONS": 300,
        "REACTION_CAPTURE": 21600,
        "SCOUT_REVIEW": 604800,
    }
    created = collection_clock()
    scheduled = created if run_after is None else _queue_anchor(run_after, "run_after")
    if scheduled.value is None:
        raise ValueError("run_after requires a non-null timestamp")
    if expires_at is None and kind in default_expiry:
        expiry = offset_schedule(
            scheduled if kind == "REACTION_CAPTURE" else created, timedelta(seconds=default_expiry[kind])
        )
    elif expires_at is not None:
        expiry = _queue_anchor(expires_at, "expires_at")
    else:
        expiry = Timestamp(None, None, "NOT_APPLICABLE")
    result: Record = {
        "job_id": new_id("JOB"),
        "type": kind,
        "priority": priority,
        "idem_key": idem_key,
        "payload": jsonable(dict(payload)),
        "status": "QUEUED",
        "attempts": 0,
        "max_attempts": max_attempts,
        "last_error": None,
    }
    for field, stamp in (
        ("created_at", created),
        ("run_after", scheduled),
        ("expires_at", expiry),
        ("finished_at", Timestamp(None, None, "PENDING")),
    ):
        result.update(stamp.fields(field))
    return JobEnvelope.model_validate(result).model_dump(mode="json", exclude_unset=True)


class RepositoryQueries:
    async def prune_market(self, before, *, book_before=None, trades_before=None):
        return await _prune_market(self, before, book_before=book_before, trades_before=trades_before)

    async def list_range(
        self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
    ):
        return await _list_range(
            self, collection, time_field, start=start, end=end, filters=filters, limit=limit, after=after
        )

    async def page_range(
        self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
    ):
        return await _range_page(
            self, collection, time_field, start=start, end=end, filters=filters, limit=limit, after=after
        )

    async def iter_range(
        self, collection, time_field, *, start=None, end=None, filters=None, batch_size=10000
    ):
        async for row in _iter_range(
            self, collection, time_field, start=start, end=end, filters=filters, batch_size=batch_size
        ):
            yield row

    async def prune_jobs(self, before):
        return await _prune_jobs(self, before)

    async def preserved_market_windows(self):
        return await _preserved_windows(self)

    async def latest_before(self, collection, time_field, cutoff, *, filters=None):
        return await _latest_before(self, collection, time_field, cutoff, filters=filters)

    async def iter_all(self, collection, *, filters=None, batch_size=1000):
        async for row in _iter_all(self, collection, filters=filters, batch_size=batch_size):
            yield row

    async def list_latest(self, collection, time_field, *, filters=None, limit=100, start=None, end=None):
        return await _list_latest(
            self, collection, time_field, filters=filters, limit=limit, start=start, end=end
        )

    async def iter_late_arrivals(
        self,
        collection,
        arrival_field,
        market_field,
        *,
        since=None,
        end=None,
        market_end,
        filters=None,
        batch_size=1000,
    ):
        async for row in _iter_late_arrivals(
            self,
            collection,
            arrival_field,
            market_field,
            since=since,
            end=end,
            market_end=market_end,
            filters=filters,
            batch_size=batch_size,
        ):
            yield row

    async def iter_metadata(self, collection, fields, *, filters=None, batch_size=1000):
        async for row in _iter_metadata(self, collection, fields, filters=filters, batch_size=batch_size):
            yield row

    async def put_market_batch(self, collection, records, *, keys=None):
        return await put_market_batch(self, collection, records, keys=keys)

    async def latest_market_records(
        self,
        collection,
        *,
        group_by=("instrument_id",),
        time_field="ts",
        filters=None,
        cutoff=None,
        assembled_at=None,
        start=None,
        instrument_ids=None,
    ):
        return await latest_market_records(
            self,
            collection,
            group_by=group_by,
            time_field=time_field,
            filters=filters,
            cutoff=cutoff,
            assembled_at=assembled_at,
            start=start,
            instrument_ids=instrument_ids,
        )

    async def market_dependency_head(
        self,
        collection,
        semantic_field,
        cutoff,
        *,
        start=None,
        filters=None,
        instrument_ids=None,
        assembled_at=None,
        order="availability",
        orders=None,
    ):
        return await market_dependency_head(
            self,
            collection,
            semantic_field,
            cutoff,
            start=start,
            filters=filters,
            instrument_ids=instrument_ids,
            assembled_at=assembled_at,
            order=order,
            orders=orders,
        )

    async def iter_market_projection(
        self,
        collection,
        fields,
        time_field,
        *,
        start=None,
        end=None,
        filters=None,
        instrument_ids=None,
        assembled_at=None,
        batch_size=1000,
    ):
        async for row in iter_market_projection(
            self,
            collection,
            fields,
            time_field,
            start=start,
            end=end,
            filters=filters,
            instrument_ids=instrument_ids,
            assembled_at=assembled_at,
            batch_size=batch_size,
        ):
            yield row

    async def market_series_counts(self, instrument_ids, start, cutoff, assembled_at=None):
        return await market_series_counts(self, instrument_ids, start, cutoff, assembled_at)


class QueueMixin(RepositoryQueries, ABC):
    @abstractmethod
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...

    @abstractmethod
    async def get(self, collection: str, key: str) -> Record | None: ...

    @abstractmethod
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> Record: ...

    @abstractmethod
    async def list(
        self, collection: str, *, filters: Record | None = None, limit: int = 1000
    ) -> list[Record]: ...

    def _exact_predicate(self, collection, filters):
        return _exact_predicate(collection, filters)

    async def enqueue(
        self,
        kind: str,
        payload: Mapping[str, JsonInput],
        idem_key: str,
        *,
        priority: int = 1,
        run_after: TimeInput | Timestamp | None = None,
        expires_at: TimeInput | Timestamp | None = None,
        max_attempts: int = 3,
    ) -> Record:
        async with self.lock(f"queue:{kind}:{idem_key}"):
            old = await self.list("ops.jobs", filters={"type": kind, "idem_key": idem_key}, limit=1)
            if old:
                JobEnvelope.model_validate(old[0])
                return old[0]
            job = _job(kind, payload, idem_key, priority, run_after, expires_at, max_attempts)
            return await self.put("ops.jobs", _text_field(job, "job_id"), job)

    async def finish_job(self, job_id: str) -> None:
        await self._transition_job(job_id, "DONE")

    async def fail_job(self, job_id: str, error: str | Exception, *, retry_after: float = 1) -> None:
        async with self.lock(f"job:{job_id}"):
            job = await self.get("ops.jobs", job_id)
            if job is None:
                raise KeyError(job_id)
            envelope = JobRetryState.from_document(job)
            if envelope.status != "RUNNING":
                return
            job["last_error"] = str(error)
            expired = envelope.expires_at is not None and parse_time(envelope.expires_at) <= utcnow()
            job["status"] = (
                "EXPIRED"
                if expired
                else ("FAILED" if envelope.attempts >= envelope.max_attempts else "QUEUED")
            )
            job.update(
                offset_schedule(collection_clock(), timedelta(seconds=retry_after)).fields("run_after")
            )
            if job["status"] in ("FAILED", "EXPIRED"):
                job.update(collection_clock().fields("finished_at"))
            else:
                job.update(Timestamp(None, None, "PENDING").fields("finished_at"))
            await self.put("ops.jobs", job_id, job)

    async def _transition_job(self, job_id: str, status: Literal["DONE"]) -> None:
        async with self.lock(f"job:{job_id}"):
            job = await self.get("ops.jobs", job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] == "RUNNING":
                job["status"] = status
                job.update(collection_clock().fields("finished_at"))
                await self.put("ops.jobs", job_id, job)


class _MemoryAdvisoryLock:
    def __init__(self) -> None:
        self.mutex = asyncio.Lock()
        self.owner: TaskOwner | None = None
        self.depth = 0
        self.users = 0


class MemoryRepository(QueueMixin):
    """Deterministic test implementation with nested rollback and task-local ownership."""

    backend: Literal["memory"] = "memory"
    clickhouse: ClickHouse | None = None

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Record]] = {}
        self._immutable: set[tuple[str, str]] = set()
        self._mutex = asyncio.Lock()
        self._owner: TaskOwner | None = None
        self._locks: dict[str, _MemoryAdvisoryLock] = {}

    async def open(self) -> Self:
        return self

    async def close(self) -> None:
        pass

    async def migrate(self) -> None:
        pass

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        outer = self._owner is not asyncio.current_task()
        if outer:
            await self._mutex.acquire()
            self._owner = asyncio.current_task()

        saved = {collection: rows.copy() for collection, rows in self._data.items()}
        if "ops.jobs" in saved:
            saved["ops.jobs"] = copy.deepcopy(saved["ops.jobs"])
        snapshot = (saved, self._immutable.copy())
        try:
            yield self
        except BaseException:
            self._data, self._immutable = snapshot
            raise
        finally:
            if outer:
                self._owner = None
                self._mutex.release()

    @asynccontextmanager
    async def _read(self) -> AsyncIterator[Self]:
        """Read consistent state without a write transaction's rollback snapshot."""
        if self._owner is asyncio.current_task():
            yield self
        else:
            async with self._mutex:
                yield self

    @asynccontextmanager
    async def lock(self, key: str) -> AsyncIterator[Self]:
        """Task-owned advisory exclusion, independent of explicit transaction scope.

        Acquire advisory keys before an explicit transaction. Waiting for a contended
        key while holding the global transaction mutex can invert that order, so fail
        immediately rather than deadlock the test implementation.
        """
        key = str(key)
        task = asyncio.current_task()
        state = self._locks.get(key)
        if state is None:
            state = self._locks[key] = _MemoryAdvisoryLock()
        state.users += 1
        acquired = False
        try:
            if state.owner is task:
                state.depth += 1
            else:
                if self._owner is task and (state.mutex.locked() or state.users > 1):
                    raise RuntimeError("Acquire contended Memory advisory locks before explicit transactions")
                await state.mutex.acquire()
                state.owner = task
                state.depth = 1
            acquired = True
            yield self
        finally:
            if acquired:
                state.depth -= 1
                if state.depth == 0:
                    state.owner = None
                    state.mutex.release()
            state.users -= 1
            if state.users == 0:
                self._locks.pop(key, None)

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> Record:
        if collection in MARKET_COLUMNS:
            changed = await self.put_market_batch(collection, [record], keys=[str(key)])
            if changed:
                return changed[0]
            stored = await self.get(collection, key)
            if stored is None:
                raise RuntimeError("Market write did not produce a persisted record")
            return stored
        _table(collection)
        key = str(key)
        document = jsonable(dict(record))
        async with self.transaction():
            old = self._data.get(collection, {}).get(key)
            if old is not None and (
                (collection, key) in self._immutable or collection in APPEND_ONLY or immutable
            ):
                if old != document:
                    raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
                if immutable:
                    self._immutable.add((collection, key))
                return copy.deepcopy(old)
            if collection == "ops.jobs" and any(
                r["type"] == document["type"] and r["idem_key"] == document["idem_key"] and k != key
                for k, r in self._data.get(collection, {}).items()
            ):
                raise ValueError("Duplicate job idempotency key")
            if collection == "ops.jobs":
                _stamp_job_clock_fields(document)
            self._data.setdefault(collection, {})[key] = copy.deepcopy(document)
            if immutable or collection in APPEND_ONLY:
                self._immutable.add((collection, key))
            return copy.deepcopy(document)

    async def get(self, collection: str, key: str) -> Record | None:
        _table(collection)
        async with self._read():
            return copy.deepcopy(self._data.get(collection, {}).get(str(key)))

    async def list(
        self, collection: str, *, filters: Record | None = None, limit: int = 1000
    ) -> list[Record]:
        _table(collection)
        if limit < 0:
            raise ValueError("limit must be nonnegative")
        async with self._read():
            return copy.deepcopy(
                [
                    r
                    for r in self._data.get(collection, {}).values()
                    if all(k in r and r[k] == v for k, v in (filters or {}).items())
                ][:limit]
            )

    async def delete(self, collection: str, key: str) -> None:
        _table(collection)
        async with self.transaction():
            if collection in APPEND_ONLY or (collection, str(key)) in self._immutable:
                raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
            self._data.get(collection, {}).pop(str(key), None)

    async def claim_job(self) -> Record | None:
        async with self.transaction():
            now = utcnow()
            candidates: list[tuple[JobEnvelope, Record]] = []
            rows = self._data.get("ops.jobs", {})
            for document in rows.values():
                job = JobEnvelope.model_validate(document)
                if job.status != "QUEUED":
                    continue
                if job.expires_at is not None and parse_time(job.expires_at) <= now:
                    document["status"] = "EXPIRED"
                    document.update(collection_clock().fields("finished_at"))
                elif job.attempts >= job.max_attempts and job.recovery_reclaim is not True:
                    document["status"] = "FAILED"
                    document.update(collection_clock().fields("finished_at"))
                elif parse_time(job.run_after) <= now:
                    candidates.append((job, document))
            running: dict[str, int] = {}
            for document in rows.values():
                job = JobEnvelope.model_validate(document)
                if job.status == "RUNNING":
                    running[job.type] = running.get(job.type, 0) + 1
            candidates = [
                pair
                for pair in candidates
                if (cap := RUNNING_CAPS.get(pair[0].type)) is None or running.get(pair[0].type, 0) < cap
            ]
            busy, fresh = _analysis_gate_memory(self._data, now)
            if busy or fresh:
                safety = now + timedelta(seconds=EXPIRY_SAFETY_S)
                candidates = [
                    pair
                    for pair in candidates
                    if pair[0].type not in YIELDS_TO_ANALYSIS
                    or pair[0].expires_at is not None
                    and parse_time(pair[0].expires_at) <= safety
                ]
            if not candidates:
                return None

            def order(pair: tuple[JobEnvelope, Record]) -> tuple[int, bool, datetime, str, str]:
                job = pair[0]
                deadline = job.expires_at if job.type in EARLIEST_EXPIRY_FIRST else None
                return (
                    job.priority,
                    deadline is None,
                    now if deadline is None else parse_time(deadline),
                    job.run_after,
                    job.job_id,
                )

            job, document = min(candidates, key=order)
            document["status"] = "RUNNING"
            document.pop("recovery_reclaim", None)
            if job.recovery_reclaim is not True:
                document["attempts"] = job.attempts + 1
            return copy.deepcopy(document)

    async def recover_jobs(self) -> int:
        async with self.transaction():
            count = 0
            for document in self._data.get("ops.jobs", {}).values():
                job = JobEnvelope.model_validate(document)
                if job.status != "RUNNING":
                    continue
                expired = job.expires_at is not None and parse_time(job.expires_at) <= utcnow()
                document["status"] = "EXPIRED" if expired else "QUEUED"
                if expired:
                    document.update(collection_clock().fields("finished_at"))
                else:
                    document.update(
                        recovery_reclaim=True,
                        recovery_count=(0 if job.recovery_count is None else job.recovery_count) + 1,
                    )
                    document.update(collection_clock().fields("recovery_at"))
                    document.update(Timestamp(None, None, "PENDING").fields("finished_at"))
                count += 1
            return count


def _optional_document(value: JsonValue, field: str) -> Record:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    return value


def _projection(collection: str, record: Record) -> Record:
    """Map §2 nested names to §14 indexed columns without changing the document."""
    values = dict(record)
    aliases = {
        "body": "text",
        "raw_path": "raw_ref",
        "claim_type": "type",
        "gates": "structural_gates",
        "buy_usd": "buy_notional",
        "sell_usd": "sell_notional",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "funding_time": "settlement_time",
        "public_at_prec": "public_at_precision",
        "published_prec": "published_at_precision",
    }
    for column, alias in aliases.items():
        if column not in values and alias in values:
            values[column] = values[alias]
            if TABLE_COLUMNS[collection].get(column) == "timestamptz" and any(
                record.get(name) is not None for name in (alias + "_prec", alias + "_precision")
            ):
                stamp = propagate_timestamp(record, alias)
                values.update({key: value for key, value in stamp.fields(column).items() if key != column})
    if collection == "ingest.source_items":
        for obj, mappings in [
            ("relevance", {"decision": "relevance_decision", "p_relevant": "relevance_p"}),
            ("route", {"decision": "route_decision", "event_id": "route_event_id", "p_same": "route_p"}),
        ]:
            for source, target in mappings.items():
                nested = record.get(obj)
                if isinstance(nested, dict) and source in nested:
                    values[target] = nested[source]
    if collection == "ops.llm_usage":
        for column, alternatives in {
            "at": ("created_at",),
            "model": ("model_id",),
            "subject_id": ("event_id", "item_id", "session_id", "proposal_id"),
        }.items():
            if values.get(column) is None:
                selected = next((a for a in alternatives if record.get(a) is not None), None)
                values[column] = record[selected] if selected is not None else None
                if (
                    column == "at"
                    and selected is not None
                    and any(
                        record.get(name) is not None for name in (selected + "_prec", selected + "_precision")
                    )
                ):
                    values.update(
                        {
                            key: value
                            for key, value in propagate_timestamp(record, selected).fields("at").items()
                            if key != "at"
                        }
                    )
        usage = _optional_document(record.get("usage"), "usage")
        for column in (
            "input_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "output_tokens",
            "web_searches",
            "stop_reason",
        ):
            if values.get(column) is None and usage.get(column) is not None:
                values[column] = usage[column]
    if collection == "ana.calls":
        opened_by = record.get("opened_by")
        if isinstance(opened_by, list):
            values["opened_by_alert"], values["opened_by_version"] = opened_by
        elif isinstance(opened_by, dict):
            values["opened_by_alert"] = opened_by.get("alert_id")
            values["opened_by_version"] = opened_by.get("version")
    if collection == "evt.claims":
        measured = record.get("value")
        if isinstance(measured, dict):
            values.setdefault("value_num", measured.get("number"))
            values.setdefault("value_unit", measured.get("unit"))
        evidence = record.get("evidence")
        if values.get("quote") is None and isinstance(evidence, list) and evidence:
            first_evidence = evidence[0]
            if isinstance(first_evidence, dict):
                values["quote"] = first_evidence.get("quote")
    if collection == "ana.decisions" and values.get("mta_id") is None:
        values["mta_id"] = _optional_document(record.get("provenance"), "provenance").get("mta_id")
    if collection == "ana.market_truth":
        detail = copy.deepcopy(_optional_document(values.get("detail"), "detail"))
        for key in ("spot", "derivatives", "liquidity", "supply", "relative"):
            if key in record:
                detail[key] = record[key]
        values["detail"] = detail

    id_arrays = {
        ("ana.decisions", "conditions"): "cond_id",
        ("out.alerts", "decisions"): "dec_id",
        ("evt.state_changes", "evidence"): "claim_id",
    }
    for column, datatype in TABLE_COLUMNS[collection].items():
        if datatype == "text[]" and values.get(column) is not None:
            array = values[column]
            if not isinstance(array, (list, tuple)):
                raise ValueError(f"{collection}.{column} must be an array")
            id_field = id_arrays.get((collection, column))
            projected_array: list[JsonValue] = []
            for element in array:
                if isinstance(element, dict) and id_field:
                    element = element.get(id_field)
                if not isinstance(element, str):
                    raise ValueError(
                        f"{collection}.{column} requires text values"
                        + (f" or objects carrying {id_field}" if id_field else "")
                    )
                projected_array.append(element)
            values[column] = projected_array
        temporal = values.get(column)
        if datatype == "timestamptz" and isinstance(temporal, dict):
            if "value" in temporal and "ts" in temporal and temporal["value"] != temporal["ts"]:
                raise ValueError(f"Conflicting timestamp value aliases for {column}")
            values[column] = temporal["value"] if "value" in temporal else temporal.get("ts")

    for field, precision_field in TIMESTAMP_PRECISION_FIELDS.get(collection, {}).items():
        names = {precision_field, field + "_prec", field + "_precision"}
        nested = record.get(field)
        declared = (
            any(record.get(name) is not None for name in names)
            or isinstance(nested, dict)
            and any(nested.get(name) is not None for name in ("precision", "prec"))
        )
        if declared:
            stamp = propagate_timestamp(record, field)
            metadata = stamp.fields(field, precision_field=precision_field)
            values.update({key: value for key, value in metadata.items() if key != field})
        else:
            for name in names:
                if record.get(name + "_reason") is not None:
                    values[precision_field + "_reason"] = record[name + "_reason"]
    return {k: values.get(k) for k in TABLE_COLUMNS[collection]}


async def _finish_critical[T](awaitable: Awaitable[T]) -> tuple[T, asyncio.CancelledError | None]:
    """Finish connection-state transitions despite repeated caller cancellation.

    Shield only driver enter/exit and pool return, never application work. Retain the
    cancellation for the caller after the connection has reached a stable state.
    """
    operation = asyncio.ensure_future(awaitable)
    cancellation = None
    while True:
        try:
            result = await asyncio.shield(operation)
            return result, cancellation
        except asyncio.CancelledError as error:
            if operation.cancelled():
                raise
            cancellation = cancellation or error


@asynccontextmanager
async def _stable_transaction(conn: AsyncConnection[StoredRow]) -> AsyncIterator[None]:
    context = conn.transaction()
    _, cancelled = await _finish_critical(context.__aenter__())
    if cancelled:
        await _finish_critical(context.__aexit__(type(cancelled), cancelled, cancelled.__traceback__))
        raise cancelled
    try:
        yield
    except BaseException as error:
        suppressed, exit_cancelled = await _finish_critical(
            context.__aexit__(type(error), error, error.__traceback__)
        )
        if exit_cancelled and not isinstance(error, asyncio.CancelledError):
            raise exit_cancelled from error
        if not suppressed:
            raise
    else:
        _, exit_cancelled = await _finish_critical(context.__aexit__(None, None, None))
        if exit_cancelled:
            raise exit_cancelled


class PostgresRepository(QueueMixin):
    backend: Literal["postgres"] = "postgres"
    clickhouse: ClickHouse | None = None

    def _exact_predicate(self, collection, filters):
        return _exact_predicate(collection, filters)

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._last_deferral: tuple[float, int, tuple[str, ...]] | None = None
        self._pool: AsyncConnectionPool[AsyncConnection[StoredRow]] | None = None
        self._connection: ContextVar[
            tuple[weakref.ReferenceType[AsyncConnection[StoredRow]], weakref.ReferenceType[TaskOwner]] | None
        ] = ContextVar(f"pg_connection_{id(self)}", default=None)

    async def open(self) -> Self:
        from psycopg.rows import dict_row

        class CancellationSafeConnection(AsyncConnection[StoredRow]):
            async def wait[T](
                connection, gen: PQGen[T], interval: float = 0.1, timeout: float | None = None
            ) -> T:

                operation = asyncio.create_task(super().wait(gen, interval=interval, timeout=timeout))
                try:
                    return await asyncio.shield(operation)
                except asyncio.CancelledError:
                    if not operation.done():
                        operation.cancel()
                    with suppress(asyncio.CancelledError):
                        await _finish_critical(operation)
                    raise

        self._pool = AsyncConnectionPool[AsyncConnection[StoredRow]](
            self.database_url,
            open=False,
            min_size=1,
            max_size=16,
            max_idle=120,
            max_lifetime=600,
            connection_class=CancellationSafeConnection,
            kwargs={"row_factory": dict_row},
        )
        await self._pool.open()
        await self._pool.wait()
        return self

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        existing = self._connection.get()
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Repository transactions require an asyncio task")

        if existing is not None and existing[1]() is task:
            connection = existing[0]()
            if connection is None:
                raise RuntimeError("Transaction connection no longer exists")
            async with _stable_transaction(connection):
                yield self
            return
        pool = self._pool
        if pool is None:
            raise RuntimeError("Repository.open() must be called first")
        conn = await pool.getconn()
        token = self._connection.set((weakref.ref(conn), weakref.ref(task)))
        try:
            async with _stable_transaction(conn):
                yield self
        finally:
            self._connection.reset(token)
            _, cancelled = await _finish_critical(pool.putconn(conn))
            if cancelled:
                raise cancelled

    def _conn(self) -> AsyncConnection[StoredRow]:
        existing = self._connection.get()
        owner = None if existing is None else existing[1]()
        if existing is None or owner is None or owner.done() or owner is not asyncio.current_task():
            raise RuntimeError("Connection access requires a transaction owned by the current task")
        connection = existing[0]()
        if connection is None:
            raise RuntimeError("Transaction connection no longer exists")
        return connection

    @asynccontextmanager
    async def lock(self, key: str) -> AsyncIterator[Self]:
        digest = int.from_bytes(hashlib.sha256(str(key).encode()).digest()[:8], "big", signed=True)
        existing = self._connection.get()
        if existing is None or existing[1]() is not asyncio.current_task():
            while True:
                async with self.transaction():
                    row = await (
                        await self._conn().execute("SELECT pg_try_advisory_xact_lock(%s) AS ok", (digest,))
                    ).fetchone()
                    if AdvisoryLockResult.model_validate(row).ok:
                        yield self
                        return
                await asyncio.sleep(random.uniform(0.05, 0.2))

        async with self.transaction():
            await self._conn().execute("SELECT pg_advisory_xact_lock(%s)", (digest,))
            yield self

    @asynccontextmanager
    async def _migration_connection(self) -> AsyncIterator[AsyncConnection[StoredRow]]:
        """Serialize versions across transaction boundaries on a dedicated session.

        The key is identical to the old transaction advisory lock, so old and
        new migration runners serialize with one another during an upgrade.
        """
        existing = self._connection.get()
        if existing is not None and existing[1]() is asyncio.current_task():
            raise RuntimeError("Migrations require their own connection outside an application transaction")
        pool = self._pool
        if pool is None:
            raise RuntimeError("Repository.open() must be called first")
        conn = await pool.getconn()
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Repository migrations require an asyncio task")
        token = self._connection.set((weakref.ref(conn), weakref.ref(task)))
        digest = int.from_bytes(hashlib.sha256(b"tank:migrations").digest()[:8], "big", signed=True)
        try:
            async with _stable_transaction(conn):
                await conn.execute("SELECT pg_advisory_lock(%s)", (digest,))
            yield conn
        finally:
            self._connection.reset(token)

            async def release() -> None:
                try:
                    async with _stable_transaction(conn):
                        await conn.execute("SELECT pg_advisory_unlock(%s)", (digest,))
                except BaseException:
                    await conn.close()
                await pool.putconn(conn)

            _, cancelled = await _finish_critical(release())
            if cancelled:
                raise cancelled

    async def migrate(self, *, target: str | None = None) -> None:
        async with self._migration_connection() as conn:
            async with _stable_transaction(conn):
                await conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS ops.schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
                )
                adopted = await (
                    await conn.execute(
                        "SELECT EXISTS (SELECT 1 FROM ops.schema_migrations) AND NOT EXISTS "
                        "(SELECT 1 FROM ops.schema_migrations WHERE version=%s) AS adopt",
                        (BASELINE,),
                    )
                ).fetchone()
                if MigrationAdoption.model_validate(adopted).adopt:
                    await conn.execute("INSERT INTO ops.schema_migrations(version) VALUES (%s)", (BASELINE,))
            for path in sorted((Path(__file__).parent / "migrations").glob("*.sql")):
                if target is not None and path.name > target:
                    break
                async with _stable_transaction(conn):
                    exists = await (
                        await conn.execute(
                            "SELECT 1 FROM ops.schema_migrations WHERE version=%s", (path.name,)
                        )
                    ).fetchone()
                    if exists is None:
                        await conn.execute(path.read_text(), prepare=False)
                        await conn.execute(
                            "INSERT INTO ops.schema_migrations(version) VALUES (%s)", (path.name,)
                        )
            async with _stable_transaction(conn):
                await conn.execute((Path(__file__).parent / "agent_grants.sql").read_text(), prepare=False)

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> Record:
        if collection in MARKET_COLUMNS:
            changed = await self.put_market_batch(collection, [record], keys=[str(key)])
            if changed:
                return changed[0]
            stored = await self.get(collection, key)
            if stored is None:
                raise RuntimeError("Market write did not produce a persisted record")
            return stored
        from psycopg import sql
        from psycopg.types.json import Jsonb

        _table(collection)
        key, document = str(key), jsonable(dict(record))
        async with self.lock(f"record:{collection}:{key}"):
            conn = self._conn()
            table = sql.Identifier(*collection.split("."))
            old = await (
                await conn.execute(
                    sql.SQL("SELECT document, immutable FROM {} WHERE storage_key=%s").format(table),
                    (key,),
                )
            ).fetchone()
            previous = None if old is None else ImmutableDocument.model_validate(old)
            if previous is not None and (previous.immutable or immutable or collection in APPEND_ONLY):
                if previous.document != document:
                    raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
                if immutable and not previous.immutable:
                    await conn.execute(
                        sql.SQL("UPDATE {} SET immutable=true WHERE storage_key=%s").format(table), (key,)
                    )
                return previous.document
            projected = _projection(collection, document)
            if collection in SERIAL_TABLES and projected.get("id") is None:
                projected.pop("id", None)
            values: list[StoredValue | Jsonb] = [key, Jsonb(document), immutable or collection in APPEND_ONLY]
            for column, value in projected.items():
                datatype = TABLE_COLUMNS[collection][column]
                parameter: StoredValue | Jsonb = value
                if value is not None:
                    if datatype == "jsonb":
                        parameter = Jsonb(value)
                    elif datatype == "timestamptz":
                        parameter = _time_field(projected, column)
                    elif datatype == "text" and not isinstance(value, str):
                        raise ValueError(f"{collection}.{column} requires text")
                values.append(parameter)
            names = ["storage_key", "document", "immutable", *projected]
            columns = sql.SQL(",").join(map(sql.Identifier, names))
            assignments = sql.SQL(",").join(
                sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(n), sql.Identifier(n)) for n in names[1:]
            )
            query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT(storage_key) DO UPDATE SET {}"
            ).format(table, columns, sql.SQL(",").join(sql.Placeholder() for _ in names), assignments)
            await conn.execute(query, values)
            return copy.deepcopy(document)

    async def get(self, collection: str, key: str) -> Record | None:
        from psycopg import sql

        from tank import clickhouse_market as chm

        _table(collection)
        if (ch := chm.client(self, collection)) is not None:
            return await chm.get(ch, collection, key)
        async with self.transaction():
            row = await (
                await self._conn().execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE storage_key=%s"
                        if collection in MARKET_COLUMNS
                        else "SELECT document FROM {} WHERE storage_key=%s"
                    ).format(sql.Identifier(*collection.split("."))),
                    (str(key),),
                )
            ).fetchone()
            return _read_record(collection, row) if row else None

    async def list(
        self, collection: str, *, filters: Record | None = None, limit: int = 1000
    ) -> list[Record]:
        if collection in MARKET_COLUMNS:
            result: list[Record] = []
            async for row in self.iter_all(collection, filters=filters, batch_size=max(1, limit)):
                if len(result) >= limit:
                    break
                result.append(row)
            return result
        from psycopg import sql
        from psycopg.types.json import Jsonb

        _table(collection)
        if limit < 0:
            raise ValueError("limit must be nonnegative")

        clauses: list[sql.Composable] = []
        args: list[StoredValue | Jsonb] = []
        for k, v in (filters or {}).items():
            clauses.append(sql.SQL("document -> %s = %s"))
            args.extend([k, Jsonb(jsonable(v))])
            datatype = TABLE_COLUMNS[collection].get(k)
            if (
                datatype
                and v is not None
                and datatype not in ("jsonb", "text[]")
                and not isinstance(v, (dict, list))
            ):
                typed_value: StoredValue = v
                if datatype == "timestamptz":
                    typed_value = _time_field({k: v}, k)
                clauses.append(sql.SQL("{} = %s").format(sql.Identifier(k)))
                args.append(typed_value)
        where = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
        async with self.transaction():
            rows = await (
                await self._conn().execute(
                    sql.SQL("SELECT * FROM {} WHERE {} ORDER BY storage_key LIMIT %s").format(
                        sql.Identifier(*collection.split(".")), where
                    ),
                    [*args, limit],
                )
            ).fetchall()
            return [_read_record(collection, r) for r in rows]

    async def delete(self, collection: str, key: str) -> None:
        from psycopg import sql

        _table(collection)
        if collection in MARKET_COLUMNS:
            from tank import clickhouse_market as chm

            if (ch := chm.client(self, collection)) is not None:
                await chm.delete(ch, collection, key)
                return
            async with self.transaction():
                await self._conn().execute(
                    sql.SQL("DELETE FROM {} WHERE storage_key=%s").format(
                        sql.Identifier(*collection.split("."))
                    ),
                    (str(key),),
                )
            return
        async with self.lock(f"record:{collection}:{key}"):
            row = await (
                await self._conn().execute(
                    sql.SQL("SELECT immutable FROM {} WHERE storage_key=%s FOR UPDATE").format(
                        sql.Identifier(*collection.split("."))
                    ),
                    (str(key),),
                )
            ).fetchone()
            if collection in APPEND_ONLY or row and row["immutable"]:
                raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
            await self._conn().execute(
                sql.SQL("DELETE FROM {} WHERE storage_key=%s").format(sql.Identifier(*collection.split("."))),
                (str(key),),
            )

    async def claim_job(self) -> Record | None:
        started = time.monotonic()
        job = await self._claim_job()
        elapsed = time.monotonic() - started

        late = None
        if job and job.get("type") == "DELIVER" and job.get("attempts") == 1 and job.get("run_after"):
            late = (utcnow() - _time_field(job, "run_after")).total_seconds()
        if elapsed > CLAIM_SLOW_S or (late is not None and late > DELIVER_LATE_S):
            log.info(
                "claim_job %.2f s, type %s, %s",
                elapsed,
                job.get("type") if job else None,
                f"first claimed {late:.2f} s after run_after" if late is not None else "no DELIVER",
            )
        return job

    async def _claim_job(self) -> Record | None:
        async with self.transaction():
            conn = self._conn()

            held = HeldCaps.model_validate(
                await (
                    await conn.execute(
                        "SELECT coalesce(array_agg(t), '{}') AS held FROM unnest(%s::text[]) AS t"
                        " WHERE pg_try_advisory_xact_lock(hashtextextended('tank:job_cap:' || t, 0))",
                        (list(RUNNING_CAPS),),
                    )
                ).fetchone()
            ).held
            row = await (
                await conn.execute(
                    """WITH expired AS (
                UPDATE ops.jobs SET status='EXPIRED', document=jsonb_set(document,'{status}','"EXPIRED"')
                WHERE status='QUEUED' AND expires_at <= now() RETURNING 1),
              failed AS (
                UPDATE ops.jobs SET status='FAILED', document=jsonb_set(document,'{status}','"FAILED"')
                WHERE status='QUEUED' AND attempts >= max_attempts
                AND coalesce((document->>'recovery_reclaim')::boolean,false)=false RETURNING 1),
              gate AS (
                SELECT EXISTS (SELECT 1 FROM ops.jobs b WHERE b.type=ANY(%(analysis)s::text[])
                    AND (b.status='RUNNING' OR (b.status='QUEUED' AND b.run_after<=now()
                      AND (b.expires_at IS NULL OR b.expires_at>now())))) AS analysis_busy,
                  EXISTS (SELECT 1 FROM evt.events e
                    WHERE e.known_to_us_at > now() - make_interval(secs => %(fresh)s)
                    AND NOT EXISTS (SELECT 1 FROM out.alerts a WHERE a.event_id=e.event_id AND a.kind='FULL'))
                    AS fresh_event),
              pick AS (
                SELECT storage_key FROM ops.jobs j WHERE status='QUEUED' AND run_after<=now()
                AND (expires_at IS NULL OR expires_at>now())
                AND (attempts<max_attempts OR document->>'recovery_reclaim'='true')
                AND NOT EXISTS (SELECT 1 FROM unnest(%(capped)s::text[], %(caps)s::int[]) AS c(type, cap)
                  WHERE c.type=j.type AND (NOT c.type=ANY(%(held)s::text[])
                    OR (SELECT count(*) FROM ops.jobs r WHERE r.status='RUNNING' AND r.type=c.type)>=c.cap))
                AND (NOT j.type=ANY(%(yields)s::text[])
                  OR j.expires_at <= now() + make_interval(secs => %(safety)s)
                  OR NOT (SELECT analysis_busy OR fresh_event FROM gate))
                ORDER BY priority,CASE WHEN type=ANY(%(edf)s::text[]) THEN expires_at END NULLS LAST,
                  run_after,storage_key FOR UPDATE SKIP LOCKED LIMIT 1),
              claimed AS (
                UPDATE ops.jobs SET status='RUNNING',
                attempts=attempts+CASE WHEN document->>'recovery_reclaim'='true' THEN 0 ELSE 1 END,
                document=document||jsonb_build_object('status','RUNNING','recovery_reclaim',false,
                  'attempts',attempts+CASE WHEN document->>'recovery_reclaim'='true' THEN 0 ELSE 1 END)
                WHERE storage_key=(SELECT storage_key FROM pick) RETURNING document)
              SELECT (SELECT document FROM claimed) AS document,
                (SELECT analysis_busy FROM gate) AS analysis_busy,
                (SELECT fresh_event FROM gate) AS fresh_event,
                (SELECT count(*) FROM ops.jobs d WHERE d.type=ANY(%(yields)s::text[]) AND d.status='QUEUED'
                  AND d.run_after<=now() AND d.expires_at > now() + make_interval(secs => %(safety)s)
                  AND (SELECT analysis_busy OR fresh_event FROM gate)) AS deferred""",
                    {
                        "analysis": list(ANALYSIS_TYPES),
                        "fresh": FRESH_EVENT_S,
                        "capped": list(RUNNING_CAPS),
                        "caps": list(RUNNING_CAPS.values()),
                        "held": held,
                        "yields": list(YIELDS_TO_ANALYSIS),
                        "safety": EXPIRY_SAFETY_S,
                        "edf": list(EARLIEST_EXPIRY_FIRST),
                    },
                )
            ).fetchone()
            result = ClaimResult.model_validate(row)
            if result.deferred:
                self._log_deferral(result.deferred, result.analysis_busy, result.fresh_event)
            return result.document

    def _log_deferral(self, count: int, busy: bool, fresh: bool) -> None:
        """Log deferred captures when the picture changes, and at least once a minute."""
        reasons = tuple(
            r for r, on in (("analysis job running or due", busy), ("event without FULL card", fresh)) if on
        )
        now = time.monotonic()
        last = self._last_deferral
        if last is not None and last[1:] == (count, reasons) and now - last[0] < DEFERRAL_LOG_S:
            return
        self._last_deferral = (now, count, reasons)
        log.info("deferring %d due REACTION_CAPTURE behind event analysis: %s", count, ", ".join(reasons))

    async def recover_jobs(self) -> int:
        async with self.transaction():
            cursor = await self._conn().execute("""UPDATE ops.jobs
              SET status=CASE WHEN expires_at<=now() THEN 'EXPIRED' ELSE 'QUEUED' END,
              document=document||jsonb_build_object('status',CASE WHEN expires_at<=now() THEN 'EXPIRED' ELSE 'QUEUED' END,
                'recovery_reclaim',expires_at IS NULL OR expires_at>now(),
                'recovery_at',to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                'recovery_at_prec','COLLECTION_ONLY','recovery_at_prec_reason',NULL,
                'recovery_count',coalesce((document->>'recovery_count')::int,0)+1)
              WHERE status='RUNNING'""")
            return cursor.rowcount


async def _preserved_windows(repo):
    """Permanent MAJOR+ windows, including pre-truth beneficiaries and benchmarks."""
    major_ids = set()
    for grade in ("MAJOR", "STRUCTURAL", "STRUCTURAL_SHOCK"):
        async for row in repo.iter_all("evt.ems_assessments", filters={"class": grade}):
            major_ids.add(row["event_id"])
    benchmarks = {"bitcoin:BTC", "ethereum:ETH", "BTC", "ETH"}
    for symbol in ("BTC", "ETH"):
        async for row in repo.iter_all("reg.tokens", filters={"symbol": symbol}):
            benchmarks.add(row["token_id"])
    windows = []
    for eid in major_ids:
        event = await repo.get("evt.events", eid)
        if event is None:
            continue
        projected = _projection("evt.events", event)
        anchors = [
            parse_time(projected[k])
            for k in ("event_time", "public_at", "known_to_us_at")
            if projected.get(k)
        ]
        if not anchors:
            continue
        tokens = set(benchmarks)
        for item in event.get("tokens_mentioned", event.get("tokens", [])):
            token = item.get("token_id") if isinstance(item, dict) else item
            if token:
                tokens.add(token)
        for collection in ("ana.market_truth", "evt.beneficiary_edges", "evt.capture_chains"):
            async for row in repo.iter_all(collection, filters={"event_id": eid}):
                if row.get("token_id"):
                    tokens.add(row["token_id"])
        async for row in repo.iter_all("evt.beneficiary_maps", filters={"event_id": eid}):
            tokens.update(row.get("analysis_set") or [])
        windows.extend(
            (token, min(anchors) - timedelta(hours=24), max(anchors) + timedelta(hours=72))
            for token in tokens
        )
    return windows


async def _prune_market(self, before, *, book_before=None, trades_before=None):
    """Prune capture after backup, preserving whole UTC days overlapping MAJOR+ windows.

    ClickHouse drops whole daily partitions of the exchange series: the caller's cutoff
    (normally 90 days), book summaries at `book_before` (72 h, 03 §14, no event-window
    exception) and prints at `trades_before`. PostgreSQL keeps only supply observations,
    capture gaps and tier transitions, deleted by the same cutoff and preserved days.
    """
    cutoff = parse_time(before)
    temporal = {
        "mkt.supply_obs": "ts",
        "mkt.capture_gaps": "observed_at",
        "ops.capture_tier_transitions": "effective_at",
    }
    if isinstance(self, MemoryRepository):
        temporal.update(
            {
                "mkt.klines_1m": "open_time",
                "mkt.flow_bars_1m": "minute_start",
                "mkt.book_summaries": "exchange_ts",
                "mkt.oi_obs": "ts",
                "mkt.funding_obs": "as_of",
                "mkt.liq_obs": "ts",
                "mkt.tickers_24h": "ts",
            }
        )
    counts = {}
    async with self.lock("tank:market_retention"):
        days = set()
        for _, start, end in await _preserved_windows(self):
            day = start.replace(hour=0, minute=0, second=0, microsecond=0)
            last = end.replace(hour=0, minute=0, second=0, microsecond=0)
            while day <= last:
                days.add(day)
                day += timedelta(days=1)
        spans = []
        for day in sorted(days):
            if spans and spans[-1][1] == day:
                spans[-1] = (spans[-1][0], day + timedelta(days=1))
            else:
                spans.append((day, day + timedelta(days=1)))
        if isinstance(self, PostgresRepository):
            from psycopg import sql

            for collection, ts in temporal.items():
                args = [cutoff]
                clauses = []
                for start, end in spans:
                    clauses.append(
                        sql.SQL("({} >= %s AND {} < %s)").format(sql.Identifier(ts), sql.Identifier(ts))
                    )
                    args.extend([start, end])
                protect = sql.SQL(" OR ").join(clauses) if clauses else sql.SQL("FALSE")
                cursor = await self._conn().execute(
                    sql.SQL("DELETE FROM {} WHERE {} < %s AND NOT ({})").format(
                        sql.Identifier(*collection.split(".")), sql.Identifier(ts), protect
                    ),
                    args,
                )
                counts[collection] = cursor.rowcount
        else:
            if book_before is not None:
                book_cutoff = parse_time(book_before)
                for key, row in list(self._data.get("mkt.book_summaries", {}).items()):
                    if row.get("exchange_ts") and parse_time(row["exchange_ts"]) < book_cutoff:
                        del self._data["mkt.book_summaries"][key]
                        self._immutable.discard(("mkt.book_summaries", key))
            for collection, ts in temporal.items():
                deleted = 0
                for key, row in list(self._data.get(collection, {}).items()):
                    if not row.get(ts):
                        continue
                    at = parse_time(row[ts])
                    protected = any(start <= at < end for start, end in spans)
                    if at < cutoff and not protected and (collection, key) not in self._immutable:
                        del self._data[collection][key]
                        self._immutable.discard((collection, key))
                        deleted += 1
                counts[collection] = deleted
    ch = getattr(self, "clickhouse", None)
    if ch is not None:
        from tank import clickhouse_market as chm

        preserved = set()
        for start, end in spans:
            day = start
            while day < end:
                preserved.add(day)
                day += timedelta(days=1)
        counts["clickhouse_partitions_dropped"] = await chm.prune(
            ch,
            cutoff,
            preserved,
            book_cutoff=parse_time(book_before) if book_before is not None else None,
            trades_cutoff=parse_time(trades_before) if trades_before is not None else None,
        )
    return counts


async def _range_page(
    self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
):
    """Return (document, (timestamp, storage key)) pairs for deterministic keyset pagination."""
    _table(collection)
    if TABLE_COLUMNS[collection].get(time_field) != "timestamptz":
        raise ValueError(f"{collection}.{time_field} is not a typed timestamp column")
    if limit < 1:
        raise ValueError("limit must be positive")
    start_dt, end_dt = parse_time(start) if start else None, parse_time(end) if end else None
    from tank import clickhouse_market as chm

    if (ch := chm.client(self, collection)) is not None:
        return await chm.range_page(
            ch, collection, time_field, start=start_dt, end=end_dt, filters=filters, limit=limit, after=after
        )
    if isinstance(self, PostgresRepository):
        from psycopg import sql
        from psycopg.types.json import Jsonb

        clauses, args = [sql.SQL("{} IS NOT NULL").format(sql.Identifier(time_field))], []
        for op, bound in [(">=", start_dt), ("<", end_dt)]:
            if bound is not None:
                clauses.append(sql.SQL("{} " + op + " %s").format(sql.Identifier(time_field)))
                args.append(bound)
        if after is not None:
            clauses.append(sql.SQL("({},storage_key) > (%s,%s)").format(sql.Identifier(time_field)))
            args.extend([parse_time(after[0]), str(after[1])])
        if filters and collection in MARKET_COLUMNS:
            extra, values = _exact_predicate(collection, filters)
            clauses.extend(extra)
            args.extend(values)
        elif filters:
            clauses.append(sql.SQL("document @> %s"))
            args.append(Jsonb(jsonable(filters)))
            for key, value in filters.items():
                clauses.append(sql.SQL("document -> %s = %s"))
                args.extend([key, Jsonb(jsonable(value))])
                if TABLE_COLUMNS[collection].get(key) == "text" and isinstance(value, str):
                    clauses.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
                    args.append(value)
        async with self.transaction():
            query = sql.SQL(
                "SELECT *, {} AS _page_time FROM {} WHERE {} ORDER BY {},storage_key LIMIT %s"
            ).format(
                sql.Identifier(time_field),
                sql.Identifier(*collection.split(".")),
                sql.SQL(" AND ").join(clauses),
                sql.Identifier(time_field),
            )
            rows = await (await self._conn().execute(query, [*args, limit])).fetchall()
            return [(_read_record(collection, r), (iso(r[time_field]), r["storage_key"])) for r in rows]
    async with self.transaction():
        matches = []
        for key, document in self._data.get(collection, {}).items():
            timestamp = _projection(collection, document).get(time_field)
            if timestamp is None:
                continue
            timestamp = parse_time(timestamp)
            if start_dt and timestamp < start_dt or end_dt and timestamp >= end_dt:
                continue
            if after and (timestamp, key) <= (parse_time(after[0]), str(after[1])):
                continue
            if all(k in document and document[k] == v for k, v in (filters or {}).items()):
                matches.append((copy.deepcopy(document), (iso(timestamp), key)))
        matches.sort(key=lambda r: (parse_time(r[1][0]), r[1][1]))
        return matches[:limit]


async def _list_range(
    self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
):
    return [
        r[0]
        for r in await _range_page(
            self, collection, time_field, start=start, end=end, filters=filters, limit=limit, after=after
        )
    ]


async def _iter_range(self, collection, time_field, *, start=None, end=None, filters=None, batch_size=10000):
    after = None
    while True:
        page = await _range_page(
            self, collection, time_field, start=start, end=end, filters=filters, limit=batch_size, after=after
        )
        for document, _ in page:
            yield document
        if len(page) < batch_size:
            return
        after = page[-1][1]


async def _prune_jobs(self, before):
    """Prune aged terminal jobs; retain references needed by jobs, sessions or tool recovery.

    Missing terminal timestamps are preserved. Queue payloads and persisted session/tool state
    are searched recursively for exact job IDs, including custom dependency field names.
    """
    cutoff = parse_time(before)

    def strings(value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for nested in value.values():
                yield from strings(nested)
        elif isinstance(value, list):
            for nested in value:
                yield from strings(nested)

    async with self.lock("tank:job_retention"):
        await _prune_expired_escalations(self, cutoff)
        candidates_by_id = set()
        for status in ("DONE", "FAILED", "EXPIRED"):
            async for job in self.iter_range(
                "ops.jobs", "finished_at", end=cutoff, filters={"status": status}
            ):
                candidates_by_id.add(job["job_id"])
        if not candidates_by_id:
            return 0
        references = set()
        async for job in self.iter_all("ops.jobs"):
            references.update(value for value in strings(job.get("payload")) if value in candidates_by_id)
            for key in ("depends_on", "dependencies", "parent_job_id", "job_ids"):
                references.update(value for value in strings(job.get(key)) if value in candidates_by_id)
        for collection in ("ops.agent_sessions", "ops.tool_calls"):
            async for record in self.iter_all(collection):
                references.update(value for value in strings(record) if value in candidates_by_id)
        candidates = list(candidates_by_id - references)
        if not candidates:
            return 0
        if isinstance(self, PostgresRepository):
            cursor = await self._conn().execute(
                """DELETE FROM ops.jobs WHERE job_id=ANY(%s)
                AND status IN ('DONE','FAILED','EXPIRED') AND finished_at<%s AND NOT immutable""",
                (candidates, cutoff),
            )
            return cursor.rowcount
        count = 0
        for job_id in candidates:
            if ("ops.jobs", job_id) not in self._immutable:
                self._data["ops.jobs"].pop(job_id, None)
                count += 1
        return count


async def _prune_expired_escalations(self, cutoff):
    """Source escalations are control records like jobs (HLD §7.1 ops: 30 days after expiry)."""
    if isinstance(self, PostgresRepository):
        await self._conn().execute(
            "DELETE FROM ops.source_escalations WHERE expires_at<%s AND NOT immutable", (cutoff,)
        )
        return
    rows = self._data.get("ops.source_escalations", {})
    for key, row in list(rows.items()):
        if (
            row.get("expires_at")
            and parse_time(row["expires_at"]) < cutoff
            and ("ops.source_escalations", key) not in self._immutable
        ):
            rows.pop(key)


async def _latest_before(self, collection, time_field, cutoff, *, filters=None):
    """Last actual observation strictly before cutoff, using typed time and stable key indexes."""
    _table(collection)
    if TABLE_COLUMNS[collection].get(time_field) != "timestamptz":
        raise ValueError(f"{collection}.{time_field} is not a typed timestamp column")
    cutoff = parse_time(cutoff)
    from tank import clickhouse_market as chm

    if (ch := chm.client(self, collection)) is not None:
        return await chm.latest_before(ch, collection, time_field, cutoff, filters=filters)
    if isinstance(self, PostgresRepository):
        from psycopg import sql
        from psycopg.types.json import Jsonb

        clauses, args = [sql.SQL("{} < %s").format(sql.Identifier(time_field))], [cutoff]
        if filters and collection in MARKET_COLUMNS:
            extra, values = _exact_predicate(collection, filters)
            clauses.extend(extra)
            args.extend(values)
        elif filters:
            clauses.append(sql.SQL("document @> %s"))
            args.append(Jsonb(jsonable(filters)))
            for key, value in filters.items():
                clauses.append(sql.SQL("document -> %s = %s"))
                args.extend([key, Jsonb(jsonable(value))])
                if TABLE_COLUMNS[collection].get(key) == "text" and isinstance(value, str):
                    clauses.append(sql.SQL("{}=%s").format(sql.Identifier(key)))
                    args.append(value)
        async with self.transaction():
            row = await (
                await self._conn().execute(
                    sql.SQL("SELECT * FROM {} WHERE {} ORDER BY {} DESC,storage_key DESC LIMIT 1").format(
                        sql.Identifier(*collection.split(".")),
                        sql.SQL(" AND ").join(clauses),
                        sql.Identifier(time_field),
                    ),
                    args,
                )
            ).fetchone()
            return _read_record(collection, row) if row else None
    async with self.transaction():
        candidates = []
        for key, row in self._data.get(collection, {}).items():
            at = _projection(collection, row).get(time_field)
            if at is None:
                continue
            at = parse_time(at)
            if at < cutoff and all(k in row and row[k] == v for k, v in (filters or {}).items()):
                candidates.append((at, key, row))
        return copy.deepcopy(max(candidates, key=lambda item: (item[0], item[1]))[2]) if candidates else None


def _exact_predicate(collection, filters):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    clauses, args = [], []
    if collection in MARKET_COLUMNS:
        for key, value in (filters or {}).items():
            key = (
                __import__("tank.market_schema", fromlist=["MARKET_ALIASES"])
                .MARKET_ALIASES.get(collection, {})
                .get(key, key)
            )
            kind = MARKET_COLUMNS[collection].get(key)
            if kind is None:
                raise ValueError(f"Unknown typed market filter {collection}.{key}")
            if value is None:
                clauses.append(sql.SQL("{} IS NULL").format(sql.Identifier(key)))
                continue

            clauses.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
            args.append(
                Jsonb(jsonable(value))
                if kind == "jsonb" and value is not None
                else parse_time(value)
                if kind == "timestamptz" and value is not None
                else value
            )
        return clauses, args
    if filters:
        clauses.append(sql.SQL("document @> %s"))
        args.append(Jsonb(jsonable(filters)))
        for key, value in filters.items():
            clauses.append(sql.SQL("document -> %s = %s"))
            args.extend([key, Jsonb(jsonable(value))])
            datatype = TABLE_COLUMNS[collection].get(key)
            if datatype == "text" and isinstance(value, str):
                clauses.append(sql.SQL("{}=%s").format(sql.Identifier(key)))
                args.append(value)
    return clauses, args


async def _iter_all(self, collection, *, filters=None, batch_size=1000):
    """Read all matching documents without a cap, using stable storage-key pages.

    Each page observes committed state. Call inside transaction when atomicity with writes is
    required; records inserted with keys before an already-read cursor are outside this scan.
    """
    _table(collection)
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    after = None
    from tank import clickhouse_market as chm

    ch = chm.client(self, collection)
    while True:
        if ch is not None:
            page = await chm.all_page(ch, collection, filters=filters, after=after, limit=batch_size)
        elif isinstance(self, PostgresRepository):
            from psycopg import sql

            clauses, args = _exact_predicate(collection, filters)
            if after is not None:
                clauses.append(sql.SQL("storage_key>%s"))
                args.append(after)
            predicate = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
            async with self.transaction():
                query = sql.SQL("SELECT * FROM {} WHERE {} ORDER BY storage_key LIMIT %s").format(
                    sql.Identifier(*collection.split(".")), predicate
                )
                rows = await (await self._conn().execute(query, [*args, batch_size])).fetchall()
            page = [(r["storage_key"], _read_record(collection, r)) for r in rows]
        else:
            async with self._read():
                page = []
                for key, row in sorted(self._data.get(collection, {}).items()):
                    if after is not None and key <= after:
                        continue
                    if all(k in row and row[k] == v for k, v in (filters or {}).items()):
                        page.append((key, copy.deepcopy(row)))
                        if len(page) == batch_size:
                            break
        for _, document in page:
            yield document
        if len(page) < batch_size:
            return
        after = page[-1][0]


async def _list_latest(self, collection, time_field, *, filters=None, limit=100, start=None, end=None):
    """Latest typed timestamp records, ordered by timestamp then stable key descending.

    Optional bounds are start-inclusive/end-exclusive. Null timestamps cannot represent a
    latest observation and are excluded. Filtering happens in SQL before the limit.
    """
    _table(collection)
    if TABLE_COLUMNS[collection].get(time_field) != "timestamptz":
        raise ValueError(f"{collection}.{time_field} is not a typed timestamp column")
    if limit < 0:
        raise ValueError("limit must be nonnegative")
    if limit == 0:
        return []
    from tank import clickhouse_market as chm

    if (ch := chm.client(self, collection)) is not None:
        return await chm.list_latest(
            ch, collection, time_field, filters=filters, limit=limit, start=start, end=end
        )
    if isinstance(self, PostgresRepository):
        from psycopg import sql

        clauses, args = _exact_predicate(collection, filters)
        clauses.append(sql.SQL("{} IS NOT NULL").format(sql.Identifier(time_field)))
        for bound, operator in ((start, ">="), (end, "<")):
            if bound is not None:
                clauses.append(sql.SQL("{} " + operator + " %s").format(sql.Identifier(time_field)))
                args.append(parse_time(bound))
        async with self.transaction():
            query = sql.SQL("SELECT * FROM {} WHERE {} ORDER BY {} DESC,storage_key DESC LIMIT %s").format(
                sql.Identifier(*collection.split(".")),
                sql.SQL(" AND ").join(clauses),
                sql.Identifier(time_field),
            )
            rows = await (await self._conn().execute(query, [*args, limit])).fetchall()
            return [_read_record(collection, r) for r in rows]
    async with self.transaction():
        candidates = []
        for key, row in self._data.get(collection, {}).items():
            timestamp = _projection(collection, row).get(time_field)
            if timestamp is None or not all(k in row and row[k] == v for k, v in (filters or {}).items()):
                continue
            timestamp = parse_time(timestamp)
            if (
                start is not None
                and timestamp < parse_time(start)
                or end is not None
                and timestamp >= parse_time(end)
            ):
                continue
            candidates.append((timestamp, key, row))
        return [
            copy.deepcopy(r[2]) for r in sorted(candidates, key=lambda r: (r[0], r[1]), reverse=True)[:limit]
        ]


async def _iter_late_arrivals(
    self,
    collection,
    arrival_field,
    market_field,
    *,
    since=None,
    end=None,
    market_end,
    filters=None,
    batch_size=1000,
):
    """Arrival pages bounded independently from the inclusive frozen semantic market cutoff.

    Unknown legacy availability is excluded rather than backdated. Consumers normally pass a
    durable availability checkpoint as since and the scheduler tick as end. Both timestamps
    are typed and indexed; no complete market history is loaded to discover late arrivals.
    """
    _table(collection)
    for field in (arrival_field, market_field):
        if TABLE_COLUMNS[collection].get(field) != "timestamptz":
            raise ValueError(f"{collection}.{field} is not a typed timestamp column")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    since = parse_time(since) if since is not None else None
    end = parse_time(end) if end is not None else None
    market_end = parse_time(market_end)
    after = None
    from tank import clickhouse_market as chm

    ch = chm.client(self, collection)
    while True:
        if ch is not None:
            page = await chm.late_page(
                ch,
                collection,
                arrival_field,
                market_field,
                since=since,
                end=end,
                market_end=market_end,
                filters=filters,
                after=after,
                limit=batch_size,
            )
        elif isinstance(self, PostgresRepository):
            from psycopg import sql

            clauses, args = _exact_predicate(collection, filters)
            clauses += [
                sql.SQL("{} IS NOT NULL").format(sql.Identifier(arrival_field)),
                sql.SQL("{} <= %s").format(sql.Identifier(market_field)),
            ]
            args.append(market_end)
            if since is not None:
                clauses.append(sql.SQL("{} >= %s").format(sql.Identifier(arrival_field)))
                args.append(since)
            if end is not None:
                clauses.append(sql.SQL("{} < %s").format(sql.Identifier(arrival_field)))
                args.append(end)
            if after is not None:
                clauses.append(sql.SQL("({},storage_key)>(%s,%s)").format(sql.Identifier(arrival_field)))
                args.extend(after)
            async with self.transaction():
                query = sql.SQL(
                    "SELECT *,{} AS _page_time FROM {} WHERE {} ORDER BY {},storage_key LIMIT %s"
                ).format(
                    sql.Identifier(arrival_field),
                    sql.Identifier(*collection.split(".")),
                    sql.SQL(" AND ").join(clauses),
                    sql.Identifier(arrival_field),
                )
                rows = await (await self._conn().execute(query, [*args, batch_size])).fetchall()
                page = [
                    (_read_record(collection, row), (row[arrival_field], row["storage_key"])) for row in rows
                ]
        else:
            async with self.transaction():
                page = []
                for key, row in self._data.get(collection, {}).items():
                    projected = _projection(collection, row)
                    arrival, market = projected.get(arrival_field), projected.get(market_field)
                    if arrival is None or market is None:
                        continue
                    arrival, market = parse_time(arrival), parse_time(market)
                    if (
                        market > market_end
                        or since is not None
                        and arrival < since
                        or end is not None
                        and arrival >= end
                    ):
                        continue
                    if after is not None and (arrival, key) <= after:
                        continue
                    if all(k in row and row[k] == v for k, v in (filters or {}).items()):
                        page.append((copy.deepcopy(row), (arrival, key)))
                page.sort(key=lambda item: item[1])
                page = page[:batch_size]
        for document, _ in page:
            yield document
        if len(page) < batch_size:
            return
        after = page[-1][1]


async def _iter_metadata(self, collection, fields, *, filters=None, batch_size=1000):
    """Page selected top-level JSON fields without returning the full stored evidence payload.

    Keys absent from a record appear as None in the projection; exact filtering continues to
    operate on the original document, preserving missing-vs-null filter semantics.
    """
    _table(collection)
    fields = list(fields)
    if not fields or len(fields) > 50 or any(not isinstance(field, str) for field in fields):
        raise ValueError("fields must contain1..50 top-level string names")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if not isinstance(self, PostgresRepository) or collection in MARKET_COLUMNS:
        async for row in self.iter_all(collection, filters=filters, batch_size=batch_size):
            yield {field: row.get(field) for field in fields}
        return
    from psycopg import sql

    after = None
    expressions = sql.SQL(",").join(sql.SQL("%s::text,document -> %s::text") for _ in fields)
    projected_args = [key for field in fields for key in (field, field)]
    while True:
        clauses, args = _exact_predicate(collection, filters)
        if after is not None:
            clauses.append(sql.SQL("storage_key>%s"))
            args.append(after)
        predicate = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
        async with self.transaction():
            query = sql.SQL(
                "SELECT jsonb_build_object({}) AS metadata,storage_key FROM {} WHERE {} ORDER BY storage_key LIMIT %s"
            ).format(expressions, sql.Identifier(*collection.split(".")), predicate)
            page = await (await self._conn().execute(query, [*projected_args, *args, batch_size])).fetchall()
        for row in page:
            yield row["metadata"]
        if len(page) < batch_size:
            return
        after = page[-1]["storage_key"]


def _read_record(collection: str, row: StoredRow) -> Record:
    if collection in MARKET_COLUMNS:
        return decode_market(collection, row)
    document = row["document"]
    if not isinstance(document, dict):
        raise ValueError("Stored document must be an object")
    return document

"""Transactional PostgreSQL storage. MemoryRepository is an explicit test double only."""

import asyncio
import copy
import hashlib
import json
import logging
import random
import time
import weakref
from collections.abc import AsyncIterator, Collection, Iterable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from contextvars import ContextVar
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol, Self, overload

from tank.market_schema import MARKET_COLUMNS
from tank.market_storage import decode as decode_market
from tank.market_storage import (
    iter_market_projection,
    latest_market_records,
    market_dependency_head,
    market_series_counts,
    put_market_batch,
)
from tank.models import (
    APPEND_ONLY,
    LEAN_DOCUMENTS,
    SERIAL_TABLES,
    TABLE_COLUMNS,
    TIMESTAMP_PRECISION_FIELDS,
    Record,
)
from tank.time_precision import Timestamp, collection_clock, offset_schedule, propagate_timestamp
from tank.util import iso, jsonable, new_id, parse_time, utcnow


class ImmutableRecordError(ValueError):
    pass


type TimeInput = datetime | str | int | float | Decimal
type Records = list[Record]
type RangeCursor = tuple[TimeInput, str]
type RangePage = list[tuple[Record, tuple[str, str]]]
type RetainedWindow = tuple[str, datetime, datetime]
type RetainedWindows = list[RetainedWindow]
type HeadMap = dict[str, Record | None]
type MarketPruneCounts = dict[str, int | dict[str, int]]


class Repository(Protocol):
    async def open(self) -> Self: ...
    async def close(self) -> None: ...
    async def migrate(self) -> None: ...
    async def put(self, collection: str, key: str, record: Record, *, immutable: bool = False) -> Record: ...
    async def put_market_batch(
        self, collection: str, records: Iterable[Record], *, keys: Iterable[str] | None = None
    ) -> Records: ...
    async def latest_market_records(
        self,
        collection: str,
        *,
        group_by: Sequence[str] = ("instrument_id",),
        time_field: str = "ts",
        filters: Record | None = None,
        cutoff: TimeInput | None = None,
        assembled_at: TimeInput | None = None,
        start: TimeInput | None = None,
        instrument_ids: Collection[str] | None = None,
    ) -> Records: ...
    @overload
    async def market_dependency_head(
        self,
        collection: str,
        semantic_field: str,
        cutoff: TimeInput,
        *,
        start: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        order: str = "availability",
        orders: None = None,
    ) -> Record | None: ...
    @overload
    async def market_dependency_head(
        self,
        collection: str,
        semantic_field: str,
        cutoff: TimeInput,
        *,
        start: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        order: str = "availability",
        orders: Collection[str],
    ) -> HeadMap: ...
    def iter_market_projection(
        self,
        collection: str,
        fields: Sequence[str],
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[Record]: ...
    async def market_series_counts(
        self,
        instrument_ids: Collection[str],
        start: TimeInput,
        cutoff: TimeInput,
        assembled_at: TimeInput | None = None,
    ) -> dict[str, int]: ...
    async def get(self, collection: str, key: str) -> Record | None: ...
    async def list(self, collection: str, *, filters: Record | None = None, limit: int = 1000) -> Records: ...
    async def delete(self, collection: str, key: str) -> None: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    async def enqueue(
        self,
        kind: str,
        payload: Record,
        idem_key: str,
        *,
        priority: int = 1,
        run_after: TimeInput | Timestamp | None = None,
        expires_at: TimeInput | Timestamp | None = None,
        max_attempts: int = 3,
    ) -> Record: ...
    async def claim_job(self) -> Record | None: ...
    async def finish_job(self, job_id: str) -> None: ...
    async def fail_job(self, job_id: str, error: object, *, retry_after: float = 1) -> None: ...
    async def recover_jobs(self) -> int: ...
    async def prune_market(
        self,
        before: TimeInput,
        *,
        book_before: TimeInput | None = None,
        trades_before: TimeInput | None = None,
    ) -> MarketPruneCounts: ...
    async def prune_jobs(self, before: TimeInput) -> int: ...
    async def preserved_market_windows(self) -> RetainedWindows: ...
    async def latest_before(
        self, collection: str, time_field: str, cutoff: TimeInput, *, filters: Record | None = None
    ) -> Record | None: ...
    def iter_all(
        self, collection: str, *, filters: Record | None = None, batch_size: int = 1000
    ) -> AsyncIterator[Record]: ...
    def iter_metadata(
        self, collection: str, fields: Sequence[str], *, filters: Record | None = None, batch_size: int = 1000
    ) -> AsyncIterator[Record]: ...
    def iter_late_arrivals(
        self,
        collection: str,
        arrival_field: str,
        market_field: str,
        *,
        since: TimeInput | None = None,
        end: TimeInput | None = None,
        market_end: TimeInput,
        filters: Record | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[Record]: ...
    async def list_latest(
        self,
        collection: str,
        time_field: str,
        *,
        filters: Record | None = None,
        limit: int = 100,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
    ) -> Records: ...
    async def list_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        limit: int = 1000,
        after: RangeCursor | None = None,
    ) -> Records: ...
    async def page_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        limit: int = 1000,
        after: RangeCursor | None = None,
    ) -> RangePage: ...
    def iter_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        batch_size: int = 10000,
    ) -> AsyncIterator[Record]: ...


log = logging.getLogger(__name__)

BASELINE = "001_schema.sql"

RUNNING_CAPS = {"REACTION_CAPTURE": 1, "RUN_MARKET_TRUTH": 1}


EARLIEST_EXPIRY_FIRST = ("REACTION_CAPTURE",)
REVISIONS_LAST = ("REACTION_CAPTURE",)
CLAIM_SLOW_S, DELIVER_LATE_S = 0.5, 1.0


YIELDS_TO_ANALYSIS = ("REACTION_CAPTURE",)
ANALYSIS_TYPES = ("RUN_MARKET_TRUTH", "FILTER_ITEM", "ROUTE_ITEM", "AGENT_MESSAGE")
FRESH_EVENT_S, EXPIRY_SAFETY_S = 600, 1200
DEFERRAL_LOG_S = 60


CLAIM_SQL = """WITH expired AS (
    UPDATE ops.jobs SET status='EXPIRED', document=jsonb_set(document,'{status}','"EXPIRED"')
    WHERE status='QUEUED' AND expires_at <= now() RETURNING 1),
  failed AS (
    UPDATE ops.jobs SET status='FAILED', document=jsonb_set(document,'{status}','"FAILED"')
    WHERE status='QUEUED' AND attempts >= max_attempts
    AND coalesce((document->>'recovery_reclaim')::boolean,false)=false RETURNING 1),
  gate AS (
    SELECT EXISTS (SELECT 1 FROM ops.jobs b WHERE b.status='RUNNING'
        AND b.type=ANY(%(analysis)s::text[]))
      OR EXISTS (SELECT 1 FROM ops.jobs b WHERE b.status='QUEUED'
        AND b.type=ANY(%(analysis)s::text[]) AND b.run_after<=now()
        AND (b.expires_at IS NULL OR b.expires_at>now())) AS analysis_busy,
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
    ORDER BY priority,CASE WHEN type=ANY(%(revisions_last)s::text[])
      THEN coalesce(document#>>'{payload,automatic_revision}'='true',false) END NULLS LAST,
      CASE WHEN type=ANY(%(edf)s::text[]) THEN expires_at END NULLS LAST,
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
      AND (SELECT analysis_busy OR fresh_event FROM gate)) AS deferred"""


def claim_params(held):
    """Parameters of CLAIM_SQL; held lists the capped job types whose claim lock this transaction holds."""
    return {
        "analysis": list(ANALYSIS_TYPES),
        "fresh": FRESH_EVENT_S,
        "capped": list(RUNNING_CAPS),
        "caps": list(RUNNING_CAPS.values()),
        "held": list(held),
        "yields": list(YIELDS_TO_ANALYSIS),
        "safety": EXPIRY_SAFETY_S,
        "edf": list(EARLIEST_EXPIRY_FIRST),
        "revisions_last": list(REVISIONS_LAST),
    }


def _analysis_gate_memory(data, now):
    """(analysis_busy, fresh_event_without_card) for the memory repository."""
    busy = any(
        job["type"] in ANALYSIS_TYPES
        and (
            job["status"] == "RUNNING"
            or (
                job["status"] == "QUEUED"
                and parse_time(job["run_after"]) <= now
                and not (job.get("expires_at") and parse_time(job["expires_at"]) <= now)
            )
        )
        for job in data.get("ops.jobs", {}).values()
    )
    carded = {a.get("event_id") for a in data.get("out.alerts", {}).values() if a.get("kind") == "FULL"}
    fresh = any(
        e.get("known_to_us_at")
        and parse_time(e["known_to_us_at"]) > now - timedelta(seconds=FRESH_EVENT_S)
        and e.get("event_id") not in carded
        for e in data.get("evt.events", {}).values()
    )
    return busy, fresh


def _table(collection):
    if collection not in TABLE_COLUMNS:
        raise ValueError(f"Unknown collection: {collection}")
    return collection


def _queue_anchor(value, field):
    if isinstance(value, Timestamp):
        return value
    return propagate_timestamp({field: value}, field)


def _stamp_job_clock_fields(record):
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


DECISION_JOBS = ("FILTER_ITEM", "ROUTE_ITEM")
DECISION_MAX_ATTEMPTS = 30
DECISION_RETRY_FIRST_S, DECISION_RETRY_MAX_S = 15, 900


def retry_delay(kind, attempts):
    """Seconds before a failed job runs again after its attempt number `attempts`.

    FILTER_ITEM and ROUTE_ITEM back off from 15 s, doubling to 15 min, so a Decision-1 outage is
    retried across the whole 6 h job window instead of failing within seconds; other kinds keep
    the short generic backoff."""
    if kind in DECISION_JOBS:
        return min(DECISION_RETRY_MAX_S, DECISION_RETRY_FIRST_S * 2 ** max(0, attempts - 1))
    return min(60, 2**attempts)


def _job(kind, payload, idem_key, priority, run_after, expires_at, max_attempts):
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
    result = {
        "job_id": new_id("JOB"),
        "type": kind,
        "priority": priority,
        "idem_key": idem_key,
        "payload": jsonable(payload),
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
    return result


class RepositoryQueries:
    """Backend-neutral repository queries shared by the Memory and Postgres repositories."""

    async def put_market_batch(
        self, collection: str, records: Iterable[Record], *, keys: Iterable[str] | None = None
    ) -> Records:
        return await put_market_batch(self, collection, records, keys=keys)

    async def latest_market_records(
        self,
        collection: str,
        *,
        group_by: Sequence[str] = ("instrument_id",),
        time_field: str = "ts",
        filters: Record | None = None,
        cutoff: TimeInput | None = None,
        assembled_at: TimeInput | None = None,
        start: TimeInput | None = None,
        instrument_ids: Collection[str] | None = None,
    ) -> Records:
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

    @overload
    async def market_dependency_head(
        self,
        collection: str,
        semantic_field: str,
        cutoff: TimeInput,
        *,
        start: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        order: str = "availability",
        orders: None = None,
    ) -> Record | None: ...

    @overload
    async def market_dependency_head(
        self,
        collection: str,
        semantic_field: str,
        cutoff: TimeInput,
        *,
        start: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        order: str = "availability",
        orders: Collection[str],
    ) -> HeadMap: ...

    async def market_dependency_head(
        self,
        collection: str,
        semantic_field: str,
        cutoff: TimeInput,
        *,
        start: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        order: str = "availability",
        orders: Collection[str] | None = None,
    ) -> Record | HeadMap | None:
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

    def iter_market_projection(
        self,
        collection: str,
        fields: Sequence[str],
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[Record]:
        return iter_market_projection(
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
        )

    async def market_series_counts(
        self,
        instrument_ids: Collection[str],
        start: TimeInput,
        cutoff: TimeInput,
        assembled_at: TimeInput | None = None,
    ) -> dict[str, int]:
        return await market_series_counts(self, instrument_ids, start, cutoff, assembled_at)

    async def prune_market(
        self,
        before: TimeInput,
        *,
        book_before: TimeInput | None = None,
        trades_before: TimeInput | None = None,
    ) -> MarketPruneCounts:
        return await _prune_market(self, before, book_before=book_before, trades_before=trades_before)

    async def prune_jobs(self, before: TimeInput) -> int:
        return await _prune_jobs(self, before)

    async def preserved_market_windows(self) -> RetainedWindows:
        return await _preserved_windows(self)

    async def latest_before(
        self, collection: str, time_field: str, cutoff: TimeInput, *, filters: Record | None = None
    ) -> Record | None:
        return await _latest_before(self, collection, time_field, cutoff, filters=filters)

    def iter_all(
        self, collection: str, *, filters: Record | None = None, batch_size: int = 1000
    ) -> AsyncIterator[Record]:
        return _iter_all(self, collection, filters=filters, batch_size=batch_size)

    def iter_metadata(
        self, collection: str, fields: Sequence[str], *, filters: Record | None = None, batch_size: int = 1000
    ) -> AsyncIterator[Record]:
        return _iter_metadata(self, collection, fields, filters=filters, batch_size=batch_size)

    def iter_late_arrivals(
        self,
        collection: str,
        arrival_field: str,
        market_field: str,
        *,
        since: TimeInput | None = None,
        end: TimeInput | None = None,
        market_end: TimeInput,
        filters: Record | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[Record]:
        return _iter_late_arrivals(
            self,
            collection,
            arrival_field,
            market_field,
            since=since,
            end=end,
            market_end=market_end,
            filters=filters,
            batch_size=batch_size,
        )

    async def list_latest(
        self,
        collection: str,
        time_field: str,
        *,
        filters: Record | None = None,
        limit: int = 100,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
    ) -> Records:
        return await _list_latest(
            self, collection, time_field, filters=filters, limit=limit, start=start, end=end
        )

    async def list_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        limit: int = 1000,
        after: RangeCursor | None = None,
    ) -> Records:
        return await _list_range(
            self, collection, time_field, start=start, end=end, filters=filters, limit=limit, after=after
        )

    async def page_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        limit: int = 1000,
        after: RangeCursor | None = None,
    ) -> RangePage:
        return await _range_page(
            self, collection, time_field, start=start, end=end, filters=filters, limit=limit, after=after
        )

    def iter_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        batch_size: int = 10000,
    ) -> AsyncIterator[Record]:
        return _iter_range(
            self, collection, time_field, start=start, end=end, filters=filters, batch_size=batch_size
        )


class QueueMixin(RepositoryQueries):
    async def enqueue(
        self, kind, payload, idem_key, *, priority=1, run_after=None, expires_at=None, max_attempts=3
    ):
        async with self.lock(f"queue:{kind}:{idem_key}"):
            old = await self.list("ops.jobs", filters={"type": kind, "idem_key": idem_key}, limit=1)
            if old:
                return old[0]
            job = _job(kind, payload, idem_key, priority, run_after, expires_at, max_attempts)
            return await self.put("ops.jobs", job["job_id"], job)

    async def finish_job(self, job_id):
        await self._transition_job(job_id, "DONE")

    async def fail_job(self, job_id, error, *, retry_after=1):
        async with self.lock(f"job:{job_id}"):
            job = await self.get("ops.jobs", job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] != "RUNNING":
                return
            job["last_error"] = str(error)
            expired = job.get("expires_at") and parse_time(job["expires_at"]) <= utcnow()
            job["status"] = (
                "EXPIRED" if expired else ("FAILED" if job["attempts"] >= job["max_attempts"] else "QUEUED")
            )
            job.update(
                offset_schedule(collection_clock(), timedelta(seconds=retry_after)).fields("run_after")
            )
            if job["status"] in ("FAILED", "EXPIRED"):
                job.update(collection_clock().fields("finished_at"))
            else:
                job.update(Timestamp(None, None, "PENDING").fields("finished_at"))
            await self.put("ops.jobs", job_id, job)

    async def _transition_job(self, job_id, status):
        async with self.lock(f"job:{job_id}"):
            job = await self.get("ops.jobs", job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] == "RUNNING":
                job["status"] = status
                job.update(collection_clock().fields("finished_at"))
                await self.put("ops.jobs", job_id, job)


class _MemoryAdvisoryLock:
    def __init__(self):
        self.mutex = asyncio.Lock()
        self.owner = None
        self.depth = 0
        self.users = 0


class MemoryRepository(QueueMixin):
    """Deterministic test implementation with nested rollback and task-local ownership."""

    def __init__(self):
        self._data = {}
        self._immutable = set()
        self._mutex = asyncio.Lock()
        self._owner = None
        self._locks = {}

    async def open(self):
        return self

    async def close(self):
        pass

    async def migrate(self):
        pass

    @asynccontextmanager
    async def transaction(self):
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
    async def _read(self):
        """Read consistent state without a write transaction's rollback snapshot."""
        if self._owner is asyncio.current_task():
            yield self
        else:
            async with self._mutex:
                yield self

    @asynccontextmanager
    async def lock(self, key):
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

    async def put(self, collection, key, record, *, immutable=False):
        if collection in MARKET_COLUMNS:
            changed = await self.put_market_batch(collection, [record], keys=[str(key)])
            return changed[0] if changed else await self.get(collection, key)
        _table(collection)
        key = str(key)
        record = jsonable(record)
        async with self.transaction():
            old = self._data.get(collection, {}).get(key)
            if old is not None and (
                (collection, key) in self._immutable or collection in APPEND_ONLY or immutable
            ):
                if old != record:
                    raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
                if immutable:
                    self._immutable.add((collection, key))
                return copy.deepcopy(old)
            if collection == "ops.jobs" and any(
                r["type"] == record["type"] and r["idem_key"] == record["idem_key"] and k != key
                for k, r in self._data.get(collection, {}).items()
            ):
                raise ValueError("Duplicate job idempotency key")
            if collection == "ops.jobs":
                _stamp_job_clock_fields(record)
            self._data.setdefault(collection, {})[key] = copy.deepcopy(record)
            if immutable or collection in APPEND_ONLY:
                self._immutable.add((collection, key))
            return copy.deepcopy(record)

    async def get(self, collection, key):
        _table(collection)
        async with self._read():
            return copy.deepcopy(self._data.get(collection, {}).get(str(key)))

    async def list(self, collection, *, filters=None, limit=1000):
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

    async def delete(self, collection, key):
        _table(collection)
        async with self.transaction():
            if collection in APPEND_ONLY or (collection, str(key)) in self._immutable:
                raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
            self._data.get(collection, {}).pop(str(key), None)

    async def claim_job(self):
        async with self.transaction():
            now = utcnow()
            candidates = []
            for job in self._data.get("ops.jobs", {}).values():
                if job["status"] != "QUEUED":
                    continue
                if job.get("expires_at") and parse_time(job["expires_at"]) <= now:
                    job["status"] = "EXPIRED"
                    job.update(collection_clock().fields("finished_at"))
                elif job["attempts"] >= job["max_attempts"] and not job.get("recovery_reclaim"):
                    job["status"] = "FAILED"
                    job.update(collection_clock().fields("finished_at"))
                elif parse_time(job["run_after"]) <= now:
                    candidates.append(job)
            running = {}
            for job in self._data.get("ops.jobs", {}).values():
                if job["status"] == "RUNNING":
                    running[job["type"]] = running.get(job["type"], 0) + 1
            candidates = [
                job
                for job in candidates
                if running.get(job["type"], 0) < RUNNING_CAPS.get(job["type"], 10**9)
            ]
            busy, fresh = _analysis_gate_memory(self._data, now)
            if busy or fresh:
                safety = now + timedelta(seconds=EXPIRY_SAFETY_S)
                candidates = [
                    job
                    for job in candidates
                    if job["type"] not in YIELDS_TO_ANALYSIS
                    or (job.get("expires_at") and parse_time(job["expires_at"]) <= safety)
                ]
            if not candidates:
                return None

            def order(r):
                deadline = r.get("expires_at") if r["type"] in EARLIEST_EXPIRY_FIRST else None
                revision = (
                    bool((r.get("payload") or {}).get("automatic_revision"))
                    if r["type"] in REVISIONS_LAST
                    else None
                )
                return (
                    r["priority"],
                    revision is None,
                    bool(revision),
                    deadline is None,
                    parse_time(deadline) if deadline else now,
                    r["run_after"],
                    r["job_id"],
                )

            job = min(candidates, key=order)
            job["status"] = "RUNNING"
            if not job.pop("recovery_reclaim", False):
                job["attempts"] += 1
            return copy.deepcopy(job)

    async def recover_jobs(self):
        async with self.transaction():
            count = 0
            for job in self._data.get("ops.jobs", {}).values():
                if job["status"] == "RUNNING":
                    job["status"] = (
                        "EXPIRED"
                        if job.get("expires_at") and parse_time(job["expires_at"]) <= utcnow()
                        else "QUEUED"
                    )
                    if job["status"] == "EXPIRED":
                        job.update(collection_clock().fields("finished_at"))
                    else:
                        job.update(
                            recovery_reclaim=True,
                            recovery_count=job.get("recovery_count", 0) + 1,
                        )
                        job.update(collection_clock().fields("recovery_at"))
                        job.update(Timestamp(None, None, "PENDING").fields("finished_at"))
                    count += 1
            return count


def _projection(collection, record):
    """Map §2 nested names to §14 indexed columns without changing the document."""
    values = dict(record)
    aliases = {
        "body": "text",
        "raw_path": "raw_ref",
        "claim_type": "type",
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
                if isinstance(record.get(obj), dict) and source in record[obj]:
                    values[target] = record[obj][source]
    if collection == "ops.llm_usage":
        for column, aliases in {
            "at": ("created_at",),
            "model": ("model_id",),
            "subject_id": ("event_id", "item_id", "session_id", "proposal_id"),
        }.items():
            if values.get(column) is None:
                selected = next((a for a in aliases if record.get(a) is not None), None)
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
        usage = record.get("usage") or {}
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
        if isinstance(record.get("opened_by"), (list, tuple)):
            values["opened_by_alert"], values["opened_by_version"] = record["opened_by"]
        elif isinstance(record.get("opened_by"), dict):
            values["opened_by_alert"] = record["opened_by"].get("alert_id")
            values["opened_by_version"] = record["opened_by"].get("version")
    if collection == "evt.claims":
        measured = record.get("value")
        if isinstance(measured, dict):
            values.setdefault("value_num", measured.get("number"))
            values.setdefault("value_unit", measured.get("unit"))
        if not values.get("quote") and record.get("evidence"):
            first_evidence = record["evidence"][0]
            if isinstance(first_evidence, dict):
                values["quote"] = first_evidence.get("quote")
    if collection == "ana.decisions" and not values.get("mta_id"):
        values["mta_id"] = (record.get("provenance") or {}).get("mta_id")

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
            projected_array = []
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
        if datatype == "timestamptz" and isinstance(values.get(column), dict):
            temporal = values[column]
            values[column] = temporal.get("value", temporal.get("ts"))

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


async def _finish_critical(awaitable):
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
async def _stable_transaction(conn):
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
    def __init__(self, database_url):
        self.database_url = database_url
        self._pool = None
        self._connection = ContextVar(f"pg_connection_{id(self)}", default=None)

    async def open(self):
        from psycopg import AsyncConnection
        from psycopg.rows import dict_row
        from psycopg_pool import AsyncConnectionPool

        class CancellationSafeConnection(AsyncConnection):
            async def wait(connection, *args, **kwargs):

                operation = asyncio.create_task(super().wait(*args, **kwargs))
                try:
                    return await asyncio.shield(operation)
                except asyncio.CancelledError:
                    if not operation.done():
                        operation.cancel()
                    with suppress(asyncio.CancelledError):
                        await _finish_critical(operation)
                    raise

        self._pool = AsyncConnectionPool(
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

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self):
        existing = self._connection.get()
        task = asyncio.current_task()

        if existing is not None and existing[1]() is task:
            async with _stable_transaction(existing[0]()):
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

    def _conn(self):
        existing = self._connection.get()
        if existing is None or existing[1]() is not asyncio.current_task() or existing[0]() is None:
            raise RuntimeError("Connection access requires a transaction owned by the current task")
        return existing[0]()

    @asynccontextmanager
    async def lock(self, key):
        digest = int.from_bytes(hashlib.sha256(str(key).encode()).digest()[:8], "big", signed=True)
        existing = self._connection.get()
        if existing is None or existing[1]() is not asyncio.current_task():
            while True:
                async with self.transaction():
                    row = await (
                        await self._conn().execute("SELECT pg_try_advisory_xact_lock(%s) AS ok", (digest,))
                    ).fetchone()
                    if row["ok"]:
                        yield self
                        return
                await asyncio.sleep(random.uniform(0.05, 0.2))

        async with self.transaction():
            await self._conn().execute("SELECT pg_advisory_xact_lock(%s)", (digest,))
            yield self

    @asynccontextmanager
    async def _migration_connection(self):
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
        token = self._connection.set((weakref.ref(conn), weakref.ref(task)))
        digest = int.from_bytes(hashlib.sha256(b"tank:migrations").digest()[:8], "big", signed=True)
        try:
            async with _stable_transaction(conn):
                await conn.execute("SELECT pg_advisory_lock(%s)", (digest,))
            yield conn
        finally:
            self._connection.reset(token)

            async def release():
                try:
                    async with _stable_transaction(conn):
                        await conn.execute("SELECT pg_advisory_unlock(%s)", (digest,))
                except BaseException:
                    await conn.close()
                await pool.putconn(conn)

            _, cancelled = await _finish_critical(release())
            if cancelled:
                raise cancelled

    async def migrate(self, *, target=None):
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
                if adopted["adopt"]:
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

    async def put(self, collection, key, record, *, immutable=False):
        if collection in MARKET_COLUMNS:
            changed = await self.put_market_batch(collection, [record], keys=[str(key)])
            return changed[0] if changed else await self.get(collection, key)
        from psycopg import sql
        from psycopg.types.json import Jsonb

        _table(collection)
        key, record = str(key), jsonable(record)
        async with self.lock(f"record:{collection}:{key}"):
            conn = self._conn()
            table = sql.Identifier(*collection.split("."))
            old = await (
                await conn.execute(
                    sql.SQL("SELECT document, immutable FROM {} WHERE storage_key=%s").format(table),
                    (key,),
                )
            ).fetchone()
            if old and (old["immutable"] or immutable or collection in APPEND_ONLY):
                if old["document"] != record:
                    raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
                if immutable and not old["immutable"]:
                    await conn.execute(
                        sql.SQL("UPDATE {} SET immutable=true WHERE storage_key=%s").format(table), (key,)
                    )
                return old["document"]
            projected = _projection(collection, record)
            if collection in SERIAL_TABLES and projected.get("id") is None:
                projected.pop("id", None)
            values = [key, Jsonb(record), immutable or collection in APPEND_ONLY]
            for column, value in projected.items():
                datatype = TABLE_COLUMNS[collection][column]
                if value is not None:
                    if datatype == "jsonb":
                        value = Jsonb(value)
                    elif datatype == "timestamptz":
                        value = parse_time(value)
                    elif datatype == "text" and not isinstance(value, str):
                        value = json.dumps(value, sort_keys=True)
                values.append(value)
            names = ["storage_key", "document", "immutable", *projected]
            columns = sql.SQL(",").join(map(sql.Identifier, names))
            assignments = sql.SQL(",").join(
                sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(n), sql.Identifier(n)) for n in names[1:]
            )
            query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT(storage_key) DO UPDATE SET {}"
            ).format(table, columns, sql.SQL(",").join(sql.Placeholder() for _ in names), assignments)
            await conn.execute(query, values)
            return copy.deepcopy(record)

    async def get(self, collection, key):
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

    async def list(self, collection, *, filters=None, limit=1000):
        if collection in MARKET_COLUMNS:
            result = []
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

        clauses, args = [], []
        document = sql.Identifier(_document_column(collection, filters or {}))
        for k, v in (filters or {}).items():
            clauses.append(sql.SQL("{} -> %s = %s").format(document))
            args.extend([k, Jsonb(jsonable(v))])
            datatype = TABLE_COLUMNS[collection].get(k)
            if (
                datatype
                and v is not None
                and datatype not in ("jsonb", "text[]")
                and not isinstance(v, (dict, list))
            ):
                typed_value = v
                if datatype == "timestamptz":
                    try:
                        typed_value = parse_time(v)
                    except (ValueError, TypeError):
                        continue
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

    async def delete(self, collection, key):
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

    async def claim_job(self):
        started = time.monotonic()
        job = await self._claim_job()
        elapsed = time.monotonic() - started

        late = None
        if job and job.get("type") == "DELIVER" and job.get("attempts") == 1 and job.get("run_after"):
            late = (utcnow() - parse_time(job["run_after"])).total_seconds()
        if elapsed > CLAIM_SLOW_S or (late is not None and late > DELIVER_LATE_S):
            log.info(
                "claim_job %.2f s, type %s, %s",
                elapsed,
                job.get("type") if job else None,
                f"first claimed {late:.2f} s after run_after" if late is not None else "no DELIVER",
            )
        return job

    async def _claim_job(self):
        async with self.transaction():
            conn = self._conn()

            held = (
                await (
                    await conn.execute(
                        "SELECT coalesce(array_agg(t), '{}') AS held FROM unnest(%s::text[]) AS t"
                        " WHERE pg_try_advisory_xact_lock(hashtextextended('tank:job_cap:' || t, 0))",
                        (list(RUNNING_CAPS),),
                    )
                ).fetchone()
            )["held"]
            row = await (
                await conn.execute(
                    CLAIM_SQL,
                    claim_params(held),
                )
            ).fetchone()
            if row["deferred"]:
                self._log_deferral(row["deferred"], row["analysis_busy"], row["fresh_event"])
            return row["document"]

    def _log_deferral(self, count, busy, fresh):
        """Log deferred captures when the picture changes, and at least once a minute."""
        reasons = tuple(
            r for r, on in (("analysis job running or due", busy), ("event without FULL card", fresh)) if on
        )
        now = time.monotonic()
        last = getattr(self, "_last_deferral", None)
        if last and last[1:] == (count, reasons) and now - last[0] < DEFERRAL_LOG_S:
            return
        self._last_deferral = (now, count, reasons)
        log.info("deferring %d due REACTION_CAPTURE behind event analysis: %s", count, ", ".join(reasons))

    async def recover_jobs(self):
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
            document = sql.Identifier(_document_column(collection, filters))
            clauses.append(sql.SQL("{} @> %s").format(document))
            args.append(Jsonb(jsonable(filters)))
            for key, value in filters.items():
                clauses.append(sql.SQL("{} -> %s = %s").format(document))
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
            document = sql.Identifier(_document_column(collection, filters))
            clauses.append(sql.SQL("{} @> %s").format(document))
            args.append(Jsonb(jsonable(filters)))
            for key, value in filters.items():
                clauses.append(sql.SQL("{} -> %s = %s").format(document))
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


def _document_column(collection, keys):
    """The jsonb column that answers reads of the given top-level keys.

    A LEAN_DOCUMENTS table answers from its generated metadata column, which PostgreSQL
    keeps equal to the document without the bulky key, so reads and filters do not
    detoast that payload; only a read of the bulky key itself uses the document.
    """
    omitted = LEAN_DOCUMENTS.get(collection)
    return "metadata" if omitted is not None and omitted not in keys else "document"


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
        document = sql.Identifier(_document_column(collection, filters))
        clauses.append(sql.SQL("{} @> %s").format(document))
        args.append(Jsonb(jsonable(filters)))
        for key, value in filters.items():
            clauses.append(sql.SQL("{} -> %s = %s").format(document))
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
    operate on the stored JSON, preserving missing-vs-null filter semantics. A LEAN_DOCUMENTS
    table is read from its metadata column, so the bulky key is not detoasted per row.
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
    after = None
    while True:
        query, args = metadata_page_sql(
            collection, fields, filters=filters, after=after, batch_size=batch_size
        )
        async with self.transaction():
            page = await (await self._conn().execute(query, args)).fetchall()
        for row in page:
            yield row["projection"]
        if len(page) < batch_size:
            return
        after = page[-1]["storage_key"]


def metadata_page_sql(collection, fields, *, filters=None, after=None, batch_size=1000):
    """The PostgreSQL query and parameters of one iter_metadata page after storage key after."""
    from psycopg import sql

    document = sql.Identifier(_document_column(collection, fields))
    expressions = sql.SQL(",").join(sql.SQL("%s::text,{} -> %s::text").format(document) for _ in fields)
    clauses, args = _exact_predicate(collection, filters)
    if after is not None:
        clauses.append(sql.SQL("storage_key>%s"))
        args.append(after)
    predicate = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
    query = sql.SQL(
        "SELECT jsonb_build_object({}) AS projection,storage_key FROM {} WHERE {} ORDER BY storage_key LIMIT %s"
    ).format(expressions, sql.Identifier(*collection.split(".")), predicate)
    return query, [*(key for field in fields for key in (field, field)), *args, batch_size]


def _read_record(collection, row):
    return decode_market(collection, row) if collection in MARKET_COLUMNS else row["document"]

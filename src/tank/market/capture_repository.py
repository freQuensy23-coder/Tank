"""Validated capture persistence; Python writes control records, never external capture data."""

from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime
from typing import Literal, Protocol

from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.contracts.repository import Collection, DocumentStore
from tank.market.capture import event_anchor
from tank.market.capture_contracts import (
    BackfillEnvelope,
    BackfillRun,
    CaptureAssessment,
    CaptureEventContext,
    CaptureTierTransition,
    HotToken,
    HotTokenIntent,
    NewBackfillRequest,
    NewRepairRequest,
    RepairEnvelope,
    RepairRequest,
)
from tank.time_precision import Timestamp
from tank.util import parse_time

type CaptureWriteCollection = Literal["mkt.hot_tokens", "mkt.backfill_runs", "mkt.capture_requests"]


class CaptureInvariantError(RuntimeError):
    pass


class CaptureDocumentStore(DocumentStore, Protocol):
    def transaction(self) -> AbstractAsyncContextManager[DocumentStore]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[DocumentStore]: ...
    async def put_market_batch(
        self,
        collection: str,
        records: Iterable[Mapping[str, JsonValue]],
        *,
        keys: Sequence[str] | None = None,
    ) -> list[dict[str, JsonValue]]: ...


class WatchExpiry(DomainModel):
    cond_id: str
    expires_at: str


def _clock_fields(field: str, stamp: Timestamp) -> dict[str, JsonValue]:
    """Control tables store the declared triplet; source events retain bucket-origin provenance."""
    return {
        field: stamp.value,
        field + "_prec": stamp.precision,
        field + "_prec_reason": stamp.precision_reason,
    }


def _projection(record: Mapping[str, JsonValue], fields: Sequence[str]) -> dict[str, JsonValue]:
    return {field: record[field] for field in fields if field in record}


def _redacted(document: dict[str, JsonValue], secrets: Sequence[str]) -> dict[str, JsonValue]:
    def text(value: str) -> str:
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return value

    def value(node: JsonValue) -> JsonValue:
        if isinstance(node, str):
            return text(node)
        if isinstance(node, list):
            return [value(child) for child in node]
        if isinstance(node, dict):
            return {text(key): value(child) for key, child in node.items()}
        return node

    return {text(key): value(child) for key, child in document.items()}


class CaptureRepository:
    """Typed named queries isolate wire documents from capture application decisions."""

    def __init__(
        self,
        store: CaptureDocumentStore,
        *,
        config_version: str,
        code_version: str,
        secrets: Callable[[], Sequence[str]],
    ) -> None:
        self._store = store
        self._config_version = config_version
        self._code_version = code_version
        self._secrets = secrets
        self._hot = Collection("mkt.hot_tokens", HotToken)
        self._backfills = Collection("mkt.backfill_runs", BackfillEnvelope)
        self._repairs = Collection("mkt.capture_requests", RepairEnvelope)
        self._tiers = Collection("ops.capture_tier_transitions", CaptureTierTransition)

    @asynccontextmanager
    async def hot_set_lock(self) -> AsyncIterator[None]:
        async with self._store.lock("market-hot-set"):
            yield None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self._store.transaction():
            yield None

    async def get_event_context(self, event_id: str) -> CaptureEventContext | None:
        record = await self._store.get("evt.events", event_id)
        if record is None:
            return None
        context = CaptureEventContext.model_validate(
            {
                **_projection(record, ("event_id",)),
                "current_ems_id": record.get("current_ems_id"),
                "anchor": event_anchor(record),
            }
        )
        if context.event_id != event_id:
            raise CaptureInvariantError("Capture event identity differs from its durable key")
        return context

    def _assessment(self, record: Mapping[str, JsonValue]) -> CaptureAssessment:
        return CaptureAssessment.model_validate(
            _projection(record, ("ems_id", "event_id", "version", "cutoff", "class"))
        )

    async def current_assessment(self, event: CaptureEventContext) -> CaptureAssessment | None:
        if event.current_ems_id is not None:
            record = await self._store.get("evt.ems_assessments", event.current_ems_id)
            if record is None:
                raise CaptureInvariantError("Capture event references a missing current assessment")
            assessment = self._assessment(record)
            if assessment.event_id != event.event_id or assessment.ems_id != event.current_ems_id:
                raise CaptureInvariantError("Current capture assessment belongs to a different event")
            return assessment
        assessments = [
            self._assessment(record)
            async for record in self._store.iter_all(
                "evt.ems_assessments", filters={"event_id": event.event_id}, batch_size=1000
            )
        ]
        if not assessments:
            return None
        return max(assessments, key=lambda row: (row.version, parse_time(row.cutoff), row.ems_id))

    async def hot_tokens(self, token_id: str | None = None) -> list[HotToken]:
        filters: dict[str, JsonValue] | None = None if token_id is None else {"token_id": token_id}
        return [
            self._hot.decode(record)
            async for record in self._store.iter_all("mkt.hot_tokens", filters=filters, batch_size=1000)
        ]

    def _metadata(self, recorded_at: Timestamp) -> dict[str, JsonValue]:
        return {
            "config_version": self._config_version,
            "code_version": self._code_version,
            **_clock_fields("recorded_at", recorded_at),
        }

    async def _write_market(
        self, collection: CaptureWriteCollection, key: str, document: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        await self._store.put_market_batch(collection, [_redacted(document, self._secrets())], keys=[key])
        stored = await self._store.get(collection, key)
        if stored is None:
            raise CaptureInvariantError("Acknowledged capture control write has no durable record")
        return stored

    async def put_hot(self, intent: HotTokenIntent, recorded_at: Timestamp) -> HotToken:
        expiry = (
            Timestamp(None, None, "PROTECTED_BY_OPEN_CALL_OR_ARMED_WATCH")
            if intent.expires_at is None
            else intent.expires_at
        )
        document = {
            "token_id": intent.token_id,
            "event_id": intent.event_id,
            "priority": intent.priority,
            **_clock_fields("since", intent.since),
            **_clock_fields("expires_at", expiry),
            **self._metadata(recorded_at),
        }
        entity = self._hot.decode(document)
        stored = await self._write_market(
            "mkt.hot_tokens",
            intent.token_id + ":" + intent.event_id,
            self._hot.encode(entity, exclude_unset=True),
        )
        return self._hot.decode(stored)

    async def delete_hot(self, token_id: str, event_id: str) -> None:
        await self._store.delete("mkt.hot_tokens", token_id + ":" + event_id)

    async def backfill(self, backfill_id: str) -> BackfillRun | None:
        record = await self._store.get("mkt.backfill_runs", backfill_id)
        if record is None:
            return None
        result = self._backfills.decode(record).root
        if result.backfill_id != backfill_id:
            raise CaptureInvariantError("Capture obligation identity differs from its durable key")
        return result

    async def backfills(self, token_id: str, event_id: str | None = None) -> list[BackfillRun]:
        filters: dict[str, JsonValue] = {"token_id": token_id}
        if event_id is not None:
            filters["event_id"] = event_id
        return [
            self._backfills.decode(record).root
            async for record in self._store.iter_all("mkt.backfill_runs", filters=filters, batch_size=1000)
        ]

    async def request_backfill(self, request: NewBackfillRequest) -> BackfillRun:
        pending = Timestamp(None, None, "PENDING")
        document = {
            "backfill_id": request.backfill_id,
            "event_id": request.event_id,
            "token_id": request.token_id,
            **_clock_fields("anchor", request.anchor),
            **_clock_fields("started_at", pending),
            **_clock_fields("finished_at", pending),
            "status": "PENDING",
            "reason": "PRE_EVENT_CAPTURE_PENDING",
            "result": None,
            **self._metadata(request.requested_at),
        }
        entity = self._backfills.decode(document)
        stored = await self._write_market(
            "mkt.backfill_runs",
            request.backfill_id,
            self._backfills.encode(entity, exclude_unset=True),
        )
        return self._backfills.decode(stored).root

    async def request_repair(self, request: NewRepairRequest) -> RepairRequest:
        pending = Timestamp(None, None, "PENDING")
        document = {
            "request_id": request.request_id,
            "kind": "REPAIR_WINDOWS",
            "token_id": request.token_id,
            **_clock_fields("start", request.start),
            **_clock_fields("end", request.end),
            "budget_s": str(request.budget_s),
            **_clock_fields("requested_at", request.requested_at),
            **_clock_fields("deadline", request.deadline),
            **_clock_fields("started_at", pending),
            **_clock_fields("finished_at", pending),
            "status": "PENDING",
            "reason": None,
            "result": None,
            **self._metadata(request.requested_at),
        }
        entity = self._repairs.decode(document)
        stored = await self._write_market(
            "mkt.capture_requests",
            request.request_id,
            self._repairs.encode(entity, exclude_unset=True),
        )
        return self._repairs.decode(stored).root

    async def repair(self, request_id: str) -> RepairRequest | None:
        record = await self._store.get("mkt.capture_requests", request_id)
        if record is None:
            return None
        result = self._repairs.decode(record).root
        if result.request_id != request_id:
            raise CaptureInvariantError("Capture repair identity differs from its durable key")
        return result

    async def has_open_call(self, event_id: str, token_id: str) -> bool:
        rows = await self._store.list(
            "ana.calls", filters={"event_id": event_id, "token_id": token_id, "status": "OPEN"}, limit=1
        )
        return bool(rows)

    async def watch_expiries(self, event_id: str, token_id: str) -> Mapping[str, datetime]:
        result = {}
        async for record in self._store.iter_all(
            "ana.watch_conditions",
            filters={"event_id": event_id, "token_id": token_id, "status": "ARMED"},
            batch_size=1000,
        ):
            expiry = WatchExpiry.model_validate(_projection(record, ("cond_id", "expires_at")))
            result[expiry.cond_id] = parse_time(expiry.expires_at)
        return result

    async def tier_history(self, token_id: str) -> list[CaptureTierTransition]:
        return [
            self._tiers.decode(record)
            async for record in self._store.iter_all(
                "ops.capture_tier_transitions", filters={"token_id": token_id}, batch_size=1000
            )
        ]

    async def append_tier(self, transition: CaptureTierTransition) -> None:
        document = _redacted(self._tiers.encode(transition), self._secrets())
        stored = await self._store.put(
            "ops.capture_tier_transitions", transition.transition_id, document, immutable=True
        )
        self._tiers.decode(stored)

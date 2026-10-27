"""Commit source report intent with acquisition, then acknowledge real notice witnesses."""

import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import Protocol, Self

from pydantic import ConfigDict, JsonValue, RootModel

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_reports import (
    LegacySourceReports,
    SourceReportDisposition,
    SourceReportIntent,
    SourceReportLedger,
    SourceReportMode,
    SourceReportNoticeWitness,
    SourceReportNotRequested,
    SourceReportOutcome,
    SourceReportRequest,
    source_report_marker_reference,
    source_report_notice_witness,
)
from tank.models import Record
from tank.time_precision import Timestamp
from tank.util import new_id, sha256

log = logging.getLogger(__name__)
REPORTING_FIELD = "entry_error_reporting"


class ReportStore(Protocol):
    async def get(self, collection: str, key: str) -> Record | None: ...
    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> Record: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...


class ReportSink(Protocol):
    async def __call__(self, intent: SourceReportIntent) -> SourceReportOutcome: ...


class ReportOutcomeDocument(RootModel[SourceReportOutcome]):
    model_config = ConfigDict(strict=True, revalidate_instances="always", hide_input_in_errors=True)


def _legacy(state: Mapping[str, JsonValue]) -> LegacySourceReports | None:
    if "entry_error_reports" not in state or state["entry_error_reports"] == []:
        return None
    value = state["entry_error_reports"]
    well_formed = isinstance(value, list) and all(
        isinstance(row, dict)
        and isinstance(row.get("seen_at"), str)
        and isinstance(row.get("entry_errors"), list)
        for row in value
    )
    return LegacySourceReports(
        field="entry_error_reports",
        field_sha256=sha256(value),
        reason="UNSCOPED_LEGACY_REPORTS" if well_formed else "MALFORMED_LEGACY_REPORTS",
    )


def _read(state: Record, source_id: str) -> tuple[SourceReportLedger, Record]:
    if "source_id" in state and state["source_id"] != source_id:
        raise ValueError("Source reporting checkpoint belongs to another source")
    if REPORTING_FIELD not in state:
        ledger = SourceReportLedger(
            contract_version=1,
            source_id=source_id,
            pending={},
            last_outcome=None,
            unresolved_legacy=_legacy(state),
        )
        return ledger, ledger.model_dump(mode="json", exclude_unset=True)
    raw = state[REPORTING_FIELD]
    if not isinstance(raw, dict):
        raise ValueError("Stored source reporting namespace must be an object")
    ledger = SourceReportLedger.model_validate(raw)
    if ledger.source_id != source_id:
        raise ValueError("Source reporting ledger belongs to another source")
    wire = dict(raw)
    legacy = _legacy(state)
    wire["unresolved_legacy"] = None if legacy is None else legacy.model_dump(mode="json")
    return SourceReportLedger.model_validate(wire), wire


def _pending(wire: Record) -> Record:
    value = wire["pending"]
    if not isinstance(value, dict):
        raise ValueError("Stored source report intent index must be an object")
    return dict(value)


class SourceReportSpool:
    def __init__(
        self, store: ReportStore, versions: ResolvedVersionIdentity, clock: Callable[[], Timestamp]
    ) -> None:
        self.store, self.versions, self.clock = store, versions, clock

    def reconcile(self, state: Record, source_id: str) -> None:
        _, wire = _read(state, source_id)
        state[REPORTING_FIELD] = wire

    def stage(self, state: Record, request: SourceReportRequest, mode: SourceReportMode) -> None:
        """The acquisition caller owns its checkpoint lock and transaction."""
        request = SourceReportRequest.model_validate(request)
        _, wire = _read(state, request.subject.source_id)
        if mode == "ENABLED":
            intent = SourceReportIntent(
                subject=request.subject,
                seen_at=request.seen_at,
                errors=request.errors,
                intent_id=new_id("source_report"),
                versions=self.versions,
                created_at=self.clock(),
            )
            pending = _pending(wire)
            pending[intent.intent_id] = intent.model_dump(mode="json", exclude_unset=True)
            wire["pending"] = pending
        else:
            disposition = SourceReportDisposition(
                intent_id=None,
                intent_revision=None,
                outcome=SourceReportNotRequested(
                    kind="NOT_REQUESTED", scope=request.subject.scope, reason=mode
                ),
                recorded_at=self.clock(),
                source_versions=self.versions,
                execution_versions=self.versions,
            )
            wire["last_outcome"] = disposition.model_dump(mode="json", exclude_unset=True)
        SourceReportLedger.model_validate(wire)
        state[REPORTING_FIELD] = wire

    async def drain(self, source_id: str, key: str, sink: ReportSink) -> None:
        async with (
            self.store.lock("source-checkpoint:" + key),
            self.store.lock("record:ops.source_checkpoints:" + key),
            self.store.transaction(),
        ):
            state = await self.store.get("ops.source_checkpoints", key)
            if state is None:
                return
            ledger, wire = _read(state, source_id)
            if state.get(REPORTING_FIELD) != wire:
                await self.store.put("ops.source_checkpoints", key, state | {REPORTING_FIELD: wire})
            records = _pending(wire)
        for identity, intent in ledger.pending.items():
            revision = sha256(records[identity])
            try:
                outcome = ReportOutcomeDocument.model_validate(await sink(intent)).root
                if outcome.scope != intent.subject.scope:
                    raise ValueError("Source report outcome belongs to another scope")
                if (
                    outcome.kind != "NOT_REQUESTED"
                    and sorted(marker.error_key for marker in outcome.markers) != intent.error_keys
                ):
                    raise ValueError("Source report outcome does not cover its actual requested errors")
                await self._acknowledge(key, intent, revision, outcome)
            except Exception as error:
                log.error("Source report intent %s remains pending after %s", identity, type(error).__name__)

    async def _acknowledge(
        self, key: str, intent: SourceReportIntent, revision: str, outcome: SourceReportOutcome
    ) -> None:
        async with AsyncExitStack() as locks:
            if outcome.kind != "NOT_REQUESTED":
                documents = {marker.marker_id for marker in outcome.markers} | {
                    marker.notice_id for marker in outcome.markers
                }
                for identity in sorted(documents):
                    await locks.enter_async_context(self.store.lock("record:out.ops_notices:" + identity))
            await locks.enter_async_context(self.store.lock("source-checkpoint:" + key))
            await locks.enter_async_context(self.store.lock("record:ops.source_checkpoints:" + key))
            async with self.store.transaction():
                state = await self.store.get("ops.source_checkpoints", key)
                if state is None:
                    raise ValueError("Source report checkpoint is absent during acknowledgement")
                _, wire = _read(state, intent.subject.source_id)
                pending = _pending(wire)
                if intent.intent_id not in pending:
                    return
                if sha256(pending[intent.intent_id]) != revision:
                    raise ValueError("Source report intent changed before acknowledgement")
                if outcome.kind != "NOT_REQUESTED":
                    notices: dict[str, SourceReportNoticeWitness] = {}
                    for marker in outcome.markers:
                        raw = await self.store.get("out.ops_notices", marker.marker_id)
                        if raw is None:
                            raise ValueError("Source report marker disappeared before acknowledgement")
                        if marker.notice_id not in notices:
                            notice = await self.store.get("out.ops_notices", marker.notice_id)
                            if notice is None:
                                raise ValueError("Source report notice disappeared before acknowledgement")
                            notices[marker.notice_id] = source_report_notice_witness(outcome.scope, notice)
                        actual = source_report_marker_reference(
                            outcome.scope, marker.error_key, raw, notices[marker.notice_id]
                        )
                        if actual != marker:
                            raise ValueError("Source report witness changed before acknowledgement")
                    del pending[intent.intent_id]
                wire["pending"] = pending
                disposition = SourceReportDisposition(
                    intent_id=intent.intent_id,
                    intent_revision=revision,
                    outcome=outcome,
                    recorded_at=self.clock(),
                    source_versions=intent.versions,
                    execution_versions=self.versions,
                )
                wire["last_outcome"] = disposition.model_dump(mode="json", exclude_unset=True)
                SourceReportLedger.model_validate(wire)
                await self.store.put("ops.source_checkpoints", key, state | {REPORTING_FIELD: wire})

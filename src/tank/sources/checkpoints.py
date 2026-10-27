"""Decode pending storage evidence into operation-owned batches without dropping corrupt records."""

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Literal, Protocol, Self

from pydantic import ConfigDict, Field, JsonValue, RootModel, ValidationError, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import DocumentStore
from tank.contracts.source_checkpoints import (
    SOURCE_QUARANTINE_ARCHIVES,
    CompletedPendingRow,
    PendingBatchMetadata,
    PendingBatchQuarantine,
    PendingBatchRecord,
    PendingCoverage,
    PendingQuarantineIssue,
    QuarantinedSourceBatch,
    QuarantineOrigin,
    SourcePendingBatch,
    SourceQuarantineArchive,
    observed_external_id,
    quarantine_identity,
)
from tank.contracts.source_checkpoints import (
    observed_identifier as declaration,
)
from tank.contracts.source_errors import SourceEntryError
from tank.contracts.source_items import PendingSourceItem, SourceContextId, pending_source_item
from tank.contracts.sources import SourceDefinition
from tank.repository import Repository
from tank.util import iso, parse_iso_time, sha256


class PendingBatchWire(PendingBatchMetadata):
    source_id: str | None = None
    rows: list[JsonValue]
    coverage: PendingCoverage
    quarantines: list[PendingBatchQuarantine] = Field(default_factory=list)


class PendingArrayWire(RootModel[list[JsonValue]]):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True)


class PendingCheckpointState(DomainModel):
    source_id: SourceContextId
    feed_key: str = Field(min_length=1)
    batches: list[PendingBatchRecord]
    changed: bool
    versions: ResolvedVersionIdentity

    @model_validator(mode="after")
    def actual_operation_context(self) -> Self:
        if any(
            isinstance(batch, SourcePendingBatch) and batch.source_id != self.source_id
            for batch in self.batches
        ) or any(
            quarantine.origin.operation_source_id != self.source_id
            or quarantine.origin.feed_key != self.feed_key
            for batch in self.batches
            for quarantine in batch.quarantines
        ):
            raise ValueError("Pending checkpoint conflicts with its actual operation context")
        return self

    @property
    def unresolved_quarantines(self) -> int:
        return sum(quarantine.unresolved for batch in self.batches for quarantine in batch.quarantines)

    def storage_records(self) -> list[JsonValue]:
        return [batch.storage_record() for batch in self.batches]

    def pending_external_ids(self) -> set[str]:
        return {
            row.discovery.external_id
            for batch in self.batches
            if isinstance(batch, SourcePendingBatch)
            for row in batch.rows
        }

    def complete_rows(self, completed: list[CompletedPendingRow]) -> "PendingCheckpointState":
        fingerprints = {completion.fingerprint for completion in completed}
        batches: list[PendingBatchRecord] = []
        for batch in self.batches:
            if isinstance(batch, QuarantinedSourceBatch):
                batches.append(batch)
                continue
            batches.append(
                SourcePendingBatch.model_validate(
                    batch.model_copy(
                        update={
                            "rows": [
                                row for row in batch.rows if sha256(row.storage_record()) not in fingerprints
                            ]
                        }
                    )
                )
            )
        return type(self).model_validate(
            self.model_copy(update={"batches": batches, "changed": self.changed or batches != self.batches})
        )

    def link_completions(
        self, completed: list[CompletedPendingRow], *, now: datetime
    ) -> "PendingCheckpointState":
        observations = [
            (index, batch.batch_id, completion.observation)
            for index, batch in enumerate(self.batches)
            if isinstance(batch, SourcePendingBatch)
            for completion in completed
            if any(sha256(row.storage_record()) == completion.fingerprint for row in batch.rows)
        ]
        batches: list[PendingBatchRecord] = []
        for index, batch in enumerate(self.batches):
            quarantines = list(batch.quarantines)
            for acquired_index, acquired_batch_id, observation in observations:
                if acquired_index <= index:
                    continue
                quarantines = [
                    quarantine.link_new_capture(
                        observation.captured,
                        observation.stored,
                        linked_at=now,
                        versions=self.versions,
                        acquisition_batch_id=acquired_batch_id,
                    )
                    for quarantine in quarantines
                ]
            batches.append(type(batch).model_validate(batch.model_copy(update={"quarantines": quarantines})))
        return type(self).model_validate(
            self.model_copy(update={"batches": batches, "changed": self.changed or batches != self.batches})
        )

    def with_archives(self, archives: Mapping[str, PendingBatchQuarantine]) -> "PendingCheckpointState":
        batches = [
            type(batch).model_validate(
                batch.model_copy(
                    update={
                        "quarantines": [
                            archives.get(quarantine.evidence_id, quarantine)
                            for quarantine in batch.quarantines
                        ]
                    }
                )
            )
            for batch in self.batches
        ]
        return type(self).model_validate(
            self.model_copy(update={"batches": batches, "changed": self.changed or batches != self.batches})
        )

    def coverage_commit(self) -> "PendingCoverageCommit":
        completed: list[SourcePendingBatch] = []
        for batch in self.batches:
            if not isinstance(batch, SourcePendingBatch) or not batch.may_commit_coverage:
                break
            completed.append(batch)
        return PendingCoverageCommit(
            before=self, completed=completed, remaining=self.batches[len(completed) :]
        )


class PendingCoverageCommit(DomainModel):
    before: PendingCheckpointState
    completed: list[SourcePendingBatch]
    remaining: list[PendingBatchRecord]

    @model_validator(mode="after")
    def actual_completed_prefix(self) -> Self:
        expected: list[SourcePendingBatch] = []
        for batch in self.before.batches:
            if not isinstance(batch, SourcePendingBatch) or not batch.may_commit_coverage:
                break
            expected.append(batch)
        if self.completed != expected or self.remaining != self.before.batches[len(expected) :]:
            raise ValueError("Coverage commit cannot cross an unresolved pending entry")
        return self


def quarantine_batch(
    *,
    source: SourceDefinition,
    key: str,
    checkpoint: Mapping[str, JsonValue],
    raw: JsonValue,
    position: int,
    issues: list[PendingQuarantineIssue],
    now: datetime,
    versions: ResolvedVersionIdentity,
) -> PendingBatchQuarantine:
    checkpoint_owner, checkpoint_declaration = declaration(checkpoint, "source_id")
    batch_wire = raw if isinstance(raw, dict) else {}
    batch_owner, batch_source_declaration = declaration(batch_wire, "source_id")
    batch_id, batch_declaration = declaration(batch_wire, "batch_id")
    digest = sha256(raw)
    origin = QuarantineOrigin(
        operation_source_id=source.source_id,
        feed_key=key,
        batch_position=position,
        declared_checkpoint_source_id=checkpoint_owner,
        checkpoint_source_declaration=checkpoint_declaration,
        declared_batch_source_id=batch_owner,
        batch_source_declaration=batch_source_declaration,
        declared_batch_id=batch_id,
        batch_declaration=batch_declaration,
    )
    return PendingBatchQuarantine(
        evidence_id=quarantine_identity(origin, digest, versions),
        origin=origin,
        raw_batch=raw,
        detected_versions=versions,
        raw_sha256=digest,
        quarantined_at=iso(now),
        quarantined_at_prec="COLLECTION_ONLY",
        quarantined_at_prec_reason=None,
        issues=issues,
    )


def validate_quarantine_context(
    source: SourceDefinition, key: str, quarantines: list[PendingBatchQuarantine]
) -> None:
    if any(
        quarantine.origin.operation_source_id != source.source_id or quarantine.origin.feed_key != key
        for quarantine in quarantines
    ):
        raise ValueError("Stored quarantine conflicts with the actual source operation")


def decode_pending_batch(
    *,
    source: SourceDefinition,
    key: str,
    checkpoint: Mapping[str, JsonValue],
    raw: JsonValue,
    position: int,
    now: datetime,
    versions: ResolvedVersionIdentity,
) -> PendingBatchRecord:
    try:
        if isinstance(raw, dict) and set(raw) == {"quarantines"}:
            preserved = QuarantinedSourceBatch.model_validate(raw)
            validate_quarantine_context(source, key, preserved.quarantines)
            return preserved
        wire = PendingBatchWire.model_validate(raw)
        if isinstance(raw, dict) and "source_id" in raw and wire.source_id != source.source_id:
            raise ValueError("Pending batch conflicts with the actual source operation")
        validate_quarantine_context(source, key, wire.quarantines)
    except (ValidationError, ValueError) as failure:
        missing = isinstance(raw, dict) and any(
            name not in raw for name in ("batch_id", "acquired_at", "acquired_at_precision", "kind")
        )
        evidence = quarantine_batch(
            source=source,
            key=key,
            checkpoint=checkpoint,
            raw=raw,
            position=position,
            issues=[
                PendingQuarantineIssue(
                    row_position=None,
                    reason="MISSING_BATCH_METADATA" if missing else "INVALID_BATCH_METADATA",
                    validation_errors=failure.error_count() if isinstance(failure, ValidationError) else 1,
                    declared_source_id=None,
                    source_declaration="NOT_APPLICABLE",
                    declared_item_id=None,
                    item_declaration="NOT_APPLICABLE",
                    declared_external_id=None,
                    external_declaration="NOT_APPLICABLE",
                )
            ],
            now=now,
            versions=versions,
        )
        return QuarantinedSourceBatch(quarantines=[evidence])

    rows: list[PendingSourceItem] = []
    issues: list[PendingQuarantineIssue] = []
    for index, row in enumerate(wire.rows):
        try:
            if not isinstance(row, dict):
                raise ValueError("Pending row must be a captured storage record")
            rows.append(pending_source_item(source.source_id, row))
        except (ValidationError, ValueError, KeyError) as failure:
            issues.append(
                PendingQuarantineIssue(
                    row_position=index,
                    reason="MISSING_PREASSIGNED_ITEM_ID"
                    if isinstance(row, dict) and "item_id" not in row
                    else "INVALID_PENDING_ROW",
                    validation_errors=failure.error_count() if isinstance(failure, ValidationError) else 1,
                    declared_source_id=declaration(row, "source_id")[0] if isinstance(row, dict) else None,
                    source_declaration=declaration(row, "source_id")[1]
                    if isinstance(row, dict)
                    else "ABSENT",
                    declared_item_id=declaration(row, "item_id")[0] if isinstance(row, dict) else None,
                    item_declaration=declaration(row, "item_id")[1] if isinstance(row, dict) else "ABSENT",
                    declared_external_id=observed_external_id(row)[0],
                    external_declaration=observed_external_id(row)[1],
                )
            )
    quarantines = list(wire.quarantines)
    if issues:
        quarantines.append(
            quarantine_batch(
                source=source,
                key=key,
                checkpoint=checkpoint,
                raw=raw,
                position=position,
                issues=issues,
                now=now,
                versions=versions,
            )
        )
    return SourcePendingBatch(
        source_id=source.source_id,
        batch_id=wire.batch_id,
        acquired_at=wire.acquired_at,
        acquired_at_precision=wire.acquired_at_precision,
        acquired_at_prec=wire.acquired_at_prec,
        acquired_at_prec_reason=wire.acquired_at_prec_reason,
        kind=wire.kind,
        rows=rows,
        coverage=wire.coverage,
        quarantines=quarantines,
    )


def decode_pending_checkpoint(
    source: SourceDefinition,
    key: str,
    checkpoint: Mapping[str, JsonValue],
    *,
    now: datetime,
    versions: ResolvedVersionIdentity,
) -> PendingCheckpointState:
    now = parse_iso_time(now)
    owner, owner_declaration = declaration(checkpoint, "source_id")
    if owner_declaration == "INVALID" or (owner is not None and owner != source.source_id):
        raise ValueError("Checkpoint owner conflicts with the actual source operation")
    raw = checkpoint.get("pending_batches", [])
    try:
        records = PendingArrayWire.model_validate(raw).root
    except ValidationError as failure:
        quarantine = quarantine_batch(
            source=source,
            key=key,
            checkpoint=checkpoint,
            raw=raw,
            position=0,
            issues=[
                PendingQuarantineIssue(
                    row_position=None,
                    reason="INVALID_BATCH_METADATA",
                    validation_errors=failure.error_count(),
                    declared_source_id=None,
                    source_declaration="NOT_APPLICABLE",
                    declared_item_id=None,
                    item_declaration="NOT_APPLICABLE",
                    declared_external_id=None,
                    external_declaration="NOT_APPLICABLE",
                )
            ],
            now=now,
            versions=versions,
        )
        batches: list[PendingBatchRecord] = [QuarantinedSourceBatch(quarantines=[quarantine])]
    else:
        batches = [
            decode_pending_batch(
                source=source,
                key=key,
                checkpoint=checkpoint,
                raw=value,
                position=index,
                now=now,
                versions=versions,
            )
            for index, value in enumerate(records)
        ]
    return PendingCheckpointState(
        source_id=source.source_id,
        feed_key=key,
        versions=versions,
        batches=batches,
        changed=[batch.storage_record() for batch in batches] != raw,
    )


class CheckpointStore(DocumentStore, Protocol):
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...


def merge_quarantine(
    existing: PendingBatchQuarantine, incoming: PendingBatchQuarantine
) -> PendingBatchQuarantine:
    if (
        existing.evidence_id != incoming.evidence_id
        or existing.origin != incoming.origin
        or existing.raw_batch != incoming.raw_batch
        or existing.raw_sha256 != incoming.raw_sha256
        or existing.detected_versions != incoming.detected_versions
        or len(existing.issues) != len(incoming.issues)
    ):
        raise ValueError("Quarantine immutable evidence cannot be replaced")
    issues: list[PendingQuarantineIssue] = []
    for previous, current in zip(existing.issues, incoming.issues, strict=True):
        if previous.model_copy(update={"recovery": None}) != current.model_copy(update={"recovery": None}):
            raise ValueError("Quarantine detected issue facts cannot be replaced")
        if (
            previous.recovery is not None
            and current.recovery is not None
            and previous.recovery != current.recovery
        ):
            raise ValueError("Quarantine recovery linkage cannot be replaced")
        issues.append(previous if previous.recovery is not None else current)
    return PendingBatchQuarantine.model_validate(existing.model_copy(update={"issues": issues}))


class QuarantineArchiveRepository:
    def __init__(self, store: CheckpointStore, versions: ResolvedVersionIdentity) -> None:
        self.store = store
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self._records = Repository(store, SOURCE_QUARANTINE_ARCHIVES)

    @staticmethod
    def key(evidence_id: str) -> str:
        return "quarantine:" + evidence_id

    async def reconcile(self, state: PendingCheckpointState) -> PendingCheckpointState:
        if state.versions != self.versions:
            raise ValueError("Pending operation version identity conflicts with its checkpoint writer")
        actual: dict[str, PendingBatchQuarantine] = {}
        for batch in state.batches:
            for quarantine in batch.quarantines:
                existing = await self._records.get(self.key(quarantine.evidence_id))
                actual[quarantine.evidence_id] = (
                    quarantine if existing is None else merge_quarantine(existing.evidence, quarantine)
                )
        return state.with_archives(actual)

    async def persist(self, state: PendingCheckpointState, *, now: datetime) -> PendingCheckpointState:
        now = parse_iso_time(now)
        if state.versions != self.versions:
            raise ValueError("Pending operation version identity conflicts with its checkpoint writer")
        actual: dict[str, PendingBatchQuarantine] = {}
        async with self.store.lock("source-checkpoint:" + state.feed_key), self.store.transaction():
            for batch in state.batches:
                for quarantine in batch.quarantines:
                    key = self.key(quarantine.evidence_id)
                    existing = await self._records.get(key)
                    canonical = (
                        quarantine if existing is None else merge_quarantine(existing.evidence, quarantine)
                    )
                    if existing is None or existing.evidence != canonical:
                        await self._records.put(
                            key,
                            SourceQuarantineArchive(
                                checkpoint_kind="PENDING_QUARANTINE",
                                source_id=state.source_id,
                                writer_versions=self.versions,
                                evidence=canonical,
                                updated_at=iso(now),
                                updated_at_prec="COLLECTION_ONLY",
                                updated_at_prec_reason=None,
                            ),
                        )
                    actual[quarantine.evidence_id] = canonical
        return state.with_archives(actual)

    async def archives_for_source(self, source: SourceDefinition) -> list[SourceQuarantineArchive]:
        return [
            record
            async for record in self._records.iter_all(
                filters={"source_id": source.source_id, "checkpoint_kind": "PENDING_QUARANTINE"}
            )
        ]


class SourceCadence(DomainModel):
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    detail_retry_at: str | None = None

    @model_validator(mode="after")
    def recorded_clocks(self) -> Self:
        for value in (self.last_attempt_at, self.last_success_at, self.last_failure_at, self.detail_retry_at):
            if value is not None:
                parse_iso_time(value)
        return self

    @classmethod
    def from_document(cls, value: Mapping[str, JsonValue]) -> "SourceCadence":
        return cls.model_validate({name: value[name] for name in cls.model_fields if name in value})

    def poll_remaining(self, *, now: datetime, interval_s: float) -> float:
        anchor = self.last_attempt_at if self.last_attempt_at is not None else self.last_success_at
        return (
            0.0
            if anchor is None
            else max(0.0, interval_s - (parse_iso_time(now) - parse_iso_time(anchor)).total_seconds())
        )

    def detail_remaining(self, *, now: datetime, interval_s: float) -> float:
        if self.detail_retry_at is None:
            return 0.0
        if self.last_failure_at is None:
            return max(0.0, (parse_iso_time(self.detail_retry_at) - parse_iso_time(now)).total_seconds())
        return max(
            0.0, interval_s - (parse_iso_time(now) - parse_iso_time(self.last_failure_at)).total_seconds()
        )


class ArticleFailure(DomainModel):
    url: str
    http_status: int = Field(ge=100, le=599)
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    observed_at: str
    observed_at_precision: Literal["COLLECTION_ONLY"]
    observed_at_prec: Literal["COLLECTION_ONLY"] | None = None
    observed_at_prec_reason: None = None
    reason: Literal["EMPTY_ARTICLE_BODY", "PUBLISHER_ARTICLE_NOT_FOUND", "PUBLISHER_HTTP_FAILURE"]

    @model_validator(mode="after")
    def actual_clock(self) -> Self:
        parse_iso_time(self.observed_at)
        return self


class UnverifiableArticleMarker(DomainModel):
    reason: Literal["METADATA_UNVERIFIABLE"]


class UnspecifiedArticleMarker(DomainModel):
    pass


type RecordedArticleFailure = ArticleFailure | UnverifiableArticleMarker | UnspecifiedArticleMarker


class HistoryBoundaryGap(DomainModel):
    reason: Literal["KNOWN_BOUNDARY_NOT_IN_AVAILABLE_HISTORY"]
    checkpoint: str = Field(min_length=1)
    observed_at: str
    observed_at_precision: Literal["COLLECTION_ONLY"]
    observed_at_prec: Literal["COLLECTION_ONLY"] | None = None
    observed_at_prec_reason: None = None

    @model_validator(mode="after")
    def actual_clock(self) -> Self:
        parse_iso_time(self.observed_at)
        return self


class SourceDegradation(DomainModel):
    status: Literal["HEALTHY", "DEGRADED", "PENDING"] | None = None
    article_errors: dict[str, RecordedArticleFailure] = Field(default_factory=dict)
    entry_errors: list[SourceEntryError] = Field(default_factory=list)
    history_gap: HistoryBoundaryGap | list[str] | None = None
    acquisition_error: str | None = None
    scan_status: str | None = None

    @classmethod
    def from_document(cls, value: Mapping[str, JsonValue]) -> "SourceDegradation":
        return cls.model_validate({name: value[name] for name in cls.model_fields if name in value})

    @property
    def blocked(self) -> bool:
        return bool(
            self.article_errors
            or self.entry_errors
            or self.history_gap
            or self.acquisition_error is not None
            or self.scan_status == "GAP_LIMIT"
        )


class SourceDuplicateCache(DomainModel):
    duplicate_ids: list[str] = Field(default_factory=list)

    @classmethod
    def from_document(cls, value: Mapping[str, JsonValue]) -> "SourceDuplicateCache":
        return cls.model_validate({name: value[name] for name in cls.model_fields if name in value})


class CompletedSourceRow(DomainModel):
    completion: CompletedPendingRow


class FailedSourceRow(DomainModel):
    original: PendingSourceItem
    error: str
    article_failure: ArticleFailure | None


type SourceRowResult = CompletedSourceRow | FailedSourceRow


class PendingCoverageWire(RootModel[PendingCoverage]):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True)


def pending_coverage(value: Mapping[str, JsonValue]) -> PendingCoverage:
    return PendingCoverageWire.model_validate(value).root

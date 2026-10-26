"""Evidence-gated eligibility for a retained lost coalescing obligation."""

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Literal, Protocol, Self

from pydantic import JsonValue, ValidationError

from tank.contracts import DomainModel
from tank.contracts.jobs import DependencyState
from tank.contracts.reaction_recovery import (
    ReactionRecoveryAlreadyApplied,
    ReactionRecoveryCommand,
    ReactionRecoveryEligibility,
    ReactionRecoveryEligible,
    ReactionRecoveryEvidence,
    ReactionRecoveryExecution,
    ReactionRecoveryExistingWork,
    ReactionRecoveryLink,
    ReactionRecoveryLinkState,
    ReactionRecoveryOutcome,
    ReactionRecoveryQueued,
    ReactionRecoveryUnresolved,
    ReactionRecoveryWitnessRequest,
    RecoveryBoundCheckpoint,
    RecoveryFailedPeer,
    RecoveryLinkedQueueWitness,
    RecoveryMarkerAbsent,
    RecoveryMarkerApplied,
    RecoveryMarkerEvidence,
    RecoveryMarkerUnresolved,
    RecoveryOriginalJob,
    RecoveryOriginalQueueWitness,
    RecoveryPeerQueueWitness,
    RecoveryQueuedWitness,
    RecoveryRetryBudget,
    RecoveryStorageKey,
    RecoveryTerminalBackfill,
    RecoveryWitnessCollection,
)
from tank.contracts.scheduler_commands import (
    ReactionAnchorFacts,
    ReactionRevisionInventory,
    ReactionTarget,
    RevisedReactionCommand,
)
from tank.contracts.scheduler_results import (
    ReactionChangeTarget,
    applied_reaction_change_receipt,
    captured_reaction_receipt,
)
from tank.contracts.stored_documents import ImmutableDocument, NativeBackfillWitness
from tank.models import Record
from tank.reaction_timing import (
    CAPTURE_EXPIRY_S,
    LATE_REVISION_DEFER_S,
    late_reaction_schedule,
)
from tank.runtime_reaction import reaction_job_command
from tank.time_precision import Timestamp, propagate_timestamp, reconcile_precision
from tank.util import parse_time, sha256

type RecoveryWitnessIndex = Mapping[
    tuple[RecoveryWitnessCollection, str], ImmutableDocument | NativeBackfillWitness | None
]


def _unsupported_source_mode(record: Mapping[str, JsonValue]) -> bool:
    if any(key in {"replay", "origin", "historical"} or key.startswith("replay_") for key in record):
        return True
    payload = record.get("payload")
    return isinstance(payload, dict) and any(
        key in {"replay", "origin", "historical"} or key.startswith("replay_") for key in payload
    )


def recovery_idempotency_key(original_job_id: str, payload_revision: str, peer_job_id: str) -> str:
    """Identify one bounded recovery budget without borrowing an operational clock."""
    return "reaction-recovery:" + sha256(
        {
            "policy_version": 1,
            "original_job_id": original_job_id,
            "payload_revision": payload_revision,
            "peer_job_id": peer_job_id,
        }
    )


def recovery_link_key(original_job_id: str) -> str:
    """An original job owns one recovery generation even if a later witness changes."""
    return "reaction-recovery-original:" + sha256({"original_job_id": original_job_id})


def recovery_policy_revision() -> str:
    return sha256(
        {
            "policy": "FAILED_LEGACY_COALESCING",
            "version": 1,
            "late_revision_defer_s": LATE_REVISION_DEFER_S,
            "capture_expiry_s": CAPTURE_EXPIRY_S,
        }
    )


def _recovery_marker(
    command: RevisedReactionCommand,
    marker: ImmutableDocument | NativeBackfillWitness | None,
    owner: ImmutableDocument | NativeBackfillWitness | None,
) -> RecoveryMarkerEvidence:
    if marker is None and owner is None:
        return RecoveryMarkerAbsent(kind="ABSENT")
    if not isinstance(marker, ImmutableDocument) or not isinstance(owner, ImmutableDocument):
        return RecoveryMarkerUnresolved(kind="UNRESOLVED")
    if not marker.immutable or not owner.immutable:
        return RecoveryMarkerUnresolved(kind="UNRESOLVED")
    try:
        target = ReactionChangeTarget(
            change_id=command.change_id,
            event_id=command.event_id,
            token_id=command.token_id,
            checkpoint=command.checkpoint,
        )
        receipt = applied_reaction_change_receipt(target, marker.document, owner.document)
    except ValueError:
        return RecoveryMarkerUnresolved(kind="UNRESOLVED")
    return RecoveryMarkerApplied(kind="APPLIED", receipt=receipt)


def admit_recovery_witnesses(
    request: ReactionRecoveryWitnessRequest, rows: RecoveryWitnessIndex
) -> ReactionRecoveryEvidence | ReactionRecoveryAlreadyApplied | ReactionRecoveryUnresolved:
    """Admit only the named locked rows; callers retain their witness scope through commit."""
    request = ReactionRecoveryWitnessRequest.model_validate(request)
    expected = {(address.collection, address.storage_key) for address in request.addresses()}
    if set(rows) != expected:
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="WITNESS_UNADMITTED")
    try:
        original_row = rows[("ops.jobs", request.original_job_id)]
        if not isinstance(original_row, ImmutableDocument):
            raise ValueError("Recovery original job witness is unavailable")
        if _unsupported_source_mode(original_row.document):
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_MODE_UNSUPPORTED")
        original_job = RecoveryOriginalQueueWitness.model_validate(
            {
                field: original_row.document[field]
                for field in RecoveryOriginalQueueWitness.model_fields
                if field in original_row.document
            }
        )
        if original_job.job_id != request.original_job_id or original_job.type != "REACTION_CAPTURE":
            raise ValueError("Recovery original job witness has another identity")
        command = reaction_job_command(original_job.payload)
        if not isinstance(command, RevisedReactionCommand):
            raise ValueError("Recovery original command is not an automatic revision")
        original = RecoveryOriginalJob.model_validate(
            {
                "job_id": original_job.job_id,
                "status": original_job.status,
                "command": command,
                "payload_revision": sha256(original_job.payload),
                "result": original_job.result,
            }
        )
        identity = recovery_idempotency_key(original.job_id, original.payload_revision, request.peer_job_id)
        if (
            original.result.root.coalesced_into != request.peer_job_id
            or request.enqueue_idempotency_key != identity
            or request.recovery_link_key != recovery_link_key(original.job_id)
            or request.change_marker_key != "reaction-change:" + command.change_id
            or request.change_owner_key != "reaction-change-owner:" + command.change_id
        ):
            raise ValueError("Recovery witness request conflicts with the actual original job")
        marker = _recovery_marker(
            command,
            rows[("ops.scheduler_checkpoints", request.change_marker_key)],
            rows[("ops.scheduler_checkpoints", request.change_owner_key)],
        )
        if isinstance(marker, RecoveryMarkerApplied):
            return ReactionRecoveryAlreadyApplied(kind="ALREADY_APPLIED", receipt=marker.receipt)
        if isinstance(marker, RecoveryMarkerUnresolved):
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="MARKER_UNRESOLVED")
        if rows[("ops.scheduler_checkpoints", request.recovery_link_key)] is not None:
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="RECOVERY_LINK_UNADMITTED")
        peer_row = rows[("ops.jobs", request.peer_job_id)]
        peer = None
        if peer_row is not None:
            if not isinstance(peer_row, ImmutableDocument):
                raise ValueError("Recovery peer witness is not a document")
            if _unsupported_source_mode(peer_row.document):
                return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_MODE_UNSUPPORTED")
            peer_job = RecoveryPeerQueueWitness.model_validate(
                {
                    field: peer_row.document[field]
                    for field in RecoveryPeerQueueWitness.model_fields
                    if field in peer_row.document
                }
            )
            if peer_job.job_id != request.peer_job_id or peer_job.type != "REACTION_CAPTURE":
                raise ValueError("Recovery peer witness has another identity")
            peer = RecoveryFailedPeer.model_validate(
                {
                    "job_id": peer_job.job_id,
                    "status": peer_job.status,
                    "command": reaction_job_command(peer_job.payload),
                }
            )
        source_row = rows[("mkt.backfill_runs", request.source_backfill_id)]
        source = None
        if source_row is not None:
            if not isinstance(source_row, NativeBackfillWitness):
                raise ValueError("Recovery source is not a native backfill witness")
            if _unsupported_source_mode(source_row.record):
                return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_MODE_UNSUPPORTED")
            source = recovery_backfill_from_record(source_row.record)
            if source.backfill_id != request.source_backfill_id:
                raise ValueError("Recovery source witness has another identity")
        checkpoint_row = rows[("out.reaction_checkpoints", request.checkpoint_key)]
        checkpoint = None
        if checkpoint_row is not None:
            if not isinstance(checkpoint_row, ImmutableDocument):
                raise ValueError("Recovery checkpoint witness is not a document")
            if _unsupported_source_mode(checkpoint_row.document):
                return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_MODE_UNSUPPORTED")
            if checkpoint_row.immutable is not True:
                raise ValueError("Recovery checkpoint witness is not immutable")
            checkpoint = RecoveryBoundCheckpoint(
                receipt=captured_reaction_receipt(request.checkpoint_key, checkpoint_row.document),
                immutable=checkpoint_row.immutable,
                anchor=ReactionAnchorFacts.model_validate(checkpoint_row.document["anchor"]),
                captured_at=propagate_timestamp(checkpoint_row.document, "captured_at"),
            )
        return ReactionRecoveryEvidence(
            original=original,
            peer=peer,
            source=source,
            checkpoint=checkpoint,
            marker=marker,
        )
    except (ValidationError, ValueError, KeyError):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="WITNESS_UNADMITTED")


class RecoveryBackfillProjection(DomainModel):
    """Only durable source identity and clocks consumed by recovery admission."""

    backfill_id: RecoveryStorageKey
    event_id: RecoveryStorageKey
    token_id: RecoveryStorageKey
    status: Literal["COMPLETE", "PARTIAL", "FAILED"]
    anchor: str | None
    anchor_prec: str | None = None
    anchor_prec_reason: str | None = None
    anchor_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    finished_at: RecoveryStorageKey
    finished_at_prec: str | None = None
    finished_at_prec_reason: str | None = None


def recovery_backfill_from_record(record: Mapping[str, JsonValue]) -> RecoveryTerminalBackfill:
    """Hash the actual source fields before normalizing their clock projection."""
    wire = RecoveryBackfillProjection.model_validate(
        {field: record[field] for field in RecoveryBackfillProjection.model_fields if field in record}
    )
    change_id = "CAP-" + sha256(
        {"backfill_id": wire.backfill_id, "status": wire.status, "finished_at": wire.finished_at}
    )
    clocks = wire.model_dump(mode="json", exclude_unset=True)
    return RecoveryTerminalBackfill(
        backfill_id=wire.backfill_id,
        change_id=change_id,
        event_id=wire.event_id,
        token_id=wire.token_id,
        status=wire.status,
        anchor=propagate_timestamp(clocks, "anchor"),
        finished_at=propagate_timestamp(clocks, "finished_at"),
    )


def recovery_eligibility(evidence: ReactionRecoveryEvidence) -> ReactionRecoveryEligibility:
    """Require source/checkpoint chronology in addition to the actual failed peer."""
    evidence = ReactionRecoveryEvidence.model_validate(evidence)
    original, peer = evidence.original, evidence.peer
    command = original.command
    target = (command.event_id, command.token_id, command.checkpoint, command.change_id)
    if isinstance(evidence.marker, RecoveryMarkerUnresolved):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="MARKER_UNRESOLVED")
    if isinstance(evidence.marker, RecoveryMarkerApplied):
        applied = evidence.marker.receipt
        if (applied.event_id, applied.token_id, applied.checkpoint, applied.change_id) != target:
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="MARKER_SCOPE_MISMATCH")
        return ReactionRecoveryAlreadyApplied(kind="ALREADY_APPLIED", receipt=applied)
    if peer is None:
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="PEER_ABSENT")
    if (
        original.result.root.coalesced_into != peer.job_id
        or original.job_id == peer.job_id
        or (peer.command.event_id, peer.command.token_id, peer.command.checkpoint) != target[:3]
        or peer.command.change_id == command.change_id
    ):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="PEER_SCOPE_MISMATCH")
    source = evidence.source
    if source is None:
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_ABSENT")
    if (
        source.finished_at.value is None
        or source.finished_at.precision != "COLLECTION_ONLY"
        or source.anchor.value is None
        or source.anchor.precision is None
        or source.anchor.precision.startswith("BUCKET(")
        and source.anchor.bucket_anchor not in {"START", "END"}
    ):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_CLOCK_UNREPORTED")
    if (source.event_id, source.token_id) != target[
        :2
    ] or command.change_id != source.change_id + ":" + command.checkpoint:
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_SCOPE_MISMATCH")
    checkpoint = evidence.checkpoint
    if checkpoint is None:
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="CHECKPOINT_ABSENT")
    receipt = checkpoint.receipt
    if (receipt.event_id, receipt.token_id, receipt.checkpoint) != target[:3] or receipt.checkpoint_key != (
        f"{receipt.event_id}:{receipt.token_id}:{receipt.checkpoint}:{receipt.revision}"
    ):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="CHECKPOINT_SCOPE_MISMATCH")
    anchor = checkpoint.anchor.timestamp
    if (
        checkpoint.captured_at.value is None
        or checkpoint.captured_at.precision != "COLLECTION_ONLY"
        or anchor.value is None
        or anchor.precision is None
        or anchor.precision.startswith("BUCKET(")
        and anchor.bucket_anchor not in {"START", "END"}
    ):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="CHECKPOINT_CLOCK_UNREPORTED")
    if (
        parse_time(anchor.value) != parse_time(source.anchor.value)
        or anchor.bucket_anchor != source.anchor.bucket_anchor
    ):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="CHECKPOINT_SCOPE_MISMATCH")
    try:
        reconcile_precision((anchor.precision, source.anchor.precision), field="anchor")
    except ValueError:
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="CHECKPOINT_SCOPE_MISMATCH")
    if parse_time(checkpoint.captured_at.value) >= parse_time(source.finished_at.value):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_MAY_ALREADY_BE_CAPTURED")
    return ReactionRecoveryEligible(
        kind="ELIGIBLE",
        original_job_id=original.job_id,
        peer_job_id=peer.job_id,
        source_backfill_id=source.backfill_id,
        checkpoint_key=receipt.checkpoint_key,
        payload_revision=original.payload_revision,
        command=command,
    )


class ReactionRecoveryStore(Protocol):
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...

    async def get(self, collection: str, key: str) -> Record | None: ...

    def iter_metadata(
        self, collection: str, fields: Sequence[str], *, filters: Record
    ) -> AsyncIterator[Record]: ...

    async def list(self, collection: str, *, filters: Record, limit: int) -> list[Record]: ...

    async def enqueue(
        self,
        kind: str,
        payload: Mapping[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        max_attempts: int,
        run_after: Timestamp,
        expires_at: Timestamp,
    ) -> Record: ...

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonValue], *, immutable: bool
    ) -> Record: ...

    def reaction_recovery_witnesses(
        self, request: ReactionRecoveryWitnessRequest
    ) -> AbstractAsyncContextManager[RecoveryWitnessIndex]: ...


def _original_queue_facts(record: Record, original_job_id: str) -> RecoveryOriginalJob:
    wire = RecoveryOriginalQueueWitness.model_validate(
        {field: record[field] for field in RecoveryOriginalQueueWitness.model_fields if field in record}
    )
    command = reaction_job_command(wire.payload)
    if wire.job_id != original_job_id or not isinstance(command, RevisedReactionCommand):
        raise ValueError("Recovery original witness has a different identity or command")
    return RecoveryOriginalJob(
        job_id=wire.job_id,
        status=wire.status,
        command=command,
        payload_revision=sha256(wire.payload),
        result=wire.result,
    )


def _witness_records(
    request: ReactionRecoveryWitnessRequest, rows: RecoveryWitnessIndex
) -> tuple[Record, Record, Record, Record]:
    original = rows[("ops.jobs", request.original_job_id)]
    peer = rows[("ops.jobs", request.peer_job_id)]
    source = rows[("mkt.backfill_runs", request.source_backfill_id)]
    checkpoint = rows[("out.reaction_checkpoints", request.checkpoint_key)]
    if (
        not isinstance(original, ImmutableDocument)
        or not isinstance(peer, ImmutableDocument)
        or not isinstance(source, NativeBackfillWitness)
        or not isinstance(checkpoint, ImmutableDocument)
        or not checkpoint.immutable
    ):
        raise ValueError("Recovery requires its actual admitted witness records")
    return original.document, peer.document, source.record, checkpoint.document


class ReactionRecovery:
    """Create at most one source-bound recovery generation without changing the old job."""

    def __init__(
        self,
        repo: ReactionRecoveryStore,
        *,
        execution: ReactionRecoveryExecution,
        clock: Callable[[], Timestamp],
    ) -> None:
        self.repo = repo
        self.execution = ReactionRecoveryExecution.model_validate(execution)
        self.clock = clock

    async def _latest_checkpoint_key(self, command: RevisedReactionCommand) -> str | None:
        target = ReactionTarget(
            event_id=command.event_id, token_id=command.token_id, checkpoint=command.checkpoint
        )
        revisions: set[int] = set()
        async for record in self.repo.iter_metadata(
            "out.reaction_checkpoints",
            ["event_id", "token_id", "checkpoint", "revision"],
            filters=target.model_dump(mode="json"),
        ):
            header = ReactionRevisionInventory.from_record(record, target)
            if header.revision in revisions:
                raise ValueError("Recovery latest revision has ambiguous stored witnesses")
            revisions.add(header.revision)
        if not revisions:
            return None
        return f"{target.event_id}:{target.token_id}:{target.checkpoint}:{max(revisions)}"

    async def recover(self, command: ReactionRecoveryCommand) -> ReactionRecoveryOutcome:
        command = ReactionRecoveryCommand.model_validate(command)
        if self.execution.replay:
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_MODE_UNSUPPORTED")
        preview = await self.repo.get("ops.jobs", command.original_job_id)
        if preview is None:
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="WITNESS_UNADMITTED")
        if _unsupported_source_mode(preview):
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="SOURCE_MODE_UNSUPPORTED")
        try:
            original = _original_queue_facts(preview, command.original_job_id)
            target = original.command
            peer_job_id = original.result.root.coalesced_into
            async with (
                self.repo.lock(f"reaction:{target.event_id}:{target.token_id}:{target.checkpoint}"),
                self.repo.lock("reaction-change-write:" + target.change_id),
            ):
                checkpoint_key = await self._latest_checkpoint_key(target)
                if checkpoint_key is None:
                    return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="CHECKPOINT_ABSENT")
                request = ReactionRecoveryWitnessRequest(
                    enqueue_idempotency_key=recovery_idempotency_key(
                        original.job_id, original.payload_revision, peer_job_id
                    ),
                    original_job_id=original.job_id,
                    peer_job_id=peer_job_id,
                    source_backfill_id=command.source_backfill_id,
                    checkpoint_key=checkpoint_key,
                    change_marker_key="reaction-change:" + target.change_id,
                    change_owner_key="reaction-change-owner:" + target.change_id,
                    recovery_link_key=recovery_link_key(original.job_id),
                )
                async with self.repo.reaction_recovery_witnesses(request) as rows:
                    admitted = admit_recovery_witnesses(request, rows)
                    if isinstance(admitted, ReactionRecoveryAlreadyApplied):
                        return admitted
                    if isinstance(admitted, ReactionRecoveryUnresolved):
                        if admitted.reason == "RECOVERY_LINK_UNADMITTED":
                            return await self._existing_work(request, rows)
                        return admitted
                    eligibility = recovery_eligibility(admitted)
                    if not isinstance(eligibility, ReactionRecoveryEligible):
                        return eligibility
                    return await self._enqueue_link(request, rows, admitted)
        except (ValidationError, KeyError, ValueError):
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="WITNESS_UNADMITTED")

    async def _existing_work(
        self, request: ReactionRecoveryWitnessRequest, rows: RecoveryWitnessIndex
    ) -> ReactionRecoveryExistingWork | ReactionRecoveryUnresolved:
        try:
            link_row = rows[("ops.scheduler_checkpoints", request.recovery_link_key)]
            if not isinstance(link_row, ImmutableDocument) or not link_row.immutable:
                raise ValueError("Recovery link has no immutable stored authority")
            link = ReactionRecoveryLink.model_validate(link_row.document)
            state = link.state
            original_record, peer_record, source_record, checkpoint_record = _witness_records(request, rows)
            if any(
                _unsupported_source_mode(record)
                for record in (original_record, peer_record, source_record, checkpoint_record)
            ):
                raise ValueError("Recovery linked witnesses have unsupported source metadata")
            original = _original_queue_facts(original_record, request.original_job_id)
            source = recovery_backfill_from_record(source_record)
            checkpoint = captured_reaction_receipt(request.checkpoint_key, checkpoint_record)
            budget = RecoveryRetryBudget.model_validate(
                {
                    field: original_record[field]
                    for field in RecoveryRetryBudget.model_fields
                    if field in original_record
                }
            )
            if (
                link.checkpoint_id != request.recovery_link_key
                or state.original_job_id != request.original_job_id
                or state.peer_job_id != request.peer_job_id
                or state.source.backfill_id != request.source_backfill_id
                or state.original_payload_revision != original.payload_revision
                or state.retry_budget != budget
                or state.policy_revision != recovery_policy_revision()
                or state.original_record_revision != sha256(original_record)
                or state.peer_record_revision != sha256(peer_record)
                or state.source_record_revision != sha256(source_record)
                or state.checkpoint_record_revision != sha256(checkpoint_record)
                or state.source != source
                or state.checkpoint != checkpoint
            ):
                raise ValueError("Recovery link conflicts with the actual current witnesses")
            record = await self.repo.get("ops.jobs", state.recovered_job_id)
            if record is None or _unsupported_source_mode(record):
                raise ValueError("Recovery linked work is unavailable or has unsupported provenance")
            work = RecoveryLinkedQueueWitness.model_validate(
                {field: record[field] for field in RecoveryLinkedQueueWitness.model_fields if field in record}
            )
            if (
                work.job_id != state.recovered_job_id
                or work.idem_key != request.enqueue_idempotency_key
                or sha256(work.payload) != state.original_payload_revision
                or work.priority != state.retry_budget.priority
                or work.max_attempts != state.retry_budget.max_attempts
                or propagate_timestamp(record, "expires_at")
                != late_reaction_schedule(Timestamp(link.at, link.at_prec, link.at_prec_reason)).expires_at
            ):
                raise ValueError("Recovery linked work has another identity or payload")
            return ReactionRecoveryExistingWork(
                kind="EXISTING_WORK",
                recovery_link_key=link.checkpoint_id,
                work=DependencyState(job_id=work.job_id, status=work.status),
            )
        except (ValidationError, ValueError, KeyError):
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="RECOVERY_LINK_UNADMITTED")

    async def _enqueue_link(
        self,
        request: ReactionRecoveryWitnessRequest,
        rows: RecoveryWitnessIndex,
        evidence: ReactionRecoveryEvidence,
    ) -> ReactionRecoveryQueued | ReactionRecoveryUnresolved:
        existing = await self.repo.list(
            "ops.jobs",
            filters={"type": "REACTION_CAPTURE", "idem_key": request.enqueue_idempotency_key},
            limit=2,
        )
        if existing:
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="WORK_WITHOUT_LINK")
        original_record, peer_record, source_record, checkpoint_record = _witness_records(request, rows)
        try:
            budget = RecoveryRetryBudget.model_validate(
                {
                    field: original_record[field]
                    for field in RecoveryRetryBudget.model_fields
                    if field in original_record
                }
            )
        except ValidationError:
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="RETRY_BUDGET_UNREPORTED")
        try:
            schedule = late_reaction_schedule(self.clock())
        except ValueError:
            return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="OPERATION_CLOCK_UNREPORTED")
        original = RecoveryOriginalQueueWitness.model_validate(
            {
                field: original_record[field]
                for field in RecoveryOriginalQueueWitness.model_fields
                if field in original_record
            }
        )
        record = await self.repo.enqueue(
            "REACTION_CAPTURE",
            original.payload,
            request.enqueue_idempotency_key,
            priority=budget.priority,
            max_attempts=budget.max_attempts,
            run_after=schedule.run_after,
            expires_at=schedule.expires_at,
        )
        queued = RecoveryQueuedWitness.model_validate(
            {field: record[field] for field in RecoveryQueuedWitness.model_fields if field in record}
        )
        if (
            queued.idem_key != request.enqueue_idempotency_key
            or sha256(queued.payload) != evidence.original.payload_revision
            or queued.priority != budget.priority
            or queued.max_attempts != budget.max_attempts
            or propagate_timestamp(record, "run_after") != schedule.run_after
            or propagate_timestamp(record, "expires_at") != schedule.expires_at
        ):
            raise ValueError("Recovery enqueue did not retain its actual request and schedule")
        if evidence.source is None or evidence.checkpoint is None:
            raise ValueError("Recovery source and checkpoint are required before linking")
        state = ReactionRecoveryLinkState(
            contract_version=1,
            policy="FAILED_LEGACY_COALESCING",
            policy_revision=recovery_policy_revision(),
            original_job_id=evidence.original.job_id,
            peer_job_id=request.peer_job_id,
            original_payload_revision=evidence.original.payload_revision,
            recovered_job_id=queued.job_id,
            retry_budget=budget,
            source=evidence.source,
            checkpoint=evidence.checkpoint.receipt,
            original_record_revision=sha256(original_record),
            peer_record_revision=sha256(peer_record),
            source_record_revision=sha256(source_record),
            checkpoint_record_revision=sha256(checkpoint_record),
            versions=self.execution.versions,
        )
        link = ReactionRecoveryLink.model_validate(
            {
                "checkpoint_id": request.recovery_link_key,
                **schedule.recorded_at.fields("at"),
                "state": state,
            }
        )
        stored = ReactionRecoveryLink.model_validate(
            await self.repo.put(
                "ops.scheduler_checkpoints",
                request.recovery_link_key,
                link.model_dump(mode="json"),
                immutable=True,
            )
        )
        if stored != link:
            raise ValueError("Recovery link conflicts with its actual successful storage effect")
        return ReactionRecoveryQueued(
            kind="QUEUED", job_id=stored.state.recovered_job_id, recovery_link_key=stored.checkpoint_id
        )

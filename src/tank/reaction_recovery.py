"""Evidence-gated eligibility for a retained lost coalescing obligation."""

from collections.abc import Mapping
from typing import Literal

from pydantic import JsonValue, ValidationError

from tank.contracts import DomainModel
from tank.contracts.jobs import JobEnvelope
from tank.contracts.reaction_recovery import (
    ReactionRecoveryAlreadyApplied,
    ReactionRecoveryEligibility,
    ReactionRecoveryEligible,
    ReactionRecoveryEvidence,
    ReactionRecoveryUnresolved,
    ReactionRecoveryWitnessRequest,
    RecoveryBoundCheckpoint,
    RecoveryFailedPeer,
    RecoveryMarkerAbsent,
    RecoveryMarkerApplied,
    RecoveryMarkerEvidence,
    RecoveryMarkerUnresolved,
    RecoveryOriginalJob,
    RecoveryStorageKey,
    RecoveryTerminalBackfill,
    RecoveryWitnessCollection,
    RetainedCoalescedReaction,
)
from tank.contracts.scheduler_commands import ReactionAnchorFacts, RevisedReactionCommand
from tank.contracts.scheduler_results import (
    ReactionChangeTarget,
    applied_reaction_change_receipt,
    captured_reaction_receipt,
)
from tank.contracts.stored_documents import ImmutableDocument, NativeBackfillWitness
from tank.runtime_reaction import reaction_job_command
from tank.time_precision import propagate_timestamp
from tank.util import parse_time, sha256

type RecoveryWitnessIndex = Mapping[
    tuple[RecoveryWitnessCollection, str], ImmutableDocument | NativeBackfillWitness | None
]


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
        original_job = JobEnvelope.model_validate(original_row.document)
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
                "result": RetainedCoalescedReaction.model_validate(original_job.result),
            }
        )
        identity = recovery_idempotency_key(original.job_id, original.payload_revision, request.peer_job_id)
        if (
            original.result.root.coalesced_into != request.peer_job_id
            or request.enqueue_idempotency_key != identity
            or request.recovery_link_key != identity
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
            peer_job = JobEnvelope.model_validate(peer_row.document)
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
            source = recovery_backfill_from_record(source_row.record)
            if source.backfill_id != request.source_backfill_id:
                raise ValueError("Recovery source witness has another identity")
        checkpoint_row = rows[("out.reaction_checkpoints", request.checkpoint_key)]
        checkpoint = None
        if checkpoint_row is not None:
            if not isinstance(checkpoint_row, ImmutableDocument):
                raise ValueError("Recovery checkpoint witness is not a document")
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
    ):
        return ReactionRecoveryUnresolved(kind="UNRESOLVED", reason="CHECKPOINT_CLOCK_UNREPORTED")
    if (
        parse_time(anchor.value) != parse_time(source.anchor.value)
        or anchor.precision != source.anchor.precision
    ):
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

"""Evidence-gated eligibility for a retained lost coalescing obligation."""

from collections.abc import Mapping
from typing import Literal

from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.contracts.reaction_recovery import (
    ReactionRecoveryAlreadyApplied,
    ReactionRecoveryEligibility,
    ReactionRecoveryEligible,
    ReactionRecoveryEvidence,
    ReactionRecoveryUnresolved,
    RecoveryMarkerApplied,
    RecoveryMarkerUnresolved,
    RecoveryStorageKey,
    RecoveryTerminalBackfill,
)
from tank.time_precision import propagate_timestamp
from tank.util import parse_time, sha256


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

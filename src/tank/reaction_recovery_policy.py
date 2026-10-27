"""Pure evidence eligibility, independent of recovery storage and queue orchestration."""

from tank.contracts.reaction_recovery import (
    ReactionRecoveryAlreadyApplied,
    ReactionRecoveryEligibility,
    ReactionRecoveryEligible,
    ReactionRecoveryEvidence,
    ReactionRecoveryUnresolved,
    RecoveryMarkerApplied,
    RecoveryMarkerUnresolved,
)
from tank.time_precision import reconcile_precision
from tank.util import parse_time


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

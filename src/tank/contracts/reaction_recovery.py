"""Exact storage addresses for a transaction-scoped recovery witness read."""

from typing import Annotated, Literal, Self

from pydantic import ConfigDict, RootModel, StringConstraints, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag
from tank.contracts.scheduler_commands import ReactionAnchorFacts, ReactionSubject, RevisedReactionCommand
from tank.contracts.scheduler_results import (
    AppliedReactionChange,
    CapturedReaction,
    CoalescedReaction,
    ReactionFingerprint,
)
from tank.time_precision import Timestamp

RecoveryStorageKey = Annotated[str, StringConstraints(min_length=1, pattern=r"\S")]
RecoveryWitnessCollection = Literal[
    "ops.jobs", "mkt.backfill_runs", "out.reaction_checkpoints", "ops.scheduler_checkpoints"
]


class LegacyCoalescedReaction(DomainModel):
    """The original writer's complete result, without a later discriminator."""

    coalesced_into: RecoveryStorageKey


class RetainedCoalescedReaction(RootModel[LegacyCoalescedReaction | CoalescedReaction]):
    """Recognize only the two actual historical coalescing result contracts."""

    model_config = ConfigDict(strict=True, hide_input_in_errors=True, revalidate_instances="always")


class RecoveryOriginalJob(DomainModel):
    job_id: RecoveryStorageKey
    status: Literal["DONE"]
    command: RevisedReactionCommand
    payload_revision: ReactionFingerprint
    result: RetainedCoalescedReaction


class RecoveryFailedPeer(DomainModel):
    job_id: RecoveryStorageKey
    status: Literal["FAILED", "EXPIRED"]
    command: RevisedReactionCommand


class RecoveryTerminalBackfill(ReactionSubject):
    backfill_id: RecoveryStorageKey
    change_id: RecoveryStorageKey
    status: Literal["COMPLETE", "PARTIAL", "FAILED"]
    anchor: Timestamp
    finished_at: Timestamp


class RecoveryBoundCheckpoint(DomainModel):
    receipt: CapturedReaction
    immutable: TrueFlag
    anchor: ReactionAnchorFacts
    captured_at: Timestamp


class RecoveryMarkerAbsent(DomainModel):
    kind: Literal["ABSENT"]


class RecoveryMarkerApplied(DomainModel):
    kind: Literal["APPLIED"]
    receipt: AppliedReactionChange


class RecoveryMarkerUnresolved(DomainModel):
    kind: Literal["UNRESOLVED"]


type RecoveryMarkerEvidence = RecoveryMarkerAbsent | RecoveryMarkerApplied | RecoveryMarkerUnresolved


class ReactionRecoveryEvidence(DomainModel):
    """Consumed facts admitted from locked witnesses, never a replacement raw record."""

    original: RecoveryOriginalJob
    peer: RecoveryFailedPeer | None
    source: RecoveryTerminalBackfill | None
    checkpoint: RecoveryBoundCheckpoint | None
    marker: RecoveryMarkerEvidence


class ReactionRecoveryEligible(DomainModel):
    """Eligibility only; a writer must retain the original locked queue payload bytes."""

    kind: Literal["ELIGIBLE"]
    original_job_id: RecoveryStorageKey
    peer_job_id: RecoveryStorageKey
    source_backfill_id: RecoveryStorageKey
    checkpoint_key: RecoveryStorageKey
    payload_revision: ReactionFingerprint
    command: RevisedReactionCommand


class ReactionRecoveryAlreadyApplied(DomainModel):
    kind: Literal["ALREADY_APPLIED"]
    receipt: AppliedReactionChange


class ReactionRecoveryUnresolved(DomainModel):
    kind: Literal["UNRESOLVED"]
    reason: Literal[
        "MARKER_UNRESOLVED",
        "MARKER_SCOPE_MISMATCH",
        "PEER_ABSENT",
        "PEER_SCOPE_MISMATCH",
        "SOURCE_ABSENT",
        "SOURCE_SCOPE_MISMATCH",
        "SOURCE_CLOCK_UNREPORTED",
        "CHECKPOINT_ABSENT",
        "CHECKPOINT_SCOPE_MISMATCH",
        "CHECKPOINT_CLOCK_UNREPORTED",
        "SOURCE_MAY_ALREADY_BE_CAPTURED",
    ]


type ReactionRecoveryEligibility = (
    ReactionRecoveryEligible | ReactionRecoveryAlreadyApplied | ReactionRecoveryUnresolved
)


class ReactionRecoveryWitnessAddress(DomainModel):
    collection: RecoveryWitnessCollection
    storage_key: RecoveryStorageKey


class ReactionRecoveryWitnessRequest(DomainModel):
    """Name actual witnesses and the enqueue lock required before the transaction."""

    enqueue_idempotency_key: RecoveryStorageKey
    original_job_id: RecoveryStorageKey
    peer_job_id: RecoveryStorageKey
    source_backfill_id: RecoveryStorageKey
    checkpoint_key: RecoveryStorageKey
    change_marker_key: RecoveryStorageKey
    change_owner_key: RecoveryStorageKey
    recovery_link_key: RecoveryStorageKey

    @model_validator(mode="after")
    def distinct_witnesses(self) -> Self:
        if self.original_job_id == self.peer_job_id:
            raise ValueError("Recovery job witnesses must be distinct")
        scheduler_keys = (self.change_marker_key, self.change_owner_key, self.recovery_link_key)
        if len(set(scheduler_keys)) != len(scheduler_keys):
            raise ValueError("Recovery scheduler witnesses must be distinct")
        return self

    def addresses(self) -> tuple[ReactionRecoveryWitnessAddress, ...]:
        """Return deterministic record-lock order without deriving missing identities."""
        coordinates: tuple[tuple[RecoveryWitnessCollection, str], ...] = (
            ("ops.jobs", self.original_job_id),
            ("ops.jobs", self.peer_job_id),
            ("mkt.backfill_runs", self.source_backfill_id),
            ("out.reaction_checkpoints", self.checkpoint_key),
            ("ops.scheduler_checkpoints", self.change_marker_key),
            ("ops.scheduler_checkpoints", self.change_owner_key),
            ("ops.scheduler_checkpoints", self.recovery_link_key),
        )
        return tuple(
            ReactionRecoveryWitnessAddress(collection=collection, storage_key=key)
            for collection, key in sorted(coordinates)
        )

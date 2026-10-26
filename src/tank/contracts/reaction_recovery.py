"""Exact storage addresses for a transaction-scoped recovery witness read."""

from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, JsonValue, RootModel, StringConstraints, model_validator

from tank.contracts import DomainModel
from tank.contracts.jobs import DependencyState
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.scalars import OneInt, TrueFlag
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


class RecoveryOriginalQueueWitness(DomainModel):
    job_id: RecoveryStorageKey
    type: Literal["REACTION_CAPTURE"]
    status: Literal["DONE"]
    payload: dict[str, JsonValue]
    result: RetainedCoalescedReaction


class RecoveryPeerQueueWitness(DomainModel):
    job_id: RecoveryStorageKey
    type: Literal["REACTION_CAPTURE"]
    status: Literal["FAILED", "EXPIRED"]
    payload: dict[str, JsonValue]


class RecoveryRetryBudget(DomainModel):
    priority: Annotated[int, Field(ge=0, le=4)]
    max_attempts: Annotated[int, Field(ge=1)]


class RecoveryLinkedQueueWitness(DependencyState, RecoveryRetryBudget):
    type: Literal["REACTION_CAPTURE"]
    idem_key: RecoveryStorageKey
    payload: dict[str, JsonValue]
    expires_at: str
    expires_at_prec: Literal["COLLECTION_ONLY"]
    expires_at_prec_reason: str | None


class RecoveryQueuedWitness(RecoveryRetryBudget):
    job_id: RecoveryStorageKey
    type: Literal["REACTION_CAPTURE"]
    status: Literal["QUEUED"]
    idem_key: RecoveryStorageKey
    payload: dict[str, JsonValue]
    run_after: str
    run_after_prec: Literal["COLLECTION_ONLY"]
    run_after_prec_reason: str | None
    expires_at: str
    expires_at_prec: Literal["COLLECTION_ONLY"]
    expires_at_prec_reason: str | None


class ReactionRecoveryCommand(DomainModel):
    original_job_id: RecoveryStorageKey
    source_backfill_id: RecoveryStorageKey


class ReactionRecoveryExecution(DomainModel):
    versions: ResolvedVersionIdentity
    replay: bool


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
    """Eligibility only; a writer must retain the actual locked queue payload."""

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
        "WITNESS_UNADMITTED",
        "RECOVERY_LINK_UNADMITTED",
        "WORK_WITHOUT_LINK",
        "SOURCE_MODE_UNSUPPORTED",
        "RETRY_BUDGET_UNREPORTED",
        "OPERATION_CLOCK_UNREPORTED",
    ]


type ReactionRecoveryEligibility = (
    ReactionRecoveryEligible | ReactionRecoveryAlreadyApplied | ReactionRecoveryUnresolved
)


class ReactionRecoveryLinkState(DomainModel):
    contract_version: OneInt
    policy: Literal["FAILED_LEGACY_COALESCING"]
    policy_revision: ReactionFingerprint
    original_job_id: RecoveryStorageKey
    peer_job_id: RecoveryStorageKey
    original_payload_revision: ReactionFingerprint
    recovered_job_id: RecoveryStorageKey
    retry_budget: RecoveryRetryBudget
    source: RecoveryTerminalBackfill
    checkpoint: CapturedReaction
    original_record_revision: ReactionFingerprint
    peer_record_revision: ReactionFingerprint
    source_record_revision: ReactionFingerprint
    checkpoint_record_revision: ReactionFingerprint
    versions: ResolvedVersionIdentity


class ReactionRecoveryLink(DomainModel):
    checkpoint_id: RecoveryStorageKey
    at: RecoveryStorageKey
    at_prec: Literal["COLLECTION_ONLY"]
    at_prec_reason: str | None
    state: ReactionRecoveryLinkState

    @model_validator(mode="after")
    def actual_operation_clock(self) -> Self:
        Timestamp(self.at, self.at_prec, self.at_prec_reason)
        return self


class ReactionRecoveryQueued(DomainModel):
    kind: Literal["QUEUED"]
    job_id: RecoveryStorageKey
    recovery_link_key: RecoveryStorageKey


class ReactionRecoveryExistingWork(DomainModel):
    """Observed work identity/status, never an assertion that source coverage was applied."""

    kind: Literal["EXISTING_WORK"]
    recovery_link_key: RecoveryStorageKey
    work: DependencyState


type ReactionRecoveryOutcome = (
    ReactionRecoveryQueued
    | ReactionRecoveryExistingWork
    | ReactionRecoveryAlreadyApplied
    | ReactionRecoveryUnresolved
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

"""Exact storage addresses for a transaction-scoped recovery witness read."""

from typing import Annotated, Literal, Self

from pydantic import StringConstraints, model_validator

from tank.contracts import DomainModel

RecoveryStorageKey = Annotated[str, StringConstraints(min_length=1, pattern=r"\S")]
RecoveryWitnessCollection = Literal[
    "ops.jobs", "mkt.backfill_runs", "out.reaction_checkpoints", "ops.scheduler_checkpoints"
]


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

"""Durable exact reconsideration demands and explicit discovery dispositions."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import OneInt
from tank.contracts.scheduler_commands import (
    ReactionAnchorFacts,
    ReactionRevision,
    ReactionSubject,
    RevisedReactionCommand,
)
from tank.contracts.scheduler_results import ReactionFingerprint, SchedulerIdentity
from tank.time_precision import Timestamp
from tank.util import parse_time


class HistoricalReactionChange(ReactionSubject):
    kind: Literal["HISTORICAL_CAPTURE_TERMINAL"]
    change_id: SchedulerIdentity
    backfill_id: SchedulerIdentity
    status: Literal["COMPLETE", "PARTIAL", "FAILED"]
    anchor: str | None
    finished_at: str

    @model_validator(mode="after")
    def declared_clocks(self) -> Self:
        parse_time(self.finished_at)
        if self.anchor is not None:
            parse_time(self.anchor)
        return self


class NormalizedReactionChange(ReactionSubject):
    kind: Literal["LATE_NORMALIZED_DATA"]
    change_id: SchedulerIdentity
    status: Literal["AVAILABLE"]
    anchor: str
    observed_at: str
    source_count: Annotated[int, Field(ge=1)]
    market_from: str
    semantic_fingerprint: ReactionFingerprint

    @model_validator(mode="after")
    def declared_clocks(self) -> Self:
        for value in (self.anchor, self.observed_at, self.market_from):
            parse_time(value)
        return self


type ReactionChangeObservation = HistoricalReactionChange | NormalizedReactionChange


class ReactionDemandBaseline(DomainModel):
    revision: ReactionRevision
    anchor: ReactionAnchorFacts
    captured_at: Timestamp
    window_end: Timestamp | None


class ReactionDemandFacts(DomainModel):
    command: RevisedReactionCommand
    source: ReactionChangeObservation
    source_revision: ReactionFingerprint
    discovered_at: Timestamp
    baseline: ReactionDemandBaseline

    @model_validator(mode="after")
    def source_owns_demand(self) -> Self:
        if (
            self.command.event_id != self.source.event_id
            or self.command.token_id != self.source.token_id
            or self.command.change_id != self.source.change_id + ":" + self.command.checkpoint
        ):
            raise ValueError("Reaction demand conflicts with its actual source scope")
        return self


class PendingReactionDemand(ReactionDemandFacts):
    phase: Literal["PENDING"]


class LinkedReactionDemand(ReactionDemandFacts):
    phase: Literal["LINKED"]
    job_id: SchedulerIdentity
    idem_key: SchedulerIdentity


class BlockedReactionDemand(ReactionDemandFacts):
    phase: Literal["BLOCKED"]
    job_id: SchedulerIdentity
    idem_key: SchedulerIdentity
    reason: Literal[
        "TERMINAL_WORK_UNAPPLIED",
        "LEGACY_COALESCED_RECOVERY_REQUIRED",
        "LINKED_WORK_UNREPORTED",
        "LINKED_WORK_SCOPE_CONFLICT",
        "APPLIED_MARKER_UNRESOLVED",
    ]


type ReactionDemand = PendingReactionDemand | LinkedReactionDemand | BlockedReactionDemand


class ReactionDiscoveryDisposition(DomainModel):
    source: ReactionChangeObservation
    source_revision: ReactionFingerprint
    discovered_at: Timestamp
    reason: Literal[
        "NO_CAPTURED_TARGET",
        "SOURCE_ANCHOR_UNREPORTED",
        "SOURCE_ANCHOR_SCOPE_UNMATCHED",
        "OUTSIDE_CAPTURED_WINDOW",
        "NOT_LATE_AT_DISCOVERY",
    ]


class FreshDemandHistory(DomainModel):
    kind: Literal["FRESH_CHECKPOINT"]


class UnreportedDemandHistory(DomainModel):
    kind: Literal["LEGACY_DEMANDS_UNREPORTED"]
    cursor: Timestamp
    record_revision: ReactionFingerprint


class ReactionDemandLedger(DomainModel):
    contract_version: OneInt
    history: FreshDemandHistory | UnreportedDemandHistory
    demands: dict[SchedulerIdentity, ReactionDemand]
    observations: dict[SchedulerIdentity, ReactionDiscoveryDisposition]

    @model_validator(mode="after")
    def exact_index_keys(self) -> Self:
        if any(key != demand.command.change_id for key, demand in self.demands.items()):
            raise ValueError("Reaction demand index conflicts with its actual change identity")
        if any(key != observation.source.change_id for key, observation in self.observations.items()):
            raise ValueError("Reaction observation index conflicts with its actual source identity")
        return self

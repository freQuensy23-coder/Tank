"""Actual scheduler execution receipts, separate from persisted financial evidence."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import FalseFlag
from tank.contracts.scheduler_commands import ReactionCheckpoint, ReactionSubject
from tank.contracts.scheduler_commands import ReactionRevision as ReactionRevision
from tank.util import sha256

type SchedulerIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]


class DeferredCondition(DomainModel):
    cond_id: SchedulerIdentity
    reason: Literal["BATCH_BUDGET", "READ_TIMEOUT"]


class ConditionEvaluationResult(DomainModel):
    evaluated: list[SchedulerIdentity]
    fired: list[SchedulerIdentity]
    deferred: list[DeferredCondition]

    @model_validator(mode="after")
    def unique_disjoint_outcomes(self) -> Self:
        evaluated = set(self.evaluated)
        fired = set(self.fired)
        deferred = {item.cond_id for item in self.deferred}
        if (
            len(evaluated) != len(self.evaluated)
            or len(fired) != len(self.fired)
            or len(deferred) != len(self.deferred)
        ):
            raise ValueError("Condition evaluation IDs must be unique within each outcome")
        if (
            not evaluated.isdisjoint(fired)
            or not evaluated.isdisjoint(deferred)
            or not fired.isdisjoint(deferred)
        ):
            raise ValueError("Condition evaluation outcomes must be disjoint")
        return self


type ReactionFingerprint = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ReactionReceiptIdentity(DomainModel):
    checkpoint_key: SchedulerIdentity
    event_id: SchedulerIdentity
    token_id: SchedulerIdentity
    checkpoint: ReactionCheckpoint


class CapturedReaction(ReactionReceiptIdentity):
    kind: Literal["CAPTURED"]
    revision: ReactionRevision
    data_fingerprint: ReactionFingerprint


class HistoricalCapturedReaction(ReactionReceiptIdentity):
    kind: Literal["HISTORICAL_CAPTURE"]
    revision: ReactionRevision
    data_fingerprint: None


class ReactionChangeTarget(DomainModel):
    change_id: SchedulerIdentity
    event_id: SchedulerIdentity
    token_id: SchedulerIdentity
    checkpoint: ReactionCheckpoint


class UnchangedReaction(ReactionChangeTarget):
    kind: Literal["UNCHANGED"]
    changed: FalseFlag
    revision: ReactionRevision
    data_fingerprint: ReactionFingerprint


class AppliedReactionChange(ReactionChangeTarget):
    kind: Literal["CHANGE_ALREADY_APPLIED"]
    changed: bool
    revision: ReactionRevision
    data_fingerprint: ReactionFingerprint


class CoalescedReaction(DomainModel):
    """A pending job reference, including historical job results; never capture completion."""

    kind: Literal["COALESCED"]
    coalesced_into: SchedulerIdentity


type ReactionCaptureResult = (
    CapturedReaction | HistoricalCapturedReaction | UnchangedReaction | AppliedReactionChange
)


def captured_reaction_receipt(checkpoint_key: str, record: Mapping[str, JsonValue]) -> CapturedReaction:
    receipt = CapturedReaction.model_validate(
        {"kind": "CAPTURED", "checkpoint_key": checkpoint_key}
        | {
            field: record[field]
            for field in ("event_id", "token_id", "checkpoint", "revision", "data_fingerprint")
            if field in record
        }
    )
    expected = f"{receipt.event_id}:{receipt.token_id}:{receipt.checkpoint}:{receipt.revision}"
    if receipt.checkpoint_key != expected:
        raise ValueError("Reaction receipt conflicts with its actual checkpoint storage key")
    return receipt


def recorded_reaction_receipt(
    checkpoint_key: str, record: Mapping[str, JsonValue]
) -> CapturedReaction | HistoricalCapturedReaction:
    if record.get("data_fingerprint") is not None:
        return captured_reaction_receipt(checkpoint_key, record)
    receipt = HistoricalCapturedReaction.model_validate(
        {
            "kind": "HISTORICAL_CAPTURE",
            "checkpoint_key": checkpoint_key,
            "data_fingerprint": None,
        }
        | {
            field: record[field]
            for field in ("event_id", "token_id", "checkpoint", "revision")
            if field in record
        }
    )
    expected = f"{receipt.event_id}:{receipt.token_id}:{receipt.checkpoint}:{receipt.revision}"
    if receipt.checkpoint_key != expected:
        raise ValueError("Historical reaction receipt conflicts with its actual checkpoint storage key")
    return receipt


class ReactionChangeState(DomainModel):
    changed: bool
    revision: ReactionRevision
    data_fingerprint: ReactionFingerprint


class ReactionChangeMarker(DomainModel):
    checkpoint_id: SchedulerIdentity
    state: ReactionChangeState


class ReactionChangeOwnership(ReactionChangeTarget):
    checkpoint_id: SchedulerIdentity
    marker_key: SchedulerIdentity
    marker_sha256: ReactionFingerprint


def applied_reaction_change_receipt(
    target: ReactionChangeTarget,
    record: Mapping[str, JsonValue],
    owner_record: Mapping[str, JsonValue],
) -> AppliedReactionChange:
    target = ReactionChangeTarget.model_validate(target)
    marker = ReactionChangeMarker.model_validate(
        {field: record[field] for field in ("checkpoint_id", "state") if field in record}
    )
    owner = ReactionChangeOwnership.model_validate(
        {
            field: owner_record[field]
            for field in ReactionChangeOwnership.model_fields
            if field in owner_record
        }
    )
    if (
        marker.checkpoint_id != "reaction-change:" + target.change_id
        or owner.checkpoint_id != "reaction-change-owner:" + target.change_id
        or owner.marker_key != marker.checkpoint_id
        or owner.change_id != target.change_id
        or owner.event_id != target.event_id
        or owner.token_id != target.token_id
        or owner.checkpoint != target.checkpoint
        or owner.marker_sha256 != sha256(record)
    ):
        raise ValueError("Reaction change receipt conflicts with its recorded scope")
    return AppliedReactionChange(
        kind="CHANGE_ALREADY_APPLIED",
        change_id=owner.change_id,
        event_id=owner.event_id,
        token_id=owner.token_id,
        checkpoint=owner.checkpoint,
        changed=marker.state.changed,
        revision=marker.state.revision,
        data_fingerprint=marker.state.data_fingerprint,
    )


def reaction_capture_result(value: ReactionCaptureResult) -> ReactionCaptureResult:
    match value:
        case CapturedReaction():
            return CapturedReaction.model_validate(value)
        case HistoricalCapturedReaction():
            return HistoricalCapturedReaction.model_validate(value)
        case UnchangedReaction():
            return UnchangedReaction.model_validate(value)
        case AppliedReactionChange():
            return AppliedReactionChange.model_validate(value)
        case _:
            raise ValueError("Reaction execution did not produce an admitted receipt")


class ReactionPackState(ReactionSubject):
    rwp_id: SchedulerIdentity


class ReactionPackCheckpoint(DomainModel):
    checkpoint_id: SchedulerIdentity
    state: ReactionPackState

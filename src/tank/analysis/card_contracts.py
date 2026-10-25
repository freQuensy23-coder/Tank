"""Publication facts, authored narrative and rendering policy independent of I/O."""

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import Field, field_validator, model_validator

from tank.analysis.decision_contracts import Action, Confidence, EventState
from tank.contracts import DomainModel
from tank.contracts.configuration import Thresholds
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

type PublicationKind = Literal["PARTIAL", "FULL", "UPDATE"]
type AssessmentClass = Literal["LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"]
type EventLifecycle = Literal[
    "DETECTED", "SCORED", "TRACKED", "ANALYZING", "MONITORING", "INVALIDATED", "CLOSED", "MERGED"
]
type EventFreshness = Literal["FRESH", "NORMAL", "POOR", "STALE"]
type NegativeEventMarker = Literal["CANCELLED", "REVERSED", "REJECTED", "PAUSED"]
type WhyBullet = Annotated[str, Field(min_length=1, max_length=220, pattern=r"\S")]
type GateReason = Literal[
    "BELOW_MAJOR",
    "STALE_STATE_CHANGE",
    "STALE",
    "NO_MATERIAL_CHANGE",
    "UPDATE_RATE_LIMIT",
    "UPDATE",
    "LOW_CERTAINTY",
    "INVALIDATED",
    "INITIAL",
]


class PublicationNarrative(DomainModel):
    kind: PublicationKind
    what: str = Field(min_length=1, max_length=500, pattern=r"\S")
    verdict: str = Field(min_length=1, max_length=160, pattern=r"\S")
    why: tuple[WhyBullet, ...] = Field(min_length=2, max_length=3)


class PublicationPolicy(DomainModel):
    card_max_chars: int = Field(gt=0)
    update_rate_limit_s: int = Field(ge=0)

    @classmethod
    def from_thresholds(cls, thresholds: Thresholds) -> Self:
        thresholds = Thresholds.model_validate(thresholds)
        return cls(
            card_max_chars=thresholds.card_max_chars,
            update_rate_limit_s=thresholds.update_rate_limit_s,
        )


class PublicationEventFacts(DomainModel):
    state: EventState
    state_verified: bool
    certainty: Confidence
    lifecycle: EventLifecycle
    freshness: EventFreshness
    trigger_freshness: EventFreshness | None
    stage_freshness: EventFreshness | None
    negative_marker: NegativeEventMarker | None
    active_fatal_ids: frozenset[str]
    has_new_milestone_fact: bool
    has_expired_call_fact: bool

    @field_validator("active_fatal_ids")
    @classmethod
    def actual_fatal_identities(cls, values: frozenset[str]) -> frozenset[str]:
        if any(not value.strip() for value in values):
            raise ValueError("Fatal publication facts require actual contradiction identities")
        return values


class PublicationDecisionStatus(DomainModel):
    token_id: str = Field(min_length=1)
    action: Action
    active: bool

    @field_validator("token_id")
    @classmethod
    def actual_token(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Publication status requires an actual token identity")
        return value


class AcknowledgedPublicationClock(DomainModel):
    stamp: Timestamp

    @model_validator(mode="after")
    def local_acknowledgement_clock(self) -> Self:
        if self.stamp.value is None:
            raise ValueError("A SENT acknowledgement requires its actual recorded clock")
        if self.stamp.precision not in {None, "EXACT", "COLLECTION_ONLY"}:
            raise ValueError("A delivery acknowledgement is a local clock, not an event bucket")
        return self

    def instant(self) -> datetime:
        if self.stamp.value is None:
            raise ValueError("A SENT acknowledgement requires its actual recorded clock")
        return parse_iso_time(self.stamp.value)


class PublicationBaseline(DomainModel):
    assessment_class: AssessmentClass
    event: PublicationEventFacts
    actions_by_token: dict[str, Action]
    acknowledged_send: AcknowledgedPublicationClock | None

    @field_validator("actions_by_token")
    @classmethod
    def actual_token_keys(cls, values: dict[str, Action]) -> dict[str, Action]:
        if any(not value.strip() for value in values):
            raise ValueError("Publication history requires actual token identities")
        return values


class PublicationChangeInputs(DomainModel):
    event: PublicationEventFacts
    decisions: tuple[PublicationDecisionStatus, ...]
    previous: PublicationBaseline | None

    @model_validator(mode="after")
    def unique_current_decisions(self) -> Self:
        if len({value.token_id for value in self.decisions}) != len(self.decisions):
            raise ValueError("Publication changes require one current decision per token")
        return self


class PublicationGateInputs(DomainModel):
    event: PublicationEventFacts
    assessment_class: AssessmentClass
    decisions: tuple[PublicationDecisionStatus, ...]
    previous: PublicationBaseline | None
    policy: PublicationPolicy
    now: datetime

    @model_validator(mode="after")
    def unique_current_decisions(self) -> Self:
        if len({value.token_id for value in self.decisions}) != len(self.decisions):
            raise ValueError("Publication gating requires one current decision per token")
        return self

    @field_validator("now")
    @classmethod
    def actual_clock(cls, value: datetime) -> datetime:
        return parse_iso_time(value)


class PublicationGateResult(DomainModel):
    allowed: bool
    reason: GateReason

    @model_validator(mode="after")
    def consistent_eligibility(self) -> Self:
        if self.allowed != (self.reason in {"INITIAL", "UPDATE"}):
            raise ValueError("Publication eligibility does not match its policy reason")
        return self

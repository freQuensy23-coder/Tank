"""Durable watch predicates, observed counters and lifecycle snapshots."""

from decimal import Decimal, InvalidOperation
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import GenerationProvenance

type WatchMetric = Literal[
    "event.state",
    "event.contradiction_fatal",
    "milestone.outcome",
    "spot.cohort.direction",
    "spot.cohort.cwb_buy",
    "spot.cohort.cwb_sell",
    "spot.classification",
    "deriv.native_oi_chg_pct",
    "deriv.native_oi_vs_normal",
    "deriv.oi_class",
    "deriv.funding_vs_normal",
    "mt.move_class",
    "mt.divergence_state",
    "dc.class",
    "price.repricing_band",
    "price.close_vs_level",
    "price.vs_anchor_pct",
    "liq.depth_p1_vs_pre",
    "liq.spread_ratio",
]
type WatchScalar = str | int | float | bool | Decimal


class WatchPersistence(DomainModel):
    count: int = Field(ge=1)
    unit: Literal["consecutive_windows"]


class WatchLeaf(DomainModel):
    metric: WatchMetric
    window: Literal["W15M", "W1H", "W4H", "W24H", "EVENT"]
    op: Literal["==", "!=", ">", ">=", "<", "<=", "in"]
    value: WatchScalar | list[WatchScalar]
    persist: WatchPersistence
    due_at: str | None = None
    due_at_prec: str | None = None
    due_at_prec_reason: str | None = None
    due_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def valid_predicate(self) -> Self:
        if self.op == "in":
            if not isinstance(self.value, list) or not self.value:
                raise ValueError("membership requires a nonempty value list")
        elif isinstance(self.value, list):
            raise ValueError("only membership accepts a value list")
        if self.op in {">", ">=", "<", "<="}:
            if isinstance(self.value, (bool, list)):
                raise ValueError("ordered watch comparisons require numeric values")
            try:
                numeric = Decimal(str(self.value))
            except InvalidOperation as exc:
                raise ValueError("ordered watch comparisons require numeric values") from exc
            if not numeric.is_finite():
                raise ValueError("ordered watch comparisons require finite numeric values")
        if self.metric.startswith(("event.", "milestone.")) and self.persist.count != 1:
            raise ValueError("event and milestone observations cannot persist across market windows")
        return self


class WatchAll(DomainModel):
    all: list["WatchLeaf | WatchAll"] = Field(min_length=1)


type WatchExpression = WatchLeaf | WatchAll


class ExactStrictMajority(DomainModel):
    kind: Literal["EXACT_STRICT_MAJORITY"]
    value: float | None
    strict_majority: bool | None


class WatchObservation(DomainModel):
    window: int
    value: WatchScalar | ExactStrictMajority | None
    data_available: bool
    unavailable_reason: str | None
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    matched: bool | None = None
    dependency_key: str | None = None
    count: int | None = Field(default=None, ge=0)


class WatchCounter(WatchObservation):
    history: list[WatchObservation] = Field(default_factory=list)


class WatchCondition(GenerationProvenance):
    cond_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    role: Literal["UPGRADE", "DOWNGRADE", "INVALIDATION", "AGENT"]
    human_text: str
    expr: WatchExpression
    status: Literal["ARMED", "FIRED", "CANCELLED"]
    created_by: str
    created_at: str
    created_at_prec: str | None
    created_at_prec_reason: str | None
    created_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    expires_at: str
    expires_at_prec: str | None
    expires_at_prec_reason: str | None
    expires_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    agent_defined: bool = False
    counters: dict[str, WatchCounter] = Field(default_factory=dict)
    milestone_id: str | None = None
    last_shown_by: str | None = None
    rearmed_from: str | None = None
    consumed_by: str | None = None
    superseded_by: str | None = None
    cancelled_by: str | None = None
    cancel_provenance: GenerationProvenance | None = None
    consumed_at: str | None = None
    consumed_at_prec: str | None = None
    consumed_at_prec_reason: str | None = None
    consumed_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    cancelled_at: str | None = None
    cancelled_at_prec: str | None = None
    cancelled_at_prec_reason: str | None = None
    cancelled_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    last_evaluated_at: str | None = None
    last_evaluated_at_prec: str | None = None
    last_evaluated_at_prec_reason: str | None = None
    last_evaluated_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    last_attempted_at: str | None = None
    last_attempted_at_prec: str | None = None
    last_attempted_at_prec_reason: str | None = None
    last_attempted_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    last_attempt_outcome: Literal["READ_TIMEOUT"] | None = None
    cleared_at: str | None = None
    cleared_at_prec: str | None = None
    cleared_at_prec_reason: str | None = None
    cleared_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    fired_at: str | None = None
    fired_at_prec: str | None = None
    fired_at_prec_reason: str | None = None
    fired_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None


class WatchCall(DomainModel):
    call_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    opened_by_alert: str
    opened_by_version: int = Field(ge=1)
    opened_at: str
    opened_at_prec: str | None
    opened_at_prec_reason: str | None
    opened_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    alert_generated_at: str
    alert_generated_at_prec: str | None
    alert_generated_at_prec_reason: str | None
    alert_generated_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    entry_ref_price: Decimal = Field(gt=0)
    invalidation_price: Decimal = Field(gt=0)
    status: Literal["OPEN", "CLOSED"]
    closed_by: Literal["EXIT", "EXPIRED"] | None = None
    closed_at: str | None = None
    closed_at_prec: str | None = None
    closed_at_prec_reason: str | None = None
    closed_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @field_validator("entry_ref_price", "invalidation_price", mode="before")
    @classmethod
    def exact_price_from_storage(cls, value: str | int | float | Decimal) -> Decimal:
        """Decode exact decimal text at the durable call boundary, preserving financial units."""
        if isinstance(value, bool):
            raise ValueError("boolean is not a call price")
        return Decimal(str(value))


class MilestoneSnapshot(DomainModel):
    kind: str = Field(min_length=1)
    due_at: str
    due_at_prec: str | None
    due_at_prec_reason: str | None
    due_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    source_id: str
    status: Literal["CONFIRMED", "VERIFIED", "FAILED", "MISSED"] | None = None
    outcome: Literal["VERIFIED", "FAILED", "MISSED"] | None = None
    expires_at: str | None = None
    expires_at_prec: str | None = None
    expires_at_prec_reason: str | None = None
    expires_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    missed_at: str | None = None
    missed_at_prec: str | None = None
    missed_at_prec_reason: str | None = None
    missed_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

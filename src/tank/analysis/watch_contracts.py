"""Durable watch predicates, observed counters and lifecycle snapshots."""

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Literal, Self

from pydantic import ConfigDict, Field, JsonValue, TypeAdapter, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.provenance import GenerationProvenance
from tank.contracts.scalars import TrueFlag
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

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


class WatchLifecycle(GenerationProvenance):
    cond_id: str = Field(min_length=1, pattern=r"\S")
    event_id: str = Field(min_length=1, pattern=r"\S")
    human_text: str
    status: Literal["ARMED", "FIRED", "CANCELLED", "EXPIRED"]
    created_by: str
    created_at: str
    created_at_prec: str | None
    created_at_prec_reason: str | None
    created_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    expires_at: str
    expires_at_prec: str | None
    expires_at_prec_reason: str | None
    expires_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    cancelled_by: str | None = None
    cancel_provenance: GenerationProvenance | None = None
    cancelled_at: str | None = None
    cancelled_at_prec: str | None = None
    cancelled_at_prec_reason: str | None = None
    cancelled_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    last_evaluated_at: str | None = None
    last_evaluated_at_prec: str | None = None
    last_evaluated_at_prec_reason: str | None = None
    last_evaluated_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    fired_at: str | None = None
    fired_at_prec: str | None = None
    fired_at_prec_reason: str | None = None
    fired_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None


class WatchCondition(WatchLifecycle):
    token_id: str = Field(min_length=1)
    role: Literal["UPGRADE", "DOWNGRADE", "INVALIDATION", "AGENT"]
    expr: WatchExpression
    agent_defined: bool = False
    counters: dict[str, WatchCounter] = Field(default_factory=dict)
    milestone_id: str | None = None
    last_shown_by: str | None = None
    rearmed_from: str | None = None
    consumed_by: str | None = None
    superseded_by: str | None = None
    consumed_at: str | None = None
    consumed_at_prec: str | None = None
    consumed_at_prec_reason: str | None = None
    consumed_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    last_attempted_at: str | None = None
    last_attempted_at_prec: str | None = None
    last_attempted_at_prec_reason: str | None = None
    last_attempted_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    last_attempt_outcome: Literal["READ_TIMEOUT"] | None = None
    cleared_at: str | None = None
    cleared_at_prec: str | None = None
    cleared_at_prec_reason: str | None = None
    cleared_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None


class TimedReviewExpression(DomainModel):
    review_at: str
    review_question: str = Field(min_length=1, max_length=500, pattern=r"\S")

    @field_validator("review_at")
    @classmethod
    def actual_requested_clock(cls, value: str) -> str:
        parse_iso_time(value)
        return value


def parse_timed_review_expression(value: Mapping[str, JsonValue]) -> TimedReviewExpression | None:
    """Recognize the timer variant without treating other expressions as valid metric predicates."""
    if "review_at" in value or "review_question" in value:
        return TimedReviewExpression.model_validate(value)
    if "all" in value:
        children = value["all"]
        if not isinstance(children, list):
            raise ValueError("A watch group requires its actual predicate array")
        for child in children:
            if not isinstance(child, dict):
                raise ValueError("A watch predicate must be an object")
            if parse_timed_review_expression(child) is not None:
                raise ValueError("A timed review cannot be nested inside metric predicates")
    return None


class TimedReviewCondition(WatchLifecycle):
    token_id: None
    cancelled_by: Literal["AGENT", "EVENT_MISSING", "EVENT_TERMINAL"] | None = None
    role: Literal["AGENT"]
    agent_defined: TrueFlag
    created_by: Literal["AGENT"]
    expr: TimedReviewExpression

    @model_validator(mode="after")
    def actual_review_schedule(self) -> Self:
        clocks = (
            (
                self.created_at,
                self.created_at_prec,
                self.created_at_prec_reason,
                self.created_at_bucket_anchor,
            ),
            (
                self.expires_at,
                self.expires_at_prec,
                self.expires_at_prec_reason,
                self.expires_at_bucket_anchor,
            ),
            (
                self.last_evaluated_at,
                self.last_evaluated_at_prec,
                self.last_evaluated_at_prec_reason,
                self.last_evaluated_at_bucket_anchor,
            ),
            (self.fired_at, self.fired_at_prec, self.fired_at_prec_reason, self.fired_at_bucket_anchor),
            (
                self.cancelled_at,
                self.cancelled_at_prec,
                self.cancelled_at_prec_reason,
                self.cancelled_at_bucket_anchor,
            ),
        )
        for value, precision, reason, anchor in clocks:
            Timestamp(value=value, precision=precision, precision_reason=reason, bucket_anchor=anchor)
        created = parse_iso_time(self.created_at)
        due = parse_iso_time(self.expr.review_at)
        expiry = parse_iso_time(self.expires_at)
        if not created < due < expiry:
            raise ValueError("Timed review must lie strictly after creation and before expiry")
        if self.human_text != self.expr.review_question:
            raise ValueError("Timed review text must preserve its actual saved question")
        if self.status == "FIRED":
            if self.fired_at is None or not due <= parse_iso_time(self.fired_at) < expiry:
                raise ValueError("Fired review requires its actual eligible evaluation clock")
        elif self.fired_at is not None:
            raise ValueError("An unfired review cannot claim a firing clock")
        if self.status == "CANCELLED" and self.cancelled_by is None:
            raise ValueError("Cancelled review requires its actual cancellation cause")
        if self.status == "EXPIRED" and (
            self.last_evaluated_at is None or parse_iso_time(self.last_evaluated_at) < expiry
        ):
            raise ValueError("Expired review requires its actual expiry evaluation clock")
        return self


class TimedReviewEventState(DomainModel):
    kind: Literal["PRESENT"]
    event_id: str = Field(min_length=1, pattern=r"\S")
    lifecycle: Literal[
        "DETECTED",
        "SCORED",
        "TRACKED",
        "ANALYZING",
        "ALERTED",
        "MONITORING",
        "COOLDOWN",
        "INVALIDATED",
        "CLOSED",
        "MERGED",
    ]


class MissingTimedReviewEvent(DomainModel):
    kind: Literal["MISSING"]
    event_id: str = Field(min_length=1, pattern=r"\S")


class UnloadedExpiredReviewEvent(DomainModel):
    kind: Literal["NOT_LOADED"]
    event_id: str = Field(min_length=1, pattern=r"\S")
    reason: Literal["REVIEW_EXPIRED"]


type TimedReviewEvent = TimedReviewEventState | MissingTimedReviewEvent | UnloadedExpiredReviewEvent


class PendingTimedReview(DomainModel):
    status: Literal["ARMED"]
    reason: Literal["NOT_DUE"]


class FireTimedReview(DomainModel):
    status: Literal["FIRED"]
    reason: Literal["REVIEW_DUE"]


class ExpireTimedReview(DomainModel):
    status: Literal["EXPIRED"]
    reason: Literal["EXPIRY_REACHED"]


class CancelTimedReview(DomainModel):
    status: Literal["CANCELLED"]
    reason: Literal["EVENT_MISSING", "EVENT_TERMINAL"]


type TimedReviewAction = PendingTimedReview | FireTimedReview | ExpireTimedReview | CancelTimedReview


type StoredWatchCondition = WatchCondition | TimedReviewCondition

_WATCH_RECORDS: TypeAdapter[StoredWatchCondition] = TypeAdapter(
    WatchCondition | TimedReviewCondition,
    config=ConfigDict(hide_input_in_errors=True),
)


def parse_watch_condition(value: Mapping[str, JsonValue]) -> StoredWatchCondition:
    return _WATCH_RECORDS.validate_python(value, strict=True)


def encode_watch_condition(value: StoredWatchCondition) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    result = decode_json(validated.model_dump_json(exclude_unset=True, warnings="error"))
    if not isinstance(result, dict):
        raise TypeError("A watch condition must serialize as an object")
    return result


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
    def exact_price_from_storage(cls, value: JsonValue | Decimal) -> Decimal:
        """Decode exact decimal text at the durable call boundary, preserving financial units."""
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise ValueError("call price requires a decimal number")
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError("call price requires a decimal number") from exc


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


class WatchRule(DomainModel):
    role: Literal["UPGRADE", "DOWNGRADE", "INVALIDATION"]
    human_text: str
    expr: WatchExpression


class WatchProposal(WatchRule):
    cond_id: str = Field(min_length=1)
    status: Literal["ARMED"] = "ARMED"
    milestone_id: str | None = None


class WatchReferencePrices(DomainModel):
    anchor_price: Decimal | None
    entry_price: Decimal | None
    invalidation_price: Decimal | None

    @field_validator("anchor_price", "entry_price", "invalidation_price", mode="before")
    @classmethod
    def exact_price(cls, value: JsonValue | Decimal) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise ValueError("reference price requires a decimal number")
        try:
            price = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError("reference price requires a decimal number") from exc
        if not price.is_finite() or price <= 0:
            raise ValueError("reference prices must be finite and positive")
        return price


class WatchPolicyInputs(DomainModel):
    action: Literal["START", "WATCH_TO_START", "ADD", "HOLD", "WAIT", "DONT_CHASE", "EXIT", "REDUCE"]
    move: Literal[
        "SPOT_LED_ACCUMULATION",
        "ORGANIC_MIXED",
        "DISTRIBUTION",
        "SELL_SIDE_ABSORPTION",
        "PASSIVE_SPOT_ABSORPTION",
        "REAL_MARKET_FRAGMENTATION",
        "PRICE_MARKUP_FAKE_OI",
        "SHORT_SQUEEZE",
        "PERP_LED_CHASE",
        "LIQUIDITY_VACUUM",
        "UNKNOWN",
    ]
    band: Literal["NEGATIVE", "UNPRICED", "EARLY", "SUBSTANTIAL", "HEAVY", "UNKNOWN"]
    prices: WatchReferencePrices
    has_open_call: bool
    wait_data: bool

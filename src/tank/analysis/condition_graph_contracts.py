"""Condition-query evidence independent of orchestration and cached wire documents."""

from datetime import datetime, timedelta
from decimal import Decimal, localcontext
from math import isfinite
from statistics import median
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, field_validator, model_validator

from tank.analysis.execution import BookWalkRequest, BookWalkResult, execute_book_walk
from tank.analysis.positioning import MissingOIDecomposition, OIDecomposition, oi_class
from tank.analysis.price_reference import ReferencePriceReturn
from tank.analysis.statistics_contracts import (
    BetaEstimate,
    DecimalRange,
    FloatBounds,
    FloatRange,
    HistoryDiagnostic,
    HistoryDiagnostics,
    UnavailableBeta,
)
from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.registry import FundamentalsEvidence, LendingPool
from tank.contracts.scalars import TrueFlag, require_integer, require_number
from tank.market.book_contracts import BookLevel
from tank.market.trade_contracts import QuoteUsdValuation
from tank.time_precision import Timestamp
from tank.util import parse_epoch, parse_iso_time

type ConditionConfidenceLevel = Literal["LOW", "MEDIUM", "HIGH"]

type ConditionWindow = Literal["W15M", "W1H", "W4H", "W24H", "EVENT"]
type ConditionMetric = Literal[
    "spot.cohort.direction",
    "spot.cohort.cwb_buy",
    "spot.cohort.cwb_sell",
    "spot.classification",
    "deriv.native_oi_chg_pct",
    "deriv.native_oi_vs_normal",
    "deriv.oi_class",
    "deriv.funding_vs_normal",
    "price.close_vs_level",
    "price.repricing_band",
    "price.vs_anchor_pct",
    "dc.class",
    "liq.depth_p1_vs_pre",
    "liq.spread_ratio",
    "mt.move_class",
    "mt.divergence_state",
]


class ConditionRequirement(DomainModel):
    metric: ConditionMetric
    window: ConditionWindow


class ConditionInterval(DomainModel):
    window: ConditionWindow
    start: datetime
    cutoff: datetime

    @field_validator("start", "cutoff")
    @classmethod
    def aware_clock(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def forward_interval(self) -> Self:
        if self.start >= self.cutoff:
            raise ValueError("Observed condition intervals require a positive duration")
        return self


class NonForwardConditionInterval(DomainModel):
    start: datetime
    cutoff: datetime

    @field_validator("start", "cutoff")
    @classmethod
    def aware_clock(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_non_forward_bounds(self) -> Self:
        if self.start < self.cutoff:
            raise ValueError("A non-forward condition interval must preserve its actual invalid bounds")
        return self


class ObservedConditionClose(DomainModel):
    status: Literal["OBSERVED"]
    reference_instrument_id: str = Field(min_length=1, pattern=r"\S")
    price: Decimal = Field(gt=0)
    observed_at: Timestamp


class UnavailableConditionClose(DomainModel):
    status: Literal["UNAVAILABLE"]
    reference_instrument_id: str | None = Field(min_length=1, pattern=r"\S")
    price: None
    observed_at: Timestamp
    reason: Literal["INCOMPLETE"]

    @model_validator(mode="after")
    def missing_price_has_no_observed_close(self) -> Self:
        if self.observed_at.value is not None:
            raise ValueError("An unavailable hourly close cannot claim an observed close clock")
        return self


type ConditionClose = ObservedConditionClose | UnavailableConditionClose


class ObservedConditionRatio(DomainModel):
    status: Literal["OBSERVED"]
    value: float = Field(ge=0)


class UnavailableConditionRatio(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["INCOMPLETE", "ZERO_BASELINE"]


type ConditionRatio = ObservedConditionRatio | UnavailableConditionRatio


class ConditionDepthValues(DomainModel):
    aggregate_depth_p1_usd: Decimal | None = Field(ge=0)
    pre_event_depth_p1_usd: Decimal | None = Field(ge=0)
    spread_ratio: float | None = Field(ge=0)

    def comparison(self) -> ConditionRatio:
        values = type(self).model_validate(self)
        if values.aggregate_depth_p1_usd is None or values.pre_event_depth_p1_usd is None:
            return UnavailableConditionRatio(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
        if values.pre_event_depth_p1_usd == 0:
            return UnavailableConditionRatio(status="UNAVAILABLE", value=None, reason="ZERO_BASELINE")
        with localcontext() as context:
            context.prec = max(128, context.prec)
            ratio = float(values.aggregate_depth_p1_usd / values.pre_event_depth_p1_usd)
        return ObservedConditionRatio(status="OBSERVED", value=ratio)


type ConditionCollection = Literal[
    "flow_bars", "klines", "books", "oi", "funding", "liquidations", "tickers", "supply"
]
type SupplyObservationKind = Literal[
    "EXCHANGE_INVENTORY",
    "WALLET_FLOWS",
    "UNLOCKS",
    "LENDING",
    "CHAIN_LIQUIDITY",
    "STABLECOIN_CHAIN",
    "FUNDAMENTALS",
]


class CollectionCadenceScope(DomainModel):
    kind: Literal["COLLECTION"]
    collection: ConditionCollection
    token_id: str | None = Field(min_length=1, pattern=r"\S")


class InstrumentCadenceScope(DomainModel):
    kind: Literal["INSTRUMENT"]
    collection: ConditionCollection
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")


class MetricCadenceScope(DomainModel):
    kind: Literal["METRIC"]
    collection: ConditionCollection
    token_id: str = Field(min_length=1, pattern=r"\S")
    metric_key: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str | None = Field(min_length=1, pattern=r"\S")


class SupplyCadenceScope(DomainModel):
    kind: Literal["SUPPLY"]
    collection: Literal["supply"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    observation_kind: SupplyObservationKind
    chain: str | None = Field(min_length=1, pattern=r"\S")


type ConditionCadenceScope = (
    CollectionCadenceScope | InstrumentCadenceScope | MetricCadenceScope | SupplyCadenceScope
)


def _bind_cadence_scope(
    scope: ConditionCadenceScope,
    *,
    collection: ConditionCollection,
    token_id: str,
    instrument_id: str | None,
    metric_key: str | None,
    observation_kind: str | None,
    chain: str | None,
) -> None:
    if scope.collection != collection:
        raise ValueError("Cadence declaration belongs to another collection")
    if scope.token_id is not None and scope.token_id != token_id:
        raise ValueError("Cadence declaration belongs to another token")
    if isinstance(scope, InstrumentCadenceScope) and scope.instrument_id != instrument_id:
        raise ValueError("Cadence declaration belongs to another instrument")
    if isinstance(scope, MetricCadenceScope):
        if scope.metric_key != metric_key:
            raise ValueError("Cadence declaration belongs to another metric")
        if scope.instrument_id is not None and scope.instrument_id != instrument_id:
            raise ValueError("Metric cadence declaration belongs to another instrument")
    if isinstance(scope, SupplyCadenceScope):
        if scope.observation_kind != observation_kind:
            raise ValueError("Cadence declaration belongs to another supply kind")
        if scope.chain is not None and scope.chain != chain:
            raise ValueError("Cadence declaration belongs to another supply chain")


class KnownCadenceIdentity(DomainModel):
    status: Literal["RECORDED"]
    versions: ResolvedVersionIdentity


class UncapturedCadenceIdentity(DomainModel):
    status: Literal["UNRECORDED"]
    reason: Literal["PLAN_IDENTITY_NOT_CAPTURED", "DECLARATION_IDENTITY_NOT_CAPTURED"]


type CadenceDeclarationIdentity = KnownCadenceIdentity | UncapturedCadenceIdentity


class DeclaredMetadataCadenceOrigin(DomainModel):
    kind: Literal["DECLARED_METADATA"]
    selector: str = Field(min_length=1, pattern=r"\S")
    declaration_identity: CadenceDeclarationIdentity


class CapturedPlanCadenceOrigin(DomainModel):
    kind: Literal["CAPTURED_PLAN"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    profile_field: Literal["kline_s", "book_s", "oi_funding_s", "supply_by_kind"]
    transition_id: str = Field(min_length=1, pattern=r"\S")
    effective_at: Timestamp
    tier: Literal["BASE", "HOT"]
    plan_identity: CadenceDeclarationIdentity

    @model_validator(mode="after")
    def real_transition(self) -> Self:
        if self.effective_at.value is None:
            raise ValueError("A captured cadence plan requires its actual effective transition clock")
        return self


class CurrentPlanCadenceOrigin(DomainModel):
    kind: Literal["CURRENT_CONFIGURED_PLAN"]
    versions: ResolvedVersionIdentity
    rule: Literal[
        "book_interval_s",
        "base_kline_interval_s",
        "supply_capture_schedule",
        "shared_socket_health_interval_s",
    ]


class MetricSemanticsCadenceOrigin(DomainModel):
    kind: Literal["SCOPED_METRIC_SEMANTICS"]


class ClosedBarCadenceOrigin(DomainModel):
    kind: Literal["CLOSED_BAR_DURATION"]
    interval_s: float = Field(gt=0)


class SettlementCadenceOrigin(DomainModel):
    kind: Literal["SETTLEMENT_INTERVAL"]
    interval_h: float = Field(gt=0)


class SourceIntervalCadenceOrigin(DomainModel):
    kind: Literal["SOURCE_INTERVAL_PLUS_PUBLISH_LAG"]
    interval_s: float = Field(gt=0)
    publish_lag_s: float | None = Field(ge=0)


type ConditionCadenceOrigin = (
    DeclaredMetadataCadenceOrigin
    | CapturedPlanCadenceOrigin
    | CurrentPlanCadenceOrigin
    | MetricSemanticsCadenceOrigin
    | ClosedBarCadenceOrigin
    | SettlementCadenceOrigin
    | SourceIntervalCadenceOrigin
)


class DeclaredConditionCadence(DomainModel):
    status: Literal["DECLARED"]
    seconds: float = Field(gt=0)
    scope: ConditionCadenceScope
    origin: ConditionCadenceOrigin

    @model_validator(mode="after")
    def qualified_allowance(self) -> Self:
        origin = self.origin
        if isinstance(origin, CapturedPlanCadenceOrigin):
            if self.scope.token_id != origin.token_id:
                raise ValueError("A captured cadence transition cannot qualify another token")
            allowed: dict[str, tuple[ConditionCollection, ...]] = {
                "kline_s": ("klines", "tickers"),
                "book_s": ("books",),
                "oi_funding_s": ("oi", "funding"),
                "supply_by_kind": ("supply",),
            }
            if self.scope.collection not in allowed[origin.profile_field]:
                raise ValueError("Captured cadence profile field disagrees with its collection")
            if origin.profile_field == "supply_by_kind" and not isinstance(self.scope, SupplyCadenceScope):
                raise ValueError("A captured supply cadence requires its actual observation kind")
        if isinstance(origin, CurrentPlanCadenceOrigin):
            rules: dict[str, tuple[ConditionCollection, ...]] = {
                "book_interval_s": ("books",),
                "base_kline_interval_s": ("klines", "tickers"),
                "supply_capture_schedule": ("supply",),
                "shared_socket_health_interval_s": ("liquidations",),
            }
            if self.scope.collection not in rules[origin.rule]:
                raise ValueError("Current configured cadence rule disagrees with its actual collection")
            if origin.rule == "supply_capture_schedule" and not isinstance(self.scope, SupplyCadenceScope):
                raise ValueError("A current supply cadence requires its actual observation kind")
        if isinstance(origin, MetricSemanticsCadenceOrigin) and not isinstance(
            self.scope, MetricCadenceScope
        ):
            raise ValueError("Metric cadence requires the actual declared metric scope")
        if isinstance(origin, ClosedBarCadenceOrigin) and (
            self.scope.collection not in {"flow_bars", "klines"} or self.seconds != origin.interval_s
        ):
            raise ValueError("Closed-bar cadence must preserve its actual collection and duration")
        if isinstance(origin, SettlementCadenceOrigin) and (
            self.scope.collection != "funding" or self.seconds != origin.interval_h * 3600
        ):
            raise ValueError("Settlement cadence must preserve its declared funding interval")
        if isinstance(origin, SourceIntervalCadenceOrigin):
            if origin.publish_lag_s is None:
                raise ValueError("An undeclared publication lag cannot become a known cadence allowance")
            if self.seconds != 2 * origin.interval_s + origin.publish_lag_s:
                raise ValueError("Source-interval allowance must include its actual declared publication lag")
        return self


class UnavailableConditionCadence(DomainModel):
    status: Literal["UNAVAILABLE"]
    seconds: None
    scope: ConditionCadenceScope
    origin: ConditionCadenceOrigin | None
    reason: Literal[
        "CADENCE_NOT_CAPTURED",
        "CAPTURE_TIER_HISTORY_UNAVAILABLE",
        "SCOPE_NOT_ESTABLISHED",
        "POLICY_IDENTITY_NOT_CAPTURED",
        "PUBLISH_LAG_NOT_CAPTURED",
    ]

    @model_validator(mode="after")
    def preserve_known_declarations(self) -> Self:
        if self.reason == "PUBLISH_LAG_NOT_CAPTURED" and (
            not isinstance(self.origin, SourceIntervalCadenceOrigin) or self.origin.publish_lag_s is not None
        ):
            raise ValueError("Missing publication lag must preserve the known source interval")
        return self


type ConditionCadence = DeclaredConditionCadence | UnavailableConditionCadence


class ConditionStreamFreshness(DomainModel):
    basis: Literal["STREAM_WATERMARK", "LAST_EVENT"]
    coverage_status: Literal["CONNECTED", "DISCONNECTED", "NOT_SUPPORTED"] | None
    detail: Literal["STREAM_CONNECTION_UNPROVEN"] | None


class ConditionFieldObservation(DomainModel):
    collection: ConditionCollection
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str | None = Field(min_length=1, pattern=r"\S")
    metric_key: str | None = Field(min_length=1, pattern=r"\S")
    observation_kind: str | None = Field(min_length=1, pattern=r"\S")
    chain: str | None = Field(min_length=1, pattern=r"\S")
    observed_at: Timestamp
    cadence: ConditionCadence
    age_s: float
    fresh: bool
    reason: Literal["INCOMPLETE", "NOT_ESTIMABLE"] | None
    stream: ConditionStreamFreshness | None

    @model_validator(mode="after")
    def honest_freshness(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("An observed field requires its actual reported clock")
        _bind_cadence_scope(
            self.cadence.scope,
            collection=self.collection,
            token_id=self.token_id,
            instrument_id=self.instrument_id,
            metric_key=self.metric_key,
            observation_kind=self.observation_kind,
            chain=self.chain,
        )
        if isinstance(self.cadence, DeclaredConditionCadence):
            expected = 0 <= self.age_s <= self.cadence.seconds
            expected_reason: Literal["INCOMPLETE", "NOT_ESTIMABLE"] | None = (
                None if expected else "INCOMPLETE"
            )
        else:
            expected = False
            expected_reason = "NOT_ESTIMABLE"
        if self.fresh != expected or self.reason != expected_reason:
            raise ValueError("Freshness must preserve the actual age and declared cadence allowance")
        if (self.collection == "liquidations") != (self.stream is not None):
            raise ValueError("Liquidation freshness requires its actual stream observation basis")
        return self


class ConditionDepthObservation(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    metric_key: str | None = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    observed_at: Timestamp
    cadence: ConditionCadence
    fresh: bool

    @model_validator(mode="after")
    def actual_depth_observation(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("A selected book requires its actual observation clock")
        _bind_cadence_scope(
            self.cadence.scope,
            collection="books",
            token_id=self.token_id,
            instrument_id=self.instrument_id,
            metric_key=self.metric_key,
            observation_kind=None,
            chain=None,
        )
        return self


class ConditionDepthNode(ConditionDepthValues):
    cutoff: datetime
    observations: tuple[ConditionDepthObservation, ...]

    @field_validator("cutoff")
    @classmethod
    def aware_cutoff(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def bound_observations(self) -> Self:
        venues = [value.venue for value in self.observations]
        if len(venues) != len(set(venues)):
            raise ValueError("A depth cache node cannot repeat a venue observation")
        for observation in self.observations:
            stamp = observation.observed_at.value
            if stamp is None:
                raise ValueError("A selected book requires its actual observation clock")
            age = (self.cutoff - parse_iso_time(stamp)).total_seconds()
            cadence = observation.cadence
            fresh = isinstance(cadence, DeclaredConditionCadence) and 0 <= age <= cadence.seconds
            if observation.fresh != fresh:
                raise ValueError("Book freshness must agree with its actual cutoff and cadence")
        return self

    def values(self) -> ConditionDepthValues:
        return ConditionDepthValues(
            aggregate_depth_p1_usd=self.aggregate_depth_p1_usd,
            pre_event_depth_p1_usd=self.pre_event_depth_p1_usd,
            spread_ratio=self.spread_ratio,
        )


class ConditionCoverageFailure(DomainModel):
    kind: Literal["COVERAGE"]
    condition: Literal["spot_coverage", "derivatives_coverage"]
    reason: Literal["INCOMPLETE"]


class ConditionRequiredFieldFailure(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    metric_key: str | None = Field(min_length=1, pattern=r"\S")
    kind: Literal["REQUIRED_FIELD"]
    condition: Literal["freshness"]
    reason: Literal["INCOMPLETE"]
    collection: ConditionCollection
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    metric_family: Literal["aggressor", "depth", "price", "oi", "funding"]
    cadence: ConditionCadence

    @model_validator(mode="after")
    def scoped_requirement(self) -> Self:
        _bind_cadence_scope(
            self.cadence.scope,
            collection=self.collection,
            token_id=self.token_id,
            instrument_id=self.instrument_id,
            metric_key=self.metric_key,
            observation_kind=None,
            chain=None,
        )
        return self


class ConditionObservedFieldFailure(DomainModel):
    kind: Literal["OBSERVED_FIELD"]
    condition: Literal["freshness"]
    observation: ConditionFieldObservation

    @model_validator(mode="after")
    def actual_failed_observation(self) -> Self:
        if self.observation.fresh:
            raise ValueError("A fresh observation cannot be a confidence failure")
        return self


type ConditionConfidenceFailure = (
    ConditionCoverageFailure | ConditionRequiredFieldFailure | ConditionObservedFieldFailure
)


class ConditionConfidenceBase(DomainModel):
    classification: ConditionConfidenceLevel
    failed: tuple[ConditionConfidenceFailure, ...]
    limiting: ConditionConfidenceFailure | None
    derivatives_native_coverage_ready: bool
    freshness: tuple[ConditionFieldObservation, ...]

    @model_validator(mode="after")
    def actual_limiting_input(self) -> Self:
        if self.classification == "HIGH":
            if self.failed or self.limiting is not None:
                raise ValueError("HIGH data confidence cannot retain a failed limiting input")
        elif not self.failed or self.limiting != self.failed[0]:
            raise ValueError("Reduced data confidence requires its actual first failed input")
        return self


class FullConditionConfidence(ConditionConfidenceBase):
    kind: Literal["FULL"]
    derivatives_classification_ready: bool


class SelectiveConditionConfidence(ConditionConfidenceBase):
    kind: Literal["SELECTIVE"]
    derivatives_classification_ready: None
    derivatives_classification_ready_reason: Literal["NOT_ESTIMABLE"]
    analysis_window: ConditionInterval


type ConditionConfidence = FullConditionConfidence | SelectiveConditionConfidence


class ObservedConditionTradeCounts(DomainModel):
    status: Literal["OBSERVED"]
    n_buy: int = Field(ge=0)
    n_sell: int = Field(ge=0)

    def total(self) -> int:
        values = type(self).model_validate(self)
        return values.n_buy + values.n_sell


class IncompleteConditionTradeCounts(DomainModel):
    status: Literal["UNAVAILABLE"]
    n_buy: int | None = Field(ge=0)
    n_sell: int | None = Field(ge=0)
    reason: Literal["INCOMPLETE"]

    @model_validator(mode="after")
    def actual_missing_count(self) -> Self:
        if self.n_buy is not None and self.n_sell is not None:
            raise ValueError("Incomplete trade counts require an actually missing side count")
        return self

    def total(self) -> None:
        type(self).model_validate(self)
        return None


type ConditionTradeCounts = ObservedConditionTradeCounts | IncompleteConditionTradeCounts


class FlowAggregateEvidenceRef(DomainModel):
    projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ConditionFlowAggregate(DomainModel):
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    start: datetime
    end: datetime
    complete: TrueFlag
    buy_qty: Decimal = Field(ge=0)
    sell_qty: Decimal = Field(ge=0)
    sum_sq_qty: Decimal = Field(ge=0)
    buy_notional: Decimal | None = Field(ge=0)
    sell_notional: Decimal | None = Field(ge=0)
    notional_reason: Literal["INCOMPLETE"] | None
    low: Decimal | None = Field(gt=0)
    low_reason: Literal["INCOMPLETE", "EMPTY_INTERVAL"] | None
    counts: ConditionTradeCounts
    evidence: FlowAggregateEvidenceRef

    @field_validator("start", "end")
    @classmethod
    def aware_bounds(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_calculation_facets(self) -> Self:
        if self.start >= self.end:
            raise ValueError("A selected aggregate requires its actual positive interval")
        missing_usd = self.buy_notional is None or self.sell_notional is None
        if missing_usd != (self.notional_reason == "INCOMPLETE"):
            raise ValueError("Flow notional absence requires its actual incomplete declaration")
        if (self.low is None) != (self.low_reason is not None):
            raise ValueError("Missing boundary price requires its explicit reason")
        if self.low_reason == "EMPTY_INTERVAL" and not (
            isinstance(self.counts, ObservedConditionTradeCounts) and self.counts.total() == 0
        ):
            raise ValueError("An empty-interval price reason requires observed zero trade counts")
        for count, quantity in ((self.counts.n_buy, self.buy_qty), (self.counts.n_sell, self.sell_qty)):
            if count is not None and (count == 0) != (quantity == 0):
                raise ValueError("Observed side counts disagree with their actual native quantity")
        volume = self.buy_qty + self.sell_qty
        if (volume == 0) != (self.sum_sq_qty == 0):
            raise ValueError("Flow squared quantity cannot invent or erase observed native flow")
        return self


class KnownConditionTradeCount(DomainModel):
    status: Literal["OBSERVED"]
    value: int = Field(ge=0)


class UnavailableConditionTradeCount(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["INCOMPLETE"]


type ConditionTradeCount = KnownConditionTradeCount | UnavailableConditionTradeCount


class ConditionBoundaryGroup(DomainModel):
    start: datetime
    cutoff: datetime
    aggregates: tuple[ConditionFlowAggregate, ...] = Field(min_length=1)

    @field_validator("start", "cutoff")
    @classmethod
    def aware_bounds(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def contained_pieces(self) -> Self:
        if self.start >= self.cutoff:
            raise ValueError("A boundary proof requires its actual positive interval")
        for aggregate in self.aggregates:
            if not self.start <= aggregate.start < aggregate.end <= self.cutoff:
                raise ValueError("A boundary aggregate falls outside its declared proof interval")
        return self

    def trade_count(self) -> ConditionTradeCount:
        group = type(self).model_validate(self)
        counts = [aggregate.counts.total() for aggregate in group.aggregates]
        if any(count is None for count in counts):
            return UnavailableConditionTradeCount(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
        total = 0
        for count in counts:
            if count is None:
                raise ValueError("Unknown trade counts cannot become zero")
            total += count
        return KnownConditionTradeCount(status="OBSERVED", value=total)


class ObservedConditionComposite(DomainModel):
    status: Literal["OBSERVED"]
    cvd: Decimal
    volume: Decimal = Field(ge=0)
    buy_share: float | None = Field(ge=0, le=1)
    bs_ratio: float | None = Field(ge=0)
    venues: tuple[str, ...] = Field(min_length=1)
    sign: Annotated[Literal[-1, 0, 1], BeforeValidator(require_integer)]

    null_reasons: dict[Literal["buy_share", "bs_ratio"], Literal["ZERO_TOTAL_VOLUME", "ZERO_SELL_QUANTITY"]]

    @model_validator(mode="after")
    def actual_native_direction(self) -> Self:
        _statistical_nulls(
            (("buy_share", self.buy_share), ("bs_ratio", self.bs_ratio)), tuple(self.null_reasons.items())
        )
        if len(self.venues) != len(set(self.venues)) or any(not venue.strip() for venue in self.venues):
            raise ValueError("A composite requires distinct actual venue identities")
        if self.cvd.copy_abs() > self.volume:
            raise ValueError("Native net flow cannot exceed total observed native volume")
        if self.volume == 0 and (self.buy_share is not None or self.bs_ratio is not None):
            raise ValueError("Zero-volume flow ratios are undefined, not observed zero fractions")
        if self.volume > 0 and self.buy_share is None:
            raise ValueError("Observed positive volume requires its actual buy share")
        if self.sign == 1 and self.cvd <= 0 or self.sign == -1 and self.cvd >= 0:
            raise ValueError("Significant flow direction contradicts the actual net native quantity")
        return self


class UnavailableConditionComposite(DomainModel):
    status: Literal["UNAVAILABLE"]
    cvd: None
    volume: None
    buy_share: None
    bs_ratio: None
    venues: tuple[()]
    sign: None
    reason: Literal["INCOMPLETE"]


type ConditionComposite = ObservedConditionComposite | UnavailableConditionComposite


type ConditionStatisticalNullReason = Literal[
    "INCOMPLETE",
    "NO_COMPLETE_VENUES",
    "ZERO_TOTAL_VOLUME",
    "ZERO_SELL_QUANTITY",
    "ZERO_DIRECTIONAL_CONTRIBUTION",
    "ZERO_OTHER_VOLUME",
    "NOT_ESTIMABLE",
]


def _statistical_nulls(
    values: tuple[tuple[str, Decimal | float | int | bool | str | None], ...],
    reasons: tuple[tuple[str, ConditionStatisticalNullReason], ...],
) -> None:
    missing = {name for name, value in values if value is None}
    declared = {name for name, _ in reasons}
    if missing != declared:
        raise ValueError("Statistical absence reasons must match exactly the unavailable fields")


class ConditionSpotBreadth(DomainModel):
    n_pos: int = Field(ge=0)
    n_neg: int = Field(ge=0)
    n_neutral: int = Field(ge=0)
    pct_pos: float | None = Field(ge=0, le=1)
    pct_neg: float | None = Field(ge=0, le=1)
    null_reasons: dict[Literal["pct_pos", "pct_neg"], ConditionStatisticalNullReason]

    @model_validator(mode="after")
    def defined_counts(self) -> Self:
        _statistical_nulls(
            (("pct_pos", self.pct_pos), ("pct_neg", self.pct_neg)), tuple(self.null_reasons.items())
        )
        total = self.n_pos + self.n_neg + self.n_neutral
        if (total == 0) != (self.pct_pos is None and self.pct_neg is None):
            raise ValueError("Venue breadth percentages must preserve their actual denominator")
        return self


class ConditionWeightedBreadth(DomainModel):
    buy: float | None = Field(ge=0, le=1)
    sell: float | None = Field(ge=0, le=1)
    neutral: float | None = Field(ge=0, le=1)
    null_reasons: dict[Literal["buy", "sell", "neutral"], ConditionStatisticalNullReason]

    @model_validator(mode="after")
    def common_denominator(self) -> Self:
        _statistical_nulls(
            (("buy", self.buy), ("sell", self.sell), ("neutral", self.neutral)),
            tuple(self.null_reasons.items()),
        )
        missing = (self.buy is None, self.sell is None, self.neutral is None)
        if any(missing) and not all(missing):
            raise ValueError("Weighted breadth sides must share the same actual volume denominator")
        return self


class ConditionSpotMajority(DomainModel):
    buy: bool | None
    sell: bool | None
    neutral: bool | None
    covered: bool | None
    null_reasons: dict[Literal["buy", "sell", "neutral", "covered"], ConditionStatisticalNullReason]

    @model_validator(mode="after")
    def explicit_unknown_predicates(self) -> Self:
        _statistical_nulls(
            (("buy", self.buy), ("sell", self.sell), ("neutral", self.neutral), ("covered", self.covered)),
            tuple(self.null_reasons.items()),
        )
        return self


class ConditionSpotTopN(DomainModel):
    n: int = Field(ge=0)
    venues: tuple[str, ...]
    agreement: float | None = Field(ge=0, le=1)
    agreement_reason: Literal["NO_COMPLETE_VENUES"] | None

    @model_validator(mode="after")
    def real_top_set(self) -> Self:
        if self.n != len(self.venues) or len(self.venues) != len(set(self.venues)):
            raise ValueError("Top-venue count must match its actual distinct venue set")
        if (self.agreement is None) != (self.agreement_reason is not None):
            raise ValueError("Missing top-venue agreement requires its actual absence reason")
        if (self.n == 0) != (self.agreement is None):
            raise ValueError("Top-venue agreement must preserve its actual count denominator")
        return self


class ConditionSpotConcentration(DomainModel):
    hhi: float | None = Field(ge=0, le=1)
    top_volume_venue: str | None = Field(min_length=1, pattern=r"\S")
    top_volume_share: float | None = Field(ge=0, le=1)
    top_contrib_venue: str | None = Field(min_length=1, pattern=r"\S")
    top_contrib_share: float | None = Field(ge=0, le=1)
    others_cwb_dir: float | None = Field(ge=0, le=1)
    null_reasons: dict[
        Literal[
            "hhi",
            "top_volume_venue",
            "top_volume_share",
            "top_contrib_venue",
            "top_contrib_share",
            "others_cwb_dir",
        ],
        ConditionStatisticalNullReason,
    ]

    @model_validator(mode="after")
    def known_concentration_facets(self) -> Self:
        _statistical_nulls(
            (
                ("hhi", self.hhi),
                ("top_volume_venue", self.top_volume_venue),
                ("top_volume_share", self.top_volume_share),
                ("top_contrib_venue", self.top_contrib_venue),
                ("top_contrib_share", self.top_contrib_share),
                ("others_cwb_dir", self.others_cwb_dir),
            ),
            tuple(self.null_reasons.items()),
        )
        return self


class ConditionIncompleteMinuteCoverage(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    reason: Literal["INCOMPLETE"]
    detail: Literal["FULL_MINUTE_COVERAGE_INCOMPLETE"]
    source_reasons: tuple[str, ...]


class ConditionUnavailableBoundaryCoverage(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    reason: Literal["INCOMPLETE"]
    detail: Literal["BOUNDARY_AGGREGATE_UNAVAILABLE"]
    source_reasons: tuple[str, ...]


class ConditionMissingFlowMetrics(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    reason: Literal["INCOMPLETE"]
    detail: Literal["FLOW_AGGREGATE_FIELDS_MISSING"]
    missing_metrics: tuple[Literal["buy_qty", "sell_qty", "sum_sq_qty"], ...] = Field(min_length=1)


type ConditionSpotExclusion = (
    ConditionIncompleteMinuteCoverage | ConditionUnavailableBoundaryCoverage | ConditionMissingFlowMetrics
)


class ConditionSpotCoverage(DomainModel):
    covered_share_est: float | None = Field(ge=0, le=1)
    exact_majority: bool | None
    n_complete: int = Field(ge=0)
    excluded: tuple[ConditionSpotExclusion, ...]
    null_reasons: dict[Literal["covered_share_est", "exact_majority"], ConditionStatisticalNullReason]

    @model_validator(mode="after")
    def real_coverage(self) -> Self:
        _statistical_nulls(
            (("covered_share_est", self.covered_share_est), ("exact_majority", self.exact_majority)),
            tuple(self.null_reasons.items()),
        )
        venues = [row.venue for row in self.excluded]
        if len(venues) != len(set(venues)):
            raise ValueError("Excluded spot venues must be distinct")
        return self


class ConditionSpotVenue(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    complete: TrueFlag
    volume_native: Decimal = Field(ge=0)
    buy_qty: Decimal = Field(ge=0)
    sell_qty: Decimal = Field(ge=0)
    cvd: Decimal
    buy_share: float | None = Field(ge=0, le=1)
    bs_ratio: float | None = Field(ge=0)
    sign: Annotated[Literal[-1, 0, 1], BeforeValidator(require_integer)]
    sum_sq_qty: Decimal = Field(ge=0)
    boundary_segments: tuple[ConditionBoundaryGroup, ...]
    volume_share: float | None = Field(ge=0, le=1)
    null_reasons: dict[Literal["buy_share", "bs_ratio", "volume_share"], ConditionStatisticalNullReason]

    @model_validator(mode="after")
    def actual_native_totals(self) -> Self:
        _statistical_nulls(
            (("buy_share", self.buy_share), ("bs_ratio", self.bs_ratio), ("volume_share", self.volume_share)),
            tuple(self.null_reasons.items()),
        )
        if self.volume_native != _sum_native(self.buy_qty, self.sell_qty) or self.cvd != _sum_native(
            self.buy_qty, self.sell_qty.copy_negate()
        ):
            raise ValueError("Spot totals must preserve the actual native buy and sell quantities")
        if (self.volume_native == 0) != (self.sum_sq_qty == 0):
            raise ValueError("Spot squared quantity cannot erase or invent observed native flow")
        if self.sign == 1 and self.cvd <= 0 or self.sign == -1 and self.cvd >= 0:
            raise ValueError("Spot direction contradicts its actual native net flow")
        return self


def _sum_native(left: Decimal, right: Decimal) -> Decimal:
    exponents: list[int] = []
    for value in (left, right):
        exponent = value.as_tuple().exponent
        if not isinstance(exponent, int):
            raise ValueError("Native arithmetic requires finite Decimal inputs")
        exponents.append(exponent)
    span = max(left.adjusted(), right.adjusted()) - min(exponents) + 1
    with localcontext() as context:
        context.prec = max(128, 2 * span + 32)
        return left + right


class ConditionSpotValues(DomainModel):
    venues: tuple[ConditionSpotVenue, ...]
    composite_max: ConditionComposite
    breadth: ConditionSpotBreadth
    cw_breadth: ConditionWeightedBreadth
    exact_majority: ConditionSpotMajority
    topn: ConditionSpotTopN
    concentration: ConditionSpotConcentration
    direction: Literal["BUY", "SELL", "NEUTRAL"] | None
    direction_reason: Literal["NO_COMPLETE_VENUES"] | None
    classification: Literal["INSUFFICIENT_DATA", "CONCENTRATED", "BROAD", "DIVERGENT"]
    coverage: ConditionSpotCoverage

    @model_validator(mode="after")
    def actual_spot_cohort(self) -> Self:
        names = tuple(venue.venue for venue in self.venues)
        if len(names) != len(set(names)):
            raise ValueError("A spot graph cannot repeat an observed venue")
        if self.coverage.n_complete != len(names):
            raise ValueError("Complete spot coverage count must match its actual venue observations")
        if set(names) & {venue.venue for venue in self.coverage.excluded}:
            raise ValueError("A spot venue cannot be both complete and excluded")
        if (
            self.breadth.n_pos != sum(venue.sign == 1 for venue in self.venues)
            or self.breadth.n_neg != sum(venue.sign == -1 for venue in self.venues)
            or self.breadth.n_neutral != sum(venue.sign == 0 for venue in self.venues)
        ):
            raise ValueError("Spot breadth counts must match the actual venue directions")
        composite = self.composite_max
        if isinstance(composite, ObservedConditionComposite):
            if composite.venues != names:
                raise ValueError("Spot composite identities must match its actual constituent venues")
            volume, cvd = Decimal(0), Decimal(0)
            for venue in self.venues:
                volume = _sum_native(volume, venue.volume_native)
                cvd = _sum_native(cvd, venue.cvd)
            if composite.volume != volume or composite.cvd != cvd:
                raise ValueError("Spot composite must preserve its exact native constituent totals")
            direction = {1: "BUY", -1: "SELL", 0: "NEUTRAL"}[composite.sign]
            if self.direction != direction or self.direction_reason is not None:
                raise ValueError("Observed spot direction must match its actual composite sign")
        elif names or self.direction is not None or self.direction_reason != "NO_COMPLETE_VENUES":
            raise ValueError("Unavailable spot flow cannot invent observed venue or direction facts")
        return self


class BasicConditionSpot(ConditionSpotValues):
    kind: Literal["BASIC"]


class ObservedConditionCohort(ObservedConditionComposite):
    cohort: tuple[str, ...] = Field(min_length=1)
    share_of_max: float = Field(ge=0, le=1)
    share_majority: TrueFlag
    velocity: float | None = Field(ge=0)
    velocity_reason: Literal["INCOMPLETE", "ZERO_BASELINE", "NOT_ESTIMABLE"] | None

    @model_validator(mode="after")
    def actual_common_venues(self) -> Self:
        if self.cohort != self.venues:
            raise ValueError("A common-cohort composite must preserve its actual venue set")
        if (self.velocity is None) != (self.velocity_reason is not None):
            raise ValueError("Missing prior-cohort velocity requires its actual absence reason")
        return self


class UnavailableConditionCohort(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NOT_COHORT_CONSISTENT"]


type ConditionCohort = ObservedConditionCohort | UnavailableConditionCohort


class ScopedConditionSpot(ConditionSpotValues):
    interval: ConditionInterval
    coverage_through: Timestamp
    composite_cohort: ConditionCohort
    cohort_consistent: dict[ConditionWindow, bool]

    @model_validator(mode="after")
    def actual_window_scope(self) -> Self:
        stamp = self.coverage_through.value
        if self.venues:
            if stamp is None or parse_iso_time(stamp) != self.interval.cutoff:
                raise ValueError("Window coverage must preserve its actual requested cutoff")
        elif stamp is not None:
            raise ValueError("No complete spot venues cannot assert observed window coverage")
        if self.interval.window in self.cohort_consistent:
            raise ValueError("A spot consistency index compares distinct windows only")
        return self


class WindowedConditionSpot(ScopedConditionSpot):
    kind: Literal["WINDOWED"]


class ObservedConditionCohortComparison(DomainModel):
    status: Literal["OBSERVED"]
    cohort: tuple[str, ...] = Field(min_length=1)
    current: ObservedConditionComposite
    other: ObservedConditionComposite
    share_of_max: float = Field(ge=0, le=1)
    share_majority: TrueFlag

    @model_validator(mode="after")
    def common_comparison_scope(self) -> Self:
        if (
            len(self.cohort) != len(set(self.cohort))
            or set(self.current.venues) != set(self.cohort)
            or set(self.other.venues) != set(self.cohort)
        ):
            raise ValueError("Window comparisons require their actual shared venue cohort")
        return self


class UnavailableConditionCohortComparison(DomainModel):
    status: Literal["UNAVAILABLE"]
    cohort: tuple[str, ...]
    share_of_max: float | None = Field(ge=0, le=1)
    share_majority: bool | None
    reason: Literal["NOT_COHORT_CONSISTENT"]


type ConditionCohortComparison = ObservedConditionCohortComparison | UnavailableConditionCohortComparison


class ObservedConditionSizeProfile(DomainModel):
    status: Literal["OBSERVED"]
    large_ticket_buy_share: float | None = Field(ge=0, le=1)
    large_ticket_sell_share: float | None = Field(ge=0, le=1)
    large_ticket_buy_majority: bool | None
    large_tickets_minority: bool | None
    large_ticket_buy_notional: Decimal = Field(ge=0)
    large_ticket_sell_notional: Decimal = Field(ge=0)
    sell_share_rising: bool | None
    compatible_venues: tuple[str, ...] = Field(min_length=1)
    null_reasons: dict[
        Literal[
            "large_ticket_buy_share",
            "large_ticket_sell_share",
            "large_ticket_buy_majority",
            "large_tickets_minority",
            "sell_share_rising",
        ],
        ConditionStatisticalNullReason,
    ]

    @model_validator(mode="after")
    def actual_large_ticket_evidence(self) -> Self:
        _statistical_nulls(
            (
                ("large_ticket_buy_share", self.large_ticket_buy_share),
                ("large_ticket_sell_share", self.large_ticket_sell_share),
                ("large_ticket_buy_majority", self.large_ticket_buy_majority),
                ("large_tickets_minority", self.large_tickets_minority),
                ("sell_share_rising", self.sell_share_rising),
            ),
            tuple(self.null_reasons.items()),
        )
        if len(self.compatible_venues) != len(set(self.compatible_venues)):
            raise ValueError("A size profile requires its actual distinct compatible venues")
        return self


class UnavailableConditionSizeProfile(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["INCOMPLETE", "NOT_SUPPORTED", "NOT_ESTIMABLE"]


class UnrequestedConditionSizeProfile(DomainModel):
    status: Literal["NOT_REQUESTED"]


type ConditionSizeProfile = (
    ObservedConditionSizeProfile | UnavailableConditionSizeProfile | UnrequestedConditionSizeProfile
)


class EnrichedConditionSpot(ScopedConditionSpot):
    kind: Literal["ENRICHED"]
    enrichment_role: Literal["PRIMARY", "OTHER_WINDOW"]
    cohort_comparisons: dict[ConditionWindow, ConditionCohortComparison]
    size_profile: ConditionSizeProfile

    @model_validator(mode="after")
    def requested_enrichment(self) -> Self:
        if self.interval.window in self.cohort_comparisons:
            raise ValueError("A spot comparison index cannot compare a window with itself")
        if set(self.cohort_comparisons) != set(self.cohort_consistent):
            raise ValueError("Enriched spot consistency must preserve its actual comparison index")
        for name, comparison in self.cohort_comparisons.items():
            if self.cohort_consistent[name] != isinstance(comparison, ObservedConditionCohortComparison):
                raise ValueError("Spot consistency must agree with the actual shared-cohort evidence")
        if self.enrichment_role == "PRIMARY" and isinstance(
            self.size_profile, UnrequestedConditionSizeProfile
        ):
            raise ValueError("Missing requested size-profile evidence is unavailable, not unrequested")
        if self.enrichment_role == "OTHER_WINDOW" and not isinstance(
            self.size_profile, UnrequestedConditionSizeProfile
        ):
            raise ValueError("Only the primary window computes the requested large-ticket profile")
        return self


type ConditionSpot = BasicConditionSpot | WindowedConditionSpot | EnrichedConditionSpot


class ConditionSpotWindowGraph(DomainModel):
    spots: dict[ConditionWindow, WindowedConditionSpot]
    durations_s: dict[ConditionWindow, Annotated[float, Field(gt=0)]]
    primary: ConditionWindow

    @model_validator(mode="after")
    def complete_window_index(self) -> Self:
        windows = {"W15M", "W1H", "W4H", "W24H", "EVENT"}
        if set(self.spots) != windows or set(self.durations_s) != windows:
            raise ValueError("A spot window graph requires the actual five named windows")
        for name, spot in self.spots.items():
            if set(spot.cohort_consistent) != windows - {name}:
                raise ValueError("A spot consistency index requires the actual other windows")
            consistent = isinstance(spot.composite_cohort, ObservedConditionCohort)
            if any(value != consistent for value in spot.cohort_consistent.values()):
                raise ValueError("Windowed spot consistency must preserve its common-cohort evidence")
            if spot.interval.window != name:
                raise ValueError("A spot window index disagrees with its interval identity")
            if (spot.interval.cutoff - spot.interval.start).total_seconds() != self.durations_s[name]:
                raise ValueError("A spot duration must preserve its actual interval bounds")
        return self


class ObservedConditionOpenInterest(DomainModel):
    status: Literal["OBSERVED"]
    requested_window_duration_s: float = Field(gt=0)
    observed_window_duration_s: float = Field(ge=0)
    oi_native_start: Decimal = Field(gt=0)
    oi_native_end: Decimal = Field(ge=0)
    oi_native_chg_pct: float = Field(ge=-1)
    oi_usd_start: Decimal | None = Field(ge=0)
    oi_usd_end: Decimal | None = Field(ge=0)
    oi_usd_chg_pct: float | None = Field(ge=-1)
    oi_usd_reason: Literal["INCOMPLETE"] | None
    oi_usd_detail: Literal["SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"] | None
    decomposition: OIDecomposition | MissingOIDecomposition
    native_normal_range: FloatRange
    usd_normal_range: FloatRange
    native_oi_vs_normal: Literal["BELOW", "WITHIN", "ABOVE"] | None
    native_beyond: bool | None
    usd_beyond: bool | None
    oi_class: Literal["REAL_NATIVE", "MIXED", "PRICE_MARKUP"] | None
    classification_ready: bool
    start_observed_at: Timestamp
    end_observed_at: Timestamp

    @model_validator(mode="after")
    def supported_comparisons(self) -> Self:
        if self.start_observed_at.value is None or self.end_observed_at.value is None:
            raise ValueError("Observed OI endpoints require their actual observation clocks")
        start = parse_iso_time(self.start_observed_at.value)
        end = parse_iso_time(self.end_observed_at.value)
        if (end - start).total_seconds() != self.observed_window_duration_s:
            raise ValueError("OI observed duration must preserve its actual endpoint clocks")
        usd_ready = self.oi_usd_start is not None and self.oi_usd_start > 0 and self.oi_usd_end is not None
        if usd_ready != (self.oi_usd_chg_pct is not None):
            raise ValueError("USD OI change requires a positive initial and known final USD quantity")
        if usd_ready:
            if self.oi_usd_reason is not None or self.oi_usd_detail is not None:
                raise ValueError("An observed USD OI change cannot carry a missing-conversion reason")
        elif self.oi_usd_reason is None or self.oi_usd_detail is None:
            raise ValueError("Unavailable USD OI change requires its actual conversion gap")
        nr = self.native_normal_range.bounds
        ur = self.usd_normal_range.bounds
        native_position = (
            None
            if nr is None
            else "BELOW"
            if self.oi_native_chg_pct < nr.lower
            else "ABOVE"
            if self.oi_native_chg_pct > nr.upper
            else "WITHIN"
        )
        native_beyond = None if nr is None else native_position != "WITHIN"
        usd_beyond = (
            None
            if ur is None or self.oi_usd_chg_pct is None
            else self.oi_usd_chg_pct < ur.lower or self.oi_usd_chg_pct > ur.upper
        )
        if self.native_oi_vs_normal != native_position or self.native_beyond != native_beyond:
            raise ValueError("Native OI comparisons must use the actual available baseline")
        if self.usd_beyond != usd_beyond:
            raise ValueError("USD OI comparison must use the actual change and baseline")
        expected_class = oi_class(
            self.oi_native_chg_pct,
            self.oi_usd_chg_pct,
            self.decomposition,
            None if nr is None else (nr.lower, nr.upper),
            None if ur is None else (ur.lower, ur.upper),
        )
        if self.oi_class != expected_class or self.classification_ready != (expected_class is not None):
            raise ValueError(
                "OI classification readiness must preserve its actual decomposition and baselines"
            )
        return self


class UnavailableConditionOpenInterest(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["INCOMPLETE"]
    detail: Literal["OI_BUCKET_EDGE_UNSPECIFIED"] | None


class UnrequestedConditionOpenInterest(DomainModel):
    status: Literal["NOT_REQUESTED"]


type ConditionOpenInterest = (
    ObservedConditionOpenInterest | UnavailableConditionOpenInterest | UnrequestedConditionOpenInterest
)


class ConditionFundingObservation(DomainModel):
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"]
    rate: Decimal
    interval_h: Decimal | None = Field(gt=0)
    equivalent_eight_hour_rate: Decimal | None
    equivalent_rate_reason: Literal["INCOMPLETE"] | None
    observed_at: Timestamp

    @model_validator(mode="after")
    def actual_rate_basis(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("A funding observation requires its actual source observation clock")
        if self.interval_h is None:
            if self.equivalent_eight_hour_rate is not None or self.equivalent_rate_reason is None:
                raise ValueError("Unknown funding settlement duration cannot produce an eight-hour rate")
        elif self.equivalent_eight_hour_rate is None or self.equivalent_rate_reason is not None:
            raise ValueError("A known funding rate and duration require an available eight-hour rate")
        return self


class AvailableConditionFunding(DomainModel):
    status: Literal["OBSERVED"]
    settled: ConditionFundingObservation | None
    estimate: ConditionFundingObservation | None
    start: ConditionFundingObservation | None
    selected_kind: Literal["SETTLED", "CURRENT_ESTIMATE"]
    normal_range: DecimalRange
    funding_vs_normal: Literal["BELOW", "WITHIN", "ABOVE"] | None

    @model_validator(mode="after")
    def selected_rate(self) -> Self:
        for kind, observation in (
            ("SETTLED", self.settled),
            ("CURRENT_ESTIMATE", self.estimate),
            ("SETTLED", self.start),
        ):
            if observation is not None and observation.kind != kind:
                raise ValueError("A funding observation must preserve its actual settlement or estimate kind")
        selected = self.estimate if self.selected_kind == "CURRENT_ESTIMATE" else self.settled
        if selected is None or selected.equivalent_eight_hour_rate is None:
            raise ValueError("Selected funding requires an actual normalized observation")
        if (
            self.selected_kind == "SETTLED"
            and self.estimate is not None
            and self.estimate.equivalent_eight_hour_rate is not None
        ):
            raise ValueError("A current normalized estimate takes precedence over settled funding")
        bounds = self.normal_range.bounds
        value = selected.equivalent_eight_hour_rate
        position = (
            None
            if bounds is None
            else "BELOW"
            if value < bounds.lower
            else "ABOVE"
            if value > bounds.upper
            else "WITHIN"
        )
        if self.funding_vs_normal != position:
            raise ValueError("Funding comparison requires the actual normalized rate and baseline")
        return self


class UnavailableConditionFunding(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["INCOMPLETE"]
    settled: ConditionFundingObservation | None
    estimate: ConditionFundingObservation | None
    start: ConditionFundingObservation | None
    normal_range: DecimalRange

    @model_validator(mode="after")
    def no_normalized_current_observation(self) -> Self:
        for kind, observation in (
            ("SETTLED", self.settled),
            ("CURRENT_ESTIMATE", self.estimate),
            ("SETTLED", self.start),
        ):
            if observation is not None and observation.kind != kind:
                raise ValueError("A funding observation must preserve its actual settlement or estimate kind")
        if any(
            observation is not None and observation.equivalent_eight_hour_rate is not None
            for observation in (self.settled, self.estimate)
        ):
            raise ValueError("A normalized current funding observation cannot be classified as missing")
        return self


class UnrequestedConditionFunding(DomainModel):
    status: Literal["NOT_REQUESTED"]


type ConditionFunding = AvailableConditionFunding | UnavailableConditionFunding | UnrequestedConditionFunding


class ConditionCurrentLiquidationCapability(DomainModel):
    kind: Literal["CURRENT_SOURCE_CAPABILITY"]
    versions: ResolvedVersionIdentity


class ConditionRecordedLiquidationCapability(DomainModel):
    kind: Literal["RECORDED_METADATA"]
    identity: KnownCadenceIdentity | UncapturedCadenceIdentity


type ConditionLiquidationCapabilityOrigin = (
    ConditionCurrentLiquidationCapability | ConditionRecordedLiquidationCapability
)


class ConditionLiquidationSide(DomainModel):
    observed_record_count: int = Field(ge=0)
    native: Decimal | None = Field(ge=0)
    usd: Decimal | None = Field(ge=0)
    usd_reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def conversion_absence(self) -> Self:
        if (self.usd is None) != (self.usd_reason is not None):
            raise ValueError("Liquidation USD absence requires its actual incomplete-evidence reason")
        if self.observed_record_count == 0 and any(
            value is not None and value != 0 for value in (self.native, self.usd)
        ):
            raise ValueError("No liquidation records cannot support a positive observed amount")
        return self


class ConditionLiquidationCaptureWindow(DomainModel):
    requested: ConditionInterval
    start: Timestamp
    observed_through: Timestamp
    complete: bool
    reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_capture_bounds(self) -> Self:
        actual_start = None if self.start.value is None else parse_iso_time(self.start.value)
        through = None if self.observed_through.value is None else parse_iso_time(self.observed_through.value)
        if actual_start is not None and through is not None and actual_start > through:
            raise ValueError("Liquidation capture bounds must preserve chronological order")
        complete = (
            actual_start is not None
            and through is not None
            and actual_start <= self.requested.start
            and through >= self.requested.cutoff
        )
        if self.complete != complete or (self.reason is None) != complete:
            raise ValueError("Liquidation completeness requires actual coverage of both requested bounds")
        return self


class ConditionLiquidations(DomainModel):
    status: Literal["REQUESTED"]
    long: ConditionLiquidationSide
    short: ConditionLiquidationSide
    source_coverage: Literal["COMPLETE", "SAMPLED", "UNKNOWN", "NOT_SUPPORTED"]
    source_origin: ConditionLiquidationCapabilityOrigin
    coverage: Literal["COMPLETE", "SAMPLED", "UNKNOWN", "NOT_SUPPORTED"]
    capture_window: ConditionLiquidationCaptureWindow

    @model_validator(mode="after")
    def stream_and_source_coverage(self) -> Self:
        expected = (
            "UNKNOWN"
            if self.source_coverage == "COMPLETE" and not self.capture_window.complete
            else self.source_coverage
        )
        if self.coverage != expected:
            raise ValueError("A complete source capability cannot establish an unproven capture window")
        for side in (self.long, self.short):
            if (
                side.observed_record_count == 0
                and not (self.capture_window.complete and self.coverage in ("COMPLETE", "SAMPLED"))
                and (side.native is not None or side.usd is not None)
            ):
                raise ValueError("An unproven empty liquidation stream cannot manufacture zero amounts")
        return self


class UnrequestedConditionLiquidations(DomainModel):
    status: Literal["NOT_REQUESTED"]


class ConditionSupplementalOIObservation(DomainModel):
    observed_at: Timestamp
    oi_native: Decimal | None = Field(ge=0)
    oi_usd: Decimal | None = Field(ge=0)
    scope: Literal["SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"]

    @model_validator(mode="after")
    def observation_clock(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("Supplemental OI evidence requires the actual provider observation clock")
        return self


class ConditionOITemporalExclusions(DomainModel):
    count: int = Field(gt=0)
    reason: Literal["INCOMPLETE"]
    detail: Literal["OI_BUCKET_EDGE_UNSPECIFIED"]
    latest_observation: ConditionSupplementalOIObservation


class ConditionDerivativeVenue(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    open_interest: ConditionOpenInterest
    funding: ConditionFunding
    liquidations: ConditionLiquidations | UnrequestedConditionLiquidations
    oi_temporal_exclusions: ConditionOITemporalExclusions | None
    basis_bps: Decimal | None
    basis_reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_basis_availability(self) -> Self:
        if (self.basis_bps is None) != (self.basis_reason is not None):
            raise ValueError("Missing derivative basis requires the actual mark/index evidence gap")
        return self


class ConditionSynchronizedOpenInterest(DomainModel):
    status: Literal["OBSERVED"]
    venues: tuple[Annotated[str, Field(min_length=1, pattern=r"\S")], ...] = Field(min_length=1)
    values: ObservedConditionOpenInterest


class UnavailableConditionSynchronizedOpenInterest(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["INCOMPLETE", "NOT_ESTIMABLE"]


class UnrequestedConditionSynchronizedOpenInterest(DomainModel):
    status: Literal["NOT_REQUESTED"]


class ConditionLiquidationTotals(DomainModel):
    long: Decimal | None = Field(ge=0)
    short: Decimal | None = Field(ge=0)
    long_reason: Literal["INCOMPLETE"] | None
    short_reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def totals_absence(self) -> Self:
        if (self.long is None) != (self.long_reason is not None) or (self.short is None) != (
            self.short_reason is not None
        ):
            raise ValueError("Missing liquidation totals require actual incomplete-evidence reasons")
        return self


class ConditionAggregateLiquidations(DomainModel):
    status: Literal["REQUESTED"]
    complete_venues_total: ConditionLiquidationTotals
    sampled_venues_min: ConditionLiquidationTotals


class ConditionIncrementalVolumeCoverage(DomainModel):
    instruments: tuple[Annotated[str, Field(min_length=1, pattern=r"\S")], ...]
    complete: bool
    unit: Literal["USD"]
    reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_instrument_coverage(self) -> Self:
        if len(self.instruments) != len(set(self.instruments)):
            raise ValueError("Incremental-volume coverage cannot duplicate an instrument")
        if self.complete and not self.instruments:
            raise ValueError("An empty instrument set cannot establish complete volume coverage")
        if self.complete != (self.reason is None):
            raise ValueError("Incomplete incremental-volume coverage requires its actual reason")
        return self


class ConditionObservedIncrementalVolume(DomainModel):
    status: Literal["OBSERVED"]
    spot_usd: Decimal = Field(ge=0)
    perp_usd: Decimal = Field(ge=0)
    spot_share: float = Field(ge=0, le=1)
    perp_share: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def actual_positive_increment(self) -> Self:
        total = _sum_native(self.spot_usd, self.perp_usd)
        if total <= 0:
            raise ValueError("Zero total incremental volume cannot produce market shares")
        with localcontext() as context:
            context.prec = 128
            expected_spot, expected_perp = float(self.spot_usd / total), float(self.perp_usd / total)
        if self.spot_share != expected_spot or self.perp_share != expected_perp:
            raise ValueError("Incremental market shares must use the actual positive USD increments")
        return self


class ConditionUnavailableIncrementalVolume(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["INCOMPLETE", "ZERO_TOTAL_VOLUME"]
    spot_usd: Decimal | None = Field(ge=0)
    perp_usd: Decimal | None = Field(ge=0)

    @model_validator(mode="after")
    def actual_missing_or_zero_increment(self) -> Self:
        incomplete = self.spot_usd is None or self.perp_usd is None
        if self.reason == "INCOMPLETE" and not incomplete:
            raise ValueError("Known incremental volumes cannot carry an incomplete-volume reason")
        if self.reason == "ZERO_TOTAL_VOLUME" and (incomplete or self.spot_usd != 0 or self.perp_usd != 0):
            raise ValueError("Zero-total volume requires both actual zero increments")
        return self


class ConditionIncrementalVolumes(DomainModel):
    status: Literal["REQUESTED"]
    spot_coverage: ConditionIncrementalVolumeCoverage
    perp_coverage: ConditionIncrementalVolumeCoverage
    values: ConditionObservedIncrementalVolume | ConditionUnavailableIncrementalVolume

    @model_validator(mode="after")
    def matching_complete_market_inputs(self) -> Self:
        if self.spot_coverage.complete != (
            self.values.spot_usd is not None
        ) or self.perp_coverage.complete != (self.values.perp_usd is not None):
            raise ValueError("Known volume increments require complete current and preceding market coverage")
        return self


class UnrequestedConditionIncrementalVolumes(DomainModel):
    status: Literal["NOT_REQUESTED"]


class ConditionObservedFundingDispersion(DomainModel):
    status: Literal["OBSERVED"]
    value: Decimal = Field(ge=0)
    observation_count: int = Field(ge=2)


class ConditionUnavailableFundingDispersion(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["INSUFFICIENT_OBSERVATIONS"]
    observation_count: int = Field(ge=0, lt=2)


class UnrequestedConditionFundingDispersion(DomainModel):
    status: Literal["NOT_REQUESTED"]


class ConditionDerivativeWindow[Venue: ConditionDerivativeVenue](DomainModel):
    interval: ConditionInterval
    per_venue: tuple[Venue, ...]
    synchronized: (
        ConditionSynchronizedOpenInterest
        | UnavailableConditionSynchronizedOpenInterest
        | UnrequestedConditionSynchronizedOpenInterest
    )
    funding_dispersion: (
        ConditionObservedFundingDispersion
        | ConditionUnavailableFundingDispersion
        | UnrequestedConditionFundingDispersion
    )
    liquidations: ConditionAggregateLiquidations | UnrequestedConditionLiquidations
    incremental_volume: ConditionIncrementalVolumes | UnrequestedConditionIncrementalVolumes

    @model_validator(mode="after")
    def actual_venue_cohorts(self) -> Self:
        identities = [venue.instrument_id for venue in self.per_venue]
        if len(identities) != len(set(identities)):
            raise ValueError("A derivative window cannot duplicate an instrument")
        if isinstance(self.synchronized, ConditionSynchronizedOpenInterest) and not set(
            self.synchronized.venues
        ) <= {venue.venue for venue in self.per_venue}:
            raise ValueError("Synchronized OI cannot claim an absent derivative venue")
        funding_rates: list[Decimal] = []
        for venue in self.per_venue:
            funding = venue.funding
            if isinstance(funding, AvailableConditionFunding):
                selected = (
                    funding.estimate if funding.selected_kind == "CURRENT_ESTIMATE" else funding.settled
                )
                if selected is None or selected.equivalent_eight_hour_rate is None:
                    raise ValueError("Selected funding must preserve its validated normalized observation")
                funding_rates.append(selected.equivalent_eight_hour_rate)
        if not isinstance(self.funding_dispersion, UnrequestedConditionFundingDispersion):
            if any(isinstance(venue.funding, UnrequestedConditionFunding) for venue in self.per_venue):
                raise ValueError("Funding dispersion cannot claim an unrequested venue calculation")
            if self.funding_dispersion.observation_count != len(funding_rates):
                raise ValueError("Funding dispersion count must preserve actual normalized observations")
            if isinstance(
                self.funding_dispersion, ConditionObservedFundingDispersion
            ) and self.funding_dispersion.value != _sum_native(max(funding_rates), -min(funding_rates)):
                raise ValueError("Funding dispersion must preserve the actual normalized rate range")
        for venue in self.per_venue:
            if isinstance(venue.open_interest, ObservedConditionOpenInterest):
                self.validate_oi_window(venue.open_interest)
            if (
                isinstance(venue.liquidations, ConditionLiquidations)
                and venue.liquidations.capture_window.requested != self.interval
            ):
                raise ValueError("Liquidation coverage must preserve the actual derivative request interval")
            if isinstance(self.liquidations, ConditionAggregateLiquidations) != isinstance(
                venue.liquidations, ConditionLiquidations
            ):
                raise ValueError(
                    "Aggregate and venue liquidation calculations must preserve actual request selection"
                )
        if isinstance(self.synchronized, ConditionSynchronizedOpenInterest):
            self.validate_oi_window(self.synchronized.values)
        if isinstance(self.liquidations, ConditionAggregateLiquidations):
            for coverage, totals in (
                ("COMPLETE", self.liquidations.complete_venues_total),
                ("SAMPLED", self.liquidations.sampled_venues_min),
            ):
                eligible = [
                    venue.liquidations
                    for venue in self.per_venue
                    if isinstance(venue.liquidations, ConditionLiquidations)
                    and venue.liquidations.coverage == coverage
                ]
                for side, supplied in (("long", totals.long), ("short", totals.short)):
                    values = [entry.long.usd if side == "long" else entry.short.usd for entry in eligible]
                    expected: Decimal | None = None
                    if values and all(value is not None for value in values):
                        expected = Decimal(0)
                        for value in values:
                            if value is None:
                                raise ValueError("Known liquidation totals require every eligible amount")
                            expected = _sum_native(expected, value)
                    if supplied != expected:
                        raise ValueError(
                            "Liquidation totals must preserve every actually eligible venue amount"
                        )
        return self

    def validate_oi_window(self, observation: ObservedConditionOpenInterest) -> None:
        if (
            observation.requested_window_duration_s
            != (self.interval.cutoff - self.interval.start).total_seconds()
        ):
            raise ValueError("OI calculations must preserve the actual requested derivative duration")
        if (
            observation.end_observed_at.value is None
            or parse_iso_time(observation.end_observed_at.value) > self.interval.cutoff
        ):
            raise ValueError("OI window endpoints cannot claim observations after the actual cutoff")


class BasicConditionDerivatives(ConditionDerivativeWindow[ConditionDerivativeVenue]):
    kind: Literal["BASIC"]


class ConditionObservedPredicate(DomainModel):
    status: Literal["OBSERVED"]
    value: bool


type ConditionPredicateUnavailableReason = Literal[
    "INCOMPLETE",
    "INSUFFICIENT_HISTORY",
    "NOT_ESTIMABLE",
    "NOT_SUPPORTED",
    "ZERO_TOTAL_VOLUME",
    "ZERO_BASELINE",
    "ZERO_NOTIONAL",
    "ZERO_NET_FLOW",
    "NO_EVENT_ANCHOR",
    "NO_VENUES",
    "QUOTE_CONVERSION_UNAVAILABLE",
]


class ConditionUnavailablePredicate(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: ConditionPredicateUnavailableReason


type ConditionPredicate = ConditionObservedPredicate | ConditionUnavailablePredicate


class ConditionNativeVolumeSupport(DomainModel):
    value: Decimal | None = Field(ge=0)
    previous: Decimal | None = Field(ge=0)
    normal_range: DecimalRange
    unit: Literal["NATIVE_TOKEN"]
    window: ConditionWindow
    reason: Literal["NOT_ESTIMABLE"] | None

    @model_validator(mode="after")
    def actual_comparison_basis(self) -> Self:
        estimable = self.value is not None and self.normal_range.bounds is not None
        if estimable != (self.reason is None):
            raise ValueError("Native-volume support requires an actual current amount and available baseline")
        return self


class ConditionDerivativeVenueSupport(DomainModel):
    status: Literal["COMPUTED"]
    perp_taker_buy_share: float | None = Field(ge=0, le=1)
    perp_taker_buy_share_reason: Literal["INCOMPLETE", "ZERO_TOTAL_VOLUME", "NOT_SUPPORTED"] | None
    perp_volume_usd: Decimal | None = Field(ge=0)
    perp_volume_reason: Literal["INCOMPLETE"] | None
    basis_normal_range: DecimalRange
    basis_vs_normal: Literal["WITHIN", "ABOVE", "BELOW"] | None
    basis_widening: ConditionPredicate
    funding_rising: ConditionPredicate

    @model_validator(mode="after")
    def absent_quantities(self) -> Self:
        if (self.perp_taker_buy_share is None) != (self.perp_taker_buy_share_reason is not None):
            raise ValueError("Missing taker share requires its actual coverage or zero-denominator reason")
        if (self.perp_volume_usd is None) != (self.perp_volume_reason is not None):
            raise ValueError("Missing perp USD volume requires its actual coverage gap")
        if self.basis_normal_range.bounds is None and self.basis_vs_normal is not None:
            raise ValueError("An unavailable basis baseline cannot establish a normal-range comparison")
        return self


class ConditionDerivativeSupportFeatures(DomainModel):
    spot_volume_up_beyond_normal: ConditionPredicate
    spot_volume_within_normal: ConditionPredicate
    spot_volume: ConditionNativeVolumeSupport
    perp_volume_up_beyond_normal: ConditionPredicate
    perp_volume_within_normal: ConditionPredicate
    perp_volume: ConditionNativeVolumeSupport
    perp_taker_flow_buy: ConditionPredicate
    basis_within_normal: ConditionPredicate
    basis_widening: ConditionPredicate
    funding_rising: ConditionPredicate
    liquidations_first_half_majority: ConditionPredicate
    large_ticket_buy_majority: ConditionPredicate
    large_tickets_minority: ConditionPredicate
    large_ticket_sell_share_rising: ConditionPredicate
    exchange_inflows: ConditionPredicate
    localized_inventory_transfer: ConditionPredicate
    ask_depth_building: ConditionPredicate
    ask_replenishment: ConditionPredicate

    @model_validator(mode="after")
    def actual_volume_predicates(self) -> Self:
        for volume, within, up in (
            (self.spot_volume, self.spot_volume_within_normal, self.spot_volume_up_beyond_normal),
            (self.perp_volume, self.perp_volume_within_normal, self.perp_volume_up_beyond_normal),
        ):
            bounds = volume.normal_range.bounds
            value = volume.value
            previous = volume.previous
            expected_within = (
                None if bounds is None or value is None else bounds.lower <= value <= bounds.upper
            )
            expected_up = (
                None
                if bounds is None or value is None or previous is None
                else value > previous and value > bounds.upper
            )
            _condition_predicate_matches(within, expected_within)
            _condition_predicate_matches(up, expected_up)
        return self


def _condition_predicate_matches(predicate: ConditionPredicate, expected: bool | None) -> None:
    if expected is None:
        if not isinstance(predicate, ConditionUnavailablePredicate):
            raise ValueError("A support predicate cannot replace unavailable inputs with a known boolean")
    elif not isinstance(predicate, ConditionObservedPredicate) or predicate.value != expected:
        raise ValueError("A support predicate must preserve its actual known calculation")


class ConditionLeadLagPoint(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    market: Literal["SPOT", "PERP"]
    first_observed_at: Timestamp

    @model_validator(mode="after")
    def actual_detection_clock(self) -> Self:
        if self.first_observed_at.value is None:
            raise ValueError("A detected first move requires its actual observation clock")
        return self


class ConditionLeadLagEvidence(DomainModel):
    interval: ConditionInterval
    evaluated_markets: tuple[Literal["SPOT", "PERP"], ...]
    spot_confirmation: ConditionPredicate
    sequence: tuple[ConditionLeadLagPoint, ...]

    @model_validator(mode="after")
    def actual_detection_scope(self) -> Self:
        if len(self.evaluated_markets) != len(set(self.evaluated_markets)):
            raise ValueError("Lead/lag market evaluation cannot duplicate a market")
        for point in self.sequence:
            if point.market not in self.evaluated_markets:
                raise ValueError("A lead/lag detection requires its actual evaluated market")
            if point.first_observed_at.value is None:
                raise ValueError("A lead/lag detection requires its actual observation clock")
            at = parse_iso_time(point.first_observed_at.value)
            if not self.interval.start <= at < self.interval.cutoff:
                raise ValueError("A first-move observation must belong to the actual requested interval")
        return self


class ObservedConditionLeadLag(ConditionLeadLagEvidence):
    status: Literal["OBSERVED"]
    first_mover: Literal["SPOT", "PERP", "SIMULTANEOUS"]

    @model_validator(mode="after")
    def observed_market_order(self) -> Self:
        if set(self.evaluated_markets) != {"SPOT", "PERP"}:
            raise ValueError("First-mover classification requires actual evaluation of both markets")
        spot = [
            parse_iso_time(point.first_observed_at.value)
            for point in self.sequence
            if point.market == "SPOT" and point.first_observed_at.value is not None
        ]
        perp = [
            parse_iso_time(point.first_observed_at.value)
            for point in self.sequence
            if point.market == "PERP" and point.first_observed_at.value is not None
        ]
        if not spot and not perp:
            raise ValueError("An observed first mover requires a detected market move")
        if spot and perp:
            lag = (min(perp) - min(spot)).total_seconds()
            expected = "SPOT" if lag >= 60 else "PERP" if lag <= -60 else "SIMULTANEOUS"
        else:
            expected = "SPOT" if spot else "PERP"
        if self.first_mover != expected:
            raise ValueError("First-mover classification must preserve actual detection order")
        return self


class UnavailableConditionLeadLag(ConditionLeadLagEvidence):
    status: Literal["UNAVAILABLE"]
    first_mover: Literal["UNKNOWN"]
    reason: Literal["INCOMPLETE", "INSUFFICIENT_HISTORY", "NOT_ESTIMABLE"]


class EnrichedConditionDerivativeVenue(ConditionDerivativeVenue):
    support: ConditionDerivativeVenueSupport

    @model_validator(mode="after")
    def actual_basis_comparison(self) -> Self:
        bounds = self.support.basis_normal_range.bounds
        value = self.basis_bps
        expected = (
            None
            if bounds is None or value is None
            else "BELOW"
            if value < bounds.lower
            else "ABOVE"
            if value > bounds.upper
            else "WITHIN"
        )
        if self.support.basis_vs_normal != expected:
            raise ValueError("Enriched basis comparison requires the actual mark/index basis and baseline")
        return self


class EnrichedConditionDerivatives(ConditionDerivativeWindow[EnrichedConditionDerivativeVenue]):
    kind: Literal["ENRICHED"]
    funding_dispersion_normal_range: DecimalRange
    funding_dispersion_beyond_normal: ConditionPredicate
    lead_lag: ObservedConditionLeadLag | UnavailableConditionLeadLag
    support_features: ConditionDerivativeSupportFeatures

    @model_validator(mode="after")
    def actual_enrichment_inputs(self) -> Self:
        dispersion = self.funding_dispersion
        bounds = self.funding_dispersion_normal_range.bounds
        beyond = (
            None
            if not isinstance(dispersion, ConditionObservedFundingDispersion) or bounds is None
            else dispersion.value < bounds.lower or dispersion.value > bounds.upper
        )
        _condition_predicate_matches(self.funding_dispersion_beyond_normal, beyond)
        if (
            self.support_features.spot_volume.window != self.interval.window
            or self.support_features.perp_volume.window != self.interval.window
        ):
            raise ValueError("Derivative support volume must preserve its actual analysis window")
        basis_values = [venue.support.basis_vs_normal for venue in self.per_venue]
        within = (
            None
            if not basis_values or any(value is None for value in basis_values)
            else all(value == "WITHIN" for value in basis_values)
        )
        _condition_predicate_matches(self.support_features.basis_within_normal, within)
        for aggregate, members in (
            (
                self.support_features.basis_widening,
                tuple(venue.support.basis_widening for venue in self.per_venue),
            ),
            (
                self.support_features.funding_rising,
                tuple(venue.support.funding_rising for venue in self.per_venue),
            ),
        ):
            known = [member.value for member in members if isinstance(member, ConditionObservedPredicate)]
            _condition_predicate_matches(aggregate, any(known) if known else None)
        return self


type ConditionDerivatives = BasicConditionDerivatives | EnrichedConditionDerivatives


class ConditionRelativeReturn(DomainModel):
    value: float | None
    reason: (
        Literal[
            "INCOMPLETE",
            "INSUFFICIENT_HISTORY",
            "NOT_ESTIMABLE",
            "NO_FRESH_ENDPOINTS",
            "NOT_APPLICABLE_BENCHMARK",
            "NOT_APPLICABLE",
        ]
        | None
    )

    @model_validator(mode="after")
    def actual_availability(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("An unavailable relative return requires its actual missing-input cause")
        return self


class ConditionRelativeNormalRange(DomainModel):
    bounds: FloatBounds | None
    reason: Literal["INCOMPLETE", "INSUFFICIENT_HISTORY", "NOT_ESTIMABLE", "DEGENERATE_HISTORY"] | None

    @model_validator(mode="after")
    def actual_history_availability(self) -> Self:
        if (self.bounds is None) != (self.reason is not None):
            raise ValueError("An unavailable relative baseline requires its actual statistical cause")
        return self


class ConditionRelativeWindow(DomainModel):
    interval: ConditionInterval
    observed_at: Timestamp
    reference_instrument_id: str | None = Field(min_length=1, pattern=r"\S")
    raw: ConditionRelativeReturn
    btc_adjusted: ConditionRelativeReturn
    eth_adjusted: ConditionRelativeReturn
    sector_adjusted: ConditionRelativeReturn
    measure: Literal["R_RAW", "R_BTC_ADJ"]
    btc_reference: ReferencePriceReturn
    eth_reference: ReferencePriceReturn
    btc_beta: BetaEstimate
    eth_beta: BetaEstimate
    percentile: float | None = Field(ge=0, le=1)
    percentile_reason: Literal["INCOMPLETE", "NOT_ESTIMABLE"] | None
    normal_range: ConditionRelativeNormalRange
    history: HistoryDiagnostic

    def relative_return(self) -> float | None:
        validated = type(self).model_validate(self)
        return validated.raw.value if validated.measure == "R_RAW" else validated.btc_adjusted.value

    def beyond_normal(self) -> bool | None:
        validated = type(self).model_validate(self)
        bounds = validated.normal_range.bounds
        value = validated.raw.value if validated.measure == "R_RAW" else validated.btc_adjusted.value
        return None if bounds is None or value is None else value < bounds.lower or value > bounds.upper

    @model_validator(mode="after")
    def actual_adjustment_and_history(self) -> Self:
        duration = (self.interval.cutoff - self.interval.start).total_seconds()
        if self.history.target_window_duration_s != duration:
            raise ValueError("A relative baseline must preserve its actual requested window duration")
        if self.raw.value is not None and self.reference_instrument_id is None:
            raise ValueError("An observed raw return requires its actual reference instrument")
        if self.raw.value is not None and self.observed_at.value is None:
            raise ValueError("An observed raw return requires its actual last price observation clock")
        if (
            self.observed_at.value is not None
            and parse_iso_time(self.observed_at.value) > self.interval.cutoff
        ):
            raise ValueError("A relative price observation cannot lie after its actual cutoff")
        if self.measure == "R_RAW":
            if self.btc_adjusted.value is not None or self.btc_adjusted.reason != "NOT_APPLICABLE_BENCHMARK":
                raise ValueError("A raw benchmark measure cannot invent a self-beta adjustment")
        else:
            self.validate_adjustment(self.btc_adjusted, self.btc_beta, self.btc_reference)
        self.validate_adjustment(self.eth_adjusted, self.eth_beta, self.eth_reference)
        measure = self.raw.value if self.measure == "R_RAW" else self.btc_adjusted.value
        expected_percentile = measure is not None and self.history.sample_count > 0
        if (self.percentile is not None) != expected_percentile:
            raise ValueError("An observed percentile requires the actual return and historical samples")
        if (self.percentile is None) != (self.percentile_reason is not None):
            raise ValueError("An unavailable percentile requires its actual missing-input reason")
        if self.normal_range.bounds is not None and self.history.sample_count == 0:
            raise ValueError("An empty statistical history cannot establish normal-return bounds")
        return self

    def validate_adjustment(
        self, adjusted: ConditionRelativeReturn, beta: BetaEstimate, reference: ReferencePriceReturn
    ) -> None:
        raw = self.raw.value
        if isinstance(beta, UnavailableBeta):
            if adjusted.value is not None or adjusted.reason != beta.reason:
                raise ValueError("Unavailable beta cannot manufacture an adjusted relative return")
            return
        if raw is None or reference.value is None:
            if adjusted.value is not None:
                raise ValueError(
                    "An adjusted relative return requires actual raw and fresh benchmark returns"
                )
            return
        if adjusted.value != raw - beta.value * reference.value:
            raise ValueError(
                "An adjusted relative return must preserve its actual beta and fresh benchmark return"
            )


class ObservedConditionMarketContext(DomainModel):
    status: Literal["OBSERVED"]
    value: Literal["STRESS", "HEADWIND", "NEUTRAL"]


class UnavailableConditionMarketContext(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["NOT_ESTIMABLE"]


class ConditionMarketContextInputs(DomainModel):
    btc_return_24h: float | None
    rv24: float | None = Field(ge=0)
    rv24_bound: datetime
    rv24_resolution_s: Annotated[Literal[3600], BeforeValidator(require_number)]
    return_normal_range: FloatRange
    return_lower_quartile: float | None
    rv_normal_range: FloatRange
    rule: Literal["normal_range"]

    @field_validator("rv24_bound")
    @classmethod
    def actual_hour_boundary(cls, value: datetime) -> datetime:
        at = parse_iso_time(value)
        if at.minute != 0 or at.second != 0 or at.microsecond != 0:
            raise ValueError("The realized-volatility bound must preserve the actual completed UTC hour")
        return at

    @model_validator(mode="after")
    def actual_return_history(self) -> Self:
        if (self.return_normal_range.bounds is None) != (self.return_lower_quartile is None):
            raise ValueError("A return quartile must preserve actual available return history")
        return self

    def classification(self) -> ObservedConditionMarketContext | UnavailableConditionMarketContext:
        validated = type(self).model_validate(self)
        returns, vols = validated.return_normal_range.bounds, validated.rv_normal_range.bounds
        r, rv, quartile = validated.btc_return_24h, validated.rv24, validated.return_lower_quartile
        if returns is None or vols is None or r is None or rv is None or quartile is None:
            return UnavailableConditionMarketContext(status="UNAVAILABLE", value=None, reason="NOT_ESTIMABLE")
        value: Literal["STRESS", "HEADWIND", "NEUTRAL"] = (
            "STRESS" if r < returns.lower or rv > vols.upper else "HEADWIND" if r < quartile else "NEUTRAL"
        )
        return ObservedConditionMarketContext(status="OBSERVED", value=value)


class ObservedConditionRepricingBand(DomainModel):
    status: Literal["OBSERVED"]
    value: Literal["NEGATIVE", "UNPRICED", "EARLY", "SUBSTANTIAL", "HEAVY"]


class UnavailableConditionRepricingBand(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal[
        "DIRECTION_NOT_DECLARED",
        "NO_EVENT_ANCHOR",
        "INCOMPLETE",
        "INSUFFICIENT_HISTORY",
        "NOT_ESTIMABLE",
        "DEGENERATE_HISTORY",
        "NO_FRESH_ENDPOINTS",
        "NOT_APPLICABLE_BENCHMARK",
        "NOT_APPLICABLE",
    ]


class DeclaredConditionRepricing(DomainModel):
    status: Literal["DIRECTION_DECLARED"]
    direction: Literal["BUY", "SELL"]
    event_return: ConditionRelativeReturn
    event_range: ConditionRelativeNormalRange
    day_range: ConditionRelativeNormalRange
    event_lower_quartile: float | None
    event_upper_quartile: float | None

    @model_validator(mode="after")
    def actual_event_quantiles(self) -> Self:
        if (self.event_lower_quartile is None) != (self.event_upper_quartile is None):
            raise ValueError("Event return quartiles require their actual paired observations")
        if self.event_range.bounds is None:
            if self.event_range.reason != "DEGENERATE_HISTORY" and self.event_lower_quartile is not None:
                raise ValueError("Unavailable event history cannot invent observed return quartiles")
        elif self.event_lower_quartile is None:
            raise ValueError("Available event history requires actual directional quartile evidence")
        if (
            self.event_lower_quartile is not None
            and self.event_upper_quartile is not None
            and self.event_lower_quartile > self.event_upper_quartile
        ):
            raise ValueError("Observed event return quartiles must preserve their actual order")
        return self

    def band(self) -> ObservedConditionRepricingBand | UnavailableConditionRepricingBand:
        validated = type(self).model_validate(self)
        raw = validated.event_return.value
        if raw is None:
            reason = validated.event_return.reason
            if reason is None:
                raise ValueError("An unavailable event return requires its actual reason")
            return UnavailableConditionRepricingBand(status="UNAVAILABLE", value=None, reason=reason)
        sign = 1 if validated.direction == "BUY" else -1
        value = raw * sign
        er, dr = validated.event_range.bounds, validated.day_range.bounds
        day_upper = None if dr is None else dr.upper if sign == 1 else -dr.lower
        if day_upper is not None and value > day_upper:
            return ObservedConditionRepricingBand(status="OBSERVED", value="HEAVY")
        if er is None:
            history_reason = validated.event_range.reason
            if history_reason is None:
                raise ValueError("Unavailable event history requires its actual cause")
            return UnavailableConditionRepricingBand(status="UNAVAILABLE", value=None, reason=history_reason)
        lower, upper = (er.lower, er.upper) if sign == 1 else (-er.upper, -er.lower)
        quartile = (
            validated.event_upper_quartile
            if sign == 1
            else -validated.event_lower_quartile
            if validated.event_lower_quartile is not None
            else None
        )
        if quartile is None:
            raise ValueError("Directional repricing requires its actual observed event quartile")
        band: Literal["NEGATIVE", "UNPRICED", "EARLY", "SUBSTANTIAL", "HEAVY"] = (
            "NEGATIVE"
            if value < lower
            else "SUBSTANTIAL"
            if value > upper
            else "EARLY"
            if value > quartile
            else "UNPRICED"
        )
        return ObservedConditionRepricingBand(status="OBSERVED", value=band)


class UndeclaredConditionRepricingDirection(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["DIRECTION_NOT_DECLARED"]

    def band(self) -> UnavailableConditionRepricingBand:
        type(self).model_validate(self)
        return UnavailableConditionRepricingBand(
            status="UNAVAILABLE", value=None, reason="DIRECTION_NOT_DECLARED"
        )


class UnavailableConditionEventRepricing(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_EVENT_ANCHOR"]

    def band(self) -> UnavailableConditionRepricingBand:
        type(self).model_validate(self)
        return UnavailableConditionRepricingBand(status="UNAVAILABLE", value=None, reason="NO_EVENT_ANCHOR")


type ConditionRepricing = (
    DeclaredConditionRepricing | UndeclaredConditionRepricingDirection | UnavailableConditionEventRepricing
)


class ObservedConditionReferencePrice(DomainModel):
    status: Literal["OBSERVED"]
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    value: Decimal = Field(gt=0)
    source: Literal["LAST_TRADE", "KLINE_CLOSE"]
    observed_at: Timestamp

    @model_validator(mode="after")
    def actual_source_clock(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("An observed reference price requires its actual source observation clock")
        return self


class UnavailableConditionReferencePrice(DomainModel):
    status: Literal["UNAVAILABLE"]
    instrument_id: str | None = Field(min_length=1, pattern=r"\S")
    value: None
    source: Literal["LAST_TRADE", "KLINE_CLOSE"] | None
    observed_at: Timestamp
    reason: Literal["INCOMPLETE"]


type ConditionReferencePrice = ObservedConditionReferencePrice | UnavailableConditionReferencePrice


class ConditionReferenceTradeEvidenceRef(DomainModel):
    projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ConditionDeclaredEventAnchor(DomainModel):
    kind: Literal["EVENT"]
    source: Literal["T0", "T1", "KNOWN_TO_US_AT"]
    at: Timestamp

    @model_validator(mode="after")
    def actual_event_clock(self) -> Self:
        if self.at.value is None:
            raise ValueError("An actual event anchor requires its captured clock value")
        return self


class ConditionAnalysisWindowStart(DomainModel):
    kind: Literal["ANALYSIS_WINDOW_START"]
    at: datetime

    @field_validator("at")
    @classmethod
    def aware_boundary(cls, value: datetime) -> datetime:
        return parse_iso_time(value)


class ConditionRelativeAnchor(DomainModel):
    origin: ConditionDeclaredEventAnchor | ConditionAnalysisWindowStart
    price: ConditionReferencePrice
    trade_evidence_ref: ConditionReferenceTradeEvidenceRef | None

    @model_validator(mode="after")
    def actual_anchor_trade(self) -> Self:
        if isinstance(self.price, ObservedConditionReferencePrice):
            if self.price.source != "LAST_TRADE" or self.trade_evidence_ref is None:
                raise ValueError("An observed anchor price requires its actual predecessor trade evidence")
        elif self.trade_evidence_ref is not None:
            raise ValueError("A missing anchor trade cannot invent original trade evidence")
        return self

    def instant(self) -> datetime:
        validated = type(self).model_validate(self)
        if isinstance(validated.origin, ConditionAnalysisWindowStart):
            return validated.origin.at
        if validated.origin.at.value is None:
            raise ValueError("An event anchor requires its actual captured clock")
        return parse_iso_time(validated.origin.at.value)


class ConditionPriceReferenceCoverage(DomainModel):
    complete: bool
    expected_duration_s: Annotated[Literal[86400], BeforeValidator(require_number)]
    boundary_segments: tuple[ConditionBoundaryGroup, ...]
    excluded: tuple[ConditionSpotExclusion, ...]
    volume_unit: Literal["NATIVE_TOKEN"]
    covered_volume_native: Decimal | None = Field(ge=0)

    @model_validator(mode="after")
    def actual_native_volume_coverage(self) -> Self:
        if self.complete != (self.covered_volume_native is not None):
            raise ValueError("Reference selection requires actual complete native-volume coverage")
        if self.complete and self.excluded:
            raise ValueError("Complete reference coverage cannot also exclude its instrument")
        return self


class ConditionPreEventRunup(DomainModel):
    window: Literal["PRE_EVENT_W24H"]
    start: datetime
    cutoff: datetime
    value: ConditionRelativeReturn
    percentile: float | None = Field(ge=0, le=1)
    percentile_reason: Literal["INCOMPLETE", "NOT_ESTIMABLE"] | None
    beyond_normal: ConditionPredicate

    @field_validator("start", "cutoff")
    @classmethod
    def aware_boundary(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_prior_day(self) -> Self:
        if (self.cutoff - self.start).total_seconds() != 86400:
            raise ValueError("Pre-event runup requires the actual preceding 24-hour interval")
        if (self.percentile is None) != (self.percentile_reason is not None):
            raise ValueError("Missing runup percentile requires its actual missing-input reason")
        return self


type ConditionOutcomeHorizon = Literal["W1H", "W4H", "W12H", "W24H", "W3D", "W7D"]


class ConditionOutcomeRange(DomainModel):
    window: ConditionOutcomeHorizon
    unit: Literal["DECIMAL_RETURN"]
    measure: Literal["R_RAW", "R_BTC_ADJ"]
    start: datetime
    cutoff: datetime
    observed_at: Timestamp
    normal_range: ConditionRelativeNormalRange
    history: HistoryDiagnostic

    @field_validator("start", "cutoff")
    @classmethod
    def aware_boundary(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_horizon_history(self) -> Self:
        if self.start >= self.cutoff:
            raise ValueError("An outcome history requires its actual positive lookback interval")
        durations = {"W1H": 3600, "W4H": 14400, "W12H": 43200, "W24H": 86400, "W3D": 259200, "W7D": 604800}
        if self.history.target_window_duration_s != durations[self.window]:
            raise ValueError("An outcome baseline must preserve the actual horizon duration")
        if self.history.sample_count == 0:
            if self.normal_range.bounds is not None or self.observed_at.value is not None:
                raise ValueError(
                    "No outcome history cannot establish normal bounds or an observed source clock"
                )
        elif isinstance(self.history, HistoryDiagnostics):
            last = self.history.latest_observed_at
            if (
                last is None
                or self.observed_at.value is None
                or parse_iso_time(self.observed_at.value) != last
            ):
                raise ValueError(
                    "Outcome observation clocks must preserve the actual last historical endpoint"
                )
        if self.observed_at.value is not None and parse_iso_time(self.observed_at.value) >= self.cutoff:
            raise ValueError("An outcome baseline cannot use future or current event observations")
        return self


class ConditionComputedOutcomeRanges(DomainModel):
    status: Literal["COMPUTED"]
    by_horizon: dict[ConditionOutcomeHorizon, ConditionOutcomeRange]

    @model_validator(mode="after")
    def actual_horizon_index(self) -> Self:
        if set(self.by_horizon) != {"W1H", "W4H", "W12H", "W24H", "W3D", "W7D"}:
            raise ValueError("Computed outcome ranges require the actual six requested horizons")
        if any(name != value.window for name, value in self.by_horizon.items()):
            raise ValueError("An outcome-range index must preserve actual horizon identities")
        return self


class UnrequestedConditionOutcomeRanges(DomainModel):
    status: Literal["NOT_REQUESTED"]


class ConditionRelativeGraph(DomainModel):
    reference_instrument_id: str | None = Field(min_length=1, pattern=r"\S")
    reference_coverage: dict[
        Annotated[str, Field(min_length=1, pattern=r"\S")], ConditionPriceReferenceCoverage
    ]
    reference_reason: Literal["INCOMPLETE"] | None
    entry: ConditionReferencePrice
    close_1h: ConditionClose
    anchor: ConditionRelativeAnchor
    windows: dict[ConditionWindow, ConditionRelativeWindow]
    pre_event_runup: ConditionPreEventRunup
    repricing: ConditionRepricing
    context: ConditionMarketContextInputs
    outcome_btc_reference: ConditionReferencePrice
    outcome_ranges: ConditionComputedOutcomeRanges | UnrequestedConditionOutcomeRanges

    @model_validator(mode="after")
    def actual_reference_and_window_scope(self) -> Self:
        if (self.reference_instrument_id is None) != (self.reference_reason is not None):
            raise ValueError("Missing relative reference selection requires its actual coverage gap")
        eligible = {
            iid: coverage.covered_volume_native
            for iid, coverage in self.reference_coverage.items()
            if coverage.complete
        }
        if self.reference_instrument_id is None:
            if eligible:
                raise ValueError("Known complete reference candidates cannot be discarded as missing")
        else:
            if self.reference_instrument_id not in eligible:
                raise ValueError("A selected reference requires actual complete native-volume coverage")
            chosen = eligible[self.reference_instrument_id]
            if chosen is None or any(volume is not None and volume > chosen for volume in eligible.values()):
                raise ValueError(
                    "Reference selection must preserve the actual greatest covered native volume"
                )
        if set(self.windows) != {"W15M", "W1H", "W4H", "W24H", "EVENT"}:
            raise ValueError("Relative graphs require the actual five computed windows")
        cutoff = self.windows["EVENT"].interval.cutoff
        for name, window in self.windows.items():
            if name != window.interval.window or window.interval.cutoff != cutoff:
                raise ValueError("Relative windows must preserve their actual shared cutoff and identity")
            if window.reference_instrument_id != self.reference_instrument_id:
                raise ValueError("Relative windows must preserve their actual selected price reference")
        durations = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400}
        event_window = self.windows["EVENT"]
        for name, window in self.windows.items():
            if (
                name != "EVENT"
                and (window.interval.cutoff - window.interval.start).total_seconds() != durations[name]
            ):
                raise ValueError("A named relative window must preserve its actual fixed duration")
            if (
                window.measure != event_window.measure
                or window.btc_beta != event_window.btc_beta
                or window.eth_beta != event_window.eth_beta
            ):
                raise ValueError(
                    "Relative windows must preserve their shared actual benchmark and beta estimates"
                )
        if self.entry.instrument_id != self.reference_instrument_id:
            raise ValueError("Entry price must preserve the actual selected reference instrument")
        if self.close_1h.reference_instrument_id != self.reference_instrument_id:
            raise ValueError("Hourly close must preserve the actual selected reference instrument")
        if isinstance(self.entry, ObservedConditionReferencePrice):
            if self.entry.observed_at.value is None:
                raise ValueError("Observed entry price requires its actual observation clock")
            entry_at = parse_iso_time(self.entry.observed_at.value)
            if entry_at > cutoff or self.entry.source == "LAST_TRADE" and entry_at == cutoff:
                raise ValueError(
                    "Entry reference must preserve its actual eligible predecessor or candle close"
                )
        if isinstance(self.close_1h, ObservedConditionClose):
            if self.close_1h.observed_at.value is None:
                raise ValueError("Hourly close requires its actual observation clock")
            close_at = parse_iso_time(self.close_1h.observed_at.value)
            if close_at > cutoff or close_at.minute != 0 or close_at.second != 0 or close_at.microsecond != 0:
                raise ValueError("Hourly close requires an actual completed hour boundary")
        if self.context.rv24_bound != cutoff.replace(minute=0, second=0, microsecond=0):
            raise ValueError("Market context must preserve its actual completed-hour calculation bound")
        if self.anchor.price.instrument_id != self.reference_instrument_id:
            raise ValueError("Anchor price must preserve the actual selected reference instrument")
        anchor = self.anchor.instant()
        if event_window.raw.value is not None and (
            not isinstance(self.anchor.price, ObservedConditionReferencePrice)
            or not isinstance(self.entry, ObservedConditionReferencePrice)
        ):
            raise ValueError("An observed event return requires actual anchor and cutoff reference prices")
        if self.windows["EVENT"].interval.start != anchor or self.pre_event_runup.cutoff != anchor:
            raise ValueError(
                "Event-relative calculations must preserve the actual anchor or analysis boundary"
            )
        if isinstance(self.anchor.price, ObservedConditionReferencePrice) and (
            self.anchor.price.observed_at.value is None
            or parse_iso_time(self.anchor.price.observed_at.value) >= anchor
        ):
            raise ValueError("Anchor reference trade must strictly precede the actual anchor boundary")
        day_window = self.windows["W24H"]
        runup = self.pre_event_runup
        expected_percentile = runup.value.value is not None and day_window.history.sample_count > 0
        if (runup.percentile is not None) != expected_percentile:
            raise ValueError("Runup percentile requires its actual return and preceding historical samples")
        bounds = day_window.normal_range.bounds
        value = runup.value.value
        _condition_predicate_matches(
            runup.beyond_normal,
            None if bounds is None or value is None else value < bounds.lower or value > bounds.upper,
        )
        if isinstance(self.outcome_ranges, ConditionComputedOutcomeRanges):
            for baseline in self.outcome_ranges.by_horizon.values():
                if baseline.cutoff != anchor or baseline.measure != event_window.measure:
                    raise ValueError(
                        "Outcome baselines must preserve their actual pre-event history and measure"
                    )
        for iid, coverage in self.reference_coverage.items():
            for group in coverage.boundary_segments:
                if any(aggregate.instrument_id != iid for aggregate in group.aggregates):
                    raise ValueError(
                        "Reference coverage cannot borrow another instrument's boundary evidence"
                    )
        if isinstance(self.repricing, DeclaredConditionRepricing):
            if not isinstance(self.anchor.origin, ConditionDeclaredEventAnchor):
                raise ValueError("Directional event repricing requires a real declared event anchor")
            event, day = self.windows["EVENT"], self.windows["W24H"]
            if (
                self.repricing.event_return.value != event.relative_return()
                or self.repricing.event_range != event.normal_range
                or self.repricing.day_range != day.normal_range
            ):
                raise ValueError(
                    "Repricing inputs must preserve the actual selected relative returns and baselines"
                )
        elif isinstance(self.anchor.origin, ConditionAnalysisWindowStart) and not isinstance(
            self.repricing, UnavailableConditionEventRepricing
        ):
            raise ValueError("Missing event anchor must remain explicitly unavailable for event repricing")
        elif isinstance(self.anchor.origin, ConditionDeclaredEventAnchor) and isinstance(
            self.repricing, UnavailableConditionEventRepricing
        ):
            raise ValueError("A real captured event anchor cannot be relabelled as missing")
        return self


class ConditionCoveredBookRange(DomainModel):
    percent: float | None = Field(ge=0)
    reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_range_metadata(self) -> Self:
        if (self.percent is None) != (self.reason is not None):
            raise ValueError("Missing book range coverage requires its actual metadata gap")
        return self


class ConditionUSDDepthAmount(DomainModel):
    value: Decimal | None = Field(ge=0)
    reason: Literal["INCOMPLETE", "DEPTH_BAND_NOT_COVERED"] | None

    @model_validator(mode="after")
    def actual_depth_availability(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("Missing USD depth requires its actual coverage or conversion gap")
        return self


class ConditionNativeDepthAmount(DomainModel):
    value: Decimal | None = Field(ge=0)
    reason: Literal["INCOMPLETE", "DEPTH_BAND_NOT_COVERED"] | None

    @model_validator(mode="after")
    def actual_depth_availability(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("Missing native depth requires its actual coverage or quantity gap")
        return self


class ConditionBookSideDepth(DomainModel):
    usd: ConditionUSDDepthAmount
    native: ConditionNativeDepthAmount

    @model_validator(mode="after")
    def matching_observed_zero(self) -> Self:
        if (
            self.usd.value is not None
            and self.native.value is not None
            and (self.usd.value == 0) != (self.native.value == 0)
        ):
            raise ValueError("Known USD and native book depth must preserve the same zero-quantity fact")
        return self


class ConditionImpactTier(DomainModel):
    notional_usd: Decimal | None = Field(ge=0)
    notional_reason: Literal["INCOMPLETE"] | None
    execution: BookWalkResult | None
    execution_reason: Literal["INCOMPLETE", "ZERO_NOTIONAL"] | None

    @model_validator(mode="after")
    def actual_execution_inputs(self) -> Self:
        if (self.notional_usd is None) != (self.notional_reason is not None):
            raise ValueError("Missing impact-tier notional requires its actual volume-history gap")
        if self.notional_usd is None:
            if self.execution is not None or self.execution_reason != "INCOMPLETE":
                raise ValueError("An unknown notional cannot produce an execution result")
        elif self.notional_usd == 0:
            if self.execution is not None or self.execution_reason != "ZERO_NOTIONAL":
                raise ValueError("A known zero notional cannot produce a positive-order execution result")
        elif self.execution is None:
            if self.execution_reason != "INCOMPLETE":
                raise ValueError("A missing impact execution requires its actual market-input gap")
        else:
            execution = self.execution
            if self.execution_reason is not None or execution.side != "ask":
                raise ValueError("An impact tier requires its actual ask execution result")
            if execution.complete:
                if (
                    execution.slippage_bps is None
                    or execution.reason is not None
                    or execution.filled_native_quantity <= 0
                    or execution.filled_quote_notional <= 0
                ):
                    raise ValueError("A completed book walk requires actual fills and signed slippage")
            elif (
                execution.slippage_bps is not None
                or execution.reason != "INSUFFICIENT_BOOK_DEPTH"
                or execution.remaining_quote_notional <= 0
            ):
                raise ValueError("An unfilled book walk cannot invent a complete slippage result")
        return self

    def slippage_bps(self) -> float | None:
        validated = type(self).model_validate(self)
        return None if validated.execution is None else validated.execution.slippage_bps


class ConditionImpactTiers(DomainModel):
    T1: ConditionImpactTier
    T2: ConditionImpactTier
    T3: ConditionImpactTier


class ConditionDepthSampleCohort(DomainModel):
    observation_count: int = Field(gt=0)
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class ObservedConditionDepthMean(DomainModel):
    status: Literal["OBSERVED"]
    value: Decimal = Field(ge=0)
    cohort: ConditionDepthSampleCohort


class UnavailableConditionDepthMean(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["INCOMPLETE"]


type ConditionDepthMean = ObservedConditionDepthMean | UnavailableConditionDepthMean


class ConditionPreBookSideDepth(DomainModel):
    usd: ConditionDepthMean
    native: ConditionDepthMean

    @model_validator(mode="after")
    def actual_mean_sample_sets(self) -> Self:
        if (
            isinstance(self.usd, ObservedConditionDepthMean)
            and isinstance(self.native, ObservedConditionDepthMean)
            and self.usd.cohort.fingerprint == self.native.cohort.fingerprint
        ):
            if self.usd.cohort.observation_count != self.native.cohort.observation_count:
                raise ValueError(
                    "Matching depth sample identities must preserve their actual observation count"
                )
            if (self.usd.value == 0) != (self.native.value == 0):
                raise ValueError("Matching USD/native mean cohorts must preserve the same zero-quantity fact")
        return self


class ComputedConditionPreDepthSides(DomainModel):
    status: Literal["COMPUTED"]
    bid: ConditionPreBookSideDepth
    ask: ConditionPreBookSideDepth
    paired_total_usd: ConditionDepthMean

    @model_validator(mode="after")
    def actual_paired_sample_set(self) -> Self:
        bid, ask, paired = self.bid.usd, self.ask.usd, self.paired_total_usd
        if isinstance(paired, ObservedConditionDepthMean):
            if not isinstance(bid, ObservedConditionDepthMean) or not isinstance(
                ask, ObservedConditionDepthMean
            ):
                raise ValueError("A paired depth mean requires actual known side samples")
            if paired.cohort.observation_count > min(
                bid.cohort.observation_count, ask.cohort.observation_count
            ):
                raise ValueError("Paired depth samples cannot exceed either actual side cohort")
        if (
            isinstance(bid, ObservedConditionDepthMean)
            and isinstance(ask, ObservedConditionDepthMean)
            and bid.cohort.fingerprint == ask.cohort.fingerprint
            and (
                bid.cohort != ask.cohort
                or not isinstance(paired, ObservedConditionDepthMean)
                or paired.cohort != bid.cohort
                or paired.value != _sum_native(bid.value, ask.value)
            )
        ):
            raise ValueError(
                "A paired depth mean must preserve its actual shared observation cohort and amounts"
            )
        return self


class UnavailableConditionPreDepthSides(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_EVENT_ANCHOR"]


class ConditionLiquidityVenue(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    observed_at: Timestamp
    integrity_ok: TrueFlag
    bid_range: ConditionCoveredBookRange
    ask_range: ConditionCoveredBookRange
    spread_bps: float | None = Field(ge=0)
    spread_reason: Literal["INCOMPLETE"] | None
    bid: ConditionBookSideDepth
    ask: ConditionBookSideDepth
    pre_depth: ComputedConditionPreDepthSides | UnavailableConditionPreDepthSides
    impact_tiers: ConditionImpactTiers
    quote_asset: str = Field(min_length=1, pattern=r"\S")
    quote_valuation: QuoteUsdValuation

    @model_validator(mode="after")
    def actual_book_evidence(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("A selected liquidity book requires its actual source observation clock")
        if (self.spread_bps is None) != (self.spread_reason is not None):
            raise ValueError("Missing book spread requires its actual market-input gap")
        if self.quote_asset == "USD" and self.quote_valuation.basis != "USD_UNIT":
            raise ValueError("A USD quote uses its exact unit identity rather than an unknown exchange rate")
        if self.quote_valuation.basis == "USD_UNIT" and self.quote_asset != "USD":
            raise ValueError("A non-USD quote cannot use USD's fixed unit identity")
        if (
            any(
                tier.execution is not None
                for tier in (self.impact_tiers.T1, self.impact_tiers.T2, self.impact_tiers.T3)
            )
            and self.quote_valuation.rate is None
        ):
            raise ValueError("A USD impact notional requires actual quote/USD valuation before execution")
        return self


class ObservedConditionDepthDrop(DomainModel):
    status: Literal["OBSERVED"]
    value: float = Field(le=1)


class UnavailableConditionDepthDrop(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["ZERO_BASELINE", "INCOMPLETE", "NOT_ESTIMABLE", "NO_EVENT_ANCHOR", "ZERO_NET_FLOW"]


class ObservedConditionDepthCohort(DomainModel):
    status: Literal["OBSERVED"]
    venues: tuple[Annotated[str, Field(min_length=1, pattern=r"\S")], ...] = Field(min_length=1)
    current_usd: Decimal = Field(ge=0)
    previous_usd: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def actual_common_venues(self) -> Self:
        if len(self.venues) != len(set(self.venues)):
            raise ValueError("A depth comparison cohort cannot duplicate a venue")
        return self

    def drop(self) -> ObservedConditionDepthDrop | UnavailableConditionDepthDrop:
        validated = type(self).model_validate(self)
        if validated.previous_usd == 0:
            return UnavailableConditionDepthDrop(status="UNAVAILABLE", value=None, reason="ZERO_BASELINE")
        with localcontext() as context:
            context.prec = 128
            value = float(1 - validated.current_usd / validated.previous_usd)
        if not isfinite(value):
            return UnavailableConditionDepthDrop(status="UNAVAILABLE", value=None, reason="NOT_ESTIMABLE")
        return ObservedConditionDepthDrop(status="OBSERVED", value=value)

    def majority_lost(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        if validated.previous_usd == 0:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="ZERO_BASELINE")
        return ConditionObservedPredicate(
            status="OBSERVED",
            value=_sum_native(validated.current_usd, validated.current_usd) < validated.previous_usd,
        )


class UnavailableConditionDepthCohort(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_COMMON_VENUES", "NO_EVENT_ANCHOR"]


class ObservedConditionLiquidityFlow(DomainModel):
    status: Literal["OBSERVED"]
    unit: Literal["NATIVE", "USD"]
    value: Decimal

    def hit_side(self) -> Literal["bid", "ask"] | None:
        validated = type(self).model_validate(self)
        return "ask" if validated.value > 0 else "bid" if validated.value < 0 else None


class UnavailableConditionLiquidityFlow(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["INCOMPLETE"]


class ConditionLiquidityNetFlow(DomainModel):
    native: Decimal | None
    native_reason: Literal["INCOMPLETE"] | None
    usd: Decimal | None
    usd_reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_units(self) -> Self:
        if (self.native is None) != (self.native_reason is not None) or (self.usd is None) != (
            self.usd_reason is not None
        ):
            raise ValueError("Missing net flow requires the actual unavailable unit cause")
        return self

    def comparison_flow(self) -> ObservedConditionLiquidityFlow | UnavailableConditionLiquidityFlow:
        validated = type(self).model_validate(self)
        if validated.native is not None:
            return ObservedConditionLiquidityFlow(status="OBSERVED", unit="NATIVE", value=validated.native)
        if validated.usd is not None:
            return ObservedConditionLiquidityFlow(status="OBSERVED", unit="USD", value=validated.usd)
        return UnavailableConditionLiquidityFlow(status="UNAVAILABLE", value=None, reason="INCOMPLETE")


class ConditionDepthLowerBound(DomainModel):
    unit: Literal["NATIVE", "USD"]
    value: Decimal | None = Field(ge=0)
    reason: Literal["INCOMPLETE", "NO_VENUES"] | None
    considered_venues: tuple[Annotated[str, Field(min_length=1, pattern=r"\S")], ...]
    covered_venues: tuple[Annotated[str, Field(min_length=1, pattern=r"\S")], ...]

    @model_validator(mode="after")
    def actual_covered_set(self) -> Self:
        if len(self.considered_venues) != len(set(self.considered_venues)) or len(self.covered_venues) != len(
            set(self.covered_venues)
        ):
            raise ValueError("A depth lower bound cannot duplicate a considered or covered venue")
        if not set(self.covered_venues) <= set(self.considered_venues):
            raise ValueError("A depth lower bound cannot borrow an unconsidered venue")
        if (self.value is not None) != bool(self.covered_venues):
            raise ValueError("A known depth lower bound requires actual covered venue amounts")
        if self.value is not None and self.reason is not None:
            raise ValueError("An observed depth lower bound cannot carry an absent-value reason")
        if self.value is None and self.reason != ("INCOMPLETE" if self.considered_venues else "NO_VENUES"):
            raise ValueError("Missing lower bound must distinguish absent venues from uncovered data")
        return self

    def fully_covered(self) -> bool:
        validated = type(self).model_validate(self)
        return bool(validated.considered_venues) and set(validated.covered_venues) == set(
            validated.considered_venues
        )

    def thinness(self, notional_usd: Decimal | None) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        if validated.unit != "USD":
            raise ValueError("A USD thinness test requires actual USD depth")
        if notional_usd is not None and (not notional_usd.is_finite() or notional_usd < 0):
            raise ValueError("A thinness test notional must be finite and nonnegative")
        if notional_usd == 0:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="ZERO_NOTIONAL")
        if validated.value is None or notional_usd is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        if validated.value >= notional_usd:
            return ConditionObservedPredicate(status="OBSERVED", value=False)
        if validated.fully_covered():
            return ConditionObservedPredicate(status="OBSERVED", value=True)
        return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")


class ConditionBookSideBounds(DomainModel):
    usd: ConditionDepthLowerBound
    native: ConditionDepthLowerBound

    @model_validator(mode="after")
    def actual_units_and_venues(self) -> Self:
        if self.usd.unit != "USD" or self.native.unit != "NATIVE":
            raise ValueError("Book side bounds must preserve their actual monetary and native units")
        if set(self.usd.considered_venues) != set(self.native.considered_venues):
            raise ValueError("Monetary and native side bounds must use the same actual considered venues")
        return self


class ConditionBookDepthBounds(DomainModel):
    bid: ConditionBookSideBounds
    ask: ConditionBookSideBounds

    @model_validator(mode="after")
    def actual_book_set(self) -> Self:
        if set(self.bid.usd.considered_venues) != set(self.ask.usd.considered_venues):
            raise ValueError("Bid and ask bounds must preserve the same actual considered books")
        return self


class UnavailableConditionPreDepthBounds(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_EVENT_ANCHOR"]


class ObservedConditionElasticity(DomainModel):
    status: Literal["OBSERVED"]
    bps_per_depth_unit: float


class UnavailableConditionElasticity(DomainModel):
    status: Literal["UNAVAILABLE"]
    bps_per_depth_unit: None
    reason: Literal["INCOMPLETE", "ZERO_NET_FLOW", "ZERO_BASELINE", "NOT_ESTIMABLE"]


class ConditionElasticityInputs(DomainModel):
    flow: ConditionLiquidityNetFlow
    raw_return: ConditionRelativeReturn
    price_beyond_normal: ConditionPredicate
    measurement_band_percent: float = Field(gt=0)
    current: ConditionBookDepthBounds
    previous: ConditionBookDepthBounds | UnavailableConditionPreDepthBounds
    average_hit_side_depth_usd: ConditionUSDDepthAmount

    def bps_per_depth_unit(self) -> ObservedConditionElasticity | UnavailableConditionElasticity:
        validated = type(self).model_validate(self)
        net, price, depth = (
            validated.flow.usd,
            validated.raw_return.value,
            validated.average_hit_side_depth_usd.value,
        )
        if net is None or price is None or depth is None:
            return UnavailableConditionElasticity(
                status="UNAVAILABLE", bps_per_depth_unit=None, reason="INCOMPLETE"
            )
        if net == 0:
            return UnavailableConditionElasticity(
                status="UNAVAILABLE", bps_per_depth_unit=None, reason="ZERO_NET_FLOW"
            )
        if depth == 0:
            return UnavailableConditionElasticity(
                status="UNAVAILABLE", bps_per_depth_unit=None, reason="ZERO_BASELINE"
            )
        with localcontext() as context:
            context.prec = 128
            denominator = float(net.copy_abs() / depth)
        if not isfinite(denominator) or denominator == 0:
            return UnavailableConditionElasticity(
                status="UNAVAILABLE", bps_per_depth_unit=None, reason="NOT_ESTIMABLE"
            )
        result = price * 10000 / denominator
        if not isfinite(result):
            return UnavailableConditionElasticity(
                status="UNAVAILABLE", bps_per_depth_unit=None, reason="NOT_ESTIMABLE"
            )
        return ObservedConditionElasticity(status="OBSERVED", bps_per_depth_unit=result)

    def weak_response(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        flow = validated.flow.comparison_flow()
        if isinstance(flow, UnavailableConditionLiquidityFlow):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        side = flow.hit_side()
        if side is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="ZERO_NET_FLOW")
        sides = validated.current.ask if side == "ask" else validated.current.bid
        bound = sides.native if flow.unit == "NATIVE" else sides.usd
        enough: ConditionPredicate
        if bound.value is None:
            enough = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        elif flow.value.copy_abs() < bound.value:
            enough = ConditionObservedPredicate(status="OBSERVED", value=False)
        elif bound.fully_covered():
            enough = ConditionObservedPredicate(status="OBSERVED", value=True)
        else:
            enough = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        raw = validated.raw_return.value
        small: ConditionPredicate = (
            ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
            if raw is None
            else ConditionObservedPredicate(
                status="OBSERVED",
                value=(raw if flow.value > 0 else -raw) < validated.measurement_band_percent / 100,
            )
        )
        return condition_predicates_all((enough, small))

    def excess_response(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        beyond = validated.price_beyond_normal
        if isinstance(beyond, ConditionObservedPredicate) and not beyond.value:
            return ConditionObservedPredicate(status="OBSERVED", value=False)
        if isinstance(validated.previous, UnavailableConditionPreDepthBounds):
            return condition_predicates_all(
                (beyond, ConditionUnavailablePredicate(status="UNAVAILABLE", reason="NO_EVENT_ANCHOR"))
            )
        flow = validated.flow.comparison_flow()
        if isinstance(flow, UnavailableConditionLiquidityFlow):
            return condition_predicates_all(
                (beyond, ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE"))
            )
        side = flow.hit_side()
        if side is None:
            return condition_predicates_all(
                (beyond, ConditionUnavailablePredicate(status="UNAVAILABLE", reason="ZERO_NET_FLOW"))
            )
        sides = validated.previous.ask if side == "ask" else validated.previous.bid
        bound = sides.native if flow.unit == "NATIVE" else sides.usd
        comparison: ConditionPredicate
        if bound.value is None:
            comparison = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        elif flow.value.copy_abs() < bound.value:
            comparison = ConditionObservedPredicate(status="OBSERVED", value=True)
        elif bound.fully_covered():
            comparison = ConditionObservedPredicate(status="OBSERVED", value=False)
        else:
            comparison = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return condition_predicates_all((beyond, comparison))


def condition_predicates_all(predicates: tuple[ConditionPredicate, ...]) -> ConditionPredicate:
    """Only an observed false necessary condition concludes a partial conjunction."""
    if not predicates:
        raise ValueError("A financial conjunction requires actual evaluated component predicates")
    validated = tuple(type(predicate).model_validate(predicate) for predicate in predicates)
    if any(
        isinstance(predicate, ConditionObservedPredicate) and not predicate.value for predicate in validated
    ):
        return ConditionObservedPredicate(status="OBSERVED", value=False)
    for predicate in validated:
        if isinstance(predicate, ConditionUnavailablePredicate):
            return predicate
    return ConditionObservedPredicate(status="OBSERVED", value=True)


def condition_predicates_any(predicates: tuple[ConditionPredicate, ...]) -> ConditionPredicate:
    """Only an observed true sufficient condition concludes a partial disjunction."""
    if not predicates:
        raise ValueError("A financial disjunction requires actual evaluated component predicates")
    validated = tuple(type(predicate).model_validate(predicate) for predicate in predicates)
    if any(isinstance(predicate, ConditionObservedPredicate) and predicate.value for predicate in validated):
        return ConditionObservedPredicate(status="OBSERVED", value=True)
    for predicate in validated:
        if isinstance(predicate, ConditionUnavailablePredicate):
            return predicate
    return ConditionObservedPredicate(status="OBSERVED", value=False)


class ConditionReplenishmentScope(DomainModel):
    interval: ConditionInterval
    refill_window_s: float = Field(gt=0)
    resilience_window_s: float = Field(gt=0)
    basis: Literal["CAPTURED_BOOK_AND_FLOW_SAMPLES"]
    window_origin: Literal["FLOW_START"]


class ConditionReplenishmentBurst(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    flow_instrument_id: str = Field(min_length=1, pattern=r"\S")
    book_instrument_id: str = Field(min_length=1, pattern=r"\S")
    flow_start: Timestamp
    flow_end: datetime
    last_trade_at: Timestamp
    baseline_bid_depth_usd: Decimal = Field(gt=0)
    net_sell_usd: Decimal = Field(gt=0)
    baseline_low_price: Decimal | None = Field(gt=0)
    baseline_low_reason: Literal["INCOMPLETE"] | None
    recovery_at: Timestamp
    flow_start_to_recovery_s: float | None = Field(gt=0)
    followup_book_sample_count: int = Field(ge=0)
    post_flow_book_sample_count: int = Field(ge=0)
    lowest_post_flow_price: Decimal | None = Field(gt=0)
    post_flow_price_sample_count: int = Field(ge=0)
    post_flow_price_start: datetime | None
    post_flow_price_cutoff: datetime | None

    @field_validator("flow_end", "post_flow_price_start", "post_flow_price_cutoff")
    @classmethod
    def aware_boundary(cls, value: datetime | None) -> datetime | None:
        return None if value is None else parse_iso_time(value)

    @model_validator(mode="after")
    def actual_burst_and_followup(self) -> Self:
        if self.flow_start.value is None:
            raise ValueError("A replenishment burst requires its actual source observation clock")
        start = parse_iso_time(self.flow_start.value)
        if self.flow_end <= start:
            raise ValueError("A burst requires its actual positive aggregate flow interval")
        last = self.last_trade_at.value
        if last is not None and not start <= parse_iso_time(last) < self.flow_end:
            raise ValueError("A captured last trade must belong to its actual flow aggregate interval")
        if self.post_flow_book_sample_count > self.followup_book_sample_count:
            raise ValueError("Post-flow book samples cannot exceed actual qualified followup samples")
        if self.net_sell_usd < self.baseline_bid_depth_usd:
            raise ValueError("An observed burst must actually reach its positive baseline bid depth")
        if (self.baseline_low_price is None) != (self.baseline_low_reason is not None):
            raise ValueError("Missing burst price requires its actual source-price gap")
        if (self.recovery_at.value is None) != (self.flow_start_to_recovery_s is None):
            raise ValueError("An observed recovery duration requires its actual recovery clock")
        if self.recovery_at.value is not None:
            elapsed = (
                parse_iso_time(self.recovery_at.value) - parse_iso_time(self.flow_start.value)
            ).total_seconds()
            after = parse_iso_time(self.recovery_at.value)
            follows_flow = (
                after > parse_iso_time(last)
                if last is not None and self.last_trade_at.precision == "EXACT"
                else after >= self.flow_end
            )
            if not follows_flow:
                raise ValueError(
                    "A recovery must follow the actual exact last trade or complete flow interval"
                )
            if self.flow_start_to_recovery_s != elapsed or self.post_flow_book_sample_count == 0:
                raise ValueError(
                    "Observed recovery must preserve the actual qualified book sample and elapsed time"
                )
        if (self.lowest_post_flow_price is not None) != (self.post_flow_price_sample_count > 0):
            raise ValueError("Observed future price minimum requires actual qualified price samples")
        if self.post_flow_price_sample_count == 0:
            if self.post_flow_price_start is not None or self.post_flow_price_cutoff is not None:
                raise ValueError("Missing post-flow price samples cannot invent observation interval bounds")
        elif (
            self.post_flow_price_start is None
            or self.post_flow_price_cutoff is None
            or not self.flow_end <= self.post_flow_price_start < self.post_flow_price_cutoff
        ):
            raise ValueError("Post-flow price samples require actual non-overlapping interval bounds")
        return self

    def validate_scope(self, scope: ConditionReplenishmentScope) -> None:
        validated = type(self).model_validate(self)
        actual = ConditionReplenishmentScope.model_validate(scope)
        if validated.flow_start.value is None:
            raise ValueError("A burst requires its actual occurrence clock")
        at = parse_iso_time(validated.flow_start.value)
        if not actual.interval.start <= at < validated.flow_end <= actual.interval.cutoff:
            raise ValueError("A replenishment burst must belong to its actual analysis interval")
        if validated.recovery_at.value is not None:
            recovery = parse_iso_time(validated.recovery_at.value)
            if recovery > actual.interval.cutoff or (recovery - at).total_seconds() > actual.refill_window_s:
                raise ValueError("A recovery observation must belong to the actual captured refill window")
        if validated.post_flow_price_cutoff is not None and (
            validated.post_flow_price_cutoff > actual.interval.cutoff
            or (validated.post_flow_price_cutoff - at).total_seconds() > actual.resilience_window_s
        ):
            raise ValueError("Post-flow price samples must belong to the actual captured resilience window")

    def replenished(self, scope: ConditionReplenishmentScope) -> ConditionPredicate:
        self.validate_scope(scope)
        at_value = self.flow_start.value
        if at_value is None:
            raise ValueError("An observed burst requires its actual source clock")
        if self.recovery_at.value is not None:
            return ConditionObservedPredicate(status="OBSERVED", value=True)
        elapsed = (scope.interval.cutoff - parse_iso_time(at_value)).total_seconds()
        if elapsed >= scope.refill_window_s and self.post_flow_book_sample_count > 0:
            return ConditionObservedPredicate(status="OBSERVED", value=False)
        return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")

    def resilient(self, scope: ConditionReplenishmentScope) -> ConditionPredicate:
        self.validate_scope(scope)
        baseline, future = self.baseline_low_price, self.lowest_post_flow_price
        if baseline is None or future is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        if future < baseline:
            return ConditionObservedPredicate(status="OBSERVED", value=False)
        at = self.flow_start.value
        if at is None:
            raise ValueError("An observed burst requires its actual source clock")
        if (scope.interval.cutoff - parse_iso_time(at)).total_seconds() >= scope.resilience_window_s:
            return ConditionObservedPredicate(status="OBSERVED", value=True)
        return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")


class ComputedConditionReplenishmentSummary(DomainModel):
    status: Literal["COMPUTED"]
    observed_bursts: int = Field(ge=0)
    observed_replenished: int = Field(ge=0)
    observed_resilient_replenished: int = Field(ge=0)
    pending_replenishment: int = Field(ge=0)
    pending_resilience: int = Field(ge=0)
    median_flow_start_to_recovery_s: float | None = Field(gt=0)
    median_recovery_reason: Literal["NO_OBSERVED_RECOVERY"] | None

    @model_validator(mode="after")
    def actual_detected_counts(self) -> Self:
        if (
            self.observed_replenished > self.observed_bursts
            or self.observed_resilient_replenished > self.observed_replenished
        ):
            raise ValueError("Replenishment counts cannot exceed actual detected burst/recovery counts")
        if (
            self.observed_replenished + self.pending_replenishment > self.observed_bursts
            or self.observed_resilient_replenished + self.pending_resilience > self.observed_bursts
        ):
            raise ValueError("Pending replenishment counts must belong to actual detected bursts")
        if (self.median_flow_start_to_recovery_s is None) != (self.median_recovery_reason is not None):
            raise ValueError("Missing recovery median requires an actual absence of observed recoveries")
        if (self.median_flow_start_to_recovery_s is not None) != (self.observed_replenished > 0):
            raise ValueError("Recovery median requires actual observed recovery clocks")
        return self


class UnavailableConditionReplenishmentSummary(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_EVALUABLE_CANDIDATES"]


class ConditionReplenishmentScan(DomainModel):
    scope: ConditionReplenishmentScope
    considered_candidate_count: int = Field(ge=0)
    observed_non_burst_count: int = Field(ge=0)
    unqualified_candidate_counts: dict[Literal["INCOMPLETE", "ZERO_BASELINE"], Annotated[int, Field(gt=0)]]
    bursts: tuple[ConditionReplenishmentBurst, ...]

    @model_validator(mode="after")
    def actual_candidate_accounting(self) -> Self:
        if self.considered_candidate_count != self.observed_non_burst_count + len(self.bursts) + sum(
            self.unqualified_candidate_counts.values()
        ):
            raise ValueError("Replenishment candidates must preserve actual tested and unavailable counts")
        identities = [
            (burst.venue, burst.flow_instrument_id, burst.flow_start.value) for burst in self.bursts
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("A replenishment scan cannot duplicate a detected flow burst")
        for burst in self.bursts:
            burst.validate_scope(self.scope)
        return self

    def summary(self) -> ComputedConditionReplenishmentSummary | UnavailableConditionReplenishmentSummary:
        validated = type(self).model_validate(self)
        if not validated.bursts and validated.observed_non_burst_count == 0:
            return UnavailableConditionReplenishmentSummary(
                status="UNAVAILABLE", reason="NO_EVALUABLE_CANDIDATES"
            )
        replenished = tuple(burst.replenished(validated.scope) for burst in validated.bursts)
        resilience = tuple(burst.resilient(validated.scope) for burst in validated.bursts)
        recoveries = [
            burst.flow_start_to_recovery_s
            for burst in validated.bursts
            if burst.flow_start_to_recovery_s is not None
        ]
        return ComputedConditionReplenishmentSummary(
            status="COMPUTED",
            observed_bursts=len(validated.bursts),
            observed_replenished=sum(
                isinstance(value, ConditionObservedPredicate) and value.value for value in replenished
            ),
            observed_resilient_replenished=sum(
                isinstance(left, ConditionObservedPredicate)
                and left.value
                and isinstance(right, ConditionObservedPredicate)
                and right.value
                for left, right in zip(replenished, resilience, strict=True)
            ),
            pending_replenishment=sum(
                isinstance(value, ConditionUnavailablePredicate) for value in replenished
            ),
            pending_resilience=sum(isinstance(value, ConditionUnavailablePredicate) for value in resilience),
            median_flow_start_to_recovery_s=median(recoveries) if recoveries else None,
            median_recovery_reason=None if recoveries else "NO_OBSERVED_RECOVERY",
        )


class ConditionLiquidityDepthTotals(DomainModel):
    bid: ConditionUSDDepthAmount
    ask: ConditionUSDDepthAmount
    combined: ConditionUSDDepthAmount


class ConditionPreDepthWindow(DomainModel):
    origin: ConditionDeclaredEventAnchor | ConditionAnalysisWindowStart
    start: datetime
    cutoff: datetime

    @field_validator("start", "cutoff")
    @classmethod
    def aware_boundary(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_preceding_hour(self) -> Self:
        if (self.cutoff - self.start).total_seconds() != 3600:
            raise ValueError("A pre-depth baseline requires the actual preceding hour")
        origin = self.origin
        if isinstance(origin, ConditionAnalysisWindowStart):
            anchor = origin.at
        else:
            if origin.at.value is None:
                raise ValueError("A declared pre-depth anchor requires its actual captured clock")
            anchor = parse_iso_time(origin.at.value)
        if self.cutoff != anchor:
            raise ValueError("The pre-depth baseline must preserve its actual anchor or analysis boundary")
        return self


class ComputedConditionPreDepth(DomainModel):
    status: Literal["COMPUTED"]
    window: ConditionPreDepthWindow
    totals: ConditionLiquidityDepthTotals
    bid_cohort: ObservedConditionDepthCohort | UnavailableConditionDepthCohort
    ask_cohort: ObservedConditionDepthCohort | UnavailableConditionDepthCohort


class UnavailableConditionPreDepth(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_EVENT_ANCHOR"]


class ObservedConditionLiquidityState(DomainModel):
    status: Literal["OBSERVED"]
    value: Literal["VACUUM", "THIN", "NORMAL"]


class UnavailableConditionLiquidityState(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: ConditionPredicateUnavailableReason


class ConditionLiquidityValues(DomainModel):
    interval: ConditionInterval
    measurement_band_percent: float = Field(gt=0)
    per_venue: tuple[ConditionLiquidityVenue, ...]
    current_depth: ConditionLiquidityDepthTotals
    previous_depth: ComputedConditionPreDepth | UnavailableConditionPreDepth
    elasticity: ConditionElasticityInputs
    replenishment: ConditionReplenishmentScan

    @model_validator(mode="after")
    def actual_book_and_policy_scope(self) -> Self:
        if self.measurement_band_percent not in (0.5, 1.0, 2.0):
            raise ValueError("Liquidity depth requires an actually supported measurement band")
        venues = tuple(venue.venue for venue in self.per_venue)
        if len(venues) != len(set(venues)):
            raise ValueError("A liquidity window cannot duplicate its selected book venue")
        for venue in self.per_venue:
            if (
                venue.observed_at.value is None
                or parse_iso_time(venue.observed_at.value) > self.interval.cutoff
            ):
                raise ValueError("A liquidity window cannot borrow a future or unclocked book")
            valuation_at = venue.quote_valuation.as_of
            if valuation_at is not None and valuation_at > self.interval.cutoff:
                raise ValueError("A liquidity window cannot borrow future quote conversion evidence")
            for coverage, amounts in ((venue.bid_range, venue.bid), (venue.ask_range, venue.ask)):
                if coverage.percent is None or coverage.percent < self.measurement_band_percent:
                    reason = "INCOMPLETE" if coverage.percent is None else "DEPTH_BAND_NOT_COVERED"
                    if (
                        amounts.usd.value is not None
                        or amounts.native.value is not None
                        or amounts.usd.reason != reason
                        or amounts.native.reason != reason
                    ):
                        raise ValueError("Depth band values require their actual declared side coverage")
        bid_values = tuple(venue.bid.usd.value for venue in self.per_venue)
        ask_values = tuple(venue.ask.usd.value for venue in self.per_venue)
        paired_values = tuple(
            None
            if venue.bid.usd.value is None or venue.ask.usd.value is None
            else _sum_native(venue.bid.usd.value, venue.ask.usd.value)
            for venue in self.per_venue
        )
        _condition_depth_total_matches(self.current_depth.bid, _condition_sum_known_depth(bid_values))
        _condition_depth_total_matches(self.current_depth.ask, _condition_sum_known_depth(ask_values))
        _condition_depth_total_matches(self.current_depth.combined, _condition_sum_known_depth(paired_values))
        _condition_lower_bound_matches(
            self.elasticity.current.bid.usd,
            tuple((venue.venue, venue.bid.usd.value) for venue in self.per_venue),
        )
        _condition_lower_bound_matches(
            self.elasticity.current.ask.usd,
            tuple((venue.venue, venue.ask.usd.value) for venue in self.per_venue),
        )
        _condition_lower_bound_matches(
            self.elasticity.current.bid.native,
            tuple((venue.venue, venue.bid.native.value) for venue in self.per_venue),
        )
        _condition_lower_bound_matches(
            self.elasticity.current.ask.native,
            tuple((venue.venue, venue.ask.native.value) for venue in self.per_venue),
        )
        if self.elasticity.measurement_band_percent != self.measurement_band_percent:
            raise ValueError("Elasticity must preserve the actual liquidity measurement band")
        if self.replenishment.scope.interval != self.interval:
            raise ValueError("Replenishment scan must preserve the actual liquidity analysis interval")
        if any(burst.venue not in venues for burst in self.replenishment.bursts):
            raise ValueError(
                "A replenishment scan cannot borrow a book venue outside its actual liquidity scope"
            )
        if self.per_venue:
            first = self.per_venue[0].impact_tiers
            for venue in self.per_venue:
                for expected, actual in (
                    (first.T1, venue.impact_tiers.T1),
                    (first.T2, venue.impact_tiers.T2),
                    (first.T3, venue.impact_tiers.T3),
                ):
                    if (
                        expected.notional_usd != actual.notional_usd
                        or expected.notional_reason != actual.notional_reason
                    ):
                        raise ValueError(
                            "Liquidity venue executions must preserve their shared actual impact test sizes"
                        )
        if isinstance(self.previous_depth, UnavailableConditionPreDepth):
            if not isinstance(self.elasticity.previous, UnavailableConditionPreDepthBounds) or any(
                not isinstance(venue.pre_depth, UnavailableConditionPreDepthSides) for venue in self.per_venue
            ):
                raise ValueError("Missing pre-depth anchor cannot retain invented baseline observations")
        else:
            previous = self.elasticity.previous
            if not isinstance(previous, ConditionBookDepthBounds):
                raise ValueError("A computed pre-depth window requires its actual comparison bounds")
            pre_bid: list[tuple[str, Decimal | None]] = []
            pre_ask: list[tuple[str, Decimal | None]] = []
            pre_bid_native: list[tuple[str, Decimal | None]] = []
            pre_ask_native: list[tuple[str, Decimal | None]] = []
            pre_paired: list[Decimal | None] = []
            for venue in self.per_venue:
                if not isinstance(venue.pre_depth, ComputedConditionPreDepthSides):
                    raise ValueError(
                        "A computed pre-depth baseline must preserve actual venue request selection"
                    )
                pre_bid.append((venue.venue, venue.pre_depth.bid.usd.value))
                pre_ask.append((venue.venue, venue.pre_depth.ask.usd.value))
                pre_bid_native.append((venue.venue, venue.pre_depth.bid.native.value))
                pre_ask_native.append((venue.venue, venue.pre_depth.ask.native.value))
                pre_paired.append(venue.pre_depth.paired_total_usd.value)
            _condition_depth_total_matches(
                self.previous_depth.totals.bid,
                _condition_sum_known_depth(tuple(value for _, value in pre_bid)),
            )
            _condition_depth_total_matches(
                self.previous_depth.totals.ask,
                _condition_sum_known_depth(tuple(value for _, value in pre_ask)),
            )
            _condition_depth_total_matches(
                self.previous_depth.totals.combined, _condition_sum_known_depth(tuple(pre_paired))
            )
            _condition_lower_bound_matches(previous.bid.usd, tuple(pre_bid))
            _condition_lower_bound_matches(previous.ask.usd, tuple(pre_ask))
            _condition_lower_bound_matches(previous.bid.native, tuple(pre_bid_native))
            _condition_lower_bound_matches(previous.ask.native, tuple(pre_ask_native))
            _condition_depth_cohort_matches(
                self.previous_depth.bid_cohort,
                tuple((venue.venue, venue.bid.usd.value) for venue in self.per_venue),
                tuple(pre_bid),
            )
            _condition_depth_cohort_matches(
                self.previous_depth.ask_cohort,
                tuple((venue.venue, venue.ask.usd.value) for venue in self.per_venue),
                tuple(pre_ask),
            )
        return self

    def _depth_majority_lost(self) -> ConditionPredicate:
        flow = self.elasticity.flow.comparison_flow()
        if isinstance(flow, UnavailableConditionLiquidityFlow):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        side = flow.hit_side()
        if side is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="ZERO_NET_FLOW")
        if isinstance(self.previous_depth, UnavailableConditionPreDepth):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="NO_EVENT_ANCHOR")
        cohort = self.previous_depth.ask_cohort if side == "ask" else self.previous_depth.bid_cohort
        if isinstance(cohort, UnavailableConditionDepthCohort):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return cohort.majority_lost()

    def _vacuum(self) -> ConditionPredicate:
        return condition_predicates_all(
            (
                self.elasticity.price_beyond_normal,
                self._depth_majority_lost(),
                self.elasticity.excess_response(),
            )
        )

    def vacuum(self) -> ConditionPredicate:
        return type(self).model_validate(self)._vacuum()

    def depth_majority_lost(self) -> ConditionPredicate:
        return type(self).model_validate(self)._depth_majority_lost()

    def depth_drop(self) -> ObservedConditionDepthDrop | UnavailableConditionDepthDrop:
        validated = type(self).model_validate(self)
        flow = validated.elasticity.flow.comparison_flow()
        if isinstance(flow, UnavailableConditionLiquidityFlow):
            return UnavailableConditionDepthDrop(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
        side = flow.hit_side()
        if side is None:
            return UnavailableConditionDepthDrop(status="UNAVAILABLE", value=None, reason="ZERO_NET_FLOW")
        if isinstance(validated.previous_depth, UnavailableConditionPreDepth):
            return UnavailableConditionDepthDrop(status="UNAVAILABLE", value=None, reason="NO_EVENT_ANCHOR")
        cohort = validated.previous_depth.ask_cohort if side == "ask" else validated.previous_depth.bid_cohort
        if isinstance(cohort, UnavailableConditionDepthCohort):
            return UnavailableConditionDepthDrop(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
        return cohort.drop()

    def _thinness(self) -> ConditionPredicate:
        if not self.per_venue:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="NO_VENUES")
        test = self.per_venue[0].impact_tiers.T2.notional_usd
        return condition_predicates_any(
            (self.elasticity.current.bid.usd.thinness(test), self.elasticity.current.ask.usd.thinness(test))
        )

    def thinness(self) -> ConditionPredicate:
        return type(self).model_validate(self)._thinness()

    def state(self) -> ObservedConditionLiquidityState | UnavailableConditionLiquidityState:
        validated = type(self).model_validate(self)
        vacuum = validated._vacuum()
        if isinstance(vacuum, ConditionUnavailablePredicate):
            return UnavailableConditionLiquidityState(status="UNAVAILABLE", value=None, reason=vacuum.reason)
        if vacuum.value:
            return ObservedConditionLiquidityState(status="OBSERVED", value="VACUUM")
        thin = validated._thinness()
        if isinstance(thin, ConditionUnavailablePredicate):
            return UnavailableConditionLiquidityState(status="UNAVAILABLE", value=None, reason=thin.reason)
        return ObservedConditionLiquidityState(status="OBSERVED", value="THIN" if thin.value else "NORMAL")


class BasicConditionLiquidity(ConditionLiquidityValues):
    kind: Literal["BASIC"]


def _condition_sum_known_depth(values: tuple[Decimal | None, ...]) -> Decimal | None:
    if not values:
        return None
    total = Decimal(0)
    for value in values:
        if value is None:
            return None
        total = _sum_native(total, value)
    return total


def _condition_depth_total_matches(actual: ConditionUSDDepthAmount, expected: Decimal | None) -> None:
    if actual.value != expected or expected is None and actual.reason != "INCOMPLETE":
        raise ValueError("Depth totals must preserve every actual required venue amount")


def _condition_lower_bound_matches(
    bound: ConditionDepthLowerBound, amounts: tuple[tuple[str, Decimal | None], ...]
) -> None:
    considered = {venue for venue, _ in amounts}
    covered = {venue for venue, amount in amounts if amount is not None}
    value = _condition_sum_known_depth(tuple(amount for _, amount in amounts if amount is not None))
    if (
        set(bound.considered_venues) != considered
        or set(bound.covered_venues) != covered
        or bound.value != value
    ):
        raise ValueError("Depth lower bounds must preserve actual considered/covered books and amounts")


def _condition_depth_cohort_matches(
    cohort: ObservedConditionDepthCohort | UnavailableConditionDepthCohort,
    current: tuple[tuple[str, Decimal | None], ...],
    previous: tuple[tuple[str, Decimal | None], ...],
) -> None:
    now = {venue: amount for venue, amount in current if amount is not None}
    before = {venue: amount for venue, amount in previous if amount is not None}
    venues = set(now) & set(before)
    if not venues:
        if not isinstance(cohort, UnavailableConditionDepthCohort) or cohort.reason != "NO_COMMON_VENUES":
            raise ValueError("An absent common depth cohort cannot become observed amounts")
    elif (
        not isinstance(cohort, ObservedConditionDepthCohort)
        or set(cohort.venues) != venues
        or cohort.current_usd != _condition_sum_known_depth(tuple(now[venue] for venue in venues))
        or cohort.previous_usd != _condition_sum_known_depth(tuple(before[venue] for venue in venues))
    ):
        raise ValueError("Depth-drop cohorts must preserve actual common venue amounts")


class ConditionBookSampleCoverage(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    metric_key: str | None = Field(min_length=1, pattern=r"\S")
    cadence: ConditionCadence
    sample_count: int = Field(ge=0)
    initial_observation: Timestamp
    latest_observation: Timestamp
    maximum_gap_s: float | None = Field(ge=0)

    @model_validator(mode="after")
    def actual_scoped_samples(self) -> Self:
        _bind_cadence_scope(
            self.cadence.scope,
            collection="books",
            token_id=self.token_id,
            instrument_id=self.instrument_id,
            metric_key=self.metric_key,
            observation_kind=None,
            chain=None,
        )
        first, last = self.initial_observation.value, self.latest_observation.value
        if self.sample_count == 0:
            if first is not None or last is not None or self.maximum_gap_s is not None:
                raise ValueError("No book samples cannot invent measured coverage clocks or gaps")
        else:
            if first is None or last is None:
                raise ValueError("Observed book samples require their actual first and last clocks")
            span = (parse_iso_time(last) - parse_iso_time(first)).total_seconds()
            if span < 0:
                raise ValueError("Book coverage clocks must preserve their actual order")
            if self.sample_count == 1:
                if self.initial_observation != self.latest_observation or self.maximum_gap_s is not None:
                    raise ValueError("One actual book sample has no measured inter-sample gap")
            elif self.maximum_gap_s is None or self.maximum_gap_s > span:
                raise ValueError("Multiple book samples require their actual bounded maximum observed gap")
        return self

    def covers(self, interval: ConditionInterval) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        actual = ConditionInterval.model_validate(interval)
        cadence = validated.cadence
        if not isinstance(cadence, DeclaredConditionCadence) or validated.sample_count == 0:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        first, last = validated.initial_observation.value, validated.latest_observation.value
        if first is None or last is None:
            raise ValueError("Observed coverage requires its actual endpoint clocks")
        a, z = parse_iso_time(first), parse_iso_time(last)
        if z > actual.cutoff:
            raise ValueError("Book coverage cannot borrow a future observation")
        endpoints = (
            a <= actual.start
            and (actual.start - a).total_seconds() <= cadence.seconds
            and (actual.cutoff - z).total_seconds() <= cadence.seconds
        )
        if validated.sample_count == 1:
            complete = endpoints
        else:
            gap = validated.maximum_gap_s
            if gap is None:
                raise ValueError("Multiple observations require their actual measured gap")
            complete = endpoints and gap <= cadence.seconds
        return ConditionObservedPredicate(status="OBSERVED", value=complete)


class RecordedConditionCaptureIdentity(DomainModel):
    status: Literal["RECORDED"]
    versions: ResolvedVersionIdentity


class UnrecordedConditionCaptureIdentity(DomainModel):
    status: Literal["UNRECORDED"]
    reason: Literal["CAPTURE_IDENTITY_NOT_CAPTURED"]


class ConditionComparableBookQuote(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    metric_key: str | None = Field(min_length=1, pattern=r"\S")
    quote_asset: str = Field(min_length=1, pattern=r"\S")
    mid_quote: Decimal = Field(gt=0)
    spread_bps: Decimal = Field(ge=0)
    observed_at: Timestamp
    quote_valuation: QuoteUsdValuation
    capture_identity: RecordedConditionCaptureIdentity | UnrecordedConditionCaptureIdentity

    @model_validator(mode="after")
    def actual_currency_identity(self) -> Self:
        if self.quote_asset == "USD" and self.quote_valuation.basis != "USD_UNIT":
            raise ValueError("A USD book comparison uses its exact currency unit identity")
        if self.quote_valuation.basis == "USD_UNIT" and self.quote_asset != "USD":
            raise ValueError("A non-USD book comparison cannot borrow USD's unit conversion")
        return self

    def fresh_at(self, at: datetime, cadence: ConditionCadence) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        policy = type(cadence).model_validate(cadence)
        _bind_cadence_scope(
            policy.scope,
            collection="books",
            token_id=validated.token_id,
            instrument_id=validated.instrument_id,
            metric_key=validated.metric_key,
            observation_kind=None,
            chain=None,
        )
        bound = parse_iso_time(at)
        if not isinstance(policy, DeclaredConditionCadence) or validated.observed_at.value is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        observed = parse_iso_time(validated.observed_at.value)
        if observed > bound or (bound - observed).total_seconds() > policy.seconds:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return ConditionObservedPredicate(status="OBSERVED", value=True)


class ConditionPrimaryBookQuote(ConditionComparableBookQuote):
    integrity_ok: TrueFlag


class ConditionBookPairSample(DomainModel):
    left: ConditionPrimaryBookQuote
    right: ConditionPrimaryBookQuote
    left_cadence: ConditionCadence
    right_cadence: ConditionCadence
    evaluated_at: datetime
    quote_conversion_uncertainty_bps: Decimal = Field(ge=0)

    @field_validator("evaluated_at")
    @classmethod
    def aware_evaluation(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_comparable_scope(self) -> Self:
        if self.left.venue == self.right.venue or self.left.token_id != self.right.token_id:
            raise ValueError("A cross-venue gap requires distinct actual venues for the same token")
        for quote, cadence in ((self.left, self.left_cadence), (self.right, self.right_cadence)):
            _bind_cadence_scope(
                cadence.scope,
                collection="books",
                token_id=quote.token_id,
                instrument_id=quote.instrument_id,
                metric_key=quote.metric_key,
                observation_kind=None,
                chain=None,
            )
        return self

    def gap(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        eligible = condition_predicates_all(
            (
                validated.left.fresh_at(validated.evaluated_at, validated.left_cadence),
                validated.right.fresh_at(validated.evaluated_at, validated.right_cadence),
            )
        )
        if isinstance(eligible, ConditionUnavailablePredicate):
            return eligible
        return _condition_book_price_gap(
            validated.left,
            validated.right,
            validated.evaluated_at,
            validated.quote_conversion_uncertainty_bps,
        )


def _condition_book_price_gap(
    left: ConditionComparableBookQuote,
    right: ConditionComparableBookQuote,
    at: datetime,
    conversion_uncertainty_bps: Decimal,
) -> ConditionPredicate:
    if left.quote_asset == right.quote_asset:
        return _condition_comparable_mid_gap(
            left.mid_quote, right.mid_quote, left.spread_bps, right.spread_bps, Decimal(0)
        )
    left_rate, right_rate = left.quote_valuation.rate, right.quote_valuation.rate
    if left_rate is None or right_rate is None:
        return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="QUOTE_CONVERSION_UNAVAILABLE")
    for valuation in (left.quote_valuation, right.quote_valuation):
        if valuation.as_of is not None and valuation.as_of > at:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
    values = (left.mid_quote, right.mid_quote, left_rate, right_rate)
    span = (
        max(value.adjusted() for value in values)
        - min(int(value.as_tuple().exponent) for value in values)
        + 1
    )
    with localcontext() as context:
        context.prec = max(128, 4 * span + 32)
        left_usd, right_usd = left.mid_quote * left_rate, right.mid_quote * right_rate
    return _condition_comparable_mid_gap(
        left_usd, right_usd, left.spread_bps, right.spread_bps, conversion_uncertainty_bps
    )


def _condition_comparable_mid_gap(
    left_mid: Decimal,
    right_mid: Decimal,
    left_spread_bps: Decimal,
    right_spread_bps: Decimal,
    conversion_cost_bps: Decimal,
) -> ConditionObservedPredicate:
    values = (left_mid, right_mid, left_spread_bps, right_spread_bps, conversion_cost_bps)
    span = (
        max(value.adjusted() for value in values)
        - min(int(value.as_tuple().exponent) for value in values)
        + 1
    )
    with localcontext() as context:
        context.prec = max(128, 4 * span + 32)
        gap = (left_mid - right_mid).copy_abs() * 10000 > (
            left_spread_bps + right_spread_bps + conversion_cost_bps
        ) * right_mid
    return ConditionObservedPredicate(status="OBSERVED", value=gap)


class ConditionReceiptVisibility(DomainModel):
    received_at: Timestamp
    assembled_at: Timestamp

    def eligible(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        received, assembled = validated.received_at.value, validated.assembled_at.value
        if received is None or assembled is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return ConditionObservedPredicate(
            status="OBSERVED", value=parse_iso_time(received) <= parse_iso_time(assembled)
        )


class ConditionIndependentRestQuote(ConditionComparableBookQuote):
    source: Literal["PUBLIC_REST"]
    visibility: ConditionReceiptVisibility
    primary_observed_at: Timestamp
    proximity_policy: ConditionCadence
    capture_confirmed_independent: bool | None
    confirmation_reason: Literal["NOT_CAPTURED", "NOT_CONFIRMED"] | None

    @model_validator(mode="after")
    def actual_confirmation_claim(self) -> Self:
        _bind_cadence_scope(
            self.proximity_policy.scope,
            collection="books",
            token_id=self.token_id,
            instrument_id=self.instrument_id,
            metric_key=self.metric_key,
            observation_kind=None,
            chain=None,
        )
        expected = (
            None
            if self.capture_confirmed_independent is True
            else "NOT_CONFIRMED"
            if self.capture_confirmed_independent is False
            else "NOT_CAPTURED"
        )
        if self.confirmation_reason != expected:
            raise ValueError(
                "Independent capture confirmation must preserve actual absent/false/true metadata"
            )
        return self

    def eligible_at(self, cutoff: datetime) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        if validated.capture_confirmed_independent is False:
            return ConditionObservedPredicate(status="OBSERVED", value=False)
        if validated.capture_confirmed_independent is None or not isinstance(
            validated.proximity_policy, DeclaredConditionCadence
        ):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        primary, secondary = validated.primary_observed_at.value, validated.observed_at.value
        if primary is None or secondary is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        a, b, bound = parse_iso_time(primary), parse_iso_time(secondary), parse_iso_time(cutoff)
        if a > bound or b > bound or abs((b - a).total_seconds()) > validated.proximity_policy.seconds:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return validated.visibility.eligible()


class ConditionIndependentPairConfirmation(DomainModel):
    left: ConditionIndependentRestQuote | None
    right: ConditionIndependentRestQuote | None
    cutoff: datetime
    quote_conversion_uncertainty_bps: Decimal = Field(ge=0)

    @field_validator("cutoff")
    @classmethod
    def aware_evaluation(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_pair_identity(self) -> Self:
        if (
            self.left is not None
            and self.right is not None
            and (self.left.venue == self.right.venue or self.left.token_id != self.right.token_id)
        ):
            raise ValueError("Independent confirmation requires distinct venues for the same actual token")
        return self

    def confirmed_gap(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        left, right = validated.left, validated.right
        if left is None or right is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        eligible = condition_predicates_all(
            (left.eligible_at(validated.cutoff), right.eligible_at(validated.cutoff))
        )
        if isinstance(eligible, ConditionUnavailablePredicate) or not eligible.value:
            return eligible
        return _condition_book_price_gap(
            left, right, validated.cutoff, validated.quote_conversion_uncertainty_bps
        )


class ObservedConditionVolatility(DomainModel):
    status: Literal["OBSERVED"]
    value: float = Field(ge=0)
    completed_hour: Timestamp
    return_count: Annotated[Literal[24], BeforeValidator(require_integer)]
    resolution_s: Annotated[Literal[3600], BeforeValidator(require_number)]

    @model_validator(mode="after")
    def actual_completed_hour(self) -> Self:
        if self.completed_hour.value is None:
            raise ValueError("Observed rolling volatility requires its actual source-hour clock")
        at = parse_iso_time(self.completed_hour.value)
        if at.minute != 0 or at.second != 0 or at.microsecond != 0:
            raise ValueError("Rolling volatility requires its actual completed UTC hour")
        return self


class UnavailableConditionVolatility(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["INCOMPLETE"]


type ConditionVolatility = ObservedConditionVolatility | UnavailableConditionVolatility


class ObservedConditionVolatilityState(DomainModel):
    status: Literal["OBSERVED"]
    value: Literal["COMPRESSED", "EXPANDED", "NORMAL"]


class UnavailableConditionVolatilityState(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["INCOMPLETE", "INSUFFICIENT_HISTORY", "DEGENERATE_HISTORY"]


class ConditionVolatilityCompression(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    reference_venue: str | None = Field(min_length=1, pattern=r"\S")
    cutoff: datetime
    window: Literal["W24H"]
    realized: ConditionVolatility
    normal_range: FloatRange

    @field_validator("cutoff")
    @classmethod
    def aware_cutoff(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_measurement_and_history(self) -> Self:
        if self.reference_venue is None and (
            isinstance(self.realized, ObservedConditionVolatility) or self.normal_range.bounds is not None
        ):
            raise ValueError("Observed volatility and its history require the actual reference venue")
        if isinstance(self.realized, ObservedConditionVolatility):
            stamp = self.realized.completed_hour.value
            if stamp is None:
                raise ValueError("Observed rolling volatility requires its actual source-hour clock")
            if parse_iso_time(stamp) > self.cutoff:
                raise ValueError("Rolling volatility cannot use a completed hour after its actual cutoff")
        if self.normal_range.bounds is not None and self.normal_range.bounds.lower < 0:
            raise ValueError("Realized-volatility history cannot have negative bounds")
        return self

    def classification(self) -> ObservedConditionVolatilityState | UnavailableConditionVolatilityState:
        validated = type(self).model_validate(self)
        if isinstance(validated.realized, UnavailableConditionVolatility):
            return UnavailableConditionVolatilityState(
                status="UNAVAILABLE", value=None, reason=validated.realized.reason
            )
        bounds = validated.normal_range.bounds
        if bounds is None:
            reason = validated.normal_range.reason
            if reason is None:
                raise ValueError("An unavailable volatility baseline requires its actual reason")
            return UnavailableConditionVolatilityState(status="UNAVAILABLE", value=None, reason=reason)
        measured = validated.realized.value
        state: Literal["COMPRESSED", "EXPANDED", "NORMAL"] = (
            "COMPRESSED" if measured < bounds.lower else "EXPANDED" if measured > bounds.upper else "NORMAL"
        )
        return ObservedConditionVolatilityState(status="OBSERVED", value=state)


class ConditionTimedBookGap(DomainModel):
    evaluated_at: datetime
    gap: ConditionPredicate

    @field_validator("evaluated_at")
    @classmethod
    def aware_evaluation(cls, value: datetime) -> datetime:
        return parse_iso_time(value)


class ConditionBookPairHistory(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    left_venue: str = Field(min_length=1, pattern=r"\S")
    right_venue: str = Field(min_length=1, pattern=r"\S")
    left_instrument_id: str = Field(min_length=1, pattern=r"\S")
    right_instrument_id: str = Field(min_length=1, pattern=r"\S")
    left_metric_key: str | None = Field(min_length=1, pattern=r"\S")
    right_metric_key: str | None = Field(min_length=1, pattern=r"\S")
    interval: ConditionInterval
    left: tuple[ConditionPrimaryBookQuote, ...]
    right: tuple[ConditionPrimaryBookQuote, ...]
    left_cadence: ConditionCadence
    right_cadence: ConditionCadence
    quote_conversion_uncertainty_bps: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def actual_pair_history(self) -> Self:
        if (
            self.interval.window != "W15M"
            or (self.interval.cutoff - self.interval.start).total_seconds() != 900
        ):
            raise ValueError("Fragmentation history requires the actual fifteen-minute evaluation interval")
        if self.left_venue == self.right_venue or self.left_instrument_id == self.right_instrument_id:
            raise ValueError("Fragmentation history requires distinct scoped venue instruments")
        for rows, venue, instrument, metric, cadence in (
            (self.left, self.left_venue, self.left_instrument_id, self.left_metric_key, self.left_cadence),
            (
                self.right,
                self.right_venue,
                self.right_instrument_id,
                self.right_metric_key,
                self.right_cadence,
            ),
        ):
            _bind_cadence_scope(
                cadence.scope,
                collection="books",
                token_id=self.token_id,
                instrument_id=instrument,
                metric_key=metric,
                observation_kind=None,
                chain=None,
            )
            previous: datetime | None = None
            for row in rows:
                if (row.token_id, row.venue, row.instrument_id, row.metric_key) != (
                    self.token_id,
                    venue,
                    instrument,
                    metric,
                ):
                    raise ValueError("Fragmentation history cannot mix token or venue instrument scopes")
                stamp = row.observed_at.value
                if stamp is None:
                    raise ValueError("A historical primary book requires its actual source clock")
                at = parse_iso_time(stamp)
                if at > self.interval.cutoff or (previous is not None and at <= previous):
                    raise ValueError("Historical primary book clocks must be ordered, distinct and nonfuture")
                previous = at
        return self

    def gap_series(self) -> tuple[ConditionTimedBookGap, ...]:
        validated = type(self).model_validate(self)
        start, cutoff = validated.interval.start, validated.interval.cutoff
        times = {start, cutoff}
        for row in (*validated.left, *validated.right):
            stamp = row.observed_at.value
            if stamp is None:
                raise ValueError("A historical primary book requires its actual source clock")
            at = parse_iso_time(stamp)
            if start <= at < cutoff:
                times.add(at)
        observations: list[ConditionTimedBookGap] = []
        for at in sorted(times):
            left = validated.latest_at(validated.left, at)
            right = validated.latest_at(validated.right, at)
            gap: ConditionPredicate
            if left is None or right is None:
                gap = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
            else:
                gap = ConditionBookPairSample(
                    left=left,
                    right=right,
                    left_cadence=validated.left_cadence,
                    right_cadence=validated.right_cadence,
                    evaluated_at=at,
                    quote_conversion_uncertainty_bps=validated.quote_conversion_uncertainty_bps,
                ).gap()
            observations.append(ConditionTimedBookGap(evaluated_at=at, gap=gap))
        return tuple(observations)

    @staticmethod
    def latest_at(
        rows: tuple[ConditionPrimaryBookQuote, ...], at: datetime
    ) -> ConditionPrimaryBookQuote | None:
        for row in reversed(rows):
            stamp = row.observed_at.value
            if stamp is None:
                raise ValueError("A historical primary book requires its actual source clock")
            if parse_iso_time(stamp) <= at:
                return row
        return None

    def _continuous_coverage(
        self, rows: tuple[ConditionPrimaryBookQuote, ...], cadence: ConditionCadence
    ) -> ConditionPredicate:
        if not isinstance(cadence, DeclaredConditionCadence):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        initial = self.latest_at(rows, self.interval.start)
        if initial is None or initial.observed_at.value is None:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        previous = parse_iso_time(initial.observed_at.value)
        if (self.interval.start - previous).total_seconds() > cadence.seconds:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        for row in rows:
            stamp = row.observed_at.value
            if stamp is None:
                raise ValueError("Continuous quote coverage requires its actual source clock")
            at = parse_iso_time(stamp)
            if at <= self.interval.start:
                continue
            if (at - previous).total_seconds() > cadence.seconds:
                return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
            previous = at
        if (self.interval.cutoff - previous).total_seconds() > cadence.seconds:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return ConditionObservedPredicate(status="OBSERVED", value=True)

    def persistent_gap(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        return condition_predicates_all(
            (
                *(point.gap for point in validated.gap_series()[:-1]),
                validated._continuous_coverage(validated.left, validated.left_cadence),
                validated._continuous_coverage(validated.right, validated.right_cadence),
            )
        )

    def gap_closed_quickly(self) -> ConditionPredicate:
        points = self.gap_series()
        previous, current = points[-2].gap, points[-1].gap
        if isinstance(current, ConditionObservedPredicate):
            closed: ConditionPredicate = ConditionObservedPredicate(
                status="OBSERVED", value=not current.value
            )
        else:
            closed = current
        return condition_predicates_all((previous, closed))


class ConditionScopedFundingRate(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    rate: ConditionFundingObservation


class ConditionIndependentFundingRate(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    rate_8h_eq: Decimal
    observed_at: Timestamp
    visibility: ConditionReceiptVisibility
    source_path: str = Field(min_length=1, pattern=r"\S")
    primary_source_path: str = Field(min_length=1, pattern=r"\S")
    independent_path_verified: bool | None
    verification_reason: Literal["NOT_CAPTURED", "NOT_VERIFIED"] | None

    @model_validator(mode="after")
    def actual_independent_claim(self) -> Self:
        expected = (
            None
            if self.independent_path_verified is True
            else "NOT_VERIFIED"
            if self.independent_path_verified is False
            else "NOT_CAPTURED"
        )
        if self.verification_reason != expected:
            raise ValueError(
                "Independent funding verification must preserve actual missing/false/true metadata"
            )
        if self.independent_path_verified is True and self.source_path == self.primary_source_path:
            raise ValueError("Verified funding confirmation requires a different actual source path")
        return self

    def eligible_at(self, cutoff: datetime) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        if validated.independent_path_verified is False:
            return ConditionObservedPredicate(status="OBSERVED", value=False)
        stamp = validated.observed_at.value
        if (
            validated.independent_path_verified is None
            or stamp is None
            or parse_iso_time(stamp) > parse_iso_time(cutoff)
        ):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return validated.visibility.eligible()


class ConditionFundingConfirmationGraph(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    assembled_at: Timestamp
    cutoff: datetime
    primary: tuple[ConditionScopedFundingRate, ...]
    independent: tuple[ConditionIndependentFundingRate, ...]
    normal_range_8h: DecimalRange

    @field_validator("cutoff")
    @classmethod
    def aware_cutoff(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_funding_cohort(self) -> Self:
        primary = {row.instrument_id: row for row in self.primary}
        if len(primary) != len(self.primary) or len({row.instrument_id for row in self.independent}) != len(
            self.independent
        ):
            raise ValueError("Funding confirmation instrument identities must be distinct within each cohort")
        for row in self.primary:
            stamp = row.rate.observed_at.value
            if row.token_id != self.token_id or stamp is None or parse_iso_time(stamp) > self.cutoff:
                raise ValueError(
                    "Primary funding must preserve actual token scope and nonfuture source clocks"
                )
        for observation in self.independent:
            if observation.visibility.assembled_at != self.assembled_at:
                raise ValueError("Independent funding visibility must use the actual snapshot assembly clock")
            source = primary.get(observation.instrument_id)
            if source is None or (observation.token_id, observation.venue) != (self.token_id, source.venue):
                raise ValueError("Independent funding cannot borrow another token or instrument cohort")
        if self.normal_range_8h.bounds is not None and self.normal_range_8h.bounds.lower < 0:
            raise ValueError("Funding dispersion history cannot have negative bounds")
        return self

    def primary_beyond_normal(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        rates: list[Decimal] = []
        for row in validated.primary:
            rate = row.rate.equivalent_eight_hour_rate
            if rate is None:
                return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
            rates.append(rate)
        return validated._dispersion_beyond(tuple(rates))

    def independent_beyond_normal(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        if len(validated.primary) < 2 or len(validated.independent) != len(validated.primary):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        eligibility = condition_predicates_all(
            tuple(row.eligible_at(validated.cutoff) for row in validated.independent)
        )
        if isinstance(eligibility, ConditionUnavailablePredicate) or not eligibility.value:
            return eligibility
        return validated._dispersion_beyond(tuple(row.rate_8h_eq for row in validated.independent))

    def _dispersion_beyond(self, rates: tuple[Decimal, ...]) -> ConditionPredicate:
        if len(rates) < 2:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        bounds = self.normal_range_8h.bounds
        if bounds is None:
            reason = self.normal_range_8h.reason
            if reason is None:
                raise ValueError("Unavailable funding dispersion history requires its actual reason")
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason=reason)
        values = (*rates, bounds.lower, bounds.upper)
        span = (
            max(value.adjusted() for value in values)
            - min(int(value.as_tuple().exponent) for value in values)
            + 1
        )
        with localcontext() as context:
            context.prec = max(128, 2 * span + 16)
            dispersion = max(rates) - min(rates)
        return ConditionObservedPredicate(
            status="OBSERVED", value=dispersion < bounds.lower or dispersion > bounds.upper
        )


class ConditionFlowCapturePiece(DomainModel):
    basis: Literal["FULL_MINUTE", "BOUNDARY"]
    aggregate: ConditionFlowAggregate
    visibility: ConditionReceiptVisibility


class ConditionFlowWindowQualification(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    assembled_at: Timestamp
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    interval: ConditionInterval
    pieces: tuple[ConditionFlowCapturePiece, ...]

    def _minute_bounds(self) -> tuple[datetime, datetime]:
        start = self.interval.start.replace(second=0, microsecond=0)
        if start < self.interval.start:
            start += timedelta(minutes=1)
        return start, self.interval.cutoff.replace(second=0, microsecond=0)

    def _boundaries(self) -> tuple[tuple[datetime, datetime], ...]:
        first, last = self._minute_bounds()
        start, end = self.interval.start, self.interval.cutoff
        ranges: list[tuple[datetime, datetime]] = []
        if first > start:
            ranges.append((start, min(first, end)))
        if last < end and (not ranges or last >= ranges[0][1]):
            ranges.append((max(last, start), end))
        return tuple(ranges)

    @model_validator(mode="after")
    def actual_qualifying_pieces(self) -> Self:
        first, last = self._minute_bounds()
        boundaries = self._boundaries()
        identities: set[tuple[datetime, datetime]] = set()
        for piece in self.pieces:
            if piece.visibility.assembled_at != self.assembled_at:
                raise ValueError("Flow visibility must use the actual snapshot assembly clock")
            row = piece.aggregate
            if (
                row.instrument_id != self.instrument_id
                or not self.interval.start <= row.start < row.end <= self.interval.cutoff
            ):
                raise ValueError(
                    "Flow capture evidence must preserve actual instrument and requested interval"
                )
            key = (row.start, row.end)
            if key in identities:
                raise ValueError("Flow capture qualification cannot double-count an aggregate interval")
            identities.add(key)
            if piece.basis == "FULL_MINUTE":
                if (
                    row.start.second != 0
                    or row.start.microsecond != 0
                    or row.end - row.start != timedelta(minutes=1)
                    or not first <= row.start < row.end <= last
                ):
                    raise ValueError(
                        "Full-minute capture requires an actual aligned complete interior minute"
                    )
            elif not any(left <= row.start < row.end <= right for left, right in boundaries):
                raise ValueError("Boundary capture cannot replace missing full-minute evidence")
        return self

    def complete(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        eligible: list[ConditionFlowCapturePiece] = []
        for piece in validated.pieces:
            visibility = piece.visibility.eligible()
            if isinstance(visibility, ConditionObservedPredicate) and visibility.value:
                eligible.append(piece)
        first, last = validated._minute_bounds()
        expected: set[datetime] = set()
        cursor = first
        while cursor < last:
            expected.add(cursor)
            cursor += timedelta(minutes=1)
        observed = {piece.aggregate.start for piece in eligible if piece.basis == "FULL_MINUTE"}
        if observed != expected:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        for left, right in validated._boundaries():
            cursor = left
            while cursor < right:
                ends = tuple(
                    piece.aggregate.end
                    for piece in eligible
                    if piece.basis == "BOUNDARY"
                    and piece.aggregate.start == cursor
                    and piece.aggregate.end <= right
                )
                if not ends:
                    return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
                cursor = max(ends)
        return ConditionObservedPredicate(status="OBSERVED", value=True)


class ConditionFragmentationPriceConfirmation(DomainModel):
    left_instrument_id: str = Field(min_length=1, pattern=r"\S")
    right_instrument_id: str = Field(min_length=1, pattern=r"\S")
    observations: tuple[ConditionIndependentPairConfirmation, ...]


class ConditionVenueSignObservation(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    value: Annotated[Literal[-1, 0, 1], BeforeValidator(require_integer)] | None
    reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_direction(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("A missing venue direction requires its actual incomplete-flow reason")
        return self


class ConditionSignHistoryWindow(DomainModel):
    interval: ConditionInterval
    observations: tuple[ConditionVenueSignObservation, ...]

    @model_validator(mode="after")
    def actual_quarter_hour(self) -> Self:
        if (
            self.interval.window != "W15M"
            or (self.interval.cutoff - self.interval.start).total_seconds() != 900
        ):
            raise ValueError("Direction history requires actual fifteen-minute subwindows")
        if len({row.venue for row in self.observations}) != len(self.observations):
            raise ValueError("A sign-history window cannot duplicate a venue observation")
        return self


class ConditionTopVenueSignHistory(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    interval: ConditionInterval
    selected_venues: tuple[str, ...]
    windows: tuple[ConditionSignHistoryWindow, ...]

    @model_validator(mode="after")
    def actual_selected_history(self) -> Self:
        selected = set(self.selected_venues)
        if len(selected) != len(self.selected_venues) or any(not venue.strip() for venue in selected):
            raise ValueError("Selected sign-history venues must be distinct actual identifiers")
        cursor = self.interval.start
        for window in self.windows:
            if window.interval.start != cursor or window.interval.cutoff > self.interval.cutoff:
                raise ValueError(
                    "Sign-history subwindows must preserve the actual consecutive requested interval"
                )
            if {row.venue for row in window.observations} != selected:
                raise ValueError(
                    "Sign history must explicitly preserve each selected venue's observation or absence"
                )
            cursor = window.interval.cutoff
        return self

    def multiple_flips(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        expected = int((validated.interval.cutoff - validated.interval.start).total_seconds() // 900)
        if not validated.selected_venues or len(validated.windows) < 3 or len(validated.windows) != expected:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        signs: dict[str, list[int]] = {venue: [] for venue in validated.selected_venues}
        for window in validated.windows:
            for observation in window.observations:
                if observation.value is None:
                    return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
                if observation.value != 0:
                    signs[observation.venue].append(observation.value)
        flipped = any(
            sum(a != b for a, b in zip(values, values[1:], strict=False)) > 1 for values in signs.values()
        )
        return ConditionObservedPredicate(status="OBSERVED", value=flipped)


class ConditionFragmentationGraph(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    assembled_at: Timestamp
    interval: ConditionInterval
    price_pairs: tuple[ConditionBookPairHistory, ...]
    top_sign_history: ConditionTopVenueSignHistory
    price_confirmations: tuple[ConditionFragmentationPriceConfirmation, ...]
    funding: ConditionFundingConfirmationGraph
    flow_capture: tuple[ConditionFlowWindowQualification, ...]

    @model_validator(mode="after")
    def actual_fragmentation_scopes(self) -> Self:
        if (
            self.interval.window != "W15M"
            or (self.interval.cutoff - self.interval.start).total_seconds() != 900
        ):
            raise ValueError(
                "Fragmentation qualification requires the actual fifteen-minute capture interval"
            )
        if (
            self.top_sign_history.token_id != self.token_id
            or self.top_sign_history.interval.cutoff != self.interval.cutoff
        ):
            raise ValueError("Top-venue sign history must preserve actual token and cutoff")
        pairs: dict[tuple[str, str], ConditionBookPairHistory] = {}
        unordered: set[frozenset[str]] = set()
        for pair in self.price_pairs:
            key = (pair.left_instrument_id, pair.right_instrument_id)
            identity = frozenset(key)
            if identity in unordered or pair.token_id != self.token_id or pair.interval != self.interval:
                raise ValueError(
                    "Fragmentation price pairs must preserve distinct actual token/window scopes"
                )
            unordered.add(identity)
            pairs[key] = pair
        seen: set[tuple[str, str]] = set()
        for confirmation in self.price_confirmations:
            key = (confirmation.left_instrument_id, confirmation.right_instrument_id)
            primary_pair = pairs.get(key)
            if primary_pair is None or key in seen:
                raise ValueError("Independent price confirmation requires its actual unique primary pair")
            seen.add(key)
            for observation in confirmation.observations:
                if (
                    observation.cutoff != self.interval.cutoff
                    or observation.quote_conversion_uncertainty_bps
                    != primary_pair.quote_conversion_uncertainty_bps
                ):
                    raise ValueError(
                        "Independent price confirmation cannot change the actual cutoff/conversion policy"
                    )
                for secondary, primary_rows, venue, instrument in (
                    (
                        observation.left,
                        primary_pair.left,
                        primary_pair.left_venue,
                        primary_pair.left_instrument_id,
                    ),
                    (
                        observation.right,
                        primary_pair.right,
                        primary_pair.right_venue,
                        primary_pair.right_instrument_id,
                    ),
                ):
                    if secondary is None:
                        continue
                    if secondary.visibility.assembled_at != self.assembled_at:
                        raise ValueError("REST visibility must use the actual snapshot assembly clock")
                    if (secondary.token_id, secondary.venue, secondary.instrument_id) != (
                        self.token_id,
                        venue,
                        instrument,
                    ):
                        raise ValueError(
                            "Independent price evidence cannot borrow another primary instrument"
                        )
                    matching = tuple(
                        row for row in primary_rows if row.observed_at == secondary.primary_observed_at
                    )
                    if not matching or secondary.quote_asset != matching[0].quote_asset:
                        raise ValueError(
                            "Independent price evidence requires its actual primary source observation and currency"
                        )
        if (
            self.funding.token_id != self.token_id
            or self.funding.cutoff != self.interval.cutoff
            or self.funding.assembled_at != self.assembled_at
        ):
            raise ValueError("Funding fragmentation must preserve actual token and cutoff")
        captures: set[str] = set()
        known_venues: dict[str, str] = {}
        for pair in self.price_pairs:
            known_venues[pair.left_instrument_id] = pair.left_venue
            known_venues[pair.right_instrument_id] = pair.right_venue
        for rate in self.funding.primary:
            prior = known_venues.get(rate.instrument_id)
            if prior is not None and prior != rate.venue:
                raise ValueError("A fragmentation instrument cannot change venue between source families")
            known_venues[rate.instrument_id] = rate.venue
        for capture in self.flow_capture:
            if capture.assembled_at != self.assembled_at:
                raise ValueError("Flow qualification cannot borrow another snapshot assembly clock")
            if (
                capture.instrument_id in captures
                or capture.token_id != self.token_id
                or capture.interval != self.interval
            ):
                raise ValueError("Flow capture qualification requires a distinct actual token/window scope")
            captures.add(capture.instrument_id)
            if known_venues.get(capture.instrument_id) != capture.venue:
                raise ValueError("Flow capture cannot qualify an unrelated fragmentation instrument")
        return self

    def _capture(self, instrument: str) -> ConditionPredicate:
        for capture in self.flow_capture:
            if capture.assembled_at != self.assembled_at:
                raise ValueError("Flow qualification cannot borrow another snapshot assembly clock")
            if capture.instrument_id == instrument:
                return capture.complete()
        return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")

    def persistent_gap(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        if not validated.price_pairs:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return condition_predicates_any(tuple(pair.persistent_gap() for pair in validated.price_pairs))

    def gap_closed_quickly(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        if not validated.price_pairs:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return condition_predicates_any(tuple(pair.gap_closed_quickly() for pair in validated.price_pairs))

    def price_path_validated(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        results: list[ConditionPredicate] = []
        confirmations = {
            (row.left_instrument_id, row.right_instrument_id): row for row in validated.price_confirmations
        }
        for pair in validated.price_pairs:
            confirmation = confirmations.get((pair.left_instrument_id, pair.right_instrument_id))
            secondary: ConditionPredicate
            if confirmation is None or not confirmation.observations:
                secondary = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
            else:
                secondary = condition_predicates_any(
                    tuple(row.confirmed_gap() for row in confirmation.observations)
                )
            results.append(
                condition_predicates_all(
                    (
                        pair.persistent_gap(),
                        secondary,
                        validated._capture(pair.left_instrument_id),
                        validated._capture(pair.right_instrument_id),
                    )
                )
            )
        if not results:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return condition_predicates_any(tuple(results))

    def funding_path_validated(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        captures = tuple(validated._capture(row.instrument_id) for row in validated.funding.primary)
        capture = (
            condition_predicates_all(captures)
            if captures
            else ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        )
        return condition_predicates_all(
            (
                validated.funding.primary_beyond_normal(),
                validated.funding.independent_beyond_normal(),
                capture,
            )
        )

    def validated(self) -> ConditionPredicate:
        checked = type(self).model_validate(self)
        return condition_predicates_any((checked.price_path_validated(), checked.funding_path_validated()))


class ConditionImpactOrder(DomainModel):
    notional_usd: Decimal | None = Field(ge=0)
    reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_order_size(self) -> Self:
        if (self.notional_usd is None) != (self.reason is not None):
            raise ValueError("Missing impact order size requires the actual incomplete volume-history reason")
        return self


class ConditionImpactOrders(DomainModel):
    T1: ConditionImpactOrder
    T2: ConditionImpactOrder
    T3: ConditionImpactOrder


class ObservedConditionSlippage(DomainModel):
    status: Literal["OBSERVED"]
    value: float


class UnavailableConditionSlippage(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    reason: Literal["INCOMPLETE", "ZERO_NOTIONAL", "INSUFFICIENT_BOOK_DEPTH", "QUOTE_CONVERSION_UNAVAILABLE"]


type ConditionSlippage = ObservedConditionSlippage | UnavailableConditionSlippage


class ConditionSplitExecutionBook(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    observed_at: Timestamp
    visibility: ConditionReceiptVisibility
    integrity_ok: TrueFlag
    quote_asset: str = Field(min_length=1, pattern=r"\S")
    quote_valuation: QuoteUsdValuation
    mid_quote: Decimal = Field(gt=0)
    allocation_depth_p1_usd: Decimal | None = Field(ge=0)
    allocation_depth_reason: Literal["INCOMPLETE", "DEPTH_BAND_NOT_COVERED"] | None
    asks: tuple[BookLevel, ...]

    @model_validator(mode="after")
    def actual_execution_book(self) -> Self:
        if (self.allocation_depth_p1_usd is None) != (self.allocation_depth_reason is not None):
            raise ValueError("Missing allocation depth requires its actual book coverage reason")
        if self.quote_asset == "USD" and self.quote_valuation.basis != "USD_UNIT":
            raise ValueError("USD allocation books require actual USD unit identity")
        if self.quote_asset != "USD" and self.quote_valuation.basis == "USD_UNIT":
            raise ValueError("A non-USD book cannot assume USD unit identity")
        return self


class ConditionSplitExecution(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    cutoff: datetime
    assembled_at: Timestamp
    books: tuple[ConditionSplitExecutionBook, ...]
    orders: ConditionImpactOrders

    @field_validator("cutoff")
    @classmethod
    def aware_cutoff(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_execution_scope(self) -> Self:
        if len({book.venue for book in self.books}) != len(self.books) or len(
            {book.instrument_id for book in self.books}
        ) != len(self.books):
            raise ValueError("Split execution requires distinct actual venue instruments")
        for book in self.books:
            stamp = book.observed_at.value
            if book.token_id != self.token_id or stamp is None or parse_iso_time(stamp) > self.cutoff:
                raise ValueError("Split execution cannot borrow another token or future/unclocked book")
            if book.visibility.assembled_at != self.assembled_at:
                raise ValueError("Split execution visibility requires its actual snapshot assembly clock")
            if book.quote_valuation.as_of is not None and book.quote_valuation.as_of > self.cutoff:
                raise ValueError("Split execution cannot borrow future quote valuation")
        return self

    def split_slippage(self, tier: Literal["T1", "T2", "T3"]) -> ConditionSlippage:
        validated = type(self).model_validate(self)
        order = {"T1": validated.orders.T1, "T2": validated.orders.T2, "T3": validated.orders.T3}[tier]
        notional = order.notional_usd
        if notional is None:
            return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
        if notional == 0:
            return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason="ZERO_NOTIONAL")
        if not validated.books:
            return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
        depths: list[Decimal] = []
        for book in validated.books:
            visibility = book.visibility.eligible()
            if (
                not isinstance(visibility, ConditionObservedPredicate)
                or not visibility.value
                or book.allocation_depth_p1_usd is None
            ):
                return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
            depths.append(book.allocation_depth_p1_usd)
        total = _condition_sum_known_depth(tuple(depths))
        if total is None:
            raise ValueError("Observed split depth requires its actual nonempty venue cohort")
        if total == 0:
            return UnavailableConditionSlippage(
                status="UNAVAILABLE", value=None, reason="INSUFFICIENT_BOOK_DEPTH"
            )
        values = (notional, total, *depths)
        span = (
            max(value.adjusted() for value in values)
            - min(int(value.as_tuple().exponent) for value in values)
            + 1
        )
        weighted: list[float] = []
        with localcontext() as context:
            context.prec = max(128, 4 * span + 32)
            for book, depth in zip(validated.books, depths, strict=True):
                if depth == 0:
                    continue
                rate = book.quote_valuation.rate
                if rate is None:
                    return UnavailableConditionSlippage(
                        status="UNAVAILABLE", value=None, reason="QUOTE_CONVERSION_UNAVAILABLE"
                    )
                if not book.asks:
                    return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
                allocation = notional * depth / total
                execution = execute_book_walk(
                    BookWalkRequest(
                        levels=list(book.asks),
                        quote_notional=allocation / rate,
                        mid_quote_price=book.mid_quote,
                        side="ask",
                    )
                )
                if execution.slippage_bps is None:
                    return UnavailableConditionSlippage(
                        status="UNAVAILABLE", value=None, reason="INSUFFICIENT_BOOK_DEPTH"
                    )
                weighted.append(float(allocation / notional) * execution.slippage_bps)
        return ObservedConditionSlippage(status="OBSERVED", value=sum(weighted))


class EnrichedConditionLiquidity(ConditionLiquidityValues):
    kind: Literal["ENRICHED"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    assembled_at: Timestamp
    fragmentation: ConditionFragmentationGraph
    vol_compression: ConditionVolatilityCompression
    execution: ConditionSplitExecution

    @model_validator(mode="after")
    def actual_enrichment_inputs(self) -> Self:
        if (
            self.fragmentation.token_id,
            self.fragmentation.interval.cutoff,
            self.fragmentation.assembled_at,
        ) != (self.token_id, self.interval.cutoff, self.assembled_at):
            raise ValueError(
                "Enriched liquidity fragmentation must preserve actual token/cutoff/assembly scope"
            )
        if (self.execution.token_id, self.execution.cutoff, self.execution.assembled_at) != (
            self.token_id,
            self.interval.cutoff,
            self.assembled_at,
        ):
            raise ValueError("Enriched liquidity execution must preserve actual token/cutoff/assembly scope")
        if (self.vol_compression.token_id, self.vol_compression.cutoff) != (
            self.token_id,
            self.interval.cutoff,
        ):
            raise ValueError("Enriched liquidity volatility must preserve its actual token and cutoff")
        books = {book.instrument_id: book for book in self.execution.books}
        if set(books) != {venue.instrument_id for venue in self.per_venue}:
            raise ValueError("Split and single-venue execution must preserve the actual selected book cohort")
        for venue in self.per_venue:
            book = books[venue.instrument_id]
            if (book.venue, book.observed_at, book.quote_asset, book.quote_valuation) != (
                venue.venue,
                venue.observed_at,
                venue.quote_asset,
                venue.quote_valuation,
            ):
                raise ValueError(
                    "Split execution cannot substitute a different selected book or quote valuation"
                )
            for impact, order in (
                (venue.impact_tiers.T1, self.execution.orders.T1),
                (venue.impact_tiers.T2, self.execution.orders.T2),
                (venue.impact_tiers.T3, self.execution.orders.T3),
            ):
                if impact.notional_usd != order.notional_usd:
                    raise ValueError("Split and single-venue impact must use the same actual USD order size")
        return self

    def best_single_venue_slippage(self, tier: Literal["T1", "T2", "T3"]) -> ConditionSlippage:
        validated = type(self).model_validate(self)
        order = {
            "T1": validated.execution.orders.T1,
            "T2": validated.execution.orders.T2,
            "T3": validated.execution.orders.T3,
        }[tier]
        if order.notional_usd is None:
            return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason="INCOMPLETE")
        if order.notional_usd == 0:
            return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason="ZERO_NOTIONAL")
        values: list[float] = []
        incomplete_depth = 0
        for venue in validated.per_venue:
            impact = {"T1": venue.impact_tiers.T1, "T2": venue.impact_tiers.T2, "T3": venue.impact_tiers.T3}[
                tier
            ]
            value = impact.slippage_bps()
            if value is not None:
                values.append(value)
            elif impact.execution is not None and impact.execution.reason == "INSUFFICIENT_BOOK_DEPTH":
                incomplete_depth += 1
        if values:
            return ObservedConditionSlippage(status="OBSERVED", value=min(values))
        reason: Literal["INCOMPLETE", "INSUFFICIENT_BOOK_DEPTH"] = (
            "INSUFFICIENT_BOOK_DEPTH"
            if validated.per_venue and incomplete_depth == len(validated.per_venue)
            else "INCOMPLETE"
        )
        return UnavailableConditionSlippage(status="UNAVAILABLE", value=None, reason=reason)


type ConditionLiquidity = BasicConditionLiquidity | EnrichedConditionLiquidity


class ConditionHoldersRevenuePeriod(DomainModel):
    provider_period: Literal["24h", "30d"]
    value_usd: Decimal | None
    status: Literal["OBSERVED", "UNAVAILABLE"]

    @model_validator(mode="after")
    def actual_provider_value(self) -> Self:
        if (self.value_usd is not None) != (self.status == "OBSERVED"):
            raise ValueError(
                "Observed holders revenue requires the actual provider value, including known zero"
            )
        return self


class ConditionHoldersRevenueChartPoint(DomainModel):
    provider_timestamp: str = Field(min_length=1, pattern=r"\S")
    value_usd: Decimal
    provider_period: Literal["UNSPECIFIED"]

    @field_validator("provider_timestamp")
    @classmethod
    def declared_source_seconds(cls, value: str) -> str:
        parse_epoch(value, unit="s")
        return value

    def observed_at(self) -> datetime:
        validated = type(self).model_validate(self)
        return parse_epoch(validated.provider_timestamp, unit="s")


class ConditionHoldersRevenueProtocol(DomainModel):
    protocol: str = Field(min_length=1, pattern=r"\S")
    provider: Literal["DefiLlama"]
    unit: Literal["USD"]
    source_url: str | None = Field(min_length=1, pattern=r"\S")
    periods: tuple[ConditionHoldersRevenuePeriod, ConditionHoldersRevenuePeriod]
    chart_available: bool | None
    latest_chart_point: ConditionHoldersRevenueChartPoint | None
    reason: str | None = Field(min_length=1, pattern=r"\S")

    @model_validator(mode="after")
    def actual_provider_periods(self) -> Self:
        if tuple(period.provider_period for period in self.periods) != ("24h", "30d"):
            raise ValueError("Holders revenue preserves the provider's distinct daily and monthly periods")
        if self.latest_chart_point is not None:
            self.latest_chart_point.observed_at()
            if not self.chart_available:
                raise ValueError("A selected chart point requires an actually captured chart")
        return self


class ConditionHoldersRevenueCoverage(DomainModel):
    observed: int = Field(ge=0)
    expected: int = Field(ge=0)
    complete: bool

    @model_validator(mode="after")
    def actual_protocol_count(self) -> Self:
        if self.observed > self.expected or self.complete != (
            self.expected > 0 and self.observed == self.expected
        ):
            raise ValueError("Holders coverage must preserve actual nonempty observed protocol counts")
        return self


class UnavailableConditionHoldersRevenue(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_CAPTURED_FUNDAMENTALS", "ONLY_FUTURE_FUNDAMENTALS"]


class CapturedConditionHoldersRevenue(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    status: Literal["OBSERVED", "STALE", "UNAVAILABLE"]
    protocols: tuple[ConditionHoldersRevenueProtocol, ...]
    as_of: Timestamp
    visibility: ConditionReceiptVisibility
    cutoff: datetime
    cadence: ConditionCadence
    identity_evidence: FundamentalsEvidence | None
    identity_reason: Literal["IDENTITY_EVIDENCE_NOT_CAPTURED"] | None
    source_observation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str | None = Field(min_length=1, pattern=r"\S")

    @field_validator("cutoff")
    @classmethod
    def aware_cutoff(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_captured_fundamentals(self) -> Self:
        _bind_cadence_scope(
            self.cadence.scope,
            collection="supply",
            token_id=self.token_id,
            instrument_id=None,
            metric_key=None,
            observation_kind="FUNDAMENTALS",
            chain=None,
        )
        if (self.identity_evidence is None) != (self.identity_reason is not None):
            raise ValueError("Absent fundamentals identity evidence requires its actual capture gap")
        if len({protocol.protocol for protocol in self.protocols}) != len(self.protocols):
            raise ValueError("Holders revenue cannot duplicate a captured protocol")
        stamp = self.as_of.value
        if stamp is None or parse_iso_time(stamp) > self.cutoff:
            raise ValueError("Captured fundamentals require the actual nonfuture source timestamp")
        if (
            self.status == "UNAVAILABLE"
            and self.protocols
            or self.status != "UNAVAILABLE"
            and not self.protocols
        ):
            raise ValueError("Captured fundamentals status must preserve actual protocol presence")
        if self.protocols and isinstance(self.cadence, DeclaredConditionCadence):
            stale = (self.cutoff - parse_iso_time(stamp)).total_seconds() > self.cadence.seconds
            if (self.status == "STALE") != stale:
                raise ValueError("Fundamentals staleness must use actual scoped cadence and source age")
        elif self.status == "STALE":
            raise ValueError("Unknown source cadence cannot establish fundamentals staleness")
        for protocol in self.protocols:
            if (
                protocol.latest_chart_point is not None
                and protocol.latest_chart_point.observed_at() > self.cutoff
            ):
                raise ValueError("Holders revenue cannot select a future provider chart point")
        return self

    def coverage(self, period: Literal["24h", "30d"]) -> ConditionHoldersRevenueCoverage:
        validated = type(self).model_validate(self)
        observed = 0
        for protocol in validated.protocols:
            for value in protocol.periods:
                if value.provider_period == period and value.value_usd is not None:
                    observed += 1
        expected = len(validated.protocols)
        return ConditionHoldersRevenueCoverage(
            observed=observed, expected=expected, complete=expected > 0 and observed == expected
        )

    def availability(self) -> Literal["COMPLETE", "PARTIAL", "UNAVAILABLE"]:
        validated = type(self).model_validate(self)
        if validated.coverage("24h").complete and validated.coverage("30d").complete:
            return "COMPLETE"
        if any(
            protocol.latest_chart_point is not None
            or any(period.value_usd is not None for period in protocol.periods)
            for protocol in validated.protocols
        ):
            return "PARTIAL"
        return "UNAVAILABLE"


type ConditionHoldersRevenue = CapturedConditionHoldersRevenue | UnavailableConditionHoldersRevenue


class ConditionInventoryBalanceChange(DomainModel):
    venue: str = Field(min_length=1, pattern=r"\S")
    chain_id: int = Field(gt=0)
    contract: str = Field(min_length=1, pattern=r"\S")
    wallet_list_commit: str = Field(min_length=1, pattern=r"\S")
    current_qty_native: Decimal = Field(ge=0)
    previous_qty_native: Decimal = Field(ge=0)

    def change(self) -> Decimal:
        validated = type(self).model_validate(self)
        return _sum_native(validated.current_qty_native, validated.previous_qty_native.copy_negate())


class ConditionInventoryCaptureError(DomainModel):
    wallet: str = Field(min_length=1, pattern=r"\S")
    chain_id: int = Field(gt=0)
    reason: str = Field(min_length=1, pattern=r"\S")


class ConditionInventoryCoverage(DomainModel):
    complete_balances: int = Field(ge=0)
    observed_balances: int = Field(ge=0)
    comparable_balances: int = Field(ge=0)
    coverage_errors: tuple[ConditionInventoryCaptureError, ...]
    unsupported_venues: tuple[str, ...]

    @model_validator(mode="after")
    def actual_balance_counts(self) -> Self:
        if not self.comparable_balances <= self.complete_balances <= self.observed_balances:
            raise ValueError("Inventory coverage requires actual nested comparable/complete/observed counts")
        if len(set(self.unsupported_venues)) != len(self.unsupported_venues) or any(
            not venue.strip() for venue in self.unsupported_venues
        ):
            raise ValueError("Unsupported inventory venues must preserve actual distinct identifiers")
        return self


class ConditionInventoryNativeChange(DomainModel):
    value: Decimal | None
    unit: Literal["NATIVE_TOKEN"]
    reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def actual_change(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("Missing native inventory change requires actual incomplete comparison evidence")
        return self


class ConditionInventoryFractionalChange(DomainModel):
    value: float | None
    reason: Literal["INCOMPLETE", "ZERO_BASELINE", "NOT_ESTIMABLE"] | None

    @model_validator(mode="after")
    def actual_ratio(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError(
                "Missing fractional inventory change requires its actual missing/zero baseline cause"
            )
        return self


class UnavailableConditionExchangeInventory(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: str = Field(min_length=1, pattern=r"\S")


class ComparableConditionExchangeInventory(DomainModel):
    status: Literal["COMPARABLE_BALANCES"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    requested_window: Literal["W24H"]
    cutoff: datetime
    current_observed_at: Timestamp
    previous_observed_at: Timestamp
    current_visibility: ConditionReceiptVisibility
    previous_visibility: ConditionReceiptVisibility
    balances: tuple[ConditionInventoryBalanceChange, ...]
    coverage: ConditionInventoryCoverage
    normal_range: FloatRange

    @field_validator("cutoff")
    @classmethod
    def aware_cutoff(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_comparable_cohort(self) -> Self:
        current = self.current_observed_at.value
        previous = self.previous_observed_at.value
        if current is None or parse_iso_time(current) > self.cutoff:
            raise ValueError("Observed inventory requires the actual nonfuture current source clock")
        if self.balances and previous is None:
            raise ValueError("Comparable inventory balances require their actual prior source clock")
        if previous is not None and (
            parse_iso_time(previous) > parse_iso_time(current)
            or parse_iso_time(previous) > self.cutoff - timedelta(days=1)
        ):
            raise ValueError(
                "Inventory comparison cannot use a prior observation after its current sample or requested as-of target"
            )
        if self.current_visibility.assembled_at != self.previous_visibility.assembled_at:
            raise ValueError(
                "Inventory comparison visibility requires the same actual snapshot assembly clock"
            )
        keys = [(row.venue, row.chain_id, row.contract, row.wallet_list_commit) for row in self.balances]
        if len(set(keys)) != len(keys) or self.coverage.comparable_balances != len(keys):
            raise ValueError(
                "Inventory comparison requires distinct actual wallet-list cohorts and matching coverage"
            )
        if not self.balances and self.normal_range.bounds is not None:
            raise ValueError("An empty comparable wallet cohort cannot establish a historical range")
        return self

    def native_change(self) -> ConditionInventoryNativeChange:
        validated = type(self).model_validate(self)
        if not validated.balances:
            return ConditionInventoryNativeChange(value=None, unit="NATIVE_TOKEN", reason="INCOMPLETE")
        change = Decimal(0)
        for balance in validated.balances:
            change = _sum_native(change, balance.change())
        return ConditionInventoryNativeChange(value=change, unit="NATIVE_TOKEN", reason=None)

    def fractional_change(self) -> ConditionInventoryFractionalChange:
        validated = type(self).model_validate(self)
        change = validated.native_change().value
        if change is None:
            return ConditionInventoryFractionalChange(value=None, reason="INCOMPLETE")
        previous = Decimal(0)
        for balance in validated.balances:
            previous = _sum_native(previous, balance.previous_qty_native)
        if previous == 0:
            return ConditionInventoryFractionalChange(value=None, reason="ZERO_BASELINE")
        span = (
            max(change.adjusted(), previous.adjusted())
            - min(int(change.as_tuple().exponent), int(previous.as_tuple().exponent))
            + 1
        )
        with localcontext() as context:
            context.prec = max(128, 2 * span + 16)
            value = float(change / previous)
        if not isfinite(value):
            return ConditionInventoryFractionalChange(value=None, reason="NOT_ESTIMABLE")
        return ConditionInventoryFractionalChange(value=value, reason=None)


type ConditionExchangeInventory = ComparableConditionExchangeInventory | UnavailableConditionExchangeInventory


class ConditionWalletFlowEvidenceRef(DomainModel):
    source_observation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    flow_index: int | None = Field(ge=0)


class ConditionWalletFlow(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    observed_at: Timestamp
    visibility: ConditionReceiptVisibility
    classification: (
        Literal[
            "UNLOCK_DISTRIBUTION", "TREASURY_INTERNAL", "MM_INVENTORY", "CEX_CUSTODY_MIGRATION", "AMBIGUOUS"
        ]
        | None
    )
    classification_reason: Literal["CLASSIFICATION_NOT_CAPTURED"] | None
    quantity_native: Decimal | None = Field(ge=0)
    quantity_reason: Literal["INCOMPLETE"] | None
    reaches_exchange: bool | None
    destination_reason: Literal["DESTINATION_NOT_CAPTURED"] | None
    evidence: ConditionWalletFlowEvidenceRef

    @model_validator(mode="after")
    def actual_transfer_facets(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("An observed wallet flow requires its actual source occurrence timestamp")
        for absent, reason in (
            (self.classification is None, self.classification_reason),
            (self.quantity_native is None, self.quantity_reason),
            (self.reaches_exchange is None, self.destination_reason),
        ):
            if absent != (reason is not None):
                raise ValueError("Missing wallet-flow facets require their actual capture-gap reason")
        return self

    def positive_supply_witness(self) -> ConditionPredicate:
        validated = type(self).model_validate(self)
        visibility = validated.visibility.eligible()
        if not isinstance(visibility, ConditionObservedPredicate) or not visibility.value:
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        if (
            validated.reaches_exchange is False
            or validated.quantity_native == 0
            or validated.classification == "CEX_CUSTODY_MIGRATION"
        ):
            return ConditionObservedPredicate(status="OBSERVED", value=False)
        if (
            validated.classification in (None, "AMBIGUOUS")
            or validated.reaches_exchange is None
            or validated.quantity_native is None
        ):
            return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
        return ConditionObservedPredicate(status="OBSERVED", value=True)


class ConditionUnprovenWalletCoverage(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["QUERY_SCOPE_NOT_CAPTURED", "PAGINATION_COMPLETENESS_NOT_CAPTURED"]


class ConditionPositiveSupplyObservation(DomainModel):
    status: Literal["OBSERVED_POSITIVE"]
    evidence: tuple[ConditionWalletFlowEvidenceRef, ...] = Field(min_length=1)


class ConditionNoPositiveSupplyObservation(DomainModel):
    status: Literal["NO_POSITIVE_OBSERVATION"]
    observation_count: int = Field(ge=0)


class UnavailableConditionSupplyObservation(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["INCOMPLETE"]


type ConditionWalletSupplyObservation = (
    ConditionPositiveSupplyObservation
    | ConditionNoPositiveSupplyObservation
    | UnavailableConditionSupplyObservation
)


class CapturedConditionWalletFlows(DomainModel):
    status: Literal["CAPTURED_RECORDS"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    interval: ConditionInterval
    assembled_at: Timestamp
    flows: tuple[ConditionWalletFlow, ...]
    successful_queries: int | None = Field(ge=0)
    query_visibility: ConditionReceiptVisibility
    coverage: ConditionUnprovenWalletCoverage
    coverage_errors: tuple[ConditionInventoryCaptureError, ...]

    @model_validator(mode="after")
    def actual_flow_scope(self) -> Self:
        if self.query_visibility.assembled_at != self.assembled_at:
            raise ValueError("Wallet query visibility requires the actual snapshot assembly clock")
        identities: set[tuple[str, int | None]] = set()
        for flow in self.flows:
            stamp = flow.observed_at.value
            if (
                flow.token_id != self.token_id
                or stamp is None
                or not self.interval.start <= parse_iso_time(stamp) < self.interval.cutoff
            ):
                raise ValueError(
                    "Wallet flows must preserve their actual token and half-open occurrence window"
                )
            if flow.visibility.assembled_at != self.assembled_at:
                raise ValueError("Wallet flows cannot borrow another snapshot assembly clock")
            key = (flow.evidence.source_observation_sha256, flow.evidence.flow_index)
            if key in identities:
                raise ValueError("Wallet flow evidence cannot be duplicated in the selected observation set")
            identities.add(key)
        return self

    def observed_evidence(self) -> ConditionWalletSupplyObservation:
        validated = type(self).model_validate(self)
        positive: list[ConditionWalletFlowEvidenceRef] = []
        visible_count = 0
        for flow in validated.flows:
            visibility = flow.visibility.eligible()
            if not isinstance(visibility, ConditionObservedPredicate) or not visibility.value:
                continue
            visible_count += 1
            result = flow.positive_supply_witness()
            if isinstance(result, ConditionObservedPredicate) and result.value:
                positive.append(flow.evidence)
        if positive:
            return ConditionPositiveSupplyObservation(status="OBSERVED_POSITIVE", evidence=tuple(positive))
        query_visibility = validated.query_visibility.eligible()
        observed_query = (
            isinstance(query_visibility, ConditionObservedPredicate)
            and query_visibility.value
            and validated.successful_queries is not None
            and validated.successful_queries > 0
        )
        if visible_count or observed_query:
            return ConditionNoPositiveSupplyObservation(
                status="NO_POSITIVE_OBSERVATION", observation_count=visible_count
            )
        return UnavailableConditionSupplyObservation(status="UNAVAILABLE", reason="INCOMPLETE")


class UnavailableConditionWalletFlows(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: str = Field(min_length=1, pattern=r"\S")


type ConditionWalletFlows = CapturedConditionWalletFlows | UnavailableConditionWalletFlows


class ConditionLendingCatalogue(DomainModel):
    source_url: str = Field(min_length=1, pattern=r"\S")
    captured_at_label: str = Field(min_length=1, pattern=r"\S")
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class RegisteredConditionLendingIdentity(DomainModel):
    status: Literal["REGISTERED"]
    pool: LendingPool
    catalogue: ConditionLendingCatalogue
    capture_identity: RecordedConditionCaptureIdentity

    @model_validator(mode="after")
    def actual_pool_identity(self) -> Self:
        if (
            not self.pool.project.strip()
            or not self.pool.chain.strip()
            or not self.pool.underlying_tokens
            or any(not contract.strip() for contract in self.pool.underlying_tokens)
        ):
            raise ValueError(
                "Registered lending identity requires actual project/chain/underlying contract scope"
            )
        return self


class UnrecordedConditionLendingIdentity(DomainModel):
    status: Literal["UNRECORDED"]
    reason: Literal["REGISTRY_POOL_IDENTITY_NOT_CAPTURED"]


class ConditionLendingUtilization(DomainModel):
    value: Decimal | None = Field(ge=0)
    reason: Literal["INCOMPLETE", "ZERO_SUPPLY"] | None

    @model_validator(mode="after")
    def actual_utilization(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("Unavailable lending utilization requires actual missing/zero supply inputs")
        return self


class ConditionLendingUtilizationChange(DomainModel):
    value: float | None
    reason: Literal["INCOMPLETE", "ZERO_SUPPLY", "NOT_ESTIMABLE"] | None

    @model_validator(mode="after")
    def actual_change(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("Unavailable lending change requires the actual missing comparison cause")
        return self


class UnavailableConditionBorrowAPR(DomainModel):
    value: None
    reason: Literal["APY_IS_NOT_APR"]


class ConditionLendingPoolCalculation(DomainModel):
    pool_id: str = Field(min_length=1, pattern=r"\S")
    identity: RegisteredConditionLendingIdentity | UnrecordedConditionLendingIdentity
    total_supply_usd: Decimal | None = Field(ge=0)
    total_borrow_usd: Decimal | None = Field(ge=0)
    previous_utilization: Decimal | None = Field(ge=0)
    borrow_apy: float | None

    @model_validator(mode="after")
    def actual_registered_pool(self) -> Self:
        if (
            isinstance(self.identity, RegisteredConditionLendingIdentity)
            and self.identity.pool.pool_id != self.pool_id
        ):
            raise ValueError("Lending utilization cannot borrow another registered pool identity")
        return self

    def utilization(self) -> ConditionLendingUtilization:
        validated = type(self).model_validate(self)
        supply, borrowed = validated.total_supply_usd, validated.total_borrow_usd
        if supply is None or borrowed is None:
            return ConditionLendingUtilization(value=None, reason="INCOMPLETE")
        if supply == 0:
            return ConditionLendingUtilization(value=None, reason="ZERO_SUPPLY")
        span = (
            max(supply.adjusted(), borrowed.adjusted())
            - min(int(supply.as_tuple().exponent), int(borrowed.as_tuple().exponent))
            + 1
        )
        with localcontext() as context:
            context.prec = max(128, 2 * span + 16)
            value = borrowed / supply
        return ConditionLendingUtilization(value=value, reason=None)

    def change(self) -> ConditionLendingUtilizationChange:
        validated = type(self).model_validate(self)
        current = validated.utilization()
        if current.value is None:
            reason = current.reason
            if reason is None:
                raise ValueError("Unknown lending utilization requires its actual cause")
            return ConditionLendingUtilizationChange(value=None, reason=reason)
        previous = validated.previous_utilization
        if previous is None:
            return ConditionLendingUtilizationChange(value=None, reason="INCOMPLETE")
        current_float, previous_float = float(current.value), float(previous)
        if not isfinite(current_float) or not isfinite(previous_float):
            return ConditionLendingUtilizationChange(value=None, reason="NOT_ESTIMABLE")
        value = current_float - previous_float
        if not isfinite(value):
            return ConditionLendingUtilizationChange(value=None, reason="NOT_ESTIMABLE")
        return ConditionLendingUtilizationChange(value=value, reason=None)

    def borrow_apr(self) -> UnavailableConditionBorrowAPR:
        type(self).model_validate(self)
        return UnavailableConditionBorrowAPR(value=None, reason="APY_IS_NOT_APR")


class CapturedConditionLending(DomainModel):
    status: Literal["CAPTURED_POOLS"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    requested_window: Literal["W24H"]
    cutoff: datetime
    observed_at: Timestamp
    previous_observed_at: Timestamp
    current_visibility: ConditionReceiptVisibility
    previous_visibility: ConditionReceiptVisibility
    pools: tuple[ConditionLendingPoolCalculation, ...] = Field(min_length=1)
    unmapped_pool_ids: tuple[str, ...]

    @field_validator("cutoff")
    @classmethod
    def aware_cutoff(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_lending_scope(self) -> Self:
        current, previous = self.observed_at.value, self.previous_observed_at.value
        if current is None or parse_iso_time(current) > self.cutoff:
            raise ValueError("Captured lending requires its actual nonfuture source timestamp")
        if any(pool.previous_utilization is not None for pool in self.pools) and previous is None:
            raise ValueError("Observed utilization change requires its actual prior capture timestamp")
        if previous is not None and (
            parse_iso_time(previous) > parse_iso_time(current)
            or parse_iso_time(previous) > self.cutoff - timedelta(days=1)
        ):
            raise ValueError("Lending comparison cannot borrow a future prior/as-of observation")
        if self.current_visibility.assembled_at != self.previous_visibility.assembled_at:
            raise ValueError("Lending comparison requires one actual snapshot assembly clock")
        keys = {pool.pool_id for pool in self.pools}
        if len(keys) != len(self.pools) or keys.intersection(self.unmapped_pool_ids):
            raise ValueError("Captured lending pool identities must be distinct from unmapped identities")
        if len(set(self.unmapped_pool_ids)) != len(self.unmapped_pool_ids) or any(
            not key.strip() for key in self.unmapped_pool_ids
        ):
            raise ValueError("Unmapped lending pool identities must preserve actual distinct identifiers")
        return self


class UnavailableConditionLending(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: str = Field(min_length=1, pattern=r"\S")


type ConditionLending = CapturedConditionLending | UnavailableConditionLending

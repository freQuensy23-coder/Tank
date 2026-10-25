"""Condition-query evidence independent of orchestration and cached wire documents."""

from datetime import datetime
from decimal import Decimal, localcontext
from typing import Annotated, Literal, Self

from pydantic import Field, field_validator, model_validator

from tank.analysis.positioning import MissingOIDecomposition, OIDecomposition, oi_class
from tank.analysis.price_reference import ReferencePriceReturn
from tank.analysis.statistics_contracts import (
    BetaEstimate,
    DecimalRange,
    FloatBounds,
    FloatRange,
    HistoryDiagnostic,
    UnavailableBeta,
)
from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

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
    complete: Literal[True]
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
    sign: Literal[-1, 0, 1]

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
    sign: Literal[0]
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
    complete: Literal[True]
    volume_native: Decimal = Field(ge=0)
    buy_qty: Decimal = Field(ge=0)
    sell_qty: Decimal = Field(ge=0)
    cvd: Decimal
    buy_share: float | None = Field(ge=0, le=1)
    bs_ratio: float | None = Field(ge=0)
    sign: Literal[-1, 0, 1]
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
    share_majority: Literal[True]
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
    share_majority: Literal[True]

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


class ConditionUnavailablePredicate(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal[
        "INCOMPLETE", "INSUFFICIENT_HISTORY", "NOT_ESTIMABLE", "NOT_SUPPORTED", "ZERO_TOTAL_VOLUME"
    ]


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
    lead_lag: ObservedConditionLeadLag | UnavailableConditionLeadLag
    support_features: ConditionDerivativeSupportFeatures

    @model_validator(mode="after")
    def actual_enrichment_inputs(self) -> Self:
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

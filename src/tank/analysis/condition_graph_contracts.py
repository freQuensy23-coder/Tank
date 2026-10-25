"""Condition-query evidence independent of orchestration and cached wire documents."""

from datetime import datetime
from decimal import Decimal, localcontext
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

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

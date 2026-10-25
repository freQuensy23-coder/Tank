"""Decision facts, pure policy results and explicit publication identity."""

from datetime import datetime
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.watch_contracts import WatchCondition, WatchProposal, WatchRule
from tank.contracts import DomainModel
from tank.contracts.provenance import GenerationProvenance, ResolvedVersionIdentity
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

type Confidence = Literal["LOW", "MEDIUM", "HIGH"]
type Grade = Literal["A", "B", "C", "D"]
type Action = Literal["START", "WATCH_TO_START", "ADD", "HOLD", "WAIT", "DONT_CHASE", "EXIT", "REDUCE"]
type ActionRule = Literal[
    "A0",
    "A1",
    "A2",
    "A3",
    "A5",
    "A6",
    "A7",
    "A8",
    "A9",
    "A10",
    "A11",
    "A12",
    "B1",
    "B2",
    "B3",
    "B4",
    "WATCH_ANCHOR_LOST",
    "WATCH_SELL_DOWNGRADE",
    "START_HIGH_DATA_GUARD",
    "START_UNRESOLVED_GUARD",
]
type TradeReason = Literal[
    "TRADE_BASE_A",
    "TRADE_BASE_B",
    "TRADE_BASE_C",
    "TRADE_BASE_D",
    "CONTEXT_HEADWIND",
    "CONTEXT_STRESS",
]


type MoveClass = Literal[
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
type RepricingBand = Literal["NEGATIVE", "UNPRICED", "EARLY", "SUBSTANTIAL", "HEAVY", "UNKNOWN"]
type NormalPosition = Literal["ABOVE", "BELOW", "WITHIN"]
type Direction = Literal["BUY", "SELL", "NEUTRAL", "UNKNOWN"]
type Breadth = Literal["BROAD", "CONCENTRATED", "DIVERGENT", "NARROW", "INSUFFICIENT_DATA", "UNKNOWN"]
type Bias = Literal["UNKNOWN", "TWO_SIDED", "BEARISH", "BULLISH", "NEUTRAL"]
type Potential = Literal["LOW", "MEDIUM", "HIGH", "EXPLOSIVE"]
type Horizon = Literal["H1", "H4", "H12", "H24", "D3", "D7"]
type EventState = Literal[
    "RUMOR", "PROPOSED", "ANNOUNCED", "APPROVED", "SIGNED", "DEPLOYED", "LIVE", "MONEY_FLOWING", "UNKNOWN"
]
type ConfidenceInput = Literal["data", "certainty", "class"]


class DecisionMilestone(DomainModel):
    reference: str | None = Field(min_length=1)


class DecisionEvent(DomainModel):
    has_premise: bool
    state: EventState | None
    state_verified: bool | None
    invalidated: bool
    fatal_contradiction: bool
    unresolved_contradiction: bool
    certainty: Confidence
    milestone: DecisionMilestone | None


class DecisionAssessment(DomainModel):
    classification: Literal["LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"]
    impact_start: Literal["NOW", "DAYS", "WEEKS", "MONTHS", "YEARS", "UNKNOWN"]
    all_dependencies_within_24h: bool | None


class DecisionBeneficiary(DomainModel):
    role: Literal["PRIMARY", "SECONDARY", "LOSER", "UNKNOWN"]
    capture_class: Literal["DIRECT", "INDIRECT", "NARRATIVE_ONLY", "UNKNOWN"]
    credible: bool
    thesis_link_credible: bool | None
    exposure_score: int | None = Field(ge=0, le=10)


class DecisionRelative(DomainModel):
    repricing_band: RepricingBand | None
    market_context: Literal["NEUTRAL", "HEADWIND", "STRESS"] | None
    event_btc_adjusted_return: Decimal | None
    anchor_price: Decimal | None = Field(gt=0)
    entry_price: Decimal | None = Field(gt=0)


class DecisionLiquidity(DomainModel):
    state: Literal["NORMAL", "THIN", "VACUUM", "UNKNOWN"] | None
    compression: Literal["COMPRESSED", "NORMAL", "EXPANDED", "UNKNOWN"] | None


class UnavailableDecisionLiquidity(DomainModel):
    reason: Literal["INCOMPLETE"]


type DecisionLiquidityInput = DecisionLiquidity | UnavailableDecisionLiquidity


class DecisionSpot(DomainModel):
    direction: Direction | None
    breadth: Breadth | None
    selling_majority: bool | None
    complete_venues: int | None = Field(ge=0)


class DecisionOi(DomainModel):
    native_beyond: bool | None
    native_change: Decimal | None
    native_vs_normal: NormalPosition | None


class DecisionPositioning(DomainModel):
    synchronized: DecisionOi | None
    venue_funding: tuple[NormalPosition | None, ...]


class DecisionDivergence(DomainModel):
    absorption_confirmed: bool
    unresolved: tuple[str, ...]


class DecisionMarket(DomainModel):
    data_confidence: Confidence
    class_confidence: Confidence
    move: MoveClass
    relative: DecisionRelative
    liquidity: DecisionLiquidityInput
    spot: DecisionSpot
    positioning: DecisionPositioning
    divergence: DecisionDivergence
    seller_exhaustion: bool | None
    alternatives: tuple[str, ...]


class DecisionOpenCall(DomainModel):
    invalidation_price: Decimal = Field(gt=0)


class DecisionCallContext(DomainModel):
    call_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    status: Literal["OPEN"]
    opened_at: Timestamp
    invalidation_price: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def actual_call(self) -> Self:
        if any(not value.strip() for value in (self.call_id, self.event_id, self.token_id)):
            raise ValueError("Decision call context requires actual identities")
        self.opened_instant()
        return self

    def opened_instant(self) -> datetime:
        if self.opened_at.value is None:
            raise ValueError("Decision call context requires its actual opening clock")
        return parse_iso_time(self.opened_at.value)


class DecisionTriggers(DomainModel):
    roles: frozenset[Literal["UPGRADE", "DOWNGRADE", "INVALIDATION"]]
    anchor_lost: bool
    sell_downgrade: bool


class DecisionInputs(DomainModel):
    event: DecisionEvent
    assessment: DecisionAssessment
    beneficiary: DecisionBeneficiary
    market: DecisionMarket
    open_call: DecisionOpenCall | None
    triggers: DecisionTriggers


class DecisionWatch(WatchRule):
    milestone: DecisionMilestone | None


class ConfidenceLimitation(DomainModel):
    input: ConfidenceInput | None
    text: str | None


class ConfidenceCaps(DomainModel):
    data: Confidence
    event: Confidence
    market_truth: Confidence


class DecisionScenarios(DomainModel):
    base: str | None
    upside: str | None
    failure: str | None


class DecisionScenarioReasons(DomainModel):
    base: str | None
    upside: str | None
    failure: str | None


class DecisionOutlook(DomainModel):
    direction: Bias
    direction_rule: Bias
    potential: Potential
    potential_rule: Potential
    best_horizon: Horizon | None
    horizon_reason: str
    confidence: Confidence
    caps: ConfidenceCaps
    confidence_limiting: ConfidenceLimitation
    tactical: bool
    scenarios: DecisionScenarios
    scenario_reasons: DecisionScenarioReasons


class DecisionCore(DomainModel):
    thesis_grade: Grade
    thesis_rule: Grade
    trade_grade: Grade
    trade_rule: Grade
    trade_reasons: list[TradeReason]
    action: Action
    action_rule: ActionRule
    tactical: bool
    investigate: bool
    confidence_limiting: ConfidenceLimitation
    outlook: DecisionOutlook


class DecisionEvaluation(DecisionCore):
    conditions: list[DecisionWatch]


class DecisionProvenance(DomainModel):
    ems_id: str = Field(min_length=1)
    mta_id: str = Field(min_length=1)


class DecisionIdentity(DomainModel):
    decision_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    cutoff: Timestamp
    thresholds_version: str = Field(min_length=1)
    versions: ResolvedVersionIdentity
    generation: GenerationProvenance
    assessment_id: str = Field(min_length=1)
    market_truth_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def actual_provenance(self) -> Self:
        if any(
            not value.strip()
            for value in (
                self.decision_id,
                self.event_id,
                self.token_id,
                self.assessment_id,
                self.market_truth_id,
                self.thresholds_version,
            )
        ):
            raise ValueError("Decision identities and declared threshold version must be nonempty")
        if self.cutoff.value is None:
            raise ValueError("A produced decision requires its actual market cutoff")
        if (
            self.generation.config_version != self.versions.config_version
            or self.generation.code_version != self.versions.code_version
        ):
            raise ValueError("Decision generation does not match its resolved version identity")
        return self


type DecisionManagement = Literal["NORMAL", "MANAGEMENT_ONLY", "REMOVED_FROM_ANALYSIS"]


class DecisionPrices(DomainModel):
    entry_ref_price: Decimal = Field(gt=0)
    invalidation_price: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def protected_reference(self) -> Self:
        if self.invalidation_price > self.entry_ref_price:
            raise ValueError("Decision invalidation cannot exceed the selected entry reference")
        return self


class ActiveDecisionTracking(DomainModel):
    kind: Literal["ACTIVE"]


class DecisionStatusWriter(DomainModel):
    versions: ResolvedVersionIdentity
    generation: GenerationProvenance

    @model_validator(mode="after")
    def actual_writer_identity(self) -> Self:
        if (
            self.generation.config_version != self.versions.config_version
            or self.generation.code_version != self.versions.code_version
        ):
            raise ValueError("Decision status generation does not match its resolved writer identity")
        return self


class EndedDecisionTracking(DomainModel):
    kind: Literal["ENDED"]
    previous_decision_id: str = Field(min_length=1)
    expired_call_ids: tuple[str, ...] = Field(min_length=1)
    status_recorded_at: Timestamp
    writer: DecisionStatusWriter

    @model_validator(mode="after")
    def actual_status_recording(self) -> Self:
        if self.status_recorded_at.value is None or self.status_recorded_at.precision != "COLLECTION_ONLY":
            raise ValueError("Ended decision tracking requires its actual local status-recording clock")
        if len(self.expired_call_ids) != len(set(self.expired_call_ids)):
            raise ValueError("Expired decision call identities must be unique")
        return self


type DecisionTracking = ActiveDecisionTracking | EndedDecisionTracking


class DecisionMetadata(DomainModel):
    identity: DecisionIdentity
    calculated_at: Timestamp
    management: DecisionManagement
    contradiction_ids: tuple[str, ...]
    prices: DecisionPrices | None
    tracking: DecisionTracking

    @model_validator(mode="after")
    def actual_calculation(self) -> Self:
        if self.calculated_at.value is None:
            raise ValueError("A produced decision requires its actual calculation clock")
        if any(not value.strip() for value in self.contradiction_ids):
            raise ValueError("Decision contradiction identities must be nonempty")
        if isinstance(self.tracking, EndedDecisionTracking):
            if self.tracking.previous_decision_id == self.identity.decision_id:
                raise ValueError("Ended tracking cannot reference itself as predecessor")
            if any(not value.strip() for value in self.tracking.expired_call_ids):
                raise ValueError("Expired call identities must be nonempty")
        return self


class DecisionMaterialization(DomainModel):
    metadata: DecisionMetadata
    condition_ids: tuple[str, ...]

    @model_validator(mode="after")
    def watch_identities(self) -> Self:
        if any(not value.strip() for value in self.condition_ids):
            raise ValueError("Decision watch identities must be nonempty")
        if len(set(self.condition_ids)) != len(self.condition_ids):
            raise ValueError("Decision watch identities must be unique")
        return self


class DecisionDraft(DomainModel):
    calculation: DecisionCore
    metadata: DecisionMetadata
    proposals: list[WatchProposal]

    @model_validator(mode="after")
    def ended_watches(self) -> Self:
        if isinstance(self.metadata.tracking, EndedDecisionTracking) and self.proposals:
            raise ValueError("Ended tracking cannot propose active watch conditions")
        return self


class DecisionRecord(DomainModel):
    calculation: DecisionCore
    metadata: DecisionMetadata
    conditions: list[WatchCondition]
    conditions_shown: tuple[str, ...] = Field(max_length=2)
    outlook_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def reconciled_watches(self) -> Self:
        identity = self.metadata.identity
        for condition in self.conditions:
            if condition.event_id != identity.event_id or condition.token_id != identity.token_id:
                raise ValueError("Decision watch belongs to a different event or token")
            if condition.status != "ARMED":
                raise ValueError("Reconciled decision watches must actually be armed")
        ids = [condition.cond_id for condition in self.conditions]
        if len(ids) != len(set(ids)) or len(self.conditions_shown) != len(set(self.conditions_shown)):
            raise ValueError("Decision watch identities must be unique")
        if not set(self.conditions_shown) <= set(ids):
            raise ValueError("Shown decision watches must belong to the reconciled set")
        if isinstance(self.metadata.tracking, EndedDecisionTracking) and self.conditions:
            raise ValueError("Ended tracking cannot retain active watch conditions")
        return self


class OutlookConditionRefs(DomainModel):
    upgrade: tuple[str, ...]
    downgrade: tuple[str, ...]

    @model_validator(mode="after")
    def actual_distinct_references(self) -> Self:
        values = self.upgrade + self.downgrade
        if any(not value.strip() for value in values) or len(values) != len(set(values)):
            raise ValueError("Outlook watch references require distinct actual identities")
        return self

    @classmethod
    def from_conditions(cls, conditions: list[WatchCondition]) -> Self:
        return cls(
            upgrade=tuple(value.cond_id for value in conditions if value.role == "UPGRADE"),
            downgrade=tuple(value.cond_id for value in conditions if value.role == "DOWNGRADE"),
        )


class ReconciledOutlook(DomainModel):
    outlook_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    cutoff: Timestamp
    versions: ResolvedVersionIdentity
    generation: GenerationProvenance
    calculation: DecisionOutlook
    conditions: OutlookConditionRefs

    @model_validator(mode="after")
    def actual_projection_origin(self) -> Self:
        if any(not value.strip() for value in (self.outlook_id, self.event_id, self.token_id)):
            raise ValueError("An outlook row requires actual identities")
        if self.cutoff.value is None:
            raise ValueError("An outlook row requires its actual semantic cutoff")
        if (
            self.generation.config_version != self.versions.config_version
            or self.generation.code_version != self.versions.code_version
        ):
            raise ValueError("Outlook generation must match its actual version identity")
        return self

"""Publication facts, authored narrative and rendering policy independent of I/O."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, field_validator, model_validator

from tank.analysis.decision_contracts import (
    Action,
    Bias,
    Confidence,
    ConfidenceLimitation,
    EventState,
    Grade,
    Horizon,
    MoveClass,
    Potential,
    RepricingBand,
    TradeReason,
)
from tank.analysis.statistics_contracts import DecimalBounds
from tank.contracts import DomainModel
from tank.contracts.configuration import Thresholds
from tank.contracts.score import ScoreAssessmentView
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


class PublicationPriorFacts(DomainModel):
    state: EventState
    state_verified: bool
    negative_marker: NegativeEventMarker | None
    active_fatal_ids: frozenset[str]

    @field_validator("active_fatal_ids")
    @classmethod
    def actual_fatal_identities(cls, values: frozenset[str]) -> frozenset[str]:
        if any(not value.strip() for value in values):
            raise ValueError("Fatal publication facts require actual contradiction identities")
        return values


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

    def previous_facts(self) -> PublicationPriorFacts:
        return PublicationPriorFacts(
            state=self.state,
            state_verified=self.state_verified,
            negative_marker=self.negative_marker,
            active_fatal_ids=self.active_fatal_ids,
        )


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
    event: PublicationPriorFacts
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


type BeneficiaryLayer = Literal[
    "ISSUER_PRODUCT",
    "VENUE_DEX",
    "SETTLEMENT_L1_L2",
    "LIQUIDITY_EXECUTION",
    "LENDING_COLLATERAL",
    "ORACLE_DATA",
    "INTEROPERABILITY",
    "CUSTODY_COMPLIANCE",
]
type BeneficiaryRole = Literal["PRIMARY", "SECONDARY", "LOSER"]
type IntegrationStage = Literal[
    "COMPATIBLE",
    "IN_DEVELOPMENT",
    "BUILT",
    "SUBMITTED",
    "APPROVED",
    "LIVE",
    "MATERIAL_USAGE",
    "ECONOMIC_FLOW",
]
type MarketWindow = Literal["W15M", "W1H", "W4H", "W24H", "EVENT"]
type IdentityKey = tuple[Literal["TOKEN", "ENTITY", "NAME"], str]


class CardEvidenceReference(DomainModel):
    item_id: str | None = Field(min_length=1)
    url: str | None = Field(min_length=1)

    @model_validator(mode="after")
    def real_reference(self) -> Self:
        if self.item_id is None and self.url is None:
            raise ValueError("Displayed claim evidence requires its actual reference")
        return self


class CardClaim(DomainModel):
    claim_id: str = Field(min_length=1)
    subject: str
    predicate: str
    object_text: str
    verified: bool
    verification_gap: str | None
    evidence: tuple[CardEvidenceReference, ...]

    @model_validator(mode="after")
    def actual_fact(self) -> Self:
        if not " ".join((self.subject, self.predicate, self.object_text)).strip():
            raise ValueError("A displayed claim requires its actual statement")
        if self.verified and not self.evidence:
            raise ValueError("A displayed verified claim requires its recorded evidence")
        return self


class CardEvent(DomainModel):
    statement: str = Field(min_length=1)
    state: EventState
    state_verified: bool
    certainty: Confidence
    freshness: EventFreshness
    public_at: Timestamp
    known_to_us_at: Timestamp
    claims: tuple[CardClaim, ...]
    token_labels: dict[str, str]
    evidence_labels: dict[str, str]
    has_expired_call_fact: bool


class CardAssessment(DomainModel):
    classification: AssessmentClass
    criterion: str = Field(min_length=1)
    scores: ScoreAssessmentView


class CardRegistryMembership(DomainModel):
    token_ids: frozenset[str]
    registry_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def actual_member_identities(self) -> Self:
        if any(not value.strip() for value in self.token_ids):
            raise ValueError("Registry membership requires actual token identities")
        return self


class CardSubject(DomainModel):
    token_id: str | None
    entity_id: str | None
    entity_name: str | None
    role: BeneficiaryRole
    credible: bool
    registry_membership: Literal["REGISTERED", "UNREGISTERED_TOKEN", "NO_MAPPED_TOKEN"]
    outside_evidence_accepted: bool
    layer: BeneficiaryLayer | None
    integration_state: IntegrationStage | None
    integration_verified: bool

    @model_validator(mode="after")
    def named_identity(self) -> Self:
        if self.token_id is None and self.entity_id is None and self.entity_name is None:
            raise ValueError("A named beneficiary requires a token, entity or recorded external name")
        if (self.token_id is None) != (self.registry_membership == "NO_MAPPED_TOKEN"):
            raise ValueError("Participant registry classification disagrees with its token identity")
        return self

    @property
    def outside_scope(self) -> Literal["UNREGISTERED_TOKEN", "NO_MAPPED_TOKEN"] | None:
        if self.registry_membership == "REGISTERED":
            return None
        return self.registry_membership

    def identity_key(self) -> IdentityKey:
        if self.token_id is not None:
            return ("TOKEN", self.token_id)
        if self.entity_id is not None:
            return ("ENTITY", self.entity_id)
        if self.entity_name is None:
            raise ValueError("A named beneficiary requires its actual identity")
        return ("NAME", self.entity_name)


class CardBeneficiary(DomainModel):
    subject: CardSubject
    capture: Literal["DIRECT", "INDIRECT", "NARRATIVE_ONLY", "UNKNOWN"]
    causal_chain: str
    least_repriced: bool

    @model_validator(mode="after")
    def market_identity(self) -> Self:
        if self.subject.token_id is None:
            raise ValueError("A beneficiary market block requires its actual token identity")
        if self.subject.outside_scope is not None:
            raise ValueError("An outside-scope participant cannot have a selected financial block")
        return self

    @property
    def token_id(self) -> str:
        if self.subject.token_id is None:
            raise ValueError("A beneficiary market block requires its actual token identity")
        return self.subject.token_id


class CardMapping(DomainModel):
    layer_explanations: dict[BeneficiaryLayer, str]
    no_credible_secondary_reason: str | None


class CardScenarios(DomainModel):
    base: str | None
    upside: str | None
    failure: str | None
    base_reason: str | None
    upside_reason: str | None
    failure_reason: str | None


class CardOutlook(DomainModel):
    direction: Bias
    potential: Potential
    horizon: Horizon | None
    confidence: Confidence
    limitation: ConfidenceLimitation
    scenarios: CardScenarios


class CardCondition(DomainModel):
    role: Literal["UPGRADE", "DOWNGRADE", "INVALIDATION", "AGENT"]
    human_text: str = Field(min_length=1)


class ActiveCardDecision(DomainModel):
    kind: Literal["ACTIVE"]
    token_id: str = Field(min_length=1)
    action: Action
    thesis_grade: Grade
    trade_grade: Grade
    raw_trade_grade: Grade
    trade_reasons: tuple[TradeReason, ...]
    tactical: bool
    outlook: CardOutlook
    conditions: tuple[CardCondition, ...]


class InactiveCardDecision(DomainModel):
    kind: Literal["INACTIVE"]
    token_id: str = Field(min_length=1)
    reason: Literal["CALL_EXPIRED", "REMOVED_FROM_ANALYSIS"]


type CardDecision = ActiveCardDecision | InactiveCardDecision


class CardDataLimit(DomainModel):
    condition: str = Field(min_length=1)
    reason: str | None


class CardDataConfidence(DomainModel):
    classification: Confidence
    limiting: CardDataLimit | None


class CardReturns(DomainModel):
    raw: Decimal | None
    btc_adjusted: Decimal | None
    btc_adjusted_reason: str | None


class CardMarketContext(DomainModel):
    state: Literal["NEUTRAL", "HEADWIND", "STRESS"] | None
    btc_return_24h: Decimal | None
    normal_return_range: DecimalBounds | None


class CardRelative(DomainModel):
    repricing: RepricingBand | None
    returns_by_window: dict[MarketWindow, CardReturns]
    context: CardMarketContext


class CardOiAttribution(DomainModel):
    position_share: Decimal = Field(ge=0, le=1)
    price_markup: Decimal


class CardOiVenue(DomainModel):
    venue: str = Field(min_length=1)
    classification: Literal["REAL_NATIVE", "MIXED", "PRICE_MARKUP"]
    native_change: Decimal | None
    attribution: CardOiAttribution | None


class CardSynchronizedOi(DomainModel):
    native_change: Decimal | None
    native_vs_normal: Literal["BELOW", "WITHIN", "ABOVE"] | None


class CardDerivatives(DomainModel):
    window: MarketWindow
    venues: tuple[CardOiVenue, ...]
    synchronized: CardSynchronizedOi | None


class CardSpotVenue(DomainModel):
    venue: str = Field(min_length=1)
    sign: Literal[-1, 0, 1]
    volume_share: Decimal | None = Field(ge=0, le=1)

    @model_validator(mode="after")
    def directional_share(self) -> Self:
        if self.sign != 0 and self.volume_share is None:
            raise ValueError("A directional complete venue requires its actual volume share")
        return self


class CardExclusion(DomainModel):
    venue: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    detail: str | None
    source_reasons: tuple[str, ...]
    missing_metrics: tuple[str, ...]


class CardSpot(DomainModel):
    window: MarketWindow
    complete_venues: tuple[CardSpotVenue, ...]
    excluded: tuple[CardExclusion, ...]


class CardRevenuePeriod(DomainModel):
    value: Decimal
    provider_period: Literal["24h", "30d"]


class CardRevenueChartPoint(DomainModel):
    epoch_seconds: Decimal
    value: Decimal


class PeriodRevenueProtocol(DomainModel):
    kind: Literal["PERIOD"]
    protocol: str = Field(min_length=1)
    unit: Literal["USD"]
    period: CardRevenuePeriod


class ChartRevenueProtocol(DomainModel):
    kind: Literal["CHART"]
    protocol: str = Field(min_length=1)
    unit: Literal["USD"]
    point: CardRevenueChartPoint


class UnavailableRevenueProtocol(DomainModel):
    kind: Literal["UNAVAILABLE"]
    protocol: str = Field(min_length=1)
    reason: str | None


type CardRevenueProtocol = PeriodRevenueProtocol | ChartRevenueProtocol | UnavailableRevenueProtocol


class CardRevenue(DomainModel):
    protocols: tuple[CardRevenueProtocol, ...] = Field(min_length=1)
    observed_at: Timestamp
    stale: bool
    complete_24h: bool

    @model_validator(mode="after")
    def actual_observed_value(self) -> Self:
        if not any(
            isinstance(value, (PeriodRevenueProtocol, ChartRevenueProtocol)) for value in self.protocols
        ):
            raise ValueError("A rendered revenue footnote requires an actual provider value")
        if self.observed_at.value is None:
            raise ValueError("Rendered fundamental values require their actual observation clock")
        return self


class CardMarket(DomainModel):
    token_id: str = Field(min_length=1)
    move: MoveClass
    absorption_confirmed: bool
    unresolved: tuple[str, ...]
    confidence: CardDataConfidence
    decisive_facts: tuple[str, ...]
    relative: CardRelative
    derivatives: CardDerivatives
    spot: CardSpot
    holders_revenue: CardRevenue | None


class PendingCardAnalysis(DomainModel):
    kind: Literal["ANALYSIS_PENDING"]


class CardInputs(DomainModel):
    registry: CardRegistryMembership
    narrative: PublicationNarrative
    policy: PublicationPolicy
    event: CardEvent
    assessment: CardAssessment
    beneficiaries: tuple[CardBeneficiary, ...] = Field(max_length=3)
    other_subjects: tuple[CardSubject, ...]
    mapping: CardMapping | PendingCardAnalysis
    decisions_by_token: dict[str, CardDecision]
    market_by_token: dict[str, CardMarket]

    @model_validator(mode="after")
    def indexed_market_identity(self) -> Self:
        subjects = tuple(value.subject for value in self.beneficiaries) + self.other_subjects
        for subject in subjects:
            expected = (
                "NO_MAPPED_TOKEN"
                if subject.token_id is None
                else "REGISTERED"
                if subject.token_id in self.registry.token_ids
                else "UNREGISTERED_TOKEN"
            )
            if subject.registry_membership != expected:
                raise ValueError("Participant classification disagrees with actual registry membership")
        for beneficiary in self.beneficiaries:
            if beneficiary.token_id not in self.registry.token_ids:
                raise ValueError("A selected financial block requires affirmative registry membership")
        if any(key != value.token_id for key, value in self.decisions_by_token.items()):
            raise ValueError("Card decision index disagrees with its token identities")
        if any(key != value.token_id for key, value in self.market_by_token.items()):
            raise ValueError("Card market index disagrees with its token identities")
        tokens = [value.token_id for value in self.beneficiaries]
        if len(tokens) != len(set(tokens)):
            raise ValueError("A card cannot repeat a selected beneficiary token")
        if isinstance(self.mapping, PendingCardAnalysis) and (
            self.narrative.kind != "PARTIAL"
            or self.beneficiaries
            or self.other_subjects
            or self.decisions_by_token
            or self.market_by_token
        ):
            raise ValueError("Pending PARTIAL analysis cannot claim mapped financial subjects or results")
        if self.narrative.kind != "PARTIAL":
            mapping = self.mapping
            if not isinstance(mapping, CardMapping):
                raise ValueError("Full analysis requires its actual beneficiary mapping")
            subjects = self.display_subjects()
            if not any(value.role == "SECONDARY" and value.credible for value in subjects):
                reason = mapping.no_credible_secondary_reason
                if reason is None or not reason.strip():
                    raise ValueError("A card without a credible secondary requires its recorded reason")
            if not any(value.role == "SECONDARY" for value in subjects):
                layers = {
                    "ISSUER_PRODUCT",
                    "VENUE_DEX",
                    "SETTLEMENT_L1_L2",
                    "LIQUIDITY_EXECUTION",
                    "LENDING_COLLATERAL",
                    "ORACLE_DATA",
                    "INTEROPERABILITY",
                    "CUSTODY_COMPLIANCE",
                }
                if set(mapping.layer_explanations) != layers or any(
                    not text.strip() for text in mapping.layer_explanations.values()
                ):
                    raise ValueError("A card without a secondary requires the actual eight layer assessments")
            for beneficiary in self.beneficiaries:
                token = beneficiary.token_id
                if token not in self.decisions_by_token:
                    raise ValueError("MISSING_DECISION: " + token)
                if (
                    isinstance(self.decisions_by_token[token], ActiveCardDecision)
                    and token not in self.market_by_token
                ):
                    raise ValueError("MISSING_MARKET_TRUTH: " + token)
        return self

    def display_subjects(self) -> tuple[CardSubject, ...]:
        named = tuple(value.subject for value in self.beneficiaries) + self.other_subjects
        primary_ids = {edge.identity_key() for edge in named if edge.role == "PRIMARY"}
        primary_entities = {
            edge.entity_id for edge in named if edge.role == "PRIMARY" and edge.entity_id is not None
        }
        shown: set[IdentityKey] = set()
        subjects: list[CardSubject] = []
        for edge in named:
            if edge.registry_membership != "REGISTERED" and not edge.outside_evidence_accepted:
                continue
            identity = edge.identity_key()
            if edge.role == "SECONDARY" and (
                not (
                    edge.credible
                    or (edge.registry_membership != "REGISTERED" and edge.outside_evidence_accepted)
                )
                or identity in primary_ids
                or edge.entity_id in primary_entities
            ):
                continue
            if identity not in shown:
                shown.add(identity)
                subjects.append(edge)
        return tuple(subjects)


class CardLengthSection(DomainModel):
    line: int = Field(ge=1)
    section: str
    chars: int = Field(ge=0)


class CardLengthDiagnostics(DomainModel):
    model_config = ConfigDict(validate_by_name=True)

    actual: int = Field(ge=0)
    max_chars: int = Field(gt=0, validation_alias="max", serialization_alias="max")
    excess: int = Field(gt=0)
    sections: tuple[CardLengthSection, ...]
    newline_chars: int = Field(ge=0)
    editable_fields: tuple[str, ...]
    guidance: str
    preview: str

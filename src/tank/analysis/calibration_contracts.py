"""Frozen calibration labels, derived history and measured probability contracts."""

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import Field, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_items import SourceIdentityHints
from tank.contracts.sources import SourceAuthorRole

type SourceID = Annotated[str, Field(pattern=r"^S(?:0[1-9]|[12][0-9]|3[0-4])$")]
type Probability = Annotated[float, Field(ge=0, le=1)]
type Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


def _aware_iso(value: str) -> str:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Calibration clocks require an explicit timezone")
    return value


def _aware_clock(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Calibration clocks require an explicit timezone")
    return value


class CalibrationSource(DomainModel):
    item_id: str = Field(min_length=1)
    source_id: SourceID
    source: str
    author: str
    title: str
    text: str
    quoted: str | None = None
    subtype: Literal["releases", "commits"] | None = None
    author_role: SourceAuthorRole | None = None
    author_organization: str | None = None
    identity_hints: SourceIdentityHints | None = None
    monitored_author: str | None = None
    source_name: str | None = None
    url: str | None = None
    published_at: str | None = None
    observed_at: str | None = None
    raw_sha256: Digest | None = None
    content_hash: Digest | None = None
    external_id: str | None = None

    @field_validator("published_at", "observed_at")
    @classmethod
    def captured_clock(cls, value: str | None) -> str | None:
        return None if value is None else _aware_iso(value)

    @model_validator(mode="after")
    def original_content(self) -> Self:
        if self.source_id == "S25" and self.subtype is None:
            raise ValueError("S25 calibration requires captured releases/commits subtype")
        if self.source_id == "S29" and self.author_role is None:
            raise ValueError("S29 calibration requires captured author role")
        if not self.title.strip() and not self.text.strip():
            raise ValueError("Original item text required")
        return self


class CalibrationLabel(DomainModel):
    item_id: str = Field(min_length=1)
    source_id: SourceID
    clock: str
    relevant: bool
    event_id: str | None
    source_item: CalibrationSource
    clock_basis: str | None = None
    label_rationale: str | None = None

    @field_validator("clock")
    @classmethod
    def historical_clock(cls, value: str) -> str:
        return _aware_iso(value)

    @model_validator(mode="after")
    def matching_identity(self) -> Self:
        if self.item_id != self.source_item.item_id or self.source_id != self.source_item.source_id:
            raise ValueError("Calibration label identity differs from its source item")
        return self


class CalibrationEntity(DomainModel):
    entity_id: str
    role: Literal["ACTOR", "COUNTERPARTY", "REGULATOR", "SUBJECT"] | None = None


class CalibrationEvent(DomainModel):
    event_id: str = Field(min_length=1)
    statement: str = Field(min_length=1)
    entities: list[CalibrationEntity | str]
    tokens_mentioned: list[str]
    event_state: str = Field(min_length=1)
    statement_basis: str | None = None
    reviewed_group_event_id: str | None = None
    opened_by: str | None = None
    known_to_us_at: str | None = None


class CalibrationPredecessor(DomainModel):
    dataset_id: str
    path: str
    raw_sha256: Digest
    labels_sha256: Digest
    history_sha256: Digest


class CalibrationProvenance(DomainModel):
    successor_of: CalibrationPredecessor
    relevance_labels: str
    event_groups: str
    corrections: list[str]
    never_candidate_placeholder_events: int = Field(ge=0)


class CalibrationPacket(DomainModel):
    dataset_id: str = Field(min_length=1)
    status: Literal["FROZEN"]
    labeler: str = Field(min_length=1)
    labeled_at: str
    routing_window_days: int = Field(gt=0)
    items: list[CalibrationLabel] = Field(min_length=1)
    events: list[CalibrationEvent] = Field(min_length=1)
    provenance: CalibrationProvenance | None = None

    @field_validator("labeled_at")
    @classmethod
    def labeling_clock(cls, value: str) -> str:
        return _aware_iso(value)

    @model_validator(mode="after")
    def unique_identities(self) -> Self:
        if len({row.item_id for row in self.items}) != len(self.items):
            raise ValueError("Items need unique item_id matching source_item")
        if len({event.event_id for event in self.events}) != len(self.events):
            raise ValueError("Events need unique event_id and a statement")
        return self


class CalibrationRow(DomainModel):
    label: CalibrationLabel
    index: int = Field(ge=0)
    at: datetime
    filtered: bool
    routed: bool
    expected_event_id: str | None
    cross_source_repeat: bool | None

    @field_validator("at")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        return _aware_clock(value)


class CalibrationHistory(DomainModel):
    known_at: datetime
    last_at: datetime
    sources: frozenset[str]

    @field_validator("known_at", "last_at")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        return _aware_clock(value)


class HistoricalCalibrationEvent(DomainModel):
    label: CalibrationEvent
    known_to_us_at: datetime | None

    @field_validator("known_to_us_at")
    @classmethod
    def aware_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _aware_clock(value)


class CandidateScore(DomainModel):
    event_id: str
    score: Probability


class FilterEvaluation(DomainModel):
    relevant: bool
    filter_score: Probability


class RoutingEvaluation(DomainModel):
    item_id: str
    routed: bool
    expected_event_id: str | None
    candidates: list[CandidateScore]


class ScoredCalibrationRow(DomainModel):
    row: CalibrationRow
    filter_score: Probability
    candidates: list[CandidateScore]

    def filter_evaluation(self) -> FilterEvaluation:
        return FilterEvaluation(relevant=self.row.label.relevant, filter_score=self.filter_score)

    def routing_evaluation(self) -> RoutingEvaluation:
        return RoutingEvaluation(
            item_id=self.row.label.item_id,
            routed=self.row.routed,
            expected_event_id=self.row.expected_event_id,
            candidates=self.candidates,
        )


class RoutingError(DomainModel):
    item_id: str
    predicted: str | None
    expected: str | None
    predicted_score: Probability | None


class RoutingSummary(DomainModel):
    cutoff: Probability
    true_positive: int = Field(ge=0)
    false_positive: int = Field(ge=0)
    false_negative: int = Field(ge=0)
    precision: Probability | None
    recall: Probability | None


class RoutingMetrics(RoutingSummary):
    errors: list[RoutingError]

    def summary(self) -> RoutingSummary:
        return RoutingSummary(
            cutoff=self.cutoff,
            true_positive=self.true_positive,
            false_positive=self.false_positive,
            false_negative=self.false_negative,
            precision=self.precision,
            recall=self.recall,
        )


class CalibrationGeneration(ResolvedVersionIdentity):
    packet_sha256: Digest
    prompt_version: str
    registry_sha256: Digest
    routing_window_days: int = Field(gt=0)
    jev_endpoint: Literal["https://opencode.ai/inference/systemone/v1/systemone"]
    jev_request_model: Literal["jev-1.13-free", "jev-1.13"]
    jev_resolved_models: list[str]


class CalibrationCounts(DomainModel):
    items: int = Field(ge=0)
    filter_path_items: int = Field(ge=0)
    relevant: int = Field(ge=0)
    routed_items: int = Field(ge=0)
    repeats: int = Field(ge=0)
    cross_source_repeats: int = Field(ge=0)
    candidate_pairs: int = Field(ge=0)
    sources: list[SourceID]


class SelectedThresholds(DomainModel):
    filter_cutoff: Probability
    routing_cutoff: Probability | None


class CalibrationChecks(DomainModel):
    sample_minimums: bool
    crypto_reach_full_analysis_95: bool
    routing_precision_recall_90: bool


class FilterMetrics(DomainModel):
    cutoff: Probability
    relevant_passed: int = Field(ge=0)
    relevant: int = Field(ge=0)
    crypto_reaching_full_analysis: int = Field(ge=0)
    filter_path_irrelevant_skipped: int = Field(ge=0)
    filter_path_irrelevant: int = Field(ge=0)


class BestMeasuredRouting(DomainModel):
    cutoff: Probability
    metrics: RoutingMetrics
    automatic_apply: Literal[False]


type CalibrationRejection = Literal[
    "sample_minimums", "crypto_reach_full_analysis_95", "routing_precision_recall_90"
]


class CalibrationReport(DomainModel):
    dataset_id: str
    generation: CalibrationGeneration
    status: Literal["PASS", "FAIL"]
    rejection_reasons: list[CalibrationRejection]
    counts: CalibrationCounts
    selected: SelectedThresholds
    routing_target_90_90_met: bool
    v0b_checks: CalibrationChecks
    filter: FilterMetrics
    routing: RoutingMetrics | None
    best_measured_routing: BestMeasuredRouting
    routing_curve: list[RoutingSummary]

    @model_validator(mode="after")
    def consistent_acceptance(self) -> Self:
        if (self.status == "PASS") != (not self.rejection_reasons):
            raise ValueError("Calibration status disagrees with its rejection reasons")
        if self.status == "PASS" and self.selected.routing_cutoff is None:
            raise ValueError("Passing calibration requires a routing cutoff")
        if self.routing_target_90_90_met != (self.selected.routing_cutoff is not None):
            raise ValueError("Routing target result disagrees with selected cutoff")
        return self


class PassedThresholdProposal(DomainModel):
    status: Literal["PASS"]
    packet_sha256: Digest
    filter_cutoff: Probability
    routing_cutoff: Probability


class FailedThresholdProposal(DomainModel):
    status: Literal["FAIL"]
    packet_sha256: Digest


class ScoredCalibrationWire(DomainModel):
    item_id: str
    source_id: SourceID
    clock: str
    relevant: bool
    event_id: str | None
    clock_basis: str | None = None
    label_rationale: str | None = None
    index: int
    at: str
    filtered: bool
    routed: bool
    expected_event_id: str | None
    cross_source_repeat: bool | None
    filter_score: Probability
    candidates: list[CandidateScore]

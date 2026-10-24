"""Closed capture lifecycle records and outcomes shared with the external Go writer."""

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, JsonValue, PositiveInt, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.configuration import Thresholds
from tank.market.capture import EventAnchor
from tank.time_precision import Timestamp
from tank.util import parse_time

type BackfillStatus = Literal["PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED"]
type RepairStatus = Literal["PENDING", "RUNNING", "COMPLETE", "FAILED", "EXPIRED"]
type CaptureTier = Literal["BASE", "HOT"]
type SupplyKind = Literal[
    "EXCHANGE_INVENTORY",
    "WALLET_FLOWS",
    "UNLOCKS",
    "LENDING",
    "CHAIN_LIQUIDITY",
    "FUNDAMENTALS",
]


def _clock(value: str | None, precision: str | None, reason: str | None, *, absent: bool = False) -> None:
    if absent and value is None and precision is None and reason is None:
        return
    Timestamp(value, precision, reason)


class CaptureMetadata(DomainModel):
    config_version: str | None = None
    code_version: str | None = None
    semantic_fingerprint: str | None = None
    recorded_at: str | None = None
    recorded_at_prec: Literal["COLLECTION_ONLY"] | None = None
    recorded_at_prec_reason: str | None = None

    @model_validator(mode="after")
    def declared_recording_clock(self) -> Self:
        _clock(self.recorded_at, self.recorded_at_prec, self.recorded_at_prec_reason, absent=True)
        return self


class CapturePointMetadata(CaptureMetadata):
    instrument_id: str | None = None
    received_ts: str | None = None
    received_ts_prec: Literal["COLLECTION_ONLY"] | None = None
    received_ts_prec_reason: str | None = None

    @model_validator(mode="after")
    def declared_receipt_clock(self) -> Self:
        _clock(self.received_ts, self.received_ts_prec, self.received_ts_prec_reason, absent=True)
        return self


class HotToken(CapturePointMetadata):
    token_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    priority: int
    since: str
    since_prec: str | None
    since_prec_reason: str | None
    expires_at: str | None
    expires_at_prec: str | None
    expires_at_prec_reason: str | None

    @model_validator(mode="after")
    def declared_lifetime(self) -> Self:
        _clock(self.since, self.since_prec, self.since_prec_reason)
        if self.expires_at is not None:
            _clock(self.expires_at, self.expires_at_prec, self.expires_at_prec_reason)
        elif self.expires_at_prec is not None:
            raise ValueError("Protected hot tokens cannot claim a finite expiry clock")
        return self


class CoverageMinutes(DomainModel):
    expected_minutes: int = Field(ge=0)
    complete_minutes: int = Field(ge=0)
    missing_or_incomplete_minutes: int = Field(ge=0)
    complete: bool


class CaptureWindowCoverage(DomainModel):
    start: str
    end: str
    observed_bucket_start: str
    observed_bucket_end: str
    timestamp_precision: Literal["BUCKET"]
    unresolved_boundary_seconds: float = Field(ge=0)
    flow: CoverageMinutes
    klines: CoverageMinutes


class CaptureWindows(DomainModel):
    W1H: CaptureWindowCoverage
    W4H: CaptureWindowCoverage
    W24H: CaptureWindowCoverage


class Pre24Reference(DomainModel):
    available: bool
    trade_id: str | int | None
    ts: str | None
    ts_prec: str | None
    reason: Literal["INCOMPLETE"] | None


class InstrumentCaptureCoverage(DomainModel):
    instrument_id: str = Field(min_length=1)
    windows: CaptureWindows
    complete: bool
    pre24_reference: Pre24Reference | None = None


class BenchmarkKlineCoverage(DomainModel):
    instrument_id: str = Field(min_length=1)
    start: str
    end: str
    expected_minutes: int = Field(ge=0)
    complete_minutes: int = Field(ge=0)
    complete: bool
    includes_anchor_close: bool


class BackfillResult(DomainModel):
    instruments: list[InstrumentCaptureCoverage]
    benchmark_klines: list[BenchmarkKlineCoverage]


class BackfillRecord(CapturePointMetadata):
    backfill_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    anchor: str | None
    anchor_prec: str | None
    anchor_prec_reason: str | None
    started_at: str | None
    started_at_prec: Literal["COLLECTION_ONLY"] | None
    started_at_prec_reason: str | None
    finished_at: str | None
    finished_at_prec: Literal["COLLECTION_ONLY"] | None
    finished_at_prec_reason: str | None
    status: BackfillStatus
    reason: str | None

    @model_validator(mode="after")
    def lifecycle_clocks(self) -> Self:
        if self.status != "FAILED":
            if self.anchor is None:
                raise ValueError("A capture obligation requires its declared anchor")
            _clock(self.anchor, self.anchor_prec, self.anchor_prec_reason)
        _clock(
            self.started_at,
            self.started_at_prec,
            self.started_at_prec_reason,
            absent=self.status == "FAILED",
        )
        _clock(self.finished_at, self.finished_at_prec, self.finished_at_prec_reason)
        if self.status in {"PENDING", "RUNNING"} and self.finished_at is not None:
            raise ValueError("An unfinished capture obligation cannot claim its finish time")
        if self.status in {"COMPLETE", "PARTIAL", "FAILED"} and self.finished_at is None:
            raise ValueError("A terminal capture obligation requires its finish observation")
        return self


class BackfillPending(BackfillRecord):
    anchor: str
    finished_at: None
    status: Literal["PENDING"]
    result: None


class BackfillRunning(BackfillRecord):
    anchor: str
    finished_at: None
    status: Literal["RUNNING"]
    result: None


class BackfillComplete(BackfillRecord):
    anchor: str
    finished_at: str
    status: Literal["COMPLETE"]
    reason: None
    result: BackfillResult


class BackfillPartial(BackfillRecord):
    anchor: str
    finished_at: str
    status: Literal["PARTIAL"]
    reason: str
    result: BackfillResult


class BackfillFailed(BackfillRecord):
    finished_at: str
    status: Literal["FAILED"]
    reason: str
    result: BackfillResult | None


type BackfillRun = Annotated[
    BackfillPending | BackfillRunning | BackfillComplete | BackfillPartial | BackfillFailed,
    Field(discriminator="status"),
]


class BackfillEnvelope(RootModel[BackfillRun]):
    model_config = ConfigDict(
        strict=True, frozen=True, validate_default=True, allow_inf_nan=False, revalidate_instances="always"
    )


class InstrumentRepairCounts(DomainModel):
    incomplete_before: int = Field(ge=0)
    incomplete_after: int = Field(ge=0)


class RepairResult(DomainModel):
    instruments: dict[str, InstrumentRepairCounts]


class RepairRecord(CaptureMetadata):
    request_id: str = Field(min_length=1)
    kind: Literal["REPAIR_WINDOWS"]
    event_id: str | None = None
    token_id: str | None
    start: str | None
    start_prec: str | None
    start_prec_reason: str | None
    end: str | None
    end_prec: str | None
    end_prec_reason: str | None
    requested_at: str | None
    requested_at_prec: str | None
    requested_at_prec_reason: str | None
    deadline: str | None
    deadline_prec: str | None
    deadline_prec_reason: str | None
    started_at: str | None
    started_at_prec: Literal["COLLECTION_ONLY"] | None
    started_at_prec_reason: str | None
    finished_at: str | None
    finished_at_prec: Literal["COLLECTION_ONLY"] | None
    finished_at_prec_reason: str | None
    writer: str | None = None
    request: None = None
    status: RepairStatus
    reason: str | None

    @model_validator(mode="after")
    def lifecycle_clocks(self) -> Self:
        if self.status == "COMPLETE":
            if self.token_id is None or self.token_id == "":
                raise ValueError("Completed repair requests require a token identity")
            for value, precision, reason in (
                (self.start, self.start_prec, self.start_prec_reason),
                (self.end, self.end_prec, self.end_prec_reason),
                (self.deadline, self.deadline_prec, self.deadline_prec_reason),
            ):
                if value is None:
                    raise ValueError("Completed repairs require their declared bounds and deadline")
                parse_time(value)
                if precision is not None or reason is not None:
                    _clock(value, precision, reason)
        _clock(self.started_at, self.started_at_prec, self.started_at_prec_reason, absent=True)
        _clock(
            self.finished_at,
            self.finished_at_prec,
            self.finished_at_prec_reason,
            absent=self.status in {"PENDING", "RUNNING"},
        )
        if self.status in {"PENDING", "RUNNING"} and self.finished_at is not None:
            raise ValueError("An unfinished repair cannot claim its finish time")
        if self.status in {"COMPLETE", "FAILED", "EXPIRED"} and self.finished_at is None:
            raise ValueError("A terminal repair requires its finish observation")
        return self


class ExecutableRepair(RepairRecord):
    token_id: str
    start: str
    end: str
    deadline: str
    budget_s: Decimal | None = Field(gt=0)


class RepairPending(RepairRecord):
    """Queued input has not yet passed the worker's business validation."""

    finished_at: None
    status: Literal["PENDING"]
    reason: None
    budget_s: str | None
    result: None


class RepairRunning(RepairRecord):
    """RUNNING records a claimed lease; invalid legacy input may still await rejection."""

    finished_at: None
    status: Literal["RUNNING"]
    reason: None
    budget_s: str | None
    result: None


class RepairComplete(ExecutableRepair):
    finished_at: str
    status: Literal["COMPLETE"]
    reason: None
    result: RepairResult


class RepairFailed(RepairRecord):
    """Rejected input remains evidence; SQL numeric text such as NaN is never a usable budget."""

    finished_at: str
    status: Literal["FAILED"]
    reason: str
    budget_s: str | None
    result: RepairResult | None


class RepairExpired(RepairRecord):
    finished_at: str
    status: Literal["EXPIRED"]
    reason: str
    budget_s: str | None
    result: None


type RepairRequest = Annotated[
    RepairPending | RepairRunning | RepairComplete | RepairFailed | RepairExpired,
    Field(discriminator="status"),
]


class RepairEnvelope(RootModel[RepairRequest]):
    model_config = ConfigDict(
        strict=True, frozen=True, validate_default=True, allow_inf_nan=False, revalidate_instances="always"
    )


class CaptureReadiness(DomainModel):
    event_id: str
    token_id: str
    status: Literal["NOT_REQUESTED", "PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED"]
    reason: str | None
    backfill_id: str | None
    anchor: str | None

    @model_validator(mode="after")
    def readiness_identity(self) -> Self:
        if self.status == "NOT_REQUESTED" and self.backfill_id is not None:
            raise ValueError("A missing capture obligation cannot claim a backfill identity")
        if self.status != "NOT_REQUESTED" and self.backfill_id is None:
            raise ValueError("A durable capture status requires its backfill identity")
        return self


class CaptureWaitOutcome(DomainModel):
    status: Literal["INCOMPLETE", "COMPLETE", "PARTIAL", "FAILED"]
    backfill_id: str | None
    reason: str | None
    bounded_wait_s: float = Field(ge=0)
    observed_at: Timestamp
    incomplete_windows_excluded: Literal[True] = True

    @model_validator(mode="after")
    def observed_collection_clock(self) -> Self:
        if self.observed_at.value is None or self.observed_at.precision != "COLLECTION_ONLY":
            raise ValueError("Capture waits require an observed collection clock")
        return self

    def to_wire(self) -> dict[str, JsonValue]:
        observed = self.observed_at.value
        if observed is None:
            raise ValueError("Capture wait requires its observation clock")
        return {
            "status": self.status,
            "backfill_id": self.backfill_id,
            "reason": self.reason,
            "bounded_wait_s": self.bounded_wait_s,
            "observed_at": observed,
            "incomplete_windows_excluded": self.incomplete_windows_excluded,
        }


class RepairCompleted(DomainModel):
    status: Literal["COMPLETE"]
    request_id: str
    instruments: dict[str, InstrumentRepairCounts]


class RepairUnavailable(DomainModel):
    status: Literal["FAILED", "EXPIRED", "INCOMPLETE"]
    request_id: str
    reason: str
    result: RepairResult | None


class RepairNotRun(DomainModel):
    status: Literal["NOT_RUN"]
    reason: Literal["REPLAY_USES_RETAINED_MARKET_INPUTS"]


type RepairOutcome = RepairCompleted | RepairUnavailable | RepairNotRun


class CaptureEventContext(DomainModel):
    event_id: str
    current_ems_id: str | None
    anchor: EventAnchor


class CaptureAssessment(DomainModel):
    ems_id: str
    event_id: str
    version: int = Field(ge=1)
    cutoff: str
    assessment_class: Literal["LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"] = Field(alias="class")


class TokenEscalation(DomainModel):
    token_id: str = Field(min_length=1)
    priority: int | None = None


class CaptureCadences(DomainModel):
    oi_funding_s: float = Field(gt=0)
    kline_s: float = Field(gt=0)
    book_s: float = Field(gt=0)
    supply_by_kind: dict[SupplyKind, PositiveInt]

    @model_validator(mode="after")
    def complete_supply_policy(self) -> Self:
        required: frozenset[SupplyKind] = frozenset(
            {
                "EXCHANGE_INVENTORY",
                "WALLET_FLOWS",
                "UNLOCKS",
                "LENDING",
                "CHAIN_LIQUIDITY",
                "FUNDAMENTALS",
            }
        )
        if self.supply_by_kind.keys() != required:
            raise ValueError("Capture cadence policy must declare every supply kind")
        return self


class CaptureTierTransition(DomainModel):
    transition_id: str
    transition_seq: int = Field(ge=1)
    event_ids: list[str]
    token_id: str
    tier: CaptureTier
    effective_at: str
    effective_at_prec: Literal["COLLECTION_ONLY"]
    effective_at_prec_reason: None
    reason: str
    capture_cadences: CaptureCadences
    config_version: str
    code_version: str


class CapturePolicy(DomainModel):
    thresholds: Thresholds
    replay: bool
    request_budget_s: Decimal = Field(gt=0)
    request_grace_s: Decimal = Field(ge=0)
    hot_ttl_hours: int = Field(gt=0)
    config_version: str
    code_version: str


def capture_time_value(timestamp: Timestamp) -> str:
    if timestamp.value is None:
        raise ValueError("Capture application clocks require an observed or declared value")
    return timestamp.value


class NewRepairRequest(DomainModel):
    request_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    start: Timestamp
    end: Timestamp
    requested_at: Timestamp
    deadline: Timestamp
    budget_s: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def bounded_request(self) -> Self:
        start, end = parse_time(capture_time_value(self.start)), parse_time(capture_time_value(self.end))
        requested = parse_time(capture_time_value(self.requested_at))
        deadline = parse_time(capture_time_value(self.deadline))
        if start >= end:
            raise ValueError("Repair request start must precede its end")
        if deadline <= requested:
            raise ValueError("Repair deadline must follow its request observation")
        return self


class HotTokenIntent(DomainModel):
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    priority: int
    since: Timestamp
    expires_at: Timestamp | None

    @model_validator(mode="after")
    def lifetime_values(self) -> Self:
        capture_time_value(self.since)
        if self.expires_at is not None:
            capture_time_value(self.expires_at)
        return self


class NewBackfillRequest(DomainModel):
    backfill_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    anchor: Timestamp
    requested_at: Timestamp

    @model_validator(mode="after")
    def declared_values(self) -> Self:
        capture_time_value(self.anchor)
        capture_time_value(self.requested_at)
        return self

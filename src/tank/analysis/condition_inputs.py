"""Consumed spot inputs, independent of transport, storage and output serialization."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from tank.analysis.condition_graph_contracts import ConditionInterval, ConditionWindow
from tank.contracts import DomainModel
from tank.contracts.configuration import Thresholds
from tank.market.trade_contracts import EmptyBoundarySegment
from tank.time_precision import Timestamp
from tank.util import parse_iso_time


class SpotInstrument(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    market_type: Literal["SPOT"]


class FlowSourceDescriptor(DomainModel):
    format: Literal["SPOT_INPUT_READ_V1"]
    collection: Literal["flow_bars", "flow_segments", "flow_parent_bars"]
    row_index: int = Field(ge=0)
    parent_index: int | None = Field(ge=0)
    original_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    effective_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_basis: Literal["INSTRUMENT_READ", "SOURCE_ROW"]
    scope: SpotInstrument
    parent_source_evidence_sha256: str | None = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def actual_parent_scope(self) -> Self:
        if (self.parent_index is None) != (self.parent_source_evidence_sha256 is None):
            raise ValueError("Embedded source lineage requires its actual parent identity")
        if self.parent_index is not None and self.collection not in {"flow_bars", "flow_parent_bars"}:
            raise ValueError("Only an embedded flow-bar piece inherits a parent flow projection")
        return self


class FlowInputEvidenceRef(DomainModel):
    collection: Literal["flow_bars", "flow_segments", "flow_parent_bars"]
    row_index: int = Field(ge=0)
    parent_index: int | None = Field(ge=0)
    source_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class SpotFlowParent(DomainModel):
    """Actual parent restrictions; a last-trade receipt is not a document write clock."""

    instrument: SpotInstrument
    start: datetime
    end: datetime
    received_at: Timestamp
    observed_at: Timestamp
    evidence: FlowInputEvidenceRef

    @model_validator(mode="after")
    def actual_parent_minute(self) -> Self:
        start, end = parse_iso_time(self.start), parse_iso_time(self.end)
        if start.second or start.microsecond or end != start + timedelta(minutes=1):
            raise ValueError("A source parent requires its actual aligned minute interval")
        if self.evidence.parent_index is not None:
            raise ValueError("A source parent cannot itself be an embedded piece")
        return self


class SpotFlowObservation(DomainModel):
    instrument: SpotInstrument
    start: datetime
    end: datetime
    kind: Literal["FULL_MINUTE", "BOUNDARY"]
    start_clock: Timestamp
    end_clock: Timestamp
    complete: bool | None
    buy_qty: Decimal | None = Field(ge=0)
    sell_qty: Decimal | None = Field(ge=0)
    sum_sq_qty: Decimal | None = Field(ge=0)
    buy_notional: Decimal | None = Field(ge=0)
    sell_notional: Decimal | None = Field(ge=0)
    low: Decimal | None = Field(gt=0)
    n_buy: int | None = Field(ge=0)
    n_sell: int | None = Field(ge=0)
    received_at: Timestamp
    observed_at: Timestamp
    source_reasons: tuple[str, ...]
    evidence: FlowInputEvidenceRef
    parent_document: SpotFlowParent | None
    generated_empty: EmptyBoundarySegment | None

    @field_validator("start", "end")
    @classmethod
    def aware_interval(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_interval_and_quantities(self) -> Self:
        if self.start >= self.end:
            raise ValueError("Spot flow requires a positive actual interval")
        if (
            self.start_clock.value is None
            or self.end_clock.value is None
            or parse_iso_time(self.start_clock.value) != self.start
            or parse_iso_time(self.end_clock.value) != self.end
        ):
            raise ValueError("Spot endpoint metadata must bind its actual source interval")
        if self.kind == "FULL_MINUTE" and (
            self.start.second or self.start.microsecond or self.end != self.start + timedelta(minutes=1)
        ):
            raise ValueError("A full-minute spot row requires its aligned UTC minute")
        if self.parent_document is not None and (
            self.parent_document.instrument != self.instrument
            or not self.parent_document.start <= self.start < self.end <= self.parent_document.end
            or self.evidence.parent_index != self.parent_document.evidence.row_index
            or self.evidence.collection != self.parent_document.evidence.collection
        ):
            raise ValueError("Embedded flow must bind its actual parent interval and scope")
        proof = self.generated_empty
        if proof is not None and (
            self.kind != "BOUNDARY"
            or proof.instrument_id != self.instrument.instrument_id
            or proof.start != self.start
            or proof.end != self.end
            or proof.buy_qty != self.buy_qty
            or proof.sell_qty != self.sell_qty
            or proof.sum_sq_qty != self.sum_sq_qty
            or proof.n_buy != self.n_buy
            or proof.n_sell != self.n_sell
            or self.observed_at.precision != "COLLECTION_ONLY"
            or self.observed_at.value is None
            or proof.observed_at != parse_iso_time(self.observed_at.value)
        ):
            raise ValueError("Generated empty visibility must bind its actual writer projection")
        for count, quantity in ((self.n_buy, self.buy_qty), (self.n_sell, self.sell_qty)):
            if count is not None and quantity is not None and (count == 0) != (quantity == 0):
                raise ValueError("Captured side count contradicts its native quantity")
        if self.buy_qty is not None and self.sell_qty is not None and self.sum_sq_qty is not None:
            no_flow = self.buy_qty == 0 and self.sell_qty == 0
            if no_flow != (self.sum_sq_qty == 0):
                raise ValueError("Captured squared quantity contradicts observed native flow")
        return self

    def visible_at(self, assembled_at: Timestamp) -> bool | None:
        if assembled_at.value is None:
            return None
        assembly = parse_iso_time(assembled_at.value)
        if self.end > assembly:
            return False
        if self.observed_at.value is not None and parse_iso_time(self.observed_at.value) > assembly:
            return False
        parent = self.parent_document
        if parent is not None:
            if parent.received_at.value is not None and parse_iso_time(parent.received_at.value) > assembly:
                return False
            if parent.observed_at.value is not None and parse_iso_time(parent.observed_at.value) > assembly:
                return False
        if self.received_at.value is not None:
            return parse_iso_time(self.received_at.value) <= assembly
        if self.generated_empty is not None:
            return True
        return None

    def visibility_basis(
        self, assembled_at: Timestamp
    ) -> Literal["SOURCE_RECEIPT", "GENERATED_EMPTY_PIECE"] | None:
        if self.visible_at(assembled_at) is not True:
            return None
        if self.received_at.value is not None:
            return "SOURCE_RECEIPT"
        if self.generated_empty is not None:
            return "GENERATED_EMPTY_PIECE"
        raise ValueError("Visible flow requires its actual receipt or generated-empty witness")


class SpotTickerObservation(DomainModel):
    instrument: SpotInstrument
    observed_at: Timestamp
    trailing_day_native_volume: Decimal | None = Field(ge=0)
    native_volume_field: Literal["volume_native", "volume"]
    received_at: Timestamp

    def visible_at(self, assembled_at: Timestamp) -> bool | None:
        if assembled_at.value is None or self.received_at.value is None or self.observed_at.value is None:
            return None
        assembly = parse_iso_time(assembled_at.value)
        return (
            parse_iso_time(self.received_at.value) <= assembly
            and parse_iso_time(self.observed_at.value) <= assembly
        )


class SpotPolicyConfiguration(DomainModel):
    significant_flow_sigma: float = Field(gt=0)
    min_spot_venues: int = Field(ge=1)

    def calculation_policy(self) -> "SpotCalculationPolicy":
        return SpotCalculationPolicy(
            significant_flow_sigma=Decimal(str(self.significant_flow_sigma)),
            min_spot_venues=self.min_spot_venues,
        )


class SpotCalculationPolicy(DomainModel):
    significant_flow_sigma: Decimal = Field(gt=0)
    min_spot_venues: int = Field(ge=1)

    @classmethod
    def from_thresholds(cls, thresholds: Thresholds) -> Self:
        actual = Thresholds.model_validate(thresholds)
        return cls(
            significant_flow_sigma=Decimal(str(actual.significant_flow_sigma)),
            min_spot_venues=actual.min_spot_venues,
        )


class SpotCalculationInputs(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    start: datetime
    cutoff: datetime
    assembled_at: Timestamp
    instruments: tuple[SpotInstrument, ...]
    minutes: tuple[SpotFlowObservation, ...]
    boundaries: tuple[SpotFlowObservation, ...]
    tickers: tuple[SpotTickerObservation, ...]
    policy: SpotCalculationPolicy

    @field_validator("start", "cutoff")
    @classmethod
    def aware_interval(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_scope(self) -> Self:
        if self.start >= self.cutoff:
            raise ValueError("Spot calculation requires a positive requested interval")
        identities = {item.instrument_id: item for item in self.instruments}
        if len(identities) != len(self.instruments):
            raise ValueError("Spot inputs cannot repeat an instrument")
        if any(item.token_id != self.token_id for item in self.instruments):
            raise ValueError("Spot instruments must belong to the requested token")
        for kind, rows in (("FULL_MINUTE", self.minutes), ("BOUNDARY", self.boundaries)):
            for row in rows:
                if row.kind != kind or identities.get(row.instrument.instrument_id) != row.instrument:
                    raise ValueError("Spot flow must bind to its actual declared input scope")
        for ticker in self.tickers:
            if identities.get(ticker.instrument.instrument_id) != ticker.instrument:
                raise ValueError("Spot ticker must bind to its actual declared input scope")
        return self


class SpotWindowCalculationInputs(DomainModel):
    source: SpotCalculationInputs
    current: dict[ConditionWindow, ConditionInterval]
    previous: dict[ConditionWindow, ConditionInterval]
    event_anchor: Timestamp
    primary: Literal["EVENT", "W4H"]

    @model_validator(mode="after")
    def actual_window_plan(self) -> Self:
        fixed = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400}
        names = {*fixed, "EVENT"}
        if set(self.current) != names or set(self.previous) != names:
            raise ValueError("Spot graph requires the actual current and prior five windows")
        if len({row.cutoff for row in self.current.values()}) != 1 or any(
            row.cutoff != self.source.cutoff for row in self.current.values()
        ):
            raise ValueError("Spot graph current windows require one actual cutoff")
        for name, current in self.current.items():
            if current.window != name or self.previous[name].window != name:
                raise ValueError("Spot graph window key must bind its actual interval")
            duration = current.cutoff - current.start
            if name in fixed and duration != timedelta(seconds=fixed[name]):
                raise ValueError("Named spot window must preserve its declared duration")
            prior = self.previous[name]
            if prior.start < self.source.start:
                raise ValueError("Prior spot interval must remain inside its actual source read scope")
            if prior.cutoff != current.start or prior.cutoff - prior.start != duration:
                raise ValueError("Prior spot window must immediately precede the current interval")
        event = self.current["EVENT"]
        anchor = self.event_anchor.value
        if anchor is not None and parse_iso_time(anchor) != event.start:
            raise ValueError("EVENT spot window must preserve its actual anchor")
        if anchor is None and event.start != self.current["W4H"].start:
            raise ValueError("Without an event anchor EVENT uses the declared W4H analysis interval")
        primary = (
            "EVENT"
            if anchor is not None and timedelta(minutes=15) <= event.cutoff - event.start <= timedelta(days=1)
            else "W4H"
        )
        if self.primary != primary:
            raise ValueError("Primary spot window must follow the actual event-anchor policy")
        return self

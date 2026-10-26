"""Consumed spot inputs, independent of transport, storage and output serialization."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.configuration import Thresholds
from tank.time_precision import Timestamp
from tank.util import parse_iso_time


class SpotInstrument(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    market_type: Literal["SPOT"]


class FlowSourceDescriptor(DomainModel):
    format: Literal["SPOT_STORAGE_READ_V1"]
    collection: Literal["flow_bars", "flow_segments"]
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
        if self.parent_index is not None and self.collection != "flow_bars":
            raise ValueError("Only an embedded flow-bar piece inherits a parent flow projection")
        return self


class FlowInputEvidenceRef(DomainModel):
    collection: Literal["flow_bars", "flow_segments"]
    row_index: int = Field(ge=0)
    parent_index: int | None = Field(ge=0)
    source_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class SpotFlowObservation(DomainModel):
    instrument: SpotInstrument
    start: datetime
    end: datetime
    kind: Literal["FULL_MINUTE", "BOUNDARY"]
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

    @field_validator("start", "end")
    @classmethod
    def aware_interval(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def actual_interval_and_quantities(self) -> Self:
        if self.start >= self.end:
            raise ValueError("Spot flow requires a positive actual interval")
        if self.kind == "FULL_MINUTE" and (
            self.start.second or self.start.microsecond or self.end != self.start + timedelta(minutes=1)
        ):
            raise ValueError("A full-minute spot row requires its aligned UTC minute")
        for count, quantity in ((self.n_buy, self.buy_qty), (self.n_sell, self.sell_qty)):
            if count is not None and quantity is not None and (count == 0) != (quantity == 0):
                raise ValueError("Captured side count contradicts its native quantity")
        if self.buy_qty is not None and self.sell_qty is not None and self.sum_sq_qty is not None:
            no_flow = self.buy_qty == 0 and self.sell_qty == 0
            if no_flow != (self.sum_sq_qty == 0):
                raise ValueError("Captured squared quantity contradicts observed native flow")
        return self

    def visible_at(self, assembled_at: Timestamp) -> bool | None:
        if assembled_at.value is None or self.received_at.value is None:
            return None
        assembly = parse_iso_time(assembled_at.value)
        if self.end > assembly or parse_iso_time(self.received_at.value) > assembly:
            return False
        return self.observed_at.value is None or parse_iso_time(self.observed_at.value) <= assembly


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

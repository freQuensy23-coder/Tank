"""Captured funding facts before selection, normalization and request visibility."""

from collections.abc import Mapping
from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.contracts import DomainModel
from tank.contracts.derivative_values import ReportedDerivativeNumber
from tank.contracts.json import decode_unique_json
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import parse_iso_time, sha256

type FundingKind = Literal["SETTLED", "CURRENT_ESTIMATE"]


class FundingInputEvidenceRef(DomainModel):
    row_index: int = Field(ge=0)
    source_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FundingObservationClock(DomainModel):
    field: Literal["settlement_time", "funding_time", "as_of"] | None
    timestamp: Timestamp

    @model_validator(mode="after")
    def no_uncaptured_field_claim(self) -> Self:
        if self.field is None and self.timestamp.value is not None:
            raise ValueError("An observed funding clock requires its actual captured field")
        return self


def funding_observation_clock(row: Mapping[str, JsonValue], kind: FundingKind) -> FundingObservationClock:
    if kind == "CURRENT_ESTIMATE":
        return FundingObservationClock(
            field="as_of" if "as_of" in row else None,
            timestamp=propagate_timestamp(row, "as_of"),
        )
    settled = propagate_timestamp(row, "settlement_time")
    funding = propagate_timestamp(row, "funding_time")
    if (
        settled.value is not None
        and funding.value is not None
        and parse_iso_time(settled.value) != parse_iso_time(funding.value)
    ):
        raise ValueError("Captured settled funding clock aliases contradict each other")
    if settled.value is not None:
        return FundingObservationClock(field="settlement_time", timestamp=settled)
    if funding.value is not None or "funding_time" in row:
        return FundingObservationClock(field="funding_time", timestamp=funding)
    return FundingObservationClock(
        field="settlement_time" if "settlement_time" in row else None, timestamp=settled
    )


class FundingInputObservation(DomainModel):
    instrument: DerivativeInstrument
    kind: FundingKind
    clock: FundingObservationClock
    received_at: Timestamp
    rate: ReportedDerivativeNumber
    interval_h: ReportedDerivativeNumber
    recorded_eight_hour_rate: ReportedDerivativeNumber
    evidence: FundingInputEvidenceRef

    @model_validator(mode="after")
    def actual_kind_clock(self) -> Self:
        if self.clock.field is not None and (self.kind == "CURRENT_ESTIMATE") != (
            self.clock.field == "as_of"
        ):
            raise ValueError("Funding clock field must retain its actual settlement or estimate meaning")
        return self

    @classmethod
    def from_record(
        cls,
        row: Mapping[str, JsonValue],
        *,
        instrument: DerivativeInstrument,
        reference: FundingInputEvidenceRef,
    ) -> Self:
        for field, expected in (
            ("instrument_id", instrument.instrument_id),
            ("token_id", instrument.token_id),
            ("venue", instrument.venue),
            ("market_type", instrument.market_type),
        ):
            if row.get(field) is not None and row[field] != expected:
                raise ValueError("Funding source contradicts its actual scope: " + field)
        raw_kind = row["kind"]
        kind: FundingKind
        if raw_kind == "SETTLED":
            kind = "SETTLED"
        elif raw_kind == "CURRENT_ESTIMATE":
            kind = "CURRENT_ESTIMATE"
        else:
            raise ValueError("Funding source requires its actual settled or current-estimate kind")
        return cls(
            instrument=instrument,
            kind=kind,
            clock=funding_observation_clock(row, kind),
            received_at=propagate_timestamp(row, "received_ts"),
            rate=ReportedDerivativeNumber.from_value(row.get("rate")),
            interval_h=ReportedDerivativeNumber.from_value(row.get("interval_h")),
            recorded_eight_hour_rate=ReportedDerivativeNumber.from_value(row.get("rate_8h_eq")),
            evidence=reference,
        )

    def visible_at(self, assembled_at: Timestamp) -> bool | None:
        if self.received_at.value is None or assembled_at.value is None:
            return None
        return parse_iso_time(self.received_at.value) <= parse_iso_time(assembled_at.value)


class FundingSourceDescriptor(DomainModel):
    format: Literal["FUNDING_INPUT_READ_V1"]
    collection: Literal["mkt.funding_obs"]
    row_index: int = Field(ge=0)
    original_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_basis: Literal["INSTRUMENT_READ", "SOURCE_ROW"]
    instrument: DerivativeInstrument


class FundingSourceProjection(DomainModel):
    original: bytes
    scope_original: bytes
    descriptor: FundingSourceDescriptor
    reference: FundingInputEvidenceRef

    @model_validator(mode="after")
    def actual_identity(self) -> Self:
        if (
            sha256(self.original) != self.descriptor.original_projection_sha256
            or sha256(self.scope_original) != self.descriptor.scope_projection_sha256
            or self.reference.row_index != self.descriptor.row_index
            or self.reference.source_projection_sha256 != self.descriptor.original_projection_sha256
            or self.reference.source_evidence_sha256 != sha256(self.descriptor)
        ):
            raise ValueError("Funding evidence must bind its actual canonical storage-read records")
        if self.descriptor.scope_basis == "SOURCE_ROW" and self.scope_original != self.original:
            raise ValueError("Self-scoped funding evidence cannot borrow a different declaration")
        return self


class FundingReadSnapshot(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instruments: tuple[DerivativeInstrument, ...]
    observations: tuple[FundingInputObservation, ...]
    source_projections: tuple[FundingSourceProjection, ...]

    @model_validator(mode="after")
    def exact_consumed_sources(self) -> Self:
        instruments = {point.instrument_id: point for point in self.instruments}
        if len(instruments) != len(self.instruments) or any(
            point.token_id != self.token_id for point in self.instruments
        ):
            raise ValueError("Funding reads require distinct instruments for the actual subject")
        sources = {source.reference: source for source in self.source_projections}
        references = tuple(point.evidence for point in self.observations)
        if (
            len(sources) != len(self.source_projections)
            or len(set(references)) != len(references)
            or set(references) != set(sources)
        ):
            raise ValueError("Funding observations must consume each actual source projection once")
        records: dict[bytes, dict[str, JsonValue]] = {}

        def source_record(payload: bytes) -> dict[str, JsonValue]:
            if payload not in records:
                parsed = decode_unique_json(payload)
                if not isinstance(parsed, dict):
                    raise ValueError("Funding evidence requires its actual source object")
                records[payload] = parsed
            return records[payload]

        for point in self.observations:
            source = sources[point.evidence]
            scope = DerivativeInstrument.from_record(source_record(source.scope_original))
            if (
                scope != point.instrument
                or scope != source.descriptor.instrument
                or instruments.get(scope.instrument_id) != scope
            ):
                raise ValueError("Funding observation must retain its actual source and scope")
            expected = FundingInputObservation.from_record(
                source_record(source.original), instrument=scope, reference=source.reference
            )
            if expected != point:
                raise ValueError("Funding consumed facts disagree with their original captured record")
        return self

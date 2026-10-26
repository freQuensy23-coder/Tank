"""Reported derivative points and source identity before endpoint qualification."""

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.configuration import Thresholds
from tank.contracts.json import decode_unique_json
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import parse_iso_time, sha256


def _oi_text(row: Mapping[str, JsonValue], field: str) -> str | None:
    value = row.get(field)
    if value is not None and not isinstance(value, str):
        raise ValueError("OI text metadata cannot be coerced: " + field)
    return value


def read_oi_quote_asset(row: Mapping[str, JsonValue]) -> str | None:
    quote = _oi_text(row, "quote_asset")
    if quote is not None and not quote.strip():
        raise ValueError("OI quote identity must be captured nonblank text or genuinely absent")
    return quote


class DerivativeInstrument(DomainModel):
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    token_id: str = Field(min_length=1, pattern=r"\S")
    venue: str = Field(min_length=1, pattern=r"\S")
    market_type: Literal["PERP_LINEAR"]

    @classmethod
    def from_record(cls, row: Mapping[str, JsonValue]) -> Self:
        if row.get("market_type") != "PERP_LINEAR":
            raise ValueError("OI read scope must declare its actual derivative market type")
        return cls.model_validate({name: row.get(name) for name in cls.model_fields})


class ReportedDerivativeNumber(DomainModel):
    status: Literal["OBSERVED", "NOT_CAPTURED", "NOT_ESTIMABLE"]
    value: Decimal | None

    @model_validator(mode="after")
    def captured_value(self) -> Self:
        if (self.status == "OBSERVED") != (self.value is not None):
            raise ValueError("A reported derivative number must preserve its actual availability")
        return self

    @classmethod
    def from_value(cls, value: JsonValue) -> Self:
        if value is None:
            return cls(status="NOT_CAPTURED", value=None)
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            return cls(status="NOT_ESTIMABLE", value=None)
        try:
            parsed = Decimal(str(value))
        except InvalidOperation:
            return cls(status="NOT_ESTIMABLE", value=None)
        return cls(
            status="OBSERVED" if parsed.is_finite() else "NOT_ESTIMABLE",
            value=parsed if parsed.is_finite() else None,
        )


class OISourceDescriptor(DomainModel):
    format: Literal["OI_INPUT_READ_V2"]
    collection: Literal["mkt.oi_obs"]
    row_index: int = Field(ge=0)
    original_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_basis: Literal["INSTRUMENT_READ", "SOURCE_ROW"]
    instrument: DerivativeInstrument


class OIInputEvidenceRef(DomainModel):
    row_index: int = Field(ge=0)
    source_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OIObservation(DomainModel):
    instrument: DerivativeInstrument
    quote_asset: str | None = Field(min_length=1, pattern=r"\S")
    observed_at: Timestamp
    received_at: Timestamp
    native_quantity: ReportedDerivativeNumber
    reported_usd: ReportedDerivativeNumber
    usd_derivation_label: str | None
    mark_price: ReportedDerivativeNumber
    mark_observed_at: Timestamp
    index_price: ReportedDerivativeNumber
    metric_key: str | None = Field(min_length=1, pattern=r"\S")
    raw_unit: str | None
    sidedness: str | None
    synchronized_eligible: bool | None
    edge_declaration: Literal["ABSENT", "DECLARED"]
    evidence: OIInputEvidenceRef

    @classmethod
    def from_record(
        cls,
        row: Mapping[str, JsonValue],
        *,
        instrument: DerivativeInstrument,
        reference: OIInputEvidenceRef,
        scope_quote_asset: str | None,
    ) -> Self:
        for field, expected in (
            ("instrument_id", instrument.instrument_id),
            ("token_id", instrument.token_id),
            ("venue", instrument.venue),
            ("market_type", instrument.market_type),
        ):
            if row.get(field) is not None and row[field] != expected:
                raise ValueError("OI source scope contradicts its qualified instrument: " + field)
        quote = read_oi_quote_asset(row)
        if quote is not None and scope_quote_asset is not None and quote != scope_quote_asset:
            raise ValueError("OI source scope contradicts its qualified instrument: quote_asset")
        eligible = row.get("synchronized_eligible")
        if eligible is not None and not isinstance(eligible, bool):
            raise ValueError("OI synchronization eligibility must be an actual captured boolean")
        stamp = row.get("ts")
        declared_edge = "ts_bucket_anchor" in row or isinstance(stamp, dict) and "bucket_anchor" in stamp
        return cls(
            instrument=instrument,
            quote_asset=scope_quote_asset if quote is None else quote,
            observed_at=propagate_timestamp(
                row, "ts", precision_aliases=("timestamp_precision", "precision")
            ),
            received_at=propagate_timestamp(row, "received_ts"),
            native_quantity=ReportedDerivativeNumber.from_value(row.get("oi_native")),
            reported_usd=ReportedDerivativeNumber.from_value(row.get("oi_usd")),
            usd_derivation_label=_oi_text(row, "oi_usd_source"),
            mark_price=ReportedDerivativeNumber.from_value(row.get("mark_price")),
            mark_observed_at=propagate_timestamp(row, "mark_as_of"),
            index_price=ReportedDerivativeNumber.from_value(row.get("index_price")),
            metric_key=_oi_text(row, "metric_key"),
            raw_unit=_oi_text(row, "raw_unit"),
            sidedness=_oi_text(row, "sidedness"),
            synchronized_eligible=eligible,
            edge_declaration="DECLARED" if declared_edge else "ABSENT",
            evidence=reference,
        )

    def reported_time(self) -> datetime | None:
        return None if self.observed_at.value is None else parse_iso_time(self.observed_at.value)

    def explicitly_unresolved_edge(self) -> bool:
        return self.edge_declaration == "DECLARED" and self.observed_at.bucket_anchor == "UNSPECIFIED"

    def visible_at(self, assembled_at: Timestamp) -> bool | None:
        if assembled_at.value is None or self.received_at.value is None:
            return None
        return parse_iso_time(self.received_at.value) <= parse_iso_time(assembled_at.value)


class OISourceProjection(DomainModel):
    original: bytes
    scope_original: bytes
    descriptor: OISourceDescriptor
    reference: OIInputEvidenceRef

    @model_validator(mode="after")
    def actual_projection(self) -> Self:
        if (
            sha256(self.original) != self.descriptor.original_projection_sha256
            or sha256(self.scope_original) != self.descriptor.scope_projection_sha256
            or self.reference.row_index != self.descriptor.row_index
            or self.reference.source_projection_sha256 != self.descriptor.original_projection_sha256
            or self.reference.source_evidence_sha256 != sha256(self.descriptor)
        ):
            raise ValueError("OI source reference must bind its actual canonical read projections")
        return self


class OIReadSnapshot(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instruments: tuple[DerivativeInstrument, ...]
    observations: tuple[OIObservation, ...]
    source_projections: tuple[OISourceProjection, ...]

    @model_validator(mode="after")
    def actual_subject(self) -> Self:
        instruments = {row.instrument_id: row for row in self.instruments}
        if len(instruments) != len(self.instruments) or any(
            row.token_id != self.token_id for row in self.instruments
        ):
            raise ValueError("OI read instruments require one distinct actual subject scope")
        records: dict[str, dict[str, JsonValue]] = {}
        scopes: dict[str, DerivativeInstrument] = {}
        scope_quotes: dict[str, str | None] = {}

        def read_record(payload: bytes, digest: str) -> dict[str, JsonValue]:
            if digest not in records:
                decoded = decode_unique_json(payload)
                if not isinstance(decoded, dict):
                    raise ValueError("OI source evidence requires its actual canonical read object")
                records[digest] = decoded
            return records[digest]

        references = {row.reference: row for row in self.source_projections}
        if len(references) != len(self.source_projections):
            raise ValueError("OI source evidence cannot overwrite another projection")
        for point in self.observations:
            source = references.get(point.evidence)
            if (
                instruments.get(point.instrument.instrument_id) != point.instrument
                or source is None
                or source.descriptor.instrument != point.instrument
            ):
                raise ValueError("OI observations must bind their actual declared scope and source")
            descriptor = source.descriptor
            if descriptor.scope_basis == "SOURCE_ROW" and source.scope_original != source.original:
                raise ValueError("OI source-row scope cannot borrow a different record's declaration")
            if descriptor.scope_projection_sha256 not in scopes:
                scope_record = read_record(source.scope_original, descriptor.scope_projection_sha256)
                scopes[descriptor.scope_projection_sha256] = DerivativeInstrument.from_record(scope_record)
                scope_quotes[descriptor.scope_projection_sha256] = read_oi_quote_asset(scope_record)
            actual_instrument = scopes[descriptor.scope_projection_sha256]
            if actual_instrument != descriptor.instrument:
                raise ValueError("OI declared instrument disagrees with its actual scope read")
            actual_point = OIObservation.from_record(
                read_record(source.original, descriptor.original_projection_sha256),
                instrument=actual_instrument,
                reference=source.reference,
                scope_quote_asset=scope_quotes[descriptor.scope_projection_sha256],
            )
            if actual_point.model_dump(mode="json") != point.model_dump(mode="json"):
                raise ValueError("OI consumed observation disagrees with its actual canonical source read")
        observed_refs = tuple(point.evidence for point in self.observations)
        if len(set(observed_refs)) != len(observed_refs) or set(observed_refs) != set(references):
            raise ValueError("OI observations must consume each actual source projection exactly once")
        if len(self.observations) != len(self.source_projections):
            raise ValueError("OI reads must preserve every consumed source observation")
        return self


class OISelectionPolicy(DomainModel):
    tolerance_s: int = Field(ge=0)
    minimum_history_days: int = Field(gt=0)
    lookback_days: int = Field(gt=0)
    quantiles: tuple[float, float]

    @model_validator(mode="after")
    def actual_quantiles(self) -> Self:
        if not 0 <= self.quantiles[0] < self.quantiles[1] <= 1:
            raise ValueError("OI baseline quantiles require actual ordered probability bounds")
        return self

    @classmethod
    def from_thresholds(cls, thresholds: Thresholds) -> Self:
        actual = Thresholds.model_validate(thresholds)
        if len(actual.normal_range_quantiles) != 2:
            raise ValueError("OI baseline requires exactly two configured quantiles")
        return cls(
            tolerance_s=actual.oi_sync_tolerance_s,
            minimum_history_days=actual.history_min_days,
            lookback_days=actual.base_retention_days,
            quantiles=(actual.normal_range_quantiles[0], actual.normal_range_quantiles[1]),
        )


class OISynchronizationRule(DomainModel):
    instrument_id: str | None = Field(min_length=1, pattern=r"\S")
    venue: str | None = Field(min_length=1, pattern=r"\S")
    market_type: Literal["PERP_LINEAR"] | None
    metric_key: str = Field(min_length=1, pattern=r"\S")
    sidedness: Literal["N_A", "SINGLE_SIDED", "UNKNOWN"] | None
    aggregation_class: str | None = Field(min_length=1, pattern=r"\S")

    def applies_to(self, instrument: DerivativeInstrument, metric_key: str | None) -> bool:
        if metric_key != self.metric_key:
            return False
        if self.instrument_id is not None:
            return self.instrument_id == instrument.instrument_id
        return self.venue == instrument.venue and self.market_type == instrument.market_type


class OIRuleReadScope(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    instruments: tuple[DerivativeInstrument, ...]
    metric_keys: tuple[str, ...]

    @model_validator(mode="after")
    def actual_subject(self) -> Self:
        if any(row.token_id != self.token_id for row in self.instruments):
            raise ValueError("OI rule scope cannot borrow a different subject's instruments")
        if len({row.instrument_id for row in self.instruments}) != len(self.instruments):
            raise ValueError("OI rule scope cannot contain duplicate instrument identities")
        if any(not key.strip() for key in self.metric_keys) or self.metric_keys != tuple(
            sorted(set(self.metric_keys))
        ):
            raise ValueError("OI rule scope requires distinct canonical nonblank captured metric keys")
        return self

    @classmethod
    def from_snapshot(cls, source: OIReadSnapshot) -> Self:
        return cls(
            token_id=source.token_id,
            instruments=source.instruments,
            metric_keys=tuple(
                sorted({point.metric_key for point in source.observations if point.metric_key is not None})
            ),
        )


class OIRuleSourceDescriptor(DomainModel):
    format: Literal["OI_SYNC_RULE_READ_V1"]
    row_index: int = Field(ge=0)
    original_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    qualified_scope_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rule_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OIRuleEvidenceRef(DomainModel):
    row_index: int = Field(ge=0)
    projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OIRuleSourceProjection(DomainModel):
    """Immutable canonical semantic-input records, never provider-byte provenance."""

    original: bytes
    rule: OISynchronizationRule
    descriptor: OIRuleSourceDescriptor
    reference: OIRuleEvidenceRef

    @model_validator(mode="after")
    def actual_projection(self) -> Self:
        original = decode_unique_json(self.original)
        if not isinstance(original, dict):
            raise ValueError("OI rule evidence requires its actual canonical input object")
        declared = OISynchronizationRule.model_validate(
            {name: original.get(name) for name in OISynchronizationRule.model_fields}
        )
        if declared != self.rule:
            raise ValueError("OI rule evidence disagrees with the consumed semantic projection")
        if (
            sha256(self.original) != self.descriptor.original_projection_sha256
            or sha256(self.rule) != self.descriptor.rule_projection_sha256
            or self.reference.projection_sha256 != self.descriptor.rule_projection_sha256
            or self.reference.row_index != self.descriptor.row_index
            or self.reference.source_evidence_sha256 != sha256(self.descriptor)
        ):
            raise ValueError("OI rule evidence must bind its actual read and qualified-scope descriptor")
        return self


class CapturedOISynchronizationRules(DomainModel):
    status: Literal["CAPTURED"]
    scope: OIRuleReadScope
    entries: tuple[OIRuleSourceProjection, ...]

    @model_validator(mode="after")
    def actual_scoped_rules(self) -> Self:
        scope_hash = sha256(self.scope)
        instruments = {value.instrument_id: value for value in self.scope.instruments}
        references = [entry.reference for entry in self.entries]
        if len(references) != len(set(references)):
            raise ValueError("OI rule evidence cannot overwrite another canonical input entry")
        for entry in self.entries:
            rule = entry.rule
            if (
                entry.descriptor.qualified_scope_sha256 != scope_hash
                or rule.metric_key not in self.scope.metric_keys
            ):
                raise ValueError("OI rule evidence must bind the actual selected subject and metric scope")
            if rule.instrument_id is not None:
                instrument = instruments.get(rule.instrument_id)
                if instrument is None or rule.venue is not None and rule.venue != instrument.venue:
                    raise ValueError("OI rule declaration contradicts its actual selected instrument scope")
        return self


class UncapturedOISynchronizationRules(DomainModel):
    status: Literal["NOT_CAPTURED"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    reason: Literal["OI_SEMANTICS_NOT_CAPTURED"]

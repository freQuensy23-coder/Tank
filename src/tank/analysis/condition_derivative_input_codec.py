"""Qualified OI storage reads without timestamp, unit or valuation invention."""

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import JsonValue

from tank.analysis.condition_derivative_inputs import (
    DerivativeInstrument,
    OIInputEvidenceRef,
    OIObservation,
    OIReadSnapshot,
    OISourceDescriptor,
    OISourceProjection,
    ReportedDerivativeNumber,
)
from tank.contracts.json import decode_json
from tank.time_precision import propagate_timestamp
from tank.util import JsonInput, canonical_json, sha256


def _wire(row: Mapping[str, JsonInput]) -> tuple[dict[str, JsonValue], bytes]:
    payload = canonical_json(row).encode()
    parsed = decode_json(payload)
    if not isinstance(parsed, dict):
        raise ValueError("An OI storage read must be an object")
    return parsed, payload


def _required_text(row: Mapping[str, JsonValue], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("OI read requires captured scope field: " + field)
    return value


def _optional_text(row: Mapping[str, JsonValue], field: str) -> str | None:
    value = row.get(field)
    if value is not None and not isinstance(value, str):
        raise ValueError("OI text metadata cannot be coerced: " + field)
    return value


def _reported(value: JsonValue) -> ReportedDerivativeNumber:
    if value is None:
        return ReportedDerivativeNumber(status="NOT_CAPTURED", value=None)
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return ReportedDerivativeNumber(status="NOT_ESTIMABLE", value=None)
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return ReportedDerivativeNumber(status="NOT_ESTIMABLE", value=None)
    return ReportedDerivativeNumber(
        status="OBSERVED" if parsed.is_finite() else "NOT_ESTIMABLE",
        value=parsed if parsed.is_finite() else None,
    )


def _instrument(row: Mapping[str, JsonValue]) -> DerivativeInstrument:
    if row.get("market_type") != "PERP_LINEAR":
        raise ValueError("OI read scope must declare its actual derivative market type")
    return DerivativeInstrument(
        instrument_id=_required_text(row, "instrument_id"),
        token_id=_required_text(row, "token_id"),
        venue=_required_text(row, "venue"),
        market_type="PERP_LINEAR",
        quote_asset=_optional_text(row, "quote_asset"),
    )


def _point(
    row: Mapping[str, JsonValue], instrument: DerivativeInstrument, reference: OIInputEvidenceRef
) -> OIObservation:
    for field, expected in (
        ("instrument_id", instrument.instrument_id),
        ("token_id", instrument.token_id),
        ("venue", instrument.venue),
        ("market_type", instrument.market_type),
    ):
        if field in row and row[field] != expected:
            raise ValueError("OI source scope contradicts its qualified instrument: " + field)
    eligible = row.get("synchronized_eligible")
    if eligible is not None and not isinstance(eligible, bool):
        raise ValueError("OI synchronization eligibility must be an actual captured boolean")
    stamp = row.get("ts")
    declared_edge = "ts_bucket_anchor" in row or isinstance(stamp, dict) and "bucket_anchor" in stamp
    return OIObservation(
        instrument=instrument,
        observed_at=propagate_timestamp(row, "ts", precision_aliases=("timestamp_precision", "precision")),
        received_at=propagate_timestamp(row, "received_ts"),
        native_quantity=_reported(row.get("oi_native")),
        reported_usd=_reported(row.get("oi_usd")),
        usd_derivation_label=_optional_text(row, "oi_usd_source"),
        mark_price=_reported(row.get("mark_price")),
        mark_observed_at=propagate_timestamp(row, "mark_as_of"),
        index_price=_reported(row.get("index_price")),
        metric_key=_optional_text(row, "metric_key"),
        raw_unit=_optional_text(row, "raw_unit"),
        sidedness=_optional_text(row, "sidedness"),
        synchronized_eligible=eligible,
        edge_declaration="DECLARED" if declared_edge else "ABSENT",
        evidence=reference,
    )


def decode_oi_inputs(
    *,
    token_id: str,
    instruments: Sequence[Mapping[str, JsonInput]],
    observations: Sequence[Mapping[str, JsonInput]],
) -> OIReadSnapshot:
    known: dict[str, DerivativeInstrument] = {}
    scopes: dict[str, bytes] = {}
    registry_ids: set[str] = set()
    for original in instruments:
        if original.get("token_id") != token_id or original.get("market_type") != "PERP_LINEAR":
            continue
        row, payload = _wire(original)
        instrument = _instrument(row)
        if instrument.instrument_id in known:
            raise ValueError("OI instrument read cannot contain ambiguous duplicate identities")
        known[instrument.instrument_id] = instrument
        scopes[instrument.instrument_id] = payload
        registry_ids.add(instrument.instrument_id)
    points: list[OIObservation] = []
    sources: list[OISourceProjection] = []
    for index, original in enumerate(observations):
        if original.get("token_id") is not None and original.get("token_id") != token_id:
            foreign_iid = original.get("instrument_id")
            if isinstance(foreign_iid, str) and foreign_iid in registry_ids:
                raise ValueError("OI source token contradicts its declared instrument scope")
            continue
        row, payload = _wire(original)
        iid = _required_text(row, "instrument_id")
        basis: Literal["INSTRUMENT_READ", "SOURCE_ROW"]
        if iid in registry_ids:
            selected = known[iid]
            scope_payload = scopes[iid]
            basis = "INSTRUMENT_READ"
        else:
            selected = _instrument(row)
            if selected.token_id != token_id:
                raise ValueError("OI source must belong to its actual requested subject")
            if iid in known and known[iid] != selected:
                raise ValueError("OI source rows cannot contradict one instrument's actual scope")
            known[iid] = selected
            scope_payload = payload
            basis = "SOURCE_ROW"
        descriptor = OISourceDescriptor(
            format="OI_INPUT_READ_V1",
            collection="mkt.oi",
            row_index=index,
            original_projection_sha256=sha256(payload),
            scope_projection_sha256=sha256(scope_payload),
            scope_basis=basis,
            instrument=selected,
        )
        reference = OIInputEvidenceRef(
            row_index=index,
            source_projection_sha256=descriptor.original_projection_sha256,
            source_evidence_sha256=sha256(descriptor),
        )
        points.append(_point(row, selected, reference))
        sources.append(
            OISourceProjection(
                original=payload,
                scope_original=scope_payload,
                descriptor=descriptor,
                reference=reference,
            )
        )
    return OIReadSnapshot(
        token_id=token_id,
        instruments=tuple(known.values()),
        observations=tuple(points),
        source_projections=tuple(sources),
    )


def resolve_oi_observation(
    snapshot: OIReadSnapshot, reference: OIInputEvidenceRef, *, expected: OIObservation
) -> Mapping[str, JsonValue]:
    snapshot = OIReadSnapshot.model_validate(snapshot)
    for source in snapshot.source_projections:
        if source.reference != reference:
            continue
        row = decode_json(source.original)
        if not isinstance(row, dict):
            raise ValueError("OI source evidence must retain its original object")
        if _point(row, source.descriptor.instrument, reference) != expected:
            raise ValueError("OI source observation disagrees with the actual consumed projection")
        return row
    raise ValueError("OI source evidence reference is not captured")

"""Qualified OI storage reads without timestamp, unit or valuation invention."""

from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import JsonValue

from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
    DerivativeInstrument,
    OIInputEvidenceRef,
    OIObservation,
    OIReadSnapshot,
    OIRuleEvidenceRef,
    OIRuleReadScope,
    OIRuleSourceDescriptor,
    OIRuleSourceProjection,
    OISourceDescriptor,
    OISourceProjection,
    OISynchronizationRule,
    read_oi_quote_asset,
)
from tank.contracts.json import decode_json, decode_unique_json
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


def decode_oi_inputs(
    *,
    token_id: str,
    instruments: Sequence[Mapping[str, JsonInput]],
    observations: Sequence[Mapping[str, JsonInput]],
) -> OIReadSnapshot:
    known: dict[str, DerivativeInstrument] = {}
    scopes: dict[str, bytes] = {}
    registry_ids: set[str] = set()
    registry_quotes: dict[str, str | None] = {}
    for original in instruments:
        if original.get("token_id") != token_id or original.get("market_type") != "PERP_LINEAR":
            continue
        row, payload = _wire(original)
        instrument = DerivativeInstrument.from_record(row)
        if instrument.instrument_id in known:
            raise ValueError("OI instrument read cannot contain ambiguous duplicate identities")
        known[instrument.instrument_id] = instrument
        scopes[instrument.instrument_id] = payload
        registry_ids.add(instrument.instrument_id)
        registry_quotes[instrument.instrument_id] = read_oi_quote_asset(row)
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
            scope_quote = registry_quotes[iid]
        else:
            selected = DerivativeInstrument.from_record(row)
            if selected.token_id != token_id:
                raise ValueError("OI source must belong to its actual requested subject")
            if iid in known and known[iid] != selected:
                raise ValueError("OI source rows cannot contradict one instrument's actual scope")
            known[iid] = selected
            scope_payload = payload
            basis = "SOURCE_ROW"
            scope_quote = read_oi_quote_asset(row)
        descriptor = OISourceDescriptor(
            format="OI_INPUT_READ_V2",
            collection="mkt.oi_obs",
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
        points.append(
            OIObservation.from_record(
                row, instrument=selected, reference=reference, scope_quote_asset=scope_quote
            )
        )
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
        row = decode_unique_json(source.original)
        if not isinstance(row, dict):
            raise ValueError("OI source evidence must retain its original object")
        scope = decode_unique_json(source.scope_original)
        if not isinstance(scope, dict):
            raise ValueError("OI scope evidence must retain its actual source object")
        projected = OIObservation.from_record(
            row,
            instrument=source.descriptor.instrument,
            reference=reference,
            scope_quote_asset=read_oi_quote_asset(scope),
        )
        if projected.model_dump(mode="json") != expected.model_dump(mode="json"):
            raise ValueError("OI source observation disagrees with the actual consumed projection")
        return row
    raise ValueError("OI source evidence reference is not captured")


def decode_oi_synchronization_rules(
    *, scope: OIRuleReadScope, records: Sequence[Mapping[str, JsonInput]]
) -> CapturedOISynchronizationRules:
    scope = OIRuleReadScope.model_validate(scope)
    entries: list[OIRuleSourceProjection] = []
    selected_ids = {value.instrument_id for value in scope.instruments}
    for index, original in enumerate(records):
        metric_key = original.get("metric_key")
        if not isinstance(metric_key, str) or metric_key not in scope.metric_keys:
            continue
        instrument_id = original.get("instrument_id")
        if isinstance(instrument_id, str) and instrument_id not in selected_ids:
            continue
        row, payload = _wire(original)
        rule = OISynchronizationRule.model_validate(
            {name: row.get(name) for name in OISynchronizationRule.model_fields}
        )
        descriptor = OIRuleSourceDescriptor(
            format="OI_SYNC_RULE_READ_V1",
            row_index=index,
            original_projection_sha256=sha256(payload),
            qualified_scope_sha256=sha256(scope),
            rule_projection_sha256=sha256(rule),
        )
        reference = OIRuleEvidenceRef(
            row_index=index,
            projection_sha256=descriptor.rule_projection_sha256,
            source_evidence_sha256=sha256(descriptor),
        )
        entries.append(
            OIRuleSourceProjection(original=payload, rule=rule, descriptor=descriptor, reference=reference)
        )
    return CapturedOISynchronizationRules(status="CAPTURED", scope=scope, entries=tuple(entries))

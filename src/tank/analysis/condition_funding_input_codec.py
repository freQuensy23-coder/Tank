"""Required funding storage reads with exact original and inherited-scope identities."""

from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import JsonValue

from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.analysis.condition_funding_inputs import (
    FundingInputEvidenceRef,
    FundingInputObservation,
    FundingReadSnapshot,
    FundingSourceDescriptor,
    FundingSourceProjection,
)
from tank.contracts.json import decode_unique_json
from tank.util import JsonInput, canonical_json, sha256


def _canonical_read(row: Mapping[str, JsonInput]) -> tuple[dict[str, JsonValue], bytes]:
    payload = canonical_json(row).encode()
    parsed = decode_unique_json(payload)
    if not isinstance(parsed, dict):
        raise ValueError("Funding storage reads require actual record objects")
    return parsed, payload


def decode_funding_inputs(
    *,
    token_id: str,
    instruments: Sequence[Mapping[str, JsonInput]],
    records: Sequence[Mapping[str, JsonInput]],
) -> FundingReadSnapshot:
    """An explicit empty read differs from an omitted required collection at composition."""
    known: dict[str, DerivativeInstrument] = {}
    scopes: dict[str, bytes] = {}
    for original in instruments:
        row, payload = _canonical_read(original)
        if row.get("token_id") is not None and row["token_id"] != token_id:
            continue
        if row.get("market_type") == "SPOT":
            continue
        instrument = DerivativeInstrument.from_record(row)
        if instrument.token_id != token_id:
            raise ValueError("Funding instrument must declare the requested subject")
        if instrument.instrument_id in known:
            raise ValueError("Funding read scope cannot repeat an instrument declaration")
        known[instrument.instrument_id] = instrument
        scopes[instrument.instrument_id] = payload
    points: list[FundingInputObservation] = []
    sources: list[FundingSourceProjection] = []
    declared = dict(known)
    scope_basis: Literal["INSTRUMENT_READ", "SOURCE_ROW"]
    for index, original in enumerate(records):
        row, payload = _canonical_read(original)
        if row.get("token_id") is not None and row["token_id"] != token_id:
            continue
        instrument_id = row.get("instrument_id")
        if not isinstance(instrument_id, str) or not instrument_id.strip():
            raise ValueError("Funding source requires its actual instrument identity")
        selected = declared.get(instrument_id)
        if selected is None:
            instrument = DerivativeInstrument.from_record(row)
            if instrument.token_id != token_id:
                raise ValueError("Funding source must declare the requested subject")
            prior = known.get(instrument_id)
            if prior is not None and prior != instrument:
                raise ValueError("Self-scoped funding records disagree about their instrument")
            scope_payload = payload
            scope_basis = "SOURCE_ROW"
            known[instrument_id] = instrument
        else:
            instrument = selected
            scope_payload = scopes[instrument_id]
            scope_basis = "INSTRUMENT_READ"
        descriptor = FundingSourceDescriptor(
            format="FUNDING_INPUT_READ_V1",
            collection="mkt.funding_obs",
            row_index=index,
            original_projection_sha256=sha256(payload),
            scope_projection_sha256=sha256(scope_payload),
            scope_basis=scope_basis,
            instrument=instrument,
        )
        reference = FundingInputEvidenceRef(
            row_index=index,
            source_projection_sha256=descriptor.original_projection_sha256,
            source_evidence_sha256=sha256(descriptor),
        )
        points.append(FundingInputObservation.from_record(row, instrument=instrument, reference=reference))
        sources.append(
            FundingSourceProjection(
                original=payload,
                scope_original=scope_payload,
                descriptor=descriptor,
                reference=reference,
            )
        )
    return FundingReadSnapshot(
        token_id=token_id,
        instruments=tuple(known.values()),
        observations=tuple(points),
        source_projections=tuple(sources),
    )

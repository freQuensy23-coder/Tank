"""Storage-read projection for spot calculations and separately retained source evidence."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Literal

from pydantic import JsonValue

from tank.analysis.condition_graph_codec import flow_aggregate_projection_sha256
from tank.analysis.condition_graph_contracts import ConditionFlowAggregateProjection, FlowAggregateEvidenceRef
from tank.analysis.condition_inputs import (
    FlowInputEvidenceRef,
    FlowSourceDescriptor,
    SpotCalculationInputs,
    SpotCalculationPolicy,
    SpotFlowObservation,
    SpotInstrument,
    SpotTickerObservation,
)
from tank.analysis.spot import aggregate_projection
from tank.contracts.json import decode_json
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import JsonInput, canonical_json, parse_iso_time, sha256


@dataclass(frozen=True, slots=True)
class FlowSourceProjection:
    original: bytes
    effective: bytes
    scope_original: bytes
    descriptor: FlowSourceDescriptor


def source_descriptor_digest(descriptor: FlowSourceDescriptor) -> str:
    validated = FlowSourceDescriptor.model_validate(descriptor)
    return sha256(decode_json(validated.model_dump_json()))


class SpotSourceEvidence:
    """Own canonical storage-read projections; these are not original HTTP bodies."""

    __slots__ = ("_rows", "_by_digest")

    def __init__(self, rows: Mapping[FlowInputEvidenceRef, FlowSourceProjection]) -> None:
        checked: dict[FlowInputEvidenceRef, FlowSourceProjection] = {}
        digests: dict[str, FlowInputEvidenceRef] = {}
        slots: set[tuple[str, int, int | None]] = set()
        for reference, projection in rows.items():
            descriptor = projection.descriptor
            slot = (reference.collection, reference.row_index, reference.parent_index)
            if slot in slots:
                raise ValueError("Spot source locator cannot identify different captured projections")
            slots.add(slot)
            if (
                sha256(projection.original) != reference.source_projection_sha256
                or descriptor.original_projection_sha256 != reference.source_projection_sha256
                or sha256(projection.effective) != descriptor.effective_projection_sha256
                or sha256(projection.scope_original) != descriptor.scope_projection_sha256
                or source_descriptor_digest(descriptor) != reference.source_evidence_sha256
                or (descriptor.collection, descriptor.row_index, descriptor.parent_index) != slot
            ):
                raise ValueError("Spot source descriptor disagrees with its actual evidence")
            if reference.source_evidence_sha256 in digests:
                raise ValueError("Spot source descriptor cannot silently overwrite another witness")
            checked[reference] = projection
            digests[reference.source_evidence_sha256] = reference
        for projection in checked.values():
            parent = projection.descriptor.parent_source_evidence_sha256
            if parent is not None:
                if parent not in digests:
                    raise ValueError("Embedded spot evidence requires its actual parent projection")
                parent_reference = digests[parent]
                parent_descriptor = checked[parent_reference].descriptor
                if (
                    parent_reference.collection != "flow_bars"
                    or parent_reference.parent_index is not None
                    or parent_reference.row_index != projection.descriptor.parent_index
                    or parent_descriptor.scope != projection.descriptor.scope
                ):
                    raise ValueError("Embedded spot lineage must bind its actual parent row and scope")
        self._rows = MappingProxyType(checked)
        self._by_digest = MappingProxyType(digests)

    def original(self, reference: FlowInputEvidenceRef) -> dict[str, JsonValue]:
        payload = self._rows[reference].original
        if sha256(payload) != reference.source_projection_sha256:
            raise ValueError("Spot source projection evidence changed")
        value = decode_json(payload)
        if not isinstance(value, dict):
            raise ValueError("Spot source projection must remain an object")
        return value

    def effective(self, reference: FlowInputEvidenceRef) -> dict[str, JsonValue]:
        self.original(reference)
        value = decode_json(self._rows[reference].effective)
        if not isinstance(value, dict):
            raise ValueError("Effective spot scope projection must remain an object")
        return value

    def source_reference(self, source_evidence_sha256: str) -> FlowInputEvidenceRef:
        return self._by_digest[source_evidence_sha256]

    def descriptor(self, reference: FlowInputEvidenceRef) -> FlowSourceDescriptor:
        return self._rows[reference].descriptor

    def resolve_flow_aggregate(
        self, reference: FlowAggregateEvidenceRef, *, expected: ConditionFlowAggregateProjection
    ) -> Mapping[str, JsonValue]:
        if flow_aggregate_projection_sha256(expected) != reference.projection_sha256:
            raise ValueError("Expected flow projection does not match its evidence reference")
        source = self.source_reference(reference.source_evidence_sha256)
        descriptor = self.descriptor(source)
        effective = self.effective(source)
        observed = _flow(
            effective,
            descriptor.scope,
            source,
            boundary=source.collection == "flow_segments" or source.parent_index is not None,
        )
        projection = aggregate_projection(observed)
        if flow_aggregate_projection_sha256(projection) != reference.projection_sha256:
            raise ValueError("Actual source financial projection does not match the requested evidence")
        return effective

    def references(self) -> tuple[FlowInputEvidenceRef, ...]:
        return tuple(self._rows)


@dataclass(frozen=True, slots=True)
class DecodedSpotInputs:
    inputs: SpotCalculationInputs
    source_evidence: SpotSourceEvidence


def _wire(row: Mapping[str, JsonInput]) -> tuple[dict[str, JsonValue], bytes]:
    payload = canonical_json(row).encode()
    value = decode_json(payload)
    if not isinstance(value, dict):
        raise ValueError("Spot source row must be an object")
    return value, payload


def _text(row: Mapping[str, JsonValue], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Spot identity requires nonempty text: " + field)
    return value


def _amount(row: Mapping[str, JsonValue], field: str) -> Decimal | None:
    value = row.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("Spot native amount requires a finite number: " + field)
    try:
        result = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError("Spot native amount requires a finite number: " + field) from error
    if not result.is_finite():
        raise ValueError("Spot native amount requires a finite number: " + field)
    return result


def _count(row: Mapping[str, JsonValue], field: str) -> int | None:
    value = row.get(field)
    if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
        raise ValueError("Spot trade count requires a captured integer: " + field)
    return value


def _identity(
    row: Mapping[str, JsonValue], known: Mapping[str, SpotInstrument], token_id: str, non_spot: frozenset[str]
) -> SpotInstrument | None:
    instrument_id = _text(row, "instrument_id")
    if instrument_id in non_spot:
        if row.get("market_type") not in (None, "PERP_LINEAR"):
            raise ValueError("Spot source market contradicts its actual instrument")
        return None
    registered = known.get(instrument_id)
    if registered is not None:
        expected = {
            "token_id": registered.token_id,
            "venue": registered.venue,
            "market_type": registered.market_type,
        }
        for field, value in expected.items():
            if row.get(field) is not None and row[field] != value:
                raise ValueError("Spot source scope contradicts its actual instrument: " + field)
        return registered
    market = _text(row, "market_type")
    if market == "PERP_LINEAR":
        return None
    if market != "SPOT":
        raise ValueError("Spot read projection does not recognize the declared market type")
    instrument = SpotInstrument(
        token_id=_text(row, "token_id"),
        instrument_id=instrument_id,
        venue=_text(row, "venue"),
        market_type="SPOT",
    )
    if instrument.token_id != token_id:
        raise ValueError("Spot source instrument belongs to another token")
    return instrument


def _flow(
    row: Mapping[str, JsonValue],
    instrument: SpotInstrument,
    reference: FlowInputEvidenceRef,
    *,
    boundary: bool,
) -> SpotFlowObservation:
    start_field = "start" if boundary else "minute_start"
    start = parse_iso_time(_text(row, start_field))
    end = parse_iso_time(_text(row, "end")) if boundary else start + timedelta(minutes=1)
    complete = row.get("complete")
    if complete is not None and not isinstance(complete, bool):
        raise ValueError("Spot completeness requires a captured boolean")
    reasons: list[str] = []
    for field in ("gap_reason", "coverage_reason"):
        if row.get(field) is not None:
            reasons.append(_text(row, field))
    return SpotFlowObservation(
        instrument=instrument,
        start=start,
        end=end,
        kind="BOUNDARY" if boundary else "FULL_MINUTE",
        complete=complete,
        buy_qty=_amount(row, "buy_qty"),
        sell_qty=_amount(row, "sell_qty"),
        sum_sq_qty=_amount(row, "sum_sq_qty"),
        buy_notional=_amount(row, "buy_notional"),
        sell_notional=_amount(row, "sell_notional"),
        low=_amount(row, "low"),
        n_buy=_count(row, "n_buy"),
        n_sell=_count(row, "n_sell"),
        received_at=propagate_timestamp(row, "received_ts"),
        observed_at=propagate_timestamp(row, "observed_at"),
        source_reasons=tuple(sorted(set(reasons))),
        evidence=reference,
    )


def decode_spot_inputs(
    *,
    token_id: str,
    start: datetime,
    cutoff: datetime,
    assembled_at: Timestamp,
    instruments: Sequence[Mapping[str, JsonInput]],
    flow_bars: Sequence[Mapping[str, JsonInput]],
    flow_segments: Sequence[Mapping[str, JsonInput]],
    tickers: Sequence[Mapping[str, JsonInput]],
    policy: SpotCalculationPolicy,
    native_ticker_volume_field: Literal["volume_native", "volume"],
) -> DecodedSpotInputs:
    known: dict[str, SpotInstrument] = {}
    identity_sources: dict[str, tuple[bytes, Literal["INSTRUMENT_READ", "SOURCE_ROW"]]] = {}
    other_markets: set[str] = set()
    for source in instruments:
        row, identity_payload = _wire(source)
        market = _text(row, "market_type")
        if market == "PERP_LINEAR":
            identity_key = _text(row, "instrument_id")
            if identity_key in other_markets or identity_key in known:
                raise ValueError("Spot instrument input cannot repeat an identity")
            other_markets.add(identity_key)
            continue
        if market != "SPOT":
            raise ValueError("Spot read projection does not recognize the declared market type")
        identity = _identity(row, known, token_id, frozenset(other_markets))
        if identity is None:
            raise ValueError("An explicitly spot instrument cannot become another market type")
        if identity.instrument_id in known:
            raise ValueError("Spot instrument input cannot repeat an identity")
        known[identity.instrument_id] = identity
        identity_sources[identity.instrument_id] = (identity_payload, "INSTRUMENT_READ")
    originals: dict[FlowInputEvidenceRef, FlowSourceProjection] = {}
    minutes: list[SpotFlowObservation] = []
    boundaries: list[SpotFlowObservation] = []
    standalone_intervals: set[tuple[str, datetime, datetime]] = set()

    def read_flow(
        source: Mapping[str, JsonInput],
        index: int,
        parent: int | None,
        *,
        boundary: bool,
        original: Mapping[str, JsonInput] | None = None,
        parent_reference: FlowInputEvidenceRef | None = None,
    ) -> SpotFlowObservation | None:
        row, payload = _wire(source)
        identity = _identity(row, known, token_id, frozenset(other_markets))
        if identity is None:
            return None
        known[identity.instrument_id] = identity
        if identity.instrument_id not in identity_sources:
            identity_sources[identity.instrument_id] = (payload, "SOURCE_ROW")
        scope_original, scope_basis = identity_sources[identity.instrument_id]
        _original_row, original_payload = _wire(source if original is None else original)
        effective: dict[str, JsonValue] = dict(row)
        effective.update(
            token_id=identity.token_id,
            instrument_id=identity.instrument_id,
            venue=identity.venue,
            market_type=identity.market_type,
        )
        row, payload = _wire(effective)
        descriptor = FlowSourceDescriptor(
            format="SPOT_STORAGE_READ_V1",
            collection="flow_segments" if boundary and parent is None else "flow_bars",
            row_index=index,
            parent_index=parent,
            original_projection_sha256=sha256(original_payload),
            effective_projection_sha256=sha256(payload),
            scope_projection_sha256=sha256(scope_original),
            scope_basis=scope_basis,
            scope=identity,
            parent_source_evidence_sha256=None
            if parent_reference is None
            else parent_reference.source_evidence_sha256,
        )
        reference = FlowInputEvidenceRef(
            collection=descriptor.collection,
            row_index=index,
            parent_index=parent,
            source_projection_sha256=descriptor.original_projection_sha256,
            source_evidence_sha256=source_descriptor_digest(descriptor),
        )
        originals[reference] = FlowSourceProjection(
            original=original_payload,
            effective=payload,
            scope_original=scope_original,
            descriptor=descriptor,
        )
        return _flow(row, identity, reference, boundary=boundary)

    for index, source in enumerate(flow_segments):
        observed = read_flow(source, index, None, boundary=True)
        if observed is not None:
            key = (observed.instrument.instrument_id, observed.start, observed.end)
            if key in standalone_intervals:
                raise ValueError("Standalone boundary source repeats an aggregate interval")
            standalone_intervals.add(key)
            boundaries.append(observed)
    for index, source in enumerate(flow_bars):
        observed = read_flow(source, index, None, boundary=False)
        if observed is None:
            continue
        minutes.append(observed)
        raw, _payload = _wire(source)
        pieces = raw.get("boundary_segments")
        if pieces is None:
            continue
        if not isinstance(pieces, list):
            raise ValueError("Embedded boundary evidence requires an array")
        for piece_index, piece in enumerate(pieces):
            if not isinstance(piece, dict):
                raise ValueError("Embedded boundary evidence requires objects")
            effective: dict[str, JsonInput] = dict(piece)
            inherited = {
                "instrument_id": observed.instrument.instrument_id,
                "token_id": observed.instrument.token_id,
                "venue": observed.instrument.venue,
                "market_type": observed.instrument.market_type,
            }
            for field, actual in inherited.items():
                if piece.get(field) is not None and piece[field] != actual:
                    raise ValueError("Embedded boundary contradicts its actual parent scope")
                effective[field] = actual
            boundary_row = read_flow(
                effective,
                piece_index,
                index,
                boundary=True,
                original=piece,
                parent_reference=observed.evidence,
            )
            if boundary_row is None:
                raise ValueError("A spot boundary cannot change its actual parent market")
            if not observed.start <= boundary_row.start < boundary_row.end <= observed.end:
                raise ValueError("Embedded boundary must remain inside its actual parent minute")
            boundaries.append(boundary_row)
    ticker_values: list[SpotTickerObservation] = []
    for source in tickers:
        row, _payload = _wire(source)
        identity = _identity(row, known, token_id, frozenset(other_markets))
        if identity is None:
            continue
        known[identity.instrument_id] = identity
        ticker_values.append(
            SpotTickerObservation(
                instrument=identity,
                observed_at=propagate_timestamp(row, "ts"),
                trailing_day_native_volume=_amount(row, native_ticker_volume_field),
                native_volume_field=native_ticker_volume_field,
                received_at=propagate_timestamp(row, "received_ts"),
            )
        )
    return DecodedSpotInputs(
        inputs=SpotCalculationInputs(
            token_id=token_id,
            start=start,
            cutoff=cutoff,
            assembled_at=assembled_at,
            instruments=tuple(known.values()),
            minutes=tuple(minutes),
            boundaries=tuple(boundaries),
            tickers=tuple(ticker_values),
            policy=policy,
        ),
        source_evidence=SpotSourceEvidence(originals),
    )

"""Storage-read projection for spot calculations and separately retained source evidence."""

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Literal, Self

from pydantic import Field, JsonValue, ValidationError, model_validator

from tank.analysis.condition_graph_codec import encode_scoped_condition_spot, flow_aggregate_projection_sha256
from tank.analysis.condition_graph_contracts import (
    ConditionFlowAggregateProjection,
    ConditionInterval,
    ConditionSpotWindowGraph,
    ConditionTopVenueSignHistory,
    ConditionWindow,
    ConditionWireClocks,
    DirectWindowConditionSpot,
    FlowAggregateEvidenceRef,
)
from tank.analysis.condition_inputs import (
    FlowInputEvidenceRef,
    FlowSourceDescriptor,
    SpotCalculationInputs,
    SpotCalculationPolicy,
    SpotCalculationSource,
    SpotFlowObservation,
    SpotFlowParent,
    SpotInstrument,
    SpotTickerObservation,
    SpotWindowCalculationInputs,
    SpotWindowPlan,
)
from tank.analysis.spot import (
    _calculate_direct_spot,
    _calculate_spot_sign_history,
    _calculate_spot_window_graph,
    aggregate_projection,
)
from tank.contracts import DomainModel
from tank.contracts.json import decode_json, decode_unique_json
from tank.market.trade_contracts import EmptyBoundarySegment
from tank.time_precision import Timestamp, offset_schedule, propagate_timestamp
from tank.util import JsonInput, canonical_json, iso, parse_iso_time, sha256


class FlowSourceProjection(DomainModel):
    original: bytes
    effective: bytes
    scope_original: bytes
    descriptor: FlowSourceDescriptor


def source_descriptor_digest(descriptor: FlowSourceDescriptor) -> str:
    validated = FlowSourceDescriptor.model_validate(descriptor)
    return _admitted_source_descriptor_digest(validated)


def _admitted_source_descriptor_digest(descriptor: FlowSourceDescriptor) -> str:
    payload = json.dumps(
        descriptor.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()
    return sha256(payload)


class FlowEvidenceEntry(DomainModel):
    reference: FlowInputEvidenceRef
    projection: FlowSourceProjection


class SpotSourceEvidence(DomainModel):
    """Immutable canonical storage-read snapshots, never claimed as provider bytes."""

    entries: tuple[FlowEvidenceEntry, ...]

    @classmethod
    def from_rows(cls, rows: Mapping[FlowInputEvidenceRef, FlowSourceProjection]) -> Self:
        return cls(
            entries=tuple(
                FlowEvidenceEntry(reference=reference, projection=projection)
                for reference, projection in rows.items()
            )
        )

    @model_validator(mode="after")
    def actual_evidence(self) -> Self:
        checked: dict[FlowInputEvidenceRef, FlowSourceProjection] = {}
        digests: dict[str, FlowInputEvidenceRef] = {}
        slots: set[tuple[str, int, int | None]] = set()
        for entry in self.entries:
            reference, projection = entry.reference, entry.projection
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
                or _admitted_source_descriptor_digest(descriptor) != reference.source_evidence_sha256
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
                    parent_reference.collection not in {"flow_bars", "flow_parent_bars"}
                    or parent_reference.parent_index is not None
                    or parent_reference.row_index != projection.descriptor.parent_index
                    or parent_reference.collection != projection.descriptor.collection
                    or parent_descriptor.scope != projection.descriptor.scope
                ):
                    raise ValueError("Embedded spot lineage must bind its actual parent row and scope")
        return self

    def projection(self, reference: FlowInputEvidenceRef) -> FlowSourceProjection:
        for entry in self.entries:
            if entry.reference == reference:
                return entry.projection
        raise ValueError("Spot source evidence reference is not captured")

    def original(self, reference: FlowInputEvidenceRef) -> dict[str, JsonValue]:
        payload = self.projection(reference).original
        if sha256(payload) != reference.source_projection_sha256:
            raise ValueError("Spot source projection evidence changed")
        value = decode_json(payload)
        if not isinstance(value, dict):
            raise ValueError("Spot source projection must remain an object")
        return value

    def effective(self, reference: FlowInputEvidenceRef) -> dict[str, JsonValue]:
        self.original(reference)
        projection = self.projection(reference)
        if sha256(projection.effective) != projection.descriptor.effective_projection_sha256:
            raise ValueError("Effective spot source evidence changed")
        value = decode_json(projection.effective)
        if not isinstance(value, dict):
            raise ValueError("Effective spot scope projection must remain an object")
        return value

    def source_reference(self, source_evidence_sha256: str) -> FlowInputEvidenceRef:
        for entry in self.entries:
            if entry.reference.source_evidence_sha256 == source_evidence_sha256:
                return entry.reference
        raise ValueError("Spot source evidence identity is not captured")

    def descriptor(self, reference: FlowInputEvidenceRef) -> FlowSourceDescriptor:
        return self.projection(reference).descriptor

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
            parent_document=None,
        )
        projection = aggregate_projection(observed)
        if flow_aggregate_projection_sha256(projection) != reference.projection_sha256:
            raise ValueError("Actual source financial projection does not match the requested evidence")
        return effective

    def references(self) -> tuple[FlowInputEvidenceRef, ...]:
        return tuple(entry.reference for entry in self.entries)


class DecodedSpotInputs(DomainModel):
    inputs: SpotCalculationInputs
    source_evidence: SpotSourceEvidence


class SpotReadIdentity(DomainModel):
    format: Literal["CANONICAL_SPOT_READ_V1"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    start: datetime
    cutoff: datetime
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def actual_scope(self) -> Self:
        if any(value.tzinfo is None or value.utcoffset() is None for value in (self.start, self.cutoff)):
            raise ValueError("Spot read identity requires aware source bounds")
        if self.start >= self.cutoff:
            raise ValueError("Spot read identity requires a forward source scope")
        return self


class SpotWindowFacts(DomainModel):
    graph: ConditionSpotWindowGraph
    source_identity: SpotReadIdentity
    visibility_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    clocks: ConditionWireClocks

    @model_validator(mode="after")
    def actual_source_scope(self) -> Self:
        self.clocks.bounds(self.graph.spots["EVENT"].interval)
        if self.clocks.token_id != self.source_identity.token_id or any(
            value.token_id != self.source_identity.token_id
            or value.interval.cutoff != self.source_identity.cutoff
            or value.interval.start < self.source_identity.start
            for value in self.graph.spots.values()
        ):
            raise ValueError("Spot window facts must bind their actual source subject and interval")
        return self


class SpotDirectFacts(DomainModel):
    result: DirectWindowConditionSpot
    source_identity: SpotReadIdentity
    visibility_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def actual_source_scope(self) -> Self:
        if (
            self.result.token_id != self.source_identity.token_id
            or self.result.interval.start != self.source_identity.start
            or self.result.interval.cutoff != self.source_identity.cutoff
        ):
            raise ValueError("Direct spot facts must bind their actual source subject and interval")
        return self


def _wire(row: Mapping[str, JsonInput]) -> tuple[dict[str, JsonValue], bytes]:
    payload = canonical_json(row).encode()
    value = decode_json(payload)
    if not isinstance(value, dict):
        raise ValueError("Spot source row must be an object")
    return value, payload


def _input_time(row: Mapping[str, JsonInput], field: str) -> datetime:
    value = row.get(field)
    if not isinstance(value, (str, datetime)):
        raise ValueError("Spot geometry requires a declared ISO or aware datetime: " + field)
    return parse_iso_time(value)


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
    row: Mapping[str, JsonValue],
    known: Mapping[str, SpotInstrument],
    token_id: str,
    non_spot: frozenset[str],
    foreign: Mapping[str, SpotInstrument],
) -> SpotInstrument | None:
    instrument_id = _text(row, "instrument_id")
    if instrument_id in non_spot:
        if row.get("market_type") not in (None, "PERP_LINEAR"):
            raise ValueError("Spot source market contradicts its actual instrument")
        return None
    foreign_scope = foreign.get(instrument_id)
    if foreign_scope is not None:
        for field, foreign_value in (
            ("token_id", foreign_scope.token_id),
            ("venue", foreign_scope.venue),
            ("market_type", foreign_scope.market_type),
        ):
            if row.get(field) is not None and row[field] != foreign_value:
                raise ValueError("Excluded benchmark source contradicts its actual instrument scope")
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


def _generated_empty_piece(row: Mapping[str, JsonValue]) -> EmptyBoundarySegment | None:
    fields = {name: row[name] for name in EmptyBoundarySegment.model_fields if name in row}
    try:
        return EmptyBoundarySegment.model_validate_json(canonical_json(fields))
    except ValidationError:
        return None


def _flow(
    row: Mapping[str, JsonValue],
    instrument: SpotInstrument,
    reference: FlowInputEvidenceRef,
    *,
    boundary: bool,
    parent_document: SpotFlowParent | None,
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
    generated_empty = _generated_empty_piece(row) if boundary else None
    return SpotFlowObservation(
        instrument=instrument,
        start=start,
        end=end,
        kind="BOUNDARY" if boundary else "FULL_MINUTE",
        start_clock=propagate_timestamp(row, start_field),
        end_clock=propagate_timestamp(row, "end")
        if boundary
        else offset_schedule(propagate_timestamp(row, "minute_start"), timedelta(minutes=1)),
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
        parent_document=parent_document,
        generated_empty=generated_empty,
    )


class SpotEvidenceLookup:
    """Admit raw reads once and own immutable source snapshots plus lookup indexes.

    These dictionaries are adapter lookup state. Calculation facts and captured
    evidence remain strict records; request assembly is supplied separately.
    """

    __slots__ = ("_entries", "_identity", "_references", "_snapshot", "_source")

    def __init__(
        self,
        *,
        token_id: str,
        start: datetime,
        cutoff: datetime,
        instruments: Sequence[Mapping[str, JsonInput]],
        flow_bars: Sequence[Mapping[str, JsonInput]],
        flow_parent_bars: Sequence[Mapping[str, JsonInput]],
        flow_segments: Sequence[Mapping[str, JsonInput]],
        tickers: Sequence[Mapping[str, JsonInput]],
        policy: SpotCalculationPolicy,
        native_ticker_volume_field: Literal["volume_native", "volume"],
    ) -> None:
        known: dict[str, SpotInstrument] = {}
        foreign: dict[str, SpotInstrument] = {}
        identity_sources: dict[str, tuple[bytes, Literal["INSTRUMENT_READ", "SOURCE_ROW"]]] = {}
        other_markets: set[str] = set()
        for source in instruments:
            row, identity_payload = _wire(source)
            market = _text(row, "market_type")
            if market == "PERP_LINEAR":
                identity_key = _text(row, "instrument_id")
                if identity_key in other_markets or identity_key in known or identity_key in foreign:
                    raise ValueError("Spot instrument input cannot repeat an identity")
                other_markets.add(identity_key)
                continue
            if market != "SPOT":
                raise ValueError("Spot read projection does not recognize the declared market type")
            if _text(row, "token_id") != token_id:
                identity_key = _text(row, "instrument_id")
                if identity_key in other_markets or identity_key in known or identity_key in foreign:
                    raise ValueError("Spot instrument input cannot repeat an identity")
                foreign[identity_key] = SpotInstrument(
                    token_id=_text(row, "token_id"),
                    instrument_id=identity_key,
                    venue=_text(row, "venue"),
                    market_type="SPOT",
                )
                continue
            identity = _identity(row, known, token_id, frozenset(other_markets), foreign)
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

        def capture_flow(
            source: Mapping[str, JsonInput],
            index: int,
            parent: int | None,
            *,
            collection: Literal["flow_bars", "flow_segments", "flow_parent_bars"],
            original: Mapping[str, JsonInput] | None = None,
            parent_reference: FlowInputEvidenceRef | None = None,
        ) -> tuple[dict[str, JsonValue], SpotInstrument, FlowInputEvidenceRef] | None:
            row, payload = _wire(source)
            identity = _identity(row, known, token_id, frozenset(other_markets), foreign)
            if identity is None:
                return None
            known[identity.instrument_id] = identity
            if identity.instrument_id not in identity_sources:
                identity_sources[identity.instrument_id] = (payload, "SOURCE_ROW")
            scope_original, scope_basis = identity_sources[identity.instrument_id]
            original_payload = payload if original is None else canonical_json(original).encode()
            effective: dict[str, JsonValue] = dict(row)
            effective.update(
                token_id=identity.token_id,
                instrument_id=identity.instrument_id,
                venue=identity.venue,
                market_type=identity.market_type,
            )
            if effective != row:
                payload = canonical_json(effective).encode()
            row = effective
            descriptor = FlowSourceDescriptor(
                format="SPOT_INPUT_READ_V1",
                collection=collection,
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
                source_evidence_sha256=_admitted_source_descriptor_digest(descriptor),
            )
            originals[reference] = FlowSourceProjection(
                original=original_payload,
                effective=payload,
                scope_original=scope_original,
                descriptor=descriptor,
            )
            return row, identity, reference

        for index, source in enumerate(flow_segments):
            piece_start, piece_end = _input_time(source, "start"), _input_time(source, "end")
            if piece_start < start or piece_end > cutoff:
                continue
            captured = capture_flow(source, index, None, collection="flow_segments")
            if captured is not None:
                row, identity, reference = captured
                observed = _flow(row, identity, reference, boundary=True, parent_document=None)
                key = (observed.instrument.instrument_id, observed.start, observed.end)
                if key in standalone_intervals:
                    raise ValueError("Standalone boundary source repeats an aggregate interval")
                standalone_intervals.add(key)
                boundaries.append(observed)
        parent_sources: tuple[
            tuple[Literal["flow_bars", "flow_parent_bars"], Sequence[Mapping[str, JsonInput]]], ...
        ] = (("flow_bars", flow_bars), ("flow_parent_bars", flow_parent_bars))
        for collection, sources in parent_sources:
            for index, source in enumerate(sources):
                source_start = _input_time(source, "minute_start")
                if source_start >= cutoff or source_start + timedelta(minutes=1) <= start:
                    continue
                captured = capture_flow(source, index, None, collection=collection)
                if captured is None:
                    continue
                raw, identity, reference = captured
                minute_start = parse_iso_time(_text(raw, "minute_start"))
                minute_end = minute_start + timedelta(minutes=1)
                if collection == "flow_bars" and start <= minute_start and minute_end <= cutoff:
                    minutes.append(_flow(raw, identity, reference, boundary=False, parent_document=None))
                pieces = raw.get("boundary_segments")
                if pieces is None:
                    continue
                if not isinstance(pieces, list):
                    raise ValueError("Embedded boundary evidence requires an array")
                if not pieces:
                    continue
                parent = SpotFlowParent(
                    instrument=identity,
                    start=minute_start,
                    end=minute_end,
                    received_at=propagate_timestamp(raw, "received_ts"),
                    observed_at=propagate_timestamp(raw, "observed_at"),
                    evidence=reference,
                )
                for piece_index, piece in enumerate(pieces):
                    if not isinstance(piece, dict):
                        raise ValueError("Embedded boundary evidence requires objects")
                    piece_start = parse_iso_time(_text(piece, "start"))
                    piece_end = parse_iso_time(_text(piece, "end"))
                    if not parent.start <= piece_start < piece_end <= parent.end:
                        raise ValueError("Embedded boundary must remain inside its actual parent minute")
                    if piece_start < start or piece_end > cutoff:
                        continue
                    effective: dict[str, JsonInput] = dict(piece)
                    inherited = {
                        "instrument_id": identity.instrument_id,
                        "token_id": identity.token_id,
                        "venue": identity.venue,
                        "market_type": identity.market_type,
                    }
                    for field, actual in inherited.items():
                        if piece.get(field) is not None and piece[field] != actual:
                            raise ValueError("Embedded boundary contradicts its actual parent scope")
                        effective[field] = actual
                    boundary_source = capture_flow(
                        effective,
                        piece_index,
                        index,
                        collection=collection,
                        original=piece,
                        parent_reference=reference,
                    )
                    if boundary_source is None:
                        raise ValueError("A spot boundary cannot change its actual parent market")
                    boundary_wire, boundary_identity, boundary_reference = boundary_source
                    boundaries.append(
                        _flow(
                            boundary_wire,
                            boundary_identity,
                            boundary_reference,
                            boundary=True,
                            parent_document=parent,
                        )
                    )
        ticker_values: list[SpotTickerObservation] = []
        for source in tickers:
            row, _payload = _wire(source)
            identity = _identity(row, known, token_id, frozenset(other_markets), foreign)
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
        self._source = SpotCalculationSource(
            token_id=token_id,
            start=start,
            cutoff=cutoff,
            instruments=tuple(known.values()),
            minutes=tuple(minutes),
            boundaries=tuple(boundaries),
            tickers=tuple(ticker_values),
            policy=policy,
        )
        self._snapshot = SpotSourceEvidence.from_rows(originals)
        self._entries: dict[FlowInputEvidenceRef, FlowSourceProjection] = {
            entry.reference: entry.projection for entry in self._snapshot.entries
        }
        self._references: dict[str, FlowInputEvidenceRef] = {
            entry.reference.source_evidence_sha256: entry.reference for entry in self._snapshot.entries
        }
        instrument_projection: list[JsonInput] = [
            value.model_dump(mode="json") for value in self._source.instruments
        ]
        ticker_projection: list[JsonInput] = [value.model_dump(mode="json") for value in self._source.tickers]
        evidence_projection: list[JsonInput] = [
            entry.reference.model_dump(mode="json") for entry in self._snapshot.entries
        ]
        self._identity = SpotReadIdentity(
            format="CANONICAL_SPOT_READ_V1",
            token_id=self._source.token_id,
            start=self._source.start,
            cutoff=self._source.cutoff,
            sha256=sha256(
                {
                    "format": "CANONICAL_SPOT_READ_V1",
                    "token_id": self._source.token_id,
                    "start": iso(self._source.start),
                    "cutoff": iso(self._source.cutoff),
                    "policy": self._source.policy.model_dump(mode="json"),
                    "instruments": instrument_projection,
                    "tickers": ticker_projection,
                    "flow_evidence": evidence_projection,
                }
            ),
        )

    @property
    def source(self) -> SpotCalculationSource:
        return self._source

    @property
    def identity(self) -> SpotReadIdentity:
        return self._identity

    def inputs_at(self, assembled_at: Timestamp) -> SpotCalculationInputs:
        return self._source.at(assembled_at)

    def visibility_fingerprint(self, assembled_at: Timestamp) -> str:
        assembly = Timestamp.model_validate(assembled_at)
        flow_visibility: list[JsonInput] = [
            {
                "source_evidence_sha256": row.evidence.source_evidence_sha256,
                "visible": row.visible_at(assembly),
            }
            for row in (*self._source.minutes, *self._source.boundaries)
        ]
        ticker_visibility: list[JsonInput] = [
            {
                "instrument_id": row.instrument.instrument_id,
                "visible": row.visible_at(assembly),
            }
            for row in self._source.tickers
        ]
        return sha256(
            {
                "format": "SPOT_ADMITTED_VISIBILITY_V1",
                "source_identity": self.identity.sha256,
                "flow_visibility": flow_visibility,
                "ticker_visibility": ticker_visibility,
            }
        )

    @property
    def snapshot(self) -> SpotSourceEvidence:
        return self._snapshot

    def source_reference(self, source_evidence_sha256: str) -> FlowInputEvidenceRef:
        try:
            return self._references[source_evidence_sha256]
        except KeyError:
            raise ValueError("Spot source evidence identity is not captured") from None

    def resolve_flow_aggregate(
        self, reference: FlowAggregateEvidenceRef, *, expected: ConditionFlowAggregateProjection
    ) -> Mapping[str, JsonValue]:
        reference = FlowAggregateEvidenceRef.model_validate(reference)
        if flow_aggregate_projection_sha256(expected) != reference.projection_sha256:
            raise ValueError("Expected flow projection does not match its evidence reference")
        source = self.source_reference(reference.source_evidence_sha256)
        captured = self._entries[source]
        if sha256(captured.original) != source.source_projection_sha256:
            raise ValueError("Spot source projection evidence changed")
        original = decode_unique_json(captured.original)
        if not isinstance(original, dict):
            raise ValueError("Spot source projection must remain an object")
        if sha256(captured.effective) != captured.descriptor.effective_projection_sha256:
            raise ValueError("Effective spot source evidence changed")
        effective = decode_unique_json(captured.effective)
        if not isinstance(effective, dict):
            raise ValueError("Effective spot scope projection must remain an object")
        observed = _flow(
            effective,
            captured.descriptor.scope,
            source,
            boundary=source.collection == "flow_segments" or source.parent_index is not None,
            parent_document=None,
        )
        projection = aggregate_projection(observed)
        if flow_aggregate_projection_sha256(projection) != reference.projection_sha256:
            raise ValueError("Actual source financial projection does not match the requested evidence")
        return effective


def _spot_window_plan(clocks: ConditionWireClocks) -> SpotWindowPlan:
    if clocks.cutoff.value is None:
        raise ValueError("Spot windows require the actual cutoff")
    end = parse_iso_time(clocks.cutoff.value)
    anchor = (
        parse_iso_time(clocks.event_anchor.value)
        if clocks.event_anchor.value is not None
        else end - timedelta(hours=4)
    )
    if anchor >= end:
        raise ValueError("Forward spot windows require an earlier actual event or analysis anchor")
    durations: dict[ConditionWindow, timedelta] = {
        "W15M": timedelta(minutes=15),
        "W1H": timedelta(hours=1),
        "W4H": timedelta(hours=4),
        "W24H": timedelta(days=1),
        "EVENT": end - anchor,
    }
    current = {
        name: ConditionInterval(window=name, start=end - duration, cutoff=end)
        for name, duration in durations.items()
    }
    previous = {
        name: ConditionInterval(window=name, start=end - 2 * duration, cutoff=end - duration)
        for name, duration in durations.items()
    }
    primary: Literal["EVENT", "W4H"] = (
        "EVENT"
        if clocks.event_anchor.value is not None
        and timedelta(minutes=15) <= end - anchor <= timedelta(days=1)
        else "W4H"
    )
    return SpotWindowPlan(
        token_id=clocks.token_id,
        source_start=min(value.start for value in previous.values()),
        cutoff=end,
        current=current,
        previous=previous,
        event_anchor=clocks.event_anchor,
        primary=primary,
    )


class SpotWindowEvidenceLookup(SpotEvidenceLookup):
    """Own one raw admission, cached window facts and their actual evidence lookup."""

    __slots__ = ("_facts",)

    def __init__(
        self,
        *,
        token_id: str,
        cutoff: Timestamp,
        event_anchor: Timestamp,
        assembled_at: Timestamp,
        instruments: Sequence[Mapping[str, JsonInput]],
        flow_bars: Sequence[Mapping[str, JsonInput]],
        flow_parent_bars: Sequence[Mapping[str, JsonInput]],
        flow_segments: Sequence[Mapping[str, JsonInput]],
        tickers: Sequence[Mapping[str, JsonInput]],
        policy: SpotCalculationPolicy,
        native_ticker_volume_field: Literal["volume_native", "volume"],
    ) -> None:
        clocks = ConditionWireClocks(token_id=token_id, cutoff=cutoff, event_anchor=event_anchor)
        plan = _spot_window_plan(clocks)
        assembly = Timestamp.model_validate(assembled_at)
        super().__init__(
            token_id=token_id,
            start=plan.source_start,
            cutoff=plan.cutoff,
            instruments=instruments,
            flow_bars=flow_bars,
            flow_parent_bars=flow_parent_bars,
            flow_segments=flow_segments,
            tickers=tickers,
            policy=policy,
            native_ticker_volume_field=native_ticker_volume_field,
        )
        self._facts = SpotWindowFacts(
            graph=_calculate_spot_window_graph(self._source, plan=plan, assembled_at=assembly),
            source_identity=self.identity,
            visibility_sha256=self.visibility_fingerprint(assembly),
            clocks=clocks,
        )

    @property
    def facts(self) -> SpotWindowFacts:
        return SpotWindowFacts.model_validate(self._facts)

    @property
    def graph(self) -> ConditionSpotWindowGraph:
        return ConditionSpotWindowGraph.model_validate(self._facts.graph)

    @property
    def clocks(self) -> ConditionWireClocks:
        return self._facts.clocks

    def matches_visibility(self, assembled_at: Timestamp) -> bool:
        return self.visibility_fingerprint(assembled_at) == self._facts.visibility_sha256

    def wire_spots(self, *, assembled_at: Timestamp) -> dict[ConditionWindow, dict[str, JsonValue]]:
        if not self.matches_visibility(assembled_at):
            raise ValueError("Spot window facts require recomputation after source visibility changes")
        return {
            name: encode_scoped_condition_spot(value, self, clocks=self._facts.clocks)
            for name, value in self._facts.graph.spots.items()
        }

    def sign_history(
        self,
        *,
        interval: ConditionInterval,
        selected_venues: tuple[str, ...],
        assembled_at: Timestamp,
    ) -> ConditionTopVenueSignHistory:
        if not self.matches_visibility(assembled_at):
            raise ValueError("Spot sign history cannot be combined with stale window visibility")
        return _calculate_spot_sign_history(
            self._source,
            interval=interval,
            selected_venues=selected_venues,
            assembled_at=Timestamp.model_validate(assembled_at),
        )


class SpotDirectEvidenceLookup(SpotEvidenceLookup):
    """Own a single requested window and the source lookup used by its serializer."""

    __slots__ = ("_facts",)

    def __init__(
        self,
        *,
        token_id: str,
        start: datetime,
        cutoff: datetime,
        window: ConditionWindow,
        assembled_at: Timestamp,
        instruments: Sequence[Mapping[str, JsonInput]],
        flow_bars: Sequence[Mapping[str, JsonInput]],
        flow_parent_bars: Sequence[Mapping[str, JsonInput]],
        flow_segments: Sequence[Mapping[str, JsonInput]],
        tickers: Sequence[Mapping[str, JsonInput]],
        policy: SpotCalculationPolicy,
        native_ticker_volume_field: Literal["volume_native", "volume"],
    ) -> None:
        assembly = Timestamp.model_validate(assembled_at)
        super().__init__(
            token_id=token_id,
            start=start,
            cutoff=cutoff,
            instruments=instruments,
            flow_bars=flow_bars,
            flow_parent_bars=flow_parent_bars,
            flow_segments=flow_segments,
            tickers=tickers,
            policy=policy,
            native_ticker_volume_field=native_ticker_volume_field,
        )
        self._facts = SpotDirectFacts(
            result=_calculate_direct_spot(self._source, window=window, assembled_at=assembly),
            source_identity=self.identity,
            visibility_sha256=self.visibility_fingerprint(assembly),
        )

    def matches_visibility(self, assembled_at: Timestamp) -> bool:
        return self.visibility_fingerprint(assembled_at) == self._facts.visibility_sha256

    @property
    def facts(self) -> SpotDirectFacts:
        return SpotDirectFacts.model_validate(self._facts)

    @property
    def result(self) -> DirectWindowConditionSpot:
        return DirectWindowConditionSpot.model_validate(self._facts.result)


def decode_spot_inputs(
    *,
    token_id: str,
    start: datetime,
    cutoff: datetime,
    assembled_at: Timestamp,
    instruments: Sequence[Mapping[str, JsonInput]],
    flow_bars: Sequence[Mapping[str, JsonInput]],
    flow_parent_bars: Sequence[Mapping[str, JsonInput]],
    flow_segments: Sequence[Mapping[str, JsonInput]],
    tickers: Sequence[Mapping[str, JsonInput]],
    policy: SpotCalculationPolicy,
    native_ticker_volume_field: Literal["volume_native", "volume"],
) -> DecodedSpotInputs:
    lookup = SpotEvidenceLookup(
        token_id=token_id,
        start=start,
        cutoff=cutoff,
        instruments=instruments,
        flow_bars=flow_bars,
        flow_parent_bars=flow_parent_bars,
        flow_segments=flow_segments,
        tickers=tickers,
        policy=policy,
        native_ticker_volume_field=native_ticker_volume_field,
    )
    return DecodedSpotInputs(inputs=lookup.inputs_at(assembled_at), source_evidence=lookup.snapshot)


class DecodedSpotWindows(DomainModel):
    inputs: SpotWindowCalculationInputs
    source_evidence: SpotSourceEvidence
    clocks: ConditionWireClocks


def decode_spot_windows(
    *,
    token_id: str,
    cutoff: Timestamp,
    event_anchor: Timestamp,
    assembled_at: Timestamp,
    instruments: Sequence[Mapping[str, JsonInput]],
    flow_bars: Sequence[Mapping[str, JsonInput]],
    flow_parent_bars: Sequence[Mapping[str, JsonInput]],
    flow_segments: Sequence[Mapping[str, JsonInput]],
    tickers: Sequence[Mapping[str, JsonInput]],
    policy: SpotCalculationPolicy,
    native_ticker_volume_field: Literal["volume_native", "volume"],
) -> DecodedSpotWindows:
    clocks = ConditionWireClocks(token_id=token_id, cutoff=cutoff, event_anchor=event_anchor)
    plan = _spot_window_plan(clocks)
    lookup = SpotEvidenceLookup(
        token_id=token_id,
        start=plan.source_start,
        cutoff=plan.cutoff,
        instruments=instruments,
        flow_bars=flow_bars,
        flow_parent_bars=flow_parent_bars,
        flow_segments=flow_segments,
        tickers=tickers,
        policy=policy,
        native_ticker_volume_field=native_ticker_volume_field,
    )
    return DecodedSpotWindows(
        inputs=SpotWindowCalculationInputs(
            source=lookup.inputs_at(assembled_at),
            current=plan.current,
            previous=plan.previous,
            event_anchor=plan.event_anchor,
            primary=plan.primary,
        ),
        source_evidence=lookup.snapshot,
        clocks=clocks,
    )

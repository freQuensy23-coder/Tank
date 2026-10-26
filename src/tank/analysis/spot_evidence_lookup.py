"""Adapter-owned lookup indexes over immutable admitted spot read snapshots."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Literal

from pydantic import JsonValue

from tank.analysis.condition_graph_codec import flow_aggregate_projection_sha256
from tank.analysis.condition_graph_contracts import ConditionFlowAggregateProjection, FlowAggregateEvidenceRef
from tank.analysis.condition_input_codec import (
    FlowSourceProjection,
    SpotSourceEvidence,
    _flow,
    decode_spot_inputs,
)
from tank.analysis.condition_inputs import (
    FlowInputEvidenceRef,
    SpotCalculationInputs,
    SpotCalculationPolicy,
    SpotCalculationSource,
)
from tank.analysis.spot import aggregate_projection
from tank.contracts.json import decode_unique_json
from tank.time_precision import Timestamp
from tank.util import JsonInput, sha256


class SpotEvidenceLookup:
    """Own raw-read admission and indexes; retain facts in strict immutable records.

    Construction accepts the actual read boundary, never a caller's purportedly
    admitted model. Index dictionaries are private lookup state, not graph facts.
    No processing clock is advanced during resolution or evidence serialization.
    """

    __slots__ = ("_entries", "_references", "_snapshot", "_source")

    def __init__(
        self,
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
    ) -> None:
        admitted = decode_spot_inputs(
            token_id=token_id,
            start=start,
            cutoff=cutoff,
            assembled_at=assembled_at,
            instruments=instruments,
            flow_bars=flow_bars,
            flow_parent_bars=flow_parent_bars,
            flow_segments=flow_segments,
            tickers=tickers,
            policy=policy,
            native_ticker_volume_field=native_ticker_volume_field,
        )
        self._source = SpotCalculationSource.from_inputs(admitted.inputs)
        self._snapshot = admitted.source_evidence
        self._entries: dict[FlowInputEvidenceRef, FlowSourceProjection] = {
            entry.reference: entry.projection for entry in self._snapshot.entries
        }
        self._references: dict[str, FlowInputEvidenceRef] = {
            entry.reference.source_evidence_sha256: entry.reference for entry in self._snapshot.entries
        }

    @property
    def source(self) -> SpotCalculationSource:
        return self._source

    def inputs_at(self, assembled_at: Timestamp) -> SpotCalculationInputs:
        return self._source.at(assembled_at)

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

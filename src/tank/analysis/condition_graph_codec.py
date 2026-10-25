"""Explicit condition-graph serialization and consumed-evidence binding."""

from collections.abc import Mapping
from copy import deepcopy
from typing import Protocol

from pydantic import JsonValue

from tank.analysis.condition_graph_contracts import (
    BasicConditionSpot,
    ConditionFlowAggregate,
    ConditionFlowAggregateProjection,
    ConditionFlowCaptureProjection,
    ConditionFlowCaptureWitness,
    FlowAggregateEvidenceRef,
    FlowCaptureEvidenceRef,
    KnownConditionTradeCount,
)
from tank.contracts.json import JsonDocument
from tank.util import iso, sha256


def _checked_projection(value: ConditionFlowAggregateProjection) -> ConditionFlowAggregateProjection:
    if type(value) not in (ConditionFlowAggregateProjection, ConditionFlowAggregate):
        raise TypeError("A declared flow aggregate projection is required")
    validated = type(value).model_validate(value)
    return ConditionFlowAggregateProjection(
        token_id=validated.token_id,
        venue=validated.venue,
        market_type=validated.market_type,
        instrument_id=validated.instrument_id,
        start=validated.start,
        end=validated.end,
        complete=validated.complete,
        buy_qty=validated.buy_qty,
        sell_qty=validated.sell_qty,
        sum_sq_qty=validated.sum_sq_qty,
        buy_notional=validated.buy_notional,
        sell_notional=validated.sell_notional,
        notional_reason=validated.notional_reason,
        low=validated.low,
        low_reason=validated.low_reason,
        counts=validated.counts,
    )


def flow_aggregate_projection_sha256(value: ConditionFlowAggregateProjection) -> str:
    projection = _checked_projection(value)
    serialized = JsonDocument.model_validate(projection.model_dump(mode="json"), strict=True).root
    envelope: dict[str, JsonValue] = {"schema": "tank.condition.flow-aggregate.v1", "projection": serialized}
    return sha256(envelope)


def validate_flow_aggregate_evidence(value: ConditionFlowAggregate) -> None:
    if type(value) is not ConditionFlowAggregate:
        raise TypeError("A bound flow aggregate is required")
    validated = ConditionFlowAggregate.model_validate(value)
    expected = flow_aggregate_projection_sha256(validated)
    if validated.evidence.projection_sha256 != expected:
        raise ValueError("Flow aggregate evidence does not match its declared projection")


def bind_flow_aggregate_evidence(
    value: ConditionFlowAggregateProjection, reference: FlowAggregateEvidenceRef
) -> ConditionFlowAggregate:
    projection = _checked_projection(value)
    if isinstance(value, ConditionFlowAggregate):
        validate_flow_aggregate_evidence(value)
    digest = flow_aggregate_projection_sha256(projection)
    validated_reference = FlowAggregateEvidenceRef.model_validate(reference)
    if validated_reference.projection_sha256 != digest:
        raise ValueError("Flow aggregate evidence does not match its declared projection")
    return ConditionFlowAggregate(
        token_id=projection.token_id,
        venue=projection.venue,
        market_type=projection.market_type,
        instrument_id=projection.instrument_id,
        start=projection.start,
        end=projection.end,
        complete=projection.complete,
        buy_qty=projection.buy_qty,
        sell_qty=projection.sell_qty,
        sum_sq_qty=projection.sum_sq_qty,
        buy_notional=projection.buy_notional,
        sell_notional=projection.sell_notional,
        notional_reason=projection.notional_reason,
        low=projection.low,
        low_reason=projection.low_reason,
        counts=projection.counts,
        evidence=validated_reference,
    )


class ConditionFlowEvidenceLookup(Protocol):
    """Resolve immutable effective wire evidence after reprojecting and binding the expected data.

    The read adapter preserves original evidence separately, refuses ambiguous projection
    identities, and never substitutes expected financial values for missing source fields.
    """

    def resolve_flow_aggregate(
        self, reference: FlowAggregateEvidenceRef, *, expected: ConditionFlowAggregateProjection
    ) -> Mapping[str, JsonValue]: ...


def flow_aggregate_evidence_wire(
    value: ConditionFlowAggregate, evidence: ConditionFlowEvidenceLookup
) -> dict[str, JsonValue]:
    validate_flow_aggregate_evidence(value)
    projection = _checked_projection(value)
    resolved = evidence.resolve_flow_aggregate(value.evidence, expected=projection)
    checked = JsonDocument.validate_value(dict(resolved))
    if not isinstance(checked, dict):
        raise ValueError("Flow aggregate proof serialization requires an object")
    return deepcopy(checked)


def _checked_capture_projection(value: ConditionFlowCaptureProjection) -> ConditionFlowCaptureProjection:
    if type(value) not in (
        ConditionFlowCaptureProjection,
        ConditionFlowCaptureWitness,
        ConditionFlowAggregateProjection,
        ConditionFlowAggregate,
    ):
        raise TypeError("A declared flow capture projection is required")
    validated = type(value).model_validate(value)
    if isinstance(validated, ConditionFlowAggregate):
        validate_flow_aggregate_evidence(validated)
    return ConditionFlowCaptureProjection(
        token_id=validated.token_id,
        venue=validated.venue,
        market_type=validated.market_type,
        instrument_id=validated.instrument_id,
        start=validated.start,
        end=validated.end,
        complete=validated.complete,
    )


def flow_capture_projection_sha256(value: ConditionFlowCaptureProjection) -> str:
    projection = _checked_capture_projection(value)
    serialized = JsonDocument.model_validate(projection.model_dump(mode="json"), strict=True).root
    envelope: dict[str, JsonValue] = {"schema": "tank.condition.flow-capture.v1", "projection": serialized}
    return sha256(envelope)


def validate_flow_capture_evidence(value: ConditionFlowCaptureWitness) -> None:
    if type(value) is not ConditionFlowCaptureWitness:
        raise TypeError("A bound flow capture witness is required")
    validated = ConditionFlowCaptureWitness.model_validate(value)
    if validated.evidence.projection_sha256 != flow_capture_projection_sha256(validated):
        raise ValueError("Flow capture evidence does not match its declared projection")


def bind_flow_capture_evidence(
    value: ConditionFlowCaptureProjection, reference: FlowCaptureEvidenceRef
) -> ConditionFlowCaptureWitness:
    projection = _checked_capture_projection(value)
    if isinstance(value, ConditionFlowCaptureWitness):
        validate_flow_capture_evidence(value)
    checked_reference = FlowCaptureEvidenceRef.model_validate(reference)
    if checked_reference.projection_sha256 != flow_capture_projection_sha256(projection):
        raise ValueError("Flow capture evidence does not match its declared projection")
    return ConditionFlowCaptureWitness(
        token_id=projection.token_id,
        venue=projection.venue,
        market_type=projection.market_type,
        instrument_id=projection.instrument_id,
        start=projection.start,
        end=projection.end,
        complete=projection.complete,
        evidence=checked_reference,
    )


def encode_basic_condition_spot(
    value: BasicConditionSpot, evidence: ConditionFlowEvidenceLookup
) -> dict[str, JsonValue]:
    validated = BasicConditionSpot.model_validate(value)
    result = JsonDocument.validate_value(
        validated.model_dump(mode="json", exclude={"kind", "token_id"}, warnings="error")
    )
    if not isinstance(result, dict):
        raise TypeError("A basic spot result must serialize as an object")
    venues: list[JsonValue] = []
    for venue in validated.venues:
        serialized_venue = JsonDocument.validate_value(
            venue.model_dump(mode="json", exclude={"boundary_segments"}, warnings="error")
        )
        if not isinstance(serialized_venue, dict):
            raise TypeError("A spot venue must serialize as an object")
        boundaries: list[JsonValue] = []
        for boundary in venue.boundary_segments:
            count = boundary.trade_count()
            serialized_boundary: dict[str, JsonValue] = {
                "start": iso(boundary.start),
                "cutoff": iso(boundary.cutoff),
                "trade_count": count.value,
                "aggregates": [
                    flow_aggregate_evidence_wire(aggregate, evidence) for aggregate in boundary.aggregates
                ],
            }
            if not isinstance(count, KnownConditionTradeCount):
                serialized_boundary["trade_count_reason"] = count.reason
            boundaries.append(serialized_boundary)
        serialized_venue["boundary_segments"] = boundaries
        venues.append(serialized_venue)
    result["venues"] = venues
    composite = JsonDocument.validate_value(
        validated.composite_max.model_dump(mode="json", exclude={"status"}, warnings="error")
    )
    result["composite_max"] = composite
    result["composite_cohort"] = None
    result["cohort_consistent"] = {}
    result["size_profile"] = None
    return result

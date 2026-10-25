from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.condition_graph_codec import (
    bind_flow_aggregate_evidence,
    flow_aggregate_evidence_wire,
    flow_aggregate_projection_sha256,
    validate_flow_aggregate_evidence,
)
from tank.analysis.condition_graph_contracts import (
    ConditionFlowAggregateProjection,
    FlowAggregateEvidenceRef,
    IncompleteConditionTradeCounts,
)
from tank.util import sha256


def projection() -> ConditionFlowAggregateProjection:
    end = datetime(2026, 10, 17, 12, tzinfo=UTC)
    return ConditionFlowAggregateProjection(
        token_id="T",
        venue="a",
        market_type="SPOT",
        instrument_id="fixture-instrument",
        start=end - timedelta(seconds=30),
        end=end,
        complete=True,
        buy_qty=Decimal(10),
        sell_qty=Decimal(0),
        sum_sq_qty=Decimal(100),
        buy_notional=None,
        sell_notional=None,
        notional_reason="INCOMPLETE",
        low=None,
        low_reason="INCOMPLETE",
        counts=IncompleteConditionTradeCounts(
            status="UNAVAILABLE", n_buy=None, n_sell=0, reason="INCOMPLETE"
        ),
    )


class EvidenceLookup:
    def __init__(self, expected: ConditionFlowAggregateProjection) -> None:
        self.expected = expected
        self.calls = 0
        self.wire: dict[str, JsonValue] = {
            "token_id": expected.token_id,
            "venue": expected.venue,
            "market_type": expected.market_type,
            "instrument_id": expected.instrument_id,
            "start": "2026-10-17T11:59:30Z",
            "end": "2026-10-17T12:00:00Z",
            "complete": True,
            "buy_qty": "10",
            "sell_qty": "0",
            "sum_sq_qty": "100",
            "buy_notional": None,
            "sell_notional": None,
            "low": None,
            "provider_note": {"unchanged": True},
            "n_sell": 0,
        }
        self.source_identity = sha256(
            {
                "schema": "fixture-storage-read-evidence.v1",
                "original": self.wire,
                "effective": self.wire,
                "parent": None,
            }
        )

    def resolve_flow_aggregate(
        self, reference: FlowAggregateEvidenceRef, *, expected: ConditionFlowAggregateProjection
    ) -> Mapping[str, JsonValue]:
        self.calls += 1
        assert expected == self.expected
        assert reference.source_evidence_sha256 == self.source_identity
        assert reference.projection_sha256 == flow_aggregate_projection_sha256(self.expected)
        return self.wire


def test_projection_hash_binds_actual_token_venue_and_market_without_id_inference() -> None:
    source = projection()
    digest = flow_aggregate_projection_sha256(source)
    assert len(digest) == 64
    for field, value in (("token_id", "OTHER"), ("venue", "other"), ("market_type", "PERP")):
        changed = type(source).model_validate(source.model_copy(update={field: value}))
        assert flow_aggregate_projection_sha256(changed) != digest
    bound = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=EvidenceLookup(source).source_identity,
        ),
    )
    assert bound.evidence.projection_sha256 == digest
    assert bound.counts.n_buy is None and bound.counts.n_sell == 0
    assert bind_flow_aggregate_evidence(bound, bound.evidence) == bound


def test_invalid_or_stale_projection_refuses_before_evidence_lookup() -> None:
    source = projection()
    bound = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=EvidenceLookup(source).source_identity,
        ),
    )
    lookup = EvidenceLookup(source)
    changed = bound.model_copy(update={"venue": "different"})
    with pytest.raises(ValueError, match="does not match"):
        flow_aggregate_evidence_wire(changed, lookup)
    assert lookup.calls == 0
    with pytest.raises(ValueError, match="does not match"):
        bind_flow_aggregate_evidence(changed, changed.evidence)
    with pytest.raises(ValidationError, match="boolean"):
        flow_aggregate_projection_sha256(source.model_copy(update={"complete": 1}))

    class ExtendedProjection(ConditionFlowAggregateProjection):
        extra: str

    with pytest.raises(TypeError, match="declared flow aggregate"):
        flow_aggregate_projection_sha256(
            ExtendedProjection(
                token_id=source.token_id,
                venue=source.venue,
                market_type=source.market_type,
                instrument_id=source.instrument_id,
                start=source.start,
                end=source.end,
                complete=source.complete,
                buy_qty=source.buy_qty,
                sell_qty=source.sell_qty,
                sum_sq_qty=source.sum_sq_qty,
                buy_notional=source.buy_notional,
                sell_notional=source.sell_notional,
                notional_reason=source.notional_reason,
                low=source.low,
                low_reason=source.low_reason,
                counts=source.counts,
                extra="not admitted",
            )
        )


def test_proof_serialization_preserves_effective_wire_and_does_not_mutate_evidence() -> None:
    source = projection()
    bound = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=EvidenceLookup(source).source_identity,
        ),
    )
    validate_flow_aggregate_evidence(bound)
    lookup = EvidenceLookup(source)
    wire = flow_aggregate_evidence_wire(bound, lookup)
    assert lookup.calls == 1 and wire == lookup.wire
    wire["provider_note"] = {"unchanged": False}
    assert lookup.wire["provider_note"] == {"unchanged": True}


def test_capture_geometry_binds_actual_scope_without_dummy_financial_totals() -> None:
    from tank.analysis.condition_graph_codec import (
        bind_flow_capture_evidence,
        flow_capture_projection_sha256,
        validate_flow_capture_evidence,
    )
    from tank.analysis.condition_graph_contracts import ConditionFlowCaptureProjection, FlowCaptureEvidenceRef

    source = projection()
    geometry = ConditionFlowCaptureProjection(
        token_id=source.token_id,
        venue=source.venue,
        market_type=source.market_type,
        instrument_id=source.instrument_id,
        start=source.start,
        end=source.end,
        complete=True,
    )
    digest = flow_capture_projection_sha256(geometry)
    assert digest != flow_aggregate_projection_sha256(source)
    reference = FlowCaptureEvidenceRef(
        projection_sha256=digest, source_evidence_sha256=EvidenceLookup(source).source_identity
    )
    witness = bind_flow_capture_evidence(geometry, reference)
    validate_flow_capture_evidence(witness)
    assert witness.start == geometry.start and witness.complete is True
    with pytest.raises(ValueError, match="does not match"):
        bind_flow_capture_evidence(
            witness.model_copy(update={"end": witness.end + timedelta(seconds=1)}), reference
        )

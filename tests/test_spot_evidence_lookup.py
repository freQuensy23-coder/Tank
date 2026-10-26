from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_graph_codec import flow_aggregate_evidence_wire
from tank.analysis.condition_graph_contracts import FlowAggregateEvidenceRef
from tank.analysis.condition_inputs import SpotCalculationPolicy
from tank.analysis.spot import aggregate_projection, bound_aggregate
from tank.analysis.spot_evidence_lookup import SpotEvidenceLookup
from tank.time_precision import collection_clock
from tank.util import JsonInput

AT = datetime(2026, 10, 19, 12, tzinfo=UTC)


def reader(row: dict[str, JsonInput], policy: SpotCalculationPolicy) -> SpotEvidenceLookup:
    return SpotEvidenceLookup(
        token_id="T",
        start=AT,
        cutoff=AT + timedelta(minutes=1),
        assembled_at=collection_clock(AT + timedelta(minutes=2)),
        instruments=[{"instrument_id": "A", "token_id": "T", "venue": "V", "market_type": "SPOT"}],
        flow_bars=[row],
        flow_parent_bars=[],
        flow_segments=[],
        tickers=[],
        policy=policy,
        native_ticker_volume_field="volume_native",
    )


def minute() -> dict[str, JsonInput]:
    return {
        "instrument_id": "A",
        "minute_start": "2026-10-19T12:00:00Z",
        "complete": True,
        "buy_qty": "3",
        "sell_qty": "1",
        "sum_sq_qty": "4",
        "received_ts": "2026-10-19T12:01:02Z",
        "writer_note": {"captured": "unchanged"},
    }


def test_actual_serializer_uses_indexed_immutable_original_evidence() -> None:
    original = minute()
    lookup = reader(original, SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=1))
    point = lookup.source.minutes[0]
    aggregate = bound_aggregate(point)
    original["buy_qty"] = "999"
    assert point.buy_qty == Decimal(3)
    assert lookup.source_reference(point.evidence.source_evidence_sha256) == point.evidence
    first = flow_aggregate_evidence_wire(aggregate, lookup)
    assert first["buy_qty"] == "3"
    assert first["writer_note"] == {"captured": "unchanged"}
    first["buy_qty"] = "888"
    assert flow_aggregate_evidence_wire(aggregate, lookup)["buy_qty"] == "3"
    assert lookup.snapshot.original(point.evidence) == minute()


def test_lookup_refuses_cross_source_and_cross_financial_projection() -> None:
    lookup = reader(minute(), SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=1))
    aggregate = bound_aggregate(lookup.source.minutes[0])
    projection = aggregate_projection(lookup.source.minutes[0])
    foreign = FlowAggregateEvidenceRef(
        projection_sha256=aggregate.evidence.projection_sha256,
        source_evidence_sha256="f" * 64,
    )
    with pytest.raises(ValueError, match="not captured"):
        lookup.resolve_flow_aggregate(foreign, expected=projection)
    with pytest.raises(ValueError, match="Expected flow projection"):
        lookup.resolve_flow_aggregate(
            aggregate.evidence, expected=projection.model_copy(update={"token_id": "OTHER"})
        )


def test_raw_read_factory_rejects_invalid_copied_policy_before_owning_facts() -> None:
    policy = SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=1)
    with pytest.raises(ValidationError):
        reader(minute(), policy.model_copy(update={"min_spot_venues": True}))


def test_lookup_retains_source_receipt_and_requires_fresh_request_assembly() -> None:
    lookup = reader(minute(), SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=1))
    early = lookup.inputs_at(collection_clock(AT + timedelta(seconds=61)))
    later = lookup.inputs_at(collection_clock(AT + timedelta(seconds=120)))
    assert early.minutes[0].visible_at(early.assembled_at) is False
    assert later.minutes[0].visible_at(later.assembled_at) is True
    assert early.minutes[0].received_at == later.minutes[0].received_at
    assert early.minutes[0].evidence == later.minutes[0].evidence
    assert "assembled_at" not in lookup.source.model_dump()

"""Typed liquidity output keeps unavailable predicates and actual measurement units."""

from datetime import timedelta

import pytest
from test_condition_graph_codec import condition_wire_clocks
from test_condition_graph_contracts import (
    AT,
    enriched_liquidity,
    fragmentation_pair_history,
    single_venue_liquidity,
)

from tank.analysis.condition_graph_codec import (
    encode_condition_fragmentation,
    encode_condition_liquidity,
    encode_condition_pre_depth_window,
)
from tank.analysis.condition_graph_contracts import ConditionAnalysisWindowStart, ConditionPreDepthWindow


def test_basic_liquidity_wire_does_not_promote_unknown_vacuum_to_normal() -> None:
    source = single_venue_liquidity()
    wire = encode_condition_liquidity(source, clocks=condition_wire_clocks())
    assert wire["aggregate_depth_p1_usd"] == "200"
    assert wire["aggregate_bid_depth_p1_usd"] == "100"
    assert wire["thin"] is False and wire["vacuum"] is None
    assert wire["liquidity_state"] is None
    assert wire["pre_event_depth_window"] == {"start": None, "cutoff": None, "reason": "NO_EVENT_ANCHOR"}
    assert wire["spread_ratio"] is None and wire["spread_ratio_reason"] == "NO_EVENT_ANCHOR"
    assert wire["vol_compression"] == {"status": "NOT_REQUESTED"}
    assert wire["fragmentation"] == {"status": "NOT_REQUESTED"}
    per = wire["per_venue"]
    assert isinstance(per, list) and len(per) == 1 and isinstance(per[0], dict)
    assert per[0]["observed_at"] == "2026-10-17T12:00:00Z"
    assert per[0]["observed_at_prec"] is None
    assert per[0]["observed_at_prec_reason"] == "PRECISION_NOT_CAPTURED"
    assert per[0]["range_covered_pct"] == {"bid": 1.0, "ask": 1.0}
    assert wire["cutoff_prec"] == "COLLECTION_ONLY"
    assert "assembled_at" not in wire


def test_liquidity_wire_preserves_zero_observed_bursts_without_inventing_recovery() -> None:
    wire = encode_condition_liquidity(single_venue_liquidity(), clocks=condition_wire_clocks())
    refill = wire["replenishment"]
    assert isinstance(refill, dict)
    assert refill["available"] is True and refill["bursts"] == 0
    assert refill["replenished"] == 0 and refill["median_recovery_s"] is None
    summary = refill["summary"]
    assert isinstance(summary, dict) and summary["median_recovery_reason"] == "NO_OBSERVED_RECOVERY"
    elasticity = wire["elasticity"]
    assert isinstance(elasticity, dict)
    assert elasticity["net_flow"] == "10" and elasticity["depth_unit"] == "NATIVE"
    assert elasticity["hit_side"] == "ask"
    assert elasticity["bps_per_depth_unit"] == pytest.approx(100)
    assert elasticity["hit_side_depth_lower_bound"] == "100"
    assert elasticity["hit_side_covered_venues"] == ["a"]
    assert elasticity["hit_side_coverage_complete"] is True
    assert elasticity["hit_side_depth_p1_usd"] == "100"
    assert elasticity["pre_event_hit_side_depth_p1_usd"] is None
    assert elasticity["avg_hit_side_depth_p1_usd"] == "100"


def test_enriched_liquidity_wire_retains_unavailable_fragmentation_and_executed_split() -> None:
    wire = encode_condition_liquidity(enriched_liquidity(), clocks=condition_wire_clocks())
    assert wire["enrichment_phase"] == "ENRICHED"
    split, best = wire["split_slippage_bps"], wire["best_single_venue_slippage_bps"]
    assert isinstance(split, dict) and split["T1"] == pytest.approx(100)
    assert isinstance(best, dict) and best["T1"] is None
    frag = wire["fragmentation"]
    assert isinstance(frag, dict)
    assert frag["persistent_gap"] is None and frag["validated"] is None
    assert frag["venues"] == [] and frag["complete_venues"] == []
    assert frag["quote_conversion_uncertainty_bps"] is None
    assert frag["quote_conversion_uncertainty_reason"] == "NO_EVALUATED_PAIRS"
    proofs = frag["predicate_proofs"]
    assert isinstance(proofs, dict)
    gap = proofs["persistent_gap"]
    assert isinstance(gap, dict) and gap["status"] == "UNAVAILABLE"
    volatility = wire["vol_compression"]
    assert isinstance(volatility, dict) and volatility["rv24"] is None
    assert volatility["reference_venue"] is None
    assert volatility["classification"] == {"status": "UNAVAILABLE", "value": None, "reason": "INCOMPLETE"}
    assert wire["assembled_at"] == "2026-10-17T12:00:00Z"


def test_liquidity_wire_refuses_borrowed_subject_clock_and_keeps_source_independent() -> None:
    source = single_venue_liquidity()
    clocks = condition_wire_clocks()
    wrong = type(clocks).model_validate(clocks.model_copy(update={"token_id": "OTHER"}))
    with pytest.raises(ValueError, match="another token"):
        encode_condition_liquidity(source, clocks=wrong)
    wire = encode_condition_liquidity(source, clocks=clocks)
    per = wire["per_venue"]
    assert isinstance(per, list) and isinstance(per[0], dict)
    per[0]["spread_bps"] = 999.0
    assert source.per_venue[0].spread_bps == 1.0
    fresh = encode_condition_liquidity(source, clocks=clocks)["per_venue"]
    assert isinstance(fresh, list) and isinstance(fresh[0], dict) and fresh[0]["spread_bps"] == 1.0


def test_pre_depth_math_window_preserves_derived_clock_precision_and_refuses_another_start() -> None:
    source = single_venue_liquidity()
    end = source.interval.start
    window = ConditionPreDepthWindow(
        origin=ConditionAnalysisWindowStart(kind="ANALYSIS_WINDOW_START", at=end),
        start=end - timedelta(hours=1),
        cutoff=end,
    )
    wire = encode_condition_pre_depth_window(window, interval=source.interval, clocks=condition_wire_clocks())
    assert wire["anchor_type"] == "ANALYSIS_WINDOW_START"
    assert wire["cutoff"] == "2026-10-17T11:00:00Z"
    assert wire["start"] == "2026-10-17T10:00:00Z"
    assert wire["cutoff_prec"] == "COLLECTION_ONLY"
    wrong = ConditionPreDepthWindow(
        origin=ConditionAnalysisWindowStart(kind="ANALYSIS_WINDOW_START", at=AT),
        start=AT - timedelta(hours=1),
        cutoff=AT,
    )
    with pytest.raises(ValueError, match="actual analysis window start"):
        encode_condition_pre_depth_window(wrong, interval=source.interval, clocks=condition_wire_clocks())


def test_pre_depth_event_anchor_keeps_unknown_precision_and_refuses_missing_captured_clock() -> None:
    from tank.analysis.condition_graph_contracts import ConditionDeclaredEventAnchor
    from tank.time_precision import Timestamp

    source = single_venue_liquidity()
    stamp = Timestamp(value="2026-10-17T11:00:00Z", precision=None, precision_reason="NOT_CAPTURED")
    window = ConditionPreDepthWindow(
        origin=ConditionDeclaredEventAnchor(kind="EVENT", source="T0", at=stamp),
        start=AT - timedelta(hours=2),
        cutoff=AT - timedelta(hours=1),
    )
    with pytest.raises(ValueError, match="actual captured event anchor"):
        encode_condition_pre_depth_window(window, interval=source.interval, clocks=condition_wire_clocks())
    context = condition_wire_clocks()
    context = type(context).model_validate(context.model_copy(update={"event_anchor": stamp}))
    wire = encode_condition_pre_depth_window(window, interval=source.interval, clocks=context)
    assert wire["anchor_type"] == "EVENT" and wire["cutoff"] == stamp.value
    assert wire["cutoff_prec"] is None and wire["cutoff_prec_reason"] == "NOT_CAPTURED"


def test_known_zero_rolling_volatility_keeps_its_actual_completed_hour_separate_from_cutoff() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionVolatilityCompression,
        ObservedConditionVolatility,
    )
    from tank.analysis.statistics_contracts import FloatBounds, FloatRange
    from tank.time_precision import Timestamp

    graph = enriched_liquidity()
    measurement = ConditionVolatilityCompression(
        token_id="T",
        reference_venue="a",
        cutoff=AT,
        window="W24H",
        realized=ObservedConditionVolatility(
            status="OBSERVED",
            value=0.0,
            completed_hour=Timestamp(value="2026-10-17T11:00:00Z", precision="EXACT"),
            return_count=24,
            resolution_s=3600,
        ),
        normal_range=FloatRange(bounds=FloatBounds(lower=0.1, upper=0.2), reason=None),
    )
    graph = type(graph).model_validate(graph.model_copy(update={"vol_compression": measurement}))
    wire = encode_condition_liquidity(graph, clocks=condition_wire_clocks())
    volatility = wire["vol_compression"]
    assert isinstance(volatility, dict)
    assert volatility["rv24"] == 0.0 and volatility["state"] == "COMPRESSED"
    assert volatility["rv24_observed_at"] == "2026-10-17T11:00:00Z"
    assert wire["cutoff"] == "2026-10-17T12:00:00Z"
    best = wire["best_single_venue_slippage_proofs"]
    assert isinstance(best, dict) and best["T1"] == {
        "status": "UNAVAILABLE",
        "value": None,
        "reason": "INCOMPLETE",
    }


def test_fragmentation_wire_keeps_measured_persistent_pairs_distinct_from_validated_capture() -> None:
    original = enriched_liquidity().fragmentation
    graph = type(original).model_validate(
        original.model_copy(update={"price_pairs": (fragmentation_pair_history(),)})
    )
    wire = encode_condition_fragmentation(graph)
    assert wire["persistent_gap"] is True and wire["gap_closed_quickly"] is True
    assert wire["venues"] == [["a", "b"]]
    assert wire["validated"] is None and wire["complete_venues"] == []
    assert wire["quote_conversion_uncertainty_bps"] == "2"
    pairs = wire["pair_observations"]
    assert isinstance(pairs, list) and len(pairs) == 1 and isinstance(pairs[0], dict)
    series = pairs[0]["gap_series"]
    assert isinstance(series, list) and len(series) == 91


def test_known_zero_net_flow_has_no_selected_side_or_fabricated_side_coverage() -> None:
    from decimal import Decimal

    from tank.analysis.condition_graph_contracts import ConditionLiquidityNetFlow

    source = single_venue_liquidity()
    zero = ConditionLiquidityNetFlow(native=Decimal(0), native_reason=None, usd=Decimal(0), usd_reason=None)
    inputs = type(source.elasticity).model_validate(source.elasticity.model_copy(update={"flow": zero}))
    source = type(source).model_validate(source.model_copy(update={"elasticity": inputs}))
    wire = encode_condition_liquidity(source, clocks=condition_wire_clocks())
    elasticity = wire["elasticity"]
    assert isinstance(elasticity, dict)
    assert elasticity["net_flow"] == "0" and elasticity["hit_side"] is None
    assert elasticity["hit_side_reason"] == "ZERO_NET_FLOW"
    assert elasticity["hit_side_coverage_complete"] is None
    assert elasticity["hit_side_depth_lower_bound"] is None

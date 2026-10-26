from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tank.analysis.condition_graph_codec import encode_scoped_condition_spot
from tank.analysis.condition_graph_contracts import (
    ConditionCohortComparison,
    ConditionSpotWindowGraph,
    ConditionWindow,
    ObservedConditionCohort,
    UnavailableConditionCohort,
)
from tank.analysis.condition_input_codec import DecodedSpotWindows, decode_spot_windows
from tank.analysis.condition_inputs import SpotCalculationPolicy
from tank.analysis.spot import calculate_spot_window_graph
from tank.time_precision import Timestamp, chosen_boundary
from tank.util import JsonInput, iso

END = datetime(2026, 10, 19, 12, tzinfo=UTC)


def source_rows() -> tuple[dict[str, JsonInput], list[dict[str, JsonInput]]]:
    instrument: dict[str, JsonInput] = {
        "instrument_id": "fixture-spot",
        "token_id": "T",
        "venue": "VENUE",
        "market_type": "SPOT",
    }
    rows: list[dict[str, JsonInput]] = [
        {
            "instrument_id": "fixture-spot",
            "minute_start": iso(END - timedelta(minutes=number)),
            "minute_start_prec": "BUCKET(60s)",
            "minute_start_prec_reason": None,
            "complete": True,
            "buy_qty": "3",
            "sell_qty": "1",
            "sum_sq_qty": "4",
            "received_ts": iso(END + timedelta(seconds=1)),
            "received_ts_prec": "COLLECTION_ONLY",
            "received_ts_prec_reason": None,
        }
        for number in range(1, 2881)
    ]
    return instrument, rows


def decode_windows(
    instruments: list[dict[str, JsonInput]], rows: list[dict[str, JsonInput]], anchor: Timestamp
) -> DecodedSpotWindows:
    return decode_spot_windows(
        token_id="T",
        cutoff=chosen_boundary(END, precision="EXACT"),
        event_anchor=anchor,
        assembled_at=Timestamp(iso(END + timedelta(seconds=2)), "COLLECTION_ONLY"),
        instruments=instruments,
        flow_bars=rows,
        flow_parent_bars=[],
        flow_segments=[],
        tickers=[],
        policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=1),
        native_ticker_volume_field="volume_native",
    )


def test_window_graph_preserves_actual_native_values_prior_cohort_and_endpoint_metadata() -> None:
    instrument, rows = source_rows()
    decoded = decode_windows(
        [instrument], rows, Timestamp(iso(END - timedelta(minutes=30)), None, "SOURCE_PRECISION_NOT_CAPTURED")
    )
    result = calculate_spot_window_graph(decoded.inputs)
    assert result.primary == "EVENT"
    assert result.spots["W15M"].venues[0].volume_native == Decimal(60)
    assert result.spots["W24H"].venues[0].volume_native == Decimal(5760)
    assert result.spots["EVENT"].venues[0].cvd == Decimal(60)
    cohort = result.spots["EVENT"].composite_cohort
    assert isinstance(cohort, ObservedConditionCohort)
    assert cohort.velocity == 1.0 and cohort.velocity_reason is None
    assert cohort.share_of_max == 1.0 and cohort.share_majority is True
    assert result.spots["EVENT"].coverage_through.precision == "BUCKET(60s)"
    wire = encode_scoped_condition_spot(result.spots["EVENT"], decoded.source_evidence, clocks=decoded.clocks)
    assert wire["start_prec"] is None
    assert wire["start_prec_reason"] == "SOURCE_PRECISION_NOT_CAPTURED"
    assert wire["observed_at_prec"] == "BUCKET(60s)"
    assert wire["start_origin"] == "CAPTURED_EVENT_ANCHOR"


def test_missing_event_anchor_keeps_analysis_scope_and_unavailable_coverage_explicit() -> None:
    instrument, _rows = source_rows()
    decoded = decode_windows([instrument], [], Timestamp(None, None, "EVENT_ANCHOR_NOT_CAPTURED"))
    result = calculate_spot_window_graph(decoded.inputs)
    assert result.primary == "W4H"
    assert result.spots["EVENT"].interval.start == result.spots["W4H"].interval.start
    assert decoded.clocks.event_anchor.value is None
    assert all(
        spot.direction is None and spot.coverage_through.value is None for spot in result.spots.values()
    )
    assert all(
        isinstance(spot.composite_cohort, UnavailableConditionCohort) for spot in result.spots.values()
    )


def test_known_zero_prior_volume_is_not_a_missing_observation_or_a_velocity_zero() -> None:
    instrument, rows = source_rows()
    for row in rows[15:30]:
        row.update(buy_qty="0", sell_qty="0", sum_sq_qty="0")
    decoded = decode_windows([instrument], rows, Timestamp(None, None, "EVENT_ANCHOR_NOT_CAPTURED"))
    result = calculate_spot_window_graph(decoded.inputs)
    cohort = result.spots["W15M"].composite_cohort
    assert isinstance(cohort, ObservedConditionCohort)
    assert cohort.volume == Decimal(60)
    assert cohort.velocity is None and cohort.velocity_reason == "ZERO_BASELINE"


def test_missing_prior_capture_does_not_erase_known_current_native_flow() -> None:
    instrument, rows = source_rows()
    del rows[1500]
    decoded = decode_windows([instrument], rows, Timestamp(None, None, "EVENT_ANCHOR_NOT_CAPTURED"))
    result = calculate_spot_window_graph(decoded.inputs)
    spot = result.spots["W24H"]
    cohort = spot.composite_cohort
    assert spot.venues[0].volume_native == Decimal(5760)
    assert isinstance(cohort, ObservedConditionCohort)
    assert cohort.velocity is None and cohort.velocity_reason == "INCOMPLETE"


def test_absent_endpoint_precision_is_not_invented_from_the_window_or_minute_spacing() -> None:
    instrument, rows = source_rows()
    del rows[0]["minute_start_prec"]
    del rows[0]["minute_start_prec_reason"]
    decoded = decode_windows([instrument], rows, Timestamp(None, None, "EVENT_ANCHOR_NOT_CAPTURED"))
    result = calculate_spot_window_graph(decoded.inputs)
    for spot in result.spots.values():
        assert spot.coverage_through.value == iso(END)
        assert spot.coverage_through.precision is None
        assert spot.coverage_through.precision_reason is not None


def test_known_zero_current_volume_has_no_arbitrary_common_cohort_majority() -> None:
    instrument, rows = source_rows()
    for row in rows:
        row.update(buy_qty="0", sell_qty="0", sum_sq_qty="0")
    decoded = decode_windows([instrument], rows, Timestamp(None, None, "EVENT_ANCHOR_NOT_CAPTURED"))
    result = calculate_spot_window_graph(decoded.inputs)
    spot = result.spots["W24H"]
    assert spot.direction == "NEUTRAL" and spot.venues[0].volume_native == Decimal(0)
    assert isinstance(spot.composite_cohort, UnavailableConditionCohort)
    assert spot.topn.n == 0 and spot.topn.agreement is None


def _comparison_graph(
    *, zero: bool = False, empty: bool = False
) -> tuple[ConditionSpotWindowGraph, dict[ConditionWindow, dict[ConditionWindow, ConditionCohortComparison]]]:
    from tank.analysis.spot import compare_spot_windows

    instruments = []
    rows = []
    for venue in ("A", "B"):
        _instrument, source = source_rows()
        instruments.append({"instrument_id": venue, "venue": venue, "token_id": "T", "market_type": "SPOT"})
        for row in source:
            if venue == "B" and row["minute_start"] == iso(END - timedelta(minutes=61)) and not zero:
                continue
            rows.append(
                dict(
                    row,
                    instrument_id=venue,
                    **({"buy_qty": "0", "sell_qty": "0", "sum_sq_qty": "0"} if zero else {}),
                )
            )
    policy = SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2)
    decoded = decode_spot_windows(
        token_id="T",
        cutoff=chosen_boundary(END, precision="EXACT"),
        event_anchor=chosen_boundary(END - timedelta(minutes=30), precision="EXACT"),
        assembled_at=Timestamp(iso(END + timedelta(seconds=2)), "COLLECTION_ONLY"),
        instruments=instruments,
        flow_bars=[] if empty else rows,
        flow_parent_bars=[],
        flow_segments=[],
        tickers=[],
        policy=policy,
        native_ticker_volume_field="volume_native",
    )
    graph = calculate_spot_window_graph(decoded.inputs)
    before = graph.model_dump_json()
    comparisons = compare_spot_windows(graph, decoded.inputs.source.policy)
    assert graph.model_dump_json() == before
    return graph, comparisons


def test_pairwise_cohort_preserves_complete_shared_native_scope_without_global_padding() -> None:
    from tank.analysis.condition_graph_contracts import (
        ObservedConditionCohortComparison,
        UnavailableConditionCohortComparison,
    )

    graph, comparisons = _comparison_graph()
    assert isinstance(graph.spots["EVENT"].composite_cohort, UnavailableConditionCohort)
    event_hour = comparisons["EVENT"]["W1H"]
    assert isinstance(event_hour, ObservedConditionCohortComparison)
    assert event_hour.cohort == ("A", "B") and event_hour.share_of_max == 1
    assert event_hour.current.volume == Decimal(240) and event_hour.current.cvd == Decimal(120)
    assert event_hour.other.volume == Decimal(480) and event_hour.other.cvd == Decimal(240)
    assert event_hour.current.sign == event_hour.other.sign == 1
    event_four = comparisons["EVENT"]["W4H"]
    assert isinstance(event_four, UnavailableConditionCohortComparison)
    assert event_four.cohort == ("A",) and event_four.share_of_max == 0.5
    assert event_four.share_majority is False
    reverse = comparisons["W4H"]["EVENT"]
    assert isinstance(reverse, UnavailableConditionCohortComparison)
    assert reverse.share_of_max == 1 and reverse.share_majority is True
    assert all(set(index) == set(graph.spots) - {window} for window, index in comparisons.items())
    from tank.analysis.spot import enrich_spot_cohorts

    enriched = enrich_spot_cohorts(
        graph, SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2)
    )
    assert enriched.spots["EVENT"].kind == "COHORT_ENRICHED"
    assert enriched.spots["EVENT"].size_profile.status == "NOT_REQUESTED"
    assert enriched.spots["EVENT"].cohort_comparisons == comparisons["EVENT"]
    assert enriched.spots["EVENT"].cohort_consistent["W1H"] is True
    assert enriched.spots["EVENT"].cohort_consistent["W4H"] is False


def test_pairwise_observed_zero_is_distinct_from_unavailable_empty_cohort() -> None:
    from tank.analysis.condition_graph_contracts import ObservedConditionComposite

    known, known_comparisons = _comparison_graph(zero=True)
    absent, absent_comparisons = _comparison_graph(empty=True)
    known_total = known.spots["EVENT"].composite_max
    assert isinstance(known_total, ObservedConditionComposite) and known_total.volume == Decimal(0)
    assert absent.spots["EVENT"].composite_max.volume is None
    assert known_comparisons["EVENT"]["W1H"].cohort == ("A", "B")
    assert absent_comparisons["EVENT"]["W1H"].cohort == ()
    assert known_comparisons["EVENT"]["W1H"].share_of_max is None
    assert known_comparisons["EVENT"]["W1H"].share_majority is None


def test_native_composite_preserves_exact_extremes_under_small_outer_decimal_context() -> None:
    from decimal import localcontext

    from tank.analysis.spot import NativeFlowTotals, combine_native_flow, compose_native_flow

    large = NativeFlowTotals(
        token_id="T",
        buy_qty=Decimal("1E150"),
        sell_qty=Decimal(0),
        volume_native=Decimal("1E150"),
        cvd=Decimal("1E150"),
        sum_sq_qty=Decimal("1E300"),
    )
    small = NativeFlowTotals(
        token_id="T",
        buy_qty=Decimal("1E-40"),
        sell_qty=Decimal(0),
        volume_native=Decimal("1E-40"),
        cvd=Decimal("1E-40"),
        sum_sq_qty=Decimal("1E-80"),
    )
    quantity = Decimal("1" + "0" * 150 + "." + "0" * 39 + "1")
    squared = Decimal("1" + "0" * 300 + "." + "0" * 79 + "1")
    with localcontext() as outer:
        outer.prec = 6
        result = combine_native_flow("T", (large, small))
        composite = compose_native_flow(
            result, ("A", "B"), SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2)
        )
        assert result.volume_native == result.cvd == quantity
        assert result.sum_sq_qty == squared
        assert composite.volume == quantity and composite.sign == 0
        assert outer.prec == 6


def test_native_composite_refuses_mixed_subjects_and_duplicated_venue_evidence() -> None:
    from tank.analysis.spot import NativeFlowTotals, combine_native_flow, compose_native_flow

    source = NativeFlowTotals(
        token_id="T",
        buy_qty=Decimal(1),
        sell_qty=Decimal(0),
        volume_native=Decimal(1),
        cvd=Decimal(1),
        sum_sq_qty=Decimal(1),
    )
    other = NativeFlowTotals(
        token_id="OTHER",
        buy_qty=Decimal(1),
        sell_qty=Decimal(0),
        volume_native=Decimal(1),
        cvd=Decimal(1),
        sum_sq_qty=Decimal(1),
    )
    with pytest.raises(ValueError, match="different token"):
        combine_native_flow("T", (source, other))
    with pytest.raises(ValueError, match="distinct observed venue scope"):
        compose_native_flow(
            source, ("A", "A"), SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2)
        )

"""Independent exact native-flow and significance boundaries for the typed producer."""

from datetime import timedelta
from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError
from test_condition_input_codec import decoded, minute

from tank.analysis.condition_graph_codec import flow_aggregate_evidence_wire
from tank.analysis.condition_graph_contracts import ObservedConditionComposite, UnavailableConditionComposite
from tank.analysis.condition_input_codec import DecodedSpotInputs, decode_spot_inputs
from tank.analysis.condition_inputs import SpotCalculationPolicy
from tank.analysis.spot import (
    MissingSpotFlowMetrics,
    NativeFlowTotals,
    calculate_spot,
    significant_flow,
    summarize_native_flow,
)
from tank.time_precision import Timestamp
from tank.util import JsonInput, parse_iso_time


@pytest.mark.parametrize(
    "buy,sell,squared,expected",
    [
        ("5", "3", "1", 1),
        ("3", "5", "1", -1),
        ("4", "4", "1", 0),
        ("5", "3", "1.0000000000000000000000000000000000001", 0),
        ("0", "0", "0", 0),
    ],
)
def test_significance_preserves_exact_equality_and_known_neutral(
    buy: str, sell: str, squared: str, expected: int
) -> None:
    inputs = decoded([dict(minute(), buy_qty=buy, sell_qty=sell, sum_sq_qty=squared)]).inputs
    totals = summarize_native_flow("T", inputs.minutes)
    assert isinstance(totals, NativeFlowTotals)
    with localcontext() as outer:
        outer.prec = 6
        assert significant_flow(totals, inputs.policy) == expected
        assert outer.prec == 6


def test_wide_native_difference_is_not_rounded_by_the_callers_decimal_context() -> None:
    inputs = decoded(
        [
            dict(
                minute(),
                buy_qty="1000000000000000000000000000001.0000000000000000000000000000001",
                sell_qty="1000000000000000000000000000000",
                sum_sq_qty="0.25",
            )
        ]
    ).inputs
    with localcontext() as outer:
        outer.prec = 6
        totals = summarize_native_flow("T", inputs.minutes)
        assert isinstance(totals, NativeFlowTotals)
        assert totals.cvd == Decimal("1.0000000000000000000000000000001")
        assert totals.volume_native == Decimal(
            "2000000000000000000000000000001.0000000000000000000000000000001"
        )
        assert significant_flow(totals, inputs.policy) == 1
        assert outer.prec == 6


def test_missing_native_metric_and_no_observations_never_become_observed_zero() -> None:
    inputs = decoded([dict(minute(), buy_qty=None)]).inputs
    absent = summarize_native_flow("T", inputs.minutes)
    assert isinstance(absent, MissingSpotFlowMetrics)
    assert absent.missing_metrics == ("buy_qty",)
    empty = summarize_native_flow("T", ())
    assert isinstance(empty, MissingSpotFlowMetrics)
    assert empty.missing_metrics == ("buy_qty", "sell_qty", "sum_sq_qty")


def test_actual_declared_sigma_changes_the_calculation_without_a_default() -> None:
    inputs = decoded([dict(minute(), buy_qty="5", sell_qty="3", sum_sq_qty="1")]).inputs
    totals = summarize_native_flow("T", inputs.minutes)
    assert isinstance(totals, NativeFlowTotals)
    assert significant_flow(totals, inputs.policy) == 1
    higher = SpotCalculationPolicy(significant_flow_sigma=Decimal(3), min_spot_venues=2)
    assert significant_flow(totals, higher) == 0
    with pytest.raises(ValidationError):
        significant_flow(totals, higher.model_copy(update={"min_spot_venues": 2.0}))


def test_native_amounts_cannot_silently_change_token_units() -> None:
    inputs = decoded([minute()]).inputs
    with pytest.raises(ValueError, match="different token quantities"):
        summarize_native_flow("OTHER", inputs.minutes)


def instrument(venue: str, identity: str) -> dict[str, JsonInput]:
    return {"token_id": "T", "instrument_id": identity, "venue": venue, "market_type": "SPOT"}


def flow(
    identity: str, *, at: str = "2026-10-19T12:00:00Z", buy: str = "9", sell: str = "1", squared: str = "10"
) -> dict[str, JsonInput]:
    return {
        "instrument_id": identity,
        "minute_start": at,
        "buy_qty": buy,
        "sell_qty": sell,
        "sum_sq_qty": squared,
        "complete": True,
        "received_ts": "2026-10-19T12:03:00Z",
        "received_ts_prec": "COLLECTION_ONLY",
        "received_ts_prec_reason": None,
    }


def case(
    identities: list[dict[str, JsonInput]],
    bars: list[dict[str, JsonInput]],
    *,
    start: str = "2026-10-19T12:00:00Z",
    cutoff: str = "2026-10-19T12:01:00Z",
    pieces: list[dict[str, JsonInput]] | None = None,
    tickers: list[dict[str, JsonInput]] | None = None,
) -> DecodedSpotInputs:
    return decode_spot_inputs(
        token_id="T",
        start=parse_iso_time(start),
        cutoff=parse_iso_time(cutoff),
        assembled_at=Timestamp("2026-10-19T12:04:00Z", "COLLECTION_ONLY"),
        instruments=identities,
        flow_parent_bars=[],
        flow_bars=bars,
        flow_segments=[] if pieces is None else pieces,
        tickers=[] if tickers is None else tickers,
        policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2),
        native_ticker_volume_field="volume_native",
    )


def test_actual_scoped_native_rows_produce_broad_spot_without_dictionary_calculations() -> None:
    source = case([instrument("A", "A:T"), instrument("B", "B:T")], [flow("A:T"), flow("B:T")])
    result = calculate_spot(source.inputs)
    assert result.token_id == "T" and result.classification == "BROAD" and result.direction == "BUY"
    assert isinstance(result.composite_max, ObservedConditionComposite)
    assert result.composite_max.volume == Decimal(20) and result.composite_max.cvd == Decimal(16)
    assert result.coverage.n_complete == 2 and result.coverage.exact_majority is True
    assert result.exact_majority.buy is True and result.cw_breadth.buy == 1
    assert result.topn.n == 2 and result.concentration.hhi == 0.5


def test_no_complete_rows_are_unavailable_but_real_zero_rows_are_observed_neutral() -> None:
    identities = [instrument("A", "A:T"), instrument("B", "B:T")]
    empty = calculate_spot(case(identities, []).inputs)
    assert isinstance(empty.composite_max, UnavailableConditionComposite)
    assert empty.composite_max.volume is None and empty.composite_max.sign is None
    assert empty.direction is None and empty.coverage.n_complete == 0
    zero = calculate_spot(
        case(
            identities,
            [
                flow("A:T", buy="0", sell="0", squared="0"),
                flow("B:T", buy="0", sell="0", squared="0"),
            ],
        ).inputs
    )
    assert isinstance(zero.composite_max, ObservedConditionComposite)
    assert zero.composite_max.volume == 0 and zero.composite_max.sign == 0
    assert zero.direction == "NEUTRAL" and zero.coverage.n_complete == 2
    assert zero.topn.n == 0 and zero.topn.agreement_reason == "ZERO_TOTAL_VOLUME"
    assert zero.concentration.top_contrib_venue is None


def test_venue_union_of_minutes_cannot_conceal_missing_required_instruments() -> None:
    source = case(
        [instrument("A", "A:one"), instrument("A", "A:two"), instrument("B", "B:T")],
        [
            flow("A:one"),
            flow("A:two", at="2026-10-19T12:01:00Z"),
            flow("B:T"),
            flow("B:T", at="2026-10-19T12:01:00Z"),
        ],
        cutoff="2026-10-19T12:02:00Z",
    )
    result = calculate_spot(source.inputs)
    assert tuple(row.venue for row in result.venues) == ("B",)
    assert tuple(row.venue for row in result.coverage.excluded) == ("A",)
    assert result.classification == "INSUFFICIENT_DATA"


def test_boundary_proof_keeps_unknown_counts_and_resolves_actual_source_fields() -> None:
    pieces: list[dict[str, JsonInput]] = [
        {
            "instrument_id": identity,
            "start": "2026-10-19T12:00:10Z",
            "end": "2026-10-19T12:00:20Z",
            "complete": True,
            "buy_qty": "3",
            "sell_qty": "1",
            "sum_sq_qty": "4",
            "received_ts": "2026-10-19T12:03:00Z",
            "received_ts_prec": "COLLECTION_ONLY",
            "observed_at": "2026-10-19T12:00:21Z",
            "observed_at_prec": "COLLECTION_ONLY",
            "retained_extra": {"encoding": "actual-test-source"},
        }
        for identity in ("A:T", "B:T")
    ]
    source = case(
        [instrument("A", "A:T"), instrument("B", "B:T")],
        [],
        pieces=pieces,
        start="2026-10-19T12:00:10Z",
        cutoff="2026-10-19T12:00:20Z",
    )
    result = calculate_spot(source.inputs)
    assert result.coverage.n_complete == 2
    boundary = result.venues[0].boundary_segments[0]
    assert boundary.trade_count().value is None
    aggregate = boundary.aggregates[0]
    resolved = flow_aggregate_evidence_wire(aggregate, source.source_evidence)
    assert resolved["retained_extra"] == {"encoding": "actual-test-source"}
    assert "n_buy" not in resolved and "n_sell" not in resolved
    assert resolved["token_id"] == "T" and resolved["venue"] == "A"
    assert "token_id" not in source.source_evidence.original(source.inputs.boundaries[0].evidence)
    with pytest.raises(ValueError, match="Actual source financial projection"):
        forged = aggregate.model_copy(update={"buy_qty": Decimal(4)})
        from tank.analysis.condition_graph_codec import flow_aggregate_projection_sha256

        forged = forged.model_copy(
            update={
                "evidence": forged.evidence.model_copy(
                    update={"projection_sha256": flow_aggregate_projection_sha256(forged)}
                )
            }
        )
        flow_aggregate_evidence_wire(forged, source.source_evidence)


def test_excluded_volume_estimate_uses_the_actual_required_instrument_set() -> None:
    identities = [instrument("A", "A:one"), instrument("A", "A:two"), instrument("B", "B:T")]
    tickers: list[dict[str, JsonInput]] = [
        {
            "instrument_id": identity,
            "ts": "2026-10-19T12:00:00Z",
            "volume_native": "86400",
            "received_ts": "2026-10-19T12:03:00Z",
            "received_ts_prec": "COLLECTION_ONLY",
        }
        for identity in ("A:one", "A:two")
    ]
    result = calculate_spot(case(identities, [flow("B:T")], tickers=tickers).inputs)
    assert result.coverage.covered_share_est == pytest.approx(1 / 13)
    assert result.coverage.exact_majority is False


def test_known_receipt_does_not_authorize_a_complete_future_minute() -> None:
    source = case(
        [instrument("A", "A:T")],
        [flow("A:T", at="2026-10-19T12:04:00Z")],
        start="2026-10-19T12:04:00Z",
        cutoff="2026-10-19T12:05:00Z",
    )
    result = calculate_spot(source.inputs)
    assert result.coverage.n_complete == 0 and result.direction is None


def test_parent_bound_piece_identity_is_not_replaced_by_an_equal_flattened_projection() -> None:
    from test_condition_input_codec import AT, generated_empty_bar

    parent = generated_empty_bar()
    embedded = parent["boundary_segments"]
    assert isinstance(embedded, list) and isinstance(embedded[0], dict)
    flat = dict(embedded[0], flattened_marker="separate captured input projection")
    source = decode_spot_inputs(
        token_id="T",
        start=AT,
        cutoff=AT + timedelta(seconds=30),
        assembled_at=Timestamp("2026-10-19T12:00:40Z", "COLLECTION_ONLY"),
        instruments=[
            {"instrument_id": "VENUE:spot:T", "token_id": "T", "venue": "binance", "market_type": "SPOT"}
        ],
        flow_bars=[],
        flow_parent_bars=[parent],
        flow_segments=[flat],
        tickers=[],
        policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2),
        native_ticker_volume_field="volume_native",
    )
    standalone, actual_parent = source.inputs.boundaries
    assert standalone.evidence.source_evidence_sha256 != actual_parent.evidence.source_evidence_sha256
    result = calculate_spot(source.inputs)
    aggregate = result.venues[0].boundary_segments[0].aggregates[0]
    assert aggregate.evidence.source_evidence_sha256 == actual_parent.evidence.source_evidence_sha256
    wire = flow_aggregate_evidence_wire(aggregate, source.source_evidence)
    assert "flattened_marker" not in wire
    assert (
        source.source_evidence.original(standalone.evidence)["flattened_marker"]
        == "separate captured input projection"
    )


def test_direct_window_keeps_actual_native_coverage_without_computing_other_windows() -> None:
    from tank.analysis.spot import calculate_direct_spot

    source = case([instrument("A", "A:T"), instrument("B", "B:T")], [flow("A:T"), flow("B:T")])
    basic = calculate_spot(source.inputs)
    direct = calculate_direct_spot(source.inputs, window="EVENT")
    assert direct.venues == basic.venues
    assert direct.composite_max == basic.composite_max
    assert direct.interval.start == source.inputs.start
    assert direct.interval.cutoff == source.inputs.cutoff
    assert direct.coverage_through.value == "2026-10-19T12:01:00Z"
    assert direct.coverage_through.precision is None
    assert direct.composite_cohort.status == "NOT_REQUESTED"
    assert direct.cohort_comparisons.status == "NOT_REQUESTED"
    assert direct.size_profile.status == "NOT_REQUESTED"


def test_direct_window_missing_receipt_does_not_invent_coverage_at_cutoff() -> None:
    from tank.analysis.spot import calculate_direct_spot

    row = flow("A:T")
    row.pop("received_ts")
    row.pop("received_ts_prec")
    row.pop("received_ts_prec_reason")
    source = case([instrument("A", "A:T")], [row])
    direct = calculate_direct_spot(source.inputs, window="EVENT")
    assert direct.venues == ()
    assert direct.coverage_through.value is None
    assert direct.coverage_through.precision_reason == "NO_COMPLETE_VENUES"

from datetime import timedelta

import pytest

from tank.analysis.condition_derivative_input_codec import decode_oi_inputs, decode_oi_synchronization_rules
from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
    OIRuleReadScope,
    OISelectionPolicy,
)
from tank.analysis.condition_graph_codec import encode_condition_derivatives
from tank.analysis.condition_graph_contracts import (
    ConditionSynchronizedOpenInterest,
    ObservedConditionOpenInterest,
)
from tank.analysis.oi_composite_history import ObservedOICompositeWindow
from tank.analysis.oi_family import calculate_oi_family
from tank.analysis.oi_history_cache import OIHistoryCache
from tank.analysis.oi_synchronization import OISynchronizationPolicy
from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator
from tank.analysis.statistics_contracts import BaselineRetention, FloatBounds, FloatRange
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import JsonInput, iso, parse_iso_time


def composite_source(
    *, extreme: bool = False, delayed: bool = False
) -> tuple[OpenInterestCalculator, OIWindowRequest, CapturedOISynchronizationRules]:
    end = parse_iso_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=1)
    receipt = end + timedelta(seconds=5 if delayed else 1)
    instruments: list[dict[str, JsonInput]] = [
        {"instrument_id": iid, "token_id": "T", "venue": "binance", "market_type": "PERP_LINEAR"}
        for iid in ("A", "B")
    ]
    rows: list[dict[str, JsonInput]] = []
    for hour in range(1, 241):
        rows.append(
            {
                "instrument_id": "A",
                "metric_key": "OI",
                "ts": iso(start - timedelta(hours=hour)),
                "received_ts": iso(receipt),
                "oi_native": ("1E-310" if hour % 2 else "1E310") if extreme else "10",
                "oi_usd": "1",
            }
        )
        if hour <= 3:
            rows.append(
                {
                    "instrument_id": "B",
                    "metric_key": "OI",
                    "ts": iso(start - timedelta(hours=hour)),
                    "received_ts": iso(end + timedelta(seconds=1)),
                    "oi_native": "20",
                    "oi_usd": None,
                }
            )
    rows.extend(
        {
            "instrument_id": iid,
            "metric_key": "OI",
            "ts": iso(at),
            "received_ts": iso(end + timedelta(seconds=1)),
            "oi_native": native,
            "oi_usd": usd,
        }
        for iid, at, native, usd in (
            ("A", start, "10", "1"),
            ("A", end, "12", "1"),
            ("B", start, "20", None),
            ("B", end, "22", None),
        )
    )
    source = decode_oi_inputs(token_id="T", instruments=instruments, observations=rows)
    rules = decode_oi_synchronization_rules(
        scope=OIRuleReadScope.from_snapshot(source),
        records=[
            {
                "instrument_id": None,
                "venue": "binance",
                "market_type": "PERP_LINEAR",
                "metric_key": "OI",
                "sidedness": "SINGLE_SIDED",
                "aggregation_class": "SINGLE",
            }
        ],
    )
    calculator = OpenInterestCalculator(
        source,
        OISelectionPolicy(tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975)),
    )
    request = OIWindowRequest(
        start=chosen_boundary(start, precision="EXACT"),
        cutoff=chosen_boundary(end, precision="EXACT"),
        assembled_at=collection_clock(end + timedelta(seconds=2)),
    )
    return calculator, request, rules


def composite_history(*, extreme: bool = False, delayed: bool = False) -> ObservedOICompositeWindow:
    calculator, request, rules = composite_source(extreme=extreme, delayed=delayed)
    result = calculator.composite_window(
        request, rules=rules, policy=OISynchronizationPolicy(eligible_venues=("binance",)), window="W1H"
    )
    assert isinstance(result, ObservedOICompositeWindow)
    return result


def test_historical_pair_cohort_does_not_require_young_current_instrument() -> None:
    result = composite_history()
    history = result.history
    assert history.current_instrument_ids == ("A", "B")
    assert history.grid_count == 240 and len(history.pairs) == 239
    assert {point.instrument_id for point in history.pairs[0].contributions} == {"A"}
    assert {point.instrument_id for point in history.pairs[-1].contributions} == {"A", "B"}
    assert history.native_normal_range.bounds == FloatBounds(lower=0.0, upper=0.0)
    assert history.usd_normal_range.bounds == FloatBounds(lower=0.0, upper=0.0)
    assert all(
        point.usd_start is None
        for pair in history.pairs
        for point in pair.contributions
        if point.instrument_id == "B"
    )
    assert result.selection.native_witness.end.native_total() == 34


def test_unrepresentable_composite_pair_is_retained_and_invalidates_whole_range() -> None:
    history = composite_history(extreme=True).history
    assert history.grid_count == 240 and len(history.pairs) == 239
    assert history.native_normal_range.bounds is None
    assert history.native_normal_range.reason == "NOT_ESTIMABLE"
    assert history.usd_normal_range.bounds == FloatBounds(lower=0.0, upper=0.0)


def test_future_receipt_history_cannot_satisfy_minimum_population() -> None:
    result = composite_history(delayed=True)
    assert result.selection.native_witness.end.native_total() == 34
    assert result.history.grid_count == 3
    assert result.history.native_normal_range.bounds is None
    assert result.history.native_normal_range.reason == "INSUFFICIENT_HISTORY"


def test_copied_population_range_cannot_override_actual_pairs() -> None:
    history = composite_history().history
    changed = history.model_copy(
        update={"native_normal_range": FloatRange(bounds=FloatBounds(lower=-0.5, upper=1.0), reason=None)}
    )
    with pytest.raises(ValueError, match="actual paired population"):
        type(history).model_validate(changed)


def test_actual_oi_only_family_preserves_native_and_unrequested_other_facts() -> None:
    calculator, request, rules = composite_source()
    calculation = calculate_oi_family(
        calculator,
        request,
        rules=rules,
        synchronization_policy=OISynchronizationPolicy(eligible_venues=("binance",)),
        history_cache=OIHistoryCache(BaselineRetention(128)),
        window="W1H",
    )
    result = calculation.result
    assert isinstance(result.synchronized, ConditionSynchronizedOpenInterest)
    observed = result.synchronized.values
    assert observed.oi_native_start == 30 and observed.oi_native_end == 34
    assert observed.native_beyond is True
    assert observed.oi_usd_start is None and observed.oi_usd_end is None
    assert observed.oi_usd_detail == "RECORDED_USD_NOT_CAPTURED"
    assert len(result.per_venue) == 2
    assert all(isinstance(point.open_interest, ObservedConditionOpenInterest) for point in result.per_venue)
    assert all(
        point.funding.status == point.basis.status == point.liquidations.status == "NOT_REQUESTED"
        for point in result.per_venue
    )
    assert result.funding_dispersion.status == result.incremental_volume.status == "NOT_REQUESTED"
    wire = encode_condition_derivatives(result)
    assert wire["synchronized"] is not None

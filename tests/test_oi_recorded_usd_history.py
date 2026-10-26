from datetime import timedelta
from decimal import Decimal

import pytest

from tank.analysis.condition_derivative_input_codec import decode_oi_inputs
from tank.analysis.condition_derivative_inputs import OISelectionPolicy
from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import JsonInput, iso, parse_iso_time


def recorded_history(
    values: tuple[str | None, str | None],
) -> tuple[OpenInterestCalculator, OIWindowRequest]:
    cutoff = parse_iso_time("2026-10-16T12:00:00Z")
    start = cutoff - timedelta(hours=1)
    instrument: dict[str, JsonInput] = {
        "instrument_id": "A",
        "token_id": "T",
        "venue": "V",
        "market_type": "PERP_LINEAR",
    }
    rows: list[dict[str, JsonInput]] = [
        {
            "instrument_id": "A",
            "ts": iso(start - timedelta(hours=hour)),
            "received_ts": iso(cutoff + timedelta(seconds=1)),
            "oi_native": "10",
            "oi_usd": values[hour % 2],
            "mark_price": "999",
        }
        for hour in range(1, 241)
    ]
    source = decode_oi_inputs(token_id="T", instruments=[instrument], observations=rows)
    return (
        OpenInterestCalculator(
            source,
            OISelectionPolicy(
                tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975)
            ),
        ),
        OIWindowRequest(
            start=chosen_boundary(start, precision="EXACT"),
            cutoff=chosen_boundary(cutoff, precision="EXACT"),
            assembled_at=collection_clock(cutoff + timedelta(seconds=2)),
        ),
    )


def test_recorded_usd_history_preserves_independent_observed_population() -> None:
    calculator, request = recorded_history(("100", "200"))
    native = calculator.native_baseline(request, window="W1H")[0]
    usd = calculator.recorded_usd_baseline(request, window="W1H")[0]
    assert native.population_basis == "OBSERVED_NATIVE_ENDPOINT_PAIRS"
    assert native.normal_range.bounds is not None
    assert native.normal_range.bounds.lower == native.normal_range.bounds.upper == 0
    assert usd.population_basis == "RECORDED_USD_ENDPOINT_PAIRS"
    assert usd.source_observation_count == usd.distinct_source_time_count == 240
    assert usd.pair_count == 239 and usd.rejected_pair_count == 1
    assert usd.normal_range.bounds is not None
    assert usd.normal_range.bounds.lower == -0.5 and usd.normal_range.bounds.upper == 1
    assert usd.first_observed_at.precision is None


@pytest.mark.parametrize("values", [(None, None), ("-1", "-1"), ("0", "0"), ("NaN", "NaN")])
def test_unavailable_usd_history_never_borrows_native_or_unclocked_marks(
    values: tuple[str | None, str | None],
) -> None:
    calculator, request = recorded_history(values)
    usd = calculator.recorded_usd_baseline(request, window="W1H")[0]
    native = calculator.native_baseline(request, window="W1H")[0]
    assert usd.source_observation_count == 240
    assert usd.pair_count == 0 and usd.rejected_pair_count == 240
    assert usd.normal_range.bounds is None and usd.normal_range.reason == "INSUFFICIENT_HISTORY"
    assert native.normal_range.bounds is not None
    assert native.normal_range.bounds.lower == native.normal_range.bounds.upper == 0


def test_unrepresentable_recorded_usd_population_keeps_every_actual_pair() -> None:
    calculator, request = recorded_history(("1E-310", "1E310"))
    usd = calculator.recorded_usd_baseline(request, window="W1H")[0]
    assert usd.source_observation_count == 240
    assert usd.pair_count == 239 and usd.rejected_pair_count == 1
    assert usd.unestimable_pair_count > 0
    assert usd.normal_range.bounds is None and usd.normal_range.reason == "NOT_ESTIMABLE"
    assert usd.instrument.token_id == "T"
    assert usd.interval.cutoff - usd.interval.start == timedelta(hours=1)


def test_recorded_positive_constant_has_known_zero_change_history() -> None:
    calculator, request = recorded_history(("12.5", "12.5"))
    usd = calculator.recorded_usd_baseline(request, window="W1H")[0]
    assert usd.normal_range.bounds is not None
    assert usd.normal_range.bounds.lower == usd.normal_range.bounds.upper == Decimal(0)

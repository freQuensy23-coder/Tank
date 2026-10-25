"""Independent exact native-flow and significance boundaries for the typed producer."""

from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError
from test_condition_input_codec import decoded, minute

from tank.analysis.condition_inputs import SpotCalculationPolicy
from tank.analysis.spot import (
    MissingSpotFlowMetrics,
    NativeFlowTotals,
    significant_flow,
    summarize_native_flow,
)


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

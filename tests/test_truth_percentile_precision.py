"""Exact financial interpolation is independent of binary float expansion."""

from decimal import Decimal, localcontext

import pytest

from tank.analysis.truth import percentile


@pytest.mark.parametrize(
    "low,high,quantile,expected",
    [
        ("1", "2", 0.05, "1.05"),
        ("-0.0004", "-0.0002", 0.025, "-0.000395"),
        (
            "1000000000000000000000000000001",
            "1000000000000000000000000000002",
            0.05,
            "1000000000000000000000000000001.05",
        ),
    ],
    ids=["usd_notional", "funding_rate", "native_open_interest"],
)
def test_financial_percentile_has_exact_decimal_weight(
    low: str, high: str, quantile: float, expected: str
) -> None:
    actual = percentile([Decimal(low), Decimal(high)], quantile)
    assert isinstance(actual, Decimal)
    assert actual == Decimal(expected)


def test_exact_interpolation_retains_large_and_subunit_digits_in_small_outer_context() -> None:
    low = Decimal("123456789012345678901234567890.000000000000000000000000000001")
    high = Decimal("123456789012345678901234567890.000000000000000000000000000003")
    with localcontext() as context:
        context.prec = 6
        actual = percentile([low, high], 0.5)
    assert actual == Decimal("123456789012345678901234567890.000000000000000000000000000002")

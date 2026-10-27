"""Selected interpolation preserves observations without unbounded dense expansion."""

from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError

from tank.analysis.funding_quantile import FundingQuantileValue, funding_quantile


@pytest.mark.parametrize(
    "values,probability,expected",
    [
        (("1", "3"), 0.25, "1.50"),
        (("-2", "2"), 0.5, "0"),
        (("0.001", "0.003", "0.005"), 0.5, "0.003"),
        (("1E1000000000", "1E1000000000"), 0.5, "1E1000000000"),
        (("-1E1000000000", "1E1000000000"), 0.5, "0"),
    ],
)
def test_exact_selected_interpolation(values: tuple[str, ...], probability: float, expected: str) -> None:
    with localcontext() as context:
        context.prec = 2
        value = funding_quantile(tuple(map(Decimal, values)), probability)
    assert value.value == Decimal(expected)
    assert value.reason is None
    assert value.position.source_count == len(values)


def test_dense_extreme_interpolation_is_explicit_without_dropping_population() -> None:
    value = funding_quantile((Decimal(1), Decimal("1E1000000000")), 0.025)
    assert value.value is None
    assert value.reason == "NOT_ESTIMABLE"
    assert value.position.source_count == 2
    assert (value.position.lower_index, value.position.upper_index) == (0, 1)
    assert value.operands.lower == 1
    assert value.operands.upper == Decimal("1E1000000000")
    assert value.required_dense_digits > 100000000


@pytest.mark.parametrize("probability,expected", [(0.0, "1"), (1.0, "1E1000000000")])
def test_compact_selected_endpoint_does_not_refuse_wide_population(probability: float, expected: str) -> None:
    value = funding_quantile((Decimal(1), Decimal("1E1000000000")), probability)
    assert value.value == Decimal(expected)
    assert value.reason is None


@pytest.mark.parametrize("probability,expected", [(0.025, "1"), (0.975, "1E1000000000")])
def test_equal_selected_bounds_remain_known(probability: float, expected: str) -> None:
    value = funding_quantile(tuple(map(Decimal, ("1", "1", "1E1000000000", "1E1000000000"))), probability)
    assert value.value == Decimal(expected)
    assert value.reason is None
    assert value.position.source_count == 4


def test_exact_interpolation_ignores_ambient_precision() -> None:
    lower = Decimal("123456789012345678901234567890.001")
    upper = Decimal("123456789012345678901234567890.003")
    with localcontext() as context:
        context.prec = 2
        value = funding_quantile((lower, upper), 0.5)
    assert value.value == Decimal("123456789012345678901234567890.002")


def test_copied_wrong_result_refuses_public_admission() -> None:
    value = funding_quantile((Decimal(1), Decimal(3)), 0.5)
    with pytest.raises(ValidationError):
        FundingQuantileValue.model_validate(value.model_copy(update={"value": Decimal(9)}))


def test_zero_lower_endpoint_does_not_materialize_unused_weight_complement() -> None:
    from tank.analysis.funding_quantile import FundingInterpolationOperands, FundingQuantilePosition

    point = FundingQuantileValue(
        value=Decimal("1E-999999999"),
        reason=None,
        required_dense_digits=0,
        position=FundingQuantilePosition(
            lower_index=0,
            upper_index=1,
            source_count=2,
            interpolation_weight=Decimal("1E-1000000000"),
        ),
        operands=FundingInterpolationOperands(
            lower=Decimal(0),
            upper=Decimal(10),
            weight=Decimal("1E-1000000000"),
        ),
    )
    assert point.value == Decimal("1E-999999999")

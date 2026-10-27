"""Exact selected funding interpolation with bounded new dense materialization."""

from collections.abc import Sequence
from decimal import Decimal
from math import isfinite
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel

DENSE_INTERPOLATION_BASE_DIGITS = 4096


class FundingQuantilePosition(DomainModel):
    lower_index: int = Field(ge=0)
    upper_index: int = Field(ge=0)
    source_count: int = Field(gt=0)
    interpolation_weight: Decimal = Field(ge=0, le=1)

    @model_validator(mode="after")
    def actual_indices(self) -> Self:
        if not self.lower_index <= self.upper_index < self.source_count:
            raise ValueError("Funding quantile indices must refer to its actual source population")
        return self


class FundingInterpolationOperands(DomainModel):
    lower: Decimal
    upper: Decimal
    weight: Decimal = Field(ge=0, le=1)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.lower > self.upper:
            raise ValueError("Funding interpolation endpoints must retain their actual order")
        return self


class _InterpolationValue(DomainModel):
    value: Decimal | None
    reason: Literal["NOT_ESTIMABLE"] | None
    required_dense_digits: int = Field(ge=0)

    @model_validator(mode="after")
    def actual_result(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("A funding quantile requires a value or its actual representability cause")
        return self


def _coefficient(value: Decimal) -> tuple[int, int]:
    shape = value.as_tuple()
    if not isinstance(shape.exponent, int):
        raise ValueError("Funding quantiles require actual finite Decimal values")
    return int(Decimal((shape.sign, shape.digits, 0))), shape.exponent


def _normalized(coefficient: int, exponent: int) -> tuple[int, int]:
    if coefficient == 0:
        return 0, 0
    shape = Decimal(coefficient).as_tuple()
    digits = shape.digits
    retained = len(digits)
    while retained > 1 and digits[retained - 1] == 0:
        retained -= 1
    return int(Decimal((shape.sign, digits[:retained], 0))), exponent + len(digits) - retained


def _weighted(value: Decimal, weight: Decimal) -> tuple[int, int]:
    left, left_power = _coefficient(value)
    right, right_power = _coefficient(weight)
    return _normalized(left * right, left_power + right_power)


def _decimal(coefficient: int, exponent: int) -> Decimal:
    shape = Decimal(coefficient).as_tuple()
    return Decimal((shape.sign, shape.digits, exponent))


def _interpolate(operands: FundingInterpolationOperands) -> _InterpolationValue:
    lower, upper, weight = operands.lower, operands.upper, operands.weight
    if lower == upper or weight == 0:
        return _InterpolationValue(value=lower, reason=None, required_dense_digits=0)
    if weight == 1:
        return _InterpolationValue(value=upper, reason=None, required_dense_digits=0)
    if lower == 0:
        coefficient, exponent = _weighted(upper, weight)
        return _InterpolationValue(
            value=_decimal(coefficient, exponent), reason=None, required_dense_digits=0
        )
    weight_coefficient, weight_power = _normalized(*_coefficient(weight))
    weight_digits = len(Decimal(abs(weight_coefficient)).as_tuple().digits)
    source_digits = sum(len(value.as_tuple().digits) for value in (lower, upper))
    complement_budget = max(DENSE_INTERPOLATION_BASE_DIGITS, 2 * (source_digits + weight_digits) + 32)
    if -weight_power > complement_budget:
        return _InterpolationValue(value=None, reason="NOT_ESTIMABLE", required_dense_digits=-weight_power)
    denominator = 10**-weight_power
    complement = _decimal(denominator - weight_coefficient, weight_power)
    left, left_power = _weighted(lower, complement)
    right, right_power = _weighted(upper, weight)
    if left == 0 or right == 0 or left_power == right_power:
        coefficient, exponent = (
            (right, right_power)
            if left == 0
            else (left, left_power)
            if right == 0
            else _normalized(left + right, left_power)
        )
        return _InterpolationValue(
            value=_decimal(coefficient, exponent), reason=None, required_dense_digits=0
        )
    lower_power = min(left_power, right_power)
    left_digits = len(Decimal(abs(left)).as_tuple().digits)
    right_digits = len(Decimal(abs(right)).as_tuple().digits)
    required = max(left_power + left_digits, right_power + right_digits) - lower_power
    budget = max(DENSE_INTERPOLATION_BASE_DIGITS, 2 * (left_digits + right_digits) + 32)
    if required > budget:
        return _InterpolationValue(value=None, reason="NOT_ESTIMABLE", required_dense_digits=required)
    coefficient, exponent = _normalized(
        left * 10 ** (left_power - lower_power) + right * 10 ** (right_power - lower_power), lower_power
    )
    return _InterpolationValue(
        value=_decimal(coefficient, exponent), reason=None, required_dense_digits=required
    )


class FundingQuantileValue(_InterpolationValue):
    position: FundingQuantilePosition
    operands: FundingInterpolationOperands

    @model_validator(mode="after")
    def bound_interpolation(self) -> Self:
        if self.position.interpolation_weight != self.operands.weight:
            raise ValueError("Funding quantile weight must bind its actual selected interpolation")
        expected = _interpolate(self.operands)
        if (
            self.value != expected.value
            or self.reason != expected.reason
            or self.required_dense_digits != expected.required_dense_digits
        ):
            raise ValueError("Funding quantile must retain its exact selected interpolation result")
        return self


def funding_quantile(values: Sequence[Decimal], probability: float) -> FundingQuantileValue:
    population = tuple(values)
    if not population or any(not isinstance(value, Decimal) or not value.is_finite() for value in population):
        raise ValueError("Funding quantiles require a nonempty finite observed population")
    if isinstance(probability, bool) or not isfinite(probability) or not 0 <= probability <= 1:
        raise ValueError("Funding quantiles require an actual probability")
    ordered = sorted(population)
    position = (len(ordered) - 1) * probability
    index = int(position)
    next_index = min(index + 1, len(ordered) - 1)
    weight = Decimal(str(position - index))
    coordinates = FundingQuantilePosition(
        lower_index=index,
        upper_index=next_index,
        source_count=len(ordered),
        interpolation_weight=weight,
    )
    operands = FundingInterpolationOperands(lower=ordered[index], upper=ordered[next_index], weight=weight)
    calculated = _interpolate(operands)
    return FundingQuantileValue(
        value=calculated.value,
        reason=calculated.reason,
        required_dense_digits=calculated.required_dense_digits,
        position=coordinates,
        operands=operands,
    )

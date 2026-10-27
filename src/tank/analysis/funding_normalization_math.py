"""Current eight-hour rate arithmetic without source adapters, clocks or I/O."""

from decimal import (
    MAX_EMAX,
    MIN_EMIN,
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DecimalException,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    localcontext,
)
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import require_integer


class FundingRateOperands(DomainModel):
    rate: Decimal
    interval_h: Decimal = Field(gt=0)


class _FundingRateValue(DomainModel):
    value: Decimal | None
    reason: Literal["NOT_ESTIMABLE"] | None

    @model_validator(mode="after")
    def available_or_reason(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("Unavailable funding arithmetic requires its representability cause")
        return self


def _calculate(operands: FundingRateOperands) -> _FundingRateValue:
    initial, duration = operands.rate.as_tuple(), operands.interval_h.as_tuple()
    if not isinstance(initial.exponent, int) or not isinstance(duration.exponent, int):
        return _FundingRateValue(value=None, reason="NOT_ESTIMABLE")
    try:
        context = Context(
            prec=128,
            rounding=ROUND_HALF_EVEN,
            Emax=MAX_EMAX,
            Emin=MIN_EMIN,
            traps=[InvalidOperation, DivisionByZero, Overflow],
        )
        with localcontext(context):
            numerator = Decimal((initial.sign, initial.digits, 0))
            denominator = Decimal((duration.sign, duration.digits, 0))
            normalized = (numerator * 8 / denominator).as_tuple()
        if not isinstance(normalized.exponent, int):
            return _FundingRateValue(value=None, reason="NOT_ESTIMABLE")
        value = Decimal(
            (
                normalized.sign,
                normalized.digits,
                normalized.exponent + initial.exponent - duration.exponent,
            )
        )
    except (DecimalException, OverflowError):
        return _FundingRateValue(value=None, reason="NOT_ESTIMABLE")
    if not value.is_finite() or value == 0 and operands.rate != 0:
        return _FundingRateValue(value=None, reason="NOT_ESTIMABLE")
    return _FundingRateValue(value=value, reason=None)


class FundingRateCalculation(_FundingRateValue):
    operands: FundingRateOperands
    calculation_precision_digits: Annotated[Literal[128], BeforeValidator(require_integer)]

    @model_validator(mode="after")
    def actual_operands(self) -> Self:
        expected = _calculate(self.operands)
        if self.value != expected.value or self.reason != expected.reason:
            raise ValueError("Funding normalization must match its actual rate and positive duration")
        return self


def normalize_funding_rate(operands: FundingRateOperands) -> FundingRateCalculation:
    operands = FundingRateOperands.model_validate(operands)
    calculated = _calculate(operands)
    return FundingRateCalculation(
        operands=operands,
        value=calculated.value,
        reason=calculated.reason,
        calculation_precision_digits=128,
    )

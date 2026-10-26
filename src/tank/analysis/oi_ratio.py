"""Exact OI endpoint comparisons independent of representable FLOAT64 display changes."""

from decimal import Decimal
from fractions import Fraction
from math import isfinite
from sys import float_info
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.statistics_contracts import FloatBounds, FloatRange
from tank.contracts import DomainModel

type OiRangePosition = Literal["BELOW", "WITHIN", "ABOVE"]
type NativeOiChangeFailure = Literal["ZERO_BASELINE", "NOT_ESTIMABLE"]
type UsdOiChangeFailure = NativeOiChangeFailure | Literal["INCOMPLETE"]

_FLOAT64_EXTREME_EXPONENT_GAP = float_info.max_10_exp + 2


class OiEndpoints(DomainModel):
    start: Decimal = Field(ge=0)
    end: Decimal = Field(ge=0)


class UsdOiEndpoints(DomainModel):
    start: Decimal | None = Field(ge=0)
    end: Decimal | None = Field(ge=0)


class _KnownChange(DomainModel):
    value: float | None = Field(ge=-1)
    reason: NativeOiChangeFailure | None
    position: OiRangePosition | None
    beyond: bool | None


def _fraction_change(points: OiEndpoints) -> Fraction:
    initial, final = points.start.as_tuple(), points.end.as_tuple()
    if not isinstance(initial.exponent, int) or not isinstance(final.exponent, int):
        raise ValueError("OI endpoint changes require finite quantities")
    common = min(initial.exponent, final.exponent)
    start = Decimal((initial.sign, initial.digits, initial.exponent - common))
    end = Decimal((final.sign, final.digits, final.exponent - common))
    return (Fraction(end) - Fraction(start)) / Fraction(start)


def _position(value: Fraction, bounds: FloatBounds) -> OiRangePosition:
    if value < Fraction(str(bounds.lower)):
        return "BELOW"
    if value > Fraction(str(bounds.upper)):
        return "ABOVE"
    return "WITHIN"


def _known_change(points: OiEndpoints, normal: FloatRange) -> _KnownChange:
    if points.start == 0:
        return _KnownChange(value=None, reason="ZERO_BASELINE", position=None, beyond=None)
    bounds = normal.bounds
    difference = points.end.adjusted() - points.start.adjusted() if points.end != 0 else 0
    position: OiRangePosition | None
    if difference > _FLOAT64_EXTREME_EXPONENT_GAP:
        position = None if bounds is None else "ABOVE"
        return _KnownChange(
            value=None,
            reason="NOT_ESTIMABLE",
            position=position,
            beyond=None if position is None else True,
        )
    if difference < -_FLOAT64_EXTREME_EXPONENT_GAP:
        position = (
            None
            if bounds is None
            else "BELOW"
            if bounds.lower > -1
            else "ABOVE"
            if bounds.upper <= -1
            else "WITHIN"
        )
        return _KnownChange(
            value=-1.0,
            reason=None,
            position=position,
            beyond=None if position is None else position != "WITHIN",
        )
    ratio = Fraction(-1) if points.end == 0 else _fraction_change(points)
    value: float | None
    reason: NativeOiChangeFailure | None
    try:
        displayed = float(ratio)
    except OverflowError:
        value, reason = None, "NOT_ESTIMABLE"
    else:
        value, reason = (
            (None, "NOT_ESTIMABLE")
            if not isfinite(displayed) or ratio != 0 and displayed == 0
            else (displayed, None)
        )
    position = None if bounds is None else _position(ratio, bounds)
    return _KnownChange(
        value=value,
        reason=reason,
        position=position,
        beyond=None if position is None else position != "WITHIN",
    )


class NativeOiChange(_KnownChange):
    endpoints: OiEndpoints
    normal_range: FloatRange

    @model_validator(mode="after")
    def actual_endpoint_change(self) -> Self:
        expected = _known_change(self.endpoints, self.normal_range)
        if (self.value, self.reason, self.position, self.beyond) != (
            expected.value,
            expected.reason,
            expected.position,
            expected.beyond,
        ):
            raise ValueError("Native OI display and comparison must preserve the exact endpoint change")
        return self


class UsdOiChange(DomainModel):
    endpoints: UsdOiEndpoints
    normal_range: FloatRange
    value: float | None = Field(ge=-1)
    reason: UsdOiChangeFailure | None
    position: OiRangePosition | None
    beyond: bool | None

    @model_validator(mode="after")
    def actual_endpoint_change(self) -> Self:
        if self.endpoints.start is None or self.endpoints.end is None:
            if (self.value, self.reason, self.position, self.beyond) != (None, "INCOMPLETE", None, None):
                raise ValueError("Missing USD endpoints cannot claim a derived change or baseline comparison")
        else:
            expected = _known_change(
                OiEndpoints(start=self.endpoints.start, end=self.endpoints.end), self.normal_range
            )
            if (self.value, self.reason, self.position, self.beyond) != (
                expected.value,
                expected.reason,
                expected.position,
                expected.beyond,
            ):
                raise ValueError("USD OI display and comparison must preserve the exact endpoint change")
        return self


def native_oi_change(start: Decimal, end: Decimal, *, normal_range: FloatRange) -> NativeOiChange:
    points = OiEndpoints(start=start, end=end)
    normal = FloatRange.model_validate(normal_range)
    result = _known_change(points, normal)
    return NativeOiChange(
        endpoints=points,
        normal_range=normal,
        value=result.value,
        reason=result.reason,
        position=result.position,
        beyond=result.beyond,
    )


def usd_oi_change(start: Decimal | None, end: Decimal | None, *, normal_range: FloatRange) -> UsdOiChange:
    points = UsdOiEndpoints(start=start, end=end)
    normal = FloatRange.model_validate(normal_range)
    if points.start is None or points.end is None:
        return UsdOiChange(
            endpoints=points,
            normal_range=normal,
            value=None,
            reason="INCOMPLETE",
            position=None,
            beyond=None,
        )
    result = _known_change(OiEndpoints(start=points.start, end=points.end), normal)
    return UsdOiChange(
        endpoints=points,
        normal_range=normal,
        value=result.value,
        reason=result.reason,
        position=result.position,
        beyond=result.beyond,
    )

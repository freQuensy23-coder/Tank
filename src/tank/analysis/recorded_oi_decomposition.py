"""Exact recorded-USD decomposition without materializing enormous exponent gaps."""

from collections.abc import Iterable
from decimal import Decimal
from fractions import Fraction
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel

DISPLAY_MATERIALIZATION_DIGITS = 4096
type ShareDisplayReason = Literal["ZERO_ABSOLUTE_TERMS", "FLOAT64_UNDERFLOW", "DISPLAY_NOT_MATERIALIZED"]
type IntegerTerm = tuple[int, int]


def _canonical(values: Iterable[IntegerTerm]) -> tuple[IntegerTerm, ...]:
    remaining = list(values)
    while True:
        grouped: dict[int, int] = {}
        for coefficient, exponent in remaining:
            if coefficient == 0:
                continue
            while coefficient % 10 == 0:
                coefficient //= 10
                exponent += 1
            grouped[exponent] = grouped.get(exponent, 0) + coefficient
        remaining = [(coefficient, exponent) for exponent, coefficient in grouped.items() if coefficient]
        if all(coefficient % 10 for coefficient, _ in remaining):
            return tuple(sorted(remaining, key=lambda value: value[1], reverse=True))


def _digits(coefficient: int) -> int:
    return Decimal(abs(coefficient)).adjusted() + 1


def _sign(values: Iterable[IntegerTerm]) -> int:
    terms = list(_canonical(values))
    while terms:
        terms.sort(key=lambda value: value[1] + _digits(value[0]) - 1, reverse=True)
        first = terms[0]
        if len(terms) == 1:
            return 1 if first[0] > 0 else -1
        second = terms[1]
        leading = first[1] + _digits(first[0]) - 1
        following = second[1] + _digits(second[0]) - 1
        if leading > following + 1 + len(str(len(terms) - 1)):
            return 1 if first[0] > 0 else -1
        exponent = min(first[1], second[1])
        combined = first[0] * 10 ** (first[1] - exponent) + second[0] * 10 ** (second[1] - exponent)
        terms = list(_canonical([*terms[2:], (combined, exponent)]))
    return 0


class RecordedOiEndpoints(DomainModel):
    native_start: Decimal = Field(ge=0)
    native_end: Decimal = Field(ge=0)
    usd_start: Decimal = Field(ge=0)
    usd_end: Decimal = Field(ge=0)


class DecimalPowerTerm(DomainModel):
    coefficient: Decimal
    exponent: int

    @model_validator(mode="after")
    def canonical_coefficient(self) -> Self:
        shape = self.coefficient.as_tuple()
        if self.coefficient == 0 or shape.exponent != 0 or shape.digits[-1] == 0:
            raise ValueError("Sparse coefficients require nonzero canonical integer digits")
        return self


class SparseDecimalPolynomial(DomainModel):
    terms: tuple[DecimalPowerTerm, ...]

    @model_validator(mode="after")
    def canonical_terms(self) -> Self:
        values = tuple((int(term.coefficient), term.exponent) for term in self.terms)
        if values != _canonical(values):
            raise ValueError("Sparse polynomial terms require their canonical ordered representation")
        return self


class RecordedDecompositionTerms(DomainModel):
    position: SparseDecimalPolynomial
    markup: SparseDecimalPolynomial
    interaction: SparseDecimalPolynomial
    denominator: DecimalPowerTerm

    @model_validator(mode="after")
    def positive_denominator(self) -> Self:
        if self.denominator.coefficient <= 0:
            raise ValueError("Recorded decomposition requires a positive common denominator")
        return self


class _RecordedMath(DomainModel):
    terms: RecordedDecompositionTerms
    native_majority: bool | None
    native_majority_reason: Literal["ZERO_ABSOLUTE_TERMS"] | None
    pos_share: float | None = Field(ge=0, le=1)
    pos_share_reason: ShareDisplayReason | None


class DefinedRecordedOiDecomposition(_RecordedMath):
    status: Literal["DEFINED"]
    basis: Literal["RECORDED_USD_IMPLIED_PRICE"]
    endpoints: RecordedOiEndpoints

    @model_validator(mode="after")
    def endpoint_bound_math(self) -> Self:
        if self.endpoints.native_start == 0 or self.endpoints.native_end == 0:
            raise ValueError("Implied-price decomposition requires both positive native endpoints")
        expected = _calculate(self.endpoints)
        if (
            self.terms,
            self.native_majority,
            self.native_majority_reason,
            self.pos_share,
            self.pos_share_reason,
        ) != (
            expected.terms,
            expected.native_majority,
            expected.native_majority_reason,
            expected.pos_share,
            expected.pos_share_reason,
        ):
            raise ValueError("Recorded decomposition must preserve exact endpoint-bound mathematics")
        return self


class UndefinedRecordedOiDecomposition(DomainModel):
    status: Literal["UNDEFINED"]
    basis: Literal["RECORDED_USD_IMPLIED_PRICE"]
    endpoints: RecordedOiEndpoints
    reason: Literal["NATIVE_ENDPOINT_ZERO"]
    zero_endpoint: Literal["START", "END", "BOTH"]
    native_majority: None
    native_majority_reason: Literal["NATIVE_ENDPOINT_ZERO"]
    pos_share: None
    pos_share_reason: Literal["NATIVE_ENDPOINT_ZERO"]

    @model_validator(mode="after")
    def actual_zero(self) -> Self:
        initial, final = self.endpoints.native_start == 0, self.endpoints.native_end == 0
        expected = "BOTH" if initial and final else "START" if initial else "END" if final else None
        if expected is None or self.zero_endpoint != expected:
            raise ValueError("Undefined recorded decomposition requires its actual zero native endpoint")
        return self


type RecordedOiDecomposition = DefinedRecordedOiDecomposition | UndefinedRecordedOiDecomposition


def _value_term(value: Decimal) -> IntegerTerm:
    shape = value.as_tuple()
    if not isinstance(shape.exponent, int):
        raise ValueError("Recorded decomposition requires finite endpoints")
    coefficient = int(Decimal((shape.sign, shape.digits, 0)))
    return coefficient, shape.exponent


def _multiply(*values: IntegerTerm) -> IntegerTerm:
    coefficient, exponent = 1, 0
    for value, power in values:
        coefficient *= value
        exponent += power
    return coefficient, exponent


def _negate(value: IntegerTerm) -> IntegerTerm:
    return -value[0], value[1]


def _polynomial(values: Iterable[IntegerTerm]) -> SparseDecimalPolynomial:
    return SparseDecimalPolynomial(
        terms=tuple(
            DecimalPowerTerm(coefficient=Decimal(coefficient), exponent=exponent)
            for coefficient, exponent in _canonical(values)
        )
    )


def _share(
    numerator: tuple[IntegerTerm, ...], denominator: tuple[IntegerTerm, ...]
) -> tuple[float | None, ShareDisplayReason | None]:
    if not denominator:
        return None, "ZERO_ABSOLUTE_TERMS"
    if not numerator:
        return 0.0, None
    first, second = (
        dict((power, coefficient) for coefficient, power in numerator),
        dict((power, coefficient) for coefficient, power in denominator),
    )
    ratio: Fraction | None = None
    if first.keys() == second.keys():
        power = next(iter(first))
        a, b = first[power], second[power]
        if all(first[key] * b == second[key] * a for key in first):
            ratio = Fraction(a, b)
    if ratio is None:
        combined = (*numerator, *denominator)
        low = min(power for _, power in combined)
        high = max(power + _digits(coefficient) for coefficient, power in combined)
        if high - low > DISPLAY_MATERIALIZATION_DIGITS:
            return None, "DISPLAY_NOT_MATERIALIZED"
        initial = sum(coefficient * 10 ** (power - low) for coefficient, power in numerator)
        final = sum(coefficient * 10 ** (power - low) for coefficient, power in denominator)
        ratio = Fraction(initial, final)
    if not 0 <= ratio <= 1:
        raise ValueError("Exact position share must remain inside its absolute-term denominator")
    result = float(ratio)
    if result == 0 and ratio:
        return None, "FLOAT64_UNDERFLOW"
    return result, None


def _calculate(endpoints: RecordedOiEndpoints) -> _RecordedMath:
    a, z = _value_term(endpoints.native_start), _value_term(endpoints.native_end)
    u, v = _value_term(endpoints.usd_start), _value_term(endpoints.usd_end)
    azu, zz_u, aa_v, azv = _multiply(a, z, u), _multiply(z, z, u), _multiply(a, a, v), _multiply(a, z, v)
    position = _canonical((zz_u, _negate(azu)))
    markup = _canonical((aa_v, _negate(azu)))
    interaction = _canonical((azv, _negate(zz_u), _negate(aa_v), azu))
    absolute = [
        tuple((coefficient * _sign(values), exponent) for coefficient, exponent in values)
        for values in (position, markup, interaction)
    ]
    total = _canonical(value for values in absolute for value in values)
    comparison = _canonical([*absolute[0], *(_negate(value) for values in absolute[1:] for value in values)])
    denominator = _canonical((_multiply(a, z),))
    share, reason = _share(absolute[0], total)
    return _RecordedMath(
        terms=RecordedDecompositionTerms(
            position=_polynomial(position),
            markup=_polynomial(markup),
            interaction=_polynomial(interaction),
            denominator=DecimalPowerTerm(coefficient=Decimal(denominator[0][0]), exponent=denominator[0][1]),
        ),
        native_majority=(_sign(comparison) > 0) if total else None,
        native_majority_reason=None if total else "ZERO_ABSOLUTE_TERMS",
        pos_share=share,
        pos_share_reason=reason,
    )


def recorded_oi_decomposition(endpoints: RecordedOiEndpoints) -> RecordedOiDecomposition:
    endpoints = RecordedOiEndpoints.model_validate(endpoints)
    initial, final = endpoints.native_start == 0, endpoints.native_end == 0
    if initial or final:
        return UndefinedRecordedOiDecomposition(
            status="UNDEFINED",
            basis="RECORDED_USD_IMPLIED_PRICE",
            endpoints=endpoints,
            reason="NATIVE_ENDPOINT_ZERO",
            zero_endpoint="BOTH" if initial and final else "START" if initial else "END",
            native_majority=None,
            native_majority_reason="NATIVE_ENDPOINT_ZERO",
            pos_share=None,
            pos_share_reason="NATIVE_ENDPOINT_ZERO",
        )
    result = _calculate(endpoints)
    return DefinedRecordedOiDecomposition(
        status="DEFINED",
        basis="RECORDED_USD_IMPLIED_PRICE",
        endpoints=endpoints,
        terms=result.terms,
        native_majority=result.native_majority,
        native_majority_reason=result.native_majority_reason,
        pos_share=result.pos_share,
        pos_share_reason=result.pos_share_reason,
    )

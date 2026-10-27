"""Exact terminating dollar terms; sparse rational facts remain authoritative."""

from decimal import Decimal
from fractions import Fraction
from typing import Literal, Self

from pydantic import model_validator

from tank.analysis.recorded_oi_decomposition import (
    DISPLAY_MATERIALIZATION_DIGITS,
    DecimalPowerTerm,
    DefinedRecordedOiDecomposition,
    RecordedOiDecomposition,
    SparseDecimalPolynomial,
    UndefinedRecordedOiDecomposition,
)
from tank.contracts import DomainModel

type DollarTermAbsence = Literal[
    "NATIVE_ENDPOINT_ZERO", "NON_TERMINATING_DECIMAL", "DISPLAY_NOT_MATERIALIZED"
]


class RecordedOiDollarTerm(DomainModel):
    value: Decimal | None
    reason: DollarTermAbsence | None

    @model_validator(mode="after")
    def actual_display(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("An unavailable exact dollar term requires its display cause")
        return self


def _materialize(polynomial: SparseDecimalPolynomial, denominator: DecimalPowerTerm) -> RecordedOiDollarTerm:
    if not polynomial.terms:
        return RecordedOiDollarTerm(value=Decimal(0), reason=None)
    values = (*polynomial.terms, denominator)
    low = min(term.exponent for term in values)
    high = max(term.exponent + term.coefficient.adjusted() + 1 for term in values)
    if high - low > DISPLAY_MATERIALIZATION_DIGITS:
        return RecordedOiDollarTerm(value=None, reason="DISPLAY_NOT_MATERIALIZED")
    numerator = sum(int(term.coefficient) * 10 ** (term.exponent - low) for term in polynomial.terms)
    divisor = int(denominator.coefficient) * 10 ** (denominator.exponent - low)
    rational = Fraction(numerator, divisor)
    remaining = rational.denominator
    twos = fives = 0
    while remaining % 2 == 0:
        twos += 1
        remaining //= 2
    while remaining % 5 == 0:
        fives += 1
        remaining //= 5
    if remaining != 1:
        return RecordedOiDollarTerm(value=None, reason="NON_TERMINATING_DECIMAL")
    scale = max(twos, fives)
    coefficient = rational.numerator * 2 ** (scale - twos) * 5 ** (scale - fives)
    shape = Decimal(coefficient).as_tuple()
    value = Decimal((shape.sign, shape.digits, -scale))
    return RecordedOiDollarTerm(value=value, reason=None)


class RecordedOiDollarTerms(DomainModel):
    source: RecordedOiDecomposition
    position: RecordedOiDollarTerm
    markup: RecordedOiDollarTerm
    interaction: RecordedOiDollarTerm

    @model_validator(mode="after")
    def bound_terms(self) -> Self:
        if isinstance(self.source, UndefinedRecordedOiDecomposition):
            expected = RecordedOiDollarTerm(value=None, reason="NATIVE_ENDPOINT_ZERO")
            if any(value != expected for value in (self.position, self.markup, self.interaction)):
                raise ValueError("A zero native endpoint cannot invent implied-price dollar terms")
        elif (
            self.position != _materialize(self.source.terms.position, self.source.terms.denominator)
            or self.markup != _materialize(self.source.terms.markup, self.source.terms.denominator)
            or self.interaction != _materialize(self.source.terms.interaction, self.source.terms.denominator)
        ):
            raise ValueError("Dollar displays must equal their exact recorded endpoint terms")
        return self


def recorded_oi_dollar_terms(source: RecordedOiDecomposition) -> RecordedOiDollarTerms:
    if isinstance(source, DefinedRecordedOiDecomposition):
        value = DefinedRecordedOiDecomposition.model_validate(source)
        return RecordedOiDollarTerms(
            source=value,
            position=_materialize(value.terms.position, value.terms.denominator),
            markup=_materialize(value.terms.markup, value.terms.denominator),
            interaction=_materialize(value.terms.interaction, value.terms.denominator),
        )
    missing = RecordedOiDollarTerm(value=None, reason="NATIVE_ENDPOINT_ZERO")
    return RecordedOiDollarTerms(
        source=UndefinedRecordedOiDecomposition.model_validate(source),
        position=missing,
        markup=missing,
        interaction=missing,
    )

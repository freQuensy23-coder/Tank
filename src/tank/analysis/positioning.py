"""Exact USD open-interest decomposition independent of capture and persistence."""

from collections.abc import Sequence
from decimal import Decimal, localcontext
from typing import Literal

from pydantic import Field

from tank.analysis.recorded_oi_decomposition import (
    DefinedRecordedOiDecomposition,
    RecordedOiDecomposition,
    UndefinedRecordedOiDecomposition,
)
from tank.contracts import DomainModel

type Scalar = Decimal | str | int | float
type MissingOIUsdDetail = Literal[
    "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE", "REPORTED_USD_INVALID", "RECORDED_USD_NOT_CAPTURED"
]


class OIDecomposition(DomainModel):
    position: Decimal
    markup: Decimal
    interaction: Decimal
    pos_share: float | None = Field(ge=0, le=1)


class MissingOIDecomposition(DomainModel):
    position: None = None
    markup: None = None
    interaction: None = None
    pos_share: None = None
    reason: Literal["INCOMPLETE"] = "INCOMPLETE"
    detail: MissingOIUsdDetail = "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"


type OiDecompositionResult = OIDecomposition | MissingOIDecomposition | RecordedOiDecomposition


def decomposition(n0: Scalar, n1: Scalar, p0: Scalar, p1: Scalar) -> OIDecomposition:
    """Preserve quantity, price and interaction terms before deriving a display share."""
    if any(isinstance(value, bool) for value in (n0, n1, p0, p1)):
        raise ValueError("boolean is not an OI quantity or mark")
    values = tuple(Decimal(str(value)) for value in (n0, n1, p0, p1))
    if not all(value.is_finite() for value in values):
        raise ValueError("OI quantities and marks must be finite")
    a, z, initial, final = values
    with localcontext() as context:
        span = (
            max(value.adjusted() for value in values)
            - min(int(value.as_tuple().exponent) for value in values)
            + 1
        )
        context.prec = max(128, 2 * span + 32)
        position = (z - a) * initial
        markup = a * (final - initial)
        interaction = (z - a) * (final - initial)
        total = abs(position) + abs(markup) + abs(interaction)
        share = None if total == 0 else float(abs(position) / total)
        return OIDecomposition(position=position, markup=markup, interaction=interaction, pos_share=share)


def _beyond(value: float | None, interval: Sequence[float] | None) -> bool | None:
    if value is None or interval is None:
        return None
    if len(interval) != 2:
        raise ValueError("normal range requires exactly two bounds")
    return value < interval[0] or value > interval[1]


def oi_class(
    dn: float | None,
    du: float | None,
    dec: OiDecompositionResult,
    nrange: Sequence[float] | None,
    urange: Sequence[float] | None,
) -> Literal["REAL_NATIVE", "MIXED", "PRICE_MARKUP"] | None:
    """The native-majority predicate uses exact dollar terms, never the rounded share."""
    return oi_class_from_comparisons(_beyond(dn, nrange), _beyond(du, urange), dec)


def oi_class_from_comparisons(
    nb: bool | None,
    ub: bool | None,
    dec: OiDecompositionResult,
) -> Literal["REAL_NATIVE", "MIXED", "PRICE_MARKUP"] | None:
    """Classify from actual baseline predicates without substituting a display ratio."""
    if any(value is not None and not isinstance(value, bool) for value in (nb, ub)):
        raise ValueError("OI baseline predicates require booleans or explicit unavailability")
    if nb is None:
        return None
    if nb:
        if isinstance(dec, DefinedRecordedOiDecomposition):
            majority = DefinedRecordedOiDecomposition.model_validate(dec).native_majority
            return None if majority is None else "REAL_NATIVE" if majority else "MIXED"
        if isinstance(dec, UndefinedRecordedOiDecomposition):
            UndefinedRecordedOiDecomposition.model_validate(dec)
            return None
        if isinstance(dec, MissingOIDecomposition) or dec.pos_share is None:
            return None
        terms = (dec.position, dec.markup, dec.interaction)
        with localcontext() as context:
            span = (
                max(value.adjusted() for value in terms)
                - min(int(value.as_tuple().exponent) for value in terms)
                + 1
            )
            context.prec = max(128, 2 * span + 32)
            return "REAL_NATIVE" if 2 * abs(terms[0]) > sum(abs(value) for value in terms) else "MIXED"
    if ub is None:
        return None
    return "PRICE_MARKUP" if ub else "MIXED"

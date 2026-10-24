"""Exact USD open-interest decomposition independent of capture and persistence."""

from collections.abc import Sequence
from decimal import Decimal, localcontext
from typing import Literal

from pydantic import Field

from tank.contracts import DomainModel

type Scalar = Decimal | str | int | float


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
    detail: Literal["SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"] = (
        "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"
    )


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
    dec: OIDecomposition | MissingOIDecomposition,
    nrange: Sequence[float] | None,
    urange: Sequence[float] | None,
) -> Literal["REAL_NATIVE", "MIXED", "PRICE_MARKUP"] | None:
    """The native-majority predicate uses exact dollar terms, never the rounded share."""
    nb = _beyond(dn, nrange)
    ub = _beyond(du, urange)
    if nb is None:
        return None
    if nb:
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

"""Pure order-book execution with explicit quote and native financial units."""

from collections.abc import Mapping, Sequence
from decimal import Decimal, localcontext
from typing import Literal

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.market.book_contracts import BookLevel, Number
from tank.market.numbers import decimal

type ExecutionSide = Literal["ask", "bid"]
type ExecutionLevelWire = BookLevel | Sequence[Number] | Mapping[str, Number]

_UNFILLED_QUOTE_TOLERANCE = Decimal.from_float(1e-8)


class BookWalkRequest(DomainModel):
    levels: list[BookLevel]
    quote_notional: Decimal = Field(gt=0)
    mid_quote_price: Decimal = Field(gt=0)
    side: ExecutionSide


class BookWalkResult(DomainModel):
    filled_quote_notional: Decimal = Field(ge=0)
    filled_native_quantity: Decimal = Field(ge=0)
    remaining_quote_notional: Decimal = Field(ge=0)
    complete: bool
    slippage_bps: float | None
    side: ExecutionSide
    reason: Literal["INSUFFICIENT_BOOK_DEPTH"] | None


def execution_request(
    levels: Sequence[ExecutionLevelWire],
    quote_notional: JsonValue | Decimal,
    mid_quote_price: JsonValue | Decimal,
    side: ExecutionSide,
) -> BookWalkRequest:
    """Decode wire levels once; retain provider lexical quantities in their validated model."""
    parsed = []
    for level in levels:
        if isinstance(level, BookLevel):
            parsed.append(level)
        elif isinstance(level, Mapping):
            parsed.append(BookLevel.from_wire(level["price"], level["qty"]))
        else:
            if len(level) < 2:
                raise ValueError("Execution level requires price and quantity")
            parsed.append(BookLevel.from_wire(level[0], level[1]))
    return BookWalkRequest(
        levels=parsed,
        quote_notional=decimal(quote_notional),
        mid_quote_price=decimal(mid_quote_price),
        side=side,
    )


def execute_book_walk(request: BookWalkRequest) -> BookWalkResult:
    """Signed basis points use supplied level prices; side labels the execution direction."""
    values = [
        request.quote_notional,
        request.mid_quote_price,
        *(value for level in request.levels for value in (level.price, level.quantity)),
    ]
    span = (
        max(value.adjusted() for value in values)
        - min(int(value.as_tuple().exponent) for value in values)
        + 1
    )
    with localcontext() as context:
        context.prec = max(128, 4 * span + len(str(len(request.levels))) + 32)
        remaining = request.quote_notional
        native = Decimal(0)
        spent = Decimal(0)
        for level in request.levels:
            take = min(remaining, level.price * level.quantity)
            native += take / level.price
            spent += take
            remaining -= take
            if remaining <= _UNFILLED_QUOTE_TOLERANCE:
                break
        complete = native > 0 and remaining <= _UNFILLED_QUOTE_TOLERANCE
        slippage = float((spent / native / request.mid_quote_price - 1) * 10000) if complete else None
        return BookWalkResult(
            filled_quote_notional=spent,
            filled_native_quantity=native,
            remaining_quote_notional=remaining,
            complete=complete,
            slippage_bps=slippage,
            side=request.side,
            reason=None if complete else "INSUFFICIENT_BOOK_DEPTH",
        )

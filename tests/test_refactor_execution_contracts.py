"""Quote and native quantities remain exact through complete and incomplete book execution."""

from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError

from tank.analysis.execution import execute_book_walk, execution_request


def test_complete_execution_preserves_quote_and_native_units() -> None:
    result = execute_book_walk(execution_request([["100", "1000"], ["101", "1000"]], "100000", "100", "ask"))
    assert result.complete is True
    assert result.filled_quote_notional == Decimal("100000")
    assert result.filled_native_quantity == Decimal("1000")
    assert result.remaining_quote_notional == 0
    assert result.slippage_bps == 0
    assert result.reason is None


def test_partial_book_reports_observed_fill_without_inventing_a_complete_price() -> None:
    result = execute_book_walk(execution_request([["100", "1"]], "1000", "100", "ask"))
    assert result.complete is False
    assert result.filled_quote_notional == 100
    assert result.filled_native_quantity == 1
    assert result.remaining_quote_notional == 900
    assert result.slippage_bps is None
    assert result.reason == "INSUFFICIENT_BOOK_DEPTH"


def test_execution_retains_atomic_quantity_with_a_huge_integer_part() -> None:
    quantity = Decimal("1" + "0" * 150 + "." + "0" * 39 + "1")
    price = Decimal("1.00000000000000000001")
    with localcontext() as context:
        context.prec = 300
        notional = quantity * price
    result = execute_book_walk(execution_request([[price, quantity]], notional, price, "ask"))
    assert result.complete is True
    assert result.filled_quote_notional == notional
    assert result.filled_native_quantity == quantity
    assert result.remaining_quote_notional == 0
    assert result.slippage_bps == 0


@pytest.mark.parametrize("notional,mid", [("0", "100"), ("100", "0"), ("-1", "100")])
def test_execution_rejects_invalid_required_financial_inputs(notional: str, mid: str) -> None:
    with pytest.raises(ValidationError):
        execution_request([["100", "1"]], notional, mid, "ask")

"""Final tool serialization protects internal fields without damaging exact numeric values."""

from decimal import Decimal

from tank.agents.wire import tool_wire_result
from tank.contracts import DomainModel


class ProjectedPrice(DomainModel):
    event_id: str
    text: str
    price: Decimal


def test_strict_domain_result_is_redacted_before_decimal_serialization() -> None:
    original = ProjectedPrice(
        event_id="EVT-" + "A" * 26,
        text="Exact public amount",
        price=Decimal("1234567890123456789012345678901234"),
    )
    wire = tool_wire_result(original)
    assert wire == {
        "text": "Exact public amount",
        "price": "1234567890123456789012345678901234",
    }
    assert original.event_id == "EVT-" + "A" * 26


def test_nested_tuple_domain_results_cannot_bypass_private_field_guard() -> None:
    original = (
        ProjectedPrice(event_id="EVT-" + "A" * 26, text="First", price=Decimal("1.25")),
        ProjectedPrice(event_id="EVT-" + "B" * 26, text="Second", price=Decimal("2.50")),
    )
    assert tool_wire_result(original) == [
        {"text": "First", "price": "1.25"},
        {"text": "Second", "price": "2.50"},
    ]

"""Behavior contracts for validated market calculations and admission."""

import asyncio
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.market.funding import FundingObservation
from tank.market.normalize import funding
from tank.market.rate_gate import PriorityGate


def observation(**changes):
    return FundingObservation.model_validate(
        dict(
            instrument_id="okx:BTC",
            token_id="bitcoin:BTC",
            kind="SETTLED",
            rate=Decimal("0"),
            interval_h=Decimal("4"),
            timestamp="2026-06-17T00:00:00Z",
            source_clock=True,
            metric_key="okx.swap.funding",
        )
        | changes
    )


def test_funding_requires_finite_typed_rate_positive_interval_and_declared_identity():
    for change in (
        {"rate": "0.1"},
        {"rate": Decimal("NaN")},
        {"interval_h": Decimal("0")},
        {"instrument_id": ""},
        {"unexpected": True},
        {"timestamp": "2026-06-17T00:00:00"},
        {"timestamp": "not-a-timestamp"},
    ):
        with pytest.raises(ValidationError):
            observation(**change)
    assert observation(rate=Decimal("0.00000000000000000001")).rate_8h_eq == Decimal("0.00000000000000000002")


def test_realized_zero_funding_is_preserved_instead_of_replaced_by_forecast():
    result = funding(
        {
            "instrument_id": "okx:BTC",
            "token_id": "bitcoin:BTC",
            "venue": "okx",
            "market_type": "PERP_LINEAR",
        },
        {"realizedRate": 0, "fundingRate": "0.001", "fundingTime": 1781654400000},
        kind="SETTLED",
        interval_h=4,
        received_ts=1781654400012,
    )
    assert Decimal(result["rate"]) == 0
    assert Decimal(result["rate_8h_eq"]) == 0
    assert result["settlement_time_prec"] == "EXACT"


@pytest.mark.parametrize("capacity", [0, -1, True])
def test_gate_rejects_capacity_that_cannot_admit_work(capacity):
    with pytest.raises(ValueError, match="positive integer"):
        PriorityGate(capacity)


async def test_cancelled_waiter_does_not_lose_capacity_or_reorder_priority():
    gate = PriorityGate()
    admitted = []

    async def worker(name, priority):
        async with gate.enter(priority):
            admitted.append(name)

    async with gate.enter():
        cancelled = asyncio.create_task(worker("cancelled", -1))
        repair = asyncio.create_task(worker("repair", 10))
        baseline = asyncio.create_task(worker("baseline", 0))
        await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
    await asyncio.gather(repair, baseline)
    assert admitted == ["baseline", "repair"]
    assert gate.active == 0
    async with gate.enter():
        assert gate.active == 1


@pytest.mark.parametrize(
    "price,quantity",
    [(True, "1"), ("NaN", "1"), ("1", "Infinity"), ("1", "-1"), ("not-price", "1"), ("1", "not-quantity")],
)
def test_order_book_rejects_invalid_level_quantities(price, quantity):
    from tank.market.book_contracts import BookLevel

    with pytest.raises(ValueError):
        BookLevel.from_wire(price, quantity)


def test_order_book_preserves_decimal_checksum_spelling_and_unavailable_usd():
    from tank.market.book_contracts import BookLevel
    from tank.market.books import OrderBook

    level = BookLevel.from_wire(Decimal("0.00000010"), Decimal("0.0100"))
    assert (level.price_spelling, level.quantity_spelling) == ("0.00000010", "0.0100")
    book = OrderBook(
        {
            "instrument_id": "coinbase:BTC",
            "token_id": "bitcoin:BTC",
            "venue": "coinbase",
            "market_type": "SPOT",
            "venue_symbol": "BTC-USDT",
            "quote_asset": "USDT",
        }
    )
    book.snapshot([["99", "2"], ["95", "3"]], [["101", "4"], ["105", "5"]], sequence=1, ts=1781654400000)
    summary = book.summary(received_ts=1781654400012)
    assert summary["quote_usd_rate"] is None
    assert summary["depth"]["bid"]["p1"]["native"] == "2"
    assert summary["depth"]["bid"]["p1"]["usd"] is None
    frozen = book.frozen()
    book.update([["99", "0"]], [], sequence=2, ts=1781654400013)
    assert frozen.summary()["best_bid"] == "99"
    assert book.summary()["best_bid"] == "95"


@pytest.mark.parametrize(
    "changes",
    [{"venue": "unknown"}, {"market_type": "TYPO"}, {"venue": "coinbase", "market_type": "PERP_LINEAR"}],
)
def test_book_instrument_rejects_unsupported_identity_instead_of_assuming_spot(changes):
    from tank.market.book_contracts import BookInstrument

    with pytest.raises(ValidationError):
        BookInstrument.from_wire(
            {
                "instrument_id": "venue:BTC",
                "token_id": "bitcoin:BTC",
                "venue": "coinbase",
                "market_type": "SPOT",
                "venue_symbol": "BTC-USD",
            }
            | changes
        )


def test_event_anchor_requires_verified_exact_event_time_and_preserves_public_precision():
    from tank.market.capture import event_anchor

    event = {
        "event_time": "2026-06-17T00:00:00Z",
        "event_time_prec": "EXACT",
        "state_verified": False,
        "public_at": "2026-06-17T01:00:00Z",
        "public_at_prec": "BUCKET(1h)",
        "known_to_us_at": "2026-06-17T02:00:00Z",
        "known_to_us_at_prec": "COLLECTION_ONLY",
    }
    selected = event_anchor(event)
    assert selected.type == "T1"
    assert selected.value == event["public_at"]
    assert selected.timestamp.precision == "BUCKET(1h)"
    assert event_anchor(event | {"state_verified": True}).type == "T0"
    with pytest.raises(ValidationError):
        event_anchor(event | {"state_verified": "true"})


def test_capture_boundaries_require_anchor_timestamp_and_include_mirrored_window():
    from tank.market.capture import capture_boundary_points
    from tank.util import parse_time

    points = capture_boundary_points("2026-06-17T04:00:00Z", {"ts": "2026-06-17T03:00:00Z"})
    assert parse_time("2026-06-17T02:00:00Z") in points
    assert parse_time("2026-06-17T03:00:00Z") in points
    with pytest.raises(KeyError):
        capture_boundary_points("2026-06-17T04:00:00Z", {})

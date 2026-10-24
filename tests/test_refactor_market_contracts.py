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

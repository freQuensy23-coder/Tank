"""Per-host admission: several requests in flight, starts spaced by the host interval."""

import asyncio
import time

import httpx
import pytest
from test_market_service import setup  # noqa: F401

from tank.market.rate_gate import PriorityGate
from tank.market.service import MarketService


@pytest.mark.asyncio
async def test_gate_admits_capacity_holders_then_lowest_priority_first():
    gate = PriorityGate(2)
    order, release = [], asyncio.Event()

    async def hold(name, priority):
        async with gate.enter(priority):
            order.append(name)
            await release.wait()

    holders = [asyncio.create_task(hold(n, p)) for n, p in (("a", 5), ("b", 5), ("low", 9), ("high", 0))]
    await asyncio.sleep(0.01)
    assert order == ["a", "b"]  # two in flight, two waiting
    release.set()
    await asyncio.gather(*holders)
    assert order == ["a", "b", "high", "low"]


@pytest.mark.asyncio
async def test_concurrent_requests_overlap_but_start_one_interval_apart(setup):  # noqa: F811
    repo, settings, _ = setup
    starts, active, peak = [], [0], [0]

    async def handler(request):
        starts.append(time.monotonic())
        active[0] += 1
        peak[0] = max(peak[0], active[0])
        await asyncio.sleep(0.3)
        active[0] -= 1
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = MarketService(repo, settings, http=http)
        await asyncio.gather(
            *(service._request("GET", "https://api.bybit.com/v5/market/time") for _ in range(4))
        )
    gaps = [b - a for a, b in zip(sorted(starts), sorted(starts)[1:], strict=False)]
    assert peak[0] > 1  # several in flight
    assert all(gap >= 0.07 for gap in gaps)  # never a burst: 0.08 s host interval


@pytest.mark.asyncio
async def test_a_negative_priority_request_takes_a_full_budget_first():
    from tank.sources.rate_limits import SharedRequestBudgets

    budgets = SharedRequestBudgets()
    await budgets.acquire("host", 1, 0.2)  # the window is full
    order = []

    async def request(name, priority):
        await budgets.acquire("host", 1, 0.2, priority=priority)
        order.append(name)

    repairs = [asyncio.create_task(request(f"repair{i}", 1)) for i in range(3)]
    await asyncio.sleep(0.05)  # the repairs wait first
    ticker = asyncio.create_task(request("ticker", -1))
    await asyncio.wait_for(asyncio.gather(ticker, *repairs), 5)
    assert order[0] == "ticker" and sorted(order[1:]) == ["repair0", "repair1", "repair2"]
    assert budgets.urgent[("host", 0.2, 1.0)] == 0

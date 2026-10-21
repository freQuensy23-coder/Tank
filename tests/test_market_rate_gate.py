"""Per-host admission: several requests in flight, starts spaced by the host interval."""

import asyncio
import time
from types import SimpleNamespace

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
    release = asyncio.Event()

    async def handler(request):
        starts.append(time.monotonic())
        active[0] += 1
        peak[0] = max(peak[0], active[0])
        if len(starts) == 4:
            release.set()
        await release.wait()
        active[0] -= 1
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = MarketService(repo, settings, http=http)
        await asyncio.gather(
            *(service._request("GET", "https://api.llama.fi/summary/fees/abc") for _ in range(4))
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


class ControlledClock:
    """Drive only the SUT module's clock/sleep; pytest's event loop stays real."""

    def __init__(self):
        self.now = 10.0
        self.sleeping = asyncio.Queue()
        self.pending = []

    def monotonic(self):
        return self.now

    async def sleep(self, delay):
        future = asyncio.get_running_loop().create_future()
        self.pending.append((self.now + delay, future))
        self.sleeping.put_nowait(delay)
        await future

    def advance(self, seconds):
        self.now += seconds
        ready, self.pending = self.pending, []
        for deadline, future in ready:
            if future.done():
                continue
            if deadline <= self.now + 1e-9:
                future.set_result(None)
            else:
                self.pending.append((deadline, future))


def control_request_clock(monkeypatch):
    from tank.market import service as module

    clock = ControlledClock()
    monkeypatch.setattr(module, "time", SimpleNamespace(**(vars(time) | {"monotonic": clock.monotonic})))
    monkeypatch.setattr(module, "asyncio", SimpleNamespace(**(vars(asyncio) | {"sleep": clock.sleep})))
    return clock


@pytest.mark.asyncio
async def test_overdue_request_starts_are_spaced_from_actual_admission(setup, monkeypatch):  # noqa: F811
    repo, settings, _ = setup
    clock = control_request_clock(monkeypatch)
    starts, active, peak = [], 0, 0
    entered, release = asyncio.Queue(), asyncio.Event()

    async def handler(request):
        nonlocal active, peak
        starts.append(clock.monotonic())
        active += 1
        peak = max(peak, active)
        entered.put_nowait(None)
        try:
            await release.wait()
        finally:
            active -= 1
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = MarketService(repo, settings, http=http)
        requests = [
            asyncio.create_task(service._request("GET", "https://api.llama.fi/summary/fees/abc"))
            for _ in range(4)
        ]
        try:
            await entered.get()
            await clock.sleeping.get()
            # Simulate a blocked loop resuming after every old reserved slot expired.
            clock.advance(0.5)
            await entered.get()
            for _ in range(2):
                await clock.sleeping.get()
                clock.advance(0.08)
                await entered.get()
            assert starts == pytest.approx([10.0, 10.5, 10.58, 10.66])
            assert peak == 4  # The pacing lock must never cover the HTTP request.
        finally:
            release.set()
            await asyncio.gather(*requests)


@pytest.mark.asyncio
async def test_cancelled_pacing_wait_does_not_reserve_a_future_start(setup, monkeypatch):  # noqa: F811
    repo, settings, _ = setup
    clock = control_request_clock(monkeypatch)
    starts, entered, release = [], asyncio.Queue(), asyncio.Event()

    async def handler(request):
        starts.append(clock.monotonic())
        entered.put_nowait(None)
        await release.wait()
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = MarketService(repo, settings, http=http)
        first = asyncio.create_task(service._request("GET", "https://api.llama.fi/summary/fees/abc"))
        waiting = following = None
        try:
            await entered.get()
            waiting = asyncio.create_task(service._request("GET", "https://api.llama.fi/summary/fees/abc"))
            assert await clock.sleeping.get() == pytest.approx(0.08)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            following = asyncio.create_task(service._request("GET", "https://api.llama.fi/summary/fees/abc"))
            delay = await clock.sleeping.get()
            assert delay == pytest.approx(0.08)
            clock.advance(delay)
            await entered.get()
            assert starts == pytest.approx([10.0, 10.08])
        finally:
            release.set()
            for task in (waiting, following):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (first, waiting, following) if task is not None), return_exceptions=True
            )

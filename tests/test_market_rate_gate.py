"""Configured provider admission preserves concurrency and actual start-time budgets."""

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from source_support import (
    FixtureBudgetClock,
    fixture_http_admission,
    fixture_request_budgets,
    fixture_versions,
)

from tank.market.rate_gate import PriorityGate
from tank.market.service import MarketService
from tank.request_budget import MemoryBudgetBackend, SharedRequestBudgets, SystemBudgetClock


@pytest.mark.asyncio
async def test_gate_admits_capacity_holders_then_lowest_priority_first() -> None:
    gate = PriorityGate(2)
    order, release = [], asyncio.Event()

    async def hold(name, priority) -> None:
        async with gate.enter(priority):
            order.append(name)
            await release.wait()

    holders = [asyncio.create_task(hold(n, p)) for n, p in (("a", 5), ("b", 5), ("low", 9), ("high", 0))]
    await asyncio.sleep(0.01)
    assert order == ["a", "b"]
    release.set()
    await asyncio.gather(*holders)
    assert order == ["a", "b", "high", "low"]


@pytest.mark.asyncio
async def test_unconfigured_requests_overlap_without_invented_provider_interval(setup) -> None:
    repo, settings, _ = setup
    clock = FixtureBudgetClock()
    admission = fixture_http_admission(budgets=fixture_request_budgets(clock=clock))
    starts: list[float] = []
    active, peak = 0, 0
    release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak
        starts.append(clock.elapsed)
        active += 1
        peak = max(peak, active)
        if len(starts) == 4:
            release.set()
        await release.wait()
        active -= 1
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = MarketService(repo, settings, http=http, versions=fixture_versions(), admission=admission)
        await asyncio.gather(
            *(service._request("GET", "https://api.llama.fi/summary/fees/abc") for _ in range(4))
        )
    assert peak == 4 and starts == [0.0, 0.0, 0.0, 0.0]
    assert clock.elapsed == 0.0


@pytest.mark.asyncio
async def test_a_negative_priority_request_takes_a_full_budget_first() -> None:
    budgets = SharedRequestBudgets(MemoryBudgetBackend(SystemBudgetClock()))
    await budgets.acquire("host", 1, 0.2)
    order = []

    async def request(name, priority) -> None:
        await budgets.acquire("host", 1, 0.2, priority=priority)
        order.append(name)

    repairs = [asyncio.create_task(request(f"repair{i}", 1)) for i in range(3)]
    await asyncio.sleep(0.05)
    ticker = asyncio.create_task(request("ticker", -1))
    await asyncio.wait_for(asyncio.gather(ticker, *repairs), 5)
    assert order[0] == "ticker" and sorted(order[1:]) == ["repair0", "repair1", "repair2"]
    assert list(budgets.urgent.values()) == [0]


class ControlledClock:
    """Drive only the SUT module's clock/sleep; pytest's event loop stays real."""

    def __init__(self) -> None:
        self.elapsed = 10.0
        self.sleeping: asyncio.Queue[float] = asyncio.Queue()
        self.pending: list[tuple[float, asyncio.Future[None]]] = []

    def monotonic(self) -> float:
        return self.elapsed

    def now(self) -> datetime:
        return datetime(2026, 10, 1, tzinfo=UTC) + timedelta(seconds=self.elapsed)

    async def sleep(self, delay: float) -> None:
        future = asyncio.get_running_loop().create_future()
        self.pending.append((self.elapsed + delay, future))
        self.sleeping.put_nowait(delay)
        await future

    def advance(self, seconds: float) -> None:
        self.elapsed += seconds
        ready, self.pending = self.pending, []
        for deadline, future in ready:
            if future.done():
                continue
            if deadline <= self.elapsed + 1e-9:
                future.set_result(None)
            else:
                self.pending.append((deadline, future))


@pytest.mark.asyncio
async def test_overdue_request_starts_are_spaced_from_actual_admission(setup) -> None:
    repo, settings, _ = setup
    clock = ControlledClock()
    starts, active, peak = [], 0, 0
    entered, release = asyncio.Queue(), asyncio.Event()

    async def handler(request: httpx.Request):
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

    async with httpx.AsyncClient(
        params={"apikey": "fixture-key"}, transport=httpx.MockTransport(handler)
    ) as http:
        service = MarketService(
            repo,
            settings,
            http=http,
            versions=fixture_versions(),
            admission=fixture_http_admission(budgets=fixture_request_budgets(clock=clock)),
        )
        requests = [
            asyncio.create_task(service._request("GET", "https://api.etherscan.io/v2/api?chainid=1"))
            for _ in range(4)
        ]
        try:
            await entered.get()
            await clock.sleeping.get()

            clock.advance(2.0)
            await entered.get()
            for _ in range(2):
                await clock.sleeping.get()
                clock.advance(1.0)
                await entered.get()
            assert starts == pytest.approx([10.0, 12.0, 13.0, 14.0])
            assert peak == 4
        finally:
            release.set()
            await asyncio.gather(*requests)


@pytest.mark.asyncio
async def test_cancelled_pacing_wait_does_not_reserve_a_future_start(setup) -> None:
    repo, settings, _ = setup
    clock = ControlledClock()
    starts, entered, release = [], asyncio.Queue(), asyncio.Event()

    async def handler(request: httpx.Request):
        starts.append(clock.monotonic())
        entered.put_nowait(None)
        await release.wait()
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(
        params={"apikey": "fixture-key"}, transport=httpx.MockTransport(handler)
    ) as http:
        service = MarketService(
            repo,
            settings,
            http=http,
            versions=fixture_versions(),
            admission=fixture_http_admission(budgets=fixture_request_budgets(clock=clock)),
        )
        first = asyncio.create_task(service._request("GET", "https://api.etherscan.io/v2/api?chainid=1"))
        waiting = following = None
        try:
            await entered.get()
            waiting = asyncio.create_task(
                service._request("GET", "https://api.etherscan.io/v2/api?chainid=1")
            )
            assert await clock.sleeping.get() == pytest.approx(1.0)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            following = asyncio.create_task(
                service._request("GET", "https://api.etherscan.io/v2/api?chainid=1")
            )
            delay = await clock.sleeping.get()
            assert delay == pytest.approx(1.0)
            clock.advance(delay)
            await entered.get()
            assert starts == pytest.approx([10.0, 11.0])
        finally:
            release.set()
            for task in (waiting, following):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (first, waiting, following) if task is not None), return_exceptions=True
            )


@pytest.mark.asyncio
async def test_delayed_response_frees_saturated_slot_without_restarting_pacer(setup) -> None:
    repo, settings, _ = setup
    clock = ControlledClock()
    starts, entered = [], asyncio.Queue()
    releases = [asyncio.Event() for _ in range(4)]

    async def handler(request: httpx.Request):
        index = len(starts)
        starts.append(clock.monotonic())
        entered.put_nowait(index)
        if index < 4:
            await releases[index].wait()
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(
        params={"apikey": "fixture-key"}, transport=httpx.MockTransport(handler)
    ) as http:
        service = MarketService(
            repo,
            settings,
            http=http,
            versions=fixture_versions(),
            admission=fixture_http_admission(budgets=fixture_request_budgets(clock=clock)),
        )
        requests, observers = [], []
        try:
            for index in range(4):
                requests.append(
                    asyncio.create_task(service._request("GET", "https://api.etherscan.io/v2/api?chainid=1"))
                )
                if index:
                    clock.advance(await clock.sleeping.get())
                assert await entered.get() == index
            requests.append(
                asyncio.create_task(service._request("GET", "https://api.etherscan.io/v2/api?chainid=1"))
            )
            await asyncio.sleep(0)
            assert len(service._request_locks["api.etherscan.io"].waiters) == 1
            clock.advance(2.0)
            released_at = clock.monotonic()
            releases[0].set()
            await requests[0]
            started = asyncio.create_task(entered.get())
            sleeping = asyncio.create_task(clock.sleeping.get())
            observers = [started, sleeping]
            await asyncio.wait(observers, return_when=asyncio.FIRST_COMPLETED)
            assert not sleeping.done(), "Response completion restarted the host's start interval"
            assert await started == 4
            assert starts[-1] == released_at
            assert all(not task.done() for task in requests[1:4])
            await requests[-1]
        finally:
            for event in releases:
                event.set()
            for task in [*requests, *observers]:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*requests, *observers, return_exceptions=True)

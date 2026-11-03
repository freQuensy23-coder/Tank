import asyncio
import pickle
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from tank.market import service as service_module
from tank.market.service import MarketService
from tank.storage import MemoryRepository
from tank.util import iso

BASE = datetime(2026, 10, 21, tzinfo=UTC)
COLLECTION = "mkt.book_summaries"
IDS = ["binance:spot:BTCUSDT", "okx:spot:BTC-USDT"]


@pytest.fixture
async def warmed(monkeypatch):
    monkeypatch.setattr(service_module, "utcnow", lambda: BASE + timedelta(days=1))
    repo = MemoryRepository()
    await repo.open()
    service = object.__new__(MarketService)
    service.repo = repo
    await repo.put_market_batch(
        COLLECTION,
        [
            {
                "instrument_id": iid,
                "token_id": "bitcoin:BTC",
                "exchange_ts": iso(BASE + timedelta(minutes=minute)),
                "recorded_at": iso(BASE),
                "mid": str(minute + 1),
                "integrity_ok": True,
                "bids": [[str(minute + 1), "1"]],
                "asks": [[str(minute + 2), "1"]],
            }
            for iid in IDS
            for minute in range(110)
        ],
    )
    for low, high in ((0, 40), (60, 100)):
        await read(service, low, high)
    assert len(next(iter(service._windows.values()))) == 2
    return service


async def read(service, low, high):
    return await service._ranged(
        COLLECTION,
        "exchange_ts",
        IDS,
        BASE + timedelta(minutes=low),
        BASE + timedelta(minutes=high),
        books=True,
    )


@pytest.mark.parametrize("phase", ["cold", "late", "forward"])
@pytest.mark.parametrize("failure", ["error", "cancel"])
async def test_failed_read_keeps_every_committed_lane(warmed, monkeypatch, phase, failure):
    service = warmed
    before = pickle.dumps(service._windows)
    committed = deepcopy(service._windows)
    reached, release = asyncio.Event(), asyncio.Event()
    original = service._ranged_read if phase != "late" else service_module.market_rows_recorded_since

    async def interrupted(*args, **kwargs):
        await original(*args, **kwargs)
        reached.set()
        await release.wait()
        raise httpx.ReadError("interrupted window read")

    target = service if phase != "late" else service_module
    name = "_ranged_read" if phase != "late" else "market_rows_recorded_since"
    monkeypatch.setattr(target, name, interrupted)
    low, high = (-20, -10) if phase == "cold" else (5, 50 if phase == "forward" else 40)
    task = asyncio.create_task(read(service, low, high))
    try:
        await asyncio.wait_for(reached.wait(), 1)
        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        else:
            release.set()
            with pytest.raises(httpx.ReadError):
                await asyncio.wait_for(task, 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert service._windows == committed
    assert pickle.dumps(service._windows) == before
    assert all(not lock.locked() for lock in service._window_locks.values())
    monkeypatch.setattr(target, name, original)
    actual = await asyncio.wait_for(read(service, low, high), 1)
    expected = await service._ranged_read(
        COLLECTION,
        "exchange_ts",
        IDS,
        BASE + timedelta(minutes=low),
        BASE + timedelta(minutes=high),
        books=True,
    )
    assert actual == expected


async def test_failed_book_placement_keeps_committed_rows_and_request_rows(warmed, monkeypatch):
    service = warmed
    requested = await read(service, 0, 40)
    requested_before = deepcopy(requested)
    before = pickle.dumps(service._windows)
    old_row = next(iter(service._windows.values()))[0]["rows"][IDS[0]][20][2]
    original = service_module._place_levels
    calls = 0

    def interrupted(points):
        nonlocal calls
        original(points)
        calls += 1
        if calls == 2:
            raise ValueError("book placement failed")

    monkeypatch.setattr(service_module, "_place_levels", interrupted)
    with pytest.raises(ValueError, match="book placement failed"):
        await read(service, 5, 50)
    assert pickle.dumps(service._windows) == before
    assert requested == requested_before
    assert all(not lock.locked() for lock in service._window_locks.values())
    monkeypatch.setattr(service_module, "_place_levels", original)
    actual = await read(service, 5, 50)
    expected = await service._ranged_read(
        COLLECTION,
        "exchange_ts",
        IDS,
        BASE + timedelta(minutes=5),
        BASE + timedelta(minutes=50),
        books=True,
    )
    assert actual == expected
    assert requested == requested_before
    new_lane = next(iter(service._windows.values()))[0]
    reused_row = next(point[2] for point in new_lane["rows"][IDS[0]] if point[2] == old_row)
    assert reused_row is old_row


@pytest.mark.parametrize("failure", [False, True])
async def test_other_key_eviction_is_not_rolled_back(warmed, monkeypatch, failure):
    service = warmed
    key = next(iter(service._windows))
    original = service_module.market_rows_recorded_since
    reached, release = asyncio.Event(), asyncio.Event()

    async def interrupted(*args, **kwargs):
        result = await original(*args, **kwargs)
        reached.set()
        await release.wait()
        if failure:
            raise httpx.ReadError("interrupted after eviction")
        return result

    monkeypatch.setattr(service_module, "market_rows_recorded_since", interrupted)
    task = asyncio.create_task(read(service, 0, 40))
    try:
        await asyncio.wait_for(reached.wait(), 1)
        monkeypatch.setattr(service_module, "WINDOW_ROWS", 1)
        service_module._bound_windows(service._windows)
        assert key not in service._windows
        release.set()
        if failure:
            with pytest.raises(httpx.ReadError):
                await asyncio.wait_for(task, 1)
        else:
            assert await asyncio.wait_for(task, 1)
        assert key not in service._windows
        assert all(not lock.locked() for lock in service._window_locks.values())
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

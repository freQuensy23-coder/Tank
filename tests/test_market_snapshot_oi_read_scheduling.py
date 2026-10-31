import asyncio
from datetime import timedelta

import httpx
import pytest
from test_market_service import setup as setup

from tank.market import service
from tank.market.service import MarketService
from tank.util import parse_time

T0 = parse_time("2026-06-17T00:00:00Z")


async def test_oi_history_and_recent_reads_overlap_without_changing_population(setup, monkeypatch):
    repo, settings, inst = setup
    historical = dict(inst, ts=(T0 - timedelta(days=3)).isoformat(), metric_key="BASE", oi_native="100.00")
    recent = dict(inst, ts=(T0 - timedelta(hours=1)).isoformat(), metric_key="BASE", oi_native="101.00")
    for key, row in (("old", historical), ("new", recent)):
        await repo.put("mkt.oi_obs", key, row)
    entered = set()
    both = asyncio.Event()
    original = service.sampled_market_history
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)
        window = market._window_rows

        async def rendezvous(which):
            entered.add(which)
            if len(entered) == 2:
                both.set()
            async with asyncio.timeout(1):
                await both.wait()

        async def sampled(*args, **kwargs):
            await rendezvous("historical")
            return await original(*args, **kwargs)

        async def recent_rows(token_id, collection, *args, **kwargs):
            if collection == "mkt.oi_obs":
                await rendezvous("recent")
            return await window(token_id, collection, *args, **kwargs)

        monkeypatch.setattr(service, "sampled_market_history", sampled)
        monkeypatch.setattr(market, "_window_rows", recent_rows)
        result = await market.snapshot(inst["token_id"], T0)
    assert entered == {"historical", "recent"}
    assert [(row["oi_native"], parse_time(row["ts"])) for row in result["oi"]] == [
        ("100.00", T0 - timedelta(days=3)),
        ("101.00", T0 - timedelta(hours=1)),
    ]


@pytest.mark.parametrize("cancel", [False, True])
async def test_owned_oi_history_task_is_drained_when_recent_read_fails(setup, monkeypatch, cancel):
    repo, settings, inst = setup
    started = asyncio.Event()
    active = set()
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)
        window = market._window_rows

        async def sampled(*args, **kwargs):
            active.add("historical")
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                active.remove("historical")

        async def recent_rows(token_id, collection, *args, **kwargs):
            if collection == "mkt.oi_obs":
                async with asyncio.timeout(1):
                    await started.wait()
                if not cancel:
                    raise ValueError("recent OI read")
                await asyncio.Event().wait()
            return await window(token_id, collection, *args, **kwargs)

        monkeypatch.setattr(service, "sampled_market_history", sampled)
        monkeypatch.setattr(market, "_window_rows", recent_rows)
        task = asyncio.create_task(market.snapshot(inst["token_id"], T0))
        try:
            async with asyncio.timeout(2):
                if cancel:
                    await started.wait()
                    task.cancel()
                with pytest.raises(
                    asyncio.CancelledError if cancel else ValueError,
                    match=None if cancel else "recent OI read",
                ):
                    await task
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert not active

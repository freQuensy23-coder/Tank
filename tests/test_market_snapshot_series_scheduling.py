import asyncio

import httpx
import pytest
from test_market_service import setup as setup

from tank.market import service as market_module
from tank.market.service import MarketService
from tank.util import iso, parse_time


async def test_selected_peer_series_starts_before_current_series_is_awaited(setup, monkeypatch):
    repo, settings, inst = setup
    cutoff = parse_time("2026-06-17T00:00:00Z")
    monkeypatch.setattr(market_module, "utcnow", lambda: cutoff)
    monkeypatch.setattr(market_module, "iso", lambda value=None: iso(cutoff if value is None else value))
    peer_started = asyncio.Event()
    async with httpx.AsyncClient() as http:
        reference = MarketService(repo, settings, http=http)
        expected = await reference.snapshot(inst["token_id"], cutoff)
        market = MarketService(repo, settings, http=http)
        original = market._condition_kline_series

        async def series(instrument, *args, **kwargs):
            if instrument["token_id"] != inst["token_id"]:
                peer_started.set()
            elif kwargs["kind"] == "price":
                await peer_started.wait()
            return await original(instrument, *args, **kwargs)

        monkeypatch.setattr(market, "_condition_kline_series", series)
        async with asyncio.timeout(0.5):
            actual = await market.snapshot(inst["token_id"], cutoff)
    assert actual == expected
    assert peer_started.is_set()
    assert set(actual["stat_price_series"]) == {
        inst["instrument_id"],
        "binance:spot:BTCUSD",
    }


@pytest.mark.parametrize("cancel", [False, True])
async def test_early_peer_series_is_drained_on_current_failure_or_cancellation(setup, monkeypatch, cancel):
    repo, settings, inst = setup
    peer_started = asyncio.Event()
    active = set()
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)

        async def series(instrument, *args, **kwargs):
            identity = (instrument["instrument_id"], kwargs["kind"])
            active.add(identity)
            try:
                if instrument["token_id"] != inst["token_id"]:
                    peer_started.set()
                    await asyncio.Event().wait()
                else:
                    await peer_started.wait()
                    if not cancel:
                        raise ValueError("current price failed")
                    await asyncio.Event().wait()
            finally:
                active.remove(identity)

        monkeypatch.setattr(market, "_condition_kline_series", series)
        task = asyncio.create_task(market.snapshot(inst["token_id"], parse_time("2026-06-17T00:00:00Z")))
        try:
            async with asyncio.timeout(0.5):
                await peer_started.wait()
                if cancel:
                    task.cancel()
                with pytest.raises(asyncio.CancelledError if cancel else ValueError):
                    await task
            assert active == set()
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

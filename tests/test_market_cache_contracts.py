"""Actual cache recovery and concurrent transport accounting at the service boundary."""

import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from pydantic import JsonValue
from source_support import fixture_http_admission, fixture_versions

from tank.market.service import MarketService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import clock_iso, iso, utcnow

type Setup = tuple[MemoryRepository, Settings, dict[str, JsonValue]]


@pytest.mark.asyncio
async def test_concurrent_requests_retain_every_success_and_failure(setup: Setup) -> None:
    repo, settings, _ = setup
    released = asyncio.Event()
    arrived = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal arrived
        arrived += 1
        if arrived >= 2:
            released.set()
        await released.wait()
        return httpx.Response(503 if request.url.path == "/failed" else 200, json={"observed": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = MarketService(
            repo, settings, versions=fixture_versions(), http=http, admission=fixture_http_admission()
        )
        results = await asyncio.wait_for(
            asyncio.gather(
                service._request("GET", "https://cache.example/first"),
                service._request("GET", "https://cache.example/second"),
                service._request("GET", "https://cache.example/failed"),
                return_exceptions=True,
            ),
            timeout=4,
        )
    assert sum(isinstance(result, RuntimeError) for result in results) == 1
    assert service._reads["cache.example"].succeeded == 2
    assert service._reads["cache.example"].failed == 1


@pytest.mark.asyncio
async def test_saved_day_counts_round_trip_as_a_real_cache_without_market_writes(setup: Setup) -> None:
    repo, settings, _ = setup
    day = utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    async with httpx.AsyncClient() as http:
        first = MarketService(
            repo, settings, versions=fixture_versions(), http=http, admission=fixture_http_admission()
        )
        first._kline_day_counts["binance:spot:ABCUSD", day] = 3
        await first._save_day_counts(force=True)
        reopened = MarketService(
            repo, settings, versions=fixture_versions(), http=http, admission=fixture_http_admission()
        )
        assert await reopened._load_day_counts() == 1
        assert reopened._kline_day_counts == {("binance:spot:ABCUSD", day): 3}
        assert not await repo.list("mkt.klines_1m")


@pytest.mark.parametrize("invalid", [True, -1, 1441])
@pytest.mark.asyncio
async def test_invalid_saved_count_never_becomes_a_known_market_observation(
    setup: Setup, invalid: int
) -> None:
    repo, settings, _ = setup
    day = utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    async with httpx.AsyncClient() as http:
        service = MarketService(
            repo, settings, versions=fixture_versions(), http=http, admission=fixture_http_admission()
        )
        path = service._day_counts_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "saved_at": clock_iso(),
                    "counts": [
                        ["binance:spot:ABCUSD", iso(day), 3],
                        ["binance:spot:ABCUSD", iso(day + timedelta(days=1)), invalid],
                    ],
                }
            )
        )
        original = path.read_bytes()
        assert await service._load_day_counts() == 0
        assert service._kline_day_counts == {}
        assert path.read_bytes() == original
        assert not await repo.list("mkt.klines_1m")


@pytest.mark.asyncio
async def test_conflicting_saved_day_entries_never_choose_the_first_count(setup: Setup) -> None:
    repo, settings, _ = setup
    day = utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    async with httpx.AsyncClient() as http:
        service = MarketService(
            repo, settings, versions=fixture_versions(), http=http, admission=fixture_http_admission()
        )
        path = service._day_counts_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "saved_at": clock_iso(),
                    "counts": [["binance:spot:ABCUSD", iso(day), 3], ["binance:spot:ABCUSD", iso(day), 4]],
                }
            )
        )
        assert await service._load_day_counts() == 0
        assert service._kline_day_counts == {}

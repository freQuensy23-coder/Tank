"""Whole-IP venue usage must not subtract sandbox traffic from service's half."""

import time

import httpx
import pytest
from test_market_service import setup  # noqa: F401

from tank.market.service import MarketService
from tank.sources.rate_limits import shared_request_budgets


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "host,ip_weight,expected_capacity,cooldown",
    [
        ("api.binance.com", 3000, 3000, False),
        ("api.binance.com", 5999, 3000, False),
        ("api.binance.com", 6000, 3000, True),
        ("fapi.binance.com", 1200, 1200, False),
        ("fapi.binance.com", 2399, 1200, False),
        ("fapi.binance.com", 2400, 1200, True),
    ],
)
async def test_ip_header_includes_competing_sandbox_half_but_service_keeps_own_budget(
    setup,  # noqa: F811
    monkeypatch,
    host,
    ip_weight,
    expected_capacity,
    cooldown,
):
    repo, settings, _ = setup
    charges = []

    async def acquire(key, capacity, window, *, weight=1, priority=0):
        charges.append((key, capacity, window, weight))

    monkeypatch.setattr(shared_request_budgets, "acquire", acquire)
    # The public header aggregates both workloads. Sandbox traffic may have
    # consumed its full half while this service performs its first cheap call.
    path = "/api/v3/ticker/24hr" if host == "api.binance.com" else "/fapi/v1/ticker/24hr"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={}, headers={"X-MBX-USED-WEIGHT-1M": str(ip_weight)})
        )
    ) as http:
        service = MarketService(repo, settings, http=http)
        await service._request_live("GET", "https://" + host + path, params={"symbol": "BTCUSDT"})
        assert charges == [
            (
                "binance.spot" if host == "api.binance.com" else "binance.usdm",
                expected_capacity,
                60,
                2 if host == "api.binance.com" else 1,
            )
        ]
        delay = service._last_request[host] - time.monotonic()
        assert (delay > 59) is cooldown
        await service.close()

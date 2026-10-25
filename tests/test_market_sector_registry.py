"""Actual MemoryRepository snapshot projections from the full configured registry."""

import copy
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import yaml
from source_support import fixture_versions

from tank.market.normalize import kline
from tank.market.service import MarketService
from tank.util import iso, parse_time

registry = yaml.safe_load((Path(__file__).resolve().parents[1] / "config/registry.yaml").read_text())


@pytest.mark.asyncio
async def test_full_registry_baskets_and_optional_sector_peer_prices_reach_normal_snapshot(setup) -> None:
    repo, settings, _ = setup
    captured = copy.deepcopy(registry)
    for token in captured["tokens"]:
        token.pop("sector", None)
    (settings.config_dir / "registry.yaml").write_text(yaml.safe_dump(captured))
    cutoff = parse_time("2026-10-01T12:00:00Z")
    peer = next(
        i
        for i in captured["instruments"]
        if i["token_id"] == "uniswap:UNI" and i["venue"] == "binance" and i["market_type"] == "SPOT"
    )
    stamp = cutoff - timedelta(minutes=1)
    row = kline(
        peer,
        [
            int(stamp.timestamp() * 1000),
            "1",
            "1",
            "1",
            "1",
            "2",
            int(cutoff.timestamp() * 1000) - 1,
            "2",
            1,
            "1",
            "1",
        ],
        now=iso(cutoff),
    )
    await repo.put_market_batch("mkt.klines_1m", [row], keys=["sector-peer"])
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http, versions=fixture_versions())

        snap = await service.snapshot("0x:ZRX", cutoff)
        assert snap["sector_baskets"] == registry["sector_baskets"]
        assert len(snap["tokens"]) == 22 and all("sector" not in t for t in snap["tokens"])
        assert any(r["instrument_id"] == peer["instrument_id"] for r in snap["klines"])
        snap["sector_baskets"][0]["constituents"].clear()
        assert service.sector_baskets == registry["sector_baskets"]
        conditional = await service.condition_snapshot(
            "world:WLD", cutoff, requirements=[{"metric": "mt.move_class", "window": "W4H"}]
        )
        assert conditional["sector_baskets"] == registry["sector_baskets"]
        assert peer["instrument_id"] in conditional["stat_price_series"]

        relative_head = conditional["domain_dependency_fingerprints"]["relative"]
        service.sector_baskets = copy.deepcopy(service.sector_baskets)
        service.sector_baskets[0]["constituents"].clear()
        changed = await service.condition_snapshot(
            "world:WLD", cutoff, requirements=[{"metric": "mt.move_class", "window": "W4H"}]
        )
        assert changed["domain_dependency_fingerprints"]["relative"] != relative_head

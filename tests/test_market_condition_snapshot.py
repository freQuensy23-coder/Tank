"""Selective producer reads must not flush capture or load unrelated histories."""

from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from tank.market.service import MarketService
from tank.util import iso, utcnow


@pytest.mark.asyncio
async def test_close_loader_reads_only_hour_ending_candle_and_reference_flow(setup):
    repo, settings, inst = setup
    calls = []
    original = repo.iter_range

    async def ranges(collection, field, **kwargs):
        calls.append((collection, field, kwargs))
        async for row in original(collection, field, **kwargs):
            yield row

    repo.iter_range = ranges
    end = utcnow().replace(minute=34, second=17, microsecond=0)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        await service.condition_snapshot(
            inst["token_id"], end, requirements=[{"metric": "price.close_vs_level", "window": "W1H"}]
        )
    assert {c[0] for c in calls} == {"mkt.flow_bars_1m", "mkt.klines_1m"}
    candle = next(c for c in calls if c[0] == "mkt.klines_1m")
    assert candle[2]["end"] - candle[2]["start"] == timedelta(minutes=1)
    assert candle[2]["end"] == end.replace(minute=0, second=0)
    flow = next(c for c in calls if c[0] == "mkt.flow_bars_1m")
    assert flow[2]["start"] == (end - timedelta(days=1)).replace(second=0)


@pytest.mark.asyncio
async def test_oi_condition_reads_the_latest_observation_and_not_the_history(setup):
    repo, settings, spot = setup
    end = utcnow() - timedelta(hours=1)
    perp = dict(spot, instrument_id="binance:perp:ABCUSD", market_type="PERP_LINEAR")

    async def observe(key, at, value):
        await repo.put(
            "mkt.oi_obs",
            key,
            {
                "instrument_id": perp["instrument_id"],
                "token_id": spot["token_id"],
                "metric_key": "binance.usdm.openInterest",
                "ts": iso(at),
                "oi_native": value,
                "recorded_at": iso(),
                "ts_prec": "EXACT",
            },
        )

    await observe("old", end - timedelta(days=2), "100")
    await observe("latest", end - timedelta(minutes=2), "150")
    calls = []
    original = repo.iter_range

    async def ranges(collection, field, **kwargs):
        calls.append((collection, field, kwargs))
        async for row in original(collection, field, **kwargs):
            yield row

    repo.iter_range = ranges
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service.instruments[perp["instrument_id"]] = perp
        requirements = [{"metric": "deriv.native_oi_chg_pct", "window": "W1H"}]
        first = await service.condition_snapshot(spot["token_id"], end, requirements=requirements)
        second = await service.condition_snapshot(spot["token_id"], end, requirements=requirements)
        assert first["domain_dependency_fingerprints"] == second["domain_dependency_fingerprints"]
        await observe("corrected", end - timedelta(minutes=2), "200")
        third = await service.condition_snapshot(spot["token_id"], end, requirements=requirements)
    assert not [c for c in calls if c[0] == "mkt.oi_obs"]
    assert [r["oi_native"] for r in first["oi"]] == ["150"]
    assert [r["oi_native"] for r in third["oi"]] == ["200"]
    assert third["domain_dependency_fingerprints"] != first["domain_dependency_fingerprints"]
    assert "funding" not in third and "flow_bars" not in third


@pytest.mark.asyncio
async def test_funding_condition_reads_the_latest_rows_and_not_the_history(setup):
    repo, settings, spot = setup
    end = utcnow() - timedelta(hours=1)
    perp = dict(spot, instrument_id="binance:perp:ABCUSD", market_type="PERP_LINEAR")

    async def observe(key, kind, at, rate):
        await repo.put(
            "mkt.funding_obs",
            key,
            {
                "instrument_id": perp["instrument_id"],
                "token_id": spot["token_id"],
                "kind": kind,
                "metric_key": "binance.usdm.funding",
                "as_of": iso(at),
                "as_of_prec": "EXACT",
                "rate": rate,
                "interval_h": "8",
                "rate_8h_eq": rate,
                "recorded_at": iso(),
            },
        )

    await observe("old", "SETTLED", end - timedelta(days=30), "0.0001")
    await observe("settled", "SETTLED", end - timedelta(hours=3), "0.0002")
    await observe("estimate", "CURRENT_ESTIMATE", end - timedelta(minutes=2), "0.0003")
    calls = []
    original = repo.iter_range

    async def ranges(collection, field, **kwargs):
        calls.append((collection, field, kwargs))
        async for row in original(collection, field, **kwargs):
            yield row

    repo.iter_range = ranges
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service.instruments[perp["instrument_id"]] = perp
        requirements = [{"metric": "deriv.funding_vs_normal", "window": "W1H"}]
        first = await service.condition_snapshot(spot["token_id"], end, requirements=requirements)
        second = await service.condition_snapshot(spot["token_id"], end, requirements=requirements)
        assert first["domain_dependency_fingerprints"] == second["domain_dependency_fingerprints"]
        await observe("corrected", "CURRENT_ESTIMATE", end - timedelta(minutes=1), "0.0004")
        third = await service.condition_snapshot(spot["token_id"], end, requirements=requirements)
    assert not [c for c in calls if c[0] == "mkt.funding_obs"]
    assert {(r["kind"], r["rate"]) for r in first["funding"]} == {
        ("SETTLED", "0.0002"),
        ("CURRENT_ESTIMATE", "0.0003"),
    }
    assert {(r["kind"], r["rate"]) for r in third["funding"]} == {
        ("SETTLED", "0.0002"),
        ("CURRENT_ESTIMATE", "0.0004"),
    }
    assert third["domain_dependency_fingerprints"] != first["domain_dependency_fingerprints"]
    assert "oi" not in third and "funding_confirmations" not in third


@pytest.mark.asyncio
async def test_composite_reads_only_dependency_metadata_and_invalidates_old_point_change(setup):
    repo, settings, inst = setup
    end = utcnow() - timedelta(hours=1)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service._range = AsyncMock(side_effect=AssertionError("no domain payload for composite"))
        req = [{"metric": "market.overall_classification", "window": "W1H"}]
        first = await service.condition_snapshot(inst["token_id"], end, requirements=req)
        second = await service.condition_snapshot(
            inst["token_id"], end + timedelta(minutes=1), requirements=req
        )
        assert first["dependency_fingerprint"] == second["dependency_fingerprint"]
        await repo.put(
            "mkt.tickers_24h",
            "late",
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "ts": iso(end - timedelta(days=2)),
                "ts_prec": "EXACT",
                "recorded_at": iso(),
                "last": "12",
            },
        )
        third = await service.condition_snapshot(inst["token_id"], end, requirements=req)
    assert third["dependency_fingerprint"] != first["dependency_fingerprint"]
    assert "flow_bars" not in third and "oi" not in third


@pytest.mark.asyncio
async def test_depth_baseline_cache_uses_source_head_and_refreshes_real_change(setup):
    repo, settings, inst = setup
    origin = utcnow() - timedelta(hours=2)
    point = {
        "instrument_id": inst["instrument_id"],
        "token_id": inst["token_id"],
        "exchange_ts": iso(origin - timedelta(minutes=30)),
        "exchange_ts_prec": "EXACT",
        "recorded_at": iso(),
        "integrity_ok": True,
        "mid": "12",
    }
    await repo.put("mkt.book_summaries", "pre-book", point)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        original = repo.iter_market_projection
        reads = []

        async def projection(collection, fields, field, **kwargs):
            reads.append((collection, fields))
            async for row in original(collection, fields, field, **kwargs):
                yield row

        repo.iter_market_projection = projection
        service._range = AsyncMock(side_effect=AssertionError("Do not load old book levels"))
        first = await service._condition_book_baseline(inst["instrument_id"], origin, iso())
        second = await service._condition_book_baseline(inst["instrument_id"], origin, iso())
        assert first == second and len(reads) == 1
        await repo.put("mkt.book_summaries", "pre-book", dict(point, mid="13", recorded_at=iso()))
        third = await service._condition_book_baseline(inst["instrument_id"], origin, iso())
        assert len(reads) == 2 and third[0]["mid"] == "13"

        assert all("bids" not in fields and "asks" not in fields for _, fields in reads)


@pytest.mark.asyncio
async def test_dc_and_depth_loader_use_bounded_inputs_and_cutoff_safe_latest(setup):
    repo, settings, inst = setup
    end = utcnow() - timedelta(hours=2)
    point = {
        "instrument_id": inst["instrument_id"],
        "token_id": inst["token_id"],
        "exchange_ts": iso(end - timedelta(seconds=5)),
        "exchange_ts_prec": "EXACT",
        "recorded_at": iso(),
        "integrity_ok": True,
        "mid": "12",
    }
    await repo.put("mkt.book_summaries", "current", point)
    await repo.put(
        "mkt.book_summaries", "future", dict(point, exchange_ts=iso(end + timedelta(seconds=5)), mid="99")
    )
    await repo.put(
        "mkt.supply_obs",
        "old-available",
        {
            "token_id": inst["token_id"],
            "kind": "FUNDAMENTALS",
            "chain": "eth",
            "available": True,
            "ts": iso(end - timedelta(days=3)),
            "ts_prec": "EXACT",
            "recorded_at": iso(),
        },
    )
    await repo.put(
        "mkt.supply_obs",
        "new-unavailable",
        {
            "token_id": inst["token_id"],
            "kind": "FUNDAMENTALS",
            "chain": "eth",
            "available": False,
            "ts": iso(end),
            "ts_prec": "EXACT",
            "recorded_at": iso(),
        },
    )
    reads = []
    original = repo.iter_range

    async def ranges(collection, field, **kwargs):
        reads.append((collection, kwargs))
        async for row in original(collection, field, **kwargs):
            yield row

    repo.iter_range = ranges
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        packet = await service.condition_snapshot(
            inst["token_id"],
            end,
            anchor={"ts": iso(end - timedelta(hours=1)), "ts_prec": "EXACT"},
            requirements=[
                {"metric": "dc.class", "window": "W1H"},
                {"metric": "liq.depth_p1_vs_pre", "window": "W1H"},
            ],
        )
    assert [b["mid"] for b in packet["books"]] == ["12"]
    assert any(p["available"] is True for p in packet["supply"])
    assert set(packet["domain_dependency_fingerprints"]) == {"dc", "depth"}
    assert all(
        end - kwargs["start"] < timedelta(days=1, minutes=1) for _, kwargs in reads if kwargs.get("start")
    )
    assert "funding_source_observations" not in packet


@pytest.mark.asyncio
async def test_the_latest_oi_observation_never_leaks_the_future_and_follows_the_cutoff(setup):
    repo, settings, spot = setup
    end = utcnow() - timedelta(days=1)
    perp = dict(spot, instrument_id="binance:perp:ABCUSD", market_type="PERP_LINEAR")
    for key, stamp, value in [
        ("past", end - timedelta(minutes=1), "100"),
        ("future", end + timedelta(hours=1), "200"),
    ]:
        await repo.put(
            "mkt.oi_obs",
            key,
            {
                "instrument_id": perp["instrument_id"],
                "token_id": spot["token_id"],
                "metric_key": "binance.usdm.openInterest",
                "ts": iso(stamp),
                "ts_prec": "EXACT",
                "recorded_at": iso(),
                "oi_native": value,
            },
        )
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service.instruments[perp["instrument_id"]] = perp
        req = [{"metric": "deriv.native_oi_chg_pct", "window": "W1H"}]
        first = await service.condition_snapshot(spot["token_id"], end, requirements=req)
        advanced = await service.condition_snapshot(
            spot["token_id"], end + timedelta(hours=2), requirements=req
        )
        historical = await service.condition_snapshot(spot["token_id"], end, requirements=req)
    assert [r["oi_native"] for r in first["oi"]] == ["100"]
    assert [r["oi_native"] for r in advanced["oi"]] == ["200"]
    assert [r["oi_native"] for r in historical["oi"]] == ["100"]


@pytest.mark.asyncio
async def test_composite_semantic_head_detects_newly_eligible_point_behind_late_correction(setup):
    repo, settings, inst = setup
    now = utcnow()
    end = now - timedelta(days=1)
    for key, stamp, recorded in [
        ("old-corrected", end - timedelta(days=1), now - timedelta(minutes=10)),
        ("precollected", end + timedelta(minutes=1), now - timedelta(hours=1)),
    ]:
        await repo.put(
            "mkt.tickers_24h",
            key,
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "ts": iso(stamp),
                "ts_prec": "EXACT",
                "recorded_at": iso(recorded),
                "last": "12",
            },
        )
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        req = [{"metric": "mt.move_class", "window": "W1H"}]
        first = await service.condition_snapshot(inst["token_id"], end, requirements=req)
        advanced = await service.condition_snapshot(
            inst["token_id"], end + timedelta(minutes=1), requirements=req
        )
    assert first["dependency_fingerprint"] != advanced["dependency_fingerprint"]


@pytest.mark.asyncio
async def test_projected_flow_restores_declared_aliases_without_inventing_missing_usd(setup):
    repo, settings, inst = setup
    end = utcnow().replace(second=0, microsecond=0) - timedelta(hours=2)
    await repo.put(
        "mkt.flow_bars_1m",
        "flow",
        {
            "instrument_id": inst["instrument_id"],
            "token_id": inst["token_id"],
            "minute_start": iso(end - timedelta(minutes=1)),
            "buy_notional": "123.4567890123456789",
            "sell_notional": None,
            "h": "12",
            "recorded_at": iso(),
        },
    )
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        rows, _ = await service._condition_projected_history(
            "mkt.flow_bars_1m",
            "minute_start",
            end - timedelta(hours=1),
            end,
            iso(),
            fields=["instrument_id", "buy_usd", "sell_usd", "h"],
            filters={"instrument_id": inst["instrument_id"]},
        )
    assert rows[0]["buy_notional"] == rows[0]["buy_usd"] == "123.4567890123456789"
    assert rows[0]["sell_notional"] is None and rows[0]["high"] == "12"


@pytest.mark.asyncio
async def test_composite_dependency_heads_read_each_scope_once(setup, monkeypatch):
    """The three heads of one scope come from one read, not one full scan per head."""
    from tank.market import service as service_module

    repo, settings, inst = setup
    end = utcnow() - timedelta(hours=1)
    await repo.put(
        "mkt.klines_1m",
        "btc",
        {
            "instrument_id": "binance:spot:BTCUSD",
            "token_id": "bitcoin:BTC",
            "open_time": iso(end - timedelta(days=2)),
            "open_time_prec": "EXACT",
            "interval_s": 60,
            "c": "2",
            "complete": True,
            "recorded_at": iso(end - timedelta(days=2) + timedelta(minutes=1)),
        },
    )
    calls = []
    real = service_module.market_dependency_head

    async def counted(store, collection, field, cutoff, **kwargs):
        calls.append((collection, kwargs.get("orders"), kwargs.get("order")))
        return await real(store, collection, field, cutoff, **kwargs)

    async def single(collection, field, cutoff, **kwargs):
        calls.append((collection, kwargs.get("orders"), kwargs.get("order")))
        return await real(repo, collection, field, cutoff, **kwargs)

    monkeypatch.setattr(service_module, "market_dependency_head", counted)
    monkeypatch.setattr(repo, "market_dependency_head", single)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        req = [
            {"metric": "market.overall_classification", "window": "W1H"},
            {"metric": "price.vs_anchor_pct", "window": "W1H"},
        ]
        packet = await service.condition_snapshot(inst["token_id"], end, requirements=req)
    orders = ("availability", "semantic_latest", "semantic_earliest")
    assert packet["dependency_fingerprint"] and "relative_history_dependency_fingerprint" not in packet
    assert calls and all(c[1] == orders and c[2] is None for c in calls)
    assert sum(c[0] == "mkt.klines_1m" for c in calls) >= 1

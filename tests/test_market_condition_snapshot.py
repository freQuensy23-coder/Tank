"""Selective producer reads must not flush capture or load unrelated histories."""

from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from tank.market.service import MarketService
from tank.util import iso, utcnow


@pytest.mark.asyncio
async def test_close_loader_reads_only_hour_ending_candle_and_reference_flow(setup) -> None:
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
async def test_native_history_cache_refreshes_late_correction_without_rereading_history(setup) -> None:
    repo, settings, spot = setup
    end = utcnow() - timedelta(hours=1)
    perp = dict(spot, instrument_id="binance:perp:ABCUSD", market_type="PERP_LINEAR")
    await repo.put(
        "mkt.oi_obs",
        "oi",
        {
            "instrument_id": perp["instrument_id"],
            "token_id": spot["token_id"],
            "metric_key": "binance.usdm.openInterest",
            "ts": iso(end - timedelta(days=2)),
            "oi_native": "100",
            "recorded_at": iso(),
            "ts_prec": "EXACT",
        },
    )
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
        assert len(calls) == 1
        await repo.put(
            "mkt.oi_obs",
            "oi",
            {
                "instrument_id": perp["instrument_id"],
                "token_id": spot["token_id"],
                "metric_key": "binance.usdm.openInterest",
                "ts": iso(end - timedelta(days=2)),
                "oi_native": "200",
                "recorded_at": iso(),
                "ts_prec": "EXACT",
            },
        )
        third = await service.condition_snapshot(spot["token_id"], end, requirements=requirements)
    assert len(calls) == 1
    assert third["oi"][0]["oi_native"] == "200"
    assert third["domain_dependency_fingerprints"] != first["domain_dependency_fingerprints"]
    assert "funding" not in third and "flow_bars" not in third


@pytest.mark.asyncio
async def test_composite_reads_only_dependency_metadata_and_invalidates_old_point_change(setup) -> None:
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
async def test_depth_baseline_cache_uses_source_head_and_refreshes_real_change(setup) -> None:
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
async def test_dc_and_depth_loader_use_bounded_inputs_and_cutoff_safe_latest(setup) -> None:
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
async def test_native_cache_cutoff_movement_never_leaks_future_or_loses_precollected_points(setup) -> None:
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
    assert {r["oi_native"] for r in advanced["oi"]} == {"100", "200"}
    assert [r["oi_native"] for r in historical["oi"]] == ["100"]


@pytest.mark.asyncio
async def test_composite_semantic_head_detects_newly_eligible_point_behind_late_correction(setup) -> None:
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
async def test_compact_kline_vectors_preserve_native_precision_and_prove_append_vs_correction(setup) -> None:
    from array import array

    repo, settings, inst = setup
    start = (utcnow() - timedelta(hours=3)).replace(second=0, microsecond=0)

    def row(number, qty, complete=True):
        return {
            "instrument_id": inst["instrument_id"],
            "token_id": inst["token_id"],
            "open_time": iso(start + timedelta(minutes=number)),
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "complete": complete,
            "c": "12.125",
            "volume_native": qty,
            "recorded_at": iso(),
        }

    quantity = "12345678901234567890.12345678901234567890"
    await repo.put_market_batch(
        "mkt.klines_1m", [row(0, quantity), row(1, "2", False)], keys=["first", "gap"]
    )
    projections = []
    original = repo.iter_market_projection

    async def projection(collection, fields, field, **kwargs):
        projections.append(tuple(fields))
        async for record in original(collection, fields, field, **kwargs):
            yield record

    repo.iter_market_projection = projection
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        cutoff = start + timedelta(minutes=2)
        first = await service._condition_kline_series(
            inst, start, cutoff, iso(), kind="volume", coverage_count=1
        )
        repeated = await service._condition_kline_series(
            inst, start, cutoff, iso(), kind="volume", coverage_count=1
        )
        assert len(projections) == 1 and first["dependency_fingerprint"] == repeated["dependency_fingerprint"]
        assert isinstance(first["times_us"], array) and first["time_basis"] == "OPEN_TIME"
        assert first["values"] == [quantity, "2"] and list(first["complete"]) == [1, 0]
        await repo.put_market_batch("mkt.klines_1m", [row(2, "3")], keys=["append"])
        appended = await service._condition_kline_series(
            inst, start, cutoff + timedelta(minutes=1), iso(), kind="volume", coverage_count=2
        )
        assert appended["append_only"] is True
        assert appended["previous_dependency_fingerprint"] == first["dependency_fingerprint"]
        assert first["values"] == [quantity, "2"]
        await repo.put_market_batch("mkt.klines_1m", [row(0, "4")], keys=["first"])
        corrected = await service._condition_kline_series(
            inst, start, cutoff + timedelta(minutes=1), iso(), kind="volume", coverage_count=2
        )
        assert corrected["append_only"] is False and corrected["values"][0] == "4"
        price = await service._condition_kline_series(
            inst, start, cutoff + timedelta(minutes=1), iso(), kind="price", coverage_count=2
        )
        assert price["time_basis"] == "CLOSE_TIME" and len(price["values"]) == 2
        assert price["times_us"][0] - first["times_us"][0] == 60_000_000
    assert all(
        not {"o", "h", "l", "quote_volume", "source_archive"}.intersection(fields) for fields in projections
    )


@pytest.mark.asyncio
async def test_projected_flow_restores_declared_aliases_without_inventing_missing_usd(setup) -> None:
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
async def test_rolling_kline_cursor_serves_an_earlier_checkpoint_without_rereading_history(setup) -> None:
    repo, settings, inst = setup
    start = (utcnow() - timedelta(hours=3)).replace(second=0, microsecond=0)

    def row(number, close, complete=True):
        return {
            "instrument_id": inst["instrument_id"],
            "token_id": inst["token_id"],
            "open_time": iso(start + timedelta(minutes=number)),
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "complete": complete,
            "c": close,
            "volume_native": close,
            "recorded_at": iso(),
        }

    await repo.put_market_batch(
        "mkt.klines_1m", [row(n, str(10 + n), n != 7) for n in range(30)], keys=[f"k{n}" for n in range(30)]
    )
    reads = []
    original = repo.iter_market_projection

    async def projection(collection, fields, field, **kwargs):
        reads.append(kwargs["start"])
        async for record in original(collection, fields, field, **kwargs):
            yield record

    async def fresh(window_start, cutoff, kind):
        async with httpx.AsyncClient() as http:
            alone = MarketService(repo, settings, http=http)
            return await alone._condition_kline_series(
                inst, window_start, cutoff, iso(), kind=kind, coverage_count=1, fixed_window=True
            )

    def same(a, b):
        keys = ("times_us", "values", "complete", "precision", "precision_reason")
        return all(
            list(a[k]) == list(b[k]) if k in ("times_us", "values", "complete") else a[k] == b[k]
            for k in keys
        )

    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        repo.iter_market_projection = projection
        for kind in ("price", "volume"):
            reads.clear()
            live = await service._condition_kline_series(
                inst,
                start + timedelta(minutes=5),
                start + timedelta(minutes=25),
                iso(),
                kind=kind,
                coverage_count=1,
                fixed_window=True,
                rolling="LIVE_SNAPSHOT",
            )

            behind = await service._condition_kline_series(
                inst,
                start + timedelta(minutes=2),
                start + timedelta(minutes=15),
                iso(),
                kind=kind,
                coverage_count=1,
                fixed_window=True,
                rolling="LIVE_SNAPSHOT",
            )
            assert len(reads) == 1 and behind["append_only"] is False
            assert behind["dependency_fingerprint"] != live["dependency_fingerprint"]
            repo.iter_market_projection = original
            assert same(
                behind, await fresh(start + timedelta(minutes=2), start + timedelta(minutes=15), kind)
            )
            repo.iter_market_projection = projection
            await repo.put_market_batch("mkt.klines_1m", [row(3, "99")], keys=["k3"])
            corrected = await service._condition_kline_series(
                inst,
                start + timedelta(minutes=2),
                start + timedelta(minutes=15),
                iso(),
                kind=kind,
                coverage_count=1,
                fixed_window=True,
                rolling="LIVE_SNAPSHOT",
            )
            later = await service._condition_kline_series(
                inst,
                start + timedelta(minutes=2),
                start + timedelta(minutes=28),
                iso(),
                kind=kind,
                coverage_count=1,
                fixed_window=True,
                rolling="LIVE_SNAPSHOT",
            )
            assert len(reads) == 2 and reads[1] > start + timedelta(minutes=20)
            assert later["append_only"] is True
            assert later["previous_dependency_fingerprint"] == corrected["dependency_fingerprint"]
            repo.iter_market_projection = original
            assert same(live, await fresh(start + timedelta(minutes=5), start + timedelta(minutes=25), kind))
            assert same(
                corrected, await fresh(start + timedelta(minutes=2), start + timedelta(minutes=15), kind)
            )
            assert same(later, await fresh(start + timedelta(minutes=2), start + timedelta(minutes=28), kind))
            repo.iter_market_projection = projection
            await repo.put_market_batch("mkt.klines_1m", [row(3, "13")], keys=["k3"])


@pytest.mark.asyncio
async def test_rolling_kline_cursor_serves_an_older_assembly_clock_without_later_rows(setup) -> None:
    repo, settings, inst = setup
    start = (utcnow() - timedelta(hours=3)).replace(second=0, microsecond=0)
    early = iso(utcnow() - timedelta(minutes=30))

    def row(number, close, recorded_at):
        return {
            "instrument_id": inst["instrument_id"],
            "token_id": inst["token_id"],
            "open_time": iso(start + timedelta(minutes=number)),
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "complete": True,
            "c": close,
            "volume_native": close,
            "recorded_at": recorded_at,
        }

    await repo.put_market_batch(
        "mkt.klines_1m", [row(n, str(10 + n), early) for n in range(20)], keys=[f"k{n}" for n in range(20)]
    )
    reads = []
    original = repo.iter_market_projection

    async def projection(collection, fields, field, **kwargs):
        reads.append(kwargs["start"])
        async for record in original(collection, fields, field, **kwargs):
            yield record

    async def series(service, assembled, cutoff=15):
        return await service._condition_kline_series(
            inst,
            start + timedelta(minutes=2),
            start + timedelta(minutes=cutoff),
            assembled,
            kind="price",
            coverage_count=1,
            fixed_window=True,
            rolling="LIVE_SNAPSHOT",
        )

    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        repo.iter_market_projection = projection
        older = iso(utcnow() - timedelta(minutes=10))
        await series(service, iso())
        behind = await series(service, older)
        assert len(reads) == 1
        repo.iter_market_projection = original
        async with httpx.AsyncClient() as other:
            assert list(behind["values"]) == list(
                (await series(MarketService(repo, settings, http=other), older))["values"]
            )
        repo.iter_market_projection = projection

        await repo.put_market_batch("mkt.klines_1m", [row(5, "99", iso())], keys=["k5"])
        await series(service, iso(), cutoff=16)
        reads.clear()
        rebuilt = await series(service, older)
        assert not reads
        assert 99.0 not in list(rebuilt["values"])
        repo.iter_market_projection = original
        async with httpx.AsyncClient() as other:
            fresh = await series(MarketService(repo, settings, http=other), older)
        assert list(rebuilt["values"]) == list(fresh["values"])

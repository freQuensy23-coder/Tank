from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
import yaml

from tank.market.service import MarketService
from tank.market.streams import connection
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import utcnow


def test_market_redaction_covers_nested_keys_and_values_and_current_credentials():
    from types import SimpleNamespace

    market = object.__new__(MarketService)
    market.settings = SimpleNamespace(telegram_token="private-token")
    value = {"private-token": [{"url": "https://example.test/private-token"}, (7, None)]}
    assert market._redact(value) == {"[REDACTED]": [{"url": "https://example.test/[REDACTED]"}, [7, None]]}
    assert "private-token" in value
    market.settings.telegram_token = "rotated-token"
    assert market._redact({"key": "rotated-token"}) == {"key": "[REDACTED]"}


@pytest.fixture
def setup(tmp_path):
    config = tmp_path / "config"
    config.mkdir()

    def inst(token, symbol):
        return {
            "instrument_id": "binance:spot:" + symbol,
            "token_id": token,
            "venue": "binance",
            "market_type": "SPOT",
            "venue_symbol": symbol,
            "quote_asset": "USD",
            "active": True,
            "contract_multiplier": "1",
            "tick_size": ".01",
            "lot_size": ".01",
        }

    target = inst("test:ABC", "ABCUSD")
    benchmark = inst("bitcoin:BTC", "BTCUSD")
    registry = {
        "tokens": [
            {"token_id": "test:ABC", "symbol": "ABC", "sector": "A"},
            {"token_id": "bitcoin:BTC", "symbol": "BTC", "sector": "BENCHMARK"},
        ],
        "instruments": [target, benchmark],
    }
    (config / "registry.yaml").write_text(yaml.safe_dump(registry))
    (config / "semantics.yaml").write_text(
        yaml.safe_dump(
            {
                "metrics": [
                    {
                        "metric_key": "binance.spot.aggTrade",
                        "semantic_confidence": "UNKNOWN",
                        "critical": True,
                    }
                ]
            }
        )
    )
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", replay=True)
    return MemoryRepository(), settings, target


@pytest.mark.asyncio
async def test_trade_archive_contention_keeps_event_loop_responsive(setup):
    import asyncio
    from threading import Event, Thread

    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        await service._ensure_market_restore()
        locked, release, fallback = Event(), Event(), Event()

        def writer():
            with service.trade_archive._lock:
                locked.set()
                if not release.wait(2):
                    fallback.set()

        thread = Thread(target=writer)
        thread.start()
        await asyncio.to_thread(locked.wait, 2)
        raw = {"a": 1, "p": "2", "q": "3", "T": int(utcnow().timestamp() * 1000), "m": False}
        task = asyncio.create_task(service.ingest_trade(inst, raw))
        try:
            # This coroutine must release the archive writer before its fallback;
            # a synchronous archive lock on the event loop prevents that.
            await asyncio.sleep(0.05)
            assert not fallback.is_set()
            assert not task.done()
        finally:
            release.set()
            await asyncio.to_thread(thread.join, 3)
        assert await task


@pytest.mark.asyncio
async def test_trade_dedup_restart_and_precise_bar_reconstruction(setup):
    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    ts = int((utcnow() - timedelta(minutes=2)).timestamp() * 1000)
    raw = {"a": 1, "p": "2.123456789012345678", "q": "3", "T": ts, "m": False}
    assert await service.ingest_trade(inst, raw)
    assert not await service.ingest_trade(inst, raw)
    await service._flush_flow()
    await service.flush_market()
    # Restart resumes durable trade IDs and reconstructs existing minute before next print.
    restarted = MarketService(repo, settings, http=http)
    assert not await restarted.ingest_trade(inst, raw)
    second = dict(raw, a=2, q="1", T=ts + 1, m=True)
    assert await restarted.ingest_trade(inst, second)
    await restarted._flush_flow()
    await restarted.flush_market()
    bars = await repo.list("mkt.flow_bars_1m")
    assert len(bars) == 1
    assert bars[0]["buy_qty"] == "3" and bars[0]["sell_qty"] == "1" and bars[0]["sum_sq_qty"] == "10"
    assert Decimal(bars[0]["buy_notional"]) == Decimal("6.370370367037037034")
    semantics = await repo.get("qa.reviews", "semantic:" + inst["instrument_id"] + ":binance.spot.aggTrade")
    assert semantics is None  # No runtime semantic promotion or self-test records.
    await http.aclose()


@pytest.mark.asyncio
async def test_snapshot_exact_cutoff_anchor_and_benchmark_history(setup):
    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    cutoff = "2026-06-17T00:02:00Z"
    for iid, minute in [
        (inst["instrument_id"], "00:00"),
        (inst["instrument_id"], "00:02"),
        ("binance:spot:BTCUSD", "00:01"),
    ]:
        row = {
            "instrument_id": iid,
            "token_id": inst["token_id"] if iid == inst["instrument_id"] else "bitcoin:BTC",
            "open_time": "2026-06-17T" + minute + ":00Z",
            "c": "2",
            "complete": True,
        }
        await repo.put("mkt.klines_1m", iid + minute, row)
    await repo.put(
        "mkt.oi_obs",
        "before",
        {
            "instrument_id": inst["instrument_id"],
            "metric_key": "binance.usdm.openInterest",
            "ts": "2026-06-17T00:01:00Z",
            "oi_native": "3",
        },
    )
    await repo.put(
        "mkt.oi_obs",
        "future",
        {
            "instrument_id": inst["instrument_id"],
            "metric_key": "binance.usdm.openInterest",
            "ts": "2026-06-17T00:03:00Z",
            "oi_native": "9",
        },
    )
    anchor = {"ts": "2026-06-16T23:00:00Z", "type": "T0"}
    snapshot = await service.snapshot(inst["token_id"], cutoff, anchor)
    assert snapshot["anchor"]["ts"] == anchor["ts"] and snapshot["anchor"]["type"] == anchor["type"]
    assert snapshot["anchor"]["ts_prec"] is None and snapshot["anchor"]["ts_prec_reason"] == "INCOMPLETE"
    assert snapshot["assembled_at_prec"] == "COLLECTION_ONLY" and snapshot["requested_cutoff_prec"] == "EXACT"
    assert len(snapshot["klines"]) == 2 and len(snapshot["oi"]) == 1
    from tank.time_precision import Timestamp

    uncertain = await service.snapshot(inst["token_id"], Timestamp(cutoff, "BUCKET(1h)"), anchor)
    assert uncertain["cutoff_prec"] == uncertain["requested_cutoff_prec"] == "BUCKET(1h)"
    assert any(i["token_id"] == "bitcoin:BTC" for i in snapshot["instruments"])
    assert (await service.snapshot(inst["token_id"], cutoff))["anchor"] is None
    await http.aclose()


@pytest.mark.asyncio
async def test_connected_empty_minute_zero_and_disconnected_absence(setup):
    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    now = utcnow().replace(second=0, microsecond=0)
    iid = inst["instrument_id"]
    service._connected_since[iid] = now - timedelta(minutes=2)
    service._stream_watermarks[iid] = {"ts": now.isoformat()}
    await service._flush_flow()
    await service.flush_market()
    bars = await repo.list("mkt.flow_bars_1m")
    closed = [b for b in bars if b["complete"]]
    assert len(closed) == 2 and all(b["buy_qty"] == "0" and b["open"] is None for b in closed)
    assert len(bars) == 3 and sum(not b["complete"] for b in bars) == 1
    await service._flush_flow()
    assert len(await repo.list("mkt.flow_bars_1m")) == 3
    other = "binance:spot:BTCUSD"
    assert not await repo.list("mkt.flow_bars_1m", filters={"instrument_id": other})
    await http.aclose()


@pytest.mark.asyncio
async def test_backfill_complete_survives_flush_without_live_connection(setup):
    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    ts = int((utcnow() - timedelta(hours=1)).timestamp() * 1000)
    raw = {"a": 1, "p": "2", "q": "3", "T": ts, "m": False}
    await service.ingest_trade(inst, raw, backfill=True)
    minute = utcnow().fromtimestamp(ts / 1000, tz=utcnow().tzinfo).replace(second=0, microsecond=0)
    await service._persist_rest_flow_ranges(
        inst, minute, minute + timedelta(minutes=1), [(minute, minute + timedelta(minutes=1))]
    )
    await service._flush_flow()
    await service.flush_market()
    bars = await repo.list("mkt.flow_bars_1m")
    assert len(bars) == 1 and bars[0]["complete"] and bars[0]["gap_reason"] is None
    await http.aclose()


@pytest.mark.asyncio
async def test_polling_venue_error_preserves_gap_not_zero(setup):
    repo, settings, inst = setup

    def handler(request):
        return httpx.Response(503, json={"error": "unavailable"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = MarketService(repo, settings, http=http)
    await service.poll_once(inst["instrument_id"])
    gaps = await repo.list("mkt.capture_gaps")
    assert {g["kind"] for g in gaps} >= {"METADATA", "KLINES", "TICKER"}
    assert not await repo.list("mkt.klines_1m") and not await repo.list("mkt.oi_obs")
    await http.aclose()


@pytest.mark.asyncio
async def test_independent_rest_confirmation_returns_evidence_without_duplicate_frame(setup):
    repo, settings, inst = setup

    def handler(request):
        return httpx.Response(200, json={"symbol": "ABCUSD", "bidPrice": "99", "askPrice": "101"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = MarketService(repo, settings, http=http)
    out = await service._independent_book(inst, {"mid": "100", "spread_bps": "200"})
    assert out["second_path_confirmed"] and out["independent_rest"]["source"] == "PUBLIC_REST"
    await service.flush_market()
    assert not await repo.list("mkt.raw_archives")
    assert out["independent_rest"]["mid"] == "100"
    assert not (settings.data_dir / "raw/market_frames").exists()
    await http.aclose()


def test_subscriptions_correct_futures_path_and_all_liquidations():
    i = {"venue": "binance", "market_type": "PERP_LINEAR", "venue_symbol": "BTCUSDT"}
    url, _ = connection(i)
    assert (
        url.startswith("wss://fstream.binance.com/market/stream?")
        and "markPrice@1s" in url
        and "forceOrder" in url
    )
    url, messages = connection(dict(i, venue="bybit"))
    assert "allLiquidation.BTCUSDT" in messages[0]["args"]
    url, messages = connection(dict(i, venue="hyperliquid"))
    assert {m["subscription"]["type"] for m in messages} == {"trades", "l2Book", "activeAssetCtx"}


@pytest.mark.asyncio
async def test_fee_protocol_resolution_exact_identity_and_partial_holders_revenue(setup):
    from tank.market.supply import SupplyCapture

    repo, settings, _inst = setup
    token = {
        "token_id": "test:ABC",
        "symbol": "ABC",
        "contracts": [{"chain": "Ethereum", "address": "0xabc"}],
        "coingecko_id": "abc-token",
    }

    def handler(request):
        path = request.url.path
        if path == "/protocols":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "1",
                        "address": "0xabc",
                        "symbol": "ABC",
                        "parentProtocol": "parent#abc",
                        "slug": "abc-v1",
                    },
                    {
                        "id": "9",
                        "address": "0xother",
                        "symbol": "ABC",
                        "gecko_id": "another-coin",
                        "slug": "wrong",
                    },
                ],
            )
        if path == "/overview/fees":
            return httpx.Response(
                200,
                json={
                    "protocols": [
                        {"defillamaId": "parent#abc", "slug": "abc"},
                        {"defillamaId": "1", "slug": "abc-v1", "parentProtocol": "parent#abc"},
                        {"defillamaId": "2", "slug": "abc-v2", "parentProtocol": "parent#abc"},
                        {"defillamaId": "9", "slug": "wrong"},
                    ]
                },
            )
        metric = request.url.params["dataType"]
        if path.endswith("abc-v2") and metric == "dailyHoldersRevenue":
            return httpx.Response(404, json={"error": "unsupported"})
        value = "3" if metric == "dailyHoldersRevenue" else "10"
        return httpx.Response(
            200, json={"total24h": value, "total30d": value, "totalDataChart": [[1781654400, value]]}
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = MarketService(repo, settings, http=http)
    capture = SupplyCapture(service)
    slugs, evidence = await capture.resolve_fee_protocols(token)
    assert slugs == ["abc-v1", "abc-v2"] and evidence["matched_protocol_ids"] == ["1"]
    result = await capture.fundamentals(token)
    assert result["metrics"]["dailyFees"]["total24h"] == "20"
    assert result["metrics"]["dailyHoldersRevenue"]["total24h"] == "3"
    assert not result["metrics"]["dailyHoldersRevenue"]["complete"]
    assert result["coverage_errors"][0]["metric"] == "dailyHoldersRevenue"
    await http.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("secret_name", ["etherscan_api_key", "openrouter_api_key", "codex_gateway_key"])
async def test_market_transport_and_recursive_metadata_never_persist_api_key(setup, secret_name):
    from tank.market.supply import SupplyCapture

    repo, settings, inst = setup
    secret = "secret-test-provider-key"
    setattr(settings, secret_name, secret)

    def handler(request):
        raise httpx.ConnectError("Connection failed: " + str(request.url), request=request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = MarketService(repo, settings, http=http)
    with pytest.raises(RuntimeError) as captured:
        await service._request("GET", "https://api.etherscan.io/v2/api", params={"apikey": secret})
    assert secret not in str(captured.value)
    capture = SupplyCapture(service)
    token = {"token_id": "test:ABC"}
    await capture.record(
        token,
        "EXCHANGE_INVENTORY",
        coverage_errors=[
            {
                "reason": "nested url?apikey=" + secret,
                "context": {"key": secret},
            }
        ],
    )
    assert secret not in str(await repo.list("mkt.supply_obs"))
    await service._raw(inst, "sensitive_response", {"nested": [{"error": secret}]})
    await service.flush_market()
    assert not await repo.list("mkt.raw_archives")
    assert not (settings.data_dir / "raw/market_frames").exists()
    await http.aclose()


@pytest.mark.asyncio
async def test_hot_escalation_priority_age_repeat_and_event_specific_demote(setup):
    repo, settings, _inst = setup
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    service.thresholds["hot_token_cap"] = 1
    await repo.put(
        "evt.events",
        "old",
        {"event_id": "old", "public_at": "2026-10-10T00:00:00Z", "current_ems_id": "major"},
    )
    await repo.put("evt.ems_assessments", "major", {"ems_id": "major", "event_id": "old", "class": "MAJOR"})
    await service.escalate("old", ["test:ABC"])
    original = await repo.get("mkt.hot_tokens", "test:ABC:old")
    await service.escalate("old", ["test:ABC"])
    assert (
        (await repo.get("mkt.hot_tokens", "test:ABC:old"))["since"]
        == original["since"]
        == "2026-10-10T00:00:00Z"
    )
    await repo.put(
        "evt.events",
        "new",
        {"event_id": "new", "public_at": "2026-10-11T00:00:00Z", "current_ems_id": "shock"},
    )
    await repo.put(
        "evt.ems_assessments", "shock", {"ems_id": "shock", "event_id": "new", "class": "STRUCTURAL_SHOCK"}
    )
    await service.escalate("new", ["bitcoin:BTC"])
    hot = await repo.list("mkt.hot_tokens")
    assert len(hot) == 1 and hot[0]["token_id"] == "bitcoin:BTC"
    service.thresholds["hot_token_cap"] = 2
    await service.escalate("old", ["bitcoin:BTC"])
    await service.demote("bitcoin:BTC", "old")
    assert len(await repo.list("mkt.hot_tokens")) == 1
    # replay escalation has no external backfill/subscription effects.
    assert not await repo.list("mkt.raw_archives") and not service.streams
    await http.aclose()


@pytest.mark.asyncio
async def test_backfill_prerequisite_is_durable_without_orphan_completion_jobs(setup):
    import asyncio

    repo, settings, inst = setup
    settings.replay = False
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    release = asyncio.Event()

    async def backfill(instrument, anchor):
        await release.wait()

    service.backfill = backfill
    await service._schedule_event_backfill("event-1", inst["token_id"], "2026-06-17T00:00:00Z")
    readiness = await service.capture_readiness("event-1", inst["token_id"])
    assert readiness["status"] == "PENDING" and readiness["reason"] == "PRE_EVENT_CAPTURE_PENDING"
    snapshot = await service.snapshot(
        inst["token_id"], "2026-06-17T01:00:00Z", {"ts": "2026-06-17T00:00:00Z", "type": "T0"}
    )
    assert snapshot["capture_status"]["status"] in ("PENDING", "RUNNING")
    assert (
        snapshot["capture_status"]["bounded_wait_s"] == 0
        and snapshot["capture_status"]["incomplete_windows_excluded"]
    )
    release.set()
    await asyncio.gather(*service._background)
    terminal = await service.capture_readiness("event-1", inst["token_id"])
    assert (
        terminal["status"] == "PARTIAL"
        and terminal["result"]["instruments"][0]["windows"]["W24H"]["flow"]["missing_or_incomplete_minutes"]
        == 1440
    )
    jobs = await repo.list("ops.jobs")
    assert jobs == []  # Original configured job waits on durable readiness.
    restarted = MarketService(repo, settings, http=http)
    await restarted._schedule_event_backfill("event-1", inst["token_id"], "2026-06-17T00:00:00Z")
    assert not restarted._background
    await http.aclose()


@pytest.mark.asyncio
async def test_registry_available_for_cold_benchmark_and_anchor_is_actual_trade(setup):
    from tank.util import iso, utcnow

    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    now = utcnow()
    anchor = now - timedelta(minutes=1)
    before = int((anchor - timedelta(milliseconds=1)).timestamp() * 1000)
    after = int((anchor + timedelta(milliseconds=1)).timestamp() * 1000)
    await service.ingest_trade(inst, {"a": 1, "p": "2.1", "q": "3", "T": before, "m": False})
    await service.ingest_trade(inst, {"a": 2, "p": "2.2", "q": "1", "T": after, "m": True})
    snapshot = await service.snapshot(inst["token_id"], iso(utcnow()), {"ts": iso(anchor), "type": "T0"})
    target = [s for s in snapshot["semantics"] if s["instrument_id"] == inst["instrument_id"]]
    benchmark = [s for s in snapshot["semantics"] if s["instrument_id"] == "binance:spot:BTCUSD"]
    assert target and benchmark
    assert all("semantic_confidence" not in row for row in target + benchmark)
    assert not await repo.list("qa.reviews", filters={"subject": "METRIC_SEMANTICS"})
    points = [point for point in snapshot["reference_points"] if point["boundary"] == iso(anchor)]
    # A source frame alone is not a proof that the boundary was exhaustively enumerated.
    assert not any(point["complete"] for point in points)
    assert "trades" not in snapshot
    await http.aclose()


@pytest.mark.asyncio
async def test_exhausted_bounded_rest_proves_first_last_and_empty_minutes_despite_live_flush(
    setup, monkeypatch
):
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda req: httpx.Response(404, json={"fixture": "archive unavailable"})
        )
    )
    service = MarketService(repo, settings, http=http)
    # This scale test isolates capture logic; rollback/locking are covered against realPG.
    # Avoid MemoryRepository's intentionally exhaustive O(n²) savepoint snapshots.
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fixture_transaction():
        yield repo

    monkeypatch.setattr(repo, "transaction", fixture_transaction)
    anchor = parse_time("2026-06-17T00:00:00Z")
    begin = anchor - timedelta(hours=24)

    class Adapter:
        async def trades(self, *, start=None, end=None, cursor=None):
            if cursor is not None:
                # Quiet initial minute: the true ID predecessor is older.
                return [
                    {
                        "a": 0,
                        "p": "2",
                        "q": "1",
                        "T": int((begin - timedelta(minutes=5)).timestamp() * 1000),
                        "m": False,
                    }
                ], None
            # Concurrent live housekeeping can evict old provisional accumulators.
            service._last_flow_flush = 0
            await service._flush_flow()
            hour = int((start - begin).total_seconds() // 3600)
            stamp = int(start.timestamp() * 1000)
            return [
                {"a": hour * 2 + 1, "p": "2", "q": "1", "T": stamp + 30000, "m": False},
                {"a": hour * 2 + 2, "p": "2", "q": "1", "T": stamp + 3599000, "m": True},
            ], None

        async def klines(self, *, start, end):
            rows = []
            stamp = start
            while stamp < end:
                ms = int(stamp.timestamp() * 1000)
                rows.append([ms, "2", "2", "2", "2", "0", ms + 59999, "0", 0, "0", "0"])
                stamp += timedelta(minutes=1)
            return rows

    service.adapter = lambda instrument, **kwargs: Adapter()
    import asyncio

    await repo.put(
        "evt.events",
        "pre24-event",
        {"event_id": "pre24-event", "public_at": iso(anchor), "known_to_us_at": iso(anchor)},
    )
    settings.replay = False
    await service.escalate("pre24-event", [inst["token_id"]])
    await asyncio.gather(*service._background)
    readiness = await service.capture_readiness("pre24-event", inst["token_id"], anchor=anchor)
    assert readiness["status"] == "COMPLETE"
    assert readiness["result"]["benchmark_klines"][0]["complete"]
    pre24 = await service.snapshot(inst["token_id"], anchor, {"ts": begin, "type": "T0"})
    reference = next(
        p["last_trade"]
        for p in pre24["reference_points"]
        if p["instrument_id"] == inst["instrument_id"] and p["boundary"] == iso(begin) and p["complete"]
    )
    assert parse_time(reference["ts"]) == begin - timedelta(minutes=5)
    assert reference["price"] == "2"
    rows = await repo.list("mkt.flow_bars_1m", filters={"instrument_id": inst["instrument_id"]}, limit=2000)
    rows = [r for r in rows if begin <= parse_time(r["minute_start"]) < anchor]
    assert len(rows) == 1440 and all(r["complete"] for r in rows)
    first = next(r for r in rows if r["minute_start"] == "2026-06-16T00:00:00Z")
    empty = next(r for r in rows if r["minute_start"] == "2026-06-16T00:01:00Z")
    assert first["buy_qty"] == "1" and first["backfill_verified"]
    assert empty["zero_trade_minute"] and empty["open"] is None and empty["buy_qty"] == "0"
    service._last_flow_flush = 0
    await service._flush_flow()
    assert (await repo.get("mkt.flow_bars_1m", inst["instrument_id"] + ":2026-06-16T00:00:00Z"))["complete"]
    coverage = await service._capture_window_coverage(inst, anchor)
    assert coverage["complete"] and coverage["windows"]["W24H"]["flow"]["complete_minutes"] == 1440
    from tank.scheduler import Scheduler
    from tank.util import iso

    await repo.put(
        "evt.events",
        "pre24-event",
        {
            "event_id": "pre24-event",
            "public_at": iso(anchor),
            "known_to_us_at": iso(anchor),
        },
    )
    (settings.config_dir / "thresholds.yaml").write_text("{}\n")
    checkpoint = await Scheduler(repo, settings, service, None).reaction_capture(
        {
            "event_id": "pre24-event",
            "token_id": inst["token_id"],
            "checkpoint": "PRE_24H",
            "anchor": {"ts": iso(anchor), "type": "T1"},
            "retro": True,
        }
    )
    assert Decimal(str(checkpoint["r_raw"])) == 0
    assert Decimal(str(checkpoint["r_btc_adj"])) == 0
    await http.aclose()


@pytest.mark.asyncio
async def test_fractional_boundary_actual_trades_and_proven_range_never_include_future(setup):
    from tank.util import parse_time

    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    anchor = parse_time("2026-06-17T00:00:17Z")
    cutoff = parse_time("2026-06-17T01:02:33Z")
    start = anchor.replace(second=0)
    service._register_capture_boundaries({inst["instrument_id"]}, cutoff, {"ts": anchor})
    for tid, stamp in enumerate(
        [
            anchor - timedelta(seconds=1),
            anchor + timedelta(seconds=1),
            cutoff - timedelta(seconds=1),
            cutoff + timedelta(seconds=1),
        ],
        1,
    ):
        await service.ingest_trade(
            inst,
            {"a": tid, "p": "2", "q": "1", "T": int(stamp.timestamp() * 1000), "m": False},
            backfill=True,
        )
    await service._persist_rest_flow_ranges(inst, start, cutoff, [(start, cutoff)])
    snapshot = await service.snapshot(inst["token_id"], cutoff, {"ts": anchor, "type": "T0"})
    segments = snapshot["flow_segments"]
    ending = [
        p
        for p in segments
        if parse_time(p["end"]) == cutoff and parse_time(p["start"]) == cutoff.replace(second=0)
    ]
    assert len(ending) == 1 and ending[0]["complete"] and Decimal(ending[0]["buy_qty"]) == 1
    prefix = [p for p in segments if parse_time(p["end"]) == anchor and parse_time(p["start"]) == start]
    assert len(prefix) == 1 and prefix[0]["complete"] and prefix[0]["last_trade"]["trade_id"] == "1"
    # Ordinary truth is durable aggregates, independent of raw retention. An
    # unrequested historical boundary remains unavailable, never interpolated.
    unknown = await service.snapshot(
        inst["token_id"], cutoff, {"ts": anchor + timedelta(seconds=1), "type": "T0"}
    )
    assert not any(parse_time(p["end"]) == anchor + timedelta(seconds=1) for p in unknown["flow_segments"])
    await http.aclose()


@pytest.mark.asyncio
async def test_snapshot_fractional_current_and_previous_15minute_flow_requires_retained_raw(setup):
    from tank.analysis.truth import spot_truth
    from tank.util import parse_time

    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        cutoff = parse_time("2026-06-17T12:00:17Z")
        anchor = cutoff - timedelta(hours=1)
        start = cutoff.replace(second=0) - timedelta(hours=2)
        service._register_capture_boundaries({inst["instrument_id"]}, cutoff, {"ts": anchor})
        tid = 0
        for minute in range(121):
            for second, qty, maker in ((10, "1", True), (20, "2", False)):
                tid += 1
                stamp = start + timedelta(minutes=minute, seconds=second)
                await service.ingest_trade(
                    inst,
                    {"a": tid, "p": "2", "q": qty, "T": int(stamp.timestamp() * 1000), "m": maker},
                    backfill=True,
                )
        await service._persist_rest_flow_ranges(inst, start, cutoff, [(start, cutoff)])

        async def windows():
            snapshot = await service.snapshot(inst["token_id"], cutoff, {"ts": anchor, "type": "T0"})
            assert all(parse_time(row["end"]) <= cutoff for row in snapshot["flow_segments"])
            return [
                spot_truth(
                    snapshot["flow_bars"],
                    snapshot["tickers"],
                    [inst],
                    cutoff - timedelta(minutes=offset + 15),
                    cutoff - timedelta(minutes=offset),
                    flow_segments=snapshot["flow_segments"],
                    assembled_at=snapshot["assembled_at"],
                )
                for offset in (0, 15)
            ]

        current, previous = await windows()
        for result in (current, previous):
            assert len(result["venues"]) == 1
            assert result["venues"][0]["buy_qty"] == Decimal(30)
            assert result["venues"][0]["sell_qty"] == Decimal(15)

        # Removing a required typed boundary segment leaves the exact window incomplete.
        missing = cutoff - timedelta(minutes=15)
        for key, row in list(repo._data["mkt.flow_bars_1m"].items()):
            if parse_time(row["minute_start"]) == missing.replace(second=0):
                row["boundary_segments"] = []
                await repo.put_market_batch("mkt.flow_bars_1m", [row], keys=[key])
        current, _ = await windows()
        assert len(current["venues"]) == 0


def add_perp_instrument(settings, spot):
    registry = settings.load_config("registry")
    instrument = dict(spot, instrument_id="binance:perp:ABCUSD", market_type="PERP_LINEAR")
    registry["instruments"].append(instrument)
    (settings.config_dir / "registry.yaml").write_text(yaml.safe_dump(registry))
    return instrument


@pytest.mark.asyncio
async def test_kraken_pre24_reference_search_expands_quiet_time_and_follows_forward_cursor(setup):
    from tank.util import iso, parse_time

    repo, settings, spot = setup
    inst = dict(spot, venue="kraken", instrument_id="kraken:spot:ABCUSD")
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        anchor = parse_time("2026-06-17T00:00:00Z")
        begin = anchor - timedelta(hours=24)
        calls = []

        def raw(stamp, tid):
            return ["2", "1", str(stamp.timestamp()), "b", "m", "", tid]

        class Adapter:
            async def trades(self, *, start=None, end=None, cursor=None):
                calls.append((start, cursor))
                if end is not None:
                    return [raw(begin, 10), raw(anchor, 11)], "main-end"
                if cursor == "older-page":
                    return [raw(begin - timedelta(minutes=5), 9), raw(begin, 10)], "crossed-bound"
                if start <= begin - timedelta(minutes=8):
                    return [raw(begin - timedelta(minutes=7), 8)], "older-page"
                return [raw(begin, 10)], "no-earlier"

            async def klines(self, **kwargs):
                return []

        service.adapter = lambda instrument, **kwargs: Adapter()
        await service.backfill(inst, anchor)
        reference = service.flow.references[(inst["instrument_id"], iso(begin))]["last_trade"]
        assert parse_time(reference["ts"]) == begin - timedelta(minutes=5)
        assert any(cursor == "older-page" for _, cursor in calls)
        assert len(calls) <= 33


@pytest.mark.asyncio
async def test_wholly_quiet_binance_spot_pre24_uses_documented_end_bound_not_fake_zero_price(setup):
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    begin = parse_time("2026-06-16T00:00:00Z")
    calls = []

    class Adapter:
        async def trades(self, *, start=None, end=None, cursor=None):
            calls.append((start, end, cursor))
            if start is None and cursor is None:
                return [
                    {
                        "a": 19,
                        "p": "7",
                        "q": "1",
                        "T": int((begin - timedelta(hours=3)).timestamp() * 1000),
                        "m": False,
                    }
                ], None
            return [], None

        async def klines(self, **kwargs):
            return []

    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service.adapter = lambda instrument, **kwargs: Adapter()
        await service.backfill(inst, begin + timedelta(days=1))
        reference = service.flow.references[(inst["instrument_id"], iso(begin))]["last_trade"]
        assert reference["price"] == "7" and parse_time(reference["ts"]) == begin - timedelta(hours=3)
        assert (None, begin - timedelta(milliseconds=1), None) in calls


@pytest.mark.asyncio
async def test_capture_tier_uses_durable_activation_and_demotion_history_not_future_hot_state(
    setup, monkeypatch
):
    import tank.market.service as module
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    perp = add_perp_instrument(settings, inst)
    clock = parse_time("2026-06-17T11:00:00Z")
    monkeypatch.setattr(module, "iso", lambda value=None: iso(clock if value is None else value))
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        token = inst["token_id"]
        assert (await service._capture_metadata(token, clock))["tier"] == "UNKNOWN"
        await repo.put(
            "mkt.oi_obs",
            "unknown-tier-oi",
            {
                "instrument_id": perp["instrument_id"],
                "token_id": token,
                "ts": iso(clock - timedelta(seconds=90)),
                "metric_key": "binance.usdm.openInterest",
                "oi_native": "1",
                "oi_usd": "2",
            },
        )
        from tank.analysis.truth import compute_market_truth

        unknown_truth = compute_market_truth(await service.snapshot(token, clock))
        check = next(
            row for row in unknown_truth["data_confidence"]["freshness"] if row["collection"] == "oi"
        )
        assert check["cadence_s"] is None and check["reason"] == "NOT_ESTIMABLE"
        assert check["cadence_source"] == "CAPTURE_TIER_HISTORY_UNAVAILABLE"
        await service._record_capture_tier(token, "BASE", "CAPTURE_POLICY_STARTED")
        baseline = clock
        clock += timedelta(hours=1)
        await repo.put("evt.events", "tier-event", {"event_id": "tier-event", "public_at": iso(clock)})
        await service.escalate("tier-event", [token])
        active = clock
        assert (await service._capture_metadata(token, active - timedelta(minutes=1)))["tier"] == "BASE"
        assert (await service._capture_metadata(token, active))["tier"] == "HOT"
        clock += timedelta(minutes=1)
        await service.demote(token)
        restarted = MarketService(repo, settings, http=http)
        assert (await restarted._capture_metadata(token, active))["tier"] == "HOT"
        assert (await restarted._capture_metadata(token, clock))["tier"] == "BASE"
        assert (await restarted._capture_metadata(token, baseline - timedelta(seconds=1)))[
            "tier"
        ] == "UNKNOWN"


@pytest.mark.asyncio
async def test_actual_producer_tier_metadata_controls_90second_oi_and_funding_freshness(setup, monkeypatch):
    from tank.analysis.truth import compute_market_truth
    from tank.util import iso, utcnow

    repo, settings, spot = setup
    perp = add_perp_instrument(settings, spot)
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    cutoff = utcnow().replace(second=0, microsecond=0)
    stamp = iso(cutoff - timedelta(seconds=90))
    iid = perp["instrument_id"]
    await repo.put(
        "mkt.oi_obs",
        "oi",
        {
            "instrument_id": iid,
            "token_id": spot["token_id"],
            "ts": stamp,
            "metric_key": "binance.usdm.openInterest",
            "oi_native": "100",
            "oi_usd": "200",
        },
    )
    await repo.put(
        "mkt.funding_obs",
        "funding",
        {
            "instrument_id": iid,
            "token_id": spot["token_id"],
            "as_of": stamp,
            "kind": "CURRENT_ESTIMATE",
            "metric_key": "binance.usdm.funding",
            "rate": "0.0001",
            "interval_h": "8",
            "rate_8h_eq": "0.0001",
        },
    )
    await repo.put(
        "mkt.hot_tokens",
        spot["token_id"] + ":event",
        {"token_id": spot["token_id"], "event_id": "event", "since": stamp},
    )
    import tank.market.service as service_module

    clock = cutoff - timedelta(seconds=1)
    monkeypatch.setattr(service_module, "iso", lambda value=None: iso(clock if value is None else value))
    await service._record_capture_tier(spot["token_id"], "HOT", "TEST_ACTUAL_POLICY_ACTIVATED")
    hot_snapshot = await service.snapshot(spot["token_id"], cutoff)
    assert hot_snapshot["capture_tier"] == hot_snapshot["capture_metadata"]["tier"] == "HOT"
    hot = compute_market_truth(hot_snapshot)
    checks = [
        c
        for c in hot["data_confidence"]["freshness"]
        if c["instrument_id"] == iid and c["collection"] in ("oi", "funding")
    ]
    assert len(checks) == 2 and all(
        c["age_s"] == 90
        and c["cadence_s"] == 60
        and c["cadence_source"] == "CAPTURE_METADATA"
        and not c["fresh"]
        for c in checks
    )
    assert hot_snapshot["capture_metadata"]["collections"]["supply"]["by_kind"]["EXCHANGE_INVENTORY"] == 900
    assert hot_snapshot["capture_metadata"]["collections"]["supply"]["by_kind"]["FUNDAMENTALS"] == 21600
    clock = cutoff + timedelta(seconds=1)
    await service.demote(spot["token_id"], "event")
    assert (await MarketService(repo, settings, http=http).snapshot(spot["token_id"], cutoff))[
        "capture_tier"
    ] == "HOT"
    base_snapshot = await service.snapshot(spot["token_id"], clock)
    assert base_snapshot["capture_tier"] == "BASE"
    base = compute_market_truth(base_snapshot)
    checks = [
        c
        for c in base["data_confidence"]["freshness"]
        if c["instrument_id"] == iid and c["collection"] in ("oi", "funding")
    ]
    assert len(checks) == 2 and all(c["cadence_s"] == 300 and c["fresh"] for c in checks)
    assert (
        base_snapshot["capture_metadata"]["collections"]["supply"]["by_kind"]["EXCHANGE_INVENTORY"] == 21600
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_liquidation_coverage_uses_processed_socket_watermark_not_last_liquidation_or_future_funding(
    setup,
):
    from tank.market.books import OrderBook
    from tank.market.streams import STREAM_PING_INTERVAL_S, process_frame
    from tank.util import iso, utcnow

    repo, settings, spot = setup
    perp = add_perp_instrument(settings, spot)
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    iid = perp["instrument_id"]
    cutoff = utcnow().replace(microsecond=0)
    stamp = cutoff - timedelta(seconds=1)
    service.books[iid] = OrderBook(perp)
    service._connected_since[iid] = cutoff - timedelta(seconds=20)
    await repo.put(
        "mkt.hot_tokens",
        spot["token_id"] + ":event",
        {"token_id": spot["token_id"], "event_id": "event", "since": iso(stamp)},
    )
    frame = {
        "data": {
            "e": "markPriceUpdate",
            "E": int(stamp.timestamp() * 1000),
            "T": int((cutoff + timedelta(hours=8)).timestamp() * 1000),
            "p": "100",
            "r": "0.0001",
        }
    }
    await process_frame(service, perp, frame)
    snapshot = await service.snapshot(spot["token_id"], cutoff)
    capture = snapshot["capture_metadata"]["collections"]["liquidations"]
    assert capture["observed_through"][iid] == iso(stamp)
    assert capture["coverage_status_by_instrument"][iid] == "CONNECTED"
    assert capture["cadence_s"] == STREAM_PING_INTERVAL_S
    assert snapshot["liquidations"] == []  # Noevent is not a fabricatedzero liquidation.
    assert capture["coverage_start"][iid] == iso(cutoff - timedelta(seconds=20))
    historical = await service.snapshot(spot["token_id"], cutoff - timedelta(hours=1))
    assert iid not in historical["capture_metadata"]["collections"]["liquidations"]["observed_through"]
    service._connected_since.pop(iid)
    disconnected = await service.snapshot(spot["token_id"], cutoff)
    assert iid not in disconnected["capture_metadata"]["collections"]["liquidations"]["observed_through"]
    assert (
        disconnected["capture_metadata"]["collections"]["liquidations"]["coverage_status_by_instrument"][iid]
        == "DISCONNECTED"
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_backfill_cannot_regress_reconnect_checkpoint(setup):
    repo, settings, inst = setup
    service = MarketService(repo, settings)
    now = int(utcnow().timestamp() * 1000)
    await service.ingest_trade(inst, {"a": 100, "p": "2", "q": "1", "T": now, "m": False})
    await service.ingest_trade(
        inst, {"a": 90, "p": "2", "q": "1", "T": now - 1000, "m": False}, backfill=True
    )
    await service.flush_market()
    checkpoint = await repo.get("mkt.capture_state", inst["instrument_id"])
    assert checkpoint["last_trade_id"] == "100"
    await service.close()


@pytest.mark.asyncio
async def test_missing_quote_volume_requires_complete_matching_prints(setup):
    repo, settings, inst = setup
    service = MarketService(repo, settings)
    minute = "2026-01-01T00:00:00Z"
    key = inst["instrument_id"] + ":" + minute
    row = {
        "instrument_id": inst["instrument_id"],
        "open_time": minute,
        "volume_native": "3",
        "quote_volume": None,
    }
    flow = {
        "instrument_id": inst["instrument_id"],
        "minute_start": minute,
        "buy_qty": "1",
        "sell_qty": "2",
        "quote_volume": "7",
        "volume_usd": "7",
        "complete": False,
    }
    await service._put("mkt.flow_bars_1m", key, flow)
    await service.flush_market()
    await service._store_kline(inst, dict(row))
    await service.flush_market()
    assert (await repo.get("mkt.klines_1m", key))["quote_volume"] is None
    flow["complete"] = True
    await service._put("mkt.flow_bars_1m", key, flow)
    await service.flush_market()
    await service._store_kline(inst, dict(row))
    await service.flush_market()
    saved = await repo.get("mkt.klines_1m", key)
    assert saved["quote_volume_usd"] == "7" and saved["quote_volume_method"] == "COMPLETE_MATCHING_RAW_PRINTS"
    await service._store_kline(inst, {**row, "volume_native": "4"})
    await service.flush_market()
    assert (await repo.get("mkt.klines_1m", key))["quote_volume"] is None
    await service.close()


def test_verified_exact_event_time_selects_t0():
    from tank.market.capture import event_anchor

    event = {
        "event_time": {"value": "2026-01-01T00:00:00Z", "precision": "EXACT"},
        "state_verified": True,
        "public_at": "2026-01-01T00:01:00Z",
    }
    assert event_anchor(event) == {
        "type": "T0",
        "ts": "2026-01-01T00:00:00Z",
        "ts_prec": "EXACT",
        "ts_prec_reason": None,
    }
    event["state_verified"] = False
    assert event_anchor(event)["type"] == "T1"


def test_missing_oi_field_is_data_error_without_runtime_semantic_promotion():
    from tank.market import normalize

    inst = {
        "venue": "binance",
        "market_type": "PERP_LINEAR",
        "instrument_id": "binance:perp:T",
        "token_id": "T",
    }
    with pytest.raises((KeyError, ValueError)):
        normalize.oi(inst, {"unexpected_open_interest_v2": "123"})


@pytest.mark.asyncio
async def test_market_and_sources_share_weighted_binance_budget(setup, monkeypatch):
    from tank.market.rate_gate import binance_request_weight
    from tank.sources.rate_limits import shared_request_budgets

    repo, settings, _ = setup
    calls = []

    async def acquire(key, capacity, window_s, *, weight=1):
        calls.append((key, capacity, window_s, weight))

    monkeypatch.setattr(shared_request_budgets, "acquire", acquire)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    ) as client:
        service = MarketService(repo, settings, http=client)
        await service._request(
            "GET", "https://api.binance.com/api/v3/depth", params={"symbol": "BTCUSDT", "limit": 1000}
        )
        await service._request("GET", "https://fapi.binance.com/fapi/v1/fundingInfo")
        await service._request("GET", "https://fapi.binance.com/futures/data/openInterestHist")
    assert calls == [
        ("binance.spot", 3000, 60, 50),
        ("binance.usdm.funding", 250, 300, 1),
        ("binance.usdm.statistics", 500, 300, 1),
    ]
    assert binance_request_weight("fapi.binance.com", "/fapi/v1/aggTrades", {}) == 20
    assert binance_request_weight("fapi.binance.com", "/fapi/v1/klines", {"limit": 1000}) == 5
    with pytest.raises(ValueError, match="Unconfigured"):
        binance_request_weight("api.binance.com", "/unverified-new-endpoint", {})


@pytest.mark.asyncio
async def test_disconnect_invalidates_evicted_durable_flow_but_preserves_archive_proof(setup):
    from tank.util import iso

    repo, settings, inst = setup
    service = MarketService(repo, settings)
    minute = (utcnow() - timedelta(minutes=10)).replace(second=0, microsecond=0)
    raw = {"a": 1, "p": "2", "q": "1", "T": int(minute.timestamp() * 1000), "m": False}
    await service.ingest_trade(inst, raw)
    key = inst["instrument_id"] + ":" + iso(minute)
    await service._flush_flow()
    await service.flush_market()
    bar = await repo.get("mkt.flow_bars_1m", key)
    bar["complete"] = True
    await repo.put("mkt.flow_bars_1m", key, bar)
    service.flow.bars.clear()
    await service.mark_capture_gap(
        inst, minute + timedelta(seconds=10), minute + timedelta(seconds=20), "DISCONNECTED"
    )
    await service.flush_market()
    assert not (await repo.get("mkt.flow_bars_1m", key))["complete"]
    bar.update(complete=True, source_archive="verified-archive")
    await repo.put("mkt.flow_bars_1m", key, bar)
    await service.mark_capture_gap(
        inst, minute + timedelta(seconds=10), minute + timedelta(seconds=20), "DISCONNECTED"
    )
    assert (await repo.get("mkt.flow_bars_1m", key))["complete"]
    await service.close()


@pytest.mark.asyncio
async def test_capture_wait_is_bounded_and_late_completion_supersedes_timeout(setup):
    import asyncio

    from tank.util import iso

    repo, settings, inst = setup
    service = MarketService(repo, settings)
    row = {
        "backfill_id": "BF-wait",
        "event_id": "E-wait",
        "token_id": inst["token_id"],
        "anchor": iso(),
        "started_at": iso(),
        "status": "RUNNING",
        "reason": "PENDING",
    }
    await repo.put("mkt.backfill_runs", row["backfill_id"], row)
    waited = await service.wait_capture("E-wait", inst["token_id"], timeout_s=0)
    assert waited["status"] == "INCOMPLETE" and waited["reason"] == "BOUNDED_PREREQUISITE_WAIT_EXPIRED"

    async def finish():
        await asyncio.sleep(0.01)
        row.update(status="COMPLETE", reason=None)
        await repo.put("mkt.backfill_runs", row["backfill_id"], row)

    worker = asyncio.create_task(finish())
    done = await service.wait_capture("E-wait", inst["token_id"], timeout_s=1)
    await worker
    assert done["status"] == "COMPLETE" and done["reason"] is None
    snapshot = await service.snapshot(inst["token_id"], iso())
    assert snapshot["capture_status"]["status"] == "COMPLETE"
    assert snapshot["capture_status"]["bounded_wait_s"] > 0
    await service.close()


@pytest.mark.asyncio
async def test_hot_expiry_protection_is_scoped_to_event_and_token(setup):
    from tank.util import iso

    repo, settings, inst = setup
    service = MarketService(repo, settings)
    await repo.put(
        "ana.calls",
        "other",
        {"call_id": "other", "event_id": "OTHER", "token_id": inst["token_id"], "status": "OPEN"},
    )
    await service.escalate("E-protected", [inst["token_id"]])
    key = inst["token_id"] + ":E-protected"
    assert (await repo.get("mkt.hot_tokens", key))["expires_at"] is not None
    await repo.put(
        "ana.calls",
        "own",
        {"call_id": "own", "event_id": "E-protected", "token_id": inst["token_id"], "status": "OPEN"},
    )
    await service.escalate("E-protected", [inst["token_id"]])
    assert (await repo.get("mkt.hot_tokens", key))["expires_at"] is None
    await repo.put(
        "ana.calls",
        "own",
        {"call_id": "own", "event_id": "E-protected", "token_id": inst["token_id"], "status": "CLOSED"},
    )
    await repo.put(
        "ana.watch_conditions",
        "armed",
        {
            "cond_id": "armed",
            "event_id": "E-protected",
            "token_id": inst["token_id"],
            "status": "ARMED",
            "expires_at": iso(utcnow() + timedelta(hours=1)),
        },
    )
    await service.escalate("E-protected", [inst["token_id"]])
    assert (await repo.get("mkt.hot_tokens", key))["expires_at"] is None
    condition = await repo.get("ana.watch_conditions", "armed")
    condition["expires_at"] = iso(utcnow() - timedelta(seconds=1))
    await repo.put("ana.watch_conditions", "armed", condition)
    await service.refresh_hot_expiry()
    restored = await repo.get("mkt.hot_tokens", key)
    from tank.util import iso, parse_time

    assert restored["expires_at"] == iso(parse_time(restored["since"]) + timedelta(hours=72))
    await service.close()


def test_documented_provider_scopes_and_hyperliquid_base_reservation():
    from tank.market.rate_gate import public_request_constraints

    def budgets(host, path="/info", params=None, payload=None):
        return public_request_constraints(host, path, params or {}, payload or {})

    assert budgets("api.bybit.com") == [("bybit", 300, 5, 1)]
    a = budgets("www.okx.com", "/api/v5/public/funding-rate", {"instId": "BTC-USDT-SWAP"})
    b = budgets("www.okx.com", "/api/v5/public/funding-rate", {"instId": "ETH-USDT-SWAP"})
    assert a[0][0] != b[0][0] and a[0][1:] == (5, 2, 1)
    assert budgets("api.hyperliquid.xyz", payload={"type": "l2Book"}) == [("hyperliquid", 600, 60, 2)]
    assert budgets("api.hyperliquid.xyz", payload={"type": "recentTrades"}) == [("hyperliquid", 600, 60, 20)]
    candle = {"type": "candleSnapshot", "req": {"interval": "1m", "startTime": 0, "endTime": 180000}}
    assert budgets("api.hyperliquid.xyz", payload=candle) == [("hyperliquid", 600, 60, 21)]
    with pytest.raises(ValueError):
        budgets("www.okx.com", "/new-undocumented-path")


@pytest.mark.asyncio
async def test_hyperliquid_actual_response_surcharge_is_settled_in_provider_slot(setup, monkeypatch):
    from tank.sources.rate_limits import shared_request_budgets

    repo, settings, _ = setup
    charged = []

    async def acquire(key, capacity, window_s, *, weight=1):
        charged.append((key, capacity, window_s, weight))

    monkeypatch.setattr(shared_request_budgets, "acquire", acquire)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=[{"tid": i} for i in range(41)])
        )
    ) as client:
        service = MarketService(repo, settings, http=client)
        result = await service._request(
            "POST", "https://api.hyperliquid.xyz/info", json={"type": "recentTrades", "coin": "BTC"}
        )
    assert len(result) == 41
    assert charged == [("hyperliquid", 600, 60, 20), ("hyperliquid", 600, 60, 3)]


@pytest.mark.asyncio
async def test_each_rate_limit_retry_consumes_shared_provider_budget(setup, monkeypatch):
    from tank.sources.rate_limits import shared_request_budgets

    repo, settings, _ = setup
    charged = []
    requests = []

    async def acquire(key, capacity, window_s, *, weight=1):
        charged.append(weight)

    async def no_sleep(seconds):
        return None

    def respond(request):
        requests.append(request)
        return (
            httpx.Response(429, headers={"Retry-After": "1"})
            if len(requests) == 1
            else httpx.Response(200, json=[])
        )

    monkeypatch.setattr(shared_request_budgets, "acquire", acquire)
    monkeypatch.setattr("tank.market.service.asyncio.sleep", no_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service = MarketService(repo, settings, http=client)
        await service._request(
            "GET", "https://api.binance.com/api/v3/aggTrades", params={"symbol": "BTCUSDT"}
        )
    assert len(requests) == 2 and charged == [4, 4]


@pytest.mark.asyncio
async def test_hyperliquid_tokens_share_observed_context_and_next_cycle_refreshes(setup, monkeypatch):
    from tank.market import normalize as norm
    from tank.sources.rate_limits import shared_request_budgets

    repo, settings, _ = setup
    requests = []
    charged = []
    clock = [0.0]

    async def acquire(key, capacity, window_s, *, weight=1):
        charged.append(weight)

    def respond(request):
        requests.append(request)
        return httpx.Response(
            200,
            json=[
                {"universe": [{"name": "BTC"}, {"name": "ETH"}]},
                [
                    {"openInterest": "1", "funding": ".001", "markPx": "100", "oraclePx": "100"},
                    {"openInterest": "2", "funding": ".002", "markPx": "200", "oraclePx": "200"},
                ],
            ],
        )

    monkeypatch.setattr(shared_request_budgets, "acquire", acquire)
    # Patch only this module's clock adapter; event-loop clock remains real.
    from types import SimpleNamespace

    monkeypatch.setattr("tank.market.service.time", SimpleNamespace(monotonic=lambda: clock[0]))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service = MarketService(repo, settings, http=client)
        instruments = [
            {
                "venue": "hyperliquid",
                "market_type": "PERP_LINEAR",
                "venue_symbol": symbol,
                "instrument_id": "hl:" + symbol,
                "token_id": symbol,
                "quote_asset": "USDC",
            }
            for symbol in ["BTC", "ETH"]
        ]
        first = [await service.adapter(inst).ticker() for inst in instruments]
        assert len(requests) == 1 and charged == [20]
        assert first[0]["_capture_observed_at"] == first[1]["_capture_observed_at"]
        original = norm.oi(instruments[0], first[0])
        clock[0] = 31
        second = await service.adapter(instruments[0]).ticker()
        assert len(requests) == 2 and charged == [20, 20]
        assert norm.oi(instruments[0], first[0])["ts"] == original["ts"]
        assert second["_capture_observed_at"] >= first[0]["_capture_observed_at"]


@pytest.mark.asyncio
async def test_hyperliquid_settled_cache_is_exact_range_and_settlement_period_scoped(setup, monkeypatch):
    from tank.sources.rate_limits import shared_request_budgets

    repo, settings, _ = setup
    calls = []
    now = ["2026-01-01T01:10:00Z"]

    async def acquire(*args, **kwargs):
        pass

    def stamp(value=None):
        from tank.util import iso

        return iso(value) if value is not None else now[0]

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json=[{"time": 1767225600000, "fundingRate": ".001"}])

    monkeypatch.setattr(shared_request_budgets, "acquire", acquire)
    monkeypatch.setattr("tank.market.service.iso", stamp)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service = MarketService(repo, settings, http=client)
        first = {"type": "fundingHistory", "coin": "BTC", "startTime": 0}
        a = await service._request("POST", "https://api.hyperliquid.xyz/info", json=first)
        now[0] = "2026-01-01T01:30:00Z"
        b = await service._request("POST", "https://api.hyperliquid.xyz/info", json=first)
        assert len(calls) == 1 and a[0]["_capture_observed_at"] == b[0]["_capture_observed_at"]
        await service._request(
            "POST", "https://api.hyperliquid.xyz/info", json={**first, "startTime": 3600000}
        )
        assert len(calls) == 2
        now[0] = "2026-01-01T02:00:00Z"
        c = await service._request("POST", "https://api.hyperliquid.xyz/info", json=first)
        assert len(calls) == 3 and c[0]["_capture_observed_at"] == now[0]


@pytest.mark.asyncio
async def test_capture_changes_are_durable_bounded_and_stable(setup):
    repo, settings, inst = setup
    service = MarketService(repo, settings)
    for key, status, finished in [
        ("a", "COMPLETE", "2026-01-01T01:00:00Z"),
        ("b", "PARTIAL", "2026-01-01T02:00:00Z"),
        ("c", "RUNNING", None),
    ]:
        await repo.put(
            "mkt.backfill_runs",
            key,
            {
                "backfill_id": key,
                "event_id": "E-change",
                "token_id": inst["token_id"],
                "anchor": "2026-01-01T00:00:00Z",
                "started_at": "2026-01-01T00:00:00Z",
                "finished_at": finished,
                "status": status,
            },
        )
    changes = [r async for r in service.capture_changes("2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z")]
    assert len(changes) == 1 and changes[0]["backfill_id"] == "a"
    restarted = MarketService(repo, settings)
    again = [r async for r in restarted.capture_changes("2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z")]
    assert again[0]["change_id"] == changes[0]["change_id"]
    await service.close()
    await restarted.close()


@pytest.mark.asyncio
async def test_optional_weight_budget_wait_does_not_block_baseline_http(setup, monkeypatch):
    import asyncio

    from tank.sources.rate_limits import shared_request_budgets

    repo, settings, _ = setup
    waiting = asyncio.Event()
    release = asyncio.Event()
    requests = []

    async def acquire(key, capacity, window_s, *, weight=1):
        if weight == 50:
            waiting.set()
            await release.wait()

    def respond(request):
        requests.append(request.url.path)
        return httpx.Response(200, json=[])

    monkeypatch.setattr(shared_request_budgets, "acquire", acquire)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service = MarketService(repo, settings, http=client)
        optional = asyncio.create_task(
            service._request(
                "GET", "https://api.binance.com/api/v3/depth", params={"limit": 1000}, _priority=2
            )
        )
        await waiting.wait()
        await asyncio.wait_for(
            service._request("GET", "https://api.binance.com/api/v3/klines", params={"limit": 3}), 0.5
        )
        assert requests == ["/api/v3/klines"]
        release.set()
        await optional
    assert requests == ["/api/v3/klines", "/api/v3/depth"]


@pytest.mark.asyncio
async def test_live_late_normalized_arrivals_coalesce_stably_and_ignore_future_points(setup):
    from tank.util import iso

    repo, settings, inst = setup
    service = MarketService(repo, settings)
    cutoff = (utcnow() - timedelta(hours=1)).replace(second=0, microsecond=0)
    captured = utcnow() - timedelta(minutes=5)
    anchor = cutoff - timedelta(hours=1)
    await repo.put(
        "out.reaction_checkpoints",
        "RWP-late",
        {
            "rwp_id": "RWP-late",
            "event_id": "E-late",
            "token_id": inst["token_id"],
            "checkpoint": "+1h",
            "revision": 1,
            "captured_at": iso(captured),
            "anchor": {"ts": iso(anchor), "type": "T0"},
            "window_end": iso(cutoff),
        },
    )
    raw = {
        "a": 1,
        "p": "2",
        "q": "1",
        "T": int((cutoff - timedelta(seconds=10)).timestamp() * 1000),
        "m": False,
    }
    await service.ingest_trade(inst, raw)
    await service.ingest_trade(inst, {**raw, "a": 2})

    async def changes():
        await service._flush_flow()
        await service.flush_market()
        return [
            r
            async for r in service.capture_changes(iso(captured), iso(utcnow() + timedelta(seconds=10)))
            if r["kind"] == "LATE_NORMALIZED_DATA"
        ]

    first = await changes()
    assert (
        len(first) == 1 and first[0]["source_count"] == 1
    )  # One actual changed aggregate; prints remain Parquet-only.
    assert first[0]["event_id"] == "E-late" and first[0]["anchor"] == iso(anchor)
    assert (await changes())[0]["change_id"] == first[0]["change_id"]
    assert not await service.ingest_trade(inst, raw)  # Immutable dedup creates no new arrival.
    await service.ingest_trade(
        inst, {**raw, "a": 3, "T": int((cutoff + timedelta(seconds=10)).timestamp() * 1000)}
    )
    assert (await changes())[0]["change_id"] == first[0]["change_id"]
    await service.ingest_trade(inst, {**raw, "a": 4})
    assert (await changes())[0]["change_id"] != first[0]["change_id"]
    await service.close()


@pytest.mark.asyncio
async def test_identical_normalized_poll_does_not_fabricate_new_availability(setup):
    from tank.util import iso

    repo, settings, inst = setup
    service = MarketService(repo, settings)
    minute = "2026-01-01T00:00:00Z"
    row = {
        "instrument_id": inst["instrument_id"],
        "open_time": minute,
        "volume_native": "1",
        "quote_volume": "2",
        "received_ts": "2026-01-01T00:01:00Z",
    }
    await service._store_kline(inst, row)
    await service.flush_market()
    key = inst["instrument_id"] + ":" + minute
    original = await repo.get("mkt.klines_1m", key)
    await service._store_kline(inst, {**row, "received_ts": iso()})
    await service.flush_market()
    unchanged = await repo.get("mkt.klines_1m", key)
    assert unchanged["recorded_at"] == original["recorded_at"]
    assert unchanged["received_ts"] == original["received_ts"]
    await service._store_kline(inst, {**row, "volume_native": "2"})
    await service.flush_market()
    assert (await repo.get("mkt.klines_1m", key))["recorded_at"] != original["recorded_at"]
    await service.close()


@pytest.mark.asyncio
async def test_cold_bybit_oi_uses_registry_without_runtime_probe(setup):
    from tank.analysis.truth import derivatives_truth
    from tank.market import normalize as norm

    repo, settings, inst = setup
    service = MarketService(repo, settings)
    inst = {
        **inst,
        "instrument_id": "bybit:perp_linear:ABCUSD",
        "venue": "bybit",
        "market_type": "PERP_LINEAR",
    }
    key = "bybit.linear.open-interest"
    service.semantics = [
        {
            "metric_key": key,
            "metric_family": "oi",
            "timestamp_semantics": "EXCHANGE_EVENT_TIME",
            "unit": "BASE_QTY",
            "sidedness": "SINGLE_SIDED",
            "aggregation_class": "oi_native_single_sided",
        }
    ]
    end = utcnow().replace(second=0, microsecond=0)
    start = end - timedelta(hours=4)
    endpoints = [
        norm.oi(inst, {"singleOpenInterest": "123.45", "timestamp": str(int(t.timestamp() * 1000))})
        for t in (start, end)
    ]
    definitions = service._instrument_semantics([inst])
    actual = derivatives_truth(
        {"token_id": inst["token_id"], "instruments": [inst], "oi": endpoints, "semantics": definitions},
        start,
        end,
    )
    assert actual["synchronized"] is not None
    assert actual["per_venue"][0]["oi_native_chg_pct"] == 0
    assert all(row["ts_prec"] == "EXACT" and row["synchronized_eligible"] for row in endpoints)
    assert not hasattr(service, "self_test")
    assert not await repo.list("qa.reviews", filters={"subject": "METRIC_SEMANTICS"})
    await service.close()


@pytest.mark.asyncio
async def test_bybit_legacy_temporal_proof_not_reused_by_condition_loader(setup):
    repo, settings, inst = setup
    service = MarketService(repo, settings)
    inst = {**inst, "venue": "bybit", "market_type": "PERP_LINEAR"}
    service.instruments = {inst["instrument_id"]: inst}
    key = "bybit.linear.open-interest"
    definition = {
        "metric_key": key,
        "timestamp_semantics": "EXCHANGE_EVENT_TIME",
    }
    service.semantics = [definition]
    await repo.put(
        "qa.reviews",
        "old-bybit-proof",
        {
            "subject": "METRIC_SEMANTICS",
            "instrument_id": inst["instrument_id"],
            "metric_key": key,
            "at": utcnow().isoformat(),
            "semantics": {
                **definition,
                "timestamp_semantics": "BUCKET_START",
                "semantic_confidence": "VERIFIED",
            },
        },
    )
    snapshot = await service.condition_snapshot(
        inst["token_id"], utcnow(), requirements=[{"metric": "deriv.native_oi_chg_pct", "window": "W4H"}]
    )
    assert len(snapshot["semantics"]) == 1
    assert "semantic_confidence" not in snapshot["semantics"][0]
    assert snapshot["semantics"][0]["timestamp_semantics"] == "EXCHANGE_EVENT_TIME"
    assert await repo.get("qa.reviews", "old-bybit-proof")  # old audit evidence remains immutable
    await service.close()

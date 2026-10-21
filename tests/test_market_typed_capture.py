"""§14 producer batching, verified raw recovery, and aggregate-only queries."""

# ruff: noqa: F811
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from test_market_service import setup  # noqa: F401

from tank.market import normalize
from tank.market.archive import TradeArchive
from tank.market.service import MarketService
from tank.util import iso, parse_time, utcnow


@pytest.mark.integration
@pytest.mark.asyncio
async def test_actual_batched_capture_restart_late_minute_and_no_sql_trades(setup):
    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        minute = utcnow().replace(second=0, microsecond=0) - timedelta(minutes=4)
        calls = []
        original = repo.put_market_batch

        async def batch(collection, rows, **kwargs):
            assert collection != "mkt.trades"
            calls.append((collection, len(rows)))
            return await original(collection, rows, **kwargs)

        repo.put_market_batch = batch
        for number in range(100):
            assert await service.ingest_trade(
                inst,
                {
                    "a": number + 1,
                    "p": "2.123456789012345678",
                    "q": "3",
                    "T": int(minute.timestamp() * 1000) + number,
                    "m": False,
                },
            )
        assert not calls
        assert not list(settings.data_dir.glob("raw/trades/**/*.jsonl"))
        await service._flush_flow()
        await service.flush_market()
        assert sum(c == "mkt.flow_bars_1m" for c, _ in calls) == 1
        restarted = MarketService(repo, settings, http=http)
        assert not await restarted.ingest_trade(
            inst,
            {"a": 1, "p": "2.123456789012345678", "q": "3", "T": int(minute.timestamp() * 1000), "m": False},
        )
        assert await restarted.ingest_trade(
            inst, {"a": 101, "p": "3", "q": "1", "T": int(minute.timestamp() * 1000) + 101, "m": True}
        )
        await restarted._flush_flow()
        await restarted.flush_market()
        bars = await repo.list("mkt.flow_bars_1m", filters={"instrument_id": inst["instrument_id"]})
        assert len(bars) == 1
        assert Decimal(bars[0]["buy_qty"]) == 300
        assert Decimal(bars[0]["sell_qty"]) == 1
        assert Decimal(bars[0]["sum_sq_qty"]) == 901
        assert bars[0]["complete"] is False


@pytest.mark.integration
def test_sealed_hour_rebuilds_a_replay_window_from_frozen_file(tmp_path):
    inst = {
        "instrument_id": "binance:spot:ABCUSD",
        "token_id": "test:ABC",
        "venue": "binance",
        "market_type": "SPOT",
        "venue_symbol": "ABCUSD",
        "quote_asset": "USD",
        "contract_multiplier": "1",
    }
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst], grace_s=0)
    for tid, second in [(1, 10), (2, 50)]:
        raw.append(
            normalize.trade(
                inst,
                {
                    "a": tid,
                    "p": str(tid),
                    "q": "1",
                    "m": False,
                    "T": int((minute + timedelta(seconds=second)).timestamp() * 1000),
                },
            )
        )
    raw.coverage(inst["instrument_id"], minute, minute + timedelta(minutes=2), complete=True)
    raw.write(raw.take(), now=minute + timedelta(hours=2))
    frozen = raw.frozen_manifest(inst["instrument_id"], minute)
    path, digest = Path(frozen["path"]), frozen["sha256"]
    replay = raw.rebuild_window(
        [frozen], inst["instrument_id"], minute, minute + timedelta(seconds=17), instrument=inst
    )
    assert replay["complete"] is True and replay["flow_bars"] == []
    assert any(
        s["end"] == iso(minute + timedelta(seconds=17)) and Decimal(s["buy_qty"]) == 1
        for s in replay["flow_segments"]
    )
    zero = raw.rebuild_window(
        [frozen],
        inst["instrument_id"],
        minute + timedelta(minutes=1),
        minute + timedelta(minutes=2),
        instrument=inst,
    )
    assert zero["complete"] and len(zero["flow_bars"]) == 1
    assert zero["flow_bars"][0]["n_buy"] == 0 and zero["flow_bars"][0]["complete"]
    assert len(raw.read_verified(path, digest)) == 2


@pytest.mark.asyncio
async def test_snapshot_does_not_rebuild_raw_windows(setup, monkeypatch):
    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)

        def forbidden(*args, **kwargs):
            raise AssertionError("ordinary snapshot cannot read raw evidence")

        monkeypatch.setattr(service.trade_archive, "read_minutes", forbidden)
        monkeypatch.setattr(service.trade_archive, "read_hour", forbidden)
        service._connected_since[inst["instrument_id"]] = utcnow() - timedelta(minutes=1)
        service._stream_watermarks[inst["instrument_id"]] = {"ts": iso()}
        snapshot = await service.snapshot(inst["token_id"], iso())
        assert "trades" not in snapshot and "boundary_trades" not in snapshot


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supervised_run_reuses_http_after_repository_failure(setup, monkeypatch):
    import asyncio

    repo, settings, _ = setup
    settings.replay = False
    requests = []

    async def serve(reader, writer):
        requests.append(await reader.readuntil(b"\r\n\r\n"))
        writer.write(
            b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{"ok":true}'
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    service = MarketService(repo, settings)
    stop = asyncio.Event()

    async def idle(*args, **kwargs):
        await stop.wait()

    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(service, "_sync_streams", noop)
    for method in ("_instrument_loop", "_supply_loop", "bootstrap_history"):
        monkeypatch.setattr(service, method, idle)
    batch = repo.put_market_batch
    failed = False

    async def fail_once(collection, rows, **kwargs):
        nonlocal failed
        if collection == "mkt.capture_gaps" and not failed:
            failed = True
            raise RuntimeError("injected repository outage")
        return await batch(collection, rows, **kwargs)

    repo.put_market_batch = fail_once
    await service._put(
        "mkt.capture_gaps",
        "fixture",
        {
            "gap_id": "fixture",
            "instrument_id": "fixture",
            "kind": "TEST",
            "reason": "TEST",
            "observed_at": iso(),
        },
    )
    with pytest.raises(RuntimeError, match="repository outage"):
        await service.run(stop)
    assert not service.http.is_closed and not service.streams
    response = await service.http.get(f"http://127.0.0.1:{port}/")
    assert response.json() == {"ok": True}

    async def stop_after_flush(*args, **kwargs):
        stop.set()

    monkeypatch.setattr(service, "_flush_flow", stop_after_flush)
    await service.run(stop)
    assert await repo.get("mkt.capture_gaps", "fixture")
    assert not service.http.is_closed and len(requests) == 1
    await service.close()
    assert service.http.is_closed
    server.close()
    await server.wait_closed()


@pytest.mark.postgres
@pytest.mark.integration
@pytest.mark.asyncio
async def test_actual_postgres_batch_producer_restart_and_late_minute(setup):
    import os

    from psycopg.conninfo import conninfo_to_dict

    from tank.storage import PostgresRepository

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Dedicated QA PostgreSQL required")
    destination = conninfo_to_dict(dsn)
    if destination.get("port") != "55437" or not destination.get("dbname", "").startswith("tank_"):
        pytest.skip("Only dedicated QA PostgreSQL allowed")
    _, settings, inst = setup
    # Isolate the real natural key from any previous run without deleting evidence.
    from tank.util import new_id

    inst["instrument_id"] += ":" + new_id("PG")
    registry = settings.load_config("registry")
    registry["instruments"] = [inst]
    import yaml

    (settings.config_dir / "registry.yaml").write_text(yaml.safe_dump(registry))
    repo = PostgresRepository(dsn)
    await repo.open()
    try:
        await repo.migrate()
        await test_actual_batched_capture_restart_late_minute_and_no_sql_trades((repo, settings, inst))
    finally:
        await repo.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_verified_daily_trade_import_uses_parquet_and_multirow_aggregates(setup):
    import hashlib
    import io
    import zipfile

    from tank.market.archive import fetch_archive

    repo, settings, _inst = setup
    day = parse_time("2026-01-01T00:00:00Z")
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as zipped:
        zipped.writestr(
            "ABCUSD.csv",
            f"1,2,3,1,1,{int(day.timestamp() * 1000000)},false\n2,4,5,2,2,{int((day + timedelta(minutes=2)).timestamp() * 1000000)},true\n",
        )
    payload = content.getvalue()
    filename = "ABCUSD-aggTrades-2026-01-01.zip"

    def response(request):
        if request.url.path.endswith("CHECKSUM"):
            return httpx.Response(200, text=hashlib.sha256(payload).hexdigest() + "  " + filename)
        return httpx.Response(200, content=payload)

    writes = []
    original = repo.put_market_batch

    async def batch(collection, rows, **kwargs):
        assert collection != "mkt.trades"
        writes.append((collection, len(rows)))
        return await original(collection, rows, **kwargs)

    repo.put_market_batch = batch
    import_started = utcnow()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        result = await fetch_archive(repo, settings, "ABCUSD", "2026-01-01", kind="aggTrades", http=http)
    import_finished = utcnow()
    assert result["complete"] and result["rows"] == 2
    bars = await repo.list("mkt.flow_bars_1m", limit=2000)
    assert len(bars) == 1440 and all(row["complete"] for row in bars)
    assert sorted(count for family, count in writes if family == "mkt.flow_bars_1m") == [440, 1000]
    registry = settings.load_config("registry")
    archive = TradeArchive(settings.data_dir, instruments=registry["instruments"])
    instrument = next(i for i in registry["instruments"] if i["venue_symbol"] == "ABCUSD")
    path = archive.path(instrument["instrument_id"], day)
    assert path.exists() and not list(path.parent.glob(".*.arrows"))  # finished hours are sealed
    decoded = archive.read_hour(path)
    by_id = {point["trade_id"]: point for point in decoded}
    assert set(by_id) == {"1", "2"}
    for point in decoded:
        assert point["capture_source"] == "ARCHIVE"
        assert point["source_archive"] == result["archive_id"]
        assert import_started <= parse_time(point["received_ts"]) <= import_finished
    assert by_id["1"]["ts"] == iso(day)
    assert by_id["2"]["ts"] == iso(day + timedelta(minutes=2))
    assert Decimal(by_id["1"]["qty_native"]) == 3
    assert Decimal(by_id["2"]["qty_native"]) == 5
    assert by_id["1"]["side"] == "BUY" and by_id["2"]["side"] == "SELL"
    import json

    assert json.loads(by_id["1"]["source_payload_json"]) == {
        "a": "1",
        "p": "2",
        "q": "3",
        "f": "1",
        "l": "1",
        "T": str(int(day.timestamp() * 1000000)),
        "m": False,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_proved_quiet_aligned_cutoff_retains_actual_prior_print_without_raw_query(setup, monkeypatch):
    from tank.analysis.truth import flow_reference

    repo, settings, inst = setup
    start = parse_time("2026-01-01T00:00:00Z")
    end = start + timedelta(minutes=2)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        await service.ingest_trade(
            inst,
            {
                "a": 1,
                "p": "2.123456789012345678",
                "q": "3",
                "T": int((start + timedelta(seconds=10)).timestamp() * 1000),
                "m": False,
            },
            backfill=True,
        )
        await service._persist_rest_flow_ranges(inst, start, end, [(start, end)])
        # Restart drops private trade buffers; ordinary analysis uses only bars.
        restarted = MarketService(repo, settings, http=http)

        def forbidden(*args, **kwargs):
            raise AssertionError("Raw recovery is not an ordinary truth reader")

        monkeypatch.setattr(restarted.trade_archive, "read_minutes", forbidden)
        monkeypatch.setattr(restarted.trade_archive, "read_hour", forbidden)
        snapshot = await restarted.snapshot(inst["token_id"], end)
        point = flow_reference(snapshot, inst["instrument_id"], end)
        assert point["trade_id"] == "1" and point["price"] == "2.123456789012345678"
        assert parse_time(point["ts"]) == start + timedelta(seconds=10)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_id_gap_invalidates_prior_and_intervening_minutes_and_nested_proofs(setup):
    repo, settings, inst = setup
    start = parse_time("2026-01-01T00:00:00Z")
    end = start + timedelta(minutes=3)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service._register_capture_boundaries(
            {inst["instrument_id"]}, end, {"ts": start + timedelta(seconds=30)}
        )
        for tid, seconds in ((1, 10), (3, 130)):
            await service.ingest_trade(
                inst,
                {
                    "a": tid,
                    "p": "2",
                    "q": "1",
                    "T": int((start + timedelta(seconds=seconds)).timestamp() * 1000),
                    "m": False,
                },
                backfill=True,
            )
        await service._persist_rest_flow_ranges(inst, start, end, [(start, end)])
        bars = await repo.list("mkt.flow_bars_1m")
        assert len(bars) == 3 and all(not row["complete"] for row in bars)
        first = next(row for row in bars if parse_time(row["minute_start"]) == start)
        assert all(
            not segment["complete"]
            for segment in first["boundary_segments"]
            if parse_time(segment["end"]) > start + timedelta(seconds=10)
        )
        assert all(
            not point["complete"]
            for point in first["reference_points"]
            if parse_time(point["boundary"]) > start + timedelta(seconds=10)
        )
        coverage = service.trade_archive.coverage_for(inst["instrument_id"], start)
        assert not service.flow.covered(start, start + timedelta(minutes=1), coverage)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_windowed_trade_import_covers_only_the_minutes_it_touches(setup):
    import hashlib
    import io
    import zipfile

    from tank.market.archive import fetch_archive

    repo, settings, _inst = setup
    day = parse_time("2026-01-01T00:00:00Z")
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as zipped:
        zipped.writestr(
            "ABCUSD.csv",
            "".join(
                f"{n},2,3,{n},{n},{int((day + timedelta(minutes=n)).timestamp() * 1000000)},false\n"
                for n in range(1, 6)
            ),
        )
    payload = content.getvalue()
    filename = "ABCUSD-aggTrades-2026-01-01.zip"
    downloads = []

    def response(request):
        downloads.append(request.url.path)
        if request.url.path.endswith("CHECKSUM"):
            return httpx.Response(200, text=hashlib.sha256(payload).hexdigest() + "  " + filename)
        return httpx.Response(200, content=payload)

    window = ("2026-01-01T00:01:30Z", "2026-01-01T00:03:10Z")
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        result = await fetch_archive(
            repo, settings, "ABCUSD", "2026-01-01", kind="aggTrades", http=http, window=window
        )
        again = await fetch_archive(
            repo, settings, "ABCUSD", "2026-01-01", kind="aggTrades", http=http, window=window
        )
    bars = sorted(await repo.list("mkt.flow_bars_1m", limit=2000), key=lambda r: r["minute_start"])
    assert [b["minute_start"] for b in bars] == [
        "2026-01-01T00:01:00Z",
        "2026-01-01T00:02:00Z",
        "2026-01-01T00:03:00Z",
    ]
    assert all(b["complete"] for b in bars) and result["rows"] == 3
    assert again["imported"] and len(downloads) == 2  # the span is already in PostgreSQL

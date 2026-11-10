"""ClickHouse market reads return what the PostgreSQL/memory reads return (HLD §7.1).

Runs against a real server: CLICKHOUSE_TEST_URL=http://user:password@host:8123.
The same typed rows go into the memory repository (put_market_batch) and into ClickHouse
(as tank-capture writes them); every routed read must give the same answer on both.
"""

import asyncio
import json
import os
import pickle
import socket
import struct
from contextlib import suppress
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest

from tank import clickhouse_market as chm
from tank import market_storage as ms
from tank.market import service as service_module
from tank.market.service import MarketService
from tank.market_schema import MARKET_COLUMNS
from tank.storage import MemoryRepository, PostgresRepository
from tank.util import iso

URL = os.environ.get("CLICKHOUSE_TEST_URL")
pytestmark = pytest.mark.integration
T0 = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
IIDS = ["binance:spot:BTCUSDT", "okx:spot:BTC-USDT"]


def kline(iid, minute, close, *, complete=True, recorded=None):
    at = T0 + timedelta(minutes=minute)
    return {
        "instrument_id": iid,
        "token_id": "bitcoin:BTC",
        "open_time": iso(at),
        "open_time_prec": "EXACT",
        "interval_s": 60,
        "o": "1",
        "h": "2",
        "l": "0.5",
        "c": close,
        "complete": complete,
        "recorded_at": iso(recorded or at + timedelta(seconds=61)),
        "recorded_at_prec": "COLLECTION_ONLY",
    }


def bar(iid, minute, *, complete=True, last="7"):
    at = T0 + timedelta(minutes=minute)
    return {
        "instrument_id": iid,
        "token_id": "bitcoin:BTC",
        "minute_start": iso(at),
        "minute_start_prec": "EXACT",
        "buy_qty": "1.5",
        "sell_qty": "0.25",
        "complete": complete,
        "last_trade": {"trade_id": last, "ts": iso(at + timedelta(seconds=30))},
        "recorded_at": iso(at + timedelta(seconds=62)),
        "recorded_at_prec": "COLLECTION_ONLY",
    }


def ch_row(collection, record, key):
    """A typed record as tank-capture inserts it (JSONEachRow)."""
    row = {"storage_key": key, "_version": 1}
    for name, kind in MARKET_COLUMNS[collection].items():
        value = record.get(name)
        if value is None:
            if kind == "text[]":
                row[name] = []
            continue
        if kind == "timestamptz":
            row[name] = value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
        elif kind == "jsonb":
            row[name] = json.dumps(value)
        elif kind == "numeric":
            row[name] = str(value)
        else:
            row[name] = value
    return row


@pytest.fixture
async def both():
    if not URL:
        pytest.fail("CLICKHOUSE_TEST_URL must point to an isolated ClickHouse test server")
    ch = chm.ClickHouse(URL)
    await ch.execute("DROP DATABASE IF EXISTS mkt")
    for statement in chm.ddl():
        await ch.execute(statement)
    memory = MemoryRepository()
    await memory.open()
    routed = PostgresRepository("postgresql:///unused")
    routed.clickhouse = ch

    async def put(collection, records):
        await memory.put_market_batch(collection, records)
        prepared = ms._prepare_records(collection, records, None, MARKET_COLUMNS[collection], False)
        body = "\n".join(json.dumps(ch_row(collection, r, k)) for k, r in prepared.items())
        response = await ch.http.post(
            ch.base,
            params={"query": f"INSERT INTO {collection} FORMAT JSONEachRow"},
            content=body.encode(),
            headers={"X-ClickHouse-User": ch.user, "X-ClickHouse-Key": ch.password},
        )
        assert response.status_code == 200, response.text

    yield memory, routed, put
    await ch.execute("DROP DATABASE IF EXISTS mkt")
    await ch.close()


async def collect(gen):
    return [row async for row in gen]


async def test_routed_reads_match_memory(both):
    memory, routed, put = both
    klines = [kline(iid, m, str(100 + m)) for iid in IIDS for m in range(6)]
    klines.append(kline(IIDS[0], 6, "0", complete=True))
    await put("mkt.klines_1m", klines)
    await put("mkt.flow_bars_1m", [bar(iid, m, last=str(m)) for iid in IIDS for m in range(4)])
    cutoff = T0 + timedelta(minutes=10)

    for repo in (memory, routed):
        assert repo is memory or chm.client(repo, "mkt.klines_1m") is not None

    async def same(call):
        a, b = await call(memory), await call(routed)
        assert a == b, (a, b)
        return a

    key = ms._prepare_records("mkt.klines_1m", [klines[0]], None, MARKET_COLUMNS["mkt.klines_1m"], False)
    (storage_key,) = key
    assert (await same(lambda r: r.get("mkt.klines_1m", storage_key)))["c"] == "100"
    page = await same(
        lambda r: collect(
            r.iter_range(
                "mkt.klines_1m",
                "open_time",
                start=T0,
                end=cutoff,
                filters={"instrument_id": IIDS[1]},
                batch_size=2,
            )
        )
    )
    assert [p["c"] for p in page] == [str(100 + m) for m in range(6)]
    await same(
        lambda r: r.latest_before(
            "mkt.klines_1m", "open_time", T0 + timedelta(minutes=3), filters={"instrument_id": IIDS[0]}
        )
    )
    await same(
        lambda r: r.list_latest(
            "mkt.flow_bars_1m", "minute_start", filters={"instrument_id": IIDS[0]}, limit=2
        )
    )
    await same(
        lambda r: collect(
            r.iter_late_arrivals(
                "mkt.klines_1m", "recorded_at", "open_time", since=T0, market_end=cutoff, batch_size=3
            )
        )
    )
    latest = await same(
        lambda r: r.latest_market_records(
            "mkt.klines_1m", time_field="open_time", cutoff=cutoff, instrument_ids=IIDS
        )
    )
    assert sorted(x["c"] for x in latest) == ["0", "105"]
    await same(
        lambda r: r.market_dependency_head(
            "mkt.klines_1m",
            "open_time",
            cutoff,
            instrument_ids=IIDS,
            orders=("availability", "semantic_latest", "semantic_earliest"),
        )
    )
    await same(
        lambda r: collect(
            r.iter_market_projection(
                "mkt.klines_1m",
                ["instrument_id", "open_time", "c"],
                "open_time",
                start=T0,
                end=cutoff,
                instrument_ids=IIDS,
                batch_size=4,
            )
        )
    )
    counts = await same(lambda r: r.market_series_counts(IIDS, T0, cutoff))
    assert counts == {IIDS[0]: 6, IIDS[1]: 6}
    await same(
        lambda r: ms.market_series_day_counts(r, IIDS, T0 - timedelta(hours=12), T0 + timedelta(days=1))
    )
    await same(lambda r: ms.market_kline_days_recorded_since(r, T0, cutoff))
    trades = await same(lambda r: ms.latest_market_trades(r, IIDS, start=T0))
    assert {t["last_trade"]["trade_id"] for t in trades} == {"3"}

    async def recorded(r):
        return [
            (iid, rec)
            for iid, rec, _ in await ms.market_rows_recorded_since(
                r, "mkt.flow_bars_1m", "minute_start", IIDS, T0, T0, cutoff
            )
        ]

    await same(recorded)
    await same(
        lambda r: ms.sampled_market_history(r, "mkt.klines_1m", "open_time", [], IIDS, T0, cutoff, 120)
    )
    await same(
        lambda r: ms.market_range_by_instrument(
            r, "mkt.flow_bars_1m", "minute_start", IIDS, T0, cutoff, batch_size=3
        )
    )


async def test_replacing_write_reads_as_the_last_one(both):
    memory, routed, put = both
    await put("mkt.flow_bars_1m", [bar(IIDS[0], 0, complete=False)])
    await put("mkt.flow_bars_1m", [bar(IIDS[0], 0, complete=True)])
    rows = await collect(routed.iter_all("mkt.flow_bars_1m"))
    assert len(rows) == 1 and rows[0]["complete"] is True


async def test_latest_per_instrument_reaches_past_the_recent_day(both):
    memory, routed, put = both
    cutoff = T0 + timedelta(days=3)
    await put(
        "mkt.klines_1m",
        [
            kline(IIDS[0], 0, "1"),
            kline(IIDS[0], 3 * 24 * 60 - 5, "2"),
            kline(IIDS[1], 0, "3"),
            kline(IIDS[1], 24 * 60, "4"),
        ],
    )
    options = {
        "time_field": "open_time",
        "cutoff": cutoff,
        "start": cutoff - timedelta(days=90),
        "instrument_ids": IIDS,
    }
    expected = await memory.latest_market_records("mkt.klines_1m", **options)
    got = await routed.latest_market_records("mkt.klines_1m", **options)
    assert got == expected
    assert [(r["instrument_id"], r["c"]) for r in got] == [(IIDS[0], "2"), (IIDS[1], "4")]


@pytest.fixture
async def interrupted_clickhouse(both):
    upstream = both[1].clickhouse
    state = SimpleNamespace(
        phase=None,
        failure=None,
        selected=0,
        armed_hydration=False,
        reached=asyncio.Event(),
        release=asyncio.Event(),
        peer_closed=asyncio.Event(),
        queries=[],
        errors=[],
    )
    handlers = set()

    async def serve(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            first, *lines = head.decode().split("\r\n")
            headers = dict(line.split(": ", 1) for line in lines if ": " in line)
            body = await reader.readexactly(int(headers.get("Content-Length", "0")))
            query = body.decode()
            state.queries.append(query)
            key_selection = query.startswith("SELECT storage_key,")
            late_selection = key_selection and "recorded_at >=" in query
            selected = (
                key_selection
                if state.phase == "cold"
                else late_selection
                if state.phase == "late"
                else key_selection and not late_selection
            )
            if selected:
                state.selected += 1
            if state.phase == "hydration" and selected:
                state.armed_hydration = True
            interrupt = state.phase is not None and (
                query.startswith("SELECT *") and state.armed_hydration
                if state.phase == "hydration"
                else selected and state.selected == (2 if state.phase == "cold" else 1)
            )
            response = await upstream.http.post(
                upstream.base.rstrip("/") + first.split(" ")[1],
                content=body,
                headers={
                    name: value for name, value in headers.items() if name.lower().startswith("x-clickhouse-")
                },
            )
            assert response.status_code == 200, response.text
            payload = response.content
            assert not interrupt or len(payload) > 1
            writer.write(
                f"HTTP/1.1 200 OK\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                + (payload[:1] if interrupt else payload)
            )
            await writer.drain()
            if interrupt:
                state.reached.set()
                await state.release.wait()
                if state.failure == "cancel":
                    assert await asyncio.wait_for(reader.read(1), 1) == b""
                    state.peer_closed.set()
                else:
                    writer.get_extra_info("socket").setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                    )
                    writer.transport.abort()
        except Exception as exc:
            state.errors.append(exc)
            state.reached.set()
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    ch = chm.ClickHouse(f"http://127.0.0.1:{port}", password=upstream.password)
    ch.user = upstream.user
    try:
        yield state, ch
    finally:
        state.release.set()
        server.close()
        await ch.close()
        for task in list(handlers):
            task.cancel()
        await asyncio.gather(*list(handlers), return_exceptions=True)
        await server.wait_closed()
    assert state.errors == []


@pytest.mark.parametrize("phase", ["cold", "late", "forward", "hydration"])
@pytest.mark.parametrize("failure", ["error", "cancel"])
async def test_interrupted_window_read_keeps_committed_lanes(
    both, interrupted_clickhouse, monkeypatch, phase, failure, record_property
):
    memory, routed, put = both
    state, ch = interrupted_clickhouse
    original_ch = routed.clickhouse
    clock = T0 + timedelta(days=4)
    monkeypatch.setattr(service_module, "utcnow", lambda: clock)
    collection, field = "mkt.flow_bars_1m", "minute_start"
    await put(collection, [bar(IIDS[0], minute) for minute in [*range(1010), *range(1200, 1204)]])
    routed.clickhouse = ch
    service = object.__new__(MarketService)
    service.repo = routed

    async def read(low, high):
        return await service._ranged(
            collection,
            field,
            [IIDS[0]],
            T0 + timedelta(minutes=low),
            T0 + timedelta(minutes=high),
        )

    await read(0, 1001)
    await read(1200, 1204)
    assert len(next(iter(service._windows.values()))) == 2
    committed = deepcopy(service._windows)
    before = pickle.dumps(service._windows)
    corrected = bar(IIDS[0], 10, last="corrected")
    corrected["recorded_at"] = iso(clock + timedelta(seconds=1))
    late = bar(IIDS[0], 10.5, last="late")
    late["recorded_at"] = iso(clock + timedelta(seconds=1))
    await put(collection, [corrected, late])
    clock += timedelta(seconds=2)
    state.phase, state.failure = phase, failure
    state.selected = 0
    low, high = (-1, 1010) if phase == "cold" else (5, 1001 if phase == "late" else 1010)
    task = asyncio.create_task(read(low, high))
    expiry = asyncio.get_running_loop().time() + 10
    try:
        async with asyncio.timeout_at(expiry):
            await asyncio.wait_for(state.reached.wait(), 2)
            assert state.errors == []
            if failure == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                state.release.set()
                await asyncio.wait_for(state.peer_closed.wait(), 1)
            else:
                state.release.set()
                with pytest.raises(httpx.ReadError) as caught:
                    await task
                chain = []
                error = caught.value
                while error is not None:
                    chain.append(type(error).__name__)
                    error = error.__cause__
                record_property("transport_exception_chain", ",".join(chain))
            assert service._windows == committed
            assert pickle.dumps(service._windows) == before
            assert all(not lock.locked() for lock in service._window_locks.values())
            state.phase = None
            actual = await read(low, high)
            expected = await ms.market_range_by_instrument(
                memory,
                collection,
                field,
                [IIDS[0]],
                T0 + timedelta(minutes=low),
                T0 + timedelta(minutes=high),
            )
            assert actual == expected
            rows = actual[IIDS[0]]
            assert len(rows) == high - max(low, 0) + 1
            assert {row["last_trade"]["trade_id"] for row in rows} >= {"corrected", "late"}
            assert asyncio.get_running_loop().time() < expiry
    finally:
        state.release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        routed.clickhouse = original_ch

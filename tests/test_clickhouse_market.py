"""ClickHouse market reads return what the PostgreSQL/memory reads return (HLD §7.1).

Runs against a real server: CLICKHOUSE_TEST_URL=http://user:password@host:8123.
The same typed rows go into the memory repository (put_market_batch) and into ClickHouse
(as tank-capture writes them); every routed read must give the same answer on both.
"""

import json
import os
from datetime import UTC, datetime, timedelta

import pytest

from tank import clickhouse_market as chm
from tank import market_storage as ms
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

    async def put(collection, records) -> None:
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


async def test_routed_reads_match_memory(both) -> None:
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


async def test_replacing_write_reads_as_the_last_one(both) -> None:
    memory, routed, put = both
    await put("mkt.flow_bars_1m", [bar(IIDS[0], 0, complete=False)])
    await put("mkt.flow_bars_1m", [bar(IIDS[0], 0, complete=True)])
    rows = await collect(routed.iter_all("mkt.flow_bars_1m"))
    assert len(rows) == 1 and rows[0]["complete"] is True


async def test_latest_per_instrument_reaches_past_the_recent_day(both) -> None:
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

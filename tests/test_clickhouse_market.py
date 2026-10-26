"""ClickHouse market reads return what the PostgreSQL/memory reads return (HLD §7.1).

Runs against a real server: CLICKHOUSE_TEST_URL=http://user:password@host:8123.
The same typed rows go into the memory repository (put_market_batch) and into ClickHouse
(as tank-capture writes them); every routed read must give the same answer on both.
"""

import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from foundation_helpers import present, text_field
from pydantic import JsonValue

from tank import clickhouse_market as chm
from tank import market_storage as ms
from tank.contracts.json import JsonDocument
from tank.market_codec import StoredRow, _prepare_records
from tank.market_schema import MARKET_COLUMNS
from tank.models import Record
from tank.storage import MemoryRepository, PostgresRepository, StorageBackend
from tank.util import JsonInput, canonical_json, iso, parse_utc_time

type PutRecords = Callable[[str, Sequence[Mapping[str, JsonInput]]], Awaitable[None]]
type Both = tuple[MemoryRepository, PostgresRepository, PutRecords]

URL = os.environ.get("CLICKHOUSE_TEST_URL")
pytestmark = pytest.mark.integration
T0 = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
IIDS = ["binance:spot:BTCUSDT", "okx:spot:BTC-USDT"]


def kline(
    iid: str, minute: int, close: str, *, complete: bool = True, recorded: datetime | None = None
) -> Record:
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
        "recorded_at": iso(at + timedelta(seconds=61) if recorded is None else recorded),
        "recorded_at_prec": "COLLECTION_ONLY",
    }


def bar(iid: str, minute: int, *, complete: bool = True, last: str = "7") -> Record:
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


def ch_row(collection: str, record: StoredRow, key: str) -> Record:
    """A typed record as tank-capture inserts it (JSONEachRow)."""
    row: Record = {"storage_key": key, "_version": 1}
    for name, kind in MARKET_COLUMNS[collection].items():
        value = record.get(name)
        if value is None:
            if kind == "text[]":
                row[name] = []
            continue
        if kind == "timestamptz":
            assert isinstance(value, datetime)
            row[name] = value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
        elif kind == "jsonb":
            row[name] = canonical_json(value)
        elif kind == "numeric":
            row[name] = str(value)
        else:
            assert not isinstance(value, (datetime, Decimal))
            row[name] = JsonDocument.validate_value(value)
    return row


@pytest.fixture
async def both() -> AsyncIterator[Both]:
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

    async def put(collection: str, records: Sequence[Mapping[str, JsonInput]]) -> None:
        await memory.put_market_batch(collection, records)
        prepared = _prepare_records(collection, records, None)
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


async def collect[T](gen: AsyncIterator[T]) -> list[T]:
    return [row async for row in gen]


async def test_routed_reads_match_memory(both: Both) -> None:
    memory, routed, put = both
    klines = [kline(iid, m, str(100 + m)) for iid in IIDS for m in range(6)]
    klines.append(kline(IIDS[0], 6, "0", complete=True))
    await put("mkt.klines_1m", klines)
    await put("mkt.flow_bars_1m", [bar(iid, m, last=str(m)) for iid in IIDS for m in range(4)])
    cutoff = T0 + timedelta(minutes=10)

    for repo in (memory, routed):
        assert repo is memory or chm.client(repo, "mkt.klines_1m") is not None

    async def same[T](call: Callable[[StorageBackend], Awaitable[T]]) -> T:
        a, b = await call(memory), await call(routed)
        assert a == b, (a, b)
        return a

    key = _prepare_records("mkt.klines_1m", [klines[0]], None)
    (storage_key,) = key
    assert present(await same(lambda r: r.get("mkt.klines_1m", storage_key)))["c"] == "100"
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
    assert sorted(text_field(x, "c") for x in latest) == ["0", "105"]
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
    trade_ids: set[str] = set()
    for record in trades:
        last_trade = record["last_trade"]
        assert isinstance(last_trade, dict)
        trade_ids.add(text_field(last_trade, "trade_id"))
    assert trade_ids == {"3"}

    async def recorded(r: StorageBackend) -> list[tuple[str, Record]]:
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


async def test_replacing_write_reads_as_the_last_one(both: Both) -> None:
    memory, routed, put = both
    await put("mkt.flow_bars_1m", [bar(IIDS[0], 0, complete=False)])
    await put("mkt.flow_bars_1m", [bar(IIDS[0], 0, complete=True)])
    rows = await collect(routed.iter_all("mkt.flow_bars_1m"))
    assert len(rows) == 1 and rows[0]["complete"] is True


async def test_latest_per_instrument_reaches_past_the_recent_day(both: Both) -> None:
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
    expected = await memory.latest_market_records(
        "mkt.klines_1m",
        time_field="open_time",
        cutoff=cutoff,
        start=cutoff - timedelta(days=90),
        instrument_ids=IIDS,
    )
    got = await routed.latest_market_records(
        "mkt.klines_1m",
        time_field="open_time",
        cutoff=cutoff,
        start=cutoff - timedelta(days=90),
        instrument_ids=IIDS,
    )
    assert got == expected
    assert [(r["instrument_id"], r["c"]) for r in got] == [(IIDS[0], "2"), (IIDS[1], "4")]


async def test_key_bounds_preserve_latest_value_and_declared_utc_storage_clocks(both: Both) -> None:
    memory, routed, put = both
    original = kline(IIDS[0], 0, "100")
    await put("mkt.klines_1m", [original])
    replacement = kline(IIDS[0], 0, "200", recorded=T0 + timedelta(minutes=2))
    await memory.put_market_batch("mkt.klines_1m", [replacement])
    prepared = _prepare_records("mkt.klines_1m", [replacement], None)
    key, record = next(iter(prepared.items()))
    replacement_row = ch_row("mkt.klines_1m", record, key)
    replacement_row["_version"] = 2
    ch = routed.clickhouse
    assert ch is not None
    await ch.insert("mkt.klines_1m", [replacement_row])
    expected = present(await memory.get("mkt.klines_1m", key))
    actual = present(await routed.get("mkt.klines_1m", key))
    assert actual == expected and actual["c"] == "200"
    naive = chm.MarketPredicateSpan(
        instrument_id=IIDS[0], lo="2026-10-21 12:00:00.000000", hi="2026-10-21 12:00:00.000000"
    )
    zoned = chm.MarketPredicateSpan(
        instrument_id=IIDS[0], lo="2026-10-21T14:00:00+02:00", hi="2026-10-21T12:00:00Z"
    )
    assert parse_utc_time(naive.lo) == parse_utc_time(zoned.lo) == T0


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("instrument_id", "   "),
        ("instrument_id", 1),
        ("lo", None),
        ("lo", True),
        ("lo", 1),
        ("lo", "2026-10-21"),
        ("lo", "2026-02-30 12:00:00"),
        ("lo", "2026-10-21 12:00:01"),
    ],
)
async def test_corrupt_key_bounds_refuse_before_any_final_value_query(
    both: Both, monkeypatch: pytest.MonkeyPatch, field: str, invalid: JsonValue
) -> None:
    _, routed, put = both
    original = kline(IIDS[0], 0, "100")
    await put("mkt.klines_1m", [original])
    key = next(iter(_prepare_records("mkt.klines_1m", [original], None)))
    ch = routed.clickhouse
    assert ch is not None
    actual_rows = ch.rows
    queries: list[str] = []

    async def corrupted_rows(query: str, params: dict[str, tuple[str, str]] | None = None) -> list[Record]:
        queries.append(query)
        rows = await actual_rows(query, params)
        assert " FINAL" not in query
        assert len(rows) == 1
        malformed = dict(rows[0])
        malformed[field] = JsonDocument.validate_value(invalid)
        return [rows[0], malformed]

    monkeypatch.setattr(ch, "rows", corrupted_rows)
    with pytest.raises(ValueError):
        await routed.get("mkt.klines_1m", key)
    assert len(queries) == 1
    assert "min(`open_time`) AS lo" in queries[0] and " FINAL" not in queries[0]

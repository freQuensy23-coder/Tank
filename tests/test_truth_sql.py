"""Market Truth statements (03 §0): the runner and typed records, the snapshot loader and the
benchmark reaction statement against a real ClickHouse server (CLICKHOUSE_TEST_URL)."""

from datetime import timedelta
from decimal import Decimal

import pytest

from tank import clickhouse_market as chm
from tank.analysis import truth_sql
from tank.analysis.truth import benchmark_reaction
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
START = END - timedelta(hours=1)
SPOT = ["binance:spot:BTCUSDT", "coinbase:spot:BTC-USD"]


def bar(iid, minute, close, usd, **changes):
    return {
        "instrument_id": iid,
        "token_id": "bitcoin:BTC",
        "market_type": "PERP_LINEAR" if ":perp" in iid else "SPOT",
        "open_time": iso(minute),
        "interval_s": 60,
        "complete": True,
        "c": str(close),
        "quote_volume_usd": str(usd),
    } | changes


def klines(**changes):
    rows = []
    for i in range(120):
        minute = END - timedelta(minutes=120 - i)
        rows.append(bar("binance:spot:BTCUSDT", minute, 100 + i, 1000, **changes.get(i, {})))
        rows.append(bar("coinbase:spot:BTC-USD", minute, 50 + i, 10))
        rows.append(bar("binance:perp_linear:BTCUSDT", minute, 1, 10**9))
    return rows


def record(**changes):
    values = dict(
        venue="binance:spot:BTCUSDT",
        quote_volume_24h=120000.0,
        price_start=Decimal("159"),
        price_start_observed_at=START,
        price_end=Decimal("219"),
        price_end_observed_at=END,
        r_raw=219 / 159 - 1,
        venues=2,
    )
    return truth_sql.BenchmarkReaction(**(values | changes))


def test_record_decodes_typed_columns_and_refuses_other_columns():
    row = {
        "venue": "binance:spot:BTCUSDT",
        "quote_volume_24h": 1054371034.1174875,
        "price_start": "75523.31000000",
        "price_start_observed_at": "2026-11-09T15:55:00.000000Z",
        "price_end": None,
        "price_end_observed_at": None,
        "r_raw": None,
        "venues": 3,
    }
    decoded = truth_sql.BenchmarkReaction.decode(row)
    assert decoded.price_start == Decimal("75523.31000000")
    assert decoded.price_start_observed_at == parse_time("2026-11-09T15:55:00Z")
    assert decoded.price_end is None and decoded.r_raw is None and decoded.venues == 3
    with pytest.raises(ValueError, match="columns"):
        truth_sql.BenchmarkReaction.decode({**row, "extra": 1})
    with pytest.raises(ValueError, match="venues is NULL"):
        truth_sql.BenchmarkReaction.decode({**row, "venues": None})


def test_statement_parameters_come_from_its_file():
    query, kinds = truth_sql.load_statement("benchmark_reaction")
    assert kinds == {
        "instrument_ids": "Array(String)",
        "window_start": "DateTime64(6, 'UTC')",
        "window_end": "DateTime64(6, 'UTC')",
    }
    assert query.endswith(")\n") and "mkt.klines_1m FINAL" in query


async def test_fetch_sends_typed_parameters_and_refuses_unknown_ones():
    sent = []

    class Client:
        async def rows(self, query, params):
            sent.append(params)
            return [
                {
                    "venue": None,
                    "quote_volume_24h": 0,
                    "price_start": None,
                    "price_start_observed_at": None,
                    "price_end": None,
                    "price_end_observed_at": None,
                    "r_raw": None,
                    "venues": 0,
                }
            ]

    result = await truth_sql.benchmark_reaction(Client(), ["a:spot:X", "b:spot:X"], START, END)
    assert result.venues == 0 and result.venue is None
    assert sent[0] == {
        "instrument_ids": ("['a:spot:X','b:spot:X']", "Array(String)"),
        "window_start": ("2026-10-16 11:00:00.000000", "DateTime64(6, 'UTC')"),
        "window_end": ("2026-10-16 12:00:00.000000", "DateTime64(6, 'UTC')"),
    }
    with pytest.raises(TypeError, match="takes"):
        await truth_sql.fetch(Client(), truth_sql.BenchmarkReaction, instrument_ids=[])


def test_benchmark_checkpoint_values_map_the_record_and_keep_not_applicable_constants():
    """03 §8 benchmark (lead ruling 2026-10-20): raw return from base klines; the rest NOT_APPLICABLE."""
    result = benchmark_reaction(record())
    assert result["r_raw_venue"] == "binance:spot:BTCUSDT"
    assert result["price_start"] == 159 and result["price_end"] == 219
    assert result["price_start_observed_at"] == iso(START) and result["price_end_observed_at"] == iso(END)
    assert result["r_raw"] == pytest.approx(219 / 159 - 1, abs=1e-12) and result["r_raw_reason"] is None
    assert result["r_raw_method"] == "BASE_KLINE_CLOSE_LARGEST_SPOT_VENUE"
    assert result["reason"] == "NOT_APPLICABLE" and "spot_cvd" in result["not_applicable"]
    empty = benchmark_reaction(
        record(
            venue=None,
            quote_volume_24h=0.0,
            price_start=None,
            price_start_observed_at=None,
            price_end=None,
            price_end_observed_at=None,
            r_raw=None,
            venues=0,
        )
    )
    assert empty["r_raw"] is None and empty["r_raw_reason"] == "INCOMPLETE"
    assert empty["r_raw_venue"] is None and empty["price_start_observed_at"] is None


def test_snapshot_rows_derive_keys_and_versions_from_identity_and_position():
    first, second = bar("binance:spot:BTCUSDT", END, 1, 1), bar("binance:spot:BTCUSDT", END, 2, 1)
    liquidation = {
        "instrument_id": "binance:perp_linear:BTCUSDT",
        "ts": iso(END),
        "side": "SELL",
        "qty_native": "1",
        "coverage": "FULL",
    }
    tables = chm.snapshot_rows(
        {"klines": [first, second], "liquidations": [liquidation, liquidation], "supply": [{"x": 1}]}
    )
    assert set(tables) == {"mkt.klines_1m", "mkt.liq_obs"}
    keys = [row["storage_key"] for row in tables["mkt.klines_1m"]]
    assert keys == ["binance:spot:BTCUSDT:2026-10-16T12:00:00Z"] * 2
    assert [row["_version"] for row in tables["mkt.klines_1m"]] == [1, 2]
    assert [row["storage_key"] for row in tables["mkt.liq_obs"]] == [
        "binance:perp_linear:BTCUSDT:2026-10-16T12:00:00Z:1",
        "binance:perp_linear:BTCUSDT:2026-10-16T12:00:00Z:2",
    ]
    assert "recorded_at" not in tables["mkt.klines_1m"][0]
    assert tables["mkt.klines_1m"][0]["open_time"] == "2026-10-16 12:00:00.000000"


async def test_drop_database_refuses_the_production_database():
    with pytest.raises(ValueError, match="production"):
        await chm.drop_database(chm.ClickHouse("http://tank@127.0.0.1:8123"))


def test_replay_client_has_a_database_of_its_own():
    from types import SimpleNamespace

    repo = SimpleNamespace()
    production = chm.install(
        repo, SimpleNamespace(clickhouse_url="http://tank@clickhouse:8123", replay=False)
    )
    assert production.database == "mkt"
    replay = chm.install(repo, SimpleNamespace(clickhouse_url="http://tank@clickhouse:8123", replay=True))
    assert replay.database.startswith("mkt_replay_") and replay.database != "mkt"
    assert chm.install(SimpleNamespace(), SimpleNamespace(clickhouse_url=None, replay=True)) is None


@pytest.mark.integration
async def test_benchmark_statement_returns_the_largest_spot_venue_base_kline_return(market_clickhouse):
    await chm.load_snapshot(market_clickhouse, {"klines": klines()})
    result = await truth_sql.benchmark_reaction(market_clickhouse, SPOT, START, END)
    assert result.venue == "binance:spot:BTCUSDT" and result.venues == 2
    assert result.quote_volume_24h == pytest.approx(120000)
    assert result.price_start == 159 and result.price_end == 219
    assert result.price_start_observed_at == START and result.price_end_observed_at == END
    assert abs(result.r_raw - (219 / 159 - 1)) < 1e-12


@pytest.mark.integration
async def test_benchmark_statement_ignores_instruments_it_was_not_given(market_clickhouse):
    await chm.load_snapshot(market_clickhouse, {"klines": klines()})
    result = await truth_sql.benchmark_reaction(
        market_clickhouse, ["binance:perp_linear:BTCUSDT"], START, END
    )
    assert result.venue == "binance:perp_linear:BTCUSDT" and result.price_end == 1
    nothing = await truth_sql.benchmark_reaction(market_clickhouse, [], START, END)
    assert nothing.venue is None and nothing.venues == 0 and nothing.r_raw is None


@pytest.mark.integration
async def test_benchmark_statement_without_rows_has_no_venue_and_no_price(market_clickhouse):
    result = await truth_sql.benchmark_reaction(market_clickhouse, SPOT, START, END)
    assert (result.venue, result.price_start, result.price_end, result.r_raw) == (None, None, None, None)
    assert result.venues == 0 and result.quote_volume_24h == 0


@pytest.mark.integration
async def test_benchmark_statement_gives_no_price_for_a_close_older_than_its_bar(market_clickhouse):
    old = [r for r in klines() if parse_time(r["open_time"]) < START - timedelta(minutes=30)]
    assert old
    await chm.load_snapshot(market_clickhouse, {"klines": old})
    result = await truth_sql.benchmark_reaction(market_clickhouse, SPOT, START, END)
    assert result.venue == "binance:spot:BTCUSDT"
    assert result.price_start is None and result.price_end is None and result.r_raw is None
    assert result.price_start_observed_at is None


@pytest.mark.integration
async def test_benchmark_statement_skips_incomplete_and_zero_close_bars(market_clickhouse):
    rows = klines(**{119: {"complete": False}, 59: {"c": "0"}})
    await chm.load_snapshot(market_clickhouse, {"klines": rows})
    result = await truth_sql.benchmark_reaction(market_clickhouse, SPOT, START, END)
    assert result.price_start == 158 and result.price_start_observed_at == START - timedelta(minutes=1)
    assert result.price_end == 218 and result.price_end_observed_at == END - timedelta(minutes=1)
    assert result.r_raw == pytest.approx(218 / 158 - 1, abs=1e-12)


def test_a_list_column_decodes_to_a_tuple_that_keeps_the_nulls_of_its_items():
    row = dict(
        window="W1H",
        venue="A",
        books=1,
        latest_at=None,
        latest_prec=None,
        latest_prec_reason=None,
        latest_spread=None,
        latest_bid=None,
        latest_ask=None,
        latest_bid_native=None,
        latest_ask_native=None,
        latest_covered=None,
        pre_books=0,
        pre_bid=None,
        pre_ask=None,
        pre_bid_native=None,
        pre_ask_native=None,
        pre_native_missing=0,
        pre_depth=None,
        pre_spread=None,
        window_bid=None,
        window_ask=None,
        bars=2,
        bursts=2,
        replenished=1,
        resilient_bursts=0,
        burst_at=["2026-10-16T11:30:00Z", "2026-10-16T11:40:00Z"],
        burst_replenished=[1, None],
        burst_recovery_s=[12.5, None],
        burst_resilient=[None, 0],
        ask_building=None,
        ask_testable_bars=0,
        ask_recovered_bars=0,
    )
    found = truth_sql.BookLiquidity.decode(row)
    assert found.burst_at == (END - timedelta(minutes=30), END - timedelta(minutes=20))
    assert (found.burst_replenished, found.burst_recovery_s, found.burst_resilient) == (
        (1, None),
        (12.5, None),
        (None, 0),
    )

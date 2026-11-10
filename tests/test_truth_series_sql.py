"""Series statements (03 §5.3, §5.4, §5.10) against a real ClickHouse server (CLICKHOUSE_TEST_URL): ticker_volumes.sql,
lead_lag.sql and series_health.sql, and the rules that read their records."""

from datetime import timedelta
from decimal import Decimal

import pytest
from relative_helpers import evidence as relative_evidence
from relative_helpers import standard_horizons

from tank import clickhouse_market as chm
from tank.analysis import truth, truth_sql
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
SECOND = timedelta(seconds=1)
A, B = "A:spot:T", "B:spot:T"
P = "P:perp_linear:T"


def snapshot(*instruments, **changes):
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + HOUR),
            anchor={"type": "T0", "ts": iso(END - 3 * HOUR)},
            instruments=[
                {
                    "instrument_id": iid,
                    "token_id": "T",
                    "venue": iid.split(":")[0],
                    "market_type": "SPOT" if ":spot:" in iid else "PERP_LINEAR",
                }
                for iid in instruments
            ],
        )
        | changes
    )


async def seed(ch, **collections):
    await chm.load_snapshot(ch, collections)


async def fetch(ch, record, **values):
    common = dict(cutoff=END, assembled=END + HOUR)
    return await truth_sql.fetch(ch, record, **(common | values))


def kline(iid, minute, o="100", c="100", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "open_time": iso(minute),
        "open_time_prec": "BUCKET(60s)",
        "interval_s": 60,
        "o": o,
        "c": c,
        "complete": True,
        "quote_volume_usd": "10",
    } | changes


def klines(iid, since, until=END, skip=(), **common):
    minute, rows = since, []
    while minute < until:
        if minute not in skip:
            rows.append(kline(iid, minute, **common))
        minute += MINUTE
    return rows


def ticker(iid, at, volume="1000", **changes):
    return {"instrument_id": iid, "token_id": "T", "ts": iso(at), "volume_native": volume} | changes


def flow_bar(iid, minute, **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "minute_start": iso(minute),
        "minute_start_prec": "BUCKET(60s)",
        "complete": True,
        "buy_qty": "1",
        "sell_qty": "1",
        "sum_sq_qty": "2",
        "n_buy": 1,
        "n_sell": 1,
    } | changes


def book(iid, at, **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "exchange_ts": iso(at),
        "exchange_ts_prec": "EXACT",
        "integrity_ok": True,
        "mid": "10",
        "spread_bps": "1",
        "range_covered_pct": {"bid": "2", "ask": "2"},
    } | changes


def oi(iid, at, key="m", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "metric_key": key,
        "ts": iso(at),
        "ts_prec": "EXACT",
        "oi_native": "5",
        "raw_value": "5",
        "raw_unit": "NATIVE",
        "sidedness": "SINGLE_SIDED",
        "received_ts": iso(at),
    } | changes


def funding(iid, at, kind="SETTLED", key="f", rate="0.0001", interval="8", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "metric_key": key,
        "kind": kind,
        "as_of": iso(at),
        "as_of_prec": "EXACT",
        "rate": rate,
        "interval_h": interval,
        "rate_8h_eq": rate,
    } | changes


def health(result, series, iid, key=None, kind=None):
    return next(
        r for r in result if (r.series, r.instrument_id, r.metric_key, r.kind) == (series, iid, key, kind)
    )


@pytest.mark.integration
async def test_a_ticker_volume_is_that_of_the_latest_ticker_at_or_before_the_cutoff(market_clickhouse):
    await seed(
        market_clickhouse,
        tickers=[
            ticker(A, END - 10 * MINUTE, "10"),
            ticker(A, END - MINUTE, "20"),
            ticker(A, END + SECOND, "99"),
            ticker(B, END - 20 * MINUTE, "5"),
            ticker(B, END - 2 * MINUTE, None),
            ticker(P, END - HOUR, "7"),
        ],
    )
    rows = await fetch(market_clickhouse, truth_sql.TickerVolume, iids=[A, B, P], near_s=900)
    assert [(r.instrument_id, r.observed_at, r.volume_native) for r in rows] == [
        (A, END - MINUTE, Decimal("20")),
        (B, END - 2 * MINUTE, None),
    ]


@pytest.mark.integration
async def test_a_ticker_recorded_or_received_after_the_assembly_time_is_not_visible(market_clickhouse):
    await seed(
        market_clickhouse,
        tickers=[
            ticker(A, END - 3 * MINUTE, "10"),
            ticker(A, END - 2 * MINUTE, "20", recorded_at=iso(END + 2 * HOUR)),
            ticker(A, END - MINUTE, "30", received_ts=iso(END + 2 * HOUR)),
        ],
    )
    (row,) = await fetch(market_clickhouse, truth_sql.TickerVolume, iids=[A], near_s=900)
    assert (row.observed_at, row.volume_native) == (END - 3 * MINUTE, Decimal("10"))


@pytest.mark.integration
async def test_ticker_volumes_reach_the_venue_rules_through_the_series_evidence(market_clickhouse):
    await seed(
        market_clickhouse, tickers=[ticker(A, END - MINUTE, "1440"), ticker(B, END - 2 * MINUTE, "720")]
    )
    evidence = await truth.series_evidence(
        market_clickhouse, snapshot(A, B), END - HOUR, END, {}, health=False, lead_lag=False
    )
    assert truth.ticker_volumes(dict(series_sql=evidence)) == {"A": Decimal("1440"), "B": Decimal("720")}


def ranged(low=-0.01, high=0.01):
    horizons = standard_horizons(END, END - 3 * HOUR, W15M=dict(history_q025=low, history_q975=high))
    return relative_evidence(ref=A, horizons=horizons)


@pytest.mark.integration
async def test_the_first_bar_that_leaves_the_range_is_found_for_each_instrument(market_clickhouse):
    since = END - 10 * MINUTE
    rising = klines(A, since, c="100")
    rising[4] = kline(A, since + 4 * MINUTE, c="101.5")
    rising[5] = kline(A, since + 5 * MINUTE, c="103")
    await seed(
        market_clickhouse,
        klines=rising + klines(B, since, c="100.5") + klines(P, since, c="98"),
    )
    rows = await fetch(
        market_clickhouse, truth_sql.LeadLagBars, iids=[A, B, P], start=since, low=-0.01, high=0.01
    )
    assert [(r.instrument_id, r.bars, r.usable, r.first_exit_at) for r in rows] == [
        (A, 10, 1, since + 4 * MINUTE),
        (B, 10, 1, None),
        (P, 10, 1, since),
    ]


@pytest.mark.integration
async def test_a_gap_an_incomplete_bar_or_a_bar_after_the_assembly_makes_the_bars_unusable(market_clickhouse):
    since = END - 10 * MINUTE
    incomplete = klines(B, since)
    incomplete[3] = kline(B, since + 3 * MINUTE, complete=False)
    late = klines(P, since)
    late[6] = kline(P, since + 6 * MINUTE, recorded_at=iso(END + 2 * HOUR))
    await seed(
        market_clickhouse,
        klines=klines(A, since, skip={since + 2 * MINUTE}) + incomplete + late,
    )
    rows = await fetch(
        market_clickhouse, truth_sql.LeadLagBars, iids=[A, B, P], start=since, low=-0.01, high=0.01
    )
    assert [(r.instrument_id, r.bars, r.usable) for r in rows] == [(A, 9, 0), (B, 10, 0), (P, 9, 0)]


@pytest.mark.integration
async def test_bars_that_have_not_closed_by_the_cutoff_and_a_zero_first_open_give_no_exit(market_clickhouse):
    since = END - 5 * MINUTE
    open_late = klines(A, since, END + 2 * MINUTE, c="150")
    await seed(
        market_clickhouse,
        klines=open_late + [kline(B, since + n * MINUTE, o="0", c="150") for n in range(5)],
    )
    rows = await fetch(
        market_clickhouse, truth_sql.LeadLagBars, iids=[A, B], start=since, low=-0.01, high=0.01
    )
    assert [(r.instrument_id, r.bars, r.usable, r.first_exit_at) for r in rows] == [
        (A, 5, 1, since),
        (B, 5, 1, None),
    ]


@pytest.mark.integration
async def test_the_series_evidence_tests_the_bars_against_the_range_of_the_relative_records(
    market_clickhouse,
):
    since = END - 3 * HOUR
    rows = klines(A, since, c="100") + klines(P, since, c="100")
    rows[60] = kline(A, since + 60 * MINUTE, c="102")
    rows[240] = kline(P, since + 60 * MINUTE, c="102")
    await seed(market_clickhouse, klines=rows)
    shown = snapshot(A, P, relative_sql=ranged())
    evidence = await truth.series_evidence(
        market_clickhouse, shown, since, END, {}, tickers=False, health=False
    )
    assert [(r.instrument_id, r.usable, r.first_exit_at) for r in evidence.lead_lag] == [
        (A, 1, since + 60 * MINUTE),
        (P, 1, since + 60 * MINUTE),
    ]
    quiet = await truth.series_evidence(
        market_clickhouse, snapshot(A, P), since, END, {}, tickers=False, health=False
    )
    assert quiet.lead_lag == ()


@pytest.mark.integration
async def test_a_bar_series_reports_the_latest_complete_bar_its_close_and_the_day_volume(market_clickhouse):
    rows = klines(A, END - 2 * HOUR, END - 2 * MINUTE)
    rows += [kline(A, END - 2 * MINUTE, c="0"), kline(A, END - MINUTE, complete=False)]
    await seed(
        market_clickhouse,
        klines=rows + [kline(A, END - 25 * HOUR, quote_volume_usd="1000")],
        flow_bars=[flow_bar(A, END - 2 * MINUTE), flow_bar(A, END - MINUTE, complete=False)],
    )
    result = await fetch(market_clickhouse, truth_sql.SeriesHealth, iids=[A], look_s=86400, near_s=900)
    k = health(result, "klines", A)
    assert (k.rows, k.last_at, k.last_interval_s, k.last_prec) == (120, END - 2 * MINUTE, 60, "BUCKET(60s)")
    assert k.valid_at == END - 2 * MINUTE
    assert k.usd_volume == pytest.approx(1200.0)
    f = health(result, "flow_bars", A)
    assert (f.rows, f.last_at, f.valid_at, f.usd_volume) == (2, END - 2 * MINUTE, END - MINUTE, None)


@pytest.mark.integration
async def test_a_book_is_fit_freshness_evidence_only_with_integrity_a_mid_and_both_sides_covered(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        books=[
            book(A, END - 100 * SECOND),
            book(A, END - 60 * SECOND, integrity_ok=False),
            book(A, END - 50 * SECOND, mid="0"),
            book(A, END - 40 * SECOND, range_covered_pct={"bid": "2"}),
            book(A, END - 30 * SECOND, range_covered_pct={"bid": "2", "ask": None}),
            book(B, END - 20 * MINUTE),
            book(B, END - 10 * SECOND, recorded_at=iso(END + 2 * HOUR)),
        ],
    )
    result = await fetch(market_clickhouse, truth_sql.SeriesHealth, iids=[A, B], look_s=86400, near_s=900)
    a = health(result, "books", A)
    assert (a.rows, a.last_at, a.valid_at, a.last_prec) == (5, END - 30 * SECOND, END - 100 * SECOND, "EXACT")
    assert not [r for r in result if r.series == "books" and r.instrument_id == B]


@pytest.mark.integration
async def test_open_interest_and_funding_are_kept_per_key_with_the_precision_of_their_latest_row(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        oi=[
            oi(P, END - 10 * MINUTE, "m1"),
            oi(P, END - 3 * MINUTE, "m1", oi_native=None),
            oi(
                P,
                END - MINUTE,
                "m2",
                ts_prec="BUCKET(300s)",
                timestamp_precision="BUCKET(300s)",
                ts_bucket_size_s="300",
                ts_bucket_anchor="END",
            ),
        ],
        funding=[
            funding(P, END - 8 * HOUR, "SETTLED"),
            funding(P, END - 2 * MINUTE, "CURRENT_ESTIMATE", interval="1"),
            funding(P, END - MINUTE, "CURRENT_ESTIMATE", interval="0"),
            funding(P, END - 3 * MINUTE, "SETTLED", interval="0", rate="0.0002"),
        ],
    )
    result = await fetch(market_clickhouse, truth_sql.SeriesHealth, iids=[P], look_s=86400, near_s=900)
    one = health(result, "oi", P, "m1")
    assert (one.rows, one.last_at, one.valid_at) == (2, END - 3 * MINUTE, END - 10 * MINUTE)
    two = health(result, "oi", P, "m2")
    assert (two.last_prec, two.last_timestamp_precision, two.last_bucket_size_s, two.last_bucket_anchor) == (
        "BUCKET(300s)",
        "BUCKET(300s)",
        300.0,
        "END",
    )
    settled = health(result, "funding", P, "f", "SETTLED")
    assert (settled.last_at, settled.last_interval_h, settled.valid_at) == (
        END - 3 * MINUTE,
        0.0,
        END - 8 * HOUR,
    )
    estimate = health(result, "funding", P, "f", "CURRENT_ESTIMATE")
    assert (estimate.last_at, estimate.last_interval_h, estimate.valid_at) == (
        END - MINUTE,
        0.0,
        END - 2 * MINUTE,
    )


@pytest.mark.integration
async def test_liquidations_and_tickers_report_their_latest_row_and_old_rows_are_out_of_range(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        liquidations=[
            {
                "instrument_id": P,
                "token_id": "T",
                "ts": iso(END - 5 * MINUTE),
                "ts_prec": "EXACT",
                "side": "LONG_LIQ",
                "qty_native": "1",
                "coverage": "SAMPLED",
            },
            {
                "instrument_id": P,
                "token_id": "T",
                "ts": iso(END - 3 * HOUR),
                "side": "LONG_LIQ",
                "qty_native": "1",
                "coverage": "SAMPLED",
            },
        ],
        tickers=[
            ticker(P, END - 2 * MINUTE, "7", kind="MARK_PRICE"),
            ticker(P, END - 4 * MINUTE, "8"),
            ticker(P, END - 30 * MINUTE, "9"),
        ],
    )
    result = await fetch(market_clickhouse, truth_sql.SeriesHealth, iids=[P], look_s=7200, near_s=900)
    assert health(result, "liquidations", P).rows == 1
    assert health(result, "liquidations", P).last_at == END - 5 * MINUTE
    assert health(result, "tickers", P, kind="MARK_PRICE").last_at == END - 2 * MINUTE
    assert health(result, "tickers", P).last_at == END - 4 * MINUTE
    assert health(result, "tickers", P).rows == 1


@pytest.mark.integration
async def test_the_series_health_feeds_the_confidence_rules_end_to_end(market_clickhouse):
    since = END - 2 * HOUR
    await seed(
        market_clickhouse,
        klines=klines(A, since, END) + klines(B, since, END - 10 * MINUTE),
        flow_bars=[flow_bar(iid, END - MINUTE) for iid in (A, B)],
        books=[book(A, END - 5 * SECOND), book(B, END - 5 * SECOND)],
    )
    shown = snapshot(A, B, capture_metadata={"tier": "HOT"}, semantics=[])
    evidence = await truth.series_evidence(
        market_clickhouse, shown, END - 3 * HOUR, END, {}, tickers=False, lead_lag=False
    )
    shown = dict(shown, series_sql=evidence)
    spot = dict(
        venues=[{"venue": "A"}, {"venue": "B"}],
        coverage={"n_complete": 2, "exact_majority": True, "covered_share_est": 1},
    )
    result = truth.data_confidence(shown, spot, {}, END)
    stale = [f for f in result["failed"] if f["condition"] == "freshness"]
    assert {(f["instrument_id"], f["collection"]) for f in stale} == {(B, "klines")}
    assert [
        c["instrument_id"] for c in result["freshness"] if c["collection"] == "klines" and not c["fresh"]
    ] == [B]
    assert result["class_"] == "MEDIUM"

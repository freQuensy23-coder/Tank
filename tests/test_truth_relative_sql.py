"""Price-relative statements (03 §5 preamble and §5.7) against a real ClickHouse server
(CLICKHOUSE_TEST_URL): relative_baselines.sql, reference_prices.sql and sector_returns.sql, and the
rules that read their records."""

import json
import statistics
from datetime import timedelta
from decimal import Decimal
from math import log, sin

import pytest

from tank import clickhouse_market as chm
from tank.analysis.truth import relative_evidence, relative_truth
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
START = END - timedelta(hours=4)
DAYS = 8
CONFIG = {"historical_lookback_days": DAYS}
MINUTE = timedelta(minutes=1)
FIRST = END - timedelta(days=DAYS)
TOKEN, BTC, ETH = "A:spot:T", "A:spot:BTC", "A:spot:ETH"
TOKENS = {TOKEN: "T", BTC: "BTC", ETH: "ETH"}


def btc_price(i):
    return 100 * (1 + 0.01 * sin(i / 700))


def token_price(i):
    return 100 * (btc_price(i) / 100) ** 1.5


def eth_price(i):
    return 50 * (1 + 0.02 * sin(i / 500))


def kline(iid, opened, close, **changes):
    return {
        "instrument_id": iid,
        "token_id": TOKENS.get(iid, iid.split(":")[-1]),
        "market_type": "SPOT",
        "open_time": iso(opened),
        "interval_s": 60,
        "complete": True,
        "c": repr(close),
    } | changes


def history(skip=(), until=END):
    """One-minute bars of the token, BTC and ETH from FIRST up to `until`; `skip` lists
    (instrument, bar number) pairs that were never captured. Bar i opens at FIRST + i minutes."""
    rows = []
    for i in range(int((until - FIRST) / MINUTE)):
        for iid, price in ((TOKEN, token_price), (BTC, btc_price), (ETH, eth_price)):
            if (iid, i) not in skip:
                rows.append(kline(iid, FIRST + i * MINUTE, price(i)))
    return rows


def snapshot(**changes):
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + timedelta(hours=1)),
            anchor={"type": "T0", "ts": iso(START)},
            instruments=[
                {"instrument_id": iid, "token_id": token, "market_type": "SPOT", "venue": "A"}
                for iid, token in TOKENS.items()
            ],
            tokens=[{"token_id": t, "symbol": t} for t in TOKENS.values()],
        )
        | changes
    )


def by_name(evidence):
    return {row.name: row for row in evidence.horizons}


def closes(price):
    """Closes by slot as relative_baselines.sql numbers them: slot s closes s minutes after FIRST."""
    return [None] + [price(i) for i in range(int((END - FIRST) / MINUTE))]


def adjusted(token, btc, k, hi, beta):
    return [token[i] / token[i - k] - 1 - beta * (btc[i] / btc[i - k] - 1) for i in range(k + 1, hi + 1)]


async def seed(ch, **collections):
    await chm.load_snapshot(ch, collections)


def flow_bar(iid, minute, **changes):
    return {
        "instrument_id": iid,
        "token_id": TOKENS.get(iid, iid.split(":")[-1]),
        "market_type": "SPOT",
        "minute_start": iso(minute),
        "complete": True,
        "buy_qty": "2",
        "sell_qty": "1",
        "sum_sq_qty": "1",
    } | changes


def flow(iid, *, until=END, since=None, skip=(), per_minute=None, **common):
    """Complete flow bars of 24 hours before `until`; `per_minute` maps a minute to changes of its bar."""
    rows, minute = [], since or until - timedelta(days=1)
    while minute < until:
        if minute not in skip:
            rows.append(flow_bar(iid, minute, **common, **(per_minute or {}).get(minute, {})))
        minute += MINUTE
    return rows


def trade(price, at):
    return {"price": price, "trade_id": price, "ts": iso(at), "ts_prec": "EXACT", "ts_prec_reason": None}


def recent(*iids, hours=2, until=END):
    """The closes of the last `hours` hours before `until`, price 100 + i / 1000 for the bar i minutes
    before the last one."""
    return [kline(iid, until - (i + 1) * MINUTE, 100 + i / 1000) for iid in iids for i in range(hours * 60)]


def reference_of(evidence, iid=TOKEN):
    (row,) = [r for r in evidence.references if r.instrument_id == iid]
    return row


@pytest.mark.integration
async def test_baselines_returns_beta_history_and_context_equal_the_series_they_were_built_from(
    market_clickhouse,
):
    await seed(market_clickhouse, klines=history(), flow_bars=flow(TOKEN))
    evidence = await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG)
    rows = by_name(evidence)
    assert (evidence.ref, evidence.btc, evidence.eth) == (TOKEN, BTC, ETH)
    assert set(rows) == {"W15M", "W1H", "W4H", "W24H", "EVENT", "PRE24H", "W12H", "W3D", "W7D"}

    token, btc, eth = closes(token_price), closes(btc_price), closes(eth_price)
    n = len(token) - 1
    hi = n - 4 * 60 - 1
    day = rows["W24H"]
    hourly_pairs = 8 * 24 - 5 - 2 + 1
    assert day.beta_btc_pairs == day.beta_eth_pairs == hourly_pairs
    assert day.beta_btc_ols == pytest.approx(1.5, rel=1e-6)
    assert day.beta_btc == pytest.approx(2 / 3 * 1.5 + 1 / 3, rel=1e-6)

    for name, k in (("W15M", 15), ("W1H", 60), ("W4H", 240), ("W24H", 1440), ("W7D", 7 * 1440)):
        row = rows[name]
        assert row.length_s == k * 60
        assert row.raw_return == pytest.approx(token[n] / token[n - k] - 1, rel=1e-12, abs=1e-15)
        assert row.btc_return == pytest.approx(btc[n] / btc[n - k] - 1, rel=1e-12, abs=1e-15)
        assert row.eth_return == pytest.approx(eth[n] / eth[n - k] - 1, rel=1e-12, abs=1e-15)
        assert row.btc_adjusted_return == pytest.approx(
            row.raw_return - day.beta_btc * row.btc_return, rel=1e-12, abs=1e-15
        )
        assert row.eth_adjusted_return == pytest.approx(
            row.raw_return - day.beta_eth * row.eth_return, rel=1e-12, abs=1e-15
        )
        assert row.measure == row.btc_adjusted_return
        sample = adjusted(token, btc, k, hi, day.beta_btc)
        assert row.sample_count == len(sample) == hi - k
        fortieths = statistics.quantiles(sample, n=40, method="inclusive")
        quarters = statistics.quantiles(sample, n=4, method="inclusive")
        assert (row.history_q025, row.history_q25, row.history_q75, row.history_q975) == pytest.approx(
            (fortieths[0], quarters[0], quarters[2], fortieths[38]), rel=1e-9, abs=1e-12
        )
        assert row.percentile == pytest.approx(sum(x <= row.measure for x in sample) / len(sample), abs=1e-12)
        assert row.history_first_end == FIRST + (k + 1) * MINUTE
        assert row.history_last_end == START - MINUTE
    assert rows["W1H"].history_resolution_s == 60
    assert rows["PRE24H"].raw_return == pytest.approx(
        token[hi + 1] / token[hi + 1 - 1440] - 1, rel=1e-12, abs=1e-15
    )

    hours = [END - timedelta(hours=h) for h in range(23, -1, -1)]
    for series, realized in ((btc, day.btc_realized_vol_24h), (token, day.ref_realized_vol_24h)):
        slots = [int((t - FIRST) / MINUTE) for t in hours]
        returns = [log(series[s] / series[s - 60]) for s in slots]
        assert realized == pytest.approx(statistics.stdev(returns), rel=1e-9)
    samples = (hi // 60 * 60 - 1500) // 60 + 1
    assert day.btc_return_24h_samples == day.btc_realized_vol_24h_samples == samples
    for name, k in (("W15M", 15), ("W1H", 60), ("W4H", 240), ("W24H", 1440)):
        window_start_slot = n - k - 1
        assert rows[name].ref_realized_vol_24h_samples == (window_start_slot // 60 * 60 - 1500) // 60 + 1
    assert (day.btc_first_close, day.btc_last_close) == (FIRST + MINUTE, END)
    assert (day.ref_hourly_first, day.ref_hourly_last) == (FIRST + timedelta(hours=1), END)

    relative = relative_truth(dict(snapshot(), relative_sql=evidence), START, END)
    window = relative["windows"]["W1H"]
    assert window["relative_return"] == window["r_btc_adj"] == rows["W1H"].btc_adjusted_return
    assert window["pctile"] == rows["W1H"].percentile
    assert window["normal_range"] == [rows["W1H"].history_q025, rows["W1H"].history_q975]
    assert window["historical_measurement"]["sample_count"] == rows["W1H"].sample_count
    assert relative["beta_btc"] == day.beta_btc and relative["beta_flags"] == {"btc": None, "eth": None}
    assert relative["context_inputs"]["rv24"] == day.btc_realized_vol_24h
    assert relative["market_context"] in ("STRESS", "HEADWIND", "NEUTRAL")
    assert relative["pre_event_runup"]["ar"] == rows["PRE24H"].btc_adjusted_return
    assert set(relative["normal_ranges"]) == {"W1H", "W4H", "W12H", "W24H", "W3D", "W7D"}


@pytest.mark.integration
async def test_baselines_age_rules_a_missing_minute_is_a_missing_price(market_clickhouse):
    last = int((END - FIRST) / MINUTE) - 1
    skip = {(BTC, last), (BTC, last - 1), (ETH, last), (TOKEN, last), (TOKEN, last - 60)}
    await seed(market_clickhouse, klines=history(skip=skip), flow_bars=flow(TOKEN))
    rows = by_name(await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG))
    token, eth = closes(token_price), closes(eth_price)
    n = len(token) - 1
    assert rows["W15M"].btc_return is None and rows["W15M"].btc_adjusted_return is None
    assert rows["W15M"].raw_return == pytest.approx(token[n - 1] / token[n - 15] - 1, rel=1e-12)
    assert rows["W15M"].eth_return == pytest.approx(eth[n - 1] / eth[n - 15] - 1, rel=1e-12)
    assert rows["W1H"].raw_return == pytest.approx(token[n - 1] / token[n - 61] - 1, rel=1e-12)


@pytest.mark.integration
async def test_baselines_read_nothing_after_the_cutoff_or_recorded_after_assembly(market_clickhouse):
    last = int((END - FIRST) / MINUTE) - 1
    await seed(
        market_clickhouse,
        klines=[
            *history(),
            kline(TOKEN, END, 999.0),
            kline(TOKEN, END + MINUTE, 999.0),
            kline(BTC, FIRST + last * MINUTE, 999.0, recorded_at=iso(END + timedelta(hours=2))),
        ],
        flow_bars=flow(TOKEN),
    )
    rows = by_name(await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG))
    token, btc = closes(token_price), closes(btc_price)
    n = len(token) - 1
    assert rows["W1H"].raw_return == pytest.approx(token[n] / token[n - 60] - 1, rel=1e-12)
    assert rows["W1H"].btc_return == pytest.approx(btc[n - 1] / btc[n - 60] - 1, rel=1e-12)


@pytest.mark.integration
async def test_baselines_of_a_benchmark_use_the_raw_return_and_need_seven_days_of_history(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        klines=[r for r in history() if r["instrument_id"] == BTC],
        flow_bars=flow(BTC),
    )
    benchmark = snapshot(
        token_id="BTC", instruments=[i for i in snapshot()["instruments"] if i["token_id"] == "BTC"]
    )
    evidence = await relative_evidence(market_clickhouse, benchmark, START, END, CONFIG)
    assert evidence.btc == BTC and evidence.eth is None and evidence.ref == BTC
    rows = by_name(evidence)
    assert rows["W1H"].measure == rows["W1H"].raw_return is not None
    window = relative_truth(dict(benchmark, relative_sql=evidence), START, END)["windows"]["W1H"]
    assert window["r_btc_adj"] is None and window["r_btc_adj_reason"] == "NOT_APPLICABLE_BENCHMARK"
    assert window["relative_return"] == window["r_raw"] is not None
    assert window["normal_range"] is not None and window["normal_range_reason"] is None

    short = await relative_evidence(market_clickhouse, benchmark, START, END, {"historical_lookback_days": 3})
    window = relative_truth(dict(benchmark, relative_sql=short), START, END)["windows"]["W1H"]
    assert window["normal_range"] is None and window["normal_range_reason"] == "INCOMPLETE"
    assert window["pctile"] is None and window["beyond_normal"] is None


@pytest.mark.integration
async def test_statements_without_any_row_give_empty_records_and_incomplete_relative_values(
    market_clickhouse,
):
    evidence = await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG)
    assert evidence.ref is None and len(evidence.horizons) == 9
    assert all(row.raw_return is None and row.percentile is None for row in evidence.horizons)
    assert all(row.sample_count == 0 for row in evidence.horizons)
    relative = relative_truth(dict(snapshot(), relative_sql=evidence), START, END)
    assert relative["ref_venue"] is None and relative["reference_reason"] == "INCOMPLETE"
    assert relative["windows"]["EVENT"]["r_raw"] is None and relative["repricing_band"] is None
    assert relative["anchor"]["ref_price"] is None and relative["anchor"]["price_reason"] == "INCOMPLETE"


@pytest.mark.integration
async def test_reference_is_the_candidate_with_the_largest_complete_24h_flow(market_clickhouse):
    ids = [f"{venue}:spot:T" for venue in "ABCDE"]
    bars = [
        *flow(ids[0], buy_qty="2"),
        *flow(ids[1], buy_qty="9", skip={END - 5 * MINUTE}),
        *flow(ids[2], buy_qty="9", per_minute={END - 10 * MINUTE: {"complete": False}}),
        *flow(ids[3], buy_qty="1", per_minute={END - 20 * MINUTE: {"sell_qty": None}}),
        *flow(ids[4], buy_qty="5"),
    ]
    await seed(market_clickhouse, klines=recent(*ids), flow_bars=bars)
    registry = snapshot(
        instruments=[
            {"instrument_id": iid, "token_id": "T", "market_type": "SPOT", "venue": iid[0]} for iid in ids
        ]
    )
    evidence = await relative_evidence(market_clickhouse, registry, START, END, CONFIG, baselines=False)
    assert evidence.ref == "E:spot:T" and evidence.horizons == () and evidence.peers == ()
    volumes = {iid: reference_of(evidence, iid).flow_volume_native for iid in ids}
    assert volumes == {
        "A:spot:T": 3 * 1440,
        "B:spot:T": None,
        "C:spot:T": None,
        "D:spot:T": None,
        "E:spot:T": 6 * 1440,
    }
    assert [reference_of(evidence, iid).flow_detail for iid in ids] == [
        None,
        "FULL_MINUTE_COVERAGE_INCOMPLETE",
        "FULL_MINUTE_COVERAGE_INCOMPLETE",
        "FLOW_AGGREGATE_FIELDS_MISSING",
        None,
    ]
    relative = relative_truth(dict(registry, relative_sql=evidence), START, END)
    coverage = relative["reference_coverage"]
    assert relative["ref_venue"] == "E:spot:T"
    assert coverage["E:spot:T"]["complete"] is True
    assert coverage["E:spot:T"]["covered_volume_native"] == 6 * 1440
    assert coverage["D:spot:T"]["complete"] is False
    assert coverage["D:spot:T"]["excluded"][0]["detail"] == "FLOW_AGGREGATE_FIELDS_MISSING"


@pytest.mark.integration
async def test_an_instrument_without_a_close_is_not_a_reference_candidate(market_clickhouse):
    await seed(market_clickhouse, flow_bars=flow(TOKEN))
    evidence = await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG, baselines=False)
    row = reference_of(evidence)
    assert evidence.ref is None and row.last_close is None and row.flow_volume_native == 3 * 1440


@pytest.mark.integration
async def test_anchor_and_entry_prices_are_the_latest_last_trade_before_each_bound(market_clickhouse):
    points = [
        {
            "boundary": iso(START),
            "complete": True,
            "last_trade": trade("100.6", START - timedelta(seconds=1)),
        },
        {
            "boundary": iso(START),
            "complete": False,
            "last_trade": trade("999", START - timedelta(milliseconds=1)),
        },
        {
            "boundary": iso(START + MINUTE),
            "complete": True,
            "last_trade": trade("998", START + timedelta(seconds=59)),
        },
    ]
    bars = flow(
        TOKEN,
        per_minute={
            START - MINUTE: {
                "last_trade": trade("100.5", START - timedelta(seconds=4)),
                "reference_points": points,
            },
            END - MINUTE: {"last_trade": trade("101.5", END - timedelta(seconds=2))},
        },
    )
    await seed(market_clickhouse, klines=recent(TOKEN, BTC), flow_bars=bars)
    evidence = await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG)
    row = reference_of(evidence)
    assert evidence.ref == TOKEN
    assert (
        json.loads(row.anchor_trade)["price"] == "100.6" and json.loads(row.entry_trade)["price"] == "101.5"
    )
    assert by_name(evidence)["EVENT"].raw_return == pytest.approx(101.5 / 100.6 - 1, rel=1e-12)
    relative = relative_truth(dict(snapshot(), relative_sql=evidence), START, END)
    assert relative["anchor"]["ref_price"] == Decimal("100.6")
    assert relative["anchor"]["price_source"] == "LAST_TRADE"
    assert relative["anchor"]["price_observed_at"] == iso(START - timedelta(seconds=1))
    assert relative["anchor"]["evidence"]["trade_id"] == "100.6"
    assert relative["entry_ref_price"] == Decimal("101.5") and relative["entry_price_source"] == "LAST_TRADE"
    assert relative["windows"]["EVENT"]["r_raw"] == pytest.approx(101.5 / 100.6 - 1, rel=1e-12)
    assert relative["outcome_btc_reference"]["price_source"] == "KLINE_CLOSE"


@pytest.mark.integration
async def test_without_trades_the_entry_is_the_last_close_and_the_event_return_is_missing(market_clickhouse):
    await seed(market_clickhouse, klines=recent(TOKEN, BTC), flow_bars=flow(TOKEN))
    evidence = await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG)
    relative = relative_truth(dict(snapshot(), relative_sql=evidence), START, END)
    assert relative["entry_price_source"] == "KLINE_CLOSE"
    assert relative["entry_ref_price"] == reference_of(evidence).last_close
    assert relative["anchor"]["ref_price"] is None and relative["windows"]["EVENT"]["r_raw"] is None
    assert relative["repricing_band"] is None
    assert relative["close_1h_price"] == reference_of(evidence).hourly_close is not None


def segment(start, finish, **changes):
    return {
        "start": iso(start),
        "end": iso(finish),
        "complete": True,
        "observed_at": iso(finish),
        "buy_qty": "4",
        "sell_qty": "1",
        "sum_sq_qty": "1",
    } | changes


async def partial_minute_reference(ch, first_segments, last_segments):
    """The reference row of a cutoff 15 seconds into a minute, whose 24-hour window therefore starts 15
    seconds into a minute too; the two partial minutes carry the given boundary segments."""
    end = END + timedelta(seconds=15)
    opened = end - timedelta(days=1)
    first, last = opened.replace(second=0, microsecond=0), end.replace(second=0, microsecond=0)
    bars = flow(TOKEN, until=last, since=first + MINUTE)
    for minute, segments in ((first, first_segments(opened, first)), (last, last_segments(last, end))):
        if segments is not None:
            bars.append(flow_bar(TOKEN, minute, complete=False, boundary_segments=segments))
    await seed(ch, klines=recent(TOKEN), flow_bars=bars)
    shown = snapshot(cutoff=iso(end), assembled_at=iso(end + timedelta(hours=1)))
    return await relative_evidence(ch, shown, START, end, CONFIG, baselines=False)


@pytest.mark.integration
async def test_partial_minute_bounds_add_the_chain_of_complete_boundary_segments(market_clickhouse):
    evidence = await partial_minute_reference(
        market_clickhouse,
        lambda opened, first: [segment(opened, first + MINUTE)],
        lambda last, end: [segment(last, end)],
    )
    row = reference_of(evidence)
    assert row.flow_detail is None
    assert row.flow_volume_native == 3 * (1440 - 1) + 5 + 5


@pytest.mark.integration
@pytest.mark.parametrize(
    "first_segments,last_segments",
    [
        (lambda opened, first: [segment(opened, first + MINUTE)], lambda last, end: None),
        (lambda opened, first: None, lambda last, end: [segment(last, end)]),
        (
            lambda opened, first: [segment(opened, first + MINUTE, complete=False)],
            lambda last, end: [segment(last, end)],
        ),
        (
            lambda opened, first: [segment(opened, first + MINUTE)],
            lambda last, end: [segment(last, end - timedelta(seconds=1))],
        ),
    ],
    ids=["no segment at the end", "no segment at the start", "incomplete segment", "segment ends early"],
)
async def test_partial_minute_bounds_without_covering_segments_exclude_the_instrument(
    market_clickhouse, first_segments, last_segments
):
    evidence = await partial_minute_reference(market_clickhouse, first_segments, last_segments)
    row = reference_of(evidence)
    assert (row.flow_volume_native, row.flow_detail) == (None, "BOUNDARY_AGGREGATE_UNAVAILABLE")
    assert evidence.ref is None


@pytest.mark.integration
async def test_flow_and_closes_recorded_after_assembly_are_not_read(market_clickhouse):
    late = iso(END + timedelta(hours=2))
    await seed(
        market_clickhouse,
        klines=[*recent(TOKEN), kline(TOKEN, END - MINUTE, 555.0, recorded_at=late)],
        flow_bars=flow(TOKEN, per_minute={END - MINUTE: {"recorded_at": late}}),
    )
    evidence = await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG, baselines=False)
    row = reference_of(evidence)
    assert row.flow_detail == "FULL_MINUTE_COVERAGE_INCOMPLETE"
    assert row.last_close != 555 and row.last_close_time == END - MINUTE


@pytest.mark.integration
async def test_hourly_close_is_the_last_close_on_a_whole_hour(market_clickhouse):
    until = END - timedelta(minutes=30)
    await seed(market_clickhouse, klines=recent(TOKEN, hours=3, until=until))
    evidence = await relative_evidence(market_clickhouse, snapshot(), START, END, CONFIG, baselines=False)
    row = reference_of(evidence)
    assert row.last_close_time == until
    assert row.hourly_close_time == END - timedelta(hours=1)
    assert row.hourly_close == Decimal(repr(100 + 30 / 1000))


@pytest.mark.integration
async def test_sector_returns_use_the_closes_at_both_bounds_and_the_age_rule(market_clickhouse):
    ids = {"P1": "A:spot:P1", "P2": "A:spot:P2", "P3": "A:spot:P3", "P4": "A:spot:P4"}
    total = int(timedelta(days=2) / MINUTE)
    rows = []
    for i in range(total):
        opened = END - timedelta(days=2) + i * MINUTE
        price = 10 + i / 1000
        rows.append(kline(ids["P1"], opened, price))
        if opened < END - 2 * MINUTE:
            rows.append(kline(ids["P2"], opened, price))
        if opened < END - 30 * MINUTE or opened == END - 3 * MINUTE:
            rows.append(kline(ids["P3"], opened, price))
    await seed(market_clickhouse, klines=rows)

    def price(back):
        return 10 + (total - back - 1) / 1000

    shown = snapshot(
        tokens=[{"token_id": t, "symbol": t, "sector": "S"} for t in ("T", "P1", "P2", "P3", "P4")],
        instruments=[
            {"instrument_id": TOKEN, "token_id": "T", "market_type": "SPOT", "venue": "A"},
            *(
                {"instrument_id": iid, "token_id": name, "market_type": "SPOT", "venue": "A"}
                for name, iid in ids.items()
            ),
        ],
    )
    evidence = await relative_evidence(market_clickhouse, shown, START, END, CONFIG)
    got = {(r.instrument_id, r.name): r.peer_return for r in evidence.peers}
    assert {iid for iid, _ in got} == {ids["P1"], ids["P2"], ids["P3"]}
    for name, back in (("W15M", 15), ("W1H", 60), ("W4H", 240), ("W24H", 1440), ("EVENT", 240)):
        assert got[ids["P1"], name] == pytest.approx(price(0) / price(back) - 1, rel=1e-12)
        assert got[ids["P2"], name] is None
    assert got[ids["P3"], "W15M"] is None
    assert got[ids["P3"], "W1H"] == pytest.approx(price(2) / price(60) - 1, rel=1e-12)

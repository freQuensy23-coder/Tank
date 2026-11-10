"""Order-book statements (03 §5.5) against a real ClickHouse server (CLICKHOUSE_TEST_URL): book_liquidity.sql,
book_tiers.sql and book_fragmentation.sql, and the rules that read their records."""

import json
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import pytest
from liquidity_helpers import window_liquidity
from volume_helpers import buckets, evidence

from tank import clickhouse_market as chm
from tank.analysis import truth, truth_sql
from tank.analysis.truth import liquidity_evidence
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
ANCHOR = END - timedelta(hours=2)
SECOND = timedelta(seconds=1)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
A, B, C = "A:spot:T", "B:spot:T", "C:spot:T"
W15M, W1H = {"W15M": 900}, {"W1H": 3600}


def snapshot(*instruments, **changes):
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + HOUR),
            instruments=[
                {"instrument_id": iid, "token_id": "T", "venue": iid.split(":")[0], "market_type": "SPOT"}
                for iid in instruments
            ],
        )
        | changes
    )


def side(usd):
    return {"p1": {"usd": usd, "native": None if usd is None else str(float(usd) / 100)}}


def book(iid, moment, bid="1000", ask="2000", spread="1", mid="100", covered=2, **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "exchange_ts": iso(moment),
        "exchange_ts_prec": "EXACT",
        "integrity_ok": True,
        "mid": mid,
        "spread_bps": spread,
        "range_covered_pct": {"bid": str(covered), "ask": str(covered)},
        "depth": {"bid": side(bid), "ask": side(ask)},
        "quote_asset": "USD",
    } | changes


def books(iid, since, until=END, step=10 * SECOND, **common):
    moment, rows = since, []
    while moment <= until:
        rows.append(book(iid, moment, **common))
        moment += step
    return rows


def bar(iid, minute, buy="0", sell="0", low="100", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "market_type": "SPOT",
        "minute_start": iso(minute),
        "complete": True,
        "buy_qty": "1",
        "sell_qty": "1",
        "sum_sq_qty": "1",
        "buy_notional": buy,
        "sell_notional": sell,
        "low": low,
    } | changes


async def seed(ch, **collections):
    await chm.load_snapshot(ch, collections)


async def evidence_of(ch, *ids, windows=W1H, anchor=ANCHOR, state=False, config=None, **changes):
    return await liquidity_evidence(
        ch, snapshot(*ids, **changes), END, config or {}, windows, anchor, state=state
    )


def venue(result, name, window="W1H"):
    return next(r for r in result.books if r.venue == name and r.window == window)


def same(value):
    return pytest.approx(value, rel=1e-9, abs=1e-15)


def vwap(spent, filled, mid=100):
    return ((spent / filled / mid) - 1) * 10000


@pytest.mark.integration
async def test_the_latest_book_of_a_venue_gives_its_depth_spread_and_the_stored_coverage(market_clickhouse):
    rows = [
        book(A, END - 30 * SECOND, bid="500", ask="600", spread="2"),
        book(A, END - 5 * SECOND, bid="1000", ask="2000", spread="3", covered="1.5"),
        book(A, END + 5 * SECOND, bid="9", ask="9"),
    ]
    await seed(market_clickhouse, books=rows)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert one.books == 2
    assert one.latest_at == END - 5 * SECOND and one.latest_prec == "EXACT" and one.latest_prec_reason is None
    assert (one.latest_spread, one.latest_bid, one.latest_ask) == (3, 1000, 2000)
    assert (one.latest_bid_native, one.latest_ask_native) == (10, 20)
    assert json.loads(one.latest_covered) == {"bid": "1.5", "ask": "1.5"}


@pytest.mark.integration
@pytest.mark.parametrize(
    "covered,known",
    [(2, True), (1, True), (0.99, False)],
    ids=["beyond", "exactly-the-band", "short-of-the-band"],
)
async def test_a_band_beyond_the_covered_range_has_no_depth_and_is_never_extrapolated(
    market_clickhouse, covered, known
):
    await seed(market_clickhouse, books=[book(A, END - SECOND, covered=covered)])
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.latest_bid, one.latest_ask) == ((1000, 2000) if known else (None, None))
    assert (one.latest_bid_native is not None) is known


@pytest.mark.integration
async def test_a_covered_range_stored_as_one_number_applies_to_both_sides(market_clickhouse):
    await seed(market_clickhouse, books=[book(A, END - SECOND, range_covered_pct="3")])
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.latest_bid, one.latest_ask) == (1000, 2000)


@pytest.mark.integration
async def test_the_configured_band_selects_the_depth_and_the_coverage_it_needs(market_clickhouse):
    rows = [book(A, END - SECOND, covered=1.5)]
    rows[0]["depth"]["bid"]["p2"] = {"usd": "7", "native": "0.07"}
    rows[0]["depth"]["bid"]["p0_5"] = {"usd": "3", "native": "0.03"}
    await seed(market_clickhouse, books=rows)
    for band, bid in ((0.5, 3), (1, 1000), (2, None)):
        one = venue(await evidence_of(market_clickhouse, A, config={"depth_band_pct": band}), "A")
        assert one.latest_bid == bid


@pytest.mark.integration
async def test_a_book_that_failed_its_integrity_check_or_came_after_the_cutoff_is_not_read(market_clickhouse):
    rows = [
        book(A, END - 20 * SECOND, bid="500"),
        book(A, END - 10 * SECOND, bid="700", integrity_ok=False),
        book(A, END + SECOND, bid="900"),
    ]
    await seed(market_clickhouse, books=rows)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.books, one.latest_bid) == (1, 500)


@pytest.mark.integration
async def test_a_book_recorded_or_received_after_the_assembly_does_not_exist_for_it(market_clickhouse):
    rows = [
        book(A, END - 30 * SECOND, bid="500"),
        book(A, END - 20 * SECOND, bid="600", recorded_at=iso(END + 2 * HOUR)),
        book(A, END - 10 * SECOND, bid="700", received_ts=iso(END + 2 * HOUR)),
    ]
    await seed(market_clickhouse, books=rows)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.books, one.latest_bid) == (1, 500)


@pytest.mark.integration
async def test_the_hour_before_the_anchor_gives_the_pre_event_means(market_clickhouse):
    rows = [
        book(A, ANCHOR - HOUR - SECOND, bid="9999", ask="9999", spread="9"),
        book(A, ANCHOR - HOUR, bid="100", ask="300", spread="1"),
        book(A, ANCHOR - 30 * MINUTE, bid="300", ask="500", spread="3"),
        book(A, ANCHOR - 20 * MINUTE, bid=None, ask="700", spread="5", covered=0.5),
        book(A, ANCHOR, bid="9999", ask="9999", spread="9"),
        book(A, END - SECOND),
    ]
    await seed(market_clickhouse, books=rows)
    one = venue(await evidence_of(market_clickhouse, A, windows={"W3H": 10800}), "A", "W3H")
    assert one.pre_books == 3
    assert (one.pre_bid, one.pre_ask) == (same(200), same(400))
    assert one.pre_depth == same((400 + 800) / 2)
    assert one.pre_spread == same((1 + 3 + 5) / 3)
    assert one.pre_native_missing == 3


@pytest.mark.integration
async def test_the_native_depth_of_the_pre_event_hour_needs_every_book_to_have_it(market_clickhouse):
    rows = [book(A, ANCHOR - 30 * MINUTE), book(A, ANCHOR - 20 * MINUTE, bid="500"), book(A, END - SECOND)]
    rows[1]["depth"]["bid"]["p1"]["native"] = None
    await seed(market_clickhouse, books=rows)
    one = venue(await evidence_of(market_clickhouse, A, windows={"W3H": 10800}), "A", "W3H")
    assert (one.pre_native_missing, one.pre_bid) == (1, same(750))
    assert one.pre_ask_native == same(20)


@pytest.mark.integration
async def test_without_an_anchor_there_is_no_pre_event_hour(market_clickhouse):
    await seed(market_clickhouse, books=books(A, END - HOUR))
    one = venue(await evidence_of(market_clickhouse, A, anchor=None), "A")
    assert (one.pre_books, one.pre_bid, one.pre_ask, one.pre_depth, one.pre_spread) == (
        0,
        None,
        None,
        None,
        None,
    )
    assert one.books > 300


@pytest.mark.integration
async def test_the_window_means_cover_the_books_from_its_start_to_the_cutoff_inclusive(market_clickhouse):
    rows = [
        book(A, END - HOUR - SECOND, bid="9999", ask="9999"),
        book(A, END - HOUR, bid="100", ask="200"),
        book(A, END - 30 * MINUTE, bid="300", ask="400"),
        book(A, END, bid="500", ask="600"),
    ]
    await seed(market_clickhouse, books=rows)
    result = await evidence_of(market_clickhouse, A, windows={"W1H": 3600, "W15M": 900})
    one = venue(result, "A")
    assert (one.window_bid, one.window_ask) == (same(300), same(400))
    short = venue(result, "A", "W15M")
    assert (short.window_bid, short.window_ask) == (500, 600)


@pytest.mark.integration
async def test_every_window_has_a_row_for_every_venue_with_books_or_bars(market_clickhouse):
    await seed(market_clickhouse, books=[book(A, END - SECOND)], flow_bars=[bar(B, END - 5 * MINUTE)])
    result = await evidence_of(market_clickhouse, A, B, windows={"W15M": 900, "W1H": 3600})
    assert sorted((r.window, r.venue, r.books, r.bars) for r in result.books) == [
        ("W15M", "A", 1, 0),
        ("W15M", "B", 0, 1),
        ("W1H", "A", 1, 0),
        ("W1H", "B", 0, 1),
    ]
    assert venue(result, "B").latest_at is None and venue(result, "B").latest_bid is None


@pytest.mark.integration
async def test_the_instruments_of_a_venue_are_one_group(market_clickhouse):
    rows = [book(A, END - 20 * SECOND, bid="500"), book("A:spot:T2", END - 10 * SECOND, bid="700")]
    await seed(market_clickhouse, books=rows)
    result = await evidence_of(market_clickhouse, A, "A:spot:T2")
    assert [(r.venue, r.books, r.latest_bid) for r in result.books] == [("A", 2, 700)]


@pytest.mark.integration
async def test_books_before_the_hour_that_precedes_the_first_window_are_not_read(market_clickhouse):
    await seed(market_clickhouse, books=[book(A, END - 3 * HOUR), book(A, END - 80 * MINUTE)])
    assert (await evidence_of(market_clickhouse, A, anchor=None)).books == ()


def burst_case(net, *, minute=None, base_age=5, refill_bid="1000", burst_low="99.5", lows=()):
    """A venue whose bid depth is 1000, a bar at `minute` that sells `net` more than it buys with the low
    `burst_low`, the book `base_age` seconds before it, the books of the next minute (10 deep, the third
    `refill_bid` deep) and one bar for each of `lows` after it."""
    minute = minute or END - 30 * MINUTE
    rows = [book(A, minute - base_age * SECOND, bid="1000")]
    rows += [book(A, minute + i * 10 * SECOND, bid=refill_bid if i == 3 else "10") for i in range(1, 6)]
    bars_ = [bar(A, minute, sell=str(net), buy="0", low=burst_low)]
    bars_ += [bar(A, minute + (i + 1) * MINUTE, low=low) for i, low in enumerate(lows)]
    return rows, bars_


@pytest.mark.integration
async def test_a_bar_whose_net_selling_reaches_the_bid_depth_is_a_burst_that_is_replenished_in_time(
    market_clickhouse,
):
    rows, bars_ = burst_case(1000, lows=("100",))
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.bursts, one.replenished, one.resilient_bursts) == (1, 1, 1)
    assert one.burst_at == (END - 30 * MINUTE,)
    assert (
        one.burst_replenished == (1,) and one.burst_recovery_s == (same(30),) and one.burst_resilient == (1,)
    )


@pytest.mark.integration
async def test_net_selling_below_the_bid_depth_is_no_burst(market_clickhouse):
    rows, bars_ = burst_case(999.99)
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    assert venue(await evidence_of(market_clickhouse, A), "A").bursts == 0


@pytest.mark.integration
@pytest.mark.parametrize("age,bursts", [(10, 1), (11, 0)], ids=["at-the-interval", "older-than-it"])
async def test_a_base_book_older_than_the_interval_is_no_baseline(market_clickhouse, age, bursts):
    rows, bars_ = burst_case(5000, base_age=age)
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    assert venue(await evidence_of(market_clickhouse, A), "A").bursts == bursts


@pytest.mark.integration
async def test_a_burst_is_not_replenished_when_no_book_in_the_next_minute_is_back_at_the_depth(
    market_clickhouse,
):
    rows, bars_ = burst_case(1000, refill_bid="999")
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.bursts, one.replenished, one.resilient_bursts) == (1, 0, 0)
    assert one.burst_replenished == (0,) and one.burst_recovery_s == (None,)


@pytest.mark.integration
async def test_a_refill_after_the_refill_window_does_not_count(market_clickhouse):
    rows, bars_ = burst_case(1000, refill_bid="1000")
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    short = venue(await evidence_of(market_clickhouse, A, config={"refill_window_s": 29}), "A")
    assert (short.replenished, short.burst_replenished) == (0, (0,))
    exact = venue(await evidence_of(market_clickhouse, A, config={"refill_window_s": 30}), "A")
    assert exact.replenished == 1


@pytest.mark.integration
async def test_a_burst_with_the_cutoff_inside_its_refill_time_has_no_replenished_verdict(market_clickhouse):
    minute = END - 90 * SECOND
    rows = [book(A, minute - 5 * SECOND, bid="1000"), book(A, minute + 20 * SECOND, bid="1000")]
    await seed(market_clickhouse, books=rows, flow_bars=[bar(A, minute, sell="1000", buy="0")])
    one = venue(await evidence_of(market_clickhouse, A, config={"refill_window_s": 120}), "A")
    assert (one.bursts, one.replenished) == (1, 0)
    assert one.burst_replenished == (None,) and one.burst_recovery_s == (same(20),)


@pytest.mark.integration
@pytest.mark.parametrize(
    "lows,verdict",
    [
        (("100", "99", "100"), 0),
        (("100", "99.499999999", "100"), 0),
        (("100", "99.5", "100"), 1),
        (("100", "100", "101"), 1),
        ((), None),
    ],
    ids=["new-low", "new-low-by-a-hair", "equal-low", "higher-lows", "no-later-bar"],
)
async def test_a_burst_is_resilient_when_no_later_bar_within_fifteen_minutes_has_a_lower_low(
    market_clickhouse, lows, verdict
):
    rows, bars_ = burst_case(1000, lows=lows)
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert one.burst_resilient == (verdict,)
    assert one.resilient_bursts == (1 if verdict == 1 else 0)


@pytest.mark.integration
@pytest.mark.parametrize("minutes_after,verdict", [(15, 0), (16, 1)], ids=["at-fifteen", "after-fifteen"])
async def test_a_low_after_the_resilience_time_does_not_count(market_clickhouse, minutes_after, verdict):
    rows, bars_ = burst_case(1000, lows=("100",) * 16)
    bars_[minutes_after]["low"] = "1"
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    assert venue(await evidence_of(market_clickhouse, A), "A").burst_resilient == (verdict,)


@pytest.mark.integration
async def test_a_burst_with_the_cutoff_inside_its_resilience_time_has_no_resilient_verdict(market_clickhouse):
    rows, bars_ = burst_case(1000, minute=END - 10 * MINUTE, lows=("100", "100"))
    await seed(market_clickhouse, books=rows, flow_bars=bars_)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.bursts, one.burst_replenished, one.burst_resilient) == (1, (1,), (None,))


@pytest.mark.integration
async def test_bursts_are_found_in_complete_bars_of_the_window_only(market_clickhouse):
    rows, bars_ = burst_case(1000, minute=END - 40 * MINUTE, lows=("100",))
    bars_.append(bar(A, END - 20 * MINUTE, sell="5000", buy="0", complete=False))
    await seed(market_clickhouse, books=rows + [book(A, END - 20 * MINUTE - 5 * SECOND)], flow_bars=bars_)
    result = await evidence_of(market_clickhouse, A, windows={"W15M": 900, "W1H": 3600})
    assert (venue(result, "A", "W15M").bursts, venue(result, "A", "W1H").bursts) == (0, 1)
    assert (venue(result, "A", "W15M").bars, venue(result, "A", "W1H").bars) == (0, 2)


@pytest.mark.integration
async def test_the_ask_depth_at_the_window_end_against_its_start_gives_the_ask_building_flag(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        books=[book(A, END - HOUR - 5 * SECOND, ask="100"), book(A, END - SECOND, ask="200")],
    )
    assert venue(await evidence_of(market_clickhouse, A), "A").ask_building == 1
    await seed(
        market_clickhouse,
        books=[book(B, END - HOUR - 5 * SECOND, ask="100"), book(B, END - SECOND, ask="50")],
    )
    assert venue(await evidence_of(market_clickhouse, B), "B").ask_building == 0


@pytest.mark.integration
async def test_the_ask_flag_needs_a_book_at_the_window_start_not_older_than_the_interval(market_clickhouse):
    await seed(market_clickhouse, books=[book(A, END - HOUR - 11 * SECOND, ask="100"), book(A, END - SECOND)])
    assert venue(await evidence_of(market_clickhouse, A), "A").ask_building is None


@pytest.mark.integration
async def test_the_ask_depth_of_the_flag_does_not_need_the_band_to_be_covered(market_clickhouse):
    rows = [
        book(A, END - HOUR - 5 * SECOND, ask="100", covered=0.1),
        book(A, END - SECOND, ask="200", covered=0.1),
    ]
    await seed(market_clickhouse, books=rows)
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.latest_ask, one.ask_building) == (None, 1)


@pytest.mark.integration
async def test_net_buying_that_reaches_the_ask_depth_is_testable_and_recovered_when_a_book_refills_it(
    market_clickhouse,
):
    minute = END - 30 * MINUTE
    rows = [book(A, minute - 5 * SECOND, ask="400"), book(A, minute + 30 * SECOND, ask="400")]
    await seed(market_clickhouse, books=rows, flow_bars=[bar(A, minute, buy="400", sell="0")])
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.ask_testable_bars, one.ask_recovered_bars) == (1, 1)


@pytest.mark.integration
async def test_net_buying_below_the_ask_depth_is_not_testable_and_a_book_below_it_does_not_recover(
    market_clickhouse,
):
    minute = END - 30 * MINUTE
    rows = [book(A, minute - 5 * SECOND, ask="400"), book(A, minute + 30 * SECOND, ask="399")]
    bars_ = [bar(A, minute, buy="400", sell="0"), bar(A, minute + 5 * MINUTE, buy="399", sell="0")]
    await seed(
        market_clickhouse,
        books=rows + [book(A, minute + 5 * MINUTE - 5 * SECOND, ask="400")],
        flow_bars=bars_,
    )
    one = venue(await evidence_of(market_clickhouse, A), "A")
    assert (one.ask_testable_bars, one.ask_recovered_bars) == (1, 0)


@pytest.mark.integration
async def test_net_buying_inside_the_refill_time_before_the_cutoff_is_not_testable(market_clickhouse):
    minute = END - MINUTE
    await seed(
        market_clickhouse,
        books=[book(A, minute - 5 * SECOND, ask="400")],
        flow_bars=[bar(A, minute, buy="9999", sell="0")],
    )
    assert venue(await evidence_of(market_clickhouse, A), "A").ask_testable_bars == 1
    assert (
        venue(await evidence_of(market_clickhouse, A, config={"refill_window_s": 61}), "A").ask_testable_bars
        == 0
    )


def levels_book(asks, **changes):
    return book(A, END - SECOND, asks=asks, **changes)


async def tier_rows(ch, tiers, *ids, windows=W1H):
    found = await truth_sql.fetch(
        ch,
        truth_sql.BookTier,
        instrument_ids=list(ids),
        groups=[i.split(":")[0] for i in ids],
        cutoff=END,
        assembled=END + HOUR,
        names=list(windows),
        tiers=tiers,
    )
    return {r.venue: r for r in found}


@pytest.mark.integration
async def test_a_tier_that_the_levels_fill_at_the_mid_has_no_slippage(market_clickhouse):
    await seed(market_clickhouse, books=[levels_book([[100, 1000], [101, 1000]])])
    one = (await tier_rows(market_clickhouse, ["100000", "", ""], A))["A"]
    assert one.slippage_t1 == 0 and one.slippage_t2 is None and one.slippage_t3 is None
    assert one.levels == 2 and one.observed_at == END - SECOND


@pytest.mark.integration
async def test_slippage_is_the_vwap_of_the_walk_against_the_mid_in_bps(market_clickhouse):
    await seed(market_clickhouse, books=[levels_book([[100, 1000], [101, 1000], [102, 1000]])])
    found = (await tier_rows(market_clickhouse, ["150000", "250000", "400000"], A))["A"]
    assert found.slippage_t1 == same(vwap(150000, 1000 + 50000 / 101))
    assert found.slippage_t2 == same(vwap(250000, 2000 + 49000 / 102))
    assert found.slippage_t3 is None


@pytest.mark.integration
async def test_levels_given_as_objects_or_strings_are_walked_like_pairs(market_clickhouse):
    await seed(
        market_clickhouse,
        books=[levels_book([{"price": "100", "qty": "1000"}, {"price": "101", "qty": "1000"}])],
    )
    assert (await tier_rows(market_clickhouse, ["100000", "", ""], A))["A"].slippage_t1 == 0
    await seed(market_clickhouse, books=[book(B, END - SECOND, asks=[["100", "10"], ["101", "1000"]])])
    found = (await tier_rows(market_clickhouse, ["2000", "", ""], B))["B"]
    assert found.slippage_t1 == same(vwap(2000, 10 + 1000 / 101))


@pytest.mark.integration
async def test_a_tier_the_levels_cannot_fill_has_no_slippage(market_clickhouse):
    await seed(market_clickhouse, books=[levels_book([[100, 1]])])
    found = (await tier_rows(market_clickhouse, ["100", "101", ""], A))["A"]
    assert found.slippage_t1 == 0 and found.slippage_t2 is None


@pytest.mark.integration
async def test_without_levels_or_a_mid_there_is_no_slippage(market_clickhouse):
    await seed(market_clickhouse, books=[book(A, END - SECOND)])
    assert (await tier_rows(market_clickhouse, ["100", "", ""], A))["A"].slippage_t1 is None
    await seed(market_clickhouse, books=[book(B, END - SECOND, asks=[[100, 1000]], mid="0")])
    assert (await tier_rows(market_clickhouse, ["100", "", ""], B))["B"].slippage_t1 is None


@pytest.mark.integration
async def test_a_quote_other_than_usd_is_converted_by_its_rate_and_has_no_slippage_without_one(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        books=[levels_book([[100, 1000], [101, 1000]], quote_asset="USDT", quote_usd_rate="0.5")],
    )
    found = (await tier_rows(market_clickhouse, ["50000", "100000", ""], A))["A"]
    assert found.slippage_t1 == 0
    assert found.slippage_t2 == same(vwap(200000, 1000 + 100000 / 101))
    unrated = book(B, END - SECOND, asks=[[100, 1000]], quote_asset="USDT", quote_usd_rate=None)
    await seed(market_clickhouse, books=[unrated])
    assert (await tier_rows(market_clickhouse, ["50000", "", ""], B))["B"].slippage_t1 is None


@pytest.mark.integration
async def test_the_split_gives_each_venue_the_part_of_the_tier_that_its_ask_depth_is_of_the_total(
    market_clickhouse,
):
    rows = [
        book(A, END - SECOND, ask="100", asks=[[100, 1000]]),
        book(B, END - SECOND, ask="300", asks=[[100, 3], [200, 1000]]),
    ]
    await seed(market_clickhouse, books=rows)
    found = await tier_rows(market_clickhouse, ["4000", "", ""], A, B)
    assert found["A"].ask_p1 == 100 and found["B"].ask_p1 == 300
    assert found["A"].split_t1 == 0 and found["A"].slippage_t1 == 0
    assert found["B"].split_t1 == same(vwap(3000, 3 + 2700 / 200))
    assert found["B"].slippage_t1 == same(vwap(4000, 3 + 3700 / 200))


@pytest.mark.integration
async def test_a_venue_without_an_ask_depth_has_no_part_of_the_split(market_clickhouse):
    rows = [
        book(A, END - SECOND, ask=None, asks=[[100, 1000]]),
        book(B, END - SECOND, ask="300", asks=[[100, 1000]]),
    ]
    await seed(market_clickhouse, books=rows)
    found = await tier_rows(market_clickhouse, ["4000", "", ""], A, B)
    assert found["A"].ask_p1 is None and found["A"].split_t1 is None and found["A"].slippage_t1 == 0


@pytest.mark.integration
async def test_only_the_latest_book_within_the_hour_before_the_cutoff_is_walked(market_clickhouse):
    rows = [
        book(A, END - 2 * HOUR, asks=[[100, 1000]]),
        book(B, END - 30 * MINUTE, asks=[[100, 1000]]),
        book(B, END - 20 * MINUTE, asks=[[100, 5]]),
    ]
    await seed(market_clickhouse, books=rows)
    found = await tier_rows(market_clickhouse, ["100", "100000", ""], A, B)
    assert sorted(found) == ["B"]
    assert found["B"].slippage_t1 == 0 and found["B"].slippage_t2 is None


@pytest.mark.integration
async def test_each_window_has_its_own_tier_notional(market_clickhouse):
    await seed(market_clickhouse, books=[levels_book([[100, 1000]])])
    both = await truth_sql.fetch(
        market_clickhouse,
        truth_sql.BookTier,
        instrument_ids=[A],
        groups=["A"],
        cutoff=END,
        assembled=END + HOUR,
        names=["W15M", "W1H"],
        tiers=["1000", "", "", "", "", "2000000"],
    )
    by = {r.window: r for r in both}
    assert (by["W15M"].slippage_t1, by["W15M"].slippage_t3) == (0, None)
    assert (by["W1H"].slippage_t1, by["W1H"].slippage_t3) == (None, None)


async def pairs_of(ch, *ids):
    found = await truth_sql.fetch(
        ch,
        truth_sql.BookPair,
        instrument_ids=list(ids),
        groups=[i.split(":")[0] for i in ids],
        cutoff=END,
        assembled=END + HOUR,
        length_us=900_000_000,
        interval_us=10_000_000,
        conversion_bps=2.0,
    )
    return {(r.a_venue, r.b_venue): r for r in found}


def quotes(iid, mid, *, since=END - 15 * MINUTE, until=END, spread="0.1", **changes):
    return books(iid, since, until, mid=mid, spread=spread, **changes)


@pytest.mark.integration
async def test_two_venues_whose_mids_differ_by_more_than_their_spreads_at_every_moment_are_a_persistent_gap(
    market_clickhouse,
):
    await seed(market_clickhouse, books=quotes(A, "100") + quotes(B, "100.10"))
    one = (await pairs_of(market_clickhouse, A, B))[("A", "B")]
    assert (one.problem, one.persistent, one.closed) == (0, 1, 0)


@pytest.mark.integration
async def test_a_gap_within_the_spreads_is_no_gap(market_clickhouse):
    await seed(market_clickhouse, books=quotes(A, "100") + quotes(B, "100.001"))
    one = (await pairs_of(market_clickhouse, A, B))[("A", "B")]
    assert (one.problem, one.persistent) == (0, 0)


@pytest.mark.integration
async def test_a_gap_that_is_absent_at_one_moment_is_not_persistent(market_clickhouse):
    dip = END - 7 * MINUTE
    rows = quotes(A, "100") + [b for b in quotes(B, "100.10") if b["exchange_ts"] != iso(dip)]
    rows.append(book(B, dip, mid="100", spread="0.1"))
    await seed(market_clickhouse, books=rows)
    one = (await pairs_of(market_clickhouse, A, B))[("A", "B")]
    assert (one.problem, one.persistent) == (0, 0)


@pytest.mark.integration
async def test_a_gap_that_closes_at_the_cutoff_is_persistent_and_closed(market_clickhouse):
    rows = quotes(A, "100", until=END - 10 * SECOND) + quotes(B, "100.10", until=END - 10 * SECOND)
    rows += [book(A, END, mid="100", spread="0.1"), book(B, END, mid="100.001", spread="0.1")]
    await seed(market_clickhouse, books=rows)
    one = (await pairs_of(market_clickhouse, A, B))[("A", "B")]
    assert (one.problem, one.persistent, one.closed) == (0, 1, 1)


@pytest.mark.integration
async def test_a_venue_with_a_book_older_than_the_interval_at_a_moment_is_a_problem(market_clickhouse):
    await seed(market_clickhouse, books=quotes(A, "100") + quotes(B, "100.10", step=13 * SECOND))
    one = (await pairs_of(market_clickhouse, A, B))[("A", "B")]
    assert (one.problem, one.persistent) == (1, 0)


@pytest.mark.integration
async def test_a_venue_without_a_book_at_the_start_of_the_period_is_a_problem(market_clickhouse):
    await seed(market_clickhouse, books=quotes(A, "100") + quotes(B, "100.10", since=END - 10 * MINUTE))
    assert (await pairs_of(market_clickhouse, A, B))[("A", "B")].problem == 1


@pytest.mark.integration
async def test_a_book_without_a_spread_is_a_problem(market_clickhouse):
    rows = quotes(A, "100") + quotes(B, "100.10")
    rows[-1]["spread_bps"] = None
    await seed(market_clickhouse, books=rows)
    assert (await pairs_of(market_clickhouse, A, B))[("A", "B")].problem == 1


@pytest.mark.integration
async def test_venues_quoted_in_different_assets_are_compared_in_usd_and_need_a_rate(market_clickhouse):
    usdt = dict(quote_asset="USDT", quote_usd_rate="0.9997")
    await seed(market_clickhouse, books=quotes(A, "100.030009", **usdt) + quotes(B, "100"))
    one = (await pairs_of(market_clickhouse, A, B))[("A", "B")]
    assert (one.problem, one.persistent) == (0, 0)
    await seed(
        market_clickhouse,
        books=quotes(C, "100.5", quote_asset="USDT", quote_usd_rate=None) + quotes(B, "100"),
    )
    assert (await pairs_of(market_clickhouse, C, B))[("B", "C")].problem == 2


@pytest.mark.integration
@pytest.mark.parametrize(
    "mid,persistent", [("100.045", 0), ("100.130039", 1)], ids=["inside-the-conversion-cost", "beyond-it"]
)
async def test_a_cross_quote_gap_must_clear_the_conversion_uncertainty(market_clickhouse, mid, persistent):
    await seed(
        market_clickhouse,
        books=quotes(A, mid, quote_asset="USDT", quote_usd_rate="0.9997") + quotes(B, "100"),
    )
    assert (await pairs_of(market_clickhouse, A, B))[("A", "B")].persistent == persistent


def rest(moment, mid, **changes):
    return {
        "mid": mid,
        "spread_bps": "1",
        "source": "PUBLIC_REST",
        "ts": iso(moment),
        "received_ts": iso(moment),
    } | changes


@pytest.mark.integration
@pytest.mark.parametrize(
    "kind,confirmed",
    [
        ("valid", 1),
        ("flag-only", 0),
        ("after-cutoff", 0),
        ("no-gap", 0),
        ("other-source", 0),
        ("stale", 0),
        ("received-late", 0),
    ],
)
async def test_a_persistent_gap_is_confirmed_only_by_an_independent_rest_book_that_shows_it(
    market_clickhouse, kind, confirmed
):
    rows = []
    for iid, mid in ((A, "100"), (B, "103")):
        for stream in quotes(iid, mid, spread="1"):
            moment = parse_time(stream["exchange_ts"])
            seen = "100" if kind == "no-gap" else mid
            if kind in ("valid", "no-gap"):
                stream["independent_rest"] = rest(moment, seen)
            elif kind == "after-cutoff":
                stream["independent_rest"] = rest(END + SECOND, seen)
            elif kind == "other-source":
                stream["independent_rest"] = rest(moment, seen, source="WEBSOCKET")
            elif kind == "stale":
                stream["independent_rest"] = rest(moment - 11 * SECOND, seen)
            elif kind == "received-late":
                stream["independent_rest"] = rest(moment, seen, received_ts=iso(END + 2 * HOUR))
            stream["second_path_confirmed"] = True
            rows.append(stream)
    await seed(market_clickhouse, books=rows)
    one = (await pairs_of(market_clickhouse, A, B))[("A", "B")]
    assert (one.persistent, one.confirmed) == (1, confirmed)


@pytest.mark.integration
async def test_a_book_that_was_not_confirmed_by_its_second_path_does_not_carry_its_rest_book(
    market_clickhouse,
):
    rows = []
    for iid, mid in ((A, "100"), (B, "103")):
        for stream in quotes(iid, mid, spread="1"):
            stream["independent_rest"] = rest(parse_time(stream["exchange_ts"]), mid)
            stream["second_path_confirmed"] = iid == B
            rows.append(stream)
    await seed(market_clickhouse, books=rows)
    assert (await pairs_of(market_clickhouse, A, B))[("A", "B")].confirmed == 0


@pytest.mark.integration
async def test_every_pair_of_venues_with_books_has_a_row_by_name(market_clickhouse):
    await seed(market_clickhouse, books=quotes(C, "100") + quotes(A, "100") + quotes(B, "100"))
    assert sorted(await pairs_of(market_clickhouse, C, A, B)) == [("A", "B"), ("A", "C"), ("B", "C")]
    assert await pairs_of(market_clickhouse, A) == {}


@pytest.mark.integration
async def test_an_assessment_reads_the_books_the_tiers_and_the_pairs_and_a_light_read_only_the_books(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        books=[book(A, END - SECOND, asks=[[100, 1000]]), book(B, END - SECOND, asks=[[100, 1000]])],
    )
    shown = dict(
        snapshot(A, B), volume_sql=evidence([A], buckets=buckets(END, ["A", "B"], volume=1_250_000.0))
    )
    result = await liquidity_evidence(market_clickhouse, shown, END, {}, W1H, ANCHOR, state=True)
    assert {r.venue for r in result.books} == {"A", "B"} and {r.venue for r in result.tiers} == {"A", "B"}
    assert [(p.a_venue, p.b_venue) for p in result.pairs] == [("A", "B")]
    assert dict(result.tier_usd) == {"W1H": (None, None, None)}
    light = await liquidity_evidence(market_clickhouse, snapshot(A, B), END, {}, W1H, ANCHOR, state=False)
    assert light.tiers == () and light.pairs == () and light.tier_usd == () and len(light.books) == 2


@pytest.mark.integration
async def test_a_token_without_spot_instruments_has_no_liquidity_evidence(market_clickhouse):
    shown = snapshot("P:perp_linear:T")
    shown["instruments"][0]["market_type"] = "PERP_LINEAR"
    assert await liquidity_evidence(market_clickhouse, shown, END, {}, W1H, ANCHOR) == truth_sql.NO_LIQUIDITY


def test_a_condition_reads_the_order_books_its_metrics_need():
    shown = dict(cutoff=iso(END), anchor={"type": "T0", "ts": iso(END - 3 * HOUR)})
    windows = truth.condition_liquidity_windows
    assert windows(shown, [{"metric": "liq.spread_ratio", "window": "W1H"}]) == ({"W15M": 900}, False)
    assert windows(shown, [{"metric": "mt.move_class", "window": "W1H"}]) == ({"EVENT": 10800}, True)
    assert windows(
        shown,
        [
            {"metric": "liq.depth_p1_vs_pre", "window": "W1H"},
            {"metric": "mt.divergence_state", "window": "W1H"},
        ],
    ) == ({"W15M": 900, "EVENT": 10800}, True)
    assert windows(shown, [{"metric": "spot.classification", "window": "W1H"}]) == ({}, False)


def default(name, value):
    return value


def test_the_scaled_tiers_follow_the_volume_of_the_venues_the_window_covers():
    shown = dict(
        snapshot(A, B), volume_sql=evidence([A], buckets=buckets(END, ["A", "B"], volume=1_250_000.0))
    )
    assert truth.tier_usd(shown, {"A", "B"}, default) == (50000, 500000, 2500000)
    assert truth.tier_usd(shown, {"A"}, default) == (25000, 250000, 1250000)
    assert truth.tier_usd(shown, set(), default) == (None, None, None)
    assert truth.tier_usd(dict(shown, volume_sql=None), {"A"}, default) == (None, None, None)
    sizes = [100, 200, 400]
    assert truth.tier_usd(
        shown, {"A"}, lambda name, value: sizes if name == "impact_tiers_usd" else value
    ) == (
        100,
        200,
        400,
    )


def tier_record(venue, ask, slip, split, window="W1H"):
    return truth_sql.BookTier(
        window=window,
        venue=venue,
        observed_at=END,
        ask_p1=ask,
        levels=1,
        slippage_t1=slip,
        slippage_t2=None,
        slippage_t3=None,
        split_t1=split,
        split_t2=None,
        split_t3=None,
    )


def enriched(records):
    spot = {
        "venues": [],
        "composite_max": {"volume": 0},
        "direction": "NEUTRAL",
        "composite_cohort": None,
        "cohort_consistent": {},
    }
    relative = {
        "windows": {"W15M": {"normal_range": None}, "W1H": {"r_raw": None}},
        "context_inputs": {"rv24": None, "rv_normal_range": None},
        "ref_venue": None,
    }
    liquidity = {
        "per_venue": [
            {
                "tier_notional": {"T1": 4000, "T2": None, "T3": None},
                "slippage_bps": {"T1": r.slippage_t1, "T2": None, "T3": None},
            }
            for r in records
        ]
    }
    shown = dict(
        snapshot(A, B),
        liquidity_sql=truth_sql.LiquidityEvidence(
            tiers=tuple(records), tier_usd=(("W1H", (4000.0, None, None)),)
        ),
    )
    truth.enrich_evidence(
        shown, END - HOUR, END, {"W1H": spot, "W15M": deepcopy(spot)}, "W1H", {}, relative, liquidity
    )
    return liquidity


def test_the_split_slippage_is_the_slippage_of_each_venues_part_weighted_by_its_ask_depth():
    found = enriched([tier_record("A", 100.0, 10.0, 20.0), tier_record("B", 300.0, 5.0, 8.0)])
    assert found["best_single_venue_slippage_bps"] == {"T1": 5.0, "T2": None, "T3": None}
    assert found["split_slippage_bps"]["T1"] == same(0.25 * 20 + 0.75 * 8)
    assert found["split_slippage_bps"]["T2"] is None


@pytest.mark.parametrize(
    "records",
    [
        [tier_record("A", 100.0, 10.0, 20.0), tier_record("B", 300.0, 5.0, None)],
        [tier_record("A", None, 10.0, None), tier_record("B", 300.0, 5.0, 8.0)],
        [],
    ],
    ids=["a-part-that-cannot-be-walked", "a-venue-without-an-ask-depth", "no-books"],
)
def test_the_split_slippage_needs_every_venue_to_have_its_part_walked(records):
    assert enriched(records)["split_slippage_bps"]["T1"] is None


def two_venue_bursts(first, second):
    """Venues A and B that each had a burst whose bid depth was back after `first` and `second` seconds."""
    rows, flow = [], []
    minute = END - 30 * MINUTE
    for iid, after in ((A, first), (B, second)):
        rows.append(book(iid, minute - 5 * SECOND, bid="1000"))
        rows += [book(iid, minute + after * SECOND, bid="1000"), book(iid, END - SECOND, bid="1000")]
        flow.append(bar(iid, minute, sell="1000", buy="0"))
    return {"token_id": "T", "books": rows, "flow_bars": flow}


def test_the_bursts_of_the_eligible_venues_are_counted_with_the_median_recovery_of_all_of_them():
    start = END - HOUR
    packet = window_liquidity(two_venue_bursts(10, 30), start, END)
    result = truth.liquidity_truth(
        packet, start, END, {"venues": [{"venue": "A"}, {"venue": "B"}]}, {"windows": {"W4H": {}}}
    )
    assert result["replenishment"]["available"] is True
    assert (result["replenishment"]["bursts"], result["replenishment"]["replenished"]) == (2, 2)
    assert result["replenishment"]["median_recovery_s"] == 20
    assert [(x["venue"], x["recovery_s"]) for x in result["replenishment"]["observations"]] == [
        ("A", 10),
        ("B", 30),
    ]
    only = truth.liquidity_truth(packet, start, END, {"venues": [{"venue": "B"}]}, {"windows": {"W4H": {}}})
    assert only["replenishment"]["bursts"] == 1 and only["replenishment"]["median_recovery_s"] == 30
    none = truth.liquidity_truth(packet, start, END, {"venues": []}, {"windows": {"W4H": {}}})
    assert none["replenishment"]["available"] is False and none["replenishment"]["bursts"] is None


@pytest.mark.asyncio
async def test_a_condition_evaluation_reads_the_order_book_windows_its_metrics_need(monkeypatch):
    seen = []

    async def attach(repo, shown, config, **kwargs):
        seen.append(kwargs)
        return shown

    async def passthrough(repo, shown, config, **kwargs):
        return shown

    monkeypatch.setattr(truth, "with_liquidity", attach)
    for name in (
        "with_oi",
        "with_volumes",
        "with_relative",
        "with_flow",
        "with_funding",
        "with_series",
        "with_supply",
    ):
        monkeypatch.setattr(truth, name, passthrough)
    monkeypatch.setattr(truth, "compute_condition_values", lambda *args, **kwargs: {})

    class Repo:
        async def get(self, *args):
            return {}

    class Market:
        async def condition_snapshot(self, token, cutoff, anchor=None, requirements=None):
            return {
                "token_id": token,
                "cutoff": cutoff,
                "anchor": {"type": "T0", "ts": iso(END - 3 * HOUR)},
            }

    service = truth.MarketTruthService(
        Repo(), SimpleNamespace(config_version="test", code_version="test"), Market()
    )
    await service.condition_values(None, "T", iso(END), [{"metric": "liq.depth_p1_vs_pre", "window": "W1H"}])
    assert seen == [dict(windows={"W15M": 900}, state=False)]
    seen.clear()
    await service.condition_values(None, "T", iso(END), [{"metric": "mt.move_class", "window": "W1H"}])
    assert seen == [dict(windows={"EVENT": 10800}, state=True)]
    seen.clear()
    await service.condition_values(None, "T", iso(END), [{"metric": "spot.classification", "window": "W1H"}])
    assert seen == []

"""Trade-flow statements (03 §5.3) against a real ClickHouse server (CLICKHOUSE_TEST_URL): flow_windows.sql and
flow_flips.sql, and the rules that read their records."""

import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from tank import clickhouse_market as chm
from tank.analysis import truth, truth_sql
from tank.analysis.truth import flow_evidence
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
SECOND = timedelta(seconds=1)
A, B, A2 = "A:spot:T", "B:spot:T", "A:spot:T2"
P, Q = "P:perp_linear:T", "Q:perp_linear:T"
W1H = {"W1H": 3600}
BUCKETS = {
    "B3": {"buy_notional": "1", "sell_notional": "1"},
    "B4": {"buy_notional": "10", "sell_notional": "4"},
    "B5": {"buy_notional": "5", "sell_notional": "1"},
}


def snapshot(*instruments, **changes):
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + HOUR),
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


def bar(iid, minute, buy="3", sell="1", sq="5", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "market_type": "SPOT" if ":spot:" in iid else "PERP_LINEAR",
        "minute_start": iso(minute),
        "complete": True,
        "buy_qty": buy,
        "sell_qty": sell,
        "sum_sq_qty": sq,
        "buy_notional": "300",
        "sell_notional": "100",
        "n_buy": 1,
        "n_sell": 1,
        "size_buckets": BUCKETS,
    } | changes


def bars(iid, since, until=END, skip=(), **common):
    minute, rows = since, []
    while minute < until:
        if minute not in skip:
            rows.append(bar(iid, minute, **common))
        minute += MINUTE
    return rows


def segment(start, finish, buy="2", sell="1", sq="3", **changes):
    return {
        "start": iso(start),
        "end": iso(finish),
        "complete": True,
        "observed_at": iso(finish),
        "buy_qty": buy,
        "sell_qty": sell,
        "sum_sq_qty": sq,
        "buy_notional": "20",
        "sell_notional": "10",
        "n_buy": 1,
        "n_sell": 1,
        "size_buckets": BUCKETS,
    } | changes


def carrier(iid, minute, *pieces):
    """The bar of a minute that holds boundary segments in place of its own complete flow."""
    return bar(iid, minute, complete=False, boundary_segments=list(pieces))


async def seed(ch, **collections):
    await chm.load_snapshot(ch, collections)


async def flow(ch, *instruments, windows=W1H, flips=None, end=END, **changes):
    return await flow_evidence(ch, snapshot(*instruments, **changes), end, {}, windows, flips or {})


def spot(result, venue, window="W1H", previous=False):
    return next(
        r for r in result.spot if r.venue == venue and r.window == window and bool(r.previous) == previous
    )


def same(value):
    return pytest.approx(value, rel=1e-9, abs=1e-15)


def sums(record):
    return (record.buy, record.sell, record.sum_sq)


@pytest.mark.integration
async def test_a_venue_sums_the_complete_minutes_of_a_window_and_of_the_window_before_it(market_clickhouse):
    await seed(
        market_clickhouse,
        flow_bars=bars(A, END - 2 * HOUR, END - HOUR, buy="2")
        + bars(A, END - HOUR, buy="3")
        + bars(B, END - HOUR),
    )
    result = await flow(market_clickhouse, A, B)
    now, before = spot(result, "A"), spot(result, "A", previous=True)
    assert (now.expected, now.minutes, now.incomplete, now.boundary_ok, now.missing) == (60, 60, 0, 1, 0)
    assert sums(now) == (same(180), same(60), same(300))
    assert (now.buy_usd, now.sell_usd) == (same(18000), same(6000))
    assert now.reasons == () and now.left_pieces == () and now.right_pieces == ()
    assert (before.expected, before.minutes, before.incomplete) == (60, 60, 0)
    assert sums(before) == (same(120), same(60), same(300))
    other = spot(result, "B", previous=True)
    assert (other.minutes, other.buy) == (0, 0)
    assert sums(spot(result, "B")) == (same(180), same(60), same(300))


@pytest.mark.integration
async def test_every_window_of_the_assessment_has_its_own_rows_and_a_group_without_a_bar_has_none_found(
    market_clickhouse,
):
    await seed(market_clickhouse, flow_bars=bars(A, END - 2 * HOUR))
    result = await flow(market_clickhouse, A, B, windows={"W15M": 900, "W1H": 3600})
    assert sorted((r.window, r.previous, r.venue) for r in result.spot) == [
        (window, previous, venue) for window in ("W15M", "W1H") for previous in (0, 1) for venue in ("A", "B")
    ]
    assert spot(result, "A", "W15M").buy == same(45) and spot(result, "A", "W15M", True).buy == same(45)
    absent = spot(result, "B", "W15M")
    assert (absent.expected, absent.minutes, absent.incomplete, absent.buy, absent.boundary_ok) == (
        15,
        0,
        0,
        0,
        1,
    )


@pytest.mark.integration
async def test_a_missing_minute_is_not_found_and_an_incomplete_one_is_counted_with_its_reason(
    market_clickhouse,
):
    rows = bars(A, END - HOUR, skip={END - 10 * MINUTE, END - 20 * MINUTE})
    rows[5].update(complete=False, gap_reason="WEBSOCKET_GAP")
    await seed(market_clickhouse, flow_bars=rows)
    now = spot(await flow(market_clickhouse, A), "A")
    assert (now.expected, now.minutes, now.incomplete) == (60, 58, 1)
    assert sums(now) == (same(174), same(58), same(290))
    assert now.reasons == ("WEBSOCKET_GAP",)


@pytest.mark.integration
async def test_a_bar_recorded_or_received_after_the_assembly_does_not_exist_for_it(market_clickhouse):
    rows = bars(A, END - HOUR)
    rows[0].update(recorded_at=iso(END + 2 * HOUR))
    rows[1].update(received_ts=iso(END + 2 * HOUR))
    rows[2].update(recorded_at=iso(END + HOUR), received_ts=iso(END + HOUR))
    await seed(market_clickhouse, flow_bars=rows)
    now = spot(await flow(market_clickhouse, A), "A")
    assert (now.minutes, now.buy) == (58, same(174))


@pytest.mark.integration
async def test_a_bar_at_the_cutoff_or_before_the_window_start_is_not_in_the_window(market_clickhouse):
    await seed(
        market_clickhouse,
        flow_bars=bars(A, END - HOUR - 5 * MINUTE, END + 5 * MINUTE, buy="3"),
    )
    now = spot(await flow(market_clickhouse, A), "A")
    assert (now.minutes, now.buy) == (60, same(180))


@pytest.mark.integration
async def test_the_instruments_of_a_venue_are_one_group_and_a_perp_is_its_own(market_clickhouse):
    await seed(
        market_clickhouse,
        flow_bars=bars(A, END - HOUR)
        + bars(A2, END - HOUR, buy="1", sell="0", sq="1")
        + bars(P, END - HOUR)
        + bars(Q, END - HOUR, buy="5"),
    )
    result = await flow(market_clickhouse, A, A2, P, Q)
    assert [r.venue for r in result.spot if r.window == "W1H" and not r.previous] == ["A"]
    one = spot(result, "A")
    assert (one.minutes, one.buy, one.sell, one.sum_sq) == (60, same(240), same(60), same(360))
    perps = {r.venue: r for r in result.perp if not r.previous}
    assert sorted(perps) == [P, Q]
    assert (perps[P].minutes, perps[P].buy) == (60, same(180)) and perps[Q].buy == same(300)


@pytest.mark.integration
async def test_the_partial_minutes_at_both_bounds_are_covered_by_chains_of_complete_segments(
    market_clickhouse,
):
    end = END + 30 * SECOND
    first, last = END - 10 * MINUTE, END
    left_pieces = [
        segment(first, first + 30 * SECOND, buy="6", sell="4", sq="9"),
        segment(first + 30 * SECOND, first + MINUTE, buy="8", sell="3", sq="7"),
    ]
    right_pieces = [
        segment(last, last + 30 * SECOND, buy="5", sell="2", sq="4"),
        segment(last + 30 * SECOND, last + MINUTE, buy="9", sell="9", sq="9"),
    ]
    rows = bars(A, first + MINUTE, last) + [carrier(A, first, *left_pieces), carrier(A, last, *right_pieces)]
    await seed(market_clickhouse, flow_bars=rows)
    result = await flow(market_clickhouse, A, windows={"W": 600}, end=end)
    now = spot(result, "A", "W")
    assert (now.expected, now.minutes, now.incomplete, now.boundary_ok, now.missing) == (9, 9, 0, 1, 0)
    assert sums(now) == (same(27 + 8 + 5), same(9 + 3 + 2), same(45 + 7 + 4))
    assert [json.loads(p)["buy_qty"] for p in now.left_pieces] == ["8"]
    assert [json.loads(p)["buy_qty"] for p in now.right_pieces] == ["5"]
    before = spot(result, "A", "W", previous=True)
    assert (before.expected, before.minutes, before.boundary_ok) == (9, 0, 0)


@pytest.mark.integration
async def test_a_chain_of_segments_that_does_not_start_at_the_bound_leaves_it_uncovered(market_clickhouse):
    first = END - 10 * MINUTE
    pieces = [
        segment(first, first + 20 * SECOND),
        segment(first + 20 * SECOND, first + 40 * SECOND),
        segment(first + 40 * SECOND, first + MINUTE),
    ]
    await seed(market_clickhouse, flow_bars=bars(A, first + MINUTE) + [carrier(A, first, *pieces)])
    now = spot(await flow(market_clickhouse, A, windows={"W": 570}), "A", "W")
    assert (now.minutes, now.expected, now.boundary_ok) == (9, 9, 0)


@pytest.mark.integration
@pytest.mark.parametrize(
    "changes",
    [
        {"complete": False},
        {"observed_at": "2026-10-16T14:00:00Z"},
        {"received_ts": "2026-10-16T14:00:00Z"},
        {"start": "2026-10-16T11:50:31Z"},
    ],
    ids=["incomplete", "observed-late", "received-late", "not-contiguous"],
)
async def test_a_boundary_segment_that_is_incomplete_late_or_detached_leaves_the_bound_uncovered(
    market_clickhouse, changes
):
    first = END - 10 * MINUTE
    pieces = [
        segment(first, first + 30 * SECOND),
        segment(first + 30 * SECOND, first + MINUTE) | changes,
    ]
    await seed(market_clickhouse, flow_bars=bars(A, first + MINUTE) + [carrier(A, first, *pieces)])
    now = spot(await flow(market_clickhouse, A, windows={"W": 570}), "A", "W")
    assert (now.minutes, now.boundary_ok) == (9, 0)


@pytest.mark.integration
@pytest.mark.parametrize("both", [False, True], ids=["one-chain", "both-chains"])
async def test_every_instrument_of_a_venue_needs_its_own_chain_over_a_partial_minute(market_clickhouse, both):
    first = END - 10 * MINUTE
    piece = segment(first + 30 * SECOND, first + MINUTE)
    rows = bars(A, first + MINUTE) + bars(A2, first + MINUTE) + [carrier(A, first, piece)]
    rows.append(carrier(A2, first, piece) if both else carrier(A2, first))
    await seed(market_clickhouse, flow_bars=rows)
    now = spot(await flow(market_clickhouse, A, A2, windows={"W": 570}), "A", "W")
    assert (now.minutes, now.boundary_ok) == (9, int(both))
    if both:
        assert now.buy == same(2 * 27 + 2 * 2) and len(now.left_pieces) == 2


@pytest.mark.integration
@pytest.mark.parametrize(
    "missing,bit",
    [("buy_qty", 1), ("sell_qty", 2), ("sum_sq_qty", 4), ("buy_notional", 8), ("sell_notional", 16)],
)
async def test_a_value_missing_from_a_segment_is_reported_and_not_summed(market_clickhouse, missing, bit):
    first = END - 10 * MINUTE
    piece = segment(first + 30 * SECOND, first + MINUTE)
    del piece[missing]
    await seed(market_clickhouse, flow_bars=bars(A, first + MINUTE) + [carrier(A, first, piece)])
    now = spot(await flow(market_clickhouse, A, windows={"W": 570}), "A", "W")
    assert (now.boundary_ok, now.missing) == (1, bit)


@pytest.mark.integration
async def test_the_notional_of_the_large_size_buckets_is_summed(market_clickhouse):
    await seed(market_clickhouse, flow_bars=bars(A, END - HOUR))
    now = spot(await flow(market_clickhouse, A), "A")
    assert (now.large_buy, now.large_sell, now.missing) == (same(900), same(300), 0)


@pytest.mark.integration
@pytest.mark.parametrize(
    "buckets",
    [
        None,
        {},
        {**BUCKETS, "B5": {"buy_notional": None, "sell_notional": "1"}},
        {key: value for key, value in BUCKETS.items() if key != "B4"},
    ],
    ids=["absent", "empty", "no-notional", "no-bucket"],
)
async def test_a_bar_without_the_large_size_buckets_or_their_notional_is_reported(market_clickhouse, buckets):
    rows = bars(A, END - HOUR)
    rows[3]["size_buckets"] = buckets
    await seed(market_clickhouse, flow_bars=rows)
    now = spot(await flow(market_clickhouse, A), "A")
    assert (now.minutes, now.missing) == (60, 32)


@pytest.mark.integration
async def test_the_reasons_of_every_segment_of_the_scanned_bars_are_reported_and_older_ones_are_not(
    market_clickhouse,
):
    scanned = END - 15 * MINUTE
    old = END - 3 * HOUR
    await seed(
        market_clickhouse,
        flow_bars=bars(A, END - HOUR, skip={scanned})
        + [
            carrier(
                A,
                scanned,
                segment(scanned, scanned + 10 * SECOND, complete=False, coverage_reason="INCOMPLETE"),
            ),
            carrier(A, old, segment(old, old + 10 * SECOND, coverage_reason="OLD")),
        ],
    )
    now = spot(await flow(market_clickhouse, A), "A")
    assert now.reasons == ("INCOMPLETE",)


@pytest.mark.integration
async def test_the_flow_of_a_window_does_not_depend_on_the_other_windows_asked_for(market_clickhouse):
    await seed(market_clickhouse, flow_bars=bars(A, END - 2 * HOUR))
    alone = await flow(market_clickhouse, A)
    together = await flow(market_clickhouse, A, windows={"W15M": 900, "W1H": 3600, "W4H": 14400})
    assert spot(alone, "A") == spot(together, "A")
    assert (spot(together, "A", "W4H").minutes, spot(together, "A", "W4H", True).minutes) == (120, 0)


@pytest.mark.integration
async def test_a_window_with_three_or_more_quarter_hours_has_the_flow_of_each_of_them(market_clickhouse):
    rows = []
    for k in range(4):
        rows += bars(A, END - HOUR + 15 * k * MINUTE, END - HOUR + 15 * (k + 1) * MINUTE, buy=str(k + 1))
    rows = [r for r in rows if r["minute_start"] != iso(END - 20 * MINUTE)]
    await seed(market_clickhouse, flow_bars=rows + bars(B, END - HOUR))
    result = await flow(
        market_clickhouse, A, B, windows={"W15M": 900, "W1H": 3600}, flips={"W15M": 900, "W1H": 3600}
    )
    assert {r.window for r in result.flips} == {"W1H"}
    steps = [r for r in result.flips if r.venue == "A"]
    assert [r.k for r in steps] == [0, 1, 2, 3]
    assert [r.expected for r in steps] == [15] * 4
    assert [r.minutes for r in steps] == [15, 15, 14, 15]
    assert [r.buy for r in steps] == [same(15), same(30), same(42), same(60)]
    assert [r.sell for r in steps] == [same(15), same(15), same(14), same(15)]
    assert all(r.boundary_ok == 1 and r.incomplete == 0 and r.missing == 0 for r in steps)
    assert [r.buy for r in result.flips if r.venue == "B"] == [same(45)] * 4


@pytest.mark.integration
async def test_a_sub_window_does_not_count_bars_recorded_after_the_assembly_or_one_that_is_incomplete(
    market_clickhouse,
):
    rows = bars(A, END - HOUR)
    rows[0].update(recorded_at=iso(END + 2 * HOUR))
    rows[16].update(complete=False)
    await seed(market_clickhouse, flow_bars=rows)
    result = await flow(market_clickhouse, A, flips=W1H)
    steps = [r for r in result.flips if r.venue == "A"]
    assert [(r.minutes, r.incomplete) for r in steps] == [(14, 0), (15, 1), (15, 0), (15, 0)]


@pytest.mark.integration
async def test_a_bound_inside_a_minute_needs_segments_at_every_quarter_hour_of_a_window_for_its_sub_windows(
    market_clickhouse,
):
    begin = END - HOUR - 30 * SECOND
    boundaries = [begin + 15 * j * MINUTE for j in range(5)]
    carriers = {}
    for j, edge in enumerate(boundaries):
        whole = edge.replace(second=0)
        if j == 2:
            continue
        carriers[whole] = carrier(
            A,
            whole,
            segment(whole, edge, buy=str(20 + j), sell="1", sq="1"),
            segment(edge, whole + MINUTE, buy=str(10 + j), sell="1", sq="1"),
        )
    rows = [bar(A, END - HOUR + m * MINUTE) for m in range(60) if (END - HOUR + m * MINUTE) not in carriers]
    await seed(market_clickhouse, flow_bars=rows + list(carriers.values()))
    result = await flow(market_clickhouse, A, windows={"W": 3630}, flips={"W": 3630})
    steps = [r for r in result.flips if r.venue == "A"]
    assert [r.k for r in steps] == [0, 1, 2, 3]
    assert [r.boundary_ok for r in steps] == [1, 0, 0, 1]
    assert [r.expected for r in steps] == [14] * 4
    assert steps[0].minutes == 14 and steps[0].buy == same(14 * 3 + 10 + 21)
    assert steps[3].buy == same(14 * 3 + 13 + 24)


def test_the_statements_take_exactly_the_parameters_the_assessment_sends():
    kinds = {name: truth_sql.load_statement(name)[1] for name in ("flow_windows", "flow_flips")}
    shared = {"instrument_ids", "groups", "cutoff", "assembled", "names", "lens_us"}
    assert set(kinds["flow_windows"]) == shared | {"large"}
    assert set(kinds["flow_flips"]) == shared


class Recorder:
    def __init__(self):
        self.sent = []

    async def rows(self, query, params=None):
        self.sent.append((query, params))
        return []


def sent_names(recorder):
    return [query.split("-- ")[1].split(":")[0] for query, _ in recorder.sent]


def shown_snapshot(*instruments):
    return snapshot(*instruments, anchor={"type": "T0", "ts": iso(END - timedelta(seconds=79500))})


@pytest.mark.asyncio
async def test_the_assessment_sends_the_flow_of_every_window_by_venue_and_by_perp_instrument():
    recorder = Recorder()
    repo = SimpleNamespace(clickhouse=recorder)
    await truth.with_flow(repo, shown_snapshot(A, B, A2, P, Q), {})
    assert sorted(sent_names(recorder)) == ["flow_flips", "flow_windows", "flow_windows"]
    by_groups = {p["groups"][0]: p for q, p in recorder.sent if q.startswith("-- flow_windows:")}
    spot_params = by_groups["['A','A','B']"]
    perp_params = by_groups[f"['{P}','{Q}']"]
    assert spot_params["instrument_ids"][0] == f"['{A}','{A2}','{B}']"
    assert spot_params["large"][0] == "['B4','B5']" and perp_params["large"][0] == "[]"
    for params in (spot_params, perp_params):
        assert params["names"][0] == "['W15M','W1H','W4H','W24H','EVENT']"
        assert params["lens_us"][0] == "['900000000','3600000000','14400000000','86400000000','79500000000']"
    flips = next(p for q, p in recorder.sent if q.startswith("-- flow_flips:"))
    assert flips["groups"][0] == "['A','A','B']" and flips["names"] == spot_params["names"]


@pytest.mark.asyncio
async def test_a_condition_sends_only_the_windows_it_names_and_no_flips_without_them():
    recorder = Recorder()
    repo = SimpleNamespace(clickhouse=recorder)
    await truth.with_flow(repo, shown_snapshot(A, P), {}, windows={"W1H": 3600}, flips={})
    assert sorted(sent_names(recorder)) == ["flow_windows", "flow_windows"]
    assert {p["names"][0] for _, p in recorder.sent} == {"['W1H']"}
    recorder.sent.clear()
    await truth.with_flow(
        repo, shown_snapshot(A, P), {}, windows={"W1H": 3600, "W4H": 14400}, flips={"W4H": 14400}
    )
    flips = next(p for q, p in recorder.sent if q.startswith("-- flow_flips:"))
    assert flips["names"][0] == "['W4H']" and flips["lens_us"][0] == "['14400000000']"


@pytest.mark.asyncio
async def test_a_token_without_spot_instruments_sends_no_spot_statement_and_one_without_instruments_none():
    recorder = Recorder()
    repo = SimpleNamespace(clickhouse=recorder)
    shown = await truth.with_flow(repo, shown_snapshot(P), {})
    assert sent_names(recorder) == ["flow_windows"] and shown["flow_sql"].spot == ()
    recorder.sent.clear()
    shown = await truth.with_flow(repo, shown_snapshot(), {})
    assert recorder.sent == [] and shown["flow_sql"].spot == () and shown["flow_sql"].perp == ()


@pytest.mark.asyncio
async def test_a_snapshot_whose_anchor_does_not_precede_its_cutoff_is_returned_as_it_is():
    recorder = Recorder()
    repo = SimpleNamespace(clickhouse=recorder)
    shown = dict(shown_snapshot(A), anchor={"type": "T0", "ts": iso(END + MINUTE)})
    assert await truth.with_flow(repo, shown, {}) is shown and recorder.sent == []


def condition_windows(requirements, anchor_age=timedelta(hours=3)):
    shown = dict(cutoff=iso(END), anchor={"type": "T0", "ts": iso(END - anchor_age)})
    return truth.condition_flow_windows(shown, requirements)


def test_a_condition_reads_the_requested_window_of_a_spot_metric_and_every_window_of_a_state():
    assert condition_windows([{"metric": "spot.cohort.direction", "window": "W4H"}]) == ({"W4H": 14400}, {})
    assert condition_windows([{"metric": "dc.class", "window": "W1H"}]) == ({"EVENT": 10800}, {})
    windows, flips = condition_windows([{"metric": "mt.move_class", "window": "W1H"}])
    assert windows == {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400, "EVENT": 10800}
    assert flips == {"EVENT": 10800}
    windows, flips = condition_windows([{"metric": "mt.move_class", "window": "W1H"}], timedelta(days=3))
    assert windows["EVENT"] == 3 * 86400 and flips == {"W4H": 14400}
    assert condition_windows([{"metric": "deriv.oi_class", "window": "W1H"}]) == ({}, {})
    shown = dict(cutoff=iso(END), anchor=None)
    assert truth.condition_flow_windows(shown, [{"metric": "spot.cohort.direction", "window": "EVENT"}]) == (
        {},
        {},
    )
    assert (
        truth.condition_flow_windows(shown, [{"metric": "mt.move_class", "window": "W1H"}])[0]["EVENT"]
        == 14400
    )


def test_the_large_size_buckets_are_those_from_the_large_ticket_threshold_up():
    assert truth.large_buckets(lambda name, default: default) == ["B4", "B5"]
    assert truth.large_buckets(lambda name, default: {"large_ticket_usd": 10000}.get(name, default)) == [
        "B3",
        "B4",
        "B5",
    ]
    assert truth.large_buckets(lambda name, default: {"large_ticket_usd": 5000}.get(name, default)) == []


def record(venue="A", **changes):
    return truth_sql.FlowWindow(
        **(
            dict(
                window="W",
                previous=0,
                venue=venue,
                expected=15,
                minutes=15,
                incomplete=0,
                boundary_ok=1,
                buy=30.0,
                sell=10.0,
                sum_sq=1.0,
                buy_usd=3000.0,
                sell_usd=1000.0,
                large_buy=0.0,
                large_sell=0.0,
                missing=0,
                reasons=(),
                left_pieces=(),
                right_pieces=(),
            )
            | changes
        )
    )


def test_a_venue_with_every_minute_complete_is_in_the_flow_and_one_without_is_excluded_with_its_reason():
    start = END - 15 * MINUTE
    flows, excluded = truth.flow_venues(
        [
            record("A"),
            record("B", minutes=14, reasons=("WEBSOCKET_GAP", "INCOMPLETE")),
            record("C", incomplete=1),
            record("D", boundary_ok=0),
            record("E", missing=2 | 4),
        ],
        start,
        END,
    )
    assert [(v["venue"], v["sign"], v["cvd"], v["volume_native"]) for v in flows] == [("A", 1, 20, 40)]
    assert [(x["venue"], x["detail"]) for x in excluded] == [
        ("B", "FULL_MINUTE_COVERAGE_INCOMPLETE"),
        ("C", "FULL_MINUTE_COVERAGE_INCOMPLETE"),
        ("D", "BOUNDARY_AGGREGATE_UNAVAILABLE"),
        ("E", "FLOW_AGGREGATE_FIELDS_MISSING"),
    ]
    assert excluded[0]["source_reasons"] == ["INCOMPLETE", "WEBSOCKET_GAP"]
    assert excluded[3]["missing_metrics"] == ["sell_qty", "sum_sq_qty"]


def test_the_chosen_segments_of_the_partial_minutes_are_the_boundary_segments_of_the_venue():
    start, end = END - 15 * MINUTE + 30 * SECOND, END + 20 * SECOND
    left = json.dumps(segment(start, start + 30 * SECOND, n_buy=3, n_sell=4))
    right = json.dumps(segment(END, end, n_buy=1, n_sell=1))
    (venue,), _ = truth.flow_venues([record(left_pieces=(left,), right_pieces=(right,))], start, end)
    first, second = venue["boundary_segments"]
    assert (first["start"], first["cutoff"], first["trade_count"]) == (
        iso(start),
        iso(start + 30 * SECOND),
        7,
    )
    assert (second["start"], second["cutoff"], second["trade_count"]) == (iso(END), iso(end), 2)
    assert first["aggregates"] == [json.loads(left)] and second["aggregates"] == [json.loads(right)]


def test_the_sign_of_a_flow_is_its_cumulative_delta_when_its_square_reaches_sigma_squared_times_the_sizes():
    from decimal import Decimal

    assert truth.flow_sign(Decimal(2), Decimal(1)) == 1
    assert truth.flow_sign(Decimal(-2), Decimal(1)) == -1
    assert truth.flow_sign(Decimal(1), Decimal(1)) == 0
    assert truth.flow_sign(Decimal(0), Decimal(1)) == 0

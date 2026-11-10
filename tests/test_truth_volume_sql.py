"""Volume statements (03 §5.4 and §5.7) against a real ClickHouse server (CLICKHOUSE_TEST_URL):
window_volumes.sql, volume_normal.sql and volume_buckets.sql, and the rules that read their records."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
from volume_helpers import buckets, evidence

from tank import clickhouse_market as chm
from tank.analysis import truth, truth_sql
from tank.analysis.truth import derivatives_truth, support_features, volume_evidence
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
START = END - timedelta(hours=1)
MINUTE = timedelta(minutes=1)
CONFIG = {"historical_lookback_days": 8, "history_min_days": 7}
SPOT, PERP, OTHER = "A:spot:T", "P:perp_linear:T", "B:spot:T"
INSTRUMENTS = (SPOT, PERP)


def snapshot(*instruments, **changes):
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + timedelta(hours=1)),
            instruments=[
                {
                    "instrument_id": iid,
                    "token_id": "T",
                    "venue": iid.split(":")[0],
                    "market_type": "SPOT" if ":spot:" in iid else "PERP_LINEAR",
                }
                for iid in instruments or INSTRUMENTS
            ],
        )
        | changes
    )


def kline(iid, minute, usd="10", native="1", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "market_type": "SPOT" if ":spot:" in iid else "PERP_LINEAR",
        "open_time": iso(minute),
        "interval_s": 60,
        "complete": True,
        "quote_volume_usd": usd,
        "volume_native": native,
        "c": "1",
    } | changes


def klines(iid, since, until=END, skip=(), **common):
    minute, rows = since, []
    while minute < until:
        if minute not in skip:
            rows.append(kline(iid, minute, **common))
        minute += MINUTE
    return rows


def flow_bar(iid, minute, buy="3", sell="2", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "market_type": "SPOT" if ":spot:" in iid else "PERP_LINEAR",
        "minute_start": iso(minute),
        "complete": True,
        "buy_qty": "2",
        "sell_qty": "1",
        "sum_sq_qty": "1",
        "buy_notional": buy,
        "sell_notional": sell,
    } | changes


def flow(iid, since, until=END, skip=(), **common):
    minute, rows = since, []
    while minute < until:
        if minute not in skip:
            rows.append(flow_bar(iid, minute, **common))
        minute += MINUTE
    return rows


def segment(start, finish, buy="4", sell="1", **changes):
    return {
        "start": iso(start),
        "end": iso(finish),
        "complete": True,
        "observed_at": iso(finish),
        "buy_notional": buy,
        "sell_notional": sell,
    } | changes


def usd(result, instrument):
    return {
        row.window: (row.current_usd, row.previous_usd)
        for row in result.windows
        if row.instrument_id == instrument
    }


def native(result, market):
    return {row.window: row for row in result.normals if row.market == market}


async def seed(ch, **collections):
    await chm.load_snapshot(ch, collections)


async def volumes(ch, windows, *instruments, end=END, config=CONFIG, **changes):
    return await volume_evidence(ch, snapshot(*instruments, **changes), end, config, windows)


@pytest.mark.integration
async def test_window_volumes_sum_the_klines_of_the_current_and_the_previous_window(market_clickhouse):
    await seed(
        market_clickhouse,
        klines=klines(SPOT, END - 2 * timedelta(hours=1), usd="10")
        + klines(PERP, START - timedelta(hours=1), usd="100"),
    )
    result = await volumes(market_clickhouse, {"W15M": 900, "W1H": 3600})
    assert usd(result, SPOT) == {"W15M": (150, 150), "W1H": (600, 600)}
    assert usd(result, PERP) == {"W15M": (1500, 1500), "W1H": (6000, 6000)}
    assert result.instruments == (PERP, SPOT)


@pytest.mark.integration
async def test_an_instrument_without_rows_has_no_volume_and_other_instruments_are_not_read(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        klines=klines(SPOT, START - timedelta(hours=1)) + klines(OTHER, START - timedelta(hours=1)),
    )
    result = await volumes(market_clickhouse, {"W1H": 3600})
    assert usd(result, PERP) == {"W1H": (None, None)}
    assert usd(result, SPOT) == {"W1H": (600, 600)}
    assert {row.instrument_id for row in result.windows} == {SPOT, PERP}


@pytest.mark.integration
async def test_a_missing_kline_minute_falls_back_to_the_complete_flow_bars_of_that_window_only(
    market_clickhouse,
):
    gap = END - 5 * MINUTE
    first = START - timedelta(hours=1)
    await seed(
        market_clickhouse,
        klines=klines(SPOT, first, skip={gap}),
        flow_bars=flow(SPOT, first),
    )
    result = await volumes(market_clickhouse, {"W15M": 900, "W1H": 3600})
    assert usd(result, SPOT) == {"W15M": (75, 150), "W1H": (300, 600)}


@pytest.mark.integration
async def test_a_window_no_source_covers_has_no_volume(market_clickhouse):
    first = START - timedelta(hours=1)
    gap = END - 5 * MINUTE
    minutes = [first + i * MINUTE for i in range(120)]
    await seed(
        market_clickhouse,
        klines=(
            klines(SPOT, first, skip={gap, END - 3 * MINUTE})
            + klines(PERP, first, skip={gap})
            + klines(OTHER, first, skip={gap})
        ),
        flow_bars=(
            [flow_bar(SPOT, m, complete=m != gap) for m in minutes if m != END - 3 * MINUTE]
            + [flow_bar(PERP, m, **({"sell_notional": None} if m == gap else {})) for m in minutes]
        ),
    )
    result = await volumes(market_clickhouse, {"W15M": 900}, SPOT, PERP, OTHER)
    assert usd(result, SPOT) == {"W15M": (None, 150)}
    assert usd(result, PERP) == {"W15M": (None, 150)}
    assert usd(result, OTHER) == {"W15M": (None, 150)}


@pytest.mark.integration
async def test_an_incomplete_kline_or_one_without_a_usd_volume_is_a_missing_minute(market_clickhouse):
    first = START - timedelta(hours=1)
    rows = klines(SPOT, first)
    rows[-3] = kline(SPOT, END - 3 * MINUTE, complete=False)
    rows[-9] = kline(SPOT, END - 9 * MINUTE, usd=None)
    await seed(market_clickhouse, klines=rows)
    result = await volumes(market_clickhouse, {"W15M": 900, "W1H": 3600}, SPOT)
    assert usd(result, SPOT) == {"W15M": (None, 150), "W1H": (None, 600)}


@pytest.mark.integration
async def test_a_window_bound_inside_a_minute_is_covered_by_boundary_segments_of_flow_bars(
    market_clickhouse,
):
    first = START - timedelta(hours=1)
    edge = END - timedelta(minutes=15, seconds=30)
    whole = edge.replace(second=0)
    pieces = [
        segment(whole, edge, buy="6", sell="4"),
        segment(edge, whole + MINUTE, buy="8", sell="3"),
    ]
    bars = flow(SPOT, first, skip={whole}) + [flow_bar(SPOT, whole, complete=False, boundary_segments=pieces)]
    await seed(market_clickhouse, klines=klines(SPOT, first), flow_bars=bars)
    result = await volumes(market_clickhouse, {"W": 930}, SPOT)
    assert usd(result, SPOT) == {"W": (86, 85)}


@pytest.mark.integration
@pytest.mark.parametrize(
    "changes",
    [
        {"complete": False},
        {"observed_at": "2026-10-16T14:00:00Z"},
        {"sell_notional": None},
        {"start": "2026-10-16T11:44:31Z"},
    ],
    ids=["incomplete", "observed-late", "unpriced", "not-contiguous"],
)
async def test_a_boundary_segment_that_is_incomplete_late_unpriced_or_detached_leaves_the_window_open(
    market_clickhouse, changes
):
    first = START - timedelta(hours=1)
    whole = END - timedelta(minutes=16)
    edge = whole + timedelta(seconds=30)
    pieces = [
        segment(whole, edge, buy="6", sell="4"),
        segment(edge, whole + MINUTE, buy="8", sell="3", **changes),
    ]
    bars = flow(SPOT, first, skip={whole}) + [flow_bar(SPOT, whole, complete=False, boundary_segments=pieces)]
    await seed(market_clickhouse, klines=klines(SPOT, first), flow_bars=bars)
    result = await volumes(market_clickhouse, {"W": 930}, SPOT)
    assert usd(result, SPOT) == {"W": (None, 85)}


@pytest.mark.integration
async def test_a_kline_recorded_after_the_assembly_does_not_exist_for_it(market_clickhouse):
    first = START - timedelta(hours=1)
    rows = klines(SPOT, first)
    rows[-1] = kline(SPOT, END - MINUTE, usd="999", recorded_at=iso(END + timedelta(hours=2)))
    await seed(market_clickhouse, klines=rows)
    result = await volumes(market_clickhouse, {"W15M": 900}, SPOT)
    assert usd(result, SPOT) == {"W15M": (None, 150)}
    later = await volumes(market_clickhouse, {"W15M": 900}, SPOT, assembled_at=iso(END + timedelta(hours=3)))
    assert usd(later, SPOT) == {"W15M": (150 - 10 + 999, 150)}


@pytest.mark.integration
async def test_native_volume_is_the_sum_over_the_market_with_its_history_normal_range(market_clickhouse):
    first = END - timedelta(days=8)
    await seed(
        market_clickhouse,
        klines=klines(SPOT, first, native="1")
        + klines(OTHER, first, native="2")
        + klines(PERP, first, native="5"),
    )
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT, OTHER, PERP)
    spot, perp = native(result, "SPOT")["W1H"], native(result, "PERP")["W1H"]
    assert (spot.value, spot.previous, spot.normal_low, spot.normal_high) == (180, 180, 180, 180)
    assert (perp.value, perp.previous, perp.normal_low, perp.normal_high) == (300, 300, 300, 300)
    assert spot.samples == perp.samples == 8 * 1440 - 120


@pytest.mark.integration
async def test_normal_range_quantiles_are_inclusive_over_the_totals_of_the_history_windows(
    market_clickhouse,
):
    first = END - timedelta(days=8)
    rows = [kline(SPOT, first + i * MINUTE, native=str(1 + (i // 1440))) for i in range(8 * 1440)]
    await seed(market_clickhouse, klines=rows)
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT)
    row = native(result, "SPOT")["W1H"]
    assert row.value == 60 * 8 and row.previous == 60 * 8
    assert 60 <= row.normal_low < row.normal_high <= 480
    totals = sorted(sum(1 + ((i + j) // 1440) for j in range(60)) for i in range(8 * 1440 - 120))
    assert row.samples == len(totals)
    n = len(totals)
    for q, got in ((0.025, row.normal_low), (0.975, row.normal_high)):
        k = (n - 1) * q
        low = int(k)
        expected = totals[low] + (totals[min(low + 1, n - 1)] - totals[low]) * (k - low)
        assert got == pytest.approx(expected)


@pytest.mark.integration
@pytest.mark.parametrize("days,ready", [(6, False), (7, True)])
async def test_the_history_must_span_the_minimum_days_before_the_window_starts(
    market_clickhouse, days, ready
):
    first = END - timedelta(hours=24, days=days, minutes=1)
    await seed(market_clickhouse, klines=klines(SPOT, first, native="1"))
    result = await volumes(
        market_clickhouse, {"W24H": 86400}, SPOT, config=CONFIG | {"historical_lookback_days": 9}
    )
    row = native(result, "SPOT")["W24H"]
    assert row.value == 1440
    assert (row.normal_low is not None) is ready
    assert (row.normal_high is not None) is ready
    assert row.samples > 0


@pytest.mark.integration
async def test_a_missing_minute_removes_every_history_window_it_falls_in_and_the_window_holding_it(
    market_clickhouse,
):
    first = END - timedelta(days=8)
    inside, old = END - 5 * MINUTE, END - timedelta(days=3)
    await seed(market_clickhouse, klines=klines(SPOT, first, skip={inside, old}))
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT)
    row = native(result, "SPOT")["W1H"]
    assert row.value is None and row.previous == 60
    assert row.samples == 8 * 1440 - 120 - 60


@pytest.mark.integration
async def test_every_instrument_of_a_market_must_cover_the_window(market_clickhouse):
    first = END - timedelta(days=8)
    await seed(
        market_clickhouse,
        klines=klines(SPOT, first) + klines(OTHER, first, until=END - timedelta(days=1)),
    )
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT, OTHER)
    row = native(result, "SPOT")["W1H"]
    assert row.value is None and row.previous is None
    assert 0 < row.samples < 8 * 1440 - 120 and row.normal_low == 120 and row.normal_high == 120


@pytest.mark.integration
async def test_a_market_without_klines_has_no_record(market_clickhouse):
    await seed(market_clickhouse, klines=klines(SPOT, END - timedelta(days=8)))
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT, PERP)
    assert set(native(result, "SPOT")) == {"W1H"} and native(result, "PERP") == {}


@pytest.mark.integration
async def test_a_cutoff_inside_a_minute_has_a_range_but_no_window_total(market_clickhouse):
    end = END + timedelta(seconds=15)
    await seed(market_clickhouse, klines=klines(SPOT, END - timedelta(days=8, minutes=1), until=end + MINUTE))
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT, end=end, cutoff=iso(end))
    row = native(result, "SPOT")["W1H"]
    assert row.value is None and row.previous is None
    assert row.normal_low == 60 and row.normal_high == 60


@pytest.mark.integration
async def test_a_window_that_is_not_whole_minutes_has_no_native_record(market_clickhouse):
    await seed(market_clickhouse, klines=klines(SPOT, END - timedelta(days=8)))
    result = await volumes(market_clickhouse, {"W1H": 3600, "EVENT": 3630}, SPOT)
    assert set(native(result, "SPOT")) == {"W1H"}


@pytest.mark.integration
async def test_native_volume_ignores_a_kline_recorded_after_the_assembly(market_clickhouse):
    first = END - timedelta(days=8)
    rows = klines(SPOT, first)
    rows[-1] = kline(SPOT, END - MINUTE, native="1", recorded_at=iso(END + timedelta(hours=2)))
    await seed(market_clickhouse, klines=rows)
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT)
    assert native(result, "SPOT")["W1H"].value is None


@pytest.mark.integration
async def test_volume_buckets_are_per_venue_quarter_hours_of_the_last_day_of_spot_flow(market_clickhouse):
    first = END - timedelta(days=1)
    await seed(
        market_clickhouse,
        flow_bars=flow(SPOT, first) + flow(OTHER, first, buy="1", sell="1") + flow(PERP, first),
    )
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT, OTHER, PERP)
    rows = result.buckets
    assert len(rows) == 96 * 2 and {row.venue for row in rows} == {"A", "B"}
    assert {(row.minutes, row.incomplete, row.missing) for row in rows} == {(15, 0, 0)}
    assert {row.notional for row in rows if row.venue == "A"} == {75}
    assert {row.notional for row in rows if row.venue == "B"} == {30}
    assert min(row.bucket_start for row in rows) == first


@pytest.mark.integration
async def test_volume_buckets_count_incomplete_and_unpriced_bars_and_cut_buckets_at_both_ends(
    market_clickhouse,
):
    end = END + timedelta(minutes=7)
    first = end - timedelta(days=1)
    bars = flow(SPOT, first, until=end)
    bars[100] = flow_bar(SPOT, first + 100 * MINUTE, complete=False)
    bars[200] = flow_bar(SPOT, first + 200 * MINUTE, buy=None)
    await seed(market_clickhouse, flow_bars=bars)
    result = await volumes(market_clickhouse, {"W1H": 3600}, SPOT, end=end, cutoff=iso(end))
    by_start = {row.bucket_start: row for row in result.buckets}
    assert len(by_start) == 97
    assert min(by_start.items())[1].minutes == 8 and max(by_start.items())[1].minutes == 7
    assert sum(row.incomplete for row in result.buckets) == 1
    assert sum(row.missing for row in result.buckets) == 1


def test_volume_statements_take_exactly_the_parameters_the_assessment_sends():
    kinds = {
        name: truth_sql.load_statement(name)[1]
        for name in ("window_volumes", "volume_normal", "volume_buckets")
    }
    assert set(kinds["window_volumes"]) == {"cutoff", "assembled", "instrument_ids", "names", "lens_us"}
    assert set(kinds["volume_normal"]) == {
        "cutoff",
        "assembled",
        "instrument_ids",
        "names",
        "lens_us",
        "lookback_s",
        "min_days",
    }
    assert set(kinds["volume_buckets"]) == {"cutoff", "assembled", "instrument_ids"}


class Recorder:
    def __init__(self):
        self.sent = []

    async def rows(self, query, params=None):
        self.sent.append((query, params))
        return []


@pytest.mark.asyncio
async def test_the_assessment_sends_every_window_and_a_condition_only_its_primary_window():
    recorder = Recorder()
    repo = SimpleNamespace(clickhouse=recorder)
    event = {"type": "T0", "ts": iso(END - timedelta(seconds=79500))}
    shown = snapshot(anchor=event)
    await truth.with_volumes(repo, shown, CONFIG)
    windows = [p for q, p in recorder.sent if "lens_us" in p]
    assert all(p["names"][0] == "['W15M','W1H','W4H','W24H','EVENT']" for p in windows)
    assert windows[0]["lens_us"][0] == "['900000000','3600000000','14400000000','86400000000','79500000000']"
    assert len(recorder.sent) == 3
    recorder.sent.clear()
    await truth.with_volumes(repo, shown, CONFIG, primary_only=True)
    assert {p["names"][0] for q, p in recorder.sent if "names" in p} == {"['EVENT']"}
    recorder.sent.clear()
    await truth.with_volumes(
        repo, snapshot(anchor={"type": "T0", "ts": iso(END - timedelta(days=3))}), CONFIG, primary_only=True
    )
    assert {p["names"][0] for q, p in recorder.sent if "names" in p} == {"['W4H']"}


def rules(**changes):
    return evidence([SPOT, PERP], windows=("W1H",), **changes)


def features(shown, window="W1H"):
    scoped = dict(snapshot(), _primary=window, volume_sql=shown)
    return support_features(scoped, START, END, {}, {}, {}, {})


def test_native_volume_flags_come_from_the_record_against_its_range():
    beyond = features(
        rules(native={"SPOT": dict(value=700.0, previous=300.0, normal_low=100.0, normal_high=500.0)})
    )
    assert beyond["spot_volume_up_beyond_normal"] is True and beyond["spot_volume_within_normal"] is False
    assert beyond["spot_volume"] == dict(
        value=700.0,
        previous=300.0,
        normal_range=[100.0, 500.0],
        unit="NATIVE_TOKEN",
        window="W1H",
        reason=None,
    )
    inside = features(
        rules(native={"SPOT": dict(value=400.0, previous=300.0, normal_low=100.0, normal_high=500.0)})
    )
    assert inside["spot_volume_up_beyond_normal"] is False and inside["spot_volume_within_normal"] is True
    falling = features(
        rules(native={"SPOT": dict(value=700.0, previous=800.0, normal_low=100.0, normal_high=500.0)})
    )
    assert falling["spot_volume_up_beyond_normal"] is False and falling["spot_volume_within_normal"] is False


@pytest.mark.parametrize(
    "changes,reason,within",
    [
        (dict(normal_low=None, normal_high=None), "NOT_ESTIMABLE", None),
        (dict(value=None), "NOT_ESTIMABLE", None),
        (dict(previous=None), None, False),
    ],
    ids=["no-range", "no-total", "no-previous"],
)
def test_native_volume_without_a_range_a_total_or_a_previous_total_has_no_up_flag(changes, reason, within):
    result = features(rules(native={"SPOT": changes}))
    assert result["spot_volume_up_beyond_normal"] is None
    assert result["spot_volume"]["reason"] == reason
    assert result["spot_volume_within_normal"] is within


def test_a_market_without_a_record_has_no_volume_flags_and_other_windows_are_not_read():
    result = features(rules(native={"PERP": None}))
    assert result["perp_volume_up_beyond_normal"] is None and result["perp_volume_within_normal"] is None
    assert result["perp_volume"]["reason"] == "NOT_ESTIMABLE"
    assert features(rules(), window="W4H")["spot_volume"]["value"] is None


def increments(shown):
    return derivatives_truth(dict(snapshot(), _primary="W1H", volume_sql=shown), START, END)


def test_incremental_share_is_the_growth_of_each_market_against_the_growth_of_both():
    result = increments(
        rules(
            usd={
                "SPOT": dict(current_usd=1000.0, previous_usd=400.0),
                "PERP": dict(current_usd=300.0, previous_usd=100.0),
            }
        )
    )
    assert result["incremental_share"]["spot"] == pytest.approx(600 / 800)
    assert result["incremental_share"]["perp"] == pytest.approx(200 / 800)
    assert result["incremental_share"]["increment_usd"] == {"spot": 600.0, "perp": 200.0}
    assert result["incremental_volume_coverage"]["spot"] == dict(
        instruments=[SPOT], complete=True, unit="USD", reason=None
    )


def test_a_market_that_shrank_adds_nothing_and_a_total_of_zero_has_no_share():
    shrank = increments(rules(usd={"PERP": dict(current_usd=100.0, previous_usd=400.0)}))
    assert shrank["incremental_share"]["perp"] == 0 and shrank["incremental_share"]["spot"] == 1
    flat = increments(
        rules(
            usd={
                "SPOT": dict(current_usd=5.0, previous_usd=5.0),
                "PERP": dict(current_usd=5.0, previous_usd=9.0),
            }
        )
    )
    assert flat["incremental_share"] is None


def test_one_instrument_without_a_volume_leaves_its_market_and_the_share_incomplete():
    result = increments(rules(usd={"SPOT": dict(previous_usd=None)}))
    assert result["incremental_share"] is None
    assert result["incremental_volume_coverage"]["spot"]["complete"] is False
    assert result["incremental_volume_coverage"]["spot"]["reason"] == "INCOMPLETE"
    assert result["incremental_volume_coverage"]["perp"]["complete"] is True


def tiers(shown, venues):
    values = truth.tier_usd(dict(snapshot(), volume_sql=shown), set(venues), lambda name, default: default)
    return dict(zip(("T1", "T2", "T3"), values, strict=True))


def test_tiers_scale_with_the_mean_bucket_volume_of_the_venues_the_window_covers():
    shown = evidence([SPOT], buckets=buckets(END, ["A", "B"], volume=1_250_000.0))
    assert tiers(shown, ["A", "B"]) == {"T1": 50000, "T2": 500000, "T3": 2500000}
    full = evidence([SPOT], buckets=buckets(END, ["A"], volume=9_000_000.0))
    assert tiers(full, ["A"]) == {"T1": 100000, "T2": 1000000, "T3": 5000000}


@pytest.mark.parametrize(
    "changes",
    [dict(minutes=14), dict(incomplete=1), dict(missing=1)],
    ids=["short", "incomplete", "unpriced"],
)
def test_a_bucket_without_every_covered_venue_complete_does_not_count(changes):
    good = buckets(END, ["A", "B"], volume=1_000_000.0, count=2)
    rows = [replace(r, notional=2_000_000.0) if r.bucket_start == good[2].bucket_start else r for r in good]
    assert tiers(evidence([SPOT], buckets=rows), ["A", "B"])["T3"] == 3_000_000
    rows[1] = replace(rows[1], **changes)
    assert tiers(evidence([SPOT], buckets=rows), ["A", "B"])["T3"] == 4_000_000
    assert tiers(evidence([SPOT], buckets=[rows[0], rows[2], rows[3]]), ["A", "B"])["T3"] == 4_000_000


def test_venues_the_window_does_not_cover_are_left_out_of_the_bucket_volume():
    shown = evidence([SPOT], buckets=buckets(END, ["A", "B"], volume=1_000_000.0, count=2))
    assert tiers(shown, ["A"])["T3"] == 1_000_000


def test_without_a_covered_bucket_the_tiers_are_unknown():
    assert tiers(evidence([SPOT]), ["A"]) == {"T1": None, "T2": None, "T3": None}
    assert tiers(evidence([SPOT], buckets=buckets(END, ["A"])), []) == {"T1": None, "T2": None, "T3": None}

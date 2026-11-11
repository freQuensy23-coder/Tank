"""Open-interest statements (03 §0, §5.4, §5.7) against a real ClickHouse server (CLICKHOUSE_TEST_URL):
oi_endpoints.sql and oi_history.sql, and the rules that read their records."""

import statistics
from datetime import timedelta
from types import SimpleNamespace

import pytest
from oi_helpers import evidence as stand_in

from tank import clickhouse_market as chm
from tank.analysis import truth, truth_sql
from tank.analysis.truth import oi_evidence
from tank.market import normalize
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
MINUTE = timedelta(minutes=1)
DAY = timedelta(days=1)
SPAN = timedelta(hours=12)
CONFIG = {"historical_lookback_days": 12, "history_min_days": 7}
BINANCE, BYBIT, HYPERLIQUID, OKX = (
    f"{venue}:perp_linear:T" for venue in ("binance", "bybit", "hyperliquid", "okx")
)
KEYS = {
    "binance": "binance.usdm.openInterest",
    "bybit": "bybit.linear.open-interest",
    "hyperliquid": "hyperliquid.perp.ctx",
    "okx": "okx.swap.open-interest",
}
W1H = {"W1H": 3600}


def snapshot(*instruments, **changes):
    venues = [iid.split(":")[0] for iid in instruments]
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + timedelta(hours=1)),
            instruments=[
                {"instrument_id": iid, "token_id": "T", "venue": venue, "market_type": "PERP_LINEAR"}
                for iid, venue in zip(instruments, venues, strict=True)
            ],
            semantics=[
                {
                    "metric_key": KEYS[venue],
                    "metric_family": "oi",
                    "instrument_id": iid,
                    "venue": venue,
                    "sidedness": "UNKNOWN" if venue == "hyperliquid" else "SINGLE_SIDED",
                    "aggregation_class": "oi_native_single_sided",
                }
                for iid, venue in zip(instruments, venues, strict=True)
            ],
        )
        | changes
    )


def observation(iid, at, native="100", usd=None, mark=None, index=None, **changes):
    venue = iid.split(":")[0]
    return {
        "instrument_id": iid,
        "token_id": "T",
        "metric_key": KEYS[venue],
        "ts": iso(at),
        "ts_prec": "EXACT",
        "timestamp_precision": "EXACT",
        "received_ts": iso(at),
        "raw_value": native,
        "raw_unit": "BASE_QTY",
        "sidedness": "SINGLE_SIDED",
        "oi_native": native,
        "oi_usd": usd,
        "mark_price": mark,
        "index_price": index,
    } | changes


def ticker(iid, at, mark):
    return {"instrument_id": iid, "token_id": "T", "ts": iso(at), "mark_price": mark}


def grid(iid, since, until=END, step=timedelta(minutes=5), level=lambda k: 100 + k % 5, usd=True, **common):
    """Observations of an instrument on a grid from `since` to `until`; the native value of the k-th one is
    level(k), its USD value ten times that."""
    rows, at, k = [], since, 0
    while at <= until:
        rows.append(
            observation(iid, at, native=str(level(k)), usd=str(10 * level(k)) if usd else None, **common)
        )
        at += step
        k += 1
    return rows


async def seed(ch, **collections):
    await chm.load_snapshot(ch, collections)


async def endpoints(ch, *instruments, windows=W1H, end=END, **changes):
    shown = snapshot(*instruments, **changes)
    return await oi_evidence(ch, shown, end, CONFIG, windows, end - 3 * DAY, history=False)


async def histories(ch, *instruments, windows=W1H, recent_start=None, **changes):
    shown = snapshot(*instruments, **changes)
    return await oi_evidence(ch, shown, END, CONFIG, windows, recent_start or END - 3 * DAY, history=True)


def endpoint(result, instrument, window="W1H"):
    return next(r for r in result.endpoints if r.window == window and r.instrument_id == instrument)


def record(result, instrument, window="W1H"):
    return next(r for r in result.history if r.window == window and r.instrument_id == instrument)


def inclusive(values):
    cuts = statistics.quantiles(values, n=40, method="inclusive")
    return cuts[0], cuts[-1]


@pytest.mark.integration
async def test_an_endpoint_is_the_nearest_observation_within_the_tolerance_the_earlier_on_a_tie(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start - timedelta(seconds=30), "100", "1000"),
            observation(BINANCE, start + timedelta(seconds=30), "200", "2000"),
            observation(BINANCE, END - timedelta(seconds=15), "110", "1100"),
            observation(BINANCE, END + timedelta(seconds=20), "999", "9990"),
            observation(BYBIT, start - timedelta(seconds=61), "50", "500"),
            observation(BYBIT, END - timedelta(seconds=60), "55", "550"),
        ],
    )
    result = await endpoints(market_clickhouse, BINANCE, BYBIT, windows={"W1H": 3600, "W15M": 900})
    first = endpoint(result, BINANCE)
    assert first.start_observed_at == start - timedelta(seconds=30)
    assert (first.oi_native_start, first.oi_usd_start) == (100, 1000)
    assert first.end_observed_at == END - timedelta(seconds=15)
    assert (first.oi_native_end, first.oi_usd_end) == (110, 1100)
    second = endpoint(result, BYBIT)
    assert second.start_observed_at is None and second.oi_native_start is None
    assert second.end_observed_at == END - timedelta(seconds=60) and second.oi_native_end == 55
    assert endpoint(result, BINANCE, "W15M").start_observed_at is None
    assert result.instruments == (BINANCE, BYBIT)


@pytest.mark.integration
async def test_an_observation_recorded_or_received_after_the_assembly_does_not_exist_for_it(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    late = END + timedelta(hours=2)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start - timedelta(seconds=40), "100", "1000"),
            observation(BINANCE, start, "900", "9000", recorded_at=iso(late)),
            observation(BINANCE, start + timedelta(seconds=10), "800", "8000", received_ts=iso(late)),
            observation(BINANCE, END, "110", "1100"),
        ],
    )
    result = await endpoints(market_clickhouse, BINANCE)
    assert endpoint(result, BINANCE).oi_native_start == 100


@pytest.mark.integration
async def test_usd_is_the_stored_value_then_the_own_mark_then_the_nearest_ticker_mark_within_the_tolerance(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start, "10", "77"),
            observation(BINANCE, END, "11", mark="4"),
            observation(BYBIT, start, "50"),
            observation(BYBIT, END, "55"),
            observation(OKX, start, "20"),
            observation(OKX, END, "22"),
        ],
        tickers=[
            ticker(BYBIT, start - timedelta(seconds=20), "2"),
            ticker(BYBIT, start + timedelta(seconds=50), "3"),
            ticker(BYBIT, END + timedelta(seconds=30), "9"),
            ticker(BYBIT, END - timedelta(seconds=90), "8"),
            ticker(OKX, start, "5"),
            ticker(OKX, END - timedelta(seconds=61), "6"),
        ],
    )
    result = await endpoints(market_clickhouse, BINANCE, BYBIT, OKX)
    stored = endpoint(result, BINANCE)
    assert stored.oi_usd_start == 77 and stored.oi_usd_end == 44 and stored.end_mark == 4
    near = endpoint(result, BYBIT)
    assert near.oi_usd_start == 100 and near.oi_usd_end is None
    partial = endpoint(result, OKX)
    assert partial.oi_usd_start == 100 and partial.oi_usd_end is None


@pytest.mark.integration
async def test_an_observation_with_an_unresolved_bucket_edge_is_no_endpoint_and_is_counted(market_clickhouse):
    start = END - timedelta(hours=1)
    unresolved = dict(
        ts_prec="BUCKET(300s)",
        timestamp_precision="BUCKET(300s)",
        ts_bucket_size_s=300,
        ts_bucket_anchor=None,
    )
    await seed(
        market_clickhouse,
        oi=[
            observation(BYBIT, start - timedelta(seconds=20), "50", "500", **unresolved),
            observation(BYBIT, start - timedelta(seconds=50), "49", "490", **unresolved),
            observation(BYBIT, END, "55", "550", **unresolved | dict(ts_bucket_anchor="END")),
            observation(BINANCE, start, "10", "100", **unresolved | dict(ts_bucket_anchor="UNSPECIFIED")),
            observation(BINANCE, END, "11", "110"),
        ],
    )
    result = await endpoints(market_clickhouse, BYBIT, BINANCE)
    shown = endpoint(result, BYBIT)
    assert shown.start_observed_at is None and shown.oi_native_start is None
    assert shown.excluded == 2 and shown.excluded_at == start - timedelta(seconds=20)
    assert (shown.excluded_oi_native, shown.excluded_oi_usd) == (50, 500)
    assert shown.excluded_ts_prec == "BUCKET(300s)" and shown.excluded_ts_bucket_anchor == "UNSPECIFIED"
    assert shown.excluded_ts_bucket_size_s == 300
    assert shown.end_observed_at == END and shown.end_ts_prec == "BUCKET(300s)"
    assert shown.end_ts_bucket_size_s == 300
    declared = endpoint(result, BINANCE)
    assert declared.excluded == 1 and declared.start_observed_at is None
    assert declared.excluded_ts_bucket_anchor == "UNSPECIFIED"


@pytest.mark.integration
async def test_the_composite_sums_the_candidate_venues_observed_at_both_bounds(market_clickhouse):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start, "100", "1000"),
            observation(BINANCE, END, "110", "1100"),
            observation(BYBIT, start, "50"),
            observation(BYBIT, END, "55"),
            observation(HYPERLIQUID, start, "10", "100"),
            observation(HYPERLIQUID, END, "11", "110"),
            observation(OKX, END, "7", "70"),
        ],
    )
    result = await endpoints(market_clickhouse, BINANCE, BYBIT, HYPERLIQUID, OKX)
    composite = endpoint(result, "composite")
    assert composite.series == 0
    assert composite.cohort == f"{BINANCE},{BYBIT}"
    assert (composite.oi_native_start, composite.oi_native_end) == (150, 165)
    assert composite.start_observed_at == start and composite.end_observed_at == END
    assert (composite.oi_usd_start, composite.oi_usd_end) == (None, None)
    assert endpoint(result, HYPERLIQUID).oi_native_end == 11
    assert [r.series for r in result.endpoints if r.window == "W1H"] == [0, 1, 2, 3, 4]


@pytest.mark.integration
async def test_the_composite_has_usd_only_when_every_member_has_it(market_clickhouse):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start, "100", "1000"),
            observation(BINANCE, END, "110", "1100"),
            observation(BYBIT, start, "50", "500"),
            observation(BYBIT, END, "55", "550"),
        ],
    )
    composite = endpoint(await endpoints(market_clickhouse, BINANCE, BYBIT), "composite")
    assert (composite.oi_usd_start, composite.oi_usd_end) == (1500, 1650)


@pytest.mark.integration
async def test_the_composite_is_taken_at_the_time_nearest_the_bound_where_every_member_observes(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start - timedelta(seconds=50), "100"),
            observation(BINANCE, start + timedelta(seconds=55), "101"),
            observation(BYBIT, start + timedelta(seconds=58), "10"),
            observation(BINANCE, END, "200"),
            observation(BYBIT, END, "20"),
        ],
    )
    composite = endpoint(await endpoints(market_clickhouse, BINANCE, BYBIT), "composite")
    assert composite.start_observed_at == start + timedelta(seconds=55)
    assert composite.oi_native_start == 111
    assert composite.oi_native_end == 220


@pytest.mark.integration
async def test_a_composite_needs_a_candidate_with_both_bounds_and_no_cohort_has_no_composite(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[observation(HYPERLIQUID, start, "10"), observation(HYPERLIQUID, END, "11")],
    )
    result = await endpoints(market_clickhouse, HYPERLIQUID)
    composite = endpoint(result, "composite")
    assert (
        composite.cohort == "" and composite.start_observed_at is None and composite.end_observed_at is None
    )
    assert endpoint(result, HYPERLIQUID).oi_native_end == 11


@pytest.mark.integration
async def test_the_basis_is_that_of_the_latest_observation_and_of_the_one_nearest_the_window_start(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start - timedelta(seconds=40), "100", mark="101", index="100"),
            observation(BINANCE, start - timedelta(seconds=10), "100", mark="105"),
            observation(BINANCE, start + timedelta(seconds=20), "100", mark="102", index="100"),
            observation(BINANCE, END - timedelta(minutes=3), "110", mark="103", index="100"),
            observation(BYBIT, start, "50", mark="100", index="0"),
            observation(BYBIT, END, "55", mark="100", index="100"),
            observation(OKX, start, "20", mark="100", index="100"),
            observation(OKX, END - timedelta(minutes=3), "22"),
        ],
    )
    result = await endpoints(market_clickhouse, BINANCE, BYBIT, OKX)
    binance = endpoint(result, BINANCE)
    assert binance.basis_start_bps == pytest.approx(200)
    assert binance.basis_bps == pytest.approx(300)
    bybit = endpoint(result, BYBIT)
    assert bybit.basis_start_bps is None and bybit.basis_bps == pytest.approx(0)
    assert endpoint(result, OKX).basis_bps is None


@pytest.mark.integration
async def test_the_end_observation_carries_its_declared_precision(market_clickhouse):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start, "100"),
            observation(
                BINANCE,
                END,
                "110",
                ts_prec="BUCKET(300s)",
                timestamp_precision="BUCKET(300s)",
                ts_bucket_size_s=300,
                ts_bucket_anchor="END",
            ),
            observation(BYBIT, start, "50"),
            observation(BYBIT, END, "55"),
        ],
    )
    result = await endpoints(market_clickhouse, BINANCE, BYBIT)
    assert endpoint(result, BINANCE).end_ts_prec == "BUCKET(300s)"
    assert endpoint(result, BINANCE).end_timestamp_precision == "BUCKET(300s)"
    assert endpoint(result, BINANCE).end_ts_bucket_size_s == 300
    assert (
        endpoint(result, BYBIT).end_ts_prec == "EXACT"
        and endpoint(result, BYBIT).end_ts_bucket_size_s is None
    )
    assert endpoint(result, BINANCE).start_ts_prec == "EXACT"
    assert endpoint(result, BINANCE).start_timestamp_precision == "EXACT"
    assert endpoint(result, BINANCE).start_ts_bucket_size_s is None
    assert endpoint(result, BYBIT).start_ts_prec == "EXACT"
    stats = truth.oi_stats(endpoint(result, BINANCE), None, timedelta(hours=1))
    assert stats["precision"] == "BUCKET(300s)"
    assert stats["start_observed_at_prec"] == "EXACT" and stats["end_observed_at_prec"] == "BUCKET(300s)"
    assert stats["oi_native_chg_pct"] == pytest.approx(0.1)


STEPS = 12


def changes(levels, steps=STEPS):
    """The change of each level against the one `steps` before it."""
    return [levels[k] / levels[k - steps] - 1 for k in range(steps, len(levels))]


def levels_of(rows):
    return [float(row["oi_native"]) for row in rows]


@pytest.mark.integration
async def test_the_history_range_is_the_inclusive_quantiles_of_the_changes_before_the_window_start(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    rows = grid(BINANCE, END - SPAN, END, level=lambda k: 100 + (k * k) % 7)
    await seed(market_clickhouse, oi=rows)
    result = await histories(market_clickhouse, BINANCE)
    shown = record(result, BINANCE)
    before = [r for r in rows if parse_time(r["ts"]) < start]
    native = changes(levels_of(before))
    usd = changes([10 * level for level in levels_of(before)])
    assert shown.native_samples == len(native) and shown.usd_samples == len(usd)
    assert (shown.native_low, shown.native_high) == pytest.approx(inclusive(native), abs=1e-12)
    assert (shown.usd_low, shown.usd_high) == pytest.approx(inclusive(usd), abs=1e-12)
    assert shown.observations == len(before)
    assert shown.first_observation == END - SPAN
    assert shown.last_observation == start - timedelta(minutes=5)
    assert shown.native_first_end == END - SPAN + timedelta(hours=1)
    assert shown.native_last_end == start - timedelta(minutes=5)
    assert shown.pair_tolerance_s == 150


@pytest.mark.integration
async def test_the_pairing_tolerance_is_half_the_median_gap_and_at_least_the_sync_tolerance(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        oi=grid(BINANCE, END - SPAN, END, step=timedelta(minutes=10))
        + grid(BYBIT, END - timedelta(hours=2), END, step=MINUTE),
    )
    result = await histories(market_clickhouse, BINANCE, BYBIT)
    assert record(result, BINANCE).pair_tolerance_s == 300
    assert record(result, BYBIT).pair_tolerance_s == 60


@pytest.mark.integration
async def test_a_window_with_no_pair_within_the_tolerance_has_no_range(market_clickhouse):
    await seed(market_clickhouse, oi=grid(BINANCE, END - SPAN, END))
    result = await histories(market_clickhouse, BINANCE, windows={"W1H": 3600, "LONG": 41400})
    long = record(result, BINANCE, "LONG")
    assert long.observations == 6 and long.native_samples == 0
    assert long.native_low is None and long.native_first_end is None
    assert record(result, BINANCE).native_samples > 0


@pytest.mark.integration
async def test_the_history_is_sampled_every_300_seconds_before_the_recent_start_and_whole_after_it(
    market_clickhouse,
):
    start = END - timedelta(hours=1)
    recent_start = END - timedelta(minutes=90)
    rows = grid(BINANCE, END - timedelta(hours=3), END, step=MINUTE, level=lambda k: 100 + k % 3)
    await seed(market_clickhouse, oi=rows)
    result = await histories(market_clickhouse, BINANCE, recent_start=recent_start)
    shown = record(result, BINANCE)
    old = [r for r in rows if parse_time(r["ts"]) < recent_start]
    buckets = {int(parse_time(r["ts"]).timestamp()) // 300 for r in old}
    recent = [r for r in rows if recent_start <= parse_time(r["ts"]) < start]
    assert shown.observations == len(buckets) + len(recent)
    assert shown.pair_tolerance_s == 60


@pytest.mark.integration
async def test_an_observation_recorded_after_the_assembly_is_no_part_of_the_history(market_clickhouse):
    rows = grid(BINANCE, END - SPAN, END, level=lambda k: 100 + k % 4)
    late = [
        observation(
            BINANCE,
            END - 5 * timedelta(hours=1) + timedelta(seconds=30 * k + 7),
            "5000",
            recorded_at=iso(END + DAY),
        )
        for k in range(50)
    ]
    await seed(market_clickhouse, oi=rows + late)
    shown = record(await histories(market_clickhouse, BINANCE), BINANCE)
    assert shown.observations == len([r for r in rows if parse_time(r["ts"]) < END - timedelta(hours=1)])
    assert shown.pair_tolerance_s == 150


@pytest.mark.integration
async def test_the_composite_history_counts_each_past_window_over_the_venues_seen_at_both_bounds(
    market_clickhouse,
):
    """03 §0/§5.4: OKX joined later than Binance and Bybit; a venue joining is never counted as a change."""
    start = END - timedelta(hours=1)
    series = {
        BINANCE: grid(BINANCE, END - SPAN, END, level=lambda k: 100 + k % 5),
        BYBIT: grid(BYBIT, END - SPAN, END, level=lambda k: 50 + k % 7, usd=False),
        OKX: grid(OKX, END - timedelta(hours=4), END, level=lambda k: 30 + k % 11),
    }
    await seed(market_clickhouse, oi=[row for rows in series.values() for row in rows])
    result = await histories(market_clickhouse, BINANCE, BYBIT, OKX)
    shown = record(result, "composite")
    assert shown.cohort == f"{BINANCE},{BYBIT},{OKX}"
    by_time = {
        iid: {
            parse_time(r["ts"]): (float(r["oi_native"]), float(r["oi_usd"]) if r["oi_usd"] else None)
            for r in rows
        }
        for iid, rows in series.items()
    }
    times = sorted({t for values in by_time.values() for t in values if t < start})
    native, usd = [], []
    for t in times:
        old = t - timedelta(hours=1)
        pairs = [(v[old], v[t]) for v in by_time.values() if old in v and t in v]
        if pairs:
            native.append(sum(n for _, (n, _) in pairs) / sum(o for (o, _), _ in pairs) - 1)
        both = [(o[1], n[1]) for o, n in pairs if o[1] is not None and n[1] is not None]
        if both:
            usd.append(sum(n for _, n in both) / sum(o for o, _ in both) - 1)
    assert native != usd
    assert shown.native_samples == len(native) and shown.usd_samples == len(usd)
    assert (shown.native_low, shown.native_high) == pytest.approx(inclusive(native), abs=1e-12)
    assert (shown.usd_low, shown.usd_high) == pytest.approx(inclusive(usd), abs=1e-12)
    assert shown.first_observation == END - SPAN and shown.last_observation == start - timedelta(minutes=5)


@pytest.mark.integration
async def test_the_basis_history_is_the_range_of_mark_over_index_before_the_window_start(market_clickhouse):
    start = END - timedelta(hours=1)
    rows = grid(
        BINANCE,
        END - SPAN,
        END,
        usd=False,
        mark="101",
        index="100",
        level=lambda k: 100,
    )
    for k, row in enumerate(rows):
        row["mark_price"] = str(100 + k % 5)
    await seed(market_clickhouse, oi=rows)
    shown = record(await histories(market_clickhouse, BINANCE), BINANCE)
    before = [(float(r["mark_price"]) / 100 - 1) * 10000 for r in rows if parse_time(r["ts"]) < start]
    assert shown.basis_samples == len(before)
    assert (shown.basis_low, shown.basis_high) == pytest.approx(inclusive(before), abs=1e-9)
    assert shown.basis_first == END - SPAN and shown.basis_last == start - timedelta(minutes=5)


@pytest.mark.integration
async def test_a_composite_without_candidates_has_no_history_record(market_clickhouse):
    await seed(market_clickhouse, oi=grid(HYPERLIQUID, END - SPAN, END))
    result = await histories(market_clickhouse, HYPERLIQUID)
    assert [r.instrument_id for r in result.history] == [HYPERLIQUID]
    assert truth.oi_normal_ranges(None, timedelta(hours=1)) == (None, None)


@pytest.mark.integration
async def test_the_assessment_reads_every_window_and_its_records_give_the_rules_their_stats(
    market_clickhouse,
):
    rows = [
        row for iid in (BINANCE, BYBIT) for row in grid(iid, END - SPAN, END, level=lambda k: 100 + k % 3)
    ]
    await seed(market_clickhouse, oi=rows)
    windows = dict(W15M=900, W1H=3600, W4H=14400, W24H=86400, EVENT=79500)
    result = await histories(market_clickhouse, BINANCE, BYBIT, windows=windows)
    assert {r.window for r in result.history} == {"W15M", "W1H", "W4H"}
    assert {r.window for r in result.endpoints} == set(windows)
    stats = truth.oi_synchronized(
        dict(oi_sql=result),
        "W1H",
        timedelta(hours=1),
        [(BINANCE, "binance"), (BYBIT, "bybit")],
    )
    assert stats["venues"] == ["binance", "bybit"]
    assert (stats["oi_native_start"], stats["oi_native_end"]) == (200, 200)
    assert stats["native_normal_range"] is None and stats["oi_class"] is None


UNRESOLVED = dict(
    ts_prec="BUCKET(300s)", timestamp_precision="BUCKET(300s)", ts_bucket_size_s=300, ts_bucket_anchor=None
)


def scoped(result, *instruments):
    return dict(
        token_id="T", instruments=snapshot(*instruments)["instruments"], oi_sql=result, _primary="W1H"
    )


@pytest.mark.integration
async def test_a_provider_bucket_with_an_unknown_edge_is_supplemental_only(market_clickhouse):
    """TDD §0/2.5: verified native units cannot resolve an undocumented bucket edge."""
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BYBIT, at, "1000", mark="100", **UNRESOLVED)
            for at in (start, start + timedelta(minutes=5), END - timedelta(minutes=5), END)
        ],
    )
    result = await endpoints(market_clickhouse, BYBIT)
    derived = truth.derivatives_truth(scoped(result, BYBIT), start, END)
    assert derived["synchronized"] is None
    venue = derived["per_venue"][0]
    assert venue["oi_native_chg_pct"] is None and venue["reason"] == "INCOMPLETE"
    observed = venue["oi_temporal_exclusions"]["latest_observation"]
    assert observed["oi_native"] == 1000 and observed["ts"] == iso(END)
    assert observed["ts_bucket_anchor"] == "UNSPECIFIED"
    assert venue["oi_temporal_exclusions"]["count"] == 2
    assert observed["scope"] == "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"


@pytest.mark.integration
async def test_observations_with_an_unknown_bucket_edge_cannot_train_a_normal_range(market_clickhouse):
    start = END - timedelta(hours=1)
    rows = grid(BYBIT, END - SPAN, END, level=lambda k: 100 + k % 5, **UNRESOLVED)
    for row in rows:
        if parse_time(row["ts"]) in (start, END):
            row.update(ts_bucket_anchor="END")
    await seed(market_clickhouse, oi=rows)
    result = await histories(market_clickhouse, BYBIT)
    assert not result.history
    assert endpoint(result, BYBIT).oi_native_end == float(rows[-1]["oi_native"])
    stats = truth.derivatives_truth(scoped(result, BYBIT), start, END)["per_venue"][0]
    assert stats["native_normal_range"] is None and stats["native_oi_vs_normal"] is None


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["END", "START", "EXACT", "ABSENT"])
async def test_known_edges_and_exact_or_absent_clocks_keep_the_requested_endpoints(market_clickhouse, kind):
    start = END - timedelta(hours=1)
    common = (
        dict(UNRESOLVED, ts_bucket_anchor=kind)
        if kind in ("END", "START")
        else {}
        if kind == "ABSENT"
        else dict(ts_prec="EXACT", timestamp_precision="EXACT", ts_bucket_size_s=None)
    )
    await seed(
        market_clickhouse,
        oi=[
            observation(BYBIT, start, "1000", "100000", **common),
            observation(BYBIT, END, "1000", "100000", **common),
        ],
    )
    result = await endpoints(market_clickhouse, BYBIT)
    composite = endpoint(result, "composite")
    assert composite.cohort == BYBIT
    assert composite.start_observed_at == start and composite.end_observed_at == END
    assert composite.oi_native_start == composite.oi_native_end == 1000
    assert endpoint(result, BYBIT).excluded == 0


@pytest.mark.integration
async def test_an_unresolved_venue_does_not_poison_the_known_composite_cohort(market_clickhouse):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(BINANCE, start, "1000", "100000", ts_prec="EXACT"),
            observation(BINANCE, END, "1000", "100000", ts_prec="EXACT"),
            observation(BYBIT, start, "1000", "100000", **UNRESOLVED),
            observation(BYBIT, END, "1000", "100000", **UNRESOLVED),
        ],
    )
    result = await endpoints(market_clickhouse, BINANCE, BYBIT)
    composite = endpoint(result, "composite")
    assert composite.cohort == BINANCE and composite.oi_native_end == 1000
    derived = truth.derivatives_truth(scoped(result, BINANCE, BYBIT), start, END)
    assert derived["synchronized"]["venues"] == ["binance"]


@pytest.mark.integration
async def test_hyperliquid_has_a_per_venue_change_and_never_enters_the_composite(market_clickhouse):
    start = END - timedelta(hours=1)
    await seed(
        market_clickhouse,
        oi=[
            observation(HYPERLIQUID, start, "1000", sidedness="UNKNOWN", synchronized_eligible=True),
            observation(HYPERLIQUID, END, "1100", sidedness="UNKNOWN", synchronized_eligible=True),
        ],
    )
    result = await endpoints(market_clickhouse, HYPERLIQUID)
    derived = truth.derivatives_truth(scoped(result, HYPERLIQUID), start, END)
    assert derived["per_venue"][0]["oi_native_chg_pct"] == pytest.approx(0.1)
    assert derived["synchronized"] is None


@pytest.mark.integration
async def test_retained_f11_binance_and_bybit_endpoints_make_the_composite(market_clickhouse):
    import hashlib
    import json
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "fixture_check/data"
    binance_file, bybit_file = (
        source / "rest_openInterestHist_fixtures.json",
        source / "bybit_oi_5m_WLDUSDT.json",
    )
    assert (
        hashlib.sha256(binance_file.read_bytes()).hexdigest()
        == "67e4afd8ee1e43e348344668a933b66b3e2490e02bd62b1ce5244d8205a88f45"
    )
    binance_raw = json.loads(binance_file.read_text())["data"]["F-11:WLDUSDT:5m"]
    bybit_raw = json.loads(bybit_file.read_text())
    end = parse_time("2026-10-16T12:30:00Z")
    rows = []
    for venue, raw in [("binance", binance_raw), ("bybit", bybit_raw)]:
        inst = {
            "instrument_id": venue + ":perp_linear:WLDUSDT",
            "token_id": "world:WLD",
            "venue": venue,
            "market_type": "PERP_LINEAR",
            "venue_symbol": "WLDUSDT",
            "quote_asset": "USD",
        }
        for r in raw:
            if parse_time(r["timestamp"]) <= end:
                row = normalize.oi(inst, r, bucket_size_s=300)
                rows.append(row | {"instrument_id": inst["instrument_id"], "received_ts": row["ts"]})
    await seed(market_clickhouse, oi=rows)
    ids = ["binance:perp_linear:WLDUSDT", "bybit:perp_linear:WLDUSDT"]
    shown = snapshot(*ids)
    result = await oi_evidence(
        market_clickhouse, shown, end, CONFIG, {"EVENT": 14400}, end - 20 * DAY, history=False
    )
    composite = endpoint(result, "composite", "EVENT")
    assert composite.cohort == ",".join(ids)
    assert composite.oi_native_start == pytest.approx(183464692 + 83937472.5)
    assert composite.oi_native_end == pytest.approx(202487919 + 86777506)
    change = composite.oi_native_end / composite.oi_native_start - 1
    assert 0.0768 <= change <= 0.0868


def test_the_statements_take_exactly_the_parameters_the_assessment_sends():
    kinds = {name: truth_sql.load_statement(name)[1] for name in ("oi_endpoints", "oi_history")}
    shared = {"cutoff", "assembled", "iids", "cand", "names", "lens_us", "sync_tol_s"}
    assert set(kinds["oi_endpoints"]) == shared
    assert set(kinds["oi_history"]) == shared | {"recent_start", "lookback_s"}


class Recorder:
    def __init__(self):
        self.sent = []

    async def rows(self, query, params=None):
        self.sent.append((query, params))
        return []


def shown_snapshot(anchor=None):
    return snapshot(BINANCE, BYBIT, HYPERLIQUID, anchor=anchor)


@pytest.mark.asyncio
async def test_the_assessment_sends_every_window_with_the_history_and_a_condition_what_it_needs():
    recorder = Recorder()
    repo = SimpleNamespace(clickhouse=recorder)
    event = {"type": "T0", "ts": iso(END - timedelta(seconds=79500))}
    await truth.with_oi(repo, shown_snapshot(event), CONFIG)
    assert {query.split("-- ")[1].split(":")[0] for query, _ in recorder.sent} == {
        "oi_endpoints",
        "oi_history",
    }
    assert len(recorder.sent) == 2
    for _, params in recorder.sent:
        assert params["names"][0] == "['W15M','W1H','W4H','W24H','EVENT']"
        assert params["lens_us"][0] == "['900000000','3600000000','14400000000','86400000000','79500000000']"
        assert params["iids"][0] == f"['{BINANCE}','{BYBIT}','{HYPERLIQUID}']"
        assert params["cand"][0] == "['1','1','0']"
        assert params["sync_tol_s"][0] == "60"
    history = next(p for q, p in recorder.sent if "recent_start" in p)
    assert history["recent_start"][0] == "2026-10-14 11:59:00.000000"
    assert history["lookback_s"][0] == str(12 * 86400)
    recorder.sent.clear()
    await truth.with_oi(repo, shown_snapshot(event), CONFIG, windows={"W1H": 3600}, history=False)
    assert len(recorder.sent) == 1 and recorder.sent[0][1]["names"][0] == "['W1H']"
    recorder.sent.clear()
    far = {"type": "T0", "ts": iso(END - timedelta(days=5))}
    await truth.with_oi(repo, shown_snapshot(far), CONFIG)
    history = next(p for q, p in recorder.sent if "recent_start" in p)
    assert history["recent_start"][0] == iso(END - timedelta(days=10, minutes=1)).replace("T", " ").replace(
        "Z", ".000000"
    )


@pytest.mark.asyncio
async def test_a_token_without_perp_instruments_sends_nothing():
    recorder = Recorder()
    repo = SimpleNamespace(clickhouse=recorder)
    spot = {
        "token_id": "T",
        "cutoff": iso(END),
        "anchor": {"type": "T0", "ts": iso(END - timedelta(hours=1))},
        "instruments": [{"instrument_id": "A:spot:T", "token_id": "T", "venue": "A", "market_type": "SPOT"}],
    }
    shown = await truth.with_oi(repo, spot, CONFIG)
    assert shown["oi_sql"] == truth_sql.NO_OI and recorder.sent == []


def condition_windows(requirements, anchor_age=timedelta(hours=3)):
    shown = dict(cutoff=iso(END), anchor={"type": "T0", "ts": iso(END - anchor_age)})
    return truth.condition_oi_windows(shown, requirements)


def test_a_condition_reads_the_requested_window_of_an_oi_metric_and_the_primary_window_of_a_state():
    assert condition_windows([{"metric": "deriv.oi_class", "window": "W4H"}]) == ({"W4H": 14400}, True)
    assert condition_windows(
        [
            {"metric": "deriv.native_oi_chg_pct", "window": "EVENT"},
            {"metric": "deriv.oi_class", "window": "W1H"},
        ]
    ) == ({"EVENT": 10800, "W1H": 3600}, True)
    assert condition_windows([{"metric": "mt.move_class", "window": "W1H"}]) == ({"EVENT": 10800}, True)
    assert condition_windows([{"metric": "dc.class", "window": "W1H"}]) == ({"EVENT": 10800}, False)
    assert condition_windows([{"metric": "dc.class", "window": "W1H"}], timedelta(days=3)) == (
        {"W4H": 14400},
        False,
    )
    assert condition_windows([{"metric": "deriv.funding_vs_normal", "window": "W1H"}]) == ({}, False)
    assert condition_windows([{"metric": "spot.cohort.direction", "window": "W1H"}]) == ({}, False)


@pytest.mark.asyncio
async def test_a_condition_evaluation_reads_the_oi_windows_its_metrics_need(monkeypatch):
    seen = []

    async def attach(repo, shown, config, **kwargs):
        seen.append(kwargs)
        return shown

    async def passthrough(repo, shown, config, **kwargs):
        return shown

    monkeypatch.setattr(truth, "with_oi", attach)
    for name in ("with_flow", "with_series", "with_supply"):
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
                "anchor": {"type": "T0", "ts": iso(END - timedelta(hours=3))},
            }

    service = truth.MarketTruthService(
        Repo(), SimpleNamespace(config_version="test", code_version="test"), Market()
    )
    await service.condition_values(None, "T", iso(END), [{"metric": "deriv.oi_class", "window": "W4H"}])
    assert seen == [dict(windows={"W4H": 14400}, history=True)]
    seen.clear()
    await service.condition_values(None, "T", iso(END), [{"metric": "dc.class", "window": "W1H"}])
    assert seen == [dict(windows={"EVENT": 10800}, history=False)]
    seen.clear()
    await service.condition_values(
        None, "T", iso(END), [{"metric": "spot.cohort.direction", "window": "W1H"}]
    )
    assert seen == []


def test_an_event_window_without_an_anchor_is_not_read_for_an_oi_metric():
    shown = dict(cutoff=iso(END), anchor=None)
    assert truth.condition_oi_windows(shown, [{"metric": "deriv.oi_class", "window": "EVENT"}]) == ({}, False)


@pytest.mark.parametrize(
    "venue,sidedness,expected",
    [
        ("binance", "SINGLE_SIDED", 1),
        ("okx", "SINGLE_SIDED", 1),
        ("bybit", "SINGLE_SIDED", 1),
        ("hyperliquid", "SINGLE_SIDED", 0),
        ("binance", "UNKNOWN", 0),
    ],
)
def test_only_single_sided_binance_okx_and_bybit_instruments_are_composite_candidates(
    venue, sidedness, expected
):
    iid = f"{venue}:perp_linear:T"
    shown = snapshot(iid)
    shown["semantics"][0]["sidedness"] = sidedness
    assert truth.oi_candidates(shown["instruments"], shown["semantics"]) == [expected]


def test_candidates_must_share_one_aggregation_class_and_have_a_definition():
    shown = snapshot(BINANCE, BYBIT, OKX)
    assert truth.oi_candidates(shown["instruments"], shown["semantics"]) == [1, 1, 1]
    shown["semantics"][1]["aggregation_class"] = "other"
    assert truth.oi_candidates(shown["instruments"], shown["semantics"]) == [0, 0, 0]
    shown = snapshot(BINANCE, BYBIT)
    shown["semantics"][0]["aggregation_class"] = None
    assert truth.oi_candidates(shown["instruments"], shown["semantics"]) == [0, 0]
    assert truth.oi_candidates(snapshot(BINANCE)["instruments"], []) == [0]


def rules(**changes):
    return stand_in(END, [BINANCE], **changes)


def stats_of(shown, window="W4H", length=timedelta(hours=4), instrument=BINANCE):
    endpoints_, histories_ = truth.oi_records(dict(oi_sql=shown), window)
    return truth.oi_stats(endpoints_[instrument], histories_.get(instrument), length)


def test_a_change_beyond_the_history_range_is_classified_with_its_decomposition():
    stats = stats_of(rules())
    assert stats["oi_native_chg_pct"] == pytest.approx(0.08)
    assert stats["oi_usd_chg_pct"] == pytest.approx(0.08)
    assert stats["native_normal_range"] == [-0.03, 0.035] and stats["native_oi_vs_normal"] == "ABOVE"
    assert stats["native_beyond"] is True and stats["usd_beyond"] is True
    assert stats["decomposition"]["pos_share"] == pytest.approx(0.08 / 0.08)
    assert stats["oi_class"] == "REAL_NATIVE" and stats["classification_ready"] is True
    assert stats["requested_window_duration_s"] == 14400 == stats["observed_window_duration_s"]


def test_the_observed_duration_is_that_of_the_endpoints_and_the_requested_one_that_of_the_window():
    shown = rules(venue={None: dict(start_observed_at=END - timedelta(hours=4) + timedelta(seconds=17))})
    stats = stats_of(shown, length=timedelta(seconds=14383))
    assert stats["observed_window_duration_s"] == 14383
    assert stats["requested_window_duration_s"] == 14383


def test_a_price_markup_is_an_oi_change_in_usd_beyond_its_range_with_the_native_one_within():
    shown = rules(
        venue={None: dict(oi_native_end=100.5, oi_usd_end=1105.0)},
    )
    stats = stats_of(shown)
    assert stats["native_oi_vs_normal"] == "WITHIN"
    assert stats["usd_beyond"] is True and stats["oi_class"] == "PRICE_MARKUP"


def test_a_missing_usd_value_leaves_the_decomposition_and_the_class_unavailable():
    shown = rules(venue={None: dict(oi_usd_start=None, oi_usd_end=None)})
    stats = stats_of(shown)
    assert stats["oi_native_chg_pct"] == pytest.approx(0.08) and stats["oi_usd_chg_pct"] is None
    assert stats["oi_usd_reason"] == "INCOMPLETE"
    assert stats["decomposition"]["reason"] == "INCOMPLETE" and stats["decomposition"]["pos_share"] is None
    assert stats["oi_class"] is None and stats["classification_ready"] is False


def test_the_own_mark_of_the_end_observation_prices_a_zero_native_end():
    shown = rules(venue={None: dict(oi_native_end=0.0, oi_usd_end=0.0, end_mark=11.0)})
    stats = stats_of(shown)
    assert stats["oi_native_chg_pct"] == -1
    assert stats["decomposition"]["position"] is not None


@pytest.mark.parametrize(
    "changes",
    [
        dict(oi_native_start=0.0),
        dict(oi_native_start=None),
        dict(oi_native_end=-1.0),
        dict(oi_native_end=None),
        dict(start_observed_at=None),
        dict(end_observed_at=None),
    ],
)
def test_an_endpoint_without_a_positive_start_and_a_native_end_has_no_stats(changes):
    assert stats_of(rules(venue={None: changes})) is None


def test_a_history_must_reach_back_the_minimum_days_and_its_window_ends_must_span_them():
    short = dict(
        first_observation=END - timedelta(days=6, hours=23), last_observation=END - timedelta(hours=4)
    )
    assert stats_of(rules(venue_history={None: short}))["native_normal_range"] is None
    narrow = dict(
        native_first_end=END - timedelta(days=3), native_last_end=END - timedelta(days=3) + timedelta(hours=2)
    )
    stats = stats_of(rules(venue_history={None: narrow}))
    assert stats["native_normal_range"] is None and stats["usd_normal_range"] == [-0.04, 0.05]
    assert stats["native_oi_vs_normal"] is None and stats["oi_class"] is None
    enough = dict(native_first_end=END - timedelta(days=7), native_last_end=END - timedelta(hours=4))
    assert stats_of(rules(venue_history={None: enough}))["native_normal_range"] == [-0.03, 0.035]
    assert stats_of(rules(history=False))["native_normal_range"] is None


def test_the_composite_carries_the_venues_of_its_cohort_in_the_order_of_the_token():
    shown = stand_in(END, [BINANCE, BYBIT, OKX], cohort=[BINANCE, OKX])
    sync = truth.oi_synchronized(
        dict(oi_sql=shown),
        "W4H",
        timedelta(hours=4),
        [(OKX, "okx"), (BYBIT, "bybit"), (BINANCE, "binance")],
    )
    assert sync["venues"] == ["okx", "binance"]
    assert truth.oi_synchronized(dict(oi_sql=shown), "W9H", timedelta(hours=4), []) is None
    assert truth.oi_synchronized({}, "W4H", timedelta(hours=4), []) is None
    assert truth.oi_synchronized(dict(oi_sql=stand_in(END, [BINANCE])), "W4H", timedelta(hours=4), []) is None


def test_the_derivatives_of_a_window_read_the_records_of_that_window():
    shown = stand_in(
        END,
        [BINANCE, BYBIT],
        cohort=[BINANCE],
        venue={"W1H": {BINANCE: dict(oi_native_end=120.0)}},
        composite={"W1H": dict(oi_native_end=120.0)},
        venue_history={"W1H": {BYBIT: dict(native_low=-0.5, native_high=0.5)}},
    )
    scoped = dict(
        token_id="T",
        instruments=snapshot(BINANCE, BYBIT)["instruments"],
        oi_sql=shown,
        _primary="W1H",
    )
    result = truth.derivatives_truth(scoped, END - timedelta(hours=1), END)
    by_venue = {p["venue"]: p for p in result["per_venue"]}
    assert by_venue["binance"]["oi_native_chg_pct"] == pytest.approx(0.2)
    assert by_venue["bybit"]["oi_native_chg_pct"] == pytest.approx(0.08)
    assert by_venue["bybit"]["native_normal_range"] == [-0.5, 0.5]
    assert result["synchronized"]["venues"] == ["binance"]
    assert result["synchronized"]["oi_native_chg_pct"] == pytest.approx(0.2)
    other = truth.derivatives_truth(dict(scoped, _primary="W4H"), END - timedelta(hours=4), END)
    assert {p["venue"]: p["oi_native_chg_pct"] for p in other["per_venue"]} == {
        "binance": pytest.approx(0.08),
        "bybit": pytest.approx(0.08),
    }


def test_an_instrument_without_a_record_has_an_incomplete_venue_and_no_composite():
    scoped = dict(
        token_id="T",
        instruments=snapshot(BINANCE)["instruments"],
        oi_sql=truth_sql.NO_OI,
        _primary="W1H",
    )
    result = truth.derivatives_truth(scoped, END - timedelta(hours=1), END)
    assert result["synchronized"] is None
    venue = result["per_venue"][0]
    assert venue["oi_native_start"] is None and venue["reason"] == "INCOMPLETE" and venue["detail"] is None
    assert venue["basis_bps"] is None


def test_unresolved_observations_are_reported_beside_an_incomplete_venue():
    shown = rules(
        venue={
            None: dict(
                oi_native_start=None,
                excluded=3,
                excluded_at=END - timedelta(minutes=1),
                excluded_oi_native=1000.0,
                excluded_oi_usd=100000.0,
                excluded_ts_prec="BUCKET(300s)",
                excluded_timestamp_precision="BUCKET(300s)",
                excluded_ts_bucket_size_s=300.0,
                excluded_ts_bucket_anchor="UNSPECIFIED",
            )
        }
    )
    scoped = dict(token_id="T", instruments=snapshot(BINANCE)["instruments"], oi_sql=shown, _primary="W1H")
    venue = truth.derivatives_truth(scoped, END - timedelta(hours=1), END)["per_venue"][0]
    assert venue["oi_class"] is None and venue["reason"] == "INCOMPLETE"
    assert venue["detail"] == "OI_BUCKET_EDGE_UNSPECIFIED"
    exclusions = venue["oi_temporal_exclusions"]
    assert exclusions["count"] == 3 and exclusions["detail"] == "OI_BUCKET_EDGE_UNSPECIFIED"
    latest = exclusions["latest_observation"]
    assert latest["oi_native"] == 1000 and latest["ts"] == iso(END - timedelta(minutes=1))
    assert latest["ts_bucket_anchor"] == "UNSPECIFIED"
    assert latest["scope"] == "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"


def test_the_basis_rules_read_the_range_the_start_value_and_the_latest_value_of_the_record():
    def features(**changes):
        shown = rules(
            venue={None: dict(basis_bps=20.0, basis_start_bps=10.0)},
            venue_history={
                None: dict(
                    basis_samples=500,
                    basis_low=-5.0,
                    basis_high=30.0,
                    basis_first=END - timedelta(days=8),
                    basis_last=END - timedelta(days=1),
                )
                | changes
            },
        )
        scoped = dict(
            token_id="T",
            instruments=snapshot(BINANCE)["instruments"],
            oi_sql=shown,
            _primary="W1H",
        )
        deriv = truth.derivatives_truth(scoped, END - timedelta(hours=1), END)
        truth.support_features(scoped, END - timedelta(hours=1), END, {}, deriv, {}, {})
        return deriv["per_venue"][0]

    venue = features()
    assert venue["basis_normal_range"] == [-5.0, 30.0] and venue["basis_vs_normal"] == "WITHIN"
    assert venue["basis_widening"] is True
    assert features(basis_high=15.0)["basis_vs_normal"] == "ABOVE"
    assert features(basis_low=25.0, basis_high=30.0)["basis_vs_normal"] == "BELOW"
    short = features(basis_first=END - timedelta(days=6, hours=23))
    assert short["basis_normal_range"] is None and short["basis_vs_normal"] is None
    assert features(basis_samples=0, basis_low=None, basis_high=None)["basis_normal_range"] is None

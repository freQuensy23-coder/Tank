"""Funding and liquidation statements (03 §0, §5.4, §5.5) against a real ClickHouse server (CLICKHOUSE_TEST_URL):
funding.sql, funding_dispersion.sql, funding_confirmations.sql and liquidations.sql, and what the assessment and
a condition send."""

import statistics
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from tank import clickhouse_market as chm
from tank.analysis import truth, truth_sql
from tank.analysis.truth import derivatives_truth, funding_evidence
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
HOUR = timedelta(hours=1)
SETTLEMENT = timedelta(hours=8)
DAY = timedelta(days=1)
CONFIG = {"historical_lookback_days": 12, "history_min_days": 7}
BINANCE, BYBIT, HYPERLIQUID, OKX = (
    f"{venue}:perp_linear:T" for venue in ("binance", "bybit", "hyperliquid", "okx")
)
W1H = {"W1H": 3600}
SINCE = END - 3 * DAY


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
                    "market_type": "PERP_LINEAR",
                }
                for iid in instruments
            ],
        )
        | changes
    )


def row(iid, at, kind, rate, interval="8", **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "kind": kind,
        "as_of": iso(at),
        "rate": rate,
        "interval_h": interval,
        "metric_key": iid.split(":")[0] + ".funding",
        "recorded_at": iso(at),
        "received_ts": iso(at),
    } | changes


def settled(iid, at, rate, interval="8", **changes):
    return row(iid, at, "SETTLED", rate, interval, **changes)


def estimate(iid, at, rate, interval="8", **changes):
    return row(iid, at, "CURRENT_ESTIMATE", rate, interval, **changes)


def history(iid, until, count, rate=lambda k: "0.0001", interval="8", **changes):
    """`count` settlements of an instrument, one every 8 hours, the k-th (from 1) `k` settlements before `until`."""
    return [settled(iid, until - SETTLEMENT * k, rate(k), interval, **changes) for k in range(1, count + 1)]


async def seed(ch, **collections):
    await chm.load_snapshot(ch, collections)


async def funding(ch, *instruments, windows=W1H, state=False, **changes):
    shown = snapshot(*instruments, **changes)
    return await funding_evidence(ch, shown, END, CONFIG, windows, SINCE, state=state)


def venue(result, instrument, window="W1H"):
    return next(r for r in result.venues if r.window == window and r.instrument_id == instrument)


def liquidation(result, instrument, window="W1H"):
    return next(r for r in result.liquidations if r.window == window and r.instrument_id == instrument)


def same(value):
    return pytest.approx(value, rel=1e-9, abs=1e-15)


def inclusive(values):
    cuts = statistics.quantiles(values, n=40, method="inclusive")
    return cuts[0], cuts[-1]


@pytest.mark.integration
async def test_a_venue_carries_its_latest_settled_rate_and_estimate_as_stored(market_clickhouse):
    await seed(
        market_clickhouse,
        funding=[
            settled(BINANCE, END - 5 * HOUR, "0.0001", "8.0"),
            settled(BINANCE, END - HOUR, "0.0002", "8.0", as_of_prec="EXACT"),
            estimate(BINANCE, END - 20 * timedelta(minutes=1), "0.0003"),
            estimate(BINANCE, END - 10 * timedelta(minutes=1), "0.0005", "4", as_of_prec="COLLECTION_ONLY"),
            settled(BYBIT, END - HOUR, "-0.00004"),
        ],
    )
    result = await funding(market_clickhouse, BINANCE, BYBIT, OKX)
    binance = venue(result, BINANCE)
    assert binance.settled_rate == Decimal("0.0002") and binance.settled_interval_h == Decimal("8.0")
    assert binance.settled_at == END - HOUR
    assert binance.settled_eq == same(0.0002)
    assert binance.estimate_rate == Decimal("0.0005") and binance.estimate_interval_h == Decimal("4")
    assert binance.estimate_at == END - 10 * timedelta(minutes=1)
    assert binance.estimate_eq == same(0.001)
    assert (binance.settled_at_prec, binance.estimate_at_prec, binance.start_at_prec) == (
        "EXACT",
        "COLLECTION_ONLY",
        "EXACT",
    )
    bybit = venue(result, BYBIT)
    assert bybit.settled_rate == Decimal("-0.00004")
    assert bybit.estimate_rate is None and bybit.estimate_at is None and bybit.estimate_eq is None
    assert not [r for r in result.venues if r.instrument_id == OKX]


@pytest.mark.integration
async def test_the_eight_hour_equivalent_is_the_rate_times_eight_over_the_interval(market_clickhouse):
    await seed(
        market_clickhouse,
        funding=[
            settled(BINANCE, END, "0.0001", "8"),
            settled(OKX, END, "0.00005", "4"),
            settled(HYPERLIQUID, END, "0.00002", "1"),
            settled(BYBIT, END, "0.0000000000000000000000000000000000000001", "1"),
        ],
    )
    result = await funding(market_clickhouse, BINANCE, OKX, HYPERLIQUID, BYBIT)
    assert [venue(result, iid).settled_eq for iid in (BINANCE, OKX, HYPERLIQUID)] == [
        same(0.0001),
        same(0.0001),
        same(0.00016),
    ]
    assert venue(result, BYBIT).settled_eq == pytest.approx(8e-40, rel=1e-9, abs=0)


@pytest.mark.integration
async def test_a_missing_rate_or_an_interval_of_zero_has_no_equivalent(market_clickhouse):
    await seed(
        market_clickhouse,
        funding=[
            settled(BINANCE, END - HOUR, "0.0001", "8"),
            settled(BINANCE, END, None, "8"),
            settled(BYBIT, END, "0.0001", "0"),
            estimate(BYBIT, END, "0.0001", None),
        ],
    )
    result = await funding(market_clickhouse, BINANCE, BYBIT)
    binance = venue(result, BINANCE)
    assert binance.settled_rate is None and binance.settled_at == END and binance.settled_eq is None
    bybit = venue(result, BYBIT)
    assert bybit.settled_rate == Decimal("0.0001") and bybit.settled_eq is None
    assert bybit.estimate_rate == Decimal("0.0001") and bybit.estimate_eq is None


@pytest.mark.integration
async def test_rows_after_the_cutoff_or_recorded_after_the_assembly_do_not_count(market_clickhouse):
    await seed(
        market_clickhouse,
        funding=[
            settled(BINANCE, END - HOUR, "0.0001"),
            settled(BINANCE, END + timedelta(minutes=1), "0.0009"),
            settled(BINANCE, END - timedelta(minutes=10), "0.0008", recorded_at=iso(END + 2 * HOUR)),
            settled(BINANCE, END - timedelta(minutes=20), "0.0007", received_ts=iso(END + 2 * HOUR)),
            settled(BINANCE, END - timedelta(minutes=30), "0.0006", recorded_at=None, received_ts=None),
        ],
    )
    result = await funding(market_clickhouse, BINANCE)
    assert venue(result, BINANCE).settled_rate == Decimal("0.0006")
    assert venue(result, BINANCE).settled_at == END - timedelta(minutes=30)


@pytest.mark.integration
async def test_the_rate_at_the_window_start_is_the_latest_settlement_at_or_before_it(market_clickhouse):
    await seed(
        market_clickhouse,
        funding=[
            settled(BINANCE, END - 5 * HOUR, "0.0001", "8"),
            settled(BINANCE, END - 4 * HOUR, "0.0002", "4"),
            settled(BINANCE, END - 3 * HOUR, "0.0003", "2"),
            settled(BINANCE, END - HOUR, "0.0004", "1"),
            settled(BINANCE, END - 30 * timedelta(minutes=1), "0.0005", "8"),
        ],
    )
    result = await funding(market_clickhouse, BINANCE, windows={"W1H": 3600, "W4H": 14400, "W24H": 86400})
    assert (venue(result, BINANCE, "W1H").start_rate, venue(result, BINANCE, "W1H").start_at) == (
        Decimal("0.0004"),
        END - HOUR,
    )
    assert venue(result, BINANCE, "W4H").start_rate == Decimal("0.0002")
    assert venue(result, BINANCE, "W4H").start_interval_h == Decimal("4")
    assert (
        venue(result, BINANCE, "W24H").start_rate is None and venue(result, BINANCE, "W24H").start_at is None
    )
    assert venue(result, BINANCE, "W1H").settled_rate == Decimal("0.0005")


@pytest.mark.integration
async def test_the_normal_range_is_the_quantiles_of_the_settled_equivalents_before_the_window_start(
    market_clickhouse,
):
    start = END - HOUR

    def level(k):
        return f"0.0000{k % 7}"

    rows = history(BINANCE, start, 30, rate=level)
    rows += [
        settled(BINANCE, start, "0.5"),
        settled(BINANCE, start + timedelta(minutes=10), "0.5"),
        settled(BINANCE, start - 100 * SETTLEMENT, "0.5"),
        settled(BINANCE, start - timedelta(hours=1), None),
        settled(BINANCE, start - timedelta(hours=2), "0.5", "0"),
        settled(BINANCE, start - timedelta(hours=3), "0.5", "-8"),
        settled(BINANCE, start - timedelta(hours=5), "0.00009", "8", rate_8h_eq="0.00007"),
        estimate(BINANCE, start - timedelta(hours=6), "0.5"),
    ]
    await seed(market_clickhouse, funding=rows)
    result = await funding(market_clickhouse, BINANCE)
    record = venue(result, BINANCE)
    values = [float(level(k)) for k in range(1, 31)] + [0.00007]
    assert record.samples == 31
    assert (record.history_first, record.history_last) == (
        start - SETTLEMENT * 30,
        start - timedelta(hours=1) * 5,
    )
    assert (record.normal_low, record.normal_high) == same(inclusive(values))


@pytest.mark.integration
async def test_each_window_has_its_own_history(market_clickhouse):
    await seed(market_clickhouse, funding=history(BINANCE, END, 40, rate=lambda k: f"0.0001{k:02d}"))
    result = await funding(market_clickhouse, BINANCE, windows={"W1H": 3600, "W24H": 86400})
    short, long = venue(result, BINANCE, "W1H"), venue(result, BINANCE, "W24H")
    assert (short.samples, long.samples) == (36, 33)
    assert long.history_last == END - 4 * SETTLEMENT
    assert short.normal_low != long.normal_low


@pytest.mark.integration
async def test_a_history_older_than_the_look_back_is_not_read(market_clickhouse):
    await seed(market_clickhouse, funding=history(BINANCE, END, 40))
    result = await funding(market_clickhouse, BINANCE, windows={"W1H": 3600})
    assert venue(result, BINANCE).samples == 36
    assert venue(result, BINANCE).history_first == END - 36 * SETTLEMENT


@pytest.mark.integration
@pytest.mark.parametrize("count,expected", [(18, None), (22, "ABOVE")])
async def test_the_current_settlement_cannot_extend_the_history_into_a_week(
    market_clickhouse, count, expected
):
    start = parse_time("2026-10-10T00:00:00Z")
    end = start + DAY
    rows = history(BINANCE, start, count)
    rows += [settled(BINANCE, end, "0.01"), settled(BINANCE, start - 9 * DAY, None)]
    await seed(market_clickhouse, funding=rows)
    shown = snapshot(BINANCE, cutoff=iso(end), assembled_at=iso(end + HOUR))
    evidence = await funding_evidence(market_clickhouse, shown, end, CONFIG, {"EVENT": 86400}, SINCE)
    result = derivatives_truth(dict(shown, funding_sql=evidence, _primary="EVENT"), start, end)["per_venue"][
        0
    ]
    assert result["funding_vs_normal"] == expected
    assert result["funding_8h_eq"] is not None


@pytest.mark.integration
async def test_the_settled_rate_and_the_estimate_keep_their_own_intervals(market_clickhouse):
    start = END - HOUR
    await seed(
        market_clickhouse,
        funding=[
            settled(BINANCE, start, "0.001", "8"),
            estimate(BINANCE, END, "0.002", "4"),
        ],
    )
    shown = snapshot(BINANCE)
    evidence = await funding_evidence(market_clickhouse, shown, END, CONFIG, {"EVENT": 3600}, SINCE)
    result = derivatives_truth(dict(shown, funding_sql=evidence, _primary="EVENT"), start, END)["per_venue"][
        0
    ]
    assert result["funding_settled_last"] == Decimal("0.001")
    assert result["funding_settled_interval_h"] == Decimal("8")
    assert result["funding_settled_last_observed_at"] == iso(start)
    assert result["funding_estimate"] == Decimal("0.002")
    assert result["funding_estimate_interval_h"] == Decimal("4")
    assert result["funding_estimate_observed_at"] == iso(END)
    assert result["funding_settled_8h_eq"] == Decimal("0.001")
    assert result["funding_estimate_8h_eq"] == Decimal("0.004")
    assert result["funding_8h_kind"] == "CURRENT_ESTIMATE"
    assert result["interval_h"] == result["funding_estimate_interval_h"]
    assert result["funding_start"] == Decimal("0.001") and result["funding_start_interval_h"] == Decimal("8")
    assert result["funding_start_observed_at"] == iso(start)


def settlements(*columns, count=40, start=END - 12 * DAY):
    """Settlements every 8 hours of the instruments of `columns` (an id and a function of the settlement number)."""
    rows = []
    for k in range(count):
        for iid, rate in columns:
            rows.append(settled(iid, start + SETTLEMENT * k, str(rate(k))))
    return rows


async def dispersion(ch, instruments, windows=W1H, since=SINCE):
    shown = snapshot(*instruments)
    evidence = await funding_evidence(ch, shown, END, CONFIG, windows, since, state=True)
    return {r.window: r for r in evidence.dispersion}


async def dispersion_of(ch, instruments, windows=W1H):
    rows = await truth_sql.fetch(
        ch,
        truth_sql.FundingDispersion,
        iids=sorted(instruments),
        lookback_s=12 * 86400,
        names=list(windows),
        lens_us=[str(seconds * 1_000_000) for seconds in windows.values()],
        cutoff=END,
        assembled=END + HOUR,
    )
    return {r.window: r for r in rows}


@pytest.mark.integration
async def test_settlements_of_one_instant_are_taken_in_descending_instrument_id_order(market_clickhouse):
    first = END - 10 * DAY
    values = {BINANCE: [1, 3, 0], BYBIT: [2, 1, 5]}
    rows = [
        settled(iid, first + SETTLEMENT * k, f"{value / 10000}")
        for k in range(3)
        for iid, series in values.items()
        for value in [series[k]]
    ]
    await seed(market_clickhouse, funding=rows)
    result = await dispersion(market_clickhouse, [BINANCE, BYBIT])
    samples = [0.0001, 0.0, 0.0002, 0.0002, 0.0005]
    record = result["W1H"]
    assert record.samples == 5
    assert (record.history_first, record.history_last) == (first, first + 2 * SETTLEMENT)
    assert (record.normal_low, record.normal_high) == same(inclusive(samples))


@pytest.mark.integration
async def test_a_dispersion_sample_needs_every_venue_to_have_a_state(market_clickhouse):
    first = END - 10 * DAY
    rows = [settled(BYBIT, first, "0.0001"), settled(BYBIT, first + SETTLEMENT, "0.0001")]
    rows += [
        settled(BINANCE, first + 2 * SETTLEMENT, "0.0003"),
        settled(BINANCE, first + 3 * SETTLEMENT, "0.0002"),
    ]
    rows += [settled(OKX, first + 4 * SETTLEMENT, None), settled(OKX, first + 5 * SETTLEMENT, "0.0001", "0")]
    await seed(market_clickhouse, funding=rows)
    two = await dispersion(market_clickhouse, [BINANCE, BYBIT])
    assert two["W1H"].samples == 2
    assert two["W1H"].history_first == first + 2 * SETTLEMENT
    assert (two["W1H"].normal_low, two["W1H"].normal_high) == same(inclusive([0.0002, 0.0001]))
    assert await dispersion_of(market_clickhouse, [BINANCE, BYBIT, OKX]) == {}


@pytest.mark.integration
async def test_each_window_takes_the_samples_before_its_own_start(market_clickhouse):
    rows = settlements((BINANCE, lambda k: 0.0001), (BYBIT, lambda k: 0.0001 + k / 100000), count=34)
    await seed(market_clickhouse, funding=rows)
    result = await dispersion(
        market_clickhouse, [BINANCE, BYBIT], windows={"W1H": 3600, "W24H": 86400, "EVENT": 300000}
    )
    assert result["W1H"].samples > result["W24H"].samples > result["EVENT"].samples
    assert result["W1H"].history_last >= result["W24H"].history_last > result["EVENT"].history_last


@pytest.mark.integration
async def test_a_dispersion_needs_two_venues_and_other_instruments_do_not_count(market_clickhouse):
    rows = settlements((BINANCE, lambda k: 0.0001), (BYBIT, lambda k: 0.0002), (OKX, lambda k: 0.0009))
    await seed(market_clickhouse, funding=rows)
    assert await dispersion(market_clickhouse, [BINANCE]) == {}
    both = await dispersion(market_clickhouse, [BINANCE, BYBIT])
    assert both["W1H"].normal_low == same(0.0001)
    assert both["W1H"].normal_high == same(0.0001)


@pytest.mark.integration
async def test_the_dispersion_ignores_rows_recorded_after_the_assembly(market_clickhouse):
    rows = settlements((BINANCE, lambda k: 0.0001), (BYBIT, lambda k: 0.0002))
    rows += [
        settled(BYBIT, END - 2 * HOUR - SETTLEMENT * k, "0.5", recorded_at=iso(END + 2 * HOUR))
        for k in range(3)
    ]
    await seed(market_clickhouse, funding=rows)
    both = await dispersion(market_clickhouse, [BINANCE, BYBIT])
    assert both["W1H"].normal_high == same(0.0001)


def observation(source, path, at, rate, **changes):
    return {
        "capture_source": source,
        "source_path": path,
        "as_of": iso(at),
        "rate": rate,
        "interval_h": "8",
        "rate_8h_eq": rate,
        "kind": "CURRENT_ESTIMATE",
        "received_ts": iso(at),
    } | changes


def rest(iid, at, rate, path="binance.usdm:rest:funding", **changes):
    return estimate(
        iid,
        at,
        rate,
        capture_source="PUBLIC_REST",
        source_path=path,
        rate_8h_eq=rate,
        source_observations={"PUBLIC_REST": observation("PUBLIC_REST", path, at, rate)},
        **changes,
    )


def socket(iid, at, rate, path="binance.usdm:ws:funding", **changes):
    return estimate(
        iid,
        at,
        rate,
        capture_source="WEBSOCKET",
        source_path=path,
        rate_8h_eq=rate,
        source_observations={"WEBSOCKET": observation("WEBSOCKET", path, at, rate)},
        **changes,
    )


async def confirmations(ch, *instruments, since=SINCE):
    rows = await truth_sql.fetch(
        ch,
        truth_sql.FundingConfirmation,
        iids=sorted(instruments),
        since=since,
        cutoff=END,
        assembled=END + HOUR,
    )
    return {r.instrument_id: r for r in rows}


@pytest.mark.integration
async def test_a_rest_estimate_is_confirmed_by_the_nearest_websocket_one_within_a_minute(market_clickhouse):
    at = END - 10 * timedelta(minutes=1)
    second = timedelta(seconds=1)
    await seed(
        market_clickhouse,
        funding=[
            rest(BINANCE, at, "0.0001"),
            socket(BINANCE, at + 59 * second, "0.0002"),
            rest(BYBIT, at, "0.0003", "bybit:rest"),
            socket(BYBIT, at + 61 * second, "0.0004", "bybit:ws"),
            rest(OKX, at, "0.0005", "okx:rest"),
            socket(OKX, at - 30 * second, "0.0006", "okx:rest"),
        ],
    )
    found = await confirmations(market_clickhouse, BINANCE, BYBIT, OKX)
    assert set(found) == {BINANCE}
    assert found[BINANCE].rate_8h_eq == same(0.0001) and found[BINANCE].as_of == at
    assert found[BINANCE].confirmed == 1


@pytest.mark.integration
async def test_the_latest_confirmed_estimate_wins_and_an_unconfirmed_later_one_does_not_replace_it(
    market_clickhouse,
):
    at = END - 30 * timedelta(minutes=1)
    minute = timedelta(minutes=1)
    await seed(
        market_clickhouse,
        funding=[
            rest(BINANCE, at, "0.0001"),
            rest(BINANCE, at + 10 * minute, "0.0002"),
            rest(BINANCE, at + 20 * minute, "0.0003"),
            socket(BINANCE, at + 10 * minute + timedelta(seconds=5), "0.0009"),
            socket(BINANCE, at + 3 * minute, "0.0009"),
            socket(BINANCE, at + 4 * minute, "0.0009", path=""),
        ],
    )
    found = (await confirmations(market_clickhouse, BINANCE))[BINANCE]
    assert found.rate_8h_eq == same(0.0002) and found.as_of == at + 10 * minute
    assert found.confirmed == 1


@pytest.mark.integration
async def test_the_nearer_of_two_websocket_observations_confirms_and_the_earlier_one_on_a_tie(
    market_clickhouse,
):
    at = END - 10 * timedelta(minutes=1)
    second = timedelta(seconds=1)
    await seed(
        market_clickhouse,
        funding=[
            rest(BINANCE, at, "0.0001", "same"),
            socket(BINANCE, at - 10 * second, "0.0002", "same"),
            socket(BINANCE, at + 10 * second, "0.0003", "other"),
        ],
    )
    assert await confirmations(market_clickhouse, BINANCE) == {}
    await seed(market_clickhouse, funding=[socket(BINANCE, at + 2 * second, "0.0004", "other")])
    assert (await confirmations(market_clickhouse, BINANCE))[BINANCE].confirmed == 1


@pytest.mark.integration
async def test_an_estimate_is_confirmed_across_the_entries_of_one_merged_row(market_clickhouse):
    at = END - 10 * timedelta(minutes=1)
    merged = estimate(
        BINANCE,
        at,
        "0.0001",
        capture_source="WEBSOCKET",
        source_path="ws",
        source_observations={
            "PUBLIC_REST": observation("PUBLIC_REST", "rest", at, "0.0001"),
            "WEBSOCKET": observation("WEBSOCKET", "ws", at + timedelta(seconds=3), "0.0002"),
        },
    )
    await seed(market_clickhouse, funding=[merged])
    found = (await confirmations(market_clickhouse, BINANCE))[BINANCE]
    assert found.rate_8h_eq == same(0.0001) and found.confirmed == 1


@pytest.mark.integration
async def test_confirmations_read_only_what_was_visible_at_the_assembly_and_since_the_given_time(
    market_clickhouse,
):
    at = END - 10 * timedelta(minutes=1)
    late = iso(END + 2 * HOUR)
    await seed(
        market_clickhouse,
        funding=[
            rest(BINANCE, at, "0.0001"),
            socket(BINANCE, at, "0.0002"),
            rest(BYBIT, at, "0.0001", "bybit:rest", recorded_at=late),
            socket(BYBIT, at, "0.0002", "bybit:ws"),
            estimate(
                OKX,
                at,
                "0.0001",
                capture_source="PUBLIC_REST",
                source_path="okx:rest",
                source_observations={
                    "PUBLIC_REST": observation("PUBLIC_REST", "okx:rest", at, "0.0001", received_ts=late)
                },
            ),
            socket(OKX, at, "0.0002", "okx:ws"),
        ],
    )
    assert set(await confirmations(market_clickhouse, BINANCE, BYBIT, OKX)) == {BINANCE}
    assert await confirmations(market_clickhouse, BINANCE, since=at + timedelta(seconds=1)) == {}


def liquidation_row(iid, at, side, usd, native, **changes):
    return {
        "instrument_id": iid,
        "token_id": "T",
        "ts": iso(at),
        "side": side,
        "notional_usd": usd,
        "qty_native": native,
        "metric_key": iid.split(":")[0] + ".liquidation",
        "coverage": "SAMPLED",
        "recorded_at": iso(at),
        "received_ts": iso(at),
    } | changes


async def liquidations(ch, *instruments, windows=W1H, **changes):
    shown = snapshot(*instruments, **changes)
    return await funding_evidence(ch, shown, END, CONFIG, windows, SINCE, state=True)


@pytest.mark.integration
async def test_liquidations_are_summed_per_side_with_the_rows_that_have_no_value_counted(market_clickhouse):
    minute = timedelta(minutes=1)
    await seed(
        market_clickhouse,
        liquidations=[
            liquidation_row(BINANCE, END - 50 * minute, "LONG_LIQ", "100.5", "2"),
            liquidation_row(BINANCE, END - 40 * minute, "LONG_LIQ", None, "3"),
            liquidation_row(BINANCE, END - 20 * minute, "SHORT_LIQ", "700", "7"),
            liquidation_row(BYBIT, END - 10 * minute, "SHORT_LIQ", "5", None),
            liquidation_row(BYBIT, END - 5 * minute, "SHORT_LIQ", "6", "1"),
        ],
    )
    result = await liquidations(market_clickhouse, BINANCE, BYBIT, OKX)
    binance = liquidation(result, BINANCE)
    assert binance.observations == 3
    assert (binance.long_count, binance.long_usd, binance.long_usd_missing) == (2, 100.5, 1)
    assert (binance.long_native, binance.long_native_missing) == (5.0, 0)
    assert (binance.short_count, binance.short_usd, binance.short_usd_missing) == (1, 700.0, 0)
    assert (binance.total_usd, binance.total_usd_missing) == (800.5, 1)
    bybit = liquidation(result, BYBIT)
    assert (bybit.long_count, bybit.long_usd, bybit.long_native) == (0, None, None)
    assert (bybit.short_native, bybit.short_native_missing) == (1.0, 1)
    assert not [r for r in result.liquidations if r.instrument_id == OKX]


@pytest.mark.integration
async def test_the_first_half_of_a_window_is_before_its_middle(market_clickhouse):
    minute = timedelta(minutes=1)
    await seed(
        market_clickhouse,
        liquidations=[
            liquidation_row(BYBIT, END - 60 * minute, "SHORT_LIQ", "1", "1"),
            liquidation_row(BYBIT, END - 31 * minute, "LONG_LIQ", "2", "1"),
            liquidation_row(BYBIT, END - 30 * minute, "SHORT_LIQ", "4", "1"),
            liquidation_row(BYBIT, END - 1 * minute, "SHORT_LIQ", "8", "1"),
        ],
    )
    record = liquidation(await liquidations(market_clickhouse, BYBIT), BYBIT)
    assert (record.first_half_count, record.first_half_usd, record.first_half_usd_missing) == (2, 3.0, 0)
    assert (record.total_usd, record.observations) == (15.0, 4)


@pytest.mark.integration
async def test_a_window_is_closed_at_its_start_and_open_at_the_cutoff_and_each_window_has_its_own_rows(
    market_clickhouse,
):
    await seed(
        market_clickhouse,
        liquidations=[
            liquidation_row(BYBIT, END - HOUR - timedelta(seconds=1), "LONG_LIQ", "1", "1"),
            liquidation_row(BYBIT, END - HOUR, "LONG_LIQ", "2", "1"),
            liquidation_row(BYBIT, END - timedelta(seconds=1), "LONG_LIQ", "4", "1"),
            liquidation_row(BYBIT, END, "LONG_LIQ", "8", "1"),
            liquidation_row(BYBIT, END - 3 * HOUR, "LONG_LIQ", "16", "1"),
        ],
    )
    result = await liquidations(market_clickhouse, BYBIT, windows={"W1H": 3600, "W4H": 14400})
    assert liquidation(result, BYBIT, "W1H").long_usd == 6.0
    assert liquidation(result, BYBIT, "W4H").long_usd == 23.0


@pytest.mark.integration
async def test_liquidations_recorded_after_the_assembly_do_not_count(market_clickhouse):
    await seed(
        market_clickhouse,
        liquidations=[
            liquidation_row(BYBIT, END - timedelta(minutes=10), "LONG_LIQ", "1", "1"),
            liquidation_row(
                BYBIT, END - timedelta(minutes=9), "LONG_LIQ", "2", "1", recorded_at=iso(END + 2 * HOUR)
            ),
            liquidation_row(BYBIT, END - timedelta(minutes=8), "LONG_LIQ", "4", "1", recorded_at=None),
        ],
    )
    assert liquidation(await liquidations(market_clickhouse, BYBIT), BYBIT).long_usd == 5.0


@pytest.mark.integration
async def test_the_liquidation_rules_read_the_sums_of_the_statement(market_clickhouse):
    minute = timedelta(minutes=1)
    await seed(
        market_clickhouse,
        liquidations=[
            liquidation_row(BYBIT, END - 50 * minute, "SHORT_LIQ", "60", "6"),
            liquidation_row(BYBIT, END - 10 * minute, "LONG_LIQ", "40", "4"),
        ],
    )
    shown = snapshot(BYBIT, _primary="W1H")
    evidence = await funding_evidence(market_clickhouse, shown, END, CONFIG, W1H, SINCE, state=True)
    result = derivatives_truth(dict(shown, funding_sql=evidence, oi=[]), END - HOUR, END)["per_venue"][0]
    assert (result["liq_short_usd"], result["liq_short_native"], result["liq_short_count"]) == (60, 6, 1)
    assert (result["liq_long_usd"], result["liq_long_native"], result["liq_long_count"]) == (40, 4, 1)


def test_the_statements_take_exactly_the_parameters_the_assessment_sends():
    kinds = {
        name: truth_sql.load_statement(name)[1]
        for name in ("funding", "funding_dispersion", "funding_confirmations", "liquidations")
    }
    shared = {"cutoff", "assembled", "iids"}
    windowed = shared | {"names", "lens_us"}
    assert set(kinds["funding"]) == windowed | {"lookback_s"}
    assert set(kinds["funding_dispersion"]) == windowed | {"lookback_s"}
    assert set(kinds["funding_confirmations"]) == shared | {"since"}
    assert set(kinds["liquidations"]) == windowed


class Recorder:
    def __init__(self, funding_rows=()):
        self.sent = []
        self.funding_rows = funding_rows

    async def rows(self, query, params=None):
        self.sent.append((query, params))
        return list(self.funding_rows) if query.startswith("-- funding:") else []


def venue_row(iid, eq):
    return {name: None for name in truth_sql.FundingVenue.__dataclass_fields__} | dict(
        window="EVENT", instrument_id=iid, samples=0, settled_eq=eq
    )


def sent_names(recorder):
    return [query.split("-- ")[1].split(":")[0] for query, _ in recorder.sent]


def shown_snapshot(anchor=None):
    return snapshot(BINANCE, BYBIT, HYPERLIQUID, anchor=anchor)


@pytest.mark.asyncio
async def test_the_assessment_sends_every_window_and_a_condition_what_it_needs():
    recorder = Recorder([venue_row(BINANCE, 0.0001), venue_row(BYBIT, None), venue_row(HYPERLIQUID, 0.0002)])
    repo = SimpleNamespace(clickhouse=recorder)
    event = {"type": "T0", "ts": iso(END - timedelta(seconds=79500))}
    await truth.with_funding(repo, shown_snapshot(event), CONFIG)
    assert sorted(sent_names(recorder)) == [
        "funding",
        "funding_confirmations",
        "funding_dispersion",
        "liquidations",
    ]
    for query, params in recorder.sent:
        if "names" in params:
            assert params["names"][0] == "['W15M','W1H','W4H','W24H','EVENT']"
            assert (
                params["lens_us"][0] == "['900000000','3600000000','14400000000','86400000000','79500000000']"
            )
        if "iids" in params and not query.startswith(("-- funding_d", "-- funding_c")):
            assert params["iids"][0] == f"['{BINANCE}','{BYBIT}','{HYPERLIQUID}']"
    for name in ("funding_dispersion", "funding_confirmations"):
        params = next(p for q, p in recorder.sent if q.startswith("-- " + name + ":"))
        assert params["iids"][0] == f"['{BINANCE}','{HYPERLIQUID}']"
    sampled = next(p for q, p in recorder.sent if q.startswith("-- funding_confirmations:"))
    assert sampled["since"][0] == "2026-10-14 11:59:00.000000"
    recorder.sent.clear()
    await truth.with_funding(repo, shown_snapshot(event), CONFIG, windows={"W1H": 3600}, state=False)
    assert sent_names(recorder) == ["funding"] and recorder.sent[0][1]["names"][0] == "['W1H']"
    assert recorder.sent[0][1]["lookback_s"][0] == str(12 * 86400)


@pytest.mark.asyncio
async def test_no_dispersion_is_asked_of_fewer_than_two_venues_with_a_rate():
    recorder = Recorder([venue_row(BINANCE, 0.0001), venue_row(BYBIT, None)])
    repo = SimpleNamespace(clickhouse=recorder)
    event = {"type": "T0", "ts": iso(END - timedelta(hours=1))}
    await truth.with_funding(repo, shown_snapshot(event), CONFIG)
    assert sorted(sent_names(recorder)) == ["funding", "liquidations"]


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
    shown = await truth.with_funding(repo, spot, CONFIG)
    assert shown["funding_sql"] == truth_sql.NO_FUNDING and recorder.sent == []


def condition_windows(requirements, anchor_age=timedelta(hours=3)):
    shown = dict(cutoff=iso(END), anchor={"type": "T0", "ts": iso(END - anchor_age)})
    return truth.condition_funding_windows(shown, requirements)


def test_a_condition_reads_the_requested_window_of_a_funding_metric_and_the_primary_window_of_a_state():
    assert condition_windows([{"metric": "deriv.funding_vs_normal", "window": "W4H"}]) == (
        {"W4H": 14400},
        False,
    )
    assert condition_windows(
        [
            {"metric": "deriv.funding_vs_normal", "window": "EVENT"},
            {"metric": "deriv.funding_vs_normal", "window": "W1H"},
        ]
    ) == ({"EVENT": 10800, "W1H": 3600}, False)
    assert condition_windows([{"metric": "mt.move_class", "window": "W1H"}]) == ({"EVENT": 10800}, True)
    assert condition_windows(
        [
            {"metric": "deriv.funding_vs_normal", "window": "W1H"},
            {"metric": "mt.divergence_state", "window": "W1H"},
        ]
    ) == ({"W1H": 3600, "EVENT": 10800}, True)
    assert condition_windows([{"metric": "mt.move_class", "window": "W1H"}], timedelta(days=3)) == (
        {"W4H": 14400},
        True,
    )
    assert condition_windows([{"metric": "deriv.oi_class", "window": "W1H"}]) == ({}, False)
    assert condition_windows([{"metric": "dc.class", "window": "W1H"}]) == ({}, False)
    shown = dict(cutoff=iso(END), anchor=None)
    assert truth.condition_funding_windows(
        shown, [{"metric": "deriv.funding_vs_normal", "window": "EVENT"}]
    ) == (
        {},
        False,
    )


@pytest.mark.asyncio
async def test_a_condition_evaluation_reads_the_funding_windows_its_metrics_need(monkeypatch):
    seen = []

    async def attach(repo, shown, config, **kwargs):
        seen.append(kwargs)
        return shown

    async def passthrough(repo, shown, config, **kwargs):
        return shown

    monkeypatch.setattr(truth, "with_funding", attach)
    for name in (
        "with_oi",
        "with_volumes",
        "with_relative",
        "with_flow",
        "with_liquidity",
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
                "anchor": {"type": "T0", "ts": iso(END - timedelta(hours=3))},
            }

    service = truth.MarketTruthService(
        Repo(), SimpleNamespace(config_version="test", code_version="test"), Market()
    )
    await service.condition_values(
        None, "T", iso(END), [{"metric": "deriv.funding_vs_normal", "window": "W4H"}]
    )
    assert seen == [dict(windows={"W4H": 14400}, state=False)]
    seen.clear()
    await service.condition_values(None, "T", iso(END), [{"metric": "mt.move_class", "window": "W1H"}])
    assert seen == [dict(windows={"EVENT": 10800}, state=True)]
    seen.clear()
    await service.condition_values(None, "T", iso(END), [{"metric": "deriv.oi_class", "window": "W1H"}])
    assert seen == []

"""Rules over the records of the series statements: lead-lag, the ticker volume estimate and the statements an
assessment and a condition send (03 §5.3, §5.4, §5.10)."""

import asyncio
from datetime import timedelta
from decimal import Decimal

import pytest
from relative_helpers import evidence as relative_evidence
from relative_helpers import horizon, standard_horizons

from tank.analysis import truth, truth_sql
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
HOUR = timedelta(hours=1)
A, B = "A:spot:T", "B:spot:T"
P, Q = "P:perp_linear:T", "Q:perp_linear:T"
IDS = [A, B, P, Q]


def instruments(*ids):
    return [
        {
            "instrument_id": iid,
            "token_id": "T",
            "venue": iid.split(":")[0],
            "market_type": "SPOT" if ":spot:" in iid else "PERP_LINEAR",
        }
        for iid in ids
    ]


def bars(**exits):
    """Lead-lag bars of instruments (`A`, `B`, `P`, `Q`) by minute of exit after 14:00, `None` for none, `...` for an
    unusable series."""
    ids = dict(A=A, B=B, P=P, Q=Q)
    records = []
    for name, minute in exits.items():
        records.append(
            truth_sql.LeadLagBars(
                instrument_id=ids[name],
                bars=10,
                usable=int(minute is not ...),
                first_exit_at=None if minute in (None, ...) else END + timedelta(minutes=minute),
            )
        )
    return truth_sql.SeriesEvidence(lead_lag=tuple(records))


def sequence(evidence, *ids):
    return truth.lead_lag_sequence(dict(instruments=instruments(*ids), series_sql=evidence))


def test_the_market_whose_first_bar_left_the_range_a_minute_or_more_earlier_leads():
    mover, order = sequence(bars(A=0, B=3, P=2, Q=5), A, B, P, Q)
    assert mover == "SPOT"
    assert [(r["venue"], r["market"], r["t_first"]) for r in order] == [
        ("A", "SPOT", iso(END)),
        ("B", "SPOT", iso(END + timedelta(minutes=3))),
        ("P", "PERP", iso(END + timedelta(minutes=2))),
        ("Q", "PERP", iso(END + timedelta(minutes=5))),
    ]
    assert sequence(bars(A=4, B=6, P=2, Q=5), A, B, P, Q)[0] == "PERP"
    assert sequence(bars(A=3, P=3), A, P)[0] == "SIMULTANEOUS"
    assert sequence(bars(A=0, P=1), A, P)[0] == "SPOT"
    assert sequence(bars(A=1, P=0), A, P)[0] == "PERP"


def test_lead_lag_is_unknown_when_a_series_is_unusable_or_a_market_has_no_bars_or_nothing_left_the_range():
    assert sequence(bars(A=0, B=..., P=2), A, B, P)[0] == "UNKNOWN"
    assert sequence(bars(A=0, B=..., P=2), A, B, P)[1] == [
        dict(venue="A", market="SPOT", t_first=iso(END)),
        dict(venue="P", market="PERP", t_first=iso(END + timedelta(minutes=2))),
    ]
    assert sequence(bars(A=None, P=None), A, P) == ("UNKNOWN", [])
    assert sequence(bars(A=0, B=2), A, B) == (
        "UNKNOWN",
        [
            dict(venue="A", market="SPOT", t_first=iso(END)),
            dict(venue="B", market="SPOT", t_first=iso(END + timedelta(minutes=2))),
        ],
    )
    assert sequence(truth_sql.NO_SERIES, A, P) == ("UNKNOWN", [])


def test_lead_lag_reads_only_the_instruments_of_the_registry():
    mover, order = sequence(bars(A=0, P=5), A)
    assert (mover, [r["venue"] for r in order]) == ("UNKNOWN", ["A"])


def test_the_normal_range_of_a_horizon_needs_a_usable_history_and_a_sample():
    horizons = standard_horizons(END, END - 3 * HOUR, W15M=dict(history_q025=-0.01, history_q975=0.02))
    evidence = relative_evidence(horizons=horizons)
    assert truth.horizon_range(evidence, "W15M", 7) == [-0.01, 0.02]
    assert truth.horizon_range(evidence, "W15M", 400) is None
    assert truth.horizon_range(evidence, "W9X", 7) is None
    empty = relative_evidence(horizons=[horizon("W15M", sample_count=0)])
    assert truth.horizon_range(empty, "W15M", 7) is None
    flat = relative_evidence(horizons=[horizon("W15M", history_q025=0.01, history_q975=0.01)])
    assert truth.horizon_range(flat, "W15M", 7) is None
    assert truth.horizon_range(truth_sql.NO_RELATIVE, "W15M", 7) is None


def test_the_ticker_volume_of_a_venue_is_that_of_its_latest_ticker_even_when_it_has_none():
    def ticker(iid, minute, volume):
        return truth_sql.TickerVolume(
            instrument_id=iid, observed_at=END + timedelta(minutes=minute), volume_native=volume
        )

    evidence = truth_sql.SeriesEvidence(
        tickers=(
            ticker("A:spot:T", -3, Decimal("10")),
            ticker("A:perp_linear:T", -1, None),
            ticker("B:spot:T", -2, Decimal("7")),
        )
    )
    assert truth.ticker_volumes(dict(series_sql=evidence)) == {"A": None, "B": Decimal("7")}
    assert truth.ticker_volumes({}) == {}


class Statements:
    """A ClickHouse client that records the statements it is sent and answers each with no rows."""

    def __init__(self):
        self.sent = []

    async def rows(self, query, params):
        self.sent.append((query.splitlines()[0].split(":")[0].removeprefix("-- "), params))
        return []


def shown(**changes):
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + HOUR),
            anchor={"type": "T0", "ts": iso(END - 3 * HOUR)},
            instruments=instruments(*IDS)
            + [{"instrument_id": "peer:spot:X", "token_id": "X", "venue": "peer", "market_type": "SPOT"}],
        )
        | changes
    )


@pytest.mark.asyncio
async def test_the_series_statements_are_sent_for_the_instruments_of_the_token_and_the_windows_of_the_snapshot():
    ch = Statements()
    horizons = standard_horizons(END, END - 3 * HOUR, W15M=dict(history_q025=-0.01, history_q975=0.02))
    snapshot = shown(relative_sql=relative_evidence(horizons=horizons))
    evidence = await truth.series_evidence(ch, snapshot, END - 3 * HOUR, END, {})
    sent = dict(ch.sent)
    assert sorted(sent) == ["lead_lag", "series_health", "ticker_volumes"]
    ids = "[" + ",".join(f"'{iid}'" for iid in sorted(IDS)) + "]"
    for params in sent.values():
        assert params["iids"] == (ids, "Array(String)")
        assert params["cutoff"] == ("2026-10-16 12:00:00.000000", "DateTime64(6, 'UTC')")
        assert params["assembled"] == ("2026-10-16 13:00:00.000000", "DateTime64(6, 'UTC')")
    assert sent["ticker_volumes"]["near_s"] == ("900", "UInt32")
    assert sent["series_health"]["near_s"] == ("900", "UInt32")
    assert sent["series_health"]["look_s"] == (str(2 * 86400 + 60), "UInt32")
    assert sent["lead_lag"]["low"] == ("-0.01", "Float64") and sent["lead_lag"]["high"] == ("0.02", "Float64")
    assert sent["lead_lag"]["start"] == ("2026-10-16 09:00:00.000000", "DateTime64(6, 'UTC')")
    assert evidence == truth_sql.NO_SERIES


@pytest.mark.asyncio
async def test_the_lead_lag_statement_needs_a_normal_range_and_each_statement_can_be_left_out():
    ch = Statements()
    await truth.series_evidence(ch, shown(), END - 3 * HOUR, END, {})
    assert sorted(name for name, _ in ch.sent) == ["series_health", "ticker_volumes"]
    ch = Statements()
    await truth.series_evidence(ch, shown(), END - 3 * HOUR, END, {}, tickers=False, health=False)
    assert ch.sent == []
    ch = Statements()
    empty = await truth.series_evidence(ch, dict(shown(), instruments=[]), END - 3 * HOUR, END, {})
    assert ch.sent == [] and empty == truth_sql.NO_SERIES


def test_a_condition_asks_for_the_series_its_metrics_read():
    def needs(*metrics):
        return truth.condition_series_needs({}, [dict(metric=m, window="W1H") for m in metrics])

    nothing = dict(tickers=False, health=False, lead_lag=False)
    assert needs("price.close_vs_level", "liq.spread_ratio", "price.repricing_band") == nothing
    assert needs("spot.cohort.direction") == dict(tickers=True, health=False, lead_lag=False)
    assert needs("deriv.funding_vs_normal") == dict(tickers=False, health=True, lead_lag=False)
    assert needs("deriv.oi_class") == dict(tickers=False, health=True, lead_lag=False)
    assert needs("dc.class") == dict(tickers=True, health=True, lead_lag=False)
    assert needs("mt.move_class") == dict(tickers=True, health=True, lead_lag=True)
    assert needs("spot.cohort.direction", "mt.divergence_state") == dict(
        tickers=True, health=True, lead_lag=True
    )


@pytest.mark.asyncio
async def test_independent_statements_run_together_and_the_dependent_ones_after_them(monkeypatch):
    log = []
    gate = asyncio.Event()
    names = ("relative", "volumes", "oi", "funding", "flow", "supply")

    def step(name, *, wait):
        async def run(repo, snapshot, config, **kwargs):
            log.append(("start", name))
            if wait:
                await gate.wait()
            log.append(("end", name))
            return dict(snapshot, **{name + "_sql": name})

        return run

    for name in names:
        monkeypatch.setattr(truth, "with_" + name, step(name, wait=True))
    monkeypatch.setattr(truth, "with_liquidity", step("liquidity", wait=False))
    monkeypatch.setattr(truth, "with_series", step("series", wait=False))
    task = asyncio.create_task(truth.with_statements(None, dict(token_id="T"), {}))
    for _ in range(10):
        await asyncio.sleep(0)
    assert sorted(log) == sorted(("start", name) for name in names)
    gate.set()
    result = await task
    assert {k: v for k, v in result.items() if k.endswith("_sql")} == {
        name + "_sql": name for name in (*names, "liquidity", "series")
    }
    assert [event for event in log if event[1] in ("liquidity", "series")][0][0] == "start"
    assert max(log.index(("end", name)) for name in names) < log.index(("start", "liquidity"))
    assert max(log.index(("end", name)) for name in names) < log.index(("start", "series"))

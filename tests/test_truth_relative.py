"""Price-relative rules (03 §5 preamble and §5.7) over the typed records of the statements: which
instruments are sent, how the records become relative values. The statements themselves run in
tests/test_truth_relative_sql.py."""

import dataclasses
import re
from datetime import timedelta
from decimal import Decimal

import pytest
from relative_helpers import (
    evidence as make_evidence,
)
from relative_helpers import (
    horizon,
    reference,
    standard_horizons,
    start_evidence,
    trade,
    with_references,
)
from test_truth_start_fixture import compact_start_snapshot

from tank.analysis import truth, truth_sql
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
START = END - timedelta(hours=4)


def as_row(record):
    """A record as the JSON row ClickHouse returns."""
    row = dataclasses.asdict(record)
    return {
        key: iso(value)
        if hasattr(value, "isoformat")
        else str(value)
        if isinstance(value, Decimal)
        else value
        for key, value in row.items()
    }


class Server:
    """A ClickHouse client that answers each statement with the given records and keeps the requests."""

    def __init__(self, **records):
        self.records, self.sent = records, []

    async def rows(self, query, params=None):
        name = re.match(r"-- (\w+):", query).group(1)
        self.sent.append((name, {key: value for key, (value, _) in params.items()}))
        return [as_row(record) for record in self.records.get(name, [])]


def registry(counts=None, **changes):
    instruments = [
        ("A:spot:T", "T"),
        ("B:spot:T", "T"),
        ("A:spot:BTC", "BTC"),
        ("B:spot:BTC", "BTC"),
        ("A:spot:ETH", "ETH"),
        ("A:spot:P1", "P1"),
        ("A:spot:P2", "P2"),
        ("B:spot:P2", "P2"),
        ("A:spot:D1", "D1"),
        ("A:spot:D2", "D2"),
        ("A:perp_linear:T", "T"),
    ]
    sectors = {"T": "S", "P1": "S", "P2": "S", "D1": "DEX_EXECUTION", "D2": "LENDING_CREDIT"}
    return (
        dict(
            token_id="T",
            cutoff=iso(END),
            assembled_at=iso(END + timedelta(hours=1)),
            anchor={"type": "T0", "ts": iso(START)},
            instruments=[
                {
                    "instrument_id": iid,
                    "token_id": token,
                    "market_type": "PERP_LINEAR" if "perp" in iid else "SPOT",
                }
                for iid, token in instruments
            ],
            tokens=[
                {"token_id": t, "symbol": t, **({"sector": sectors[t]} if t in sectors else {})}
                for t in ("T", "BTC", "ETH", "P1", "P2", "D1", "D2")
            ],
            series_coverage_counts=counts
            or {"A:spot:T": 5, "B:spot:T": 9, "A:spot:BTC": 7, "B:spot:BTC": 7, "A:spot:ETH": 3}
            | {"A:spot:P1": 4, "A:spot:P2": 1, "B:spot:P2": 6, "A:spot:D1": 2, "A:spot:D2": 2},
        )
        | changes
    )


def sent(server, name):
    return [params for statement, params in server.sent if statement == name]


def test_statements_take_the_parameters_the_rules_send_and_records_name_their_columns():
    assert truth_sql.load_statement("reference_prices")[1] == {
        "instrument_ids": "Array(String)",
        "cutoff": "DateTime64(6, 'UTC')",
        "start": "DateTime64(6, 'UTC')",
        "assembled": "DateTime64(6, 'UTC')",
        "lookback_s": "UInt32",
    }
    assert truth_sql.load_statement("sector_returns")[1] == {
        "peer_ids": "Array(String)",
        "cutoff": "DateTime64(6, 'UTC')",
        "assembled": "DateTime64(6, 'UTC')",
        "lookback_s": "UInt32",
        "starts": "Array(String)",
        "ends": "Array(String)",
        "names": "Array(String)",
    }
    kinds = truth_sql.load_statement("relative_baselines")[1]
    assert kinds["event_raw"] == "Float64" and kinds["lens_us"] == "Array(String)"
    assert set(kinds) == {
        "ref",
        "btc",
        "eth",
        "is_btc",
        "cutoff",
        "start",
        "assembled",
        "lookback_s",
        "blume",
        "min_pairs",
        "event_raw",
        "names",
        "lens_us",
        "ends",
    }
    for record in (
        truth_sql.ReferencePrice,
        truth_sql.SectorReturn,
        truth_sql.RelativeHorizon,
    ):
        text = truth_sql.load_statement(record.statement)[0]
        for field in dataclasses.fields(record):
            assert re.search(rf"\bAS {field.name}\b|^\s*{field.name},?\s*$", text, re.M), (
                record.statement,
                field.name,
            )


async def test_evidence_sends_the_instruments_the_registry_chooses():
    server = Server(
        reference_prices=[
            reference("A:spot:T", flow_volume_native=10.0),
            reference("B:spot:T", flow_volume_native=30.0),
            reference("A:spot:BTC", flow_volume_native=99.0),
        ],
        sector_returns=[],
        relative_baselines=standard_horizons(END, START),
    )
    evidence = await truth.relative_evidence(server, registry(), START, END, {})
    assert (evidence.ref, evidence.btc, evidence.eth) == ("B:spot:T", "A:spot:BTC", "A:spot:ETH")
    (references,) = sent(server, "reference_prices")
    assert references["instrument_ids"] == "['A:spot:T','B:spot:T','A:spot:BTC']"
    assert (
        references["cutoff"] == "2026-10-16 12:00:00.000000"
        and references["start"] == "2026-10-16 08:00:00.000000"
    )
    assert references["assembled"] == "2026-10-16 13:00:00.000000" and references["lookback_s"] == str(
        90 * 86400
    )
    (sectors,) = sent(server, "sector_returns")
    assert sectors["peer_ids"] == "['A:spot:P1','B:spot:P2']"
    assert sectors["names"] == "['W15M','W1H','W4H','W24H','EVENT']"
    assert sectors["starts"] == (
        "['2026-10-16 11:45:00.000000','2026-10-16 11:00:00.000000','2026-10-16 08:00:00.000000',"
        "'2026-10-15 12:00:00.000000','2026-10-16 08:00:00.000000']"
    )
    (baselines,) = sent(server, "relative_baselines")
    assert (baselines["ref"], baselines["btc"], baselines["eth"], baselines["is_btc"]) == (
        "B:spot:T",
        "A:spot:BTC",
        "A:spot:ETH",
        "0",
    )
    assert baselines["names"] == "['W15M','W1H','W4H','W24H','EVENT','PRE24H','W12H','W3D','W7D']"
    assert baselines["lens_us"] == (
        "['900000000','3600000000','14400000000','86400000000','14400000000','86400000000',"
        "'43200000000','259200000000','604800000000']"
    )
    assert (
        baselines["ends"].count("2026-10-16 12:00:00.000000") == 8
        and "2026-10-16 08:00:00.000000" in baselines["ends"]
    )
    assert (baselines["blume"], baselines["min_pairs"], baselines["event_raw"]) == (str(2 / 3), "168", "nan")
    assert len(evidence.horizons) == 9


async def test_evidence_of_conditions_skips_outcome_horizons_and_knobs_reach_the_statements():
    server = Server(reference_prices=[reference("A:spot:T")], relative_baselines=[])
    config = {"historical_lookback_days": 30, "history_min_days": 3, "beta_blume_weight": 0.5}
    await truth.relative_evidence(server, registry(), START, END, config, include_outcome=False)
    (baselines,) = sent(server, "relative_baselines")
    assert baselines["names"] == "['W15M','W1H','W4H','W24H','EVENT','PRE24H']"
    assert (baselines["blume"], baselines["min_pairs"], baselines["lookback_s"]) == (
        "0.5",
        "72",
        str(30 * 86400),
    )
    assert baselines["ref"] == "A:spot:T" and baselines["event_raw"] == "nan"


async def test_close_only_evidence_reads_no_baselines_and_no_sector_peers():
    server = Server(reference_prices=[reference("A:spot:T", flow_volume_native=1.0)])
    evidence = await truth.relative_evidence(server, registry(), START, END, {}, baselines=False)
    assert [name for name, _ in server.sent] == ["reference_prices"]
    assert evidence.ref == "A:spot:T" and evidence.horizons == () and evidence.peers == ()


async def test_sector_peers_fall_back_to_the_defi_sectors_when_fewer_than_two_have_a_series():
    counts = {"A:spot:T": 1, "A:spot:P1": 5, "A:spot:P2": 0, "B:spot:P2": 0, "A:spot:D1": 2, "A:spot:D2": 2}
    server = Server(reference_prices=[], sector_returns=[], relative_baselines=[])
    await truth.relative_evidence(server, registry(counts), START, END, {})
    (sectors,) = sent(server, "sector_returns")
    assert sectors["peer_ids"] == "['A:spot:D1','A:spot:D2','A:spot:P1']"


async def test_event_return_comes_from_the_trade_prices_and_is_sent_to_the_baselines():
    trades = dict(
        anchor_trade=trade("100", "2026-10-16T07:59:59Z"), entry_trade=trade("101.1", "2026-10-16T11:59:59Z")
    )
    server = Server(
        reference_prices=[reference("A:spot:T", flow_volume_native=1.0, **trades)], relative_baselines=[]
    )
    await truth.relative_evidence(server, registry(), START, END, {})
    (baselines,) = sent(server, "relative_baselines")
    assert float(baselines["event_raw"]) == pytest.approx(0.011, abs=1e-15)
    server = Server(
        reference_prices=[
            reference(
                "A:spot:T",
                flow_volume_native=1.0,
                last_close=Decimal("102"),
                anchor_trade=trades["anchor_trade"],
            )
        ],
        relative_baselines=[],
    )
    await truth.relative_evidence(server, registry(), START, END, {})
    assert float(sent(server, "relative_baselines")[0]["event_raw"]) == pytest.approx(0.02, abs=1e-15)


async def test_evidence_without_an_assembly_time_reads_every_recorded_row():
    server = Server(reference_prices=[])
    await truth.relative_evidence(server, registry(assembled_at=None), START, END, {}, baselines=False)
    assert sent(server, "reference_prices")[0]["assembled"].startswith("2299-12-31")


def test_reference_is_the_candidate_with_a_price_and_the_largest_complete_volume():
    rows = [
        reference("A:spot:T", flow_volume_native=5.0),
        reference("B:spot:T", flow_volume_native=5.0),
        reference("C:spot:T", flow_volume_native=9.0, last_close=None),
        reference("D:spot:T", flow_volume_native=None, flow_detail="FULL_MINUTE_COVERAGE_INCOMPLETE"),
        reference("A:spot:BTC", flow_volume_native=50.0),
    ]
    candidates = ["A:spot:T", "B:spot:T", "C:spot:T", "D:spot:T"]
    assert truth.reference_choice(rows, candidates) == "A:spot:T"
    assert truth.reference_choice(rows[2:], candidates) is None


def snapshot_with(evidence, **changes):
    return dict(registry(), relative_sql=evidence) | changes


ALL = ("W15M", "W1H", "W4H", "W24H", "EVENT", "PRE24H", "W12H", "W3D", "W7D")


def everywhere(**changes):
    """Horizons whose every row carries `changes` (what the statement repeats on each row)."""
    return standard_horizons(END, START, **{name: changes for name in ALL})


def test_beta_flags_and_unusable_history_follow_the_returned_counts():
    horizons = everywhere(beta_btc_pairs=100, beta_eth_pairs=2000, beta_eth_ols=None)
    relative = truth.relative_truth(snapshot_with(make_evidence(horizons=horizons)), START, END)
    assert relative["beta_flags"] == {"btc": "INSUFFICIENT_HISTORY", "eth": "NOT_ESTIMABLE"}
    assert relative["beta_btc"] == 1.1
    short = everywhere(price_first_close=END - timedelta(days=2))
    relative = truth.relative_truth(snapshot_with(make_evidence(horizons=short)), START, END)
    event = relative["windows"]["EVENT"]
    assert event["normal_range"] is None and event["normal_range_reason"] == "INCOMPLETE"
    assert event["pctile"] is None and event["historical_measurement"]["exact"] is False
    assert relative["repricing_band"] is None
    assert relative["pre_event_runup"]["pctile"] is None and relative["pre_event_runup"]["flag"] is None


def test_windows_are_the_returns_of_the_statement_with_their_history_diagnostics():
    horizons = standard_horizons(
        END,
        START + timedelta(seconds=17),
        EVENT=dict(
            raw_return=0.03, btc_adjusted_return=0.02, measure=0.02, eth_adjusted_return=0.01, percentile=0.9
        ),
        W1H=dict(
            raw_return=0.001, btc_adjusted_return=0.0005, measure=0.0005, history_q025=0.0, history_q975=1e-14
        ),
    )
    start = START + timedelta(seconds=17)
    relative = truth.relative_truth(
        snapshot_with(make_evidence(references=[reference("A:spot:T")], horizons=horizons)), start, END
    )
    event = relative["windows"]["EVENT"]
    assert event["r_raw"] == 0.03 and event["r_btc_adj"] == 0.02 and event["relative_return"] == 0.02
    assert event["r_eth_adj"] == 0.01 and event["pctile"] == 0.9
    assert event["normal_range"] == [-0.02, 0.02] and event["beyond_normal"] is False
    assert event["historical_window_duration_s"] == event["requested_window_duration_s"] == 14383
    assert event["historical_window_resolution_s"] == 60
    assert event["historical_measurement"] == dict(
        target_window_duration_s=14383.0,
        observed_duration_bounds_s=[14400.0, 14400.0],
        endpoint_staleness_bounds_s=[17.0, 17.0],
        exact=False,
        resolution_s=60,
        alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
        sample_count=1000,
        latest_observed_at="2026-10-15T00:00:00Z",
    )
    assert relative["windows"]["W1H"]["normal_range"] is None
    assert relative["windows"]["W1H"]["normal_range_reason"] == "DEGENERATE_HISTORY"
    assert relative["windows"]["W15M"]["historical_measurement"]["exact"] is True
    assert event["observed_at"] == "2026-10-16T12:00:00Z" and event["precision"] == "BUCKET(60s)"
    assert event["coverage"] == {"reference_venue": "A:spot:T", "complete_24h": True}


def test_pre_event_runup_and_repricing_band_use_the_24h_history():
    horizons = standard_horizons(
        END,
        START,
        PRE24H=dict(measure=0.05, percentile=0.97, history_q025=-0.04, history_q975=0.046),
        EVENT=dict(raw_return=0.03, btc_adjusted_return=0.03, measure=0.03),
    )
    relative = truth.relative_truth(snapshot_with(make_evidence(horizons=horizons)), START, END)
    assert relative["pre_event_runup"]["ar"] == 0.05 and relative["pre_event_runup"]["pctile"] == 0.97
    assert relative["pre_event_runup"]["flag"] is True
    assert relative["pre_event_runup"]["start"] == iso(START - timedelta(days=1))
    assert relative["repricing_band"] == "SUBSTANTIAL"
    loser = truth.relative_truth(
        snapshot_with(make_evidence(horizons=horizons), expected_direction="LOSER"), START, END
    )
    assert loser["repricing_sign"] == -1 and loser["repricing_band"] == "NEGATIVE"


def test_market_context_and_volatility_compression_read_the_btc_and_reference_statistics():
    horizons = standard_horizons(
        END,
        START,
        W24H=dict(btc_return=-0.04, btc_realized_vol_24h=0.004, ref_realized_vol_24h=0.001),
    )
    base = snapshot_with(make_evidence(horizons=horizons))
    relative = truth.relative_truth(base, START, END)
    assert relative["market_context"] == "STRESS"
    assert relative["context_inputs"] == dict(
        btc_return_24h=-0.04,
        rv24=0.004,
        rv24_observed_cutoff=iso(END),
        rv24_resolution_s=3600,
        return_normal_range=[-0.03, 0.06],
        return_lower_quartile=-0.008,
        rv_normal_range=[0.001, 0.007],
        rule="normal_range",
    )
    calm = standard_horizons(END, START, W24H=dict(btc_return=0.0, btc_realized_vol_24h=0.004))
    assert (
        truth.relative_truth(snapshot_with(make_evidence(horizons=calm)), START, END)["market_context"]
        == "NEUTRAL"
    )
    short = everywhere(btc_first_close=END - timedelta(days=3))
    relative = truth.relative_truth(snapshot_with(make_evidence(horizons=short)), START, END)
    assert relative["market_context"] is None and relative["context_inputs"]["return_normal_range"] is None


@pytest.mark.parametrize(
    "changes,state,reason",
    [
        ({"ref_realized_vol_24h": 0.001}, "COMPRESSED", None),
        ({"ref_realized_vol_24h": 0.03}, "EXPANDED", None),
        ({"ref_realized_vol_24h": 0.01}, "NORMAL", None),
        ({"ref_hourly_first": END - timedelta(days=3)}, None, "INCOMPLETE"),
        ({"ref_realized_vol_24h_samples": 0}, None, "INCOMPLETE"),
        ({"ref_realized_vol_24h": None}, None, "INCOMPLETE"),
    ],
)
def test_volatility_compression_compares_the_reference_realized_volatility_with_its_history(
    changes, state, reason
):
    packet = compact_start_snapshot()
    start = packet["relative_sql"]
    packet["relative_sql"] = dataclasses.replace(
        start, horizons=tuple(dataclasses.replace(row, **changes) for row in start.horizons)
    )
    compression = truth.compute_market_truth(packet)["liquidity"]["vol_compression"]
    assert (compression["state"], compression["reason"]) == (state, reason)
    assert compression["window"] == "W24H"
    if reason is None:
        assert compression["normal_range"] == [0.004, 0.02]


def test_sector_return_needs_two_peers_and_falls_back_to_the_defi_sectors():
    horizons = standard_horizons(
        END, START, EVENT=dict(raw_return=0.03, btc_adjusted_return=0.03, measure=0.03)
    )

    def peers(**by_instrument):
        return [
            truth_sql.SectorReturn(instrument_id=iid, name=name, peer_return=value)
            for iid, value in by_instrument.items()
            for name in ("W15M", "W1H", "W4H", "W24H", "EVENT")
        ]

    def window(rows, **changes):
        base = snapshot_with(make_evidence(horizons=horizons, peers=rows), **changes)
        return truth.relative_truth(base, START, END)["windows"]["EVENT"]

    measured = window(peers(**{"A:spot:P1": 0.01, "B:spot:P2": 0.02}))
    assert measured["r_sector_adj"] == pytest.approx(0.03 - 0.015) and measured["sector_reason"] is None
    one = window(peers(**{"A:spot:P1": 0.01, "A:spot:D1": 0.04, "A:spot:D2": 0.06}))
    assert one["r_sector_adj"] == pytest.approx(0.03 - 0.05)
    assert window(peers(**{"A:spot:P1": 0.01}))["sector_reason"] == "NOT_ESTIMABLE"
    assert window([])["r_sector_adj"] is None
    tokens = [
        dict(t, sector="IDENTITY_PAYMENTS") if t["token_id"] == "T" else t for t in registry()["tokens"]
    ]
    payments = window(peers(**{"A:spot:P1": 0.01, "B:spot:P2": 0.02}), tokens=tokens)
    assert payments["r_sector_adj"] is None and payments["sector_reason"] == "NOT_APPLICABLE"


def test_reference_anchor_entry_and_btc_reference_come_from_the_reference_rows():
    anchor = trade("100.5", "2026-10-16T07:59:59.500000Z", "a1")
    entry = trade("101.5", "2026-10-16T11:59:58Z", "e1")
    references = [
        reference("A:spot:T", flow_volume_native=2.0, anchor_trade=anchor, entry_trade=entry),
        reference("B:spot:T", flow_volume_native=None, flow_detail="BOUNDARY_AGGREGATE_UNAVAILABLE"),
        reference("A:spot:BTC", last_close=Decimal("75000.5"), last_close_time=END - timedelta(minutes=1)),
    ]
    relative = truth.relative_truth(
        snapshot_with(
            make_evidence(references=references, btc="A:spot:BTC", horizons=standard_horizons(END, START))
        ),
        START,
        END,
    )
    assert relative["ref_venue"] == "A:spot:T" and relative["reference_reason"] is None
    assert relative["anchor"]["ref_price"] == Decimal("100.5")
    assert relative["anchor"]["price_observed_at"] == "2026-10-16T07:59:59.500000Z"
    assert relative["anchor"]["evidence"] == dict(
        price="100.5",
        trade_id="a1",
        ts="2026-10-16T07:59:59.500000Z",
        ts_prec="EXACT",
        ts_prec_reason=None,
        instrument_id="A:spot:T",
    )
    assert relative["anchor"]["price_source"] == "LAST_TRADE" and relative["anchor"]["type"] == "T0"
    assert relative["entry_ref_price"] == Decimal("101.5") and relative["entry_price_source"] == "LAST_TRADE"
    assert relative["entry_price_observed_at"] == "2026-10-16T11:59:58Z"
    assert relative["close_1h_price"] == Decimal("99")
    assert relative["outcome_btc_reference"] == dict(
        instrument_id="A:spot:BTC",
        entry_ref_price=Decimal("75000.5"),
        observed_at=iso(END - timedelta(minutes=1)),
        price_source="KLINE_CLOSE",
        reason=None,
    )
    assert relative["reference_coverage"]["A:spot:T"] == dict(
        complete=True,
        expected_duration_s=86400,
        boundary_segments=[],
        excluded=[],
        volume_unit="NATIVE_TOKEN",
        covered_volume_native=2.0,
    )
    assert relative["reference_coverage"]["B:spot:T"]["excluded"] == [
        dict(venue="B", reason="INCOMPLETE", detail="BOUNDARY_AGGREGATE_UNAVAILABLE")
    ]
    assert "A:spot:BTC" not in relative["reference_coverage"]
    assert set(relative["normal_ranges"]) == {"W1H", "W4H", "W12H", "W24H", "W3D", "W7D"}
    assert relative["normal_ranges"]["W1H"]["value"] == [-0.02, 0.02]
    assert relative["normal_ranges"]["W1H"]["coverage"] == {"sample_count": 1000}
    assert relative["normal_ranges"]["W1H"]["precision"] == "BUCKET(60s)"


def test_the_condition_close_is_the_hourly_close_of_the_cutoff_hour_only():
    evidence = start_evidence(END, START)
    close = truth.condition_reference_close(dict(token_id="T", relative_sql=evidence), END)
    assert close["ref_venue"] == "A:spot:T" and close["close_1h_price"] == Decimal("101.1")
    assert close["close_1h_observed_at"] == iso(END) and close["close_1h_observed_at_prec"] == "BUCKET(60s)"
    later = truth.condition_reference_close(
        dict(token_id="T", relative_sql=evidence), END + timedelta(minutes=5)
    )
    assert later["close_1h_price"] == Decimal("101.1") and later["ref_venue"] == "A:spot:T"
    stale = with_references(evidence, hourly_close_time=END - timedelta(hours=1))
    missing = truth.condition_reference_close(dict(token_id="T", relative_sql=stale), END)
    assert missing["close_1h_price"] is None and missing["close_1h_observed_at"] is None
    nothing = truth.condition_reference_close(dict(token_id="T"), END)
    assert nothing["ref_venue"] is None and nothing["close_1h_price"] is None


def test_condition_results_are_keyed_by_the_statement_records():
    first = start_evidence(END, START)
    second = with_references(first, last_close=Decimal("1"))
    requirements = [dict(metric="price.close_vs_level", window="W1H")]
    cache = {}
    one = truth.compute_condition_values(
        dict(token_id="T", cutoff=iso(END), relative_sql=first), requirements, cache=cache
    )
    again = truth.compute_condition_values(
        dict(token_id="T", cutoff=iso(END), relative_sql=first), requirements, cache=cache
    )
    other = truth.compute_condition_values(
        dict(token_id="T", cutoff=iso(END), relative_sql=with_references(first, hourly_close=Decimal("7"))),
        requirements,
        cache=cache,
    )
    assert one["_condition_evaluation"]["proofs"][0]["reused"] is False
    assert again["_condition_evaluation"]["proofs"][0]["reused"] is True
    assert other["_condition_evaluation"]["proofs"][0]["reused"] is False
    assert other["relative"]["close_1h_price"] == "7"
    assert second is not first
    assert horizon("W1H").length_s == 3600


def test_volatility_compression_of_each_window_reads_the_history_that_ends_where_it_starts():
    packet = compact_start_snapshot()
    start = packet["relative_sql"]
    ranges = {"W15M": (0.001, 0.002), "W1H": (0.003, 0.004), "W4H": (0.005, 0.006), "EVENT": (0.007, 0.008)}
    packet["relative_sql"] = dataclasses.replace(
        start,
        horizons=tuple(
            dataclasses.replace(
                row,
                ref_realized_vol_24h_q025=ranges[row.name][0],
                ref_realized_vol_24h_q975=ranges[row.name][1],
            )
            if row.name in ranges
            else row
            for row in start.horizons
        ),
    )
    windows = truth.compute_market_truth(packet)["liquidity"]["windows"]
    for name, bounds in ranges.items():
        assert windows[name]["vol_compression"]["normal_range"] == list(bounds)

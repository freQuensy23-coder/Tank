import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tank.analysis.truth import (
    _oi_stats,
    compute_market_truth,
    decomposition,
    market_context,
    oi_class,
    repricing,
    spot_truth,
    walk_book,
)
from tank.util import iso, parse_time


def test_oi_formula_vectors():
    d = decomposition(100, 108.18, 1, 1.0705)
    assert d["position"] == Decimal("8.18")
    assert d["markup"] == Decimal("7.05")
    assert d["interaction"] == Decimal("0.57669")
    assert d["pos_share"] > 0.5
    assert oi_class(0.0818, 0.1580669, d, [-0.03, 0.035], [-0.04, 0.05]) == "REAL_NATIVE"
    d = decomposition(100, 100.5, 1, 1.1)
    assert oi_class(0.005, 0.1055, d, [-0.03, 0.035], [-0.04, 0.05]) == "PRICE_MARKUP"
    assert oi_class(0.005, 0.1055, d, None, [-0.04, 0.05]) is None


def flow_case(values, qs):
    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=1)
    rows = [
        dict(
            instrument_id=f"{v}:spot:T",
            minute_start=iso(start),
            buy_qty=(vol + cvd) / 2,
            sell_qty=(vol - cvd) / 2,
            sum_sq_qty=q,
            complete=True,
        )
        for (v, vol, cvd), q in zip(values, qs)
    ]
    return spot_truth(rows, [], [], start, end)


def test_t3_capital_weighted_broad_sell():
    s = flow_case([("A", 600, -40), ("B", 250, -10), ("C", 100, 5), ("D", 25, 6), ("E", 25, 3)], [1] * 5)
    assert s["direction"] == "SELL"
    assert s["classification"] == "BROAD"
    assert s["breadth"]["pct_pos"] == 0.6
    assert s["cw_breadth"]["sell"] == 0.85
    assert s["concentration"]["others_cwb_dir"] == 0.625


def test_t4_single_venue_spike():
    s = flow_case([("A", 300, 60), ("B", 300, -2), ("C", 300, 1), ("D", 100, 0)], [1, 4, 4, 4])
    assert s["classification"] == "CONCENTRATED"
    assert s["composite_max"]["cvd"] == 59
    assert s["concentration"]["others_cwb_dir"] == 0


def test_missing_is_not_zero_and_no_leakage():
    s = compute_market_truth(
        dict(
            token_id="WLD",
            cutoff="2026-10-16T12:00:00Z",
            flow_bars=[
                dict(
                    instrument_id="binance:spot:WLD",
                    minute_start="2026-10-17T12:00:00Z",
                    buy_qty=1,
                    sell_qty=0,
                    sum_sq_qty=1,
                    complete=True,
                )
            ],
        )
    )
    assert s["data_confidence"]["class"] == "LOW"
    assert s["move_class"] == "UNKNOWN"
    assert s["relative"]["market_context"] is None
    assert s["liquidity"]["liquidity_state"] is None
    assert s["supply"]["supply_in_evidence"] == "UNKNOWN"
    assert s["spot"]["W1H"]["coverage"]["n_complete"] == 0


def test_repricing_and_baseline_context():
    assert repricing(0.23, [-0.03, 0, 0.02, 0.05], [-0.04, 0, 0.02, 0.046]) == "HEAVY"
    assert market_context(-0.0325, 0.0045, [-0.0301, -0.0085, 0, 0.02], [0.003, 0.005, 0.01]) == "STRESS"


def test_walk_book_requires_actual_fill():
    assert walk_book([[100, 1000], [101, 1000]], 100000, 100) == 0
    assert walk_book([[100, 1]], 100000, 100) is None


def test_real_rest_fixture_endpoints():
    path = Path(__file__).resolve().parents[1] / "fixture_check/data/rest_openInterestHist_fixtures.json"
    data = json.loads(path.read_text())["data"]
    cases = [
        ("F-12:QNTUSDT:1h", "2026-09-26T19:00:00Z", "2026-09-26T23:00:00Z", 0.0042),
        ("F-13:ONDOUSDT:1h", "2026-09-24T12:00:00Z", "2026-09-24T16:00:00Z", 0.5732),
        ("F-14:QNTUSDT:1h", "2026-09-27T19:00:00Z", "2026-09-27T23:00:00Z", -0.3036),
    ]
    for key, a, z, expected in cases:
        rows = [
            dict(
                ts=iso(parse_time(r["timestamp"])),
                oi_native=r["sumOpenInterest"],
                oi_usd=r["sumOpenInterestValue"],
            )
            for r in data[key]
        ]
        result = _oi_stats(rows, parse_time(a), parse_time(z))
        assert result["oi_native_chg_pct"] == pytest.approx(expected, abs=0.0001)


def test_t6_funding_and_t8_semantics():
    from tank.analysis.truth import data_confidence, derivatives_truth

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=4)
    funding = [
        dict(instrument_id=f"{v}:perp:T", kind="SETTLED", settlement_time=iso(end), rate=r, interval_h=h)
        for v, r, h in [("X", 0.0001, 8), ("Y", 0.00005, 4), ("Z", 0.00002, 1)]
    ]
    oi = [dict(instrument_id=f"{v}:perp:T", ts=iso(end), oi_native=100, oi_usd=100) for v in ("X", "Y", "Z")]
    d = derivatives_truth(dict(oi=oi, funding=funding), start, end)
    assert [v["funding_8h_eq"] for v in d["per_venue"]] == [
        Decimal(".0001"),
        Decimal(".0001"),
        Decimal(".00016"),
    ]
    assert d["funding_dispersion_8h"] == Decimal(".00006")
    spot = dict(coverage=dict(n_complete=3, covered_share_est=0.8))
    snap = dict(
        semantics=[dict(metric_key="aggressor", critical=True, semantic_confidence="DOCUMENTED_UNTESTED")]
    )
    c = data_confidence(snap, spot, dict(per_venue=[]), end)
    assert c["class_"] == "MEDIUM"
    assert c["limiting"]["condition"] == "semantics"


def test_f11_rest_synchronized_native():
    path = Path(__file__).resolve().parents[1] / "fixture_check/data"
    data = json.loads((path / "rest_openInterestHist_fixtures.json").read_text())["data"]
    a, z = parse_time("2026-10-16T08:30:00Z"), parse_time("2026-10-16T12:30:00Z")
    bn = {parse_time(r["timestamp"]): float(r["sumOpenInterest"]) for r in data["F-11:WLDUSDT:5m"]}
    by = {
        parse_time(r["timestamp"]): float(r["singleOpenInterest"])
        for r in json.loads((path / "bybit_oi_5m_WLDUSDT.json").read_text())
    }
    native0, native1 = bn[a] + by[a], bn[z] + by[z]
    assert native1 / native0 - 1 == pytest.approx(0.0817617185)


def test_real_archive_oi_regressions():
    import csv
    import io
    import zipfile

    path = Path(__file__).resolve().parents[1] / "fixture_check/data"
    results = json.loads((path.parent / "results.json").read_text())
    rest = json.loads((path / "rest_openInterestHist_fixtures.json").read_text())["data"]
    for fixture, symbol, a, z, cls in [
        ("F-12", "QNTUSDT", "2026-09-26T19:00:00Z", "2026-09-26T23:00:00Z", "PRICE_MARKUP"),
        ("F-13", "ONDOUSDT", "2026-09-24T12:00:00Z", "2026-09-24T16:00:00Z", "REAL_NATIVE"),
        ("F-14", "QNTUSDT", "2026-09-27T19:00:00Z", "2026-09-27T23:00:00Z", "MIXED"),
    ]:
        rows = {}
        for file in sorted((path / "metrics").glob(symbol + "-*")):
            with zipfile.ZipFile(file) as archive:
                for name in archive.namelist():
                    for r in csv.DictReader(io.TextIOWrapper(archive.open(name))):
                        if not r["sum_open_interest"] or not r["sum_open_interest_value"]:
                            continue
                        n, u = float(r["sum_open_interest"]), float(r["sum_open_interest_value"])
                        if n > 0 and u > 0:
                            ts = parse_time(r["create_time"])
                            rows[ts] = dict(ts=iso(ts), oi_native=n, oi_usd=u)
        # REST is authoritative for event endpoints; archive supplies distributions only.
        for r in rest[f"{fixture}:{symbol}:1h"]:
            t = parse_time(r["timestamp"])
            if t in (parse_time(a), parse_time(z)):
                rows[t] = dict(ts=iso(t), oi_native=r["sumOpenInterest"], oi_usd=r["sumOpenInterestValue"])
        out = _oi_stats(list(rows.values()), parse_time(a), parse_time(z))
        assert out["oi_class"] == cls
        assert out["native_normal_range"][1] == pytest.approx(results[fixture]["n_p97_5"])


def fixture_klines(filename, token, interval_s):
    path = Path(__file__).resolve().parents[1] / "fixture_check/data" / filename
    return [
        dict(
            instrument_id=f"binance:spot:{token}USDT",
            token_id=token,
            open_time=iso(parse_time(r[0])),
            o=r[1],
            c=r[4],
            quote_volume=r[7],
            interval_s=interval_s,
            complete=True,
        )
        for r in json.loads(path.read_text())
    ]


def test_real_f08_does_not_invent_anchor_trades_or_spot_coverage():
    from tank.analysis.truth import relative_truth

    end = parse_time("2026-09-24T20:15:00Z")
    start = parse_time("2026-09-24T00:00:00Z")
    rows = []
    for filename, token, interval in [
        ("spot_1h_QNTUSDT.json", "QNT", 3600),
        ("spot_1h_BTCUSDT.json", "BTC", 3600),
        ("spot_15m_QNTUSDT_0924.json", "QNT", 900),
        ("spot_15m_BTCUSDT_0924.json", "BTC", 900),
    ]:
        rows += fixture_klines(filename, token, interval)
    result = relative_truth(dict(token_id="QNT", cutoff=iso(end), klines=rows), start, end)
    assert result["repricing_band"] is None
    assert result["ref_venue"] is None
    assert result["anchor"]["ref_price"] is None
    assert result["anchor"]["price_reason"] == "INCOMPLETE"
    # These saved packets contain price bars, not trades; the measured fixture's heavy value
    # can validate the band formula but cannot certify an actual last-trade anchor.
    measured = json.loads((Path(__file__).resolve().parents[1] / "fixture_check/results.json").read_text())[
        "F-08"
    ]
    assert (
        repricing(
            measured["windows"][0]["adj"],
            [-0.03, 0, 0.02, 0.05],
            [measured["h24_p2_5"], 0, measured["h24_p97_5"]],
        )
        == "HEAVY"
    )


def test_real_f16_baseline_stress_is_exposed():
    from tank.analysis.truth import relative_truth

    end = parse_time("2026-09-16T00:00:00Z")
    start = end - timedelta(days=1)
    rows = fixture_klines("spot_1h_BTCUSDT.json", "BTC", 3600)
    result = relative_truth(dict(token_id="BTC", cutoff=iso(end), klines=rows), start, end)
    assert result["market_context"] == "STRESS"
    assert result["context_inputs"]["btc_return_24h"] == pytest.approx(-0.03254567, abs=0.00001)
    assert "known_rule_mismatch" not in result["context_inputs"]


def test_t5_pairwise_constant_cohort():
    from tank.analysis.truth import enrich_evidence

    def truth(values):
        return dict(
            venues=[dict(venue=k, volume_native=v, cvd=0, sum_sq_qty=1, sign=0) for k, v in values.items()],
            composite_max={"volume": sum(values.values())},
            cohort_consistent={},
            direction="NEUTRAL",
            composite_cohort=None,
        )

    spots = {
        "W24H": truth({"binance": 500, "okx": 200, "coinbase": 250, "kraken": 50}),
        "W1H": truth({"binance": 500, "okx": 200, "kraken": 50, "bybit": 250}),
    }
    end = parse_time("2026-10-16T12:00:00Z")
    rel = dict(
        ref_venue=None,
        windows={"W15M": {"normal_range": None}, "W1H": {"r_raw": None}},
        context_inputs={"rv24": None, "rv_normal_range": None},
    )
    d = {}
    liquidity = {"per_venue": []}
    enrich_evidence({"cutoff": iso(end)}, end - timedelta(hours=1), end, spots, "W1H", d, rel, liquidity)
    comparison = spots["W1H"]["cohort_comparisons"]["W24H"]
    assert comparison["cohort"] == ["binance", "kraken", "okx"]
    assert comparison["share_of_max"] == 0.75
    assert spots["W1H"]["cohort_consistent"]["W24H"] is True


@pytest.mark.asyncio
async def test_assessment_persists_with_event_anchor():
    from types import SimpleNamespace

    from tank.analysis.truth import MarketTruthService

    class Repo:
        records = []

        async def get(self, collection, key):
            return {
                "event_time": {"value": "2026-10-16T08:00:00Z", "precision": "EXACT", "verified": True},
                "state_verified": True,
                "known_to_us_at": "2026-10-16T08:10:00Z",
            }

        async def put(self, collection, key, record, **kwargs):
            self.records.append((collection, key, record))
            return record

    class Market:
        async def snapshot(self, token_id, cutoff, anchor=None):
            return {"token_id": token_id, "cutoff": cutoff, "anchor": anchor}

    repo = Repo()
    service = MarketTruthService(repo, SimpleNamespace(config_version="test", code_version="test"), Market())
    result = await service.assess("EVT-1", "WLD", "2026-10-16T12:00:00Z")
    assert repo.records[0][0] == "ana.market_truth"
    assert result["relative"]["anchor"]["type"] == "T0"
    assert result["config_version"] == "test"


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_asymmetric_depth_uses_actual_hit_side_and_excludes_perps(side):
    from tank.analysis.truth import liquidity_truth

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=15)
    bid, ask = (10000, 100) if side == "BUY" else (100, 10000)

    def book(iid, stamp, bid, ask):
        return dict(
            instrument_id=iid,
            exchange_ts=iso(stamp),
            integrity_ok=True,
            mid=100,
            spread_bps=1,
            range_covered_pct={"bid": 2, "ask": 2},
            depth={"bid": {"p1": {"usd": bid}}, "ask": {"p1": {"usd": ask}}},
        )

    books = [
        book("A:spot:T", start - timedelta(seconds=1), bid, ask),
        book("A:spot:T", end, bid, ask),
        book("A:perp:T", end, 1e9, 1e9),
    ]
    rows = [
        dict(
            instrument_id="A:spot:T",
            minute_start=iso(start + timedelta(minutes=i)),
            complete=True,
            buy_notional=1020 if side == "BUY" else 1000,
            sell_notional=1000 if side == "BUY" else 1020,
        )
        for i in range(15)
    ]
    r = liquidity_truth(
        {"books": books, "flow_bars": rows},
        start,
        end,
        {"venues": [{"venue": "A"}]},
        {"windows": {"W4H": {"r_raw": 0.005 if side == "BUY" else -0.005, "beyond_normal": True}}},
    )
    assert r["elasticity"]["hit_side"] == "ask" if side == "BUY" else r["elasticity"]["hit_side"] == "bid"
    assert r["elasticity"]["hit_side_depth_p1_usd"] == 100
    assert r["elasticity"]["weak_response"] is True
    assert r["elasticity"]["excess_response"] is False
    assert r["liquidity_state"] == "THIN"
    assert len(r["per_venue"]) == 1


def test_confidence_rejects_descriptive_fake_semantics_without_actual_data():
    from tank.analysis.truth import data_confidence

    end = parse_time("2026-10-16T12:00:00Z")
    result = data_confidence(
        {
            "semantics": [
                {"metric_key": "aggressor depth price", "semantic_confidence": "VERIFIED", "critical": True}
            ]
        },
        {"venues": [{"venue": "A"}, {"venue": "B"}], "coverage": {"n_complete": 2, "covered_share_est": 1}},
        {"per_venue": []},
        end,
    )
    assert result["class_"] != "HIGH"
    assert result["critical_semantics_verified"] is False


def test_confidence_requires_present_fresh_reference_and_books():
    from tank.analysis.truth import data_confidence

    end = parse_time("2026-10-16T12:00:00Z")
    snapshot = {
        "token_id": "T",
        "instruments": [],
        "semantics": [],
        "flow_bars": [],
        "klines": [],
        "books": [],
    }
    for venue in ("A", "B"):
        iid = f"{venue}:spot:T"
        snapshot["instruments"].append(
            {"instrument_id": iid, "token_id": "T", "venue": venue, "market_type": "SPOT"}
        )
        for family in ("aggressor", "depth", "price"):
            snapshot["semantics"].append(
                {
                    "metric_key": f"{venue}.{family}",
                    "metric_family": family,
                    "venue": venue,
                    "market_type": "SPOT",
                    "semantic_confidence": "VERIFIED",
                }
            )
        snapshot["flow_bars"].append(
            {"instrument_id": iid, "minute_start": iso(end - timedelta(minutes=1)), "complete": True}
        )
        snapshot["klines"].append(
            {"instrument_id": iid, "open_time": iso(end - timedelta(minutes=2)), "complete": True, "c": 100}
        )
    spot = {"venues": [{"venue": "A"}, {"venue": "B"}], "coverage": {"n_complete": 2, "covered_share_est": 1}}
    result = data_confidence(snapshot, spot, {"per_venue": []}, end)
    assert result["class_"] == "MEDIUM"
    assert any(f.get("collection") == "books" for f in result["failed"])
    # Make price more than one cadence stale; semantic verification cannot mask it.
    snapshot["klines"][0]["open_time"] = iso(end - timedelta(minutes=3))
    result = data_confidence(snapshot, spot, {"per_venue": []}, end)
    assert any(
        f.get("collection") == "klines" and f.get("instrument_id") == "A:spot:T" for f in result["failed"]
    )


def test_nonminute_event_anchor_uses_true_asof_bounds_and_reports_precision():
    from runpy import run_path

    from tank.analysis.truth import relative_truth

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(snapshot["cutoff"])
    start = end - timedelta(hours=4) + timedelta(seconds=17)
    result = relative_truth(snapshot, start, end)
    event = result["windows"]["EVENT"]
    assert event["normal_range"] is not None
    assert event["historical_window_duration_s"] == event["requested_window_duration_s"] == 14383
    measurement = event["historical_measurement"]
    assert measurement["exact"] is False
    assert measurement["target_window_duration_s"] == 14383
    assert measurement["observed_duration_bounds_s"] == [14400, 14400]
    assert measurement["endpoint_staleness_bounds_s"] == [17, 17]
    assert result["anchor"]["ts"] == iso(start)


@pytest.mark.parametrize("confirmation", ["VALID", "FLAG_ONLY", "FUTURE", "NO_REST_GAP"])
def test_fragmentation_needs_actual_cutoff_safe_independent_rest_gap(confirmation):
    from tank.analysis.truth import enrich_evidence

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=15)
    books = []
    for i in range(90):
        ts = start + timedelta(seconds=10 * i)
        for venue, mid in [("A", 100), ("B", 103)]:
            rest = {
                "mid": mid if confirmation != "NO_REST_GAP" else 100,
                "spread_bps": 1,
                "source": "PUBLIC_REST",
                "ts": iso(end + timedelta(seconds=1) if confirmation == "FUTURE" else ts),
                "received_ts": iso(end + timedelta(seconds=1) if confirmation == "FUTURE" else ts),
            }
            books.append(
                {
                    "instrument_id": f"{venue}:spot:T",
                    "exchange_ts": iso(ts),
                    "integrity_ok": True,
                    "mid": mid,
                    "spread_bps": 1,
                    "second_path_confirmed": True,
                    "independent_rest": rest if confirmation != "FLAG_ONLY" else None,
                }
            )
    spot = {
        "venues": [
            {"venue": "A", "volume_native": 1, "cvd": 0, "sum_sq_qty": 1, "sign": 0},
            {"venue": "B", "volume_native": 1, "cvd": 0, "sum_sq_qty": 1, "sign": 0},
        ],
        "composite_max": {"volume": 2},
        "direction": "NEUTRAL",
        "composite_cohort": None,
        "cohort_consistent": {},
    }
    relative = {
        "windows": {"W15M": {"normal_range": None}, "W1H": {"r_raw": None}},
        "context_inputs": {"rv24": None, "rv_normal_range": None},
        "ref_venue": None,
    }
    liquidity = {"per_venue": []}
    flow = [
        {
            "instrument_id": f"{v}:spot:T",
            "minute_start": iso(start + timedelta(minutes=i)),
            "buy_qty": 1,
            "sell_qty": 0,
            "sum_sq_qty": ".01",
            "complete": True,
        }
        for v in ("A", "B")
        for i in range(15)
    ]
    enrich_evidence(
        {"books": books, "flow_bars": flow, "cutoff": iso(end)},
        start,
        end,
        {"W1H": spot},
        "W1H",
        {},
        relative,
        liquidity,
    )
    assert liquidity["fragmentation"]["persistent_gap"] is True
    assert liquidity["fragmentation"]["validated"] is (confirmation == "VALID")


def test_vacuum_detects_loss_on_sell_side_despite_deep_asks():
    from tank.analysis.truth import liquidity_truth

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=15)

    def book(t, bid):
        return {
            "instrument_id": "A:spot:T",
            "exchange_ts": iso(t),
            "integrity_ok": True,
            "mid": 100,
            "spread_bps": 1,
            "range_covered_pct": {"bid": 2, "ask": 2},
            "depth": {"bid": {"p1": {"usd": bid}}, "ask": {"p1": {"usd": 10000}}},
        }

    rows = [
        {
            "instrument_id": "A:spot:T",
            "minute_start": iso(start + timedelta(minutes=i)),
            "complete": True,
            "buy_notional": 1000,
            "sell_notional": 1000 + 50 / 15,
        }
        for i in range(15)
    ]
    result = liquidity_truth(
        {"books": [book(start - timedelta(seconds=1), 100), book(end, 20)], "flow_bars": rows},
        start,
        end,
        {"venues": [{"venue": "A"}]},
        {"windows": {"W4H": {"r_raw": -0.03, "beyond_normal": True}}},
    )
    assert result["depth_drop"] == pytest.approx(0.8)
    assert result["elasticity"]["excess_response"] is True
    assert result["elasticity"]["avg_hit_side_depth_p1_usd"] == 20
    assert result["liquidity_state"] == "VACUUM"


def test_oi_seconds_anchor_uses_actual_synchronized_observation_duration():
    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=4, seconds=-17)
    rows = [
        {"ts": iso(end + timedelta(minutes=i)), "oi_native": 100, "oi_usd": 100}
        for i in range(-8 * 1440, 0, 5)
    ]
    rows.append({"ts": iso(end), "oi_native": 108, "oi_usd": 108})
    result = _oi_stats(rows, start, end)
    assert result["native_normal_range"] == [0, 0]
    assert result["observed_window_duration_s"] == 14400
    assert result["requested_window_duration_s"] == 14383
    assert result["oi_class"] == "REAL_NATIVE"


def test_native_only_oi_preserves_native_and_sync_without_inventing_usd():
    from runpy import run_path

    from tank.analysis.truth import data_confidence, derivatives_truth

    fixture = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(fixture["cutoff"])
    start = end - timedelta(hours=4)
    for row in fixture["oi"]:
        row["oi_native"] = 100 if parse_time(row["ts"]) < end else 108
        row["oi_usd"] = None
        row["oi_usd_reason"] = "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"
    result = derivatives_truth(fixture, start, end)
    for stats in (result["per_venue"][0], result["synchronized"]):
        assert stats["oi_native_start"] == 100
        assert stats["oi_native_end"] == 108
        assert stats["oi_native_chg_pct"] == pytest.approx(0.08)
        assert stats["native_normal_range"] == [0, 0]
        assert stats["native_beyond"] is True
        assert stats["oi_usd_chg_pct"] is None
        assert stats["decomposition"]["position"] is None
        assert stats["decomposition"]["pos_share"] is None
        assert stats["oi_class"] is None
        assert stats["classification_ready"] is False
    assert result["synchronized"]["venues"] == ["binance"]
    spot = {"venues": [{"venue": "A"}, {"venue": "B"}], "coverage": {"n_complete": 2, "covered_share_est": 1}}
    conf = data_confidence(fixture, spot, result, end)
    assert conf["derivatives_native_coverage_ready"] is True
    assert conf["derivatives_classification_ready"] is False
    assert conf["class_"] == "HIGH"
    assert not any(f["condition"] == "derivatives_classification_readiness" for f in conf["failed"])
    assert not any(f["condition"] == "derivatives_coverage" for f in conf["failed"])


def test_supply_wrappers_preserve_unavailable_and_do_not_treat_unlock_as_sale():
    from tank.analysis.truth import supply_truth

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=4)
    unavailable = [
        {"kind": kind, "observed_at": iso(end), "available": False, "reason": "SOURCE_FAILED"}
        for kind in ("EXCHANGE_INVENTORY", "WALLET_FLOWS", "UNLOCKS", "LENDING", "CHAIN_LIQUIDITY")
    ]
    assert supply_truth({"supply": unavailable}, start, end)["supply_in_evidence"] == "UNKNOWN"
    unlock = {
        "kind": "UNLOCKS",
        "observed_at": iso(end),
        "available": True,
        "unlocks": [{"date": iso(end + timedelta(days=2)), "qty": 100, "kind": "POTENTIAL_SUPPLY"}],
    }
    result = supply_truth({"supply": [unlock]}, start, end)
    assert result["supply_in_evidence"] == "FALSE"
    assert len(result["unlocks_next_14d"]) == 1
    assert result["unlocks_next_14d"][0]["inference"] == "POTENTIAL_SUPPLY_IS_NOT_A_SALE"


def test_supply_inventory_own_history_wrapped_flows_chain_and_lending():
    from tank.analysis.truth import supply_truth

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=4)
    rows = []
    for h in range(-8 * 24, 1, 6):
        rows.append(
            {
                "kind": "EXCHANGE_INVENTORY",
                "observed_at": iso(end + timedelta(hours=h)),
                "available": True,
                "balances": [
                    {
                        "venue": "binance",
                        "chainid": 1,
                        "contract": "T",
                        "wallet_list_commit": "pinned",
                        "qty_native": 120 if h == 0 else 100,
                        "complete": True,
                    }
                ],
            }
        )
    rows.extend(
        [
            {
                "kind": "WALLET_FLOWS",
                "observed_at": iso(end),
                "available": True,
                "flows": [
                    {
                        "classification": "AMBIGUOUS",
                        "reaches_exchange": True,
                        "qty_native": 1,
                        "ts": iso(end - timedelta(minutes=1)),
                    }
                ],
            },
            {
                "kind": "CHAIN_LIQUIDITY",
                "observed_at": iso(end),
                "available": True,
                "chain": "Ethereum",
                "series": [
                    {"ts": iso(end - timedelta(days=7)), "supply_usd": 100},
                    {"ts": iso(end - timedelta(days=1)), "supply_usd": 110},
                    {"ts": iso(end), "supply_usd": 115},
                    {"ts": iso(end + timedelta(days=1)), "supply_usd": 99999},
                ],
            },
            {
                "kind": "LENDING",
                "observed_at": iso(end),
                "available": True,
                "pools": [{"pool_id": "P", "utilization": 0.5, "borrow_apy": 7}],
            },
        ]
    )
    result = supply_truth({"supply": rows}, start, end)
    assert result["exchange_inventory"]["chg_native_24h"] == 20
    assert result["exchange_inventory"]["chg_pct"] == pytest.approx(0.2)
    assert result["exchange_inventory"]["normal_range"] == [0, 0]
    assert result["supply_in_evidence"] == "TRUE"
    assert result["flows"][0]["class"] == "AMBIGUOUS"
    assert result["stablecoin_chain"]["supply_chg_24h"] == 5
    assert result["stablecoin_chain"]["supply_chg_7d"] == 15
    assert result["lending"]["pools"][0]["borrow_apr"] is None
    assert result["lending"]["pools"][0]["borrow_apy"] == 7


def test_derivative_windows_keep_one_hour_watch_separate_from_four_hour_crowding():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(snapshot["cutoff"])
    snapshot["anchor"]["ts"] = iso(end - timedelta(hours=2))
    for row in snapshot["oi"]:
        native = 108 if parse_time(row["ts"]) >= end - timedelta(hours=2) else 100
        row["oi_native"] = native
        row["oi_usd"] = native * 100
    for row in snapshot["funding"]:
        row["rate"] = 0.0001 if parse_time(row["settlement_time"]) >= end - timedelta(hours=2) else 0
    result = compute_market_truth(snapshot)
    windows = result["derivatives"]["windows"]
    assert set(windows) == {"W15M", "W1H", "W4H", "W24H", "EVENT"}
    assert result["derivatives"]["window"] == "EVENT"
    assert result["derivatives"]["synchronized"]["oi_native_chg_pct"] == 0
    assert windows["W1H"]["synchronized"]["oi_native_chg_pct"] == 0
    assert windows["W1H"]["synchronized"]["native_oi_vs_normal"] == "WITHIN"
    assert windows["W4H"]["synchronized"]["oi_native_chg_pct"] == pytest.approx(0.08)
    assert windows["W4H"]["synchronized"]["native_oi_vs_normal"] == "ABOVE"
    assert windows["W4H"]["synchronized"]["oi_class"] == "REAL_NATIVE"
    assert Decimal(windows["W1H"]["per_venue"][0]["funding_start"]) == Decimal(".0001")
    assert Decimal(windows["W4H"]["per_venue"][0]["funding_start"]) == 0
    for name, seconds in [("W15M", 900), ("W1H", 3600), ("W4H", 14400), ("W24H", 86400), ("EVENT", 7200)]:
        assert windows[name]["cutoff"] == iso(end)
        assert windows[name]["start"] == iso(end - timedelta(seconds=seconds))
        assert result["liquidity"]["windows"][name]["elasticity"]["window"] == name
        assert result["liquidity"]["windows"][name]["replenishment"]["window"] == name
        # Lead-lag and compression retain their specified scopes even inside a rolling pack.
        assert windows[name]["lead_lag"]["window"] == "EVENT"
        assert result["liquidity"]["windows"][name]["vol_compression"]["window"] == "W24H"
    assert (
        result["liquidity"]["windows"]["W1H"]["elasticity"]["bps_per_depth_unit"]
        != result["liquidity"]["windows"]["W4H"]["elasticity"]["bps_per_depth_unit"]
    )


def test_reference_requires_complete_24h_flow_and_last_actual_trade():
    from runpy import run_path

    from tank.analysis.truth import relative_truth

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(snapshot["cutoff"])
    start = parse_time(snapshot["anchor"]["ts"])
    snapshot["anchor_trades"][0]["price"] = 99
    result = relative_truth(snapshot, start, end)
    assert result["ref_venue"] == "A:spot:T"
    assert result["anchor"]["ref_price"] == 99
    assert result["anchor"]["price_source"] == "LAST_TRADE"
    assert result["windows"]["EVENT"]["r_raw"] == pytest.approx(101.1 / 99 - 1)
    # A was the largest venue by tie ordering; one missing minute makes B the only eligible ref.
    snapshot["flow_bars"][0]["complete"] = False
    result = relative_truth(snapshot, start, end)
    assert result["ref_venue"] == "B:spot:T"
    assert result["anchor"]["ref_price"] == 100
    snapshot["anchor_trades"] = []
    result = relative_truth(snapshot, start, end)
    assert result["anchor"]["ref_price"] is None
    assert result["windows"]["EVENT"]["r_raw"] is None
    assert result["repricing_band"] is None


def test_slug_identity_wld_sector_is_not_applicable_and_self_is_excluded():
    from runpy import run_path

    from tank.analysis.truth import relative_truth

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    snapshot["token_id"] = "worldcoin:WLD"
    snapshot["tokens"] = [
        {"token_id": "worldcoin:WLD", "symbol": "WLD"},
        {"token_id": "bitcoin:BTC", "symbol": "BTC"},
    ]
    for collection in ("instruments", "flow_bars", "klines", "books", "oi", "funding", "anchor_trades"):
        for row in snapshot[collection]:
            if row.get("token_id") == "T":
                row["token_id"] = "worldcoin:WLD"
            elif row.get("token_id") == "BTC":
                row["token_id"] = "bitcoin:BTC"
    result = relative_truth(snapshot, parse_time(snapshot["anchor"]["ts"]), parse_time(snapshot["cutoff"]))
    assert result["ref_venue"] == "A:spot:T"
    assert result["windows"]["EVENT"]["sector_reason"] == "NOT_APPLICABLE"
    assert result["windows"]["EVENT"]["r_sector_adj"] is None


def test_dense_minute_btc_context_matches_saved_hourly_downsample():
    from tank.analysis.truth import relative_truth

    end = parse_time("2026-09-16T00:00:00Z")
    start = end - timedelta(days=1)
    hourly = [
        r
        for r in fixture_klines("spot_1h_BTCUSDT.json", "BTC", 3600)
        if end - timedelta(days=10) <= parse_time(r["open_time"]) < end
    ]
    dense = []
    previous = hourly[0]["o"]
    for row in hourly:
        t = parse_time(row["open_time"])
        for minute in range(60):
            dense.append(
                dict(
                    row,
                    open_time=iso(t + timedelta(minutes=minute)),
                    interval_s=60,
                    c=row["c"] if minute == 59 else previous,
                    quote_volume=float(row["quote_volume"]) / 60,
                )
            )
        previous = row["c"]
    a = relative_truth({"token_id": "BTC", "cutoff": iso(end), "klines": hourly}, start, end)
    b = relative_truth({"token_id": "BTC", "cutoff": iso(end), "klines": dense}, start, end)
    assert b["context_inputs"]["rv24"] == pytest.approx(a["context_inputs"]["rv24"])
    assert b["context_inputs"]["rv_normal_range"] == pytest.approx(a["context_inputs"]["rv_normal_range"])
    assert b["market_context"] == a["market_context"]


def test_supported_move_has_decisive_measured_facts_and_capture_readiness():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    snapshot["capture_status"] = {
        "status": "PARTIAL",
        "reason": "Some old backfill unavailable",
        "backfill_id": "BF-1",
    }
    result = compute_market_truth(snapshot)
    assert result["move_class"] == "SPOT_LED_ACCUMULATION"
    assert 2 <= len(result["decisive_facts"]) <= 3
    assert all(isinstance(fact, str) for fact in result["decisive_facts"])
    for fact in result["decisive_fact_details"]:
        assert fact["value"] is not None
        assert fact["unit"] and fact["window"] == "EVENT"
        assert fact["evidence"] and fact["coverage"] and fact["text"]
    assert result["detail"]["capture_status"]["backfill_id"] == "BF-1"
    assert result["data_confidence"]["capture_readiness"]["prerequisite_pending"] is True


def test_fractional_window_boundary_trades_need_continuity_proof_and_are_exact():
    start = parse_time("2026-10-16T12:00:17Z")
    end = parse_time("2026-10-16T12:02:17Z")
    rows = []
    trades = []
    proofs = []
    instruments = []
    for venue in ("A", "B"):
        iid = f"{venue}:spot:T"
        instruments.append({"instrument_id": iid, "market_type": "SPOT"})
        for minute, buy, sell, q in [
            ("12:00:00", 999, 0, 1),
            ("12:01:00", 3, 1, 10),
            ("12:02:00", 999, 0, 1),
        ]:
            rows.append(
                {
                    "instrument_id": iid,
                    "minute_start": f"2026-10-16T{minute}Z",
                    "buy_qty": buy,
                    "sell_qty": sell,
                    "sum_sq_qty": q,
                    "complete": True,
                }
            )
        for n, ts, side, qty in [
            (1, "12:00:10", "SELL", 100),
            (2, "12:00:20", "BUY", 5),
            (3, "12:02:05", "SELL", 2),
            (4, "12:02:20", "BUY", 999),
        ]:
            trades.append(
                {
                    "instrument_id": iid,
                    "trade_id": str(n),
                    "ts": f"2026-10-16T{ts}Z",
                    "side": side,
                    "qty_native": qty,
                }
            )
        proofs.extend(
            [
                {
                    "instrument_id": iid,
                    "start": "2026-10-16T12:00:00Z",
                    "end": "2026-10-16T12:01:00Z",
                    "complete": True,
                    "observed_at": "2026-10-16T12:01:00Z",
                },
                {
                    "instrument_id": iid,
                    "start": "2026-10-16T12:02:00Z",
                    "end": iso(end),
                    "complete": True,
                    "observed_at": iso(end),
                },
            ]
        )
    unknown = spot_truth(rows, [], instruments, start, end, boundary_trades=trades)
    assert unknown["coverage"]["n_complete"] == 0
    assert unknown["composite_max"]["cvd"] is None
    assert all(
        x["detail"] == "BOUNDARY_TRADES_OR_CONTINUITY_PROOF_UNAVAILABLE"
        for x in unknown["coverage"]["excluded"]
    )
    measured = spot_truth(rows, [], instruments, start, end, boundary_trades=trades, boundary_coverage=proofs)
    assert measured["coverage"]["n_complete"] == 2
    assert measured["composite_max"]["cvd"] == 10
    assert measured["composite_max"]["volume"] == 22
    assert all(v["sum_sq_qty"] == 39 for v in measured["venues"])
    assert all(len(v["boundary_segments"]) == 2 for v in measured["venues"])
    for proof in proofs:
        proof["observed_at"] = iso(end + timedelta(seconds=1))
    assert (
        spot_truth(
            rows,
            [],
            instruments,
            start,
            end,
            boundary_trades=trades,
            boundary_coverage=proofs,
            assembled_at=iso(end),
        )["coverage"]["n_complete"]
        == 0
    )
    assert (
        spot_truth(
            rows,
            [],
            instruments,
            start,
            end,
            boundary_trades=trades,
            boundary_coverage=proofs,
            assembled_at=iso(end + timedelta(seconds=2)),
        )["coverage"]["n_complete"]
        == 2
    )


@pytest.mark.asyncio
async def test_assessment_cpu_runs_off_event_loop_and_default_cutoff_is_closed_minute(monkeypatch):
    import threading
    from types import SimpleNamespace

    import tank.analysis.truth as module

    main_thread = threading.get_ident()
    called = []
    original = module.compute_market_truth

    def observed(*args, **kwargs):
        called.append(threading.get_ident())
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "compute_market_truth", observed)

    class Repo:
        async def get(self, *args):
            return {}

        async def put(self, *args, **kwargs):
            return args[2]

    class Market:
        async def snapshot(self, token, cutoff, anchor=None):
            return {"token_id": token, "cutoff": cutoff, "anchor": anchor}

    service = module.MarketTruthService(
        Repo(), SimpleNamespace(config_version="test", code_version="test"), Market()
    )
    result = await service.assess(None, "T")
    assert called[0] != main_thread
    assert parse_time(result["cutoff"]).second == 0 and parse_time(result["cutoff"]).microsecond == 0
    explicit = await service.assess(None, "T", "2026-10-16T12:02:17Z")
    assert parse_time(explicit["cutoff"]).second == 17


def test_closed_bar_collection_latency_is_not_future_market_data():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(snapshot["cutoff"])
    snapshot["assembled_at"] = iso(end + timedelta(seconds=1))
    for row in snapshot["flow_bars"]:
        if parse_time(row["minute_start"]) == end - timedelta(minutes=1):
            row["received_ts"] = iso(end + timedelta(milliseconds=5))
    measured = compute_market_truth(snapshot)
    assert measured["data_confidence"]["class"] == "HIGH"
    assert measured["spot"]["W24H"]["coverage"]["n_complete"] == 2
    assert measured["relative"]["ref_venue"] is not None
    assert measured["time_policy"] == "SEMANTIC_MARKET_TIME_WITH_ASSEMBLY_AVAILABILITY"
    # An unavailable tool/result row cannot enter a snapshot assembled before it arrived.
    snapshot["assembled_at"] = iso(end)
    unavailable = compute_market_truth(snapshot)
    assert unavailable["spot"]["W24H"]["coverage"]["n_complete"] == 0
    assert unavailable["data_confidence"]["class"] != "HIGH"


def test_canonical_text_facts_print_units_windows_and_preserve_audit_companion():
    from runpy import run_path

    from tank.analysis.cards import compose_card
    from tank.analysis.decisions import DecisionEngine

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    truth = compute_market_truth(snapshot)
    assert all(isinstance(f, str) for f in truth["decisive_facts"])
    assert len(truth["decisive_facts"]) == len(truth["decisive_fact_details"])
    for text, detail in zip(truth["decisive_facts"], truth["decisive_fact_details"]):
        assert detail["unit"] in text and detail["window"] in text
        assert "coverage" in text or "covered" in text or "complete" in text
    event = {"event_id": "E", "event_state": "APPROVED", "state_verified": True, "freshness": "FRESH"}
    assessment = {"class": "MAJOR", "certainty": "HIGH", "impact_horizon": {"impact_start": "NOW"}}
    beneficiary = {"token_id": "T", "role": "PRIMARY", "credible": True, "capture_class": "DIRECT"}
    decision = DecisionEngine().evaluate(event, assessment, beneficiary, truth)
    card = compose_card(
        event, assessment, [beneficiary], [decision], [truth], verdict="Verified synthetic event."
    )
    assert "NATIVE_TOKEN" in card
    assert "EVENT" in card
    assert "complete spot venues" in card


def test_hot_derivative_cadence_is_60s_and_base_is_300s():
    from tank.analysis.truth import available_field_freshness

    end = parse_time("2026-10-16T12:00:00Z")
    point = iso(end - timedelta(seconds=90))
    snapshot = {
        "oi": [{"instrument_id": "A:perp:T", "ts": point, "oi_native": 100}],
        "funding": [
            {
                "instrument_id": "A:perp:T",
                "kind": "CURRENT_ESTIMATE",
                "as_of": point,
                "rate": 0.0001,
                "interval_h": 8,
            }
        ],
        "capture_metadata": {"tier": "HOT"},
    }
    hot = available_field_freshness(snapshot, end)
    assert {x["collection"] for x in hot if not x["fresh"]} == {"oi", "funding"}
    assert all(x["cadence_s"] == 60 for x in hot)
    snapshot["capture_metadata"]["tier"] = "BASE"
    base = available_field_freshness(snapshot, end)
    assert all(x["fresh"] and x["cadence_s"] == 300 for x in base)
    snapshot["capture_metadata"]["collections"] = {"oi": {"cadence_s": 30}}
    scoped = available_field_freshness(snapshot, end)
    assert next(x for x in scoped if x["collection"] == "oi")["cadence_s"] == 30


def test_stale_available_noncritical_supply_prevents_high_but_missing_is_na():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(snapshot["cutoff"])
    snapshot["capture_metadata"] = {"tier": "HOT"}
    snapshot["supply"] = [
        {"kind": "LENDING", "observed_at": iso(end - timedelta(seconds=1801)), "available": True, "pools": []}
    ]
    result = compute_market_truth(snapshot)
    assert result["data_confidence"]["class"] == "MEDIUM"
    assert any(
        f.get("collection") == "supply" and f.get("kind") == "LENDING" and f.get("cadence_s") == 1800
        for f in result["data_confidence"]["failed"]
    )
    snapshot["supply"][0]["available"] = False
    snapshot["supply"][0]["reason"] = "NOT_SUPPORTED"
    missing = compute_market_truth(snapshot)
    assert missing["data_confidence"]["class"] == "HIGH"
    assert missing["supply"]["lending"]["value"] is None
    assert missing["supply"]["supply_in_evidence"] == "UNKNOWN"


def test_null_measurements_have_companion_context_without_wrapping_literal_fields():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    result = compute_market_truth(snapshot)
    assert result["relative"]["windows"]["EVENT"]["r_eth_adj"] is None
    meta = result["measurement_metadata"]["relative.windows.EVENT.r_eth_adj"]
    assert meta["value"] is None and meta["unit"] == "DECIMAL_FRACTION"
    assert meta["window"] == "EVENT" and meta["cutoff"] == result["cutoff"]
    assert meta["reason"] == "INCOMPLETE"
    assert meta["precision"] is None and meta["precision_reason"] == "NOT_ESTIMABLE"
    assert "Literal TDD" in result["measurement_context"]["shape_policy"]
    assert isinstance(result["relative"]["windows"]["EVENT"]["r_raw"], float)


def test_hot_90_second_derivative_points_are_medium_without_weakening_sync_rule():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(snapshot["cutoff"])
    point = end - timedelta(seconds=90)
    for collection, time_key in [("oi", "ts"), ("funding", "settlement_time")]:
        for row in snapshot[collection]:
            if parse_time(row[time_key]) > point:
                row[time_key] = iso(point)
    snapshot["funding"].append(
        {
            "instrument_id": "binance:perp:T",
            "token_id": "T",
            "kind": "CURRENT_ESTIMATE",
            "as_of": iso(point),
            "rate": 0,
            "interval_h": 8,
            "metric_key": "binance.perp.funding",
        }
    )
    snapshot["capture_metadata"] = {"tier": "HOT"}
    hot = compute_market_truth(snapshot)
    assert hot["data_confidence"]["class"] == "MEDIUM"
    assert {"oi", "funding"} <= {
        f.get("collection") for f in hot["data_confidence"]["failed"] if f["condition"] == "freshness"
    }
    snapshot["capture_metadata"]["tier"] = "BASE"
    base = compute_market_truth(snapshot)
    assert not any(
        f["condition"] == "freshness" and f.get("collection") in ("oi", "funding")
        for f in base["data_confidence"]["failed"]
    )
    # The separate ≤60s synchronization requirement still rejects a 90s-old boundary,
    # even though base collection cadence permits the point's age.
    assert base["derivatives"]["synchronized"] is None


def test_freshness_uses_semantic_point_time_and_snapshot_assembly_availability():
    from tank.analysis.truth import available_field_freshness

    end = parse_time("2026-10-16T12:00:00Z")
    point = {
        "instrument_id": "A:perp:T",
        "ts": iso(end - timedelta(seconds=30)),
        "received_ts": iso(end + timedelta(seconds=2)),
        "oi_native": 1,
    }
    snapshot = {
        "oi": [point],
        "capture_metadata": {"tier": "HOT"},
        "assembled_at": iso(end + timedelta(seconds=3)),
    }
    check = available_field_freshness(snapshot, end)[0]
    assert check["fresh"] and check["age_s"] == 30
    snapshot["assembled_at"] = iso(end + timedelta(seconds=1))
    assert available_field_freshness(snapshot, end) == []
    snapshot["assembled_at"] = iso(end + timedelta(seconds=3))
    point["ts"] = iso(end + timedelta(seconds=1))
    assert available_field_freshness(snapshot, end) == []


def test_outcome_btc_reference_and_horizon_ranges_are_frozen_from_entry_history():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    result = compute_market_truth(snapshot)
    relative = result["relative"]
    btc = relative["outcome_btc_reference"]
    assert btc["instrument_id"] == "A:spot:BTC"
    assert Decimal(btc["entry_ref_price"]) == Decimal(100)
    assert btc["price_source"] == "KLINE_CLOSE"
    assert set(relative["normal_ranges"]) == {"W1H", "W4H", "W12H", "W24H", "W3D", "W7D"}
    for name, measured in relative["normal_ranges"].items():
        assert measured["unit"] == "DECIMAL_RETURN" and measured["window"] == name
        assert measured["coverage"]["sample_count"] > 0
        assert measured["value"] is not None and measured["reason"] is None
        assert parse_time(measured["cutoff"]) == parse_time(snapshot["anchor"]["ts"])
    # Post-entry market points cannot refit the frozen own-history distribution.
    snapshot["klines"].append(
        {
            "instrument_id": "A:spot:T",
            "token_id": "T",
            "open_time": "2026-10-17T12:00:00Z",
            "interval_s": 60,
            "c": 999999,
            "complete": True,
        }
    )
    again = compute_market_truth(snapshot)
    assert again["relative"]["normal_ranges"] == relative["normal_ranges"]
    assert again["relative"]["beta_btc"] == relative["beta_btc"]


def test_complete_liquidation_total_requires_whole_window_capture_not_recent_connection():
    from tank.analysis.truth import derivatives_truth

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=4)
    iid = "bybit:perp:T"
    snapshot = {
        "oi": [
            {"instrument_id": iid, "ts": iso(start), "oi_native": 100, "oi_usd": 100},
            {"instrument_id": iid, "ts": iso(end), "oi_native": 100, "oi_usd": 100},
        ],
        "liquidations": [
            {
                "instrument_id": iid,
                "ts": iso(end - timedelta(minutes=1)),
                "side": "SHORT_LIQ",
                "notional_usd": 100,
            }
        ],
        "capture_metadata": {
            "collections": {
                "liquidations": {
                    "coverage_start": {iid: iso(end - timedelta(minutes=5))},
                    "observed_through": {iid: iso(end)},
                }
            }
        },
    }
    partial = derivatives_truth(snapshot, start, end)
    assert partial["per_venue"][0]["liq_source_coverage"] == "COMPLETE"
    assert partial["per_venue"][0]["liq_coverage"] == "UNKNOWN"
    assert partial["liquidations"]["complete_venues_total"]["short"] is None
    snapshot["capture_metadata"]["collections"]["liquidations"]["coverage_start"][iid] = iso(start)
    complete = derivatives_truth(snapshot, start, end)
    assert complete["per_venue"][0]["liq_coverage"] == "COMPLETE"
    assert complete["liquidations"]["complete_venues_total"]["short"] == 100
    snapshot["liquidations"] = []
    empty = derivatives_truth(snapshot, start, end)
    assert empty["liquidations"]["complete_venues_total"] == {"long": 0, "short": 0}
    snapshot["capture_metadata"]["collections"]["liquidations"]["coverage_start"][iid] = iso(
        start + timedelta(seconds=1)
    )
    incomplete = derivatives_truth(snapshot, start, end)
    assert incomplete["liquidations"]["complete_venues_total"] == {"long": None, "short": None}


def test_native_sums_preserve_large_and_tiny_quantities_without_float_roundtrip():
    from tank.analysis.truth import _sum

    value = _sum([{"qty": "1E150"}, {"qty": "1E-40"}], "qty")
    assert isinstance(value, Decimal)
    assert format(value, "f") == "1" + "0" * 150 + "." + "0" * 39 + "1"
    assert _sum([{"qty": "0.0000000000000000000000000000000000000001"}] * 3, "qty") == Decimal("3E-40")


def test_decimal_significance_does_not_round_a_subthreshold_flow_into_buy():
    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=1)
    rows = [
        {
            "instrument_id": f"{v}:spot:T",
            "minute_start": iso(start),
            "buy_qty": "1",
            "sell_qty": "0",
            "sum_sq_qty": "0.2500000000000000000000000000000000000001",
            "complete": True,
        }
        for v in ("A", "B")
    ]
    result = spot_truth(rows, [], [], start, end)
    assert result["breadth"]["n_neutral"] == 2
    assert all(v["sign"] == 0 for v in result["venues"])
    for row in rows:
        row["sum_sq_qty"] = "0.25"
    exact = spot_truth(rows, [], [], start, end)
    assert all(v["sign"] == 1 for v in exact["venues"])


def test_exact_usd_decomposition_and_funding_interval_conversion():
    from decimal import localcontext

    from tank.analysis.truth import derivatives_truth

    n0 = "10000000000000000000000000000000000000000.000000000000000001"
    n1 = "10000000000000000000000000000000000000000.000000000000000002"
    d = decomposition(n0, n1, "1.23456789", "1.23456790")
    assert d["position"] == Decimal("0.00000000000000000123456789")
    assert d["interaction"] == Decimal("0.00000000000000000000000001")
    with localcontext() as ctx:
        ctx.prec = 128
        assert d["markup"] == Decimal(n0) * Decimal("0.00000001")
        assert d["position"] + d["markup"] + d["interaction"] == Decimal(n1) * Decimal(
            "1.23456790"
        ) - Decimal(n0) * Decimal("1.23456789")
    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=4)
    iid = "A:perp:T"
    result = derivatives_truth(
        {
            "oi": [{"instrument_id": iid, "ts": iso(end), "oi_native": "1", "oi_usd": "1"}],
            "funding": [
                {
                    "instrument_id": iid,
                    "kind": "SETTLED",
                    "settlement_time": iso(end),
                    "rate": "0.0000000000000000000000000000000000000001",
                    "interval_h": 1,
                }
            ],
        },
        start,
        end,
    )
    assert result["per_venue"][0]["funding_8h_eq"] == Decimal("8E-40")


def test_successful_measurements_have_unit_precision_coverage_and_exact_json_values():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    result = compute_market_truth(snapshot)
    cvd = result["measurement_metadata"]["spot.EVENT.composite_max.cvd"]
    assert cvd["value"] == result["spot"]["EVENT"]["composite_max"]["cvd"]
    assert isinstance(cvd["value"], str)
    assert cvd["unit"] == "NATIVE_TOKEN" and cvd["window"] == "EVENT"
    assert cvd["observed_at"] == result["cutoff"]
    assert cvd["precision"] == "BUCKET(60s)" and cvd["precision_reason"] is None
    assert cvd["coverage"]["n_complete"] == 2 and cvd["reason"] is None
    usd = result["measurement_metadata"]["derivatives.synchronized.decomposition.position"]
    assert usd["unit"] == "USD" and usd["representation"] == "DECIMAL_STRING"
    assert usd["precision"] is None and usd["precision_reason"] == "NOT_ESTIMABLE"
    normal = result["measurement_metadata"]["relative.normal_ranges.W4H.value[0]"]
    assert normal["unit"] == "DECIMAL_RETURN" and normal["window"] == "W4H"
    assert normal["coverage"]["sample_count"] > 0 and normal["observed_at"] is not None


def test_market_truth_json_preserves_exact_large_plus_tiny_native_volume():
    from runpy import run_path

    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    for row in snapshot["flow_bars"]:
        row.update(buy_qty="0", sell_qty="0", sum_sq_qty="0", buy_notional="0", sell_notional="0")
    for venue, qty, squared, usd in [("A", "1E150", "1E300", "1E152"), ("B", "1E-40", "1E-80", "1E-38")]:
        last = max(
            (r for r in snapshot["flow_bars"] if r["instrument_id"] == f"{venue}:spot:T"),
            key=lambda r: r["minute_start"],
        )
        last.update(buy_qty=qty, sum_sq_qty=squared, buy_notional=usd)
    result = compute_market_truth(snapshot)
    expected = "1" + "0" * 150 + "." + "0" * 39 + "1"
    assert format(Decimal(result["spot"]["EVENT"]["composite_max"]["volume"]), "f") == expected
    assert (
        result["measurement_metadata"]["spot.EVENT.composite_max.volume"]["value"]
        == result["spot"]["EVENT"]["composite_max"]["volume"]
    )


def test_position_majority_rule_uses_exact_dollar_terms_not_rounded_display_share():
    position = Decimal("0.5000000000000000000000000000000000000001")
    dec = {"position": position, "markup": Decimal(".5"), "interaction": Decimal(0), "pos_share": 0.5}
    assert oi_class(0.08, 0.1, dec, [-0.01, 0.01], [-0.01, 0.01]) == "REAL_NATIVE"


def exact_selling_spot(epsilon):
    from decimal import localcontext

    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=1)
    rows = []
    with localcontext() as ctx:
        ctx.prec = 128
        for venue, volume, cvd in [
            ("A", Decimal(".5") + epsilon, Decimal("-.2")),
            ("B", Decimal(".5"), Decimal("-.2")),
            ("C", Decimal(".5"), Decimal(".05")),
            ("D", Decimal(".5"), Decimal(".05")),
        ]:
            rows.append(
                {
                    "instrument_id": f"{venue}:spot:T",
                    "minute_start": iso(start),
                    "buy_qty": str((volume + cvd) / 2),
                    "sell_qty": str((volume - cvd) / 2),
                    "sum_sq_qty": "1E-24",
                    "complete": True,
                }
            )
    return spot_truth(rows, [], [], start, end)


def test_exact_capital_majority_drives_distribution_without_rounded_display_rule():
    from tank.analysis.truth import select_move, spot_majority

    tiny = exact_selling_spot(Decimal("1E-40"))
    half = exact_selling_spot(Decimal(0))
    assert tiny["cw_breadth"]["sell"] == half["cw_breadth"]["sell"] == 0.5
    assert tiny["exact_majority"]["sell"] is True
    assert half["exact_majority"]["sell"] is False
    assert tiny["classification"] == "BROAD" and half["classification"] == "DIVERGENT"
    assert spot_majority(tiny, "SELL") is True and spot_majority(half, "SELL") is False
    deriv = {
        "synchronized": None,
        "per_venue": [],
        "incremental_share": None,
        "lead_lag": {"first_mover": "UNKNOWN"},
        "liquidations": {"complete_venues_total": {"long": None, "short": None}},
    }
    liq = {
        "elasticity": {"weak_response": False, "excess_response": False},
        "depth_drop": 0,
        "spread_ratio": 1,
        "liquidity_state": "NORMAL",
        "replenishment": {"bursts": None, "replenished": None, "resilient_bursts": None},
    }
    relative = {"windows": {"W1H": {"r_btc_adj": 0.01, "normal_range": [-0.1, 0.1]}}}
    yes = select_move(tiny, deriv, liq, {"supply_in_evidence": "TRUE"}, relative, "HIGH", "W1H")
    no = select_move(half, deriv, liq, {"supply_in_evidence": "TRUE"}, relative, "HIGH", "W1H")
    # The divergence discriminator retains its precedence; this checks eligibility,
    # not a new override of the existing price-flow rule.
    assert "DISTRIBUTION" in yes["eligible_before_divergence"]
    assert "DISTRIBUTION" not in no["eligible_before_divergence"]


@pytest.mark.parametrize("epsilon,expected", [(Decimal("1E-40"), True), (Decimal(0), False)])
def test_cohort_majority_uses_exact_volume_not_rounded_share(epsilon, expected):
    from decimal import localcontext

    from tank.analysis.truth import enrich_evidence

    end = parse_time("2026-10-16T12:00:00Z")
    with localcontext() as ctx:
        ctx.prec = 128

        def truth(values):
            return {
                "venues": [
                    {"venue": v, "volume_native": n, "cvd": Decimal(0), "sum_sq_qty": Decimal(1), "sign": 0}
                    for v, n in values.items()
                ],
                "composite_max": {"volume": sum(values.values())},
                "direction": "NEUTRAL",
                "composite_cohort": None,
                "cohort_consistent": {},
            }

        spots = {
            "W1H": truth({"A": Decimal(".5") + epsilon, "B": Decimal(".5"), "C": Decimal(1)}),
            "W24H": truth({"A": Decimal(1), "B": Decimal(1)}),
        }
    relative = {
        "ref_venue": None,
        "windows": {"W15M": {"normal_range": None}, "W1H": {"r_raw": None}},
        "context_inputs": {"rv24": None, "rv_normal_range": None},
    }
    enrich_evidence(
        {"cutoff": iso(end)}, end - timedelta(hours=1), end, spots, "W1H", {}, relative, {"per_venue": []}
    )
    comparison = spots["W1H"]["cohort_comparisons"]["W24H"]
    assert comparison["share_of_max"] == 0.5
    assert comparison["share_majority"] is expected
    assert spots["W1H"]["cohort_consistent"]["W24H"] is expected


def test_distribution_cohort_buy_to_sell_does_not_require_current_selling_majority():
    from tank.analysis.truth import select_move

    spot = {
        "direction": "SELL",
        "classification": "DIVERGENT",
        "exact_majority": {"sell": False},
        "cohort_comparisons": {
            "W24H": {"current": {"sign": -1}, "other": {"sign": 1}, "share_majority": True}
        },
    }
    deriv = {
        "synchronized": None,
        "per_venue": [],
        "incremental_share": None,
        "lead_lag": {"first_mover": "UNKNOWN"},
        "liquidations": {"complete_venues_total": {"long": None, "short": None}},
        "support_features": {
            "large_ticket_sell_share_rising": True,
            "ask_depth_building": True,
            "exchange_inflows": True,
        },
    }
    liq = {
        "elasticity": {"weak_response": False, "excess_response": False},
        "depth_drop": 0,
        "spread_ratio": 1,
        "liquidity_state": "NORMAL",
        "replenishment": {"bursts": None, "replenished": None, "resilient_bursts": None},
    }
    relative = {"windows": {"W1H": {"r_btc_adj": 0, "normal_range": [-0.1, 0.1]}}}
    result = select_move(spot, deriv, liq, {"supply_in_evidence": "TRUE"}, relative, "HIGH", "W1H")
    assert result["eligible_support_before_divergence"]["DISTRIBUTION"] == 3
    spot["cohort_comparisons"]["W24H"]["share_majority"] = False
    assert (
        "DISTRIBUTION"
        not in select_move(spot, deriv, liq, {"supply_in_evidence": "TRUE"}, relative, "HIGH", "W1H")[
            "eligible_before_divergence"
        ]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_time,precision,state_verified,kind",
    [
        ("2026-10-16T08:00:00Z", "EXACT", True, "T0"),
        ({"value": "2026-10-16T08:00:00Z", "precision": "EXACT"}, None, True, "T0"),
        ("2026-10-16T08:00:00Z", "EXACT", False, "T1"),
        ({"value": "2026-10-16T08:00:00Z", "precision": "DATE_ONLY"}, None, True, "T1"),
    ],
)
async def test_assessment_calls_snapshot_with_shared_canonical_anchor(
    event_time, precision, state_verified, kind
):
    from types import SimpleNamespace

    from tank.analysis.truth import MarketTruthService
    from tank.market.capture import event_anchor

    event = {
        "event_time": event_time,
        "event_time_prec": precision,
        "state_verified": state_verified,
        "public_at": "2026-10-16T09:00:00Z",
        "known_to_us_at": "2026-10-16T10:00:00Z",
    }

    class Repo:
        async def get(self, *args):
            return event

        async def put(self, *args, **kwargs):
            pass

    class Market:
        calls = []

        async def snapshot(self, token_id, cutoff, anchor=None):
            self.calls.append(anchor)
            return {"token_id": token_id, "cutoff": cutoff, "anchor": anchor}

    market = Market()
    result = await MarketTruthService(
        Repo(), SimpleNamespace(config_version="test", code_version="test"), market
    ).assess("EVT", "T", "2026-10-16T12:00:00Z")
    assert market.calls == [event_anchor(event)]
    assert market.calls[0]["type"] == kind
    assert result["relative"]["anchor"]["ts"] == market.calls[0]["ts"]

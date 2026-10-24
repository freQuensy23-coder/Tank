"""Synthetic deterministic evidence generator for the positive START integration path."""

from copy import deepcopy
from datetime import timedelta
from functools import lru_cache
from math import sin

import pytest

from tank.analysis.truth import compute_market_truth
from tank.util import iso, parse_time


def synthetic_start_snapshot():
    end = parse_time("2026-10-16T12:00:00Z")
    anchor = end - timedelta(hours=4)
    snap = {
        "token_id": "T",
        "event_id": "EVT-SYNTHETIC",
        "cutoff": iso(end),
        "anchor": {"type": "T0", "ts": iso(anchor)},
        "instruments": [],
        "flow_bars": [],
        "klines": [],
        "books": [],
        "oi": [],
        "funding": [],
        "semantics": [],
        "anchor_trades": [],
    }
    for venue in ("A", "B"):
        iid = f"{venue}:spot:T"
        snap["anchor_trades"].append(
            {
                "instrument_id": iid,
                "trade_id": "SYNTHETIC-ANCHOR-" + venue,
                "ts": iso(anchor - timedelta(microseconds=1)),
                "price": 100,
                "received_ts": iso(anchor),
            }
        )
        snap["instruments"].append(
            {"instrument_id": iid, "token_id": "T", "venue": venue, "market_type": "SPOT"}
        )
        for family in ("aggressor", "depth", "price"):
            snap["semantics"].append(
                {
                    "metric_key": f"{venue}.spot.{family}",
                    "metric_family": family,
                    "venue": venue,
                    "market_type": "SPOT",
                    "semantic_confidence": "VERIFIED",
                    "critical": True,
                    "trade_aggregation": "PER_FILL",
                }
            )
        for h in range(-192, 0):
            t = anchor + timedelta(hours=h)
            p = 100 * (1 + 0.02 * sin((h + 1) / 6))
            snap["klines"].append(
                {
                    "instrument_id": iid,
                    "token_id": "T",
                    "open_time": iso(t),
                    "interval_s": 3600,
                    "o": p,
                    "c": p,
                    "quote_volume": 100000,
                    "quote_volume_usd": 100000,
                    "volume_native": 1000,
                    "complete": True,
                }
            )
        for minute in range(-1440, 0):
            t = end + timedelta(minutes=minute)
            snap["flow_bars"].append(
                {
                    "instrument_id": iid,
                    "token_id": "T",
                    "minute_start": iso(t),
                    "buy_qty": 100,
                    "sell_qty": 50,
                    "sum_sq_qty": 1,
                    "buy_notional": 10000,
                    "sell_notional": 5000,
                    "complete": True,
                    "low": 99,
                }
            )
        for minute in range(240):
            t = anchor + timedelta(minutes=minute)
            p = 100 * (1 + 0.011 * (minute + 1) / 240)
            snap["klines"].append(
                {
                    "instrument_id": iid,
                    "token_id": "T",
                    "open_time": iso(t),
                    "interval_s": 60,
                    "o": 100,
                    "c": p,
                    "quote_volume": 15000,
                    "quote_volume_usd": 15000,
                    "volume_native": 150,
                    "complete": True,
                }
            )
        for stamp in (anchor - timedelta(minutes=30), end):
            snap["books"].append(
                {
                    "instrument_id": iid,
                    "token_id": "T",
                    "exchange_ts": iso(stamp),
                    "integrity_ok": True,
                    "mid": 101.1,
                    "spread_bps": 1,
                    "quote_asset": "USD",
                    "range_covered_pct": {"bid": 2, "ask": 2},
                    "depth": {"bid": {"p1": {"usd": 100000000}}, "ask": {"p1": {"usd": 100000000}}},
                    "bids": [[101, 1000000]],
                    "asks": [[101.2, 1000000]],
                }
            )
    btc = "A:spot:BTC"
    for h in range(-192, 0):
        snap["klines"].append(
            {
                "instrument_id": btc,
                "token_id": "BTC",
                "open_time": iso(anchor + timedelta(hours=h)),
                "interval_s": 3600,
                "o": 100,
                "c": 100,
                "quote_volume": 100000,
                "quote_volume_usd": 100000,
                "volume_native": 1000,
                "complete": True,
            }
        )
    for minute in range(240):
        snap["klines"].append(
            {
                "instrument_id": btc,
                "token_id": "BTC",
                "open_time": iso(anchor + timedelta(minutes=minute)),
                "interval_s": 60,
                "o": 100,
                "c": 100,
                "quote_volume": 1000,
                "quote_volume_usd": 1000,
                "volume_native": 10,
                "complete": True,
            }
        )
    iid = "binance:perp:T"
    snap["instruments"].append(
        {"instrument_id": iid, "token_id": "T", "venue": "binance", "market_type": "PERP_LINEAR"}
    )
    for family in ("oi", "funding"):
        snap["semantics"].append(
            {
                "metric_key": f"binance.perp.{family}",
                "metric_family": family,
                "venue": "binance",
                "market_type": "PERP_LINEAR",
                "semantic_confidence": "VERIFIED",
                "critical": True,
                "sidedness": "SINGLE_SIDED",
                "aggregation_class": "oi_native_single_sided",
            }
        )
    for minute in range(-8 * 1440 - 240, 1, 5):
        t = end + timedelta(minutes=minute)
        snap["oi"].append(
            {
                "instrument_id": iid,
                "token_id": "T",
                "ts": iso(t),
                "oi_native": 1000,
                "oi_usd": 100000,
                "metric_key": "binance.perp.oi",
            }
        )
        snap["funding"].append(
            {
                "instrument_id": iid,
                "token_id": "T",
                "kind": "SETTLED",
                "settlement_time": iso(t),
                "rate": 0,
                "interval_h": 8,
                "metric_key": "binance.perp.funding",
            }
        )
    snap["klines"].append(
        {
            "instrument_id": iid,
            "token_id": "T",
            "open_time": iso(end - timedelta(minutes=1)),
            "interval_s": 60,
            "o": 100,
            "c": 101.1,
            "quote_volume": 10000,
            "quote_volume_usd": 10000,
            "volume_native": 100,
            "complete": True,
        }
    )
    anchors = snap.pop("anchor_trades")
    for row in anchors:
        parent = next(
            bar
            for bar in snap["flow_bars"]
            if bar["instrument_id"] == row["instrument_id"]
            and parse_time(bar["minute_start"]) + timedelta(minutes=1) == anchor
        )
        parent["last_trade"] = row
        parent["reference_points"] = [dict(boundary=iso(anchor), complete=True, last_trade=row)]
    for iid in ("A:spot:T", "B:spot:T"):
        parent = next(
            bar
            for bar in snap["flow_bars"]
            if bar["instrument_id"] == iid and parse_time(bar["minute_start"]) + timedelta(minutes=1) == end
        )
        parent["last_trade"] = dict(
            trade_id="SYNTHETIC-END", price="101.1", ts=iso(end - timedelta(microseconds=1))
        )
        parent["reference_points"] = [dict(boundary=iso(end), complete=True, last_trade=parent["last_trade"])]
    return snap


@lru_cache(maxsize=1)
def _compact_start_seed():
    snapshot = synthetic_start_snapshot()

    funding = snapshot["funding"]
    end = parse_time(snapshot["cutoff"])
    boundaries = {end - timedelta(seconds=seconds) for seconds in (0, 900, 3600, 14400, 86400)}
    boundaries.add(parse_time(snapshot["anchor"]["ts"]))
    snapshot["funding"] = [
        row
        for index, row in enumerate(funding)
        if index % 96 == 0 or parse_time(row["settlement_time"]) in boundaries
    ]

    snapshot["oi"] = snapshot["oi"][::3]
    return snapshot


def compact_start_snapshot():

    return deepcopy(_compact_start_seed())


def test_synthetic_start_truth_positive_path() -> None:
    result = compute_market_truth(compact_start_snapshot())
    assert result["data_confidence"]["class"] == "HIGH", result["data_confidence"]
    assert result["move_class"] == "SPOT_LED_ACCUMULATION", result
    assert result["relative"]["repricing_band"] == "EARLY", result["relative"]
    assert result["liquidity"]["liquidity_state"] == "NORMAL"


def test_confidence_diagnostics_do_not_add_unlisted_grade_conditions() -> None:
    snapshot = compact_start_snapshot()
    snapshot["capture_status"] = {"status": "RUNNING", "reason": "BACKFILL_PENDING"}

    for row in snapshot["oi"]:
        row["oi_usd"] = None
    result = compute_market_truth(snapshot)
    assert result["data_confidence"]["class"] == "HIGH", result["data_confidence"]
    assert result["data_confidence"]["derivatives_classification_ready"] is False
    assert result["data_confidence"]["failed"] == []


def test_threshold_configuration_is_consumed_and_isolated_between_calls() -> None:
    snapshot = compact_start_snapshot()

    end = parse_time(snapshot["cutoff"])
    start = end - timedelta(minutes=15)
    snapshot["anchor"] = {"type": "T0", "ts": iso(start)}
    snapshot["instruments"] = [row for row in snapshot["instruments"] if row["market_type"] == "SPOT"]
    spot_ids = {row["instrument_id"] for row in snapshot["instruments"]}
    snapshot["flow_bars"] = [
        row
        for row in snapshot["flow_bars"]
        if row["instrument_id"] in spot_ids and start <= parse_time(row["minute_start"]) < end
    ]
    snapshot["klines"] = [
        row
        for row in snapshot["klines"]
        if row["instrument_id"] in spot_ids and parse_time(row["open_time"]) == end - timedelta(minutes=1)
    ]
    snapshot["books"] = [
        row
        for row in snapshot["books"]
        if row["instrument_id"] in spot_ids and parse_time(row["exchange_ts"]) == end
    ]
    snapshot["semantics"] = [row for row in snapshot["semantics"] if row["market_type"] == "SPOT"]
    snapshot["oi"], snapshot["funding"] = [], []
    strict = compute_market_truth(snapshot, config={"min_spot_venues": 3})
    assert strict["data_confidence"]["class"] == "LOW"
    assert strict["data_confidence"]["limiting"]["condition"] == "spot_coverage"
    neutral = compute_market_truth(snapshot, config={"significant_flow_sigma": 1000000})
    assert neutral["spot"][neutral["primary_window"]]["direction"] == "NEUTRAL"
    baseline = compute_market_truth(snapshot)
    assert baseline["data_confidence"]["class"] == "HIGH"
    assert baseline["spot"][baseline["primary_window"]]["direction"] == "BUY"


@pytest.fixture(scope="module")
def legacy_semantic_baseline():
    snapshot = compact_start_snapshot()
    for row in snapshot["semantics"]:
        row.pop("semantic_confidence", None)
    baseline = compute_market_truth(snapshot)
    assert baseline["data_confidence"]["class"] == "HIGH"

    def fresh():

        return deepcopy((snapshot, baseline))

    return fresh


@pytest.mark.parametrize("status", ["DOCUMENTED_UNTESTED", "UNKNOWN", "INFERRED"])
def test_legacy_semantic_status_cannot_reduce_confidence_or_mask_real_freshness(
    legacy_semantic_baseline, status
) -> None:
    snapshot, baseline = legacy_semantic_baseline()
    for row in snapshot["semantics"]:
        row["semantic_confidence"] = status
    actual = compute_market_truth(snapshot)
    assert actual["data_confidence"] == baseline["data_confidence"]
    assert actual["derivatives"]["synchronized"] == baseline["derivatives"]["synchronized"]
    if status == "INFERRED":
        snapshot["books"] = []
        missing = compute_market_truth(snapshot)
        assert missing["data_confidence"]["class"] == "MEDIUM"
        assert missing["data_confidence"]["limiting"]["condition"] == "freshness"


def test_missing_instrument_registry_and_selftest_records_do_not_cap_observed_confidence() -> None:
    snapshot = compact_start_snapshot()
    end = parse_time(snapshot["cutoff"])
    baseline = compute_market_truth(snapshot)
    assert baseline["data_confidence"]["class"] == "HIGH"

    snapshot["instruments"] = []
    for row in snapshot["semantics"]:
        row["semantic_confidence"] = "DOCUMENTED_UNTESTED"
    observed = compute_market_truth(snapshot)
    assert observed["data_confidence"] == baseline["data_confidence"]
    assert observed["spot"] == baseline["spot"]
    assert observed["derivatives"] == baseline["derivatives"]

    snapshot["books"] = [
        row for row in snapshot["books"] if parse_time(row["exchange_ts"]) < end - timedelta(seconds=10)
    ]
    stale = compute_market_truth(snapshot)
    assert stale["data_confidence"]["class"] == "MEDIUM"
    assert stale["data_confidence"]["limiting"]["condition"] == "freshness"
    assert stale["data_confidence"]["limiting"]["collection"] == "books"


async def _assess_loser_snapshot():
    from types import SimpleNamespace

    from tank.analysis.truth import MarketTruthService

    class Repo:
        async def get(self, *args):
            return {"public_at": "2026-10-16T08:00:00Z"}

        async def list(self, *args, **kwargs):
            return [
                {"map_version": 1, "edge_id": "old", "role": "PRIMARY"},
                {"map_version": 2, "edge_id": "new", "role": "LOSER"},
            ]

        async def put(self, collection, key, record, **kwargs) -> None:
            assert collection == "ana.market_truth" and kwargs["immutable"] is True

    class Market:
        async def snapshot(self, token_id, cutoff, anchor=None):
            return compact_start_snapshot()

    return await MarketTruthService(
        Repo(), SimpleNamespace(config_version="test", code_version="test"), Market()
    ).assess("EVT", "T", "2026-10-16T12:00:00Z")


def test_latest_durable_beneficiary_role_sets_loser_repricing_direction() -> None:
    import asyncio

    result = asyncio.run(_assess_loser_snapshot())
    assert result["relative"]["expected_direction"] == "LOSER"
    assert result["relative"]["repricing_sign"] == -1
    assert result["relative"]["repricing_band"] == "UNPRICED"
    assert compute_market_truth(compact_start_snapshot())["relative"]["repricing_band"] == "EARLY"

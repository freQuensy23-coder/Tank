"""TDD §0 timestamp siblings distinguish chosen bounds from source provenance."""

from types import SimpleNamespace

import pytest
from test_truth_start_fixture import synthetic_start_snapshot

from tank.analysis.truth import MarketTruthService, compute_market_truth
from tank.util import parse_time


def packet():
    value = synthetic_start_snapshot()
    # Precision propagation needs concrete source endpoints, not eight days
    # of observations for unrelated confidence/normal-range classification.
    keep = {"A:spot:T", "binance:perp:T"}
    value["instruments"] = [r for r in value["instruments"] if r["instrument_id"] in keep]
    value["semantics"] = [r for r in value["semantics"] if r["venue"] in {"A", "binance"}]
    value["flow_bars"] = [
        r for r in value["flow_bars"] if r["instrument_id"] == "A:spot:T" and r.get("reference_points")
    ]
    value["klines"] = [
        r
        for r in value["klines"]
        if r["instrument_id"] in keep
        and parse_time(r["open_time"])
        in {parse_time("2026-10-16T08:00:00Z"), parse_time("2026-10-16T11:59:00Z")}
    ]
    for collection, stamp in (("oi", "ts"), ("funding", "settlement_time")):
        value[collection] = [
            r
            for r in value[collection]
            if parse_time(r[stamp])
            in {parse_time("2026-10-16T08:00:00Z"), parse_time("2026-10-16T12:00:00Z")}
        ]
    value["books"] = [r for r in value["books"] if r["instrument_id"] == "A:spot:T"]
    value["anchor"]["ts_prec"] = "BUCKET(1h)"
    value["assembled_at"] = "2026-10-16T12:00:02Z"
    value["assembled_at_prec"] = "COLLECTION_ONLY"
    for row in value["funding"]:
        row["settlement_time_prec"] = "EXACT"
    for row in value["klines"]:
        row["open_time_prec"] = f"BUCKET({row['interval_s']}s)"
    return value


def test_pure_truth_selected_bounds_exact_and_source_precision_propagated():
    from copy import deepcopy

    snapshot = packet()
    original = deepcopy(snapshot)
    result = compute_market_truth(snapshot)
    assert snapshot == original
    assert result["cutoff_prec"] == "EXACT"
    assert result["requested_cutoff_prec"] == "EXACT"
    assert result["assembled_at_prec"] == "COLLECTION_ONLY"
    assert result["relative"]["anchor"]["ts_prec"] == "BUCKET(1h)"
    assert result["relative"]["windows"]["EVENT"]["start_prec"] == "BUCKET(1h)"
    assert result["relative"]["windows"]["EVENT"]["cutoff_prec"] == "EXACT"
    venue = result["derivatives"]["per_venue"][0]
    assert venue["funding_settled_last_observed_at_prec"] == "EXACT"
    assert venue["funding_start_observed_at_prec"] == "EXACT"
    assert result["relative"]["anchor"]["price_observed_at_prec"] is None
    assert result["relative"]["anchor"]["price_observed_at_prec_reason"] == "INCOMPLETE"


@pytest.mark.asyncio
async def test_actual_service_preserves_event_public_and_anchor_precision_without_iso_inference():
    event = {"public_at": "2026-10-16T08:00:00Z", "public_at_prec": "BUCKET(1h)"}

    class Repo:
        async def get(self, *args):
            return event

        async def put(self, *args, **kwargs):
            pass

    class Market:
        async def snapshot(self, token_id, cutoff, anchor=None):
            value = packet()
            value["anchor"] = anchor
            return value

    result = await MarketTruthService(
        Repo(), SimpleNamespace(config_version="t", code_version="t"), Market()
    ).assess("E", "T", "2026-10-16T12:00:00Z")
    assert result["event_public_at_prec"] == "BUCKET(1h)"
    assert result["relative"]["anchor"]["ts_prec"] == "BUCKET(1h)"
    del event["public_at_prec"]
    legacy = await MarketTruthService(
        Repo(), SimpleNamespace(config_version="t", code_version="t"), Market()
    ).assess("E", "T", "2026-10-16T12:00:00Z")
    assert legacy["event_public_at_prec"] is None
    assert legacy["relative"]["anchor"]["ts_prec"] is None


@pytest.mark.parametrize("precision", ["BUCKET(1h)", None])
def test_supplied_uncertain_cutoff_and_offset_window_bounds_keep_uncertainty(precision):
    snapshot = packet()
    snapshot["cutoff_prec"] = precision
    snapshot["cutoff_prec_reason"] = "INCOMPLETE" if precision is None else None
    result = compute_market_truth(snapshot)
    assert result["cutoff_prec"] == precision
    assert result["requested_cutoff_prec"] == precision
    assert result["relative"]["windows"]["W1H"]["start_prec"] == precision
    assert result["relative"]["windows"]["W1H"]["cutoff_prec"] == precision
    assert result["analysis_window"]["end_prec"] == precision


@pytest.mark.integration
def test_actual_book_canonical_exchange_timestamp_reaches_liquidity_precision():
    from tank.market.books import OrderBook

    snapshot = packet()
    iid = "A:spot:T"
    instrument = {
        "instrument_id": iid,
        "token_id": "T",
        "venue": "coinbase",
        "venue_symbol": "T-USD",
        "market_type": "SPOT",
        "quote_asset": "USD",
    }
    book = OrderBook(instrument)
    book.snapshot([["100", "1000000"]], [["100.1", "1000000"]], ts=snapshot["cutoff"])
    summary = book.summary(received_ts=snapshot["assembled_at"])
    assert summary["exchange_ts_prec"] == "EXACT"
    snapshot["books"] = [row for row in snapshot["books"] if row["instrument_id"] != iid] + [summary]
    result = compute_market_truth(snapshot)
    observed = next(row for row in result["liquidity"]["per_venue"] if row["venue"] == "A")
    assert observed["precision"] == "EXACT"

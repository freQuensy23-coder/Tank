"""funding_confirmations matches the pairwise scan it replaced, without parsing every pair."""

import random
from datetime import timedelta

import pytest

from tank.market import service
from tank.market.service import funding_confirmations
from tank.util import iso, parse_time


def pairwise(observations, distinct_paths):
    """The replaced quadratic scan (market/service.py before 21 Oct 2026), kept as the oracle."""
    pairs = []
    for rest in observations:
        if rest.get("capture_source") != "PUBLIC_REST" or rest.get("kind") != "CURRENT_ESTIMATE":
            continue
        matching = [
            ws
            for ws in observations
            if ws.get("instrument_id") == rest.get("instrument_id")
            and ws.get("capture_source") == "WEBSOCKET"
            and ws.get("kind") == rest.get("kind")
            and (not distinct_paths or (ws.get("source_path") and rest.get("source_path")))
            and (not distinct_paths or ws["source_path"] != rest["source_path"])
            and abs((parse_time(ws["as_of"]) - parse_time(rest["as_of"])).total_seconds()) <= 60
        ]
        if matching:
            ws = min(
                matching,
                key=lambda row: abs((parse_time(row["as_of"]) - parse_time(rest["as_of"])).total_seconds()),
            )
            pairs.append((rest, ws))
    return pairs


def observations(seed):
    rng = random.Random(seed)
    start = parse_time("2026-10-21T00:00:00Z")
    rows = []
    for _ in range(400):
        rows.append(
            {
                "instrument_id": rng.choice(["binance:perp_linear:BTCUSDT", "okx:perp_linear:BTC-USDT-SWAP"]),
                "capture_source": rng.choice(["WEBSOCKET", "WEBSOCKET", "PUBLIC_REST"]),
                "kind": rng.choice(["CURRENT_ESTIMATE", "CURRENT_ESTIMATE", "SETTLED"]),
                "as_of": iso(start + timedelta(seconds=15 * rng.randrange(0, 240))),
                "source_path": rng.choice(["ws:markPrice", "rest:premiumIndex", None]),
            }
        )
    return rows


def test_indexed_confirmations_equal_the_pairwise_scan_including_ties_and_edges() -> None:
    for seed in range(20):
        rows = observations(seed)
        for distinct in (False, True):
            got = funding_confirmations(rows, distinct_paths=distinct)
            want = pairwise(rows, distinct)
            assert [(id(a), id(b)) for a, b in got] == [(id(a), id(b)) for a, b in want]


def test_each_websocket_row_is_parsed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = observations(7)
    calls = []
    real = service.parse_time
    monkeypatch.setattr(service, "parse_time", lambda value: calls.append(value) or real(value))
    funding_confirmations(rows)
    websocket = sum(1 for r in rows if r["capture_source"] == "WEBSOCKET" and r["kind"] == "CURRENT_ESTIMATE")
    rest = sum(1 for r in rows if r["capture_source"] == "PUBLIC_REST" and r["kind"] == "CURRENT_ESTIMATE")
    assert len(calls) == websocket + rest

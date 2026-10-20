"""Retained real Kraken pages: quiet prefix is not evidence of missing trades."""

import base64
import gzip
import hashlib
import json
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from test_market_service import setup as setup

from tank.market.adapters import VenueAdapter
from tank.market.service import MarketService
from tank.util import parse_time


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "truncated_prefix", "partial_end", "interior_gap", "ts_disorder"])
async def test_retained_kraken_forward_range_requires_boundary_and_terminal_proof(setup, mode):
    fixture_path = Path(__file__).with_name("fixtures") / "market/kraken-qnt-quiet-start-20260923.json.gz"
    fixture = json.loads(gzip.decompress(fixture_path.read_bytes()))
    cache = {}
    for request in fixture["requests"]:
        blob = base64.b64decode(fixture["bodies"][request["body_sha256"]])
        assert hashlib.sha256(blob).hexdigest() == request["body_sha256"]
        assert len(blob) == request["body_bytes"]
        assert request["exact_response_arrival"] is None
        assert request["request_started_at"] and request["body_persisted_no_later_than"]
        cache[request["url"]] = json.loads(blob)
    shifted = {}
    if mode == "ts_disorder":
        # Two consecutive trade IDs of one minute whose timestamps run backwards: ID
        # continuity is still complete, the audit must not read it as a gap.
        rows = {
            int(r[6]): r
            for request in fixture["requests"]
            if "/Trades?" in request["url"]
            for pair, page in cache[request["url"]]["result"].items()
            if pair != "last"
            for r in page
        }
        trade_id = next(
            i
            for i in sorted(rows)[100:]
            if i + 1 in rows
            and int(float(rows[i][2]) // 60) == int((float(rows[i + 1][2]) + 0.001) // 60)
            and float(rows[i][2]) < float(rows[i + 1][2])
        )
        shifted[trade_id] = float(rows[trade_id + 1][2]) + 0.001
    repo, settings, original = setup
    instrument = dict(original, instrument_id="kraken:spot:QNT/USD", venue="kraken", venue_symbol="QNT/USD")
    cutoff = parse_time("2026-09-24T20:15:00Z")
    requests = []

    async def request(method, url, *, params):
        key = str(httpx.URL(url, params=params))
        requests.append(key)
        body = deepcopy(cache[key])  # An unknown/unfinished cursor cannot reach the network.
        if "/Trades?" in key:
            for pair, rows in body["result"].items():
                if pair == "last":
                    continue
                if mode == "truncated_prefix":
                    body["result"][pair] = [r for r in rows if r[6] != 1788055]
                elif mode == "interior_gap":
                    body["result"][pair] = [r for r in rows if r[6] != 1789000]
                elif mode == "partial_end":
                    body["result"][pair] = [r for r in rows if parse_time(r[2]) < cutoff]
                for r in body["result"][pair]:
                    if int(r[6]) in shifted:
                        r[2] = type(r[2])(shifted[int(r[6])])
        return body

    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service.adapter = lambda inst, **kwargs: VenueAdapter(inst, request)

        async def no_klines(*args, **kwargs):
            pass

        service._backfill_klines = no_klines
        await service.backfill(instrument, cutoff)
        bars = await repo.list("mkt.flow_bars_1m", limit=2000)
        gaps = await repo.list("mkt.capture_gaps", limit=1000)
        if mode in ("full", "ts_disorder"):
            assert len(bars) == 1440 and all(r["complete"] for r in bars)
            assert not [g for g in gaps if g["kind"] == "PRE_EVENT_FLOW"]
            first = min((r for r in bars if r.get("first_trade")), key=lambda r: r["minute_start"])
            assert first["minute_start"] == "2026-09-23T20:15:00Z"
            assert first["first_trade"]["trade_id"] == "1788055"
            reference = service.flow.references[(instrument["instrument_id"], "2026-09-23T20:15:00Z")]
            assert reference["last_trade"]["trade_id"] == "1788054"
        elif mode == "interior_gap":
            assert len(bars) == 1440 and any(not r["complete"] for r in bars)
            assert any(r.get("gap_reason") == "REST_TRADE_ID_GAP" for r in bars)
        else:
            assert not bars
            assert any(g["kind"] == "PRE_EVENT_FLOW" for g in gaps)
        assert requests

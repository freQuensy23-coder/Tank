import asyncio
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tank.market import flow, normalize, streams
from tank.time_precision import collection_clock
from tank.util import iso, parse_time


def instrument():
    return {
        "instrument_id": "binance:spot:ABCUSD",
        "token_id": "test:ABC",
        "venue": "binance",
        "market_type": "SPOT",
        "venue_symbol": "ABCUSD",
        "quote_asset": "USD",
    }


def test_batched_flow_matches_native_bars_segments_and_reference_points_without_discarded_snapshots(
    monkeypatch,
):
    at = parse_time("2026-10-20T00:00:00Z")
    stamp = collection_clock(at)
    monkeypatch.setattr(flow, "collection_clock", lambda *a, **k: stamp)
    rows = [
        normalize.trade(
            instrument(),
            {
                "a": i + 1,
                "p": str(10 + i % 3),
                "q": "0.123456789",
                "T": int(at.timestamp() * 1000) + i * 25,
                "m": bool(i % 2),
            },
        )
        for i in range(2000)
    ]
    regular, batched = flow.FlowAccumulator(), flow.FlowAccumulator()
    for acc in (regular, batched):
        acc.register_boundaries(
            instrument()["instrument_id"], [at + timedelta(seconds=15), at + timedelta(seconds=30)]
        )
        acc.prove_range(
            instrument()["instrument_id"], at, at + timedelta(minutes=1), complete=True, source="TEST"
        )
    for row in rows:
        regular.add(row, continuity=False)
    key = (instrument()["instrument_id"], iso(at))
    expected = regular.record(key, complete=True)
    assert expected["n_buy"] == expected["n_sell"] == 1000
    assert Decimal(expected["buy_qty"]) == Decimal("0.123456789") * 1000
    assert Decimal(expected["sum_sq_qty"]) == Decimal("0.123456789") ** 2 * 2000
    assert [p["last_trade"]["trade_id"] for p in expected["reference_points"]] == ["600", "1200"]
    calls = []
    original = flow.FlowAccumulator.record

    def counted(self, *a, **kw):
        calls.append(kw.get("segments", True))
        return original(self, *a, **kw)

    monkeypatch.setattr(flow.FlowAccumulator, "record", counted)
    for row in rows:
        assert batched.add(row, continuity=False, emit_record=False) is None
    assert batched.record(key, complete=True) == expected
    assert len(calls) < 20


async def test_stream_frame_does_not_run_whole_flow_flush(monkeypatch):
    endpoint = "wss://stream.binance.com:9443/ws"
    inst = instrument()
    service = SimpleNamespace(
        _stop=asyncio.Event(),
        _stream_controls={endpoint: asyncio.Queue()},
        _stream_groups={endpoint: {inst["instrument_id"]}},
        instruments={inst["instrument_id"]: inst},
        thresholds={},
        books={},
        _connected_since={},
        _stream_watermarks={},
        _raw=AsyncMock(),
        _gap=AsyncMock(),
        mark_capture_gap=AsyncMock(),
        _flush_flow=AsyncMock(),
    )

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def send(self, data):
            pass

        async def recv(self):
            await asyncio.sleep(0.01)
            return '{"e":"aggTrade","s":"ABCUSD","a":1,"p":"1","q":"1","m":false,"T":1}'

    async def process(market, inst, payload):
        market._stop.set()

    monkeypatch.setattr("websockets.connect", lambda *a, **kw: Socket())
    monkeypatch.setattr(streams, "prepare_member", AsyncMock())
    handled = AsyncMock(side_effect=process)
    monkeypatch.setattr(streams, "process_frame", handled)
    await asyncio.wait_for(streams.stream_group_loop(service, endpoint), 2)
    handled.assert_awaited_once()
    service._flush_flow.assert_not_awaited()

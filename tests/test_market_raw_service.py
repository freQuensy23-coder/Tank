"""Trade bytes are represented in normalized Parquet, never frame journals."""

# ruff: noqa: F811
import json

import httpx
import pytest
from test_market_service import setup  # noqa: F401

from tank.market import normalize
from tank.market.service import MarketService


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "frame",
    [
        {"stream": "abcusdt@aggTrade", "data": {"e": "aggTrade", "a": 1}},
        {"type": "match", "trade_id": 1},
        {"arg": {"channel": "trades"}, "data": [{"tradeId": "1"}]},
        {"topic": "publicTrade.ABCUSDT", "data": [{"i": "1"}]},
        {"channel": "trade", "data": [{"trade_id": 1}]},
        {"channel": "trades", "data": [{"tid": 1}]},
    ],
)
async def test_six_venue_trade_frames_are_not_duplicated_in_raw_frame_journals(setup, frame):
    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        await service._raw(inst, "websocket", frame)
        await service.flush_market()
        assert not await repo.list("mkt.raw_archives")
        assert not list(settings.data_dir.rglob("*.jsonl"))


@pytest.mark.asyncio
async def test_backfill_source_payload_is_kept_by_normalized_trade_without_frame_copy(setup):
    repo, settings, inst = setup
    rows = [{"a": 1, "p": "12.12345678901234567890", "q": "2", "m": False, "T": 1781654400000}]
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        await service._raw(inst, "backfill_trades", {"request": {"fromId": 1}, "response": rows})
        await service.flush_market()
        assert not await repo.list("mkt.raw_archives")
        assert not (settings.data_dir / "raw/market_frames").exists()
        point = normalize.trade(inst, rows[0], source="REST")
        assert json.loads(point["source_payload_json"]) == rows[0]
        assert point["price"] == rows[0]["p"]


@pytest.mark.asyncio
async def test_coinbase_initial_last_match_retained_only_as_parquet_without_live_flow_claim(setup):
    from tank.market.archive import RawArchive

    repo, settings, original = setup
    inst = dict(original, venue="coinbase", instrument_id="coinbase:spot:ABC-USD", venue_symbol="ABC-USD")
    frame = {
        "type": "last_match",
        "trade_id": 123,
        "price": "12.345678901234567890",
        "size": "2",
        "side": "sell",
        "time": "2026-10-17T12:00:00Z",
    }
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service.instruments[inst["instrument_id"]] = inst
        service.trade_archive.instruments[inst["instrument_id"]] = inst
        await service._raw(inst, "websocket", frame)
        assert not service.flow.bars
        assert not (settings.data_dir / "raw/market_frames").exists()
        manifests = service.trade_archive.flush()
    assert len(manifests) == 1 and manifests[0]["path"].endswith(".parquet")
    rows = RawArchive.read_verified(manifests[0]["path"], manifests[0]["sha256"])
    assert rows[0]["capture_source"] == "WS_INITIAL_SNAPSHOT"
    assert json.loads(rows[0]["source_payload_json"]) == frame
    assert not list((settings.data_dir / "raw/trades").rglob("*.jsonl"))

import asyncio
import json
import time

import httpx
import pytest
import yaml

from tank.market.books import OrderBook
from tank.market.service import MarketService
from tank.market.streams import endpoint_key, endpoint_keys, route_frame, subscription_messages
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import utcnow


def binance_spot_service(tmp_path, monkeypatch):
    """Two Binance spot products behind fake sockets; returns (service, repo, http, instruments, sockets)."""
    from tank.market import streams

    config = tmp_path / "config"
    config.mkdir()
    instruments = [
        {
            "instrument_id": "binance:spot:" + s,
            "token_id": t,
            "venue": "binance",
            "market_type": "SPOT",
            "venue_symbol": s,
            "quote_asset": "USD",
            "active": True,
        }
        for s, t in [("ABCUSD", "test:ABC"), ("BTCUSD", "bitcoin:BTC")]
    ]
    (config / "registry.yaml").write_text(
        yaml.safe_dump(
            {
                "tokens": [
                    {"token_id": "test:ABC", "symbol": "ABC"},
                    {"token_id": "bitcoin:BTC", "symbol": "BTC"},
                ],
                "instruments": instruments,
            }
        )
    )
    (config / "semantics.yaml").write_text("metrics: []\n")
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", replay=False)
    repo = MemoryRepository()
    http = httpx.AsyncClient()
    service = MarketService(repo, settings, http=http)
    service._stop = asyncio.Event()
    sockets = []

    class Socket:
        def __init__(self):
            self.messages = []
            self.incoming = asyncio.Queue()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def send(self, message):
            self.messages.append(json.loads(message))

        async def recv(self):
            return await self.incoming.get()

    def connect(url, **kwargs):
        assert kwargs["max_size"] == 32 * 1024 * 1024
        socket = Socket()
        sockets.append((url, socket))
        return socket

    async def prepare(market, instrument, **kwargs):
        iid = instrument["instrument_id"]
        market.books[iid] = OrderBook(instrument)
        market._connected_since[iid] = utcnow()

    monkeypatch.setattr("websockets.connect", connect)
    monkeypatch.setattr(streams, "prepare_member", prepare)
    return service, repo, http, instruments, sockets


async def settle(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_multiplexed_endpoint_incremental_hot_membership_without_reconnect(tmp_path, monkeypatch):
    service, repo, http, instruments, sockets = binance_spot_service(tmp_path, monkeypatch)
    await repo.put(
        "mkt.hot_tokens",
        "test:ABC:e",
        {"token_id": "test:ABC", "event_id": "e", "priority": 1, "since": "2026-01-01T00:00:00Z"},
    )
    await service._sync_streams()
    await settle(lambda: bool(sockets) and bool(sockets[0][1].messages))
    original = next(iter(service.streams.values()))
    socket = sockets[0][1]
    assert socket.messages[0]["method"] == "SUBSCRIBE" and "abcusd@aggTrade" in socket.messages[0]["params"]
    await repo.put(
        "mkt.hot_tokens",
        "bitcoin:BTC:e",
        {"token_id": "bitcoin:BTC", "event_id": "e", "priority": 1, "since": "2026-01-01T00:00:00Z"},
    )
    await service._sync_streams()
    await settle(lambda: any("btcusd@aggTrade" in m.get("params", []) for m in socket.messages))
    assert len(sockets) == 1 and next(iter(service.streams.values())) is original
    await service.demote("test:ABC", "e")
    await settle(
        lambda: any(
            m["method"] == "UNSUBSCRIBE" and "abcusd@aggTrade" in m["params"] for m in socket.messages
        )
    )
    assert len(sockets) == 1 and service._stream_groups[endpoint_key(instruments[0])] == {
        "binance:spot:BTCUSD"
    }
    service._stop.set()
    await asyncio.wait_for(original, 2)
    await http.aclose()


@pytest.mark.asyncio
async def test_queued_frames_drain_without_an_event_loop_turn_each(tmp_path, monkeypatch):
    from tank.market import streams

    service, repo, http, _, sockets = binance_spot_service(tmp_path, monkeypatch)
    processed = []

    async def count(market, instrument, raw):
        processed.append(raw)

    monkeypatch.setattr(streams, "process_frame", count)
    await repo.put(
        "mkt.hot_tokens",
        "test:ABC:e",
        {"token_id": "test:ABC", "event_id": "e", "priority": 1, "since": "2026-01-01T00:00:00Z"},
    )
    await service._sync_streams()
    await settle(lambda: bool(sockets) and bool(sockets[0][1].messages))
    turns = [0]

    async def hog():
        while True:
            turns[0] += 1
            time.sleep(0.01)
            await asyncio.sleep(0)

    hogging = asyncio.create_task(hog())
    for n in range(300):
        frame = {"stream": "abcusd@aggTrade", "data": {"e": "aggTrade", "s": "ABCUSD", "a": n}}
        sockets[0][1].incoming.put_nowait(json.dumps(frame))
    await settle(lambda: len(processed) == 300)
    assert turns[0] < 30
    hogging.cancel()
    service._stop.set()
    await asyncio.wait_for(next(iter(service.streams.values())), 2)
    await http.aclose()


def test_nine_paths_use_nine_routed_connections_and_global_liquidation_is_singleton():
    instruments = [
        {"venue": v, "market_type": m, "venue_symbol": s}
        for v, m, s in [
            ("binance", "SPOT", "BTCUSDT"),
            ("binance", "PERP_LINEAR", "BTCUSDT"),
            ("coinbase", "SPOT", "BTC-USD"),
            ("okx", "SPOT", "BTC-USDT"),
            ("okx", "PERP_LINEAR", "BTC-USDT-SWAP"),
            ("bybit", "SPOT", "BTCUSDT"),
            ("bybit", "PERP_LINEAR", "BTCUSDT"),
            ("kraken", "SPOT", "XBT/USD"),
            ("hyperliquid", "PERP_LINEAR", "BTC"),
        ]
    ]
    assert len({endpoint_key(i) for i in instruments}) == 8
    assert len({endpoint for i in instruments for endpoint in endpoint_keys(i)}) == 9
    okx = [instruments[3], instruments[4], dict(instruments[4], venue_symbol="ETH-USDT-SWAP")]
    messages = subscription_messages(okx)
    assert len(messages) == 1 and not any(a["channel"] == "liquidation-orders" for a in messages[0]["args"])
    assert len(messages[0]["args"]) == 14


def test_multimarket_frames_route_by_exact_instrument_identity():
    instruments = [
        {"venue": "okx", "market_type": m, "venue_symbol": s}
        for m, s in [("SPOT", "BTC-USDT"), ("PERP_LINEAR", "BTC-USDT-SWAP")]
    ]
    packet = {
        "arg": {"channel": "liquidation-orders", "instType": "SWAP"},
        "data": [
            {"instId": "BTC-USDT-SWAP", "details": [{"side": "sell"}]},
            {"instId": "ETH-USDT-SWAP", "details": []},
        ],
    }
    routed = list(route_frame(instruments, packet))
    assert (
        len(routed) == 1 and routed[0][0]["market_type"] == "PERP_LINEAR" and len(routed[0][1]["data"]) == 1
    )
    bybit = [
        {"venue": "bybit", "market_type": "SPOT", "venue_symbol": "BTCUSDT"},
        {"venue": "bybit", "market_type": "SPOT", "venue_symbol": "BTCUSDC"},
    ]
    assert len(list(route_frame(bybit, {"topic": "publicTrade.BTCUSDT", "data": []}))) == 1


@pytest.mark.asyncio
async def test_independent_rest_confirmation_is_background_and_does_not_stall_book_capture(tmp_path):
    from tank.util import iso

    config = tmp_path / "config"
    config.mkdir()
    instrument = {
        "instrument_id": "coinbase:spot:ABC-USD",
        "token_id": "test:ABC",
        "venue": "coinbase",
        "market_type": "SPOT",
        "venue_symbol": "ABC-USD",
        "quote_asset": "USD",
        "active": True,
        "metadata_status": "LIVE_VERIFIED",
    }
    (config / "registry.yaml").write_text(
        yaml.safe_dump({"tokens": [{"token_id": "test:ABC", "symbol": "ABC"}], "instruments": [instrument]})
    )
    (config / "semantics.yaml").write_text("metrics: []\n")
    repo = MemoryRepository()
    http = httpx.AsyncClient()
    service = MarketService(
        repo, Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data"), http=http
    )
    book = OrderBook(instrument)
    book.snapshot([["99", "1"]], [["101", "1"]], sequence=1, ts=iso())
    service.books[instrument["instrument_id"]] = book
    release = asyncio.Event()

    async def independent(inst, row):
        await release.wait()
        return {"second_path_confirmed": True, "second_path_reason": None}

    service._independent_book = independent
    await asyncio.wait_for(service._save_book(instrument), 0.2)
    await service.flush_market()
    rows = await repo.list("mkt.book_summaries")
    assert len(rows) == 1 and not rows[0]["second_path_confirmed"]
    release.set()
    await asyncio.gather(*service._background)
    await service.flush_market()
    assert (await repo.list("mkt.book_summaries"))[0]["second_path_confirmed"]
    await http.aclose()

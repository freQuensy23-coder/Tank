"""Actual Binance routed-public depth contract and independent stream ownership."""

# ruff: noqa: F811
import asyncio
import json
import time
from datetime import timedelta

import httpx
import pytest
from test_market_service import setup  # noqa: F401

from tank.market.service import MarketService
from tank.market.streams import (
    connection,
    endpoint_keys,
    prepare_member,
    record_stream_watermark,
    stream_group_loop,
    subscription_messages,
)
from tank.util import iso, utcnow

DEPTH = "wss://fstream.binance.com/public/stream"
REGULAR = "wss://fstream.binance.com/market/stream"


def perp(inst, symbol="BTCUSDT"):
    return dict(
        inst, instrument_id="binance:perp_linear:" + symbol, market_type="PERP_LINEAR", venue_symbol=symbol
    )


def test_usdm_channels_use_official_routed_endpoints():
    instruments = [perp({"venue": "binance"}, symbol) for symbol in ("BTCUSDT", "ETHUSDT")]
    assert endpoint_keys(instruments[0]) == (REGULAR, DEPTH)
    assert "@depth" not in connection(instruments[0])[0]
    assert connection(instruments[0], depth_only=True)[0] == DEPTH + "?streams=btcusdt@depth@100ms"
    regular = subscription_messages(instruments, endpoint=REGULAR)[0]["params"]
    books = subscription_messages(instruments, endpoint=DEPTH)[0]["params"]
    assert books == ["btcusdt@depth@100ms", "ethusdt@depth@100ms"]
    assert len(regular) == 6 and all("@depth" not in topic for topic in regular)
    assert subscription_messages(instruments, endpoint=DEPTH, subscribe=False)[0]["method"] == "UNSUBSCRIBE"


@pytest.mark.asyncio
async def test_shared_futures_routes_follow_hot_membership_once_per_endpoint(setup):
    repo, settings, inst = setup
    service = MarketService(repo, settings)
    btc, eth = perp(inst), perp(inst, "ETHUSDT")
    eth["token_id"] = "ethereum:ETH"
    service.instruments = {i["instrument_id"]: i for i in (btc, eth)}
    service._last_hot_expiry_refresh = time.monotonic()
    for token in (inst["token_id"], "ethereum:ETH"):
        await repo.put_market_batch(
            "mkt.hot_tokens",
            [{"event_id": "fixture", "token_id": token, "since": iso(), "priority": 0}],
            keys=[token],
        )
    await service._sync_streams()
    assert set(service._stream_groups) == {REGULAR, DEPTH}
    assert all(group == set(service.instruments) for group in service._stream_groups.values())
    await repo.delete("mkt.hot_tokens", "ethereum:ETH")
    await service._sync_streams()
    assert all(group == {btc["instrument_id"]} for group in service._stream_groups.values())
    await service.close()


@pytest.mark.asyncio
async def test_depth_initialization_owns_book_only_and_regular_reconnect_preserves_it(setup):
    repo, settings, inst = setup
    instrument = perp(inst)
    calls = []

    def request(req):
        calls.append(req.url.path)
        return httpx.Response(
            200,
            json={
                "lastUpdateId": 100,
                "bids": [["99", "1"]],
                "asks": [["101", "1"]],
                "E": int(utcnow().timestamp() * 1000),
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as http:
        service = MarketService(repo, settings, http=http)
        iid = instrument["instrument_id"]
        service.instruments = {iid: instrument}
        service._metadata.add(iid)
        await prepare_member(service, instrument, depth_only=True)
        assert calls == ["/fapi/v1/depth"] and service.books[iid].sequence == 100
        assert iid not in service._connected_since and not service._background
        book = service.books[iid]
        # First connection of a hot token: backfill covers its anchor up to the socket.
        service._hot_since[instrument["token_id"]] = iso(utcnow() - timedelta(minutes=5))
        await prepare_member(service, instrument)
        assert service.books[iid] is book and calls == ["/fapi/v1/depth"]
        assert iid in service._connected_since and len(service._background) == 1
        for task in service._background:
            task.cancel()
        await asyncio.gather(*service._background, return_exceptions=True)
        record_stream_watermark(service, instrument, {"e": "aggTrade", "E": int(utcnow().timestamp() * 1000)})
        before = dict(service._stream_watermarks[iid])
        record_stream_watermark(
            service,
            instrument,
            {"e": "depthUpdate", "E": int((utcnow() + timedelta(seconds=1)).timestamp() * 1000)},
        )
        assert service._stream_watermarks[iid] == before
        await service.close()


@pytest.mark.asyncio
async def test_depth_socket_failure_preserves_trade_connection_and_watermark(setup, monkeypatch):
    repo, settings, inst = setup
    instrument = perp(inst)
    iid = instrument["instrument_id"]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda req: httpx.Response(
                200, json={"lastUpdateId": 100, "bids": [["99", "1"]], "asks": [["101", "1"]]}
            )
        )
    ) as http:
        service = MarketService(repo, settings, http=http)
        service.instruments = {iid: instrument}
        service._metadata.add(iid)
        since = utcnow() - timedelta(seconds=10)
        service._connected_since[iid] = since
        service._stream_watermarks[iid] = {"ts": iso(since), "received_ts": iso()}
        before = dict(service._stream_watermarks[iid])
        service._stop = asyncio.Event()
        service._stream_groups[DEPTH] = {iid}
        service._stream_controls[DEPTH] = asyncio.Queue()

        class Socket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def send(self, payload):
                assert json.loads(payload)["params"] == ["btcusdt@depth@100ms"]

            async def recv(self):
                await asyncio.sleep(0.01)
                raise RuntimeError("fixture public-depth outage")

        monkeypatch.setattr("websockets.connect", lambda *args, **kwargs: Socket())
        original = service._gap

        async def gap(inst, kind, reason, **kwargs):
            assert kind == "ORDER_BOOK_STREAM"
            result = await original(inst, kind, reason, **kwargs)
            service._stop.set()
            return result

        service._gap = gap

        async def forbidden(*args, **kwargs):
            raise AssertionError("depth socket cannot invalidate trade flow")

        service.mark_capture_gap = forbidden
        await asyncio.wait_for(stream_group_loop(service, DEPTH), 1)
        assert service._connected_since[iid] == since and service._stream_watermarks[iid] == before
        assert not service.books[iid].integrity_ok
        await service.close()

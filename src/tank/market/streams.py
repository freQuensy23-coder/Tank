"""Public WebSocket subscriptions; all received frames are archived before parsing."""

import asyncio
import json
import time
from decimal import Decimal

from datetime import timedelta

from tank.util import iso, parse_time, sha256, utcnow

from .books import OrderBook
from .normalize import SemanticError, liquidation, oi, venue_key

STREAM_PING_INTERVAL_S = 20
# The client closes with 1011 when a pong is late; the loop on prod stalls for seconds.
STREAM_PING_TIMEOUT_S = 60


def connection(instrument, *, depth_only=False):
    key = venue_key(instrument)
    s = instrument["venue_symbol"]
    perp = not key.endswith("spot")
    if key.startswith("binance"):
        channels = [s.lower() + "@aggTrade", s.lower() + "@depth@100ms"]
        if perp:
            channels = (
                [s.lower() + "@depth@100ms"]
                if depth_only
                else [s.lower() + "@aggTrade", s.lower() + "@markPrice@1s", s.lower() + "@forceOrder"]
            )
        root = (
            (
                "wss://fstream.binance.com/public/stream"
                if depth_only
                else "wss://fstream.binance.com/market/stream"
            )
            if perp
            else "wss://data-stream.binance.vision/stream"
        )
        return root + "?streams=" + ("/".join(channels)), []
    if key.startswith("coinbase"):
        return "wss://ws-feed.exchange.coinbase.com", [
            {"type": "subscribe", "product_ids": [s], "channels": ["matches", "level2_batch", "heartbeat"]}
        ]
    if key.startswith("okx"):
        args = [{"channel": c, "instId": s} for c in ("trades", "books")]
        if perp:
            args.extend(
                [{"channel": c, "instId": s} for c in ("open-interest", "funding-rate", "mark-price")]
            )
            args.append({"channel": "index-tickers", "instId": s.removesuffix("-SWAP")})
            args.append({"channel": "liquidation-orders", "instType": "SWAP"})
        return "wss://ws.okx.com/ws/v5/public", [{"op": "subscribe", "args": args}]
    if key.startswith("bybit"):
        args = ["publicTrade." + s, "orderbook.200." + s]
        if perp:
            args.extend(["tickers." + s, "allLiquidation." + s])
        return "wss://stream.bybit.com/v5/public/" + key.split(".")[1], [{"op": "subscribe", "args": args}]
    if key.startswith("kraken"):
        symbol = instrument.get("ws_symbol", s.replace("XBT", "BTC"))
        return "wss://ws.kraken.com/v2", [
            {"method": "subscribe", "params": {"channel": "trade", "symbol": [symbol], "snapshot": False}},
            {
                "method": "subscribe",
                "params": {"channel": "book", "symbol": [symbol], "depth": 1000, "snapshot": True},
            },
        ]
    return "wss://api.hyperliquid.xyz/ws", [
        {"method": "subscribe", "subscription": {"type": c, "coin": s}}
        for c in ("trades", "l2Book", "activeAssetCtx")
    ]


async def process_frame(self, instrument, raw):
    iid = instrument["instrument_id"]
    key = venue_key(instrument)
    book = self.books.get(iid) if key == "binance.usdm" else self.books[iid]
    data = raw.get("data", raw)
    if key.startswith("binance"):
        event = data.get("e")
        if event == "aggTrade":
            await self.ingest_trade(instrument, data)
        elif event == "depthUpdate":
            bridging = self._book_initial_bridge.get(iid, True)
            # Drop events the snapshot already contains: spot u <= lastUpdateId; the
            # USDⓈ-M bridge event itself has u == lastUpdateId, so only u < lastUpdateId.
            if int(data["u"]) < int(book.sequence) + (1 if not bridging or key.endswith("spot") else 0):
                return
            if bridging:
                # Futures initial bridge includes lastUpdateId (spot lastUpdateId+1).
                target = int(book.sequence) + (1 if key.endswith("spot") else 0)
                if not int(data["U"]) <= target <= int(data["u"]):
                    raise SemanticError("initial Binance snapshot bridge missing")
                self._book_initial_bridge[iid] = False
                book._levels("bid", data["b"])
                book._levels("ask", data["a"])
                book.sequence = data["u"]
                book.exchange_ts = iso(data["E"])
                book.exchange_ts_prec = "EXACT"
            else:
                book.update(
                    data["b"],
                    data["a"],
                    sequence=data["u"],
                    previous=data.get("pu"),
                    first=data["U"],
                    ts=data["E"],
                )
            await self._save_book(instrument)
        elif event == "forceOrder":
            await save_liquidation(self, instrument, data)
        elif event == "markPriceUpdate":
            due = self.context_due(iid, "mark")
            await save_mark(
                self, instrument, data["p"], data["E"], index=data.get("i"), index_ts=data["E"], persist=due
            )
            if due and instrument.get("funding_interval_h"):
                await save_funding(
                    self,
                    instrument,
                    {"lastFundingRate": data["r"], "time": data["E"]},
                    "CURRENT_ESTIMATE",
                    instrument["funding_interval_h"],
                )
    elif key.startswith("coinbase"):
        event = raw.get("type")
        if event in ("match", "last_match"):
            if event == "match":
                await self.ingest_trade(instrument, raw)
        elif event == "snapshot":
            book.snapshot(raw["bids"], raw["asks"], sequence=0, ts=iso(), ts_prec="COLLECTION_ONLY")
            await self._save_book(instrument)
        elif event == "l2update":
            if book.sequence is None:
                return  # updates before this subscription's snapshot belong to no book yet
            bids = [r[1:] for r in raw["changes"] if r[0] == "buy"]
            asks = [r[1:] for r in raw["changes"] if r[0] == "sell"]
            book.update(bids, asks, sequence=int(book.sequence) + 1, ts=raw["time"])
            await self._save_book(instrument)
    elif key.startswith("okx"):
        channel = raw.get("arg", {}).get("channel")
        rows = data if isinstance(data, list) else []
        for item in rows:
            expected_symbol = (
                instrument["venue_symbol"].removesuffix("-SWAP")
                if channel == "index-tickers"
                else instrument["venue_symbol"]
            )
            if item.get("instId", expected_symbol) != expected_symbol:
                continue
            if channel == "trades":
                await self.ingest_trade(instrument, item)
            elif channel == "books":
                if raw.get("action") == "snapshot":
                    book.snapshot(
                        item["bids"],
                        item["asks"],
                        sequence=item["seqId"],
                        ts=item["ts"],
                        checksum=item.get("checksum"),
                    )
                else:
                    book.update(
                        item["bids"],
                        item["asks"],
                        sequence=item["seqId"],
                        previous=item["prevSeqId"],
                        ts=item["ts"],
                        checksum=item.get("checksum"),
                        verify=self.summary_due(iid),
                    )
                await self._save_book(instrument)
            elif channel == "open-interest":
                if not self.context_due(iid, "oi"):
                    continue
                row = oi(instrument, item)
                await self._put("mkt.oi_obs", iid + ":" + row["metric_key"] + ":" + row["ts"], row)
            elif channel == "funding-rate":
                prev = item.get("prevFundingTime")
                cur = item.get("fundingTime")
                if prev and cur and self.context_due(iid, "funding"):
                    await save_funding(
                        self,
                        instrument,
                        item,
                        "CURRENT_ESTIMATE",
                        (Decimal(cur) - Decimal(prev)) / 3600000,
                    )
            elif channel == "mark-price":
                await save_mark(
                    self, instrument, item["markPx"], item["ts"], persist=self.context_due(iid, "mark")
                )
            elif channel == "index-tickers":
                context = self._price_contexts.setdefault(iid, {})
                context.update(index_price=item["idxPx"], index_ts=iso(item["ts"]))
                if context.get("mark_price") is not None:
                    await save_mark(
                        self,
                        instrument,
                        context["mark_price"],
                        context["mark_ts"],
                        index=item["idxPx"],
                        index_ts=item["ts"],
                        persist=self.context_due(iid, "mark"),
                    )
            elif channel == "liquidation-orders":
                for detail in item.get("details", []):
                    await save_liquidation(self, instrument, detail)
    elif key.startswith("bybit"):
        topic = raw.get("topic", "")
        if topic.startswith("publicTrade."):
            for item in data:
                if raw.get("cs") is not None:
                    item = dict(item, _stream_sequence=raw["cs"])
                await self.ingest_trade(instrument, item)
        elif topic.startswith("orderbook."):
            if raw.get("type") == "snapshot" or data.get("u") == 1:
                book.snapshot(data["b"], data["a"], sequence=data["u"], ts=raw.get("cts", raw.get("ts")))
            else:
                book.update(data["b"], data["a"], sequence=data["u"], ts=raw.get("cts", raw.get("ts")))
            await self._save_book(instrument)
        elif topic.startswith("allLiquidation."):
            for item in data:
                await save_liquidation(self, instrument, item)
        elif topic.startswith("tickers."):
            context = self._ticker_contexts.setdefault(iid, {})
            if raw.get("type") == "snapshot":
                context.clear()
            context.update(data)
            if (data.get("markPrice") or data.get("indexPrice")) and context.get("markPrice") is not None:
                await save_mark(
                    self,
                    instrument,
                    context["markPrice"],
                    raw["ts"],
                    index=context.get("indexPrice"),
                    index_ts=raw["ts"],
                    persist=self.context_due(iid, "mark"),
                )
            if (
                data.get("fundingRate")
                and instrument.get("funding_interval_h")
                and self.context_due(iid, "funding")
            ):
                await save_funding(
                    self, instrument, data, "CURRENT_ESTIMATE", instrument["funding_interval_h"]
                )
    elif key.startswith("kraken"):
        channel = raw.get("channel")
        for item in data if isinstance(data, list) else []:
            if channel == "trade":
                await self.ingest_trade(instrument, item)
            elif channel == "book":
                if raw.get("type") == "snapshot":
                    book.snapshot(
                        item["bids"],
                        item["asks"],
                        sequence=0,
                        ts=item.get("timestamp"),
                        checksum=item.get("checksum"),
                    )
                elif book.sequence is None:
                    continue  # updates before this subscription's snapshot belong to no book yet
                else:
                    book.update(
                        item.get("bids", []),
                        item.get("asks", []),
                        sequence=int(book.sequence) + 1,
                        ts=item["timestamp"],
                        checksum=item.get("checksum"),
                        verify=self.summary_due(iid),
                    )
                # Kraken depth is maintained at subscribed depth before checksum.
                await self._save_book(instrument)
    else:
        channel = raw.get("channel")
        if channel == "trades":
            for item in data:
                await self.ingest_trade(instrument, item)
        elif channel == "l2Book":
            bids, asks = data["levels"]
            book.snapshot(
                [[r["px"], r["sz"]] for r in bids],
                [[r["px"], r["sz"]] for r in asks],
                sequence=data["time"],
                ts=data["time"],
            )
            await self._save_book(instrument)
        elif channel == "activeAssetCtx":
            ctx = data["ctx"]
            due = self.context_due(iid, "context")
            if due:
                row = oi(instrument, ctx)
                await self._put("mkt.oi_obs", iid + ":" + row["metric_key"] + ":" + row["ts"], row)
                await save_funding(self, instrument, ctx, "CURRENT_ESTIMATE", "1")
            await save_mark(
                self, instrument, ctx["markPx"], iso(), index=ctx.get("oraclePx"), index_ts=iso(), persist=due
            )

    if not (key == "binance.usdm" and data.get("e") == "depthUpdate"):
        record_stream_watermark(self, instrument, raw)


WATERMARK_S = 0.25


def record_stream_watermark(self, instrument, raw):
    """Track observed publicsocket progress, never nextfunding/event timestamps.

    Sampled at most every WATERMARK_S per instrument: the watermark only ever lags
    the socket, so proofs built on it stay conservative.
    """
    iid = instrument["instrument_id"]
    due = self.__dict__.setdefault("_watermark_due", {})
    now = time.monotonic()
    if due.get(iid, 0) > now:
        return
    key = venue_key(instrument)
    data = raw.get("data", raw)
    stamps = []
    if key == "binance.usdm" and data.get("e") == "depthUpdate":
        return  # Independent public depth cannot prove regular trade/liq continuity.
    if key.startswith("binance"):
        stamp = data.get("E")
        if stamp is not None:
            stamps.append(stamp)
    elif key.startswith("bybit"):
        stamp = raw.get("cts", raw.get("ts"))
        if stamp is not None:
            stamps.append(stamp)
    elif key.startswith("okx"):
        for item in data if isinstance(data, list) else []:
            if item.get("ts") is not None:
                stamps.append(item["ts"])
            for detail in item.get("details", []):
                if detail.get("ts") is not None:
                    stamps.append(detail["ts"])
    elif key.startswith("coinbase"):
        if raw.get("time"):
            stamps.append(raw["time"])
    elif key.startswith("kraken"):
        # One ISO format per feed: the latest string is the latest time (parse it once).
        if isinstance(data, list):
            latest = max(
                (r["timestamp"] for r in data if isinstance(r, dict) and r.get("timestamp")), default=None
            )
            if latest:
                stamps.append(latest)
    elif isinstance(data, dict):
        if data.get("time") is not None:
            stamps.append(data["time"])
    elif isinstance(data, list):
        stamps.extend(r["time"] for r in data if r.get("time") is not None)
    if not stamps:
        return
    due[iid] = now + WATERMARK_S
    stamp = max(parse_time(t) for t in stamps)
    previous = self._stream_watermarks.get(iid)
    if previous is None or stamp >= parse_time(previous["ts"]):
        self._stream_watermarks[iid] = {
            "ts": iso(stamp),
            "received_ts": iso(),
            "source": "OBSERVED_SHARED_PUBLIC_SOCKET_EXCHANGE_TIME",
        }


def endpoint_key(instrument):
    return connection(instrument)[0].split("?", 1)[0]


def endpoint_keys(instrument):
    """USD-M depth and regular data have distinct documented routed sockets."""
    primary = endpoint_key(instrument)
    return (
        (primary, connection(instrument, depth_only=True)[0].split("?", 1)[0])
        if venue_key(instrument) == "binance.usdm"
        else (primary,)
    )


def subscription_messages(instruments, *, subscribe=True, endpoint=None):
    if not instruments:
        return []
    venue = venue_key(instruments[0])
    action = "subscribe" if subscribe else "unsubscribe"
    if venue.startswith("binance"):
        channels = []
        for instrument in instruments:
            channels.extend(
                connection(instrument, depth_only=bool(endpoint and endpoint.endswith("/public/stream")))[0]
                .split("?streams=", 1)[1]
                .split("/")
            )
        return [{"method": action.upper(), "params": sorted(set(channels)), "id": 1}]
    messages = []
    for instrument in instruments:
        _, items = connection(instrument)
        for item in items:
            item = dict(item)
            if "op" in item:
                item["op"] = action
            elif "method" in item:
                item["method"] = action
            elif "type" in item:
                item["type"] = action
            # OKX liquidation-orders is global and subscribed once per connection.
            if venue.startswith("okx"):
                item["args"] = [a for a in item["args"] if a["channel"] != "liquidation-orders"]
            messages.append(item)
    if venue.startswith(("okx", "bybit")):
        args = list(
            {json.dumps(arg, sort_keys=True): arg for item in messages for arg in item["args"]}.values()
        )
        # Bybit spot accepts at most 10 topics per subscribe request ("args size >10").
        size = 10 if venue.startswith("bybit") else len(args) or 1
        return [{"op": action, "args": args[i : i + size]} for i in range(0, len(args), size)]
    if venue.startswith("coinbase"):
        return [
            {
                "type": action,
                "product_ids": [i["venue_symbol"] for i in instruments],
                "channels": ["matches", "level2_batch", "heartbeat"],
            }
        ]
    if venue.startswith("kraken"):
        symbols = [i.get("ws_symbol", i["venue_symbol"].replace("XBT", "BTC")) for i in instruments]
        return [{"method": action, "params": dict(item["params"], symbol=symbols)} for item in messages[:2]]
    return messages


def routing_symbol(instrument):
    """The product identity a venue's frames carry for this instrument."""
    if venue_key(instrument).startswith("kraken"):
        return instrument.get("ws_symbol", instrument["venue_symbol"].replace("XBT", "BTC"))
    return instrument["venue_symbol"]


def route_indexed(index, venue, raw):
    """Route a frame by the product identity it names (index: symbol -> instrument)."""
    data = raw.get("data", raw)
    if venue == "binance":
        symbol = data.get("s", data.get("o", {}).get("s")) if isinstance(data, dict) else None
        instrument = index.get(symbol)
        return [(instrument, raw)] if instrument else []
    if venue == "coinbase":
        instrument = index.get(raw.get("product_id"))
        return [(instrument, raw)] if instrument else []
    if venue == "bybit":
        instrument = index.get(raw.get("topic", "").rsplit(".", 1)[-1])
        return [(instrument, raw)] if instrument else []
    if not isinstance(data, list):
        if venue == "hyperliquid" and isinstance(data, dict):
            instrument = index.get(data.get("coin"))
            return [(instrument, raw)] if instrument else []
        return []
    groups = {}
    if venue == "okx":
        arg = raw.get("arg", {})
        default = arg.get("instId")
        for row in data:
            symbol = row.get("instId", default)
            if arg.get("channel") == "index-tickers":
                symbol = (symbol or "") + "-SWAP"  # an index feeds its perpetual
            groups.setdefault(symbol, []).append(row)
    else:
        field = "symbol" if venue == "kraken" else "coin"
        for row in data:
            groups.setdefault(row.get(field), []).append(row)
    return [(index[symbol], dict(raw, data=rows)) for symbol, rows in groups.items() if symbol in index]


def route_frame(instruments, raw):
    """Route only exact product identities; split multi-product data arrays."""
    instruments = list(instruments)
    if not instruments:
        return iter(())
    index = {routing_symbol(i): i for i in instruments}
    return iter(route_indexed(index, venue_key(instruments[0]).split(".")[0], raw))


async def prepare_member(self, instrument, *, depth_only=False):
    iid = instrument["instrument_id"]
    key = venue_key(instrument)
    if iid not in self._metadata:
        await self.discover(instrument)
    regular_usdm = key == "binance.usdm" and not depth_only
    if regular_usdm:
        self.books.setdefault(iid, OrderBook(instrument))
    else:
        self.books[iid] = OrderBook(instrument)
        self._book_initial_bridge[iid] = True
    book = self.books[iid]
    if key.startswith("binance") and not regular_usdm:
        root = "https://api.binance.com" if key.endswith("spot") else "https://fapi.binance.com"
        path = "/api/v3/depth" if key.endswith("spot") else "/fapi/v1/depth"
        snap = await self._request(
            "GET", root + path, params={"symbol": instrument["venue_symbol"], "limit": 1000}
        )
        await self._raw(instrument, "book_snapshot", snap)
        book.snapshot(
            snap["bids"],
            snap["asks"],
            sequence=snap["lastUpdateId"],
            ts=snap.get("E"),
            ts_prec="EXACT" if snap.get("E") is not None else "COLLECTION_ONLY",
        )
    if depth_only:
        return  # Depth never owns the trade cursor, backfill or flow continuity.
    self._connected_since[iid] = utcnow()
    await self._ensure_market_restore()
    previous = self._capture_checkpoints.get(iid)
    if previous:
        # 03 §5.2: after a reconnect, backfill over REST from the last seen print.
        await self._gap(
            instrument, "RECONNECT", "REST reconciliation pending", start=previous["last_ts"], end=iso()
        )
    else:
        # First connection: the escalation backfills the pre-event windows up to the
        # anchor (03 §5.1); this one covers the anchor up to the socket.
        since = self._hot_since.get(instrument["token_id"])
        if since is None or parse_time(since) >= utcnow():
            return
        previous = {"last_ts": iso(max(parse_time(since), utcnow() - timedelta(hours=24)))}
    if self._active_backfills.get(iid):
        # One REST repair per instrument at a time; keep the earliest unrepaired print.
        # service.backfill starts it when the running one ends.
        pending = self._pending_reconnects
        if iid not in pending or parse_time(previous["last_ts"]) < parse_time(pending[iid]["last_ts"]):
            pending[iid] = previous
        return
    task = asyncio.create_task(self.backfill(instrument, iso(), checkpoint=previous))
    self._background.add(task)
    task.add_done_callback(self._background.discard)


async def stream_group_loop(self, endpoint):
    import websockets

    retry = 1
    control = self._stream_controls[endpoint]
    depth_only = endpoint == "wss://fstream.binance.com/public/stream"
    while self._stop is not None and not self._stop.is_set() and self._stream_groups.get(endpoint):
        opened = utcnow()
        last_observed = opened
        active = set()
        preparing = {}
        pending = {}
        try:
            url = endpoint
            async with websockets.connect(
                url,
                ping_interval=STREAM_PING_INTERVAL_S,
                ping_timeout=STREAM_PING_TIMEOUT_S,
                max_queue=256,
                max_size=32 * 1024 * 1024,
                close_timeout=5,
            ) as socket:
                global_liqs = False
                venue = venue_key(self.instruments[next(iter(self._stream_groups[endpoint]))]).split(".")[0]
                index = {}
                sent_at = [0.0]

                async def send(message):
                    # Binance closes a connection that receives more than 5 messages
                    # a second; keep control messages at most 4 per second.
                    if venue == "binance":
                        wait = sent_at[0] + 0.25 - time.monotonic()
                        if wait > 0:
                            await asyncio.sleep(wait)
                        sent_at[0] = time.monotonic()
                    await socket.send(json.dumps(message))

                async def membership(desired, active=active, preparing=preparing, pending=pending):
                    nonlocal global_liqs
                    removed = active - set(desired)
                    added = set(desired) - active
                    for message in subscription_messages(
                        [self.instruments[i] for i in removed], subscribe=False, endpoint=endpoint
                    ):
                        await send(message)
                    for iid in removed:
                        if iid in preparing:
                            old_task = preparing.pop(iid)
                            old_task.cancel()
                            await asyncio.gather(old_task, return_exceptions=True)
                        if not depth_only:
                            self._connected_since.pop(iid, None)
                            self._stream_watermarks.pop(iid, None)
                        pending.pop(iid, None)
                        if depth_only or venue_key(self.instruments[iid]) != "binance.usdm":
                            self.books.pop(iid, None)
                    for message in subscription_messages(
                        [self.instruments[i] for i in added], endpoint=endpoint
                    ):
                        await send(message)
                    for iid in added:
                        pending[iid] = []
                        preparing[iid] = asyncio.create_task(
                            prepare_member(self, self.instruments[iid], depth_only=depth_only)
                        )
                    active.difference_update(removed)
                    active.update(added)
                    index.clear()
                    index.update({routing_symbol(self.instruments[i]): self.instruments[i] for i in active})
                    want_liqs = endpoint.startswith("wss://ws.okx.com/") and any(
                        venue_key(self.instruments[i]) == "okx.swap" for i in active
                    )
                    if want_liqs != global_liqs:
                        await socket.send(
                            json.dumps(
                                {
                                    "op": "subscribe" if want_liqs else "unsubscribe",
                                    "args": [{"channel": "liquidation-orders", "instType": "SWAP"}],
                                }
                            )
                        )
                        global_liqs = want_liqs

                async def reset_member(iid, reason, preparing=preparing, pending=pending, opened=opened):
                    inst = self.instruments[iid]
                    if depth_only:
                        if iid in self.books:
                            self.books[iid].integrity_ok = False
                            self.books[iid].gap_reason = "ORDER_BOOK_STREAM_MEMBER_GAP"
                        await self._gap(
                            inst,
                            "ORDER_BOOK_STREAM",
                            "STREAM_MEMBER_GAP: " + str(reason),
                            start=opened,
                            end=iso(),
                        )
                    else:
                        since = self._connected_since.pop(iid, None) or opened
                        await self.mark_capture_gap(inst, since, iso(), "STREAM_MEMBER_GAP: " + str(reason))
                    old = preparing.pop(iid, None)
                    if old is not None:
                        old.cancel()
                        await asyncio.gather(old, return_exceptions=True)
                    for message in subscription_messages([inst], subscribe=False, endpoint=endpoint):
                        await send(message)
                    for message in subscription_messages([inst], endpoint=endpoint):
                        await send(message)
                    pending[iid] = []

                    async def delayed_prepare():
                        await asyncio.sleep(1)
                        await prepare_member(self, inst, depth_only=depth_only)

                    preparing[iid] = asyncio.create_task(delayed_prepare())

                await membership(self._stream_groups[endpoint])
                retry = 1
                last_message = time.monotonic()
                while active and not self._stop.is_set():
                    if time.monotonic() - last_message > float(self.thresholds.get("quiet_socket_s", 30)):
                        raise TimeoutError("NO_MESSAGE_FOR_30_SECONDS")
                    for iid, task in list(preparing.items()):
                        if task.done():
                            preparing.pop(iid)
                            try:
                                task.result()
                                for queued in pending.pop(iid, []):
                                    await process_frame(self, self.instruments[iid], queued)
                            except Exception as exc:  # noqa: BLE001 - isolate one instrument's metadata/book failure
                                await reset_member(iid, exc)
                    # Poll membership changes and stop instead of awaiting them beside recv():
                    # a recv task plus asyncio.wait costs a full event-loop turn per frame,
                    # so a busy loop drains fewer frames than arrive, websockets pauses
                    # reading at max_queue and the unread pong ends in a keepalive timeout.
                    if self._stop.is_set():
                        break
                    try:
                        desired = control.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    else:
                        await membership(desired)
                    try:
                        # recv() returns without yielding while frames are queued; canceling it is safe.
                        async with asyncio.timeout(0.1):
                            frame = await socket.recv()
                    except TimeoutError:
                        continue
                    self._frames[endpoint] = self._frames.get(endpoint, 0) + 1
                    last_message = time.monotonic()
                    last_observed = utcnow()
                    if frame == "pong":
                        continue
                    raw = json.loads(frame, parse_float=Decimal)
                    if (
                        raw.get("event") == "error"
                        or raw.get("success") is False
                        or raw.get("type") == "error"
                    ):
                        raise SemanticError("subscription rejected " + str(raw))
                    for instrument, payload in route_indexed(index, venue, raw):
                        iid = instrument["instrument_id"]
                        if venue == "coinbase" and raw.get("type") == "last_match":
                            await self._raw(instrument, "websocket", raw)
                        if iid in preparing:
                            pending[iid].append(payload)
                            if len(pending[iid]) > 1024:
                                await reset_member(iid, "initial book buffer exhausted")
                        else:
                            try:
                                await process_frame(self, instrument, payload)
                            except Exception as exc:  # noqa: BLE001 - bad product book must not disconnect unrelated products
                                await reset_member(iid, exc)
                    # The market supervisor batches flow once per second.
                    # A frame only ingests its updates; rebuilding every bar
                    # here multiplies work by the WebSocket message rate.
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - one connection failure preserves per-member gaps and reconnects
            for iid in active:
                if depth_only:
                    if iid in self.books:
                        self.books[iid].integrity_ok = False
                        self.books[iid].gap_reason = "ORDER_BOOK_STREAM_DISCONNECTED"
                    await self._gap(
                        self.instruments[iid],
                        "ORDER_BOOK_STREAM",
                        "STREAM_DISCONNECTED: " + str(exc),
                        start=last_observed,
                        end=iso(),
                    )
                else:
                    await self.mark_capture_gap(
                        self.instruments[iid], last_observed, iso(), "STREAM_DISCONNECTED: " + str(exc)
                    )
        finally:
            for task in preparing.values():
                task.cancel()
            await asyncio.gather(*preparing.values(), return_exceptions=True)
            if not depth_only:
                for iid in active:
                    self._connected_since.pop(iid, None)
        if self._stop is not None:
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=retry)
            except TimeoutError:
                pass
        retry = min(retry * 2, 60)


async def save_liquidation(self, instrument, raw):
    row = liquidation(instrument, raw)
    await self._put("mkt.liq_obs", instrument["instrument_id"] + ":" + sha256(row), row)


async def save_mark(
    self,
    instrument,
    price,
    ts,
    *,
    index=None,
    index_ts=None,
    source="WEBSOCKET",
    received_ts=None,
    index_clock_basis=None,
    persist=True,
):
    """Track the newest mark/index; store a point only when `persist` (stream cadence)."""
    iid = instrument["instrument_id"]
    context = self._price_contexts.setdefault(iid, {})
    received = iso(received_ts)
    venue = venue_key(instrument)
    rest_path = (
        ":rest:metaAndAssetCtxs"
        if venue == "hyperliquid.perp"
        else ":rest:tickers"
        if venue.startswith("bybit")
        else ":rest:premiumIndex"
        if venue == "binance.usdm"
        else ":rest:mark-price"
    )
    source_path = venue + (rest_path if source == "PUBLIC_REST" else ":websocket:mark_context")
    mark_fact = {
        "capture_source": source,
        "source_path": source_path,
        "as_of": iso(ts),
        "received_ts": received,
        "clock_basis": "COLLECTION_TIME" if source == "PUBLIC_REST" else "SOURCE_TIME",
    }
    if context.get("mark_ts") is None or parse_time(ts) >= parse_time(context["mark_ts"]):
        context.update(mark_price=str(price), mark_ts=iso(ts), mark_provenance=mark_fact)
    incoming_index_ts = iso(index_ts or ts)
    if index is not None and (
        context.get("index_ts") is None or parse_time(incoming_index_ts) >= parse_time(context["index_ts"])
    ):
        index_path = (
            venue + ":rest:index-tickers" if venue == "okx.swap" and source == "PUBLIC_REST" else source_path
        )
        context.update(
            index_price=str(index),
            index_ts=incoming_index_ts,
            index_provenance={
                "capture_source": source,
                "source_path": index_path,
                "as_of": incoming_index_ts,
                "received_ts": received,
                "clock_basis": index_clock_basis or mark_fact["clock_basis"],
            },
        )
    if not persist:
        return
    ts = context["mark_ts"]
    price = context["mark_price"]
    aligned = (
        context.get("index_price") is not None
        and abs((parse_time(context["index_ts"]) - parse_time(ts)).total_seconds()) <= 60
    )
    observed = max(parse_time(ts), parse_time(context["index_ts"])) if aligned else parse_time(ts)
    observed_facts = [
        fact
        for name in ("mark_provenance", "index_provenance")
        if (fact := context.get(name)) and parse_time(fact["as_of"]) == observed
    ]
    observed_precision = (
        "EXACT"
        if observed_facts and all(fact.get("clock_basis") == "SOURCE_TIME" for fact in observed_facts)
        else "COLLECTION_ONLY"
        if observed_facts
        and all(fact.get("clock_basis") in ("SOURCE_TIME", "COLLECTION_TIME") for fact in observed_facts)
        else None
    )
    row = {
        "instrument_id": iid,
        "token_id": instrument["token_id"],
        "ts": iso(observed),
        "ts_prec": observed_precision,
        "received_ts": iso(received_ts),
        "kind": "MARK_PRICE",
        "mark_price": str(price),
        "mark_as_of": iso(ts),
        "index_price": context["index_price"] if aligned else None,
        "index_as_of": context.get("index_ts") if aligned else None,
        "index_reason": None if aligned else "ALIGNED_INDEX_PRICE_UNAVAILABLE",
        "capture_source": source,
        "field_sources": {
            field: context[name]
            for field, name in (("mark_price", "mark_provenance"), ("index_price", "index_provenance"))
            if context.get(name) and (field != "index_price" or aligned)
        },
        "quote_asset": instrument.get("quote_asset"),
    }
    saved = await self.save_ticker_observation(instrument, row)
    # A same-time durable point may predate this process restart and retain a
    # newer component than the incoming partial payload.
    context.update(mark_price=saved["mark_price"], mark_ts=saved["mark_as_of"])
    if saved.get("field_sources", {}).get("mark_price"):
        context["mark_provenance"] = saved["field_sources"]["mark_price"]
    if saved.get("index_price") is not None:
        context.update(index_price=saved["index_price"], index_ts=saved["index_as_of"])
        if saved.get("field_sources", {}).get("index_price"):
            context["index_provenance"] = saved["field_sources"]["index_price"]


async def save_funding(self, instrument, raw, kind, interval):
    await self._funding(instrument, raw, kind, interval, source="WEBSOCKET")

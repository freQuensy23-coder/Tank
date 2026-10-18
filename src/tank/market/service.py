from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from contextlib import suppress
from datetime import timedelta
from decimal import Decimal, localcontext

import httpx

from tank.util import iso, jsonable, new_id, parse_time, sha256, utcnow

from . import normalize as norm
from .adapters import VenueAdapter
from .archive import RawArchive
from .capture import event_anchor
from .flow import FlowAccumulator
from .rate_gate import PriorityGate, binance_request_weight, public_request_constraints
from .streams import STREAM_PING_INTERVAL_S, endpoint_key, stream_group_loop

log = logging.getLogger(__name__)
COLLECTIONS = {
    "flow_bars": ("mkt.flow_bars_1m", "minute_start"),
    "klines": ("mkt.klines_1m", "open_time"),
    "books": ("mkt.book_summaries", "exchange_ts"),
    "oi": ("mkt.oi_obs", "ts"),
    "funding": ("mkt.funding_obs", "as_of"),
    "liquidations": ("mkt.liq_obs", "ts"),
    "tickers": ("mkt.tickers_24h", "ts"),
    "supply": ("mkt.supply_obs", "ts"),
}


class MarketService:
    def __init__(self, repo, settings, *, http=None):
        self.repo = repo
        self.settings = settings
        self.http = http or httpx.AsyncClient(timeout=30, headers={"User-Agent": settings.user_agent})
        self._owns_http = http is None
        self.capture_only = getattr(settings, "capture_only", False)
        registry = settings.load_config("registry")
        self.tokens = {r["token_id"]: r for r in registry["tokens"]}
        self.instruments = {
            r["instrument_id"]: dict(r) for r in registry["instruments"] if r.get("active", True)
        }
        self.semantics = settings.load_config("semantics")["metrics"]
        self.archive = RawArchive(settings.data_dir)
        self.flow = FlowAccumulator()
        self.books = {}
        self.streams = {}
        self._stop = None
        self._last_request = {}
        self._response_cache = {}
        self._cache_locks = {}
        self._request_locks = {}
        self._poll_due = {}
        self._metadata = set()
        self._book_due = {}
        self._fx = {}
        self._backfills = set()
        self._capture_waits = {}
        self._connected_since = {}
        self._sample_ids = {}
        self._zero_cursor = {}
        self._semantic_verified = set()
        self._background = set()
        self._stream_groups = {}
        self._stream_controls = {}
        self._book_initial_bridge = {}
        self._stream_watermarks = {}
        self._price_contexts = {}
        self._ticker_contexts = {}
        self._book_confirmations = {}
        self._flushed_bars = {}
        self._last_flow_flush = 0
        self._last_hot_expiry_refresh = 0
        try:
            self.thresholds = settings.load_config("thresholds")
        except FileNotFoundError:
            self.thresholds = {}

    def _redact(self, value):
        secrets = [
            getattr(self.settings, name, None)
            for name in (
                "etherscan_api_key",
                "tavily_api_key",
                "typesafe_api_key",
                "openrouter_api_key",
                "openma_api_key",
                "codex_gateway_key",
                "telegram_token",
            )
        ]
        if isinstance(value, str):
            for secret in secrets:
                if secret:
                    value = value.replace(secret, "[REDACTED]")
            return value
        if isinstance(value, dict):
            return {self._redact(k): self._redact(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._redact(v) for v in value]
        return value

    def provenance(self, row):
        return self._redact(
            dict(row, config_version=self.settings.config_version, code_version=self.settings.code_version)
        )

    async def _put(self, collection, key, row, *, immutable=False):
        if collection in {"mkt.trades", *(value[0] for value in COLLECTIONS.values())}:
            if collection != "mkt.trades":
                previous = await self.repo.get(collection, str(key))
                metadata = {"recorded_at", "received_ts", "code_version", "config_version"}

                def content(value):
                    return {name: item for name, item in jsonable(value).items() if name not in metadata}

                if previous is not None and sha256(content(previous)) == sha256(content(row)):
                    return previous
            row = dict(row, recorded_at=iso())
        if collection == "qa.reviews" and await self.repo.get(collection, str(key)) is not None:
            key = str(key) + ":" + new_id("REV")
            row = dict(row, review_id=key)
        return await self.repo.put(collection, str(key), self.provenance(jsonable(row)), immutable=immutable)

    async def mark_capture_gap(self, instrument, start, end, reason):
        iid = instrument["instrument_id"]
        self.flow.mark_gap(iid, start, end, str(reason))
        rows = await self._range(
            "mkt.flow_bars_1m",
            "minute_start",
            start=parse_time(start).replace(second=0, microsecond=0),
            end=parse_time(end) + timedelta(microseconds=1),
            filters={"instrument_id": iid},
        )
        for row in rows:
            if row.get("backfill_verified") or row.get("source_archive"):
                continue
            row.update(complete=False, gap_reason=str(reason))
            await self._put("mkt.flow_bars_1m", iid + ":" + row["minute_start"], row)
        await self._gap(instrument, "STREAM", reason, start=start, end=end)

    async def _gap(self, instrument, kind, reason, *, start=None, end=None):
        reason = str(reason)
        for secret in (self.settings.etherscan_api_key,):
            if secret:
                reason = reason.replace(secret, "[REDACTED]")
        row = {
            "gap_id": new_id("GAP"),
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            "kind": kind,
            "reason": str(reason),
            "start": iso(start),
            "end": iso(end),
            "observed_at": iso(),
            "status": "OPEN",
        }
        await self._put("mkt.capture_gaps", row["gap_id"], row, immutable=True)
        return row

    async def _raw(self, instrument, kind, payload):
        result = await asyncio.to_thread(
            self.archive.write, instrument["instrument_id"], kind, self._redact(payload)
        )
        result["token_id"] = instrument["token_id"]
        await self._put("mkt.raw_archives", result["archive_id"], result)
        for sealed in self.archive.drain_completed():
            if sealed["instrument_id"] in self.instruments:
                sealed["token_id"] = self.instruments[sealed["instrument_id"]]["token_id"]
            await self._put("mkt.raw_archives", sealed["archive_id"], sealed)
        return result

    async def close(self):
        for sealed in await asyncio.to_thread(self.archive.flush_all):
            if sealed["instrument_id"] in self.instruments:
                sealed["token_id"] = self.instruments[sealed["instrument_id"]]["token_id"]
            await self._put("mkt.raw_archives", sealed["archive_id"], sealed)
        if self._owns_http:
            await self.http.aclose()

    async def _request(self, method, url, **kwargs):
        host = httpx.URL(url).host
        cache_key = None
        cache_ttl = 0
        payload = kwargs.get("json", {})
        if host == "api.hyperliquid.xyz":
            if payload.get("type") == "metaAndAssetCtxs":
                cache_key, cache_ttl = "hyperliquid:contexts", 30
            elif payload.get("type") == "fundingHistory":
                cache_key = "hyperliquid:funding:" + sha256(
                    {"request": payload, "settlement_period": iso()[:13]}
                )
                cache_ttl = 3600
        for expired_key, (captured_mono, _) in list(self._response_cache.items()):
            ttl = 30 if expired_key == "hyperliquid:contexts" else 3600
            if time.monotonic() - captured_mono >= ttl:
                self._response_cache.pop(expired_key, None)
        if not cache_key:
            return await self._request_live(method, url, **kwargs)
        # One response per shared request: cache hits neither consume budget nor
        # change the original HTTP observation timestamp.
        cache_lock_key = (
            "hyperliquid:funding:" + payload["coin"] if payload.get("type") == "fundingHistory" else cache_key
        )
        async with self._cache_locks.setdefault(cache_lock_key, asyncio.Lock()):
            if cache_key in self._response_cache:
                cached_at, cached_body = self._response_cache[cache_key]
                if time.monotonic() - cached_at < cache_ttl:
                    return cached_body
            body = await self._request_live(method, url, **kwargs)
            observed_at = iso()
            rows = body[1] if payload.get("type") == "metaAndAssetCtxs" else body
            for row in rows:
                row["_capture_observed_at"] = observed_at
            self._response_cache[cache_key] = (time.monotonic(), body)
            return body

    async def _request_live(self, method, url, **kwargs):
        priority = kwargs.pop("_priority", 0)
        host = httpx.URL(url).host
        interval = (
            1
            if host == "api.kraken.com"
            else 0.21
            if host == "api.exchange.coinbase.com"
            else 0.35
            if host == "api.etherscan.io"
            else 0.08
        )
        lock = self._request_locks.setdefault(host, PriorityGate())

        async def admit_budget():
            from tank.sources.rate_limits import shared_request_budgets

            provider = (
                "kraken"
                if host == "api.kraken.com"
                else "coinbase"
                if host == "api.exchange.coinbase.com"
                else None
            )
            if provider:
                await shared_request_budgets.acquire(provider, 1 if provider == "kraken" else 5, 1)
            if host in ("api.binance.com", "fapi.binance.com"):
                path = httpx.URL(url).path
                weight = binance_request_weight(host, path, kwargs.get("params", {}))
                if weight:
                    await shared_request_budgets.acquire(
                        "binance.spot" if host == "api.binance.com" else "binance.usdm",
                        3000 if host == "api.binance.com" else 1200,
                        60,
                        weight=weight,
                    )
                if host == "fapi.binance.com" and path in (
                    "/fapi/v1/fundingRate",
                    "/fapi/v1/fundingInfo",
                ):
                    await shared_request_budgets.acquire("binance.usdm.funding", 250, 300)
                if host == "fapi.binance.com" and path == "/futures/data/openInterestHist":
                    await shared_request_budgets.acquire("binance.usdm.statistics", 500, 300)
            for budget_key, capacity, window, charge in public_request_constraints(
                host, httpx.URL(url).path, kwargs.get("params", {}), kwargs.get("json", {})
            ):
                await shared_request_budgets.acquire(budget_key, capacity, window, weight=charge)

        for attempt in range(4):
            # Waiting for a weighted repair budget must not own the HTTP slot:
            # a newly ready cheap baseline request can still be served first.
            await admit_budget()
            async with lock.enter(priority):
                remaining = interval - (time.monotonic() - self._last_request.get(host, 0))
                if remaining > 0:
                    await asyncio.sleep(remaining)
                try:
                    response = await self.http.request(method, url, **kwargs)
                except httpx.HTTPError as exc:
                    raise RuntimeError(f"{host}: {type(exc).__name__}") from None
                self._last_request[host] = time.monotonic()
                if response.status_code in (418, 429):
                    delay = float(response.headers.get("Retry-After", "60"))
                    self._last_request[host] = time.monotonic() + min(max(delay, 1), 60)
                    continue
                if response.is_error:
                    raise RuntimeError(f"{host}: HTTP {response.status_code}")
                # Binance reports consumed weight, and Tank observes its half-limit budgets.
                weight = response.headers.get("X-MBX-USED-WEIGHT-1M")
                if weight and int(weight) >= (1200 if host == "fapi.binance.com" else 3000):
                    self._last_request[host] = time.monotonic() + 60
                # JSON floats must never enter native quantity or monetary arithmetic.
                body = json.loads(response.content, parse_float=Decimal)
                if host == "api.hyperliquid.xyz" and kwargs.get("json", {}).get("type") == "recentTrades":
                    if not isinstance(body, list):
                        raise norm.SemanticError("Hyperliquid recentTrades response is not a list")
                    # No documented maximum response count: serialize requests and
                    # debit actual surcharge before this provider slot is released.
                    # This cannot promise a mathematical precharge size bound.
                    from tank.sources.rate_limits import shared_request_budgets

                    extra = (len(body) + 19) // 20
                    while extra:
                        charge = min(extra, 600)
                        await shared_request_budgets.acquire("hyperliquid", 600, 60, weight=charge)
                        extra -= charge
                return body
        raise RuntimeError(f"{host}: rate limited after bounded retries")

    def adapter(self, instrument, *, priority=0):
        async def request(method, url, **kwargs):
            return await self._request(method, url, _priority=priority, **kwargs)

        return VenueAdapter(instrument, request)

    async def _fx_rate(self, instrument):
        quote = instrument.get("quote_asset", "").upper()
        if quote == "USD":
            instrument["quote_usd_rate"] = "1"
            return
        cached = self._fx.get(quote)
        if cached and (utcnow() - parse_time(cached["ts"])).total_seconds() < 300:
            instrument.update(quote_usd_rate=cached["rate"], quote_usd_rate_as_of=cached["ts"])
            return
        if quote not in ("USDT", "USDC"):
            return
        body = await self._request("GET", f"https://api.exchange.coinbase.com/products/{quote}-USD/ticker")
        rate = str(norm.decimal(body["price"]))
        ts = iso(body["time"])
        self._fx[quote] = {"rate": rate, "ts": ts}
        instrument.update(quote_usd_rate=rate, quote_usd_rate_as_of=ts)

    async def discover(self, instrument):
        iid = instrument["instrument_id"]
        metadata = await self.adapter(instrument).discover()
        instrument.update(metadata, resolved_at=iso())
        await self._put("reg.instruments", iid, instrument)
        self._metadata.add(iid)

    async def poll_once(self, instrument_id, *, derivatives=True):
        instrument = self.instruments[instrument_id]
        adapter = self.adapter(instrument)
        if instrument_id not in self._metadata:
            try:
                await self.discover(instrument)
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                await self._gap(instrument, "METADATA", exc)
        try:
            await self._fx_rate(instrument)
        except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
            await self._gap(instrument, "QUOTE_FX", exc)
        for kind in ("klines", "ticker", "derivatives"):
            if kind == "derivatives" and (not derivatives or norm.venue_key(instrument).endswith(".spot")):
                continue
            try:
                if kind == "klines":
                    rows = await adapter.klines()
                    await self._raw(instrument, "klines", rows)
                    for raw in rows:
                        row = norm.kline(instrument, raw)
                        await self._store_kline(instrument, row)
                elif kind == "ticker":
                    await self._ticker(instrument, await adapter.ticker())
                else:
                    oi, current, settled, interval, mark = await adapter.derivatives()
                    await self._raw(
                        instrument,
                        "derivatives",
                        {
                            "oi": oi,
                            "current": current,
                            "settled": settled,
                            "interval": interval,
                            "mark": mark,
                        },
                    )
                    instrument["funding_interval_h"] = interval
                    await self._put("reg.instruments", instrument_id, instrument)
                    for raw in oi:
                        await self._verify_semantic(instrument, self._oi_key(instrument, raw), raw)
                        row = norm.oi(
                            instrument, raw, mark=mark, mark_ts=raw.get("_capture_observed_at", iso())
                        )
                        if row.get("mark_price") is not None:
                            index = current.get("indexPrice", current.get("oraclePx")) if current else None
                            row.update(
                                index_price=str(index) if index is not None else None,
                                index_as_of=raw.get("_capture_observed_at", iso())
                                if index is not None
                                else None,
                            )
                        await self._put(
                            "mkt.oi_obs", instrument_id + ":" + row["metric_key"] + ":" + row["ts"], row
                        )
                    if current:
                        from .streams import save_mark

                        await save_mark(
                            self,
                            instrument,
                            mark,
                            current.get("_capture_observed_at", iso()),
                            index=current.get("indexPrice", current.get("oraclePx")),
                            index_ts=current.get("index_time", current.get("_capture_observed_at", iso())),
                            source="PUBLIC_REST",
                        )
                        await self._funding(instrument, current, "CURRENT_ESTIMATE", interval)
                    ordered = sorted(
                        settled,
                        key=lambda r: parse_time(
                            r.get("fundingTime", r.get("fundingRateTimestamp", r.get("time")))
                        ),
                    )
                    previous = None
                    for raw in ordered:
                        stamp = parse_time(
                            raw.get("fundingTime", raw.get("fundingRateTimestamp", raw.get("time")))
                        )
                        historical_interval = (
                            Decimal(str((stamp - previous).total_seconds())) / 3600
                            if previous is not None
                            else Decimal(1)
                            if norm.venue_key(instrument) == "hyperliquid.perp"
                            else None
                        )
                        previous = stamp
                        if historical_interval is None:
                            await self._gap(
                                instrument,
                                "FUNDING_INTERVAL",
                                "Oldest settled funding interval unobserved",
                                start=stamp,
                                end=stamp,
                            )
                            continue
                        await self._funding(instrument, raw, "SETTLED", historical_interval)
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                await self._gap(instrument, kind.upper(), exc)

    async def _funding(self, instrument, raw, kind, interval, *, source="PUBLIC_REST"):
        row = norm.funding(instrument, raw, kind=kind, interval_h=interval)
        await self._verify_semantic(instrument, row["metric_key"], raw)
        row["capture_source"] = source
        row["source_path"] = norm.venue_key(instrument) + (
            ":websocket:funding" if source == "WEBSOCKET" else ":rest:funding"
        )
        row["semantic_confidence"] = (
            "VERIFIED"
            if (instrument["instrument_id"], row["metric_key"]) in self._semantic_verified
            else "UNKNOWN"
        )
        ts = row.get("as_of", row.get("funding_time"))
        await self._put(
            "mkt.funding_obs", instrument["instrument_id"] + ":" + kind + ":" + ts + ":" + source, row
        )

    async def _store_kline(self, instrument, row):
        """Enrich missing exchange quote volume only with complete matching prints."""
        iid = instrument["instrument_id"]
        if row.get("quote_volume") is None:
            flow = await self.repo.get("mkt.flow_bars_1m", iid + ":" + row["open_time"])
            if flow and flow.get("complete") and flow.get("quote_volume") is not None:
                with localcontext() as context:
                    context.prec = 100
                    quantity = Decimal(str(flow["buy_qty"])) + Decimal(str(flow["sell_qty"]))
                if quantity == Decimal(str(row["volume_native"])):
                    row["quote_volume"] = flow["quote_volume"]
                    row["quote_volume_method"] = "COMPLETE_MATCHING_RAW_PRINTS"
                    if flow.get("volume_usd") is not None:
                        row["quote_volume_usd"] = flow["volume_usd"]
                        row["quote_volume_usd_reason"] = None
        await self._put("mkt.klines_1m", iid + ":" + row["open_time"], row)

    async def _ticker(self, instrument, raw):
        await self._raw(instrument, "ticker", raw)
        key = norm.venue_key(instrument)
        if key.startswith("binance"):
            native, quote, price = raw.get("volume"), raw.get("quoteVolume"), raw.get("lastPrice")
        elif key.startswith("okx"):
            native, quote, price = raw.get("vol24h"), raw.get("volCcy24h"), raw.get("last")
        elif key.startswith("bybit"):
            native, quote, price = raw.get("volume24h"), raw.get("turnover24h"), raw.get("lastPrice")
        elif key.startswith("coinbase"):
            native, quote, price = raw.get("volume"), None, raw.get("last")
        elif key.startswith("kraken"):
            native, quote, price = raw["v"][1], None, raw["c"][0]
        else:
            native, quote, price = None, raw.get("dayNtlVlm"), raw.get("markPx")
        # OKX swap vol24h is contracts, volCcy24h is BASE rather than quote.
        if key == "okx.swap":
            native = raw.get("volCcy24h")
            quote = None
        if price is not None:
            await self._verify_semantic(
                instrument,
                key + ".reference_price",
                {"price": price, "ts": raw.get("_capture_observed_at", iso())},
            )
        fx = instrument.get("quote_usd_rate")
        usd = norm.decimal(quote) * norm.decimal(fx) if quote is not None and fx is not None else None
        row = {
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            "ts": iso(raw.get("ts", raw.get("closeTime", raw.get("_capture_observed_at", iso())))),
            "received_ts": raw.get("_capture_observed_at", iso()),
            "volume_native": native,
            "quote_volume": quote,
            "volume_usd": usd,
            "last": price,
            "mark_price": raw.get("markPrice", raw.get("markPx")),
            "quote_asset": instrument.get("quote_asset"),
            "usd_reason": None if usd is not None else "24H_QUOTE_NOTIONAL_UNAVAILABLE",
        }
        await self._put("mkt.tickers_24h", instrument["instrument_id"] + ":" + row["ts"], row)

    async def ingest_trade(self, instrument, raw, *, backfill=False):
        try:
            row = norm.trade(instrument, raw, source="REST" if backfill else "WS")
        except norm.SemanticError as exc:
            await self.invalidate_semantics(instrument, "aggressor", exc)
            raise
        iid = instrument["instrument_id"]
        key = iid + ":" + row["trade_id"]
        metric = row["metric_key"]
        previous = self._sample_ids.get((iid, metric))
        self._sample_ids[iid, metric] = row["trade_id"]
        if previous is not None and previous != row["trade_id"]:
            await self._verify_semantic(
                instrument,
                metric,
                {
                    "id_behavior": "UNIQUE",
                    "previous": previous,
                    "current": row["trade_id"],
                    "normalized_sample": row,
                },
            )
        row["semantic_confidence"] = "VERIFIED" if (iid, metric) in self._semantic_verified else "UNKNOWN"
        async with self.repo.lock("market-trade:" + iid):
            if await self.repo.get("mkt.trades", key):
                return False
            minute = iso(parse_time(row["ts"]).replace(second=0, microsecond=0))
            if (iid, minute) not in self.flow.bars:
                stored = await self._range(
                    "mkt.trades",
                    "ts",
                    start=minute,
                    end=parse_time(minute) + timedelta(minutes=1),
                    filters={"instrument_id": iid},
                )
                for prior in sorted(
                    (
                        r
                        for r in stored
                        if iso(parse_time(r["ts"]).replace(second=0, microsecond=0)) == minute
                    ),
                    key=lambda r: parse_time(r["ts"]),
                ):
                    self.flow.add(prior, continuity=False)
            async with self.repo.transaction():
                await self._put("mkt.trades", key, row, immutable=True)
                bar = self.flow.add(row, continuity=not backfill)
                if backfill:
                    self.flow.bars[iid, bar["minute_start"]]["gap_reason"] = "BACKFILL_WINDOW_NOT_VERIFIED"
                    bar["gap_reason"] = "BACKFILL_WINDOW_NOT_VERIFIED"
                await self._put("mkt.flow_bars_1m", iid + ":" + bar["minute_start"], bar)
                checkpoint = await self.repo.get("mkt.capture_state", iid)
                if checkpoint is None or parse_time(row["ts"]) >= parse_time(checkpoint["last_ts"]):
                    await self._put(
                        "mkt.capture_state",
                        iid,
                        {
                            "instrument_id": iid,
                            "last_trade_id": row["trade_id"],
                            "last_ts": row["ts"],
                            "updated_at": iso(),
                        },
                    )
        return True

    async def _save_book(self, instrument):
        iid = instrument["instrument_id"]
        now = time.monotonic()
        if self._book_due.get(iid, 0) > now:
            return
        self._book_due[iid] = now + float(self.thresholds.get("book_interval_s", 10))
        row = self.books[iid].summary()
        key = iid + ":" + row["exchange_ts"]
        row.update(second_path_confirmed=False, second_path_reason="REST_CONFIRMATION_PENDING")
        if row["integrity_ok"]:
            await self._verify_semantic(instrument, norm.venue_key(instrument) + ".book.depth", row)
        await self._put("mkt.book_summaries", key, row)
        if not row["integrity_ok"]:
            return
        previous = self._book_confirmations.get(iid)
        if previous is not None and not previous.done():
            return

        async def confirm():
            try:
                row.update(
                    await asyncio.wait_for(
                        self._independent_book(instrument, row),
                        timeout=float(self.thresholds.get("book_interval_s", 10)),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - confirmation failure must not stall capture
                row.update(second_path_confirmed=False, second_path_reason=str(exc))
            await self._put("mkt.book_summaries", key, row)

        task = asyncio.create_task(confirm())
        self._book_confirmations[iid] = task
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _flush_flow(self):
        monotonic_now = time.monotonic()
        if monotonic_now - self._last_flow_flush < 1:
            return
        self._last_flow_flush = monotonic_now
        now = utcnow()
        for iid, since in list(self._connected_since.items()):
            cursor = self._zero_cursor.get(iid, since.replace(second=0, microsecond=0))
            cursor = max(
                cursor,
                since.replace(second=0, microsecond=0)
                + (timedelta(minutes=1) if since.second or since.microsecond else timedelta()),
            )
            while cursor + timedelta(minutes=1) <= now:
                key = (iid, iso(cursor))
                if key not in self.flow.bars:
                    inst = self.instruments[iid]
                    row = {
                        "instrument_id": iid,
                        "token_id": inst["token_id"],
                        "minute_start": iso(cursor),
                        "buy_qty": "0",
                        "sell_qty": "0",
                        "buy_notional": "0",
                        "sell_notional": "0",
                        "n_buy": 0,
                        "n_sell": 0,
                        "sum_sq_qty": "0",
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": None,
                        "vwap": None,
                        "size_buckets": {
                            f"B{i}": {
                                "buy_qty": "0",
                                "sell_qty": "0",
                                "buy_notional": "0",
                                "sell_notional": "0",
                                "n": 0,
                            }
                            for i in range(1, 6)
                        },
                        "first_trade_id": None,
                        "last_trade_id": None,
                        "complete": True,
                        "gap_reason": None,
                        "zero_trade_minute": True,
                    }
                    # Existing backfilled prints must not be overwritten by an empty accumulator.
                    if await self.repo.get("mkt.flow_bars_1m", iid + ":" + key[1]) is None:
                        await self._put("mkt.flow_bars_1m", iid + ":" + key[1], row)
                cursor += timedelta(minutes=1)
            self._zero_cursor[iid] = cursor
        for key in list(self.flow.bars):
            if parse_time(key[1]) + timedelta(minutes=1) > now:
                continue
            since = self._connected_since.get(key[0])
            complete = self.flow.bars[key].get("_backfill_verified", False) or (
                since is not None and since <= parse_time(key[1])
            )
            row = self.flow.record(key, complete=complete)
            row["received_ts"] = iso()
            fingerprint = (
                row["last_trade_id"],
                row["n_buy"],
                row["n_sell"],
                row["complete"],
                row["gap_reason"],
            )
            if self._flushed_bars.get(key) == fingerprint:
                if parse_time(key[1]) + timedelta(minutes=3) < now:
                    self.flow.bars.pop(key, None)
                    self._flushed_bars.pop(key, None)
                continue
            existing = await self.repo.get("mkt.flow_bars_1m", key[0] + ":" + key[1])
            if not (
                existing
                and (existing.get("source_archive") or existing.get("backfill_verified"))
                and existing.get("complete")
                and not row["complete"]
            ):
                await self._put("mkt.flow_bars_1m", key[0] + ":" + key[1], row)
            self._flushed_bars[key] = fingerprint
            # Keep one recent minute; historical normalization uses durable trades.
            if parse_time(key[1]) + timedelta(minutes=3) < now:
                self.flow.bars.pop(key, None)
                self._flushed_bars.pop(key, None)

    @staticmethod
    def _ranges_cover(ranges, start, end):
        cursor = start
        for lo, hi in sorted(ranges):
            if lo > cursor:
                return False
            cursor = max(cursor, hi)
            if cursor >= end:
                return True
        return cursor >= end

    async def _persist_rest_flow_ranges(self, instrument, start, end, proven_ranges):
        if not proven_ranges:
            return
        iid = instrument["instrument_id"]
        accumulator = FlowAccumulator()
        bad_minutes = set()
        previous = None
        range_minute_counts = [{} for _ in proven_ranges]
        async for row in self.repo.iter_range(
            "mkt.trades", "ts", start=start, end=end, filters={"instrument_id": iid}, batch_size=10000
        ):
            if (
                previous
                and row["venue"] in ("binance", "coinbase", "kraken")
                and int(row["trade_id"]) != int(previous["trade_id"]) + 1
            ):
                bad_minutes.update(
                    [
                        iso(parse_time(row["ts"]).replace(second=0, microsecond=0)),
                        iso(parse_time(previous["ts"]).replace(second=0, microsecond=0)),
                    ]
                )
            accumulator.add(row, continuity=False)
            stamp = iso(parse_time(row["ts"]).replace(second=0, microsecond=0))
            ts = parse_time(row["ts"])
            for index, (lo, hi) in enumerate(proven_ranges):
                if lo <= ts < hi:
                    range_minute_counts[index][stamp] = range_minute_counts[index].get(stamp, 0) + 1
            previous = row
        for range_index, (lo, hi) in enumerate(proven_ranges):
            bad = any(parse_time(m) < hi and parse_time(m) + timedelta(minutes=1) > lo for m in bad_minutes)
            review_id = "coverage:" + iid + ":" + sha256([iso(lo), iso(hi)])
            await self._put(
                "qa.reviews",
                review_id,
                {
                    "review_id": review_id,
                    "subject": "MARKET_CAPTURE_COVERAGE",
                    "label": "INCOMPLETE" if bad else "COMPLETE",
                    "at": iso(),
                    "instrument_id": iid,
                    "start": iso(lo),
                    "end": iso(hi),
                    "complete": not bad,
                    "reason": "REST_TRADE_ID_GAP" if bad else None,
                    "observed_through": iso(hi),
                    "source": "EXHAUSTED_REST_RANGE",
                    "minute_trade_counts": range_minute_counts[range_index],
                    "missing_minute_count_means": "EXHAUSTED_RANGE_OBSERVED_ZERO",
                },
            )
        minute = start.replace(second=0, microsecond=0)
        if minute < start:
            minute += timedelta(minutes=1)
        while minute + timedelta(minutes=1) <= end:
            if self._ranges_cover(proven_ranges, minute, minute + timedelta(minutes=1)):
                stamp = iso(minute)
                key = (iid, stamp)
                if key in accumulator.bars:
                    bar = accumulator.record(key, complete=stamp not in bad_minutes)
                else:
                    bar = {
                        "instrument_id": iid,
                        "token_id": instrument["token_id"],
                        "minute_start": stamp,
                        "buy_qty": "0",
                        "sell_qty": "0",
                        "buy_notional": "0",
                        "sell_notional": "0",
                        "n_buy": 0,
                        "n_sell": 0,
                        "sum_sq_qty": "0",
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": None,
                        "vwap": None,
                        "size_buckets": {
                            f"B{i}": {
                                "buy_qty": "0",
                                "sell_qty": "0",
                                "buy_notional": "0",
                                "sell_notional": "0",
                                "n": 0,
                            }
                            for i in range(1, 6)
                        },
                        "first_trade_id": None,
                        "last_trade_id": None,
                        "complete": stamp not in bad_minutes,
                        "gap_reason": None,
                        "zero_trade_minute": True,
                    }
                bar["received_ts"] = iso()
                bar.update(
                    backfill_verified=bar["complete"],
                    capture_method="REST_RANGE_EXHAUSTED",
                    capture_proof={
                        "range_set_sha256": sha256([[iso(lo), iso(hi)] for lo, hi in proven_ranges]),
                        "continuity": "ID_CONSECUTIVE"
                        if norm.venue_key(instrument).split(".")[0] in ("binance", "coinbase", "kraken")
                        else "EXHAUSTED_PAGINATED_ENDPOINT",
                    },
                )
                if stamp in bad_minutes:
                    bar["gap_reason"] = "REST_TRADE_ID_GAP"
                await self._put("mkt.flow_bars_1m", iid + ":" + stamp, bar)
                self.flow.bars.pop(key, None)
                self._flushed_bars.pop(key, None)
            minute += timedelta(minutes=1)

    async def backfill(self, instrument, anchor, *, checkpoint=None):
        """Page immutable prints. Unproven endpoints/retention remain explicit gaps."""
        key = norm.venue_key(instrument)
        start = parse_time(checkpoint["last_ts"]) if checkpoint else parse_time(anchor) - timedelta(hours=24)
        end = parse_time(anchor)
        adapter = self.adapter(instrument, priority=1)
        cursor = None
        seen = set()
        times = []
        proven_ranges = []
        # Binance timestamp slices cannot exceed one hour. Others paginate to oldest print.
        slices = (
            [
                (start + timedelta(hours=i), min(start + timedelta(hours=i + 1), end))
                for i in range(max(1, int((end - start).total_seconds() // 3600) + 1))
                if start + timedelta(hours=i) < end
            ]
            if key.startswith("binance")
            else [(start, end)]
        )
        try:
            for slice_index, (lo, hi) in enumerate(slices):
                cursor = (
                    int(checkpoint["last_trade_id"]) + 1
                    if checkpoint and key.startswith("binance") and slice_index == 0
                    else None
                )
                exhausted = False
                for _ in range(10000):
                    rows, next_cursor = await adapter.trades(
                        start=lo, end=hi - timedelta(milliseconds=1), cursor=cursor
                    )
                    if not rows:
                        if key.startswith("binance"):
                            exhausted = True
                        break
                    await self._raw(
                        instrument,
                        "backfill_trades",
                        {
                            "request": {
                                "start": iso(lo),
                                "end": iso(hi - timedelta(milliseconds=1)),
                                "cursor": cursor,
                            },
                            "response": rows,
                        },
                    )
                    normalized = []
                    for raw in rows:
                        t = norm.trade(instrument, raw)
                        normalized.append((t, raw))
                    normalized.sort(key=lambda x: parse_time(x[0]["ts"]))
                    for t, raw in normalized:
                        ts = parse_time(t["ts"])
                        if start <= ts < end:
                            times.append(ts)
                            await self.ingest_trade(instrument, raw, backfill=True)
                    if key.startswith("binance"):
                        if len(rows) < 1000 or parse_time(normalized[-1][0]["ts"]) >= hi:
                            exhausted = True
                            break
                        next_cursor = int(normalized[-1][0]["trade_id"]) + 1
                    elif key.startswith("kraken"):
                        if parse_time(normalized[-1][0]["ts"]) >= end:
                            exhausted = bool(times and min(times) <= start)
                            break
                    elif parse_time(normalized[0][0]["ts"]) <= start:
                        exhausted = True
                        break
                    if next_cursor is None or str(next_cursor) in seen:
                        break
                    seen.add(str(next_cursor))
                    cursor = next_cursor
                if exhausted:
                    proven_ranges.append((lo, min(hi, utcnow())))
            await self._persist_rest_flow_ranges(instrument, start, end, proven_ranges)
            for hours in () if checkpoint else (24, 4, 1):
                lo = end - timedelta(hours=hours)
                if not self._ranges_cover(proven_ranges, lo, end):
                    await self._gap(
                        instrument,
                        "PRE_EVENT_FLOW",
                        f"{hours}h window not fully covered by retained REST trades",
                        start=lo,
                        end=end,
                    )
        except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
            await self._gap(instrument, "PRE_EVENT_FLOW", exc, start=start, end=end)
        # Verified Binance print archives cover only fully published historical days.
        # Retention/ID gaps are retained as evidence even if an archive repairs coverage.
        if checkpoint and not self._ranges_cover(proven_ranges, start, end):
            await self._gap(instrument, "RECONNECT", "CHECKPOINT_RANGE_NOT_EXHAUSTED", start=start, end=end)
        if not checkpoint and key.startswith("binance"):
            missing = not self._ranges_cover(proven_ranges, start, end)
            if missing:
                from .archive import fetch_archive

                day = start.date()
                while day <= end.date():
                    if day < utcnow().date():
                        try:
                            await fetch_archive(
                                self.repo,
                                self.settings,
                                instrument["venue_symbol"],
                                day.isoformat(),
                                market_type=instrument["market_type"],
                                kind="aggTrades",
                                http=self.http,
                            )
                        except Exception as exc:  # noqa: BLE001 - archive absence preserves explicit missing historical coverage
                            await self._gap(
                                instrument,
                                "TRADE_ARCHIVE",
                                exc,
                                start=day.isoformat(),
                                end=iso(parse_time(day.isoformat()) + timedelta(days=1)),
                            )
                    day += timedelta(days=1)
        # Backfill OHLC without pretending klines are aggressive flow.
        for offset in range(0, 24 * 60, 240):
            lo = start + timedelta(minutes=offset)
            hi = min(lo + timedelta(minutes=240), end)
            try:
                rows = await adapter.klines(start=lo, end=hi)
                await self._raw(instrument, "backfill_klines", rows)
                for raw in rows:
                    row = norm.kline(instrument, raw)
                    if start <= parse_time(row["open_time"]) < end:
                        await self._store_kline(instrument, row)
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                await self._gap(instrument, "PRE_EVENT_KLINES", exc, start=lo, end=hi)

    async def escalate(self, event_id, tokens):
        if self.capture_only:
            raise RuntimeError("Token escalation is disabled in capture-only phase")
        async with self.repo.lock("market-hot-set"):
            now = iso()
            event = await self.repo.get("evt.events", event_id) or {}
            anchor_record = event_anchor(event) if event else {"type": "KNOWN_TO_US_AT", "ts": now}
            anchor = anchor_record["ts"]
            assessment = (
                await self.repo.get("evt.ems_assessments", event["current_ems_id"])
                if event.get("current_ems_id")
                else None
            )
            if assessment is None:
                assessments = await self.repo.list(
                    "evt.ems_assessments", filters={"event_id": event_id}, limit=10000
                )
                assessment = (
                    max(assessments, key=lambda r: (int(r.get("version", 0)), r.get("cutoff", "")))
                    if assessments
                    else {}
                )
            event_priority = {"LOW": 1, "MATERIAL": 2, "MAJOR": 3, "STRUCTURAL_SHOCK": 4}.get(
                assessment.get("class"), 1
            )
            for value in tokens:
                token_id = value["token_id"] if isinstance(value, dict) else value
                if token_id not in self.tokens:
                    raise ValueError("unknown token " + token_id)
                priority = (
                    int(value.get("priority", event_priority)) if isinstance(value, dict) else event_priority
                )
                key = token_id + ":" + event_id
                existing = await self.repo.get("mkt.hot_tokens", key)
                protected = await self.hot_protected(event_id, token_id)
                await self._put(
                    "mkt.hot_tokens",
                    key,
                    {
                        "token_id": token_id,
                        "event_id": event_id,
                        "priority": priority,
                        "since": existing.get("since", anchor) if existing else anchor,
                        "expires_at": None if protected else iso(parse_time(anchor) + timedelta(hours=72)),
                    },
                )
            hot = await self.repo.list("mkt.hot_tokens", limit=10000)
            group = {}
            for r in hot:
                group.setdefault(r["token_id"], []).append(r)
            cap = int(self.thresholds.get("hot_token_cap", 15))
            ranked = sorted(
                group,
                key=lambda t: (
                    max(x["priority"] for x in group[t]),
                    max(
                        x["since"] for x in group[t] if x["priority"] == max(y["priority"] for y in group[t])
                    ),
                ),
                reverse=True,
            )
            for token_id in ranked[cap:]:
                for r in group[token_id]:
                    await self.repo.delete("mkt.hot_tokens", token_id + ":" + r["event_id"])
            retained = set(ranked[:cap])
        if self.settings.replay:
            return
        await self._sync_streams()
        for token_id in retained:
            if token_id in [v.get("token_id") if isinstance(v, dict) else v for v in tokens]:
                await self._schedule_event_backfill(event_id, token_id, anchor)

    async def _capture_window_coverage(self, instrument, anchor):
        end = parse_time(anchor)
        result = {"instrument_id": instrument["instrument_id"], "windows": {}}
        for hours in (1, 4, 24):
            start = end - timedelta(hours=hours)
            lo = start.replace(second=0, microsecond=0)
            if start > lo:
                lo += timedelta(minutes=1)
            hi = end.replace(second=0, microsecond=0)
            expected = max(0, int((hi - lo).total_seconds() // 60))
            metrics = {}
            for name, collection, field in [
                ("flow", "mkt.flow_bars_1m", "minute_start"),
                ("klines", "mkt.klines_1m", "open_time"),
            ]:
                rows = await self._range(
                    collection,
                    field,
                    start=lo,
                    end=hi,
                    filters={"instrument_id": instrument["instrument_id"]},
                )
                complete = sum(1 for r in rows if r.get("complete"))
                metrics[name] = {
                    "expected_minutes": expected,
                    "complete_minutes": complete,
                    "missing_or_incomplete_minutes": max(0, expected - complete),
                    "complete": complete == expected and expected > 0,
                }
            result["windows"][f"W{hours}H"] = dict(
                start=iso(start),
                end=iso(end),
                observed_bucket_start=iso(lo),
                observed_bucket_end=iso(hi),
                timestamp_precision="BUCKET",
                unresolved_boundary_seconds=(lo - start).total_seconds() + (end - hi).total_seconds(),
                **metrics,
            )
        result["complete"] = all(
            w["flow"]["complete"] and w["klines"]["complete"] and w["unresolved_boundary_seconds"] == 0
            for w in result["windows"].values()
        )
        return result

    async def wait_capture(self, event_id, token_id, *, timeout_s=30):
        """Await durable prerequisite or return an explicit bounded incomplete result.

        Capture workers and sockets continue independently; cancellation never cancels
        the prerequisite capture obligation.
        """
        started = time.monotonic()
        latest = None
        while True:
            rows = await self.repo.list(
                "mkt.backfill_runs", filters={"event_id": event_id, "token_id": token_id}, limit=10000
            )
            latest = max(rows, key=lambda row: row["started_at"]) if rows else None
            elapsed = time.monotonic() - started
            terminal = latest and latest["status"] in ("COMPLETE", "PARTIAL", "FAILED")
            if terminal or elapsed >= timeout_s:
                result = {
                    "status": latest["status"] if terminal else "INCOMPLETE",
                    "backfill_id": latest["backfill_id"] if latest else None,
                    "reason": latest.get("reason") if terminal else "BOUNDED_PREREQUISITE_WAIT_EXPIRED",
                    "bounded_wait_s": elapsed,
                    "observed_at": iso(),
                    "incomplete_windows_excluded": True,
                }
                if latest:
                    self._capture_waits[latest["backfill_id"]] = result
                return result
            await asyncio.sleep(min(0.25, max(0, timeout_s - elapsed)))

    async def _schedule_event_backfill(self, event_id, token_id, anchor):
        marker = (token_id, event_id, iso(anchor))
        if marker in self._backfills:
            return
        backfill_id = "BF-" + sha256({"event_id": event_id, "token_id": token_id, "anchor": iso(anchor)})
        old = await self.repo.get("mkt.backfill_runs", backfill_id)
        if old and old.get("status") in ("COMPLETE", "PARTIAL"):
            self._backfills.add(marker)
            return
        row = {
            "backfill_id": backfill_id,
            "event_id": event_id,
            "token_id": token_id,
            "anchor": iso(anchor),
            "started_at": iso(),
            "finished_at": None,
            "status": "PENDING",
            "reason": "PRE_EVENT_CAPTURE_PENDING",
            "result": None,
        }
        await self._put("mkt.backfill_runs", backfill_id, row)
        self._backfills.add(marker)

        async def capture():
            try:
                row.update(status="RUNNING")
                await self._put("mkt.backfill_runs", backfill_id, row)
                instruments = [i for i in self.instruments.values() if i["token_id"] == token_id]
                await asyncio.gather(*(self.backfill(inst, anchor) for inst in instruments))
                coverage = await asyncio.gather(
                    *(self._capture_window_coverage(inst, anchor) for inst in instruments)
                )
                ready = bool(coverage) and all(r["complete"] for r in coverage)
                row.update(
                    status="COMPLETE" if ready else "PARTIAL",
                    finished_at=iso(),
                    reason=None if ready else "INCOMPLETE_VENUE_WINDOWS_EXCLUDED",
                    result={"instruments": coverage},
                )
                async with self.repo.transaction():
                    await self._put("mkt.backfill_runs", backfill_id, row)
            except asyncio.CancelledError:
                # Persisted RUNNING obligation is resumed from hot membership after restart.
                raise
            except Exception as exc:  # noqa: BLE001 - durable failed prerequisite must remain inspectable
                row.update(status="FAILED", finished_at=iso(), reason=str(exc))
                await self._put("mkt.backfill_runs", backfill_id, row)

        task = asyncio.create_task(capture())
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def capture_changes(self, since=None, end=None):
        """Durable historical completions and coalesced actual late normalized data."""
        async for row in self.repo.iter_range(
            "mkt.backfill_runs", "finished_at", start=since, end=end, batch_size=1000
        ):
            if row.get("status") not in ("COMPLETE", "PARTIAL", "FAILED") or not row.get("finished_at"):
                continue
            yield {
                **row,
                "change_id": "CAP-"
                + sha256(
                    {
                        "backfill_id": row["backfill_id"],
                        "status": row["status"],
                        "finished_at": row["finished_at"],
                    }
                ),
                "kind": "HISTORICAL_CAPTURE_TERMINAL",
            }
        async for change in self._late_normalized_changes(since, end):
            yield change

    async def _late_normalized_changes(self, since, end):
        fields = ["event_id", "token_id", "anchor", "checkpoint", "revision", "captured_at", "window_end"]
        latest = {}
        async for row in self.repo.iter_metadata("out.reaction_checkpoints", fields, batch_size=1000):
            if not row.get("anchor") or not row.get("window_end") or not row.get("captured_at"):
                continue  # Legacy unknown availability/window metadata is not invented.
            key = (row["event_id"], row["token_id"], row["checkpoint"])
            if key not in latest or int(row.get("revision") or 1) > int(latest[key].get("revision") or 1):
                latest[key] = row
        if not latest:
            return
        targets = {}
        for row in latest.values():
            anchor = row["anchor"]["ts"] if isinstance(row["anchor"], dict) else row["anchor"]
            key = (row["event_id"], row["token_id"], iso(anchor))
            target = targets.setdefault(
                key, {"windows": [], "digest": hashlib.sha256(), "count": 0, "highwater": None}
            )
            target["windows"].append((parse_time(row["window_end"]), parse_time(row["captured_at"])))
        max_cutoff = max(parse_time(row["window_end"]) for row in latest.values())
        first_capture = min(parse_time(row["captured_at"]) for row in latest.values())
        start = max(parse_time(since), first_capture) if since is not None else first_capture
        benchmark_tokens = {
            token_id for token_id, token in self.tokens.items() if token.get("symbol") in ("BTC", "ETH")
        }
        peers = {
            token_id
            for token_id, token in self.tokens.items()
            if str(token.get("sector", "")).upper() == "DEFI"
        }
        families = [
            ("mkt.trades", "ts", 0),
            ("mkt.flow_bars_1m", "minute_start", 60),
            ("mkt.klines_1m", "open_time", 60),
            ("mkt.book_summaries", "exchange_ts", 0),
            ("mkt.oi_obs", "ts", 0),
            ("mkt.funding_obs", "as_of", 0),
            ("mkt.funding_obs", "funding_time", 0),
            ("mkt.liq_obs", "ts", 0),
            ("mkt.tickers_24h", "ts", 0),
            ("mkt.supply_obs", "ts", 0),
        ]
        for collection, semantic_field, offset in families:
            async for row in self.repo.iter_late_arrivals(
                collection,
                "recorded_at",
                semantic_field,
                since=start,
                end=end or iso(),
                market_end=max_cutoff - timedelta(seconds=offset),
                batch_size=1000,
            ):
                stamp = parse_time(row[semantic_field]) + timedelta(seconds=offset)
                arrival = parse_time(row["recorded_at"])
                source_token = row.get("token_id") or self.instruments.get(row.get("instrument_id"), {}).get(
                    "token_id"
                )
                if source_token is None:
                    continue
                signature = sha256({"collection": collection, "data": row})
                for key, target in targets.items():
                    _, token_id, anchor = key
                    comparator = source_token in benchmark_tokens and collection in (
                        "mkt.trades",
                        "mkt.flow_bars_1m",
                        "mkt.klines_1m",
                    )
                    comparator = comparator or (source_token in peers and collection == "mkt.klines_1m")
                    if source_token != token_id and not comparator:
                        continue
                    if stamp < parse_time(anchor) - timedelta(days=90):
                        continue
                    if not any(
                        stamp <= cutoff and arrival >= captured for cutoff, captured in target["windows"]
                    ):
                        continue
                    target["digest"].update(signature.encode())
                    target["count"] += 1
                    target["highwater"] = (
                        max(target["highwater"], arrival) if target["highwater"] else arrival
                    )
        for (event_id, token_id, anchor), target in targets.items():
            if target["count"]:
                fingerprint = target["digest"].hexdigest()
                yield {
                    "event_id": event_id,
                    "token_id": token_id,
                    "anchor": anchor,
                    "observed_at": iso(target["highwater"]),
                    "kind": "LATE_NORMALIZED_DATA",
                    "status": "AVAILABLE",
                    "change_id": "CAP-LIVE-"
                    + sha256(
                        {
                            "event_id": event_id,
                            "token_id": token_id,
                            "anchor": anchor,
                            "highwater": iso(target["highwater"]),
                            "fingerprint": fingerprint,
                        }
                    ),
                    "source_count": target["count"],
                    "semantic_fingerprint": fingerprint,
                    "reason": "Actual late normalized data became available after frozen reaction capture",
                }

    async def capture_readiness(self, event_id, token_id, *, anchor=None):
        if anchor is not None:
            # Same deterministic primary key as schedule: exact anchor cannot inherit
            # a completed obligation for a previous version of the event time.
            backfill_id = "BF-" + sha256({"event_id": event_id, "token_id": token_id, "anchor": iso(anchor)})
            row = await self.repo.get("mkt.backfill_runs", backfill_id)
            if (
                row
                and row.get("event_id") == event_id
                and row.get("token_id") == token_id
                and iso(row["anchor"]) == iso(anchor)
            ):
                return row
            return {
                "status": "NOT_REQUESTED",
                "reason": "NO_DURABLE_CAPTURE_REQUEST_FOR_EXACT_ANCHOR",
                "anchor": iso(anchor),
            }
        latest = None
        async for row in self.repo.iter_range(
            "mkt.backfill_runs",
            "started_at",
            filters={"event_id": event_id, "token_id": token_id},
            batch_size=1000,
        ):
            if latest is None or row["started_at"] > latest["started_at"]:
                latest = row
        return latest or {"status": "NOT_REQUESTED", "reason": "NO_DURABLE_CAPTURE_REQUEST"}

    async def hot_protected(self, event_id, token_id, now=None):
        now = parse_time(now) if now is not None else utcnow()
        calls = await self.repo.list(
            "ana.calls", filters={"event_id": event_id, "token_id": token_id, "status": "OPEN"}, limit=1
        )
        if calls:
            return True
        async for condition in self.repo.iter_all(
            "ana.watch_conditions", filters={"event_id": event_id, "token_id": token_id, "status": "ARMED"}
        ):
            if condition.get("expires_at") is None or parse_time(condition["expires_at"]) > now:
                return True
        return False

    async def refresh_hot_expiry(self, now=None):
        """Reconcile persisted protection after restart and call/watch transitions."""
        rows = await self.repo.list("mkt.hot_tokens", limit=10000)
        result = []
        for row in rows:
            protected = await self.hot_protected(row["event_id"], row["token_id"], now)
            event = await self.repo.get("evt.events", row["event_id"])
            anchor = event_anchor(event)["ts"] if event else row["since"]
            expiry = None if protected else iso(parse_time(anchor) + timedelta(hours=72))
            if row.get("expires_at") != expiry:
                row = {**row, "expires_at": expiry}
                await self._put("mkt.hot_tokens", row["token_id"] + ":" + row["event_id"], row)
            result.append(row)
        return result

    async def demote(self, token_id, event_id=None):
        for row in await self.repo.list("mkt.hot_tokens", filters={"token_id": token_id}, limit=10000):
            if event_id is None or row["event_id"] == event_id:
                await self.repo.delete("mkt.hot_tokens", token_id + ":" + row["event_id"])
        await self._sync_streams()

    async def _sync_streams(self):
        if not self.capture_only and time.monotonic() - self._last_hot_expiry_refresh >= 60:
            await self.refresh_hot_expiry()
            self._last_hot_expiry_refresh = time.monotonic()
        hot = (
            set()
            if self.capture_only
            else {r["token_id"] for r in await self.repo.list("mkt.hot_tokens", limit=10000)}
        )
        desired = {}
        for iid, inst in self.instruments.items():
            if inst["token_id"] in hot:
                desired.setdefault(endpoint_key(inst), set()).add(iid)
        for endpoint, task in list(self.streams.items()):
            if endpoint not in desired:
                self._stream_groups.pop(endpoint, None)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                self.streams.pop(endpoint, None)
                self._stream_controls.pop(endpoint, None)
        for endpoint, members in desired.items():
            previous = self._stream_groups.get(endpoint, set())
            self._stream_groups[endpoint] = members
            if self._stop is None or self.settings.replay:
                continue
            if endpoint not in self.streams or self.streams[endpoint].done():
                self._stream_controls[endpoint] = asyncio.Queue()
                self.streams[endpoint] = asyncio.create_task(stream_group_loop(self, endpoint))
            elif members != previous:
                self._stream_controls[endpoint].put_nowait(set(members))

    @norm.precise
    async def _independent_book(self, instrument, stream):
        key = norm.venue_key(instrument)
        s = instrument["venue_symbol"]
        adapter = self.adapter(instrument, priority=2)
        if key.startswith("binance"):
            raw = await self._request(
                "GET",
                (
                    "https://api.binance.com/api/v3/ticker/bookTicker"
                    if key.endswith("spot")
                    else "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
                ),
                params={"symbol": s},
                _priority=2,
            )
            bid, ask = raw["bidPrice"], raw["askPrice"]
            ts = raw.get("time", iso())
        elif key.startswith("okx"):
            raw = await adapter.ticker()
            bid, ask = raw["bidPx"], raw["askPx"]
            ts = raw["ts"]
        elif key.startswith("bybit"):
            raw = await adapter.ticker()
            bid, ask = raw["bid1Price"], raw["ask1Price"]
            ts = iso()
        elif key.startswith("coinbase"):
            raw = await self._request("GET", "https://api.exchange.coinbase.com/products/" + s + "/ticker")
            bid, ask = raw["bid"], raw["ask"]
            ts = iso()
        elif key.startswith("kraken"):
            raw = await adapter.ticker()
            bid, ask = raw["b"][0], raw["a"][0]
            ts = iso()
        else:
            raw = await adapter.info({"type": "l2Book", "coin": s})
            bid, ask = raw["levels"][0][0]["px"], raw["levels"][1][0]["px"]
            ts = raw["time"]
        await self._raw(instrument, "independent_rest_book", raw)
        bid, ask = norm.decimal(bid), norm.decimal(ask)
        if bid <= 0 or ask <= bid:
            raise norm.SemanticError("independent REST book is empty or crossed")
        mid = (bid + ask) / 2
        spread = (ask - bid) / mid * 10000
        divergence = abs(mid / norm.decimal(stream["mid"]) - 1) * 10000
        tick = instrument.get("tick_size")
        tick_bps = norm.decimal(tick) / mid * 10000 if tick else Decimal(0)
        tolerance = max(spread, norm.decimal(stream["spread_bps"]), 2 * tick_bps)
        return jsonable(
            {
                "second_path_confirmed": divergence <= tolerance,
                "independent_rest": {
                    "best_bid": bid,
                    "best_ask": ask,
                    "mid": mid,
                    "spread_bps": spread,
                    "ts": iso(ts),
                    "received_ts": iso(),
                    "source": "PUBLIC_REST",
                    "price_divergence_bps": divergence,
                    "tolerance_bps": tolerance,
                    "tolerance_method": "MAX_OBSERVED_SPREAD_OR_TWO_TICKS",
                },
                "second_path_reason": None if divergence <= tolerance else "REST_STREAM_DISAGREEMENT",
            }
        )

    def _oi_key(self, instrument, raw):
        key = norm.venue_key(instrument)
        return (
            key + (".openInterestHist" if "sumOpenInterest" in raw else ".openInterest")
            if key == "binance.usdm"
            else key + (".ctx" if key.startswith("hyperliquid") else ".open-interest")
        )

    async def _verify_semantic(self, instrument, metric_key, sample):
        try:
            return await self._verify_semantic_sample(instrument, metric_key, sample)
        except (norm.SemanticError, KeyError, TypeError, ValueError) as exc:
            definition = next((row for row in self.semantics if row["metric_key"] == metric_key), {})
            await self.invalidate_semantics(instrument, definition.get("metric_family"), exc)
            raise

    async def _verify_semantic_sample(self, instrument, metric_key, sample):
        scope = (instrument["instrument_id"], metric_key)
        already_verified = scope in self._semantic_verified
        definition = next((r for r in self.semantics if r["metric_key"] == metric_key), None)
        if definition is None:
            return
        # Strict field/unit normalization is the adapter test; documented sidedness remains
        # UNKNOWN for Hyperliquid even after successful native quantity capture.
        if metric_key.endswith((".openInterest", ".openInterestHist", ".open-interest", ".ctx")):
            norm.oi(instrument, sample)
        elif metric_key.endswith(".funding"):
            if instrument.get("funding_interval_h") is None:
                return
            rate = sample.get(
                "fundingRate", sample.get("lastFundingRate", sample.get("funding", sample.get("r")))
            )
            norm.decimal(rate)
            if norm.decimal(instrument["funding_interval_h"]) <= 0:
                raise norm.SemanticError("invalid funding interval")
        elif metric_key.endswith(".book.depth"):
            if instrument.get("metadata_status") != "LIVE_VERIFIED" or not sample.get("integrity_ok"):
                return
            if not sample.get("bids") or not sample.get("asks"):
                return
            if definition.get("unit") == "USD_NOTIONAL" and sample.get("quote_usd_rate") is None:
                return
            if norm.decimal(sample["best_bid"]) >= norm.decimal(sample["best_ask"]):
                raise norm.SemanticError("crossed book")
        elif metric_key.endswith(".reference_price"):
            if norm.decimal(sample["price"]) <= 0:
                raise norm.SemanticError("invalid reference price")
            parse_time(sample["ts"])
        elif metric_key.endswith(".liquidations"):
            norm.liquidation(instrument, sample)
        else:
            samples = sample.get("samples") or [sample.get("normalized_sample")]
            if sample.get("id_behavior") != "UNIQUE" or not samples or samples[0] is None:
                return
            for normalized in samples:
                if normalized.get("metric_key") != metric_key:
                    raise norm.SemanticError("sample semantic mismatch")
                if norm.decimal(normalized["qty_native"]) <= 0:
                    raise norm.SemanticError("sample native quantity invalid")
        if metric_key not in instrument.setdefault("semantic_verified_keys", []):
            instrument["semantic_verified_keys"].append(metric_key)
        if already_verified:
            return
        row = dict(
            definition,
            semantic_confidence="VERIFIED",
            verification_status="LIVE_SELF_TEST_PASSED",
            verified_at=iso(),
            verified_by="tank.market",
            sample_sha256=sha256(sample),
            instrument_id=instrument["instrument_id"],
        )
        review_id = "semantic:" + instrument["instrument_id"] + ":" + metric_key
        proof = {
            "review_id": review_id,
            "subject": "METRIC_SEMANTICS",
            "label": "VERIFIED",
            "at": iso(),
            "instrument_id": instrument["instrument_id"],
            "metric_key": metric_key,
            "semantic_confidence": "VERIFIED",
            "semantics": row,
        }
        await self._put("qa.reviews", review_id, proof)
        self._semantic_verified.add(scope)
        if metric_key not in instrument.setdefault("semantic_verified_keys", []):
            instrument["semantic_verified_keys"].append(metric_key)

    async def invalidate_semantics(self, instrument, family, reason):
        for definition in self.semantics:
            metric = definition["metric_key"]
            if not metric.startswith(norm.venue_key(instrument) + "."):
                continue
            if family and definition.get("metric_family") != family:
                continue
            self._semantic_verified.discard((instrument["instrument_id"], metric))
            if metric in instrument.get("semantic_verified_keys", []):
                instrument["semantic_verified_keys"].remove(metric)
            review_id = "semantic:" + instrument["instrument_id"] + ":" + metric
            await self._put(
                "qa.reviews",
                review_id,
                {
                    "review_id": review_id,
                    "subject": "METRIC_SEMANTICS",
                    "label": "UNKNOWN",
                    "at": iso(),
                    "instrument_id": instrument["instrument_id"],
                    "metric_key": metric,
                    "semantic_confidence": "UNKNOWN",
                    "semantics": dict(
                        definition,
                        instrument_id=instrument["instrument_id"],
                        semantic_confidence="UNKNOWN",
                        verification_status="SELF_TEST_FAILED",
                        failure=str(reason),
                        verified_at=iso(),
                    ),
                },
            )

    async def self_test(self, instrument_id):
        instrument = self.instruments[instrument_id]
        results = []
        try:
            await self.discover(instrument)
            rows, _ = await self.adapter(instrument).trades()
            samples = [norm.trade(instrument, r, source="REST") for r in rows]
            if len({r["trade_id"] for r in samples}) < 2:
                raise norm.SemanticError("need at least two distinct public trade IDs")
            await self._verify_semantic(
                instrument, samples[0]["metric_key"], {"id_behavior": "UNIQUE", "samples": samples[:2]}
            )
            results.append({"metric_key": samples[0]["metric_key"], "status": "PASS"})
        except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
            results.append({"instrument_id": instrument_id, "status": "UNKNOWN", "reason": str(exc)})
            if isinstance(exc, (norm.SemanticError, KeyError, ValueError, TypeError)):
                await self.invalidate_semantics(instrument, "aggressor", exc)
            else:
                await self._gap(instrument, "SELF_TEST_UNAVAILABLE", exc)
        return results

    async def _range(self, collection, field, *, start=None, end=None, filters=None):
        return [
            row
            async for row in self.repo.iter_range(
                collection, field, start=start, end=end, filters=filters, batch_size=10000
            )
        ]

    async def _boundary_capture(self, ids, cutoff, anchor):
        end = parse_time(cutoff)
        points = {end}
        for hours in (1, 2, 4, 8, 24, 48):
            points.add(end - timedelta(hours=hours))
        if anchor:
            at = parse_time(anchor.get("ts") if isinstance(anchor, dict) else anchor)
            points.add(at)
            points.add(at - (end - at))
        minutes = {p.replace(second=0, microsecond=0) for p in points if p.second or p.microsecond}
        trades = {}
        proofs = []
        for iid in ids:
            ranges = await self.repo.list(
                "qa.reviews",
                filters={"subject": "MARKET_CAPTURE_COVERAGE", "instrument_id": iid},
                limit=100000,
            )
            for minute in minutes:
                if minute >= end:
                    continue
                all_rows = await self._range(
                    "mkt.trades",
                    "ts",
                    start=minute,
                    end=minute + timedelta(minutes=1),
                    filters={"instrument_id": iid},
                )
                rows = [r for r in all_rows if parse_time(r["ts"]) < end]
                for row in rows:
                    trades[iid + ":" + row["trade_id"]] = row
                for proof in ranges:
                    lo, hi = parse_time(proof["start"]), parse_time(proof["end"])
                    if not (lo < minute + timedelta(minutes=1) and hi > minute):
                        continue
                    expected = proof.get("minute_trade_counts", {}).get(iso(minute), 0)
                    retained = sum(1 for r in all_rows if lo <= parse_time(r["ts"]) < hi)
                    known = "minute_trade_counts" in proof
                    complete = bool(proof["complete"] and known and retained == expected)
                    proofs.append(
                        {
                            "instrument_id": iid,
                            "start": iso(max(lo, minute)),
                            "end": iso(min(hi, minute + timedelta(minutes=1))),
                            "complete": complete,
                            "reason": proof.get("reason")
                            if complete
                            else "RAW_BOUNDARY_ENUMERATION_UNVERIFIED",
                            "observed_through": iso(min(hi, minute + timedelta(minutes=1))),
                            "observed_at": proof["at"],
                            "source": "EXHAUSTED_REST_RANGE",
                            "expected_trade_count": expected if known else None,
                            "retained_trade_count": retained,
                        }
                    )
                bar = await self.repo.get("mkt.flow_bars_1m", iid + ":" + iso(minute))
                if (
                    bar
                    and bar.get("complete")
                    and minute + timedelta(minutes=1) <= end
                    and len(all_rows) == int(bar.get("n_buy", 0)) + int(bar.get("n_sell", 0))
                ):
                    proofs.append(
                        {
                            "instrument_id": iid,
                            "start": iso(minute),
                            "end": iso(minute + timedelta(minutes=1)),
                            "complete": True,
                            "reason": None,
                            "observed_through": iso(minute + timedelta(minutes=1)),
                            "observed_at": bar.get("received_ts"),
                            "source": "COMPLETE_FLOW_MINUTE",
                        }
                    )
                since = self._connected_since.get(iid)
                latest = self.flow.last.get(iid)
                watermark = parse_time(latest["ts"]) if latest else None
                state = self.flow.bars.get((iid, iso(minute)))
                recorded = state or bar
                enumerated = recorded is not None and len(all_rows) == int(recorded.get("n_buy", 0)) + int(
                    recorded.get("n_sell", 0)
                )
                if (
                    since
                    and watermark
                    and watermark > minute
                    and enumerated
                    and not recorded.get("gap_reason")
                ):
                    lo = max(since, minute)
                    hi = min(watermark, minute + timedelta(minutes=1))
                    if hi > lo:
                        proofs.append(
                            {
                                "instrument_id": iid,
                                "start": iso(lo),
                                "end": iso(hi),
                                "complete": True,
                                "reason": None,
                                "observed_through": iso(hi),
                                "observed_at": iso(),
                                "source": "CONTINUOUS_STREAM_TRADE_WATERMARK",
                            }
                        )
                if not any(
                    p["instrument_id"] == iid
                    and p["complete"]
                    and parse_time(p["start"]) <= minute
                    and parse_time(p["end"]) >= min(minute + timedelta(minutes=1), end)
                    for p in proofs
                ):
                    proofs.append(
                        {
                            "instrument_id": iid,
                            "start": iso(minute),
                            "end": iso(min(minute + timedelta(minutes=1), end)),
                            "complete": False,
                            "reason": "BOUNDARY_CONTINUITY_NOT_PROVEN",
                            "observed_through": None,
                            "observed_at": iso(),
                            "source": "GAP",
                        }
                    )
        return list(trades.values()), proofs

    async def _capture_metadata(self, token_id, cutoff):
        from .supply import supply_cadences

        hot = (
            []
            if self.capture_only
            else await self.repo.list("mkt.hot_tokens", filters={"token_id": token_id}, limit=10000)
        )
        tier = "HOT" if hot else "BASE"
        base = float(self.thresholds.get("base_kline_interval_s", 60))
        oi = float(
            self.thresholds.get(
                "hot_oi_funding_interval_s" if hot else "base_oi_funding_interval_s", 60 if hot else 300
            )
        )
        watermarks = {}
        coverage_start = {}
        states = {}
        push = {}
        end = parse_time(cutoff)
        for iid, instrument in self.instruments.items():
            if instrument["token_id"] != token_id or norm.venue_key(instrument).endswith(".spot"):
                continue
            key = norm.venue_key(instrument)
            if key == "hyperliquid.perp":
                states[iid] = "NOT_SUPPORTED"
                continue
            states[iid] = "CONNECTED" if iid in self._connected_since else "DISCONNECTED"
            observed = self._stream_watermarks.get(iid)
            since = self._connected_since.get(iid)
            if (
                observed
                and states[iid] == "CONNECTED"
                and since <= end
                and parse_time(observed["ts"]) >= since
            ):
                watermarks[iid] = iso(min(parse_time(observed["ts"]), end))
                coverage_start[iid] = iso(since)
            push[iid] = 1 if key == "binance.usdm" else 0.5 if key == "bybit.linear" else None
        return {
            "tier": tier,
            "tier_basis": "PERSISTED_MKT_HOT_TOKENS",
            "hot_events": [r["event_id"] for r in hot],
            "collections": {
                "flow_bars": {"cadence_s": 60, "basis": "ONE_MINUTE_CLOSED_FLOW_BAR"},
                "klines": {"cadence_s": base},
                "books": {"cadence_s": float(self.thresholds.get("book_interval_s", 10))},
                "oi": {"cadence_s": oi},
                "funding": {"cadence_s": oi},
                "tickers": {"cadence_s": base},
                "supply": {"by_kind": supply_cadences(bool(hot))},
                "liquidations": {
                    "cadence_s": STREAM_PING_INTERVAL_S,
                    "basis": "CONFIGURED_SHARED_SOCKET_HEALTH_INTERVAL",
                    "feed_push_cadence_s_by_instrument": push,
                    "observed_through": watermarks,
                    "coverage_start": coverage_start,
                    "coverage_status_by_instrument": states,
                    "watermark_basis": "OBSERVED_SHARED_PUBLIC_SOCKET_EXCHANGE_TIME_BOUNDED_BY_CUTOFF",
                },
            },
        }

    async def snapshot(self, token_id, cutoff, anchor=None):
        cutoff_dt = parse_time(cutoff)
        result = {
            "token_id": token_id,
            "cutoff": iso(cutoff),
            "anchor": anchor
            if isinstance(anchor, dict)
            else {"ts": iso(anchor), "type": "T0"}
            if anchor is not None
            else None,
            "config_version": self.settings.config_version,
            "code_version": self.settings.code_version,
        }
        instruments = [r for r in self.instruments.values() if r["token_id"] == token_id]
        ids = {r["instrument_id"] for r in instruments}
        for name, (collection, field) in COLLECTIONS.items():
            values = []
            for iid in ids if name != "supply" else [None]:
                filters = {"instrument_id": iid} if iid is not None else {"token_id": token_id}
                range_start = cutoff_dt - timedelta(days=90)
                rows = await self._range(
                    collection,
                    field,
                    start=range_start,
                    end=cutoff_dt + timedelta(microseconds=1),
                    filters=filters,
                )
                if name == "funding":
                    settled = await self._range(
                        collection,
                        "funding_time",
                        start=range_start,
                        end=cutoff_dt + timedelta(microseconds=1),
                        filters=filters,
                    )
                    rows.extend(settled)
                for row in rows:
                    stamp = row.get(field) or row.get("funding_time") or row.get("settlement_time")
                    if stamp is None or parse_time(stamp) > cutoff_dt:
                        continue
                    if (
                        name in ("flow_bars", "klines")
                        and parse_time(stamp) + timedelta(minutes=1) > cutoff_dt
                    ):
                        continue
                    if name == "books" and row.get("independent_rest"):
                        independent = row["independent_rest"]
                        if any(parse_time(independent[f]) > cutoff_dt for f in ("ts",) if independent.get(f)):
                            row = dict(
                                row,
                                second_path_confirmed=False,
                                independent_rest=None,
                                second_path_reason="CONFIRMATION_AFTER_CUTOFF",
                            )
                    values.append(row)
            result[name] = sorted(
                values, key=lambda r: r.get(field) or r.get("funding_time") or r.get("settlement_time")
            )
        peers = {
            t["token_id"]
            for t in self.tokens.values()
            if t.get("symbol") in ("BTC", "ETH") or t.get("sector") not in (None, "BENCHMARK")
        } - {token_id}
        for inst in self.instruments.values():
            if inst["token_id"] not in peers or not norm.venue_key(inst).endswith(".spot"):
                continue
            rows = await self._range(
                "mkt.klines_1m",
                "open_time",
                start=cutoff_dt - timedelta(days=90),
                end=cutoff_dt,
                filters={"instrument_id": inst["instrument_id"]},
            )
            result["klines"].extend(
                r for r in rows if parse_time(r["open_time"]) + timedelta(minutes=1) <= cutoff_dt
            )
            instruments.append(inst)
        proofs = await self.repo.list("qa.reviews", filters={"subject": "METRIC_SEMANTICS"}, limit=100000)
        scoped = {
            (r["instrument_id"], r["metric_key"]): r
            for r in sorted(proofs, key=lambda proof: proof.get("at", ""))
            if r.get("instrument_id") and r.get("metric_key")
        }
        semantics = []
        for instrument in instruments:
            prefix = norm.venue_key(instrument)
            for definition in self.semantics:
                if not definition["metric_key"].startswith(prefix + "."):
                    continue
                proof = scoped.get((instrument["instrument_id"], definition["metric_key"]))
                row = dict(
                    definition,
                    instrument_id=instrument["instrument_id"],
                    scope="PER_INSTRUMENT",
                    semantic_confidence="DOCUMENTED_UNTESTED",
                    verification_status="NO_CURRENT_INSTRUMENT_PROOF",
                )
                if proof:
                    row.update(proof["semantics"])
                semantics.append(row)
        result["semantics"] = semantics
        result["semantic_proofs"] = [
            p for (iid, _), p in scoped.items() if iid in {i["instrument_id"] for i in instruments}
        ]
        result["instruments"] = instruments
        result["trades"] = []
        result["anchor_trades"] = {}
        result["cutoff_trades"] = {}
        for instrument in instruments:
            if not norm.venue_key(instrument).endswith(".spot"):
                continue
            iid = instrument["instrument_id"]
            if anchor_ts_value := (
                (anchor.get("ts") if isinstance(anchor, dict) else anchor) if anchor is not None else None
            ):
                observed = await self.repo.latest_before(
                    "mkt.trades", "ts", anchor_ts_value, filters={"instrument_id": iid}
                )
                if observed:
                    result["anchor_trades"][iid] = observed
                    result["trades"].append(observed)
            latest = await self.repo.latest_before("mkt.trades", "ts", cutoff, filters={"instrument_id": iid})
            if latest:
                result["cutoff_trades"][iid] = latest
                if not any(
                    t["instrument_id"] == iid and t["trade_id"] == latest["trade_id"]
                    for t in result["trades"]
                ):
                    result["trades"].append(latest)
        result["tokens"] = list(self.tokens.values())
        gaps = await self.repo.list("mkt.capture_gaps", filters={"token_id": token_id}, limit=100000)
        result["gaps"] = [g for g in gaps if parse_time(g["start"]) <= cutoff_dt]
        runs = await self.repo.list("mkt.backfill_runs", filters={"token_id": token_id}, limit=10000)
        anchor_ts = (
            anchor.get("ts") if isinstance(anchor, dict) else iso(anchor) if anchor is not None else None
        )
        runs = [r for r in runs if anchor_ts is None or parse_time(r["anchor"]) == parse_time(anchor_ts)]
        result["boundary_trades"], result["boundary_coverage"] = await self._boundary_capture(
            ids, cutoff, anchor
        )
        result["backfill_runs"] = runs
        latest = max(runs, key=lambda r: r["started_at"]) if runs else None
        result["capture_status"] = {
            "status": latest["status"] if latest else "NOT_REQUESTED",
            "reason": latest.get("reason") if latest else "NO_DURABLE_CAPTURE_REQUEST",
            "backfill_id": latest.get("backfill_id") if latest else None,
            "observed_at": iso(),
            "bounded_wait_s": 0,
            "incomplete_windows_excluded": True,
        }
        if latest and latest["backfill_id"] in self._capture_waits:
            waited = self._capture_waits[latest["backfill_id"]]
            result["capture_status"]["bounded_wait_s"] = waited["bounded_wait_s"]
            if latest["status"] in ("PENDING", "RUNNING"):
                result["capture_status"].update(waited)
        result["funding_confirmations"] = []
        for rest in result["funding"]:
            if rest.get("capture_source") != "PUBLIC_REST" or rest.get("kind") != "CURRENT_ESTIMATE":
                continue
            matching = [
                ws
                for ws in result["funding"]
                if ws.get("instrument_id") == rest.get("instrument_id")
                and ws.get("capture_source") == "WEBSOCKET"
                and ws.get("kind") == rest.get("kind")
                and abs((parse_time(ws["as_of"]) - parse_time(rest["as_of"])).total_seconds()) <= 60
            ]
            if matching:
                ws = min(
                    matching,
                    key=lambda row: abs(
                        (parse_time(row["as_of"]) - parse_time(rest["as_of"])).total_seconds()
                    ),
                )
                result["funding_confirmations"].append(
                    {
                        **rest,
                        "independent_path_verified": True,
                        "primary_source_path": ws.get("source_path"),
                    }
                )
        result["capture_metadata"] = await self._capture_metadata(token_id, cutoff)
        result["capture_tier"] = result["capture_metadata"]["tier"]
        result["assembled_at"] = iso()
        return result

    async def coverage(self):
        hot = {r["token_id"] for r in await self.repo.list("mkt.hot_tokens", limit=10000)}
        result = []
        for iid, inst in self.instruments.items():
            state = await self.repo.get("mkt.capture_state", iid)
            gaps = await self.repo.list("mkt.capture_gaps", filters={"instrument_id": iid}, limit=10000)
            result.append(
                {
                    "instrument_id": iid,
                    "token_id": inst["token_id"],
                    "venue": inst["venue"],
                    "market_type": inst["market_type"],
                    "tier": "HOT" if inst["token_id"] in hot else "BASE",
                    "stream_connected": iid in self._connected_since,
                    "last_trade_ts": state.get("last_ts") if state else None,
                    "metadata_status": inst.get("metadata_status", "UNVERIFIED"),
                    "gap_count": len(gaps),
                    "latest_gap": gaps[-1] if gaps else None,
                }
            )
        from pathlib import Path

        paths = [
            ("binance", "SPOT"),
            ("binance", "PERP_LINEAR"),
            ("okx", "SPOT"),
            ("okx", "PERP_LINEAR"),
            ("bybit", "SPOT"),
            ("bybit", "PERP_LINEAR"),
            ("coinbase", "SPOT"),
            ("kraken", "SPOT"),
            ("hyperliquid", "PERP_LINEAR"),
        ]
        matrix = []
        for token_id in self.tokens:
            for venue, market_type in paths:
                rows = [
                    row
                    for row in result
                    if row["token_id"] == token_id
                    and row["venue"] == venue
                    and row["market_type"] == market_type
                ]
                matrix.append(
                    {
                        "token_id": token_id,
                        "venue": venue,
                        "market_type": market_type,
                        "status": "LIVE_VERIFIED"
                        if rows and all(row["metadata_status"] == "LIVE_VERIFIED" for row in rows)
                        else "CONFIGURED_UNTESTED"
                        if rows
                        else "NO_REGISTRY_INSTRUMENT",
                        "instruments": rows,
                    }
                )
        path = Path(self.settings.data_dir) / "market" / "coverage" / "latest.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps({"assembled_at": iso(), "matrix": matrix}, indent=2))
        temp.replace(path)
        return result

    async def _instrument_loop(self, instrument):
        iid = instrument["instrument_id"]
        deriv_due = 0
        previous_hot = False
        self_test_due = 0
        while not self._stop.is_set():
            started = time.monotonic()
            hot = not self.capture_only and bool(
                await self.repo.list(
                    "mkt.hot_tokens", filters={"token_id": instrument["token_id"]}, limit=100
                )
            )
            if hot and not previous_hot:
                deriv_due = min(deriv_due, started)
            previous_hot = hot
            do_deriv = started >= deriv_due
            if started >= self_test_due:
                await self.self_test(iid)
                self_test_due = time.monotonic() + 3600
            try:
                await self.poll_once(iid, derivatives=do_deriv)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                await self._gap(instrument, "POLL", exc)
            if do_deriv:
                deriv_due = time.monotonic() + float(
                    self.thresholds.get(
                        "hot_oi_funding_interval_s" if hot else "base_oi_funding_interval_s",
                        60 if hot else 300,
                    )
                )
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(),
                    max(
                        1,
                        float(self.thresholds.get("base_kline_interval_s", 60))
                        - (time.monotonic() - started),
                    ),
                )

    async def bootstrap_history(self, *, days=90):
        """Durable background bootstrap, newest days first; failures remain visible gaps."""
        from .archive import fetch_archive

        instruments = [i for i in self.instruments.values() if norm.venue_key(i).startswith("binance")]
        for age in range(1, days + 1):
            date = (utcnow() - timedelta(days=age)).date().isoformat()
            # OI is inexpensive and needed for uncontaminated normal distributions.
            for inst in instruments:
                if self._stop is not None and self._stop.is_set():
                    return
                kinds = ["metrics", "klines"] if norm.venue_key(inst) == "binance.usdm" else ["klines"]
                for kind in kinds:
                    try:
                        await fetch_archive(
                            self.repo,
                            self.settings,
                            inst["venue_symbol"],
                            date,
                            market_type=inst["market_type"],
                            kind=kind,
                            http=self.http,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                        await self._gap(
                            inst,
                            "HISTORICAL_" + kind.upper(),
                            exc,
                            start=date + "T00:00:00Z",
                            end=iso(parse_time(date) + timedelta(days=1)),
                        )

    async def _supply_loop(self):
        from .supply import SupplyCapture

        capture = SupplyCapture(self)
        while not self._stop.is_set():
            hot = (
                set()
                if self.capture_only
                else {r["token_id"] for r in await self.repo.list("mkt.hot_tokens", limit=10000)}
            )
            try:
                await capture.poll(hot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                log.warning("supply capture failed: %s", type(exc).__name__)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), 60)

    async def _archive_maintenance(self):
        from .archive import fetch_archive

        while not self._stop.is_set():
            for sealed in await asyncio.to_thread(self.archive.flush_expired):
                await self._put("mkt.raw_archives", sealed["archive_id"], sealed)
            latest_retries = {}
            for record in await self.repo.list(
                "qa.reviews", filters={"subject": "ARCHIVE_RETRY"}, limit=100000
            ):
                group = sha256(record.get("args", {}))
                if group not in latest_retries or record["at"] > latest_retries[group]["at"]:
                    latest_retries[group] = record
            for retry in latest_retries.values():
                if retry.get("label") != "PENDING":
                    continue
                if parse_time(retry["retry_at"]) > utcnow():
                    continue
                try:
                    await fetch_archive(
                        self.repo, self.settings, http=self.http, _scheduled_retry=True, **retry["args"]
                    )
                    retry.update(label="COMPLETE", completed_at=iso(), at=iso())
                except Exception as exc:  # noqa: BLE001 - durable retry survives provider failures
                    retry.update(
                        label="FAILED",
                        at=iso(),
                        reason=type(exc).__name__,
                        retry_at=iso(
                            utcnow() + timedelta(seconds=self.thresholds.get("archive_retry_s", 3600))
                        ),
                    )
                await self._put("qa.reviews", retry["review_id"], retry)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), 60)

    async def run(self, stop):
        self._stop = stop
        if self.settings.replay:
            return
        await self._sync_streams()
        for hot in [] if self.capture_only else await self.repo.list("mkt.hot_tokens", limit=10000):
            event = await self.repo.get("evt.events", hot["event_id"]) or {}
            await self._schedule_event_backfill(
                hot["event_id"], hot["token_id"], event_anchor(event)["ts"] if event else hot["since"]
            )
        workers = [asyncio.create_task(self._instrument_loop(inst)) for inst in self.instruments.values()]
        workers.append(asyncio.create_task(self._supply_loop()))
        workers.append(asyncio.create_task(self._archive_maintenance()))
        workers.append(
            asyncio.create_task(
                self.bootstrap_history(days=int(self.thresholds.get("historical_lookback_days", 90)))
            )
        )
        try:
            while not stop.is_set():
                await self._flush_flow()
                await self._sync_streams()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), 1)
        finally:
            for task in [*workers, *self.streams.values(), *self._background]:
                task.cancel()
            await asyncio.gather(*workers, *self.streams.values(), *self._background, return_exceptions=True)
            self.streams.clear()
            await self.close()

"""Public REST endpoint contracts for the nine supported venue/market paths."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from tank.util import iso, parse_time

from .normalize import SemanticError, venue_key


class VenueAdapter:
    def __init__(self, instrument, request):
        self.instrument = instrument
        self.request = request
        self.key = venue_key(instrument)
        self.symbol = instrument["venue_symbol"]

    async def get(self, base, path, **params):
        if self.key.startswith("kraken") and "pair" in params:
            params["pair"] = params["pair"].replace("/", "")
        return await self.request("GET", base + path, params=params)

    async def info(self, payload):
        return await self.request("POST", "https://api.hyperliquid.xyz/info", json=payload)

    def result(self, body):
        if self.key.startswith("okx"):
            if str(body.get("code")) != "0":
                raise SemanticError("OKX response error " + str(body.get("msg")))
            return body["data"]
        if self.key.startswith("bybit"):
            if body.get("retCode") != 0:
                raise SemanticError("Bybit response error " + str(body.get("retMsg")))
            return body["result"]["list"]
        if self.key.startswith("kraken"):
            if body.get("error"):
                raise SemanticError("Kraken response error " + str(body["error"]))
            return body["result"]
        return body

    async def discover(self):
        """Instrument units come from the semantics registry (03 §2.10); only the two
        listed metadata endpoints are read: OKX ctVal and Hyperliquid meta (02 §4.4, §4.7)."""
        key = self.key
        s = self.symbol
        if key == "okx.swap":
            r = self.result(
                await self.get("https://www.okx.com", "/api/v5/public/instruments", instType="SWAP", instId=s)
            )[0]
            if r["ctValCcy"] != s.split("-")[0]:
                raise SemanticError("OKX contract value currency not base")
            return {
                "tick_size": r["tickSz"],
                "lot_size": r["lotSz"],
                "active": r["state"] == "live",
                "metadata_status": "LIVE_VERIFIED",
                "contract_multiplier": str(Decimal(r["ctVal"]) * Decimal(r.get("ctMult") or "1")),
            }
        if key == "hyperliquid.perp":
            meta, _ = await self.info({"type": "metaAndAssetCtxs"})
            r = next(x for x in meta["universe"] if x["name"] == s)
            return {
                "lot_size": str(10 ** -int(r["szDecimals"])),
                "contract_multiplier": "1",
                "funding_interval_h": "1",
                "active": not r.get("isDelisted", False),
                "metadata_status": "LIVE_VERIFIED",
                "tick_size": None,
                "tick_size_reason": "SIGNIFICANT_FIGURES_AND_SIZE_DEPENDENT",
            }
        # Base-unit quantities by the semantics registry; no metadata endpoint is listed.
        return {"contract_multiplier": "1", "metadata_status": "SEMANTICS_REGISTRY"}

    async def klines(self, *, start=None, end=None):
        k = self.key
        s = self.symbol
        params = {}
        if k.startswith("binance"):
            base = "https://api.binance.com" if k.endswith("spot") else "https://fapi.binance.com"
            path = "/api/v3/klines" if k.endswith("spot") else "/fapi/v1/klines"
            if start:
                params["startTime"] = int(parse_time(start).timestamp() * 1000)
            if end:
                params["endTime"] = int(parse_time(end).timestamp() * 1000)
            return await self.get(base, path, symbol=s, interval="1m", limit=1000 if start else 3, **params)
        if k.startswith("okx"):
            if end:
                params["after"] = int(parse_time(end).timestamp() * 1000)
            if start:
                params["before"] = int(parse_time(start).timestamp() * 1000)
            return self.result(
                await self.get(
                    "https://www.okx.com",
                    "/api/v5/market/history-candles" if start else "/api/v5/market/candles",
                    instId=s,
                    bar="1m",
                    limit=300 if start else 3,
                    **params,
                )
            )
        if k.startswith("bybit"):
            if start:
                params["start"] = int(parse_time(start).timestamp() * 1000)
            if end:
                params["end"] = int(parse_time(end).timestamp() * 1000)
            return self.result(
                await self.get(
                    "https://api.bybit.com",
                    "/v5/market/kline",
                    category=k.split(".")[1],
                    symbol=s,
                    interval="1",
                    limit=1000 if start else 3,
                    **params,
                )
            )
        if k.startswith("coinbase"):
            if not start:
                start = parse_time(iso()) - timedelta(minutes=3)
            if start:
                params["start"] = iso(start)
            if end:
                params["end"] = iso(end)
            return await self.get(
                "https://api.exchange.coinbase.com", "/products/" + s + "/candles", granularity=60, **params
            )
        if k.startswith("kraken"):
            params["since"] = int(parse_time(start or (parse_time(iso()) - timedelta(minutes=3))).timestamp())
            rows = self.result(
                await self.get("https://api.kraken.com", "/0/public/OHLC", pair=s, interval=1, **params)
            )
            return next(v for key, v in rows.items() if key != "last")
        return await self.info(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": s,
                    "interval": "1m",
                    "startTime": int(
                        parse_time(start or (parse_time(iso()) - timedelta(minutes=3))).timestamp() * 1000
                    ),
                    "endTime": int(parse_time(end or iso()).timestamp() * 1000),
                },
            }
        )

    async def ticker(self):
        k = self.key
        s = self.symbol
        if k.startswith("binance"):
            return await self.get(
                "https://api.binance.com" if k.endswith("spot") else "https://fapi.binance.com",
                "/api/v3/ticker/24hr" if k.endswith("spot") else "/fapi/v1/ticker/24hr",
                symbol=s,
            )
        if k.startswith("okx"):
            return self.result(await self.get("https://www.okx.com", "/api/v5/market/ticker", instId=s))[0]
        if k.startswith("bybit"):
            return self.result(
                await self.get(
                    "https://api.bybit.com", "/v5/market/tickers", category=k.split(".")[1], symbol=s
                )
            )[0]
        if k.startswith("coinbase"):
            return await self.get("https://api.exchange.coinbase.com", "/products/" + s + "/stats")
        if k.startswith("kraken"):
            return next(
                iter(
                    self.result(await self.get("https://api.kraken.com", "/0/public/Ticker", pair=s)).values()
                )
            )
        meta, contexts = await self.info({"type": "metaAndAssetCtxs"})
        return contexts[next(i for i, r in enumerate(meta["universe"]) if r["name"] == s)]

    async def derivatives(self):
        k = self.key
        s = self.symbol
        if k == "binance.usdm":
            oi = await self.get("https://fapi.binance.com", "/fapi/v1/openInterest", symbol=s)
            history = await self.get(
                "https://fapi.binance.com", "/futures/data/openInterestHist", symbol=s, period="5m", limit=2
            )
            current = await self.get("https://fapi.binance.com", "/fapi/v1/premiumIndex", symbol=s)
            settled = await self.get("https://fapi.binance.com", "/fapi/v1/fundingRate", symbol=s, limit=1000)
            info = await self.get("https://fapi.binance.com", "/fapi/v1/fundingInfo")
            interval = next((r["fundingIntervalHours"] for r in info if r["symbol"] == s), 8)
            return [oi, *history], current, settled, str(interval), current["markPrice"]
        if k == "okx.swap":
            oi = self.result(
                await self.get(
                    "https://www.okx.com", "/api/v5/public/open-interest", instType="SWAP", instId=s
                )
            )
            current = self.result(
                await self.get("https://www.okx.com", "/api/v5/public/funding-rate", instId=s)
            )[0]
            settled = self.result(
                await self.get(
                    "https://www.okx.com", "/api/v5/public/funding-rate-history", instId=s, limit=100
                )
            )
            prev = current.get("prevFundingTime")
            following = current.get("fundingTime")
            if not prev or not following:
                raise SemanticError("OKX funding interval timestamps absent")
            interval = str(
                (__import__("decimal").Decimal(following) - __import__("decimal").Decimal(prev)) / 3600000
            )
            # Mark and index come from the listed WebSocket channels only (02 §4.4).
            mark = None
            return oi, current, settled, interval, mark
        if k == "bybit.linear":
            oi = self.result(
                await self.get(
                    "https://api.bybit.com",
                    "/v5/market/open-interest",
                    category="linear",
                    symbol=s,
                    intervalTime="5min",
                    limit=200,
                )
            )
            current = await self.ticker()
            settled = self.result(
                await self.get(
                    "https://api.bybit.com",
                    "/v5/market/funding/history",
                    category="linear",
                    symbol=s,
                    limit=200,
                )
            )
            interval = current.get("fundingIntervalHour") or self.instrument.get("funding_interval_h")
            if interval is None:
                raise SemanticError("Bybit funding interval absent")
            return oi, current, settled, str(interval), current["markPrice"]
        if k == "hyperliquid.perp":
            current = await self.ticker()
            settled = await self.info(
                {
                    "type": "fundingHistory",
                    "coin": s,
                    "startTime": (int(parse_time(iso()).timestamp()) // 3600 - 7 * 24) * 3600000,
                }
            )
            return [current], current, settled, "1", current["markPx"]
        return [], None, [], None, None

    async def trades(self, *, start=None, end=None, cursor=None):
        k = self.key
        s = self.symbol
        params = {}
        if k.startswith("binance"):
            if start:
                params["startTime"] = int(parse_time(start).timestamp() * 1000)
            if end:
                params["endTime"] = int(parse_time(end).timestamp() * 1000)
            if cursor is not None:
                params = {"fromId": cursor}
            return await self.get(
                "https://api.binance.com" if k.endswith("spot") else "https://fapi.binance.com",
                "/api/v3/aggTrades" if k.endswith("spot") else "/fapi/v1/aggTrades",
                symbol=s,
                limit=1000,
                **params,
            ), None
        if k.startswith("okx"):
            if cursor:
                params["after"] = cursor
            rows = self.result(
                await self.get(
                    "https://www.okx.com", "/api/v5/market/history-trades", instId=s, limit=100, **params
                )
            )
            return rows, rows[-1]["tradeId"] if rows else None
        if k.startswith("bybit"):
            return self.result(
                await self.get(
                    "https://api.bybit.com",
                    "/v5/market/recent-trade",
                    category=k.split(".")[1],
                    symbol=s,
                    limit=1000,
                )
            ), None
        if k.startswith("coinbase"):
            if cursor:
                params["after"] = cursor
            rows = await self.get(
                "https://api.exchange.coinbase.com", "/products/" + s + "/trades", limit=1000, **params
            )
            return rows, rows[-1]["trade_id"] if rows else None
        if k.startswith("kraken"):
            if cursor:
                params["since"] = cursor
            elif start:
                params["since"] = int(parse_time(start).timestamp() * 1_000_000_000)
            rows = self.result(await self.get("https://api.kraken.com", "/0/public/Trades", pair=s, **params))
            return next(v for key, v in rows.items() if key != "last"), rows["last"]
        # 02 §4.7 lists no REST trade history: those minutes stay incomplete (03 §5.1).
        raise SemanticError("NO_LISTED_TRADE_BACKFILL")

"""Priority admission: baseline capture outranks repair and optional REST confirmation."""

import asyncio
import heapq
from contextlib import asynccontextmanager
from itertools import count


class PriorityGate:
    """At most `capacity` holders at once; waiters are admitted lowest priority number first."""

    def __init__(self, capacity=1):
        self.capacity = capacity
        self.active = 0
        self.waiters = []
        self.sequence = count()

    def release(self):
        while self.waiters:
            _, _, future = heapq.heappop(self.waiters)
            if not future.done():
                future.set_result(True)  # the slot passes to this waiter
                return
        self.active -= 1

    @asynccontextmanager
    async def enter(self, priority=0):
        if self.active >= self.capacity:
            future = asyncio.get_running_loop().create_future()
            heapq.heappush(self.waiters, (priority, next(self.sequence), future))
            try:
                await future
            except BaseException:
                if future.done() and not future.cancelled():
                    self.release()
                else:
                    future.cancel()
                raise
        else:
            self.active += 1
        try:
            yield
        finally:
            self.release()


def binance_request_weight(host, path, params):
    """Public endpoint weights from official Binance contracts, verified17Oct2026.

    Sources: binance/binance-spot-api-docs/rest-api.md and Binance USD-M market-data.
    Unknown endpoints fail closed instead of silently charging one unit.
    """
    limit = int(params.get("limit", 100))
    if host == "api.binance.com":
        # exchangeInfo is read by the Binance listing source (tank.sources), not by market.
        fixed = {"/api/v3/exchangeInfo": 20, "/api/v3/aggTrades": 4, "/api/v3/klines": 2}
        if path in fixed:
            return fixed[path]
        if path == "/api/v3/depth":
            return 5 if limit <= 100 else 25 if limit <= 500 else 50 if limit <= 1000 else 250
        if path == "/api/v3/ticker/24hr":
            return 2 if params.get("symbol") else 80
    elif host == "fapi.binance.com":
        fixed = {
            "/fapi/v1/exchangeInfo": 1,
            "/fapi/v1/aggTrades": 20,
            "/fapi/v1/openInterest": 1,
            "/futures/data/openInterestHist": 0,
            "/fapi/v1/fundingRate": 0,
            "/fapi/v1/fundingInfo": 0,
        }
        if path in fixed:
            return fixed[path]
        if path == "/fapi/v1/klines":
            limit = int(params.get("limit", 500))
            return 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
        if path == "/fapi/v1/depth":
            return 2 if limit <= 50 else 5 if limit <= 100 else 10 if limit <= 500 else 20
        if path == "/fapi/v1/ticker/24hr":
            return 1 if params.get("symbol") else 40
        if path == "/fapi/v1/premiumIndex":
            return 1 if params.get("symbol") else 10
    raise ValueError("Unconfigured Binance request weight: " + path)


def public_request_constraints(host, path, params, payload):
    """Verified public provider budgets; keys include the documented scope."""
    if host == "api.bybit.com":
        return [("bybit", 300, 5, 1)]
    if host in ("www.okx.com", "openapi.okx.com"):
        caps = {
            "/api/v5/market/books": (20, "IP"),
            "/api/v5/market/trades": (50, "IP"),
            "/api/v5/market/history-trades": (10, "IP"),
            "/api/v5/market/candles": (20, "IP"),
            "/api/v5/market/history-candles": (10, "IP"),
            "/api/v5/public/open-interest": (10, "instrument"),
            "/api/v5/public/funding-rate": (5, "instrument"),
            "/api/v5/public/funding-rate-history": (5, "instrument"),
            "/api/v5/public/instruments": (10, "instrument_type"),
            "/api/v5/market/ticker": (10, "IP"),
            "/api/v5/market/tickers": (10, "IP"),
        }
        if path not in caps:
            raise ValueError("Unconfigured OKX request limit: " + path)
        capacity, scope = caps[path]
        suffix = (
            params.get("instId", "ALL")
            if scope == "instrument"
            else params.get("instType", "ALL")
            if scope == "instrument_type"
            else "IP"
        )
        return [("okx:" + path + ":" + suffix, capacity, 2, 1)]
    if host == "api.hyperliquid.xyz":
        kind = payload.get("type")
        if kind in ("meta", "metaAndAssetCtxs"):
            weight = 20
        elif kind == "fundingHistory":
            weight = 45  # 20 +ceil(documented500 time-range response rows/20).
        elif kind == "candleSnapshot":
            request = payload.get("req", {})
            if request.get("interval") == "1m" and "endTime" in request and "startTime" in request:
                rows = min(5000, max(0, (int(request["endTime"]) - int(request["startTime"])) // 60000 + 2))
            else:
                rows = 5000
            weight = 20 + (rows + 59) // 60
        else:
            raise ValueError("Unconfigured Hyperliquid request weight: " + str(kind))
        return [("hyperliquid", 600, 60, weight)]
    return []

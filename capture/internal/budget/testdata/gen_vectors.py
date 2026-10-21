"""Request budget vectors from the Python reference (tank.market.rate_gate and the
admit_budget plan of MarketService._request_once).

    uv run python capture/internal/budget/testdata/gen_vectors.py > capture/internal/budget/testdata/vectors.json
"""

import json

from tank.market.rate_gate import binance_request_weight, public_request_constraints


def plan(host, path, params, payload):
    """MarketService._request_once.admit_budget, as the buckets it acquires in order."""
    out = []
    provider = (
        "kraken" if host == "api.kraken.com" else "coinbase" if host == "api.exchange.coinbase.com" else None
    )
    if provider:
        out.append((provider, 1 if provider == "kraken" else 5, 1, 1))
    if host in ("api.binance.com", "fapi.binance.com"):
        weight = binance_request_weight(host, path, params)
        if weight:
            out.append(
                (
                    "binance.spot" if host == "api.binance.com" else "binance.usdm",
                    3000 if host == "api.binance.com" else 1200,
                    60,
                    weight,
                )
            )
        if host == "fapi.binance.com" and path in ("/fapi/v1/fundingRate", "/fapi/v1/fundingInfo"):
            out.append(("binance.usdm.funding", 250, 300, 1))
        if host == "fapi.binance.com" and path == "/futures/data/openInterestHist":
            out.append(("binance.usdm.statistics", 500, 300, 1))
    for key, capacity, window, charge in public_request_constraints(host, path, params, payload):
        out.append((key, capacity, window, charge))
    return out


def bucket_name(key, window, capacity):
    return f"{key}|{float(window)!r}|{float(capacity)!r}"


WEIGHTS = []
for host, path, params in [
    ("api.binance.com", "/api/v3/exchangeInfo", {}),
    ("api.binance.com", "/api/v3/aggTrades", {"limit": "1000"}),
    ("api.binance.com", "/api/v3/aggTrades", {"limit": "1"}),
    ("api.binance.com", "/api/v3/klines", {"limit": "3"}),
    ("api.binance.com", "/api/v3/depth", {}),
    ("api.binance.com", "/api/v3/depth", {"limit": "5"}),
    ("api.binance.com", "/api/v3/depth", {"limit": "100"}),
    ("api.binance.com", "/api/v3/depth", {"limit": "101"}),
    ("api.binance.com", "/api/v3/depth", {"limit": "500"}),
    ("api.binance.com", "/api/v3/depth", {"limit": "501"}),
    ("api.binance.com", "/api/v3/depth", {"limit": "1000"}),
    ("api.binance.com", "/api/v3/depth", {"limit": "1001"}),
    ("api.binance.com", "/api/v3/depth", {"limit": "5000"}),
    ("api.binance.com", "/api/v3/ticker/24hr", {"symbol": "BTCUSDT"}),
    ("api.binance.com", "/api/v3/ticker/24hr", {}),
    ("api.binance.com", "/api/v3/ticker/24hr", {"symbol": ""}),
    ("api.binance.com", "/api/v3/trades", {}),
    ("fapi.binance.com", "/fapi/v1/exchangeInfo", {}),
    ("fapi.binance.com", "/fapi/v1/aggTrades", {"limit": "1000"}),
    ("fapi.binance.com", "/fapi/v1/openInterest", {"symbol": "BTCUSDT"}),
    ("fapi.binance.com", "/futures/data/openInterestHist", {"limit": "2"}),
    ("fapi.binance.com", "/fapi/v1/fundingRate", {"limit": "1000"}),
    ("fapi.binance.com", "/fapi/v1/fundingInfo", {}),
    ("fapi.binance.com", "/fapi/v1/klines", {}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "3"}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "99"}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "100"}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "499"}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "500"}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "1000"}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "1001"}),
    ("fapi.binance.com", "/fapi/v1/klines", {"limit": "1500"}),
    ("fapi.binance.com", "/fapi/v1/depth", {}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "5"}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "50"}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "51"}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "100"}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "101"}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "500"}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "501"}),
    ("fapi.binance.com", "/fapi/v1/depth", {"limit": "1000"}),
    ("fapi.binance.com", "/fapi/v1/ticker/24hr", {"symbol": "BTCUSDT"}),
    ("fapi.binance.com", "/fapi/v1/ticker/24hr", {}),
    ("fapi.binance.com", "/fapi/v1/premiumIndex", {"symbol": "BTCUSDT"}),
    ("fapi.binance.com", "/fapi/v1/premiumIndex", {}),
    ("fapi.binance.com", "/fapi/v1/trades", {}),
    ("api.kraken.com", "/0/public/Trades", {}),
]:
    try:
        WEIGHTS.append(
            {
                "host": host,
                "path": path,
                "params": params,
                "weight": binance_request_weight(host, path, params),
            }
        )
    except ValueError as exc:
        WEIGHTS.append({"host": host, "path": path, "params": params, "error": str(exc)})

CANDLE = {"coin": "BTC", "interval": "1m", "startTime": 1792578000000, "endTime": 1792578180000}
PLANS = []
for host, path, params, payload in [
    ("api.kraken.com", "/0/public/Trades", {"pair": "XBTUSD"}, {}),
    ("api.kraken.com", "/0/public/OHLC", {"pair": "XBTUSD", "interval": "1"}, {}),
    ("api.exchange.coinbase.com", "/products/BTC-USD/trades", {"limit": "1000"}, {}),
    ("api.exchange.coinbase.com", "/products/USDT-USD/stats", {}, {}),
    ("api.binance.com", "/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1m", "limit": "3"}, {}),
    ("api.binance.com", "/api/v3/aggTrades", {"symbol": "BTCUSDT", "limit": "1000"}, {}),
    ("api.binance.com", "/api/v3/depth", {"symbol": "BTCUSDT", "limit": "1000"}, {}),
    ("api.binance.com", "/api/v3/depth", {"symbol": "BTCUSDT", "limit": "5"}, {}),
    ("api.binance.com", "/api/v3/ticker/24hr", {"symbol": "BTCUSDT"}, {}),
    ("fapi.binance.com", "/fapi/v1/openInterest", {"symbol": "BTCUSDT"}, {}),
    (
        "fapi.binance.com",
        "/futures/data/openInterestHist",
        {"symbol": "BTCUSDT", "period": "5m", "limit": "2"},
        {},
    ),
    ("fapi.binance.com", "/fapi/v1/premiumIndex", {"symbol": "BTCUSDT"}, {}),
    ("fapi.binance.com", "/fapi/v1/fundingRate", {"symbol": "BTCUSDT", "limit": "1000"}, {}),
    ("fapi.binance.com", "/fapi/v1/fundingInfo", {}, {}),
    ("fapi.binance.com", "/fapi/v1/aggTrades", {"symbol": "BTCUSDT", "limit": "1000"}, {}),
    ("fapi.binance.com", "/fapi/v1/depth", {"symbol": "BTCUSDT", "limit": "1000"}, {}),
    ("fapi.binance.com", "/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "1m", "limit": "1000"}, {}),
    ("fapi.binance.com", "/fapi/v1/unknown", {}, {}),
    ("api.bybit.com", "/v5/market/kline", {"category": "linear", "symbol": "BTCUSDT"}, {}),
    (
        "api.bybit.com",
        "/v5/market/recent-trade",
        {"category": "spot", "symbol": "BTCUSDT", "limit": "1000"},
        {},
    ),
    ("www.okx.com", "/api/v5/market/books", {}, {}),
    ("www.okx.com", "/api/v5/market/trades", {"instId": "BTC-USDT", "limit": "1"}, {}),
    ("www.okx.com", "/api/v5/market/history-trades", {"instId": "BTC-USDT", "limit": "100"}, {}),
    ("www.okx.com", "/api/v5/market/candles", {"instId": "BTC-USDT"}, {}),
    ("www.okx.com", "/api/v5/market/history-candles", {"instId": "BTC-USDT"}, {}),
    ("www.okx.com", "/api/v5/public/open-interest", {"instType": "SWAP", "instId": "BTC-USDT-SWAP"}, {}),
    ("www.okx.com", "/api/v5/public/open-interest", {"instType": "SWAP"}, {}),
    ("www.okx.com", "/api/v5/public/funding-rate", {"instId": "BTC-USDT-SWAP"}, {}),
    ("www.okx.com", "/api/v5/public/funding-rate-history", {"instId": "BTC-USDT-SWAP", "limit": "100"}, {}),
    ("www.okx.com", "/api/v5/public/instruments", {"instType": "SWAP", "instId": "BTC-USDT-SWAP"}, {}),
    ("www.okx.com", "/api/v5/public/instruments", {}, {}),
    ("www.okx.com", "/api/v5/market/ticker", {"instId": "BTC-USDT"}, {}),
    ("www.okx.com", "/api/v5/market/tickers", {"instType": "SPOT"}, {}),
    ("openapi.okx.com", "/api/v5/market/ticker", {"instId": "BTC-USDT"}, {}),
    ("www.okx.com", "/api/v5/public/mark-price", {"instId": "BTC-USDT-SWAP"}, {}),
    ("api.hyperliquid.xyz", "/info", {}, {"type": "meta"}),
    ("api.hyperliquid.xyz", "/info", {}, {"type": "metaAndAssetCtxs"}),
    (
        "api.hyperliquid.xyz",
        "/info",
        {},
        {"type": "fundingHistory", "coin": "BTC", "startTime": 1791972000000},
    ),
    ("api.hyperliquid.xyz", "/info", {}, {"type": "candleSnapshot", "req": CANDLE}),
    (
        "api.hyperliquid.xyz",
        "/info",
        {},
        {"type": "candleSnapshot", "req": dict(CANDLE, endTime=1792578000000 + 86400000)},
    ),
    (
        "api.hyperliquid.xyz",
        "/info",
        {},
        {"type": "candleSnapshot", "req": dict(CANDLE, endTime=1792578000000 + 10 * 86400000)},
    ),
    (
        "api.hyperliquid.xyz",
        "/info",
        {},
        {"type": "candleSnapshot", "req": dict(CANDLE, endTime=1792578000000 - 1)},
    ),
    (
        "api.hyperliquid.xyz",
        "/info",
        {},
        {"type": "candleSnapshot", "req": dict(CANDLE, endTime=1792578000000 - 120001)},
    ),
    ("api.hyperliquid.xyz", "/info", {}, {"type": "candleSnapshot", "req": dict(CANDLE, interval="5m")}),
    (
        "api.hyperliquid.xyz",
        "/info",
        {},
        {"type": "candleSnapshot", "req": {"coin": "BTC", "interval": "1m"}},
    ),
    ("api.hyperliquid.xyz", "/info", {}, {"type": "candleSnapshot"}),
    ("api.hyperliquid.xyz", "/info", {}, {"type": "l2Book", "coin": "BTC"}),
    ("api.hyperliquid.xyz", "/info", {}, {}),
    ("data-api.binance.vision", "/api/v3/klines", {}, {}),
]:
    try:
        rows = plan(host, path, params, payload)
        PLANS.append(
            {
                "host": host,
                "path": path,
                "params": params,
                "payload": payload,
                "buckets": [
                    {"key": k, "capacity": c, "window_s": w, "weight": x, "name": bucket_name(k, w, c)}
                    for k, c, w, x in rows
                ],
            }
        )
    except ValueError as exc:
        PLANS.append({"host": host, "path": path, "params": params, "payload": payload, "error": str(exc)})

NAMES = [
    {"key": k, "window_s": w, "capacity": c, "name": bucket_name(k, w, c)}
    for k, w, c in [
        ("kraken", 1, 1),
        ("binance.spot", 60, 3000),
        ("okx:/x:IP", 2, 10),
        ("hyperliquid", 60, 600),
        ("x", 0.5, 2.5),
        ("x", 300, 250),
        ("x", 1e16, 1e-5),
    ]
]

print(json.dumps({"weights": WEIGHTS, "plans": PLANS, "names": NAMES}, indent=1, sort_keys=True))

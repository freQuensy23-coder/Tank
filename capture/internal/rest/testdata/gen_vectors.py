"""REST adapter vectors from the Python reference: tank.market.adapters.VenueAdapter
driven through MarketService._request (its Hyperliquid response cache), with canned venue
responses. Each case records the exact requests and the result.

    uv run python capture/internal/rest/testdata/gen_vectors.py > capture/internal/rest/testdata/vectors.json
"""

import asyncio
import json
from datetime import timedelta
from decimal import Decimal

from tank.market import adapters, service
from tank.market.adapters import VenueAdapter
from tank.market.service import MarketService
from tank.util import iso as real_iso
from tank.util import jsonable, parse_time

NOW = "2026-10-21T12:34:56.789012Z"


def fixed_iso(value=None):
    return real_iso(value if value is not None else NOW)


adapters.iso = fixed_iso
service.iso = fixed_iso

INSTRUMENTS = {
    "binance.spot": {
        "instrument_id": "binance:spot:BTCUSDT",
        "token_id": "bitcoin",
        "venue": "binance",
        "market_type": "SPOT",
        "venue_symbol": "BTCUSDT",
        "quote_asset": "USDT",
        "contract_multiplier": "1",
    },
    "binance.usdm": {
        "instrument_id": "binance:perp_linear:BTCUSDT",
        "token_id": "bitcoin",
        "venue": "binance",
        "market_type": "PERP_LINEAR",
        "venue_symbol": "BTCUSDT",
        "quote_asset": "USDT",
        "contract_multiplier": "1",
    },
    "okx.spot": {
        "instrument_id": "okx:spot:BTC-USDT",
        "token_id": "bitcoin",
        "venue": "okx",
        "market_type": "SPOT",
        "venue_symbol": "BTC-USDT",
        "quote_asset": "USDT",
        "contract_multiplier": "1",
    },
    "okx.swap": {
        "instrument_id": "okx:perp_linear:BTC-USDT-SWAP",
        "token_id": "bitcoin",
        "venue": "okx",
        "market_type": "PERP_LINEAR",
        "venue_symbol": "BTC-USDT-SWAP",
        "quote_asset": "USDT",
        "contract_multiplier": None,
    },
    "bybit.spot": {
        "instrument_id": "bybit:spot:BTCUSDT",
        "token_id": "bitcoin",
        "venue": "bybit",
        "market_type": "SPOT",
        "venue_symbol": "BTCUSDT",
        "quote_asset": "USDT",
        "contract_multiplier": "1",
    },
    "bybit.linear": {
        "instrument_id": "bybit:perp_linear:BTCUSDT",
        "token_id": "bitcoin",
        "venue": "bybit",
        "market_type": "PERP_LINEAR",
        "venue_symbol": "BTCUSDT",
        "quote_asset": "USDT",
        "contract_multiplier": "1",
    },
    "coinbase.spot": {
        "instrument_id": "coinbase:spot:BTC-USD",
        "token_id": "bitcoin",
        "venue": "coinbase",
        "market_type": "SPOT",
        "venue_symbol": "BTC-USD",
        "quote_asset": "USD",
        "contract_multiplier": "1",
        "tick_size": "0.01",
    },
    "kraken.spot": {
        "instrument_id": "kraken:spot:XBT/USD",
        "token_id": "bitcoin",
        "venue": "kraken",
        "market_type": "SPOT",
        "venue_symbol": "XBT/USD",
        "quote_asset": "USD",
        "contract_multiplier": "1",
    },
    "hyperliquid.perp": {
        "instrument_id": "hyperliquid:perp_linear:BTC",
        "token_id": "bitcoin",
        "venue": "hyperliquid",
        "market_type": "PERP_LINEAR",
        "venue_symbol": "BTC",
        "quote_asset": "USD",
        "contract_multiplier": "1",
        "funding_interval_h": 1,
    },
}

R = {
    "binance.klines": '[[1792578000000,"83200.10","83210.00","83190.50","83205.20","12.345",1792578059999,"1027000.12",123,"6.1","507000.1","0"]]',
    "binance.ticker": '{"symbol":"BTCUSDT","lastPrice":"83205.20","volume":"1234.5","quoteVolume":"102700000.1","openTime":1792491296789,"closeTime":1792577696789,"count":100}',
    "binance.aggTrades": '[{"a":1001,"p":"83200.10","q":"0.500","f":2001,"l":2002,"T":1792578000123,"m":true},{"a":1002,"p":"83200.20","q":"0.001","f":2003,"l":2003,"T":1792578000456,"m":false}]',
    "binance.depth": '{"lastUpdateId":987654,"bids":[["83200.00","1.5"],["83199.90","2"]],"asks":[["83200.10","0.7"],["83200.20","1"]]}',
    "binance.depth.crossed": '{"lastUpdateId":987654,"bids":[["83200.10","1.5"]],"asks":[["83200.10","0.7"]]}',
    "usdm.depth": '{"lastUpdateId":5550001,"E":1792578000100,"T":1792578000099,"bids":[["83210.0","3"]],"asks":[["83210.1","4"]]}',
    "usdm.oi": '{"openInterest":"81234.567","symbol":"BTCUSDT","time":1792578000123}',
    "usdm.oihist": '[{"symbol":"BTCUSDT","sumOpenInterest":"81000.1","sumOpenInterestValue":"6739000000.5","CMCCirculatingSupply":"1","timestamp":1792577400000},{"symbol":"BTCUSDT","sumOpenInterest":"81100.2","sumOpenInterestValue":"6748000000.5","CMCCirculatingSupply":"1","timestamp":1792577700000}]',
    "usdm.premium": '{"symbol":"BTCUSDT","markPrice":"83210.10000000","indexPrice":"83205.00000000","estimatedSettlePrice":"83190.0","lastFundingRate":"0.00010000","interestRate":"0.00010000","nextFundingTime":1792598400000,"time":1792578000000}',
    "usdm.funding": '[{"symbol":"BTCUSDT","fundingTime":1792540800000,"fundingRate":"0.00010000","markPrice":"83000.0"},{"symbol":"BTCUSDT","fundingTime":1792569600000,"fundingRate":"0.00009000","markPrice":"83100.0"}]',
    "usdm.fundingInfo": '[{"symbol":"ETHUSDT","adjustedFundingRateCap":"0.02","fundingIntervalHours":4},{"symbol":"BTCUSDT","adjustedFundingRateCap":"0.02","fundingIntervalHours":8}]',
    "usdm.fundingInfo.absent": '[{"symbol":"ETHUSDT","fundingIntervalHours":4}]',
    "okx.candles": '{"code":"0","msg":"","data":[["1792578000000","83200.1","83210","83190.5","83205.2","12.3","1020000","1020000","1"],["1792577940000","83190.1","83200","83180.5","83200.1","10.3","850000","850000","1"]]}',
    "okx.ticker": '{"code":"0","msg":"","data":[{"instType":"SPOT","instId":"BTC-USDT","last":"83205.2","vol24h":"1234.5","volCcy24h":"102700000.1","ts":"1792578000123"}]}',
    "okx.instruments": '{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC","tickSz":"0.1","lotSz":"0.01","state":"live"}]}',
    "okx.instruments.nomult": '{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","ctVal":"0.0100","ctMult":"","ctValCcy":"BTC","tickSz":"0.1","lotSz":"0.01","state":"suspend"}]}',
    "okx.instruments.usd": '{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","ctVal":"100","ctMult":"1","ctValCcy":"USD","tickSz":"0.1","lotSz":"1","state":"live"}]}',
    "okx.oi": '{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","oi":"2500000","oiCcy":"25000","oiUsd":"2080000000","ts":"1792578000123"}]}',
    "okx.fundingrate": '{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","fundingRate":"0.0001","nextFundingRate":"","fundingTime":"1792598400000","prevFundingTime":"1792569600000","ts":"1792578000123"}]}',
    "okx.fundingrate.noprev": '{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","fundingRate":"0.0001","fundingTime":"1792598400000","ts":"1792578000123"}]}',
    "okx.fundinghistory": '{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","fundingRate":"0.0001","realizedRate":"0.0001","fundingTime":"1792569600000","method":"current_period"}]}',
    "okx.trades": '{"code":"0","msg":"","data":[{"instId":"BTC-USDT","side":"buy","sz":"0.1","px":"83200","tradeId":"555002","ts":"1792578000456"},{"instId":"BTC-USDT","side":"sell","sz":"0.2","px":"83199.9","tradeId":"555001","ts":"1792578000123"}]}',
    "okx.empty": '{"code":"0","msg":"","data":[]}',
    "okx.error": '{"code":"50011","msg":"Too Many Requests","data":[]}',
    "bybit.kline": '{"retCode":0,"retMsg":"OK","result":{"category":"spot","symbol":"BTCUSDT","list":[["1792578000000","83200.1","83210","83190.5","83205.2","12.3","1020000"]]},"time":1792578000999}',
    "bybit.tickers": '{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","lastPrice":"83205.2","markPrice":"83210.1","indexPrice":"83205.0","fundingRate":"0.0001","nextFundingTime":"1792598400000","fundingIntervalHour":"8","openInterest":"12345.6","volume24h":"1234.5","turnover24h":"102700000.1"}]},"time":1792578000999}',
    "bybit.tickers.nointerval": '{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","lastPrice":"83205.2","markPrice":"83210.1","indexPrice":"83205.0","fundingRate":"0.0001","fundingIntervalHour":""}]},"time":1792578000999}',
    "bybit.oi": '{"retCode":0,"retMsg":"OK","result":{"symbol":"BTCUSDT","category":"linear","list":[{"openInterest":"12345.6","timestamp":"1792577700000"},{"openInterest":"12340.1","timestamp":"1792577400000"}],"nextPageCursor":"lastid%3D123"},"time":1792578000999}',
    "bybit.oi.last": '{"retCode":0,"retMsg":"OK","result":{"symbol":"BTCUSDT","category":"linear","list":[],"nextPageCursor":""},"time":1792578000999}',
    "bybit.funding": '{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","fundingRate":"0.0001","fundingRateTimestamp":"1792569600000"}]},"time":1792578000999}',
    "bybit.trades": '{"retCode":0,"retMsg":"OK","result":{"category":"spot","list":[{"execId":"2100000000012345678","symbol":"BTCUSDT","price":"83200.1","size":"0.01","side":"Buy","time":"1792578000123","isBlockTrade":false,"seq":"5432101"}]},"time":1792578000999}',
    "bybit.error": '{"retCode":10006,"retMsg":"Too many visits!","result":{},"time":1792578000999}',
    "coinbase.candles": "[[1792578000,83190.5,83210,83200.1,83205.2,12.3456789],[1792577940,83180.5,83200,83190.1,83200.1,10.3]]",
    "coinbase.stats": '{"open":"82000.00","high":"84000.00","low":"81000.00","last":"83205.20","volume":"1234.5","volume_30day":"40000"}',
    "coinbase.usdt": '{"open":"1.0001","high":"1.0003","low":"0.9998","last":"1.00010000","volume":"123456789"}',
    "coinbase.trades": '[{"time":"2026-10-21T12:00:00.456789Z","trade_id":123457,"price":"83200.10","size":"0.10000000","side":"buy"},{"time":"2026-10-21T12:00:00.123456Z","trade_id":123456,"price":"83200.00","size":"0.20000000","side":"sell"}]',
    "coinbase.book": '{"bids":[["83200.00","1.20000000",3]],"asks":[["83200.50","0.40000000",1]],"sequence":123456789,"auction_mode":false,"auction":null,"time":"2026-10-21T12:00:00.123456Z"}',
    "coinbase.book.empty": '{"bids":[["0","1.2",3]],"asks":[["83200.50","0.4",1]],"sequence":1}',
    "kraken.ohlc": '{"error":[],"result":{"XXBTZUSD":[[1792578000,"83200.1","83210.0","83190.5","83205.2","83201.0","12.30000000",123]],"last":1792578000}}',
    "kraken.ticker": '{"error":[],"result":{"XXBTZUSD":{"a":["83205.30000","1","1.000"],"b":["83205.20000","2","2.000"],"c":["83205.20000","0.00100000"],"v":["100.1","1234.5"],"p":["83100.1","83000.2"],"t":[1000,20000],"l":["81000.0","81000.0"],"h":["84000.0","84000.0"],"o":"82000.0"}}}',
    "kraken.trades": '{"error":[],"result":{"XXBTZUSD":[["83204.30000","0.00040066",1792578672.6709492,"b","l","",112344411],["83204.30000","0.00006050",1792578675.5948038,"s","m","",112344412]],"last":"1792578675594803712"}}',
    "kraken.error": '{"error":["EGeneral:Invalid arguments","EQuery:Unknown asset pair"]}',
    "hl.meta": '[{"universe":[{"name":"ETH","szDecimals":4,"maxLeverage":25},{"name":"BTC","szDecimals":5,"maxLeverage":40},{"name":"OLD","szDecimals":0,"isDelisted":true}]},[{"funding":"0.0000100","openInterest":"20000.1","markPx":"3100.1","oraclePx":"3100.0","dayNtlVlm":"1000"},{"funding":"0.0000125","openInterest":"1000.12345","markPx":"83210.0","oraclePx":"83205.0","midPx":"83209.5","premium":"0.0001","dayNtlVlm":"100000000.0"},{"funding":"0.0","openInterest":"0","markPx":"1.0","oraclePx":"1.0"}]]',
    "hl.funding": '[{"coin":"BTC","fundingRate":"0.0000125","premium":"0.0001","time":1792574400000},{"coin":"BTC","fundingRate":"0.0000100","premium":"0.00008","time":1792578000000}]',
    "hl.candles": '[{"t":1792578000000,"T":1792578059999,"s":"BTC","i":"1m","o":"83200.1","c":"83205.2","h":"83210","l":"83190.5","v":"12.3","n":123}]',
}

S = parse_time("2026-10-21T10:00:00.123456Z")
E = parse_time("2026-10-21T12:00:00Z")


class Stub:
    """MarketService._request with its Hyperliquid cache, serving canned responses."""

    def __init__(self, responses):
        self.service = object.__new__(MarketService)
        self.service._response_cache = {}
        self.service._cache_locks = {}
        self.responses = list(responses)
        self.requests = []
        stub = self

        async def live(method, url, **kwargs):
            priority = kwargs.pop("_priority", 0)
            stub.requests.append(
                {
                    "method": method,
                    "url": url,
                    "params": {k: str(v) for k, v in (kwargs.get("params") or {}).items()},
                    "json": jsonable(kwargs.get("json")),
                    "priority": priority,
                }
            )
            if not stub.responses:
                raise RuntimeError("no canned response")
            return json.loads(R[stub.responses.pop(0)], parse_float=Decimal)

        self.service._request_live = live

    def adapter(self, instrument, priority=0):
        async def request(method, url, **kwargs):
            return await self.service._request(method, url, _priority=priority, **kwargs)

        return VenueAdapter(instrument, request)


CASES = []


def case(name, key, call, responses, args=None, priority=0, instrument=None):
    CASES.append(
        {
            "name": name,
            "key": key,
            "call": call,
            "responses": responses,
            "args": args or {},
            "priority": priority,
            "instrument": instrument or INSTRUMENTS[key],
        }
    )


def times(start=None, end=None, cursor=None):
    return {
        "start": real_iso(start) if start else None,
        "end": real_iso(end) if end else None,
        "cursor": cursor,
    }


for key, latest, ranged in [
    ("binance.spot", ["binance.klines"], ["binance.klines"]),
    ("binance.usdm", ["binance.klines"], ["binance.klines"]),
    ("okx.spot", ["okx.candles"], ["okx.candles"]),
    ("okx.swap", ["okx.candles"], ["okx.candles"]),
    ("bybit.spot", ["bybit.kline"], ["bybit.kline"]),
    ("bybit.linear", ["bybit.kline"], ["bybit.kline"]),
    ("coinbase.spot", ["coinbase.candles"], ["coinbase.candles"]),
    ("kraken.spot", ["kraken.ohlc"], ["kraken.ohlc"]),
    ("hyperliquid.perp", ["hl.candles"], ["hl.candles"]),
]:
    case(key + " klines latest", key, "klines", latest)
    case(key + " klines range", key, "klines", ranged, times(S, E), priority=1)
case("coinbase.spot klines start only", "coinbase.spot", "klines", ["coinbase.candles"], times(S))
case("kraken.spot klines end only", "kraken.spot", "klines", ["kraken.ohlc"], times(None, E))
case("okx.spot klines error", "okx.spot", "klines", ["okx.error"])
case("bybit.spot klines error", "bybit.spot", "klines", ["bybit.error"])
case("kraken.spot klines error", "kraken.spot", "klines", ["kraken.error"])

for key, responses in [
    ("binance.spot", ["binance.ticker"]),
    ("binance.usdm", ["binance.ticker"]),
    ("okx.spot", ["okx.ticker"]),
    ("bybit.linear", ["bybit.tickers"]),
    ("coinbase.spot", ["coinbase.stats"]),
    ("kraken.spot", ["kraken.ticker"]),
    ("hyperliquid.perp", ["hl.meta"]),
]:
    case(key + " ticker", key, "ticker", responses)
case("okx.spot ticker empty", "okx.spot", "ticker", ["okx.empty"])

case(
    "binance.usdm derivatives",
    "binance.usdm",
    "derivatives",
    ["usdm.oi", "usdm.oihist", "usdm.premium", "usdm.funding", "usdm.fundingInfo"],
    priority=-1,
)
case(
    "binance.usdm derivatives default interval",
    "binance.usdm",
    "derivatives",
    ["usdm.oi", "usdm.oihist", "usdm.premium", "usdm.funding", "usdm.fundingInfo.absent"],
)
case("okx.swap derivatives", "okx.swap", "derivatives", ["okx.oi", "okx.fundingrate", "okx.fundinghistory"])
case(
    "okx.swap derivatives no prev",
    "okx.swap",
    "derivatives",
    ["okx.oi", "okx.fundingrate.noprev", "okx.fundinghistory"],
)
case(
    "bybit.linear derivatives", "bybit.linear", "derivatives", ["bybit.oi", "bybit.tickers", "bybit.funding"]
)
case(
    "bybit.linear derivatives registry interval",
    "bybit.linear",
    "derivatives",
    ["bybit.oi", "bybit.tickers.nointerval", "bybit.funding"],
    instrument=dict(INSTRUMENTS["bybit.linear"], funding_interval_h="8"),
)
case(
    "bybit.linear derivatives no interval",
    "bybit.linear",
    "derivatives",
    ["bybit.oi", "bybit.tickers.nointerval", "bybit.funding"],
)
case("hyperliquid.perp derivatives", "hyperliquid.perp", "derivatives", ["hl.meta", "hl.funding"])
case("binance.spot derivatives", "binance.spot", "derivatives", [])

case(
    "binance.spot trades range",
    "binance.spot",
    "trades",
    ["binance.aggTrades"],
    times(S, E - timedelta(milliseconds=1)),
)
case("binance.spot trades cursor", "binance.spot", "trades", ["binance.aggTrades"], times(S, E, 1003))
case(
    "binance.spot trades end bound",
    "binance.spot",
    "trades",
    ["binance.aggTrades"],
    times(None, S - timedelta(milliseconds=1)),
)
case("binance.usdm trades cursor zero", "binance.usdm", "trades", ["binance.aggTrades"], times(None, None, 0))
case("okx.spot trades end", "okx.spot", "trades", ["okx.trades"], times(S, E - timedelta(milliseconds=1)))
case("okx.spot trades cursor", "okx.spot", "trades", ["okx.trades"], times(S, E, "555001"))
case("okx.spot trades empty", "okx.spot", "trades", ["okx.empty"], times(None, None, "1"))
case("okx.swap trades latest", "okx.swap", "trades", ["okx.trades"])
case("bybit.spot trades", "bybit.spot", "trades", ["bybit.trades"], times(S, E))
case("coinbase.spot trades", "coinbase.spot", "trades", ["coinbase.trades"], times(S, E))
case("coinbase.spot trades cursor", "coinbase.spot", "trades", ["coinbase.trades"], times(S, E, 123456))
case("kraken.spot trades start", "kraken.spot", "trades", ["kraken.trades"], times(S))
case(
    "kraken.spot trades cursor",
    "kraken.spot",
    "trades",
    ["kraken.trades"],
    times(S, None, "1792578675594803712"),
)
case("kraken.spot trades error", "kraken.spot", "trades", ["kraken.error"], times(S))
case("hyperliquid.perp trades", "hyperliquid.perp", "trades", [], times(S, E))

case("bybit.linear oi history", "bybit.linear", "oi_history", ["bybit.oi"], times(S, E), priority=2)
case(
    "bybit.linear oi history cursor",
    "bybit.linear",
    "oi_history",
    ["bybit.oi.last"],
    times(S, E, "lastid%3D123"),
)
case("binance.usdm oi history", "binance.usdm", "oi_history", [], times(S, E))

case("okx.swap discover", "okx.swap", "discover", ["okx.instruments"])
case("okx.swap discover no multiplier", "okx.swap", "discover", ["okx.instruments.nomult"])
case("okx.swap discover usd contract", "okx.swap", "discover", ["okx.instruments.usd"])
case("hyperliquid.perp discover", "hyperliquid.perp", "discover", ["hl.meta"])
case(
    "hyperliquid.perp discover eth",
    "hyperliquid.perp",
    "discover",
    ["hl.meta"],
    instrument=dict(INSTRUMENTS["hyperliquid.perp"], venue_symbol="ETH"),
)
case(
    "hyperliquid.perp discover delisted",
    "hyperliquid.perp",
    "discover",
    ["hl.meta"],
    instrument=dict(INSTRUMENTS["hyperliquid.perp"], venue_symbol="OLD"),
)
case(
    "hyperliquid.perp discover missing",
    "hyperliquid.perp",
    "discover",
    ["hl.meta"],
    instrument=dict(INSTRUMENTS["hyperliquid.perp"], venue_symbol="NONE"),
)
case("binance.spot discover", "binance.spot", "discover", [])

case("binance.spot depth", "binance.spot", "depth", ["binance.depth"])
case("binance.usdm depth", "binance.usdm", "depth", ["usdm.depth"])

STREAM = {"mid": "83200.05", "spread_bps": "0.012"}
case(
    "binance.spot independent book", "binance.spot", "independent_book", ["binance.depth"], {"stream": STREAM}
)
case(
    "binance.spot independent book far",
    "binance.spot",
    "independent_book",
    ["binance.depth"],
    {"stream": {"mid": "83300.05", "spread_bps": "0.5"}},
)
case(
    "binance.spot independent book crossed",
    "binance.spot",
    "independent_book",
    ["binance.depth.crossed"],
    {"stream": STREAM},
)
case(
    "coinbase.spot independent book",
    "coinbase.spot",
    "independent_book",
    ["coinbase.book"],
    {"stream": {"mid": "83200.25", "spread_bps": "0.06"}},
)
case(
    "coinbase.spot independent book no tick",
    "coinbase.spot",
    "independent_book",
    ["coinbase.book"],
    {"stream": {"mid": "83200.25", "spread_bps": "0.06"}},
    instrument=dict(INSTRUMENTS["coinbase.spot"], tick_size=None),
)
case(
    "coinbase.spot independent book empty",
    "coinbase.spot",
    "independent_book",
    ["coinbase.book.empty"],
    {"stream": STREAM},
)
case("okx.spot independent book", "okx.spot", "independent_book", [], {"stream": STREAM})

case("fx USDT", "coinbase.spot", "fx", ["coinbase.usdt"], {"quote": "USDT"})


async def run(stub, c):
    instrument = c["instrument"]
    adapter = stub.adapter(instrument, c["priority"])
    args = c["args"]
    start = parse_time(args["start"]) if args.get("start") else None
    end = parse_time(args["end"]) if args.get("end") else None
    cursor = args.get("cursor")
    call = c["call"]
    if call == "klines":
        return await adapter.klines(start=start, end=end)
    if call == "ticker":
        return await adapter.ticker()
    if call == "derivatives":
        return list(await adapter.derivatives())
    if call == "trades":
        return list(await adapter.trades(start=start, end=end, cursor=cursor))
    if call == "oi_history":
        return list(await adapter.oi_history(start=start, end=end, cursor=cursor))
    if call == "discover":
        return await adapter.discover()
    if call == "depth":
        key = adapters.venue_key(instrument)
        root = "https://api.binance.com" if key.endswith("spot") else "https://fapi.binance.com"
        path = "/api/v3/depth" if key.endswith("spot") else "/fapi/v1/depth"
        return await stub.service._request(
            "GET",
            root + path,
            _priority=c["priority"],
            params={"symbol": instrument["venue_symbol"], "limit": 1000},
        )
    if call == "independent_book":
        return await MarketService._independent_book(stub.service, instrument, args["stream"])
    if call == "fx":
        body = await stub.service._request(
            "GET", f"https://api.exchange.coinbase.com/products/{args['quote']}-USD/stats"
        )
        return str(service.norm.decimal(body["last"]))
    raise ValueError(call)


OUT = []
for c in CASES:
    stub = Stub(c["responses"])
    try:
        record = dict(c, result=jsonable(asyncio.run(run(stub, c))), error=None)
    except Exception as exc:  # noqa: BLE001 - the error message is the vector
        record = dict(c, result=None, error=f"{type(exc).__name__}: {exc}")
    record["requests"] = stub.requests
    record["responses"] = [R[name] for name in c["responses"]]
    OUT.append(record)


print(json.dumps({"now": NOW, "cases": OUT}, indent=1, sort_keys=True))

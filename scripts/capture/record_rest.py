"""Record one REST poll cycle per venue market path for the REST parity harness.

For two instruments per venue path, tank's MarketService runs discover, poll_once
(derivatives, klines, ticker, with the stablecoin USD rate) and one trades page against the
live public endpoints; every exchange (method, url, params, json body -> status, body) is
stored in capture/testdata/rest/<path>.json.gz. A venue whose REST is unreachable from the
recording host (HTTP 403/451) gets the SYNTHETIC bodies below instead: the shapes of its
documented responses with the recording's prices.

Run: uv run python scripts/capture/record_rest.py [--only okx-swap ...]
"""

import argparse
import asyncio
import gzip
import json
import sys
import tempfile
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))

from parity import FRAMES, ROOT, instruments_of, load, setup_config

OUT = ROOT / "capture" / "testdata" / "rest"
PATHS = [
    "binance-spot",
    "binance-usdm",
    "bybit-spot",
    "bybit-linear",
    "coinbase-spot",
    "kraken-spot",
    "okx-spot",
    "okx-swap",
    "hyperliquid-perp",
]


def ms(text):
    from tank.util import parse_time

    return int(parse_time(text).timestamp() * 1000)


def synthetic(path, params, now):
    """Documented response shapes for venues the recording host cannot reach."""
    t = ms(now) // 60000 * 60000
    symbol = params.get("symbol", "ONDOUSDT")
    price = "0.4561" if symbol.startswith("ONDO") else "2.0045"
    if path in ("/api/v3/klines", "/fapi/v1/klines"):
        return [
            [
                t - 120000,
                price,
                "0.4570",
                "0.4550",
                price,
                "1234.5",
                t - 60001,
                "563.21",
                41,
                "600.1",
                "273.8",
                "0",
            ],
            [t - 60000, price, "0.4566", "0.4558", price, "981.2", t - 1, "447.6", 33, "512.0", "233.5", "0"],
            [t, price, price, price, price, "12.0", t + 59999, "5.47", 2, "6.0", "2.73", "0"],
        ]
    if path in ("/api/v3/ticker/24hr", "/fapi/v1/ticker/24hr"):
        return {
            "symbol": symbol,
            "lastPrice": price,
            "priceChange": "0.0012",
            "priceChangePercent": "0.26",
            "volume": "1234567.8",
            "quoteVolume": "563210.4",
            "openTime": t - 86400000,
            "closeTime": t,
            "count": 12345,
        }
    if path in ("/api/v3/aggTrades", "/fapi/v1/aggTrades"):
        return [
            {
                "a": 29965265 + i,
                "p": price,
                "q": f"{10 + i}.5",
                "f": 4000 + 2 * i,
                "l": 4001 + 2 * i,
                "T": t - 30000 + 1000 * i,
                "m": i % 2 == 0,
            }
            for i in range(5)
        ]
    if path == "/fapi/v1/openInterest":
        return {"openInterest": "51234567.8", "symbol": symbol, "time": t - 1500}
    if path == "/futures/data/openInterestHist":
        return [
            {
                "symbol": symbol,
                "sumOpenInterest": "51230000.1",
                "sumOpenInterestValue": "23370000.5",
                "timestamp": t - 600000,
            },
            {
                "symbol": symbol,
                "sumOpenInterest": "51234000.9",
                "sumOpenInterestValue": "23372000.4",
                "timestamp": t - 300000,
            },
        ]
    if path == "/fapi/v1/premiumIndex":
        return {
            "symbol": symbol,
            "markPrice": price,
            "indexPrice": price,
            "estimatedSettlePrice": price,
            "lastFundingRate": "0.00010000",
            "interestRate": "0.00010000",
            "nextFundingTime": t + 3600000,
            "time": t - 1000,
        }
    if path == "/fapi/v1/fundingRate":
        return [
            {"symbol": symbol, "fundingTime": t - 57600000, "fundingRate": "0.00010000", "markPrice": price},
            {"symbol": symbol, "fundingTime": t - 28800000, "fundingRate": "0.00008000", "markPrice": price},
        ]
    if path == "/fapi/v1/fundingInfo":
        return [
            {
                "symbol": symbol,
                "adjustedFundingRateCap": "0.02",
                "adjustedFundingRateFloor": "-0.02",
                "fundingIntervalHours": 8,
            }
        ]
    category = params.get("category", "spot")
    envelope = lambda result: {"retCode": 0, "retMsg": "OK", "result": result, "retExtInfo": {}, "time": t}
    if path == "/v5/market/kline":
        rows = [[str(t - 60000 * k), price, price, price, price, "1000.5", "456.1"] for k in (0, 1, 2)]
        return envelope({"category": category, "symbol": symbol, "list": rows})
    if path == "/v5/market/tickers":
        row = {
            "symbol": symbol,
            "lastPrice": price,
            "prevPrice24h": price,
            "price24hPcnt": "0.0026",
            "highPrice24h": price,
            "lowPrice24h": price,
            "turnover24h": "563210.4",
            "volume24h": "1234567.8",
        }
        if category == "linear":
            row.update(
                markPrice=price,
                indexPrice=price,
                openInterest="51234567",
                openInterestValue="23370000.1",
                fundingRate="0.0001",
                nextFundingTime=str(t + 3600000),
                fundingIntervalHour="8",
            )
        return envelope({"category": category, "list": [row]})
    if path == "/v5/market/open-interest":
        rows = [{"openInterest": "51234567", "timestamp": str(t - 300000 * k)} for k in (1, 2)]
        return envelope({"symbol": symbol, "category": "linear", "list": rows, "nextPageCursor": ""})
    if path == "/v5/market/funding/history":
        rows = [
            {"symbol": symbol, "fundingRate": "0.0001", "fundingRateTimestamp": str(t - 28800000 * k)}
            for k in (1, 2)
        ]
        return envelope({"category": "linear", "list": rows})
    if path == "/v5/market/recent-trade":
        rows = [
            {
                "execId": f"21000000000{1234 + i}",
                "symbol": symbol,
                "price": price,
                "size": f"{3 + i}",
                "side": "Buy" if i % 2 else "Sell",
                "time": str(t - 20000 + 1000 * i),
                "isBlockTrade": False,
                "seq": str(5432101 + i),
            }
            for i in range(5)
        ]
        return envelope({"category": category, "list": rows})
    raise KeyError(path)


async def record(name):
    from tank.market.service import MarketService
    from tank.settings import Settings
    from tank.storage import MemoryRepository
    from tank.util import iso

    meta, _ = load(FRAMES / f"{name}.jsonl.gz")
    instruments = instruments_of(meta)
    now = iso()
    exchanges = []
    with tempfile.TemporaryDirectory() as directory:
        settings = Settings(
            _env_file=None, config_dir=setup_config(directory, instruments), data_dir=Path(directory) / "data"
        )
        async with httpx.AsyncClient(
            timeout=30, headers={"User-Agent": "TankV0/1.0 (parity recording)"}
        ) as http:
            original = http.request

            async def request(method, url, **kwargs):
                response = await original(method, url, **kwargs)
                body = response.text
                status = response.status_code
                if status in (403, 451):
                    status = 200
                    body = json.dumps(synthetic(httpx.URL(url).path, dict(kwargs.get("params") or {}), now))
                exchanges.append(
                    {
                        "method": method,
                        "url": str(httpx.URL(url)),
                        "params": {k: str(v) for k, v in (kwargs.get("params") or {}).items()},
                        "json": kwargs.get("json"),
                        "status": status,
                        "body": body,
                    }
                )
                return httpx.Response(status, content=body.encode(), request=response.request)

            http.request = request
            service = MarketService(MemoryRepository(), settings, http=http)
            service._market_restored = True
            service.instruments = {i["instrument_id"]: dict(i) for i in instruments}
            for iid, inst in service.instruments.items():
                await service.poll_once(iid, derivatives=True, klines=True, ticker=True)
                try:
                    await service.adapter(inst).trades()
                except Exception as exc:  # noqa: BLE001 - Hyperliquid lists no REST trades
                    print(name, iid, "trades:", type(exc).__name__, exc)
    OUT.mkdir(parents=True, exist_ok=True)
    payload = {
        "recorded_at": now,
        "fixture": name,
        "instruments": meta["instruments"],
        "exchanges": exchanges,
    }
    with gzip.open(OUT / f"{name}.json.gz", "wt") as stream:
        json.dump(payload, stream, sort_keys=True)
    print(name, len(exchanges), "exchanges", sum(1 for e in exchanges if e["status"] != 200), "errors")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*")
    args = parser.parse_args()
    for name in args.only or PATHS:
        asyncio.run(record(name))


if __name__ == "__main__":
    main()

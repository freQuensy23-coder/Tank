import asyncio
import hashlib
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

P = Path(__file__).parent
records = []


def now():
    return datetime.now(UTC).isoformat()


async def main():
    async with httpx.AsyncClient(timeout=25, trust_env=False, follow_redirects=True) as c:

        async def req(name, url, params=None, method="GET"):
            r = {"name": name, "url": url, "params": params, "method": method, "started_at": now()}
            try:
                z = await c.request(method, url, params=params)
                body = z.content
                f = P / (name + ".body")
                f.write_bytes(body)
                r.update(
                    received_at=now(),
                    status=z.status_code,
                    headers=dict(z.headers),
                    sha256=hashlib.sha256(body).hexdigest(),
                    bytes=len(body),
                    path=f.name,
                )
            except httpx.HTTPError as e:
                r.update(received_at=now(), error=type(e).__name__)
                z = None
            records.append(r)
            (P / "receipts.json").write_text(json.dumps(records, indent=2) + "\n")
            print(json.dumps({k: v for k, v in r.items() if k != "headers"}), flush=True)
            await asyncio.sleep(0.6)
            return z

        for day in ["15", "16"]:
            await req(
                "binance-head-" + day,
                "https://data.binance.vision/data/spot/daily/aggTrades/WLDUSDT/WLDUSDT-aggTrades-2026-10-"
                + day
                + ".zip",
                method="HEAD",
            )
        await req("okx-docs", "https://www.okx.com/docs-v5/en/")
        await req(
            "coinbase-docs",
            "https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades",
        )
        await req("kraken-docs", "https://docs.kraken.com/api/docs/rest-api/get-recent-trades/")
        await req(
            "okx-probe",
            "https://www.okx.com/api/v5/market/history-trades",
            {"instId": "WLD-USDT", "type": "2", "after": "1792153800000", "limit": "100"},
        )
        await req(
            "coinbase-probe", "https://api.exchange.coinbase.com/products/WLD-USD/trades", {"limit": 100}
        )
        await req(
            "kraken-probe",
            "https://api.kraken.com/0/public/Trades",
            {"pair": "WLDUSD", "since": "1792139400000000000", "count": 1000},
        )
        cutoff = datetime(2026, 10, 16, 12, 30, tzinfo=UTC)
        start = cutoff - timedelta(days=9)
        end = int(cutoff.timestamp() * 1000) - 1
        seen = set()
        for page in range(16):
            params = {
                "category": "linear",
                "symbol": "WLDUSDT",
                "intervalTime": "5min",
                "startTime": int(start.timestamp() * 1000),
                "endTime": end,
                "limit": 200,
            }
            z = await req("bybit-oi-%02d" % page, "https://api.bybit.com/v5/market/open-interest", params)
            if z is None or z.status_code != 200:
                break
            j = z.json()
            items = j.get("result", {}).get("list", [])
            if j.get("retCode") != 0 or not items:
                break
            ts = [int(x["timestamp"]) for x in items]
            new = set(ts) - seen
            seen.update(ts)
            if not new:
                break
            end = min(ts) - 1
            if end < int(start.timestamp() * 1000):
                break
        print("DONE", len(seen), "OI points; disk free", shutil.disk_usage(P).free, flush=True)


asyncio.run(main())

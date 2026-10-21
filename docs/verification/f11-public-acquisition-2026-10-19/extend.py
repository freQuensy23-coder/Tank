import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import yaml

from tank.market.archive import fetch_archive
from tank.storage import MemoryRepository

P = Path(__file__).parent
receipts = []


def now():
    return datetime.now(UTC).isoformat()


async def main():
    async with httpx.AsyncClient(timeout=30, trust_env=False, follow_redirects=True) as c:

        async def get(name, url, params=None):
            a = now()
            z = await c.get(url, params=params)
            body = z.content
            f = P / (name + ".body")
            f.write_bytes(body)
            r = {
                "name": name,
                "url": url,
                "params": params,
                "started_at": a,
                "received_at": now(),
                "status": z.status_code,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "path": f.name,
                "headers": dict(z.headers),
            }
            receipts.append(r)
            (P / "extended-receipts.json").write_text(json.dumps(receipts, indent=2) + "\n")
            print(name, z.status_code, len(body), flush=True)
            await asyncio.sleep(0.7)
            return z

        # Exact original archive bytes acquired once; production importer consumes saved HTTP responses.
        archive_bodies = {}
        for day in ["15", "16"]:
            url = (
                "https://data.binance.vision/data/spot/daily/aggTrades/WLDUSDT/WLDUSDT-aggTrades-2026-10-"
                + day
                + ".zip"
            )
            for suffix, label in [("", "zip"), (".CHECKSUM", "checksum")]:
                z = await get("binance-" + day + "-" + label, url + suffix)
                z.raise_for_status()
                archive_bodies[url + suffix] = z.content
        registry = yaml.safe_load(Path("config/registry.yaml").read_text())
        settings = SimpleNamespace(
            data_dir=P / "production-import",
            config_version=hashlib.sha256(Path("config/registry.yaml").read_bytes()).hexdigest(),
            code_version=hashlib.sha256(Path("src/tank/market/archive.py").read_bytes()).hexdigest(),
            load_config=lambda k: registry if k == "registry" else {"archive_retry_s": 3600},
        )
        repo = MemoryRepository()

        def response(request):
            return httpx.Response(200, content=archive_bodies[str(request.url)])

        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as saved:
            results = []
            for day in ["15", "16"]:
                results.append(
                    await fetch_archive(
                        repo, settings, "WLDUSDT", "2026-10-" + day, kind="aggTrades", http=saved
                    )
                )
        (P / "production-import-results.json").write_text(json.dumps(results, indent=2, default=str) + "\n")
        # OKX starts immediately before cutoff and walks backward on actual trade IDs.
        after = "1792153800000"
        typ = "2"
        for page in range(150):
            z = await get(
                "okx-history-%03d" % page,
                "https://www.okx.com/api/v5/market/history-trades",
                {"instId": "WLD-USDT", "type": typ, "after": after, "limit": "100"},
            )
            j = z.json()
            rs = j.get("data", [])
            if j.get("code") != "0" or not rs:
                break
            if min(int(x["ts"]) for x in rs) <= 1792139400000:
                break
            nxt = min(rs, key=lambda x: int(x["tradeId"]))["tradeId"]
            if nxt == after:
                break
            after = nxt
            typ = "1"
        # Coinbase backward ID pagination; stop at first pre-start observation.
        after = None
        for page in range(50):
            params = {"limit": 1000}
            if after is not None:
                params["after"] = after
            z = await get(
                "coinbase-history-%03d" % page,
                "https://api.exchange.coinbase.com/products/WLD-USD/trades",
                params,
            )
            if z.status_code != 200:
                break
            rs = z.json()
            if not isinstance(rs, list) or not rs:
                break
            if (
                min(datetime.fromisoformat(x["time"].replace("Z", "+00:00")).timestamp() for x in rs)
                <= 1792139400
            ):
                break
            nxt = z.headers.get("cb-after")
            if not nxt or nxt == after:
                break
            after = nxt
        # Kraken forward since cursor; request actual boundary, no inferred zero quiet minutes.
        since = "1792139400000000000"
        for page in range(30):
            z = await get(
                "kraken-history-%03d" % page,
                "https://api.kraken.com/0/public/Trades",
                {"pair": "WLDUSD", "since": since, "count": 1000},
            )
            j = z.json()
            if j.get("error"):
                break
            result = j.get("result", {})
            rs = result.get("WLDUSD", [])
            if not rs:
                break
            if max(float(x[2]) for x in rs) >= 1792153800:
                break
            nxt = str(result.get("last"))
            if nxt == since:
                break
            since = nxt
    print("DONE", flush=True)


asyncio.run(main())

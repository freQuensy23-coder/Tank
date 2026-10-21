"""Record public exchange WebSocket frames as tank-capture parity fixtures.

Subscribes exactly as tank.market.streams does (connection/subscription_messages) for
the registry instruments named below and writes one gzipped JSON-lines file per market
path to capture/testdata/frames/: a meta line, then {"t", "endpoint", "text"} per frame
(receive clock, endpoint, the frame text as received). Binance spot depth also gets the
REST snapshot of its documented sync procedure. Public market data only; no keys.
Run: uv run python scripts/capture/record_frames.py [seconds]
"""

import asyncio
import gzip
import json
import sys
from pathlib import Path

import httpx
import websockets
import yaml

from tank.market.streams import connection, subscription_messages
from tank.util import iso

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "capture" / "testdata" / "frames"
TOKENS = ("ondo:ONDO", "layerzero:ZRO")
PATHS = {
    "binance-spot": ("binance", "SPOT"),
    "binance-usdm": ("binance", "PERP_LINEAR"),
    "coinbase-spot": ("coinbase", "SPOT"),
    "okx-spot": ("okx", "SPOT"),
    "okx-swap": ("okx", "PERP_LINEAR"),
    "bybit-spot": ("bybit", "SPOT"),
    "bybit-linear": ("bybit", "PERP_LINEAR"),
    "kraken-spot": ("kraken", "SPOT"),
    "hyperliquid-perp": ("hyperliquid", "PERP_LINEAR"),
}


def instruments(venue, market):
    registry = yaml.safe_load((ROOT / "config" / "registry.yaml").read_text())
    return [
        i
        for i in registry["instruments"]
        if i["token_id"] in TOKENS and i["venue"] == venue and i["market_type"] == market
    ]


async def record_socket(url, messages, frames, seconds):
    async with websockets.connect(url, max_size=32 * 1024 * 1024, open_timeout=20) as socket:
        for message in messages:
            await socket.send(json.dumps(message))
            await asyncio.sleep(0.3)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds
        while (left := deadline - loop.time()) > 0:
            try:
                text = await asyncio.wait_for(socket.recv(), left)
            except TimeoutError:
                break
            frames.append({"t": iso(), "endpoint": url.split("?", 1)[0], "text": text})


async def binance_snapshots(http, insts, frames):
    await asyncio.sleep(2)  # documented sync: buffer stream events, then fetch the snapshot
    for inst in insts:
        response = await http.get(
            "https://data-api.binance.vision/api/v3/depth",
            params={"symbol": inst["venue_symbol"], "limit": 1000},
        )
        response.raise_for_status()
        frames.append(
            {"t": iso(), "rest": "depth_snapshot", "instrument_id": inst["instrument_id"], "text": response.text}
        )


async def okx_contracts(http, insts):
    for inst in insts:
        if inst["market_type"] != "PERP_LINEAR":
            continue
        body = (
            await http.get(
                "https://www.okx.com/api/v5/public/instruments",
                params={"instType": "SWAP", "instId": inst["venue_symbol"]},
            )
        ).json()
        row = body["data"][0]
        inst["contract_multiplier"] = str(
            __import__("decimal").Decimal(row["ctVal"]) * __import__("decimal").Decimal(row.get("ctMult") or "1")
        )


async def record(name, venue, market, seconds, http):
    insts = instruments(venue, market)
    frames = []
    jobs = []
    if venue == "okx":
        await okx_contracts(http, insts)
    if venue == "binance" and market == "PERP_LINEAR":
        regular, _ = connection(insts[0])
        depth, _ = connection(insts[0], depth_only=True)
        for url, depth_only in ((regular, False), (depth, True)):
            root = url.split("?", 1)[0]
            subs = subscription_messages(insts, endpoint=root)
            jobs.append(record_socket(root, subs, frames, seconds))
    else:
        url, _ = connection(insts[0])
        root = url.split("?", 1)[0]
        messages = subscription_messages(insts, endpoint=root)
        if venue == "okx" and market == "PERP_LINEAR":
            messages.append({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]})
        jobs.append(record_socket(root, messages, frames, seconds))
        if venue == "binance":
            jobs.append(binance_snapshots(http, insts, frames))
    await asyncio.gather(*jobs)
    frames.sort(key=lambda f: f["t"])
    meta = {
        "fixture": name,
        "venue": venue,
        "market_type": market,
        "instruments": insts,
        "recorded_at": frames[0]["t"] if frames else iso(),
        "seconds": seconds,
        "source": "public WebSocket frames recorded by scripts/capture/record_frames.py",
    }
    OUT.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(meta, sort_keys=True)] + [json.dumps(f, sort_keys=True) for f in frames]
    (OUT / f"{name}.jsonl.gz").write_bytes(gzip.compress(("\n".join(lines) + "\n").encode(), mtime=0))
    print(name, len(frames), "frames")


async def main():
    seconds = float(sys.argv[1]) if len(sys.argv) > 1 else 150
    only = set(sys.argv[2:])
    async with httpx.AsyncClient(timeout=30) as http:
        await asyncio.gather(
            *(
                record(name, venue, market, seconds, http)
                for name, (venue, market) in PATHS.items()
                if not only or name in only
            )
        )


if __name__ == "__main__":
    asyncio.run(main())

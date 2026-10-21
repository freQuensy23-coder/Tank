"""Parity harness: recorded exchange frames through the Python reference.

The same fixture (capture/testdata/frames/*.jsonl.gz: real public frames) runs through
tank.market (route_indexed, process_frame, normalize, OrderBook, MarketService._put,
save_mark/_funding/save_ticker_observation, FlowAccumulator) with every clock driven
by the recorded receive times, and through tank-capture's Go ports (capture/internal/
parity). Both must produce identical normalized trades, typed rows, flow bars,
semantic fingerprints, stream watermarks and errors.

  --write          regenerate capture/testdata/parity/<fixture>.json.gz (digests)
  --check          Python output still matches the committed digests (CI)
  --go BINARY      run `BINARY <fixture>` (capture-parity) and diff full outputs
  --dump FIXTURE   print the full Python output of one fixture

Run: uv run python scripts/capture/parity.py --check [--go capture/bin/capture-parity]
"""

import argparse
import asyncio
import copy
import gzip
import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[2]
FRAMES = ROOT / "capture" / "testdata" / "frames"
EXPECTED = ROOT / "capture" / "testdata" / "parity"
CONFIG_VERSION = "parity-config"
CODE_VERSION = "parity-code"
USDT_RATE = "0.99950"


def load(path):
    lines = gzip.decompress(path.read_bytes()).decode().splitlines()
    return json.loads(lines[0]), [json.loads(line) for line in lines[1:] if line]


class Clock:
    def __init__(self, start):
        from tank.util import parse_time

        self.t0 = parse_time(start)
        self.t = self.t0

    def set(self, value):
        from tank.util import parse_time

        self.t = parse_time(value)

    def utcnow(self):
        return self.t

    def time(self):
        return self.t.timestamp()

    def monotonic(self):
        return (self.t - self.t0).total_seconds()


def digest(value):
    from tank.util import canonical_json

    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def instruments_of(meta):
    """Fixture instruments with the runtime fields a live service would hold."""
    out = []
    for inst in meta["instruments"]:
        inst = dict(inst)
        if inst.get("quote_asset") == "USDT":
            inst["quote_usd_rate"] = USDT_RATE
            inst["quote_usd_rate_as_of"] = meta["recorded_at"]
        if inst["market_type"] != "SPOT" and inst.get("funding_interval_h") is None:
            inst["funding_interval_h"] = "8"
        out.append(inst)
    return out


def setup_config(directory, instruments):
    registry = yaml.safe_load((ROOT / "config" / "registry.yaml").read_text())
    tokens = {i["token_id"] for i in instruments}
    config = Path(directory) / "config"
    config.mkdir()
    (config / "registry.yaml").write_text(
        yaml.safe_dump(
            {
                "tokens": [t for t in registry["tokens"] if t["token_id"] in tokens],
                "instruments": instruments,
            }
        )
    )
    for name in ("semantics.yaml", "thresholds.yaml"):
        (config / name).write_text((ROOT / "config" / name).read_text())
    return config


async def run(path):
    from tank.market import books as books_module
    from tank.market import normalize as norm
    from tank.market import service as service_module
    from tank.market import streams
    from tank.market.books import OrderBook
    from tank.market.flow import FlowAccumulator
    from tank.market.service import MarketService
    from tank.market_storage import encode, semantic_fingerprint
    from tank.settings import Settings
    from tank.storage import MemoryRepository
    from tank.util import iso, jsonable, parse_time

    meta, frames = load(path)
    instruments = instruments_of(meta)
    clock = Clock(meta["recorded_at"])
    out = {"trades": [], "rows": [], "bars": [], "fingerprints": {}, "watermarks": {}, "errors": []}
    with tempfile.TemporaryDirectory() as directory, mock.patch("tank.util.utcnow", clock.utcnow), mock.patch.object(
        service_module, "utcnow", clock.utcnow
    ), mock.patch.object(streams, "utcnow", clock.utcnow), mock.patch.object(
        service_module, "time", clock
    ), mock.patch.object(books_module, "time", clock), mock.patch.object(streams, "time", clock), mock.patch.dict(
        norm.QUOTE_USD_HISTORY, clear=True
    ):
        norm.remember_quote_usd("USDT", [(meta["recorded_at"], USDT_RATE)])
        settings = Settings(
            _env_file=None,
            config_dir=setup_config(directory, instruments),
            data_dir=Path(directory) / "data",
            replay=False,
            config_version=CONFIG_VERSION,
            code_version=CODE_VERSION,
        )
        async with httpx.AsyncClient() as http:
            service = MarketService(MemoryRepository(), settings, http=http)
        service._stop = asyncio.Event()
        service._market_restored = True
        service.instruments = {i["instrument_id"]: i for i in instruments}
        accumulator = FlowAccumulator()

        async def ingest(instrument, raw, *, backfill=False, row=None):
            row = norm.trade(instrument, raw, source="WS")
            out["trades"].append(row)
            accumulator.add(row, continuity=True, emit_record=False)
            return True

        original_put = service._put

        async def put(collection, key, row, *, immutable=False):
            before = service._pending_market.get(collection, {}).get(str(key))
            result = await original_put(collection, key, row, immutable=immutable)
            after = service._pending_market.get(collection, {}).get(str(key))
            if after is not None and after is not before:
                out["rows"].append({"collection": collection, "key": str(key), "row": jsonable(copy.deepcopy(after))})
            return result

        async def no_second_path(instrument, row):
            raise RuntimeError("PARITY_NO_SECOND_PATH")

        service.ingest_trade = ingest
        service._put = put
        service._independent_book = no_second_path
        venue = streams.venue_key(instruments[0]).split(".")[0]
        index = {streams.routing_symbol(i): i for i in instruments}
        # Binance spot frames wait for their REST depth snapshot (prepare_member). USD-M depth
        # has no reachable REST snapshot from the recording host: the levels of its first
        # events, merged until both sides hold a level, are the snapshot their last event
        # bridges (u == lastUpdateId), which then runs as the bridge event.
        waiting = {}
        merged = {}
        for inst in instruments:
            iid = inst["instrument_id"]
            service.books[iid] = OrderBook(inst)
            service._book_initial_bridge[iid] = True
            if venue == "binance" and inst["market_type"] == "SPOT":
                waiting[iid] = []
            elif venue == "binance":
                merged[iid] = ({}, {})

        async def process(number, instrument, payload):
            try:
                await streams.process_frame(service, instrument, payload)
            except Exception as exc:  # noqa: BLE001 - recorded for parity, then the member resets
                name = "SemanticError: " + str(exc) if isinstance(exc, norm.SemanticError) else "Exception"
                out["errors"].append([number, instrument["instrument_id"], name])
                service.books[instrument["instrument_id"]] = OrderBook(instrument)
                service._book_initial_bridge[instrument["instrument_id"]] = True
            await asyncio.gather(*list(service._background))

        for number, frame in enumerate(frames):
            clock.set(frame["t"])
            if frame.get("rest") == "depth_snapshot":
                inst = service.instruments[frame["instrument_id"]]
                snap = json.loads(frame["text"], parse_float=Decimal)
                service.books[inst["instrument_id"]].snapshot(
                    snap["bids"],
                    snap["asks"],
                    sequence=snap["lastUpdateId"],
                    ts=snap.get("E"),
                    ts_prec="EXACT" if snap.get("E") is not None else "COLLECTION_ONLY",
                )
                for queued_number, queued in waiting.pop(inst["instrument_id"], []):
                    await process(queued_number, inst, queued)
                continue
            if frame["text"] == "pong":
                continue
            raw = json.loads(frame["text"], parse_float=Decimal)
            if not isinstance(raw, dict) or raw.get("event") == "error" or raw.get("success") is False:
                continue
            for instrument, payload in streams.route_indexed(index, venue, raw):
                iid = instrument["instrument_id"]
                data = payload.get("data", payload)
                if iid in waiting:
                    waiting[iid].append((number, payload))
                    continue
                if iid in merged and isinstance(data, dict) and data.get("e") == "depthUpdate":
                    bids, asks = merged[iid]
                    for side, rows in ((bids, data["b"]), (asks, data["a"])):
                        for price, qty in rows:
                            if Decimal(qty):
                                side[price] = qty
                            else:
                                side.pop(price, None)
                    if not (bids and asks):
                        continue
                    service.books[iid].snapshot(
                        [[p, q] for p, q in bids.items()],
                        [[p, q] for p, q in asks.items()],
                        sequence=data["u"],
                        ts=data["E"],
                    )
                    merged.pop(iid)
                await process(number, instrument, payload)
        last = frames[-1]["t"] if frames else meta["recorded_at"]
        clock.set(last)
        out["watermarks"] = {iid: dict(mark) for iid, mark in sorted(service._stream_watermarks.items())}
        out["bars"] = flow_bars(accumulator, instruments, FlowAccumulator, parse_time, iso, timedelta)
        for item in out["rows"]:
            out["fingerprints"][item["collection"] + "|" + item["key"]] = semantic_fingerprint(
                encode(item["collection"], item["row"]), item["collection"]
            )
        for bar in out["bars"]:
            stored = await bar_row(service, original_put, bar)
            out["fingerprints"]["mkt.flow_bars_1m|" + bar["instrument_id"] + ":" + bar["minute_start"]] = (
                semantic_fingerprint(encode("mkt.flow_bars_1m", stored), "mkt.flow_bars_1m")
            )
    return out


async def bar_row(service, put, bar):
    """The row MarketService._put queues for a flow bar."""
    key = bar["instrument_id"] + ":" + bar["minute_start"]
    service._market_cache.get("mkt.flow_bars_1m", {}).pop(key, None)
    await put("mkt.flow_bars_1m", key, bar)
    return service._pending_market["mkt.flow_bars_1m"].pop(key)


def flow_bars(acc, instruments, FlowAccumulator, parse_time, iso, timedelta):  # noqa: N803
    """Bars with the same coverage proofs, empty minutes and boundaries on both sides."""
    out = []
    for inst in instruments:
        iid = inst["instrument_id"]
        minutes = sorted(parse_time(k[1]) for k in acc.bars if k[0] == iid)
        if not minutes:
            continue
        first, last = minutes[0], minutes[-1]
        acc.register_boundaries(
            iid,
            [
                first + timedelta(seconds=37, microseconds=500000),
                first + timedelta(minutes=1),
                last + timedelta(seconds=12, microseconds=345678),
            ],
        )
        acc.prove_range(iid, first + timedelta(seconds=15), last + timedelta(minutes=1), complete=True, source="PARITY")
        middle = first + timedelta(minutes=(len(minutes) // 2))
        acc.prove_range(
            iid,
            middle + timedelta(seconds=20),
            middle + timedelta(seconds=30),
            complete=False,
            source="PARITY",
            reason="STREAM_MEMBER_GAP",
        )
        cursor = first
        while cursor <= last:
            acc.empty_minute(inst, cursor)
            cursor += timedelta(minutes=1)
        for key in sorted(k for k in acc.bars if k[0] == iid):
            minute = parse_time(key[1])
            bar = acc.bars[key]
            complete = FlowAccumulator.covered(minute, minute + timedelta(minutes=1), bar.get("_coverage", []))
            out.append(acc.record(key, complete=complete))
    return out


def digests(out):
    return {
        "trades": [digest(t) for t in out["trades"]],
        "rows": [{"collection": r["collection"], "key": r["key"], "sha256": digest(r["row"])} for r in out["rows"]],
        "bars": [digest(b) for b in out["bars"]],
        "fingerprints": out["fingerprints"],
        "watermarks": out["watermarks"],
        "errors": out["errors"],
    }


def expected_path(fixture):
    return EXPECTED / (fixture.name.removesuffix(".jsonl.gz") + ".json.gz")


def fixtures():
    return sorted(FRAMES.glob("*.jsonl.gz"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--go")
    parser.add_argument("--dump")
    args = parser.parse_args()
    failures = 0
    for fixture in fixtures():
        if args.dump and fixture.name != args.dump and not fixture.name.startswith(args.dump):
            continue
        out = asyncio.run(run(fixture))
        if args.dump:
            print(json.dumps(out, indent=1, sort_keys=True))
            continue
        summary = digests(out)
        if args.write:
            EXPECTED.mkdir(parents=True, exist_ok=True)
            text = json.dumps(summary, sort_keys=True, indent=1) + "\n"
            expected_path(fixture).write_bytes(gzip.compress(text.encode(), mtime=0))
        if args.check:
            committed = json.loads(gzip.decompress(expected_path(fixture).read_bytes()))
            if committed != json.loads(json.dumps(summary)):
                failures += 1
                print("PYTHON DRIFT", fixture.name, file=sys.stderr)
        if args.go:
            go = json.loads(subprocess.run([args.go, str(fixture)], check=True, capture_output=True).stdout)
            mismatch = compare(out, go)
            if mismatch:
                failures += 1
                print("GO MISMATCH", fixture.name, mismatch, file=sys.stderr)
        print(
            fixture.name,
            {k: len(v) for k, v in out.items()},
            "ok" if not failures else "",
        )
    sys.exit(1 if failures else 0)


def compare(python, go):
    python = json.loads(json.dumps(python))
    for section in ("trades", "rows", "bars", "errors"):
        left, right = python[section], go.get(section, [])
        if len(left) != len(right):
            return f"{section}: {len(left)} python vs {len(right)} go"
        for i, (a, b) in enumerate(zip(left, right, strict=True)):
            if a != b:
                return f"{section}[{i}]:\npython {json.dumps(a, sort_keys=True)}\ngo     {json.dumps(b, sort_keys=True)}"
    for section in ("fingerprints", "watermarks"):
        if python[section] != go.get(section):
            return f"{section} differ"
    return None


if __name__ == "__main__":
    main()

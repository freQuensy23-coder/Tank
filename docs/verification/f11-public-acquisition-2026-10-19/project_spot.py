"""Data-only historical projection through unchanged production primitives; no DB or models."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import yaml

from tank.market.archive import TradeArchive
from tank.market.flow import FlowAccumulator
from tank.market.normalize import trade
from tank.util import parse_time

P = Path(__file__).parent
registry = yaml.safe_load(Path("config/registry.yaml").read_text())["instruments"]
insts = {x["venue"]: x for x in registry if x["token_id"] == "world:WLD" and x["market_type"] == "SPOT"}
start = parse_time("2026-10-16T08:30:00Z")
end = parse_time("2026-10-16T12:30:00Z")
receipts = []
for f in ["receipts.json", "extended-receipts.json", "resume-receipts.json"]:
    receipts += json.loads((P / f).read_text())
byname = {r["name"]: r for r in receipts}
manifests = []
bars = []
summaries = {}
bindings = {}
archive = TradeArchive(P / "rest-spot-projection", instruments=list(insts.values()))
for venue in ["binance", "okx", "coinbase", "kraken"]:
    inst = insts[venue]
    rows = {}
    used = []
    allraw = []
    if venue == "binance":
        for f in sorted(
            (P / "production-import/raw/trades/binance/spot/WLDUSDT").glob("2026-10-*/*.index.json")
        ):
            idx = json.loads(f.read_text())
            path = idx["sealed_path"]
            sha = idx["sealed_sha256"]
            decoded = TradeArchive.read_verified(path, sha)
            used.append(
                {
                    "path": path,
                    "sha256": sha,
                    "index": str(f),
                    "index_sha256": hashlib.sha256(f.read_bytes()).hexdigest(),
                }
            )
            for y in decoded:
                rows[str(y["trade_id"])] = y
    else:
        names = sorted(n for n in byname if n.startswith(venue + "-history-"))
        if venue == "kraken":
            names = ["kraken-probe", *names]
        for name in names:
            r = byname[name]
            body = (P / r["path"]).read_bytes()
            assert hashlib.sha256(body).hexdigest() == r["sha256"]
            j = json.loads(body)
            raw = j["data"] if venue == "okx" else j["result"]["WLDUSD"] if venue == "kraken" else j
            used.append(
                {
                    "path": r["path"],
                    "sha256": r["sha256"],
                    "received_at": r["received_at"],
                    "request": r["params"],
                }
            )
            for x in raw:
                y = trade(inst, x, received_ts=r["received_at"], source="REST")
                tid = str(y["trade_id"])
                prior = rows.get(tid)
                if prior:
                    assert all(prior[k] == y[k] for k in ["ts", "price", "qty_native", "side"])
                rows[tid] = y
    ordered = sorted(rows.values(), key=lambda x: (parse_time(x["ts"]), int(x["trade_id"])))
    ids = sorted(int(x["trade_id"]) for x in ordered)
    gaps = [(a, b) for a, b in zip(ids, ids[1:], strict=False) if b != a + 1]
    first = parse_time(ordered[0]["ts"])
    last = parse_time(ordered[-1]["ts"])
    full = (
        (not gaps and first <= start and last >= end)
        if venue == "coinbase"
        else (not gaps and first <= start)
        if venue == "okx"
        else (not gaps and last >= end)
        if venue == "kraken"
        else True
    )
    # OKX request after=end proves upper exclusion; Kraken request since=start proves lower bound.
    assert full, (venue, len(ordered), first, last, len(gaps))
    selected = [x for x in ordered if start <= parse_time(x["ts"]) < end]
    flow = FlowAccumulator()
    for y in selected:
        flow.add(y)
        if venue != "binance":
            archive.append(y)
    minute = start
    while minute < end:
        flow.empty_minute(inst, minute)
        minute += timedelta(minutes=1)
    received = max((r.get("received_at", "") for r in used), default="") or datetime.now(UTC).isoformat()
    flow.prove_range(
        inst["instrument_id"],
        start,
        end,
        complete=True,
        source="VERIFIED_BINANCE_DAILY_ARCHIVE"
        if venue == "binance"
        else "ACTUAL_REST_CURSOR_CHAIN_AND_BOUNDARY_REQUEST",
        observed_at=received,
    )
    for key in sorted(flow.bars):
        bars.append(flow.record(key, complete=True))
    if venue != "binance":
        archive.coverage(
            inst["instrument_id"],
            start,
            end,
            complete=True,
            source="ACTUAL_REST_CURSOR_CHAIN_AND_BOUNDARY_REQUEST",
            observed_at=received,
        )
    windows = {}
    for h in [1, 4]:
        subset = [x for x in selected if parse_time(x["ts"]) >= end - timedelta(hours=h)]
        buy = sum((Decimal(x["qty_native"]) for x in subset if x["side"] == "BUY"), Decimal(0))
        sell = sum((Decimal(x["qty_native"]) for x in subset if x["side"] == "SELL"), Decimal(0))
        q2 = sum((Decimal(x["qty_native"]) ** 2 for x in subset), Decimal(0))
        windows[str(h) + "h"] = {
            "trade_count": len(subset),
            "buy_native": str(buy),
            "sell_native": str(sell),
            "cvd_native": str(buy - sell),
            "sum_qty_native_sq": str(q2),
            "buy_share": str(buy / (buy + sell)) if buy + sell else None,
        }
    summaries[venue] = {
        "instrument_id": inst["instrument_id"],
        "raw_unique_rows": len(ordered),
        "raw_first_event": ordered[0]["ts"],
        "raw_last_event": ordered[-1]["ts"],
        "id_gap_count": len(gaps),
        "window_complete": True,
        "window": {"start": start.isoformat(), "end_exclusive": end.isoformat()},
        "windows": windows,
        "proof_scope": "Original API cursor chain plus documented temporal request boundaries; archive SHA+IDs for Binance. No live latency claim.",
        "USD_policy": "USDT not converted without actual historical FX; USD quote uses actual prices, no substitute peg.",
    }
    bindings[venue] = used
manifests = archive.flush(seal_before=end + timedelta(hours=1))
(P / "rest-spot-parquet-manifests.json").write_text(json.dumps(manifests, indent=2) + "\n")
(P / "spot-flowbars-production.json").write_text(
    json.dumps(
        {
            "representation": "HISTORICAL_RECONSTRUCTION",
            "original_capture_receipts": "receipts.json + extended-receipts.json + resume-receipts.json",
            "rows": bars,
        },
        indent=2,
    )
    + "\n"
)
(P / "spot-projection-report.json").write_text(
    json.dumps(
        {
            "status": "PRODUCTION_PRIMITIVE_PROJECTION_NOT_FULL_MARKETTRUTH_ACCEPTANCE",
            "venues": summaries,
            "raw_bindings": bindings,
            "production_source_hashes": {
                f: hashlib.sha256(Path(f).read_bytes()).hexdigest()
                for f in [
                    "src/tank/market/normalize.py",
                    "src/tank/market/flow.py",
                    "src/tank/market/archive.py",
                ]
            },
            "no_SQL_or_model": True,
            "remaining": "24h cross-venue baseline, verified semantic/USD scope and absent historical books/liquidations remain explicit; no labels calibrated.",
        },
        indent=2,
    )
    + "\n"
)
print(json.dumps(summaries, indent=2))

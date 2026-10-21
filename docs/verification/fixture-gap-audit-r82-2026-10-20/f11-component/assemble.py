"""Offline successor from previously captured F11 observations; no product changes."""

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
OUT = Path(__file__).resolve().parent
BASE = ROOT / "data/replay/candidates/F11-market-only-partial-20261019"
CAP = ROOT / "docs/verification/f11-public-acquisition-2026-10-19"
POINT = ROOT / "data/replay/candidates/F11-bybit-point-successor-20261019"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def verify(path, expected):
    assert path.is_file(), path
    actual = digest(path)
    assert actual == expected, (str(path), expected, actual)
    return {"path": str(path.relative_to(ROOT)), "sha256": actual}


base_manifest = read(BASE / "manifest.json")
base_path = ROOT / base_manifest["packet_path"]
verify(base_path, base_manifest["packet_sha256"])
base = read(base_path)
snapshot = deepcopy(base["records"][0]["record"])
cutoff = snapshot["cutoff"]
assert cutoff == "2026-10-16T12:30:00Z" and snapshot["anchor"] is None

status = read(CAP / "acquisition-status.json")
flow_path = CAP / "spot-flowbars-production.json"
bybit_path = CAP / "bybit-oi-normalized-successor.json"
verify(flow_path, status["artifact_SHA256"][flow_path.name])
verify(bybit_path, status["artifact_SHA256"][bybit_path.name])
flow = read(flow_path)
bybit = read(bybit_path)
raw_evidence = []
for binding in bybit["raw_bindings"]:
    path = CAP / binding["path"]
    raw_evidence.append(verify(path, binding["sha256"]))
    raw_rows = {str(row["timestamp"]): row for row in read(path)["result"]["list"]}
    for row in bybit["rows"]:
        if row["raw_capture_sha256"] != binding["sha256"]:
            continue
        millis = str(int(datetime.fromisoformat(row["ts"].replace("Z", "+00:00")).timestamp()) * 1000)
        raw = raw_rows[millis]
        assert Decimal(row["oi_native"]) == Decimal(raw["singleOpenInterest"])
        assert row["received_ts"].replace("Z", "+00:00") == datetime.fromisoformat(
            binding["received_at"]
        ).isoformat().replace("+00:00", "+00:00")

point_manifest = read(POINT / "manifest.json")
point_path = ROOT / point_manifest["normalized_path"]
verify(point_path, point_manifest["normalized_sha256"])
point_raw = ROOT / point_manifest["input_source"]["path"]
raw_evidence.append(verify(point_raw, point_manifest["input_source"]["sha256"]))
point_rows = read(point_path)
point_end = next(row for row in point_rows if row["ts"] == cutoff)
assert point_end["oi_native"] == "86777506"
raw_endpoint = next(
    row for row in read(point_raw)["result"]["list"] if str(row["timestamp"]) == "1792153800000"
)
assert Decimal(raw_endpoint["singleOpenInterest"]) == Decimal(point_end["oi_native"])

# Prefer the later capture for overlapping history only after exact numerical
# agreement. The actual 12:30 endpoint has its own earlier HTTP receipt.
oi = {(row["instrument_id"], row["ts"]): deepcopy(row) for row in snapshot["oi"]}
shared = 0
for row in bybit["rows"]:
    assert row["ts"] <= cutoff
    key = (row["instrument_id"], row["ts"])
    if key in oi:
        assert Decimal(oi[key]["oi_native"]) == Decimal(row["oi_native"])
        shared += 1
    oi[key] = deepcopy(row)
oi[(point_end["instrument_id"], point_end["ts"])] = deepcopy(point_end)
snapshot["oi"] = sorted(oi.values(), key=lambda row: (row["instrument_id"], row["ts"]))
snapshot["flow_bars"] = deepcopy(flow["rows"])
assert len(snapshot["flow_bars"]) == 960
assert all("2026-10-16T08:30:00Z" <= row["minute_start"] < cutoff for row in snapshot["flow_bars"])
assert all(row["complete"] is True for row in snapshot["flow_bars"])
assert {row["venue"] for row in snapshot["flow_bars"]} == {"binance", "okx", "coinbase", "kraken"}
assert not snapshot["books"] and not snapshot["tickers"]

assembled = datetime.now(UTC).isoformat().replace("+00:00", "Z")
snapshot["assembled_at"] = assembled
snapshot["assembled_at_prec"] = "COLLECTION_ONLY"
snapshot["assembled_at_prec_reason"] = None
snapshot["missing_inputs"]["flow_bars"] = (
    "BYBIT_SPOT_UNAVAILABLE; CROSS_VENUE_24H_REFERENCE_COVERAGE_UNAVAILABLE"
)
snapshot["provenance"]["successor"] = {
    "assembled_at": assembled,
    "base_assembled_at": base["records"][0]["record"]["assembled_at"],
    "base_packet": str(base_path.relative_to(ROOT)),
    "base_packet_sha256": digest(base_path),
    "flow_input": str(flow_path.relative_to(ROOT)),
    "flow_input_sha256": digest(flow_path),
    "bybit_history_input": str(bybit_path.relative_to(ROOT)),
    "bybit_history_input_sha256": digest(bybit_path),
    "bybit_endpoint_input": str(point_path.relative_to(ROOT)),
    "bybit_endpoint_input_sha256": digest(point_path),
    "historical_live_availability": False,
    "observation_values_and_receipt_times_changed": False,
}

source_files = [
    base_path,
    BASE / "manifest.json",
    flow_path,
    bybit_path,
    CAP / "spot-projection-report.json",
    CAP / "acquisition-status.json",
    CAP / "f11-raw-expected-discrepancy-audit.json",
    POINT / "manifest.json",
    point_path,
    CAP / "receipts.json",
    CAP / "extended-receipts.json",
    CAP / "resume-receipts.json",
]
projection = read(CAP / "spot-projection-report.json")
spot_raw = []
for venue, bindings in projection["raw_bindings"].items():
    for binding in bindings:
        path = Path(binding["path"])
        if not path.is_absolute():
            path = CAP / path
        spot_raw.append(dict(venue=venue, **verify(path, binding["sha256"])))

path = OUT / "snapshot.json"
path.write_text(json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n")
manifest = {
    "artifact_kind": "HOST_ONLY_AUTHENTIC_MARKET_SUCCESSOR_NOT_ACCEPTANCE",
    "snapshot_path": str(path.relative_to(ROOT)),
    "snapshot_sha256": digest(path),
    "target_token": snapshot["token_id"],
    "cutoff": cutoff,
    "anchor": snapshot["anchor"],
    "assembled_at": assembled,
    "preserved": [
        "target/cutoff/anchor",
        "registry context/instruments/tokens",
        "original observation values and received_ts",
        "semantic labels and missing market context",
        "no news/NEW_ITEM/MAJOR/catalyst fabricated",
    ],
    "counts": {
        "flow_bars": len(snapshot["flow_bars"]),
        "oi": len(snapshot["oi"]),
        "bybit_history": len(bybit["rows"]),
        "additional_bybit_endpoint": 1,
        "overlapping_bybit_rows_checked_equal": shared,
    },
    "sources": [{"path": str(p.relative_to(ROOT)), "sha256": digest(p)} for p in source_files],
    "raw_bybit_bindings": raw_evidence,
    "raw_spot_bindings": spot_raw,
    "authority_caveats": [
        "Historical acquisition on Oct19; no contemporaneous availability or live latency claim.",
        "Bybit9day capture ends12:25; separate SHA-bound capture supplies actual12:30 endpoint.",
        "Earlier Bybit unit/timestamp audit did not qualify historical contemporaneous mark/FX; do not use current ticker prices.",
        "No fabricated USD for USDT, Bybit historical OI USD or mark/FX endpoint alignment.",
        "No Bybit spot trades, historical depth/books, full cross-venue24h ranking/tickers, or funding added.",
        "Binance event OI retains actual REST authority; no archive endpoint substituted.",
        "Current configured Bybit point semantics retained; old BUCKET/UNSPECIFIED evidence remains unchanged.",
    ],
    "genuine_disagreement": {
        "four_venue_4h_cvd_native": "186034.84410000",
        "four_venue_1h_cvd_native": "-287242.07507000",
        "direction": "NEUTRAL",
        "old_assertion": "SELL / -1.73M4h",
        "no_rows_side_or_thresholds_fitted": True,
    },
    "external_model_native_database_calls": 0,
}
(OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps({k: manifest[k] for k in ["snapshot_path", "snapshot_sha256", "counts"]}, indent=2))

"""Independent retained-wire arithmetic, comparing production primitive outputs. No network."""

import csv
import hashlib
import html
import io
import json
import re
import zipfile
from datetime import datetime
from decimal import Decimal as D
from pathlib import Path

from tank.market.archive import TradeArchive

P = Path(__file__).parent
start = D("1792139400")
end = D("1792153800")
receipts = []
for n in ["receipts.json", "extended-receipts.json", "resume-receipts.json"]:
    receipts += json.load(open(P / n))
byname = {r["name"]: r for r in receipts}
raw = {v: {} for v in ["binance", "okx", "coinbase", "kraken"]}
used = {v: [] for v in raw}
for day in ["15", "16"]:
    rec = byname["binance-" + day + "-zip"]
    body = (P / rec["path"]).read_bytes()
    assert hashlib.sha256(body).hexdigest() == rec["sha256"]
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        for row in csv.reader(io.TextIOWrapper(z.open(z.namelist()[0]))):
            tid, price, qty, first, last, us, maker = row[:7]
            t = D(us) / D(1000000)
            raw["binance"][tid] = (t, D(qty), -1 if maker.lower() == "true" else 1, {"m": maker, "T": us})
    used["binance"].append(rec["path"])
for venue in ["okx", "coinbase", "kraken"]:
    names = sorted(n for n in byname if n.startswith(venue + "-history-"))
    if venue == "kraken":
        names = ["kraken-probe"] + names
    for n in names:
        rec = byname[n]
        body = (P / rec["path"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == rec["sha256"]
        j = json.loads(body, parse_float=D)
        rs = j["data"] if venue == "okx" else j["result"]["WLDUSD"] if venue == "kraken" else j
        for r in rs:
            if venue == "okx":
                tid = str(r["tradeId"])
                t = D(r["ts"]) / 1000
                q = D(r["sz"])
                sgn = 1 if r["side"] == "buy" else -1
            elif venue == "coinbase":
                tid = str(r["trade_id"])
                t = D(str(datetime.fromisoformat(r["time"].replace("Z", "+00:00")).timestamp()))
                q = D(r["size"])
                sgn = 1 if r["side"] == "sell" else -1
            else:
                tid = str(r[6])
                t = D(r[2])
                q = D(r[1])
                sgn = 1 if r[3] == "b" else -1
            if tid in raw[venue]:
                assert raw[venue][tid][:3] == (t, q, sgn)
            raw[venue][tid] = (t, q, sgn, r)
        used[venue].append(rec["path"])
projection = json.load(open(P / "spot-projection-report.json"))
independent = {}
for venue, rows in raw.items():
    independent[venue] = {}
    for hours in [1, 4]:
        selected = [v for v in rows.values() if end - hours * 3600 <= v[0] < end]
        cvd = sum((q * s for t, q, s, r in selected), D(0))
        vol = sum((q for t, q, s, r in selected), D(0))
        q2 = sum((q * q for t, q, s, r in selected), D(0))
        expected = projection["venues"][venue]["windows"][str(hours) + "h"]
        assert (
            cvd == D(expected["cvd_native"])
            and q2 == D(expected["sum_qty_native_sq"])
            and len(selected) == expected["trade_count"]
        )
        independent[venue][str(hours) + "h"] = {
            "trade_count": len(selected),
            "native_volume": str(vol),
            "cvd_native": str(cvd),
            "sum_qty_sq": str(q2),
            "all_timestamps_in_half_open_window": True,
        }
# Verify exactly one transformation from retained original maker side to persisted taker side.
checks = {
    "coinbase_rows": 0,
    "coinbase_wrong_flip_count": 0,
    "kraken_rows": 0,
    "kraken_quantity_mismatch_count": 0,
}
for m in json.load(open(P / "rest-spot-parquet-manifests.json")):
    venue = m["instrument_id"].split(":")[0]
    if venue not in {"coinbase", "kraken"}:
        continue
    for r in TradeArchive.read_verified(m["path"], m["sha256"]):
        original = raw[venue][str(r["trade_id"])]
        t, q, sgn, wire = original
        assert start <= t < end
        if venue == "coinbase":
            checks["coinbase_rows"] += 1
            checks["coinbase_wrong_flip_count"] += r["side"] != ("BUY" if wire["side"] == "sell" else "SELL")
            assert json.loads(r["source_payload_json"]) == wire
        else:
            checks["kraken_rows"] += 1
            checks["kraken_quantity_mismatch_count"] += D(r["qty_native"]) != q
assert not checks["coinbase_wrong_flip_count"] and not checks["kraken_quantity_mismatch_count"]
spot = json.load(open(P / "actual-spot-truth.host-only.json"))
official = re.sub(r"\s+", " ", html.unescape(re.sub("<[^>]+>", " ", (P / "coinbase-docs.body").read_text())))
pos = official.index("The side of a trade indicates the maker order side.")
docquote = official[pos : pos + 333]
cvd4 = sum((D(v["4h"]["cvd_native"]) for v in independent.values()), D(0))
cb = D(independent["coinbase"]["4h"]["cvd_native"])
counter = cvd4 - 2 * cb
out = {
    "status": "CONFIRMED_REFERENCE_DISCREPANCY_NO_CAUSAL_CODE_BUG_FOUND",
    "fixed_window": {
        "start_inclusive": "2026-10-16T08:30:00Z",
        "end_exclusive": "2026-10-16T12:30:00Z",
        "epoch_seconds": [str(start), str(end)],
        "timezone": "UTC",
    },
    "independent_saved_wire_arithmetic": independent,
    "producer_transport_checks": checks,
    "coinbase_side_contract": {
        "url": "https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades",
        "saved_body_sha256": hashlib.sha256((P / "coinbase-docs.body").read_bytes()).hexdigest(),
        "literal_excerpt": docquote,
        "conclusion": "REST payload is original maker-side response; production does exactly one maker→taker inversion. No pre-transformed raw input or double flip detected.",
    },
    "kraken_units": {
        "source_contract": "TDD5.3 base qty and b=BUY; current config metric kraken.spot.trade BASE_QTY/TAKER_SIDE",
        "independent_check": "All persisted selected quantities equal raw REST tuple volume[1], without price multiplication/division. Documented tuple position and request since cursor retained.",
        "limitation": "Saved REST endpoint documentation calls field volume and does not alone newly verify BASE_QTY/taker semantics; confidence remains unchanged DOCUMENTED_UNTESTED. No new semantic verification claim.",
    },
    "actual_current_spot_truth": {
        k: {
            "direction": v["direction"],
            "classification": v["classification"],
            "composite_max": v["composite_max"],
            "coverage": v["coverage"],
            "breadth": v["breadth"],
            "cw_breadth": v["cw_breadth"],
        }
        for k, v in spot.items()
    },
    "counterfactual_not_adopted": {
        "purpose": "Diagnose whether failing to flip Coinbase alone explains old quote",
        "coinbase_maker_side_used_as_taker_4h_CVD": str(counter),
        "PRD_4h_CVD": "-1730000",
        "conclusion": "Incorrect Coinbase side alone does not reproduce -1.73M; no evidence to attribute old result solely to this error.",
    },
    "old_expected_evidence": {
        "prd": "01_Tank_V0_PRD.md:386-395 quotes Addendum002-B: -1.73M4h/-1.06M1h/buyshare45.7%/Kraken~2.5%.",
        "tdd": "03_Tank_V0_Technical_Design.md:1634 requires1h/4hSELL;1686 requires absCVD>=2sqrtSumQ2.",
        "fixture_scripts": "fixture_check/scripts/compute.py:113-132 computes OI and a separate Binance hourly08:00→12:00 volume alternative, not spot CVD. No retained original Addendum002-B per-venue trade script/CSV found in fixture_check.",
        "causal_limit": "Cannot establish old wrong side/window without original raw/script. The current wrong-side counterfactual and hourly alternative do not themselves prove the cause.",
    },
    "preservation": "Original expected references, rawcaptures, producerprojection and actualspot_truth outputs preserved; no thresholds/assertions/production edits.",
}
(P / "f11-raw-expected-discrepancy-audit.json").write_text(json.dumps(out, indent=2) + "\n")
print(
    json.dumps(
        {
            "status": out["status"],
            "checks": checks,
            "sum4h": str(cvd4),
            "wrong_side_counterfactual": str(counter),
        }
    )
)

"""Read-only rerun of retained market components; never historical acceptance."""

import asyncio
import hashlib
import json
import time
from pathlib import Path
from types import SimpleNamespace

from tank.analysis.truth import compute_market_truth
from tank.replay import ReplayMarketReader, load_inputs
from tank.util import jsonable

ROOT = Path.cwd()
OUT = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


async def main():
    code = ["src/tank/analysis/truth.py", "src/tank/replay.py", "src/tank/analysis/decisions.py"]
    result = {
        "scope": "OFFLINE_CURRENT_CODE_MARKET_COMPONENT_RECHECK_NOT_FULL_FIXTURE_ACCEPTANCE",
        "code_hashes": {p: sha(p) for p in code},
        "inputs_modified": False,
        "source_timestamps_modified": False,
        "news_event_beneficiary_or_catalyst_seeded": False,
        "external_model_native_telegram_x_calls": 0,
        "cases": [],
    }
    for number in range(12, 17):
        started = time.monotonic()
        fid = f"F-{number:02}"
        mp = ROOT / "data/replay/candidates/pure-market-retained-20261019" / fid / "manifest.json"
        manifest = json.loads(mp.read_text())
        path = ROOT / manifest["input_path"]
        assert sha(path) == manifest["sha256"]
        verified_sources = 0
        for source in manifest["sources"]:
            if source.get("path"):
                assert sha(ROOT / source["path"]) == source["sha256"]
                verified_sources += 1
        packet = json.loads(path.read_text())
        settings = SimpleNamespace(config_version=packet["config_version"], code_version=packet["code_version"])
        inputs = load_inputs(path, settings)
        token = packet["records"][0]["record"]["token_id"]
        snapshot = await ReplayMarketReader(inputs).snapshot(token, inputs.cutoff)
        truth = compute_market_truth(snapshot, config=inputs.config["thresholds"])
        point = {
            "fixture_id": fid,
            "input": {"path": str(path.relative_to(ROOT)), "sha256": sha(path)},
            "manifest": {"path": str(mp.relative_to(ROOT)), "sha256": sha(mp)},
            "source_file_hashes_verified": verified_sources,
            "cutoff": snapshot["cutoff"],
            "row_counts": {k: len(snapshot.get(k) or []) for k in ["klines", "oi", "flow_bars", "books"]},
            "move_class": truth["move_class"],
            "data_confidence": truth["data_confidence"]["class"],
            "market_context": truth["relative"].get("market_context"),
            "weak_response": truth["liquidity"]["elasticity"].get("weak_response"),
            "oi_per_venue": [
                {k: row.get(k) for k in ["instrument_id", "oi_class", "oi_native_chg_pct", "oi_usd_chg_pct", "reason"]}
                for row in (truth.get("derivatives") or {}).get("per_venue", [])
            ],
            "seconds": round(time.monotonic() - started, 3),
        }
        (OUT / f"{fid}.current-truth.json").write_text(json.dumps(jsonable(truth), indent=2) + "\n")
        result["cases"].append(point)
        print(json.dumps(point), flush=True)
        del snapshot, truth, packet, inputs
    (OUT / "current-components.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    asyncio.run(main())

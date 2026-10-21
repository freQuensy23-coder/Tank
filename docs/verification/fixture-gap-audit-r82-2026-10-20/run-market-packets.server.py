"""Test-only execution through existing offline production replay helpers."""
import asyncio
import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import compute_market_truth
from tank.replay import ReplayMarketReader, replay_bundle
from tank.settings import _capture_tree, _digest
from tank.storage import MemoryRepository
from tank.util import iso, jsonable

ROOT = Path("/srv/tank")
HERE = Path(__file__).resolve().parent
PACKETS = HERE / "server-market-packets-v2"
RUN = HERE / "server-market-run-v1"
spec = importlib.util.spec_from_file_location("qa_prepared", HERE / "prepare-market-packets.server-v2.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
sys.path.insert(0, str(ROOT / "tests"))
from fixture_acceptance import assertion_passes, check_completed, check_ready

async def runner(inputs, repo):
    token = next(row["record"]["token_id"] for row in inputs.records if row["collection"] == "replay.market_snapshots")
    snapshot = await ReplayMarketReader(inputs).snapshot(token, inputs.cutoff)
    truth = compute_market_truth(snapshot, config=inputs.config["thresholds"])
    decision = DecisionEngine().evaluate({}, {}, {"token_id": token, "role": "UNKNOWN"}, truth)
    return {
        "scope": "PRODUCTION_MARKET_VALIDATION_EMPTY_ECONOMIC_PREMISES_NO_NEWS_OR_NATIVE",
        "market_truth": [truth],
        "decisions": [decision],
        "observations": {"price_markup_oi_flag": "PRICE_MARKUP_OI" in truth.get("flags", [])},
        "source_rows": {key: len(snapshot.get(key) or []) for key in ["klines", "oi", "flow_bars", "books"]},
        "market_wording": None,
        "wording_phase": "PLANNED_NOT_RUN",
        "policy_controls": None,
        "policy_scope": "Separate disclosed synthetic controls; no historical business facts seeded",
    }

async def main():
    RUN.mkdir(exist_ok=False)
    prepared = helper.read_json(PACKETS / "index.json")
    summary = []
    for record in prepared["prepared"]:
        case = record["case"]
        disk = os.statvfs(ROOT)
        assert disk.f_bavail * disk.f_frsize >= 8 * 1024**3, "SERVER_FREE_SPACE_FLOOR"
        errors = check_ready(ROOT, case)
        assert not errors, (case["fixture_id"], errors)
        path = ROOT / case["acceptance_input"]["path"]
        packet = helper.read_json(path)
        current_code = _digest(_capture_tree(ROOT / "src/tank", {".py", ".sql", ".json"}))
        assert packet["code_version"] == current_code, "SOURCE_CODE_CHANGED_AFTER_PREPARATION"
        settings = SimpleNamespace(code_version=packet["code_version"], config_version=packet["config_version"])
        folder = RUN / case["fixture_id"]
        folder.mkdir()
        temporary = folder / "runtime-input.json"
        # Existing loader needs plain JSON. This is one lossless temporary
        # representation of the exact prepared compressed input, not a new mode.
        with path.open("rb") as raw, helper.zstandard.ZstdDecompressor().stream_reader(raw) as plain, temporary.open("wb") as dest:
            import shutil
            shutil.copyfileobj(plain, dest)
        del packet
        repo = MemoryRepository()
        started = iso()
        try:
            result = await replay_bundle(temporary, repo, settings, runner=runner, output_dir=folder)
        finally:
            await repo.close()
        temporary.unlink()
        input_copy = folder / "inputs.json"
        helper.compress_verified(input_copy)
        result["status"] = "COMPLETED"
        frozen = folder / "frozen-output.json"
        frozen.write_text(json.dumps(jsonable(result), separators=(",", ":")) + "\n")
        assert frozen.is_file()
        # Expectations are opened only after the actual execution output froze.
        seal = helper.read_json(ROOT / case["sealed_outcomes"]["path"])
        checks = [{"assertion": assertion, "passed": assertion_passes(result["output"], assertion)} for assertion in seal["assertions"]]
        manual = {
            "input_sha256": seal["input_sha256"],
            "frozen_output_sha256": helper.reference(frozen)["sha256"],
            "checks": [{
                "requirement": row["requirement"],
                "passed": False,
                "reason": "Full literal wording/policy review is pending. Production formula predicates are separate; no native/full acceptance claim.",
                "evidence": [helper.reference(frozen)],
            } for row in seal["manual_predicates"]],
        }
        manual_path = folder / "manual-review.pending.json"
        manual_path.write_text(json.dumps(manual, ensure_ascii=False, indent=2) + "\n")
        receipt = {"started_at": started, "frozen_output": helper.reference(frozen), "manual_review": helper.reference(manual_path),
                   "scope": "Completed formula phase only; native wording and full literal acceptance pending"}
        receipt_path = folder / "run.json"
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
        evaluated = {**case, "acceptance_run": helper.reference(receipt_path)}
        row = {
            "fixture_id": case["fixture_id"],
            "ready": True,
            "formula_phase": "COMPLETED",
            "automated_checks": checks,
            "automated_passed": all(check["passed"] for check in checks),
            "full_completed_errors": check_completed(ROOT, evaluated),
            "case": evaluated,
            "economic_context": "EMPTY_UNINVENTED",
            "native_wording_phase": "PLANNED_NOT_RUN",
            "canonical_adopted": False,
        }
        (folder / "evaluation.json").write_text(json.dumps(row, ensure_ascii=False, indent=2) + "\n")
        summary.append(row)
        print(case["fixture_id"], "COMPLETED", "automated_passed=" + str(row["automated_passed"]), flush=True)
        del result
    (RUN / "summary.json").write_text(json.dumps({"scope": "TEST_ONLY_AUTHENTIC_MARKET_PACKETS_NO_PRODUCT_INTEGRATION", "cases": summary}, ensure_ascii=False, indent=2) + "\n")

if __name__ == "__main__":
    asyncio.run(main())

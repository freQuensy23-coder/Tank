"""Read-only independent all-27 portfolio ledger; no canonical adoption."""

import hashlib
import importlib.util
import json
import shutil
from pathlib import Path

ROOT = Path("/srv/tank")
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("acceptance", ROOT / "tests/fixture_acceptance.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    canonical = ROOT / "tests/fixtures/manifest.json"
    original_sha = sha(canonical)
    projections = {}
    old_root = "/Users/a.mametyev/Downloads/Tank_V0_MVP_Docs/"

    def portable_resolve(root, value):
        text = str(value)
        if text.startswith(old_root):
            path = Path(root) / text[len(old_root) :]
        elif text.startswith("/data/"):
            path = Path(root) / text.lstrip("/")
        else:
            path = Path(text)
            if not path.is_absolute():
                path = Path(root) / path
        if str(path) != text:
            projections[text] = str(path)
        return path

    gate.resolve = portable_resolve
    assert original_sha == "980c3836a1cd12106376cf3615e0339b47a7c916e3b9906df1cf63def93256ee"
    portfolio = json.loads((HERE / "portfolio.candidate.json").read_text())
    private = json.loads((HERE / "news-private-reference-audit.json").read_text())
    contexts = {x["fixture_id"]: x for x in private["current_contexts"]}
    old = {x["fixture_id"]: x for x in json.loads((HERE / "current-gate-audit.json").read_text())}
    formulas = {
        x["fixture_id"]: x
        for x in json.loads((HERE / "server-market-run-v2/summary.json").read_text())["cases"]
    }
    formulas["F-11"] = json.loads(
        (HERE / "server-market-run-f11-baseline-v3/F-11/evaluation.json").read_text()
    )
    ready_original = gate.check_ready
    ready_capture = {}

    def capture_ready(root, case):
        result = ready_original(root, case)
        ready_capture[case["fixture_id"]] = list(result)
        return result

    gate.check_ready = capture_ready
    rows = []
    next_steps = {
        "F-02": "Recover authentic cutoff-bounded unlock-version evidence; current captured 20 Oct calendar proves past 19 Sep entries, not pre-cutoff availability or an upcoming sale.",
        "F-03": "Recover authentic cutoff-bounded COW token-capture evidence; no forced WAIT for an excluded secondary.",
        "F-04": "Recover later binding vote/implementation and 5 Oct overhang sources; proposal alone is conditional.",
        "F-09": "Recover case-specific ONDO/DTCC link plus original native/USD/price evidence; generic SEC relief does not prove named benefit.",
        "F-10": "Stage unchanged authentic two-NEW_ITEM packet against qualified current tuple; run existing two-session CLI after root release.",
        "F-11": "Preserve financial FAIL. Native readonly wording remains; missing Bybit spot/cohort and synchronized USD/marks preclude proving the full literal case.",
        "F-12": "Fresh no-internet readonly wording must never call PRICE_MARKUP new positions/leverage.",
        "F-13": "Preserve financial FAIL. Recover authentic spot/perp flow/cohort for PERP_LED_CHASE if available; do not substitute archive OI for REST authority.",
        "F-14": "Fresh readonly wording must describe native positions closing; do not admit 23:00-labelled bar ending24:00 before cutoff.",
        "F-15": "Fresh readonly wording must state weak_response n/a and avoid absorption/accumulation; missing historical books are intentional.",
        "F-16": "Fresh readonly context wording remains; eight separate disclosed synthetic policy controls pass, no fabricated historical event.",
    }
    for case in portfolio["fixtures"]:
        fid = case["fixture_id"]
        errors = gate.check_completed(ROOT, case)
        data_ready = not ready_capture[fid]
        market = fid in formulas
        context = contexts.get(fid, {})
        missing_parent = [
            x["declared"]
            for x in context.get("private_parent_paths", {}).values()
            if not x["exists_in_server_projection"]
        ]
        requires_private = context.get("runtime_host_private_proof_required_by_current_validator", False)
        execution = (
            "FORMULA_COMPLETED"
            if market
            else "FAIL_MISSING_CURRENT_PRIVATE_PARENT"
            if requires_private and missing_parent
            else "CURRENT_NATIVE_ADMISSION_AND_TUPLE_STAGING_NOT_RUN"
        )
        f = formulas.get(fid)
        formula_status = ("PASS" if f["automated_passed"] else "FAIL") if f else "NOT_APPLICABLE_NEWS_CASE"
        row = {
            "fixture_id": fid,
            "ready_packet": "PASS" if data_ready else "FAIL",
            "ready_errors": ready_capture[fid],
            "literal_spec_preserved": all(
                case[k] == gate.specification_cases(ROOT)[fid][k]
                for k in ["case_description", "cutoff_spec", "assertion_spec"]
            ),
            "executable": execution,
            "formula": formula_status,
            "formula_checks": f["automated_checks"] if f else [],
            "historical_completed_receipt_valid_on_server": not errors if not market else None,
            "prior_mac_gate_completed": not old[fid]["completed_errors"],
            "completed_gate_errors": errors,
            "manual_current_platform": "PENDING_FRESH_NATIVE_WORDING"
            if market
            else "NOT_RERUN_ON_CURRENT_PLATFORM",
            "full_current_platform_acceptance": False,
            "runtime_missing_private_parents": missing_parent if requires_private else [],
            "next_step": next_steps.get(
                fid,
                "Recover exact unmasked parent/registry from original sealed evidence before fresh replay admission."
                if missing_parent
                else "Requalify and stage unchanged inputs/expected predicates on the current native tuple; historical bound acceptance does not certify this platform.",
            ),
            "case": case,
        }
        rows.append(row)
        print(
            fid,
            row["ready_packet"],
            execution,
            formula_status,
            "RECORDED_GATE=" + str(not errors),
            flush=True,
        )
    assert sha(canonical) == original_sha
    count_pass = sum(check["passed"] for x in formulas.values() for check in x["automated_checks"])
    count_fail = sum(not check["passed"] for x in formulas.values() for check in x["automated_checks"])
    (HERE / "completed-gate-host-path-projections.json").write_text(
        json.dumps(
            {"scope": "HOST_ONLY_LOOKUP_NO_SOURCE_FIELD_REWRITE", "projections": projections}, indent=2
        )
        + "\n"
    )
    result = {
        "scope": "PROPOSED_QA_PORTFOLIO_NOT_CANONICAL_ADOPTION_OR_CURRENT_PLATFORM_FULL_ACCEPTANCE",
        "canonical_manifest_sha256_unchanged": original_sha,
        "ready_packets": sum(x["ready_packet"] == "PASS" for x in rows),
        "total_cases": len(rows),
        "all_literal_definitions_preserved": all(x["literal_spec_preserved"] for x in rows),
        "formula_cases_completed": len(formulas),
        "formula_cases_automated_pass": sum(x["automated_passed"] for x in formulas.values()),
        "formula_predicates_pass": count_pass,
        "formula_predicates_fail": count_fail,
        "recorded_news_acceptance_receipts_valid_on_server": sum(
            x["historical_completed_receipt_valid_on_server"] is True for x in rows
        ),
        "fresh_native_cases_run_in_this_scope": 0,
        "inverse_recovery_exact_parent_matches": 0,
        "inverse_recovery_receipt": "inverse-mask-recovery-v2/receipt.json",
        "original_raw_aliases_restored_with_exact_sha": 6,
        "current_private_parent_gap_cases": [
            x["fixture_id"] for x in rows if x["runtime_missing_private_parents"]
        ],
        "f16_separate_synthetic_policy_controls": "8_OF_8_PASS_NOT_HISTORICAL_ACCEPTANCE",
        "free_bytes": shutil.disk_usage(ROOT).free,
        "cases": rows,
        "limits": "CPU100%,Memory512MiB,noPG/native/external/model/Telegram/X; main product changes handled separately in r85-narrow-corrections.",
    }
    (HERE / "server-final-27-matrix.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    lines = [
        "# All 27 QA packets on server",
        "",
        f"Prepared packets: **{result['ready_packets']}/27**. Formula predicates: **{count_pass} PASS / {count_fail} FAIL**, with genuine F11/F13 negatives retained. Existing bound news acceptance receipts valid on this server: **{result['recorded_news_acceptance_receipts_valid_on_server']}/21**. No fresh native/model run occurred in this scope. These are separate obligations; readiness is not full acceptance.",
        "",
        "|Case|Ready|Executable now / actual execution|Formula|Current native/manual|",
        "|---|---|---|---|---|",
    ]
    for x in rows:
        label = (
            "formula completed"
            if x["executable"] == "FORMULA_COMPLETED"
            else "FAIL: private parent missing"
            if x["runtime_missing_private_parents"]
            else "admission/staging pending"
        )
        manual = (
            "wording pending"
            if x["fixture_id"] in formulas
            else "not rerun; recorded "
            + ("PASS" if x["historical_completed_receipt_valid_on_server"] else "FAIL/unbound")
        )
        lines.append(
            "|"
            + x["fixture_id"]
            + "|"
            + x["ready_packet"]
            + "|"
            + label
            + "|"
            + ("—" if x["formula"].startswith("NOT_") else x["formula"])
            + "|"
            + manual
            + "|"
        )
    lines += [
        "",
        "The exact source recovery restored67 artifacts /23,007,161bytes with67SHA and67mode matches. Fifteen older referenced paths are absent on both Mac and server:11occur only inside historical private transformations;4are active unmasked identity-proof parents forF01(packet+registry),F17(packet),F24(packet). The current source bodies are present. A search of15related authentic transferred packet candidates found no exact canonical-SHA substitute. This is why generic readiness27/27 is stronger than zero-data placeholders but weaker than fresh runtime admission. See [dependency audit](news-private-reference-audit.json) and [recovery receipt](news-source-recovery-receipt.json).",
        "",
        "F11 successor adds25,632verified5m baseline rows from89complete original Binance archive days,19Jul–15Oct, strictly before16Oct08:30. Original67BinanceREST+2593BybitOI and960spotbars remain exact. Binance nowREAL_NATIVE; synchronizednative+8.17617%isABOVE normal. BybitUSD/mark context remains absent; aggregate decomposition and fullmove stayunproved. Actual4hCVD+186,034.8441,1h−287,242.07507 andNEUTRAL directions are unchanged. [Impact and source receipts](f11-baseline-impact.json).",
        "",
        "F16's eight separate disclosed synthetic controls pass A→B/B→C/C→D/Dfloor, nonSTRUCTURALnonBULLISH and no blanket action veto; they do not add a catalyst to historicalF16 or replace native wording. [Controls](f16-policy-controls/evaluation.json).",
        "",
        "Market-source authority:TDD11.1(lines1576–1580)and11.3(lines1628–1646), with an explicitMarketTruth case and archive/REST windows. No fabricatedNEW_ITEM/MAJOR event was added. Masking maps and entire unchanged literal assertions remain outside inputs; expectations sealed before runs; outputs frozen before evaluator reads. The actual formula code pin is69f1e3bbc2828bfc4d0edf5f1d2734bcad2bda607e82b3082e1e094227a204da; later publication-only narrow changes are separately tested and do not retroactively relabel these runs.",
        "",
        "## Exact remaining steps",
        "",
    ]
    for x in rows:
        if x["fixture_id"] in next_steps or x["runtime_missing_private_parents"]:
            lines.append("- " + x["fixture_id"] + ": " + x["next_step"])
    lines += [
        "",
        "Current canonical manifest was not edited:SHA256"
        + original_sha
        + ". No numerical financial expectation, cutoff or original capture clock was rewritten. Source-path projection remains host-only. [Machine ledger and hash-bound case/run references](server-final-27-matrix.json).",
    ]
    (HERE / "server-final-27-matrix.md").write_text("\n".join(lines) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ["cases"]}, indent=2))


if __name__ == "__main__":
    main()

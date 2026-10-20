"""Test-only assembly; existing readers/masking, no news, new mode or model."""

import copy
import hashlib
import json
import sys
from pathlib import Path

from tank.replay import digest, prepare_masked_packet
from tank.util import iso

ROOT = Path.cwd()
HERE = Path(__file__).resolve().parent
OUT = HERE / "market-packets-v2"
sys.path.insert(0, str(ROOT / "tests"))
from fixture_acceptance import check_ready  # noqa: E402


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def reference(path):
    return {"path": str(path.relative_to(ROOT)), "sha256": sha(path)}


def equality(path, value):
    return {"path": path, "equals": value}


def replacements(packet, target):
    """Mask the target identity while retaining BTC's benchmark meaning."""
    token = next(t for t in packet["registry"]["tokens"] if t["token_id"] == target)
    symbol, entity_id = token["symbol"], token["entity_id"]
    result = {target: "asset-k", entity_id: "project-k"}
    if symbol.casefold() == entity_id.casefold():
        result[entity_id] = "ASSETK"
    elif symbol != "BTC":
        result[symbol] = "ASSETK"
    for instrument in packet["registry"]["instruments"]:
        if instrument["token_id"] != target:
            continue
        # Longest-match substitution also masks the same pair inside its
        # instrument ID, so venue/market type and all joins remain unchanged.
        name = instrument.get("symbol") or instrument.get("venue_symbol")
        if name and name not in {"BTC", "ETH"}:
            result.setdefault(name, name.replace(symbol, "ASSETK"))
    for name in [symbol + "USDT", symbol + "USD", symbol + "-USD", symbol + "/USD", symbol + "-USDT-SWAP"]:
        result.setdefault(name, name.replace(symbol, "ASSETK"))
    seen = {key.casefold() for key in result}
    entity = next((e for e in packet["registry"]["entities"] if e["entity_id"] == entity_id), {})
    aliases = [token.get("name"), *entity.get("names", []), *entity.get("aliases", [])]
    aliases += [r.get("handle") for r in packet["registry"].get("x_accounts", []) if r.get("entity_id") == entity_id]
    for number, name in enumerate(aliases):
        if name and name.casefold() not in seen and name not in {"BTC", "ETH"}:
            result[name] = "IdentityAlias" + str(number)
            seen.add(name.casefold())
    return result


def assertions(number):
    truth = ["market_truth", 0]
    decision = ["decisions", 0]
    oi = truth + ["derivatives", "per_venue", 0]
    if number == 11:
        return [
            equality(truth + ["spot", "W1H", "direction"], "SELL"),
            equality(truth + ["spot", "W4H", "direction"], "SELL"),
            {"path": decision + ["action"], "in": ["DONT_CHASE", "WAIT"]},
        ]
    if number == 12:
        return [equality(oi + ["oi_class"], "PRICE_MARKUP"), equality(["observations", "price_markup_oi_flag"], True)]
    if number == 13:
        return [
            equality(oi + ["oi_class"], "REAL_NATIVE"),
            equality(truth + ["move_class"], "PERP_LED_CHASE"),
            {"path": truth + ["move_class"], "not_equals": "SPOT_LED_ACCUMULATION"},
            {"path": decision + ["action"], "in": ["DONT_CHASE", "WAIT"]},
        ]
    if number == 14:
        return [
            {"path": oi + ["oi_native_chg_pct"], "lte": 0},
            {"path": truth + ["move_class"], "in": ["SHORT_SQUEEZE", "LIQUIDITY_VACUUM", "UNKNOWN"]},
            {"path": decision + ["outlook", "direction"], "in": ["TWO_SIDED", "UNKNOWN"]},
            {"path": truth + ["move_class"], "not_equals": "SPOT_LED_ACCUMULATION"},
        ]
    if number == 15:
        return [
            equality(truth + ["liquidity", "elasticity", "weak_response"], None),
            equality(truth + ["move_class"], "UNKNOWN"),
            {"path": truth + ["move_class"], "not_in": ["SPOT_LED_ACCUMULATION", "SELL_SIDE_ABSORPTION"]},
        ]
    return [equality(truth + ["relative", "market_context"], "STRESS")]


def main():
    OUT.mkdir(exist_ok=False)
    manifest = json.loads((ROOT / "tests/fixtures/manifest.json").read_text())
    cases = {c["fixture_id"]: c for c in manifest["fixtures"]}
    code = json.loads((ROOT / "docs/verification/requirements-integration-2026-10-20/candidate-r82-runtime-tuple.json").read_text())["code_version"]
    prepared = []
    for number in range(11, 17):
        fid = f"F-{number:02}"
        folder = OUT / fid
        inputs, private, sealed = (folder / name for name in ["inputs", "private", "sealed"])
        for path in [inputs, private, sealed]:
            path.mkdir(parents=True)
        if number == 11:
            source_manifest = ROOT / "data/replay/candidates/F11-market-only-partial-20261019/manifest.json"
            original = json.loads(source_manifest.read_text())
            source_path = ROOT / original["packet_path"]
            assert sha(source_path) == original["packet_sha256"]
            source = json.loads(source_path.read_text())
            successor_manifest = HERE / "f11-component/manifest.json"
            successor = json.loads(successor_manifest.read_text())
            snapshot_path = ROOT / successor["snapshot_path"]
            assert sha(snapshot_path) == successor["snapshot_sha256"]
            packet = copy.deepcopy(source)
            packet["records"][0]["record"] = json.loads(snapshot_path.read_text())
            packet["assembled_at"] = successor["assembled_at"]
            packet["records"][0]["available_at"] = successor["assembled_at"]
            retained_sources = [reference(source_manifest), reference(source_path), reference(successor_manifest), reference(snapshot_path)]
        else:
            source_manifest = ROOT / "data/replay/candidates/pure-market-retained-20261019" / fid / "manifest.json"
            original = json.loads(source_manifest.read_text())
            source_path = ROOT / original["input_path"]
            assert sha(source_path) == original["sha256"]
            source = json.loads(source_path.read_text())
            packet = copy.deepcopy(source)
            retained_sources = [reference(source_manifest), reference(source_path)]
        original_records = copy.deepcopy(packet["records"])
        packet["code_version"] = code
        packet["config_version"] = "fixture-market:" + digest(
            {"registry": packet["registry"], "config": packet["config"]}
        )
        packet["fixture_id"] = fid
        packet["input_sha256"] = digest({k: v for k, v in packet.items() if k != "input_sha256"})
        assert packet["records"] == original_records
        assert not any(r["collection"].startswith(("evt.", "ingest.", "ana.")) for r in packet["records"])
        original_path = private / "original.json"
        original_path.write_text(json.dumps(packet, separators=(",", ":")))
        target = packet["records"][0]["record"]["token_id"]
        masked_path, map_path = inputs / "input.json", private / "map.json"
        mapping = replacements(packet, target)
        prepare_masked_packet(original_path, masked_path, map_path, mapping)
        masked = json.loads(masked_path.read_text())
        seal = {
            "fixture_id": fid,
            "input_sha256": digest(masked),
            "assertion_spec": cases[fid]["assertion_spec"],
            "sealed_at": iso(),
            "assertions": assertions(number),
            # The entire literal clause is retained, including numerical,
            # wording and policy parts not covered by the scalar predicates.
            "manual_predicates": [{"requirement": cases[fid]["assertion_spec"]}],
        }
        seal_path = sealed / "outcomes.json"
        seal_path.write_text(json.dumps(seal, ensure_ascii=False, indent=2) + "\n")
        case = {
            **{k: cases[fid][k] for k in ["fixture_id", "case_description", "cutoff_spec", "assertion_spec"]},
            "acceptance_input": reference(masked_path),
            "private_name_map": reference(map_path),
            "sealed_outcomes": reference(seal_path),
        }
        errors = check_ready(ROOT, case)
        record = {
            "case": case,
            "readiness_errors": errors,
            "sources_host_only": retained_sources,
            "no_news_event_catalyst_or_assessment_seeded": True,
            "original_cutoff_preserved": masked["cutoff"] == source["cutoff"],
            "original_observations_and_times_preserved_before_identity_mask": True,
            "benchmark_symbol_exception": "BTC remains the benchmark symbol to preserve production market-context semantics" if number == 16 else None,
            "financial_performance_claim": False,
            "native_wording_phase": "PLANNED_NOT_RUN",
        }
        (folder / "prepared.json").write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n")
        prepared.append(record)
        print(fid, "READY" if not errors else errors, flush=True)
    (OUT / "index.json").write_text(json.dumps({"scope": "TEST_ONLY_MARKET_VALIDATION_NOT_CANONICAL_ADOPTION", "prepared": prepared}, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()

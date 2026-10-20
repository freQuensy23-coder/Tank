import hashlib
import json
from collections import Counter
from pathlib import Path

import yaml

C = Path(__file__).resolve().parents[1] / "config"


def load(n):
    return yaml.safe_load((C / (n + ".yaml")).read_text())


def test_exact_universe_and_coverage():
    r = load("registry")
    assert len(r["tokens"]) == 22
    assert len(r["instruments"]) == 181
    assert len({i["instrument_id"] for i in r["instruments"]}) == 181
    assert Counter(a["role"] for a in r["x_accounts"]) == {
        "ORGANIZATION": 41,
        "FOUNDER_C_LEVEL": 37,
        "OTHER": 8,
    }
    assert len({a["handle"].lower() for a in r["x_accounts"]}) == 86
    for token in r["tokens"]:
        ins = [i for i in r["instruments"] if i["token_id"] == token["token_id"]]
        assert sum(i["market_type"] == "SPOT" for i in ins) >= 3
        assert sum(i["market_type"] == "PERP_LINEAR" for i in ins) >= 2
    assert not any(
        i["venue"] == "binance" and i["market_type"] == "SPOT" and i["token_id"] == "lighter:LIT"
        for i in r["instruments"]
    )


def test_sources_have_concrete_endpoints():
    s = load("sources")["sources"]
    assert {x["source_id"] for x in s} == {f"S{i:02}" for i in range(1, 35)}
    for source in s:
        assert source["feeds"], source["source_id"]
        for f in source["feeds"]:
            assert f["url"].startswith("https://")
            assert f["interval_s"] > 0
            if f["kind"] == "html":
                assert f.get("article_pattern"), source["source_id"]
    assert len(next(x for x in s if x["source_id"] == "S14")["feeds"]) == 9
    assert len(next(x for x in s if x["source_id"] == "S25")["feeds"]) == 12


def test_cutoffs_are_bound_to_recorded_calibration():
    t = load("thresholds")
    evidence = t["calibration_evidence"]
    path = C.parent / evidence["report"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == evidence["report_sha256"]
    report = json.loads(path.read_text())
    assert report["selected"] == {key: t[key] for key in ("filter_cutoff", "routing_cutoff")}
    assert report["routing_selection_objective"] == t["routing_selection_objective"]
    retest = C.parent / evidence["independent_retest"]
    assert hashlib.sha256(retest.read_bytes()).hexdigest() == evidence["independent_retest_sha256"]
    assert json.loads(retest.read_text())["thresholds_tuned_after_retest"] is False
    assert t["market_context_rule"] == "normal_range"
    assert "known_f16_conflict" not in t


def test_wallet_artifacts_and_skills():
    for row in load("exchange_addresses")["wallet_lists"]:
        assert row["status"] == "VERIFIED"
        assert len(row["addresses"]) == row["expected_evm_addresses"]
    for name in ["tank-event-analyst", "tank-data", "tank-scout-reviewer"]:
        manifest = yaml.safe_load((C / "skills" / name / "SKILL.md").read_text().split("---", 2)[1])
        assert manifest["name"] == name
        assert manifest["version"].startswith("baseline-")
    for p in (C / "skills").rglob("*.json"):
        json.loads(p.read_text())


def test_every_semantic_has_authoritative_canonical_contract_without_runtime_verification():
    semantics = load("semantics")
    rows = semantics["metrics"]
    assert "self_test_policy" not in semantics
    required = {
        "metric_key",
        "venue",
        "market_type",
        "endpoint_or_stream",
        "meaning",
        "unit",
        "contract_multiplier_ref",
        "sidedness",
        "side_semantics",
        "trade_aggregation",
        "scope",
        "calc_method",
        "timestamp_semantics",
        "timestamp_unit",
        "timestamp_precision",
        "price_contaminated",
        "requires_native_normalization",
        "aggregation_class",
        "inference_limits",
        "doc_ref",
        "critical",
    }
    assert len({r["metric_key"] for r in rows}) == len(rows)
    for row in rows:
        assert required <= row.keys(), row["metric_key"]
        assert (
            not {"confidence", "semantic_confidence", "verified_at", "verified_by", "verification_status"}
            & row.keys()
        )
        assert row["source_basis_date"] == "2026-10-16"
        assert row["unit"] in {
            "BASE_QTY",
            "CONTRACTS",
            "QUOTE_NOTIONAL",
            "USD_NOTIONAL",
            "RATE_PER_INTERVAL",
            "PRICE",
            "BPS",
            "COUNT",
        }
        assert row["sidedness"] in {"SINGLE_SIDED", "BOTH_SIDES", "N_A", "UNKNOWN"}
        assert row["side_semantics"] in {"TAKER_SIDE", "MAKER_SIDE", "BUYER_IS_MAKER_FLAG", "N_A"}
        assert row["timestamp_unit"] in {"S", "MS", "US"}
        assert row["timestamp_precision"] in {"EXACT", "BUCKET", "COLLECTION_ONLY"}
        assert isinstance(row["critical"], bool) and isinstance(row["inference_limits"], list)
    assert {r["metric_family"] for r in rows if r["critical"]} == {
        "aggressor",
        "depth",
        "price",
        "oi",
        "funding",
    }
    oi = [r for r in rows if r["metric_family"] == "oi"]
    assert all(r["aggregation_class"] == "oi_native_single_sided" for r in oi if r["venue"] != "hyperliquid")
    assert next(r for r in oi if r["venue"] == "hyperliquid")["sidedness"] == "UNKNOWN"


def test_nested_mutation_schemas_are_closed():
    def visit(schema):
        if schema.get("type") == "object":
            assert schema.get("additionalProperties") is False
            assert schema.get("properties")
        for value in schema.get("properties", {}).values():
            visit(value)
        if "items" in schema:
            visit(schema["items"])

    for name in ["set_assessment", "set_beneficiaries"]:
        visit(json.loads((C / "skills" / "tank-event-analyst" / (name + ".schema.json")).read_text()))


def test_fundamentals_provider_identity_and_metric_evidence():
    registry = load("registry")
    evidence = json.loads((C / "fundamentals_evidence.json").read_text())
    checks = {r["token_id"]: r for r in evidence["protocols"]}
    assert len(checks) == 22
    for token in registry["tokens"]:
        entry = checks[token["token_id"]]
        assert set(entry["checks"]) == {"dailyFees", "dailyRevenue", "dailyHoldersRevenue"}
        if token["fundamentals_status"] == "SUPPORTED":
            assert len(token["defillama_protocol_slugs"]) == 1
            assert token["defillama_protocol_slug"] == token["fee_protocol_slug"] == entry["slug"]
            assert entry["checks"]["dailyFees"]["identity_verified"] is True
            assert entry["checks"]["dailyFees"]["resolved_id"] == entry["expected_id"]
        else:
            assert token["symbol"] in {"ZRX", "QNT", "WLD"}
            assert token["defillama_protocol_slugs"] == []
            assert token["fundamentals_evidence"]["reason"]
        for check in entry["checks"].values():
            assert len(check["response_sha256"]) == 64
            assert check["url"].startswith("https://api.llama.fi/summary/fees/")
    assert next(t for t in registry["tokens"] if t["symbol"] == "XPL")["native_chain_id"] == 9745
    assert (
        next(t for t in registry["tokens"] if t["symbol"] == "WLD")["supplementary_chain_fee_slug"]
        == "world-chain"
    )
    assert checks["bitcoin:BTC"]["checks"]["dailyHoldersRevenue"]["status"] == "NOT_SUPPORTED"


def test_documented_multi_organization_authority_preserves_universe():
    r = load("registry")
    account = next(a for a in r["x_accounts"] if a["handle"] == "sama")
    assert account["entity_id"] == "world"
    assert {a["entity_id"] for a in account["official_authorities"]} == {"world", "openai"}
    assert all(a["evidence_refs"] and a["authority_scope"] for a in account["official_authorities"])
    assert len(r["x_accounts"]) == 86 and len(r["tokens"]) == 22
    assert next(e for e in r["entities"] if e["entity_id"] == "openai")["tokens"] == []


def test_agent_tool_descriptions_are_self_contained_and_synced():
    import re

    for directory in (C / "skills").iterdir():
        if not (directory / "tools.json").exists():
            continue
        for tool in json.loads((directory / "tools.json").read_text()):
            schema_path = directory / (tool["name"] + ".schema.json")
            if not schema_path.exists():
                schema_path = C / "skills" / "tank-event-analyst" / (tool["name"] + ".schema.json")
            schema = json.loads(schema_path.read_text())
            assert tool["description"] == schema["description"]
            assert not re.search(
                r"§|validators below|same validators|Tank get item", tool["description"], re.I
            )
            assert len(tool["description"].split()) >= 15


def test_data_guide_matches_typed_market_column_contract():
    from tank.market_schema import MARKET_COLUMNS

    guide = (C / "skills/tank-data/references/DATA_GUIDE.md").read_text()
    assert "CREATE TABLE mkt." not in guide
    assert "FROM mkt.trades" not in guide
    for table, columns in MARKET_COLUMNS.items():
        section = guide.split("## " + table + "\n", 1)[1].split("\n## ", 1)[0]
        for field, data_type in columns.items():
            assert f"| `{field}` | `{data_type}` |" in section

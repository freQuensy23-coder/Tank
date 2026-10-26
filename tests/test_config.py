from collections import Counter
from pathlib import Path
from typing import Literal

import yaml
from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.sources import SourceConfiguration
from tank.settings import Settings

C = Path(__file__).resolve().parents[1] / "config"


class SkillManifest(DomainModel):
    name: str
    version: str
    description: str


class FundamentalsCheck(DomainModel):
    url: str
    http_status: int
    response_sha256: str
    resolved_name: str | None
    resolved_id: str | None
    identity_verified: bool
    has_historical_series: bool
    total24h_is_available: bool
    methodology: dict[str, str] | None
    status: Literal["SUPPORTED", "NOT_SUPPORTED"]
    reason: str | None


class FundamentalsChecks(DomainModel):
    dailyFees: FundamentalsCheck
    dailyRevenue: FundamentalsCheck
    dailyHoldersRevenue: FundamentalsCheck


class CatalogChild(DomainModel):
    slug: str
    id: str
    parentProtocol: str


class ProtocolEvidence(DomainModel):
    token_id: str
    slug: str
    expected_id: str | None
    catalog_children: list[CatalogChild]
    checks: FundamentalsChecks


class EvidenceCatalogs(DomainModel):
    fees: str
    chains: str
    protocols: str


class FundamentalsEvidence(DomainModel):
    verified_at: str
    provider: str
    catalogs: EvidenceCatalogs
    catalog_sha256: str
    protocols: list[ProtocolEvidence]


def test_exact_universe_and_coverage() -> None:
    registry = Settings(config_dir=C, _env_file=None).registry()
    assert len(registry.tokens) == 22
    assert len(registry.instruments) == 181
    assert len({instrument.instrument_id for instrument in registry.instruments}) == 181
    assert Counter(account.role for account in registry.x_accounts) == {
        "ORGANIZATION": 41,
        "FOUNDER_C_LEVEL": 37,
        "OTHER": 8,
    }
    assert len({account.handle.lower() for account in registry.x_accounts}) == 86
    for token in registry.tokens:
        instruments = [
            instrument for instrument in registry.instruments if instrument.token_id == token.token_id
        ]
        assert sum(instrument.market_type == "SPOT" for instrument in instruments) >= 3
        assert sum(instrument.market_type == "PERP_LINEAR" for instrument in instruments) >= 2
    assert not any(
        instrument.venue == "binance"
        and instrument.market_type == "SPOT"
        and instrument.token_id == "lighter:LIT"
        for instrument in registry.instruments
    )


def test_sources_have_concrete_endpoints() -> None:
    sources = Settings(config_dir=C, _env_file=None).load_config_as("sources", SourceConfiguration).sources
    assert {source.source_id for source in sources} == {f"S{i:02}" for i in range(1, 35)}
    for source in sources:
        assert source.feeds, source.source_id
        for feed in source.feeds:
            assert feed.url.startswith("https://")
            assert feed.interval_s > 0
            if feed.kind == "html":
                assert feed.article_pattern, source.source_id
    assert len(next(source for source in sources if source.source_id == "S14").feeds) == 9
    assert len(next(source for source in sources if source.source_id == "S25").feeds) == 12


def test_wallet_artifacts_and_skills() -> None:
    for row in Settings(config_dir=C, _env_file=None).exchange_addresses().wallet_lists:
        assert row.status == "VERIFIED"
        assert len(row.addresses) == row.expected_evm_addresses
    for name in ["tank-event-analyst", "tank-data", "tank-scout-reviewer"]:
        manifest = SkillManifest.model_validate(
            yaml.safe_load((C / "skills" / name / "SKILL.md").read_text().split("---", 2)[1]), strict=True
        )
        assert manifest.name == name
        assert manifest.version.startswith("baseline-")
    for path in (C / "skills").rglob("*.json"):
        decode_json(path.read_text())


def test_every_semantic_has_authoritative_canonical_contract_without_runtime_verification() -> None:
    rows = Settings(config_dir=C, _env_file=None).semantics().metrics
    assert len({row.metric_key for row in rows}) == len(rows)
    assert all(row.source_basis_date == "2026-10-16" for row in rows)
    assert {row.metric_family for row in rows if row.critical} == {
        "aggressor",
        "depth",
        "price",
        "oi",
        "funding",
    }
    oi = [row for row in rows if row.metric_family == "oi"]
    assert all(row.aggregation_class == "oi_native_single_sided" for row in oi if row.venue != "hyperliquid")
    assert next(row for row in oi if row.venue == "hyperliquid").sidedness == "UNKNOWN"


def test_nested_mutation_schemas_are_closed() -> None:
    def visit(schema: JsonValue) -> None:
        assert isinstance(schema, dict)
        properties = schema.get("properties")
        if schema.get("type") == "object":
            assert schema["additionalProperties"] is False
            assert properties
        if properties is not None:
            assert isinstance(properties, dict)
            for value in properties.values():
                visit(value)
        if "items" in schema:
            visit(schema["items"])

    for name in ["set_assessment", "set_beneficiaries"]:
        visit(decode_json((C / "skills" / "tank-event-analyst" / (name + ".schema.json")).read_text()))


def test_fundamentals_provider_identity_and_metric_evidence() -> None:
    registry = Settings(config_dir=C, _env_file=None).registry()
    evidence = FundamentalsEvidence.model_validate_json((C / "fundamentals_evidence.json").read_bytes())
    checks = {row.token_id: row for row in evidence.protocols}
    assert len(checks) == 22
    for token in registry.tokens:
        entry = checks[token.token_id]
        if token.fundamentals_status == "SUPPORTED":
            assert len(token.defillama_protocol_slugs) == 1
            assert token.defillama_protocol_slug == token.fee_protocol_slug == entry.slug
            assert entry.checks.dailyFees.identity_verified is True
            assert entry.checks.dailyFees.resolved_id == entry.expected_id
        else:
            assert token.symbol in {"ZRX", "QNT", "WLD"}
            assert token.defillama_protocol_slugs == []
            assert token.fundamentals_evidence.reason
        for check in (entry.checks.dailyFees, entry.checks.dailyRevenue, entry.checks.dailyHoldersRevenue):
            assert len(check.response_sha256) == 64
            assert check.url.startswith("https://api.llama.fi/summary/fees/")
    assert next(token for token in registry.tokens if token.symbol == "XPL").native_chain_id == 9745
    assert (
        next(token for token in registry.tokens if token.symbol == "WLD").supplementary_chain_fee_slug
        == "world-chain"
    )
    assert checks["bitcoin:BTC"].checks.dailyHoldersRevenue.status == "NOT_SUPPORTED"


def test_dependency_map_preserves_evidence_gated_promotion() -> None:
    dependencies = Settings(config_dir=C, _env_file=None).dependencies()
    assert len(dependencies.edges) == 37
    assert all(edge.evidence_required_for_promotion for edge in dependencies.edges)


def test_thresholds_roundtrip_current_operational_configuration() -> None:
    settings = Settings(config_dir=C, _env_file=None)
    configured = settings.load_config("thresholds")
    validated = settings.thresholds()
    assert "card_max_chars" not in configured
    assert validated.model_dump(mode="json") == configured

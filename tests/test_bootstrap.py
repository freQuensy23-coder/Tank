"""Offline managed registry preparation against the actual captured configuration."""

import hashlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Never

import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.bootstrap import _config_timestamp_metadata, sync_registry
from tank.contracts.json import encode_json
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import canonical_json


class FixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


class PagedRepository(MemoryRepository):
    async def list(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None, limit: int = 1000
    ) -> Never:
        raise AssertionError("Registry reconciliation must use uncapped iteration")

    def iter_all(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None, batch_size: int = 1000
    ) -> AsyncGenerator[dict[str, JsonValue]]:
        return super().iter_all(collection, filters=filters, batch_size=7)


def assert_config_retained(stored: JsonValue, configured: JsonValue) -> None:
    if isinstance(configured, dict):
        assert isinstance(stored, dict)
        for key, value in configured.items():
            assert_config_retained(stored[key], value)
    elif isinstance(configured, list):
        assert isinstance(stored, list) and len(stored) == len(configured)
        for actual, expected in zip(stored, configured, strict=True):
            assert_config_retained(actual, expected)
    else:
        assert type(stored) is type(configured)
        assert stored == configured


def records(document: dict[str, JsonValue], field: str) -> list[dict[str, JsonValue]]:
    values = document[field]
    assert isinstance(values, list)
    result: list[dict[str, JsonValue]] = []
    for value in values:
        assert isinstance(value, dict)
        result.append(value)
    return result


def documents() -> dict[str, dict[str, JsonValue]]:
    settings = FixtureSettings()
    return {
        name: settings.load_config(name)
        for name in ("sources", "registry", "dependency_map", "semantics", "exchange_addresses")
    }


def write_documents(root: Path, captured: dict[str, dict[str, JsonValue]]) -> None:
    for name, value in captured.items():
        (root / (name + ".yaml")).write_bytes(encode_json(value))


async def test_canonical_registry_bootstrap_and_idempotence() -> None:
    repo = PagedRepository()
    settings = FixtureSettings(config_version="test-config", code_version="test-code")
    counts = await sync_registry(
        repo, settings, versions=declared_versions_fixture("test-config", "test-code")
    )
    registry = settings.registry()
    expected = {
        "reg.sources": len(records(settings.load_config("sources"), "sources")),
        "reg.tokens": len(registry.tokens),
        "reg.instruments": len(registry.instruments),
        "reg.entities": len(registry.entities),
        "reg.sector_baskets": len(registry.sector_baskets),
        "reg.entity_aliases": sum(
            len(set(entity.aliases))
            + (len(entity.qualified_aliases) if entity.qualified_aliases is not None else 0)
            for entity in registry.entities
        ),
        "reg.dependency_map": len(settings.dependencies().edges),
        "reg.metric_semantics": len(settings.semantics().metrics),
        "reg.exchange_addresses": sum(
            len(wallet.addresses) for wallet in settings.exchange_addresses().wallet_lists
        ),
    }
    assert counts.to_wire() == expected
    for table, count in expected.items():
        assert count > 0
        assert len([row async for row in repo.iter_all(table)]) == count
    raw = settings.load_config("registry")
    for section, identifier in (
        ("tokens", "token_id"),
        ("instruments", "instrument_id"),
        ("entities", "entity_id"),
    ):
        for configured in records(raw, section):
            key = configured[identifier]
            assert isinstance(key, str)
            stored = await repo.get("reg." + section, key)
            assert stored is not None
            assert_config_retained(stored, configured)
    for configured in records(settings.load_config("sources"), "sources"):
        key = configured["source_id"]
        assert isinstance(key, str)
        stored = await repo.get("reg.sources", key)
        assert stored is not None
        assert_config_retained(stored, configured)
    source = await repo.get("reg.sources", "S01")
    assert source is not None
    assert source["name"] == "SEC" and source["config_version"] == "test-config"
    assert source["config_file"] == "sources.yaml"
    assert source["config_sha256"] == hashlib.sha256(settings.read_config_bytes("sources.yaml")).hexdigest()
    assert source["config_schema_version"] == settings.load_config("sources")["version"]
    assert await repo.get("reg.sources", "S34") is not None
    for source_id, publisher in (("S26", "BNY"), ("S27", "The Clearing House"), ("S28", "Anchorage Digital")):
        stored = await repo.get("reg.sources", source_id)
        assert stored is not None and stored["author_organization"] == publisher
    treasury = await repo.get("reg.sources", "S07")
    assert treasury is not None and "author_organization" not in treasury
    assert [feed["author_organization"] for feed in records(treasury, "feeds")] == [
        "U.S. Department of the Treasury",
        "Office of Foreign Assets Control",
    ]
    assert (
        await sync_registry(repo, settings, versions=declared_versions_fixture("test-config", "test-code"))
        == counts
    )
    for table, count in expected.items():
        assert len([row async for row in repo.iter_all(table)]) == count
    wallets = [row async for row in repo.iter_all("reg.exchange_addresses")]
    assert all(row["chain"] is None and row["chain_scope"] == "EVM" for row in wallets)
    assert all(row["commit"] and row["repository"] for row in wallets)


async def test_sync_removes_configured_tail_and_preserves_discovery(tmp_path: Path) -> None:
    captured = documents()
    write_documents(tmp_path, captured)
    settings = FixtureSettings(config_dir=tmp_path)
    repo = PagedRepository()
    await sync_registry(repo, settings, versions=declared_versions_fixture())
    await repo.put("reg.sources", "discovered", {"source_id": "discovered"})
    existing = await repo.get("reg.sources", "S01")
    assert existing is not None
    await repo.put("reg.sources", "S01", {**existing, "discovery_owned": {"flag": True, "serial": 1}})
    sources = records(captured["sources"], "sources")
    sources[0]["name"] = "updated"
    captured["sources"]["sources"] = [sources[0]]
    write_documents(tmp_path, captured)
    await sync_registry(repo, settings, versions=declared_versions_fixture())
    assert await repo.get("reg.sources", "S34") is None
    source = await repo.get("reg.sources", "S01")
    assert source is not None and source["name"] == "updated"
    assert canonical_json(source["discovery_owned"]) == canonical_json({"flag": True, "serial": 1})
    assert await repo.get("reg.sources", "discovered") is not None
    assert len([row async for row in repo.iter_all("reg.sources")]) == 2


async def test_duplicate_source_ids_fail_before_any_mutation(tmp_path: Path) -> None:
    captured = documents()
    first = records(captured["sources"], "sources")[0]
    captured["sources"]["sources"] = [first, first]
    write_documents(tmp_path, captured)
    repo = PagedRepository()
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        await sync_registry(repo, FixtureSettings(config_dir=tmp_path), versions=declared_versions_fixture())
    assert [row async for row in repo.iter_all("reg.sources")] == []


async def test_static_config_clocks_preserve_declared_precision_and_unknown_legacy(tmp_path: Path) -> None:
    captured: dict[str, JsonValue] = {
        "checked_at": "2026-10-17T12:00:00+00:00",
        "checked_at_precision": "COLLECTION_ONLY",
        "evidence": {
            "verified_at": "2026-10-16",
            "review": {"verified_at": {"value": "2026-10-15T00:00:00Z", "precision": "BUCKET(1d)"}},
        },
    }
    original = canonical_json(captured)
    serialized = _config_timestamp_metadata(captured)
    assert isinstance(serialized, dict)
    assert_config_retained(serialized, captured)
    assert serialized["checked_at_prec"] == "COLLECTION_ONLY"
    evidence = serialized["evidence"]
    assert isinstance(evidence, dict)
    assert evidence["verified_at_prec"] is None and evidence["verified_at_prec_reason"] == "INCOMPLETE"
    review = evidence["review"]
    assert isinstance(review, dict) and review["verified_at_prec"] == "BUCKET(1d)"
    assert canonical_json(captured) == original
    values = documents()
    entity = records(values["registry"], "entities")[0]
    entity["identity_reviewed_at"] = "2026-10-18"
    entity["identity_reviewed_at_prec"] = "BUCKET"
    entity["identity_reviewed_at_bucket_size_s"] = 86400
    write_documents(tmp_path, values)
    repo = PagedRepository()
    settings = FixtureSettings(config_dir=tmp_path)
    await sync_registry(repo, settings, versions=declared_versions_fixture())
    stored = await repo.get("reg.entities", "bitcoin")
    assert stored is not None and stored["identity_reviewed_at"] == "2026-10-18"
    assert stored["identity_reviewed_at_prec"] == "BUCKET(86400s)"
    instrument = await repo.get("reg.instruments", "binance:spot:BTCUSDT")
    assert instrument is not None and instrument["resolved_at"] is None
    assert instrument["resolved_at_prec"] is None and instrument["resolved_at_prec_reason"] == "INCOMPLETE"


async def test_conflicting_static_clock_precision_refused_before_registry_mutation(tmp_path: Path) -> None:
    captured = documents()
    source = records(captured["sources"], "sources")[0]
    source["endpoint_verification"] = {
        "checked_at": "2026-10-17T12:00:00Z",
        "status": "VERIFIED",
        "basis": "Synthetic explicit clock",
        "checked_at_prec": "EXACT",
        "checked_at_precision": "BUCKET(1d)",
    }
    write_documents(tmp_path, captured)
    repo = PagedRepository()
    with pytest.raises(ValueError, match="Conflicting timestamp precision"):
        await sync_registry(repo, FixtureSettings(config_dir=tmp_path), versions=declared_versions_fixture())
    assert [row async for row in repo.iter_all("reg.sources")] == []


async def test_alias_projection_never_promotes_display_names_or_drops_qualifiers(tmp_path: Path) -> None:
    captured = documents()
    entity: dict[str, JsonValue] = {
        "entity_id": "example",
        "kind": "OTHER",
        "names": ["Generic display name"],
        "aliases": ["Explicit alias"],
        "tokens": [],
        "official_sources": [],
        "layers_served": [],
        "competes_with": [],
        "qualified_aliases": [{"alias": "Qualified name", "requires_any": ["TOKEN"]}],
    }
    captured["registry"]["entities"] = [entity]
    write_documents(tmp_path, captured)
    repo = PagedRepository()
    await sync_registry(repo, FixtureSettings(config_dir=tmp_path), versions=declared_versions_fixture())
    aliases = {row["alias"]: row async for row in repo.iter_all("reg.entity_aliases")}
    assert set(aliases) == {"Explicit alias", "Qualified name"}
    assert aliases["Qualified name"]["requires_any"] == ["TOKEN"]


@pytest.mark.parametrize(
    "policy",
    [
        {"aliases": "display"},
        {"aliases": [""]},
        {"aliases": [1]},
        {"qualified_aliases": {}},
        {"qualified_aliases": [{"alias": "Lighter", "requires_any": []}]},
        {"qualified_aliases": [{"alias": "", "requires_any": ["LIT"]}]},
        {"qualified_aliases": [{"alias": "Lighter", "requires_any": [1]}]},
        {"qualified_aliases": [{"alias": "Lighter"}]},
    ],
)
async def test_invalid_routing_alias_policy_refused_before_registry_mutation(
    tmp_path: Path, policy: dict[str, JsonValue]
) -> None:
    captured = documents()
    entity: dict[str, JsonValue] = {
        "entity_id": "example",
        "kind": "OTHER",
        "names": ["Synthetic entity"],
        "aliases": [],
        "tokens": [],
        "official_sources": [],
        "layers_served": [],
        "competes_with": [],
    }
    entity.update(policy)
    captured["registry"]["entities"] = [entity]
    write_documents(tmp_path, captured)
    repo = PagedRepository()
    await repo.put("reg.sources", "existing", {"source_id": "existing"})
    with pytest.raises(ValueError, match="aliases"):
        await sync_registry(repo, FixtureSettings(config_dir=tmp_path), versions=declared_versions_fixture())
    assert [row async for row in repo.iter_all("reg.sources")] == [{"source_id": "existing"}]
    assert [row async for row in repo.iter_all("reg.entities")] == []


async def test_corrupt_managed_identity_rolls_back_all_preceding_registry_writes() -> None:
    repo = PagedRepository()
    corrupt: dict[str, JsonValue] = {"token_id": "historical", "registry_managed_by": "tank.bootstrap"}
    await repo.put("reg.tokens", "historical", corrupt)
    with pytest.raises(KeyError, match="registry_storage_key"):
        await sync_registry(repo, FixtureSettings(), versions=declared_versions_fixture())
    assert [row async for row in repo.iter_all("reg.sources")] == []
    assert await repo.get("reg.tokens", "historical") == corrupt


async def test_removing_configured_qualifier_clears_owned_state_without_erasing_discovery(
    tmp_path: Path,
) -> None:
    captured = documents()
    entity: dict[str, JsonValue] = {
        "entity_id": "example",
        "kind": "OTHER",
        "names": ["Synthetic entity"],
        "aliases": [],
        "tokens": [],
        "official_sources": [],
        "layers_served": [],
        "competes_with": [],
        "qualified_aliases": [{"alias": "Registered", "requires_any": ["TOKEN"]}],
    }
    captured["registry"]["entities"] = [entity]
    write_documents(tmp_path, captured)
    repo = PagedRepository()
    settings = FixtureSettings(config_dir=tmp_path)
    await sync_registry(repo, settings, versions=declared_versions_fixture())
    aliases = [row async for row in repo.iter_all("reg.entity_aliases")]
    assert len(aliases) == 1 and aliases[0]["requires_any"] == ["TOKEN"]
    key = aliases[0]["registry_storage_key"]
    assert isinstance(key, str)
    await repo.put("reg.entity_aliases", key, {**aliases[0], "discovery_owned": {"flag": True, "serial": 1}})
    entity["aliases"] = ["Registered"]
    del entity["qualified_aliases"]
    write_documents(tmp_path, captured)
    await sync_registry(repo, settings, versions=declared_versions_fixture())
    updated = await repo.get("reg.entity_aliases", key)
    assert updated is not None and "requires_any" not in updated
    assert canonical_json(updated["discovery_owned"]) == canonical_json({"flag": True, "serial": 1})


async def test_metric_family_replacement_removes_all_old_family_fields_and_preserves_discovery(
    tmp_path: Path,
) -> None:
    captured = documents()
    metrics = records(captured["semantics"], "metrics")
    depth = next(
        row
        for row in metrics
        if row["metric_family"] == "depth"
        and row["market_type"] == "PERP_LINEAR"
        and "current_doc_ref" in row
    )
    funding = next(
        row for row in metrics if row["metric_family"] == "funding" and row["venue"] == depth["venue"]
    )
    key = depth["metric_key"]
    assert isinstance(key, str)
    captured["semantics"]["metrics"] = [depth]
    write_documents(tmp_path, captured)
    repo = PagedRepository()
    settings = FixtureSettings(config_dir=tmp_path)
    await sync_registry(repo, settings, versions=declared_versions_fixture())
    original = await repo.get("reg.metric_semantics", key)
    assert original is not None and "current_doc_ref" in original
    await repo.put("reg.metric_semantics", key, {**original, "discovery_owned": {"flag": True, "serial": 1}})
    replacement = dict(funding)
    replacement["metric_key"] = key
    captured["semantics"]["metrics"] = [replacement]
    write_documents(tmp_path, captured)
    await sync_registry(repo, settings, versions=declared_versions_fixture())
    updated = await repo.get("reg.metric_semantics", key)
    assert updated is not None and updated["metric_family"] == "funding"
    assert "current_doc_ref" not in updated and "endpoint_change_evidence" not in updated
    assert updated["estimate"] == funding["estimate"] and updated["settled"] == funding["settled"]
    assert canonical_json(updated["discovery_owned"]) == canonical_json({"flag": True, "serial": 1})

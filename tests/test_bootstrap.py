"""Offline startup registry coverage against the actual captured configuration."""

from pathlib import Path

import pytest
import yaml

from tank.bootstrap import sync_registry
from tank.settings import Settings
from tank.storage import MemoryRepository


class PagedRepository(MemoryRepository):
    async def list(self, *args, **kwargs) -> None:
        raise AssertionError("Registry reconciliation must use uncapped iteration")

    def iter_all(self, collection, *, filters=None, batch_size=1000):
        return super().iter_all(collection, filters=filters, batch_size=7)


def assert_config_retained(stored, configured) -> None:
    if isinstance(configured, dict):
        for key, value in configured.items():
            assert_config_retained(stored[key], value)
    elif isinstance(configured, list):
        assert len(stored) == len(configured)
        for actual, expected in zip(stored, configured, strict=True):
            assert_config_retained(actual, expected)
    else:
        assert stored == configured


async def test_canonical_registry_bootstrap_and_idempotence() -> None:
    repo = PagedRepository()
    settings = Settings(_env_file=None, config_version="test-config", code_version="test-code")
    counts = await sync_registry(repo, settings)
    registry = settings.load_config("registry")
    expected = {
        "reg.sources": len(settings.load_config("sources")["sources"]),
        **{
            f"reg.{section}": len(registry[section])
            for section in ("tokens", "instruments", "entities", "sector_baskets")
        },
        "reg.entity_aliases": sum(
            len(set(entity.get("aliases", []))) + len(entity.get("qualified_aliases", []))
            for entity in registry["entities"]
        ),
        "reg.dependency_map": len(settings.load_config("dependency_map")["edges"]),
        "reg.metric_semantics": len(settings.load_config("semantics")["metrics"]),
        "reg.exchange_addresses": sum(
            len(wallet["addresses"]) for wallet in settings.load_config("exchange_addresses")["wallet_lists"]
        ),
    }
    assert counts == expected
    for table, count in expected.items():
        assert count > 0
        assert len([row async for row in repo.iter_all(table)]) == count
    for section, identifier in (
        ("tokens", "token_id"),
        ("instruments", "instrument_id"),
        ("entities", "entity_id"),
    ):
        for configured in registry[section]:
            stored = await repo.get(f"reg.{section}", configured[identifier])
            assert stored is not None
            assert_config_retained(stored, configured)
    for configured in settings.load_config("sources")["sources"]:
        stored = await repo.get("reg.sources", configured["source_id"])
        assert stored is not None
        assert_config_retained(stored, configured)
    source = await repo.get("reg.sources", "S01")
    assert source["name"] == "SEC"
    assert source["config_version"] == "test-config"
    assert source["config_file"] == "sources.yaml"
    assert len(source["config_sha256"]) == 64
    assert await repo.get("reg.sources", "S34")
    for source_id, publisher in (
        ("S26", "BNY"),
        ("S27", "The Clearing House"),
        ("S28", "Anchorage Digital"),
    ):
        assert (await repo.get("reg.sources", source_id))["author_organization"] == publisher
    treasury = await repo.get("reg.sources", "S07")
    assert "author_organization" not in treasury
    assert [feed["author_organization"] for feed in treasury["feeds"]] == [
        "U.S. Department of the Treasury",
        "Office of Foreign Assets Control",
    ]
    assert await sync_registry(repo, settings) == counts
    for table, count in counts.items():
        assert len([row async for row in repo.iter_all(table)]) == count
    wallets = [row async for row in repo.iter_all("reg.exchange_addresses")]
    assert all(row["chain"] is None and row["chain_scope"] == "EVM" for row in wallets)
    assert all(row["commit"] and row["repository"] for row in wallets)


async def test_sync_removes_configured_tail_and_preserves_discovery(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, config_dir=tmp_path)
    for name, contents in {
        "sources": {"sources": [{"source_id": f"S{i}", "name": str(i)} for i in range(35)]},
        "registry": {},
        "dependency_map": {},
        "semantics": {},
        "exchange_addresses": {},
    }.items():
        (tmp_path / f"{name}.yaml").write_text(yaml.safe_dump(contents))
    repo = PagedRepository()
    await sync_registry(repo, settings)
    await repo.put("reg.sources", "discovered", {"source_id": "discovered"})
    (tmp_path / "sources.yaml").write_text(
        yaml.safe_dump({"sources": [{"source_id": "S0", "name": "updated"}]})
    )
    await sync_registry(repo, settings)
    assert await repo.get("reg.sources", "S34") is None
    assert (await repo.get("reg.sources", "S0"))["name"] == "updated"
    assert await repo.get("reg.sources", "discovered")
    assert len([row async for row in repo.iter_all("reg.sources")]) == 2


async def test_duplicate_source_ids_fail_before_any_mutation(tmp_path: Path) -> None:
    (tmp_path / "sources.yaml").write_text("sources: [{source_id: S01}, {source_id: S01}]")
    repo = PagedRepository()
    with pytest.raises(ValueError, match="Duplicate configured registry key"):
        await sync_registry(repo, Settings(_env_file=None, config_dir=tmp_path))
    assert [row async for row in repo.iter_all("reg.sources")] == []


async def test_static_config_clocks_preserve_declared_precision_and_unknown_legacy(tmp_path: Path) -> None:
    sources = {
        "sources": [
            {
                "source_id": "S01",
                "checked_at": "2026-10-17T12:00:00+00:00",
                "checked_at_precision": "COLLECTION_ONLY",
                "evidence": {
                    "verified_at": "2026-10-16",
                    "review": {"verified_at": {"value": "2026-10-15T00:00:00Z", "precision": "BUCKET(1d)"}},
                },
            }
        ]
    }
    registry = {
        "instruments": [{"instrument_id": "example", "resolved_at": None}],
        "tokens": [
            {
                "token_id": "example:T",
                "identity_reviewed_at": "2026-10-18",
                "identity_reviewed_at_prec": "BUCKET",
                "identity_reviewed_at_bucket_size_s": 86400,
            }
        ],
    }
    for name, value in {
        "sources": sources,
        "registry": registry,
        "dependency_map": {},
        "semantics": {},
        "exchange_addresses": {},
    }.items():
        (tmp_path / f"{name}.yaml").write_text(yaml.safe_dump(value))
    repo = PagedRepository()
    settings = Settings(_env_file=None, config_dir=tmp_path)
    await sync_registry(repo, settings)
    source = await repo.get("reg.sources", "S01")
    assert_config_retained(source, sources["sources"][0])
    assert source["checked_at_prec"] == "COLLECTION_ONLY"
    assert source["evidence"]["verified_at_prec"] is None
    assert source["evidence"]["verified_at_prec_reason"] == "INCOMPLETE"
    assert source["evidence"]["review"]["verified_at_prec"] == "BUCKET(1d)"
    instrument = await repo.get("reg.instruments", "example")
    assert instrument["resolved_at"] is None and instrument["resolved_at_prec"] is None
    assert instrument["resolved_at_prec_reason"] == "INCOMPLETE"
    token = await repo.get("reg.tokens", "example:T")
    assert token["identity_reviewed_at"] == "2026-10-18"
    assert token["identity_reviewed_at_prec"] == "BUCKET(86400s)"
    await sync_registry(repo, settings)
    assert await repo.get("reg.sources", "S01") == source


async def test_conflicting_static_clock_precision_refused_before_registry_mutation(tmp_path: Path) -> None:
    (tmp_path / "sources.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "source_id": "S01",
                        "checked_at": "2026-10-17T12:00:00Z",
                        "checked_at_prec": "EXACT",
                        "checked_at_precision": "BUCKET(1d)",
                    }
                ]
            }
        )
    )
    repo = PagedRepository()
    with pytest.raises(ValueError, match="Conflicting timestamp precision"):
        await sync_registry(repo, Settings(_env_file=None, config_dir=tmp_path))
    assert [row async for row in repo.iter_all("reg.sources")] == []


async def test_alias_projection_never_promotes_display_names_or_drops_qualifiers(tmp_path: Path) -> None:
    for name, value in {
        "sources": {},
        "registry": {
            "entities": [
                {
                    "entity_id": "example",
                    "names": ["Generic display name"],
                    "aliases": ["Explicit alias"],
                    "qualified_aliases": [{"alias": "Qualified name", "requires_any": ["TOKEN"]}],
                }
            ]
        },
        "dependency_map": {},
        "semantics": {},
        "exchange_addresses": {},
    }.items():
        (tmp_path / f"{name}.yaml").write_text(yaml.safe_dump(value))
    repo = PagedRepository()
    await sync_registry(repo, Settings(_env_file=None, config_dir=tmp_path))
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
async def test_invalid_routing_alias_policy_refused_before_registry_mutation(tmp_path: Path, policy) -> None:
    (tmp_path / "sources.yaml").write_text("sources: [{source_id: replacement}]")
    (tmp_path / "registry.yaml").write_text(
        yaml.safe_dump({"entities": [{"entity_id": "example", **policy}]})
    )
    repo = PagedRepository()
    await repo.put("reg.sources", "existing", {"source_id": "existing"})
    with pytest.raises(ValueError, match="aliases"):
        await sync_registry(repo, Settings(_env_file=None, config_dir=tmp_path))
    assert [row async for row in repo.iter_all("reg.sources")] == [{"source_id": "existing"}]
    assert [row async for row in repo.iter_all("reg.entities")] == []

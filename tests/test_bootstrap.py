"""Offline startup registry coverage against the actual captured configuration."""

import pytest
import yaml

from tank.bootstrap import sync_registry
from tank.settings import Settings
from tank.storage import MemoryRepository


class PagedRepository(MemoryRepository):
    async def list(self, *args, **kwargs):
        raise AssertionError("Registry reconciliation must use uncapped iteration")

    def iter_all(self, collection, *, filters=None, batch_size=1000):
        return super().iter_all(collection, filters=filters, batch_size=7)


async def test_canonical_registry_bootstrap_and_idempotence():
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
            len(set(entity.get("aliases", []) + entity.get("names", []))) for entity in registry["entities"]
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
            assert all(stored[key] == value for key, value in configured.items())
    for configured in settings.load_config("sources")["sources"]:
        stored = await repo.get("reg.sources", configured["source_id"])
        assert stored is not None
        assert all(stored[key] == value for key, value in configured.items())
    source = await repo.get("reg.sources", "S01")
    assert source["name"] == "SEC"
    assert source["config_version"] == "test-config"
    assert source["config_file"] == "sources.yaml"
    assert len(source["config_sha256"]) == 64
    assert await repo.get("reg.sources", "S34")
    assert await sync_registry(repo, settings) == counts
    for table, count in counts.items():
        assert len([row async for row in repo.iter_all(table)]) == count
    wallets = [row async for row in repo.iter_all("reg.exchange_addresses")]
    assert all(row["chain"] is None and row["chain_scope"] == "EVM" for row in wallets)
    assert all(row["commit"] and row["repository"] for row in wallets)


async def test_sync_removes_configured_tail_and_preserves_discovery(tmp_path):
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


async def test_duplicate_source_ids_fail_before_any_mutation(tmp_path):
    (tmp_path / "sources.yaml").write_text("sources: [{source_id: S01}, {source_id: S01}]")
    repo = PagedRepository()
    with pytest.raises(ValueError, match="Duplicate configured registry key"):
        await sync_registry(repo, Settings(_env_file=None, config_dir=tmp_path))
    assert [row async for row in repo.iter_all("reg.sources")] == []

"""Materialize the captured, versioned configuration into queryable registry tables."""

from __future__ import annotations

import hashlib
import json

from tank.settings import Settings
from tank.storage import Repository


def _key(*parts):
    return json.dumps(parts, separators=(",", ":"), ensure_ascii=False)


def _registry_rows(settings):
    rows = {}

    def add(table, key, row, filename, document):
        bucket = rows.setdefault(table, {})
        if key in bucket:
            raise ValueError(f"Duplicate configured registry key: {table}/{key}")
        bucket[key] = {
            **row,
            "registry_managed_by": "tank.bootstrap",
            "registry_storage_key": key,
            "config_version": settings.config_version,
            "code_version": settings.code_version,
            "config_file": filename,
            "config_sha256": hashlib.sha256(settings.read_config_bytes(filename)).hexdigest(),
            "config_schema_version": document.get("version"),
        }

    sources = settings.load_config("sources")
    for source in sources.get("sources", []):
        add("reg.sources", source["source_id"], source, "sources.yaml", sources)
    registry = settings.load_config("registry")
    for section, identifier in (
        ("tokens", "token_id"),
        ("instruments", "instrument_id"),
        ("entities", "entity_id"),
    ):
        for row in registry.get(section, []):
            record = dict(row)
            if section == "entities" and "name" not in record:
                record["name"] = next(iter(record.get("names", [])), record[identifier])
            add("reg." + section, row[identifier], record, "registry.yaml", registry)
    # Names participate in alias lookup as well as explicitly declared aliases.
    for entity in registry.get("entities", []):
        for alias in sorted(set(entity.get("aliases", []) + entity.get("names", []))):
            add(
                "reg.entity_aliases",
                _key(entity["entity_id"], alias),
                {"entity_id": entity["entity_id"], "alias": alias},
                "registry.yaml",
                registry,
            )
    for basket in registry.get("sector_baskets", []):
        identifier = basket.get("basket_id") or basket["sector"]
        add("reg.sector_baskets", identifier, {**basket, "basket_id": identifier}, "registry.yaml", registry)
    dependencies = settings.load_config("dependency_map")
    for edge in dependencies.get("edges", []):
        identifier = edge.get("edge_id") or _key(edge["entity_id"], edge.get("token_id"))
        add(
            "reg.dependency_map",
            identifier,
            {**edge, "edge_id": identifier},
            "dependency_map.yaml",
            dependencies,
        )
    semantics = settings.load_config("semantics")
    for metric in semantics.get("metrics", []):
        add("reg.metric_semantics", metric["metric_key"], metric, "semantics.yaml", semantics)
    addresses = settings.load_config("exchange_addresses")
    for wallet in addresses.get("wallet_lists", []):
        for address in wallet.get("addresses", []):
            # The pinned wallet lists assert EVM scope, not a particular EVM chain.
            record = {k: v for k, v in wallet.items() if k != "addresses"}
            record.update(address=address, chain=wallet.get("chain"), chain_scope="EVM")
            add(
                "reg.exchange_addresses",
                _key(wallet["venue"], wallet.get("chain"), address),
                record,
                "exchange_addresses.yaml",
                addresses,
            )
    # Include empty collections so removal of their last configured row is reconciled.
    for table in (
        "sources",
        "tokens",
        "instruments",
        "entities",
        "entity_aliases",
        "sector_baskets",
        "dependency_map",
        "metric_semantics",
        "exchange_addresses",
    ):
        rows.setdefault("reg." + table, {})
    return rows


async def sync_registry(repo: Repository, settings: Settings) -> dict[str, int]:
    """Atomically sync configured rows, preserving independently discovered registry rows.

    Keys come from declared IDs or natural-key tuples. No source or entity identity is
    inferred from a URL, account, or exchange name. Semantics are reset to their configured
    startup confidence before the current process runs adapter self-tests.
    """
    rows = _registry_rows(settings)  # Validate all input before the first mutation.
    async with repo.lock("tank:registry_sync"):
        for table, configured in rows.items():
            stale = []
            async for old in repo.iter_all(table, filters={"registry_managed_by": "tank.bootstrap"}):
                key = old["registry_storage_key"]
                if key not in configured:
                    stale.append(key)
            for key in stale:
                await repo.delete(table, key)
            for key, record in configured.items():
                old = await repo.get(table, key)
                await repo.put(table, key, {**(old or {}), **record})
    return {table: len(configured) for table, configured in rows.items()}

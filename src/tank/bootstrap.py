"""Materialize the captured, versioned configuration into queryable registry tables."""

from __future__ import annotations

import hashlib
import json

from tank.settings import Settings
from tank.storage import Repository
from tank.time_precision import propagate_timestamp

# Explicit configuration provenance clocks, including nested provider/source evidence.
# Their values belong to the captured configuration, never to this bootstrap run.
CONFIG_TIMESTAMP_FIELDS = frozenset({"verified_at", "resolved_at", "checked_at", "identity_reviewed_at"})


def _config_timestamp_metadata(value):
    if isinstance(value, list):
        return [_config_timestamp_metadata(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: _config_timestamp_metadata(item) for key, item in value.items()}
    for field in CONFIG_TIMESTAMP_FIELDS.intersection(value):
        metadata = propagate_timestamp(value, field).fields(field)
        # Keep the exact captured value/descriptor, including date-only legacy evidence.
        # Only declared provenance is normalized; ISO formatting never establishes it.
        metadata.pop(field)
        result.update(metadata)
    return result


def _key(*parts):
    return json.dumps(parts, separators=(",", ":"), ensure_ascii=False)


def _routing_aliases(entity):
    def nonempty_string(value):
        return isinstance(value, str) and bool(value.strip())

    aliases = entity.get("aliases", [])
    qualified = entity.get("qualified_aliases", [])
    if not isinstance(aliases, list) or not all(nonempty_string(alias) for alias in aliases):
        raise ValueError("Entity aliases must be a list of nonempty strings")
    if not isinstance(qualified, list):
        raise ValueError("Entity qualified_aliases must be a list")
    for rule in qualified:
        if (
            not isinstance(rule, dict)
            or set(rule) != {"alias", "requires_any"}
            or not nonempty_string(rule["alias"])
            or not isinstance(rule["requires_any"], list)
            or not rule["requires_any"]
            or not all(nonempty_string(term) for term in rule["requires_any"])
        ):
            raise ValueError(
                "Qualified aliases require a nonempty alias and nonempty requires_any string list"
            )
    return [{"alias": alias} for alias in sorted(set(aliases))] + qualified


def validate_registry(settings):
    """Read and validate the complete configured registry without database mutation.

    Returns prepared rows so startup sync and pre-activation reload validation use
    exactly the same rules. Call before replacing an active runtime generation.
    """
    rows = {}

    def add(table, key, row, filename, document):
        bucket = rows.setdefault(table, {})
        if key in bucket:
            raise ValueError(f"Duplicate configured registry key: {table}/{key}")
        bucket[key] = {
            **_config_timestamp_metadata(row),
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
    # Routing aliases are explicitly declared; display names are not routing policy.
    for entity in registry.get("entities", []):
        for alias in _routing_aliases(entity):
            add(
                "reg.entity_aliases",
                _key(entity["entity_id"], alias["alias"]),
                {"entity_id": entity["entity_id"], **alias},
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
    inferred from a URL, account, or exchange name. Configured metric meanings are
    authoritative immediately; legacy verification metadata does not gate their use.
    """
    rows = validate_registry(settings)  # Validate all input before the first mutation.
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

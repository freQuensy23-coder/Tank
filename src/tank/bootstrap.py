"""Validate captured configuration before atomically materializing managed registry rows."""

import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Literal, Protocol, Self, assert_never

from pydantic import BaseModel, Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.registry import (
    AggressorSemantics,
    DependencyConfiguration,
    DepthSemantics,
    ExchangeAddressesConfiguration,
    FundingSemantics,
    LiquidationSemantics,
    OpenInterestSemantics,
    PriceSemantics,
    RegisteredMetric,
    RegistryConfiguration,
    RegistryDependency,
    RegistryEntity,
    RegistryInstrument,
    RegistrySectorBasket,
    RegistryToken,
    RegistryVenue,
    RegistryWalletList,
    SemanticsConfiguration,
)
from tank.contracts.sources import SourceConfiguration, SourceDefinition
from tank.time_precision import propagate_timestamp

type RegistryTable = Literal[
    "reg.sources",
    "reg.tokens",
    "reg.instruments",
    "reg.entities",
    "reg.entity_aliases",
    "reg.sector_baskets",
    "reg.dependency_map",
    "reg.metric_semantics",
    "reg.exchange_addresses",
]
type RegistryFile = Literal[
    "sources.yaml", "registry.yaml", "dependency_map.yaml", "semantics.yaml", "exchange_addresses.yaml"
]
CONFIG_TIMESTAMP_FIELDS = frozenset({"verified_at", "resolved_at", "checked_at", "identity_reviewed_at"})


class ManagedRegistryIdentity(DomainModel):
    registry_storage_key: str


class RegistryArtifact(DomainModel):
    config_version: str
    code_version: str
    config_file: RegistryFile
    config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    config_schema_version: str


class RegistryProvenance(RegistryArtifact):
    registry_managed_by: Literal["tank.bootstrap"]
    registry_storage_key: str


class RegistrySettings(Protocol):
    @property
    def config_version(self) -> str: ...
    @property
    def code_version(self) -> str: ...
    def read_config_bytes(self, relative: str) -> bytes: ...
    def load_config_as[T: BaseModel](self, name: str, schema: type[T]) -> T: ...
    def registry(self) -> RegistryConfiguration: ...
    def dependencies(self) -> DependencyConfiguration: ...
    def semantics(self) -> SemanticsConfiguration: ...
    def exchange_addresses(self) -> ExchangeAddressesConfiguration: ...


class RegistryStore(Protocol):
    async def get(self, collection: RegistryTable, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: RegistryTable, key: str, record: dict[str, JsonValue]
    ) -> dict[str, JsonValue]: ...
    async def delete(self, collection: RegistryTable, key: str) -> None: ...
    def iter_all(
        self, collection: RegistryTable, *, filters: dict[str, JsonValue]
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...


class EntityAliasRecord(DomainModel):
    entity_id: str
    alias: str
    requires_any: list[str] | None = None


class SectorBasketRecord(RegistrySectorBasket):
    basket_id: str


class DependencyRecord(RegistryDependency):
    edge_id: str


class ExchangeAddressRecord(DomainModel):
    venue: RegistryVenue
    repository: str
    path: str
    commit: str
    expected_evm_addresses: int
    status: str
    verified_count: int
    address: str
    chain: str | None
    chain_scope: Literal["EVM"]

    @classmethod
    def from_wallet(cls, wallet: RegistryWalletList, address: str) -> Self:
        return cls(
            venue=wallet.venue,
            repository=wallet.repository,
            path=wallet.path,
            commit=wallet.commit,
            expected_evm_addresses=wallet.expected_evm_addresses,
            status=wallet.status,
            verified_count=wallet.verified_count,
            address=address,
            chain=wallet.chain,
            chain_scope="EVM",
        )


class PreparedRow[T: DomainModel](DomainModel):
    record: T
    provenance: RegistryProvenance


class PreparedRegistry(DomainModel):
    sources: list[PreparedRow[SourceDefinition]]
    tokens: list[PreparedRow[RegistryToken]]
    instruments: list[PreparedRow[RegistryInstrument]]
    entities: list[PreparedRow[RegistryEntity]]
    entity_aliases: list[PreparedRow[EntityAliasRecord]]
    sector_baskets: list[PreparedRow[SectorBasketRecord]]
    dependency_map: list[PreparedRow[DependencyRecord]]
    metric_semantics: list[PreparedRow[RegisteredMetric]]
    exchange_addresses: list[PreparedRow[ExchangeAddressRecord]]

    @model_validator(mode="after")
    def unique_storage_keys(self) -> Self:
        _unique_rows(self.sources, "reg.sources")
        _unique_rows(self.tokens, "reg.tokens")
        _unique_rows(self.instruments, "reg.instruments")
        _unique_rows(self.entities, "reg.entities")
        _unique_rows(self.entity_aliases, "reg.entity_aliases")
        _unique_rows(self.sector_baskets, "reg.sector_baskets")
        _unique_rows(self.dependency_map, "reg.dependency_map")
        _unique_rows(self.metric_semantics, "reg.metric_semantics")
        _unique_rows(self.exchange_addresses, "reg.exchange_addresses")
        return self


class RegistrySyncResult(DomainModel):
    sources: int
    tokens: int
    instruments: int
    entities: int
    entity_aliases: int
    sector_baskets: int
    dependency_map: int
    metric_semantics: int
    exchange_addresses: int

    def to_wire(self) -> dict[str, int]:
        return {
            "reg.sources": self.sources,
            "reg.tokens": self.tokens,
            "reg.instruments": self.instruments,
            "reg.entities": self.entities,
            "reg.entity_aliases": self.entity_aliases,
            "reg.sector_baskets": self.sector_baskets,
            "reg.dependency_map": self.dependency_map,
            "reg.metric_semantics": self.metric_semantics,
            "reg.exchange_addresses": self.exchange_addresses,
        }


def _unique_rows[T: DomainModel](rows: list[PreparedRow[T]], table: RegistryTable) -> None:
    keys: set[str] = set()
    for row in rows:
        key = row.provenance.registry_storage_key
        if key in keys:
            raise ValueError(f"Duplicate configured registry key: {table}/{key}")
        keys.add(key)


def _config_timestamp_metadata(value: JsonValue) -> JsonValue:
    """Serialize declared clocks at the registry document boundary, retaining original values."""
    if isinstance(value, list):
        return [_config_timestamp_metadata(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: _config_timestamp_metadata(item) for key, item in value.items()}
    for field in CONFIG_TIMESTAMP_FIELDS.intersection(value):
        metadata = propagate_timestamp(value, field).fields(field)
        metadata.pop(field)
        result.update(metadata)
    return result


def _key(*parts: str | None) -> str:
    return json.dumps(parts, separators=(",", ":"), ensure_ascii=False)


def _routing_aliases(entity: RegistryEntity) -> list[EntityAliasRecord]:
    aliases = [
        EntityAliasRecord(entity_id=entity.entity_id, alias=alias) for alias in sorted(set(entity.aliases))
    ]
    if entity.qualified_aliases is not None:
        aliases.extend(
            EntityAliasRecord(
                entity_id=entity.entity_id,
                alias=rule.alias,
                requires_any=rule.requires_any,
            )
            for rule in entity.qualified_aliases
        )
    return aliases


def _artifact(settings: RegistrySettings, filename: RegistryFile, version: str) -> RegistryArtifact:
    return RegistryArtifact(
        config_version=settings.config_version,
        code_version=settings.code_version,
        config_file=filename,
        config_schema_version=version,
        config_sha256=hashlib.sha256(settings.read_config_bytes(filename)).hexdigest(),
    )


def _row[T: DomainModel](
    schema: type[PreparedRow[T]],
    record: T,
    key: str,
    artifact: RegistryArtifact,
) -> PreparedRow[T]:
    return schema(
        record=record,
        provenance=RegistryProvenance(
            registry_managed_by="tank.bootstrap",
            registry_storage_key=key,
            config_version=artifact.config_version,
            code_version=artifact.code_version,
            config_file=artifact.config_file,
            config_sha256=artifact.config_sha256,
            config_schema_version=artifact.config_schema_version,
        ),
    )


def _entity_record(entity: RegistryEntity) -> RegistryEntity:
    if "name" in entity.model_fields_set:
        return entity
    display = entity.names[0] if entity.names else entity.entity_id
    return RegistryEntity.model_validate(entity.model_copy(update={"name": display}))


def validate_registry(settings: RegistrySettings) -> PreparedRegistry:
    """Prepare complete typed rows without database mutation before generation activation."""
    sources = settings.load_config_as("sources", SourceConfiguration)
    registry = settings.registry()
    dependencies = settings.dependencies()
    semantics = settings.semantics()
    addresses = settings.exchange_addresses()
    sources_artifact = _artifact(settings, "sources.yaml", sources.version)
    registry_artifact = _artifact(settings, "registry.yaml", registry.version)
    dependency_artifact = _artifact(settings, "dependency_map.yaml", dependencies.version)
    semantics_artifact = _artifact(settings, "semantics.yaml", semantics.version)
    addresses_artifact = _artifact(settings, "exchange_addresses.yaml", addresses.version)
    aliases = [alias for entity in registry.entities for alias in _routing_aliases(entity)]
    baskets = [
        SectorBasketRecord.model_validate(
            basket.model_copy(
                update={
                    "basket_id": basket.sector if basket.basket_id is None else basket.basket_id,
                }
            ).model_dump(mode="python")
        )
        for basket in registry.sector_baskets
    ]
    edges = [
        DependencyRecord.model_validate(
            edge.model_copy(
                update={
                    "edge_id": _key(edge.entity_id, edge.token_id) if edge.edge_id is None else edge.edge_id,
                }
            ).model_dump(mode="python")
        )
        for edge in dependencies.edges
    ]
    wallets = [
        ExchangeAddressRecord.from_wallet(wallet, address)
        for wallet in addresses.wallet_lists
        for address in wallet.addresses
    ]
    prepared = PreparedRegistry(
        sources=[
            _row(
                PreparedRow[SourceDefinition],
                source,
                source.source_id,
                sources_artifact,
            )
            for source in sources.sources
        ],
        tokens=[
            _row(PreparedRow[RegistryToken], token, token.token_id, registry_artifact)
            for token in registry.tokens
        ],
        instruments=[
            _row(
                PreparedRow[RegistryInstrument],
                instrument,
                instrument.instrument_id,
                registry_artifact,
            )
            for instrument in registry.instruments
        ],
        entities=[
            _row(
                PreparedRow[RegistryEntity],
                _entity_record(entity),
                entity.entity_id,
                registry_artifact,
            )
            for entity in registry.entities
        ],
        entity_aliases=[
            _row(
                PreparedRow[EntityAliasRecord],
                alias,
                _key(alias.entity_id, alias.alias),
                registry_artifact,
            )
            for alias in aliases
        ],
        sector_baskets=[
            _row(
                PreparedRow[SectorBasketRecord],
                basket,
                basket.basket_id,
                registry_artifact,
            )
            for basket in baskets
        ],
        dependency_map=[
            _row(
                PreparedRow[DependencyRecord],
                edge,
                edge.edge_id,
                dependency_artifact,
            )
            for edge in edges
        ],
        metric_semantics=[
            _row(
                PreparedRow[RegisteredMetric],
                metric,
                metric.metric_key,
                semantics_artifact,
            )
            for metric in semantics.metrics
        ],
        exchange_addresses=[
            _row(
                PreparedRow[ExchangeAddressRecord],
                wallet,
                _key(wallet.venue, wallet.chain, wallet.address),
                addresses_artifact,
            )
            for wallet in wallets
        ],
    )
    _preflight(prepared)
    return prepared


def _validate_documents[T: DomainModel](rows: list[PreparedRow[T]]) -> None:
    for row in rows:
        _document(row)


def _preflight(prepared: PreparedRegistry) -> None:
    _validate_documents(prepared.sources)
    _validate_documents(prepared.tokens)
    _validate_documents(prepared.instruments)
    _validate_documents(prepared.entities)
    _validate_documents(prepared.entity_aliases)
    _validate_documents(prepared.sector_baskets)
    _validate_documents(prepared.dependency_map)
    _validate_documents(prepared.metric_semantics)
    _validate_documents(prepared.exchange_addresses)


def _document[T: DomainModel](row: PreparedRow[T]) -> dict[str, JsonValue]:
    raw = decode_json(row.record.model_dump_json(exclude_unset=True))
    record = _config_timestamp_metadata(raw)
    metadata = decode_json(row.provenance.model_dump_json())
    if not isinstance(record, dict) or not isinstance(metadata, dict):
        raise TypeError("Managed registry records must serialize as objects")
    return {**record, **metadata}


def _schema_fields(*schemas: type[DomainModel]) -> frozenset[str]:
    return frozenset(name for schema in schemas for name in schema.model_fields)


def _owned_fields(table: RegistryTable) -> frozenset[str]:
    provenance = _schema_fields(RegistryProvenance)
    match table:
        case "reg.sources":
            return provenance | _schema_fields(SourceDefinition)
        case "reg.tokens":
            return provenance | _schema_fields(RegistryToken)
        case "reg.instruments":
            return provenance | _schema_fields(RegistryInstrument)
        case "reg.entities":
            return provenance | _schema_fields(RegistryEntity)
        case "reg.entity_aliases":
            return provenance | _schema_fields(EntityAliasRecord)
        case "reg.sector_baskets":
            return provenance | _schema_fields(SectorBasketRecord)
        case "reg.dependency_map":
            return provenance | _schema_fields(DependencyRecord)
        case "reg.metric_semantics":
            return provenance | _schema_fields(
                AggressorSemantics,
                DepthSemantics,
                FundingSemantics,
                LiquidationSemantics,
                OpenInterestSemantics,
                PriceSemantics,
            )
        case "reg.exchange_addresses":
            return provenance | _schema_fields(ExchangeAddressRecord)
        case _:
            assert_never(table)


async def _sync_rows[T: DomainModel](
    repo: RegistryStore,
    table: RegistryTable,
    rows: list[PreparedRow[T]],
) -> int:
    """Merge opaque independently discovered extensions only at the persistence adapter."""
    configured = {row.provenance.registry_storage_key for row in rows}
    stale: list[str] = []
    async for old in repo.iter_all(table, filters={"registry_managed_by": "tank.bootstrap"}):
        key = ManagedRegistryIdentity.model_validate(
            {"registry_storage_key": old["registry_storage_key"]}
        ).registry_storage_key
        if key not in configured:
            stale.append(key)
    for key in stale:
        await repo.delete(table, key)
    for row in rows:
        key = row.provenance.registry_storage_key
        document = _document(row)
        existing = await repo.get(table, key)
        if existing is None:
            await repo.put(table, key, document)
        else:
            owned = _owned_fields(table)
            unowned = {name: value for name, value in existing.items() if name not in owned}
            await repo.put(table, key, {**unowned, **document})
    return len(rows)


async def sync_registry(repo: RegistryStore, settings: RegistrySettings) -> RegistrySyncResult:
    """Atomically sync validated captured rows while retaining independently discovered rows."""
    prepared = validate_registry(settings)
    async with repo.lock("tank:registry_sync"), repo.transaction():
        return RegistrySyncResult(
            sources=await _sync_rows(repo, "reg.sources", prepared.sources),
            tokens=await _sync_rows(repo, "reg.tokens", prepared.tokens),
            instruments=await _sync_rows(repo, "reg.instruments", prepared.instruments),
            entities=await _sync_rows(repo, "reg.entities", prepared.entities),
            entity_aliases=await _sync_rows(repo, "reg.entity_aliases", prepared.entity_aliases),
            sector_baskets=await _sync_rows(repo, "reg.sector_baskets", prepared.sector_baskets),
            dependency_map=await _sync_rows(repo, "reg.dependency_map", prepared.dependency_map),
            metric_semantics=await _sync_rows(repo, "reg.metric_semantics", prepared.metric_semantics),
            exchange_addresses=await _sync_rows(repo, "reg.exchange_addresses", prepared.exchange_addresses),
        )

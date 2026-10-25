"""Complete synthetic registry configuration for the autonomous runtime fixture."""

import hashlib
from datetime import datetime
from pathlib import Path

from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.registry import (
    DependencyConfiguration,
    ExchangeAddressesConfiguration,
    FundamentalsEvidence,
    FundamentalsMetrics,
    LendingPoolCatalog,
    RegistryConfiguration,
    RegistryEntity,
    RegistryInstrument,
    RegistryToken,
    SemanticsConfiguration,
)
from tank.util import iso


def document(
    value: RegistryConfiguration
    | DependencyConfiguration
    | SemanticsConfiguration
    | ExchangeAddressesConfiguration,
) -> dict[str, JsonValue]:
    decoded = decode_json(value.model_dump_json())
    if not isinstance(decoded, dict):
        raise ValueError("Fixture configuration must serialize as a JSON object")
    return decoded


class BootstrapFixture(DomainModel):
    registry: RegistryConfiguration
    dependency_map: DependencyConfiguration
    semantics: SemanticsConfiguration
    exchange_addresses: ExchangeAddressesConfiguration

    def documents(self) -> dict[str, dict[str, JsonValue]]:
        return {
            "registry": document(self.registry),
            "dependency_map": document(self.dependency_map),
            "semantics": document(self.semantics),
            "exchange_addresses": document(self.exchange_addresses),
        }


def bootstrap_fixture(directory: Path, observed_at: datetime) -> BootstrapFixture:
    directory.mkdir(mode=0o700, exist_ok=False)
    catalog_path = directory / "empty-lending-pools.json"
    catalog_bytes = b"[]\n"
    catalog_path.write_bytes(catalog_bytes)
    registry_path = directory / "registry.json"
    token = RegistryToken(
        token_id="T",
        symbol="T",
        name="Synthetic T",
        project="Synthetic runtime fixture",
        entity_id="synthetic-venue",
        identity="SYNTHETIC_OFFLINE_FIXTURE",
        contracts=[],
        native=False,
        coingecko_id="SYNTHETIC_NO_PROVIDER_ID",
        unlock_slug=None,
        supply_unavailable_reason="SYNTHETIC_FIXTURE_HAS_NO_SUPPLY_PROVIDER",
        official_accounts=[],
        sector="SYNTHETIC",
        defillama_protocol_slugs=[],
        defillama_protocol_slug=None,
        fee_protocol_slug=None,
        fundamentals_status="NOT_SUPPORTED",
        fundamentals_evidence=FundamentalsEvidence(
            catalog_url=registry_path.as_uri(),
            summary_id=None,
            verified_at=iso(observed_at),
            metrics=FundamentalsMetrics(
                dailyFees="NOT_SUPPORTED",
                dailyRevenue="NOT_SUPPORTED",
                dailyHoldersRevenue="NOT_SUPPORTED",
            ),
            reason="SYNTHETIC_FIXTURE_HAS_NO_FUNDAMENTALS_PROVIDER",
        ),
        defillama_fee_scope="NOT_SUPPORTED",
        native_chain_id=None,
        defillama_chain_name=None,
        chain_identity_evidence=None,
        fundamentals_inference_limits=["Synthetic fixture contains no provider observations"],
        supplementary_chain_fee_slug=None,
        supplementary_chain_fee_warning=None,
    )
    entities = [
        RegistryEntity(
            entity_id="binance",
            kind="EXCHANGE",
            name="Binance",
            names=["Binance"],
            aliases=[],
            tokens=[],
            official_sources=[],
            layers_served=["LIQUIDITY_EXECUTION"],
            competes_with=[],
            chains=[],
            qualified_aliases=[],
            authority_evidence=[],
            identity_basis="SYNTHETIC_OFFLINE_FIXTURE_REFERENT",
            identity_evidence=[registry_path.as_uri()],
            identity_reviewed_at=None,
            source_mapping_basis="Synthetic fixture defines no authoritative publisher relationship",
        ),
        RegistryEntity(
            entity_id="synthetic-venue",
            kind="PROTOCOL",
            name="Synthetic venue",
            names=["Synthetic venue"],
            aliases=[],
            tokens=["T"],
            official_sources=[],
            layers_served=["LIQUIDITY_EXECUTION"],
            competes_with=[],
            chains=[],
            qualified_aliases=[],
            authority_evidence=[],
            identity_basis="SYNTHETIC_OFFLINE_FIXTURE",
            identity_evidence=[registry_path.as_uri()],
            identity_reviewed_at=None,
            source_mapping_basis="Synthetic fixture defines no authoritative publisher relationship",
        ),
    ]
    registry = RegistryConfiguration(
        version="synthetic-runtime-fixture-v1",
        tokens=[token],
        instruments=[
            RegistryInstrument(
                instrument_id="binance:spot:T",
                token_id="T",
                venue="binance",
                market_type="SPOT",
                venue_symbol="TUSD",
                quote_asset="USD",
                active=True,
                contract_multiplier=None,
                tick_size=None,
                lot_size=None,
                funding_interval_h=None,
                metadata_status="SYNTHETIC_NOT_DISCOVERED",
                resolved_at=None,
                coverage_verified_date=observed_at.date().isoformat(),
            )
        ],
        entities=entities,
        sector_baskets=[],
        x_accounts=[],
        lending_pool_catalog=LendingPoolCatalog(
            source_url=catalog_path.as_uri(),
            captured_at=iso(observed_at),
            source_sha256=hashlib.sha256(catalog_bytes).hexdigest(),
            pools=[],
        ),
    )
    registry_path.write_text(registry.model_dump_json(indent=2) + "\n")
    return BootstrapFixture(
        registry=registry,
        dependency_map=DependencyConfiguration(version="synthetic-runtime-fixture-v1", edges=[]),
        semantics=SemanticsConfiguration(version="synthetic-runtime-fixture-v1", metrics=[]),
        exchange_addresses=ExchangeAddressesConfiguration(
            version="synthetic-runtime-fixture-v1",
            wallet_lists=[],
            unavailable={
                "binance": "SYNTHETIC_FIXTURE_HAS_NO_WALLET_PROVIDER",
                "coinbase": "SYNTHETIC_FIXTURE_HAS_NO_WALLET_PROVIDER",
                "okx": "SYNTHETIC_FIXTURE_HAS_NO_WALLET_PROVIDER",
                "bybit": "SYNTHETIC_FIXTURE_HAS_NO_WALLET_PROVIDER",
                "kraken": "SYNTHETIC_FIXTURE_HAS_NO_WALLET_PROVIDER",
                "hyperliquid": "SYNTHETIC_FIXTURE_HAS_NO_WALLET_PROVIDER",
            },
            setup_required="Synthetic offline fixture has no wallet provider",
        ),
    )

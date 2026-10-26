"""Actual Runtime composition with owned resources and refused unrelated HTTP."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from shutil import copytree

import pytest
import respx
from pydantic_settings import SettingsConfigDict
from source_support import source_definition

from tank import runtime_composition
from tank.contracts.json import encode_json
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.registry import (
    DependencyConfiguration,
    ExchangeAddressesConfiguration,
    LendingPoolCatalog,
    RegistryConfiguration,
    SemanticsConfiguration,
)
from tank.contracts.sources import SourceConfiguration
from tank.runtime import Runtime
from tank.runtime_failure_repository import RepositoryRuntimeFailures
from tank.settings import Settings, resolve_versions
from tank.storage import MemoryRepository, PostgresRepository, Repository
from tank.time_precision import collection_clock
from tank.util import clock_iso, sha256


class RuntimeFailureFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def keep_fixture_logging(settings: Settings) -> None:
    return None


def runtime_failure_config(root: Path) -> Path:
    config = root / "config"
    config.mkdir(parents=True, exist_ok=True)
    baseline = Path(__file__).resolve().parents[1] / "config"
    for name in ("thresholds.yaml", "api_limits.yaml", "openma.yaml"):
        (config / name).write_bytes((baseline / name).read_bytes())
    copytree(baseline / "skills", config / "skills", dirs_exist_ok=True)
    catalog = encode_json({"pools": []})
    (config / "fixture-pools.json").write_bytes(catalog)
    registry = RegistryConfiguration(
        version="runtime-failure-fixture",
        tokens=[],
        instruments=[],
        entities=[],
        sector_baskets=[],
        x_accounts=[],
        lending_pool_catalog=LendingPoolCatalog(
            source_url="fixture:runtime-failure-empty-pools",
            captured_at=clock_iso(),
            source_sha256=sha256(catalog),
            pools=[],
        ),
    )
    sources = SourceConfiguration(
        version="runtime-failure-fixture",
        contact_email="runtime@example.test",
        sources=[source_definition({"source_id": "S30", "name": "Synthetic S30", "feeds": []})],
    )
    semantics = SemanticsConfiguration(version="runtime-failure-fixture", metrics=[])
    dependencies = DependencyConfiguration(version="runtime-failure-fixture", edges=[])
    wallets = ExchangeAddressesConfiguration(
        version="runtime-failure-fixture",
        wallet_lists=[],
        unavailable={},
        setup_required="This synthetic failure fixture declares no wallet catalogs",
    )
    for name, document in (
        ("registry", registry),
        ("sources", sources),
        ("semantics", semantics),
        ("dependency_map", dependencies),
        ("exchange_addresses", wallets),
    ):
        (config / (name + ".yaml")).write_text(document.model_dump_json())
    return config


@asynccontextmanager
async def runtime_failure_fixture(
    repo: MemoryRepository | PostgresRepository, tmp_path: Path, *, ops_progress_enabled: bool
) -> AsyncIterator[Runtime]:
    settings = resolve_versions(
        RuntimeFailureFixtureSettings(
            config_dir=runtime_failure_config(tmp_path),
            data_dir=tmp_path / "data",
            backup_dir=tmp_path / "backup",
            database_url="postgresql://synthetic.invalid/tank",
            openma_url="http://native.invalid",
            telegram_token="synthetic-telegram-token",
            telegram_alerts_chat="synthetic-alerts",
            telegram_ops_chat="synthetic-ops",
            openma_api_key="synthetic-openma-key",
            codex_gateway_key="synthetic-gateway-key",
            codex_auth_path=tmp_path / "synthetic-auth.json",
            foundry_api_key="synthetic-foundry-key",
            ops_progress_enabled=ops_progress_enabled,
        )
    )
    with pytest.MonkeyPatch.context() as patch, respx.mock(assert_all_mocked=True, assert_all_called=False):
        patch.setattr(runtime_composition, "configure_logging", keep_fixture_logging)
        runtime = runtime_composition.build_runtime(repo, settings)
        try:
            runtime.validate()
            yield runtime
        finally:
            await runtime.close()


def bind_worker_fragment_failure_store(
    runtime: Runtime, repo: Repository, versions: ResolvedVersionIdentity
) -> None:
    """Bind real failure persistence for existing focused worker fragments."""
    runtime.failures = RepositoryRuntimeFailures(repo, versions, collection_clock)
    runtime._failure_reconcile_attempted_at = None

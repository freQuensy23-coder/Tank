"""Production composition shares one required scheduler and closes its owned resources."""

from pathlib import Path
from types import MethodType

import pytest
from pydantic_settings import SettingsConfigDict

from tank import runtime_composition
from tank.analysis.page_access import LivePageAccess
from tank.event_tools import EventToolService
from tank.http_admission import ConfiguredHttpAdmission
from tank.request_budget import MemoryBudgetBackend, RequestLedger, build_budget_backend
from tank.runtime_failure_repository import RepositoryRuntimeFailures
from tank.settings import Settings, resolve_versions
from tank.sources import SourceService
from tank.storage import MemoryRepository, PostgresRepository


class CompositionFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.asyncio
async def test_production_composition_binds_the_real_scheduler_and_owns_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_global_logging(settings: Settings) -> None:
        return None

    monkeypatch.setattr(runtime_composition, "configure_logging", no_global_logging)
    settings = resolve_versions(
        CompositionFixtureSettings(
            data_dir=tmp_path / "data",
            backup_dir=tmp_path / "backup",
            telegram_token="synthetic-telegram-token",
            telegram_alerts_chat="synthetic-alerts",
            telegram_ops_chat="synthetic-ops",
            openma_api_key="synthetic-openma-key",
            codex_gateway_key="synthetic-gateway-key",
        )
    )
    repo = MemoryRepository()
    runtime = runtime_composition.build_runtime(repo, settings)
    try:
        dispatch = runtime.sessions.tool_dispatch
        assert isinstance(dispatch, MethodType)
        tools = dispatch.__self__
        assert isinstance(tools, EventToolService)
        assert tools.scheduler is runtime.scheduler
        assert isinstance(runtime.budget_backend, MemoryBudgetBackend)
        assert isinstance(runtime.sources, SourceService)
        assert runtime.sources.budgets is runtime.budget_scope
        assert isinstance(runtime.market.admission, ConfiguredHttpAdmission)
        assert runtime.market.admission.budgets is runtime.budget_scope
        assert runtime.market.admission.versions == settings.require_versions()
        assert isinstance(tools.pages, LivePageAccess)
        assert tools.pages.admission is runtime.market.admission
        assert isinstance(runtime.failures, RepositoryRuntimeFailures)
        assert runtime.failures.store is repo
        assert runtime.failures.versions == settings.require_versions()
        assert runtime.budget_binding is None
        with pytest.raises(RuntimeError, match="has not been bound"):
            await runtime.budget_scope.wait_host("example.invalid")
        assert runtime.scheduler.market is runtime.market
        assert runtime.scheduler.truth is runtime.truth
        assert runtime.scheduler.versions == settings.require_versions()
        assert runtime.scheduler.thresholds == settings.thresholds()
        assert runtime.scheduler.closing is False
    finally:
        await runtime.close()
    assert runtime.scheduler.closing is True and not runtime.scheduler.background
    assert runtime.sessions.http.is_closed and runtime.market.http.is_closed


def test_production_budget_backend_uses_explicit_repository_kind_without_connecting() -> None:
    memory = build_budget_backend(MemoryRepository(), owner="tank-core")
    assert isinstance(memory, MemoryBudgetBackend)
    postgres = build_budget_backend(
        PostgresRepository("postgresql://synthetic.invalid/tank"), owner="tank-core"
    )
    assert isinstance(postgres, RequestLedger)
    assert postgres.database_url == "postgresql://synthetic.invalid/tank"
    assert postgres.owner == "tank-core"
    assert postgres.connection is None

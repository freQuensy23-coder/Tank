"""Production composition shares one required scheduler and closes its owned resources."""

from pathlib import Path
from types import MethodType

import pytest
from pydantic_settings import SettingsConfigDict

from tank import runtime_composition
from tank.event_tools import EventToolService
from tank.settings import Settings, resolve_versions
from tank.storage import MemoryRepository


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
        assert runtime.scheduler.market is runtime.market
        assert runtime.scheduler.truth is runtime.truth
        assert runtime.scheduler.versions == settings.require_versions()
        assert runtime.scheduler.thresholds == settings.thresholds()
        assert runtime.scheduler.closing is False
    finally:
        await runtime.close()
    assert runtime.scheduler.closing is True and not runtime.scheduler.background
    assert runtime.sessions.http.is_closed and runtime.market.http.is_closed

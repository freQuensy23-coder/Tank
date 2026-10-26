"""Real worker composition with the existing controlled acquisition and numerical ports."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
import respx
from pydantic_settings import SettingsConfigDict

from tank import runtime_composition
from tank.agents import SessionService
from tank.analysis.truth import MarketTruthService
from tank.runtime import Runtime
from tank.runtime_market import RuntimeMarketPort
from tank.scheduler import Scheduler
from tank.settings import Settings, resolve_versions
from tank.storage import StorageBackend


class RefreshFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def preserve_logging(settings: Settings) -> None:
    return None


@asynccontextmanager
async def refresh_worker(
    repo: StorageBackend,
    settings: Settings,
    market: RuntimeMarketPort,
    truth: MarketTruthService,
    scheduler: Scheduler,
    sessions: SessionService,
) -> AsyncIterator[Runtime]:
    configuration = resolve_versions(
        RefreshFixtureSettings(
            config_dir=settings.config_dir,
            data_dir=settings.data_dir,
            database_url=settings.database_url,
            backup_dir=settings.data_dir / "runtime-backups",
            openma_url=settings.openma_url,
            openma_model=settings.openma_model,
            telegram_token="synthetic-runtime-telegram-token",
            telegram_alerts_chat="synthetic-runtime-alerts",
            telegram_ops_chat="synthetic-runtime-ops",
            openma_api_key="synthetic-runtime-native-key",
            codex_gateway_key="synthetic-runtime-gateway-key",
            codex_auth_path=settings.data_dir / "unread-runtime-auth.json",
            workers=settings.workers,
            replay=settings.replay,
            ops_progress_enabled=settings.ops_progress_enabled,
            shutdown_grace_s=settings.shutdown_grace_s,
        )
    )
    with (
        pytest.MonkeyPatch.context() as patch,
        respx.mock(assert_all_mocked=True, assert_all_called=False),
    ):
        patch.setattr(runtime_composition, "configure_logging", preserve_logging)
        runtime = runtime_composition.build_runtime(repo, configuration)
        original_market = runtime.market
        original_sessions = runtime.sessions
        original_scheduler = runtime.scheduler
        runtime.market = market
        runtime.truth = truth
        runtime.scheduler = scheduler
        runtime.sessions = sessions
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            yield runtime
        finally:
            stop.set()
            await worker
            await runtime.close()
            await original_scheduler.close()
            await original_sessions.close()
            await original_market.close()

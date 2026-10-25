"""The reaction test composition owns real scheduling and capture dependencies."""

import asyncio
from pathlib import Path

import pytest
from pydantic_settings import SettingsConfigDict
from scheduler_support import operational_reaction_services
from source_support import fixture_request_budgets, fixture_versions

from tank.analysis.truth import MarketTruthService
from tank.market.service import MarketService
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.mark.asyncio
async def test_operational_reaction_fixture_owns_actual_services_and_drains_tasks(tmp_path: Path) -> None:
    class FixtureSettings(Settings):
        model_config = SettingsConfigDict(env_file=None)

    settings = FixtureSettings(data_dir=tmp_path)
    started, finished = asyncio.Event(), asyncio.Event()

    async def active_work() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finished.set()

    async with operational_reaction_services(
        MemoryRepository(), settings, versions=fixture_versions(), budgets=fixture_request_budgets()
    ) as services:
        assert isinstance(services.market, MarketService)
        assert isinstance(services.truth, MarketTruthService)
        assert services.scheduler.market is services.market
        assert services.scheduler.truth is services.truth
        assert services.scheduler.thresholds == settings.thresholds()
        services.scheduler.launch("owned-lifetime-proof", active_work)
        await asyncio.wait_for(started.wait(), timeout=1)
    assert finished.is_set()
    assert services.scheduler.closing and not services.scheduler.background
    assert services.market.http.is_closed

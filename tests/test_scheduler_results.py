"""Condition evaluation consumes an actual clock and returns observed execution outcomes."""

import pytest
from pydantic_settings import SettingsConfigDict
from source_support import fixture_versions

from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp, collection_clock
from tank.util import utcnow


class ResultSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


async def test_missing_evaluation_clock_refuses_without_inventing_now_or_writing() -> None:
    repo = MemoryRepository()
    settings = ResultSettings()
    scheduler = Scheduler(repo, settings, None, None, versions=fixture_versions())
    with pytest.raises(ValueError, match="actual cutoff"):
        await scheduler.eval_conditions(Timestamp(None, None, "NOT_CAPTURED"))
    assert await repo.list("ops.jobs") == []
    assert await repo.list("ana.watch_conditions") == []
    await scheduler.close()


async def test_actual_empty_evaluation_has_three_explicit_empty_outcomes() -> None:
    repo = MemoryRepository()
    settings = ResultSettings()
    scheduler = Scheduler(repo, settings, None, None, versions=fixture_versions())
    result = await scheduler.eval_conditions(collection_clock(utcnow()))
    assert result.evaluated == [] and result.fired == [] and result.deferred == []
    assert result.model_dump(mode="json") == {"evaluated": [], "fired": [], "deferred": []}
    assert await repo.list("ops.jobs") == []
    await scheduler.close()

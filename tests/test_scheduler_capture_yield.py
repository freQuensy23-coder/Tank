"""A running reaction capture yields to Market Truth (lead ruling 2026-10-21)."""

import asyncio
from datetime import timedelta

import pytest
from source_support import fixture_versions

import tank.scheduler as scheduler_module
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import utcnow


async def test_capture_waits_while_market_truth_runs_and_logs_it(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    repo = MemoryRepository()
    truth = await repo.enqueue("RUN_MARKET_TRUTH", {}, "mt", priority=1)
    await repo.claim_job()
    polls = []
    real_sleep = asyncio.sleep

    async def fast_sleep(seconds) -> None:
        polls.append(seconds)
        if len(polls) == 3:
            await repo.finish_job(truth["job_id"])
        await real_sleep(0)

    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fast_sleep)
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    payload = {"event_id": "EVT-1", "token_id": "bitcoin:BTC", "checkpoint": "POST_1H"}
    with caplog.at_level(logging.INFO, logger="tank.scheduler"):
        await scheduler._yield_to_market_truth(payload, utcnow())
    assert len(polls) == 3
    assert "yielded 6 s to RUN_MARKET_TRUTH" in caplog.text


async def test_capture_near_expiry_does_not_yield(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = MemoryRepository()
    await repo.enqueue("RUN_MARKET_TRUTH", {}, "mt", priority=1)
    await repo.claim_job()

    async def no_sleep(seconds) -> None:
        raise AssertionError("a capture near its expiry must not wait")

    monkeypatch.setattr(scheduler_module.asyncio, "sleep", no_sleep)
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    checkpoint = utcnow() - timedelta(hours=5, minutes=45)
    await scheduler._yield_to_market_truth({"event_id": "EVT-1"}, checkpoint)

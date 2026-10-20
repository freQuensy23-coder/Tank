import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tank.capture import CaptureService


def settings(**overrides):
    return SimpleNamespace(capture_only=True, replay=False, **overrides)


@pytest.mark.asyncio
async def test_capture_starts_after_registry_and_partitions_without_model_jobs(monkeypatch):
    order = []
    stop = asyncio.Event()
    repo = SimpleNamespace(maintain_partitions=AsyncMock(side_effect=lambda: order.append("partitions")))

    async def sync(actual_repo, actual_settings):
        assert actual_repo is repo
        assert actual_settings.capture_only
        order.append("registry")

    monkeypatch.setattr("tank.capture.sync_registry", sync)

    async def source_run(flag):
        order.append("sources")
        assert order[:2] == ["registry", "partitions"]
        await flag.wait()

    async def market_run(flag):
        order.append("market")
        stop.set()
        await flag.wait()

    sources = SimpleNamespace(run=source_run, close=AsyncMock())
    market = SimpleNamespace(run=market_run, close=AsyncMock())
    service = CaptureService(repo, settings(), sources=sources, market=market)
    await service.run(stop)
    assert {"sources", "market"} <= set(order)
    sources.close.assert_awaited_once()
    market.close.assert_awaited_once()
    assert not hasattr(service, "gateway")
    assert not hasattr(service, "handlers")


@pytest.mark.asyncio
async def test_startup_failure_closes_capture_resources(monkeypatch):
    monkeypatch.setattr("tank.capture.sync_registry", AsyncMock(side_effect=ValueError("bad registry")))
    source = SimpleNamespace(run=AsyncMock(), close=AsyncMock())
    market = SimpleNamespace(run=AsyncMock(), close=AsyncMock())
    service = CaptureService(SimpleNamespace(), settings(), sources=source, market=market)
    with pytest.raises(ValueError, match="bad registry"):
        await service.run(asyncio.Event())
    source.run.assert_not_awaited()
    market.run.assert_not_awaited()
    source.close.assert_awaited_once()
    market.close.assert_awaited_once()


@pytest.mark.parametrize(
    "config", [SimpleNamespace(capture_only=False), SimpleNamespace(capture_only=True, replay=True)]
)
def test_capture_requires_explicit_live_phase(config):
    with pytest.raises(ValueError):
        CaptureService(SimpleNamespace(), config, sources=object(), market=object())


def test_capture_is_not_a_public_service_command():
    from click.testing import CliRunner

    from tank.cli import main

    result = CliRunner().invoke(main, ["capture"])
    assert result.exit_code == 2
    assert "No such command 'capture'" in result.output
    assert "capture" not in main.commands


@pytest.mark.asyncio
async def test_capture_backup_retries_and_runs_once_per_utc_day(monkeypatch):
    stop = asyncio.Event()
    instants = iter(
        datetime.fromisoformat(value).replace(tzinfo=UTC)
        for value in (
            "2026-10-18T01:59:30",  # Before the configured backup hour.
            "2026-10-18T02:00:00",  # A failed attempt must be retried.
            "2026-10-18T02:00:30",  # Successful creation completes this day.
            "2026-10-18T23:59:30",  # No repeated archive scan after success.
            "2026-10-19T00:00:00",  # The next day still honors the backup hour.
            "2026-10-19T02:00:00",  # A restart-created archive can be verified.
            "2026-10-19T02:00:30",
        )
    )
    monkeypatch.setattr("tank.capture.utcnow", lambda: next(instants))
    maintenance = AsyncMock(
        side_effect=[
            {"status": "deferred"},
            OSError("backup disk temporarily unavailable"),
            {"status": "created"},
            {"status": "deferred"},
            {"status": "verified"},
        ]
    )
    service = CaptureService(
        SimpleNamespace(), settings(), sources=object(), market=object(), maintenance=maintenance
    )
    ticks = 0

    async def pause(flag, seconds):
        nonlocal ticks
        assert flag is stop and seconds == 30
        ticks += 1
        if ticks == 7:
            stop.set()

    monkeypatch.setattr(service, "_pause", pause)
    await service._housekeeping(stop)
    assert [call.args[0].isoformat() for call in maintenance.await_args_list] == [
        "2026-10-18T01:59:30+00:00",
        "2026-10-18T02:00:00+00:00",
        "2026-10-18T02:00:30+00:00",
        "2026-10-19T00:00:00+00:00",
        "2026-10-19T02:00:00+00:00",
    ]


@pytest.mark.asyncio
async def test_capture_shutdown_cancels_backup_and_closes_collection(monkeypatch):
    monkeypatch.setattr("tank.capture.sync_registry", AsyncMock())
    stop = asyncio.Event()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def maintenance(now):
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    async def collect(flag):
        await flag.wait()

    sources = SimpleNamespace(run=collect, close=AsyncMock())
    market = SimpleNamespace(run=collect, close=AsyncMock())
    service = CaptureService(
        SimpleNamespace(), settings(), sources=sources, market=market, maintenance=maintenance
    )
    task = asyncio.create_task(service.run(stop))
    await asyncio.wait_for(started.wait(), 1)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert cancelled.is_set()
    sources.close.assert_awaited_once()
    market.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_configured_capture_uses_existing_backup_procedure_only(monkeypatch, tmp_path):
    from tank.scheduler import Scheduler

    stop = asyncio.Event()

    async def maintenance(scheduler, now):
        assert scheduler.repo is repo
        assert scheduler.settings is config
        assert scheduler.market is None and scheduler.truth is None
        stop.set()
        return {"status": "verified"}

    monkeypatch.setattr("tank.capture.sync_registry", AsyncMock())
    monkeypatch.setattr(Scheduler, "maintenance", maintenance)
    run = AsyncMock(side_effect=AssertionError("event scheduler must not start"))
    tick = AsyncMock(side_effect=AssertionError("event jobs must not start"))
    monkeypatch.setattr(Scheduler, "run", run)
    monkeypatch.setattr(Scheduler, "tick", tick)
    repo, config = SimpleNamespace(), settings(backup_dir=tmp_path)

    async def collect(flag):
        await flag.wait()

    service = CaptureService(
        repo, config, sources=SimpleNamespace(run=collect), market=SimpleNamespace(run=collect)
    )
    await asyncio.wait_for(service.run(stop), 1)
    run.assert_not_awaited()
    tick.assert_not_awaited()


def test_unconfigured_capture_does_not_construct_scheduler(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("backup is not configured")

    monkeypatch.setattr("tank.scheduler.Scheduler", unexpected)
    service = CaptureService(SimpleNamespace(), settings(), sources=object(), market=object())
    assert service.maintenance is None

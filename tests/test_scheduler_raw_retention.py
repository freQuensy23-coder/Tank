"""Independent raw TTL scheduling; all file fixtures live beneath pytest tmp_path."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import tank.scheduler as scheduler_module
from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import parse_time

NOW = parse_time("2026-10-19T12:00:00Z")


def quiet_scheduler(settings=None):
    repo = MemoryRepository()
    repo.prune_market = AsyncMock()
    repo.prune_jobs = AsyncMock()
    scheduler = Scheduler(repo, settings or SimpleNamespace(backup_dir=None), None, None)
    for method in (
        "expire_calls",
        "demote",
        "lifecycle",
        "heartbeat",
        "detect_scout_hooks",
        "partition_maintenance",
    ):
        setattr(scheduler, method, AsyncMock())
    scheduler.prune_raw_files = AsyncMock(return_value=["synthetic-expired-raw"])
    return scheduler


@pytest.mark.parametrize("backup_state", ["missing-config", "unavailable-disk"])
async def test_tick_runs_raw_ttl_despite_real_backup_gate_failure(backup_state, tmp_path):
    settings = SimpleNamespace(
        backup_dir=None if backup_state == "missing-config" else tmp_path / "absent-disk"
    )
    scheduler = quiet_scheduler(settings)
    try:
        await scheduler.tick(NOW)
        await asyncio.gather(*scheduler.background.values())
        scheduler.prune_raw_files.assert_awaited_once_with(NOW, None)
        scheduler.repo.prune_market.assert_not_awaited()
        scheduler.repo.prune_jobs.assert_not_awaited()
        marker = await scheduler.repo.get("ops.scheduler_checkpoints", "raw-retention")
        assert marker["at"] == "2026-10-19T12:00:00Z"
        assert marker["at_prec"] == "COLLECTION_ONLY"
    finally:
        await scheduler.close()


async def test_raw_singleflight_and_once_per_utc_day_survives_restart():
    scheduler = quiet_scheduler()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked(now, backup):
        assert backup is None
        entered.set()
        await release.wait()
        return []

    scheduler.prune_raw_files = AsyncMock(side_effect=blocked)
    now = parse_time("2026-10-19T00:30:00+03:00")
    first = asyncio.create_task(scheduler.raw_retention(now))
    await entered.wait()
    second = asyncio.create_task(scheduler.raw_retention(now))
    await asyncio.sleep(0)
    assert scheduler.prune_raw_files.await_count == 1
    release.set()
    await asyncio.gather(first, second)
    await scheduler.raw_retention(parse_time("2026-10-18T23:59:59Z"))
    scheduler.prune_raw_files.assert_awaited_once()
    restarted = Scheduler(scheduler.repo, SimpleNamespace(), None, None)
    restarted.prune_raw_files = AsyncMock(return_value=[])
    await restarted.raw_retention(parse_time("2026-10-18T23:59:59Z"))
    restarted.prune_raw_files.assert_not_awaited()
    await restarted.raw_retention(parse_time("2026-10-19T00:00:00Z"))
    restarted.prune_raw_files.assert_awaited_once()


async def test_repeated_tick_keeps_one_active_raw_task_while_backup_blocks():
    scheduler = quiet_scheduler()
    raw_entered = asyncio.Event()
    backup_entered = asyncio.Event()
    release = asyncio.Event()

    async def raw(now, backup):
        raw_entered.set()
        await release.wait()
        return []

    async def backup(now):
        backup_entered.set()
        await release.wait()

    scheduler.prune_raw_files = AsyncMock(side_effect=raw)
    scheduler.maintenance = AsyncMock(side_effect=backup)
    try:
        await scheduler.tick(NOW)
        await raw_entered.wait()
        await backup_entered.wait()
        task = scheduler.background["raw-retention"]
        for _ in range(3):
            await scheduler.tick(NOW)
        assert scheduler.background["raw-retention"] is task
        scheduler.prune_raw_files.assert_awaited_once()
        assert not scheduler.background["backup"].done()
        release.set()
        await asyncio.gather(*scheduler.background.values())
        await scheduler.tick(NOW)
        await asyncio.gather(*scheduler.background.values())
        scheduler.prune_raw_files.assert_awaited_once()
    finally:
        release.set()
        await scheduler.close()


async def test_failed_raw_pass_does_not_mark_day_complete_and_retries():
    scheduler = quiet_scheduler()
    scheduler.prune_raw_files = AsyncMock(side_effect=[OSError("synthetic failure"), []])
    with pytest.raises(OSError, match="synthetic failure"):
        await scheduler.raw_retention(NOW)
    assert await scheduler.repo.get("ops.scheduler_checkpoints", "raw-retention") is None
    await scheduler.raw_retention(NOW)
    await scheduler.raw_retention(NOW)
    assert scheduler.prune_raw_files.await_count == 2


async def test_backup_only_prune_false_does_not_trigger_either_retention(monkeypatch, tmp_path):
    final = tmp_path / f"tank-{NOW.date()}"
    final.mkdir()
    (final / "COMPLETE").write_text("synthetic verified fixture")
    verify = Mock(return_value={})
    monkeypatch.setattr(scheduler_module, "verify_backup", verify)
    scheduler = quiet_scheduler(SimpleNamespace(backup_dir=tmp_path))
    result = await scheduler.maintenance(NOW, force=True, prune=False)
    assert result["retention"] is False
    verify.assert_called_once()
    scheduler.repo.prune_market.assert_not_awaited()
    scheduler.repo.prune_jobs.assert_not_awaited()
    scheduler.prune_raw_files.assert_not_awaited()
    assert await scheduler.repo.get("ops.scheduler_checkpoints", "raw-retention") is None


async def test_successful_backup_pruning_uses_same_daily_guard(monkeypatch, tmp_path):
    final = tmp_path / f"tank-{NOW.date()}"
    final.mkdir()
    (final / "COMPLETE").write_text("synthetic verified fixture")
    monkeypatch.setattr(scheduler_module, "verify_backup", Mock(return_value={}))
    scheduler = quiet_scheduler(SimpleNamespace(backup_dir=tmp_path))
    await scheduler.raw_retention(NOW)
    await scheduler.maintenance(NOW, force=True, prune=True)
    scheduler.prune_raw_files.assert_awaited_once()
    scheduler.repo.prune_market.assert_awaited_once()
    scheduler.repo.prune_jobs.assert_awaited_once()
    assert (final / "RETENTION_COMPLETE").is_file()


async def test_missing_backup_still_prunes_expired_temporary_raw_file(tmp_path):
    """With no backup configured, the daily raw TTL still removes expired raw
    day directories of the HLD §7.2 layout and keeps recent ones."""
    data = tmp_path / "data"
    old = (NOW - timedelta(days=70)).date().isoformat()
    recent = (NOW - timedelta(days=2)).date().isoformat()
    for day in (old, recent):
        directory = data / "raw" / "trades" / "binance" / "spot" / "ABCUSDT" / day
        directory.mkdir(parents=True)
        (directory / "12.parquet").write_bytes(b"x")
    scheduler = quiet_scheduler(
        SimpleNamespace(backup_dir=None, data_dir=data, load_config=lambda name: {"instruments": []})
    )

    scheduler.prune_raw_files = Scheduler.prune_raw_files.__get__(scheduler, Scheduler)
    try:
        await scheduler.tick(NOW)
        await asyncio.gather(*scheduler.background.values())
        assert not (data / "raw/trades/binance/spot/ABCUSDT" / old).exists()
        assert (data / "raw/trades/binance/spot/ABCUSDT" / recent / "12.parquet").is_file()
        assert await scheduler.repo.get("ops.scheduler_checkpoints", "raw-retention")
        scheduler.repo.prune_market.assert_not_awaited()
        scheduler.repo.prune_jobs.assert_not_awaited()
    finally:
        await scheduler.close()

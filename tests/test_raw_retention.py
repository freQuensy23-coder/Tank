"""Raw market file retention by HLD §7.2 path: TTL, MAJOR+ days, cache age, safety."""

import os
from datetime import timedelta
from types import SimpleNamespace

import pytest

from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


def settings(root, **extra):
    return SimpleNamespace(data_dir=root, load_config=lambda name: {"instruments": []}, **extra)


def trade_day(root, day):
    directory = root / "raw/trades/binance/spot/ABCUSD" / day.date().isoformat()
    directory.mkdir(parents=True)
    (directory / "12.parquet").write_bytes(b"sealed hour")
    return directory


async def major_event(repo, at):
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "event_time": iso(at), "public_at": iso(at), "known_to_us_at": iso(at)},
    )
    await repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": "e", "class": "MAJOR"})
    await repo.put("ana.market_truth", "truth", {"mta_id": "truth", "event_id": "e", "token_id": "test:ABC"})


@pytest.mark.asyncio
async def test_ttl_removes_old_days_and_keeps_recent_and_major_event_days(tmp_path):
    now = utcnow().replace(hour=8, minute=0, second=0, microsecond=0)
    repo = MemoryRepository()
    await major_event(repo, now - timedelta(days=60))
    old = trade_day(tmp_path, now - timedelta(days=70))
    event = trade_day(tmp_path, now - timedelta(days=60))
    recent = trade_day(tmp_path, now - timedelta(days=2))
    books = tmp_path / "raw/books/binance/ABCUSD" / (now - timedelta(days=70)).date().isoformat()
    books.mkdir(parents=True)
    removed = await Scheduler(repo, settings(tmp_path), None, None).prune_raw_files(now, None)
    assert sorted(row["path"] for row in removed) == sorted([str(old), str(books)])
    assert all(row["reason"] == "MARKET_RAW_TTL" for row in removed)
    assert event.exists() and recent.exists() and not old.exists()


@pytest.mark.asyncio
async def test_archive_cache_expires_by_download_age(tmp_path):
    now = utcnow()
    cache = tmp_path / "archive/binance/spot/klines"
    cache.mkdir(parents=True)
    stale, fresh = cache / "ABCUSD-1m-2026-01-01.zip", cache / "ABCUSD-1m-2026-10-01.zip"
    for path, age in ((stale, 200), (fresh, 10)):
        path.write_bytes(b"verified archive")
        path.with_suffix(".zip.CHECKSUM").write_text("sha  " + path.name)
        for target in (path, path.with_suffix(".zip.CHECKSUM")):
            os.utime(target, (now.timestamp() - age * 86400,) * 2)
    removed = await Scheduler(MemoryRepository(), settings(tmp_path), None, None).prune_raw_files(now, None)
    assert sorted(row["path"] for row in removed) == sorted(
        [str(stale), str(stale.with_suffix(".zip.CHECKSUM"))]
    )
    assert fresh.exists() and fresh.with_suffix(".zip.CHECKSUM").exists()


@pytest.mark.asyncio
async def test_symlinked_and_protected_days_are_never_removed(tmp_path):
    now = utcnow()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.parquet").write_bytes(b"not market data")
    parent = tmp_path / "data/raw/trades/binance/spot/ABCUSD"
    parent.mkdir(parents=True)
    linked = parent / (now - timedelta(days=70)).date().isoformat()
    linked.symlink_to(outside, target_is_directory=True)
    auth_day = trade_day(tmp_path / "data", now - timedelta(days=71))
    auth = auth_day / "auth.json"
    auth.write_text("FAKE_PRIVATE_AUTH")
    scheduler = Scheduler(
        MemoryRepository(), settings(tmp_path / "data", codex_auth_path=auth), None, None
    )
    assert await scheduler.prune_raw_files(now, None) == []
    assert (outside / "keep.parquet").exists() and auth.exists()

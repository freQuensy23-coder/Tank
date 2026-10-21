"""Residual scheduler behavior against literal job/lifecycle contracts."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.time_precision import chosen_boundary
from tank.util import iso, parse_time

NOW = parse_time("2026-10-18T12:00:00Z")


def quiet_scheduler(repo):
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    scheduler.launch = lambda *args: None
    scheduler.expire_calls = AsyncMock()
    scheduler.demote = AsyncMock()
    scheduler.lifecycle = AsyncMock()
    scheduler.heartbeat = AsyncMock()
    scheduler.analysis_tokens = AsyncMock(return_value={"T"})
    scheduler.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    return scheduler


@pytest.mark.asyncio
async def test_verified_backup_is_cached_per_process_day_but_force_and_restart_reverify(
    monkeypatch, tmp_path
):
    final = tmp_path / f"tank-{NOW.date()}"
    final.mkdir()
    (final / "COMPLETE").write_text(iso(NOW))

    calls = []
    monkeypatch.setattr("tank.scheduler.verify_backup", lambda path, **kwargs: calls.append(path))
    settings = SimpleNamespace(backup_dir=tmp_path)
    scheduler = Scheduler(MemoryRepository(), settings, None, None)
    scheduler.retention = AsyncMock()
    for _ in range(3):
        await scheduler.maintenance(NOW)
    assert calls == [final]
    assert scheduler.retention.await_count == 3
    await scheduler.maintenance(NOW, force=True)
    assert calls == [final, final]
    restarted = Scheduler(MemoryRepository(), settings, None, None)
    await restarted.maintenance(NOW, prune=False)
    assert calls == [final, final, final]


@pytest.mark.asyncio
async def test_failed_backup_verification_never_enters_cache(monkeypatch, tmp_path):
    final = tmp_path / f"tank-{NOW.date()}"
    final.mkdir()
    (final / "COMPLETE").write_text(iso(NOW))
    count = 0

    def fail(*args, **kwargs):
        nonlocal count
        count += 1
        raise ValueError("checksum mismatch")

    monkeypatch.setattr("tank.scheduler.verify_backup", fail)
    scheduler = Scheduler(MemoryRepository(), SimpleNamespace(backup_dir=tmp_path), None, None)
    scheduler.retention = AsyncMock()
    for _ in range(2):
        with pytest.raises(ValueError, match="checksum"):
            await scheduler.maintenance(NOW)
    assert count == 2
    scheduler.retention.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_milestone_is_timer_missed_not_failed_source_outcome():
    repo = MemoryRepository()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    await repo.put(
        "evt.events",
        "e",
        {
            "event_id": "e",
            "lifecycle": "TRACKED",
            "known_to_us_at": iso(NOW - timedelta(days=8)),
            "next_milestone": {
                "kind": "launch",
                "due_at": iso(NOW - timedelta(hours=3)),
                "due_at_prec": "BUCKET(1h)",
                "status": "PENDING",
            },
        },
    )
    await scheduler.expire_milestone("e", NOW)
    await scheduler.expire_milestone("e", NOW)
    event = await repo.get("evt.events", "e")
    assert event["next_milestone"]["status"] == "MISSED"
    assert event["next_milestone"]["due_at_prec"] == "BUCKET(1h)"
    assert event["next_milestone"]["missed_at_prec"] == "COLLECTION_ONLY"
    assert "outcome" not in event["next_milestone"]
    jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(jobs) == 1 and jobs[0]["payload"]["verification_required"]
    await scheduler.lifecycle(NOW)
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "CLOSED"


@pytest.mark.asyncio
async def test_terminal_capture_is_recorded_as_gap_and_not_resurrected_or_claimed_captured():
    repo = MemoryRepository()
    scheduler = quiet_scheduler(repo)
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "lifecycle": "MONITORING", "public_at": iso(NOW), "public_at_prec": "EXACT"},
    )
    job = await repo.enqueue(
        "REACTION_CAPTURE",
        {"event_id": "e", "token_id": "T", "checkpoint": "POST_72H"},
        "e:T:POST_72H",
        max_attempts=5,
    )
    await repo.put("ops.jobs", job["job_id"], {**job, "status": "FAILED", "attempts": 5})
    await scheduler.tick(NOW)
    gap = await repo.get("ops.scheduler_checkpoints", "reaction-gap:e:T:POST_72H")
    assert gap["state"]["status"] == "FAILED" and gap["state"]["captured"] is False
    assert not await repo.list("out.reaction_checkpoints")
    await repo.delete("ops.jobs", job["job_id"])
    await scheduler.tick(NOW)
    assert not await repo.list("ops.jobs", filters={"idem_key": "e:T:POST_72H"})

    real_scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    real_scheduler.analysis_tokens = AsyncMock(return_value={"T"})
    await real_scheduler.lifecycle(NOW)
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "MONITORING"


@pytest.mark.asyncio
async def test_checkpoint_expiry_does_not_slide_on_later_ticks_and_scans_are_batched(monkeypatch):
    repo = MemoryRepository()
    scheduler = quiet_scheduler(repo)
    origin = NOW - timedelta(hours=1)
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "lifecycle": "TRACKED", "public_at": iso(origin), "public_at_prec": "EXACT"},
    )
    listed = []
    original = repo.list

    async def observe(collection, **kwargs):
        listed.append((collection, kwargs))
        return await original(collection, **kwargs)

    monkeypatch.setattr(repo, "list", observe)
    await scheduler.tick(NOW)
    job = next(j for j in await original("ops.jobs") if j["idem_key"] == "e:T:POST_15M")
    assert job["expires_at"] == iso(NOW + timedelta(hours=6))
    assert job["run_after"] == iso(NOW)
    assert job["payload"]["cutoff"] == iso(origin + timedelta(minutes=15))
    assert job["payload"]["retro"] is True
    assert job["max_attempts"] == 5
    assert not any(collection == "out.reaction_checkpoints" for collection, _ in listed)
    await scheduler.tick(NOW + timedelta(days=5))
    gap = await repo.get("ops.scheduler_checkpoints", "reaction-gap:e:T:POST_15M")
    assert gap["state"]["expires_at"] == job["expires_at"]


@pytest.mark.asyncio
async def test_conditions_share_truth_for_identical_event_token_cutoff():
    repo = MemoryRepository()
    truth = SimpleNamespace(assess=AsyncMock(return_value={"move_class": "SPOT_LED_ACCUMULATION"}))
    scheduler = Scheduler(repo, SimpleNamespace(), None, truth)
    await repo.put("evt.events", "e", {"event_id": "e"})
    for identifier in ("one", "two"):
        await repo.put(
            "ana.watch_conditions",
            identifier,
            {
                "cond_id": identifier,
                "event_id": "e",
                "token_id": "T",
                "status": "ARMED",
                "expires_at": iso(NOW + timedelta(days=1)),
                "expr": {"metric": "mt.move_class", "op": "==", "value": "OTHER"},
            },
        )
    await scheduler.eval_conditions(chosen_boundary(NOW, precision="EXACT").fields("cutoff"))
    truth.assess.assert_awaited_once()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relative",
    [
        "raw/trades/v/spot/T/{day}",
        "raw/books/v/T/{day}",
        "raw/market_frames/{day}",
        "archive/binance/spot/klines",
    ],
)
async def test_raw_retention_by_path_with_verified_backup(tmp_path, relative):
    import hashlib
    import json
    import os
    import shutil

    from tank.scheduler import verify_backup

    data = tmp_path / "data"
    backup = tmp_path / "backup"
    archive = relative.startswith("archive/")
    stamp = NOW - timedelta(days=200 if archive else 70)
    directory = data / relative.format(day=stamp.date().isoformat())
    directory.mkdir(parents=True)
    saved_root = backup / "data"
    saved_root.mkdir(parents=True)
    path = directory / (f"T-1m-{stamp.date()}.zip" if archive else "12.parquet")
    path.write_bytes(b"verified market bytes")
    if archive:
        os.utime(path, (stamp.timestamp(),) * 2)
    saved = saved_root / path.relative_to(data)
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, saved)
    files = {saved.relative_to(backup).as_posix(): hashlib.sha256(saved.read_bytes()).hexdigest()}
    for name in ("tank.dump", "oma.dump"):
        payload = b"fixture dump (no external database)"
        (backup / name).write_bytes(payload)
        files[name] = hashlib.sha256(payload).hexdigest()
    manifest = {"version": 1, "roots": {"data": str(data.resolve())}, "files": files}
    (backup / "manifest.json").write_text(json.dumps(manifest))
    (backup / "COMPLETE").write_text(iso(NOW))
    verify_backup(backup)
    scheduler = Scheduler(
        MemoryRepository(),
        SimpleNamespace(data_dir=data, load_config=lambda name: {"instruments": []}),
        None,
        None,
    )
    removed = await scheduler.prune_raw_files(NOW, backup)
    assert [row["path"] for row in removed] == [str(path if archive else directory)]
    assert not path.exists() and saved.is_file()


@pytest.mark.asyncio
async def test_backup_cache_reverifies_replaced_manifest_identity(monkeypatch, tmp_path):
    final = tmp_path / f"tank-{NOW.date()}"
    final.mkdir()
    (final / "COMPLETE").write_text(iso(NOW))
    manifest = final / "manifest.json"
    manifest.write_text("first manifest")
    calls = []
    monkeypatch.setattr(
        "tank.scheduler.verify_backup", lambda path, **kwargs: calls.append(path) or {"files": {}}
    )
    scheduler = Scheduler(MemoryRepository(), SimpleNamespace(backup_dir=tmp_path), None, None)
    await scheduler.maintenance(NOW, prune=False)
    await scheduler.maintenance(NOW, prune=False)
    assert len(calls) == 1
    replacement = final / "replacement"
    replacement.write_text("changed manifest identity")
    replacement.replace(manifest)
    await scheduler.maintenance(NOW, prune=False)
    assert len(calls) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bundles_only_backup_allows_raw_ttl_and_preserves_permanent_evidence(tmp_path):
    import hashlib
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    data, backup = tmp_path / "data", tmp_path / "backup"
    raw = data / "raw/trades/v/spot/T" / (NOW - timedelta(days=70)).date().isoformat() / "12.parquet"
    raw.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"ts": iso(NOW - timedelta(days=70)), "instrument_id": "v:T"}]), raw)
    permanent = {
        data / "bundles/fixture.json.zst": b"immutable bundle fixture",
        data / "raw/items/S01/2026/08/09/item.json.zst": b"immutable source fixture",
    }
    for path, payload in permanent.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    backup.mkdir()
    files = {}
    for name in ("tank.dump", "oma.dump"):
        (backup / name).write_bytes(b"fixture dump")
        files[name] = hashlib.sha256(b"fixture dump").hexdigest()
    (backup / "manifest.json").write_text(
        json.dumps({"roots": {"bundles": str(data / "bundles")}, "files": files})
    )
    (backup / "COMPLETE").write_text(iso(NOW))
    repo = MemoryRepository()
    scheduler = Scheduler(
        repo, SimpleNamespace(data_dir=data, load_config=lambda name: {"instruments": []}), None, None
    )
    removed = await scheduler.prune_raw_files(NOW, backup)
    assert [row["path"] for row in removed] == [str(raw.parent)]
    assert not raw.exists()
    assert {path: path.read_bytes() for path in permanent} == permanent
    assert (backup / "tank.dump").read_bytes() == b"fixture dump"
    assert (backup / "oma.dump").read_bytes() == b"fixture dump"


@pytest.mark.asyncio
async def test_selective_condition_union_never_calls_full_assess_or_persists_missing_truth():
    repo = MemoryRepository()
    truth = SimpleNamespace(
        assess=AsyncMock(side_effect=AssertionError("full assessment must not run in minute evaluator")),
        condition_values=AsyncMock(
            return_value={
                "_condition_evaluation": {
                    "requires_market_truth": [{"metric": "mt.move_class", "window": "W1H"}],
                    "request_key": "unchanged-input-fingerprint",
                }
            }
        ),
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, truth)
    await repo.put("evt.events", "e", {"event_id": "e"})
    for identifier, metric in (("one", "price.close_vs_level"), ("two", "mt.move_class")):
        await repo.put(
            "ana.watch_conditions",
            identifier,
            {
                "cond_id": identifier,
                "event_id": "e",
                "token_id": "T",
                "status": "ARMED",
                "expires_at": iso(NOW + timedelta(days=1)),
                "expr": {"metric": metric, "op": "!=", "value": "unsafe-default"},
            },
        )
    for minute in (0, 1):
        await scheduler.eval_conditions(
            chosen_boundary(NOW + timedelta(minutes=minute), precision="EXACT").fields("cutoff")
        )
    assert truth.condition_values.await_count == 2
    assert truth.condition_values.call_args.kwargs["requirements"] == [
        {"metric": "mt.move_class", "window": "W1H"},
        {"metric": "price.close_vs_level", "window": "W1H"},
    ]
    truth.assess.assert_not_awaited()
    assert not await repo.list("ana.market_truth")
    assert not await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert not await repo.list("ops.jobs", filters={"type": "RUN_MARKET_TRUTH"})
    condition = await repo.get("ana.watch_conditions", "one")
    assert condition["counters"]["root"]["data_available"] is False
    assert condition["counters"]["root"]["unavailable_reason"] == "INCOMPLETE"


@pytest.mark.asyncio
@pytest.mark.parametrize("late_value", [None, "SELL"])
async def test_closed_hour_completion_or_correction_is_rechecked_without_double_count(late_value):
    repo = MemoryRepository()
    first = NOW.replace(hour=7)
    second = NOW.replace(hour=8)
    values = {iso(first): "BUY", iso(second): late_value}

    async def selective(event_id, token_id, cutoff, *, requirements):
        value = values[cutoff.value]
        return {"spot": {"W1H": {"direction": value}}, "_condition_evaluation": {"request_key": str(value)}}

    truth = SimpleNamespace(
        condition_values=selective, assess=AsyncMock(side_effect=AssertionError("no full truth"))
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, truth)
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c",
        {
            "cond_id": "c",
            "event_id": "e",
            "token_id": "T",
            "status": "ARMED",
            "created_at": iso(first - timedelta(minutes=30)),
            "expires_at": iso(NOW + timedelta(days=1)),
            "expr": {
                "metric": "spot.cohort.direction",
                "op": "==",
                "value": "BUY",
                "window": "W1H",
                "persist": {"count": 2},
            },
        },
    )
    for stamp in (first, first + timedelta(minutes=3)):
        await scheduler.eval_conditions(chosen_boundary(stamp, precision="EXACT").fields("cutoff"))
        assert (await repo.get("ana.watch_conditions", "c"))["counters"]["root"]["count"] == 1
    await scheduler.eval_conditions(chosen_boundary(second, precision="EXACT").fields("cutoff"))
    assert (await repo.get("ana.watch_conditions", "c"))["status"] == "ARMED"
    values[iso(second)] = "BUY"
    await scheduler.eval_conditions(
        chosen_boundary(second + timedelta(minutes=3), precision="EXACT").fields("cutoff")
    )
    condition = await repo.get("ana.watch_conditions", "c")
    assert condition["status"] == "FIRED"
    assert condition["counters"]["root"]["count"] == 2
    assert len(condition["counters"]["root"]["history"]) == 2
    truth.assess.assert_not_awaited()
    assert not await repo.list("ana.market_truth")


@pytest.mark.asyncio
async def test_late_correction_of_preceding_closed_hour_cannot_false_fire():
    repo = MemoryRepository()
    first, second = NOW.replace(hour=7), NOW.replace(hour=8)
    values = {iso(first): "BUY", iso(second): "BUY"}

    async def selective(event_id, token_id, cutoff, *, requirements):
        return {
            "spot": {"W1H": {"direction": values[cutoff.value]}},
            "_condition_evaluation": {"request_key": str(values)},
        }

    scheduler = Scheduler(repo, SimpleNamespace(), None, SimpleNamespace(condition_values=selective))
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c",
        {
            "cond_id": "c",
            "event_id": "e",
            "token_id": "T",
            "status": "ARMED",
            "expires_at": iso(NOW + timedelta(days=1)),
            "expr": {"metric": "spot.cohort.direction", "op": "==", "value": "BUY", "persist": {"count": 2}},
        },
    )
    await scheduler.eval_conditions(chosen_boundary(first, precision="EXACT").fields("cutoff"))
    values[iso(first)] = "SELL"
    await scheduler.eval_conditions(chosen_boundary(second, precision="EXACT").fields("cutoff"))
    condition = await repo.get("ana.watch_conditions", "c")
    assert condition["status"] == "ARMED" and condition["counters"]["root"]["count"] == 1
    assert condition["counters"]["root"]["history"][0]["matched"] is False


def test_depth_ratio_preserves_strict_decimal_boundary():
    from tank.scheduler import compare, depth_ratio

    ratio = depth_ratio(
        {"aggregate_depth_p1_usd": "200.0000000000000000000000000000000001", "pre_event_depth_p1_usd": "400"}
    )
    assert compare(ratio, ">", "0.5") is True
    assert (
        compare(depth_ratio({"aggregate_depth_p1_usd": "200", "pre_event_depth_p1_usd": "400"}), ">", "0.5")
        is False
    )

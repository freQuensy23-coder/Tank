from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.mark.asyncio
async def test_recursive_hourly_persistence_does_not_count_same_window_twice():
    repo = MemoryRepository()
    now = utcnow().replace(minute=0, second=0, microsecond=0)
    truth = SimpleNamespace(
        assess=AsyncMock(return_value={"spot": {"W1H": {"direction": "BUY", "cw_breadth": {"buy": 0.8}}}})
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, truth)
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c",
        {
            "cond_id": "c",
            "event_id": "e",
            "token_id": "t",
            "status": "ARMED",
            "expires_at": iso(now + timedelta(days=1)),
            "expr": {
                "all": [
                    {
                        "metric": "spot.cohort.direction",
                        "window": "W1H",
                        "op": "==",
                        "value": "BUY",
                        "persist": {"count": 2},
                    },
                    {
                        "metric": "spot.cohort.cwb_buy",
                        "window": "W1H",
                        "op": ">",
                        "value": 0.5,
                        "persist": {"count": 2},
                    },
                ]
            },
        },
    )
    await scheduler.eval_conditions({"cutoff": iso(now)})
    await scheduler.eval_conditions({"cutoff": iso(now + timedelta(minutes=30))})
    assert (await repo.get("ana.watch_conditions", "c"))["status"] == "ARMED"
    await scheduler.eval_conditions({"cutoff": iso(now + timedelta(hours=1))})
    assert (await repo.get("ana.watch_conditions", "c"))["status"] == "FIRED"
    assert len(await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})) == 1


@pytest.mark.asyncio
async def test_tracked_event_closes_after_seven_days_without_news():
    repo = MemoryRepository()
    now = utcnow()
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "lifecycle": "TRACKED", "known_to_us_at": iso(now - timedelta(days=8))},
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    await scheduler.lifecycle(now)
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "CLOSED"


@pytest.mark.asyncio
async def test_pre_event_capture_uses_pre_window_and_is_immutable(monkeypatch):
    import tank.analysis.truth

    repo = MemoryRepository()
    now = utcnow()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={"cutoff": iso(now)}))
    monkeypatch.setattr(
        tank.analysis.truth,
        "compute_market_truth",
        lambda snapshot, config: {"relative": {"windows": {"EVENT": {"r_raw": 0.01}}}},
    )
    scheduler = Scheduler(repo, SimpleNamespace(config_version="cfg", code_version="code"), market, None)
    # An analysed token: BTC/ETH outside the analysis set follow the benchmark path instead.
    payload = {
        "event_id": "e",
        "token_id": "test:T",
        "checkpoint": "PRE_24H",
        "anchor": {"type": "T1", "ts": iso(now)},
        "retro": True,
    }
    await scheduler.reaction_capture(payload)
    await scheduler.reaction_capture(payload)
    args, kwargs = market.snapshot.call_args
    assert args[0] == "test:T"
    assert args[1].value == iso(now)
    assert args[1].precision is None
    assert args[1].precision_reason == "INCOMPLETE"
    assert kwargs["anchor"]["ts"] == iso(now - timedelta(days=1))
    assert market.snapshot.await_count == 1
    rows = await repo.list("out.reaction_checkpoints")
    assert len(rows) == 1 and rows[0]["r_raw"] == 0.01


@pytest.mark.asyncio
async def test_missing_market_value_never_fires_not_equal_condition():
    # Incomplete funding must not be interpreted as safe funding.
    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c",
        {
            "cond_id": "c",
            "event_id": "e",
            "token_id": "t",
            "status": "ARMED",
            "expires_at": iso(now + timedelta(days=1)),
            "expr": {"metric": "deriv.funding_vs_normal", "window": "W1H", "op": "!=", "value": "ABOVE"},
        },
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, SimpleNamespace(assess=AsyncMock(return_value={})))
    await scheduler.eval_conditions({"cutoff": iso(now)})
    assert (await repo.get("ana.watch_conditions", "c"))["status"] == "ARMED"


@pytest.mark.asyncio
async def test_cooldown_requires_all_analysis_and_benchmark_checkpoints():
    repo = MemoryRepository()
    now = utcnow()
    await repo.put(
        "evt.events",
        "e",
        {
            "event_id": "e",
            "lifecycle": "MONITORING",
            "known_to_us_at": iso(now - timedelta(days=4)),
            "analysis_set": ["token:T"],
        },
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    for token in ["token:T", "bitcoin:BTC"]:
        await repo.put(
            "out.reaction_checkpoints",
            token,
            {"rwp_id": token, "event_id": "e", "token_id": token, "checkpoint": "POST_72H"},
            immutable=True,
        )
    await scheduler.lifecycle(now)
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "MONITORING"
    await repo.put(
        "out.reaction_checkpoints",
        "eth",
        {"rwp_id": "eth", "event_id": "e", "token_id": "ethereum:ETH", "checkpoint": "POST_72H"},
        immutable=True,
    )
    await scheduler.lifecycle(now)
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "COOLDOWN"


@pytest.mark.asyncio
async def test_failed_backup_never_prunes_market(monkeypatch, tmp_path):
    import asyncio

    repo = MemoryRepository()
    repo.prune_market = AsyncMock()
    repo.prune_jobs = AsyncMock()
    process = SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"failure")), returncode=1)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    settings = SimpleNamespace(
        backup_dir=tmp_path,
        backup_hour_utc=2,
        database_url="postgresql://u:secret@localhost/tank",
        data_dir=tmp_path / "data",
        market_retention_days=90,
    )
    scheduler = Scheduler(repo, settings, None, None)
    with pytest.raises(RuntimeError, match="pg_dump failed"):
        await scheduler.maintenance(utcnow().replace(hour=8))
    repo.prune_market.assert_not_called()
    assert not list(tmp_path.glob("*/COMPLETE"))


@pytest.mark.asyncio
async def test_planned_backup_copies_bundles_and_dumps_only(monkeypatch, tmp_path):
    import asyncio
    from pathlib import Path

    from tank.scheduler import verify_backup

    data, config, backup = (tmp_path / key for key in ("data", "config", "backup"))
    for root in (data / "bundles", config, backup):
        root.mkdir(parents=True)
    (data / "bundles" / "hash.json.zst").write_bytes(b"fixture bundle")
    (data / "market-cache").write_bytes(b"not selected")
    (config / "registry.yaml").write_text("not selected")

    async def dump(*args, **kwargs):
        Path(args[args.index("--file") + 1]).write_bytes(b"fixture dump")
        return SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"")), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", dump)
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items", "missing", {"item_id": "missing", "raw_ref": str(data / "absent.zst")}
    )
    settings = SimpleNamespace(
        backup_dir=backup,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
    )
    result = await Scheduler(repo, settings, None, None).maintenance(utcnow(), force=True, prune=False)
    final = Path(result["path"])
    manifest = verify_backup(final)
    assert set(manifest["roots"]) == {"bundles"}
    assert set(manifest["files"]) == {"tank.dump", "oma.dump", "bundles/hash.json.zst"}
    assert not (final / "data").exists() and not (final / "config").exists()
    assert (data / "market-cache").exists()


@pytest.mark.asyncio
async def test_missing_unselected_source_reference_does_not_expand_backup_scope(monkeypatch, tmp_path):
    import asyncio
    from pathlib import Path

    from tank.scheduler import verify_backup

    data, config, backup = (tmp_path / key for key in ("data", "config", "backup"))
    for root in (data / "bundles", config, backup):
        root.mkdir(parents=True)
    (data / "bundles" / "hash.json.zst").write_bytes(b"fixture bundle")
    (data / "market-cache").write_bytes(b"not selected")
    (config / "registry.yaml").write_text("not selected")

    async def dump(*args, **kwargs):
        Path(args[args.index("--file") + 1]).write_bytes(b"fixture dump")
        return SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"")), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", dump)
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items", "missing", {"item_id": "missing", "raw_ref": str(data / "absent.zst")}
    )
    settings = SimpleNamespace(
        backup_dir=backup,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
    )
    result = await Scheduler(repo, settings, None, None).maintenance(utcnow(), force=True, prune=False)
    final = Path(result["path"])
    manifest = verify_backup(final)
    assert set(manifest["roots"]) == {"bundles"}
    assert set(manifest["files"]) == {"tank.dump", "oma.dump", "bundles/hash.json.zst"}
    assert not (final / "data").exists() and not (final / "config").exists()
    assert (data / "market-cache").exists()


@pytest.mark.asyncio
async def test_heartbeat_preserves_unknown_llm_cost():
    from tank.settings import Settings

    repo = MemoryRepository()
    await repo.put("ops.llm_usage", "unknown", {"at": iso(), "cost_usd": None})
    await repo.put("ops.llm_usage", "known", {"at": iso(), "cost_usd": "1.25"})
    await Scheduler(repo, Settings(), None, None).heartbeat(utcnow())
    notices = await repo.list("out.ops_notices")
    assert "known subtotal $1.25; unknown cost records=1" in notices[0]["text"]


@pytest.mark.asyncio
async def test_relocated_backup_recovers_source_web_replay_config_logs_and_both_databases(
    monkeypatch, tmp_path
):
    import asyncio
    import shutil
    from pathlib import Path

    from tank.scheduler import resolve_backup_reference, verify_backup

    live = tmp_path / "live"
    data = live / "data"
    config = live / "config"
    logs = live / "logs"
    backup = tmp_path / "disk"
    backup.mkdir()
    contents = {
        "raw/sources/a.gz": b"compressed-source-evidence",
        "raw/web/a.txt": b"offical web quote",
        "replay/bundles/hash/inputs.json": b'{"packet":"pinned"}',
        "bundles/hash.json": b'{"claim":"frozen"}',
        "market/raw/one.parquet": b"event-window-market",
    }
    for name, body in contents.items():
        path = data / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    config.mkdir()
    (config / "sources.yaml").write_text("sources: []")
    logs.mkdir()
    (logs / "openma.log").write_text("permanent session transcript")
    profile = data / "chrome-profile"
    profile.mkdir()
    (profile / "Cookies").write_text("private")

    async def fake_dump(*args, **kwargs):
        Path(args[args.index("--file") + 1]).write_bytes(("dump " + kwargs["env"]["PGDATABASE"]).encode())
        return SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"")), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_dump)
    repo = MemoryRepository()
    repo.prune_market = AsyncMock()
    repo.prune_jobs = AsyncMock()
    raw_ref = str(data / "raw/sources/a.gz")
    await repo.put("ingest.source_items", "item", {"item_id": "item", "raw_ref": raw_ref})
    settings = SimpleNamespace(
        backup_dir=backup,
        backup_hour_utc=2,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
        logs_dir=logs,
        x_profile_dir=profile,
        market_retention_days=90,
        load_config=lambda name: {"instruments": []},
    )
    now = utcnow().replace(hour=8)
    legacy_full_backup_fixture(settings, now)
    await Scheduler(repo, settings, None, None).maintenance(now)
    final = backup / f"tank-{now.date()}"
    verify_backup(final)
    assert (final / "tank.dump").read_bytes() == b"dump tank"
    assert (final / "oma.dump").read_bytes() == b"dump oma"
    assert not (final / "data/chrome-profile").exists()
    # Simulate loss of the original host and move the backup to a new recovery mount.
    shutil.rmtree(live)
    recovery = tmp_path / "recovery"
    shutil.copytree(final, recovery)
    for name, body in contents.items():
        historical_ref = str(data / name)
        assert resolve_backup_reference(recovery, historical_ref).read_bytes() == body
    assert (recovery / "config/sources.yaml").read_text() == "sources: []"
    assert (recovery / "logs/openma.log").read_text() == "permanent session transcript"
    verify_backup(recovery)
    (recovery / "data/raw/web/a.txt").write_text("corrupt")
    with pytest.raises(ValueError, match="checksum"):
        verify_backup(recovery)
    repo.prune_market.assert_awaited_once()


@pytest.mark.asyncio
async def test_market_file_retention_preserves_event_windows_and_permanent_evidence(monkeypatch, tmp_path):
    import asyncio
    import hashlib
    import os
    from pathlib import Path

    from tank.scheduler import resolve_backup_reference

    now = utcnow().replace(hour=8)
    data = tmp_path / "data"
    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text("instruments: []")
    backup = tmp_path / "backup"
    backup.mkdir()
    repo = MemoryRepository()
    repo.prune_market = AsyncMock()
    repo.prune_jobs = AsyncMock()
    anchor = now - timedelta(days=60)
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "event_time": iso(anchor), "public_at": iso(anchor), "known_to_us_at": iso(anchor)},
    )
    await repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": "e", "class": "MAJOR"})
    await repo.put("ana.market_truth", "truth", {"mta_id": "truth", "event_id": "e", "token_id": "token:T"})
    # Raw market files are found by their HLD §7.2 path; a UTC day overlapping the
    # MAJOR event window is kept, other days go after 30 days.
    paths = {}
    for key, age in [("event", 60), ("old", 70), ("new", 2)]:
        day = (now - timedelta(days=age)).date().isoformat()
        paths[key] = data / "raw/trades/venue/spot/T" / day / "08.parquet"
        paths[key].parent.mkdir(parents=True)
        paths[key].write_bytes(b"sealed hour")
    old_date = (now - timedelta(days=200)).date().isoformat()
    cached = data / "archive/binance/spot/klines" / f"TUSDT-1m-{old_date}.zip"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"verified historical cache")
    cached.with_suffix(".zip.CHECKSUM").write_text(hashlib.sha256(cached.read_bytes()).hexdigest())
    for target in (cached, cached.with_suffix(".zip.CHECKSUM")):
        os.utime(target, ((now - timedelta(days=200)).timestamp(),) * 2)
    source = data / "raw/sources/permanent.gz"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"permanent source")

    async def fake_dump(*args, **kwargs):
        Path(args[args.index("--file") + 1]).write_bytes(b"dump")
        return SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"")), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_dump)
    settings = SimpleNamespace(
        backup_dir=backup,
        backup_hour_utc=2,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
        logs_dir=tmp_path / "logs",
        x_profile_dir=tmp_path / "profile",
        market_retention_days=90,
        load_config=lambda name: {"instruments": []},
    )
    scheduler = Scheduler(repo, settings, None, None)
    legacy_full_backup_fixture(settings, now)
    await scheduler.maintenance(now)
    assert paths["event"].exists() and paths["new"].exists()
    assert (
        not paths["old"].exists() and not cached.exists() and not cached.with_suffix(".zip.CHECKSUM").exists()
    )
    assert source.exists()
    final = backup / f"tank-{now.date()}"
    assert resolve_backup_reference(final, str(paths["old"])).is_file()
    await scheduler.maintenance(now)
    repo.prune_jobs.assert_awaited_once()


async def real_decision_watch_conditions(monkeypatch, tmp_path):
    """Use real EventToolService persistence and real DecisionEngine condition generation."""
    from test_decisions import inputs

    import tank.event_tools as tools_module
    from tank.analysis.decisions import DecisionEngine
    from tank.event_tools import EventToolService
    from tank.settings import Settings

    created = utcnow().replace(hour=11, minute=15, second=0, microsecond=0)
    monkeypatch.setattr(tools_module, "utcnow", lambda: created)
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path)
    event, assessment, beneficiary, market = inputs()
    event.update(current_ems_id="ems", lifecycle="MONITORING")
    assessment.update(ems_id="ems", event_id="e")
    beneficiary.update(edge_id="edge", event_id="e", map_version=1)
    market.update(mta_id="truth", event_id="e", cutoff=iso(created))
    market["data_confidence"]["class"] = "MEDIUM"
    await repo.put("evt.events", "e", event)
    await repo.put("evt.ems_assessments", "ems", assessment)
    await repo.put("evt.beneficiary_maps", "map", {"event_id": "e", "map_version": 1, "analysis_set": ["T"]})
    await repo.put("evt.beneficiary_edges", "edge", beneficiary)
    await repo.put("ana.market_truth", "truth", market)
    service = EventToolService(repo, settings, None, DecisionEngine(settings))
    decision = await service.get_decision(
        {"event_id": "e", "token_id": "T"}, {"session_id": "session", "call_id": "decision"}
    )
    assert decision["action"] == "WATCH_TO_START"
    conditions = await repo.list("ana.watch_conditions")
    assert conditions and all(c.get("created_at") and c.get("expires_at") for c in conditions)
    return repo, settings, created, conditions


@pytest.mark.asyncio
async def test_real_generated_conditions_use_two_closed_nonoverlapping_hours(monkeypatch, tmp_path):
    from tank.util import parse_time

    repo, settings, created, conditions = await real_decision_watch_conditions(monkeypatch, tmp_path)
    calls = []

    async def assess(event_id, token_id, cutoff):
        calls.append(cutoff)
        stamp = parse_time(cutoff.value)
        assert cutoff.precision == ("EXACT" if stamp.minute == 0 and stamp.second == 0 else None)
        # A rolling first-minute sample disagrees; only completed-hour observations qualify.
        direction = "BUY" if stamp.minute == 0 and stamp.second == 0 else "SELL"
        return {
            "spot": {"W1H": {"direction": direction, "cw_breadth": {"buy": 0.8, "sell": 0.2}}},
            "derivatives": {"synchronized": {"native_oi_vs_normal": "WITHIN"}},
            "relative": {"close_1h_price": "10"},
        }

    scheduler = Scheduler(repo, settings, None, SimpleNamespace(assess=assess))
    upgrade = next(c for c in conditions if c["role"] == "UPGRADE")
    await scheduler.eval_conditions({"cutoff": iso(created.replace(hour=12, minute=7))})
    first = await repo.get("ana.watch_conditions", upgrade["cond_id"])
    assert first["status"] == "ARMED"
    assert first["counters"]["root0"]["count"] == 1
    assert first["counters"]["root0"]["cutoff"] == iso(created.replace(hour=12, minute=0))
    await scheduler.eval_conditions({"cutoff": iso(created.replace(hour=12, minute=59))})
    assert (await repo.get("ana.watch_conditions", upgrade["cond_id"]))["counters"]["root0"]["count"] == 1
    await scheduler.eval_conditions({"cutoff": iso(created.replace(hour=13, minute=1))})
    final = await repo.get("ana.watch_conditions", upgrade["cond_id"])
    assert final["status"] == "FIRED" and final["counters"]["root0"]["count"] == 2
    assert final["counters"]["root0"]["cutoff"] == iso(created.replace(hour=13, minute=0))
    fired = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert sum(j["payload"].get("condition", {}).get("cond_id") == upgrade["cond_id"] for j in fired) == 1


@pytest.mark.asyncio
async def test_real_generated_fatal_condition_fires_next_minute_without_hour_throttle(monkeypatch, tmp_path):
    repo, settings, created, conditions = await real_decision_watch_conditions(monkeypatch, tmp_path)
    truth = SimpleNamespace(assess=AsyncMock(return_value={"relative": {"close_1h_price": "10"}}))
    scheduler = Scheduler(repo, settings, None, truth)
    fatal = next(c for c in conditions if c["expr"].get("metric") == "event.contradiction_fatal")
    await scheduler.eval_conditions({"cutoff": iso(created.replace(hour=12, minute=0))})
    assert (await repo.get("ana.watch_conditions", fatal["cond_id"]))["status"] == "ARMED"
    await repo.put(
        "ana.contradictions",
        "fatal",
        {
            "contradiction_id": "fatal",
            "event_id": "e",
            "type": "FATAL",
            "detected_at": iso(created.replace(hour=12, minute=1)),
        },
    )
    await scheduler.eval_conditions({"cutoff": iso(created.replace(hour=12, minute=1))})
    assert (await repo.get("ana.watch_conditions", fatal["cond_id"]))["status"] == "FIRED"


def test_condition_derivatives_use_requested_window_without_primary_fallback():
    from tank.scheduler import metric_value

    truth = {
        "derivatives": {
            "synchronized": {"native_oi_vs_normal": "ABOVE"},
            "windows": {
                "W1H": {
                    "synchronized": {"native_oi_vs_normal": "WITHIN"},
                    "per_venue": [{"funding_vs_normal": "BELOW"}],
                },
                "W4H": {"synchronized": {"native_oi_vs_normal": "ABOVE"}},
            },
        }
    }
    assert metric_value(truth, {}, "deriv.native_oi_vs_normal", "W1H") == "WITHIN"
    assert metric_value(truth, {}, "deriv.funding_vs_normal", "W1H") == "BELOW"
    assert metric_value(truth, {}, "deriv.native_oi_vs_normal", "W15M") is None


@pytest.mark.asyncio
async def test_post72_reaction_uses_cumulative_event_derivatives(monkeypatch):
    import tank.analysis.truth

    repo = MemoryRepository()
    now = utcnow()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    event_derivatives = {
        "synchronized": {"oi_native_chg_pct": 12, "oi_class": "REAL_NATIVE"},
        "per_venue": [{"venue": "v", "funding_start": -0.01, "funding_current": 0.02}],
        "liquidations": {"long": 100, "short": 200},
    }
    truth = {
        "relative": {"windows": {"EVENT": {"r_raw": 0.2}}},
        "derivatives": {
            "synchronized": {"oi_native_chg_pct": -4, "oi_class": "UNWIND"},
            "per_venue": [{"venue": "v", "funding_start": 0}],
            "liquidations": {"long": 1, "short": 2},
            "windows": {"EVENT": event_derivatives, "W4H": {"synchronized": {"oi_native_chg_pct": -4}}},
        },
    }
    monkeypatch.setattr(tank.analysis.truth, "compute_market_truth", lambda *a, **k: truth)
    scheduler = Scheduler(repo, SimpleNamespace(config_version="cfg", code_version="code"), market, None)
    await scheduler.reaction_capture(
        {
            "event_id": "e",
            "token_id": "T",
            "checkpoint": "POST_72H",
            "anchor": {"type": "T1", "ts": iso(now - timedelta(hours=72))},
            "retro": False,
        }
    )
    record = (await repo.list("out.reaction_checkpoints"))[0]
    assert record["oi_native_chg_pct"] == 12 and record["oi_class"] == "REAL_NATIVE"
    assert record["funding"] == event_derivatives["per_venue"]
    assert record["liquidations"] == event_derivatives["liquidations"]


@pytest.mark.asyncio
async def test_automatic_scout_hooks_dedup_and_ignore_low_confidence_or_single_unknown(tmp_path):
    from tank.settings import Settings

    repo = MemoryRepository()
    now = utcnow()
    settings = Settings(data_dir=tmp_path)
    scheduler = Scheduler(repo, settings, None, None)
    for token, confidence in [("T", "HIGH"), ("LOW", "LOW")]:
        for i, hours in enumerate([25, 12, 0]):
            await repo.put(
                "ana.market_truth",
                f"{token}{i}",
                {
                    "mta_id": f"{token}{i}",
                    "event_id": "e",
                    "token_id": token,
                    "cutoff": iso(now - timedelta(hours=hours)),
                    "move_class": "UNKNOWN",
                    "data_confidence": {"class": confidence},
                    "divergence": {"state": "UNKNOWN" if token == "T" else "NONE"},
                },
            )
    await repo.put(
        "ana.market_truth",
        "single",
        {
            "mta_id": "single",
            "event_id": "e",
            "token_id": "SINGLE",
            "cutoff": iso(now),
            "move_class": "UNKNOWN",
            "data_confidence": {"class": "HIGH"},
            "divergence": {"state": "UNKNOWN"},
        },
    )
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "known_to_us_at": iso(now), "public_at": iso(now - timedelta(hours=2))},
    )
    await repo.put(
        "evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": "e", "version": 1, "class": "MAJOR"}
    )
    await scheduler.detect_scout_hooks(now)
    proposals = await repo.list("scout.expansion_proposals")
    assert {p["hook"] for p in proposals} == {
        "MARKET_TRUTH_STATE",
        "ALTERNATIVE_EXPLANATION",
        "SOURCE_OPPORTUNITY",
    }
    assert not any("LOW" in p["concept_name"] or "SINGLE" in p["concept_name"] for p in proposals)
    assert all(p["recommendation"] == "SUBCATEGORY" and p["occurrences"] == 1 for p in proposals)
    jobs = await repo.list("ops.jobs")
    assert len(jobs) == 3 and all(
        j["priority"] == 4 and j["type"] == "SCOUT_REVIEW" and j["expires_at"] for j in jobs
    )
    await scheduler.detect_scout_hooks(now)
    assert len(await repo.list("ops.jobs")) == 3
    assert all(p["occurrences"] == 1 for p in await repo.list("scout.expansion_proposals"))


@pytest.mark.asyncio
async def test_scout_earlier_primary_discovery_and_contradictions_without_truth(tmp_path):
    from tank.settings import Settings

    repo = MemoryRepository()
    now = utcnow()
    scheduler = Scheduler(repo, Settings(data_dir=tmp_path), None, None)
    await repo.put("evt.events", "e", {"event_id": "e", "known_to_us_at": iso(now), "public_at": iso(now)})
    for key, klass, sid, published, observed in [
        ("secondary", "SECONDARY", "S30", now - timedelta(minutes=10), now - timedelta(minutes=5)),
        ("primary", "PRIMARY", "S01", now - timedelta(minutes=30), now),
    ]:
        await repo.put(
            "ingest.source_items",
            key,
            {
                "item_id": key,
                "source_id": sid,
                "source_class": klass,
                "published_at": iso(published),
                "observed_at": iso(observed),
            },
        )
        await repo.put(
            "evt.event_observations",
            key,
            {"obs_id": key, "event_id": "e", "item_id": key, "observed_at": iso(observed)},
        )
    for key, kind in [("u", "UNRESOLVED"), ("d", "DIAGNOSTIC")]:
        await repo.put(
            "ana.contradictions",
            key,
            {"contradiction_id": key, "event_id": "e", "token_id": "T", "type": kind},
        )
    await scheduler.detect_scout_hooks(now)
    proposals = await repo.list("scout.expansion_proposals")
    assert {p["hook"] for p in proposals} == {"SOURCE_OPPORTUNITY", "ALTERNATIVE_EXPLANATION"}
    source = next(p for p in proposals if p["hook"] == "SOURCE_OPPORTUNITY")
    assert source["evidence"] == ["primary", "secondary"]


@pytest.mark.asyncio
async def test_reaction_cpu_computation_does_not_block_async_heartbeat(monkeypatch):
    import asyncio
    import threading

    import tank.analysis.truth

    repo = MemoryRepository()
    now = utcnow()
    started = threading.Event()
    release = threading.Event()
    loop_advanced = asyncio.Event()

    def slow_compute(snapshot, *, config):
        started.set()
        # A bounded emergency release avoids hanging the test if this regresses to inline execution.
        if not release.wait(timeout=1):
            raise AssertionError("Async heartbeat could not run while CPU worker was waiting")
        return {"relative": {"windows": {"EVENT": {}}}}

    monkeypatch.setattr(tank.analysis.truth, "compute_market_truth", slow_compute)
    scheduler = Scheduler(
        repo,
        SimpleNamespace(config_version="cfg", code_version="code"),
        SimpleNamespace(snapshot=AsyncMock(return_value={})),
        None,
    )

    async def heartbeat():
        while not started.is_set():
            await asyncio.sleep(0)
        loop_advanced.set()
        release.set()

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        await scheduler.reaction_capture(
            {
                "event_id": "e",
                "token_id": "T",
                "checkpoint": "POST_1H",
                "anchor": {"type": "T1", "ts": iso(now - timedelta(hours=1))},
                "retro": False,
            }
        )
        assert loop_advanced.is_set()
    finally:
        release.set()
        await heartbeat_task


@pytest.mark.asyncio
async def test_reaction_revision_appends_reasoned_frozen_history_without_duplicate_storm(monkeypatch):
    from copy import deepcopy

    import tank.analysis.truth

    repo = MemoryRepository()
    now = utcnow()
    scheduler = Scheduler(
        repo,
        SimpleNamespace(config_version="cfg", code_version="code"),
        SimpleNamespace(snapshot=AsyncMock(return_value={})),
        None,
    )
    values = [0.01, 0.02]

    def compute(*args, **kwargs):
        return {"relative": {"windows": {"EVENT": {"r_raw": values.pop(0)}}}}

    monkeypatch.setattr(tank.analysis.truth, "compute_market_truth", compute)
    original = {
        "event_id": "e",
        "token_id": "T",
        "checkpoint": "POST_1H",
        "anchor": {"type": "T1", "ts": iso(now - timedelta(hours=1))},
        "retro": True,
    }
    await scheduler.reaction_capture(original)
    frozen = deepcopy(await repo.get("out.reaction_checkpoints", "e:T:POST_1H:1"))
    with pytest.raises(ValueError, match="reason"):
        await scheduler.reaction_capture(
            {"event_id": "e", "token_id": "T", "checkpoint": "POST_1H", "revision": 2}
        )
    revision = {
        "event_id": "e",
        "token_id": "T",
        "checkpoint": "POST_1H",
        "revision": 2,
        "reason": "Late archive trades filled an exchange gap",
    }
    await scheduler.reaction_capture(revision)
    await scheduler.reaction_capture(revision)
    await scheduler.reaction_capture(original)
    rows = await repo.list("out.reaction_checkpoints")
    assert len(rows) == 2 and (await repo.get("out.reaction_checkpoints", "e:T:POST_1H:1")) == frozen
    latest = await repo.get("out.reaction_checkpoints", "e:T:POST_1H:2")
    assert (
        latest["r_raw"] == 0.02
        and latest["reason"] == revision["reason"]
        and latest["supersedes_revision"] == 1
    )
    assert scheduler.market.snapshot.await_count == 2


@pytest.mark.asyncio
async def test_call_expiry_sets_durable_marker_before_message_and_does_not_exit():
    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.calls",
        "c",
        {
            "call_id": "c",
            "event_id": "e",
            "token_id": "T",
            "status": "OPEN",
            "opened_at": iso(now - timedelta(days=8)),
        },
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    await scheduler.expire_calls(now)
    event = await repo.get("evt.events", "e")
    call = await repo.get("ana.calls", "c")
    assert event["call_expired"] and event["call_expired_ids"] == ["c"]
    assert call["closed_by"] == "EXPIRED" and call["status"] == "CLOSED"
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1 and messages[0]["payload"]["kind"] == "CALL_EXPIRED"
    await scheduler.expire_calls(now)
    assert len(await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})) == 1


@pytest.mark.asyncio
async def test_heartbeat_reports_actual_queued_and_running_analysis_backlog(tmp_path):
    from tank.settings import Settings

    repo = MemoryRepository()
    now = utcnow()
    await repo.enqueue("FILTER_ITEM", {}, "filter")
    await repo.enqueue("AGENT_MESSAGE", {}, "message")
    running = await repo.enqueue("RUN_MARKET_TRUTH", {}, "truth")
    await repo.put("ops.jobs", running["job_id"], {**running, "status": "RUNNING"})
    await repo.enqueue("DELIVER", {}, "delivery")
    await repo.enqueue("MAINTENANCE", {}, "maintenance")
    await Scheduler(repo, Settings(data_dir=tmp_path), None, None).heartbeat(now)
    notice = (await repo.list("out.ops_notices"))[0]
    assert notice["analysis_backlog"] == {"queued": 2, "running": 1}
    assert "analysis backlog queued=2, running=1" in notice["text"]
    assert notice["queue_lag_s"] >= 0


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_location", ["data", "config", "logs"])
async def test_backup_excludes_fake_auth_secrets_and_browser_state_in_every_evidence_root(
    monkeypatch, tmp_path, auth_location
):
    import asyncio
    from pathlib import Path

    from tank.scheduler import verify_backup

    root = tmp_path / auth_location
    data = root / "data"
    config = root / "config"
    logs = root / "logs"
    target = root / "backup"
    for directory in (data, config, logs, target):
        directory.mkdir(parents=True)
    (data / "retained.txt").write_text("permanent data")
    (config / "registry.yaml").write_text("instruments: []")
    (logs / "service.log").write_text("redacted service log")
    roots = {"data": data, "config": config, "logs": logs}
    auth_dir = roots[auth_location] / "subscription-auth"
    auth_dir.mkdir()
    auth = auth_dir / "auth.json"
    auth.write_text("FAKE_SUBSCRIPTION_PRIVATE_TOKEN")
    (auth_dir / "refresh.tmp").write_text("FAKE_REFRESH_PRIVATE_TOKEN")
    secrets = config / "docker-secrets"
    secrets.mkdir()
    (secrets / "gateway_key").write_text("FAKE_GATEWAY_PRIVATE_KEY")
    storage = data / "x-storage-state.json"
    storage.write_text("FAKE_X_PRIVATE_STATE")
    profile = logs / "browser-profile"
    profile.mkdir()
    (profile / "Cookies").write_text("FAKE_X_PRIVATE_COOKIES")
    now = utcnow().replace(hour=8)
    # Stale failed generations must not leak formerly copied secret files into a retry.
    stale = target / f".tank-{now.date()}.partial"
    stale.mkdir()
    (stale / "stale-secret.txt").write_text("FAKE_STALE_PRIVATE_SECRET")

    async def dump(*args, **kwargs):
        Path(args[args.index("--file") + 1]).write_bytes(b"fixture database dump without credentials")
        return SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"")), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", dump)
    repo = MemoryRepository()
    settings = SimpleNamespace(
        backup_dir=target,
        backup_hour_utc=2,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
        logs_dir=logs,
        x_profile_dir=profile,
        x_storage_state_path=storage,
        secrets_dir=secrets,
        codex_auth_path=auth,
    )
    result = await Scheduler(repo, settings, None, None).maintenance(now, force=True, prune=False)
    backup = Path(result["path"])
    manifest = verify_backup(backup)
    all_bytes = b"".join(path.read_bytes() for path in backup.rglob("*") if path.is_file())
    assert b"PRIVATE_" not in all_bytes and b"FAKE_STALE" not in all_bytes
    assert not (backup / "data").exists()
    assert not (backup / "config").exists() and not (backup / "logs").exists()
    assert manifest["roots"] == {"bundles": str((data / "bundles").resolve())}
    assert "subscription sign-in directory" in manifest["excluded"]
    assert all(path.exists() for path in (auth, storage, profile / "Cookies", secrets / "gateway_key"))


@pytest.mark.asyncio
async def test_raw_retention_never_opens_or_removes_subscription_auth_parent(monkeypatch, tmp_path):
    import hashlib
    import json

    data = tmp_path / "data"
    auth_dir = data / "raw/trades/venue/spot/T" / (utcnow() - timedelta(days=90)).date().isoformat()
    auth_dir.mkdir(parents=True)
    auth = auth_dir / "auth.json"
    auth.write_text("FAKE_PRIVATE_AUTH")
    possible_raw = auth_dir / "old.parquet"
    possible_raw.write_bytes(b"FAKE_PRIVATE_REFRESH_STATE")
    backup = tmp_path / "backup"
    backup.mkdir()
    for name in ("tank.dump", "oma.dump"):
        (backup / name).write_bytes(b"dump")
    manifest = {
        "files": {
            name: hashlib.sha256((backup / name).read_bytes()).hexdigest()
            for name in ("tank.dump", "oma.dump")
        },
        "roots": {"data": str(data)},
        "reference_base": str(tmp_path),
    }
    (backup / "manifest.json").write_text(json.dumps(manifest))
    (backup / "COMPLETE").write_text("complete")
    repo = MemoryRepository()
    now = utcnow()
    settings = SimpleNamespace(
        data_dir=data, codex_auth_path=auth, load_config=lambda name: {"instruments": []}
    )
    assert await Scheduler(repo, settings, None, None).prune_raw_files(now, backup) == []
    assert auth.exists() and possible_raw.exists()


def test_existing_backup_with_newly_protected_credential_is_rejected_before_read(monkeypatch, tmp_path):
    import hashlib
    import json
    from pathlib import Path

    from tank.scheduler import protected_evidence_paths, verify_backup

    data = tmp_path / "data"
    auth = data / "auth-volume/auth.json"
    backup = tmp_path / "backup"
    backup.mkdir()
    copied = backup / "data/auth-volume/auth.json"
    copied.parent.mkdir(parents=True)
    copied.write_text("FAKE_PRIVATE_CREDENTIAL")
    files = {}
    for name in ("tank.dump", "oma.dump"):
        (backup / name).write_bytes(b"dump")
        files[name] = hashlib.sha256(b"dump").hexdigest()
    files["data/auth-volume/auth.json"] = hashlib.sha256(b"FAKE_PRIVATE_CREDENTIAL").hexdigest()
    (backup / "manifest.json").write_text(json.dumps({"files": files, "roots": {"data": str(data)}}))
    (backup / "COMPLETE").write_text("complete")
    original = Path.read_bytes

    def read(path, *args, **kwargs):
        if path == copied:
            raise AssertionError("Protected credentials must not be opened during verification")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", read)
    with pytest.raises(ValueError, match="protected credential"):
        verify_backup(backup, protected=protected_evidence_paths(SimpleNamespace(codex_auth_path=auth)))


@pytest.mark.asyncio
async def test_tick_does_not_wait_for_backup_and_partitions_run_independently():
    import asyncio

    repo = MemoryRepository()
    repo.maintain_partitions = AsyncMock()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked(now):
        entered.set()
        await release.wait()

    scheduler.maintenance = blocked
    scheduler.detect_scout_hooks = AsyncMock()
    scheduler.heartbeat = AsyncMock()
    try:
        await asyncio.wait_for(scheduler.tick(), 0.5)
        await asyncio.wait_for(entered.wait(), 0.5)
        await asyncio.sleep(0)
        repo.maintain_partitions.assert_awaited_once()
        await asyncio.wait_for(scheduler.tick(), 0.5)
        assert not scheduler.background["backup"].done()
    finally:
        release.set()
        await scheduler.close()


@pytest.mark.asyncio
async def test_milestone_payload_contains_only_verified_outcome_and_quotes():
    repo = MemoryRepository()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    event = {
        "event_id": "e",
        "milestone_resolutions": [
            {
                "kind": "VOTE",
                "due_at": "2026-01-01T00:00:00Z",
                "outcome": "Passed",
                "quotes": ["Official vote passed"],
                "evidence": ["claim1"],
            }
        ],
    }
    await repo.put("evt.events", "e", event)
    await scheduler.milestone_check({"event_id": "e", "milestone": "VOTE", "due_at": "2026-01-01T00:00:00Z"})
    jobs = await repo.list("ops.jobs")
    payload = jobs[0]["payload"]
    assert (
        payload["outcome"] == "Passed"
        and payload["quotes"] == ["Official vote passed"]
        and not payload["verification_required"]
    )
    await scheduler.milestone_check({"event_id": "e", "milestone": "VOTE", "due_at": "2026-02-01T00:00:00Z"})
    jobs = await repo.list("ops.jobs")
    pending = next(j["payload"] for j in jobs if j["payload"]["verification_required"])
    assert pending["outcome"] is None and pending["quotes"] == []


@pytest.mark.asyncio
async def test_reaction_future_expiry_and_completed_checkpoint_not_recreated():
    repo = MemoryRepository()
    now = utcnow()
    anchor = iso(now)
    await repo.put(
        "evt.events",
        "e",
        {
            "event_id": "e",
            "lifecycle": "MONITORING",
            "known_to_us_at": anchor,
            "event_time": anchor,
            "public_at": anchor,
            "current_ems_id": "ems",
        },
    )
    await repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": "e", "class": "MAJOR"})
    await repo.put(
        "out.reaction_checkpoints",
        "done",
        {"event_id": "e", "token_id": "bitcoin:BTC", "checkpoint": "PRE_1H", "revision": 1},
        immutable=True,
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    scheduler.detect_scout_hooks = AsyncMock()
    scheduler.heartbeat = AsyncMock()
    scheduler.maintenance = AsyncMock()
    try:
        await scheduler.tick(now)
        jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        assert not [
            j
            for j in jobs
            if j["payload"]["token_id"] == "bitcoin:BTC" and j["payload"]["checkpoint"] == "PRE_1H"
        ]
        future = next(j for j in jobs if j["payload"]["checkpoint"] == "POST_72H")
        from tank.util import parse_time

        assert parse_time(future["run_after"]) == now + timedelta(hours=72)
        assert parse_time(future["expires_at"]) == now + timedelta(hours=78)
    finally:
        await scheduler.close()


@pytest.mark.asyncio
async def test_scheduler_honors_configured_call_expiry():
    repo = MemoryRepository()
    now = utcnow()
    settings = SimpleNamespace(load_config=lambda name: {"call_expiry_days": 2, "hot_token_cap": 1})
    scheduler = Scheduler(repo, settings, None, None)
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.calls",
        "c",
        {
            "call_id": "c",
            "event_id": "e",
            "token_id": "t",
            "status": "OPEN",
            "opened_at": iso(now - timedelta(days=3)),
        },
    )
    await scheduler.expire_calls(now)
    assert (await repo.get("ana.calls", "c"))["status"] == "CLOSED"
    assert (await repo.get("evt.events", "e"))["call_expired"] is True


@pytest.mark.asyncio
async def test_reaction_pack_ulid_stable_for_group_across_restart_and_legacy():
    repo = MemoryRepository()
    settings = SimpleNamespace()
    await repo.put(
        "out.reaction_checkpoints",
        "legacy",
        {"rwp_id": "event:token", "event_id": "e", "token_id": "t", "checkpoint": "PRE_1H", "revision": 1},
        immutable=True,
    )
    first = await Scheduler(repo, settings, None, None).reaction_pack_id("e", "t")
    second = await Scheduler(repo, settings, None, None).reaction_pack_id("e", "t")
    assert first == second and first.startswith("RWP-") and len(first) == 30
    assert (await repo.get("out.reaction_checkpoints", "legacy"))["rwp_id"] == "event:token"
    assert await Scheduler(repo, settings, None, None).reaction_pack_id("e", "other") != first


@pytest.mark.asyncio
async def test_reaction_targets_use_current_analysis_map_not_historical_edges():
    repo = MemoryRepository()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    await repo.put(
        "evt.beneficiary_maps", "old", {"event_id": "e", "map_version": 1, "analysis_set": ["old"]}
    )
    await repo.put(
        "evt.beneficiary_maps", "current", {"event_id": "e", "map_version": 2, "analysis_set": ["current"]}
    )
    await repo.put("evt.beneficiary_edges", "edge", {"event_id": "e", "map_version": 1, "token_id": "old"})
    assert await scheduler.analysis_tokens({"event_id": "e"}) == {"current", "bitcoin:BTC", "ethereum:ETH"}


def legacy_full_backup_fixture(settings, now):
    """Historical manifest compatibility fixture, not the current backup policy."""
    import json
    import shutil
    from pathlib import Path

    from tank.scheduler import file_digest, is_protected_evidence_path, protected_evidence_paths

    final = Path(settings.backup_dir) / f"tank-{now.date()}"
    final.mkdir()
    protected = protected_evidence_paths(settings)
    roots = {"data": Path(settings.data_dir), "config": Path(settings.config_dir)}
    if Path(settings.logs_dir).is_dir():
        roots["logs"] = Path(settings.logs_dir)
    for label, root in roots.items():
        for source in root.rglob("*"):
            if source.is_file() and not is_protected_evidence_path(source, protected):
                target = final / label / source.relative_to(root)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
    for name, payload in (("tank.dump", b"dump tank"), ("oma.dump", b"dump oma")):
        (final / name).write_bytes(payload)
    files = {
        path.relative_to(final).as_posix(): file_digest(path) for path in final.rglob("*") if path.is_file()
    }
    (final / "manifest.json").write_text(
        json.dumps({"files": files, "roots": {label: str(root.resolve()) for label, root in roots.items()}})
    )
    (final / "COMPLETE").write_text(iso(now))
    return final


@pytest.mark.asyncio
async def test_reaction_measurement_releases_database_scope_and_duplicate_commit_is_idempotent(monkeypatch):
    import asyncio
    from contextlib import asynccontextmanager

    import tank.analysis.truth

    class TransactionalLocks(MemoryRepository):
        @asynccontextmanager
        async def lock(self, key):
            async with super().lock(key), self.transaction():
                yield self

    repo = TransactionalLocks()
    entered = 0
    both_reading = asyncio.Event()

    async def snapshot(*args, **kwargs):
        nonlocal entered
        assert repo._owner is None, "Market capture would inherit a long database transaction"
        entered += 1
        if entered == 2:
            both_reading.set()
        await asyncio.wait_for(both_reading.wait(), 2)
        return {}

    monkeypatch.setattr(tank.analysis.truth, "compute_market_truth", lambda snapshot, config: {})
    scheduler = Scheduler(
        repo,
        SimpleNamespace(config_version="cfg", code_version="code"),
        SimpleNamespace(snapshot=snapshot),
        None,
    )
    payload = {
        "event_id": "e",
        "token_id": "T",
        "checkpoint": "POST_1H",
        "anchor": {"type": "T1", "ts": iso(utcnow() - timedelta(hours=1))},
    }
    first, duplicate = await asyncio.gather(
        scheduler.reaction_capture(payload), scheduler.reaction_capture(payload)
    )
    assert first == duplicate
    assert len(await repo.list("out.reaction_checkpoints")) == 1


@pytest.mark.asyncio
async def test_reaction_creation_registers_only_five_post_jobs_and_starts_pre_without_waiting():
    import asyncio

    repo = MemoryRepository()
    now = utcnow()
    event = {"event_id": "new-major", "public_at": iso(now), "public_at_prec": "EXACT"}
    scheduler = Scheduler(repo, SimpleNamespace(), object(), None)
    scheduler.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    scheduler.analysis_tokens = AsyncMock(return_value={"token:T"})
    started, release = asyncio.Event(), asyncio.Event()
    captured = []

    async def capture(payload, *, wait_for_slot=False):
        assert wait_for_slot  # background PRE capture waits for a slot
        captured.append(payload)
        started.set()
        await release.wait()

    scheduler.reaction_capture = capture
    try:
        await asyncio.wait_for(scheduler.ensure_reactions(event, now=now), timeout=1)
        await asyncio.wait_for(started.wait(), timeout=1)
        await scheduler.ensure_reactions(event, now=now)
        jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        assert {j["payload"]["checkpoint"] for j in jobs} == {
            "POST_15M",
            "POST_1H",
            "POST_4H",
            "POST_24H",
            "POST_72H",
        }
        assert len(jobs) == 5
        assert len(scheduler.background) == 1
        assert len(captured) == 1 and captured[0]["checkpoint"] == "PRE_24H"
        assert captured[0]["retro"] is True
        assert all(j["priority"] == 2 and j["max_attempts"] == 5 for j in jobs)
        release.set()
        await asyncio.gather(*scheduler.background.values())
        assert [p["checkpoint"] for p in captured] == ["PRE_24H", "PRE_4H", "PRE_1H"]
    finally:
        await scheduler.close()


@pytest.mark.asyncio
async def test_reaction_pre_capture_restart_resumes_only_missing_immutable_rows(monkeypatch):
    import asyncio

    monkeypatch.setattr("tank.analysis.truth.compute_market_truth", lambda *a, **k: {})
    repo = MemoryRepository()
    now = utcnow()
    event = {"event_id": "resume-major", "public_at": iso(now), "public_at_prec": "EXACT"}
    settings = SimpleNamespace(config_version="config", code_version="code")
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    first = Scheduler(repo, settings, market, None)
    first.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    first.analysis_tokens = AsyncMock(return_value={"token:T"})
    original = await first.reaction_capture(
        {
            "event_id": event["event_id"],
            "token_id": "token:T",
            "checkpoint": "PRE_24H",
            "anchor": first.anchor(event),
            "retro": True,
        }
    )
    await first.close()
    restarted = Scheduler(repo, settings, market, None)
    restarted.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    restarted.analysis_tokens = AsyncMock(return_value={"token:T"})
    try:
        await restarted.ensure_reactions(event, now=now)
        await asyncio.gather(*restarted.background.values())
        rows = await repo.list("out.reaction_checkpoints")
        assert len(rows) == 3
        assert next(r for r in rows if r["checkpoint"] == "PRE_24H") == original
        assert market.snapshot.await_count == 3
        assert len(await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})) == 5
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_reaction_pre_error_retries_missing_windows_and_close_cancels_owned_work():
    import asyncio

    repo = MemoryRepository()
    event = {"event_id": "e", "public_at": iso(), "public_at_prec": "EXACT"}
    scheduler = Scheduler(repo, SimpleNamespace(), object(), None)
    scheduler.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    scheduler.analysis_tokens = AsyncMock(return_value={"T"})
    scheduler.reaction_capture = AsyncMock(side_effect=RuntimeError("synthetic capture failure"))
    await scheduler.ensure_reactions(event)
    await asyncio.gather(*scheduler.background.values())  # failure remains retryable, never a PRE job
    assert scheduler.reaction_capture.await_count == 1
    started = asyncio.Event()

    async def block(payload, **_):
        started.set()
        await asyncio.Event().wait()

    scheduler.reaction_capture = block
    await scheduler.ensure_reactions(event)
    await asyncio.wait_for(started.wait(), timeout=1)
    task = next(iter(scheduler.background.values()))
    await scheduler.close()
    assert task.cancelled() and not scheduler.background
    assert len(await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})) == 5


@pytest.mark.asyncio
async def test_dynamic_scheduler_work_does_not_retain_completed_task_frames():
    import asyncio

    scheduler = Scheduler(MemoryRepository(), SimpleNamespace(), None, None)
    operation = AsyncMock()
    for number in range(200):
        scheduler.launch(f"reaction-pre:event-{number}:token", operation)
    tasks = tuple(scheduler.background.values())
    await asyncio.gather(*tasks)
    assert operation.await_count == 200
    assert not scheduler.background
    await scheduler.close()


@pytest.mark.asyncio
async def test_older_completion_callback_preserves_same_name_replacement_until_shutdown():
    import asyncio

    scheduler = Scheduler(MemoryRepository(), SimpleNamespace(), None, None)
    started = asyncio.Event()

    async def replacement():
        started.set()
        await asyncio.Event().wait()

    async def first():
        # This callback runs after the first task finishes but before its done
        # callbacks, exercising replacement while the old cleanup is queued.
        asyncio.get_running_loop().call_soon(scheduler.launch, "same", replacement)

    scheduler.launch("same", first)
    original = scheduler.background["same"]
    await asyncio.wait_for(started.wait(), timeout=1)
    current = scheduler.background["same"]
    assert current is not original and not current.done()
    await scheduler.close()
    assert current.cancelled() and not scheduler.background


@pytest.mark.asyncio
async def test_rearmed_market_watch_needs_a_new_window_and_a_clear():
    """W1: a re-armed watch never fires on the evidence that fired its
    predecessor, and a lasting level does not re-fire every window."""
    repo = MemoryRepository()
    armed = utcnow().replace(minute=5, second=0, microsecond=0)
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c2",
        {
            "cond_id": "c2",
            "event_id": "e",
            "token_id": "t",
            "role": "INVALIDATION",
            "status": "ARMED",
            "rearmed_from": "c1",
            "created_at": iso(armed),
            "expires_at": iso(armed + timedelta(days=1)),
            "expr": {"metric": "price.close_vs_level", "window": "W1H", "op": "<", "value": "10"},
        },
    )
    price = {"relative": {"close_1h_price": "9"}}
    truth = SimpleNamespace(assess=AsyncMock(side_effect=lambda *a, **k: price))
    scheduler = Scheduler(repo, SimpleNamespace(), None, truth)
    # The level that fired the predecessor still holds: no window after arming yet.
    await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=10))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    # A full window later the level still holds: a level never re-fires.
    await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=61))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    # Price recovers (the watch clears), then closes below again: it fires.
    price["relative"]["close_1h_price"] = "10.5"
    await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=62))})
    cleared = await repo.get("ana.watch_conditions", "c2")
    assert cleared["status"] == "ARMED" and cleared["cleared_at"]
    price["relative"]["close_1h_price"] = "9"
    await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=63))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "FIRED"


@pytest.mark.asyncio
async def test_rearmed_instant_watch_fires_again_only_after_it_cleared():
    """W1: an instant predicate (event.*, milestone.*) re-arms on an edge, so a
    lasting fact does not re-fire every tick."""
    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c2",
        {
            "cond_id": "c2",
            "event_id": "e",
            "token_id": "t",
            "role": "INVALIDATION",
            "status": "ARMED",
            "rearmed_from": "c1",
            "created_at": iso(now),
            "expires_at": iso(now + timedelta(days=1)),
            "expr": {"metric": "event.contradiction_fatal", "window": "W1H", "op": "==", "value": True},
        },
    )
    fatal = {"contradiction_id": "f1", "event_id": "e", "type": "FATAL"}
    await repo.put("ana.contradictions", "f1", fatal)
    scheduler = Scheduler(repo, SimpleNamespace(), None, SimpleNamespace(assess=AsyncMock(return_value={})))
    await scheduler.eval_conditions({"cutoff": iso(now + timedelta(minutes=1))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    await repo.put("ana.contradictions", "f1", {**fatal, "resolved_at": iso(now)})
    await scheduler.eval_conditions({"cutoff": iso(now + timedelta(minutes=2))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    await repo.put("ana.contradictions", "f2", {**fatal, "contradiction_id": "f2"})
    await scheduler.eval_conditions({"cutoff": iso(now + timedelta(minutes=3))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "FIRED"


@pytest.mark.asyncio
async def test_rearmed_persistent_watch_clears_only_on_an_observed_false_window():
    """W1: an unfinished first window after arming is not a clear, so lasting
    spot BUY after START does not re-fire the same upgrade every two hours."""
    repo = MemoryRepository()
    armed = utcnow().replace(minute=10, second=0, microsecond=0)
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c2",
        {
            "cond_id": "c2",
            "event_id": "e",
            "token_id": "t",
            "role": "UPGRADE",
            "status": "ARMED",
            "rearmed_from": "c1",
            "created_at": iso(armed),
            "expires_at": iso(armed + timedelta(days=1)),
            "expr": {
                "metric": "spot.cohort.direction",
                "window": "W1H",
                "op": "==",
                "value": "BUY",
                "persist": {"count": 2},
            },
        },
    )
    from tank.util import parse_time

    sell_hours = set()

    def by_window(event_id, token_id, cutoff):
        hour = parse_time(cutoff.value).replace(minute=0, second=0, microsecond=0)
        return {"spot": {"W1H": {"direction": "SELL" if hour in sell_hours else "BUY"}}}

    truth = SimpleNamespace(assess=AsyncMock(side_effect=by_window))
    scheduler = Scheduler(repo, SimpleNamespace(), None, truth)
    for minutes in (5, 55, 115, 175):
        await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=minutes))})
        row = await repo.get("ana.watch_conditions", "c2")
        assert row["status"] == "ARMED" and not row.get("cleared_at")
    sell_hours.add((armed + timedelta(minutes=235)).replace(minute=0))
    await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=235))})
    assert (await repo.get("ana.watch_conditions", "c2")).get("cleared_at")
    await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=295))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    await scheduler.eval_conditions({"cutoff": iso(armed + timedelta(minutes=355))})
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "FIRED"


@pytest.mark.asyncio
async def test_late_data_revisions_coalesce_to_one_queued_job_per_checkpoint():
    """03 §8.6: one queued late revision per (event, token, checkpoint); later
    capture changes do not pile up duplicate jobs ahead of live checkpoints."""
    repo = MemoryRepository()
    now = utcnow()
    anchor = iso(now - timedelta(hours=2))
    for label in ("PRE_24H", "POST_15M"):
        await repo.put(
            "out.reaction_checkpoints",
            "e:t:" + label,
            {
                "event_id": "e",
                "token_id": "t",
                "checkpoint": label,
                "revision": 1,
                "captured_at": iso(now - timedelta(hours=1)),
                "anchor": {"ts": anchor},
            },
        )

    class Market:
        def __init__(self):
            self.round = 0

        async def capture_changes(self, since, end):
            self.round += 1
            yield {
                "change_id": "CAP-LIVE-" + str(self.round),
                "event_id": "e",
                "token_id": "t",
                "anchor": anchor,
                "observed_at": iso(now),
            }

    scheduler = Scheduler(repo, SimpleNamespace(), Market(), None)
    for _ in range(3):
        await scheduler.detect_late_data(now)
    jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
    assert sorted(job["payload"]["checkpoint"] for job in jobs) == ["POST_15M", "PRE_24H"]


@pytest.mark.asyncio
async def test_queued_duplicate_late_revision_is_skipped_at_run_time():
    """03 §8.6: with another automatic revision of the same checkpoint still
    queued, a running one returns at once; the queued one measures all data."""
    repo = MemoryRepository()
    payload = {"event_id": "e", "token_id": "t", "checkpoint": "PRE_4H", "automatic_revision": True}
    queued = await repo.enqueue(
        "REACTION_CAPTURE", dict(payload, change_id="newer"), "reaction-late:x:PRE_4H", priority=2
    )
    market = SimpleNamespace(snapshot=AsyncMock(side_effect=AssertionError("must not measure")))
    scheduler = Scheduler(repo, SimpleNamespace(), market, None)
    result = await scheduler.reaction_capture(dict(payload, change_id="older"))
    assert result == {"coalesced_into": queued["job_id"]}
    market.snapshot.assert_not_called()


@pytest.mark.asyncio
async def test_reaction_measurements_are_limited_and_extra_jobs_requeue():
    """At most REACTION_MEASUREMENT_SLOTS measurements run at once; another job
    is deferred (re-queued without an attempt) instead of holding a worker."""
    import asyncio

    from tank.delivery import DeliveryDeferred
    from tank.scheduler import REACTION_MEASUREMENT_SLOTS

    scheduler = Scheduler(MemoryRepository(), SimpleNamespace(), None, None)
    release = asyncio.Event()
    started = []

    async def plan(payload):
        return {"complete": False, "payload": payload, "key": payload["checkpoint"]}

    async def measure(payload):
        started.append(payload["checkpoint"])
        await release.wait()
        return {}

    async def commit(plan, measured):
        return {"committed": plan["key"]}

    scheduler._reaction_plan, scheduler._measure_reaction, scheduler._commit_reaction = plan, measure, commit
    payloads = [{"event_id": "e", "token_id": "t", "checkpoint": f"C{n}"} for n in range(3)]
    running = [asyncio.create_task(scheduler.reaction_capture(p)) for p in payloads[:REACTION_MEASUREMENT_SLOTS]]
    await asyncio.sleep(0)
    with pytest.raises(DeliveryDeferred):
        await scheduler.reaction_capture(payloads[-1])
    # A background PRE capture waits for a slot instead of deferring.
    waiting = asyncio.create_task(scheduler.reaction_capture(payloads[-1], wait_for_slot=True))
    await asyncio.sleep(0)
    assert not waiting.done() and len(started) == REACTION_MEASUREMENT_SLOTS
    release.set()
    assert [r["committed"] for r in await asyncio.gather(*running)] == ["C0", "C1"]
    assert (await waiting) == {"committed": "C2"}
    assert await scheduler.reaction_capture(payloads[-1]) == {"committed": "C2"}

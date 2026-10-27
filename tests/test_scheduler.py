from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from foundation_helpers import object_field
from pydantic import JsonValue
from scheduler_support import operational_reaction_services
from source_support import fixture_request_budgets, fixture_versions

from tank.analysis.page_access import LivePageAccess
from tank.analysis.watch_contracts import WatchCondition, WatchLeaf
from tank.contracts.milestone_checks import MilestoneCheckCommand
from tank.contracts.scheduler_commands import ReactionTarget, reaction_command_from_wire
from tank.contracts.scheduler_results import CapturedReaction
from tank.models import Record
from tank.runtime_contracts import ConditionFiredMessage, parse_agent_message
from tank.runtime_reaction import ReactionCoalescingDeferred, ReactionJobInvocation, capture_reaction_job
from tank.scheduler import REACTION_RETRY_S, Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp, collection_clock, propagate_timestamp
from tank.util import clock_iso, iso, sha256, utcnow


@pytest.mark.asyncio
async def test_recursive_hourly_persistence_does_not_count_same_window_twice() -> None:
    repo = MemoryRepository()
    now = utcnow().replace(minute=0, second=0, microsecond=0)
    truth = SimpleNamespace(
        assess=AsyncMock(return_value={"spot": {"W1H": {"direction": "BUY", "cw_breadth": {"buy": 0.8}}}})
    )
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        truth,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
    await scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now)}, "cutoff"))
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(now + timedelta(minutes=30))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c"))["status"] == "ARMED"
    await scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now + timedelta(hours=1))}, "cutoff"))
    assert (await repo.get("ana.watch_conditions", "c"))["status"] == "FIRED"
    assert len(await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})) == 1


@pytest.mark.asyncio
async def test_tracked_event_closes_after_seven_days_without_news() -> None:
    repo = MemoryRepository()
    now = utcnow()
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "lifecycle": "TRACKED", "known_to_us_at": iso(now - timedelta(days=8))},
    )
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    await scheduler.lifecycle(now)
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "CLOSED"


@pytest.mark.asyncio
async def test_pre_event_capture_uses_pre_window_and_is_immutable(monkeypatch: pytest.MonkeyPatch) -> None:
    import tank.analysis.truth

    repo = MemoryRepository()
    now = utcnow()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={"cutoff": iso(now)}))
    monkeypatch.setattr(
        tank.analysis.truth,
        "compute_market_truth",
        lambda snapshot, config: {"relative": {"windows": {"EVENT": {"r_raw": 0.01}}}},
    )
    scheduler = Scheduler(
        repo,
        Settings(config_version="cfg", code_version="code"),
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )

    payload = {
        "event_id": "e",
        "token_id": "test:T",
        "checkpoint": "PRE_24H",
        "anchor": {"type": "T1", "ts": iso(now)},
        "retro": True,
    }
    await scheduler.reaction_capture(reaction_command_from_wire(payload))
    await scheduler.reaction_capture(reaction_command_from_wire(payload))
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
async def test_missing_market_value_never_fires_not_equal_condition() -> None:

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
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        SimpleNamespace(assess=AsyncMock(return_value={})),
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    await scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now)}, "cutoff"))
    assert (await repo.get("ana.watch_conditions", "c"))["status"] == "ARMED"


@pytest.mark.asyncio
async def test_cooldown_requires_all_analysis_and_benchmark_checkpoints() -> None:
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
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
async def test_failed_backup_never_prunes_market(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import asyncio

    repo = MemoryRepository()
    repo.prune_market = AsyncMock()
    repo.prune_jobs = AsyncMock()
    process = SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"failure")), returncode=1)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    settings = Settings(
        backup_dir=tmp_path,
        backup_hour_utc=2,
        database_url="postgresql://u:secret@localhost/tank",
        data_dir=tmp_path / "data",
        market_retention_days=90,
    )
    scheduler = Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    with pytest.raises(RuntimeError, match="pg_dump failed"):
        await scheduler.maintenance(utcnow().replace(hour=8))
    repo.prune_market.assert_not_called()
    assert not list(tmp_path.glob("*/COMPLETE"))


@pytest.mark.asyncio
async def test_planned_backup_copies_bundles_and_dumps_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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
    settings = Settings(
        backup_dir=backup,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
    )
    result = await Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).maintenance(utcnow(), force=True, prune=False)
    final = Path(result["path"])
    manifest = verify_backup(final)
    assert set(manifest.roots) == {"bundles"}
    assert set(manifest.files) == {"tank.dump", "oma.dump", "bundles/hash.json.zst"}
    assert not (final / "data").exists() and not (final / "config").exists()
    assert (data / "market-cache").exists()


@pytest.mark.asyncio
async def test_heartbeat_preserves_unknown_llm_cost() -> None:
    from tank.settings import Settings

    repo = MemoryRepository()
    await repo.put("ops.llm_usage", "unknown", {"at": clock_iso(), "cost_usd": None})
    await repo.put("ops.llm_usage", "known", {"at": clock_iso(), "cost_usd": "1.25"})
    await Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).heartbeat(utcnow())
    notices = await repo.list("out.ops_notices")
    assert "known subtotal $1.25; unknown cost records=1" in notices[0]["text"]


@pytest.mark.asyncio
async def test_relocated_backup_recovers_source_web_replay_config_logs_and_both_databases(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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
    }
    for name, body in contents.items():
        path = data / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    config.mkdir()
    (config / "sources.yaml").write_text("sources: []")
    logs.mkdir()
    (logs / "openma.log").write_text("permanent session transcript")

    async def fake_dump(*args, **kwargs):
        Path(args[args.index("--file") + 1]).write_bytes(("dump " + kwargs["env"]["PGDATABASE"]).encode())
        return SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"")), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_dump)
    repo = MemoryRepository()
    repo.prune_market = AsyncMock()
    repo.prune_jobs = AsyncMock()
    raw_ref = str(data / "raw/sources/a.gz")
    await repo.put("ingest.source_items", "item", {"item_id": "item", "raw_ref": raw_ref})
    settings = Settings(
        _env_file=None,
        backup_dir=backup,
        backup_hour_utc=2,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
        logs_dir=logs,
        market_retention_days=90,
    )
    now = utcnow().replace(hour=8)
    legacy_full_backup_fixture(settings, now)
    await Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).maintenance(now)
    final = backup / f"tank-{now.date()}"
    verify_backup(final)
    assert (final / "tank.dump").read_bytes() == b"dump tank"
    assert (final / "oma.dump").read_bytes() == b"dump oma"

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


async def real_decision_watch_conditions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[MemoryRepository, Settings, datetime, list[WatchCondition]]:
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
    object_field(market, "data_confidence")["class"] = "MEDIUM"
    await repo.put("evt.events", "e", event)
    await repo.put("evt.ems_assessments", "ems", assessment)
    await repo.put("evt.beneficiary_maps", "map", {"event_id": "e", "map_version": 1, "analysis_set": ["T"]})
    await repo.put("evt.beneficiary_edges", "edge", beneficiary)
    await repo.put("ana.market_truth", "truth", market)
    reaction_settings = settings
    reaction_versions = fixture_versions()
    async with operational_reaction_services(
        repo, reaction_settings, versions=reaction_versions, budgets=fixture_request_budgets()
    ) as reaction_services:
        service = EventToolService(
            repo,
            reaction_settings,
            reaction_services.truth,
            DecisionEngine(),
            versions=reaction_versions,
            scheduler=reaction_services.scheduler,
            pages=LivePageAccess(reaction_services.admission),
        )
        decision = await service.get_decision(
            {"event_id": "e", "token_id": "T"}, {"session_id": "session", "call_id": "decision"}
        )
        assert decision["action"] == "WATCH_TO_START"
        conditions = [WatchCondition.model_validate(row) for row in await repo.list("ana.watch_conditions")]
        assert conditions and all(condition.created_at and condition.expires_at for condition in conditions)
        return repo, settings, created, conditions


@pytest.mark.asyncio
async def test_real_generated_conditions_use_two_closed_nonoverlapping_hours(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from tank.util import parse_time

    repo, settings, created, conditions = await real_decision_watch_conditions(monkeypatch, tmp_path)
    calls: list[Timestamp] = []

    async def assess(event_id: str, token_id: str, cutoff: Timestamp) -> Record:
        calls.append(cutoff)
        assert cutoff.value is not None
        stamp = parse_time(cutoff.value)
        assert cutoff.precision == ("EXACT" if stamp.minute == 0 and stamp.second == 0 else None)

        direction = "BUY" if stamp.minute == 0 and stamp.second == 0 else "SELL"
        return {
            "spot": {"W1H": {"direction": direction, "cw_breadth": {"buy": 0.8, "sell": 0.2}}},
            "derivatives": {"synchronized": {"native_oi_vs_normal": "WITHIN"}},
            "relative": {"close_1h_price": "10"},
        }

    scheduler = Scheduler(
        repo,
        settings,
        None,
        SimpleNamespace(assess=assess),
        thresholds=settings.thresholds(),
        versions=fixture_versions(),
    )
    upgrade = next(condition for condition in conditions if condition.role == "UPGRADE")
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(created.replace(hour=12, minute=7))}, "cutoff")
    )
    first = WatchCondition.model_validate(await repo.get("ana.watch_conditions", upgrade.cond_id))
    assert first.status == "ARMED"
    assert first.counters["root0"].count == 1
    assert first.counters["root0"].cutoff == iso(created.replace(hour=12, minute=0))
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(created.replace(hour=12, minute=59))}, "cutoff")
    )
    assert (
        WatchCondition.model_validate(await repo.get("ana.watch_conditions", upgrade.cond_id))
        .counters["root0"]
        .count
        == 1
    )
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(created.replace(hour=13, minute=1))}, "cutoff")
    )
    final = WatchCondition.model_validate(await repo.get("ana.watch_conditions", upgrade.cond_id))
    assert final.status == "FIRED" and final.counters["root0"].count == 2
    assert final.counters["root0"].cutoff == iso(created.replace(hour=13, minute=0))
    fired = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    messages = [parse_agent_message(object_field(job, "payload")) for job in fired]
    assert (
        sum(
            isinstance(message, ConditionFiredMessage) and message.condition.cond_id == upgrade.cond_id
            for message in messages
        )
        == 1
    )


@pytest.mark.asyncio
async def test_real_generated_fatal_condition_fires_next_minute_without_hour_throttle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo, settings, created, conditions = await real_decision_watch_conditions(monkeypatch, tmp_path)
    truth = SimpleNamespace(assess=AsyncMock(return_value={"relative": {"close_1h_price": "10"}}))
    scheduler = Scheduler(
        repo,
        settings,
        None,
        truth,
        thresholds=settings.thresholds(),
        versions=fixture_versions(),
    )
    fatal = next(
        condition
        for condition in conditions
        if isinstance(condition.expr, WatchLeaf) and condition.expr.metric == "event.contradiction_fatal"
    )
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(created.replace(hour=12, minute=0))}, "cutoff")
    )
    assert (
        WatchCondition.model_validate(await repo.get("ana.watch_conditions", fatal.cond_id)).status == "ARMED"
    )
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
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(created.replace(hour=12, minute=1))}, "cutoff")
    )
    assert (
        WatchCondition.model_validate(await repo.get("ana.watch_conditions", fatal.cond_id)).status == "FIRED"
    )


def test_condition_derivatives_use_requested_window_without_primary_fallback() -> None:
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
async def test_post72_reaction_uses_cumulative_event_derivatives(monkeypatch: pytest.MonkeyPatch) -> None:
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
    scheduler = Scheduler(
        repo,
        Settings(config_version="cfg", code_version="code"),
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    await scheduler.reaction_capture(
        reaction_command_from_wire(
            {
                "event_id": "e",
                "token_id": "T",
                "checkpoint": "POST_72H",
                "anchor": {"type": "T1", "ts": iso(now - timedelta(hours=72))},
                "retro": False,
            }
        )
    )
    record = (await repo.list("out.reaction_checkpoints"))[0]
    assert record["oi_native_chg_pct"] == 12 and record["oi_class"] == "REAL_NATIVE"
    assert record["funding"] == event_derivatives["per_venue"]
    assert record["liquidations"] == event_derivatives["liquidations"]


@pytest.mark.asyncio
async def test_automatic_scout_hooks_dedup_and_ignore_low_confidence_or_single_unknown(
    tmp_path: Path,
) -> None:
    from tank.settings import Settings

    repo = MemoryRepository()
    now = utcnow()
    settings = Settings(data_dir=tmp_path)
    scheduler = Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
async def test_scout_earlier_primary_discovery_and_contradictions_without_truth(tmp_path: Path) -> None:
    from tank.settings import Settings

    repo = MemoryRepository()
    now = utcnow()
    scheduler = Scheduler(
        repo,
        Settings(data_dir=tmp_path),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
async def test_reaction_cpu_computation_does_not_block_async_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

        if not release.wait(timeout=1):
            raise AssertionError("Async heartbeat could not run while CPU worker was waiting")
        return {"relative": {"windows": {"EVENT": {}}}}

    monkeypatch.setattr(tank.analysis.truth, "compute_market_truth", slow_compute)
    scheduler = Scheduler(
        repo,
        Settings(config_version="cfg", code_version="code"),
        SimpleNamespace(snapshot=AsyncMock(return_value={})),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )

    async def heartbeat() -> None:
        while not started.is_set():
            await asyncio.sleep(0)
        loop_advanced.set()
        release.set()

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        await scheduler.reaction_capture(
            reaction_command_from_wire(
                {
                    "event_id": "e",
                    "token_id": "T",
                    "checkpoint": "POST_1H",
                    "anchor": {"type": "T1", "ts": iso(now - timedelta(hours=1))},
                    "retro": False,
                }
            )
        )
        assert loop_advanced.is_set()
    finally:
        release.set()
        await heartbeat_task


@pytest.mark.asyncio
async def test_reaction_revision_appends_reasoned_frozen_history_without_duplicate_storm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from copy import deepcopy

    import tank.analysis.truth

    repo = MemoryRepository()
    now = utcnow()
    scheduler = Scheduler(
        repo,
        Settings(config_version="cfg", code_version="code"),
        SimpleNamespace(snapshot=AsyncMock(return_value={})),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
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
    await scheduler.reaction_capture(reaction_command_from_wire(original))
    frozen = deepcopy(await repo.get("out.reaction_checkpoints", "e:T:POST_1H:1"))
    with pytest.raises(ValueError, match="reason"):
        await scheduler.reaction_capture(
            reaction_command_from_wire(
                {"event_id": "e", "token_id": "T", "checkpoint": "POST_1H", "revision": 2}
            )
        )
    revision = {
        "event_id": "e",
        "token_id": "T",
        "checkpoint": "POST_1H",
        "revision": 2,
        "reason": "Late archive trades filled an exchange gap",
    }
    await scheduler.reaction_capture(reaction_command_from_wire(revision))
    await scheduler.reaction_capture(reaction_command_from_wire(revision))
    await scheduler.reaction_capture(reaction_command_from_wire(original))
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
async def test_call_expiry_sets_durable_marker_before_message_and_does_not_exit() -> None:
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
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
async def test_heartbeat_reports_actual_queued_and_running_analysis_backlog(tmp_path: Path) -> None:
    from tank.settings import Settings

    repo = MemoryRepository()
    now = utcnow()
    await repo.enqueue("FILTER_ITEM", {}, "filter")
    await repo.enqueue("AGENT_MESSAGE", {}, "message")
    running = await repo.enqueue("RUN_MARKET_TRUTH", {}, "truth")
    await repo.put("ops.jobs", running["job_id"], {**running, "status": "RUNNING"})
    await repo.enqueue("DELIVER", {}, "delivery")
    await repo.enqueue("MAINTENANCE", {}, "maintenance")
    await Scheduler(
        repo,
        Settings(data_dir=tmp_path),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).heartbeat(now)
    notice = (await repo.list("out.ops_notices"))[0]
    assert notice["analysis_backlog"] == {"queued": 2, "running": 1}
    assert "analysis backlog queued=2, running=1" in notice["text"]
    assert notice["queue_lag_s"] >= 0


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_location", ["data", "config", "logs"])
async def test_backup_excludes_fake_auth_secrets_and_browser_state_in_every_evidence_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, auth_location
) -> None:
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
    now = utcnow().replace(hour=8)

    stale = target / f".tank-{now.date()}.partial"
    stale.mkdir()
    (stale / "stale-secret.txt").write_text("FAKE_STALE_PRIVATE_SECRET")

    async def dump(*args, **kwargs):
        Path(args[args.index("--file") + 1]).write_bytes(b"fixture database dump without credentials")
        return SimpleNamespace(communicate=AsyncMock(return_value=(b"", b"")), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", dump)
    repo = MemoryRepository()
    settings = Settings(
        backup_dir=target,
        backup_hour_utc=2,
        database_url="postgresql:///tank",
        openma_database_url="postgresql:///oma",
        data_dir=data,
        config_dir=config,
        logs_dir=logs,
        codex_auth_path=auth,
    )
    result = await Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).maintenance(now, force=True, prune=False)
    backup = Path(result["path"])
    manifest = verify_backup(backup)
    all_bytes = b"".join(path.read_bytes() for path in backup.rglob("*") if path.is_file())
    assert b"PRIVATE_" not in all_bytes and b"FAKE_STALE" not in all_bytes
    assert not (backup / "data").exists()
    assert not (backup / "config").exists() and not (backup / "logs").exists()
    assert manifest.roots == {"bundles": str((data / "bundles").resolve())}
    assert "subscription sign-in directory" in manifest.excluded
    assert all(path.exists() for path in (auth, secrets / "gateway_key"))


def test_existing_backup_with_newly_protected_credential_is_rejected_before_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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
    (backup / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "created_at": clock_iso(),
                "reference_base": str(tmp_path),
                "files": files,
                "roots": {"data": str(data)},
                "database_dumps": ["tank.dump", "oma.dump"],
                "excluded": [],
                "restore": "Synthetic protected-path refusal fixture",
            }
        )
    )
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
async def test_tick_does_not_wait_for_backup() -> None:
    import asyncio

    repo = MemoryRepository()
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked(now) -> None:
        entered.set()
        await release.wait()

    scheduler.maintenance = blocked
    scheduler.detect_scout_hooks = AsyncMock()
    scheduler.heartbeat = AsyncMock()
    try:
        await asyncio.wait_for(scheduler.tick(), 0.5)
        await asyncio.wait_for(entered.wait(), 0.5)
        await asyncio.sleep(0)
        await asyncio.wait_for(scheduler.tick(), 0.5)
        assert not scheduler.background["backup"].done()
    finally:
        release.set()
        await scheduler.close()


@pytest.mark.asyncio
async def test_milestone_payload_contains_only_verified_outcome_and_quotes() -> None:
    repo = MemoryRepository()
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
    await scheduler.milestone_check(
        MilestoneCheckCommand(event_id="e", milestone="VOTE", due_at="2026-01-01T00:00:00Z")
    )
    jobs = await repo.list("ops.jobs")
    payload = jobs[0]["payload"]
    assert (
        payload["outcome"] == "Passed"
        and payload["quotes"] == ["Official vote passed"]
        and not payload["verification_required"]
    )
    await scheduler.milestone_check(
        MilestoneCheckCommand(event_id="e", milestone="VOTE", due_at="2026-02-01T00:00:00Z")
    )
    jobs = await repo.list("ops.jobs")
    pending = next(j["payload"] for j in jobs if j["payload"]["verification_required"])
    assert pending["outcome"] is None and pending["quotes"] == []


@pytest.mark.asyncio
async def test_reaction_future_expiry_and_completed_checkpoint_not_recreated() -> None:
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
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
async def test_scheduler_honors_configured_call_expiry() -> None:
    repo = MemoryRepository()
    now = utcnow()
    settings = Settings(_env_file=None)
    scheduler = Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None)
        .thresholds()
        .model_copy(update={"call_expiry_days": 2, "hot_token_cap": 1}),
        versions=fixture_versions(),
    )
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
async def test_reaction_pack_ulid_stable_for_group_across_restart_and_legacy() -> None:
    repo = MemoryRepository()
    settings = Settings()
    await repo.put(
        "out.reaction_checkpoints",
        "legacy",
        {"rwp_id": "event:token", "event_id": "e", "token_id": "t", "checkpoint": "PRE_1H", "revision": 1},
        immutable=True,
    )
    first = await Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).reaction_pack_id("e", "t")
    second = await Scheduler(
        repo,
        settings,
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).reaction_pack_id("e", "t")
    assert first == second and first.startswith("RWP-") and len(first) == 30
    assert (await repo.get("out.reaction_checkpoints", "legacy"))["rwp_id"] == "event:token"
    assert (
        await Scheduler(
            repo,
            settings,
            None,
            None,
            thresholds=Settings(_env_file=None).thresholds(),
            versions=fixture_versions(),
        ).reaction_pack_id("e", "other")
        != first
    )


@pytest.mark.asyncio
async def test_reaction_targets_use_current_analysis_map_not_historical_edges() -> None:
    repo = MemoryRepository()
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
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
        json.dumps(
            {
                "version": 1,
                "created_at": iso(now),
                "reference_base": str(Path.cwd()),
                "files": files,
                "roots": {label: str(root.resolve()) for label, root in roots.items()},
                "database_dumps": ["tank.dump", "oma.dump"],
                "excluded": sorted(protected),
                "restore": "Synthetic historical recovery fixture",
            }
        )
    )
    (final / "COMPLETE").write_text(iso(now))
    return final


@pytest.mark.asyncio
async def test_reaction_measurement_releases_database_scope_and_duplicate_commit_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        Settings(config_version="cfg", code_version="code"),
        SimpleNamespace(snapshot=snapshot),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    payload = {
        "event_id": "e",
        "token_id": "T",
        "checkpoint": "POST_1H",
        "anchor": {"type": "T1", "ts": iso(utcnow() - timedelta(hours=1))},
    }
    first, duplicate = await asyncio.gather(
        scheduler.reaction_capture(reaction_command_from_wire(payload)),
        scheduler.reaction_capture(reaction_command_from_wire(payload)),
    )
    assert first == duplicate
    assert len(await repo.list("out.reaction_checkpoints")) == 1


@pytest.mark.asyncio
async def test_reaction_creation_registers_only_five_post_jobs_and_starts_pre_without_waiting() -> None:
    import asyncio

    repo = MemoryRepository()
    now = utcnow()
    event = {"event_id": "new-major", "public_at": iso(now), "public_at_prec": "EXACT"}
    started, release = asyncio.Event(), asyncio.Event()
    measured_anchors: list[Timestamp] = []

    async def snapshot(
        token_id: str, cutoff: Timestamp, *, anchor: dict[str, JsonValue], scope: str
    ) -> dict[str, JsonValue]:
        measured_anchors.append(propagate_timestamp(anchor, "ts"))
        started.set()
        await release.wait()
        return {}

    market = SimpleNamespace(snapshot=AsyncMock(side_effect=snapshot))
    scheduler = Scheduler(
        repo,
        Settings(),
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    scheduler.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    scheduler.analysis_tokens = AsyncMock(return_value={"token:T"})
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
        assert len(measured_anchors) == 1
        assert measured_anchors[0].value == iso(now - timedelta(hours=24))
        assert all(j["priority"] == 2 and j["max_attempts"] == 5 for j in jobs)
        release.set()
        await asyncio.gather(*scheduler.background.values())
        checkpoints = await repo.list("out.reaction_checkpoints")
        first = next(record for record in checkpoints if record["checkpoint"] == "PRE_24H")
        assert first["retro"] is True and len(checkpoints) == 3
        assert market.snapshot.await_count == 3
        assert [stamp.value for stamp in measured_anchors] == [
            iso(now - timedelta(hours=hours)) for hours in (24, 4, 1)
        ]
    finally:
        await scheduler.close()


@pytest.mark.asyncio
async def test_reaction_pre_capture_restart_resumes_only_missing_immutable_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    monkeypatch.setattr("tank.analysis.truth.compute_market_truth", lambda *a, **k: {})
    repo = MemoryRepository()
    now = utcnow()
    event = {"event_id": "resume-major", "public_at": iso(now), "public_at_prec": "EXACT"}
    settings = Settings(config_version="config", code_version="code")
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    first = Scheduler(
        repo,
        settings,
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    first.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    first.analysis_tokens = AsyncMock(return_value={"token:T"})
    original = await first.reaction_capture(
        reaction_command_from_wire(
            {
                "event_id": event["event_id"],
                "token_id": "token:T",
                "checkpoint": "PRE_24H",
                "anchor": first.anchor(event),
                "retro": True,
            }
        )
    )
    assert isinstance(original, CapturedReaction)
    original_record = await repo.get("out.reaction_checkpoints", original.checkpoint_key)
    assert original_record is not None
    await first.close()
    restarted = Scheduler(
        repo,
        settings,
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    restarted.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    restarted.analysis_tokens = AsyncMock(return_value={"token:T"})
    try:
        await restarted.ensure_reactions(event, now=now)
        await asyncio.gather(*restarted.background.values())
        rows = await repo.list("out.reaction_checkpoints")
        assert len(rows) == 3
        assert next(r for r in rows if r["checkpoint"] == "PRE_24H") == original_record
        assert market.snapshot.await_count == 3
        assert len(await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})) == 5
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_reaction_pre_error_retries_missing_windows_and_close_cancels_owned_work() -> None:
    import asyncio

    repo = MemoryRepository()
    event = {"event_id": "e", "public_at": clock_iso(), "public_at_prec": "EXACT"}
    scheduler = Scheduler(
        repo,
        Settings(),
        object(),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    scheduler.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    scheduler.analysis_tokens = AsyncMock(return_value={"T"})
    scheduler.reaction_capture = AsyncMock(side_effect=RuntimeError("synthetic capture failure"))
    await scheduler.ensure_reactions(event)
    await asyncio.gather(*scheduler.background.values())
    assert scheduler.reaction_capture.await_count == 1
    started = asyncio.Event()

    async def block(payload, **_) -> None:
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
async def test_dynamic_scheduler_work_does_not_retain_completed_task_frames() -> None:
    import asyncio

    scheduler = Scheduler(
        MemoryRepository(),
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    operation = AsyncMock()
    for number in range(200):
        scheduler.launch(f"reaction-pre:event-{number}:token", operation)
    tasks = tuple(scheduler.background.values())
    await asyncio.gather(*tasks)
    assert operation.await_count == 200
    assert not scheduler.background
    await scheduler.close()


@pytest.mark.asyncio
async def test_older_completion_callback_preserves_same_name_replacement_until_shutdown() -> None:
    import asyncio

    scheduler = Scheduler(
        MemoryRepository(),
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    started = asyncio.Event()

    async def replacement() -> None:
        started.set()
        await asyncio.Event().wait()

    async def first() -> None:

        asyncio.get_running_loop().call_soon(scheduler.launch, "same", replacement)

    scheduler.launch("same", first)
    original = scheduler.background["same"]
    await asyncio.wait_for(started.wait(), timeout=1)
    current = scheduler.background["same"]
    assert current is not original and not current.done()
    await scheduler.close()
    assert current.cancelled() and not scheduler.background


@pytest.mark.asyncio
async def test_rearmed_market_watch_needs_a_new_window_and_a_clear() -> None:
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
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        truth,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )

    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=10))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"

    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=61))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"

    price["relative"]["close_1h_price"] = "10.5"
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=62))}, "cutoff")
    )
    cleared = await repo.get("ana.watch_conditions", "c2")
    assert cleared["status"] == "ARMED" and cleared["cleared_at"]
    price["relative"]["close_1h_price"] = "9"
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=63))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "FIRED"


@pytest.mark.asyncio
async def test_rearmed_instant_watch_fires_again_only_after_it_cleared() -> None:
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
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        SimpleNamespace(assess=AsyncMock(return_value={})),
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(now + timedelta(minutes=1))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    await repo.put("ana.contradictions", "f1", {**fatal, "resolved_at": iso(now)})
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(now + timedelta(minutes=2))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    await repo.put("ana.contradictions", "f2", {**fatal, "contradiction_id": "f2"})
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(now + timedelta(minutes=3))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "FIRED"


@pytest.mark.asyncio
async def test_rearmed_persistent_watch_clears_only_on_an_observed_false_window() -> None:
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
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        truth,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    for minutes in (5, 55, 115, 175):
        await scheduler.eval_conditions(
            propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=minutes))}, "cutoff")
        )
        row = await repo.get("ana.watch_conditions", "c2")
        assert row["status"] == "ARMED" and not row.get("cleared_at")
    sell_hours.add((armed + timedelta(minutes=235)).replace(minute=0))
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=235))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2")).get("cleared_at")
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=295))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "ARMED"
    await scheduler.eval_conditions(
        propagate_timestamp({"cutoff": iso(armed + timedelta(minutes=355))}, "cutoff")
    )
    assert (await repo.get("ana.watch_conditions", "c2"))["status"] == "FIRED"


@pytest.mark.asyncio
async def test_late_data_revisions_coalesce_to_one_queued_job_per_checkpoint() -> None:
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
                "anchor": {"type": "T0", "ts": anchor, "ts_prec": "EXACT"},
            },
        )

    class Market:
        def __init__(self) -> None:
            self.round = 0

        async def capture_changes(self, since, end):
            self.round += 1
            yield {
                "change_id": "CAP-LIVE-" + str(self.round),
                "event_id": "e",
                "token_id": "t",
                "anchor": anchor,
                "observed_at": iso(now),
                "kind": "LATE_NORMALIZED_DATA",
                "status": "AVAILABLE",
                "source_count": 1,
                "market_from": anchor,
                "semantic_fingerprint": sha256({"authored_observation": iso(now)}),
            }

    scheduler = Scheduler(
        repo,
        Settings(),
        Market(),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    for _ in range(3):
        await scheduler.detect_late_data(now)
    jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
    assert sorted(job["payload"]["checkpoint"] for job in jobs) == ["POST_15M", "PRE_24H"]


@pytest.mark.asyncio
async def test_late_revision_is_deferred_so_following_late_completions_fold_into_it() -> None:
    """r114: each late completion after a restart re-queued every touched checkpoint. A late
    revision now waits LATE_REVISION_DEFER_S, so completions meanwhile fold into it; once it
    has run, a newer completion queues a new revision (frozen-once and revision history unchanged)."""
    import tank.scheduler as scheduler_module
    from tank.util import parse_time

    repo = MemoryRepository()
    now = utcnow().replace(microsecond=0)
    anchor = iso(now - timedelta(hours=2))
    await repo.put(
        "out.reaction_checkpoints",
        "e:t:POST_15M",
        {
            "event_id": "e",
            "token_id": "t",
            "checkpoint": "POST_15M",
            "revision": 1,
            "captured_at": iso(now - timedelta(hours=1)),
            "anchor": {"type": "T0", "ts": anchor, "ts_prec": "EXACT"},
        },
    )

    class Market:
        round = 0

        async def capture_changes(self, since, end):
            Market.round += 1
            yield {
                "change_id": f"CAP-LIVE-{Market.round}",
                "event_id": "e",
                "token_id": "t",
                "anchor": anchor,
                "observed_at": iso(now),
                "kind": "LATE_NORMALIZED_DATA",
                "status": "AVAILABLE",
                "source_count": 1,
                "market_from": anchor,
                "semantic_fingerprint": sha256({"authored_observation": iso(now)}),
            }

    scheduler = Scheduler(
        repo,
        Settings(),
        Market(),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    for minute in range(0, 30, 5):
        await scheduler.detect_late_data(now + timedelta(minutes=minute))
    (job,) = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
    assert parse_time(job["run_after"]) == now + timedelta(seconds=scheduler_module.LATE_REVISION_DEFER_S)
    await repo.put("ops.jobs", job["job_id"], dict(job, status="DONE"))
    await scheduler.detect_late_data(now + timedelta(minutes=40))
    jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
    assert len(jobs) == 2


@pytest.mark.asyncio
async def test_queued_duplicate_late_revision_is_skipped_at_run_time() -> None:
    """A pending EDF peer defers an actual claim without declaring capture success."""
    repo = MemoryRepository()
    payload = {
        "event_id": "e",
        "token_id": "t",
        "checkpoint": "PRE_4H",
        "automatic_revision": True,
        "reason": "Observed capture completion",
    }
    older = await repo.enqueue(
        "REACTION_CAPTURE", dict(payload, change_id="older"), "reaction-late:older:PRE_4H", priority=2
    )
    claimed = await repo.claim_job()
    assert claimed is not None and claimed["job_id"] == older["job_id"]
    queued = await repo.enqueue(
        "REACTION_CAPTURE", dict(payload, change_id="newer"), "reaction-late:x:PRE_4H", priority=1
    )
    market = SimpleNamespace(snapshot=AsyncMock(side_effect=AssertionError("must not measure")))
    scheduler = Scheduler(
        repo,
        Settings(),
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    with pytest.raises(ReactionCoalescingDeferred) as pending:
        await capture_reaction_job(
            scheduler,
            repo,
            ReactionJobInvocation.from_record(claimed),
            clock=collection_clock,
            retry_delay=timedelta(seconds=REACTION_RETRY_S),
        )
    assert pending.value.pending.coalesced_into == queued["job_id"]
    unchanged = await repo.get("ops.jobs", older["job_id"])
    assert unchanged is not None and unchanged["status"] == "RUNNING" and "result" not in unchanged
    market.snapshot.assert_not_called()


@pytest.mark.asyncio
async def test_reaction_measurements_are_limited_and_extra_jobs_requeue() -> None:
    """Real checkpoint commits share the measurement bound and preserve deferred ownership."""
    import asyncio

    from pydantic import JsonValue

    from tank.delivery import DeliveryDeferred
    from tank.scheduler import REACTION_MEASUREMENT_SLOTS

    repo = MemoryRepository()
    release = asyncio.Event()
    at_capacity = asyncio.Event()
    started: list[str] = []

    async def snapshot(
        token_id: str, cutoff: Timestamp, *, anchor: dict[str, JsonValue], scope: str
    ) -> dict[str, JsonValue]:
        start = propagate_timestamp(anchor, "ts")
        assert start.value is not None
        started.append(start.value)
        if len(started) == REACTION_MEASUREMENT_SLOTS:
            at_capacity.set()
        await release.wait()
        return {}

    market = SimpleNamespace(snapshot=AsyncMock(side_effect=snapshot))
    scheduler = Scheduler(
        repo,
        Settings(),
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    stamp = iso(utcnow())
    checkpoints = ["PRE_1H", "PRE_4H", "PRE_24H"]
    payloads = [
        {
            "event_id": "e",
            "token_id": "t",
            "checkpoint": checkpoint,
            "anchor": {"type": "T1", "ts": stamp, "ts_prec": "EXACT", "ts_prec_reason": None},
        }
        for checkpoint in checkpoints
    ]
    running = [
        asyncio.create_task(scheduler.reaction_capture(reaction_command_from_wire(payload)))
        for payload in payloads[:REACTION_MEASUREMENT_SLOTS]
    ]
    await asyncio.wait_for(at_capacity.wait(), 1)
    with pytest.raises(DeliveryDeferred):
        await scheduler.reaction_capture(reaction_command_from_wire(payloads[-1]))
    waiting = asyncio.create_task(
        scheduler.reaction_capture(reaction_command_from_wire(payloads[-1]), wait_for_slot=True)
    )
    await asyncio.sleep(0)
    assert not waiting.done() and len(started) == REACTION_MEASUREMENT_SLOTS
    release.set()
    receipts = [*await asyncio.gather(*running), await waiting]
    assert all(isinstance(receipt, CapturedReaction) for receipt in receipts)
    assert {receipt.checkpoint for receipt in receipts if isinstance(receipt, CapturedReaction)} == set(
        checkpoints
    )
    assert await scheduler.reaction_capture(reaction_command_from_wire(payloads[-1])) == receipts[-1]
    records = await repo.list("out.reaction_checkpoints")
    assert len(records) == 3 and market.snapshot.await_count == 3
    for receipt in receipts:
        assert isinstance(receipt, CapturedReaction)
        record = await repo.get("out.reaction_checkpoints", receipt.checkpoint_key)
        assert record is not None and record["data_fingerprint"] == receipt.data_fingerprint
    await scheduler.close()


@pytest.mark.asyncio
async def test_eval_conditions_budget_stops_cleanly_and_rotates(monkeypatch: pytest.MonkeyPatch) -> None:
    """03 §3.8 (20 s): a slow batch stops starting conditions after its budget
    and completes; the least recently evaluated go first."""
    import tank.scheduler as scheduler_module

    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "e", {"event_id": "e"})
    for cond_id, role, last in [
        ("old-upgrade", "UPGRADE", iso(now - timedelta(minutes=5))),
        ("new-upgrade", "UPGRADE", iso(now - timedelta(minutes=1))),
        ("exit", "INVALIDATION", iso(now)),
    ]:
        await repo.put(
            "ana.watch_conditions",
            cond_id,
            {
                "cond_id": cond_id,
                "event_id": "e",
                "token_id": "t",
                "role": role,
                "status": "ARMED",
                "last_evaluated_at": last,
                "expires_at": iso(now + timedelta(days=1)),
                "expr": {"metric": "event.contradiction_fatal", "window": "W1H", "op": "==", "value": True},
            },
        )
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITIONS_BUDGET_S", -1)
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        SimpleNamespace(assess=AsyncMock(return_value={})),
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    await scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now)}, "cutoff"))
    for cond_id in ("exit", "old-upgrade", "new-upgrade"):
        assert "counters" not in await repo.get("ana.watch_conditions", cond_id)
    order = []
    original = repo.get

    async def tracking_get(collection, key):
        if collection == "ana.watch_conditions":
            order.append(key)
        return await original(collection, key)

    repo.get = tracking_get
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITIONS_BUDGET_S", 60)
    await scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now)}, "cutoff"))

    assert order == ["old-upgrade", "new-upgrade", "exit"]


@pytest.mark.asyncio
async def test_late_data_after_a_window_end_does_not_revise_that_window() -> None:
    """03 §8.6: a change whose market data starts after a checkpoint window's
    end cannot change it; only windows ending at or after it are revised."""
    repo = MemoryRepository()
    now = utcnow()
    anchor = now - timedelta(hours=2)
    for label, end in (("PRE_1H", anchor), ("POST_1H", anchor + timedelta(hours=1))):
        await repo.put(
            "out.reaction_checkpoints",
            "e:t:" + label,
            {
                "event_id": "e",
                "token_id": "t",
                "checkpoint": label,
                "revision": 1,
                "captured_at": iso(now - timedelta(minutes=30)),
                "window_end": iso(end),
                "anchor": {"type": "T0", "ts": iso(anchor), "ts_prec": "EXACT"},
            },
        )

    class Market:
        async def capture_changes(self, since, end):
            yield {
                "change_id": "CAP-LIVE-1",
                "event_id": "e",
                "token_id": "t",
                "anchor": iso(anchor),
                "observed_at": iso(now),
                "kind": "LATE_NORMALIZED_DATA",
                "status": "AVAILABLE",
                "source_count": 1,
                "semantic_fingerprint": sha256({"authored_observation": iso(now)}),
                "market_from": iso(anchor + timedelta(minutes=20)),
            }

    await Scheduler(
        repo,
        Settings(),
        Market(),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).detect_late_data(now)
    jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
    assert [job["payload"]["checkpoint"] for job in jobs] == ["POST_1H"]


@pytest.mark.asyncio
async def test_eval_conditions_progresses_when_one_market_read_outlasts_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """r107: a single market read took longer than the whole job, so every batch
    died in its first condition and restarted there. The read now gets only the
    remaining budget, the attempt is recorded, cheap conditions go first and
    the next batch starts with the others."""
    import asyncio

    import tank.scheduler as scheduler_module

    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "e", {"event_id": "e"})

    def condition(cond_id, token, expr, role="UPGRADE"):
        return {
            "cond_id": cond_id,
            "event_id": "e",
            "token_id": token,
            "role": role,
            "status": "ARMED",
            "expires_at": iso(now + timedelta(days=1)),
            "expr": expr,
        }

    market = {"metric": "dc.class", "window": "W1H", "op": "==", "value": "HIGH"}
    for row in (
        condition("slow", "slow-token", market, role="INVALIDATION"),
        condition("fast", "fast-token", market),
        condition(
            "fatal",
            "slow-token",
            {"metric": "event.contradiction_fatal", "window": "W1H", "op": "==", "value": True},
        ),
    ):
        await repo.put("ana.watch_conditions", row["cond_id"], row)
    reads = []

    async def condition_values(event_id, token_id, cutoff, requirements):
        reads.append(token_id)
        if token_id == "slow-token":
            await asyncio.sleep(5)
        return {"data_confidence": {"class": "LOW"}}

    monkeypatch.setattr(scheduler_module, "EVAL_CONDITIONS_BUDGET_S", 0.2)
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        SimpleNamespace(condition_values=condition_values),
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    await asyncio.wait_for(scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now)}, "cutoff")), 2)
    assert (await repo.get("ana.watch_conditions", "fatal")).get("last_evaluated_at")
    slow = await repo.get("ana.watch_conditions", "slow")
    assert slow.get("last_attempted_at") and "counters" not in slow
    await asyncio.wait_for(
        scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now + timedelta(minutes=1))}, "cutoff")),
        2,
    )
    assert (await repo.get("ana.watch_conditions", "fast")).get("last_evaluated_at")
    assert reads[:2] == ["slow-token", "fast-token"]


@pytest.mark.asyncio
async def test_eval_conditions_per_condition_budget_and_explicit_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lead ruling r108: one slow condition cannot spend the batch; the job
    completes with an outcome per condition."""
    import asyncio

    import tank.scheduler as scheduler_module

    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "e", {"event_id": "e"})
    market = {"metric": "dc.class", "window": "W1H", "op": "==", "value": "HIGH"}
    for cond_id, token in (("slow", "slow-token"), ("fast", "fast-token")):
        await repo.put(
            "ana.watch_conditions",
            cond_id,
            {
                "cond_id": cond_id,
                "event_id": "e",
                "token_id": token,
                "role": "UPGRADE",
                "status": "ARMED",
                "last_evaluated_at": iso(now - timedelta(minutes=5 if cond_id == "slow" else 1)),
                "expires_at": iso(now + timedelta(days=1)),
                "expr": market,
            },
        )

    async def condition_values(event_id, token_id, cutoff, requirements):
        if token_id == "slow-token":
            await asyncio.sleep(5)
        return {"data_confidence": {"class": "HIGH"}}

    monkeypatch.setattr(scheduler_module, "EVAL_CONDITIONS_BUDGET_S", 2)
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITION_BUDGET_S", 0.1)
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        SimpleNamespace(condition_values=condition_values),
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    outcomes = await asyncio.wait_for(
        scheduler.eval_conditions(propagate_timestamp({"cutoff": iso(now)}, "cutoff")), 3
    )
    assert [(item.cond_id, item.reason) for item in outcomes.deferred] == [("slow", "READ_TIMEOUT")]
    assert outcomes.fired == ["fast"]
    slow = await repo.get("ana.watch_conditions", "slow")
    assert slow["last_attempt_outcome"] == "READ_TIMEOUT" and slow["status"] == "ARMED"
    assert (await repo.get("ana.watch_conditions", "fast"))["status"] == "FIRED"


@pytest.mark.asyncio
async def test_pre_windows_each_read_only_their_checkpoint_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """r110: reaction captures read days of 10 s books; each PRE window now reads its own
    checkpoint window (snapshot scope REACTION), anchored at its own start."""
    import tank.analysis.truth as truth_module

    repo = MemoryRepository()
    now = utcnow().replace(second=0, microsecond=0)
    await repo.put("evt.events", "e", {"event_id": "e"})
    calls = []

    class Market:
        async def snapshot(self, token_id, cutoff, *, anchor=None, scope=None):
            calls.append((anchor["ts"], scope))
            return {"token_id": token_id, "anchor": anchor}

    def fake_truth(snapshot, config=None):
        return {"relative": {"windows": {"EVENT": {"r_raw": 0.01}}}}

    monkeypatch.setattr(truth_module, "compute_market_truth", fake_truth)
    scheduler = Scheduler(
        repo,
        Settings(config_version="c", code_version="c"),
        Market(),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    for label in ("PRE_24H", "PRE_4H", "PRE_1H"):
        payload = {
            "event_id": "e",
            "token_id": "test:T",
            "checkpoint": label,
            "anchor": {"type": "T1", "ts": iso(now)},
            "retro": True,
        }
        await scheduler.reaction_capture(reaction_command_from_wire(payload), wait_for_slot=True)
    assert sorted(calls) == sorted(
        (iso(now - timedelta(seconds=s)), "REACTION") for s in (86400, 14400, 3600)
    )
    rows = await repo.list("out.reaction_checkpoints")
    assert sorted(row["checkpoint"] for row in rows) == ["PRE_1H", "PRE_24H", "PRE_4H"]


@pytest.mark.asyncio
async def test_latest_revision_is_read_as_metadata_not_the_whole_row() -> None:
    """r110: the newest revision was decoded whole (up to 2 MB of Market Truth)
    for every capture; the plan reads its fields as metadata only."""
    repo = MemoryRepository()
    row = {
        "event_id": "e",
        "token_id": "t",
        "checkpoint": "PRE_1H",
        "revision": 2,
        "anchor": {"type": "T1", "ts": iso(utcnow()), "ts_prec": "EXACT", "ts_prec_reason": None},
        "retro": True,
        "reason": "Late capture completed: x",
        "data_fingerprint": sha256({"captured_fixture": "metadata-only"}),
        "market_truth": {"blob": "x" * 100_000},
    }
    await repo.put("out.reaction_checkpoints", "e:t:PRE_1H:1", dict(row, revision=1))
    await repo.put("out.reaction_checkpoints", "e:t:PRE_1H:2", row)
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )

    async def no_full_get(collection, key) -> None:
        raise AssertionError("full row read: " + key)

    repo.get = no_full_get
    latest = await scheduler._latest_reaction_revision(
        ReactionTarget.model_validate({field: row[field] for field in ReactionTarget.model_fields})
    )
    assert latest is not None and latest.revision == 2
    assert latest.data_fingerprint == row["data_fingerprint"]
    assert "market_truth" not in latest.model_dump()


@pytest.mark.asyncio
async def test_failed_capture_job_of_this_event_records_a_gap_and_other_events_jobs_do_not():
    import asyncio

    repo = MemoryRepository()
    now = utcnow()
    event = {"event_id": "evt-1", "public_at": iso(now - timedelta(hours=2)), "public_at_prec": "EXACT"}
    scheduler = Scheduler(
        repo,
        Settings(),
        object(),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    scheduler.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    scheduler.analysis_tokens = AsyncMock(return_value={"token:T"})
    scheduler.reaction_capture = AsyncMock()
    try:
        await scheduler.ensure_reactions(event, now=now)
        await asyncio.gather(*scheduler.background.values())
        for job in await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"}):
            failed = job["payload"]["checkpoint"] == "POST_15M"
            await repo.put("ops.jobs", job["job_id"], {**job, "status": "FAILED" if failed else "QUEUED"})
        await repo.enqueue("REACTION_CAPTURE", {"event_id": "evt-10"}, "evt-10:token:T:POST_1H", priority=2)
        other = (await repo.list("ops.jobs", filters={"idem_key": "evt-10:token:T:POST_1H"}))[0]
        await repo.put("ops.jobs", other["job_id"], {**other, "status": "FAILED"})
        await scheduler.ensure_reactions(event, now=now)
        gaps = await repo.list("ops.scheduler_checkpoints", filters={"kind": "REACTION_CAPTURE_GAP"})
        assert [(g["state"]["event_id"], g["state"]["checkpoint"]) for g in gaps] == [("evt-1", "POST_15M")]
        assert len(await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})) == 6
    finally:
        await scheduler.close()


@pytest.mark.asyncio
async def test_scout_write_uses_required_version_facet_with_a_custom_fixture_root(tmp_path: Path) -> None:
    from tank.contracts.provenance import ResolvedVersionIdentity

    settings = Settings(config_dir=tmp_path / "no-runtime-config", data_dir=tmp_path)
    versions = ResolvedVersionIdentity(
        config_version="declared-scout-config", code_version="declared-scout-code"
    )
    repo = MemoryRepository()
    scheduler = Scheduler(repo, settings, None, None, versions=versions, thresholds=Settings().thresholds())
    result = await scheduler.scout_proposal(
        "UNCLASSIFIED_EVENT",
        "Synthetic fixture concept",
        ["fixture:source-evidence"],
        "An isolated controlled observation",
        utcnow(),
    )
    assert result is not None
    stored = await repo.get("scout.expansion_proposals", result["prp_id"])
    assert stored is not None
    assert stored["config_version"] == versions.config_version
    assert stored["code_version"] == versions.code_version
    assert len(await repo.list("ops.jobs", filters={"type": "SCOUT_REVIEW"})) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_identity", [None, "another-event"])
async def test_reaction_scheduling_port_requires_the_actual_event_identity(
    stored_identity: str | None,
) -> None:
    repo = MemoryRepository()
    scheduler = Scheduler(repo, Settings(), None, None, versions=fixture_versions())
    if stored_identity is not None:
        await repo.put("evt.events", "requested-event", {"event_id": stored_identity})
    with pytest.raises(ValueError, match="does not exist|differs from its lookup"):
        await scheduler.ensure_event_reactions("requested-event")
    assert await repo.list("ops.jobs") == []

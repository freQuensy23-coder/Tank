"""Producer clock provenance, independent of deployment services or source precision guesses."""

import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from source_support import fixture_versions

from tank.contracts.scheduler_commands import reaction_command_from_wire
from tank.contracts.scheduler_results import CapturedReaction, UnchangedReaction
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import propagate_timestamp
from tank.util import parse_time

STAMP = "2026-10-17T12:00:00Z"


@pytest.mark.parametrize(
    "field,extra,kind,precision",
    [
        ("event_time", {"state_verified": True, "event_time_prec": "EXACT"}, "T0", "EXACT"),
        ("public_at", {"public_at_prec": "BUCKET(1h)"}, "T1", "BUCKET(1h)"),
        ("known_to_us_at", {"known_to_us_at_prec": "COLLECTION_ONLY"}, "KNOWN_TO_US_AT", "COLLECTION_ONLY"),
        ("public_at", {}, "T1", None),
    ],
)
def test_selected_event_anchor_preserves_provenance(field, extra, kind, precision) -> None:
    anchor = Scheduler.anchor({field: STAMP, **extra})
    assert anchor["type"] == kind
    assert anchor["ts_prec"] == precision
    if precision is None:
        assert anchor["ts_prec_reason"] == "INCOMPLETE"


@pytest.mark.asyncio
async def test_tick_preserves_source_milestone_and_offset_job_precision() -> None:
    repo = MemoryRepository()
    now = parse_time(STAMP)
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    scheduler.launch = lambda *args: None
    scheduler.expire_calls = AsyncMock()
    scheduler.demote = AsyncMock()
    scheduler.lifecycle = AsyncMock()
    scheduler.heartbeat = AsyncMock()
    await repo.put(
        "evt.events",
        "e",
        {
            "event_id": "e",
            "lifecycle": "TRACKED",
            "public_at": STAMP,
            "public_at_prec": "BUCKET(1h)",
            "analysis_set": ["T"],
            "next_milestone": {
                "due_at": STAMP,
                "due_at_prec": "BUCKET(1d)",
                "kind": "launch",
            },
        },
    )
    scheduler.latest_assessment = AsyncMock(return_value={"class": "MAJOR"})
    scheduler.analysis_tokens = AsyncMock(return_value={"T"})
    await scheduler.tick(now)
    jobs = await repo.list("ops.jobs")
    evaluation = next(j for j in jobs if j["type"] == "EVAL_CONDITIONS")
    assert evaluation["payload"]["cutoff_prec"] == "BUCKET(1m)"
    assert evaluation["expires_at_prec"] == "COLLECTION_ONLY"
    milestone = next(j for j in jobs if j["type"] == "MILESTONE_CHECK")
    assert milestone["payload"]["due_at_prec"] == "BUCKET(1d)"
    assert milestone["expires_at_prec"] == "BUCKET(1d)"
    reaction = next(
        j for j in jobs if j["type"] == "REACTION_CAPTURE" and j["payload"]["checkpoint"] == "POST_1H"
    )
    assert reaction["run_after_prec"] == reaction["expires_at_prec"] == "BUCKET(1h)"
    assert reaction["payload"]["cutoff_prec"] == "BUCKET(1h)"


@pytest.mark.asyncio
@pytest.mark.parametrize("precision", ["BUCKET(1h)", None])
async def test_reaction_windows_inherit_anchor_and_capture_clock_is_collection(
    monkeypatch: pytest.MonkeyPatch, precision
) -> None:
    monkeypatch.setattr("tank.analysis.truth.compute_market_truth", lambda *a, **kw: {})
    repo = MemoryRepository()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    scheduler = Scheduler(
        repo,
        Settings(config_version="fixture", code_version="fixture"),
        market,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    anchor = {"type": "T1", "ts": STAMP}
    if precision:
        anchor["ts_prec"] = precision
    receipt = await scheduler.reaction_capture(
        reaction_command_from_wire(
            {
                "event_id": "e",
                "token_id": "T",
                "checkpoint": "PRE_1H",
                "anchor": anchor,
            }
        )
    )
    assert isinstance(receipt, CapturedReaction)
    record = await repo.get("out.reaction_checkpoints", receipt.checkpoint_key)
    assert record is not None
    assert record["window_start_prec"] == record["window_end_prec"] == precision
    assert record["captured_at_prec"] == "COLLECTION_ONLY"
    assert record["anchor"]["ts_prec"] == precision
    assert record["window_start"] == "2026-10-17T11:00:00Z"
    if precision is None:
        assert record["window_start_prec_reason"] == "INCOMPLETE"
    assert market.snapshot.call_args.kwargs["anchor"]["ts_prec"] == precision
    assert market.snapshot.call_args.args[1].precision == precision
    pack = await repo.get("ops.scheduler_checkpoints", "reaction-pack:e:T")
    assert pack["at_prec"] == "COLLECTION_ONLY"


@pytest.mark.asyncio
async def test_retention_json_records_clock_and_offset_cutoffs(tmp_path: Path) -> None:
    repo = SimpleNamespace(
        prune_market=AsyncMock(), prune_jobs=AsyncMock(), get=AsyncMock(return_value=None), put=AsyncMock()
    )
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    await scheduler.retention(parse_time(STAMP), tmp_path)
    metadata = json.loads((tmp_path / "RETENTION_COMPLETE.json").read_text())
    assert metadata["completed_at_prec"] == "COLLECTION_ONLY"
    assert metadata["market_cutoff_prec"] == metadata["jobs_cutoff_prec"] == "COLLECTION_ONLY"
    assert parse_time(metadata["market_cutoff"]) == parse_time(STAMP) - timedelta(days=90)


@pytest.mark.asyncio
async def test_legacy_condition_cutoff_stays_explicit_unknown() -> None:
    repo = MemoryRepository()
    scheduler = Scheduler(
        repo,
        Settings(),
        None,
        SimpleNamespace(assess=AsyncMock(return_value={})),
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
            "token_id": "T",
            "status": "ARMED",
            "expires_at": "2026-10-18T12:00:00Z",
            "expr": {"metric": "event.state", "op": "==", "value": "APPROVED"},
        },
    )
    await scheduler.eval_conditions(propagate_timestamp({"cutoff": STAMP}, "cutoff"))
    row = await repo.get("ana.watch_conditions", "c")
    assert row["last_evaluated_at_prec"] is None
    assert row["last_evaluated_at_prec_reason"] == "INCOMPLETE"
    assert row["counters"]["root"]["cutoff_prec"] is None


@pytest.mark.asyncio
async def test_provenance_only_addition_does_not_create_late_data_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tank.analysis.truth.compute_market_truth", lambda *a, **kw: {})
    repo = MemoryRepository()
    scheduler = Scheduler(
        repo,
        Settings(config_version="fixture", code_version="fixture"),
        SimpleNamespace(snapshot=AsyncMock(return_value={})),
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    original = await scheduler.reaction_capture(
        reaction_command_from_wire(
            {
                "event_id": "e",
                "token_id": "T",
                "checkpoint": "POST_1H",
                "anchor": {"type": "T1", "ts": STAMP},
            }
        )
    )

    assert isinstance(original, CapturedReaction)
    original_record = await repo.get("out.reaction_checkpoints", original.checkpoint_key)
    assert original_record is not None
    legacy = {
        key: value for key, value in original_record.items() if not key.endswith(("_prec", "_prec_reason"))
    }
    repo = MemoryRepository()
    scheduler.repo = repo
    await repo.put("out.reaction_checkpoints", "e:T:POST_1H:1", legacy)
    result = await scheduler.reaction_capture(
        reaction_command_from_wire(
            {
                "event_id": "e",
                "token_id": "T",
                "checkpoint": "POST_1H",
                "automatic_revision": True,
                "change_id": "late",
                "reason": "Late capture completed",
            }
        )
    )
    assert isinstance(result, UnchangedReaction) and result.changed is False
    assert len(await repo.list("out.reaction_checkpoints")) == 1

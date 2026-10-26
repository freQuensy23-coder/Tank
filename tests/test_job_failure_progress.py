"""Typed failure reporting preserves durable notices and legacy adapter semantics."""

import pytest
from agent_source_fixtures import source_item_fixture
from pydantic import ValidationError

from tank.contracts.progress import FailureProgressTarget, JobFailureProgressView, ProgressPreferences
from tank.ops_progress import job_failure_progress, record_job_failure_progress
from tank.storage import MemoryRepository


@pytest.mark.parametrize("event_target", [False, True])
async def test_typed_failure_view_reports_actual_subject_once(event_target: bool) -> None:
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items", "item", source_item_fixture("item", source_id="S30").storage_record()
    )
    if event_target:
        await repo.put("evt.event_observations", "link", {"item_id": "item", "event_id": "event"})
    target = (
        FailureProgressTarget(event_id="event") if event_target else FailureProgressTarget(item_id="item")
    )
    view = JobFailureProgressView(
        job_id="job",
        type="RUN_MARKET_TRUTH" if event_target else "FILTER_ITEM",
        status="EXPIRED",
        target=target,
    )
    settings = ProgressPreferences(ops_progress_enabled=True, replay=False)
    await record_job_failure_progress(repo, settings, view, "EXPIRED")
    await record_job_failure_progress(repo, settings, view, "EXPIRED")
    [notice] = await repo.list("out.ops_notices")
    assert notice["subject_id"] == "item" and notice["stage"] == "PROCESSING_EXPIRED"
    assert notice["reason"] == f"Этап {view.type}; EXPIRED"
    [delivery] = await repo.list("ops.jobs")
    assert delivery["type"] == "DELIVER" and delivery["payload"] == {
        "notice_id": notice["notice_id"],
        "channel": "telegram:ops",
    }


@pytest.mark.parametrize(
    "preferences",
    [
        ProgressPreferences(ops_progress_enabled=False, replay=False),
        ProgressPreferences(ops_progress_enabled=True, replay=True),
    ],
)
async def test_disabled_typed_and_wire_reporting_keep_no_effects(preferences: ProgressPreferences) -> None:
    repo = MemoryRepository()
    view = JobFailureProgressView(
        job_id="job", type="FILTER_ITEM", status="FAILED", target=FailureProgressTarget(item_id="item")
    )
    await record_job_failure_progress(repo, preferences, view, "FAILED")
    await job_failure_progress(repo, preferences, {}, "FAILED")
    assert await repo.list("out.ops_notices") == [] and await repo.list("ops.jobs") == []


async def test_wire_adapter_and_typed_view_share_durable_deduplication() -> None:
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items", "item", source_item_fixture("item", source_id="S30").storage_record()
    )
    settings = ProgressPreferences(ops_progress_enabled=True, replay=False)
    wire = await repo.enqueue("FILTER_ITEM", {"item_id": "item"}, "filter:item")
    await job_failure_progress(repo, settings, wire, "actual failure")
    view = JobFailureProgressView.model_validate(
        {
            "job_id": wire["job_id"],
            "type": wire["type"],
            "status": wire["status"],
            "target": {"item_id": "item"},
        }
    )
    await record_job_failure_progress(repo, settings, view, "actual failure")
    [notice] = await repo.list("out.ops_notices")
    assert notice["stage"] == "PROCESSING_FAILED" and notice["reason"] == "Этап FILTER_ITEM; actual failure"
    deliveries = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    assert len(deliveries) == 1


async def test_enabled_typed_entry_revalidates_mutated_model_before_effects() -> None:
    repo = MemoryRepository()
    original = JobFailureProgressView(
        job_id="job", type="FILTER_ITEM", status="FAILED", target=FailureProgressTarget(item_id="item")
    )
    invalid = original.model_copy(update={"job_id": ""})
    with pytest.raises(ValidationError):
        await record_job_failure_progress(
            repo, ProgressPreferences(ops_progress_enabled=True, replay=False), invalid, "FAILED"
        )
    assert await repo.list("out.ops_notices") == [] and await repo.list("ops.jobs") == []

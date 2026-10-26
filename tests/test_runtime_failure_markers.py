"""Unknown reconciliation clock facts cannot masquerade as a handled failure."""

import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue
from test_storage import repo as repo

from tank.runtime_contracts import read_failure_marker
from tank.runtime_failure_repository import RepositoryRuntimeFailures
from tank.storage import StorageBackend
from tank.time_precision import collection_clock


@pytest.mark.parametrize(
    "record",
    [
        {},
        {
            "followup_failure_recorded_at": None,
            "followup_failure_recorded_at_prec": None,
            "followup_failure_recorded_at_prec_reason": None,
        },
    ],
)
def test_absent_or_wholly_null_failure_clock_is_unreported(record: dict[str, JsonValue]) -> None:
    assert read_failure_marker(record, "followup_failure_recorded_at") is None


def test_explicit_unknown_failure_clock_preserves_reason_without_claiming_occurrence() -> None:
    unknown = read_failure_marker(
        {
            "followup_failure_recorded_at": None,
            "followup_failure_recorded_at_prec_reason": "Occurrence not reported",
        },
        "followup_failure_recorded_at",
    )
    assert unknown is not None and unknown.value is None
    assert unknown.precision_reason == "Occurrence not reported"
    observed = collection_clock()
    recorded: dict[str, JsonValue] = {**observed.fields("followup_failure_recorded_at")}
    assert read_failure_marker(recorded, "followup_failure_recorded_at") == observed


async def test_pending_unknown_clock_remains_eligible_without_a_handled_marker(repo: StorageBackend) -> None:
    job = await repo.enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "unreported-marker")
    job.update(status="FAILED", **collection_clock().fields("finished_at"))
    job.update(
        followup_failure_recorded_at=None, followup_failure_recorded_at_prec_reason="Occurrence not reported"
    )
    job_id = job["job_id"]
    assert isinstance(job_id, str)
    await repo.put("ops.jobs", job_id, job)
    adapter = RepositoryRuntimeFailures(repo, declared_versions_fixture(), collection_clock)
    items = [item async for item in adapter.pending(progress_requested=False)]
    assert len(items) == 1 and items[0].failure.header.job_id == job_id
    assert items[0].marker is not None and items[0].marker.value is None
    assert await repo.get("ops.jobs", job_id) == job
    assert await repo.list("out.user_questions") == [] and await repo.list("out.deliveries") == []

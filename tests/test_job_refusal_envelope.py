"""Queue persistence retains only reported, job-bound historical refusal receipts."""

import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import ValidationError
from test_storage import repo as repo

from tank.contracts.jobs import JobEnvelope
from tank.contracts.question_retirement import (
    QuestionFailureRefusalReceipt,
    QuestionFailureRefusalRequest,
    UnretainedQuestionJournal,
)
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.job_repository import JobRepository
from tank.storage import StorageBackend
from tank.time_precision import collection_clock


async def queued(store: StorageBackend) -> tuple[StorageBackend, JobRepository, JobEnvelope]:
    jobs = JobRepository(store)
    return store, jobs, await jobs.enqueue("FILTER_ITEM", {"item_id": "item"}, "refusal-fixture")


def receipt(job_id: str) -> QuestionFailureRefusalReceipt:
    return QuestionFailureRefusalReceipt(
        request=QuestionFailureRefusalRequest(
            job_id=job_id,
            job_status="FAILED",
            witness=UnretainedQuestionJournal(
                target=FollowupRefusalTarget(
                    question_id="historical-question", session_id="historical-session"
                ),
                reason="INPUT_JOURNAL_NOT_RETAINED",
            ),
        ),
        controller=declared_versions_fixture(),
        recorded_at=collection_clock(),
    )


async def test_unreported_refusal_ledger_stays_absent_in_model_and_queue_wire(repo: StorageBackend) -> None:
    store, jobs, job = await queued(repo)
    assert "host_question_failure_refusals" not in job.model_dump(mode="json")
    await jobs.put(job)
    stored = await store.get("ops.jobs", job.job_id)
    assert stored is not None and "host_question_failure_refusals" not in stored
    wire = job.model_dump(mode="json", exclude_unset=True)
    wire["host_question_failure_refusals"] = None
    with pytest.raises(ValidationError, match="cannot be null"):
        JobEnvelope.model_validate(wire)


async def test_reported_refusal_roundtrips_historical_status_and_target_without_relabeling(
    repo: StorageBackend,
) -> None:
    store, jobs, job = await queued(repo)
    recorded = receipt(job.job_id)
    changed = JobEnvelope.model_validate(
        job.model_copy(update={"host_question_failure_refusals": {recorded.request.key(): recorded}})
    )
    saved = await jobs.put(changed)
    assert saved.status == "QUEUED"
    assert saved.host_question_failure_refusals == {recorded.request.key(): recorded}
    loaded = await jobs.get(job.job_id)
    assert loaded is not None and loaded.host_question_failure_refusals is not None
    actual = loaded.host_question_failure_refusals[recorded.request.key()]
    assert actual.request.job_status == "FAILED"
    assert actual.request.witness.target.question_id == "historical-question"
    assert actual.recorded_at == recorded.recorded_at and actual.controller == recorded.controller
    stored = await store.get("ops.jobs", job.job_id)
    assert stored is not None and "host_question_failure_refusals" in stored


@pytest.mark.parametrize("case", ["MALFORMED_KEY", "WRONG_DIGEST", "WRONG_JOB", "NULL"])
async def test_invalid_refusal_ledger_is_refused_before_any_queue_write(
    repo: StorageBackend, case: str
) -> None:
    store, jobs, job = await queued(repo)
    recorded = receipt("other-job" if case == "WRONG_JOB" else job.job_id)
    key = (
        "malformed"
        if case == "MALFORMED_KEY"
        else "0" * 64
        if case == "WRONG_DIGEST"
        else recorded.request.key()
    )
    candidate = job.model_copy(
        update={"host_question_failure_refusals": None if case == "NULL" else {key: recorded}}
    )
    before = await store.get("ops.jobs", job.job_id)
    with pytest.raises(ValidationError):
        await jobs.put(candidate)
    assert await store.get("ops.jobs", job.job_id) == before

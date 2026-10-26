"""Actual durable command admission is closed before worker effects."""

import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue, ValidationError
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import JobEnvelope
from tank.runtime_dispatch_contracts import (
    DeliverCommand,
    DependencyState,
    RuntimeCommandRecord,
)
from tank.storage import MemoryRepository, StorageBackend


@pytest.mark.asyncio
async def test_claimed_delivery_decodes_exact_declared_dependency_metadata() -> None:
    jobs = JobRepository(MemoryRepository())
    dependencies: list[JsonValue] = ["prior"]
    await jobs.enqueue(
        "DELIVER",
        {"alert_id": "alert", "version": 1, "channel": "telegram:alerts", "_depends_on": dependencies},
        "delivery",
    )
    claim = await jobs.claim()
    assert claim is not None
    assert isinstance(claim, JobEnvelope)
    command = RuntimeCommandRecord.model_validate({"type": claim.type, "payload": claim.payload}).root
    assert isinstance(command, DeliverCommand)
    assert command.payload.depends_on == ["prior"]
    assert command.payload.model_dump(mode="json", by_alias=True, exclude_unset=True) == claim.payload


@pytest.mark.parametrize(
    "payload",
    [
        {"alert_id": "alert", "version": True, "channel": "telegram:alerts"},
        {"alert_id": "alert", "version": 1, "channel": "telegram:alerts", "depends_on": []},
        {"alert_id": "alert", "version": 1},
    ],
)
def test_closed_dispatch_refuses_invalid_delivery_facts(payload: dict[str, str | int | list[str]]) -> None:
    with pytest.raises(ValidationError):
        RuntimeCommandRecord.model_validate({"type": "DELIVER", "payload": payload})


def test_dispatch_refuses_unknown_kind_and_dependency_projection_requires_actual_identity() -> None:
    with pytest.raises(ValidationError):
        RuntimeCommandRecord.model_validate({"type": "UNREGISTERED", "payload": {}})
    with pytest.raises(ValidationError):
        DependencyState.model_validate({"status": "DONE"})
    assert DependencyState(job_id="prior", status="DONE").status == "DONE"


@pytest.mark.asyncio
async def test_worker_queue_quarantines_invalid_command_once_without_changing_unrelated_facts() -> None:
    from tank.contracts.jobs import InvalidJobClaim
    from tank.runtime_worker_repository import RepositoryWorkerQueue

    repo = MemoryRepository()
    job = await JobRepository(repo).enqueue(
        "DELIVER", {"alert_id": "alert", "version": True, "channel": "telegram:alerts"}, "invalid"
    )
    queue = RepositoryWorkerQueue(repo)
    invalid = await queue.claim()
    assert isinstance(invalid, InvalidJobClaim)
    assert invalid.error_fields == ("payload",)
    assert await queue.reject_invalid(invalid)
    terminal = await repo.get("ops.jobs", job.job_id)
    assert terminal is not None
    assert terminal["payload"] == job.payload
    assert terminal["status"] == "FAILED" and terminal["last_error"] == "INVALID_QUEUE_ENVELOPE"
    assert not await queue.reject_invalid(invalid)
    assert await queue.claim() is None


@pytest.mark.asyncio
async def test_worker_queue_preserves_changed_claim_and_requires_real_dependency_scope() -> None:
    from tank.contracts.jobs import InvalidJobClaim
    from tank.runtime_worker_repository import RepositoryWorkerQueue

    repo = MemoryRepository()
    job = await JobRepository(repo).enqueue("UNKNOWN", {}, "unregistered")
    queue = RepositoryWorkerQueue(repo)
    invalid = await queue.claim()
    assert isinstance(invalid, InvalidJobClaim)
    changed = await repo.get("ops.jobs", job.job_id)
    assert changed is not None
    changed["payload"] = {"new_fact": "preserve"}
    await repo.put("ops.jobs", job.job_id, changed)
    assert not await queue.reject_invalid(invalid)
    assert await repo.get("ops.jobs", job.job_id) == changed
    await repo.put(
        "ops.jobs",
        "dependency",
        {**job.model_dump(mode="json", exclude_unset=True), "job_id": "wrong", "idem_key": "dependency"},
    )
    with pytest.raises(ValueError, match="identity"):
        await queue.dependency("dependency")
    assert await queue.dependency("missing") is None


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_worker_command_quarantine_preserves_real_claim_revision(
    postgres_template: PostgresTemplate,
) -> None:
    import os

    from tank.contracts.jobs import InvalidJobClaim
    from tank.runtime_worker_repository import RepositoryWorkerQueue

    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        job = await JobRepository(repo).enqueue(
            "DELIVER", {"alert_id": "alert", "version": True, "channel": "telegram:alerts"}, "invalid"
        )
        queue = RepositoryWorkerQueue(repo)
        invalid = await queue.claim()
        assert isinstance(invalid, InvalidJobClaim)
        assert await queue.reject_invalid(invalid)
        stored = await repo.get("ops.jobs", job.job_id)
        assert stored is not None
        assert stored["status"] == "FAILED" and stored["payload"] == job.payload
        assert stored["finished_at"] is not None
        assert not await queue.reject_invalid(invalid)


@pytest.mark.asyncio
async def test_dependency_wait_then_completion_uses_actual_state_without_consuming_an_attempt() -> None:
    from tank.runtime_dispatch_contracts import ClaimedWork, ReadyDependencies, WaitingDependencies
    from tank.runtime_worker_repository import RepositoryWorkerQueue

    repo = MemoryRepository()
    jobs = JobRepository(repo)
    dependency = await jobs.enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "prior", priority=4)
    dependencies: list[JsonValue] = [dependency.job_id]
    queued = await jobs.enqueue(
        "DELIVER",
        {"alert_id": "alert", "version": 1, "channel": "telegram:alerts", "_depends_on": dependencies},
        "waiting",
        priority=0,
    )
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork) and work.job.job_id == queued.job_id
    waiting = await queue.dependencies(work)
    assert isinstance(waiting, WaitingDependencies) and waiting.job_ids == (dependency.job_id,)
    observed = await repo.get("ops.jobs", queued.job_id)
    assert observed is not None and observed["attempts"] == 1 and observed["status"] == "RUNNING"
    prior = await jobs.claim()
    assert isinstance(prior, JobEnvelope) and prior.job_id == dependency.job_id
    await jobs.finish(dependency.job_id)
    assert isinstance(await queue.dependencies(work), ReadyDependencies)


@pytest.mark.asyncio
async def test_missing_dependency_is_an_explicit_failure_not_a_completed_or_waiting_state() -> None:
    from tank.runtime_dispatch_contracts import ClaimedWork, DependencyFailure, MissingDependency
    from tank.runtime_worker_repository import RepositoryWorkerQueue

    repo = MemoryRepository()
    dependencies: list[JsonValue] = ["missing"]
    await JobRepository(repo).enqueue(
        "DELIVER",
        {"alert_id": "alert", "version": 1, "channel": "telegram:alerts", "_depends_on": dependencies},
        "missing-prior",
    )
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    with pytest.raises(DependencyFailure) as failure:
        await queue.dependencies(work)
    assert isinstance(failure.value.failed, MissingDependency)
    assert failure.value.failed.job_id == "missing"


@pytest.mark.asyncio
async def test_worker_quarantine_uses_raw_revision_with_unreported_nested_receipt_clock_facets(
    repo: StorageBackend,
) -> None:
    from test_job_claim_revision import requeued_job_with_partial_receipt_clock

    from tank.contracts.jobs import InvalidJobClaim
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.util import canonical_json

    _, job_id = await requeued_job_with_partial_receipt_clock(repo)
    original = await repo.get("ops.jobs", job_id)
    assert original is not None
    queue = RepositoryWorkerQueue(repo)
    invalid = await queue.claim()
    assert isinstance(invalid, InvalidJobClaim)
    assert await queue.reject_invalid(invalid)
    stored = await repo.get("ops.jobs", job_id)
    assert stored is not None and stored["status"] == "FAILED"
    assert canonical_json(stored["host_question_failure_refusals"]) == canonical_json(
        original["host_question_failure_refusals"]
    )
    assert not await queue.reject_invalid(invalid)

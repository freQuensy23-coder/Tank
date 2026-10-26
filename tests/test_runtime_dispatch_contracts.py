"""Actual durable command admission is closed before worker effects."""

import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue, ValidationError
from test_storage import repo as repo

from tank.contracts.jobs import JobEnvelope
from tank.job_repository import JobRepository
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


async def test_worker_requeue_preserves_actual_scope_and_refunds_only_its_claimed_attempt(
    repo: StorageBackend,
) -> None:
    from datetime import timedelta

    from tank.runtime_dispatch_contracts import ClaimedWork, RequeueRequest
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import collection_clock, offset_schedule

    job = await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "requeue")
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    not_before = offset_schedule(collection_clock(), timedelta(seconds=30))
    assert await queue.requeue(
        RequeueRequest(claim=work.scope(), not_before=not_before, reason="DELIVERY_WAIT")
    )
    stored = await repo.get("ops.jobs", job.job_id)
    assert stored is not None and stored["status"] == "QUEUED" and stored["attempts"] == 0
    assert stored["run_after"] == not_before.value and stored["payload"] == job.payload
    assert not await queue.requeue(
        RequeueRequest(claim=work.scope(), not_before=not_before, reason="DELIVERY_WAIT")
    )
    assert await repo.get("ops.jobs", job.job_id) == stored


async def test_changed_command_cannot_be_requeued_by_an_older_claim(repo: StorageBackend) -> None:
    from datetime import timedelta

    from tank.runtime_dispatch_contracts import ClaimedWork, RequeueRequest
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import collection_clock, offset_schedule

    job = await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "original"}, "scope")
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    changed = await repo.get("ops.jobs", job.job_id)
    assert changed is not None
    changed["payload"] = {"question_id": "changed"}
    await repo.put("ops.jobs", job.job_id, changed)
    assert not await queue.requeue(
        RequeueRequest(
            claim=work.scope(),
            not_before=offset_schedule(collection_clock(), timedelta(seconds=1)),
            reason="DELIVERY_WAIT",
        )
    )
    assert await repo.get("ops.jobs", job.job_id) == changed


async def test_worker_expiry_requires_actual_due_boundary_and_preserves_attempt_count(
    repo: StorageBackend,
) -> None:
    from datetime import timedelta

    from tank.runtime_dispatch_contracts import ClaimedWork
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import Timestamp, collection_clock, offset_schedule

    observed = collection_clock()
    expires = offset_schedule(observed, timedelta(days=1))
    job = await JobRepository(repo).enqueue(
        "TELEGRAM_ANSWER", {"question_id": "question"}, "expiry", expires_at=expires
    )
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    with pytest.raises(ValueError, match="actual controller"):
        await queue.expire(work.scope(), Timestamp(None, None, "No observation clock retained"))
    assert not await queue.expire(work.scope(), observed)
    assert await queue.expire(work.scope(), offset_schedule(observed, timedelta(days=2)))
    stored = await repo.get("ops.jobs", job.job_id)
    assert stored is not None and stored["status"] == "EXPIRED" and stored["attempts"] == 1
    assert stored["finished_at"] is not None and stored["payload"] == job.payload
    assert not await queue.expire(work.scope(), offset_schedule(observed, timedelta(days=3)))


async def test_requeue_does_not_normalize_unrelated_historical_receipt_fields(repo: StorageBackend) -> None:
    from datetime import timedelta

    from test_job_claim_revision import requeued_job_with_partial_receipt_clock

    from tank.runtime_dispatch_contracts import ClaimedWork, RequeueRequest
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import collection_clock, offset_schedule
    from tank.util import canonical_json

    _, job_id = await requeued_job_with_partial_receipt_clock(repo)
    original = await repo.get("ops.jobs", job_id)
    assert original is not None
    original["payload"] = {"item_id": "item"}
    await repo.put("ops.jobs", job_id, original)
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    assert await queue.requeue(
        RequeueRequest(
            claim=work.scope(),
            not_before=offset_schedule(collection_clock(), timedelta(seconds=1)),
            reason="DEPENDENCY_WAIT",
        )
    )
    stored = await repo.get("ops.jobs", job_id)
    assert stored is not None and stored["status"] == "QUEUED"
    assert canonical_json(stored["host_question_failure_refusals"]) == canonical_json(
        original["host_question_failure_refusals"]
    )


async def test_admission_defer_clock_is_closed_and_the_next_typed_claim_remains_valid(
    repo: StorageBackend,
) -> None:
    from tank.runtime_dispatch_contracts import ClaimedWork, RequeueRequest
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import collection_clock

    job = await JobRepository(repo).enqueue(
        "AGENT_MESSAGE", {"kind": "NEW_ITEM", "item_id": "item"}, "admission"
    )
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    assert await queue.requeue(
        RequeueRequest(claim=work.scope(), not_before=collection_clock(), reason="SESSION_ADMISSION")
    )
    stored = await repo.get("ops.jobs", job.job_id)
    assert stored is not None and stored["attempts"] == 0
    assert stored["slot_waiting_since"] == stored["created_at"]
    assert stored["slot_waiting_since_prec"] == stored["created_at_prec"]
    assert stored["slot_waiting_since_prec_reason"] == stored["created_at_prec_reason"]
    reclaimed = await queue.claim()
    assert isinstance(reclaimed, ClaimedWork) and reclaimed.job.attempts == 1
    assert reclaimed.job.slot_waiting_since == work.job.created_at


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "facets",
    [
        {},
        {
            "slot_waiting_since_prec": None,
            "slot_waiting_since_prec_reason": None,
            "slot_waiting_since_bucket_anchor": None,
        },
    ],
)
async def test_legacy_slot_clock_does_not_gain_invented_precision_or_reason(
    facets: dict[str, JsonValue],
) -> None:
    repo = MemoryRepository()
    job = await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "legacy-clock")
    document = {
        **job.model_dump(mode="json", exclude_unset=True),
        "slot_waiting_since": "2026-10-26T00:00:00Z",
        **facets,
    }
    decoded = JobEnvelope.model_validate(document)
    serialized = decoded.model_dump(mode="json", exclude_unset=True)
    assert serialized == document
    assert decoded.slot_waiting_since_prec is None and decoded.slot_waiting_since_prec_reason is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "clock",
    [
        {"slot_waiting_since": "2026-10-26T00:00:00"},
        {"slot_waiting_since": None, "slot_waiting_since_prec": "EXACT"},
        {"slot_waiting_since": "2026-10-26T00:00:00Z", "slot_waiting_since_prec": "NOT_A_PRECISION"},
    ],
)
async def test_invalid_slot_clock_refuses_before_any_repository_write(clock: dict[str, JsonValue]) -> None:
    repo = MemoryRepository()
    jobs = JobRepository(repo)
    job = await jobs.enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "bad-clock")
    before = await repo.get("ops.jobs", job.job_id)
    with pytest.raises(ValidationError):
        await jobs.put(job.model_copy(update=clock))
    assert await repo.get("ops.jobs", job.job_id) == before


async def test_unproved_zero_claim_attempt_is_quarantined_without_inventing_one(repo: StorageBackend) -> None:
    from tank.contracts.jobs import InvalidJobClaim
    from tank.runtime_worker_repository import RepositoryWorkerQueue

    job = await JobRepository(repo).enqueue("FILTER_ITEM", {"item_id": "item"}, "unproved-recovery")
    stored = await repo.get("ops.jobs", job.job_id)
    assert stored is not None
    stored["recovery_reclaim"] = True
    await repo.put("ops.jobs", job.job_id, stored)
    queue = RepositoryWorkerQueue(repo)
    invalid = await queue.claim()
    assert isinstance(invalid, InvalidJobClaim) and invalid.attempts == 0
    assert invalid.error_fields == ("claim",)
    assert await queue.reject_invalid(invalid)
    terminal = await repo.get("ops.jobs", job.job_id)
    assert terminal is not None and terminal["status"] == "FAILED" and terminal["attempts"] == 0
    assert await queue.claim() is None


async def test_worker_completion_preserves_receipts_and_refuses_second_ack(repo: StorageBackend) -> None:
    from test_job_claim_revision import requeued_job_with_partial_receipt_clock

    from tank.runtime_dispatch_contracts import ClaimedWork
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import collection_clock
    from tank.util import canonical_json

    _, job_id = await requeued_job_with_partial_receipt_clock(repo)
    original = await repo.get("ops.jobs", job_id)
    assert original is not None
    original["payload"] = {"item_id": "item"}
    await repo.put("ops.jobs", job_id, original)
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    observed = collection_clock()
    completed = await queue.complete(work.scope(), {"accepted": True}, observed)
    assert completed is not None and completed.status == "DONE"
    stored = await repo.get("ops.jobs", job_id)
    assert stored is not None and stored["result"] == {"accepted": True}
    assert stored["finished_at"] == observed.value
    assert canonical_json(stored["host_question_failure_refusals"]) == canonical_json(
        original["host_question_failure_refusals"]
    )
    assert await queue.complete(work.scope(), {"accepted": False}, collection_clock()) is None
    assert await repo.get("ops.jobs", job_id) == stored


@pytest.mark.parametrize("changed_field", ["attempts", "payload", "status"])
async def test_stale_worker_cannot_complete_or_fail_changed_claim(
    repo: StorageBackend, changed_field: str
) -> None:
    from tank.runtime_dispatch_contracts import ClaimedWork, WorkerFailureRequest
    from tank.runtime_retry import ApplicationBackoff
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import collection_clock

    job = await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "stale")
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    changed = await repo.get("ops.jobs", job.job_id)
    assert changed is not None
    if changed_field == "attempts":
        changed["attempts"] = 2
    elif changed_field == "payload":
        changed["payload"] = {"question_id": "other"}
    else:
        changed["status"] = "QUEUED"
    await repo.put("ops.jobs", job.job_id, changed)
    assert await queue.complete(work.scope(), None, collection_clock()) is None
    assert (
        await queue.fail(
            WorkerFailureRequest(
                claim=work.scope(),
                error_class="RuntimeError",
                schedule=ApplicationBackoff(basis="APPLICATION_BACKOFF", attempts=work.job.attempts),
                observed_at=collection_clock(),
            )
        )
        is None
    )
    assert await repo.get("ops.jobs", job.job_id) == changed


@pytest.mark.parametrize("terminal", [False, True])
async def test_worker_failure_uses_actual_attempt_and_declared_zero_delay(
    repo: StorageBackend, terminal: bool
) -> None:
    from tank.runtime_dispatch_contracts import ClaimedWork, WorkerFailureRequest
    from tank.runtime_retry import DeliveryRetryDelay
    from tank.runtime_worker_repository import RepositoryWorkerQueue
    from tank.time_precision import collection_clock

    job = await JobRepository(repo).enqueue(
        "TELEGRAM_ANSWER", {"question_id": "question"}, "failure", max_attempts=1 if terminal else 2
    )
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    observed = collection_clock()
    result = await queue.fail(
        WorkerFailureRequest(
            claim=work.scope(),
            error_class="DeliveryError",
            schedule=DeliveryRetryDelay(basis="DELIVERY_DIRECTIVE", seconds=0),
            observed_at=observed,
        )
    )
    assert result is not None and result.status == ("FAILED" if terminal else "QUEUED")
    assert result.attempts == 1 and result.run_after == observed.value
    assert result.finished_at == (observed.value if terminal else None)
    stored = await repo.get("ops.jobs", job.job_id)
    assert stored is not None and stored["last_error"] == "DeliveryError"
    if not terminal:
        reclaimed = await queue.claim()
        assert isinstance(reclaimed, ClaimedWork) and reclaimed.job.attempts == 2


async def test_constructed_work_cannot_replace_command_behind_original_claim() -> None:
    from tank.runtime_dispatch_contracts import ClaimedWork, TelegramAnswerCommand
    from tank.runtime_worker_repository import RepositoryWorkerQueue

    repo = MemoryRepository()
    await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "original"}, "actual")
    work = await RepositoryWorkerQueue(repo).claim()
    assert isinstance(work, ClaimedWork)
    changed = work.model_copy(
        update={
            "command": TelegramAnswerCommand.model_validate(
                {"type": "TELEGRAM_ANSWER", "payload": {"question_id": "other"}}
            )
        }
    )
    with pytest.raises(ValidationError, match="original claimed payload"):
        ClaimedWork.model_validate(changed, by_name=True)
    changed_digest = work.model_copy(update={"payload_revision": "0" * 64})
    with pytest.raises(ValidationError, match="revision differs"):
        ClaimedWork.model_validate(changed_digest, by_name=True)

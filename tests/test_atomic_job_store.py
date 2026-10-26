"""Actual backend claim receipts fence sparse lifecycle mutations and raw quarantine."""

from typing import Literal

import pytest
from pydantic import JsonValue, ValidationError
from test_job_claim_revision import requeued_job_with_partial_receipt_clock
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import InvalidJobClaim, JobClaimMutation, JobClaimScope, JobEnvelope
from tank.models import Record
from tank.storage import StorageBackend
from tank.time_precision import Timestamp, chosen_boundary, collection_clock
from tank.util import canonical_json, sha256


def claim_scope(document: Record) -> JobClaimScope:
    current = JobEnvelope.model_validate(document)
    token = current.claim_token
    assert token is not None
    return JobClaimScope(
        job_id=current.job_id,
        kind=current.type,
        claim_token=token,
        attempts=current.attempts,
        recovery_count=current.recovery_count,
        payload_revision=sha256(document["payload"]),
    )


async def claimed_document(repo: StorageBackend, key: str) -> Record:
    due = chosen_boundary("2026-10-01T00:00:00Z", precision="EXACT")
    values: list[JsonValue] = [1, True, None]
    queued = await repo.enqueue("TEST", {"nested": {"values": values}}, key, run_after=due)
    assert "claim_token" not in queued
    observed = await repo.get("ops.jobs", str(queued["job_id"]))
    assert observed is not None and "claim_token" not in observed
    claimed = await repo.claim_job()
    assert claimed is not None and claimed["job_id"] == queued["job_id"]
    return claimed


def completion(current: JobEnvelope) -> JobClaimMutation:
    assert current.status == "RUNNING" and current.claim_token is not None
    return JobClaimMutation(status="DONE", result=None, finished_at=collection_clock())


@pytest.mark.parametrize("mode", ["refund", "recovery"])
async def test_each_actual_claim_has_a_new_receipt_even_when_attempt_is_preserved(
    repo: StorageBackend, mode: Literal["refund", "recovery"]
) -> None:
    first = await claimed_document(repo, "receipt-" + mode)
    original = claim_scope(first)
    if mode == "refund":

        def refund(current: JobEnvelope) -> JobClaimMutation:
            return JobClaimMutation(
                status="QUEUED",
                attempts=current.attempts - 1,
                run_after=chosen_boundary("2026-10-01T00:00:00Z", precision="EXACT"),
                finished_at=Timestamp(None, None, "PENDING"),
            )

        assert await repo.transition_claimed_job(original, refund) is not None
    else:
        assert await repo.recover_jobs() == 1
    second = await repo.claim_job()
    assert second is not None
    fresh = claim_scope(second)
    assert fresh.attempts == original.attempts and fresh.claim_token != original.claim_token
    if mode == "refund":
        assert fresh.recovery_count == original.recovery_count
    else:
        assert original.recovery_count is None and fresh.recovery_count == 1
    before = canonical_json(second)
    calls = 0

    def stale(current: JobEnvelope) -> JobClaimMutation:
        nonlocal calls
        calls += 1
        return completion(current)

    assert await repo.transition_claimed_job(original, stale) is None
    assert calls == 0
    assert canonical_json(await repo.get("ops.jobs", fresh.job_id)) == before
    completed = await repo.transition_claimed_job(fresh, completion)
    assert completed is not None and completed.status == "DONE" and completed.result is None
    assert completed.claim_token == fresh.claim_token


async def test_callback_fresh_model_cannot_mutate_unreported_source_payload(repo: StorageBackend) -> None:
    first = await claimed_document(repo, "callback-isolation")
    scope = claim_scope(first)
    before = canonical_json(first["payload"])

    def mutate_view(current: JobEnvelope) -> JobClaimMutation:
        nested = current.payload["nested"]
        assert isinstance(nested, dict)
        values = nested["values"]
        assert isinstance(values, list)
        values.append("not a declared mutation")
        return JobClaimMutation(status="DONE", result={"acknowledged": True}, finished_at=collection_clock())

    completed = await repo.transition_claimed_job(scope, mutate_view)
    assert completed is not None
    stored = await repo.get("ops.jobs", scope.job_id)
    assert stored is not None and canonical_json(stored["payload"]) == before
    assert canonical_json(completed.payload) == before


async def test_callback_failure_and_unvalidated_mutation_rollback_leave_actual_claim_owned(
    repo: StorageBackend,
) -> None:
    first = await claimed_document(repo, "callback-rollback")
    scope = claim_scope(first)
    before = canonical_json(first)

    def explode(current: JobEnvelope) -> JobClaimMutation:
        current.payload["unowned"] = True
        raise RuntimeError("abort owned transition")

    with pytest.raises(RuntimeError, match="abort owned transition"):
        await repo.transition_claimed_job(scope, explode)
    assert canonical_json(await repo.get("ops.jobs", scope.job_id)) == before

    def invalid(current: JobEnvelope) -> JobClaimMutation:
        assert current.claim_token == scope.claim_token
        return JobClaimMutation.model_construct(status="DONE", result=None, finished_at=None)

    with pytest.raises(ValidationError):
        await repo.transition_claimed_job(scope, invalid)
    assert canonical_json(await repo.get("ops.jobs", scope.job_id)) == before
    assert await repo.transition_claimed_job(scope, completion) is not None


async def test_sparse_completion_preserves_nested_original_receipt_omissions(repo: StorageBackend) -> None:
    _, job_id = await requeued_job_with_partial_receipt_clock(repo)
    queued = await repo.get("ops.jobs", job_id)
    assert queued is not None
    queued["payload"] = {"item_id": "repaired-before-current-claim"}
    await repo.put("ops.jobs", job_id, queued)
    claimed = await repo.claim_job()
    assert claimed is not None
    scope = claim_scope(claimed)
    before = canonical_json(claimed["host_question_failure_refusals"])
    completed = await repo.transition_claimed_job(scope, completion)
    assert completed is not None
    stored = await repo.get("ops.jobs", job_id)
    assert stored is not None and canonical_json(stored["host_question_failure_refusals"]) == before


async def invalid_document(repo: StorageBackend, key: str) -> InvalidJobClaim:
    values: list[JsonValue] = [1, True, None]
    queued = await repo.enqueue("TEST", {"retained": values}, key)
    job_id = queued["job_id"]
    assert isinstance(job_id, str)
    queued["fake-private-extra"] = {"value": "fake-private-value"}
    await repo.put("ops.jobs", job_id, queued)
    invalid = await JobRepository(repo).claim()
    assert isinstance(invalid, InvalidJobClaim)
    assert "fake-private" not in invalid.model_dump_json()
    return invalid


async def test_quarantine_is_atomic_once_and_preserves_original_malformed_body(repo: StorageBackend) -> None:
    invalid = await invalid_document(repo, "quarantine-original")
    original = await repo.get("ops.jobs", invalid.job_id)
    assert original is not None
    assert await JobRepository(repo).reject_invalid(invalid)
    stored = await repo.get("ops.jobs", invalid.job_id)
    assert stored is not None and stored["status"] == "FAILED"
    assert stored["claim_token"] == invalid.claim_token
    assert stored["payload"] == original["payload"]
    assert stored["fake-private-extra"] == original["fake-private-extra"]
    assert (
        stored["last_error"] == "INVALID_QUEUE_ENVELOPE" and stored["finished_at_prec"] == "COLLECTION_ONLY"
    )
    before_repeat = canonical_json(stored)
    assert not await JobRepository(repo).reject_invalid(invalid)
    assert canonical_json(await repo.get("ops.jobs", invalid.job_id)) == before_repeat


async def test_old_quarantine_token_cannot_reject_a_reclaimed_invalid_body(repo: StorageBackend) -> None:
    first = await invalid_document(repo, "quarantine-generation")
    assert await repo.recover_jobs() == 1
    second = await JobRepository(repo).claim()
    assert isinstance(second, InvalidJobClaim) and second.claim_token != first.claim_token
    current = await repo.get("ops.jobs", second.job_id)
    assert current is not None
    before = canonical_json(current)
    stale = first.model_copy(update={"revision": sha256(current)})
    assert not await repo.quarantine_job_claim(stale)
    assert canonical_json(await repo.get("ops.jobs", second.job_id)) == before
    assert await repo.quarantine_job_claim(second)

"""Command quarantine binds the exact unprojected claim, not a normalized model dump."""

from agent_source_fixtures import declared_versions_fixture
from pydantic import ValidationError
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import ClaimedJob, InvalidJobClaim, JobEnvelope
from tank.contracts.json import JsonDocument
from tank.contracts.question_retirement import (
    QuestionFailureRefusalReceipt,
    QuestionFailureRefusalRequest,
    UnretainedQuestionJournal,
)
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.models import Record
from tank.runtime_contracts import ItemJob
from tank.storage import StorageBackend
from tank.time_precision import Timestamp, chosen_boundary, collection_clock
from tank.util import canonical_json, sha256


async def requeued_job_with_partial_receipt_clock(store: StorageBackend) -> tuple[JobRepository, str]:
    jobs = JobRepository(store)
    due = chosen_boundary("2026-10-17T12:00:00Z", precision="EXACT")
    job = await jobs.enqueue("FILTER_ITEM", {"item_id": 1}, "raw-revision", max_attempts=1, run_after=due)
    previous_claim = await store.claim_job()
    assert previous_claim is not None and previous_claim["job_id"] == job.job_id
    await store.fail_job(job.job_id, "previous synthetic command failure")
    previous = await store.get("ops.jobs", job.job_id)
    assert previous is not None and previous["status"] == "FAILED"
    request = QuestionFailureRefusalRequest(
        job_id=job.job_id,
        job_status="FAILED",
        witness=UnretainedQuestionJournal(
            target=FollowupRefusalTarget(question_id="historical-question", session_id="historical-session"),
            reason="INPUT_JOURNAL_NOT_RETAINED",
        ),
    )
    receipt = QuestionFailureRefusalReceipt(
        request=request, controller=declared_versions_fixture(), recorded_at=collection_clock()
    )
    receipt_wire = JsonDocument.model_validate_json(receipt.model_dump_json(warnings="error")).root
    assert isinstance(receipt_wire, dict)
    clock = receipt_wire["recorded_at"]
    assert isinstance(clock, dict)
    assert clock["precision_reason"] is None and clock["bucket_anchor"] is None
    del clock["precision_reason"]
    del clock["bucket_anchor"]
    document: Record = {
        **previous,
        "status": "QUEUED",
        "attempts": 0,
        "max_attempts": 3,
        **Timestamp(None, None, "PENDING").fields("finished_at"),
        **due.fields("run_after"),
        "host_question_failure_refusals": {request.key(): receipt_wire},
    }
    await store.put("ops.jobs", job.job_id, document)
    return jobs, job.job_id


def command_refusal(claimed: ClaimedJob) -> InvalidJobClaim:
    try:
        ItemJob.model_validate(claimed.envelope.payload)
    except ValidationError as error:
        codes = tuple(
            sorted(
                {
                    detail["type"]
                    for detail in error.errors(include_input=False, include_context=False, include_url=False)
                }
            )
        )
    else:
        raise AssertionError("The authored command contains a non-string item identifier")
    assert codes == ("string_type",)
    return InvalidJobClaim(
        job_id=claimed.envelope.job_id,
        kind=claimed.envelope.type,
        attempts=claimed.envelope.attempts,
        revision=claimed.revision,
        error_codes=codes,
        error_fields=("payload",),
    )


async def test_command_quarantine_uses_original_claim_revision_despite_valid_model_normalization(
    repo: StorageBackend,
) -> None:
    jobs, job_id = await requeued_job_with_partial_receipt_clock(repo)
    claimed = await jobs.claim_with_revision()
    assert isinstance(claimed, ClaimedJob)
    original = await repo.get("ops.jobs", job_id)
    assert original is not None and claimed.revision == sha256(original)
    normalized = JsonDocument.model_validate_json(
        claimed.envelope.model_dump_json(exclude_unset=True, warnings="error")
    ).root
    assert claimed.revision != sha256(normalized)
    invalid = command_refusal(claimed)
    normalized_revision = invalid.model_copy(update={"revision": sha256(normalized)})
    assert not await jobs.reject_invalid(normalized_revision)
    untouched = await repo.get("ops.jobs", job_id)
    assert untouched is not None and canonical_json(untouched) == canonical_json(original)
    assert await jobs.reject_invalid(invalid)
    stored = await repo.get("ops.jobs", job_id)
    assert stored is not None and stored["status"] == "FAILED"
    assert stored["last_error"] == "INVALID_QUEUE_ENVELOPE"
    assert stored["payload"] == {"item_id": 1}
    assert canonical_json(stored["host_question_failure_refusals"]) == canonical_json(
        original["host_question_failure_refusals"]
    )
    before_repeat = canonical_json(stored)
    assert not await jobs.reject_invalid(invalid)
    current = await repo.get("ops.jobs", job_id)
    assert current is not None and canonical_json(current) == before_repeat
    assert await jobs.claim_with_revision() is None


async def test_command_quarantine_cannot_fail_a_changed_claim_revision(repo: StorageBackend) -> None:
    jobs, job_id = await requeued_job_with_partial_receipt_clock(repo)
    claimed = await jobs.claim_with_revision()
    assert isinstance(claimed, ClaimedJob)
    invalid = command_refusal(claimed)
    original = await repo.get("ops.jobs", job_id)
    assert original is not None
    changed: Record = {**original, "last_error": "changed after claim"}
    await repo.put("ops.jobs", job_id, changed)
    assert not await jobs.reject_invalid(invalid)
    current = await repo.get("ops.jobs", job_id)
    assert current is not None and current["status"] == "RUNNING"
    assert canonical_json(current) == canonical_json(changed)


async def test_revisioned_claim_keeps_initial_invalid_envelope_quarantine(repo: StorageBackend) -> None:
    jobs = JobRepository(repo)
    queued = await jobs.enqueue(
        "FILTER_ITEM",
        {"item_id": "valid-item"},
        "extra-envelope-field",
        run_after=chosen_boundary("2026-10-17T12:00:00Z", precision="EXACT"),
    )
    original = await repo.get("ops.jobs", queued.job_id)
    assert original is not None
    await repo.put("ops.jobs", queued.job_id, {**original, "unexpected_field": {"retained": True}})
    invalid = await jobs.claim_with_revision()
    assert isinstance(invalid, InvalidJobClaim)
    current = await repo.get("ops.jobs", queued.job_id)
    assert current is not None and invalid.revision == sha256(current)
    assert invalid.error_fields == ("extra_field",)
    assert await jobs.reject_invalid(invalid)
    current = await repo.get("ops.jobs", queued.job_id)
    assert current is not None and current["status"] == "FAILED"
    assert current["unexpected_field"] == {"retained": True}


async def test_existing_claim_still_returns_the_validated_envelope(repo: StorageBackend) -> None:
    jobs = JobRepository(repo)
    queued = await jobs.enqueue(
        "FILTER_ITEM",
        {"item_id": "valid-item"},
        "envelope-only",
        run_after=chosen_boundary("2026-10-17T12:00:00Z", precision="EXACT"),
    )
    claimed = await jobs.claim()
    assert isinstance(claimed, JobEnvelope)
    assert claimed.job_id == queued.job_id and claimed.status == "RUNNING"
    assert claimed.payload == {"item_id": "valid-item"}

"""Pure claim fencing and sparse mutations preserve observed wire facts."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.jobs import DependencyState, JobClaimMutation, JobClaimScope, JobEnvelope
from tank.time_precision import Timestamp
from tank.util import canonical_json, sha256


def job_document() -> dict[str, JsonValue]:
    return {
        "job_id": "job",
        "type": "FILTER_ITEM",
        "priority": 1,
        "idem_key": "original",
        "status": "RUNNING",
        "attempts": 1,
        "max_attempts": 3,
        "last_error": None,
        "created_at": "2026-10-26T00:00:00Z",
        "created_at_prec": "COLLECTION_ONLY",
        "created_at_prec_reason": None,
        "run_after": "2026-10-26T00:00:00Z",
        "run_after_prec": "BUCKET(1m)",
        "run_after_prec_reason": None,
        "run_after_bucket_anchor": "START",
        "expires_at": None,
        "expires_at_prec": None,
        "expires_at_prec_reason": "UNBOUNDED",
        "finished_at": None,
        "finished_at_prec": None,
        "finished_at_prec_reason": "PENDING",
        "payload": {"item_id": "item"},
        "result": {"prior": [1, 1.0, True, False, 0, None]},
    }


def scope_for(document: dict[str, JsonValue]) -> JobClaimScope:
    return JobClaimScope(
        job_id="job",
        kind="FILTER_ITEM",
        claim_token="actual-token",
        attempts=1,
        recovery_count=None,
        payload_revision=sha256(document["payload"]),
    )


def test_historical_token_absence_stays_unreported_and_reported_null_refuses() -> None:
    document = job_document()
    job = JobEnvelope.model_validate(document)
    assert job.claim_token is None and "claim_token" not in job.model_fields_set
    assert "claim_token" not in job.model_dump(mode="json")
    restored = JobEnvelope.model_validate_json(job.model_dump_json())
    assert restored.claim_token is None and "claim_token" not in restored.model_fields_set
    assert canonical_json(job.model_dump(mode="json", exclude_unset=True)) == canonical_json(document)
    with pytest.raises(ValidationError, match="cannot be null"):
        JobEnvelope.model_validate({**document, "claim_token": None})
    with pytest.raises(ValidationError, match="cannot be null"):
        JobEnvelope.model_validate(job.model_copy(update={"claim_token": None}))


@pytest.mark.parametrize("token", [None, "", " ", True, 1])
def test_current_scope_requires_the_actual_nonempty_token(token: JsonValue) -> None:
    with pytest.raises(ValidationError):
        JobClaimScope.model_validate(
            {
                "job_id": "job",
                "kind": "FILTER_ITEM",
                "claim_token": token,
                "attempts": 1,
                "recovery_count": None,
                "payload_revision": "a" * 64,
            }
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("claim_token", "replacement"),
        ("status", "QUEUED"),
        ("attempts", True),
        ("recovery_count", False),
        ("payload", {"item_id": "other"}),
    ],
)
def test_scope_matches_only_actual_original_running_claim(field: str, value: JsonValue) -> None:
    document: dict[str, JsonValue] = {**job_document(), "claim_token": "actual-token"}
    scope = scope_for(document)
    assert scope.matches(document)
    assert not scope.matches({**document, field: value})
    assert not scope.matches(job_document())


def test_original_payload_revision_distinguishes_boolean_integer_and_float() -> None:
    document: dict[str, JsonValue] = {
        **job_document(),
        "claim_token": "actual-token",
        "payload": {"number": 1},
    }
    scope = scope_for(document)
    assert scope.matches(document)
    assert not scope.matches({**document, "payload": {"number": 1.0}})
    assert not scope.matches({**document, "payload": {"number": True}})


def test_sparse_mutation_replaces_clock_family_without_normalizing_unrelated_result() -> None:
    document: dict[str, JsonValue] = {**job_document(), "claim_token": "actual-token"}
    before = canonical_json(document)
    stamp = Timestamp("2026-10-26T00:01:00Z", "COLLECTION_ONLY")
    mutation = JobClaimMutation(status="QUEUED", attempts=0, run_after=stamp)
    updated = mutation.apply_to(document)
    assert updated["status"] == "QUEUED" and updated["attempts"] == 0
    assert updated["run_after"] == stamp.value and updated["run_after_prec"] == "COLLECTION_ONLY"
    assert "run_after_bucket_anchor" not in updated
    assert canonical_json(updated["result"]) == canonical_json(document["result"])
    assert "recovery_count" not in updated and updated["claim_token"] == "actual-token"
    assert canonical_json(document) == before


def test_completion_distinguishes_explicit_void_result_from_unreported_result() -> None:
    stamp = Timestamp("2026-10-26T00:01:00Z", "COLLECTION_ONLY")
    with pytest.raises(ValidationError, match="actual result"):
        JobClaimMutation(status="DONE", finished_at=stamp)
    mutation = JobClaimMutation(status="DONE", finished_at=stamp, result=None)
    updated = mutation.apply_to(job_document())
    assert updated["result"] is None and updated["finished_at"] == stamp.value
    pending = JobClaimMutation(status="QUEUED", finished_at=Timestamp(None, None, "PENDING"))
    assert pending.apply_to(job_document())["finished_at"] is None


@pytest.mark.parametrize(
    "wire",
    [
        {"status": "QUEUED", "attempts": None},
        {"status": "QUEUED", "attempts": True},
        {"status": "QUEUED", "run_after": None},
        {"status": "QUEUED", "claim_token": "forged"},
        {"status": "QUEUED", "payload": {"item_id": "other"}},
        {"status": "FAILED"},
        {"status": "EXPIRED", "finished_at": None},
    ],
)
def test_mutation_refuses_unknown_ownership_fields_and_invalid_lifecycle_facts(
    wire: dict[str, JsonValue],
) -> None:
    with pytest.raises(ValidationError):
        JobClaimMutation.model_validate(wire)


def test_empty_observed_error_text_is_distinct_from_unreported_error() -> None:
    message = str(Exception())
    assert message == ""
    stamp = Timestamp("2026-10-26T00:01:00Z", "COLLECTION_ONLY")
    mutation = JobClaimMutation(status="FAILED", last_error=message, finished_at=stamp)
    updated = mutation.apply_to(job_document())
    assert JobEnvelope.model_validate(updated).last_error == ""
    unreported = JobClaimMutation(status="FAILED", last_error=None, finished_at=stamp)
    assert unreported.apply_to(job_document())["last_error"] is None


def test_controlled_stop_records_only_actual_paired_interruption_facts() -> None:
    stamp = Timestamp("2026-10-26T00:01:00Z", "COLLECTION_ONLY")
    mutation = JobClaimMutation(
        status="QUEUED",
        attempts=0,
        run_after=stamp,
        interrupted_at=stamp,
        interruption_reason="CONTROLLED_GENERATION_STOP",
    )
    original = job_document()
    updated = mutation.apply_to(original)
    assert updated["attempts"] == 0 and updated["interrupted_at"] == stamp.value
    assert updated["interrupted_at_prec"] == "COLLECTION_ONLY"
    assert updated["interruption_reason"] == "CONTROLLED_GENERATION_STOP"
    assert updated["payload"] == original["payload"] and updated["result"] == original["result"]
    with pytest.raises(ValidationError, match="clock and reason together"):
        JobClaimMutation(status="QUEUED", interrupted_at=stamp)
    with pytest.raises(ValidationError, match="clock and reason together"):
        JobClaimMutation(status="QUEUED", interruption_reason="CONTROLLED_GENERATION_STOP")
    with pytest.raises(ValidationError, match="unfinished attempt"):
        JobClaimMutation(
            status="FAILED",
            interrupted_at=stamp,
            interruption_reason="CONTROLLED_GENERATION_STOP",
            finished_at=stamp,
        )


def test_historical_dependency_state_needs_only_its_actual_identity_and_status() -> None:
    from tank.runtime_dispatch_contracts import DependencyState as ReexportedDependencyState

    assert ReexportedDependencyState is DependencyState
    historical = DependencyState.model_validate({"job_id": "historical-job", "status": "DONE"})
    assert historical.job_id == "historical-job" and historical.status == "DONE"
    with pytest.raises(ValidationError):
        DependencyState.model_validate({"status": "DONE"})
    with pytest.raises(ValidationError):
        DependencyState.model_validate({"job_id": "historical-job", "status": True})

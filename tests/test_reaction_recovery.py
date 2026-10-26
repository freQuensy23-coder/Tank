"""Mechanical witness admission before recovery eligibility or storage effects."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.reaction_recovery import (
    ReactionRecoveryWitnessRequest,
    RetainedCoalescedReaction,
)


def request_fields() -> dict[str, str]:
    return {
        "enqueue_idempotency_key": "recover:original",
        "original_job_id": "original",
        "peer_job_id": "peer",
        "source_backfill_id": "backfill",
        "checkpoint_key": "event:token:24h:r2",
        "change_marker_key": "reaction-change:change",
        "change_owner_key": "reaction-change-owner:change",
        "recovery_link_key": "reaction-recovery:original",
    }


def test_recovery_witnesses_have_exact_deterministic_addresses() -> None:
    request = ReactionRecoveryWitnessRequest.model_validate(request_fields())
    assert [(row.collection, row.storage_key) for row in request.addresses()] == [
        ("mkt.backfill_runs", "backfill"),
        ("ops.jobs", "original"),
        ("ops.jobs", "peer"),
        ("ops.scheduler_checkpoints", "reaction-change-owner:change"),
        ("ops.scheduler_checkpoints", "reaction-change:change"),
        ("ops.scheduler_checkpoints", "reaction-recovery:original"),
        ("out.reaction_checkpoints", "event:token:24h:r2"),
    ]
    assert ReactionRecoveryWitnessRequest.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize("field", tuple(request_fields()))
@pytest.mark.parametrize("value", [None, "", " \t", 0, False])
def test_recovery_required_identity_cannot_be_invented(field: str, value: object) -> None:
    fields: dict[str, object] = dict(request_fields())
    fields[field] = value
    with pytest.raises(ValidationError):
        ReactionRecoveryWitnessRequest.model_validate(fields)


@pytest.mark.parametrize(
    ("field", "alias"),
    [("peer_job_id", "original_job_id"), ("recovery_link_key", "change_marker_key")],
)
def test_recovery_witness_roles_cannot_alias(field: str, alias: str) -> None:
    fields = request_fields()
    fields[field] = fields[alias]
    with pytest.raises(ValidationError):
        ReactionRecoveryWitnessRequest.model_validate(fields)


@pytest.mark.parametrize(
    "result",
    [{"coalesced_into": "peer"}, {"kind": "COALESCED", "coalesced_into": "peer"}],
)
def test_actual_retained_coalescing_results_preserve_wire_presence(
    result: dict[str, JsonValue],
) -> None:
    admitted = RetainedCoalescedReaction.model_validate(result)
    assert admitted.root.coalesced_into == "peer"
    assert admitted.model_dump(mode="json") == result
    assert RetainedCoalescedReaction.model_validate_json(admitted.model_dump_json()) == admitted


@pytest.mark.parametrize(
    "result",
    [
        {},
        {"coalesced_into": None},
        {"coalesced_into": ""},
        {"coalesced_into": "peer", "revision": 2},
        {"kind": "CAPTURED", "coalesced_into": "peer"},
        {"kind": None, "coalesced_into": "peer"},
        {"kind": "COALESCED", "coalesced_into": "peer", "applied": True},
    ],
)
def test_partial_or_mixed_results_cannot_authorize_recovery(
    result: dict[str, JsonValue],
) -> None:
    with pytest.raises(ValidationError):
        RetainedCoalescedReaction.model_validate(result)


def test_retained_result_validation_does_not_echo_payload_values() -> None:
    with pytest.raises(ValidationError) as failure:
        RetainedCoalescedReaction.model_validate({"coalesced_into": {"secret": "sensitive-value"}})
    assert "sensitive-value" not in str(failure.value)

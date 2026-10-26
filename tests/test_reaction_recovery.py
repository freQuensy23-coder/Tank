"""Mechanical witness admission before recovery eligibility or storage effects."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.reaction_recovery import (
    ReactionRecoveryEvidence,
    ReactionRecoveryWitnessRequest,
    RetainedCoalescedReaction,
)
from tank.reaction_recovery import recovery_eligibility
from tank.time_precision import Timestamp, collection_clock
from tank.util import sha256


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


def recovery_evidence_fields() -> dict[str, JsonValue]:
    source: dict[str, JsonValue] = {
        "backfill_id": "source",
        "status": "FAILED",
        "finished_at": "2026-10-20T12:00:00Z",
    }
    source_change = "CAP-" + sha256(source)
    command: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "POST_24H",
        "automatic_revision": True,
        "change_id": source_change + ":POST_24H",
        "reason": "Declared terminal capture failure",
    }
    anchor = Timestamp("2026-10-19T00:00:00Z", "EXACT")
    return {
        "original": {
            "job_id": "original",
            "status": "DONE",
            "command": command,
            "payload_revision": sha256(command),
            "result": {"coalesced_into": "peer"},
        },
        "peer": {
            "job_id": "peer",
            "status": "FAILED",
            "command": command | {"change_id": "different-source:POST_24H"},
        },
        "source": {
            "event_id": "event",
            "token_id": "token",
            "backfill_id": "source",
            "change_id": source_change,
            "status": "FAILED",
            "anchor": anchor.model_dump(mode="json"),
            "finished_at": collection_clock("2026-10-20T12:00:00Z").model_dump(mode="json"),
        },
        "checkpoint": {
            "receipt": {
                "kind": "CAPTURED",
                "checkpoint_key": "event:token:POST_24H:1",
                "event_id": "event",
                "token_id": "token",
                "checkpoint": "POST_24H",
                "revision": 1,
                "data_fingerprint": sha256({"declared_fixture": "checkpoint"}),
            },
            "immutable": True,
            "anchor": {"type": "T0", **anchor.fields("ts")},
            "captured_at": collection_clock("2026-10-20T11:00:00Z").model_dump(mode="json"),
        },
        "marker": {"kind": "ABSENT"},
    }


def test_recovery_needs_failed_peer_source_identity_and_older_bound_checkpoint() -> None:
    evidence = ReactionRecoveryEvidence.model_validate(recovery_evidence_fields())
    result = recovery_eligibility(evidence)
    assert result.kind == "ELIGIBLE"
    assert result.original_job_id == "original"
    assert result.peer_job_id == "peer"
    assert result.payload_revision == evidence.original.payload_revision
    assert result.command == evidence.original.command


@pytest.mark.parametrize(
    ("field", "reason"),
    [("peer", "PEER_ABSENT"), ("source", "SOURCE_ABSENT"), ("checkpoint", "CHECKPOINT_ABSENT")],
)
def test_missing_marker_and_failed_peer_alone_are_not_recovery_evidence(field: str, reason: str) -> None:
    fields = recovery_evidence_fields()
    fields[field] = None
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == reason


@pytest.mark.parametrize("captured_at", ["2026-10-20T12:00:00Z", "2026-10-20T12:00:01Z"])
def test_later_checkpoint_cannot_authorize_replaying_possibly_applied_work(captured_at: str) -> None:
    fields = recovery_evidence_fields()
    checkpoint = fields["checkpoint"]
    assert isinstance(checkpoint, dict)
    checkpoint["captured_at"] = collection_clock(captured_at).model_dump(mode="json")
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == "SOURCE_MAY_ALREADY_BE_CAPTURED"


def test_unbound_marker_is_not_missing_marker() -> None:
    fields = recovery_evidence_fields()
    fields["marker"] = {"kind": "UNRESOLVED"}
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == "MARKER_UNRESOLVED"

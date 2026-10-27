"""Pinned execution-status correlation and complete native barrier facts."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.session_completion import (
    CompletionSessionSnapshot,
    CompletionToolInput,
    NativeExecutionBarrier,
    NativeExecutionPage,
    NativeExecutionQuery,
    NativeExecutionState,
    execution_barrier,
)


@pytest.mark.parametrize("state", ["QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"])
def test_native_execution_state_preserves_actual_phase_and_settlement(state: str) -> None:
    outcome = state if state in {"COMPLETED", "FAILED", "CANCELLED"} else None
    admitted = NativeExecutionState.model_validate(
        {"execution_id": "input", "state": state, "settled_outcome": outcome}
    )
    assert admitted.state == state and admitted.settled_outcome == outcome
    wrong = None if outcome is not None else "COMPLETED"
    with pytest.raises(ValidationError):
        NativeExecutionState.model_validate(
            {"execution_id": "input", "state": state, "settled_outcome": wrong}
        )


@pytest.mark.parametrize("ids", [[], ["same", "same"], ["x" * 192], [str(i) for i in range(65)]])
def test_execution_query_requires_actual_unique_bounded_ids(ids: list[str]) -> None:
    with pytest.raises(ValidationError):
        NativeExecutionQuery(execution_ids=ids)


def test_execution_response_requires_exact_requested_order_and_count() -> None:
    query = NativeExecutionQuery(execution_ids=["first", "second"])
    rows = [
        NativeExecutionState(execution_id=identity, state="COMPLETED", settled_outcome="COMPLETED")
        for identity in query.execution_ids
    ]
    assert [row.execution_id for row in NativeExecutionPage(data=rows).correlate(query)] == [
        "first",
        "second",
    ]
    for wrong in (rows[::-1], rows[:1], [rows[0], rows[0]]):
        with pytest.raises(ValueError, match="requested identities"):
            NativeExecutionPage(data=wrong).correlate(query)
    with pytest.raises(ValidationError):
        NativeExecutionPage.model_validate({"data": [rows[0].model_dump()], "has_more": False})


def terminal_record() -> dict[str, JsonValue]:
    return {
        "session_id": "session",
        "message_log": [
            {"message_id": "owner", "remote_event_id": "input", "body": {"type": "user.message"}}
        ],
        "native_execution_receipts": {
            "idle": {
                "id": "idle",
                "type": "session.status_idle",
                "execution_id": "input",
                "stop_reason": {"type": "end_turn"},
            }
        },
        "native_execution_states": {
            "input": {"execution_id": "input", "state": "COMPLETED", "settled_outcome": "COMPLETED"}
        },
    }


def test_barrier_uses_terminal_facts_without_unused_retry_or_body_and_keeps_wire() -> None:
    record = terminal_record()
    snapshot = CompletionSessionSnapshot.from_record(record)
    result = execution_barrier(snapshot, [])
    assert result.complete and snapshot.journal[0].retry_attempt is None
    assert result.storage_record() == {
        "complete": True,
        "accepted_input_ids": ["input"],
        "pending_input_ids": [],
        "failed_input_ids": [],
        "unidentified_inputs": [],
        "pending_action_ids": [],
        "terminal_receipts": record["native_execution_receipts"],
        "execution_states": record["native_execution_states"],
    }
    deferred = result.incomplete()
    assert deferred.complete is False and deferred.accepted_input_ids == ["input"]
    assert deferred.storage_record()["terminal_receipts"] == record["native_execution_receipts"]


def test_barrier_refuses_foreign_tool_and_mutated_false_completion() -> None:
    snapshot = CompletionSessionSnapshot.from_record(terminal_record())
    foreign = CompletionToolInput(
        session_id="foreign", call_id="call", input_id=None, message_id=None, attempt=None, returned_at=None
    )
    with pytest.raises(ValueError, match="another session"):
        execution_barrier(snapshot, [foreign])
    result = execution_barrier(snapshot, [])
    corrupt = result.model_copy(update={"accepted_input_ids": []})
    with pytest.raises(ValidationError):
        corrupt.storage_record()
    with pytest.raises(ValidationError):
        NativeExecutionBarrier.model_validate(result.model_copy(update={"complete": 1}))


@pytest.mark.parametrize("clock", ["", "2026-10-26T00:00:00", "bad"])
def test_reported_receipt_and_return_clocks_require_actual_aware_occurrences(clock: str) -> None:
    record = terminal_record()
    record["native_execution_receipts"] = {
        "idle": {"id": "idle", "type": "session.status_idle", "execution_id": "input", "processed_at": clock}
    }
    with pytest.raises(ValidationError):
        CompletionSessionSnapshot.from_record(record)
    with pytest.raises(ValidationError):
        CompletionToolInput(
            session_id="session",
            call_id="call",
            input_id=None,
            message_id=None,
            attempt=None,
            returned_at=clock,
        )


@pytest.mark.parametrize("malformed", ["STATE", "IDENTITY", "SELF_REPLACEMENT"])
def test_reported_supersession_facts_cannot_contradict_native_scope(malformed: str) -> None:
    record = terminal_record()
    state: dict[str, JsonValue] = {"execution_id": "old", "state": "FAILED", "settled_outcome": "FAILED"}
    replacement = "input"
    if malformed == "STATE":
        state.update(state="COMPLETED", settled_outcome="COMPLETED")
    elif malformed == "IDENTITY":
        state["execution_id"] = "foreign"
    else:
        replacement = "old"
    record["superseded_native_inputs"] = {
        "old": {
            "reason": "FAILED_PRIOR_TURN_ATTEMPT",
            "queue_state": state,
            "replacement_input_id": replacement,
            "message_id": "owner",
        }
    }
    with pytest.raises(ValidationError):
        CompletionSessionSnapshot.from_record(record)


@pytest.mark.parametrize("journal", [None, [], [{"message_id": "old-owner", "remote_event_id": "old"}]])
def test_nonnull_completion_owner_requires_its_actual_journal_row(
    journal: list[dict[str, str]] | None,
) -> None:
    record: dict[str, JsonValue] = {"session_id": "session", "active_message_id": "missing-owner"}
    if journal is not None:
        rows: list[JsonValue] = [dict(row) for row in journal]
        record["message_log"] = rows
    with pytest.raises(ValidationError, match="active owner"):
        CompletionSessionSnapshot.from_record(record)


def test_host_observation_is_scoped_new_provenance_not_a_historical_default() -> None:
    from agent_source_fixtures import declared_versions_fixture

    from tank.agents.context import clean_value
    from tank.contracts.session_completion import HostExecutionObservation
    from tank.time_precision import Timestamp

    record = terminal_record()
    assert CompletionSessionSnapshot.from_record(record).execution_observation is None
    assert "native_execution_observation" not in record
    receipt = HostExecutionObservation(
        source="HOST_MANAGED_EXECUTION_STATUS",
        observation_id="host-read",
        session_id="session",
        execution_ids=["input"],
        recorded_at=Timestamp("2026-10-27T00:00:00Z", "COLLECTION_ONLY"),
        versions=declared_versions_fixture(),
    )
    record["native_execution_observation"] = receipt.model_dump(mode="json")
    assert CompletionSessionSnapshot.from_record(record).execution_observation == receipt
    assert clean_value(
        {"native_execution_observation": receipt.model_dump(mode="json"), "text": "Public fact"}
    ) == {"text": "Public fact"}
    for corrupted in (
        None,
        {**receipt.model_dump(mode="json"), "session_id": "foreign"},
        {**receipt.model_dump(mode="json"), "execution_ids": ["unobserved"]},
    ):
        record["native_execution_observation"] = corrupted
        with pytest.raises((ValidationError, ValueError)):
            CompletionSessionSnapshot.from_record(record)

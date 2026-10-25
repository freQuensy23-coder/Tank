"""Only a retry-ready phase consumes a retained input body or prior acceptance."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.native_retry import (
    ExhaustedRetryState,
    JournalRetryOwner,
    OriginRetryOwner,
    ReadyRetryState,
    UnknownRetryState,
    retry_state,
)


@pytest.mark.parametrize("count", [1, 2, 17])
@pytest.mark.parametrize(
    "body",
    [None, {"type": "user.message"}, "opaque retained body", {"unrelated": [False, 0, None]}],
)
def test_exhausted_phase_consumes_only_actual_owner_and_count(count: int, body: JsonValue) -> None:
    active: dict[str, JsonValue] = {
        "message_id": "message",
        "retry_count": count,
        "body": body,
        "remote_event_id": {"unreported": "historical-input"},
    }
    state = retry_state({"session_id": "session"}, active)
    assert state == ExhaustedRetryState(
        kind="EXHAUSTED", owner=JournalRetryOwner(kind="JOURNAL", message_id="message"), attempt=count
    )


def test_exhausted_origin_does_not_require_a_stored_body_or_prior_input() -> None:
    assert retry_state({"session_id": "session", "retry_count": 1}, None) == ExhaustedRetryState(
        kind="EXHAUSTED", owner=OriginRetryOwner(kind="ORIGIN", session_id="session"), attempt=1
    )


def test_ready_phase_preserves_original_body_and_known_zero() -> None:
    body: dict[str, JsonValue] = {
        "type": "user.message",
        "content": [{"type": "text", "text": "Original actual input"}],
    }
    active: dict[str, JsonValue] = {
        "message_id": "message",
        "retry_count": 0,
        "body": body,
        "remote_event_id": "prior",
    }
    state = retry_state({"session_id": "session"}, active)
    assert isinstance(state, ReadyRetryState) and state.attempt == 0
    assert state.body is not None and state.body.model_dump(mode="json") == body
    assert state.previous_input_id == "prior"


@pytest.mark.parametrize("body", [{"type": "user.message"}, "opaque", {"unrelated": []}])
def test_ready_phase_never_relaxes_its_required_input_contract(body: JsonValue) -> None:
    with pytest.raises(ValidationError):
        retry_state(
            {"session_id": "session"},
            {"message_id": "message", "retry_count": 0, "body": body},
        )


def test_ready_missing_body_remains_explicitly_unreported() -> None:
    state = retry_state({"session_id": "session"}, {"message_id": "message", "retry_count": 0})
    assert isinstance(state, ReadyRetryState) and state.body is None


@pytest.mark.parametrize("count", [True, -1, "1", 1.0])
def test_invalid_counts_cannot_become_exhaustion_evidence(count: JsonValue) -> None:
    with pytest.raises(ValidationError):
        retry_state({"session_id": "session"}, {"message_id": "message", "retry_count": count})


def test_unknown_count_remains_unknown_without_consuming_malformed_body() -> None:
    state = retry_state(
        {"session_id": "session"}, {"message_id": "message", "body": {"type": "user.message"}}
    )
    assert state == UnknownRetryState(kind="UNKNOWN", reason="RETRY_COUNT_NOT_RETAINED")


def test_unknown_journal_owner_does_not_fall_back_to_an_exhausted_origin() -> None:
    state = retry_state(
        {"session_id": "session", "retry_count": 1, "message_log": [{"message_id": "unowned"}]},
        None,
    )
    assert state == UnknownRetryState(kind="UNKNOWN", reason="CURRENT_INPUT_OWNER_NOT_RETAINED")

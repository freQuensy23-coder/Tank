"""Message results preserve locally pending and observed acceptance phases."""

import pytest
from pydantic import JsonValue

from tank.agents.message_dispatch import (
    AcceptedMessageDispatch,
    IndexedMessageDispatch,
    PendingMessageDispatch,
    observe_message_dispatch,
)


def message_record() -> dict[str, JsonValue]:
    return {
        "message_id": "message",
        "retry_count": 0,
        "turn_status": "ACCEPTING",
        "body": {"type": "user.message", "content": [{"type": "text", "text": "Question"}]},
        "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "question"},
    }


def test_local_intent_is_pending_and_index_only_is_explicitly_unreported() -> None:
    journal: list[JsonValue] = [message_record()]
    pending = observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")
    assert isinstance(pending, PendingMessageDispatch) and pending.intent.attempt == 0
    index: list[JsonValue] = ["message"]
    indexed = observe_message_dispatch({"session_id": "session", "messages": index}, "message")
    assert isinstance(indexed, IndexedMessageDispatch)
    assert indexed.phase == "INDEXED_UNREPORTED"


@pytest.mark.parametrize("status", ["UNREPORTED", "EMPTY"])
def test_explicit_acceptance_does_not_require_unused_body_or_historical_budget(status: str) -> None:
    journal: list[JsonValue] = [
        {
            "message_id": "message",
            "initial_input_acceptance": {"status": status},
            "body": "Unconsumed historical text",
            "remote_event_id": None,
        }
    ]
    result = observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")
    assert isinstance(result, AcceptedMessageDispatch)
    assert result.acceptance.receipt.status == status and result.acceptance.sent_at is None


def test_unproved_or_contradictory_acceptance_is_not_reported_as_dispatched() -> None:
    with pytest.raises(ValueError, match="no actual"):
        observe_message_dispatch({"session_id": "session"}, "message")
    row = message_record()
    del row["retry_count"]
    journal: list[JsonValue] = [row]
    with pytest.raises(KeyError, match="retry_count"):
        observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")
    row.update(initial_input_acceptance={"status": "ACCEPTED", "event_id": "native"}, remote_event_id="other")
    with pytest.raises(ValueError, match="contradicts"):
        observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")

"""Input binding preserves callback source isolation and validates journal ownership."""

import pytest
from pydantic import JsonValue

from tank.agents.context_binding import ContextBinding


@pytest.mark.parametrize("active", [False, True])
def test_callback_without_source_cannot_borrow_session_origin(active: bool) -> None:
    payload: dict[str, JsonValue] = {"kind": "CONDITION_TRIGGERED", "event_id": "current"}
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "event_id": "current",
        "item_id": "original",
        "last_payload": payload,
    }
    if active:
        session["active_message_id"] = "callback"
        session["message_log"] = [{"message_id": "callback", "payload": payload}]
    binding = ContextBinding.from_session(session)
    assert binding.observation_source() is None
    assert binding.incoming_source() == "original"


def test_active_source_owns_observation_after_an_earlier_origin() -> None:
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "item_id": "original",
        "active_message_id": "incoming",
        "message_log": [
            {"message_id": "earlier", "payload": {"kind": "NEW_ITEM", "item_id": "original"}},
            {"message_id": "incoming", "payload": {"kind": "ITEM_ADDED", "item_id": "incoming"}},
        ],
    }
    binding = ContextBinding.from_session(session)
    assert binding.observation_source() == binding.incoming_source() == "incoming"
    session["message_log"] = []
    assert binding.observation_source() == "incoming"


def test_unaccepted_session_without_input_journal_has_no_active_owner() -> None:
    binding = ContextBinding.from_session({"session_id": "new", "item_id": None, "event_id": None})
    assert binding.active_message_id is None
    assert binding.trigger.active_message is False
    assert binding.observation_source() is binding.incoming_source() is None


def test_legacy_source_without_journal_retains_its_explicit_origin() -> None:
    binding = ContextBinding.from_session({"session_id": "legacy", "item_id": "explicit-origin"})
    assert binding.observation_source() == "explicit-origin"


@pytest.mark.parametrize(
    "defect",
    [
        {"active_message_id": 1},
        {"event_id": True},
        {"message_log": None},
        {"message_log": [{"message_id": ""}]},
        {"message_log": [{"message_id": "x", "payload": {"item_id": True}}], "active_message_id": "x"},
        {"message_log": [{"message_id": "x", "payload": []}], "active_message_id": "x"},
        {"message_log": [{"message_id": "x"}, {"message_id": "x"}], "active_message_id": "x"},
    ],
)
def test_malformed_scope_or_input_owner_is_refused(defect: dict[str, JsonValue]) -> None:
    session: dict[str, JsonValue] = {"session_id": "session", **defect}
    with pytest.raises(ValueError):
        ContextBinding.from_session(session)

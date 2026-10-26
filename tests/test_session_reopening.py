"""Question reopening changes only the actual controller's consumed session facts."""

import copy

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.session_reopening import reopen_question_session
from tank.runtime_contracts import UserQuestionMessage
from tank.time_precision import Timestamp, collection_clock
from tank.util import canonical_json


def failed_session() -> dict[str, JsonValue]:
    journal: list[JsonValue] = [
        {
            "message_id": "old",
            "payload": {"kind": "USER_QUESTION"},
            "turn_status": "PENDING",
            "body": "Opaque old input",
        },
        {"message_id": "done", "payload": {"kind": "USER_QUESTION"}, "turn_status": "DONE"},
        {"message_id": "current", "payload": {"kind": "USER_QUESTION"}, "turn_status": "ACCEPTING"},
    ]
    return {
        "session_id": "session",
        "status": "FAILED",
        "message_log": journal,
        "unrelated": {"known_zero": 0, "unknown": None},
    }


def test_reopening_retires_actual_unfinished_owners_and_preserves_completed_current_and_opaque_facts() -> (
    None
):
    session = failed_session()
    original = copy.deepcopy(session)
    unrelated = canonical_json(session["unrelated"])
    at = collection_clock()
    reopen_question_session(
        session,
        UserQuestionMessage(kind="USER_QUESTION", session_id="session", question_id="question"),
        "current",
        at,
    )
    assert session["retired_message_ids"] == ["old"]
    assert session["status"] == "OPEN" and session["runtime_status"] == "IDLE"
    journal = session["message_log"]
    old_journal = original["message_log"]
    assert isinstance(journal, list) and isinstance(old_journal, list)
    assert isinstance(journal[0], dict) and isinstance(old_journal[0], dict)
    assert journal[0] == {**old_journal[0], "turn_status": "FAILED"}
    assert journal[1:] == old_journal[1:]
    assert canonical_json(session["unrelated"]) == unrelated
    assert session["reopened"] == [
        {"question_id": "question", "previous_status": "FAILED", **at.fields("at")}
    ]


@pytest.mark.parametrize("invalid", ["foreign", "clock", "duplicate", "null_index"])
def test_invalid_reopening_refuses_before_mutating_observed_session(invalid: str) -> None:
    session = failed_session()
    target = "other" if invalid == "foreign" else "session"
    at = Timestamp(None, None, "No observed occurrence") if invalid == "clock" else collection_clock()
    if invalid == "duplicate":
        journal = session["message_log"]
        assert isinstance(journal, list)
        journal.append(copy.deepcopy(journal[0]))
    if invalid == "null_index":
        session["retired_message_ids"] = None
    original = copy.deepcopy(session)
    with pytest.raises((ValueError, ValidationError)):
        reopen_question_session(
            session,
            UserQuestionMessage(kind="USER_QUESTION", session_id=target, question_id="question"),
            "current",
            at,
        )
    assert session == original

"""Phase-specific archive admission and actual activity-clock policy."""

from datetime import timedelta

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.session_retention import (
    ArchiveDenied,
    PendingArchiveJob,
    PendingArchiveTool,
    QuestionActivity,
    RetainedSession,
    RetentionPolicy,
    SessionActivity,
    SessionSweepEntry,
    TerminalArchiveCandidate,
    archive_candidate,
    archive_tool,
    latest_activity,
)
from tank.util import parse_time


@pytest.mark.parametrize(
    "session,event,reason",
    [
        ({}, {"lifecycle": "MONITORING"}, "EVENT_NOT_TERMINAL"),
        ({}, {"status": "CLOSED"}, "EVENT_NOT_TERMINAL"),
        ({"runtime_status": "RUNNING"}, {"lifecycle": "CLOSED"}, "SESSION_NOT_IDLE"),
        ({}, {"lifecycle": "INVALIDATED"}, "SESSION_NOT_IDLE"),
    ],
)
def test_archive_denial_requires_no_unconsumed_identity_or_publication_history(
    session: dict[str, JsonValue], event: dict[str, JsonValue], reason: str
) -> None:
    result = archive_candidate(session, event)
    assert isinstance(result, ArchiveDenied) and result.reason == reason


def test_closed_archive_requires_only_its_consumed_scope_not_denial_metadata() -> None:
    result = archive_candidate(
        {"session_id": "session", "event_id": "event", "runtime_status": "IDLE"},
        {"event_id": "event", "lifecycle": "CLOSED", "session_finalized_at": {"unused": "history"}},
    )
    assert isinstance(result, TerminalArchiveCandidate) and result.event_id == "event"
    with pytest.raises(ValueError):
        archive_candidate(
            {"session_id": "session", "event_id": "other-event", "runtime_status": "IDLE"},
            {"event_id": "event", "lifecycle": "CLOSED"},
        )


def test_pending_tool_denial_does_not_require_an_unused_historical_return_clock() -> None:
    result = archive_tool(
        {"session_id": "session", "status": "PENDING", "returned_at": {"unused": "history"}}, "session"
    )
    assert isinstance(result, PendingArchiveTool) and result.pending
    assert archive_tool({"session_id": "session", "status": "DONE"}, "session").pending
    with pytest.raises(ValueError):
        archive_tool({"session_id": "other-session", "status": "PENDING"}, "session")


@pytest.mark.parametrize("precision", [None, "BUCKET(1s)"])
def test_latest_activity_and_fixed_retention_preserve_actual_clock_uncertainty(
    precision: str | None,
) -> None:
    record: dict[str, JsonValue] = {
        "session_id": "session",
        "opened_at": "2026-10-01T00:00:00Z",
        "message_log": [{"requested_at": "2026-10-02T00:00:00Z"}],
    }
    question: dict[str, JsonValue] = {"session_id": "session", "asked_at": "2026-10-03T00:00:00Z"}
    if precision is not None:
        question.update(asked_at_prec=precision, asked_at_bucket_anchor="END")
    selected = latest_activity(
        SessionActivity.from_record(record), [QuestionActivity.from_record(question, "session")]
    )
    due = RetentionPolicy(keep_days=7).deadline(selected)
    assert selected.value == question["asked_at"]
    assert parse_time(due.value) == parse_time(selected.value) + timedelta(days=7)
    assert due.precision == selected.precision == precision
    assert due.bucket_anchor == selected.bucket_anchor == ("END" if precision is not None else None)
    assert due.precision_reason == selected.precision_reason == (None if precision else "INCOMPLETE")
    assert "opened_at_prec" not in record and "requested_at_prec" not in record


def test_unreported_activity_cannot_become_current_time_or_retention_authority() -> None:
    session = SessionActivity.from_record({"session_id": "session", "message_log": []})
    with pytest.raises(ValueError, match="no recorded message time"):
        latest_activity(session, [])
    with pytest.raises(ValueError):
        QuestionActivity.from_record(
            {"session_id": "other-session", "asked_at": "2026-10-03T00:00:00Z"}, "session"
        )


@pytest.mark.parametrize("clock", ["", "2026-10-03T00:00:00", "not-a-clock", True])
def test_reported_invalid_activity_is_not_silently_treated_as_absent(clock: JsonValue) -> None:
    with pytest.raises((ValueError, TypeError, ValidationError)):
        SessionActivity.from_record({"session_id": "session", "last_event_at": clock})


@pytest.mark.parametrize("days", [0, -1, True, 1.5])
def test_retention_policy_requires_its_actual_positive_integer_days(days: JsonValue) -> None:
    with pytest.raises(ValidationError):
        RetentionPolicy.model_validate({"keep_days": days})


def test_pending_jobs_keep_event_session_and_publication_scope_separate() -> None:
    job = PendingArchiveJob.from_record({"status": "RUNNING", "payload": {"alert_id": "actual-alert"}})
    assert job.blocks(session_id="session", event_id="event", alert_ids={"actual-alert"})
    assert not job.blocks(session_id="session", event_id="event", alert_ids={"other-alert"})
    unbound = PendingArchiveJob.from_record({"status": "QUEUED", "payload": {"token_ids": ["T"]}})
    assert not unbound.blocks(session_id="session", event_id="event", alert_ids=set())


def test_sweep_admits_only_actual_idle_retention_without_guessed_runtime_state() -> None:
    now = parse_time("2026-10-27T00:00:00Z")
    record: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "IDLE",
        "archive_after": "2026-10-28T00:00:00Z",
    }
    entry = SessionSweepEntry.from_record(record)
    assert not entry.should_probe(following=False, periodic_probe=True, now=now)
    assert entry.should_probe(following=True, periodic_probe=True, now=now)
    record["runtime_status"] = None
    assert SessionSweepEntry.from_record(record).should_probe(following=False, periodic_probe=False, now=now)


def test_unreported_unbound_and_bound_event_scope_cannot_be_interchanged() -> None:
    record: dict[str, JsonValue] = {"session_id": "session", "no_event": True}
    assert not RetainedSession.from_record(record).known_eventless
    record["event_id"] = None
    assert RetainedSession.from_record(record).known_eventless
    record["event_id"] = "actual-event"
    state = RetainedSession.from_record(record)
    assert not state.known_eventless and state.bound_event_id == "actual-event"

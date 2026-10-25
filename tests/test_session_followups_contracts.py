"""Pure eligibility preserves actual lifecycle precedence and historical unknowns."""

import copy

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.session_followups import (
    Archived,
    FollowupCandidate,
    FollowupRefusalTarget,
    FollowupSession,
    NoSession,
    RetentionArchived,
    Terminated,
    followup_eligibility,
)
from tank.util import canonical_json


def session(**facts: JsonValue) -> FollowupSession:
    return FollowupSession.from_record({"session_id": "session", "status": "OPEN", **facts})


def test_absent_and_replay_have_distinct_evidence() -> None:
    assert followup_eligibility(None) == NoSession(status="NO_SESSION", reason="ABSENT")
    assert followup_eligibility(session(replay_run_id="replay")) == NoSession(
        status="NO_SESSION", reason="REPLAY"
    )


def test_recorded_termination_has_precedence_over_actual_missing_resource() -> None:
    facts = session(
        status="ENDED",
        termination_receipt={"event_id": "actual-termination"},
        remote_state={"kind": "MISSING", "missing": True, "session_id": "session"},
    )
    assert followup_eligibility(facts) == Terminated(status="TERMINATED", reason="TERMINATED")


def test_missing_resource_is_not_native_termination() -> None:
    facts = session(remote_state={"kind": "MISSING", "missing": True, "session_id": "session"})
    assert facts.termination is None and facts.status == "OPEN"
    assert followup_eligibility(facts) == NoSession(status="NO_SESSION", reason="NATIVE_MISSING")


@pytest.mark.parametrize("status", ["OPEN", "FAILED"])
def test_actual_local_turn_state_only_allows_native_preflight(status: str) -> None:
    assert followup_eligibility(session(status=status)) == FollowupCandidate(status="CANDIDATE")


def test_unknown_historical_facts_are_never_terminal_evidence_or_rewritten() -> None:
    row: dict[str, JsonValue] = {
        "session_id": "session",
        "remote_state": {"id": "session", "unrelated_native_extension": {"count": 0}},
        "unrelated_controller_extension": {"facts": [False, 0, None, "raw"]},
    }
    before = canonical_json(copy.deepcopy(row))
    facts = FollowupSession.from_record(row)
    assert facts.status is None and facts.termination is None
    assert followup_eligibility(facts) == FollowupCandidate(status="CANDIDATE")
    assert canonical_json(row) == before


@pytest.mark.parametrize("days", [1, 7])
def test_retention_requires_actual_reported_duration(days: int) -> None:
    assert followup_eligibility(
        session(status="ENDED", archive_reason="RETENTION", archive_keep_days=days)
    ) == RetentionArchived(status="ARCHIVED", reason="RETENTION", keep_days=days)


def test_known_retention_with_unreported_duration_does_not_invent_days() -> None:
    assert followup_eligibility(session(status="ENDED", archive_reason="RETENTION")) == Archived(
        status="ARCHIVED", reason="RETENTION_UNREPORTED"
    )


def test_reasonless_ended_history_preserves_early_closure_policy() -> None:
    assert followup_eligibility(session(status="ENDED")) == Archived(
        status="ARCHIVED", reason="EARLY_CLOSURE"
    )


def test_reported_native_archive_is_closed_without_relabeling_local_state() -> None:
    facts = session(
        remote_state={
            "kind": "FOUND",
            "session_id": "session",
            "status": "idle",
            "archived_at": "2026-10-25T02:00:00+02:00",
        }
    )
    assert facts.status == "OPEN"
    assert followup_eligibility(facts) == Archived(status="ARCHIVED", reason="ARCHIVED")


def test_actual_legacy_native_termination_does_not_require_invented_archive_clock() -> None:
    facts = session(remote_state={"id": "session", "status": "terminated"})
    assert followup_eligibility(facts) == Terminated(status="TERMINATED", reason="TERMINATED")


@pytest.mark.parametrize(
    "remote",
    [
        {"kind": "MISSING", "missing": True, "session_id": "foreign"},
        {"kind": "MISSING", "missing": 1, "session_id": "session"},
        {"id": "foreign", "status": "terminated"},
        {"id": "foreign", "session_id": "session"},
        {"kind": "FOUND", "session_id": "session", "status": "idle", "archived_at": ""},
    ],
)
def test_contradictory_or_malformed_remote_evidence_refuses(remote: JsonValue) -> None:
    with pytest.raises((ValueError, ValidationError)):
        session(remote_state=remote)


@pytest.mark.parametrize("days", [True, 0, 1.0, "7"])
def test_retention_period_cannot_coerce_unknown_or_invalid_counts(days: JsonValue) -> None:
    with pytest.raises(ValidationError):
        session(status="ENDED", archive_reason="RETENTION", archive_keep_days=days)


def test_unidentified_termination_payload_cannot_become_a_terminal_reason() -> None:
    with pytest.raises(KeyError):
        session(termination_receipt={"reason": "unowned"})


def test_checked_at_metadata_is_projection_only_and_preserves_actual_missing_identity() -> None:
    facts = session(
        remote_state={
            "kind": "MISSING",
            "missing": True,
            "session_id": "session",
            "checked_at": "2026-10-25T00:00:00Z",
            "checked_at_prec": "COLLECTION_ONLY",
            "checked_at_prec_reason": None,
        }
    )
    assert followup_eligibility(facts) == NoSession(status="NO_SESSION", reason="NATIVE_MISSING")


@pytest.mark.parametrize(
    "target",
    [
        {"question_id": "", "session_id": "session"},
        {"question_id": "question", "session_id": ""},
        {"question_id": " ", "session_id": "session"},
        {"question_id": "question"},
        {"session_id": "session"},
    ],
)
def test_refusal_requires_actual_question_and_session_identity(target: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        FollowupRefusalTarget.model_validate(target)


def test_refusal_target_preserves_both_actual_identities() -> None:
    target = FollowupRefusalTarget(question_id="question", session_id="session")
    assert target.model_dump(mode="json") == {"question_id": "question", "session_id": "session"}

"""Host replay abort bookkeeping consumes only actual ownership and message phases."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.session_completion import (
    HostReplayAbortReceipt,
    ReplayAbortFailure,
    ReplayAbortFinalized,
    ReplayAbortJournal,
    ReplayAbortOwnership,
    ReplayAbortRefused,
    ReplayAbortTarget,
)
from tank.time_precision import collection_clock


@pytest.mark.parametrize("run", [None, "different-run", "actual-run"])
def test_abort_ownership_requires_the_actual_session_and_reported_run(run: str | None) -> None:
    target = ReplayAbortTarget(session_id="session", replay_run_id=run)
    actual = ReplayAbortOwnership.from_record(
        {"session_id": "session", "replay_run_id": "actual-run"}, "session"
    )
    assert actual.matches(target) is (run == "actual-run")
    with pytest.raises(ValueError):
        ReplayAbortOwnership.from_record({"session_id": "foreign", "replay_run_id": run}, "session")


@pytest.mark.parametrize("reported", [False, True])
def test_unreported_journal_and_actual_empty_journal_remain_distinct(reported: bool) -> None:
    record: dict[str, JsonValue] = {"active_message_id": None}
    if reported:
        record["message_log"] = []
    journal = ReplayAbortJournal.from_record(record)
    assert journal.messages == (() if reported else None)
    assert ("message_log" in record) is reported


def test_abort_message_phase_needs_no_unused_body_counter_or_native_acceptance() -> None:
    journal = ReplayAbortJournal.from_record(
        {
            "active_message_id": "pending",
            "message_log": [
                {"message_id": "done", "turn_status": "DONE"},
                {"message_id": "failed", "turn_status": "FAILED"},
                {"message_id": "pending", "turn_status": "ACCEPTING"},
                {"message_id": "unreported"},
            ],
        }
    )
    assert journal.messages is not None
    assert [entry.message_id for entry in journal.messages if entry.unfinished] == ["pending", "unreported"]


@pytest.mark.parametrize(
    "record",
    [
        {"active_message_id": "missing", "message_log": []},
        {"active_message_id": "missing"},
        {"message_log": None},
        {"message_log": [{"message_id": "duplicate"}, {"message_id": "duplicate"}]},
    ],
)
def test_abort_cannot_hide_an_inconsistent_owner_or_journal(record: dict[str, JsonValue]) -> None:
    with pytest.raises((ValueError, ValidationError)):
        ReplayAbortJournal.from_record(record)


def test_actual_empty_exception_text_stays_empty_and_receipt_is_host_not_native() -> None:
    failure = ReplayAbortFailure(error_type="RuntimeError", reason=str(RuntimeError()))
    receipt = HostReplayAbortReceipt(
        error_type=failure.error_type,
        reason=failure.reason,
        replay_run_id="run",
        recorded_at=collection_clock(),
    ).storage_record()
    assert receipt["reason"] == "" and receipt["origin"] == "HOST_REPLAY_ABORT"
    assert "execution_id" not in receipt and "type" not in receipt and "retry_count" not in receipt


@pytest.mark.parametrize("incorrect", [0, 1, 0.0, 1.0, "false", "true"])
def test_abort_outcome_flags_do_not_coerce_wire_numbers(incorrect: JsonValue) -> None:
    with pytest.raises(ValidationError):
        ReplayAbortRefused.model_validate({"session_id": "session", "finalized": incorrect})
    with pytest.raises(ValidationError):
        ReplayAbortFinalized.model_validate(
            {"session_id": "session", "finalized": incorrect, "failed_message_ids": []}
        )

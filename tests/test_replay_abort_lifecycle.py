"""Actual host replay abort ownership, persistence and unchanged native evidence."""

import copy
from pathlib import Path

import pytest
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture
from test_native_execution_barrier import seed_execution_inputs
from test_session_completion_lineage import completion_repo as completion_repo

from tank.contracts.session_completion import (
    ReplayAbortFailure,
    ReplayAbortFinalized,
    ReplayAbortRefused,
    ReplayAbortTarget,
)
from tank.storage import StorageBackend


async def seed_abort_session(repo: StorageBackend) -> dict[str, JsonValue]:
    session = await seed_execution_inputs(repo, 1)
    session["replay_run_id"] = "owned-run"
    session["terminal_failure"] = {
        "event_id": "actual-native-failure",
        "error": "runtime_failed",
        "reason": "Actual earlier native failure",
    }
    session["message_log"] = [
        {"message_id": "done", "turn_status": "DONE", "source_completion_verified": {"outcome": "DONE"}},
        {"message_id": "failed", "turn_status": "FAILED", "terminal_failure": {"reason": "Native failure"}},
        {"message_id": "pending", "turn_status": "ACCEPTING", "body": {"type": "user.message"}},
        {"message_id": "unreported"},
    ]
    session["active_message_id"] = "pending"
    await repo.put("ops.agent_sessions", "session", session)
    return session


async def test_actual_owned_abort_preserves_terminal_native_and_completed_message_facts(
    tmp_path: Path, completion_repo: StorageBackend
) -> None:
    repo = completion_repo
    original = await seed_abort_session(repo)
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        outcome = await runtime.sessions.finalize_replay_abort(
            ReplayAbortTarget(session_id="session", replay_run_id="owned-run"),
            ReplayAbortFailure(error_type="RuntimeError", reason=str(RuntimeError())),
        )
    assert isinstance(outcome, ReplayAbortFinalized)
    assert outcome.failed_message_ids == ("pending", "unreported")
    assert outcome.model_dump(mode="json")["failed_message_ids"] == ["pending", "unreported"]
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None
    assert current["terminal_failure"] == original["terminal_failure"]
    assert current["native_execution_receipts"] == original["native_execution_receipts"]
    assert current["status"] == "FAILED" and current["runtime_status"] == "IDLE"
    assert current["active_message_id"] is None and current["pending_retry_event"] is None
    journal, old = current["message_log"], original["message_log"]
    assert isinstance(journal, list) and isinstance(old, list)
    assert journal[:2] == old[:2]
    for row in journal[2:]:
        assert isinstance(row, dict) and row["turn_status"] == "FAILED" and "retry_count" not in row
        receipt = row["replay_abort"]
        assert isinstance(receipt, dict)
        assert receipt["origin"] == "HOST_REPLAY_ABORT" and receipt["reason"] == ""
        assert "execution_id" not in receipt and "type" not in receipt
    assert await repo.list("ops.jobs") == []


@pytest.mark.parametrize("run", [None, "foreign-run"])
async def test_actual_unowned_abort_needs_no_failure_or_unused_journal_body(
    tmp_path: Path, completion_repo: StorageBackend, run: str | None
) -> None:
    repo = completion_repo
    original = await seed_abort_session(repo)
    original["message_log"] = {"unused": "invalid for an owned operation"}
    await repo.put("ops.agent_sessions", "session", original)
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        outcome = await runtime.sessions.finalize_replay_abort(
            ReplayAbortTarget(session_id="session", replay_run_id=run), None
        )
    assert isinstance(outcome, ReplayAbortRefused) and outcome.finalized is False
    assert await repo.get("ops.agent_sessions", "session") == original
    assert await repo.list("ops.jobs") == []


@pytest.mark.parametrize("invalid", ["failure", "owner", "duplicate", "journal_null"])
async def test_owned_abort_missing_required_facts_refuses_before_any_persisted_mutation(
    tmp_path: Path, completion_repo: StorageBackend, invalid: str
) -> None:
    repo = completion_repo
    original = await seed_abort_session(repo)
    if invalid == "owner":
        original["active_message_id"] = "missing"
    elif invalid == "duplicate":
        journal = original["message_log"]
        assert isinstance(journal, list)
        journal.append(copy.deepcopy(journal[0]))
    elif invalid == "journal_null":
        original["message_log"] = None
    await repo.put("ops.agent_sessions", "session", original)
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        with pytest.raises(ValueError):
            await runtime.sessions.finalize_replay_abort(
                ReplayAbortTarget(session_id="session", replay_run_id="owned-run"),
                None
                if invalid == "failure"
                else ReplayAbortFailure(error_type="RuntimeError", reason="failure"),
            )
    assert await repo.get("ops.agent_sessions", "session") == original
    assert await repo.list("ops.jobs") == []

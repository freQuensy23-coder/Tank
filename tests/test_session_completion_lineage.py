"""Completion diagnostics and failed-input retirement preserve actual retry provenance."""

from pathlib import Path

import pytest
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture

from tank.storage import MemoryRepository


@pytest.mark.parametrize("attempt", [None, 0, 1])
async def test_source_completion_preserves_unknown_and_known_retry_attempts(
    tmp_path: Path, attempt: int | None
) -> None:
    repo = MemoryRepository()
    message: dict[str, JsonValue] = {
        "message_id": "owner",
        "payload": {"kind": "NEW_ITEM", "item_id": "item"},
        "remote_event_id": "accepted",
    }
    if attempt is not None:
        message["retry_count"] = attempt
    session: dict[str, JsonValue] = {"session_id": "session", "message_log": [message]}
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        outcomes = await runtime.sessions.source_completion_snapshot(session)
    assert outcomes["owner"]["retry_attempt"] == attempt
    assert outcomes["owner"]["native_input_id"] == "accepted"
    assert outcomes["owner"]["outcome"] == "MISSING_SOURCE_ITEM"
    assert ("retry_count" in message) is (attempt is not None)


@pytest.mark.parametrize("tool_attempt", [None, 0])
async def test_failed_tool_input_requires_observed_older_attempt_before_supersession(
    tmp_path: Path, tool_attempt: int | None
) -> None:
    repo = MemoryRepository()
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "owner",
        "message_log": [
            {
                "message_id": "owner",
                "payload": {"kind": "NEW_ITEM", "item_id": "item"},
                "body": {"type": "user.message"},
                "retry_count": 1,
                "remote_event_id": "replacement",
            }
        ],
        "native_execution_receipts": {
            "idle": {
                "id": "idle",
                "type": "session.status_idle",
                "execution_id": "replacement",
                "stop_reason": {"type": "end_turn"},
            }
        },
    }
    call: dict[str, JsonValue] = {
        "call_id": "call",
        "session_id": "session",
        "native_turn_message_id": "owner",
        "remote_event_id": "failed-tool",
        "returned_at": "2026-10-26T00:00:00Z",
    }
    if tool_attempt is not None:
        call["native_turn_retry_count"] = tool_attempt
    await repo.put("ops.agent_sessions", "session", session)
    await repo.put("ops.tool_calls", "session:call", call)

    def routes(router) -> None:
        router.get("http://native.invalid/v1/sessions/session/executions").respond(
            200,
            json={
                "data": [
                    {"execution_id": "failed-tool", "state": "FAILED", "settled_outcome": "FAILED"},
                    {"execution_id": "replacement", "state": "COMPLETED", "settled_outcome": "COMPLETED"},
                ]
            },
        )

    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=False, http_routes=routes
    ) as runtime:
        result = await runtime.sessions.execution_barrier(session, verify_remote=True)
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None
    if tool_attempt is None:
        assert "superseded_native_inputs" not in stored
        assert result["complete"] is False
        assert result["failed_input_ids"] == ["failed-tool"]
    else:
        retired = stored["superseded_native_inputs"]
        assert isinstance(retired, dict)
        assert retired["failed-tool"]["replacement_input_id"] == "replacement"
        assert result["complete"] is True
    assert stored["message_log"] == session["message_log"]
    assert "retry_count" not in stored

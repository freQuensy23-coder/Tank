"""Completion diagnostics and failed-input retirement preserve actual retry provenance."""

import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import respx
from agent_source_fixtures import source_record_fixture
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture

from tank.contracts.session_completion import CompletionSessionSnapshot
from tank.storage import MemoryRepository, StorageBackend
from tank.time_precision import collection_clock


@pytest.fixture(
    params=["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def completion_repo(
    request: pytest.FixtureRequest, postgres_template: PostgresTemplate
) -> AsyncIterator[StorageBackend]:
    if request.param == "memory":
        yield MemoryRepository()
    else:
        async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
            await repo.migrate()
            yield repo


@pytest.mark.parametrize("attempt", [None, 0, 1])
async def test_source_completion_preserves_unknown_and_known_retry_attempts(
    tmp_path: Path, attempt: int | None, completion_repo: StorageBackend
) -> None:
    repo = completion_repo
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
    assert outcomes.entries["owner"].retry_attempt == attempt
    assert outcomes.entries["owner"].native_input_id == "accepted"
    assert outcomes.entries["owner"].outcome == "MISSING_SOURCE_ITEM"
    assert ("retry_count" in message) is (attempt is not None)


@pytest.mark.parametrize("tool_attempt", [None, 0])
async def test_failed_tool_input_requires_observed_older_attempt_before_supersession(
    tmp_path: Path, tool_attempt: int | None, completion_repo: StorageBackend
) -> None:
    repo = completion_repo
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "opened_by_item": "item",
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
        "tool": "get_source_item",
        "input": {"item_id": "item"},
        "status": "DONE",
        "requested_at": "2026-10-25T23:59:00Z",
        "native_turn_message_id": "owner",
        "remote_event_id": "failed-tool",
        "returned_at": "2026-10-26T00:00:00Z",
    }
    if tool_attempt is not None:
        call["native_turn_retry_count"] = tool_attempt
    session.update(
        {
            "agent_version_reason": "The synthetic completion fixture does not retain an agent version",
            "last_event_seq": 0,
            "last_item_at_reason": "No source item timestamp was retained by this synthetic session",
            **collection_clock().fields("opened_at"),
        }
    )
    await repo.put(
        "ingest.source_items",
        "item",
        source_record_fixture(
            {
                "item_id": "item",
                "published_at": "2026-10-25T23:58:00Z",
                "published_prec": "EXACT",
                "published_prec_reason": None,
            }
        ),
    )
    await repo.put("ops.agent_sessions", "session", session)
    await repo.put("ops.tool_calls", "session:call", call)

    def routes(router: respx.MockRouter) -> None:
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
        result = await runtime.sessions.execution_barrier(
            CompletionSessionSnapshot.from_record(session), verify_remote=True
        )
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None
    if tool_attempt is None:
        assert "superseded_native_inputs" not in stored
        assert result.complete is False
        assert result.failed_input_ids == ["failed-tool"]
    else:
        retired = stored["superseded_native_inputs"]
        assert isinstance(retired, dict)
        receipt = retired["failed-tool"]
        assert isinstance(receipt, dict) and receipt["replacement_input_id"] == "replacement"
        assert result.complete is True
    assert stored["message_log"] == session["message_log"]
    assert "retry_count" not in stored


@pytest.mark.parametrize("attempt", [None, 0, 1])
async def test_independently_completed_input_settles_without_unused_retry_or_body(
    tmp_path: Path, attempt: int | None, completion_repo: StorageBackend
) -> None:
    repo = completion_repo
    message: dict[str, JsonValue] = {
        "message_id": "owner",
        "payload": {"kind": "SCOUT_REVIEW", "proposal_id": "proposal"},
        "body": {"type": "user.message"},
        "remote_event_id": "accepted",
        "turn_status": "RUNNING",
    }
    if attempt is not None:
        message["retry_count"] = attempt
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "proposal_id": "proposal",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "owner",
        "messages": ["owner"],
        "message_log": [message],
        "native_execution_states": {
            "accepted": {"execution_id": "accepted", "state": "COMPLETED", "settled_outcome": "COMPLETED"}
        },
        "native_execution_receipts": {
            "idle": {
                "id": "idle",
                "type": "session.status_idle",
                "execution_id": "accepted",
                "stop_reason": {"type": "end_turn"},
            }
        },
    }
    session.update(
        {
            "agent_version_reason": "The synthetic completion fixture does not retain an agent version",
            "last_event_seq": 0,
            "last_item_at_reason": "No source item timestamp was retained by this synthetic session",
            **collection_clock().fields("opened_at"),
        }
    )
    await repo.put("ops.agent_sessions", "session", session)
    requests: list[str] = []

    def routes(router: respx.MockRouter) -> None:
        def terminal_history(request: httpx.Request) -> httpx.Response:
            requests.append(request.method)
            return httpx.Response(200, json={"data": [], "next_page": None})

        router.get("http://native.invalid/v1/sessions/session/events").mock(side_effect=terminal_history)

    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=False, http_routes=routes
    ) as runtime:
        result = await runtime.sessions.completed_turn("session")
        assert result.complete is True
        repeated = await runtime.sessions.completed_turn("session")
        assert repeated.complete is True
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["runtime_status"] == "IDLE" and stored["active_message_id"] is None
    journal = stored["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    assert journal[0]["turn_status"] == "DONE" and journal[0]["body"] == {"type": "user.message"}
    assert journal[0].get("retry_count") == attempt
    assert ("retry_count" in journal[0]) is (attempt is not None)
    proof = journal[0]["source_completion_verified"]
    assert (
        isinstance(proof, dict) and proof["retry_attempt"] == attempt and proof["outcome"] == "NOT_APPLICABLE"
    )
    assert requests == ["GET"]
    assert await repo.list("ops.jobs") == []

"""Enabled progress cannot replace independent native completion provenance."""

from pathlib import Path

import httpx
import pytest
import respx
from agent_source_fixtures import source_record_fixture
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture
from test_session_completion_lineage import completion_repo as completion_repo

from tank.storage import StorageBackend
from tank.time_precision import collection_clock


@pytest.mark.parametrize("attempt", [None, 0])
async def test_ops_enabled_source_completion_preserves_unreported_attempt(
    completion_repo: StorageBackend, tmp_path: Path, attempt: int | None
) -> None:
    repo = completion_repo
    message: dict[str, JsonValue] = {
        "message_id": "owner",
        "payload": {"kind": "NEW_ITEM", "item_id": "item"},
        "body": {"type": "user.message"},
        "remote_event_id": "accepted",
        "turn_status": "RUNNING",
    }
    if attempt is not None:
        message["retry_count"] = attempt
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "opened_by_item": "item",
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
        "agent_version_reason": "This synthetic completion does not retain an agent version",
        "last_event_seq": 0,
        "last_item_at_reason": "This synthetic session does not retain the source receipt clock",
        **collection_clock().fields("opened_at"),
    }
    await repo.put(
        "ingest.source_items",
        "item",
        source_record_fixture(
            {
                "item_id": "item",
                "source_id": "S30",
                "published_at": "2026-10-25T12:00:00Z",
                "published_prec": "EXACT",
                "published_prec_reason": None,
            }
        ),
    )
    await repo.put("ops.agent_sessions", "session", session)
    requests: list[str] = []

    def routes(router: respx.MockRouter) -> None:
        def terminal_history(request: httpx.Request) -> httpx.Response:
            requests.append(request.method)
            return httpx.Response(200, json={"data": [], "next_page": None})

        router.get("http://native.invalid/v1/sessions/session/events").mock(side_effect=terminal_history)

    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=True, http_routes=routes
    ) as runtime:
        assert (await runtime.sessions.completed_turn("session")).complete is True
        assert (await runtime.sessions.completed_turn("session")).complete is True
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["runtime_status"] == "IDLE" and stored["active_message_id"] is None
    journal = stored["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    assert ("retry_count" in journal[0]) is (attempt is not None)
    assert journal[0].get("retry_count") == attempt
    assert journal[0]["turn_status"] == "DONE"
    proof = journal[0]["source_completion_verified"]
    assert isinstance(proof, dict) and proof["retry_attempt"] == attempt and proof["outcome"] == "SKIPPED"
    notices = await repo.list("out.ops_notices")
    assert len(notices) == 1 and notices[0]["stage"] == "AGENT_REJECTED"
    assert notices[0]["reason"] == "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION"
    jobs = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    assert len(jobs) == 1
    assert requests == ["GET"]

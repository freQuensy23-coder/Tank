"""Unknown historical host retry budgets cannot authorize native input effects."""

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from pydantic import JsonValue
from test_session_outbound_intents import pending_record

from tank.agents.native_retry import UnknownNativeRetryState
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["journal", "origin", "stale-journal"])
async def test_unknown_historical_retry_state_refuses_before_posting_and_preserves_retained_row(
    owner: str,
) -> None:
    repo = MemoryRepository()
    body: dict[str, JsonValue] = {
        "type": "user.message",
        "content": [{"type": "text", "text": "Retained original input"}],
    }
    original: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "last_message": body,
    }
    if owner != "origin":
        original["message_log"] = [
            {
                "message_id": "message",
                "body": body,
                "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "question"},
                "turn_status": "RUNNING",
            }
        ]
        if owner == "journal":
            original["active_message_id"] = "message"
        else:
            original["retry_count"] = 0
    await repo.put("ops.agent_sessions", "session", original)
    calls: list[str] = []

    def forbidden(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        raise AssertionError("Unproved retry state must not invoke native HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(UnknownNativeRetryState) as refused:
            await service.process_event("session", {"id": "failure", "type": "agent.refusal"})
    assert refused.value.reason == (
        "CURRENT_INPUT_OWNER_NOT_RETAINED" if owner == "stale-journal" else "RETRY_COUNT_NOT_RETAINED"
    )
    assert calls == []
    assert await repo.get("ops.agent_sessions", "session") == original
    assert "terminal_failure" not in original


@pytest.mark.asyncio
async def test_new_declared_input_budget_is_not_blocked_by_unknown_origin_history() -> None:
    repo = MemoryRepository()
    stale_body: dict[str, JsonValue] = {
        "type": "user.message",
        "content": [{"type": "text", "text": "Unproved historical input"}],
    }
    origin: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "IDLE",
        "last_message": stale_body,
    }
    await repo.put("ops.agent_sessions", "session", origin)
    posted: list[bytes] = []
    keys: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST" and request.url.path.endswith("/events")
        posted.append(request.content)
        keys.append(request.headers["idempotency-key"])
        return httpx.Response(200, json=acknowledge_native_input(request, "native-" + str(len(posted))))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(UnknownNativeRetryState):
            await service.process_event("session", {"id": "old-refusal", "type": "agent.refusal"})
        assert posted == []
        fresh: dict[str, JsonValue] = {
            **origin,
            "runtime_status": "PENDING",
            "active_message_id": "message",
            "message_log": [pending_record()],
            "messages": [],
        }
        await repo.put("ops.agent_sessions", "session", fresh)
        await service.post_message_intent("session", "message")
        await service.process_event(
            "session", {"id": "new-failure", "type": "session.error", "execution_id": "native-1"}
        )
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and "retry_count" not in stored
    journal = stored["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    assert journal[0]["retry_count"] == 1
    assert journal[0]["native_input_attempts"] == {"native-1": 0, "native-2": 1}
    assert len(posted) == 2 and posted[0] == posted[1]
    assert keys == ["message:message", "turn-retry:session:message"]
    assert stored["last_message"] == stale_body


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 2])
@pytest.mark.parametrize("body", [None, {"type": "user.message"}, "opaque retained body"])
async def test_exhausted_retry_records_terminal_failure_without_consuming_historical_body(
    count: int, body: JsonValue
) -> None:
    repo = MemoryRepository()
    done: dict[str, JsonValue] = {
        "message_id": "done",
        "turn_status": "DONE",
        "immutable": {"historical": [False, 0]},
    }
    active: dict[str, JsonValue] = {
        "message_id": "active",
        "turn_status": "RUNNING",
        "retry_count": count,
        "body": body,
        "remote_event_id": {"unknown": "unused prior input"},
        "payload": {"kind": "USER_QUESTION", "question_id": "question", "session_id": "session"},
    }
    original: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "active",
        "message_log": [done, active],
    }
    await repo.put("ops.agent_sessions", "session", original)

    def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Exhausted retry cannot post or fetch native input")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        service = SessionService(
            repo,
            Settings(_env_file=None, ops_progress_enabled=False),
            None,
            http=http,
            versions=declared_versions_fixture(),
        )
        await service.process_event("session", {"id": "final-failure", "type": "session.error"})
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["status"] == "FAILED" and stored["runtime_status"] == "IDLE"
    assert stored["active_message_id"] is None
    journal = stored["message_log"]
    assert isinstance(journal, list) and journal[0] == done
    failed = journal[1]
    assert isinstance(failed, dict) and failed["body"] == body
    assert failed["remote_event_id"] == {"unknown": "unused prior input"}
    assert failed["retry_count"] == count and failed["turn_status"] == "FAILED"
    terminal = stored["terminal_failure"]
    assert isinstance(terminal, dict) and terminal["event_id"] == "final-failure"

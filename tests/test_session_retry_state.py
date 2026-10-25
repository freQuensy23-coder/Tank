"""Unknown historical host retry budgets cannot authorize native input effects."""

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue

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
        "next_tool_call_ordinal": 0,
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

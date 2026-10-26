"""Pre-fix reproduction of unproved historical ownership blocking later native frames."""

import json
from typing import Literal

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from pydantic import JsonValue
from session_followup_fixtures import UnexpectedFollowupRefusals
from test_native_transport_contracts import failure, settings, transport_state

from tank.agents.input_ownership import UnprovedInputOwnership
from tank.agents.native_transport import NativeRetryRefusalLedger
from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["managed-page", "node-page", "stream"])
async def test_unproved_owner_candidate_reproduces_across_restart_and_fresh_control(
    transport: Literal["managed-page", "node-page", "stream"],
) -> None:
    repo = MemoryRepository()
    original = transport_state()
    journal = original["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    journal[0].pop("native_input_attempts")
    await repo.put("ops.agent_sessions", "session", original)
    sequenced = transport != "managed-page"
    old = failure("old-failure", "old-input", 1 if sequenced else None)
    fresh = failure("fresh-failure", "fresh-input", 2 if sequenced else None)
    frames = [old, fresh]
    posts: list[httpx.Request] = []
    reads: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            posts.append(request)
            return httpx.Response(200, json=acknowledge_native_input(request, "fresh-retry"))
        reads.append(request.url.path)
        if transport == "stream":
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text="".join(f"id: {event['seq']}\ndata: {json.dumps(event)}\n\n" for event in frames),
            )
        page: dict[str, JsonValue] = {
            "data": [event for event in frames],
            **({"next_page": None} if transport == "managed-page" else {"has_more": False}),
        }
        return httpx.Response(200, json=page)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        for controller in ("first", "restarted"):
            service = SessionService(
                repo,
                settings(),
                None,
                http=http,
                versions=declared_versions_fixture(controller, "synthetic-code"),
                refusals=UnexpectedFollowupRefusals(),
            )
            with pytest.raises(UnprovedInputOwnership, match="no recorded retry attempt"):
                if transport == "stream":
                    await service.stream("session")
                else:
                    await service.catch_up("session")
            await service.close()
            stored = await repo.get("ops.agent_sessions", "session")
            assert stored == original
            assert stored is not None
            assert NativeRetryRefusalLedger.from_record("session", stored).entries == {}
            assert posts == []
        assert len(reads) == 2
        frames = [fresh]
        control = SessionService(
            repo,
            settings(),
            None,
            http=http,
            versions=declared_versions_fixture("fresh-control", "synthetic-code"),
            refusals=UnexpectedFollowupRefusals(),
        )
        if transport == "stream":
            await control.stream("session")
        else:
            await control.catch_up("session")
        await control.close()
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["active_message_id"] == "fresh"
    current = stored["message_log"]
    assert isinstance(current, list) and isinstance(current[0], dict) and isinstance(current[1], dict)
    assert "retry_count" not in current[0] and "native_input_attempts" not in current[0]
    assert current[1]["retry_count"] == 1
    assert len(posts) == 1 and posts[0].headers["idempotency-key"] == "turn-retry:session:fresh"

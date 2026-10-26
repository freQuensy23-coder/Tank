"""Durable host refusal preserves later independently owned native frames."""

import json
from typing import Literal

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from pydantic import JsonValue
from session_followup_fixtures import UnexpectedFollowupRefusals
from test_native_transport_contracts import failure, settings, transport_state

from tank.agents.native_transport import NativeRetryRefusalLedger
from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["managed-page", "node-page", "stream"])
async def test_unproved_owner_is_durably_refused_without_starving_fresh_owner(
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
            if transport == "stream":
                await service.stream("session")
            else:
                await service.catch_up("session")
            await service.close()
            stored = await repo.get("ops.agent_sessions", "session")
            assert stored is not None
            receipts = NativeRetryRefusalLedger.from_record("session", stored).entries
            assert len(receipts) == 1
            receipt = next(iter(receipts.values()))
            assert receipt.reason == "CURRENT_INPUT_ATTEMPT_NOT_RETAINED"
            assert receipt.frame.event_id == "old-failure"
            assert receipt.frame.execution_id == "old-input"
            assert receipt.controller.config_version == "first"
        assert len(reads) == 2
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["active_message_id"] == "fresh"
    current = stored["message_log"]
    assert isinstance(current, list) and isinstance(current[0], dict) and isinstance(current[1], dict)
    assert "retry_count" not in current[0] and "native_input_attempts" not in current[0]
    assert current[1]["retry_count"] == 1
    assert len(posts) == 1 and posts[0].headers["idempotency-key"] == "turn-retry:session:fresh"


@pytest.mark.asyncio
async def test_unproved_turn_attempt_does_not_block_actual_billing_or_native_termination() -> None:
    repo = MemoryRepository()
    original = transport_state()
    journal = original["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    journal[0].pop("native_input_attempts")
    for row in journal:
        assert isinstance(row, dict)
        row["payload"] = {"kind": "SCOUT_REVIEW", "proposal_id": "synthetic-proposal"}
    original["prompt_version"] = None
    await repo.put("ops.agent_sessions", "session", original)
    requests: list[httpx.Request] = []

    def refuse_http(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("Observed billing and termination require no input posting")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = SessionService(
            repo,
            settings(),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        await service.observe_native_event(
            "session",
            {
                "id": "billing",
                "type": "span.model_request_end",
                "execution_id": "old-input",
                "model_usage": {"input_tokens": 5, "output_tokens": 2},
                "processed_at": "2026-10-26T00:00:00Z",
            },
            1,
        )
        usage = await repo.get("ops.llm_usage", "session:billing")
        assert usage is not None and usage["input_tokens"] == 5 and usage["output_tokens"] == 2
        await service.observe_native_event(
            "session",
            {"id": "termination", "type": "session.status_terminated", "execution_id": "old-input"},
            2,
        )
        await service.close()
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["status"] == "ENDED"
    receipt = stored["termination_receipt"]
    assert isinstance(receipt, dict) and receipt["event_id"] == "termination"
    assert stored["last_event_seq"] == 2
    assert NativeRetryRefusalLedger.from_record("session", stored).entries == {}
    assert requests == []


@pytest.mark.asyncio
async def test_contradictory_lineage_is_not_relabelled_as_a_handled_host_refusal() -> None:
    from pydantic import ValidationError

    repo = MemoryRepository()
    original = transport_state()
    journal = original["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    journal[0]["retry_count"] = 1
    await repo.put("ops.agent_sessions", "session", original)
    requests: list[httpx.Request] = []

    def refuse_http(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("Contradictory ownership cannot authorize input")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = SessionService(
            repo,
            settings(),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(ValidationError, match="contradicts"):
            await service.observe_native_event("session", failure("conflict", "old-input", 1), 1)
        assert service.seen_events == {}
        await service.close()
    assert await repo.get("ops.agent_sessions", "session") == original
    assert requests == []

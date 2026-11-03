import asyncio
import copy
import json

import httpx
import pytest
from test_session_event_merge import AT, seed, settings

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository
from tank.util import sha256


def custom(identity, execution="input-root"):
    return {
        "id": identity,
        "type": "agent.custom_tool_use",
        "execution_id": execution,
        "name": "record_event",
        "input": {"is_event": False, "no_event_reason": identity},
        "processed_at": AT,
    }


def declaration(*events, execution="input-root"):
    return {
        "execution_id": execution,
        "state": "COMPLETED",
        "settled_outcome": "COMPLETED",
        "custom_tool_uses": [
            {"declaration_id": "declaration-" + execution, "event": copy.deepcopy(event)}
            for event in events
        ],
    }


def terminal(*identities, execution="input-root"):
    return {
        "id": "declaration-" + execution,
        "type": "session.status_idle",
        "execution_id": execution,
        "stop_reason": {"type": "requires_action", "event_ids": list(identities)},
        "processed_at": AT,
    }


@pytest.mark.parametrize("failure", ["session.status_terminated", "execution.status_failed"])
async def test_streamed_custom_call_without_committed_declaration_has_no_business_effect(failure):
    repo = MemoryRepository()
    session = await seed(repo)
    session["message_log"][0]["retry_count"] = 1
    session["message_log"][0]["native_input_attempts"] = {"input-root": 1}
    await repo.put("ops.agent_sessions", "s", session)
    callbacks = []

    async def dispatch(name, arguments, context):
        await repo.put("evt.events", "forbidden", {"event_id": "forbidden"})
        return {"is_event": False}

    def handler(request):
        if request.method == "POST":
            callbacks.append(request.content)
            return httpx.Response(200, json={"data": [{"id": "callback"}]})
        assert request.url.path.endswith("/executions")
        return httpx.Response(200, json={"data": [declaration()]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), dispatch, http=http)
        async with asyncio.timeout(1):
            await service.process_event("s", custom("speculative"), seq=1)
            await service.process_event(
                "s", {"id": "failed", "type": failure, "execution_id": "input-root"}, seq=2
            )
        assert await repo.get("evt.events", "forbidden") is None
        assert await repo.get("ops.tool_calls", "s:speculative") is None
        assert callbacks == []
        assert (await repo.get("ops.agent_sessions", "s"))["status"] in {"FAILED", "ENDED"}
        await service.close()


@pytest.mark.parametrize("is_error", [True, False, None])
async def test_local_resolution_and_corrected_committed_group_execute_only_unresolved_call(is_error):
    repo = MemoryRepository()
    await seed(repo)
    committed = []
    effects = []
    callbacks = []
    rejected, corrected, sibling = custom("local"), custom("corrected"), custom("sibling")

    async def dispatch(name, arguments, context):
        effects.append(arguments["no_event_reason"])
        return {"is_event": False, "reason": arguments["no_event_reason"]}

    def handler(request):
        if request.method == "POST":
            body = json.loads(request.content)["events"][0]
            callbacks.append(body)
            return httpx.Response(200, json={"data": [{"id": "callback-" + body["custom_tool_use_id"]}]})
        assert request.url.path.endswith("/executions")
        return httpx.Response(200, json={"data": [declaration(*committed)]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), dispatch, http=http)
        await service.process_event("s", rejected)
        await service.process_event(
            "s",
            {
                "id": "local-result",
                "type": "agent.tool_result",
                "tool_use_id": "local",
                "is_error": is_error,
                "processed_at": AT,
            },
        )
        await service.process_event("s", corrected)
        await service.process_event("s", sibling)
        assert effects == [] and callbacks == []
        committed[:] = [corrected, sibling]
        await service.process_event("s", terminal("corrected", "sibling"))
        await service.process_event("s", corrected)
        await service.process_event("s", terminal("corrected", "sibling"))
        assert effects == ["corrected", "sibling"]
        assert [body["custom_tool_use_id"] for body in callbacks] == ["corrected", "sibling"]
        assert all(not body.get("is_error") for body in callbacks)
        assert await repo.get("ops.tool_calls", "s:local") is None
        assert (await repo.get("ops.agent_sessions", "s"))["no_event"] is True
        for event in (corrected, sibling):
            row = await repo.get("ops.tool_calls", "s:" + event["id"])
            assert row["native_source_execution_id"] == "input-root"
            assert row["native_turn_message_id"] == "root" and row["native_turn_retry_count"] == 0
            assert row["native_declaration_id"] == "declaration-input-root"
            assert row["result"] == {"is_event": False, "reason": event["id"]}
        await service.close()


@pytest.mark.parametrize("boundary", ["before_declaration", "before_effect", "lost_ack"])
async def test_restart_keeps_committed_call_effect_and_exact_callback_owned_once(boundary):
    repo = MemoryRepository()
    await seed(repo)
    event = custom("once")
    committed = []
    posts = []
    effects = []

    async def dispatch(name, arguments, context):
        effects.append(arguments["no_event_reason"])
        return {"is_event": False, "reason": "once"}

    def handler(request):
        if request.method == "POST":
            posts.append((request.headers["idempotency-key"], request.content))
            if boundary == "lost_ack" and len(posts) == 1:
                raise httpx.ReadError("Lost ACK after native acceptance", request=request)
            return httpx.Response(200, json={"data": [{"id": "accepted-once"}]})
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": [event, terminal("once")] if committed else [event]})
        assert request.url.path.endswith("/executions")
        return httpx.Response(200, json={"data": [declaration(*committed)]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), dispatch, http=http)
        await service.process_event("s", event, seq=3)
        assert effects == []
        committed[:] = [event]
        if boundary == "lost_ack":
            with pytest.raises(httpx.ReadError):
                await service.process_event("s", terminal("once"), seq=4)
            assert effects == ["once"]
            row = await repo.get("ops.tool_calls", "s:once")
            assert row["status"] == "DONE" and not row.get("returned_at")
        elif boundary == "before_effect":
            current = await repo.get("ops.agent_sessions", "s")
            current["native_execution_receipts"] = {"declaration-input-root": terminal("once")}
            current["processed_events"].append("declaration-input-root")
            await repo.put("ops.agent_sessions", "s", current)
        await service.close()
        restarted = SessionService(repo, settings(), dispatch, http=http)
        await restarted.catch_up("s")
        await restarted.catch_up("s")
        assert effects == ["once"]
        assert len(posts) == (2 if boundary == "lost_ack" else 1)
        assert posts[0] == posts[-1]
        assert posts[0][0] == "tool:" + sha256("s:once")
        row = await repo.get("ops.tool_calls", "s:once")
        assert row["remote_event_id"] == "accepted-once" and row["returned_at"]
        assert json.loads(posts[-1][1])["events"][0] == row["native_result_body"]
        await restarted.close()

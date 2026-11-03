import asyncio
import copy
import json
import os
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlsplit

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
            current.setdefault("processed_events", []).append("declaration-input-root")
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


@asynccontextmanager
async def native_peer(handler):
    failures = []

    async def accept(reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            lines = headers.decode().split("\r\n")
            method, target, _ = lines[0].split(" ")
            fields = dict(line.lower().split(": ", 1) for line in lines[1:] if line)
            body = await reader.readexactly(int(fields.get("content-length", 0)))
            assert "managed-agents-2026-04-01" in fields["anthropic-beta"]
            result = await handler(method, target, fields, body)
            encoded = json.dumps(result).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                + str(len(encoded)).encode() + b"\r\n\r\n" + encoded
            )
            await writer.drain()
        except Exception as error:
            failures.append(error)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    try:
        yield "http://127.0.0.1:" + str(server.sockets[0].getsockname()[1])
    finally:
        server.close()
        await server.wait_closed()
        assert failures == []


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_native_http_declaration_effect_ack_and_admission_barrier(postgres_template):
    database = os.environ["TANK_TEST_DATABASE_URL"]
    async with postgres_template.repository(database) as repo:
        await seed(repo)
        event = custom("durable")
        declared = asyncio.Event()
        effect_started = asyncio.Event()
        effect_release = asyncio.Event()
        ack_started = asyncio.Event()
        ack_release = asyncio.Event()
        callbacks = []
        states = {"input-root": "RUNNING"}
        history = [event]

        async def handler(method, target, headers, body):
            path = urlsplit(target)
            if method == "GET" and path.path.endswith("/executions"):
                ids = parse_qs(path.query)["ids[]"]
                return {"data": [
                    {
                        "execution_id": identity,
                        "state": states[identity],
                        "settled_outcome": "COMPLETED" if states[identity] == "COMPLETED" else None,
                        "custom_tool_uses": declaration(event)["custom_tool_uses"]
                        if declared.is_set() and identity == "input-root" else [],
                    }
                    for identity in ids
                ]}
            if method == "GET" and path.path.endswith("/events"):
                return {"data": copy.deepcopy(history)}
            assert method == "POST" and path.path.endswith("/events")
            callback = json.loads(body)["events"][0]
            assert callback["custom_tool_use_id"] == "durable"
            callbacks.append((headers["idempotency-key"], body))
            states["accepted-durable"] = "RUNNING"
            ack_started.set()
            await ack_release.wait()
            return {"data": [{"id": "accepted-durable"}]}

        async def dispatch(name, arguments, context):
            effect_started.set()
            await effect_release.wait()
            await repo.put("evt.events", "effect", {"event_id": "effect", "statement": "once"})
            return {"is_event": False, "reason": "durable"}

        async with native_peer(handler) as url, httpx.AsyncClient() as http:
            opts = settings()
            opts.openma_url = url
            service = SessionService(repo, opts, dispatch, http=http)
            task = None
            try:
                async with asyncio.timeout(5):
                    await service.process_event("s", event, seq=1)
                    assert not effect_started.is_set()
                    assert await repo.get("ops.tool_calls", "s:durable") is None
                    declared.set()
                    states["input-root"] = "COMPLETED"
                    history.append(terminal("durable"))
                    task = asyncio.create_task(service.process_event("s", terminal("durable"), seq=2))
                    await effect_started.wait()
                    current = await repo.get("ops.agent_sessions", "s")
                    assert not (await service.execution_barrier(current, verify_remote=True))["complete"]
                    assert await service.slot_state() == ({"s"}, [])
                    effect_release.set()
                    await ack_started.wait()
                    call = await repo.get("ops.tool_calls", "s:durable")
                    assert call["status"] == "DONE" and not call.get("returned_at")
                    assert (await repo.get("evt.events", "effect"))["statement"] == "once"
                    current = await repo.get("ops.agent_sessions", "s")
                    barrier = await service.execution_barrier(current, verify_remote=True)
                    assert not barrier["complete"] and barrier["unidentified_inputs"] == ["unreturned-tool:durable"]
                    assert await service.slot_state() == ({"s"}, [])
                    ack_release.set()
                    await task
                    assert not (await service.completed_turn("s"))["complete"]
                    states["accepted-durable"] = "COMPLETED"
                    history.append({
                        "id": "finished", "type": "session.status_idle", "execution_id": "accepted-durable",
                        "stop_reason": {"type": "end_turn"}, "processed_at": AT,
                    })
                    await service.process_event("s", history[-1])
                    service.execution_polls.clear()
                    assert (await service.completed_turn("s"))["complete"]
                    assert await service.slot_state() == (set(), [])
                    assert len(callbacks) == 1
                    row = await repo.get("ops.tool_calls", "s:durable")
                    assert row["native_turn_message_id"] == "root"
                    assert row["native_source_execution_id"] == "input-root"
                    assert row["native_declaration_id"] == "declaration-input-root"
                    assert row["remote_event_id"] == "accepted-durable"
            finally:
                effect_release.set()
                ack_release.set()
                if task is not None:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await service.close()

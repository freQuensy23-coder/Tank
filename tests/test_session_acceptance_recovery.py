import asyncio
import json
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from test_agents_execution_barrier import config, idle
from test_session_binding_concurrency import fixture_repo
from test_session_event_merge import AT, message, seed, settings

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@asynccontextmanager
async def native_peer():
    accepted = {}
    requests = []
    effects = []

    async def serve(reader, writer):
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            lines = header.decode().split("\r\n")
            headers = dict(line.lower().split(": ", 1) for line in lines[1:] if ": " in line)
            method, path, _ = lines[0].split()
            if method == "POST":
                body = await reader.readexactly(int(headers["content-length"]))
                key = headers["idempotency-key"]
                requests.append((key, body))
                if key in accepted:
                    assert accepted[key][0] == body
                else:
                    identity = "native-tool-accepted" if key.startswith("tool:") else "native-accepted"
                    accepted[key] = (body, identity)
                    effects.append(json.loads(body)["events"][0])
                response = {"data": [{"id": accepted[key][1]}]}
            elif "/executions" in path:
                response = {
                    "data": [
                        {
                            "execution_id": identity,
                            "state": "COMPLETED",
                            "settled_outcome": "COMPLETED",
                        }
                        for identity in parse_qs(urlsplit(path).query).get("ids[]", [])
                    ]
                }
            else:
                identity = (
                    "native-tool-accepted"
                    if any(key.startswith("tool:") for key in accepted)
                    else "native-accepted"
                )
                response = {"data": [idle("native-ended:" + identity, identity)]}
            payload = json.dumps(response).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                + str(len(payload)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + payload
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    try:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}", requests, effects
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.parametrize("response", [{}, {"data": []}, {"data": [{"id": ""}]}])
async def test_missing_acceptance_identity_keeps_original_intent_recoverable(response):
    repo = MemoryRepository()
    session = await seed(repo)
    intent = session["message_log"][0]
    intent.update(remote_event_id=None, native_input_attempts={}, turn_status="ACCEPTING")
    intent.pop("sent_at")
    await repo.put("ops.agent_sessions", "s", session)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response))
    ) as client:
        service = SessionService(repo, config(), None, http=client)
        with pytest.raises(ValueError, match="accept"):
            await service.post_message_intent("s", "root")
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][0] == intent
        assert not (await service.execution_barrier(current))["complete"]


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
@pytest.mark.parametrize("turn_status", ["ACCEPTING", "PENDING", "RUNNING"])
async def test_restart_recovers_sent_input_identity_without_reexecuting_native_turn(
    backend, postgres_template, turn_status
):
    async with fixture_repo(backend, postgres_template) as repo:
        session = await seed(repo)
        intent = session["message_log"][0]
        intent.update(remote_event_id=None, native_input_attempts={}, turn_status=turn_status)
        await repo.put("ops.agent_sessions", "s", session)
        durable = await repo.get("ops.agent_sessions", "s")
        assert durable is not None
        durable_body = durable["message_log"][0]["body"]
        assert durable_body == intent["body"]
        item = await repo.get("ingest.source_items", "i")
        item["extraction_status"] = "DONE"
        await repo.put("ingest.source_items", "i", item)
        async with native_peer() as (url, requests, effects):
            settings = config()
            settings.openma_url = url
            async with httpx.AsyncClient() as client:
                first = SessionService(repo, settings, None, http=client)
                await first.request(
                    "POST",
                    "sessions/s/events",
                    json={"events": [durable_body]},
                    idem="message:root",
                )
                restarted = SessionService(repo, settings, None, http=client)
                await restarted.recover_outbound_inputs("s")
                current = await repo.get("ops.agent_sessions", "s")
                assert current["message_log"][0]["remote_event_id"] == "native-accepted"
                assert current["message_log"][0]["body"] == intent["body"]
                assert requests[0] == requests[1]
                assert len(effects) == 1
                assert (await restarted.completed_turn("s"))["complete"]
                current = await repo.get("ops.agent_sessions", "s")
                assert current["active_message_id"] is None
                assert current["runtime_status"] == "IDLE"
                assert current["message_log"][0]["turn_status"] == "DONE"
                await restarted.recover_outbound_inputs("s")
                assert len(requests) == 2


async def test_invalid_retry_ack_retains_original_failure_for_exact_replay():
    repo = MemoryRepository()
    await seed(repo)
    posts = []

    def handler(request):
        posts.append((request.headers["idempotency-key"], request.content))
        return httpx.Response(200, json={} if len(posts) == 1 else {"data": [{"id": "retry-input"}]})

    error = {"id": "failure", "type": "session.error", "execution_id": "input-root"}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(repo, config(), None, http=client)
        with pytest.raises(ValueError, match="acceptance"):
            await service.process_event("s", error)
        before = await repo.get("ops.agent_sessions", "s")
        assert before["message_log"][0]["remote_event_id"] == "input-root"
        assert before["message_log"][0]["retry_count"] == 0
        assert before["pending_retry_event"]["id"] == "failure"
        restarted = SessionService(repo, config(), None, http=client)
        await restarted.recover_outbound_inputs("s")
        after = await repo.get("ops.agent_sessions", "s")
        assert posts[0] == posts[1]
        assert after["message_log"][0]["remote_event_id"] == "retry-input"
        assert after["message_log"][0]["retry_count"] == 1
        assert after["pending_retry_event"] is None


async def test_historical_failed_attempt_and_pending_tool_settle_before_slot_release():
    repo = MemoryRepository()
    session = await seed(repo)
    session["message_log"][0].update(
        retry_count=1,
        remote_event_id="retry-input",
        superseded_native_input_ids=["input-root"],
        native_input_attempts={"input-root": 0, "retry-input": 1},
    )
    session["superseded_native_inputs"] = {"input-root": {"reason": "TURN_FAILED_AND_RETRIED"}}
    session["message_log"].append(message("later", "later-input"))
    session["active_message_id"] = "later"
    session["native_execution_receipts"] = {
        "later-end": idle("later-end", "later-input"),
        "retry-end": idle("retry-end", "retry-input"),
    }
    await repo.put("ops.agent_sessions", "s", session)
    old = {
        "session_id": "s",
        "call_id": "old-tool",
        "tool": "get_item",
        "arguments": {},
        "result": {},
        "status": "DONE",
        "returned_at": AT,
        "remote_event_id": "old-tool-input",
        "native_turn_message_id": "root",
        "native_turn_retry_count": 0,
    }
    await repo.put("ops.tool_calls", "s:old-tool", old)
    pending = {**old, "call_id": "pending", "native_turn_message_id": "later", "status": "PENDING"}
    pending.pop("returned_at")
    pending.pop("remote_event_id")
    await repo.put("ops.tool_calls", "s:pending", pending)
    history = []

    def handler(request):
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": identity,
                            "state": "FAILED" if identity == "old-tool-input" else "COMPLETED",
                            "settled_outcome": "FAILED" if identity == "old-tool-input" else "COMPLETED",
                        }
                        for identity in request.url.params.get_list("ids[]")
                    ]
                },
            )
        assert request.method == "GET"
        return httpx.Response(200, json={"data": history})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(repo, settings(), None, http=client)
        blocked = await service.completed_turn("s")
        assert not blocked["complete"]
        assert blocked["unidentified_inputs"] == ["unreturned-tool:pending"]
        assert (await service.slot_state())[0] == {"s"}
        pending.update(status="DONE", returned_at=AT, remote_event_id="pending-result")
        await repo.put("ops.tool_calls", "s:pending", pending)
        history.append(idle("pending-end", "pending-result"))
        service.execution_polls.clear()
        assert (await service.completed_turn("s"))["complete"]
        after = await repo.get("ops.agent_sessions", "s")
        assert after["active_message_id"] is None
        assert after["runtime_status"] == "IDLE"
        assert after["superseded_native_inputs"]["old-tool-input"]["reason"] == "FAILED_PRIOR_TURN_ATTEMPT"
        assert (await service.slot_state())[0] == set()
        assert after["message_log"][1]["turn_status"] == "DONE"
        restarted = SessionService(repo, settings(), None, http=client)
        assert (await restarted.completed_turn("s"))["complete"]

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest
from test_agents_execution_barrier import config, idle
from test_session_binding_concurrency import fixture_repo
from test_session_event_merge import seed

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
                    accepted[key] = (body, "native-accepted")
                    effects.append(json.loads(body)["events"][0])
                response = {"data": [{"id": accepted[key][1]}]}
            elif "/executions" in path:
                response = {
                    "data": [
                        {
                            "execution_id": "native-accepted",
                            "state": "COMPLETED",
                            "settled_outcome": "COMPLETED",
                        }
                    ]
                }
            else:
                response = {"data": [idle("native-ended", "native-accepted")]}
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
                    json={"events": [intent["body"]]},
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

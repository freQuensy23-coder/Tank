"""Finite control-plane reads against an actual stalled loopback TCP peer."""

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest
from test_agents import seed_session, settings

from tank.agents import sessions as session_module
from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@asynccontextmanager
async def stalled_http_peer():
    release = asyncio.Event()
    handlers = set()
    requests = []

    async def accept(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            await reader.readuntil(b"\r\n\r\n")
            requests.append(True)
            await release.wait()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    try:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}", requests
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*tuple(handlers), return_exceptions=True)


@pytest.mark.parametrize("injected_unbounded_client", [False, True])
async def test_stalled_rest_read_times_out_without_changing_sse(monkeypatch, injected_unbounded_client):
    async with stalled_http_peer() as (url, accepted):
        opts, _ = settings(openma_url=url)
        injected = httpx.AsyncClient(timeout=None) if injected_unbounded_client else None
        service = SessionService(MemoryRepository(), opts, None, http=injected)
        try:
            if not injected_unbounded_client:
                assert service.http.timeout.read == 30
                assert service.stream_http.timeout.read is None
                assert service.http is not service.stream_http
            else:
                assert injected.timeout.read is None
            monkeypatch.setattr(session_module, "REST_TIMEOUT_S", 0.05, raising=False)
            with pytest.raises(httpx.ReadTimeout):
                await asyncio.wait_for(service.request("GET", "sessions/stalled"), timeout=1)
            assert accepted
            assert service.stream_http.timeout.read is None
        finally:
            await service.close()
            if injected is not None:
                await injected.aclose()


async def test_rest_ack_timeout_retains_result_and_reuses_native_ack_body():
    repo = MemoryRepository()
    await seed_session(repo)
    opts, _ = settings()
    dispatched, posts = [], []

    async def dispatch(tool, arguments, context):
        dispatched.append(tool)
        return {"ok": True}

    def acknowledge(request):
        posts.append(json.loads(request.content)["events"][0])
        assert request.extensions["timeout"]["read"] == 30
        if len(posts) == 1:
            raise httpx.ReadTimeout("Synthetic lost native ACK", request=request)
        return httpx.Response(200, json={"data": [{"id": "accepted"}]})

    event = {"id": "call", "type": "agent.custom_tool_use", "name": "fetch_page", "input": {}}
    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge), timeout=None) as http:
        first = SessionService(repo, opts, dispatch, http=http)
        with pytest.raises(httpx.ReadTimeout):
            await first.process_event("ses_1", event)
        stored = await repo.get("ops.tool_calls", "ses_1:call")
        assert stored["status"] == "DONE" and not stored.get("returned_at")
        resumed = SessionService(repo, opts, dispatch, http=http)
        await resumed.process_event("ses_1", event)
        await resumed.process_event("ses_1", event)
    assert dispatched == ["fetch_page"]
    assert len(posts) == 2 and posts[0] == posts[1]
    stored = await repo.get("ops.tool_calls", "ses_1:call")
    assert stored["status"] == "DONE" and stored["returned_at"]
    assert stored["native_turn_call_index"] == 0
    assert (await repo.get("ops.agent_sessions", "ses_1"))["status"] == "OPEN"

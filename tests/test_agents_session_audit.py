import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from tank.agents import SessionService
from tank.storage import MemoryRepository
from tank.util import iso


def settings(replay=False):
    config = {
        "agent_id": "agent",
        "environment_id": "live",
        "replay_environment_id": "replay",
        "prompt_version": "prompt",
        "reasoning_effort": "medium",
    }
    return SimpleNamespace(
        openma_url="http://oma",
        openma_api_key="fake-only",
        openma_model="gpt-6.1-sol",
        config_version="current-config",
        code_version="current-code",
        replay=replay,
        load_config=lambda name: config,
    )


async def seed(repo, **changes):
    record = {
        "session_id": "s",
        "event_id": "e",
        "item_id": "one",
        "status": "OPEN",
        "model_id": "gpt-6.1-sol",
        "prompt_version": "prompt",
        "effort": "medium",
        "runtime_status": "RUNNING",
        "messages": [],
        "message_log": [],
    }
    record.update(changes)
    await repo.put("ops.agent_sessions", "s", record)
    return record


@pytest.mark.asyncio
async def test_refused_tool_reason_survives_lost_reply_and_does_not_stall():
    repo = MemoryRepository()
    await seed(repo)
    calls = []
    posts = []

    async def dispatch(*args):
        calls.append(args)
        await repo.put("evt.events", "bad", {"event_id": "bad"})
        raise ValueError("V7 freshness is inconsistent")

    def lost(request):
        posts.append(json.loads(request.content))
        raise httpx.ReadError("lost", request=request)

    event = {"id": "refused", "type": "agent.custom_tool_use", "name": "record_event", "input": {}}
    async with httpx.AsyncClient(transport=httpx.MockTransport(lost)) as http:
        with pytest.raises(httpx.ReadError):
            await SessionService(repo, settings(), dispatch, http=http).process_event("s", event, 3)
    assert await repo.get("evt.events", "bad") is None
    assert (await repo.get("ops.tool_calls", "s:refused"))["status"] == "REFUSED"

    def accepted(request):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as http:
        await SessionService(repo, settings(), dispatch, http=http).process_event("s", event, 3)
    assert len(calls) == 1 and posts[0] == posts[1]
    response = posts[1]["events"][0]
    assert response["is_error"] is True
    assert json.loads(response["content"][0]["text"])["error"]["reason"] == "V7 freshness is inconsistent"
    assert (await repo.get("ops.agent_sessions", "s"))["last_event_seq"] == 3


@pytest.mark.asyncio
async def test_failed_turn_retries_original_message_and_new_message_cannot_reset_retry():
    repo = MemoryRepository()
    await seed(repo, runtime_status="IDLE", reservation_id="reservation")
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    await repo.put(
        "ops.routing_reservations",
        "reservation",
        {"reservation_id": "reservation", "first_item_id": "one", "status": "PENDING", "last_item_at": iso()},
    )
    posts = []
    await repo.put("ingest.source_items", "one", {"item_id": "one", "text": "first"})
    await repo.put("ingest.source_items", "two", {"item_id": "two", "text": "second"})

    def handler(request):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http)
        service.start_stream = lambda sid: None
        await service.send_message({"kind": "NEW_ITEM", "item_id": "one", "event_id": "e"})
        await service.process_event("s", {"id": "running", "type": "session.status_running"})
        await service.send_message({"kind": "ITEM_ADDED", "item_id": "two", "event_id": "e"})
        await service.process_event("s", {"id": "failure1", "type": "session.error"})
        assert posts[2] == posts[0] and posts[2] != posts[1]
        await service.send_message({"kind": "CONDITION_FIRED", "event_id": "e", "condition_id": "extra"})
        await service.process_event("s", {"id": "failure2", "type": "session.error"})
    session = await repo.get("ops.agent_sessions", "s")
    assert session["status"] == "FAILED" and session["message_log"][0]["retry_count"] == 1
    assert len(posts) == 4
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "FAILED"
    assert (await repo.get("ops.routing_reservations", "reservation"))["status"] == "CLOSED"


@pytest.mark.asyncio
async def test_replay_runs_verdict_code_and_uses_fresh_remote_session():
    repo = MemoryRepository()
    await seed(repo)
    calls = []
    creates = []
    await repo.put(
        "ops.tool_calls",
        "original",
        {
            "call_id": "old",
            "session_id": "old",
            "event_id": "e",
            "tool": "get_decision",
            "arguments": {},
            "status": "DONE",
            "result": {"action": "WAIT"},
        },
    )

    async def dispatch(tool, args, context):
        calls.append(tool)
        return {"action": "EXIT"}

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        body = json.loads(request.content)
        if request.url.path == "/v1/sessions":
            creates.append(body)
            return httpx.Response(200, json={"id": "fresh"})
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(replay=True), dispatch, http=http)
        await service.process_event(
            "s", {"id": "verdict", "type": "agent.custom_tool_use", "name": "get_decision", "input": {}}
        )
        replay = await service.resolve({"event_id": "e", "replay_run_id": "run1"})
        assert replay["session_id"] == "fresh"
        assert (await service.resolve({"event_id": "e", "replay_run_id": "run1"}))["session_id"] == "fresh"
        with pytest.raises(ValueError, match="original session"):
            await service.resolve({"session_id": "s", "replay_run_id": "run1"})
    assert calls == ["get_decision"] and (await repo.get("ops.tool_calls", "s:verdict"))["result"] == {
        "action": "EXIT"
    }
    assert len(creates) == 1 and creates[0]["environment_id"] == "replay"


@pytest.mark.asyncio
@pytest.mark.parametrize("no_event", [False, True])
async def test_follow_stops_idle_sse_and_closes_no_event_session(no_event):
    repo = MemoryRepository()
    await seed(repo, event_id=None if no_event else "e", no_event=no_event)
    if not no_event:
        await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    posts = []
    streamed = []
    cancelled = []

    def handler(request):
        posts.append(request.url.path)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http)

        async def stream(sid):
            streamed.append(sid)
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(sid)

        async def catchup(sid):
            await asyncio.sleep(0)
            await service.process_event(
                sid, {"id": "idle", "type": "session.status_idle", "stop_reason": {"type": "end_turn"}}
            )

        service.stream = stream
        service.catch_up = catchup
        await asyncio.wait_for(service.follow("s"), timeout=1)
    assert streamed == ["s"] and cancelled == ["s"]
    session = await repo.get("ops.agent_sessions", "s")
    assert session["status"] == ("ENDED" if no_event else "OPEN")
    assert posts == (["/v1/sessions/s/archive"] if no_event else [])


@pytest.mark.asyncio
async def test_usage_keeps_effort_stop_reason_and_search_count():
    repo = MemoryRepository()
    session = await seed(repo)
    async with httpx.AsyncClient() as http:
        await SessionService(repo, settings(), None, http=http).usage(
            session,
            {
                "id": "span",
                "type": "span.model_request_end",
                "finish_reason": "tool-calls",
                "model_usage": {
                    "input_tokens": 5,
                    "output_tokens": 2,
                    "server_tool_use": {"web_search_requests": 3},
                },
                "processed_at": iso(),
            },
        )
    usage = await repo.get("ops.llm_usage", "s:span")
    assert (usage["effort"], usage["stop_reason"], usage["web_searches"]) == ("medium", "tool-calls", 3)
    assert usage["billing_basis"] == "SUBSCRIPTION" and usage["cost_usd"] == "0"


@pytest.mark.asyncio
async def test_credential_url_never_reaches_tool_ledger_or_callback():
    from tank.util import canonical_json

    repo = MemoryRepository()
    await seed(repo)
    called = []
    posted = []
    fake = "fake_test_key_91c82"

    async def dispatch(*args):
        called.append(args)
        return {"never": "called"}

    def handler(request):
        posted.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), dispatch, http=http)
        await service.process_event(
            "s",
            {
                "id": "credential",
                "type": "agent.custom_tool_use",
                "name": "fetch_page",
                "input": {"url": "https://api.etherscan.io/v2/api?chainid=1&apikey=" + fake},
            },
        )
    stored = await repo.get("ops.tool_calls", "s:credential")
    assert stored["status"] == "REFUSED" and stored["arguments_redacted"] is True
    assert fake not in canonical_json(stored) and fake not in canonical_json(posted)
    assert called == [] and posted[0]["events"][0]["is_error"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["telegram", "gateway", "dsn"])
async def test_configured_tokens_and_dsn_userinfo_never_enter_session_ledger(case):
    from tank.util import canonical_json

    repo = MemoryRepository()
    await seed(repo)
    opts = settings()
    sentinel = "FAKE_sensitive_sentinel_58"
    if case == "telegram":
        opts.telegram_token = sentinel
        arguments = {"url": "https://api.telegram.org/bot" + sentinel + "/getMe"}
    elif case == "gateway":
        opts.codex_gateway_key = sentinel
        arguments = {"url": "https://example.test/?q=" + sentinel}
    else:
        opts.database_url = "postgresql://tank_agent:" + sentinel + "@postgres/tank"
        arguments = {"url": opts.database_url}
    posted = []

    async def dispatch(*args):
        raise AssertionError("Credential-bearing call reached callback")

    def handler(request):
        posted.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await SessionService(repo, opts, dispatch, http=http).process_event(
            "s",
            {
                "id": "secret_" + case,
                "type": "agent.custom_tool_use",
                "name": "fetch_page",
                "input": arguments,
            },
        )
    stored = await repo.get("ops.tool_calls", "s:secret_" + case)
    assert stored["status"] == "REFUSED" and stored["arguments_redacted"] is True
    assert sentinel not in canonical_json(stored) and sentinel not in canonical_json(posted)

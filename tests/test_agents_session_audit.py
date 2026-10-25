import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from agent_source_fixtures import (
    declared_versions_fixture,
    native_tool_arguments_fixture,
    source_record_fixture,
)

from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.openma_sync import agent_tools
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import clock_iso


def settings(replay=False):
    config = {
        "agent_id": "agent",
        "environment_id": "live",
        "replay_environment_id": "replay",
        "prompt_version": "prompt",
        "reasoning_effort": "medium",
    }
    opts = SimpleNamespace(
        ops_progress_enabled=False,
        thresholds=Settings(_env_file=None).thresholds,
        read_config_json=Settings(_env_file=None).read_config_json,
        openma_url="http://oma",
        openma_api_key="fake-only",
        openma_model="gpt-6.1-sol",
        config_version="current-config",
        code_version="current-code",
        replay=replay,
        load_config=lambda name: {"registry": {"tokens": [{"token_id": "T", "symbol": "T"}]}}.get(
            name, config
        ),
    )
    opts.configured_secrets = lambda: frozenset(
        value
        for name, value in vars(opts).items()
        if isinstance(value, str) and name.endswith(("_key", "_token", "_password"))
    )
    return opts


async def seed(repo, **changes):
    record = {
        "session_id": "s",
        "model_context_version": CONTEXT_VERSION,
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
    from tank.util import sha256

    record["tools_sha256"] = sha256(agent_tools(settings(), "tank-event-analyst"))
    record["prompt_label"] = record["prompt_version"]
    record["prompt_version"] = sha256(
        {"system": "Resolved synthetic fixture prompt", "label": record["prompt_label"]}
    )
    record["prompt_version_basis"] = "RESOLVED_SESSION_AGENT_SNAPSHOT"
    await repo.put("ops.agent_sessions", "s", record)
    return record


@pytest.mark.asyncio
async def test_refused_tool_reason_survives_lost_reply_and_does_not_stall() -> None:
    repo = MemoryRepository()
    await seed(repo)
    calls = []
    posts = []

    async def dispatch(*args) -> None:
        calls.append(args)
        await repo.put("evt.events", "bad", {"event_id": "bad"})
        raise ValueError("V7 freshness is inconsistent")

    def lost(request: httpx.Request) -> None:
        posts.append(json.loads(request.content))
        raise httpx.ReadError("lost", request=request)

    event = {
        "id": "refused",
        "type": "agent.custom_tool_use",
        "name": "record_event",
        "input": native_tool_arguments_fixture("record_event"),
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(lost)) as http:
        with pytest.raises(httpx.ReadError):
            await SessionService(
                repo, settings(), dispatch, http=http, versions=declared_versions_fixture()
            ).process_event("s", event, 3)
    assert await repo.get("evt.events", "bad") is None
    assert (await repo.get("ops.tool_calls", "s:refused"))["status"] == "REFUSED"

    def accepted(request: httpx.Request):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as http:
        await SessionService(
            repo, settings(), dispatch, http=http, versions=declared_versions_fixture()
        ).process_event("s", event, 3)
    assert len(calls) == 1 and posts[0] == posts[1]
    response = posts[1]["events"][0]
    assert response["is_error"] is True
    assert json.loads(response["content"][0]["text"])["error"]["reason"] == "V7 freshness is inconsistent"
    assert (await repo.get("ops.agent_sessions", "s"))["last_event_seq"] == 3


@pytest.mark.asyncio
async def test_failed_turn_retries_original_message_and_new_message_cannot_reset_retry() -> None:
    repo = MemoryRepository()
    await seed(repo, runtime_status="IDLE")
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    posts = []
    await repo.put("ingest.source_items", "one", source_record_fixture({"item_id": "one", "text": "first"}))
    await repo.put("ingest.source_items", "two", source_record_fixture({"item_id": "two", "text": "second"}))

    def handler(request: httpx.Request):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
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
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "TRACKED"


@pytest.mark.asyncio
async def test_replay_runs_verdict_code_and_uses_fresh_remote_session() -> None:
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
        if context.get("_decision_market_refresh_only"):
            return {"market_refresh_completed": True, "prepared_condition_ids": []}
        calls.append(tool)
        return {"action": "EXIT"}

    def handler(request: httpx.Request):
        if request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        body = json.loads(request.content)
        if request.url.path == "/v1/sessions":
            creates.append(body)
            return httpx.Response(200, json={"id": "fresh"})
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo, settings(replay=True), dispatch, http=http, versions=declared_versions_fixture()
        )
        await service.process_event(
            "s",
            {
                "id": "verdict",
                "type": "agent.custom_tool_use",
                "name": "get_decision",
                "input": {"token_id": "T"},
            },
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
async def test_follow_retains_open_idle_sse_and_keeps_rejected_material_session_open(no_event) -> None:
    repo = MemoryRepository()
    await seed(
        repo,
        event_id=None if no_event else "e",
        no_event=no_event,
        message_log=[
            {"message_id": "initial", "remote_event_id": "accepted-initial", "turn_status": "RUNNING"}
        ],
        active_message_id="initial",
    )
    if not no_event:
        await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    posts = []
    streamed = []
    cancelled = []
    idle_seen = asyncio.Event()

    def handler(request: httpx.Request):
        if request.method == "POST":
            posts.append(request.url.path)
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                        for identity in request.url.params.get_list("ids[]")
                    ]
                },
            )
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())

        async def stream(sid) -> None:
            streamed.append(sid)
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(sid)

        async def catchup(sid) -> None:
            await asyncio.sleep(0)
            await service.process_event(
                sid,
                {
                    "id": "idle",
                    "type": "session.status_idle",
                    "execution_id": "accepted-initial",
                    "stop_reason": {"type": "end_turn"},
                },
            )
            idle_seen.set()

        service.stream = stream
        service.catch_up = catchup
        follower = asyncio.create_task(service.follow("s"))
        await asyncio.wait_for(idle_seen.wait(), timeout=1)
        if no_event:
            await asyncio.wait_for(follower, timeout=1)
        else:
            await asyncio.sleep(0)
            assert not follower.done()
            follower.cancel()
            await asyncio.gather(follower, return_exceptions=True)
    assert streamed == ["s"] and cancelled == ["s"]
    session = await repo.get("ops.agent_sessions", "s")
    assert session["status"] == "OPEN" and posts == []
    assert bool(session.get("archive_after")) == no_event


@pytest.mark.asyncio
async def test_usage_keeps_effort_stop_reason_and_search_count() -> None:
    repo = MemoryRepository()
    session = await seed(repo)
    async with httpx.AsyncClient() as http:
        await SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture()).usage(
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
                "processed_at": clock_iso(),
            },
        )
    usage = await repo.get("ops.llm_usage", "s:span")
    assert (usage["effort"], usage["stop_reason"], usage["web_searches"]) == ("medium", "tool-calls", 3)
    assert usage["billing_basis"] == "SUBSCRIPTION" and usage["cost_usd"] == "0"


@pytest.mark.asyncio
async def test_credential_url_never_reaches_tool_ledger_or_callback() -> None:
    from tank.util import canonical_json

    repo = MemoryRepository()
    await seed(repo)
    called = []
    posted = []
    fake = "fake_test_key_91c82"

    async def dispatch(*args):
        called.append(args)
        return {"never": "called"}

    def handler(request: httpx.Request):
        posted.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), dispatch, http=http, versions=declared_versions_fixture())
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
async def test_configured_tokens_and_dsn_userinfo_never_enter_session_ledger(case) -> None:
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

    async def dispatch(*args) -> None:
        raise AssertionError("Credential-bearing call reached callback")

    def handler(request: httpx.Request):
        posted.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await SessionService(
            repo, opts, dispatch, http=http, versions=declared_versions_fixture()
        ).process_event(
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


@pytest.mark.asyncio
async def test_legacy_prompt_label_requires_exact_native_snapshot() -> None:
    repo = MemoryRepository()
    session = await seed(repo)
    session.update(prompt_version="baseline-2.3", prompt_version_basis=None, prompt_label=None)
    requests = []
    snapshot = {
        "id": "agent",
        "version": 9,
        "system": "Resolved old session bytes",
        "skills": [],
        "tools": [],
        "model": {"id": "gpt-6.1-sol"},
    }

    def transport(request: httpx.Request):
        requests.append(request.url.path)
        return httpx.Response(200, json={"data": {"id": "s", "agent": snapshot}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        await service.hydrate_prompt_provenance(session)
        actual = session["prompt_version"]
        await service.hydrate_prompt_provenance(session)
    assert requests == ["/v1/sessions/s"]
    assert len(actual) == 64 and actual != "baseline-2.3"
    assert session["legacy_prompt_descriptor"]["prompt_version"] == "baseline-2.3"
    assert session["prompt_label"] == "baseline-2.3"
    assert session["recovered_prompt_descriptor"]["agent_version"] == "9"


@pytest.mark.asyncio
async def test_native_queued_tool_result_executions_continue_after_transient_idle() -> None:
    """Native r8 rev30 idle preceded queued executions rev31..52, not session completion."""
    repo = MemoryRepository()
    await seed(repo)
    queue = asyncio.Queue()
    first_idle = asyncio.Event()
    all_results = asyncio.Event()
    calls, responses, history = [], [], []
    names = [
        "get_item",
        "get_registry",
        "list_open_events",
        "fetch_page",
        "search_tank_items",
        "record_event",
        "search_tank_items",
    ]

    async def dispatch(name, arguments, context):
        calls.append(name)
        if name == "record_event":
            await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "ANALYZING"})
        return {"accepted": name}

    async def emit(event) -> None:
        history.append(event)
        await queue.put(("data: " + json.dumps(event) + "\n\n").encode())

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                yield await queue.get()

    async def transport(request: httpx.Request):
        if request.url.path.endswith("/events/stream"):
            return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})
        if request.url.path.endswith("/events") and request.method == "GET":
            return httpx.Response(200, json={"data": list(history)})
        if request.url.path.endswith("/events"):
            result = json.loads(request.content)["events"][0]
            responses.append(result["custom_tool_use_id"])
            if len(responses) == 3:
                await emit(
                    {"id": "native-rev30", "type": "session.status_idle", "stop_reason": {"type": "end_turn"}}
                )
            if len(responses) == 7:
                all_results.set()
            return httpx.Response(200, json={})
        raise AssertionError("Unexpected native request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SessionService(repo, settings(), dispatch, http=http, versions=declared_versions_fixture())
        original_process = service.process_event

        async def process(sid, event, seq=None) -> None:
            await original_process(sid, event, seq)
            if event["id"] == "native-rev30":
                first_idle.set()

        service.process_event = process
        service.start_stream("s")
        for number, name in enumerate(names[:3]):
            await emit(
                {
                    "id": f"call-{number}",
                    "type": "agent.custom_tool_use",
                    "name": name,
                    "input": {"query": "Synthetic evidence"}
                    if name == "search_tank_items"
                    else native_tool_arguments_fixture(name)
                    if name in {"record_event", "fetch_page"}
                    else {},
                }
            )
        await asyncio.wait_for(first_idle.wait(), timeout=2)

        for number, name in enumerate(names[3:], start=3):
            await emit({"id": f"run-{number}", "type": "session.status_running"})
            await emit(
                {
                    "id": f"call-{number}",
                    "type": "agent.custom_tool_use",
                    "name": name,
                    "input": {"query": "Synthetic evidence"}
                    if name == "search_tank_items"
                    else native_tool_arguments_fixture(name)
                    if name in {"record_event", "fetch_page"}
                    else {},
                }
            )
            await emit(
                {"id": f"idle-{number}", "type": "session.status_idle", "stop_reason": {"type": "end_turn"}}
            )
        try:
            await asyncio.wait_for(all_results.wait(), timeout=2)
            await service.catch_up("s")
            assert calls == names
            assert len(set(responses)) == 7
            rows = await repo.list("ops.tool_calls")
            assert len(rows) == 7 and all(row["status"] == "DONE" and row.get("returned_at") for row in rows)
            assert (await repo.get("ops.agent_sessions", "s"))["last_event_id"] == "idle-6"
            assert not service.tasks["s"].done()
        finally:
            await service.close()


@pytest.mark.asyncio
async def test_supervisor_resumes_open_idle_session_following_after_restart() -> None:
    repo = MemoryRepository()
    await seed(repo, runtime_status="IDLE")
    service = SessionService(repo, settings(), None, http=object(), versions=declared_versions_fixture())
    stop = asyncio.Event()
    followed = []

    def start(sid) -> None:
        followed.append(sid)
        stop.set()

    service.start_stream = start
    await service.run(stop)
    assert followed == ["s"]
    assert (await repo.get("ops.agent_sessions", "s"))["status"] == "OPEN"


@pytest.mark.asyncio
async def test_production_sse_pool_is_independent_unbounded_and_closed_with_control() -> None:
    service = SessionService(MemoryRepository(), settings(), None, versions=declared_versions_fixture())
    control, streaming = service.http, service.stream_http
    assert control is not streaming
    assert control._transport._pool is not streaming._transport._pool
    import sys

    assert streaming._transport._pool._max_connections == sys.maxsize
    assert streaming.timeout.read is None
    assert control._transport._pool._max_connections == 100
    await service.close()
    assert control.is_closed and streaming.is_closed


@pytest.mark.asyncio
async def test_control_result_progresses_while_independent_sse_request_remains_open() -> None:
    repo = MemoryRepository()
    await seed(repo)
    entered, release, acknowledged = asyncio.Event(), asyncio.Event(), asyncio.Event()
    stream_closed = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            entered.set()
            await release.wait()
            if False:
                yield b""

        async def aclose(self) -> None:
            stream_closed.append(True)

    async def stream_transport(request: httpx.Request):
        assert request.url.path.endswith("/events/stream")
        return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})

    async def control_transport(request: httpx.Request):
        assert request.method == "POST" and request.url.path.endswith("/events")
        acknowledged.set()
        return httpx.Response(200, json={})

    async with (
        httpx.AsyncClient(
            transport=httpx.MockTransport(control_transport), limits=httpx.Limits(max_connections=1)
        ) as control,
        httpx.AsyncClient(
            transport=httpx.MockTransport(stream_transport),
            limits=httpx.Limits(max_connections=1),
            timeout=None,
        ) as streaming,
    ):
        service = SessionService(
            repo, settings(), None, http=control, stream_http=streaming, versions=declared_versions_fixture()
        )
        task = asyncio.create_task(service.stream("s"))
        await asyncio.wait_for(entered.wait(), timeout=1)
        try:
            await asyncio.wait_for(
                service.request("POST", "sessions/s/events", json={"events": []}), timeout=1
            )
            assert acknowledged.is_set() and not task.done()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await service.close()
    assert stream_closed == [True]


@pytest.mark.asyncio
async def test_unbound_terminal_turn_failure_retries_once_then_marks_source_failed_after_restart() -> None:
    repo = MemoryRepository()
    await seed(repo, event_id=None, runtime_status="IDLE")
    await repo.put(
        "ingest.source_items",
        "one",
        source_record_fixture({"item_id": "one", "text": "Original item", "extraction_status": "PENDING"}),
    )
    posts = []

    def transport(request: httpx.Request):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        first = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        first.start_stream = lambda sid: None
        await first.send_message({"kind": "NEW_ITEM", "item_id": "one"})
        await first.process_event("s", {"id": "error-one", "type": "session.error"})
        assert posts[1] == posts[0]
        assert (await repo.get("ingest.source_items", "one"))["extraction_status"] == "PENDING"
        resumed = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        async with repo.lock("routing"):
            await asyncio.wait_for(
                resumed.process_event("s", {"id": "error-two", "type": "session.error"}), timeout=1
            )
        await resumed.process_event("s", {"id": "error-two", "type": "session.error"})
    session = await repo.get("ops.agent_sessions", "s")
    assert session["status"] == "FAILED"
    assert session["message_log"][0]["retry_count"] == 1
    assert session["message_log"][0]["turn_status"] == "FAILED"
    assert (await repo.get("ingest.source_items", "one"))["extraction_status"] == "FAILED"
    assert len(posts) == 2 and not await repo.list("ops.tool_calls")


@pytest.mark.parametrize("http_status", [404, 503])
async def test_new_replay_session_restores_captured_external_error_and_acknowledges_once(http_status) -> None:
    repo = MemoryRepository()
    await seed(repo, replay_source_session_id="original")
    result = {
        "ok": False,
        "error": {
            "type": "HTTPStatusError",
            "reason": f"Source request failed (HTTP {http_status})",
            "category": "SOURCE_HTTP",
        },
    }
    args = {"url": "https://captured.invalid/unavailable"}
    await repo.put(
        "ops.tool_calls",
        "original:old-error",
        {
            "call_id": "old-error",
            "session_id": "original",
            "tool": "fetch_page",
            "arguments": args,
            "status": "ERROR",
            "is_error": True,
            "result": result,
            "returned_at": clock_iso(),
        },
    )
    posts = []

    async def forbidden(*arguments) -> None:
        raise AssertionError("Captured external errors must never issue a new fetch")

    def control(request: httpx.Request):
        posts.append(json.loads(request.content)["events"][0])
        if len(posts) == 1:
            raise httpx.ConnectError("Synthetic lost native acknowledgement", request=request)
        return httpx.Response(200, json={})

    event = {"id": "fresh-error", "type": "agent.custom_tool_use", "name": "fetch_page", "input": args}
    async with httpx.AsyncClient(transport=httpx.MockTransport(control)) as http:
        first = SessionService(
            repo, settings(replay=True), forbidden, http=http, versions=declared_versions_fixture()
        )
        with pytest.raises(httpx.ConnectError):
            await first.process_event("s", event)
        replayed = await repo.get("ops.tool_calls", "s:fresh-error")
        assert replayed["status"] == "ERROR" and replayed["result"] == result
        resumed = SessionService(
            repo, settings(replay=True), forbidden, http=http, versions=declared_versions_fixture()
        )
        await resumed.process_event("s", event)
        await resumed.process_event("s", event)
    assert len(posts) == 2 and posts[0] == posts[1]
    assert posts[1]["is_error"] is True
    assert json.loads(posts[1]["content"][0]["text"]) == result
    assert (await repo.get("ops.tool_calls", "s:fresh-error"))["returned_at"]
    assert (await repo.get("ops.agent_sessions", "s"))["status"] == "OPEN"

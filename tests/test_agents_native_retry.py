"""Native retry notifications are not terminal host-attempt failures."""

import copy
import json
from types import SimpleNamespace

import httpx
import pytest

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository

INITIAL = "sevt_req_2e22f77108bd91574b7c9dc221bc999b7f504c6ef8a4b9ab1e7af3c3445fd864_0"
CALLBACK = "sevt_req_307a37e6867554883982be7e265843eb3c8a8f4d2244517084ecd68c79da84cf_0"
RETRY = "sevt_req_81c757631379004a5af5ed4330bff07fc2d423d27d377ad45b45e87df8fc5010_0"


def event(identity, kind, execution, **fields):
    return {
        "id": identity,
        "type": kind,
        "execution_id": execution,
        "processed_at": "2026-10-19T21:23:20.427Z",
        **fields,
    }


def error(identity, execution, status):
    return event(
        identity,
        "session.error",
        execution,
        error={"type": "unknown_error", "message": "transport unavailable", "retry_status": {"type": status}},
    )


async def setup(recovery_flags=None, ready_calls=None):
    repo = MemoryRepository()
    session = {
        "session_id": "s",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "item_id": "item",
        "active_message_id": "message",
        "messages": ["message"],
        "message_log": [
            {
                "message_id": "message",
                "body": {"type": "user.message", "content": [{"type": "text", "text": "original input"}]},
                "payload": {"kind": "SCOUT_REVIEW", "item_id": "item"},
                "remote_event_id": INITIAL,
                "retry_count": 0,
                "turn_status": "RUNNING",
            }
        ],
    }
    await repo.put("ops.agent_sessions", "s", session)
    await repo.put("ingest.source_items", "item", {"item_id": "item", "extraction_status": "DONE"})
    await repo.put(
        "ops.tool_calls",
        "s:original-tool",
        {
            "call_id": "original-tool",
            "session_id": "s",
            "tool": "get_item",
            "status": "DONE",
            "returned_at": "2026-10-19T21:23:00Z",
            "remote_event_id": CALLBACK,
            "native_turn_message_id": "message",
            "native_turn_retry_count": 0,
        },
    )
    states = {INITIAL: "COMPLETED", CALLBACK: "FAILED", RETRY: "RUNNING"}
    posts, history = [], []

    def handler(request):
        if request.method == "POST":
            assert request.url.path == "/v1/sessions/s/events"
            posts.append({"body": json.loads(request.content), "key": request.headers["idempotency-key"]})
            return httpx.Response(200, json={"data": [{"id": RETRY}]})
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": identity,
                            "state": states[identity],
                            "settled_outcome": states[identity]
                            if states[identity] in {"COMPLETED", "FAILED", "CANCELLED"}
                            else None,
                            **(recovery_flags or {}).get(identity, {}),
                            **(
                                {"ready_tool_call_ids": (ready_calls or {}).get(identity, [])}
                                if request.url.params.get("include") == "tool_readiness"
                                else {}
                            ),
                        }
                        for identity in request.url.params.get_list("ids[]")
                    ]
                },
            )
        assert request.url.path.endswith("/events")
        return httpx.Response(200, json={"data": history})

    settings = SimpleNamespace(
        openma_url="http://native",
        openma_api_key="fake-test",
        replay=False,
        openma_model="test",
        config_version="test",
        code_version="test",
        load_config=lambda name: {},
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return repo, SessionService(repo, settings, None, http=http), http, states, posts, history


@pytest.mark.parametrize("outcome", ["COMPLETED", "EXHAUSTED", "QUEUE_FAILED", "TERMINATED"])
async def test_retained_old_exhausted_new_retrying_sequence_waits_for_actual_outcome(outcome):
    repo, service, http, states, posts, history = await setup()
    try:
        await service.process_event("s", error("old-exhausted", CALLBACK, "exhausted"))
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][0]["retry_count"] == 1
        assert current["message_log"][0]["remote_event_id"] == RETRY
        assert len(posts) == 1
        await service.process_event("s", event("new-running", "session.status_running", RETRY))
        retrying = error("new-retrying", RETRY, "retrying")
        rescheduled = event("new-rescheduled", "session.status_rescheduled", RETRY)
        await service.process_event("s", retrying)
        await service.process_event("s", rescheduled)
        await service.process_event("s", retrying)
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN"
        assert current["runtime_status"] == "PENDING"
        assert current["active_message_id"] == "message"
        assert current["message_log"][0]["turn_status"] == "RUNNING"
        assert current["message_log"][0]["retry_count"] == 1
        assert "terminal_failure" not in current and not current.get("pending_retry_event")
        assert len(posts) == 1
        assert not (await service.completed_turn("s"))["complete"]

        await service.process_event("s", error("late-old-exhausted", CALLBACK, "exhausted"))
        assert (await repo.get("ops.agent_sessions", "s"))["status"] == "OPEN"
        if outcome == "COMPLETED":
            states[RETRY] = "COMPLETED"
            ended = event("new-ended", "session.status_idle", RETRY, stop_reason={"type": "end_turn"})
            history.append(ended)
            await service.process_event("s", ended)
            assert (await service.completed_turn("s"))["complete"]
            current = await repo.get("ops.agent_sessions", "s")
            assert current["status"] == "OPEN" and current["message_log"][0]["turn_status"] == "DONE"
        elif outcome == "EXHAUSTED":
            states[RETRY] = "FAILED"
            await service.process_event("s", error("new-exhausted", RETRY, "exhausted"))
            current = await repo.get("ops.agent_sessions", "s")
            assert current["status"] == "FAILED"
            assert current["terminal_failure"]["execution_id"] == RETRY
            assert current["terminal_failure"]["event_id"] == "new-exhausted"
        elif outcome == "QUEUE_FAILED":
            states[RETRY] = "FAILED"
            service.execution_polls.pop("s", None)
            service.history_polls.pop("s", None)
            assert not (await service.completed_turn("s"))["complete"]
            current = await repo.get("ops.agent_sessions", "s")
            assert current["status"] == "FAILED"
            assert current["terminal_failure"]["execution_id"] == RETRY
        else:
            await service.process_event("s", event("terminated", "session.status_terminated", RETRY))
            current = await repo.get("ops.agent_sessions", "s")
            assert current["status"] == "ENDED" and current["active_message_id"] is None
            assert current["message_log"][0]["turn_status"] == "FAILED"
        assert len(posts) == 1
    finally:
        await service.close()
        await http.aclose()


async def test_restart_keeps_native_retry_execution_and_does_not_post_again():
    repo, service, http, states, posts, history = await setup()
    try:
        await service.process_event("s", error("old-exhausted", CALLBACK, "exhausted"))
        await service.process_event("s", error("native-retrying", RETRY, "retrying"))
        await service.process_event("s", event("rescheduled", "session.status_rescheduled", RETRY))
        settings = service.settings
        await service.close()
        service = SessionService(repo, settings, None, http=http)
        await service.process_event("s", error("native-retrying", RETRY, "retrying"))
        assert not (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN" and current["message_log"][0]["retry_count"] == 1
        assert len(posts) == 1
        states[RETRY] = "COMPLETED"
        ended = event("new-ended", "session.status_idle", RETRY, stop_reason={"type": "end_turn"})
        history.append(ended)
        await service.process_event("s", ended)
        assert (await service.completed_turn("s"))["complete"]
        assert len(posts) == 1
    finally:
        await service.close()
        await http.aclose()


async def test_native_startup_retry_does_not_consume_first_host_retry():
    repo, service, http, states, posts, _history = await setup()
    try:
        states[INITIAL] = "RUNNING"
        await service.process_event("s", error("startup-retrying", INITIAL, "retrying"))
        await service.process_event("s", event("startup-rescheduled", "session.status_rescheduled", INITIAL))
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN" and current["message_log"][0]["retry_count"] == 0
        assert not posts
        states[CALLBACK] = "COMPLETED"
        await service.process_event("s", error("startup-exhausted", INITIAL, "exhausted"))
        assert not posts
        states[INITIAL] = "FAILED"
        service.execution_polls.pop("s", None)
        await service.completed_turn("s")
        assert len(posts) == 1
        assert (await repo.get("ops.agent_sessions", "s"))["message_log"][0]["retry_count"] == 1
    finally:
        await service.close()
        await http.aclose()


async def test_unknown_execution_retry_notification_cannot_adopt_the_active_turn():
    repo, service, http, _states, posts, _history = await setup()
    try:
        before = copy.deepcopy(await repo.get("ops.agent_sessions", "s"))
        with pytest.raises(ValueError, match="no acknowledged accepted-input owner"):
            await service.process_event("s", error("unknown-retrying", "unknown-input", "retrying"))
        assert await repo.get("ops.agent_sessions", "s") == before
        assert not posts
    finally:
        await service.close()
        await http.aclose()


async def test_uncorrelated_terminal_error_leaves_healthy_turn_eligible_to_settle():
    repo, service, http, states, posts, history = await setup()
    try:
        states[CALLBACK] = "COMPLETED"
        terminal = error("unknown-terminal", "unknown-input", "terminal")
        history.append(terminal)
        await service.process_event("s", terminal)
        ended = event("ended", "session.status_idle", CALLBACK, stop_reason={"type": "end_turn"})
        history.append(ended)
        await service.process_event("s", ended)
        assert (await service.completed_turn("s"))["complete"]
        session = await repo.get("ops.agent_sessions", "s")
        assert session["message_log"][0]["turn_status"] == "DONE"
        assert not posts
    finally:
        await service.close()
        await http.aclose()


@pytest.mark.parametrize(
    ("projection_fields", "refused"),
    [({}, False), ({"recovery_refused": "true"}, False), ({"recovery_refused": True}, True)],
)
async def test_terminal_error_waits_for_correlated_queue_failure(projection_fields, refused):
    repo, service, http, states, posts, history = await setup({INITIAL: projection_fields})
    try:
        states[INITIAL] = "RUNNING"
        states[CALLBACK] = "COMPLETED"
        terminal = error("terminal", INITIAL, "terminal")
        terminal["error"]["message"] = "Recovery refused ambiguous committed output"
        history.append(terminal)
        await service.process_event("s", terminal)
        await service.process_event("s", terminal)
        assert not (await service.completed_turn("s"))["complete"]
        running = await repo.get("ops.agent_sessions", "s")
        assert running["status"] == "OPEN"
        assert running["message_log"][0]["retry_count"] == 0
        assert not running.get("pending_retry_event")
        assert not posts

        settings = service.settings
        await service.close()
        service = SessionService(repo, settings, None, http=http)
        states[INITIAL] = "FAILED"
        assert not (await service.completed_turn("s"))["complete"]
        await service.process_event("s", terminal)
        await service.completed_turn("s")
        current = await repo.get("ops.agent_sessions", "s")
        message = current["message_log"][0]
        if refused:
            assert not posts
            assert current["status"] == "FAILED"
            assert current["retired_message_ids"] == ["message"]
            assert message["turn_status"] == "FAILED"
            assert message["remote_event_id"] == INITIAL and message["retry_count"] == 0
            assert message["terminal_failure"] == current["terminal_failure"]
            assert current["terminal_failure"]["reason"] == "RECOVERY_REFUSED"
            assert current["terminal_failure"]["queue_status"]["recovery_refused"] is True
        else:
            assert current["status"] == "OPEN"
            assert message["remote_event_id"] == RETRY and message["retry_count"] == 1
            assert [post["key"] for post in posts] == ["turn-retry:s:message"]
    finally:
        await service.close()
        await http.aclose()


async def test_restored_retry_intent_rechecks_durable_no_replay_status():
    repo, service, http, states, posts, _history = await setup({INITIAL: {"recovery_refused": True}})
    try:
        states[INITIAL] = "FAILED"
        session = await repo.get("ops.agent_sessions", "s")
        stale = {"execution_id": INITIAL, "state": "FAILED", "settled_outcome": "FAILED"}
        session["native_execution_states"] = {INITIAL: stale}
        session["pending_retry_event"] = event(
            "execution-status-failed:" + INITIAL,
            "execution.status_failed",
            INITIAL,
            queue_status=stale,
            _host_turn_message_id="message",
        )
        await repo.put("ops.agent_sessions", "s", session)
        await service.recover_outbound_inputs("s")
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][0]["remote_event_id"] == INITIAL
        assert not current["pending_retry_event"]
        assert not posts
    finally:
        await service.close()
        await http.aclose()


@pytest.mark.parametrize("terminal_state", ["FAILED", "CANCELLED"])
@pytest.mark.parametrize("unseen_tool", [False, True])
async def test_refusal_retires_only_its_owner_and_blocks_catch_up_tools(terminal_state, unseen_tool):
    repo, service, http, states, posts, history = await setup({CALLBACK: {"recovery_refused": True}})
    try:
        states[CALLBACK] = "RUNNING"
        await service.process_event("s", error("before-settlement", CALLBACK, "exhausted"))
        assert not posts
        session = await repo.get("ops.agent_sessions", "s")
        original = session["message_log"][0]
        original["payload"]["kind"] = "NEW_ITEM"
        queued = {
            **copy.deepcopy(original),
            "message_id": "queued",
            "remote_event_id": "queued-input",
            "payload": {"kind": "SCOUT_REVIEW"},
            "turn_status": "PENDING",
        }
        session["message_log"].append(queued)
        await repo.put("ops.agent_sessions", "s", session)
        await repo.put("ingest.source_items", "item", {"item_id": "item", "extraction_status": "PENDING"})
        await repo.put(
            "ops.tool_calls",
            "s:unreturned-tool",
            {
                "call_id": "unreturned-tool",
                "session_id": "s",
                "native_turn_message_id": "message",
                "status": "PENDING",
            },
        )
        states["queued-input"] = "QUEUED"
        session = await repo.get("ops.agent_sessions", "s")
        await service.execution_barrier(session, verify_remote=True)

        async def unexpected_tool(*args):
            pytest.fail("Catch-up dispatched a tool from a refused execution")

        service.tool = unexpected_tool
        if unseen_tool:
            history.append(
                event("unseen-tool", "agent.custom_tool_use", CALLBACK, name="send_telegram_file", input={})
            )
            await service.catch_up("s")
            current = await repo.get("ops.agent_sessions", "s")
            assert "unseen-tool" not in current.get("processed_events", [])
            assert await repo.get("ops.tool_calls", "s:unseen-tool") is None
        else:
            service.execution_polls.pop("s", None)
        states[CALLBACK] = terminal_state

        assert not (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN"
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][0]["terminal_failure"]["execution_id"] == CALLBACK
        assert current["native_execution_states"][CALLBACK]["recovery_refused"] is True
        assert current["message_log"][1] == queued
        assert (await repo.get("ingest.source_items", "item"))["extraction_reason"] == "SESSION_FAILED"

        await service.process_event("s", event("queued-running", "session.status_running", "queued-input"))
        await service.process_event("s", error("late-terminal", CALLBACK, "terminal"))
        assert (await repo.get("ops.agent_sessions", "s"))["active_message_id"] == "queued"
        states["queued-input"] = "COMPLETED"
        ended = event("queued-ended", "session.status_idle", "queued-input", stop_reason={"type": "end_turn"})
        history.append(ended)
        await service.process_event("s", ended)
        assert (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert [row["turn_status"] for row in current["message_log"]] == ["FAILED", "DONE"]
        assert "completion_checked" not in current["message_log"][0]
        assert not posts
    finally:
        await service.close()
        await http.aclose()


async def test_refusal_of_superseded_attempt_cannot_fail_replacement():
    repo, service, http, states, posts, history = await setup({CALLBACK: {"recovery_refused": True}})
    try:
        states[INITIAL] = "FAILED"
        states[CALLBACK] = "RUNNING"
        await service.process_event("s", error("old-exhausted", INITIAL, "exhausted"))
        states[CALLBACK] = "FAILED"
        await service.process_event("s", event("replacement-running", "session.status_running", RETRY))
        await service.process_event("s", error("old-terminal", CALLBACK, "terminal"))
        assert not (await service.completed_turn("s"))["complete"]
        session = await repo.get("ops.agent_sessions", "s")
        assert session["status"] == "OPEN"
        assert session["message_log"][0]["remote_event_id"] == RETRY
        assert session["message_log"][0]["turn_status"] == "RUNNING"
        assert not session.get("retired_message_ids")
        assert not session.get("terminal_failure")

        states[RETRY] = "COMPLETED"
        ended = event("replacement-ended", "session.status_idle", RETRY, stop_reason={"type": "end_turn"})
        history.append(ended)
        await service.process_event("s", ended)
        assert (await service.completed_turn("s"))["complete"]
        assert [post["key"] for post in posts] == ["turn-retry:s:message"]
    finally:
        await service.close()
        await http.aclose()


@pytest.mark.parametrize("phase_state", ["QUEUED", "RUNNING", "COMPLETED"])
async def test_history_tool_waits_for_its_exact_committed_callback(phase_state):
    ready = {CALLBACK: ["other-call"]}
    repo, service, http, states, posts, history = await setup(ready_calls=ready)
    call = event("replayed-call", "agent.custom_tool_use", CALLBACK, name="get_item", input={})
    history.extend(
        [
            call,
            event(
                "unsettled-idle",
                "session.status_idle",
                CALLBACK,
                stop_reason={"type": "requires_action", "event_ids": [call["id"]]},
            ),
        ]
    )
    states[CALLBACK] = "RUNNING"
    dispatched = []

    async def dispatch(name, arguments, context):
        dispatched.append(name)
        return {"item_id": "item"}

    service.tool_dispatch = dispatch
    try:
        await service.catch_up("s")
        current = await repo.get("ops.agent_sessions", "s")
        assert current["processed_events"] == ["unsettled-idle"]
        assert not dispatched and not posts
        assert await repo.get("ops.tool_calls", "s:replayed-call") is None

        ready[CALLBACK].append(call["id"])
        states[CALLBACK] = phase_state
        await service.catch_up("s")
        await service.catch_up("s")
        assert dispatched == ["get_item"]
        assert len(posts) == 1
        assert (await repo.get("ops.tool_calls", "s:replayed-call"))["remote_event_id"] == RETRY
    finally:
        await service.close()
        await http.aclose()


async def test_history_tool_reports_invalid_readiness_without_dispatch_or_dedup():
    repo, service, http, states, posts, history = await setup(ready_calls={CALLBACK: None})
    states[CALLBACK] = "RUNNING"
    history.append(event("invalid-readiness", "agent.custom_tool_use", CALLBACK, name="get_item", input={}))
    try:
        with pytest.raises(ValueError, match="tool readiness projection"):
            await service.catch_up("s")
        current = await repo.get("ops.agent_sessions", "s")
        assert "invalid-readiness" not in current.get("processed_events", [])
        assert await repo.get("ops.tool_calls", "s:invalid-readiness") is None
        assert not posts
    finally:
        await service.close()
        await http.aclose()


async def test_live_tool_waits_for_committed_phase():
    ready = {CALLBACK: []}
    repo, service, http, states, posts, _ = await setup(ready_calls=ready)
    states[CALLBACK] = "RUNNING"
    dispatched = []

    async def dispatch(name, arguments, context):
        dispatched.append(name)
        return {"item_id": "item"}

    service.tool_dispatch = dispatch
    try:
        call = event("live-call", "agent.custom_tool_use", CALLBACK, name="get_item", input={})
        assert await service.process_event("s", call, from_native=True) is False
        assert not dispatched and not posts
        assert await repo.get("ops.tool_calls", "s:live-call") is None
        ready[CALLBACK].append(call["id"])
        await service.process_event("s", call, from_native=True)
        await service.process_event("s", call, from_native=True)
        assert dispatched == ["get_item"] and len(posts) == 1
    finally:
        await service.close()
        await http.aclose()

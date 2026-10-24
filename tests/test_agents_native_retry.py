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


async def setup():
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

    def handler(request: pytest.FixtureRequest):
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
async def test_retained_old_exhausted_new_retrying_sequence_waits_for_actual_outcome(outcome) -> None:
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


async def test_restart_keeps_native_retry_execution_and_does_not_post_again() -> None:
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


async def test_native_startup_retry_does_not_consume_first_host_retry() -> None:
    repo, service, http, states, posts, _history = await setup()
    try:
        states[INITIAL] = "RUNNING"
        await service.process_event("s", error("startup-retrying", INITIAL, "retrying"))
        await service.process_event("s", event("startup-rescheduled", "session.status_rescheduled", INITIAL))
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN" and current["message_log"][0]["retry_count"] == 0
        assert not posts
        await service.process_event("s", error("startup-exhausted", INITIAL, "exhausted"))
        assert len(posts) == 1
        assert (await repo.get("ops.agent_sessions", "s"))["message_log"][0]["retry_count"] == 1
    finally:
        await service.close()
        await http.aclose()


async def test_unknown_execution_retry_notification_cannot_adopt_the_active_turn() -> None:
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

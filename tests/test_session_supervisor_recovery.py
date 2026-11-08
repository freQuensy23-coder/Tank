"""The session supervisor recovers each session independently of the others and of idle maintenance."""

import asyncio
import json

import httpx
import pytest
from test_agents import settings
from test_agents_execution_barrier import config, idle, seeded
from test_event_tools import complete_event_fixture
from test_session_binding_concurrency import seed

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


def session_of(request):
    return request.url.path.split("/")[3]


def executions(request, states):
    sid = session_of(request)
    return httpx.Response(
        200,
        json={
            "data": [
                {
                    "execution_id": key,
                    "state": states.get((sid, key), "RUNNING"),
                    "settled_outcome": states.get((sid, key)),
                }
                for key in request.url.params.get_list("ids[]")
            ]
        },
    )


async def stop_supervisor(service, task, stop):
    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await service.close()


async def test_slow_tool_in_one_busy_session_does_not_hold_back_another_session_or_admission():
    repo = MemoryRepository()
    opts, _ = settings()
    template, _ = await seed(repo)
    await repo.put("ops.agent_sessions", "s", dict(template, status="ENDED"))
    await repo.put("evt.events", "slow-event", complete_event_fixture("slow-event"))
    await repo.put("ops.agent_sessions", "a-slow", dict(template, session_id="a-slow", event_id="slow-event"))
    item = await repo.get("ingest.source_items", "i")
    await repo.put("ingest.source_items", "j", dict(item, item_id="j", extraction_status="DONE"))
    fast_message = dict(
        template["message_log"][0],
        remote_event_id="input-b",
        payload={"kind": "NEW_ITEM", "item_id": "j"},
    )
    await repo.put(
        "ops.agent_sessions",
        "b-fast",
        dict(template, session_id="b-fast", opened_by_item="j", message_log=[fast_message]),
    )
    history = {
        "a-slow": [
            {
                "id": "market-truth-call",
                "type": "agent.custom_tool_use",
                "name": "get_market_truth",
                "input": {},
                "execution_id": "input",
            }
        ],
        "b-fast": [idle("b-end", "input-b")],
    }
    states = {("b-fast", "input-b"): "COMPLETED"}
    entered, release, settled, woke = (asyncio.Event() for _ in range(4))

    def handler(request):
        sid = session_of(request)
        if request.method == "POST":
            return httpx.Response(200, json={"data": [{"id": f"result-{sid}"}]})
        if request.url.path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, text="")
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": history[sid]})
        if request.url.path.endswith("/executions"):
            return executions(request, states)
        raise AssertionError(request.url.path)

    async def dispatch(tool, arguments, context):
        assert (tool, context["session_id"]) == ("get_market_truth", "a-slow")
        entered.set()
        await release.wait()
        return {"ok": True}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, dispatch, http=http)
        finish = service.finish_if_idle
        wake = service.wake_waiting

        async def finish_if_idle(sid):
            complete = await finish(sid)
            if sid == "b-fast" and complete:
                settled.set()
            return complete

        async def wake_waiting():
            await wake()
            woke.set()

        service.finish_if_idle = finish_if_idle
        service.wake_waiting = wake_waiting
        stop = asyncio.Event()
        task = asyncio.create_task(service.run(stop))
        try:
            async with asyncio.timeout(2):
                await entered.wait()
                await settled.wait()
                await woke.wait()
                await service.tasks["b-fast"]
            assert not release.is_set()
            fast = await repo.get("ops.agent_sessions", "b-fast")
            assert fast["runtime_status"] == "IDLE" and fast["active_message_id"] is None
            assert fast["verified_native_completion"]["complete"] is True
            slow = await repo.get("ops.agent_sessions", "a-slow")
            assert slow["runtime_status"] == "RUNNING" and slow["active_message_id"] == "m"
            assert not service.tasks["a-slow"].done()
            running, _ = await service.slot_state()
            assert running == {"a-slow"}
        finally:
            release.set()
            await stop_supervisor(service, task, stop)


async def test_restarted_follower_recovers_lost_acceptance_before_replaying_tool_history():
    repo = MemoryRepository()
    opts, _ = settings()
    template, _ = await seed(repo)
    await repo.put("evt.events", "event", complete_event_fixture("event"))
    message = dict(
        template["message_log"][0],
        body={"type": "user.message", "content": [{"type": "text", "text": "Product announced"}]},
        turn_status="ACCEPTING",
        remote_event_id=None,
    )
    await repo.put("ops.agent_sessions", "s", dict(template, event_id="event", message_log=[message]))
    posts = []

    def handler(request):
        if request.method == "POST":
            body = json.loads(request.content)["events"][0]
            posts.append((body["type"], request.headers.get("Idempotency-Key")))
            if body["type"] == "user.message":
                return httpx.Response(200, json={"data": [{"id": "input"}]})
            return httpx.Response(200, json={"data": [{"id": "returned"}]})
        if request.url.path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, text="")
        if request.url.path.endswith("/events"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "market-truth-call",
                            "type": "agent.custom_tool_use",
                            "name": "get_market_truth",
                            "input": {},
                            "execution_id": "input",
                        }
                    ]
                },
            )
        if request.url.path.endswith("/executions"):
            return executions(request, {})
        raise AssertionError(request.url.path)

    async def dispatch(tool, arguments, context):
        return {"ok": True}

    async def returned_call():
        while True:
            call = await repo.get("ops.tool_calls", "s:market-truth-call")
            if call and call.get("remote_event_id"):
                return call
            await asyncio.sleep(0.01)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, dispatch, http=http)
        stop = asyncio.Event()
        task = asyncio.create_task(service.run(stop))
        try:
            call = await asyncio.wait_for(returned_call(), 2)
            assert call["native_turn_message_id"] == "m" and call["remote_event_id"] == "returned"
            assert posts[0] == ("user.message", "message:m")
            assert [kind for kind, _ in posts] == ["user.message", "user.custom_tool_result"]
            assert (await repo.get("ops.agent_sessions", "s"))["message_log"][0]["remote_event_id"] == "input"
        finally:
            await stop_supervisor(service, task, stop)


async def test_idle_maintenance_failure_does_not_stop_other_sessions_or_operations(caplog):
    repo = MemoryRepository()
    for sid in ("a-broken", "b-healthy"):
        await repo.put(
            "ops.agent_sessions", sid, {"session_id": sid, "status": "OPEN", "runtime_status": "IDLE"}
        )
    service = SessionService(repo, config(), None, http=object())
    stop = asyncio.Event()
    answered = asyncio.Event()
    checked = []

    async def finish_if_idle(sid):
        if sid == "a-broken":
            raise ValueError("private provider diagnostic")
        checked.append(sid)
        return True

    async def sweep_failed():
        raise RuntimeError("private sweep diagnostic")

    async def reconcile_answers():
        answered.set()

    async def wake_waiting():
        await answered.wait()
        stop.set()

    service.finish_if_idle = finish_if_idle
    service.sweep_failed = sweep_failed
    service.reconcile_answers = reconcile_answers
    service.wake_waiting = wake_waiting
    try:
        with caplog.at_level("WARNING", logger="tank.agents.sessions"):
            await asyncio.wait_for(service.run(stop), 1)
        assert checked == ["b-healthy"]
        assert "a-broken" in caplog.text and "ValueError" in caplog.text
        assert "sweep_failed" in caplog.text and "RuntimeError" in caplog.text
        assert "private" not in caplog.text
    finally:
        await service.close()


@pytest.mark.parametrize("runtime_status", ["RUNNING", "IDLE"])
async def test_supervisor_leaves_a_session_with_a_live_follower_to_it(runtime_status):
    repo = MemoryRepository()
    session = await seeded(repo)
    session.update(
        runtime_status=runtime_status, active_message_id="message" if runtime_status == "RUNNING" else None
    )
    await repo.put("ops.agent_sessions", "s", session)
    service = SessionService(repo, config(), None, http=object())
    stop = asyncio.Event()
    maintained = asyncio.Event()
    follower = asyncio.create_task(asyncio.Event().wait())
    service.tasks["s"] = follower
    reconciled = []

    async def finish_if_idle(sid):
        reconciled.append(sid)
        return False

    async def reconcile_answers():
        maintained.set()

    async def wake_waiting():
        await maintained.wait()
        stop.set()

    service.finish_if_idle = finish_if_idle
    service.reconcile_answers = reconcile_answers
    service.wake_waiting = wake_waiting
    try:
        await asyncio.wait_for(service.run(stop), 1)
        assert reconciled == []
        assert service.tasks["s"] is follower and not follower.done()
    finally:
        await service.close()


async def test_maintenance_and_a_new_follower_never_reconcile_the_same_session_at_once():
    repo = MemoryRepository()
    session = await seeded(repo)
    session.update(runtime_status="IDLE", active_message_id=None)
    await repo.put("ops.agent_sessions", "s", session)
    service = SessionService(repo, config(), None, http=object())
    entered, release, following = (asyncio.Event() for _ in range(3))
    active = 0
    maximum = 0

    async def completed_turn(sid):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        entered.set()
        await release.wait()
        active -= 1
        return {"complete": False}

    async def catch_up(sid):
        nonlocal maximum
        maximum = max(maximum, active + 1)
        following.set()

    async def stream(sid):
        await release.wait()

    service.completed_turn = completed_turn
    service.catch_up = catch_up
    service.stream = stream
    task = asyncio.create_task(service.reconcile_idle_sessions([session], 0))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        service.start_stream("s")
        for _ in range(5):
            await asyncio.sleep(0)
        assert not following.is_set()
        release.set()
        await asyncio.wait_for(following.wait(), 1)
        await asyncio.wait_for(task, 1)
        assert maximum == 1
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()

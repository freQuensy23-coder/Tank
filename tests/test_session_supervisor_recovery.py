import asyncio
import json

import httpx
import pytest
from test_agents_execution_barrier import config
from test_session_completion_recovery import seed

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


async def test_busy_recovery_and_admission_are_not_blocked_by_idle_maintenance():
    repo = MemoryRepository()
    session = await seed(repo)
    session.update(session_id="z-busy", runtime_status="RUNNING")
    await repo.put("ops.agent_sessions", "z-busy", session)
    await repo.put("ops.agent_sessions", "s", {**session, "session_id": "s", "status": "ENDED"})
    for index in range(192):
        sid = f"a-idle-{index:03}"
        await repo.put(
            "ops.agent_sessions",
            sid,
            {"session_id": sid, "status": "OPEN", "runtime_status": "IDLE"},
        )
    entered, block, recovered, woke = (asyncio.Event() for _ in range(4))
    stop = asyncio.Event()
    results = []

    def handler(request):
        if request.method == "POST":
            results.append(json.loads(request.content))
            recovered.set()
            return httpx.Response(200, json={"data": [{"id": "returned-result"}]})
        if request.url.path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, text="")
        if request.url.path.endswith("/events"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "pending-call",
                            "type": "agent.custom_tool_use",
                            "name": "get_item",
                            "input": {"item_id": "i"},
                            "execution_id": "input-root",
                        }
                    ]
                },
            )
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": key, "state": "RUNNING", "settled_outcome": None}
                        for key in request.url.params.get_list("ids[]")
                    ]
                },
            )
        raise AssertionError(request.url.path)

    async def dispatch(tool, arguments, context):
        assert (tool, arguments, context["session_id"]) == (
            "get_item",
            {"item_id": "i"},
            "z-busy",
        )
        return {"item_id": "i", "text": "Retained source"}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), dispatch, http=http)
        finish = service.finish_if_idle

        async def maintenance(sid):
            if sid.startswith("a-idle-"):
                entered.set()
                await block.wait()
                return True
            return await finish(sid)

        async def wake():
            woke.set()

        service.finish_if_idle = maintenance
        service.wake_waiting = wake
        task = asyncio.create_task(service.run(stop))
        try:
            async with asyncio.timeout(2):
                await entered.wait()
                await recovered.wait()
                await woke.wait()
            stored = await repo.get("ops.tool_calls", "z-busy:pending-call")
            assert stored["result"] == {"item_id": "i", "text": "Retained source"}
            assert stored["remote_event_id"] == "returned-result"
            assert len(results) == 1
            assert not block.is_set()
            assert (await repo.get("ops.agent_sessions", "z-busy"))["active_message_id"] == "root"
            stop.set()
            await asyncio.wait_for(task, 1)
        finally:
            stop.set()
            block.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await service.close()


async def test_busy_live_follower_is_not_reconciled_again_by_supervisor():
    repo = MemoryRepository()
    session = await seed(repo)
    session["runtime_status"] = "RUNNING"
    await repo.put("ops.agent_sessions", "s", session)
    service = SessionService(repo, config(), None, http=object())
    stop = asyncio.Event()
    follower = asyncio.create_task(asyncio.Event().wait())
    service.tasks["s"] = follower

    async def forbidden(sid):
        pytest.fail("Supervisor reconciled a session owned by its live follower")

    async def wake():
        stop.set()

    service.finish_if_idle = forbidden
    service.wake_waiting = wake
    try:
        await asyncio.wait_for(service.run(stop), 1)
        assert not follower.done()
        assert (await repo.get("ops.agent_sessions", "s"))["active_message_id"] == "root"
    finally:
        await service.close()


async def test_idle_maintenance_failure_does_not_prevent_next_session_reconciliation(caplog):
    repo = MemoryRepository()
    service = SessionService(repo, config(), None, http=object())
    checked = []
    sessions = [
        {"session_id": sid, "status": "OPEN", "runtime_status": "IDLE"} for sid in ("a-broken", "b-healthy")
    ]
    for session in sessions:
        await repo.put("ops.agent_sessions", session["session_id"], session)

    async def finish(sid):
        if sid == "a-broken":
            raise ValueError("private provider diagnostic")
        checked.append(sid)
        return True

    service.finish_if_idle = finish
    try:
        await service.reconcile_idle_sessions(sessions, 0)
        assert checked == ["b-healthy"]
        assert "a-broken" in caplog.text and "ValueError" in caplog.text
        assert "private provider diagnostic" not in caplog.text
    finally:
        await service.close()


async def test_completion_reconciliation_is_serialized_during_follower_handoff():
    repo = MemoryRepository()
    await seed(repo)
    service = SessionService(repo, config(), None, http=object())
    entered, release, second = (asyncio.Event() for _ in range(3))
    calls = 0
    active = 0
    maximum = 0

    async def complete(sid):
        nonlocal calls, active, maximum
        calls += 1
        active += 1
        maximum = max(maximum, active)
        if calls == 1:
            entered.set()
            await release.wait()
        else:
            second.set()
        active -= 1
        return {"complete": False}

    service.completed_turn = complete
    attempting = asyncio.Event()

    async def handoff():
        attempting.set()
        return await service.finish_if_idle("s")

    tasks = [asyncio.create_task(service.finish_if_idle("s"))]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        tasks.append(asyncio.create_task(handoff()))
        await asyncio.wait_for(attempting.wait(), 1)
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 1)
        assert second.is_set() and calls == 2
        assert maximum == 1
        assert (await repo.get("ops.agent_sessions", "s"))["active_message_id"] == "root"
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await service.close()

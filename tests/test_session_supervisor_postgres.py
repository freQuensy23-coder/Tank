import asyncio
import copy
import json

import pytest
from aiohttp import web
from test_agents_execution_barrier import idle
from test_session_binding_concurrency import fixture_repo
from test_session_completion_recovery import message, seed, settings, waiting_job

from tank.agents.sessions import AdmissionDeferred, SessionService


@pytest.mark.integration
@pytest.mark.postgres
async def test_restart_recovery_dispatches_pending_tool_and_releases_admission_over_real_http(
    postgres_template,
):
    async with fixture_repo("postgres", postgres_template) as repo:
        session = await seed(repo)
        session["runtime_status"] = "RUNNING"
        session["message_log"][0]["payload"] = {"kind": "SCOUT_REVIEW", "session_id": "s"}
        old = message("old", "input-old", status="DONE")
        old["payload"] = {"kind": "SCOUT_REVIEW", "session_id": "s"}
        session["message_log"].insert(0, old)
        session["messages"].insert(0, "old")
        previous = idle("old-end", "input-old")
        session["native_execution_receipts"] = {previous["id"]: previous}
        await repo.put("ops.agent_sessions", "s", session)
        item = await repo.get("ingest.source_items", "i")
        await repo.put("ingest.source_items", "i", {**item, "extraction_status": "DONE"})
        await repo.put(
            "ops.agent_sessions",
            "a-idle",
            {
                **copy.deepcopy(session),
                "session_id": "a-idle",
                "runtime_status": "IDLE",
                "active_message_id": None,
                "message_log": [],
                "messages": [],
                "native_execution_receipts": {},
            },
        )
        job = await waiting_job(repo)
        history = [
            previous,
            {
                "id": "pending-tool",
                "type": "agent.custom_tool_use",
                "name": "get_item",
                "input": {"item_id": "i"},
                "execution_id": "input-root",
            },
            {
                **idle("current-requires-action", "input-root", "requires_action"),
                "stop_reason": {"type": "requires_action", "event_ids": ["pending-tool"]},
            },
        ]
        posts, dispatched = [], []
        states = {"input-old": "COMPLETED", "input-root": "COMPLETED"}
        settled = asyncio.Event()
        block = asyncio.Event()
        maintenance_entered = asyncio.Event()

        async def events(request):
            if request.method == "GET":
                return web.json_response({"data": copy.deepcopy(history)})
            body = await request.json()
            posts.append(body)
            result = body["events"][0]
            assert result["type"] == "user.custom_tool_result"
            assert result["custom_tool_use_id"] == "pending-tool"
            assert json.loads(result["content"][0]["text"])["text"] == "Product announced"
            assert not result.get("is_error")
            states["input-result"] = "COMPLETED"
            history.append(idle("current-end", "input-result"))
            return web.json_response({"data": [{"id": "input-result"}]})

        async def executions(request):
            return web.json_response(
                {
                    "data": [
                        {"execution_id": key, "state": states[key], "settled_outcome": states[key]}
                        for key in request.query.getall("ids[]")
                    ]
                }
            )

        async def stream(request):
            return web.Response(text="", content_type="text/event-stream")

        async def dispatch(tool, arguments, context):
            assert tool == "get_item" and context["session_id"] == "s"
            source = await repo.get("ingest.source_items", arguments["item_id"])
            dispatched.append(source["item_id"])
            return {"item_id": source["item_id"], "text": source["text"]}

        app = web.Application()
        app.router.add_route("*", "/v1/sessions/s/events", events)
        app.router.add_get("/v1/sessions/s/executions", executions)
        app.router.add_get("/v1/sessions/s/events/stream", stream)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        options = settings()
        options.openma_url = f"http://127.0.0.1:{port}"
        stop = asyncio.Event()
        service = SessionService(repo, options, dispatch)
        finish = service.finish_if_idle

        async def maintenance(sid):
            if sid == "a-idle":
                maintenance_entered.set()
                await block.wait()
                return True
            complete = await finish(sid)
            if complete:
                settled.set()
            return complete

        service.finish_if_idle = maintenance
        task = None
        try:
            with pytest.raises(AdmissionDeferred):
                await service.admit(job)
            task = asyncio.create_task(service.run(stop))
            async with asyncio.timeout(5):
                await maintenance_entered.wait()
                await settled.wait()
            assert not block.is_set()
            current = await repo.get("ops.agent_sessions", "s")
            assert current["runtime_status"] == "IDLE" and current["active_message_id"] is None
            assert current["verified_native_completion"]["complete"] is True
            assert current["verified_native_completion"]["pending_action_ids"] == []
            assert all(row["turn_status"] == "DONE" for row in current["message_log"])
            call = await repo.get("ops.tool_calls", "s:pending-tool")
            assert call["returned_at"] and call["remote_event_id"] == "input-result"
            assert call["result"] == {"item_id": "i", "text": "Product announced"}
            await service.wake_waiting()
            awakened = await repo.get("ops.jobs", job["job_id"])
            assert awakened["run_after"] != job["run_after"]
            await service.admit(awakened)
            assert (await repo.get("ops.jobs", job["job_id"]))["slot_admitted"] is True
            stop.set()
            await asyncio.wait_for(task, 1)
            await service.close()
            restarted = SessionService(repo, options, dispatch)
            try:
                assert (await restarted.completed_turn("s"))["complete"] is True
                assert dispatched == ["i"] and len(posts) == 1
            finally:
                await restarted.close()
        finally:
            stop.set()
            block.set()
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await service.close()
            await runner.cleanup()

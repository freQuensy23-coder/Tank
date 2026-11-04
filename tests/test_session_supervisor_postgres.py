import asyncio
import copy
import hashlib
import json

import pytest
from aiohttp import web
from test_agents_execution_barrier import idle
from test_session_binding_concurrency import fixture_repo
from test_session_supervisor_recovery import message, seed, settings, waiting_job

from tank.agents.sessions import AdmissionDeferred, SessionService


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("lost_acceptance", [False, True])
async def test_restart_recovery_dispatches_pending_tool_and_releases_admission_over_real_http(
    postgres_template, lost_acceptance
):
    async with fixture_repo("postgres", postgres_template) as repo:
        session = await seed(repo)
        session["runtime_status"] = "RUNNING"
        session["message_log"][0]["payload"] = {"kind": "SCOUT_REVIEW", "session_id": "s"}
        if lost_acceptance:
            intent = session["message_log"][0]
            intent.update(turn_status="ACCEPTING", remote_event_id=None, native_input_attempts={})
            intent.pop("sent_at")
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
        posts, dispatched, accepted = [], [], []
        states = {"input-old": "COMPLETED", "input-root": "COMPLETED"}
        settled = asyncio.Event()
        tool_entered, tool_release = asyncio.Event(), asyncio.Event()
        result_entered, result_release = asyncio.Event(), asyncio.Event()
        block = asyncio.Event()
        maintenance_entered = asyncio.Event()

        async def events(request):
            if request.method == "GET":
                assert len(accepted) == int(lost_acceptance)
                return web.json_response({"data": copy.deepcopy(history)})
            body = await request.json()
            if body["events"][0]["type"] == "user.message":
                assert lost_acceptance
                assert request.headers["Idempotency-Key"] == "message:root"
                accepted.append(body)
                return web.json_response({"data": [{"id": "input-root"}]})
            result_entered.set()
            await result_release.wait()
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
            tool_entered.set()
            await tool_release.wait()
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
                await tool_entered.wait()
                with pytest.raises(AdmissionDeferred):
                    await service.admit(job)
                assert not (await repo.get("ops.jobs", job["job_id"])).get("slot_admitted")
                tool_release.set()
                await result_entered.wait()
                with pytest.raises(AdmissionDeferred):
                    await service.admit(job)
                pending = await repo.get("ops.tool_calls", "s:pending-tool")
                assert pending is not None
                assert pending["status"] == "DONE" and not pending.get("returned_at")
                assert not pending.get("remote_event_id")
                blocked = await repo.get("ops.agent_sessions", "s")
                barrier = await service.execution_barrier(blocked, verify_remote=True)
                assert barrier["complete"] is False
                assert "unreturned-tool:pending-tool" in barrier["unidentified_inputs"]
                assert not (await repo.get("ops.jobs", job["job_id"])).get("slot_admitted")
                result_release.set()
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
            tool_release.set()
            result_release.set()
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await service.close()
            await runner.cleanup()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("turn_status", ["PENDING", "RUNNING"])
@pytest.mark.parametrize("identity", ["saved-attempt-0", "saved-attempt-1", "legacy-retry"])
async def test_sent_at_identity_recovery_preserves_supervisor_ack_and_history_barriers(
    postgres_template, turn_status, identity
):
    async with fixture_repo("postgres", postgres_template) as repo:
        session = await seed(repo)
        session["runtime_status"] = "RUNNING"
        attempt = int(identity != "saved-attempt-0")
        intent = session["message_log"][0]
        intent.update(
            turn_status=turn_status,
            remote_event_id=None,
            retry_count=attempt,
            native_input_attempts={"input-root": attempt} if identity != "legacy-retry" else {},
        )
        if attempt:
            intent["native_input_attempts"]["input-prior"] = 0
            intent["superseded_native_input_ids"] = ["input-prior"]
            intent["retry_call_ordinal_cutoffs"] = {"0": 0}
            session.update(
                pending_retry_event=None,
                processed_events=["prior-failure"],
                superseded_native_inputs={
                    "input-prior": {
                        "reason": "TURN_FAILED_AND_RETRIED",
                        "failure_event_id": "prior-failure",
                        "replacement_input_id": (
                            None if identity == "legacy-retry" else "input-root"
                        ),
                    }
                },
            )
        await repo.put("ops.agent_sessions", "s", session)
        session = await repo.get("ops.agent_sessions", "s")
        original_intent = copy.deepcopy(session["message_log"][0])
        expected_body = {"events": [original_intent["body"]]}
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
                "superseded_native_inputs": {},
            },
        )
        job = await waiting_job(repo)
        history = [
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
        states = {"input-root": "COMPLETED", "input-prior": "FAILED"}
        input_posts, result_posts, dispatched, reads = [], [], [], []
        peer_attempts, peer_failures = [], []
        tool_entered, tool_release = asyncio.Event(), asyncio.Event()
        result_entered, result_release = asyncio.Event(), asyncio.Event()
        result_acked, post_ack_history = asyncio.Event(), asyncio.Event()
        native_end, drain_entered, drain_release = (asyncio.Event() for _ in range(3))
        maintenance_entered, maintenance_block, settled = (asyncio.Event() for _ in range(3))
        maintenance_cancelled = asyncio.Event()

        @web.middleware
        async def record_peer_failure(request, handler):
            peer_attempts.append((request.method, request.path))
            try:
                return await handler(request)
            except Exception as exc:
                if not peer_failures:
                    peer_failures.append(
                        (request.method, request.path, type(exc).__name__, str(exc)[:500])
                    )
                raise

        async def assert_recovered(request):
            current = await repo.get("ops.agent_sessions", "s")
            recovered = current["message_log"][0]
            assert recovered["remote_event_id"] == "input-root"
            assert recovered["sent_at"] == original_intent["sent_at"]
            assert recovered["retry_count"] == attempt
            assert await service.native_input_owner(current, "input-root") == ("root", attempt)
            assert len(input_posts) == (2 if identity == "legacy-retry" else 0)
            reads.append(request.path)

        async def events(request):
            if request.method == "GET":
                await assert_recovered(request)
                if result_acked.is_set():
                    post_ack_history.set()
                    await native_end.wait()
                    drain_entered.set()
                    await drain_release.wait()
                return web.json_response({"data": copy.deepcopy(history)})
            wire = await request.read()
            body = json.loads(wire)
            if body["events"][0]["type"] == "user.message":
                assert identity == "legacy-retry"
                assert body == expected_body
                assert request.headers["Idempotency-Key"] == "turn-retry:s:root"
                input_posts.append((request.headers["Idempotency-Key"], wire))
                assert input_posts[-1] == input_posts[0]
                return web.json_response({"data": [{"id": "input-root"}]})
            assert request.headers["Idempotency-Key"] == (
                "tool:" + hashlib.sha256(b"s:pending-tool").hexdigest()
            )
            result = body["events"][0]
            assert result["type"] == "user.custom_tool_result"
            assert result["custom_tool_use_id"] == "pending-tool"
            assert json.loads(result["content"][0]["text"])["text"] == "Product announced"
            assert not result.get("is_error")
            result_entered.set()
            await result_release.wait()
            result_posts.append(body)
            states["input-result"] = "COMPLETED"
            result_acked.set()
            return web.json_response({"data": [{"id": "input-result"}]})

        async def executions(request):
            await assert_recovered(request)
            return web.json_response(
                {
                    "data": [
                        {
                            "execution_id": key,
                            "state": states.get(key, "UNKNOWN"),
                            "settled_outcome": states.get(key, "UNKNOWN"),
                        }
                        for key in request.query.getall("ids[]")
                    ]
                }
            )

        async def stream(request):
            await assert_recovered(request)
            return web.Response(text="", content_type="text/event-stream")

        async def dispatch(tool, arguments, context):
            assert (tool, arguments, context["session_id"]) == ("get_item", {"item_id": "i"}, "s")
            source = await repo.get("ingest.source_items", arguments["item_id"])
            dispatched.append(source["item_id"])
            tool_entered.set()
            await tool_release.wait()
            return {"item_id": source["item_id"], "text": source["text"]}

        app = web.Application(middlewares=[record_peer_failure])
        app.router.add_route("*", "/v1/sessions/s/events", events)
        app.router.add_get("/v1/sessions/s/executions", executions)
        app.router.add_get("/v1/sessions/s/events/stream", stream)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        options = settings()
        options.openma_url = f"http://127.0.0.1:{runner.addresses[0][1]}"
        service = SessionService(repo, options, dispatch)
        finish = service.finish_if_idle
        stop = asyncio.Event()

        async def maintenance(sid):
            if sid == "a-idle":
                maintenance_entered.set()
                try:
                    await maintenance_block.wait()
                except asyncio.CancelledError:
                    maintenance_cancelled.set()
                    raise
                return True
            complete = await finish(sid)
            if complete:
                settled.set()
            return complete

        async def assert_slot_held():
            current = await repo.get("ops.agent_sessions", "s")
            assert current["runtime_status"] != "IDLE"
            assert current["active_message_id"] == "root"
            with pytest.raises(AdmissionDeferred):
                await service.admit(job)
            assert not (await repo.get("ops.jobs", job["job_id"])).get("slot_admitted")

        service.finish_if_idle = maintenance
        task = None
        try:
            if identity == "legacy-retry":
                await service.request(
                    "POST", "sessions/s/events", json=expected_body, idem="turn-retry:s:root"
                )
            await assert_slot_held()
            task = asyncio.create_task(service.run(stop))
            async with asyncio.timeout(5):
                await maintenance_entered.wait()
                await tool_entered.wait()
                await assert_slot_held()
                tool_release.set()
                await result_entered.wait()
                pending = await repo.get("ops.tool_calls", "s:pending-tool")
                assert pending["status"] == "DONE" and not pending.get("returned_at")
                assert not pending.get("remote_event_id")
                assert pending["native_source_execution_id"] == "input-root"
                assert pending["native_turn_message_id"] == "root"
                assert pending["native_turn_retry_count"] == attempt
                current = await repo.get("ops.agent_sessions", "s")
                barrier = await service.execution_barrier(current, verify_remote=True)
                assert barrier["complete"] is False
                assert "unreturned-tool:pending-tool" in barrier["unidentified_inputs"]
                await assert_slot_held()
                result_release.set()
                await post_ack_history.wait()
                returned = await repo.get("ops.tool_calls", "s:pending-tool")
                assert returned["returned_at"] and returned["remote_event_id"] == "input-result"
                current = await repo.get("ops.agent_sessions", "s")
                owner = await service.native_input_owner(current, "input-result")
                assert owner == ("root", attempt)
                barrier = await service.execution_barrier(current, verify_remote=True)
                assert barrier["complete"] is False
                await assert_slot_held()
                history.append(idle("current-end", "input-result"))
                native_end.set()
                await drain_entered.wait()
                current = await repo.get("ops.agent_sessions", "s")
                assert "current-end" not in current.get("native_execution_receipts", {})
                assert not (await service.execution_barrier(current))["complete"]
                await assert_slot_held()
                drain_release.set()
                await settled.wait()
            assert not peer_failures, peer_failures
            assert not maintenance_block.is_set()
            current = await repo.get("ops.agent_sessions", "s")
            assert current["runtime_status"] == "IDLE" and current["active_message_id"] is None
            assert current["message_log"][0]["turn_status"] == "DONE"
            assert current["message_log"][0]["sent_at"] == original_intent["sent_at"]
            assert current["verified_native_completion"]["complete"] is True
            assert current["verified_native_completion"]["pending_action_ids"] == []
            assert (
                current["native_execution_receipts"]["current-end"]["stop_reason"]["type"]
                == "end_turn"
            )
            if attempt:
                assert (
                    current["superseded_native_inputs"]["input-prior"]["replacement_input_id"]
                    == "input-root"
                )
            assert returned["result"] == {"item_id": "i", "text": "Product announced"}
            await service.wake_waiting()
            awakened = await repo.get("ops.jobs", job["job_id"])
            assert awakened["run_after"] != job["run_after"]
            await service.admit(awakened)
            assert (await repo.get("ops.jobs", job["job_id"]))["slot_admitted"] is True
            stop.set()
            await asyncio.wait_for(task, 1)
            assert task.done() and maintenance_cancelled.is_set()
            assert not any(lock.locked() for lock in service.reconciliation_locks.values())
            await service.close()
            restarted = SessionService(repo, options, dispatch)
            try:
                assert (await restarted.completed_turn("s"))["complete"] is True
                assert dispatched == ["i"] and len(result_posts) == 1
                await restarted.recover_outbound_inputs("s")
                await restarted.catch_up("s")
                assert dispatched == ["i"] and len(result_posts) == 1
                assert len(input_posts) == (2 if identity == "legacy-retry" else 0)
                assert "/v1/sessions/s/events" in reads
                assert "/v1/sessions/s/events/stream" in reads
                assert not peer_failures, peer_failures
            finally:
                await restarted.close()
        finally:
            stop.set()
            maintenance_block.set()
            tool_release.set()
            result_release.set()
            native_end.set()
            drain_release.set()
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await service.close()
            await runner.cleanup()
        assert not peer_failures, peer_failures

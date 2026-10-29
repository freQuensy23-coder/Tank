import asyncio
import copy
import json

import httpx
import pytest
from test_agents_execution_barrier import config, idle
from test_session_binding_concurrency import fixture_repo

from tank.agents.context import CONTEXT_VERSION
from tank.agents.sessions import AdmissionDeferred, SessionService
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import parse_time, sha256

AT = "2026-10-27T10:00:00Z"


def settings():
    value = config()
    load_config = value.load_config
    value.load_config = lambda name: {"max_running_sessions": 1} if name == "openma" else load_config(name)
    return value


def message(key, identity, *, status="RUNNING", attempt=0):
    return {
        "message_id": key,
        "body": {"type": "user.message", "content": [{"type": "text", "text": key}]},
        "payload": {"kind": "ITEM_ADDED", "item_id": "i", "session_id": "s"},
        "requested_at": AT,
        "sent_at": AT,
        "remote_event_id": identity,
        "retry_count": attempt,
        "turn_status": status,
        "native_input_attempts": {identity: attempt} if identity else {},
    }


async def seed(repo):
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "source_id": "S01",
            "feed": "fixture",
            "external_id": "i",
            "text": "Product announced",
            "source_class": "PRIMARY",
            "observed_at": AT,
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": AT,
            "published_prec": "EXACT",
            "raw_ref": "/fixture/raw",
            "raw_sha256": sha256("Product announced"),
            "content_hash": sha256("Product announced"),
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        },
    )
    session = {
        "session_id": "s",
        "status": "OPEN",
        "runtime_status": "IDLE",
        "item_id": "i",
        "opened_by_item": "i",
        "opened_at": AT,
        "opened_at_prec": "COLLECTION_ONLY",
        "last_item_at": AT,
        "last_item_at_prec": "COLLECTION_ONLY",
        "last_event_seq": 0,
        "agent_version": "1",
        "prompt_version": "a" * 64,
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "model_id": "fixture-model",
        "config_version": "test",
        "code_version": "test",
        "model_context_version": CONTEXT_VERSION,
        "active_message_id": "root",
        "messages": ["root"],
        "message_log": [message("root", "input-root")],
    }
    await repo.put("ops.agent_sessions", "s", session)
    return session


def native_reader(states, history, requests):
    def handler(request):
        requests.append(request)
        assert request.method == "GET", "Recovery must not resend an already accepted input"
        if request.url.path == "/v1/sessions/s/executions":
            identities = request.url.params.get_list("ids[]")
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
                        for identity in identities
                    ]
                },
            )
        assert request.url.path == "/v1/sessions/s/events"
        return httpx.Response(200, json={"data": history, "next_page": None})

    return handler


async def waiting_job(repo):
    job = await repo.enqueue(
        "AGENT_MESSAGE",
        {"kind": "MILESTONE", "session_id": "waiting-session"},
        "waiting-for-completion",
        priority=1,
    )
    job.update(slot_waiting_since=AT, run_after="2099-01-01T00:00:00Z")
    await repo.put("ops.jobs", job["job_id"], job)
    return job


@pytest.mark.parametrize("attempt", [0, 1])
async def test_recovery_restores_exact_current_attempt_without_resending(attempt):
    repo = MemoryRepository()
    session = await seed(repo)
    lost = message("lost", None, status="ACCEPTING", attempt=attempt)
    lost["native_input_attempts"] = {"input-lost": attempt}
    if attempt:
        lost["native_input_attempts"]["input-prior"] = 0
        lost["superseded_native_input_ids"] = ["input-prior"]
        session["superseded_native_inputs"] = {"input-prior": {"reason": "TURN_FAILED_AND_RETRIED"}}
    session["message_log"].append(lost)
    session["messages"].append("lost")
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    states = {"input-root": "COMPLETED", "input-lost": "COMPLETED", "input-prior": "FAILED"}
    accepted = {"id": "input-lost", **copy.deepcopy(lost["body"])}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(native_reader(states, [accepted], requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        await service.recover_outbound_inputs("s")
        recovered = await repo.get("ops.agent_sessions", "s")
        row = recovered["message_log"][1]
        assert row["remote_event_id"] == "input-lost"
        assert row["turn_status"] == "PENDING"
        assert row["sent_at"] == AT
        assert row["native_input_attempts"] == lost["native_input_attempts"]
        assert await service.native_input_owner(recovered, "input-lost") == ("lost", attempt)
        assert recovered["active_message_id"] == "root"
        assert not (await service.execution_barrier(recovered))["complete"]
        await service.recover_outbound_inputs("s")
        assert (await repo.get("ops.agent_sessions", "s"))["message_log"][1] == row
        assert not any(request.method == "POST" for request in requests)
        await service.close()


async def test_running_receipt_uses_durable_accepted_identity_not_previous_active_message():
    repo = MemoryRepository()
    session = await seed(repo)
    lost = message("lost", None, status="ACCEPTING", attempt=1)
    lost["native_input_attempts"] = {"input-prior": 0, "input-lost": 1}
    lost["superseded_native_input_ids"] = ["input-prior"]
    session["message_log"].append(lost)
    session["messages"].append("lost")
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(native_reader({}, [], requests))) as http:
        service = SessionService(repo, settings(), None, http=http)
        await service.process_event(
            "s",
            {
                "id": "lost-input-running",
                "type": "session.status_running",
                "execution_id": "input-lost",
                "processed_at": AT,
            },
        )
        current = await repo.get("ops.agent_sessions", "s")
        assert current["active_message_id"] == "lost"
        assert current["message_log"][1]["turn_status"] == "RUNNING"
        assert await service.native_input_owner(current, "input-lost") == ("lost", 1)
        assert not any(request.method == "POST" for request in requests)
        await service.close()


@pytest.mark.parametrize("case", ["ambiguous", "prior-attempt", "missing", "foreign-message", "foreign-tool"])
async def test_recovery_refuses_unproven_or_conflicting_accepted_identity(case):
    repo = MemoryRepository()
    session = await seed(repo)
    lost = message("lost", None, status="ACCEPTING", attempt=1)
    lost["native_input_attempts"] = {
        "ambiguous": {"input-lost": 1, "input-other": 1},
        "prior-attempt": {"input-prior": 0},
        "missing": {},
        "foreign-message": {"input-root": 1},
        "foreign-tool": {"input-tool": 1},
    }[case]
    session["message_log"].append(lost)
    session["messages"].append("lost")
    if case == "foreign-tool":
        await repo.put(
            "ops.tool_calls",
            "s:foreign-tool",
            {
                "session_id": "s",
                "call_id": "foreign-tool",
                "tool": "get_item",
                "status": "DONE",
                "native_turn_message_id": "root",
                "native_turn_retry_count": 0,
                "remote_event_id": "input-tool",
                "returned_at": AT,
            },
        )
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    states = {
        "input-root": "COMPLETED",
        "input-lost": "COMPLETED",
        "input-other": "COMPLETED",
        "input-prior": "FAILED",
        "input-tool": "COMPLETED",
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(native_reader(states, [], requests))) as http:
        service = SessionService(repo, settings(), None, http=http)
        await service.recover_outbound_inputs("s")
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][1]["remote_event_id"] is None
        assert current["message_log"][1]["turn_status"] == "ACCEPTING"
        barrier = await service.execution_barrier(current, verify_remote=True)
        assert not barrier["complete"]
        assert "message:lost" in barrier["unidentified_inputs"]
        assert current["active_message_id"] == "root"
        assert not any(request.method == "POST" for request in requests)
        await service.close()


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
async def test_recovered_ack_settles_all_inputs_before_releasing_admission(
    backend, postgres_template, monkeypatch
):
    monkeypatch.setattr("tank.agents.sessions.collection_clock", lambda: collection_clock(AT))
    async with fixture_repo(backend, postgres_template) as repo:
        session = await seed(repo)
        lost = message("lost", None, status="ACCEPTING")
        lost["native_input_attempts"] = {"input-lost": 0}
        session["message_log"].append(lost)
        session["messages"].append("lost")
        end = idle("last-end", "input-tool")
        session["native_execution_receipts"] = {end["id"]: end}
        await repo.put("ops.agent_sessions", "s", session)
        await repo.put(
            "ops.tool_calls",
            "s:continuation",
            {
                "session_id": "s",
                "call_id": "continuation",
                "tool": "get_item",
                "input": {"item_id": "i"},
                "requested_at": AT,
                "requested_at_prec": "COLLECTION_ONLY",
                "status": "DONE",
                "native_turn_message_id": "lost",
                "native_turn_retry_count": 0,
                "remote_event_id": "input-tool",
                "returned_at": AT,
            },
        )
        job = await waiting_job(repo)
        states = {"input-root": "COMPLETED", "input-lost": "COMPLETED", "input-tool": "RUNNING"}
        requests = []
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(native_reader(states, [end], requests))
        ) as http:
            service = SessionService(repo, settings(), None, http=http)
            service.clock_timestamp = lambda: collection_clock(AT)
            with pytest.raises(AdmissionDeferred):
                await service.admit(job)
            assert not (await service.completed_turn("s"))["complete"]
            current = await repo.get("ops.agent_sessions", "s")
            assert current["message_log"][1]["remote_event_id"] == "input-lost"
            assert current["active_message_id"] == "root"
            assert (await repo.get("ingest.source_items", "i"))["extraction_status"] == "PENDING"
            with pytest.raises(AdmissionDeferred):
                await service.admit(job)
            states["input-tool"] = "COMPLETED"
            await service.process_event("s", idle("tool-committed", "input-tool"))
            result = await service.completed_turn("s")
            assert result["complete"]
            assert result["accepted_input_ids"] == ["input-lost", "input-root", "input-tool"]
            current = await repo.get("ops.agent_sessions", "s")
            assert current["active_message_id"] is None
            assert current["runtime_status"] == "IDLE"
            assert all(row["turn_status"] == "DONE" for row in current["message_log"])
            settled = await repo.get("ingest.source_items", "i")
            assert (settled["extraction_status"], settled["extraction_reason"]) == (
                "FAILED",
                "AGENT_INCOMPLETE",
            )
            await service.wake_waiting()
            awakened = await repo.get("ops.jobs", job["job_id"])
            assert parse_time(awakened["run_after"]) == parse_time(AT)
            await service.admit(job)
            assert (await repo.get("ops.jobs", job["job_id"]))["slot_admitted"] is True
            assert not any(request.method == "POST" for request in requests)
            await service.close()


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
@pytest.mark.parametrize("ack_before_reload", [False, True])
async def test_event_reload_cannot_overwrite_ack_of_message_absent_from_initial_snapshot(
    backend, postgres_template, monkeypatch, ack_before_reload
):
    async with fixture_repo(backend, postgres_template) as repo:
        await seed(repo)
        tool_started = asyncio.Event()
        release_tool = asyncio.Event()
        message_posted = asyncio.Event()
        release_ack = asyncio.Event()
        stale_reload = asyncio.Event()
        release_merge = asyncio.Event()
        original_get = repo.get
        event_task = None
        send_task = None
        requests = []

        async def get(collection, key):
            row = await original_get(collection, key)
            if (
                collection == "ops.tool_calls"
                and key == "s:old-tool"
                and row
                and row.get("returned_at")
                and asyncio.current_task() is event_task
            ):
                stale_reload.set()
                await release_merge.wait()
            return row

        monkeypatch.setattr(repo, "get", get)

        async def dispatch(tool, arguments, context):
            assert tool == "web_search"
            assert arguments["query"] == "Product announcement"
            tool_started.set()
            await release_tool.wait()
            return {"results": []}

        async def handler(request):
            requests.append(request)
            assert request.method == "POST"
            body = json.loads(request.content)["events"][0]
            if body["type"] == "user.message":
                message_posted.set()
                await release_ack.wait()
                return httpx.Response(200, json={"data": [{"id": "input-new"}]})
            assert body["type"] == "user.custom_tool_result"
            assert body["custom_tool_use_id"] == "old-tool"
            return httpx.Response(200, json={"data": [{"id": "input-tool"}]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            service = SessionService(repo, settings(), dispatch, http=http)
            service.start_stream = lambda sid: None
            service.clock_timestamp = lambda: collection_clock(AT)
            payload = {"kind": "ITEM_ADDED", "item_id": "i", "session_id": "s"}
            try:
                async with asyncio.timeout(5):
                    event_task = asyncio.create_task(
                        service.process_event(
                            "s",
                            {
                                "id": "old-tool",
                                "type": "agent.custom_tool_use",
                                "execution_id": "input-root",
                                "name": "web_search",
                                "input": {"query": "Product announcement"},
                                "processed_at": AT,
                            },
                            seq=17,
                        )
                    )
                    await tool_started.wait()
                    send_task = asyncio.create_task(service.send_message(payload))
                    await message_posted.wait()
                    if ack_before_reload:
                        release_ack.set()
                        await send_task
                    release_tool.set()
                    await stale_reload.wait()
                    release_ack.set()
                    await send_task
                    async with repo.lock("session:s"):
                        concurrent = await original_get("ops.agent_sessions", "s")
                        concurrent["message_log"][0].update(
                            retry_count=1,
                            turn_status="DONE",
                            remote_event_id="input-root-retry",
                            native_input_attempts={"input-root": 0, "input-root-retry": 1},
                        )
                        concurrent["native_execution_receipts"] = {
                            "concurrent-end": idle("concurrent-end", "input-root-retry")
                        }
                        concurrent["superseded_native_inputs"] = {
                            "input-root": {"reason": "TURN_FAILED_AND_RETRIED"}
                        }
                        concurrent["processed_events"] = ["concurrent-event"]
                        concurrent["runtime_status"] = "IDLE"
                        await repo.put("ops.agent_sessions", "s", concurrent)
                    acknowledged = await original_get("ops.agent_sessions", "s")
                    accepted = next(
                        row for row in acknowledged["message_log"] if row["message_id"] == sha256(payload)
                    )
                    assert accepted["remote_event_id"] == "input-new"
                    assert accepted["turn_status"] == "PENDING"
                    release_merge.set()
                    await event_task
                current = await original_get("ops.agent_sessions", "s")
                row = next(row for row in current["message_log"] if row["message_id"] == sha256(payload))
                assert row["remote_event_id"] == "input-new"
                assert row["turn_status"] == "PENDING"
                assert row["sent_at"] == accepted["sent_at"]
                assert row["native_input_attempts"] == {"input-new": 0}
                assert current["messages"] == ["root", sha256(payload)]
                assert current["processed_events"] == ["concurrent-event", "old-tool"]
                assert current["message_log"][0] == acknowledged["message_log"][0]
                assert current["native_execution_receipts"] == acknowledged["native_execution_receipts"]
                assert current["superseded_native_inputs"] == acknowledged["superseded_native_inputs"]
                assert current["runtime_status"] == "IDLE"
                assert current["last_event_seq"] == 17
                assert (await original_get("ops.tool_calls", "s:old-tool"))["remote_event_id"] == "input-tool"
                assert [json.loads(request.content)["events"][0]["type"] for request in requests] == [
                    "user.message",
                    "user.custom_tool_result",
                ]
            finally:
                for task in (event_task, send_task):
                    if task is not None and not task.done():
                        task.cancel()
                await asyncio.gather(
                    *(task for task in (event_task, send_task) if task is not None), return_exceptions=True
                )
                await service.close()


async def seed_reopened_failure(repo):
    session = await seed(repo)
    failure = {
        "event_id": "execution-status-failed:input-failed-retry",
        "event_type": "execution.status_failed",
        "execution_id": "input-failed-retry",
        "reason": "retained_configuration_changed",
        "recorded_at": AT,
    }
    failed = message("root", "input-failed-retry", status="FAILED", attempt=1)
    failed["native_input_attempts"]["input-failed-initial"] = 0
    failed["superseded_native_input_ids"] = ["input-failed-initial"]
    failed["terminal_failure"] = copy.deepcopy(failure)
    later = message("milestone", "input-milestone")
    later["payload"] = {"kind": "MILESTONE", "session_id": "s"}
    end = idle("milestone-end", "input-milestone")
    session.update(
        active_message_id="milestone",
        message_log=[failed, later],
        messages=["root", "milestone"],
        processed_events=[failure["event_id"], end["id"]],
        terminal_failure=copy.deepcopy(failure),
        superseded_native_inputs={"input-failed-initial": {"reason": "TURN_FAILED_AND_RETRIED"}},
        native_execution_receipts={end["id"]: end},
    )
    await repo.put("ops.agent_sessions", "s", session)
    item = await repo.get("ingest.source_items", "i")
    await repo.put("ingest.source_items", "i", dict(item, extraction_status="FAILED"))
    return session, end


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
async def test_preexisting_milestone_reopen_settles_without_reprocessing_historical_final_failure(
    backend, postgres_template, monkeypatch
):
    monkeypatch.setattr("tank.agents.sessions.collection_clock", lambda: collection_clock(AT))
    async with fixture_repo(backend, postgres_template) as repo:
        session, end = await seed_reopened_failure(repo)
        job = await waiting_job(repo)
        states = {"input-failed-retry": "FAILED", "input-milestone": "COMPLETED"}
        requests = []
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(native_reader(states, [end], requests))
        ) as http:
            service = SessionService(repo, settings(), None, http=http)
            service.clock_timestamp = lambda: collection_clock(AT)
            with pytest.raises(AdmissionDeferred):
                await service.admit(job)
            result = await service.completed_turn("s")
            assert result["complete"]
            assert "input-failed-retry" not in result["accepted_input_ids"]
            current = await repo.get("ops.agent_sessions", "s")
            assert current["status"] == "OPEN"
            assert current["active_message_id"] is None
            assert current["runtime_status"] == "IDLE"
            assert current["message_log"][0]["turn_status"] == "FAILED"
            historical_failure = session["message_log"][0]["terminal_failure"]
            assert current["message_log"][0]["terminal_failure"] == historical_failure
            assert current["message_log"][1]["turn_status"] == "DONE"
            assert current["processed_events"].count(session["terminal_failure"]["event_id"]) == 1
            assert (await repo.get("ingest.source_items", "i"))["extraction_status"] == "FAILED"
            await service.wake_waiting()
            awakened = await repo.get("ops.jobs", job["job_id"])
            assert parse_time(awakened["run_after"]) == parse_time(AT)
            await service.admit(job)
            assert (await repo.get("ops.jobs", job["job_id"]))["slot_admitted"] is True
            assert (await service.completed_turn("s"))["complete"]
            await service.close()


@pytest.mark.integration
async def test_ordinary_milestone_after_exhausted_failure_can_complete():
    repo = MemoryRepository()
    session = await seed(repo)
    session["message_log"][0]["retry_count"] = 1
    session["message_log"][0]["native_input_attempts"] = {"input-root": 1}
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    end = idle("new-milestone-end", "input-milestone")
    reader = native_reader({"input-root": "FAILED", "input-milestone": "COMPLETED"}, [end], requests)

    def handler(request):
        if request.method == "POST":
            requests.append(request)
            body = json.loads(request.content)
            assert body["events"][0]["type"] == "user.message"
            return httpx.Response(200, json={"data": [{"id": "input-milestone"}]})
        return reader(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http)
        service.start_stream = lambda sid: None
        service.clock_timestamp = lambda: collection_clock(AT)
        await service.process_event(
            "s",
            {
                "id": "execution-status-failed:input-root",
                "type": "execution.status_failed",
                "execution_id": "input-root",
                "reason": "retained_configuration_changed",
                "processed_at": AT,
            },
        )
        failed = await repo.get("ops.agent_sessions", "s")
        assert failed["status"] == "FAILED"
        await service.send_message({"kind": "MILESTONE", "session_id": "s"})
        await service.process_event("s", end)
        assert (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN"
        assert current["active_message_id"] is None
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][0]["terminal_failure"] == failed["message_log"][0]["terminal_failure"]
        assert current["message_log"][1]["turn_status"] == "DONE"
        assert len([request for request in requests if request.method == "POST"]) == 1
        await service.close()


@pytest.mark.integration
@pytest.mark.parametrize("continuation", ["RUNNING", "UNRETURNED"])
async def test_historical_failure_recovery_cannot_hide_unsettled_tool_work(continuation):
    repo = MemoryRepository()
    _, end = await seed_reopened_failure(repo)
    call = {
        "session_id": "s",
        "call_id": "unfinished-tool",
        "tool": "add_observation",
        "status": "DONE" if continuation == "RUNNING" else "PENDING",
        "native_turn_message_id": "root",
        "native_turn_retry_count": 1,
    }
    if continuation == "RUNNING":
        call.update(remote_event_id="input-unfinished", returned_at=AT)
    await repo.put("ops.tool_calls", "s:unfinished-tool", call)
    states = {"input-failed-retry": "FAILED", "input-milestone": "COMPLETED", "input-unfinished": "RUNNING"}
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(native_reader(states, [end], requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        result = await service.completed_turn("s")
        assert not result["complete"]
        assert "input-unfinished" in result["pending_input_ids"] or (
            "unreturned-tool:unfinished-tool" in result["unidentified_inputs"]
        )
        current = await repo.get("ops.agent_sessions", "s")
        assert current["active_message_id"] == "milestone"
        assert current["message_log"][1]["turn_status"] != "DONE"
        job = await waiting_job(repo)
        with pytest.raises(AdmissionDeferred):
            await service.admit(job)
        await service.close()


@pytest.mark.parametrize("failed_input", ["message", "effectful-tool-result"])
async def test_current_exhausted_native_failure_remains_failure_not_successful_completion(failed_input):
    repo = MemoryRepository()
    session = await seed(repo)
    session["message_log"][0].update(
        retry_count=1, turn_status="FAILED" if failed_input == "message" else "RUNNING"
    )
    session["message_log"][0]["native_input_attempts"] = {"input-root": 1}
    identity = "input-root" if failed_input == "message" else "input-effectful-result"
    if failed_input == "effectful-tool-result":
        await repo.put(
            "ops.tool_calls",
            "s:effectful-tool",
            {
                "session_id": "s",
                "call_id": "effectful-tool",
                "tool": "add_observation",
                "status": "DONE",
                "native_turn_message_id": "root",
                "native_turn_retry_count": 1,
                "remote_event_id": identity,
                "returned_at": AT,
                "result": {"obs_id": "existing-observation"},
            },
        )
    end = idle("failed-finalizer-end", identity)
    session["native_execution_receipts"] = {end["id"]: end}
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    states = {"input-root": "COMPLETED", identity: "FAILED"}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(native_reader(states, [end], requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        assert not (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "FAILED"
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][0]["terminal_failure"]["execution_id"] == identity
        assert "root" not in current.get("retired_message_ids", [])
        assert (await repo.get("ingest.source_items", "i"))["extraction_status"] == "FAILED"
        if failed_input == "effectful-tool-result":
            call = await repo.get("ops.tool_calls", "s:effectful-tool")
            assert call["status"] == "DONE"
            assert call["result"] == {"obs_id": "existing-observation"}
            assert call["remote_event_id"] == "input-effectful-result"
        await service.close()

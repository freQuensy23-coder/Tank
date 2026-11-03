import asyncio
import copy
import json
from contextlib import asynccontextmanager
from datetime import timedelta

import httpx
import pytest
from test_agent_completion_reminder import reminders, settings
from test_agents_execution_barrier import idle
from test_session_admission import fixture_repo
from test_session_binding_concurrency import seed

from tank.agents.context import CONTEXT_VERSION
from tank.agents.sessions import AdmissionDeferred, SessionService
from tank.event_tools import EventToolService
from tank.ops_cards import snapshot
from tank.settings import Settings
from tank.util import iso, utcnow

BACKENDS = ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]


@asynccontextmanager
async def peer(backend, handler):
    if backend == "memory":
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            yield http, "http://native"
        return
    connections = set()

    async def respond(reader, writer):
        connections.add(writer)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            lines = header.decode().split("\r\n")
            method, path, _ = lines[0].split()
            length = next(
                (int(line.split(":", 1)[1]) for line in lines if line.lower().startswith("content-length:")),
                0,
            )
            body = await reader.readexactly(length)
            response = handler(httpx.Request(method, "http://native" + path, content=body))
            if asyncio.iscoroutine(response):
                response = await response
            content = response.content
            writer.write(
                f"HTTP/1.1 {response.status_code} OK\r\nContent-Type: application/json\r\nContent-Length: {len(content)}\r\nConnection: close\r\n\r\n".encode()
                + content
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            connections.discard(writer)

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    try:
        async with httpx.AsyncClient() as http:
            yield http, f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    finally:
        server.close()
        await server.wait_closed()
        for writer in list(connections):
            writer.close()
            await writer.wait_closed()


async def cached_source(repo, service, domain):
    session, arguments = await seed(repo)
    result = await domain.dispatch("record_event", arguments, {"session_id": "s"})
    assert result["event_id"]
    await service.process_event("s", idle("native-end", "input"))
    session = await repo.get("ops.agent_sessions", "s")
    session["config_version"] = "test"
    session["code_version"] = "test"
    session["model_context_version"] = CONTEXT_VERSION
    session["messages"] = ["m"]
    session["message_log"][0].update(turn_status="DONE", completion_checked=True)
    session["native_execution_states"] = {
        "input": {"execution_id": "input", "state": "COMPLETED", "settled_outcome": "COMPLETED"}
    }
    session["verified_native_completion"] = await service.execution_barrier(session)
    assert session["verified_native_completion"]["complete"]
    session["verified_source_completion"] = await service.source_completion_snapshot(session)
    await repo.put("ops.agent_sessions", "s", session)
    job = await repo.enqueue("AGENT_MESSAGE", session["message_log"][0]["payload"], "i:NEW_ITEM")
    job["status"] = "DONE"
    await repo.put("ops.jobs", job["job_id"], job)
    job = await repo.get("ops.jobs", job["job_id"])
    return session, job


def native_handler(request):
    if request.url.path.endswith("/executions"):
        return httpx.Response(
            200,
            json={
                "data": [
                    {"execution_id": key, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                    for key in request.url.params.get_list("ids[]")
                ]
            },
        )
    if request.url.path.endswith("/events") and request.method == "GET":
        return httpx.Response(200, json={"data": []})
    raise AssertionError(str(request.url))


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("legacy_reminder", [False, True])
async def test_cached_checked_source_reconciles_once_across_controllers_and_restart(
    backend, legacy_reminder, tmp_path, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings()
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        first = SessionService(repo, config, domain.dispatch, http=http)
        second = SessionService(repo, config, domain.dispatch, http=http)
        original, source_job = await cached_source(repo, first, domain)
        if legacy_reminder:
            await first.remind_unassessed(
                original, original["message_log"][0], 20, expires_at=source_job["expires_at"]
            )
            await repo.put("ops.agent_sessions", "s", original)
        item = await repo.get("ingest.source_items", "i")
        event = await repo.get("evt.events", original["event_id"])
        results = list(await asyncio.gather(first.settled_turn("s"), second.settled_turn("s")))
        restarted = SessionService(repo, config, domain.dispatch, http=http)
        for _ in range(3):
            results.append(await restarted.settled_turn("s"))
        assert all(result == original["verified_native_completion"] for result in results)
        [job] = await reminders(repo)
        assert job["payload"]["event_id"] == original["event_id"]
        assert job["payload"]["turn_message_id"] == "m"
        assert job["payload"]["reminded_kind"] == "NEW_ITEM"
        assert job["expires_at"] == source_job["expires_at"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["completion_reminders_sent"] == 1
        assert current["assessment_reminder_attempts"] == {original["event_id"]: 1}
        assert current["assessment_obligation_verified"] == {
            "version": 1,
            "event_id": original["event_id"],
            "message_id": "m",
            "native_input_id": "input",
            "retry_attempt": 0,
        }
        assert current["verified_native_completion"] == original["verified_native_completion"]
        assert current["verified_source_completion"] == original["verified_source_completion"]
        assert await repo.get("evt.events", original["event_id"]) == event
        updated = await repo.get("ingest.source_items", "i")
        assert updated["extraction_status"] == "DONE"
        assert updated["observed_at"] == item["observed_at"]
        assert not await repo.list("evt.ems_assessments")


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "fence",
    [
        "assessed",
        "merged",
        "closed",
        "failed",
        "ended",
        "archived",
        "terminated",
        "expired",
        "missing_job",
        "reanalysed",
    ],
)
async def test_fresh_state_before_obligation_commit_prevents_reminder(
    backend, fence, tmp_path, postgres_template, monkeypatch
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings()
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        service = SessionService(repo, config, domain.dispatch, http=http)
        original, job = await cached_source(repo, service, domain)
        waiting, release = asyncio.Event(), asyncio.Event()
        lock = repo.lock
        task = None

        @asynccontextmanager
        async def pause_lock(key):
            if key == "session:s" and asyncio.current_task() is task:
                waiting.set()
                await release.wait()
            async with lock(key):
                yield repo

        monkeypatch.setattr(repo, "lock", pause_lock)
        task = asyncio.create_task(service.settled_turn("s"))
        await asyncio.wait_for(waiting.wait(), 2)
        event = await repo.get("evt.events", original["event_id"])
        if fence == "assessed":
            event["current_ems_id"] = "already-accepted"
        elif fence == "merged":
            await repo.put("evt.events", "earlier-parent", dict(event, event_id="earlier-parent"))
            event.update(lifecycle="MERGED", merged_into="earlier-parent")
        elif fence == "closed":
            event["lifecycle"] = "CLOSED"
        elif fence in {"failed", "ended"}:
            original["status"] = fence.upper()
        elif fence == "archived":
            original["archived_at"] = iso()
        elif fence == "terminated":
            original["remote_state"] = {"status": "terminated"}
        elif fence == "expired":
            job.update(status="EXPIRED", expires_at=iso(utcnow() - timedelta(seconds=1)))
            await repo.put("ops.jobs", job["job_id"], job)
        elif fence == "missing_job":
            await repo.delete("ops.jobs", job["job_id"])
        elif fence == "reanalysed":
            item = await repo.get("ingest.source_items", "i")
            item.update(reanalysis_attempt=1, extraction_status="PENDING")
            await repo.put("ingest.source_items", "i", item)
        await repo.put("evt.events", event["event_id"], event)
        await repo.put("ops.agent_sessions", "s", original)
        release.set()
        try:
            await asyncio.wait_for(task, 3)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert not await reminders(repo)
        current = await repo.get("ops.agent_sessions", "s")
        assert current.get("completion_reminders_sent", 0) == 0
        assert await repo.get("evt.events", event["event_id"]) == event
        assert await repo.get("ops.jobs", job["job_id"]) == (None if fence == "missing_job" else job)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("limit", [1, 2])
async def test_new_committed_refused_assessment_turn_spends_one_next_bounded_attempt(
    backend, limit, tmp_path, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings(limit=limit)
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        service = SessionService(repo, config, domain.dispatch, http=http)
        original, _ = await cached_source(repo, service, domain)
        await service.settled_turn("s")
        for attempt in range(1, limit + 1):
            job = (await reminders(repo))[-1]
            assert job["payload"]["attempt"] == attempt
            with pytest.raises(ValueError):
                await domain.dispatch(
                    "set_assessment",
                    {"event_id": original["event_id"], "assessment": {}},
                    {"session_id": "s"},
                )
            current = await repo.get("ops.agent_sessions", "s")
            identity = "reminder-input-" + str(attempt)
            current["message_log"].append(
                {
                    "message_id": "reminder-turn-" + str(attempt),
                    "remote_event_id": identity,
                    "retry_count": 0,
                    "payload": copy.deepcopy(job["payload"]),
                    "turn_status": "RUNNING",
                }
            )
            current["native_execution_states"][identity] = {
                "execution_id": identity,
                "state": "COMPLETED",
                "settled_outcome": "COMPLETED",
            }
            await repo.put("ops.agent_sessions", "s", current)
            await service.process_event("s", idle("reminder-end-" + str(attempt), identity))
            assert (await service.settled_turn("s"))["complete"]
            assert (await service.settled_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["completion_reminders_sent"] == limit
        assert current["assessment_obligation_verified"]["message_id"] == "reminder-turn-" + str(limit)
        assert len(await reminders(repo)) == limit
        item = await repo.get("ingest.source_items", "i")
        assert item["extraction_status"] == "DONE" and item["assessment_incomplete"] is True
        assert (await repo.get("evt.events", original["event_id"])).get("current_ems_id") is None
        assert not await repo.list("evt.ems_assessments")


@pytest.mark.parametrize("backend", BACKENDS)
async def test_obligation_enqueue_and_receipt_roll_back_together(
    backend, tmp_path, postgres_template, monkeypatch
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings()
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        service = SessionService(repo, config, domain.dispatch, http=http)
        original, _ = await cached_source(repo, service, domain)
        enqueue = repo.enqueue

        async def interrupted(*args, **kwargs):
            job = await enqueue(*args, **kwargs)
            if job["payload"].get("missing") == "ASSESSMENT":
                raise RuntimeError("interrupted obligation transaction")
            return job

        with monkeypatch.context() as context:
            context.setattr(repo, "enqueue", interrupted)
            with pytest.raises(RuntimeError, match="interrupted obligation transaction"):
                await service.settled_turn("s")
        assert not await reminders(repo)
        assert await repo.get("ops.agent_sessions", "s") == original
        restarted = SessionService(repo, config, domain.dispatch, http=http)
        assert (await restarted.settled_turn("s"))["complete"]
        [job] = await reminders(repo)
        assert job["payload"]["attempt"] == 1
        assert (await repo.get("ops.agent_sessions", "s"))["completion_reminders_sent"] == 1


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("expired", [False, True])
async def test_merge_handoff_keeps_its_deadline_and_session_event_not_display_event(
    backend, expired, tmp_path, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings()
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        service = SessionService(repo, config, domain.dispatch, http=http)
        original, _ = await cached_source(repo, service, domain)
        owner = original["event_id"]
        source = {"kind": "ITEM_ADDED", "item_id": "i", "event_id": owner, "session_id": "s"}
        original["message_log"][0]["payload"] = source
        await repo.put("ops.agent_sessions", "s", original)
        observations = await repo.list("evt.event_observations", filters={"item_id": "i", "event_id": owner})
        [observation] = observations
        expiry = iso(utcnow() + timedelta(hours=-1 if expired else 1))
        merged = await repo.enqueue(
            "AGENT_MESSAGE", source, "merge:" + observation["obs_id"], expires_at=expiry
        )
        event = await repo.get("evt.events", owner)
        await repo.put("evt.events", "foreign-display", dict(event, event_id="foreign-display"))
        await repo.put(
            "evt.event_observations",
            "zzz-display",
            dict(observation, obs_id="zzz-display", event_id="foreign-display"),
        )
        displayed = await snapshot(repo, "i")
        assert displayed is not None and displayed["event"]["event_id"] == "foreign-display"
        assert (await service.settled_turn("s"))["complete"]
        jobs = await reminders(repo)
        current = await repo.get("ops.agent_sessions", "s")
        assert current["assessment_obligation_verified"]["event_id"] == owner
        assert current.get("completion_reminders_sent", 0) == (0 if expired else 1)
        if expired:
            assert not jobs
        else:
            [job] = jobs
            assert job["payload"]["event_id"] == owner
            assert job["payload"]["reminded_kind"] == "ITEM_ADDED"
            assert job["expires_at"] == merged["expires_at"]
        assert await repo.get("evt.events", owner) == event
        assert await repo.get("evt.events", "foreign-display") == dict(event, event_id="foreign-display")


@pytest.mark.integration
@pytest.mark.postgres
async def test_real_http_refused_tool_ack_barrier_then_obligation_and_other_job_admission(
    tmp_path, postgres_template
):
    ack_waiting, release_ack = asyncio.Event(), asyncio.Event()
    posts = []

    async def handler(request):
        if request.method == "POST":
            body = json.loads(request.content)["events"][0]
            posts.append(body)
            assert body["type"] == "user.custom_tool_result"
            ack_waiting.set()
            await release_ack.wait()
            return httpx.Response(200, json={"data": [{"id": "callback"}]})
        return native_handler(request)

    async with fixture_repo("postgres", postgres_template) as repo, peer("postgres", handler) as (http, url):
        config = settings()
        config.openma_url = url
        load_config = config.load_config
        config.load_config = lambda name: (
            {"max_running_sessions": 1} if name == "openma" else load_config(name)
        )
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        service = SessionService(repo, config, domain.dispatch, http=http)
        original, _ = await cached_source(repo, service, domain)
        current = await repo.get("ops.agent_sessions", "s")
        current.update(runtime_status="RUNNING", active_message_id="m")
        await repo.put("ops.agent_sessions", "s", current)
        waiting = await repo.enqueue(
            "SCOUT_REVIEW", {"kind": "SCOUT_REVIEW", "proposal_id": "other"}, "other", priority=0
        )
        tool = asyncio.create_task(
            service.process_event(
                "s",
                {
                    "id": "refused-assessment",
                    "type": "agent.custom_tool_use",
                    "execution_id": "input",
                    "name": "set_assessment",
                    "input": {"assessment": {}},
                },
            )
        )
        acknowledgement = asyncio.create_task(ack_waiting.wait())
        try:
            done, _ = await asyncio.wait(
                {tool, acknowledgement}, timeout=3, return_when=asyncio.FIRST_COMPLETED
            )
            if tool in done:
                await tool
            assert acknowledgement in done
            call = await repo.get("ops.tool_calls", "s:refused-assessment")
            assert call["status"] == "REFUSED" and not call.get("returned_at")
            assert call["result"]["error"]["reason"]
            assert not (await service.settled_turn("s"))["complete"]
            assert not await reminders(repo)
            with pytest.raises(AdmissionDeferred):
                await service.admit(waiting)
            assert (await repo.get("ops.jobs", waiting["job_id"]))["attempts"] == 0
            release_ack.set()
            await asyncio.wait_for(tool, 3)
            call = await repo.get("ops.tool_calls", "s:refused-assessment")
            assert call["status"] == "REFUSED" and call["remote_event_id"] == "callback"
            assert call["returned_at"] and len(posts) == 1
            await service.process_event("s", idle("callback-end", "callback"))
            assert (await service.settled_turn("s"))["complete"]
            [job] = await reminders(repo)
            assert job["payload"]["missing"] == "ASSESSMENT"
            assert (await repo.get("ingest.source_items", "i"))["extraction_status"] == "DONE"
            assert (await repo.get("evt.events", original["event_id"])).get("current_ems_id") is None
            await service.admit(waiting)
            admitted = await repo.get("ops.jobs", waiting["job_id"])
            assert admitted["slot_admitted"] is True and admitted["attempts"] == 0
            await service.settled_turn("s")
            assert len(await reminders(repo)) == 1 and len(posts) == 1
        finally:
            release_ack.set()
            tool.cancel()
            acknowledgement.cancel()
            await asyncio.gather(tool, acknowledgement, return_exceptions=True)

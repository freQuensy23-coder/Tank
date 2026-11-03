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

from tank.agents.sessions import SessionService
from tank.event_tools import EventToolService
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
async def test_cached_checked_source_reconciles_once_across_controllers_and_restart(
    backend, tmp_path, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings()
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        first = SessionService(repo, config, domain.dispatch, http=http)
        second = SessionService(repo, config, domain.dispatch, http=http)
        original, source_job = await cached_source(repo, first, domain)
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
@pytest.mark.parametrize("fence", ["assessed", "merged", "closed", "failed", "ended", "archived", "terminated", "expired", "reanalysed"])
async def test_fresh_state_before_obligation_commit_prevents_reminder(
    backend, fence, tmp_path, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings()
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        service = SessionService(repo, config, domain.dispatch, http=http)
        original, job = await cached_source(repo, service, domain)
        event = await repo.get("evt.events", original["event_id"])
        if fence == "assessed":
            event["current_ems_id"] = "already-accepted"
        elif fence == "merged":
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
        elif fence == "reanalysed":
            item = await repo.get("ingest.source_items", "i")
            item.update(reanalysis_attempt=1, extraction_status="PENDING")
            await repo.put("ingest.source_items", "i", item)
        await repo.put("evt.events", event["event_id"], event)
        await repo.put("ops.agent_sessions", "s", original)
        await service.settled_turn("s")
        assert not await reminders(repo)
        current = await repo.get("ops.agent_sessions", "s")
        assert current.get("completion_reminders_sent", 0) == 0
        assert await repo.get("evt.events", event["event_id"]) == event
        assert await repo.get("ops.jobs", job["job_id"]) == job


@pytest.mark.parametrize("backend", BACKENDS)
async def test_new_committed_refused_assessment_turn_spends_one_next_bounded_attempt(
    backend, tmp_path, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo, peer(backend, native_handler) as (http, url):
        config = settings(limit=1)
        config.openma_url = url
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        service = SessionService(repo, config, domain.dispatch, http=http)
        original, _ = await cached_source(repo, service, domain)
        await service.settled_turn("s")
        [job] = await reminders(repo)
        with pytest.raises(ValueError):
            await domain.dispatch("set_assessment", {"assessment": {}}, {"session_id": "s"})
        current = await repo.get("ops.agent_sessions", "s")
        current["message_log"].append(
            {
                "message_id": "reminder-turn",
                "remote_event_id": "reminder-input",
                "retry_count": 0,
                "payload": copy.deepcopy(job["payload"]),
                "turn_status": "RUNNING",
            }
        )
        current["native_execution_states"]["reminder-input"] = {
            "execution_id": "reminder-input", "state": "COMPLETED", "settled_outcome": "COMPLETED"
        }
        await repo.put("ops.agent_sessions", "s", current)
        await service.process_event("s", idle("reminder-end", "reminder-input"))
        assert (await service.settled_turn("s"))["complete"]
        assert (await service.settled_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["completion_reminders_sent"] == 1
        assert current["assessment_obligation_verified"]["message_id"] == "reminder-turn"
        assert len(await reminders(repo)) == 1
        item = await repo.get("ingest.source_items", "i")
        assert item["extraction_status"] == "DONE" and item["assessment_incomplete"] is True
        assert (await repo.get("evt.events", original["event_id"]))["current_ems_id"] is None
        assert not await repo.list("evt.ems_assessments")

"""Blocked external tools cannot prevent unrelated durable work from progressing."""

import asyncio
import json

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from test_agents import seed_session, settings
from test_session_binding_concurrency import fixture_repo, seed

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@pytest.mark.parametrize("tool", ["web_search", "fetch_page", "get_market_truth"])
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_external_tool_burst_allows_unrelated_worker_progress(backend, tool, postgres_template) -> None:
    async with fixture_repo(backend, postgres_template) as repo:
        await exercise_external_tool_burst(repo, tool)


async def exercise_external_tool_burst(repo, tool) -> None:
    opts, _ = settings()
    release, all_entered = asyncio.Event(), asyncio.Event()
    entered, posts = set(), []
    sessions = []
    template, _ = await seed(repo)
    from test_event_tools import complete_event_fixture

    for index in range(24):
        event_id = f"fixture-event-{index}"
        await repo.put("evt.events", event_id, complete_event_fixture(event_id))
        session = dict(template, session_id=f"session-{index}", event_id=event_id)
        await repo.put("ops.agent_sessions", session["session_id"], session)
        sessions.append(session)

    async def dispatch(name, arguments, context):
        entered.add(context["session_id"])
        if len(entered) == len(sessions):
            all_entered.set()
        await release.wait()
        return {"ok": True, "session_id": context["session_id"]}

    def acknowledge(request: pytest.FixtureRequest):
        posts.append(json.loads(request.content)["events"][0])
        return httpx.Response(200, json={"data": [{"id": f"ack-{len(posts)}"}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as http:
        controller = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        tasks = [
            asyncio.create_task(
                controller.process_event(
                    session["session_id"],
                    {"id": "call", "type": "agent.custom_tool_use", "name": tool, "input": {}},
                )
            )
            for session in sessions
        ]
        try:
            await asyncio.wait_for(all_entered.wait(), timeout=2)

            await asyncio.wait_for(
                repo.put("ops.agent_sessions", "worker", dict(template, session_id="worker")),
                timeout=2,
            )
            assert (await repo.get("ops.agent_sessions", "worker"))["status"] == "OPEN"
        finally:
            release.set()
            await asyncio.gather(*tasks)
        assert len(posts) == len(sessions)
        for session in sessions:
            call = await repo.get("ops.tool_calls", session["session_id"] + ":call")
            assert call["status"] == "DONE" and call["returned_at"]
            assert call["native_turn_call_index"] == 0
            await controller.process_event(
                session["session_id"],
                {"id": "call", "type": "agent.custom_tool_use", "name": tool, "input": {}},
            )
        assert len(posts) == len(sessions)


@pytest.mark.parametrize("status", [404, 503])
async def test_external_error_survives_lost_ack_without_refetch_or_new_ordinal(status) -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo)
    dispatched, posts = [], []

    async def dispatch(name, arguments, context) -> None:
        dispatched.append(name)
        request = httpx.Request("GET", "https://source.invalid/article")
        httpx.Response(status, request=request).raise_for_status()

    def acknowledge(request: pytest.FixtureRequest):
        posts.append(json.loads(request.content)["events"][0])
        if len(posts) == 1:
            raise httpx.ConnectError("Lost native ACK", request=request)
        return httpx.Response(200, json={"data": [{"id": "ack"}]})

    event = {
        "id": "call",
        "type": "agent.custom_tool_use",
        "name": "fetch_page",
        "input": {"url": "https://source.invalid/article"},
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as http:
        first = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        with pytest.raises(httpx.ConnectError):
            await first.process_event("ses_1", event)
        stored = await repo.get("ops.tool_calls", "ses_1:call")
        assert stored["status"] == "ERROR" and stored["is_error"]
        assert not stored.get("returned_at")
        resumed = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        await resumed.process_event("ses_1", event)
        await resumed.process_event("ses_1", event)
    assert dispatched == ["fetch_page"]
    assert len(posts) == 2 and posts[0] == posts[1]
    assert posts[1]["is_error"] is True
    stored = await repo.get("ops.tool_calls", "ses_1:call")
    assert stored["status"] == "ERROR" and stored["returned_at"]
    assert stored["native_turn_call_index"] == 0
    assert (await repo.get("ops.agent_sessions", "ses_1"))["next_tool_call_ordinal"] == 1
    assert (await repo.get("ops.agent_sessions", "ses_1"))["status"] == "OPEN"

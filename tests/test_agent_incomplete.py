"""A turn that ends without an event, observation or rejection fails the item, not the session."""

import json
from types import SimpleNamespace

import httpx
import pytest
from test_agents_session_audit import settings as session_settings
from test_ops_cards import source

from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.agents.incomplete import AGENT_INCOMPLETE, fail_incomplete_turn, reanalyse
from tank.ops_cards import render, snapshot
from tank.ops_progress import item_progress, session_progress
from tank.storage import MemoryRepository


@pytest.fixture
def settings():
    return SimpleNamespace(ops_progress_enabled=True, ops_progress_delay_s=0)


def item_session(item_id, *, event_id=None, status="DONE"):
    return {
        "session_id": "session-incomplete",
        "event_id": event_id,
        "status": "OPEN",
        "message_log": [
            {
                "message_id": "m",
                "payload": {"kind": "NEW_ITEM", "item_id": item_id},
                "sent_at": "2026-10-28T10:00:00Z",
                "turn_status": status,
            }
        ],
    }


async def test_unfinished_item_becomes_agent_incomplete_and_flags_an_eventless_session():
    repo = MemoryRepository()
    item = await source(repo)
    session = item_session(item["item_id"])
    assert await fail_incomplete_turn(repo, session, item["item_id"])
    stored = await repo.get("ingest.source_items", item["item_id"])
    assert (stored["extraction_status"], stored["extraction_reason"]) == ("FAILED", AGENT_INCOMPLETE)
    assert stored["extraction_failed_session_id"] == "session-incomplete"
    assert stored["extraction_failed_at"]
    assert session["agent_incomplete"] is True


@pytest.mark.parametrize("status", ["DONE", "SKIPPED", "FAILED"])
async def test_a_finished_item_is_left_alone_and_an_event_session_is_not_flagged(status):
    repo = MemoryRepository()
    item = await source(repo, extraction_status=status, extraction_reason="kept")
    session = item_session(item["item_id"], event_id="EVT-private")
    assert not await fail_incomplete_turn(repo, session, item["item_id"])
    assert await repo.get("ingest.source_items", item["item_id"]) == item
    assert "agent_incomplete" not in session
    other = await source(repo, item_id="ITM-other")
    assert await fail_incomplete_turn(repo, session, other["item_id"])
    assert "agent_incomplete" not in session


async def test_incomplete_item_is_an_ops_processing_error_not_a_rejection(settings):
    repo = MemoryRepository()
    item = await source(repo)
    session = item_session(item["item_id"])
    await fail_incomplete_turn(repo, session, item["item_id"])
    await session_progress(repo, settings, session)
    notices = await repo.list("out.ops_notices")
    assert [(row["stage"], row["reason"]) for row in notices] == [("PROCESSING_FAILED", AGENT_INCOMPLETE)]
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "PROCESSING_FAILED"
    assert card["status"] == "Ошибка обработки"
    assert "Агент завершил ход без записи события или решения об отклонении" in card["text"]
    assert "Отклонено агентом" not in card["text"]


async def test_deliberate_rejection_still_renders_as_agent_rejection(settings):
    repo = MemoryRepository()
    item = await source(
        repo,
        extraction_status="SKIPPED",
        extraction_decision="NO_EVENT",
        extraction_reason="Мнение без нового фактического утверждения",
    )
    await item_progress(
        repo, settings, item["item_id"], "AGENT_REJECTED", "Мнение без нового фактического утверждения"
    )
    await session_progress(repo, settings, item_session(item["item_id"]))
    assert {row["stage"] for row in await repo.list("out.ops_notices")} == {"AGENT_REJECTED"}
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "AGENT_REJECTED"
    assert card["status"] == "Отклонено агентом"
    assert "Мнение без нового фактического утверждения" in card["text"]


@pytest.mark.parametrize("flagged", [True, False])
async def test_idle_sweep_archives_an_eventless_incomplete_session_after_retention(flagged):
    repo = MemoryRepository()
    item = await source(repo)
    session = {
        **item_session(item["item_id"]),
        "session_id": "s",
        "item_id": item["item_id"],
        "agent_incomplete": flagged,
        "opened_at": "2026-01-01T00:00:00Z",
    }
    session["message_log"][0]["sent_at"] = "2026-01-01T00:00:00Z"
    await repo.put("ops.agent_sessions", "s", session)
    archived = []

    def handler(request):
        archived.append(request.url.path)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), lambda *args: None, http=http)

        async def complete(sid):
            return {"complete": True, "accepted_input_ids": []}

        async def barrier(current, **kwargs):
            return {"complete": True, "accepted_input_ids": []}

        service.completed_turn = complete
        service.execution_barrier = barrier
        assert await service.finish_if_idle("s")
    stored = await repo.get("ops.agent_sessions", "s")
    assert archived == (["/v1/sessions/s/archive"] if flagged else [])
    assert stored["status"] == ("ENDED" if flagged else "OPEN")


@pytest.mark.parametrize(
    "closed", [{"status": "FAILED"}, {"status": "ENDED"}, {"archived_at": "2026-10-01T00:00:00Z"}]
)
async def test_new_message_for_a_closed_session_opens_a_fresh_one(closed):
    repo = MemoryRepository()
    await repo.put(
        "ops.agent_sessions",
        "dead",
        {
            "session_id": "dead",
            "event_id": "e",
            "status": "OPEN",
            "model_context_version": CONTEXT_VERSION,
            **closed,
        },
    )
    creates = []

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        creates.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "fresh"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), lambda *args: None, http=http)
        fresh = await service.resolve({"kind": "ITEM_ADDED", "event_id": "e", "item_id": "i"})
        assert fresh["session_id"] == "fresh" and fresh["event_id"] == "e"
        assert (await service.resolve({"kind": "ITEM_ADDED", "event_id": "e", "item_id": "j"}))[
            "session_id"
        ] == ("fresh")
    assert len(creates) == 1
    assert creates[0]["metadata"]["tank_identity"] != (await repo.get("ops.agent_sessions", "dead")).get(
        "tank_identity"
    )


async def test_open_session_is_still_reused():
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "live", {"session_id": "live", "event_id": "e", "status": "OPEN"})

    def handler(request):
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), lambda *args: None, http=http)
        assert (await service.resolve({"kind": "ITEM_ADDED", "event_id": "e"}))["session_id"] == "live"


async def test_reanalysis_dry_run_lists_without_changing_and_apply_requeues_once_per_attempt():
    repo = MemoryRepository()
    failed = await source(
        repo,
        item_id="ITM-failed",
        extraction_status="FAILED",
        extraction_reason=AGENT_INCOMPLETE,
        routing_message={"kind": "NEW_ITEM", "item_id": "ITM-failed"},
        route={"decision": "NEW_SESSION"},
    )
    legacy = await source(
        repo,
        item_id="ITM-legacy",
        extraction_status="SKIPPED",
        extraction_reason="NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION",
    )
    await source(repo, item_id="ITM-rejected", extraction_status="SKIPPED", extraction_reason="Мнение")
    await source(repo, item_id="ITM-crashed", extraction_status="FAILED")
    await source(repo, item_id="ITM-done", extraction_status="DONE")

    preview = await reanalyse(repo)
    assert preview["apply"] is False
    assert sorted(row["item_id"] for row in preview["items"]) == ["ITM-failed", "ITM-legacy"]
    assert await repo.get("ingest.source_items", "ITM-failed") == failed
    assert await repo.list("ops.jobs") == []

    applied = await reanalyse(repo, item_ids=["ITM-failed", "ITM-done"], apply=True)
    assert [(row["item_id"], row["queued"]) for row in applied["items"]] == [("ITM-failed", True)]
    item = await repo.get("ingest.source_items", "ITM-failed")
    assert item["extraction_status"] == "PENDING" and item["extraction_reason"] is None
    assert item["reanalysis_attempt"] == 1 and "routing_message" not in item and "route" not in item
    assert item["reanalysis_history"][0]["previous_reason"] == AGENT_INCOMPLETE
    jobs = await repo.list("ops.jobs")
    assert [(job["type"], job["payload"], job["idem_key"]) for job in jobs] == [
        ("ROUTE_ITEM", {"item_id": "ITM-failed"}, "ITM-failed:reanalysis:1")
    ]
    assert (await reanalyse(repo, item_ids=["ITM-failed"], apply=True))["items"] == []

    await reanalyse(repo, item_ids=[legacy["item_id"]], apply=True)
    assert (await repo.get("ingest.source_items", "ITM-legacy"))["extraction_status"] == "PENDING"
    assert len(await repo.list("ops.jobs")) == 2


@pytest.mark.parametrize(
    "closed", [{"status": "FAILED"}, {"status": "ENDED"}, {"archived_at": "2026-10-01T00:00:00Z"}]
)
async def test_condition_addressed_to_a_closed_session_is_routed_by_its_event(closed):
    repo = MemoryRepository()
    await repo.put(
        "ops.agent_sessions",
        "dead",
        {
            "session_id": "dead",
            "event_id": "e",
            "status": "OPEN",
            "model_context_version": CONTEXT_VERSION,
            **closed,
        },
    )
    await repo.put("ops.agent_sessions", "live", {"session_id": "live", "event_id": "e", "status": "OPEN"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500))) as http:
        service = SessionService(repo, session_settings(), lambda *args: None, http=http)
        fired = {"kind": "CONDITION_FIRED", "event_id": "e", "session_id": "dead", "condition_id": "c"}
        readdressed = await service.readdressed(fired)
        assert readdressed == {"kind": "CONDITION_FIRED", "event_id": "e", "condition_id": "c"}
        assert (await service.resolve(readdressed))["session_id"] == "live"
        merged = {"kind": "ITEM_ADDED", "event_id": "e", "item_id": "i", "session_id": "dead"}
        assert (await service.resolve(await service.readdressed(merged)))["session_id"] == "live"
        to_live = dict(fired, session_id="live")
        assert await service.readdressed(to_live) == to_live
        question = {"kind": "USER_QUESTION", "event_id": "e", "session_id": "dead", "question_id": "q"}
        assert await service.readdressed(question) == question


async def test_a_retained_session_receives_changed_tool_schemas_at_its_next_input():
    repo = MemoryRepository()
    item = await source(repo)
    await repo.put(
        "ops.agent_sessions",
        "kept",
        {
            "session_id": "kept",
            "event_id": "e",
            "status": "OPEN",
            "runtime_status": "IDLE",
            "model_context_version": CONTEXT_VERSION,
            "tools_sha256": "digest-of-the-previous-schemas",
            "message_log": [],
            "messages": [],
        },
    )
    nullable = [
        {
            "name": "list_open_events",
            "description": "d",
            "input_schema": {"type": "object", "properties": {"token_id": {"type": ["string", "null"]}}},
        }
    ]
    settings = session_settings()
    settings.read_config_json = lambda relative: nullable
    requests = []

    def handler(request):
        requests.append((request.method, request.url.path, json.loads(request.content or b"{}")))
        if request.method == "GET" and request.url.path == "/v1/sessions/kept":
            return httpx.Response(200, json={"id": "kept", "status": "idle", "agent": {"id": "agt"}})
        if request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        return httpx.Response(200, json={"id": "evt-input"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings, lambda *args: None, http=http)
        assert service.tools_changed(await repo.get("ops.agent_sessions", "kept"))
        try:
            await service.send_message({"kind": "ITEM_ADDED", "event_id": "e", "item_id": item["item_id"]})
        finally:
            await service.stop_followers()
        synced = await repo.get("ops.agent_sessions", "kept")
    updates = [row for row in requests if row[:2] == ("POST", "/v1/sessions/kept")]
    assert len(updates) == 1
    assert updates[0][2]["agent"]["tools"][-1]["input_schema"]["properties"]["token_id"]["type"] == [
        "string",
        "null",
    ]
    assert not service.tools_changed(synced)


async def test_admission_looks_up_the_same_generation_as_routing():
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "dead", {"session_id": "dead", "event_id": "e", "status": "ENDED"})
    await repo.put(
        "ops.agent_sessions",
        "live",
        {"session_id": "live", "event_id": "e", "status": "OPEN", "runtime_status": "RUNNING"},
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500))) as http:
        service = SessionService(repo, session_settings(), lambda *args: None, http=http)
        target = await service.existing_session({"kind": "ITEM_ADDED", "event_id": "e", "item_id": "i"})
        fired = await service.existing_session(
            {"kind": "CONDITION_FIRED", "event_id": "e", "session_id": "dead"}
        )
    assert (target["session_id"], target["runtime_status"]) == ("live", "RUNNING")
    assert fired["session_id"] == "live"

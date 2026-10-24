"""Persistent sessions switch to the ID contract at an idle boundary, without new record IDs in context."""

import copy
import json
from pathlib import Path

import httpx
import pytest

from tank.agents.context import CONTEXT_TRANSITION, CONTEXT_VERSION, ModelContext
from tank.agents.sessions import SessionService
from tank.delivery import DeliveryDeferred
from tank.event_tools import EventToolService
from tank.openma_sync import agent_tools
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, new_id, sha256


@pytest.fixture
async def legacy(tmp_path: Path):
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, openma_url="http://oma", _env_file=None)
    item = {
        "item_id": new_id("itm"),
        "url": "https://example.org/launch",
        "text": "Live",
        "observed_at": iso(),
    }
    event = {"event_id": new_id("evt"), "statement": "Protocol launch", "public_at": iso()}
    payload = {"kind": "NEW_ITEM", "item_id": item["item_id"]}
    session = {
        "session_id": "session_legacy",
        "model_context_version": CONTEXT_VERSION - 1,
        "model_catalog": {"events": {event["event_id"]: {"statement": "Stale description"}}},
        "event_id": event["event_id"],
        "item_id": item["item_id"],
        "status": "OPEN",
        "runtime_status": "IDLE",
        "model_id": "test",
        "prompt_version": sha256("legacy snapshot"),
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "messages": ["old_message"],
        "message_log": [{"message_id": "old_message", "payload": payload, "turn_status": "DONE"}],
        "last_payload": payload,
    }
    await repo.put("ingest.source_items", item["item_id"], item)
    await repo.put("evt.events", event["event_id"], event)
    await repo.put("ops.agent_sessions", session["session_id"], session)
    return repo, settings, session, item, event


class NativeSession:
    def __init__(self) -> None:
        self.status = "idle"
        self.requests = []
        self.tools = []
        self.fail_update = None
        self.lose_result = False

    def __call__(self, request: pytest.FixtureRequest):
        body = json.loads(request.content or b"{}")
        self.requests.append((request.method, request.url.path, body))
        if request.method == "GET":
            return httpx.Response(
                200, json={"id": "session_legacy", "status": self.status, "agent": {"tools": self.tools}}
            )
        if request.url.path == "/v1/sessions/session_legacy":
            if self.fail_update:
                return httpx.Response(self.fail_update, json={})
            assert self.status == "idle"
            self.tools = body["agent"]["tools"]
            return httpx.Response(200, json={})
        assert request.url.path == "/v1/sessions/session_legacy/events"
        if self.lose_result and body["events"][0]["type"] == "user.custom_tool_result":
            self.lose_result = False
            raise httpx.ReadError("lost acceptance", request=request)
        return httpx.Response(200, json={"data": [{"id": "accepted_input"}]})


@pytest.mark.parametrize(
    "kind", ["ITEM_ADDED", "MILESTONE", "CONDITION_FIRED", "CALL_EXPIRED", "SCOUT_REVIEW"]
)
async def test_legacy_callbacks_upgrade_before_delivery_and_new_turns_reject_ids(legacy, kind) -> None:
    repo, settings, session, item, event = legacy
    payload = {
        "kind": kind,
        "session_id": session["session_id"],
        "event_id": event["event_id"],
        "item_id": item["item_id"],
    }
    if kind == "SCOUT_REVIEW":
        session["proposal_id"] = "proposal_legacy"
        await repo.put(
            "scout.expansion_proposals",
            "proposal_legacy",
            {"prp_id": "proposal_legacy", "concept_name": "New source"},
        )
        await repo.put("ops.agent_sessions", session["session_id"], session)
    original_history = copy.deepcopy(session["message_log"])
    native = NativeSession()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as http:
        service = SessionService(
            repo, settings, EventToolService(repo, settings, None, None).dispatch, http=http
        )
        service.start_stream = lambda sid: None
        current = await service.send_message(payload)
        assert current["model_context_version"] == CONTEXT_VERSION
        assert current["message_log"][0] == original_history[0]
        assert current["message_log"][-1]["model_context_version"] == CONTEXT_VERSION
        assert not service.legacy_contract(current) and "model_catalog" not in current
        if kind != "SCOUT_REVIEW":
            with pytest.raises(ValueError, match=r"\$\.event is no longer accepted: pass event_id"):
                await service.invoke_tool(
                    current, "get_event", {"event": {"statement": "Stale description"}}, "old"
                )
            resolved = {}
            read = await service.invoke_tool(
                current, "get_event", {"event_id": event["event_id"]}, "new_call", resolved_arguments=resolved
            )
            assert read["event"]["event_id"] == resolved["event_id"] == event["event_id"]
    posts = [row for row in native.requests if row[0] == "POST"]
    assert [row[1] for row in posts] == ["/v1/sessions/session_legacy", "/v1/sessions/session_legacy/events"]
    text = posts[-1][2]["events"][0]["content"][0]["text"]
    scout = kind == "SCOUT_REVIEW"
    assert json.loads(text)["context_update"] == CONTEXT_TRANSITION[scout]
    for lookup in (
        "«Claims of an event»",
        "«Events by statement, time or token»",
        "«Source document by URL»",
    ):
        assert lookup in text
    assert ("full built-in toolset is now enabled" in text) is scout
    assert ("Finding record IDs for tool arguments" in text) is not scout
    assert current["context_note_version"] == CONTEXT_VERSION
    assert event["event_id"] not in text and item["item_id"] not in text
    assert native.tools == agent_tools(settings, "tank-scout-reviewer" if scout else "tank-event-analyst")
    assert native.tools[0]["default_config"]["enabled"] is True
    assert native.tools[0]["configs"] == [{"name": "browser", "enabled": True}]


async def test_old_description_call_in_a_legacy_turn_is_refused_once_and_the_id_call_runs(legacy) -> None:
    repo, settings, session, item, event = legacy
    session.update(runtime_status="REQUIRES_ACTION", active_message_id="old_message")
    session["message_log"][0]["turn_status"] = "RUNNING"
    item.update(source_id="S01", source_class="PRIMARY")
    event.update(certainty="HIGH", event_state="LIVE", lifecycle="MONITORING", state_verified=True)
    await repo.put("ingest.source_items", item["item_id"], item)
    await repo.put("evt.events", event["event_id"], event)
    await repo.put("ops.agent_sessions", session["session_id"], session)
    count = 0
    domain = EventToolService(repo, settings, None, None)

    async def dispatch(*args):
        nonlocal count
        count += 1
        return await domain.dispatch(*args)

    native = NativeSession()
    native.status = "running"
    claim = {
        "type": "STATE",
        "subject": "Protocol",
        "predicate": "is",
        "object": "live",
        "state_claim": "LIVE",
        "directionality": "BOTH",
        "evidence_class": "PRIMARY_FACT",
        "quote": "Live",
        "verified": True,
    }
    stamp = {"value": item["observed_at"], "precision": "EXACT", "basis": "Source", "quote": None}
    old = {
        "id": "old_call",
        "name": "add_observation",
        "input": {
            "document": {"url": item["url"], "observed_at": item["observed_at"]},
            "link": "CORROBORATION",
            "public_at": stamp,
            "freshness": "FRESH",
            "claims": [claim],
        },
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as http:
        service = SessionService(repo, settings, dispatch, http=http)
        with pytest.raises(ValueError, match="native tool boundary"):
            await service.tool(session, old)
        assert count == 0
        assert not any(row[0] == "POST" for row in native.requests)
        native.status = "idle"
        native.lose_result = True
        with pytest.raises(httpx.ReadError):
            await service.tool(session, old)
        stored = await repo.get("ops.tool_calls", "session_legacy:old_call")
        assert stored["status"] == "REFUSED" and "pass item_id" in stored["result"]["error"]["reason"]
        frozen = stored["native_result_body"]
        current = await repo.get("ops.agent_sessions", session["session_id"])
        assert current["model_context_version"] == CONTEXT_VERSION
        assert current["legacy_model_message_ids"] == ["old_message"]
        restarted = SessionService(repo, settings, dispatch, http=http)
        await restarted.tool(current, old)
        assert count == 0
        result_posts = [row[2]["events"][0] for row in native.requests if row[1].endswith("/events")]
        assert result_posts == [frozen, frozen]
        text = frozen["content"][0]["text"]
        assert "pass item_id" in text and event["event_id"] not in text
        assert json.loads(text)["context_update"] == CONTEXT_TRANSITION[False]
        new = {
            "id": "new_call",
            "name": "add_observation",
            "input": {key: value for key, value in old["input"].items() if key != "document"},
        }
        await restarted.tool(current, new)
        assert count == 1
        claims = await repo.list("evt.claims")
        observations = await repo.list("evt.event_observations")
        assert len(claims) == len(observations) == 1
        assert claims[0]["evidence"] == [{"item_id": item["item_id"], "quote": "Live"}]
        text = [row[2]["events"][0] for row in native.requests if row[1].endswith("/events")][-1]["content"][
            0
        ]["text"]
        assert "Live" in text and claims[0]["claim_id"] not in text and observations[0]["obs_id"] not in text
        assert "context_update" not in text
    bound = await ModelContext(repo, settings, current).arguments("get_decision", {"token_id": "world:WLD"})
    assert bound == {"token_id": "world:WLD", "event_id": event["event_id"]}


async def test_busy_legacy_session_defers_new_callbacks_without_changing_the_active_input(legacy) -> None:
    repo, settings, session, item, event = legacy
    session.update(runtime_status="RUNNING", active_message_id="old_message")
    await repo.put("ops.agent_sessions", session["session_id"], session)
    native = NativeSession()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as http:
        service = SessionService(repo, settings, None, http=http)
        with pytest.raises(DeliveryDeferred):
            await service.send_message(
                {"kind": "ITEM_ADDED", "event_id": event["event_id"], "item_id": item["item_id"]}
            )
    assert not native.requests
    assert await repo.get("ops.agent_sessions", session["session_id"]) == session


@pytest.mark.parametrize("status", [400, 429, 503])
async def test_failed_migration_never_marks_old_contract_current_or_delivers_a_callback(
    legacy, status
) -> None:
    repo, settings, session, _, event = legacy
    native = NativeSession()
    native.fail_update = status
    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as http:
        service = SessionService(repo, settings, None, http=http)
        with pytest.raises(httpx.HTTPStatusError):
            await service.send_message({"kind": "MILESTONE", "event_id": event["event_id"]})
    assert not any(row[1].endswith("/events") for row in native.requests)
    assert await repo.get("ops.agent_sessions", session["session_id"]) == session


async def test_old_remote_snapshot_is_not_recovered_as_a_new_context(legacy) -> None:
    repo, settings, _, _, _ = legacy
    item_id = "new_source"
    requests = []

    def handler(request: pytest.FixtureRequest):
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "old_remote",
                            "metadata": {"tank_identity": sha256({"identity": item_id, "scout": False})},
                        }
                    ]
                },
            )
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "new_remote"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        current = await SessionService(repo, settings, None, http=http).resolve(
            {"kind": "NEW_ITEM", "item_id": item_id}
        )
    assert current["session_id"] == "new_remote" and current["model_context_version"] == CONTEXT_VERSION
    assert len(requests) == 1

"""Continue pre-deployment sessions without exposing new record identifiers."""

import copy
import json

import httpx
import pytest

from tank.agents.context import CONTEXT_VERSION, identifier_field
from tank.agents.sessions import SessionService
from tank.delivery import DeliveryDeferred
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, new_id, sha256


@pytest.fixture
async def legacy(tmp_path):
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
    def __init__(self):
        self.status = "idle"
        self.requests = []
        self.tools = []
        self.fail_update = None
        self.lose_result = False

    def __call__(self, request):
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
async def test_legacy_callbacks_upgrade_before_delivery_and_new_turns_reject_ids(legacy, kind):
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
        assert not service.legacy_contract(current)
        if kind != "SCOUT_REVIEW":
            with pytest.raises(ValueError, match="internal fields"):
                await service.invoke_tool(current, "get_event", {"event_id": event["event_id"]}, "new_call")
    posts = [row for row in native.requests if row[0] == "POST"]
    assert [row[1] for row in posts] == ["/v1/sessions/session_legacy", "/v1/sessions/session_legacy/events"]
    text = posts[-1][2]["events"][0]["content"][0]["text"]
    assert "context_update" in text and event["event_id"] not in text and item["item_id"] not in text
    assert native.tools[0]["default_config"]["enabled"] is (kind != "SCOUT_REVIEW")
    for tool in native.tools[1:]:
        assert not any(identifier_field(key) for key in tool["input_schema"].get("properties", {}))


async def test_legacy_tool_waits_for_native_idle_then_retries_without_repeating_dispatch(legacy):
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
    request = {
        "id": "old_call",
        "name": "add_observation",
        "input": {
            "event_id": event["event_id"],
            "item_id": item["item_id"],
            "link": "CORROBORATION",
            "public_at": {
                "value": item["observed_at"],
                "precision": "EXACT",
                "basis": "Source",
                "quote": None,
            },
            "freshness": "FRESH",
            "claims": [
                {
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
            ],
        },
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as http:
        service = SessionService(repo, settings, dispatch, http=http)
        with pytest.raises(ValueError, match="native tool boundary"):
            await service.tool(session, request)
        assert count == 1
        assert not any(row[0] == "POST" for row in native.requests)
        native.status = "idle"
        native.lose_result = True
        with pytest.raises(httpx.ReadError):
            await service.tool(session, request)
        frozen = (await repo.get("ops.tool_calls", "session_legacy:old_call"))["native_result_body"]
        current = await repo.get("ops.agent_sessions", session["session_id"])
        assert current["model_context_version"] == CONTEXT_VERSION
        assert current["legacy_model_message_ids"] == ["old_message"]
        restarted = SessionService(repo, settings, dispatch, http=http)
        await restarted.tool(current, request)
        assert count == 1
        claims = await repo.list("evt.claims")
        observations = await repo.list("evt.event_observations")
        assert len(claims) == len(observations) == 1
        assert claims[0]["evidence"] == [{"item_id": item["item_id"], "quote": "Live"}]
        result_posts = [row[2]["events"][0] for row in native.requests if row[1].endswith("/events")]
        assert result_posts == [frozen, frozen]
        text = frozen["content"][0]["text"]
        assert "Live" in text and event["event_id"] not in text and "context_update" in text
        assert claims[0]["claim_id"] not in text and observations[0]["obs_id"] not in text
        resolved = {}
        await restarted.invoke_tool(
            current,
            "get_item",
            {"item_id": item["item_id"]},
            "late_parallel_call",
            resolved_arguments=resolved,
        )
        assert resolved == {"item_id": item["item_id"]}
        refusal = await restarted.invoke_tool(
            current, "get_decision", {"event_id": "foreign", "token": "world:WLD"}, "foreign_call"
        )
        assert refusal["reason"] == "EVENT_SCOPE"


async def test_busy_legacy_session_defers_new_callbacks_without_changing_the_active_input(legacy):
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
async def test_failed_migration_never_marks_old_contract_current_or_delivers_a_callback(legacy, status):
    repo, settings, session, _, event = legacy
    native = NativeSession()
    native.fail_update = status
    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as http:
        service = SessionService(repo, settings, None, http=http)
        with pytest.raises(httpx.HTTPStatusError):
            await service.send_message({"kind": "MILESTONE", "event_id": event["event_id"]})
    assert not any(row[1].endswith("/events") for row in native.requests)
    assert await repo.get("ops.agent_sessions", session["session_id"]) == session


async def test_old_remote_snapshot_is_not_recovered_as_a_new_context(legacy):
    repo, settings, _, _, _ = legacy
    item_id = "new_source"
    requests = []

    def handler(request):
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

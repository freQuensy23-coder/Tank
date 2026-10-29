"""A merge hands the merged item to the target event's live session exactly once (03 §5.1, Closed sessions)."""

import json
from datetime import timedelta

import httpx
import pytest
from test_agents_session_audit import settings as session_settings
from test_session_binding_concurrency import fixture_repo

from tank.agents import SessionService
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.util import iso, sha256, utcnow


async def source_item(repo, item_id, text, observed_at):
    await repo.put(
        "ingest.source_items",
        item_id,
        {
            "item_id": item_id,
            "source_id": "S01",
            "feed": "fixture",
            "external_id": item_id,
            "text": text,
            "source_class": "PRIMARY",
            "observed_at": observed_at,
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": observed_at,
            "published_prec": "EXACT",
            "raw_ref": "/fixture/raw",
            "raw_sha256": sha256(text),
            "content_hash": sha256(text),
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        },
    )


async def analyst_session(repo, session_id, item_id, at):
    await repo.put(
        "ops.agent_sessions",
        session_id,
        {
            "session_id": session_id,
            "event_id": None,
            "status": "OPEN",
            "runtime_status": "RUNNING",
            "opened_by_item": item_id,
            "item_id": item_id,
            "opened_at": at,
            "opened_at_prec": "COLLECTION_ONLY",
            "last_item_at": at,
            "last_item_at_prec": "COLLECTION_ONLY",
            "last_event_seq": 0,
            "agent_version": "1",
            "model_id": "gpt-6.1-sol",
            "active_message_id": "m-" + item_id,
            "message_log": [
                {
                    "message_id": "m-" + item_id,
                    "remote_event_id": "input-" + item_id,
                    "retry_count": 0,
                    "payload": {"kind": "NEW_ITEM", "item_id": item_id},
                }
            ],
        },
    )


async def record(tools, session_id, item_id, statement, at):
    return await tools.dispatch(
        "record_event",
        {
            "item_id": item_id,
            "is_event": True,
            "statement": statement,
            "certainty": "MEDIUM",
            "certainty_reason": "Fixture",
            "entities": [],
            "token_ids": [],
            "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
            "public_at": {"value": at, "precision": "EXACT", "basis": "Fixture", "quote": None},
            "freshness": "FRESH",
            "claims": [],
            "next_milestone": None,
            "checked_sources": [],
        },
        {"session_id": session_id, "role": "analyst"},
    )


class OpenMA:
    """The OpenMA sessions API as the handoff uses it: sessions found by identity, inputs accepted once."""

    def __init__(self):
        self.sessions = []
        self.inputs = []

    def __call__(self, request):
        body = json.loads(request.content or b"{}")
        path = request.url.path
        if request.method == "GET" and path == "/v1/sessions":
            return httpx.Response(200, json={"data": self.sessions, "next_page": None})
        if request.method == "POST" and path == "/v1/sessions":
            created = {"id": f"session_fresh_{len(self.sessions)}", "metadata": body["metadata"]}
            self.sessions.append(created)
            return httpx.Response(200, json=created)
        if request.method == "POST" and path.endswith("/events"):
            key = request.headers.get("idempotency-key")
            if key not in {row[1] for row in self.inputs}:
                self.inputs.append((path.split("/")[3], key, body["events"][0]))
            return httpx.Response(200, json={"data": [{"id": "native-" + sha256(key)[:8]}]})
        return httpx.Response(404, json={})


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_record_event_then_merge_into_reaches_a_fresh_session_of_a_failed_target_once(
    tmp_path, backend, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo:
        earlier, later = iso(utcnow() - timedelta(hours=2)), iso(utcnow())
        tools = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        await source_item(repo, "ITM-target", "ICE plans perpetual futures with trade xyz", earlier)
        await analyst_session(repo, "session_target", "ITM-target", earlier)
        target = await record(tools, "session_target", "ITM-target", "ICE plans US perps", earlier)
        failed = await repo.get("ops.agent_sessions", "session_target")
        failed["status"] = "FAILED"
        await repo.put("ops.agent_sessions", "session_target", failed)

        await source_item(repo, "ITM-merged", "ICE and trade[XYZ] pick Hyperliquid for perps", later)
        await analyst_session(repo, "session_source", "ITM-merged", later)
        source = await record(tools, "session_source", "ITM-merged", "ICE perps on Hyperliquid", later)
        await tools.dispatch(
            "merge_into",
            {"event_id": target["event_id"]},
            {"session_id": "session_source", "role": "analyst"},
        )
        [observation] = await repo.list("evt.event_observations", filters={"item_id": "ITM-merged"})
        [job] = await repo.list("ops.jobs", filters={"idem_key": "merge:" + observation["obs_id"]})
        assert job["payload"] == {
            "kind": "ITEM_ADDED",
            "event_id": target["event_id"],
            "item_id": "ITM-merged",
            "session_id": "session_target",
        }
        assert (await repo.get("evt.events", source["event_id"]))["merged_into"] == target["event_id"]

        openma = OpenMA()
        settings = session_settings()
        original_put, interrupted = repo.put, []

        async def put(collection, key, record, *args, **kwargs):
            if collection == "ops.agent_sessions" and key.startswith("session_fresh") and not interrupted:
                interrupted.append(key)
                raise ConnectionError("restart before the fresh session row was stored")
            return await original_put(collection, key, record, *args, **kwargs)

        repo.put = put
        async with httpx.AsyncClient(transport=httpx.MockTransport(openma)) as http:
            for attempt in range(3):
                service = SessionService(repo, settings, tools.dispatch, http=http)
                service.closed = True
                try:
                    delivered = await service.send_message(job["payload"])
                except ConnectionError:
                    assert attempt == 0
                await service.stop_followers()

        assert interrupted == ["session_fresh_0"]
        assert len(openma.sessions) == 1

        sessions = await repo.list("ops.agent_sessions", filters={"event_id": target["event_id"]})
        assert sorted((row["session_id"], row["status"]) for row in sessions) == [
            ("session_fresh_0", "OPEN"),
            ("session_target", "FAILED"),
        ]
        assert delivered["session_id"] == "session_fresh_0"
        assert delivered["opened_by_item"] == "ITM-merged"
        assert [
            (sid, json.loads(body["content"][0]["text"])["message"]["kind"]) for sid, _, body in openma.inputs
        ] == [("session_fresh_0", "ITEM_ADDED")]
        [intent] = delivered["message_log"]
        assert intent["payload"] == {
            "kind": "ITEM_ADDED",
            "event_id": target["event_id"],
            "item_id": "ITM-merged",
        }
        assert delivered["messages"] == [intent["message_id"]]
        assert (await repo.get("ops.agent_sessions", "session_target"))["message_log"] == failed[
            "message_log"
        ]

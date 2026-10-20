"""A completed native turn may intentionally produce no event observation."""

import copy
import json
import os
import uuid
from urllib.parse import urlsplit, urlunsplit

import httpx
import pytest
from test_agents_execution_barrier import config, idle
from test_session_binding_concurrency import fixture_repo, seed

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository, PostgresRepository
from tank.util import sha256


@pytest.mark.parametrize("backend", ["memory", "postgres"])
@pytest.mark.parametrize(
    "bound,outcome",
    [
        (bound, outcome)
        for bound in [False, True]
        for outcome in ["ignored", "DONE", "SKIPPED"]
        if not (bound and outcome == "SKIPPED")
    ],
)
async def test_settled_turn_accepts_agent_choice_without_source_observation(
    tmp_path, backend, outcome, bound
):
    admin = None
    database = None
    if backend == "postgres":
        import psycopg
        from psycopg import sql

        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Disposable PostgreSQL administrator DSN required")
        database = "tank_source_completion_" + uuid.uuid4().hex[:12]
        admin = await psycopg.AsyncConnection.connect(url, autocommit=True)
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        repo = await PostgresRepository(urlunsplit(urlsplit(url)._replace(path="/" + database))).open()
    else:
        repo = MemoryRepository()
    try:
        if admin:
            await repo.migrate()
        at = "2026-10-18T23:00:00Z"
        await repo.put(
            "ingest.source_items",
            "item",
            {
                "item_id": "item",
                "source_id": "S01",
                "feed": "fixture:official",
                "external_id": "item",
                "source_class": "PRIMARY",
                "text": "Product announced",
                "title": "Product announced",
                "observed_at": at,
                "observed_at_prec": "COLLECTION_ONLY",
                "published_at": at,
                "published_prec": "EXACT",
                "raw_ref": str(tmp_path / "raw.txt"),
                "raw_sha256": sha256("Product announced"),
                "content_hash": sha256("Product announced"),
                "relevance": {"decision": "ALWAYS"},
                "extraction_status": "PENDING",
            },
        )
        from tank.event_tools import EventToolService
        from tank.settings import Settings

        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        event_id = None
        if bound:
            original = await repo.get("ingest.source_items", "item")
            await repo.put(
                "ingest.source_items",
                "origin",
                dict(original, item_id="origin", external_id="origin", extraction_status="PENDING"),
            )
            domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
            result = await domain.dispatch(
                "record_event",
                {
                    "item_id": "origin",
                    "is_event": True,
                    "statement": "Product announced",
                    "certainty": "MEDIUM",
                    "certainty_reason": "Synthetic fixture",
                    "entities": [],
                    "tokens": [],
                    "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
                    "public_at": {
                        "value": at,
                        "precision": "EXACT",
                        "basis": "Fixture publication",
                        "quote": None,
                    },
                    "freshness": "FRESH",
                    "claims": [],
                    "next_milestone": None,
                    "checked_sources": [],
                },
                {},
            )
            event_id = result["event_id"]
        await repo.put(
            "ops.agent_sessions",
            "s",
            {
                "session_id": "s",
                "event_id": event_id,
                "status": "OPEN",
                "runtime_status": "RUNNING",
                "item_id": "item",
                "opened_by_item": "item",
                "agent_version": "1",
                "prompt_version": sha256("Synthetic fixture prompt"),
                "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
                "model_id": "fixture-model",
                "config_version": "test",
                "code_version": "test",
                "opened_at": at,
                "opened_at_prec": "COLLECTION_ONLY",
                "last_item_at": at,
                "last_item_at_prec": "COLLECTION_ONLY",
                "last_event_seq": 0,
                "active_message_id": "message",
                "messages": ["message"],
                "message_log": [
                    {
                        "message_id": "message",
                        "body": {"type": "user.message", "content": []},
                        "payload": {
                            "kind": "ITEM_ADDED" if bound else "NEW_ITEM",
                            "item_id": "item",
                            **({"event_id": event_id} if bound else {}),
                        },
                        "remote_event_id": "initial",
                        "retry_count": 0,
                        "turn_status": "RUNNING",
                    }
                ],
            },
        )
        posts, accepted = [], []

        def handler(request):
            if request.method == "POST":
                event = json.loads(request.content)["events"][0]
                identity = "retry" if event["type"] == "user.message" else "ack-" + str(len(accepted) + 1)
                if event["type"] == "user.message":
                    posts.append(request.content)
                accepted.append(identity)
                return httpx.Response(200, json={"data": [{"id": identity}]})
            if request.url.path.endswith("/executions"):
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            {"execution_id": i, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                            for i in request.url.params.get_list("ids[]")
                        ]
                    },
                )
            if request.url.path.endswith("/events"):
                return httpx.Response(200, json={"data": []})
            if request.method == "GET" and request.url.path.endswith("/sessions/s"):
                return httpx.Response(
                    200,
                    json={
                        "agent": {
                            "id": "fixture-agent",
                            "version": 1,
                            "system": "Synthetic fixture prompt",
                            "skills": [],
                            "tools": [],
                            "model": {"id": "fixture-model"},
                        }
                    },
                )
            raise AssertionError(request.url)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            service = SessionService(repo, config(), domain.dispatch, http=client)
            sequence = 0

            async def emit(tool, arguments):
                nonlocal sequence
                sequence += 1
                mapped = await repo.get("ops.agent_sessions", "s")
                native = mapped["message_log"][0]["remote_event_id"]
                call_id = "producer-" + str(sequence)
                await service.process_event(
                    "s",
                    {
                        "id": call_id,
                        "type": "agent.custom_tool_use",
                        "execution_id": native,
                        "name": tool,
                        "input": arguments,
                    },
                )
                call = await repo.get("ops.tool_calls", "s:" + call_id)
                assert call["status"] == "DONE" and not call["result"].get("error"), call["result"]
                return call

            async def finish_source():
                public = {"value": at, "precision": "EXACT", "basis": "Fixture publication", "quote": None}
                if bound:
                    return await emit(
                        "add_observation",
                        {
                            "event_id": event_id,
                            "item_id": "item",
                            "link": "CORROBORATION",
                            "certainty": "MEDIUM",
                            "certainty_reason": "Synthetic fixture",
                            "claims": [],
                            "public_at": public,
                            "freshness": "FRESH",
                        },
                    )
                origin = await emit(
                    "record_event",
                    {
                        "item_id": "item",
                        "is_event": outcome != "SKIPPED",
                        "statement": "Product announced",
                        "certainty": "MEDIUM",
                        "certainty_reason": "Synthetic fixture",
                        "entities": [],
                        "tokens": [],
                        "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
                        "public_at": public,
                        "freshness": "FRESH",
                        "claims": [],
                        "next_milestone": None,
                        "checked_sources": [],
                    },
                )
                if outcome == "SKIPPED":
                    return origin
                return origin

            if outcome in {"DONE", "SKIPPED"}:
                await finish_source()
            await service.process_event("s", idle("first-end", accepted[-1] if accepted else "initial"))
            legacy = await repo.get("ops.agent_sessions", "s")
            legacy["message_log"][0]["turn_status"] = "DONE"
            legacy["native_execution_states"] = {
                identity: {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                for identity in ["initial", *accepted]
            }
            legacy["verified_native_completion"] = await service.execution_barrier(legacy)
            await repo.put("ops.agent_sessions", "s", legacy)
            result = await service.completed_turn("s")
            assert result["complete"] and not posts
            final = await repo.get("ops.agent_sessions", "s")
            assert final["status"] == "OPEN"
            proof = final["message_log"][0]["source_completion_verified"]
            assert proof["version"] == 4 and proof["item_id"] == "item"
            assert proof["native_input_id"] == "initial" and proof["retry_attempt"] == 0
            assert proof["outcome"] == ("SKIPPED" if outcome == "ignored" else outcome)
            assert "workflow" not in proof
            if outcome == "ignored":
                observations = await repo.list("evt.event_observations", filters={"item_id": "item"})
                assert not observations
                if bound:
                    assert (await repo.get("evt.events", event_id))["lifecycle"] != "FAILED"
    finally:
        await repo.close()
        if admin:
            from psycopg import sql

            await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
            await admin.close()


@pytest.mark.parametrize("backend", ["memory", "postgres"])
async def test_ignored_item_keeps_event_and_call_for_next_native_update(tmp_path, backend):
    from tank.event_tools import EventToolService
    from tank.settings import Settings

    async with fixture_repo(backend) as repo:
        _session, extraction = await seed(repo)
        domain = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        origin = await domain.dispatch("record_event", extraction, {"session_id": "s"})
        eid = origin["event_id"]
        original = await repo.get("ingest.source_items", "i")
        for item_id, text in [("reprint", "Same product announcement"), ("launch", "Product now live")]:
            await repo.put(
                "ingest.source_items",
                item_id,
                dict(
                    original,
                    item_id=item_id,
                    external_id=item_id,
                    text=text,
                    title=text,
                    content_hash=sha256(text),
                    raw_sha256=sha256(text),
                    extraction_status="PENDING",
                ),
            )
        current = await repo.get("ops.agent_sessions", "s")
        current["message_log"][0].update(
            body={"type": "user.message", "content": []},
            payload={"kind": "ITEM_ADDED", "item_id": "reprint", "event_id": eid},
            turn_status="RUNNING",
        )
        await repo.put("ops.agent_sessions", "s", current)
        call = {
            "call_id": "active-call",
            "event_id": eid,
            "token_id": "world:WLD",
            "status": "OPEN",
            "opened_by_alert": "prior-alert",
            "opened_by_version": 1,
            "opened_at": extraction["public_at"]["value"],
            "opened_at_prec": "EXACT",
            "entry_ref_price": "10",
            "invalidation_price": "9",
        }
        await repo.put("ana.calls", "active-call", call)
        before = copy.deepcopy(await repo.get("evt.events", eid))
        accepted = []

        def handler(request):
            if request.method == "POST":
                assert request.url.path == "/v1/sessions/s/events"
                accepted.append(json.loads(request.content)["events"][0])
                return httpx.Response(200, json={"data": [{"id": "accepted-" + str(len(accepted))}]})
            if request.url.path.endswith("/executions"):
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            {"execution_id": i, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                            for i in request.url.params.get_list("ids[]")
                        ]
                    },
                )
            if request.url.path.endswith("/events"):
                return httpx.Response(200, json={"data": []})
            raise AssertionError(request.url)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            service = SessionService(repo, config(), domain.dispatch, http=http)
            # Drive the native receipt stream explicitly in this protocol test.
            service.start_stream = lambda sid: None
            await service.process_event("s", idle("ignored-end", "input"))
            assert await service.finish_if_idle("s")
            assert not accepted
            settled = await repo.get("ops.agent_sessions", "s")
            assert settled["status"] == "OPEN" and settled["event_id"] == eid
            assert settled["message_log"][0]["retry_count"] == 0
            assert settled["message_log"][0]["turn_status"] == "DONE"
            assert (await repo.get("ingest.source_items", "reprint"))["extraction_status"] == "SKIPPED"
            assert not await repo.list("evt.event_observations", filters={"item_id": "reprint"})
            assert await repo.get("evt.events", eid) == before
            assert await repo.get("ana.calls", "active-call") == call

            updated = await service.send_message({"kind": "ITEM_ADDED", "event_id": eid, "item_id": "launch"})
            assert updated["session_id"] == "s" and len(updated["message_log"]) == 2
            next_input = updated["message_log"][1]["remote_event_id"]
            await service.process_event(
                "s",
                {
                    "id": "real-update",
                    "type": "agent.custom_tool_use",
                    "execution_id": next_input,
                    "name": "add_observation",
                    "input": {
                        "event_id": eid,
                        "item_id": "launch",
                        "link": "STATE_UPDATE",
                        "claims": [
                            {
                                "type": "STATE",
                                "subject": "World",
                                "predicate": "launches",
                                "object": "Product",
                                "state_claim": "LIVE",
                                "verified": True,
                                "directionality": "BOTH",
                                "evidence_class": "PRIMARY_FACT",
                                "quote": "Product now live",
                            }
                        ],
                        "public_at": extraction["public_at"],
                        "freshness": "FRESH",
                    },
                },
            )
            tool = await repo.get("ops.tool_calls", "s:real-update")
            assert tool["status"] == "DONE", tool["result"]
            await service.process_event("s", idle("updated-end", tool["remote_event_id"]))
            assert await service.finish_if_idle("s")
            final = await repo.get("ops.agent_sessions", "s")
            assert final["status"] == "OPEN" and final["event_id"] == eid
            assert [row["turn_status"] for row in final["message_log"]] == ["DONE", "DONE"]
            assert [row["retry_count"] for row in final["message_log"]] == [0, 0]
            assert (await repo.get("ingest.source_items", "launch"))["extraction_status"] == "DONE"
            observations = await repo.list("evt.event_observations", filters={"item_id": "launch"})
            assert len(observations) == 1 and observations[0]["event_id"] == eid
            assert (await repo.get("evt.events", eid))["event_state"] == "LIVE"
            assert await repo.get("ana.calls", "active-call") == call
            assert len(accepted) == 2  # One new source input and its actual tool result.

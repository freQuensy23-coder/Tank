"""Native turn completion does not enforce class assessment or publication."""

import copy
import json

import httpx
import pytest
from test_agents_execution_barrier import config, idle
from test_event_tools import extraction, unknown_assessment
from test_session_binding_concurrency import fixture_repo, seed

from tank.agents.sessions import SessionService
from tank.event_tools import EventToolService
from tank.settings import Settings


@pytest.mark.parametrize("backend", ["memory", "postgres"])
@pytest.mark.parametrize(
    "case",
    [
        "missing_exhaust",
        "late_repair",
        "LOW",
        "MATERIAL",
        "SKIPPED",
        "merge",
        "corroboration",
        "wrong_event",
        "bad_class",
        "cache_revision",
    ],
)
async def test_new_origin_completion_does_not_require_class_or_card(tmp_path, backend, case):
    async with fixture_repo(backend) as repo:
        session, args = await seed(repo)
        session["message_log"][0]["body"] = {"type": "user.message", "content": []}
        await repo.put("ops.agent_sessions", "s", session)
        args["tokens"] = ["world:WLD"]
        args["claims"] = extraction()["claims"]
        tools = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        requests, posts, accepted = [], [], []

        def handler(request):
            requests.append((request.method, request.url.path))
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
            raise AssertionError(request.url)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            service = SessionService(repo, config(), tools.dispatch, http=client)
            system_context = service.tool_context(session, "fixture-setup") | {
                "session_id": None,
                "role": "system",
            }
            count = 0

            async def emit(tool, arguments):
                nonlocal count
                count += 1
                current = await repo.get("ops.agent_sessions", "s")
                native = current["message_log"][0]["remote_event_id"]
                identity = "producer-" + str(count)
                await service.process_event(
                    "s",
                    {
                        "id": identity,
                        "type": "agent.custom_tool_use",
                        "execution_id": native,
                        "name": tool,
                        "input": arguments,
                    },
                )
                call = await repo.get("ops.tool_calls", "s:" + identity)
                assert call["status"] == "DONE" and not call["result"].get("error"), call["result"]
                return call

            async def seed_other(name):
                item = await repo.get("ingest.source_items", "i")
                await repo.put(
                    "ingest.source_items",
                    name,
                    dict(item, item_id=name, external_id=name, extraction_status="PENDING"),
                )
                return await tools.dispatch("record_event", dict(args, item_id=name), system_context)

            if case == "corroboration":
                prior = await tools.dispatch("record_event", args, system_context)
                eid = prior["event_id"]
                item = await repo.get("ingest.source_items", "i")
                await repo.put(
                    "ingest.source_items",
                    "follow",
                    dict(item, item_id="follow", external_id="follow", extraction_status="PENDING"),
                )
                mapped = await repo.get("ops.agent_sessions", "s")
                mapped["event_id"] = eid
                mapped["message_log"][0]["payload"] = {
                    "kind": "ITEM_ADDED",
                    "item_id": "follow",
                    "event_id": eid,
                }
                await repo.put("ops.agent_sessions", "s", mapped)
                await emit(
                    "add_observation",
                    {
                        "event_id": eid,
                        "item_id": "follow",
                        "link": "CORROBORATION",
                        "claims": [],
                        "certainty": "MEDIUM",
                        "certainty_reason": "Synthetic fixture",
                        "public_at": args["public_at"],
                        "freshness": "FRESH",
                    },
                )
            else:
                if case == "SKIPPED":
                    args["is_event"] = False
                origin = await emit("record_event", args)
                eid = origin["result"].get("event_id")
                if case == "merge":
                    target = await seed_other("target")
                    await emit("merge_into", {"event_id": target["event_id"]})
                if case in {"LOW", "MATERIAL", "wrong_event", "bad_class", "cache_revision"}:
                    assessment = await emit(
                        "set_assessment",
                        {
                            "event_id": eid,
                            "assessment": unknown_assessment(klass="LOW" if case == "LOW" else "MATERIAL"),
                        },
                    )
                    if case in {"wrong_event", "bad_class"}:
                        corrupted = copy.deepcopy(assessment)
                        if case == "wrong_event":
                            other = await seed_other("other")
                            corrupted["result"]["event_id"] = other["event_id"]
                        else:
                            corrupted["result"]["class"] = "NOT_A_CLASS"
                        await repo.put("ops.tool_calls", "s:" + assessment["call_id"], corrupted)
            await service.process_event("s", idle("end-initial", accepted[-1]))
            legacy = await repo.get("ops.agent_sessions", "s")
            legacy["message_log"][0]["turn_status"] = "DONE"
            legacy["verified_native_completion"] = await service.execution_barrier(legacy)
            legacy["verified_source_completion"] = {"m": {"version": 1, "item_id": "i", "outcome": "DONE"}}
            await repo.put("ops.agent_sessions", "s", legacy)
            result = await service.completed_turn("s")
            assert result["complete"] and not posts
            current = await repo.get("ops.agent_sessions", "s")
            assert current["status"] == "OPEN"
            assert current["message_log"][0]["turn_status"] == "DONE"
            before = copy.deepcopy(current["verified_source_completion"])
            requests.clear()
            assert (await service.completed_turn("s"))["complete"] and not requests
            if case == "cache_revision":
                await tools.dispatch(
                    "set_assessment",
                    {"event_id": eid, "assessment": unknown_assessment(klass="LOW")},
                    system_context,
                )
                assert (await service.completed_turn("s"))["complete"] and not requests
                assert (await repo.get("ops.agent_sessions", "s"))["verified_source_completion"] == before
            assert await repo.list("out.alerts") == []

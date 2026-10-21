"""Source completion and native settlement do not enforce the skill playbook."""

from datetime import timedelta

import httpx
import pytest
from test_agents_execution_barrier import config, idle
from test_event_tools import unknown_assessment
from test_session_binding_concurrency import fixture_repo, seed

from tank.agents.sessions import SessionService
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.util import iso, utcnow


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.parametrize(
    "case",
    [
        "NO_EVENT",
        "LOW",
        "MATERIAL",
        "MAJOR_INCOMPLETE",
        "STALE_GATE",
        "FULL_EMPTY_SET",
        "HYDRATE",
        "SHOCK_RESUME",
    ],
)
async def test_actual_producer_consumer_workflow(tmp_path, backend, case, postgres_template):
    async with fixture_repo(backend, postgres_template) as repo:
        session, args = await seed(repo)
        session["message_log"][0]["body"] = {"type": "user.message", "content": []}
        if case == "HYDRATE":
            session.pop("prompt_version", None)
            session.pop("prompt_version_basis", None)
        await repo.put("ops.agent_sessions", "s", session)
        args.update(
            tokens=["world:WLD"],
            certainty="HIGH",
            claims=[
                {
                    "type": "STATE",
                    "subject": "World",
                    "predicate": "announces",
                    "object": "Product",
                    "state_claim": "APPROVED",
                    "verified": True,
                    "directionality": "BOTH",
                    "evidence_class": "PRIMARY_FACT",
                    "quote": "Product announced",
                }
            ],
        )
        if case == "NO_EVENT":
            args["is_event"] = False
        if case == "STALE_GATE":
            args["public_at"]["value"] = iso(utcnow() - timedelta(days=3))
            args["freshness"] = "STALE"
        posts = []

        def handler(request):
            if request.method == "POST":
                posts.append(request.content)
                return httpx.Response(200, json={"data": [{"id": "accepted-" + str(len(posts))}]})
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
            if request.url.path.endswith("/sessions/s"):
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "id": "s",
                            "agent": {
                                "id": "agent",
                                "version": 1,
                                "system": "Actual resolved test-session bytes",
                                "skills": [],
                                "tools": [],
                                "model": {"id": "gpt-6.1-sol"},
                            },
                        }
                    },
                )
            raise AssertionError(request.url)

        tools = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            service = SessionService(repo, config(), tools.dispatch, http=client)
            count = 0
            execution_id = "input"

            async def emit(tool, arguments):
                nonlocal count
                count += 1
                call_id = "tool-" + str(count)
                await service.process_event(
                    "s",
                    {
                        "id": call_id,
                        "type": "agent.custom_tool_use",
                        "execution_id": execution_id,
                        "name": tool,
                        "input": arguments,
                    },
                )
                return await repo.get("ops.tool_calls", "s:" + call_id)

            origin = await emit("record_event", args)
            assert origin["status"] == "DONE", origin["result"]
            if case != "NO_EVENT":
                eid = origin["result"]["event_id"]
                assessment = await emit(
                    "set_assessment",
                    {
                        "event_id": eid,
                        "assessment": unknown_assessment(
                            criterion="G4" if case == "SHOCK_RESUME" else "M2",
                            klass="STRUCTURAL_SHOCK"
                            if case == "SHOCK_RESUME"
                            else "LOW"
                            if case == "HYDRATE"
                            else case
                            if case in {"LOW", "MATERIAL"}
                            else "MAJOR",
                        ),
                    },
                )
                assert assessment["result"]["class"] == (
                    "STRUCTURAL_SHOCK"
                    if case == "SHOCK_RESUME"
                    else "LOW"
                    if case == "HYDRATE"
                    else case
                    if case in {"LOW", "MATERIAL"}
                    else "MAJOR"
                )
                if case == "SHOCK_RESUME":
                    partial = await emit(
                        "publish_card",
                        {
                            "event_id": eid,
                            "kind": "PARTIAL",
                            "verdict": "Product announced",
                            "why": ["Official primary fact"],
                        },
                    )
                    assert partial["result"]["published"]
                if case in {"FULL_EMPTY_SET", "SHOCK_RESUME"}:
                    from tank.event_tools import LAYERS

                    mapping = await emit(
                        "set_beneficiaries",
                        {
                            "event_id": eid,
                            "edges": [],
                            "layer_reasons": {
                                layer: "No credible beneficiary established from this fixture"
                                for layer in LAYERS
                            },
                        },
                    )
                    assert mapping["result"]["analysis_set"] == []
                    publication = await emit(
                        "publish_card",
                        {
                            "event_id": eid,
                            "kind": "FULL",
                            "verdict": "Product announced",
                            "why": ["Official primary fact", "No credible beneficiary established"],
                        },
                    )
                    assert publication["result"]["published"], publication["result"]
                    assert publication["result"]["publication_receipt"]["disposition"] == "PUBLISHED"
                if case == "STALE_GATE":
                    publication = await emit(
                        "publish_card",
                        {"event_id": eid, "kind": "FULL", "verdict": "No retained market claim", "why": []},
                    )
                    assert publication["result"]["publication_receipt"]["reason"] == "STALE"
                    assert publication["result"]["publication_receipt"]["disposition"] == "SUPPRESSED"
                    event = await repo.get("evt.events", eid)
                    assert event["lifecycle"] == "MONITORING"
                    assert event["monitoring_at_prec"] == "COLLECTION_ONLY"
                    assert "thresholds" not in event  # Publication context is not event state.
                    assert not await repo.list("out.alerts")
                    assert not await repo.list("ops.jobs", filters={"type": "DELIVER"})
            last = (await repo.get("ops.tool_calls", "s:tool-" + str(count)))["remote_event_id"]
            await service.process_event("s", idle("end", last))
            result = await service.completed_turn("s")
            if case == "HYDRATE":
                ledgers = await repo.list("ops.tool_calls")
                assert [row["native_turn_call_index"] for row in ledgers] == list(range(len(ledgers))), (
                    "Hydration reset processing ordinals"
                )
                assert (await repo.get("ops.agent_sessions", "s"))["event_id"] == eid
            assert result["complete"]
            requests_before = len(posts)
            assert (await service.completed_turn("s"))["complete"]
            assert len(posts) == requests_before
            assert len(await repo.list("out.alerts")) == (
                2 if case == "SHOCK_RESUME" else 1 if case == "FULL_EMPTY_SET" else 0
            )


@pytest.mark.parametrize(
    "tools", [[], ["publish_card"], ["get_decision", "publish_card", "set_beneficiaries", "set_assessment"]]
)
async def test_native_idle_accepts_agent_chosen_order_without_semantic_retry(tools):
    from test_agents_execution_barrier import seeded

    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    session = await seeded(repo)
    session["message_log"][0]["payload"] = {"kind": "NEW_ITEM", "item_id": "item"}
    # A historical failed playbook proof must not keep enforcing the old order.
    session["message_log"][0]["workflow_completion_verified"] = {
        "valid": False,
        "reason": "MISSING_CAUSAL_TURN_MARKET_TRUTH",
    }
    await repo.put("ops.agent_sessions", "s", session)
    posts = []
    reads = []

    def handler(request):
        if request.method == "POST":
            posts.append(request.content)
            return httpx.Response(200, json={"data": [{"id": "ack-" + str(len(posts))}]})
        reads.append(request.url.path)
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                        for identity in request.url.params.get_list("ids[]")
                    ]
                },
            )
        return httpx.Response(200, json={"data": []})

    async def dispatch(*args):
        return {"ok": True}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(repo, config(), dispatch, http=client)

        # Provenance is irrelevant to this fixture's harmless fake dispatcher.
        async def hydrate(session):
            pass

        service.hydrate_prompt_provenance = hydrate
        for index, tool in enumerate(tools):
            await service.process_event(
                "s",
                {
                    "id": "call-" + str(index),
                    "type": "agent.custom_tool_use",
                    "execution_id": "accepted-message",
                    "name": tool,
                    "input": {},
                },
            )
        await service.process_event(
            "s", idle("end", "ack-" + str(len(posts)) if posts else "accepted-message")
        )
        assert (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN" and current["message_log"][0]["turn_status"] == "DONE"
        assert current["message_log"][0]["retry_count"] == 0
        assert len(posts) == len(tools)  # Only original durable custom-tool results.
        assert not current.get("host_analysis_outcomes")
        assert "workflow_completion_verified" not in current["message_log"][0]
        assert current["message_log"][0]["legacy_workflow_completion_verified"] == {
            "valid": False,
            "reason": "MISSING_CAUSAL_TURN_MARKET_TRUTH",
        }
        reads.clear()
        for _ in range(3):
            assert (await service.completed_turn("s"))["complete"]
        assert not reads and len(posts) == len(tools)
        # Genuine process errors retain the once-only retry and terminal failure.
        await service.process_event(
            "s",
            {
                "id": "error-one",
                "type": "session.error",
                "execution_id": "accepted-message",
                "message": "Worker failed",
            },
        )
        retry = (await repo.get("ops.agent_sessions", "s"))["message_log"][0]
        assert retry["retry_count"] == 1 and retry["turn_status"] == "RUNNING"
        await service.process_event(
            "s",
            {
                "id": "error-two",
                "type": "session.error",
                "execution_id": retry["remote_event_id"],
                "message": "Worker failed again",
            },
        )
        final = await repo.get("ops.agent_sessions", "s")
        assert final["status"] == "FAILED" and final["message_log"][0]["turn_status"] == "FAILED"

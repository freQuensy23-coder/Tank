import copy
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from tank.agents.sessions import SessionService
from tank.replay import semantic_verdict_output
from tank.settings import Settings
from tank.storage import MemoryRepository


def config():
    """Protocol settings; the model boundary still reads the real registry and sources."""
    real = Settings(_env_file=None)
    return SimpleNamespace(
        openma_url="http://native",
        openma_api_key="fake-protocol-test",
        configured_secrets=lambda: frozenset({"fake-protocol-test"}),
        replay=False,
        openma_model="gpt-6.1-sol",
        config_version="test",
        code_version="test",
        load_config=lambda name: real.load_config(name) if name in {"registry", "sources"} else {},
    )


async def seeded(repo, initial="accepted-message"):
    session = {
        "session_id": "s",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "item_id": "item",
        "message_log": [
            {
                "message_id": "message",
                "body": {"type": "user.message", "content": []},
                "payload": {"kind": "SCOUT_REVIEW", "item_id": "item"},
                "remote_event_id": initial,
                "retry_count": 0,
                "turn_status": "RUNNING",
            }
        ],
        "active_message_id": "message",
        "messages": ["message"],
    }
    await repo.put("ops.agent_sessions", "s", session)
    await repo.put("ingest.source_items", "item", {"item_id": "item", "extraction_status": "DONE"})
    return session


def idle(identity, execution, reason="end_turn"):
    return {
        "id": identity,
        "type": "session.status_idle",
        "execution_id": execution,
        "stop_reason": {"type": reason},
        "processed_at": "2026-10-18T23:00:00Z",
    }


@pytest.mark.historical
@pytest.mark.integration
async def test_actual_r11_intermediate_idle_does_not_complete_tool_result_execution() -> None:

    timeline = json.loads(Path("docs/verification/f01-cli-r11-2026-10-19/native-timeline.json").read_text())
    initial = next(row["id"] for row in timeline if row["type"] == "user.message")
    returned = next(row["id"] for row in timeline if row["type"] == "user.custom_tool_result")
    first_idle = next(row for row in timeline if row["type"] == "session.status_idle")
    repo = MemoryRepository()
    await seeded(repo, initial)
    states = {initial: "COMPLETED", returned: "RUNNING"}
    dispatched = []
    history = []
    reads = []

    def handler(request: pytest.FixtureRequest):
        if request.url.path.endswith("/executions"):
            ids = request.url.params.get_list("ids[]")
            reads.append(ids)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": key,
                            "state": states[key],
                            "settled_outcome": states[key]
                            if states[key] in {"COMPLETED", "FAILED", "CANCELLED"}
                            else None,
                        }
                        for key in ids
                    ]
                },
            )
        if request.method == "POST":
            return httpx.Response(200, json={"data": [{"id": returned}]})
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": history})
        raise AssertionError(str(request.url))

    async def dispatch(tool, args, context):
        dispatched.append(tool)
        return {"item_id": "item"}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), dispatch, http=http)
        await service.process_event(
            "s",
            {
                "id": "native-tool",
                "type": "agent.custom_tool_use",
                "name": "get_item",
                "input": {},
            },
        )
        wire = idle(first_idle["id"], first_idle["source_position"]["execution_id"])
        await service.process_event("s", wire)
        assert not (await service.completed_turn("s"))["complete"]
        assert (await repo.get("ops.agent_sessions", "s"))["active_message_id"] == "message"
        states[returned] = "COMPLETED"
        end = idle("after-tool-result-end", returned)
        history.append(end)
        await service.process_event("s", end)
        assert (await service.completed_turn("s"))["complete"]
        assert dispatched == ["get_item"]
        assert (await repo.get("ops.tool_calls", "s:native-tool"))["remote_event_id"] == returned
        await service.close()


async def test_finalizer_failure_is_not_completed_and_retry_supersedes_only_failed_attempt() -> None:
    repo = MemoryRepository()
    await seeded(repo)
    states = {"accepted-message": "FAILED", "accepted-retry": "RUNNING"}
    posts = []

    def handler(request: pytest.FixtureRequest):
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": key,
                            "state": states[key],
                            "settled_outcome": states[key]
                            if states[key] in {"COMPLETED", "FAILED", "CANCELLED"}
                            else None,
                        }
                        for key in request.url.params.get_list("ids[]")
                    ]
                },
            )
        if request.method == "POST":
            posts.append(json.loads(request.content))
            return httpx.Response(200, json={"data": [{"id": "accepted-retry"}]})
        return httpx.Response(200, json={"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        await service.process_event("s", idle("ended-before-afterExecution", "accepted-message"))
        assert not (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][0]["retry_count"] == 1 and len(posts) == 1
        assert current["message_log"][0]["remote_event_id"] == "accepted-retry"
        assert "accepted-message" in current["superseded_native_inputs"]
        states["accepted-retry"] = "COMPLETED"
        await service.process_event("s", idle("retry-ended", "accepted-retry"))
        assert (await service.completed_turn("s"))["complete"]
        assert (await repo.get("ops.agent_sessions", "s"))["status"] == "OPEN"
        await service.close()


async def test_restart_recovers_correlated_duplicate_idle_without_business_reexecution() -> None:
    repo = MemoryRepository()
    session = await seeded(repo)
    session.update(runtime_status="IDLE", processed_events=["old-idle"])
    await repo.put("ops.agent_sessions", "s", session)

    def handler(request: pytest.FixtureRequest):
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": "accepted-message",
                            "state": "COMPLETED",
                            "settled_outcome": "COMPLETED",
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"data": [idle("old-idle", "accepted-message")]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        await service.catch_up("s")
        assert (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["processed_events"] == ["old-idle"]
        assert current["native_execution_receipts"]["old-idle"]["execution_id"] == "accepted-message"
        await service.close()


def test_no_card_outputs_still_compare_state_class_and_observation_references() -> None:
    first = {
        "decisions": [],
        "alerts": [],
        "events": [{"event_id": "event-a", "event_state": "PROPOSED"}],
        "observations": [{"obs_id": "obs-a", "event_id": "event-a", "item_id": "original-source"}],
        "assessments": [{"ems_id": "ems-a", "event_id": "event-a", "class": "LOW", "score": 2}],
    }
    other = json.loads(
        json.dumps(first).replace("event-a", "event-b").replace("obs-a", "obs-b").replace("ems-a", "ems-b")
    )
    assert semantic_verdict_output(first) == semantic_verdict_output(other)
    changed = copy.deepcopy(other)
    changed["assessments"][0]["class"] = "MATERIAL"
    assert semantic_verdict_output(first) != semantic_verdict_output(changed)
    changed = copy.deepcopy(other)
    changed["events"][0]["event_state"] = "LIVE"
    assert semantic_verdict_output(first) != semantic_verdict_output(changed)


async def test_settled_snapshot_drains_all_pages_and_rechecks_new_tool_ack() -> None:
    repo = MemoryRepository()
    await seeded(repo)
    states = {"accepted-message": "COMPLETED", "accepted-late-tool": "RUNNING"}
    pages = []
    dispatched = []
    start = idle("initial-ended", "accepted-message")
    late = {
        "id": "same-time-late-tool",
        "type": "agent.custom_tool_use",
        "name": "get_item",
        "input": {},
        "processed_at": start["processed_at"],
    }

    def handler(request: pytest.FixtureRequest):
        if request.url.path.endswith("/executions"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": key,
                            "state": states[key],
                            "settled_outcome": "COMPLETED" if states[key] == "COMPLETED" else None,
                        }
                        for key in request.url.params.get_list("ids[]")
                    ]
                },
            )
        if request.method == "POST":
            return httpx.Response(200, json={"data": [{"id": "accepted-late-tool"}]})
        page = request.url.params.get("page")
        pages.append(page)
        return httpx.Response(
            200, json={"data": [late] if page else [start], "next_page": None if page else "late-page"}
        )

    async def dispatch(*args):
        dispatched.append(True)
        return {"item_id": "item"}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), dispatch, http=http)
        await service.process_event("s", start)
        assert not (await service.completed_turn("s"))["complete"]
        assert pages == [None, "late-page"] and dispatched == [True]
        assert (await repo.get("ops.agent_sessions", "s"))["active_message_id"] == "message"
        await service.close()


async def test_message_lost_ack_reuses_durable_exact_body_after_source_changes() -> None:
    repo = MemoryRepository()
    session = await seeded(repo)
    session.update(messages=[], message_log=[], active_message_id=None)
    await repo.put("ops.agent_sessions", "s", session)
    await repo.put("ingest.source_items", "item", {"item_id": "item", "text": "Original source"})
    bodies = []
    keys = []

    def handler(request: pytest.FixtureRequest):
        bodies.append(request.content)
        keys.append(request.headers.get("Idempotency-Key"))
        if len(bodies) == 1:
            raise httpx.ReadError("lost acceptance reply", request=request)
        return httpx.Response(200, json={"data": [{"id": "accepted-original-message"}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), None, http=http)

        async def resolve(payload):
            return await repo.get("ops.agent_sessions", "s")

        service.resolve = resolve
        service.start_stream = lambda sid: None
        payload = {"kind": "NEW_ITEM", "item_id": "item", "session_id": "s"}
        with pytest.raises(httpx.ReadError):
            await service.send_message(payload)
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][0]["turn_status"] == "ACCEPTING"
        assert not (await service.execution_barrier(current))["complete"]
        await repo.put("ingest.source_items", "item", {"item_id": "item", "text": "Later enrichment"})
        restarted = SessionService(repo, config(), None, http=http)
        restarted.resolve = resolve
        restarted.start_stream = lambda sid: None
        await restarted.send_message(payload)
        assert bodies[0] == bodies[1] and keys[0] == keys[1]
        current = await repo.get("ops.agent_sessions", "s")
        assert len(current["message_log"]) == 1
        assert current["message_log"][0]["remote_event_id"] == "accepted-original-message"
        await service.close()
        await restarted.close()


async def test_tool_result_lost_ack_restart_reuses_result_and_records_original_accepted_id() -> None:
    repo = MemoryRepository()
    await seeded(repo)
    bodies = []
    dispatched = []

    def handler(request: pytest.FixtureRequest):
        bodies.append(request.content)
        if len(bodies) == 1:
            raise httpx.ReadError("lost reply", request=request)
        return httpx.Response(200, json={"data": [{"id": "accepted-original-tool-result"}]})

    async def dispatch(*args):
        dispatched.append(True)
        return {"item_id": "item", "text": "Result once"}

    event = {
        "id": "tool-once",
        "type": "agent.custom_tool_use",
        "name": "get_item",
        "input": {},
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), dispatch, http=http)
        with pytest.raises(httpx.ReadError):
            await service.process_event("s", event)
        restarted = SessionService(repo, config(), dispatch, http=http)
        await restarted.process_event("s", event)
        assert dispatched == [True] and bodies[0] == bodies[1]
        assert (await repo.get("ops.tool_calls", "s:tool-once"))[
            "remote_event_id"
        ] == "accepted-original-tool-result"
        await service.close()
        await restarted.close()


async def test_correlated_tool_before_ack_defers_instead_of_using_previous_message() -> None:
    repo = MemoryRepository()
    session = await seeded(repo, "old-input")
    session["message_log"].append(
        {
            "message_id": "new-message",
            "remote_event_id": None,
            "body": {"type": "user.message", "content": []},
            "payload": {"kind": "ITEM_ADDED", "item_id": "item"},
            "turn_status": "ACCEPTING",
            "retry_count": 0,
        }
    )
    await repo.put("ops.agent_sessions", "s", session)
    dispatched = []

    async def dispatch(*args):
        dispatched.append(args)
        return {"item_id": "item"}

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": [{"id": "ack-tool"}]})
        )
    ) as http:
        service = SessionService(repo, config(), dispatch, http=http)
        event = {
            "id": "early-tool",
            "type": "agent.custom_tool_use",
            "execution_id": "new-input",
            "name": "get_item",
            "input": {},
        }
        with pytest.raises(ValueError, match="acknowledged"):
            await service.process_event("s", event)
        assert not dispatched and "early-tool" not in (await repo.get("ops.agent_sessions", "s")).get(
            "processed_events", []
        )
        current = await repo.get("ops.agent_sessions", "s")
        current["message_log"][1].update(
            remote_event_id="new-input", turn_status="RUNNING", native_input_attempts={"new-input": 0}
        )
        await repo.put("ops.agent_sessions", "s", current)
        await service.process_event("s", event)
        assert len(dispatched) == 1
        call = await repo.get("ops.tool_calls", "s:early-tool")
        assert call["native_turn_message_id"] == "new-message" and call["native_turn_retry_count"] == 0
        await service.close()


async def test_late_previous_attempt_tool_retains_immutable_attempt_lineage() -> None:
    repo = MemoryRepository()
    session = await seeded(repo, "retry-input")
    session["message_log"][0].update(
        retry_count=1,
        superseded_native_input_ids=["old-input"],
        native_input_attempts={"old-input": 0, "retry-input": 1},
    )
    session["superseded_native_inputs"] = {"old-input": {"reason": "TURN_FAILED_AND_RETRIED"}}
    await repo.put("ops.agent_sessions", "s", session)

    def handler(request: pytest.FixtureRequest):
        return httpx.Response(200, json={"data": [{"id": "late-result"}]})

    async def dispatch(*args):
        return {"item_id": "item"}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), dispatch, http=http)
        await service.process_event(
            "s",
            {
                "id": "late-old-tool",
                "type": "agent.custom_tool_use",
                "execution_id": "old-input",
                "name": "get_item",
                "input": {},
            },
        )
        call = await repo.get("ops.tool_calls", "s:late-old-tool")
        assert call["native_turn_retry_count"] == 0 and call["native_turn_message_id"] == "message"
        await service.process_event(
            "s",
            {
                "id": "late-old-error",
                "type": "session.error",
                "execution_id": "late-result",
                "error": {"message": "old failed attempt"},
            },
        )
        assert (await repo.get("ops.agent_sessions", "s"))["status"] == "OPEN"
        assert (await repo.get("ops.agent_sessions", "s"))["message_log"][0]["retry_count"] == 1
        await service.close()


async def test_equal_time_terminal_then_running_order_uses_committed_queue_not_last_arrival() -> None:
    repo = MemoryRepository()
    await seeded(repo)
    ended = idle("a-idle", "accepted-message")
    running = {
        "id": "z-running",
        "type": "session.status_running",
        "execution_id": "accepted-message",
        "processed_at": ended["processed_at"],
    }
    reads = []

    def handler(request: pytest.FixtureRequest):
        if request.url.path.endswith("/executions"):
            reads.append(True)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": "accepted-message",
                            "state": "COMPLETED",
                            "settled_outcome": "COMPLETED",
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"data": [ended, running]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        await service.process_event("s", ended)
        await service.process_event("s", running)
        assert (await repo.get("ops.agent_sessions", "s"))["runtime_status"] == "RUNNING"
        assert (await service.completed_turn("s"))["complete"]
        assert reads == [True]
        assert (await repo.get("ops.agent_sessions", "s"))["runtime_status"] == "IDLE"
        await service.close()


async def test_settled_open_turn_reuses_proof_without_history_or_session_writes() -> None:
    repo = MemoryRepository()
    await seeded(repo)
    requests = []

    def handler(request: pytest.FixtureRequest):
        requests.append(request.url.path)
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
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": [], "has_more": False})
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(repo, config(), lambda *args: None, http=client)
        await service.process_event("s", idle("idle-final", "accepted-message"))
        assert await service.finish_if_idle("s")
        before = copy.deepcopy(await repo.get("ops.agent_sessions", "s"))
        requests.clear()
        writes = []
        original_put = repo.put

        async def watched_put(collection, key, value):
            writes.append((collection, key))
            return await original_put(collection, key, value)

        repo.put = watched_put
        for _ in range(5):
            assert await service.finish_if_idle("s")
        assert requests == []
        assert writes == []
        assert await repo.get("ops.agent_sessions", "s") == before
        changed = copy.deepcopy(before)
        changed["message_log"].append(
            {"message_id": "next", "remote_event_id": "new-input", "retry_count": 0, "turn_status": "RUNNING"}
        )
        changed["runtime_status"] = "RUNNING"
        await original_put("ops.agent_sessions", "s", changed)
        assert not (await service.execution_barrier(changed))["complete"]
        assert changed["status"] == "OPEN"


async def test_retry_lost_ack_recovers_durable_failure_intent_without_second_retry() -> None:
    repo = MemoryRepository()
    await seeded(repo)
    posted = []

    def handler(request: pytest.FixtureRequest):
        if request.method == "POST":
            posted.append((request.headers["idempotency-key"], request.content))
            if len(posted) == 1:
                raise httpx.ReadError("lost acknowledgement", request=request)
            return httpx.Response(200, json={"data": [{"id": "retry-accepted"}]})
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(repo, config(), lambda *args: None, http=client)
        error = {"id": "failed-execution", "type": "session.error", "execution_id": "accepted-message"}
        with pytest.raises(httpx.ReadError):
            await service.process_event("s", error)
        durable = await repo.get("ops.agent_sessions", "s")
        assert durable["pending_retry_event"]["id"] == error["id"]
        assert error["id"] not in durable.get("processed_events", [])
        restarted = SessionService(repo, config(), lambda *args: None, http=client)
        await restarted.recover_outbound_inputs("s")
        recovered = await repo.get("ops.agent_sessions", "s")
        assert posted[0] == posted[1]
        assert recovered["pending_retry_event"] is None
        assert recovered["message_log"][0]["retry_count"] == 1
        assert recovered["message_log"][0]["remote_event_id"] == "retry-accepted"
        await restarted.recover_outbound_inputs("s")
        assert len(posted) == 2


@pytest.mark.parametrize("source_state", ["PENDING", "FAILED", "MISSING"])
async def test_native_completion_preserves_source_failures_without_failing_session(source_state) -> None:
    repo = MemoryRepository()
    session = await seeded(repo)
    session["message_log"][0]["turn_status"] = "DONE"
    session["message_log"][0]["payload"]["kind"] = "NEW_ITEM"
    session["native_execution_states"] = {
        "accepted-message": {
            "execution_id": "accepted-message",
            "state": "COMPLETED",
            "settled_outcome": "COMPLETED",
        }
    }
    await repo.put("ops.agent_sessions", "s", session)
    posts = []

    def handler(request: pytest.FixtureRequest):
        if request.method == "POST":
            posts.append(request.content)
            return httpx.Response(200, json={"data": [{"id": "replacement"}]})
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": []})
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
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(repo, config(), lambda *args: None, http=client)
        await service.process_event("s", idle("legacy-end", "accepted-message"))
        legacy = await repo.get("ops.agent_sessions", "s")
        legacy["message_log"][0]["turn_status"] = "DONE"
        legacy["verified_native_completion"] = await service.execution_barrier(legacy)
        assert legacy["verified_native_completion"]["complete"]
        await repo.put("ops.agent_sessions", "s", legacy)
        if source_state == "MISSING":
            await repo.delete("ingest.source_items", "item")
        else:
            await repo.put(
                "ingest.source_items", "item", {"item_id": "item", "extraction_status": source_state}
            )
        assert (await service.completed_turn("s"))["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN" and not posts
        assert current["message_log"][0]["retry_count"] == 0
        outcome = current["verified_source_completion"]["message"]["outcome"]
        assert (
            outcome
            == {"PENDING": "SKIPPED", "FAILED": "FAILED", "MISSING": "MISSING_SOURCE_ITEM"}[source_state]
        )


@pytest.mark.parametrize("pending", [False, True])
async def test_legacy_semantic_repair_receipt_is_audited_without_retry_or_failure(pending) -> None:
    repo = MemoryRepository()
    session = await seeded(repo)
    original = {"type": "user.message", "content": [{"type": "text", "text": "original NEW_ITEM"}]}
    session["message_log"][0].update(
        body=copy.deepcopy(original), payload={"kind": "NEW_ITEM", "item_id": "item"}
    )
    session["event_id"] = "bound-event"
    receipt = {
        "id": "incomplete",
        "type": "host.analysis_incomplete",
        "execution_id": "accepted-message",
        "completion_snapshot": {"item_id": "item", "outcome": "PENDING"},
        "_host_retry_body": {"type": "user.message", "content": []},
    }
    if pending:
        session["pending_retry_event"] = copy.deepcopy(receipt)
    await repo.put("ops.agent_sessions", "s", session)
    event = {"event_id": "bound-event", "lifecycle": "ANALYZING"}
    call = {"call_id": "open-call", "event_id": "bound-event", "status": "OPEN"}
    await repo.put("evt.events", "bound-event", event)
    await repo.put("ana.calls", "open-call", call)

    def handler(request: pytest.FixtureRequest) -> None:
        raise AssertionError("Legacy semantic repair must not call OpenMA")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(repo, config(), None, http=client)
        if pending:
            await service.recover_outbound_inputs("s")
        else:
            await service.process_event("s", receipt)
        await service.process_event("s", receipt)
        after = await repo.get("ops.agent_sessions", "s")
        assert after["status"] == "OPEN" and not after.get("pending_retry_event")
        assert after["message_log"] == session["message_log"]
        assert after["host_analysis_outcomes"] == {
            receipt["id"]: {**receipt, "disposition": "IGNORED_LEGACY_SEMANTIC_GATE"}
        }
        assert after["processed_events"] == [receipt["id"]]
        assert await repo.get("evt.events", "bound-event") == event
        assert await repo.get("ana.calls", "open-call") == call

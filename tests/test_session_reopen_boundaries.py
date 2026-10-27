import copy

import httpx
import pytest
from test_agents_execution_barrier import idle
from test_session_completion_recovery import (
    AT,
    message,
    native_reader,
    seed_reopened_failure,
    settings,
)
from test_telegram_followups import openma

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import sha256


async def failed_session_with_question(repo):
    session, _ = await seed_reopened_failure(repo)
    session.update(
        status="FAILED",
        runtime_status="IDLE",
        active_message_id=None,
        messages=["root"],
        message_log=session["message_log"][:1],
        processed_events=[session["terminal_failure"]["event_id"]],
        native_execution_receipts={},
    )
    await repo.put("ops.agent_sessions", "s", session)
    await repo.put(
        "out.user_questions",
        "q_1",
        {
            "question_id": "q_1",
            "session_id": "s",
            "status": "QUEUED",
            "text": "What happened after the failure?",
            "chat_id": "42",
            "reply_to_message_id": 9,
            "reference": {"kind": "CARD", "text": "Original alert"},
        },
    )
    return session, {"kind": "USER_QUESTION", "session_id": "s", "question_id": "q_1"}


def question_transport(states, history, posts, requests):
    reader = native_reader(states, history, requests)
    followup = openma(posts, history)

    def handler(request):
        if request.url.path.endswith("/executions"):
            return reader(request)
        return followup(request)

    return handler


def owned_tool(call_id, *, attempt, identity=None):
    call = {
        "session_id": "s",
        "call_id": call_id,
        "tool": "add_observation",
        "input": {"item_id": "i"},
        "status": "DONE",
        "native_turn_message_id": "root",
        "native_turn_retry_count": attempt,
        "result": {"obs_id": "already-recorded-observation"},
    }
    if identity is not None:
        call.update(remote_event_id=identity, returned_at=AT)
    return call


@pytest.mark.parametrize(
    "obligation",
    [
        "unprocessed-failure",
        "pending-retry",
        "accepted-running",
        "running-tool",
        "unknown-tool",
        "unreturned-effectful-tool",
    ],
)
async def test_reader_question_reopening_preserves_unresolved_obligations(obligation):
    repo = MemoryRepository()
    session, payload = await failed_session_with_question(repo)
    original_failure = copy.deepcopy(session["message_log"][0]["terminal_failure"])
    states = {"input-failed-retry": "FAILED", "in_q": "COMPLETED"}
    pending_retry = {
        "id": "retry-not-yet-accepted",
        "type": "session.error",
        "execution_id": "input-failed-retry",
        "processed_at": AT,
    }
    call = None
    if obligation == "unprocessed-failure":
        session["processed_events"] = []
    elif obligation == "pending-retry":
        session["pending_retry_event"] = pending_retry
    elif obligation == "accepted-running":
        session["message_log"].append(message("live", "input-live"))
        session["messages"].append("live")
        session.update(active_message_id="live", runtime_status="RUNNING")
        states["input-live"] = "RUNNING"
    else:
        identity = None if obligation == "unreturned-effectful-tool" else "input-tool"
        call = owned_tool("unfinished-tool", attempt=1, identity=identity)
        await repo.put("ops.tool_calls", "s:unfinished-tool", call)
        if identity:
            states[identity] = "RUNNING" if obligation == "running-tool" else "UNKNOWN"
    await repo.put("ops.agent_sessions", "s", session)
    end = idle("question-end", "in_q")
    posts, requests = [], []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(question_transport(states, [end], posts, requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        service.start_stream = lambda sid: None
        service.clock_timestamp = lambda: collection_clock(AT)
        await service.send_message(payload)
        reopened = await repo.get("ops.agent_sessions", "s")
        if obligation == "pending-retry":
            assert reopened["pending_retry_event"] == pending_retry
        elif obligation == "accepted-running":
            assert reopened["active_message_id"] == "live"
            assert reopened["runtime_status"] == "RUNNING"
            assert reopened["message_log"][1]["turn_status"] == "RUNNING"
        await service.process_event("s", end)
        current = await repo.get("ops.agent_sessions", "s")
        barrier = await service.execution_barrier(current, verify_remote=True)
        assert not barrier["complete"]
        if obligation == "accepted-running":
            assert "input-live" in barrier["pending_input_ids"]
            assert "live" not in current.get("retired_message_ids", [])
        elif obligation in {"running-tool", "unknown-tool"}:
            assert "input-tool" in barrier["pending_input_ids"]
        elif obligation == "unreturned-effectful-tool":
            assert "unreturned-tool:unfinished-tool" in barrier["unidentified_inputs"]
        else:
            assert "input-failed-retry" in barrier["accepted_input_ids"]
        assert "root" not in current.get("retired_message_ids", [])
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][0]["terminal_failure"] == original_failure
        if obligation == "unprocessed-failure":
            assert original_failure["event_id"] not in current["processed_events"]
        if call is not None:
            assert await repo.get("ops.tool_calls", "s:unfinished-tool") == call
        assert posts[0][0] == "/v1/sessions/s/events"
        assert posts[0][1]["events"][0]["type"] == "user.message"
        assert "What happened after the failure?" in posts[0][1]["events"][0]["content"][0]["text"]
        assert len(posts) == 1
        await service.close()


async def test_reader_question_completes_after_exact_handled_failure_without_erasing_failed_audit():
    repo = MemoryRepository()
    session, payload = await failed_session_with_question(repo)
    original_failure = copy.deepcopy(session["message_log"][0]["terminal_failure"])
    end = idle("question-end", "in_q")
    history = [
        {"id": "question-running", "type": "session.status_running", "execution_id": "in_q"},
        {
            "id": "question-answer",
            "type": "agent.message",
            "content": [{"type": "text", "text": "The original attempt remains failed."}],
        },
        end,
    ]
    states = {"input-failed-retry": "FAILED", "in_q": "COMPLETED"}
    posts, requests = [], []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(question_transport(states, history, posts, requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        service.start_stream = lambda sid: None
        service.clock_timestamp = lambda: collection_clock(AT)
        await service.send_message(payload)
        await service.process_event("s", end)
        result = await service.completed_turn("s")
        assert result["complete"]
        assert result["accepted_input_ids"] == ["in_q"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == "OPEN"
        assert current["active_message_id"] is None
        assert current["runtime_status"] == "IDLE"
        assert "root" not in current.get("retired_message_ids", [])
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][0]["terminal_failure"] == original_failure
        assert current["terminal_failure"] == original_failure
        assert current["processed_events"].count(original_failure["event_id"]) == 1
        question_turn = next(row for row in current["message_log"] if row["message_id"] == sha256(payload))
        assert question_turn["turn_status"] == "DONE"
        assert question_turn["remote_event_id"] == "in_q"
        assert (await repo.get("ingest.source_items", "i"))["extraction_status"] == "FAILED"
        question = await repo.get("out.user_questions", "q_1")
        assert question["answer_status"] == "ANSWERED"
        assert question["answer_text"] == "The original attempt remains failed."
        assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1
        assert (await service.completed_turn("s"))["complete"]
        assert len(posts) == 1
        await service.close()


async def add_superseded_tool(repo, session, *, persisted):
    call = owned_tool("prior-tool", attempt=0, identity="input-prior-tool")
    await repo.put("ops.tool_calls", "s:prior-tool", call)
    state = {"execution_id": "input-prior-tool", "state": "FAILED", "settled_outcome": "FAILED"}
    if persisted:
        session["native_execution_states"] = {"input-prior-tool": state}
        session["superseded_native_inputs"]["input-prior-tool"] = {
            "reason": "FAILED_PRIOR_TURN_ATTEMPT",
            "queue_state": state,
            "replacement_input_id": "input-failed-retry",
            "message_id": "root",
        }
    await repo.put("ops.agent_sessions", "s", session)
    return call


@pytest.mark.parametrize("persisted", [False, True])
async def test_superseded_failed_tool_does_not_block_later_milestone_after_exhausted_failure(persisted):
    repo = MemoryRepository()
    session, end = await seed_reopened_failure(repo)
    original_failure = copy.deepcopy(session["message_log"][0]["terminal_failure"])
    call = await add_superseded_tool(repo, session, persisted=persisted)
    states = {
        "input-prior-tool": "FAILED",
        "input-failed-retry": "FAILED",
        "input-milestone": "COMPLETED",
    }
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(native_reader(states, [end], requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        service.clock_timestamp = lambda: collection_clock(AT)
        result = await service.completed_turn("s")
        assert result["complete"]
        assert result["accepted_input_ids"] == ["input-milestone"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["superseded_native_inputs"]["input-prior-tool"]["message_id"] == "root"
        assert current["status"] == "OPEN"
        assert current["active_message_id"] is None
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][0]["terminal_failure"] == original_failure
        assert current["terminal_failure"] == original_failure
        assert current["message_log"][1]["turn_status"] == "DONE"
        assert "root" not in current.get("retired_message_ids", [])
        assert (await repo.get("ingest.source_items", "i"))["extraction_status"] == "FAILED"
        assert await repo.get("ops.tool_calls", "s:prior-tool") == call
        assert (await service.completed_turn("s"))["complete"]
        await service.close()


@pytest.mark.parametrize("continuation", ["RUNNING", "UNKNOWN", "UNRETURNED"])
async def test_superseded_failure_exclusion_preserves_current_attempt_tool_barrier(continuation):
    repo = MemoryRepository()
    session, end = await seed_reopened_failure(repo)
    prior_call = await add_superseded_tool(repo, session, persisted=True)
    identity = None if continuation == "UNRETURNED" else "input-current-tool"
    current_call = owned_tool("current-tool", attempt=1, identity=identity)
    await repo.put("ops.tool_calls", "s:current-tool", current_call)
    states = {
        "input-prior-tool": "FAILED",
        "input-failed-retry": "FAILED",
        "input-milestone": "COMPLETED",
        "input-current-tool": continuation,
    }
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(native_reader(states, [end], requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        service.clock_timestamp = lambda: collection_clock(AT)
        barrier = await service.execution_barrier(session, verify_remote=True)
        assert not barrier["complete"]
        if continuation == "UNRETURNED":
            assert "unreturned-tool:current-tool" in barrier["unidentified_inputs"]
        else:
            assert "input-current-tool" in barrier["pending_input_ids"]
            assert barrier["execution_states"]["input-current-tool"]["state"] == continuation
        blocked = await repo.get("ops.agent_sessions", "s")
        assert blocked["active_message_id"] == "milestone"
        assert blocked["message_log"][1]["turn_status"] == "RUNNING"
        assert "root" not in blocked.get("retired_message_ids", [])
        assert await repo.get("ops.tool_calls", "s:current-tool") == current_call
        states["input-current-tool"] = "COMPLETED"
        current_call.update(remote_event_id="input-current-tool", returned_at=AT)
        await repo.put("ops.tool_calls", "s:current-tool", current_call)
        await service.process_event("s", idle("current-tool-end", "input-current-tool"))
        result = await service.completed_turn("s")
        assert result["complete"]
        assert result["accepted_input_ids"] == ["input-current-tool", "input-milestone"]
        settled = await repo.get("ops.agent_sessions", "s")
        assert settled["active_message_id"] is None
        assert settled["message_log"][0]["turn_status"] == "FAILED"
        assert settled["message_log"][0]["terminal_failure"] == session["message_log"][0]["terminal_failure"]
        assert settled["message_log"][1]["turn_status"] == "DONE"
        assert await repo.get("ops.tool_calls", "s:prior-tool") == prior_call
        assert (await repo.get("ops.tool_calls", "s:current-tool"))["result"] == {
            "obs_id": "already-recorded-observation"
        }
        await service.close()

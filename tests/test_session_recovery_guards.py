import copy

import httpx
import pytest
from test_session_completion_recovery import AT, message, native_reader, seed, seed_reopened_failure, settings

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@pytest.mark.parametrize(
    "case",
    [
        "boolean-attempt",
        "string-attempt",
        "negative-attempt",
        "blank-id",
        "malformed-map",
        "conflicting-id",
        "retired",
        "superseded",
        "failed-message",
        "done-message",
        "failed-session",
        "ended-session",
        "archived-session",
    ],
)
async def test_saved_acceptance_proof_never_revives_invalid_or_terminal_work(case):
    repo = MemoryRepository()
    session = await seed(repo)
    lost = message("lost", None, status="ACCEPTING")
    lost["native_input_attempts"] = {"input-lost": 0}
    if case in {"boolean-attempt", "string-attempt", "negative-attempt"}:
        lost["native_input_attempts"] = {
            "input-lost": {"boolean-attempt": False, "string-attempt": "0", "negative-attempt": -1}[case]
        }
    elif case == "blank-id":
        lost["native_input_attempts"] = {" ": 0}
    elif case == "malformed-map":
        lost["native_input_attempts"] = ["input-lost"]
    elif case == "conflicting-id":
        lost["remote_event_id"] = "input-conflicting"
    elif case == "retired":
        session["retired_message_ids"] = ["lost"]
    elif case == "superseded":
        session["superseded_native_inputs"] = {"input-lost": {"reason": "TURN_FAILED_AND_RETRIED"}}
    elif case in {"failed-message", "done-message"}:
        lost["turn_status"] = "FAILED" if case == "failed-message" else "DONE"
    elif case in {"failed-session", "ended-session"}:
        session["status"] = "FAILED" if case == "failed-session" else "ENDED"
    elif case == "archived-session":
        session["remote_state"] = {"status": "terminated", "archived_at": AT}
    session["message_log"].append(lost)
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(native_reader({}, [], requests))) as http:
        service = SessionService(repo, settings(), None, http=http)
        await service.recover_outbound_inputs("s")
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][1] == lost
        assert current["status"] == session["status"]
        assert current["active_message_id"] == "root"
        assert not (await service.execution_barrier(current))["complete"]
        assert requests == []
        await service.close()


@pytest.mark.parametrize("status", ["ACCEPTING", "PENDING", "RUNNING"])
@pytest.mark.parametrize("has_timestamp", [False, True])
async def test_repaired_acceptance_preserves_execution_progress_and_timestamp(status, has_timestamp):
    repo = MemoryRepository()
    session = await seed(repo)
    lost = message("lost", None, status=status)
    lost["native_input_attempts"] = {"input-lost": 0}
    if not has_timestamp:
        lost.pop("sent_at")
    session["message_log"].append(lost)
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(native_reader({}, [], requests))) as http:
        service = SessionService(repo, settings(), None, http=http)
        await service.recover_outbound_inputs("s")
        current = await repo.get("ops.agent_sessions", "s")
        recovered = current["message_log"][1]
        expected = dict(lost, remote_event_id="input-lost")
        if status == "ACCEPTING":
            expected["turn_status"] = "PENDING"
        assert recovered == expected
        assert current["messages"] == ["root", "lost"]
        assert current["active_message_id"] == "root"
        assert requests == []
        await service.close()


@pytest.mark.parametrize(
    "case",
    [
        "unprocessed",
        "wrong-execution",
        "wrong-event",
        "foreign-owner",
        "pending-retry",
        "running",
        "unknown",
        "no-later-acceptance",
        "later-failed",
        "legacy-session-proof",
        "unreturned",
        "running-tool",
        "unknown-tool",
        "missing-action",
    ],
)
async def test_historical_failure_requires_exact_handled_terminal_boundary(case):
    repo = MemoryRepository()
    session, end = await seed_reopened_failure(repo)
    failed = session["message_log"][0]
    failed_id = failed["remote_event_id"]
    states = {failed_id: "FAILED", "input-milestone": "COMPLETED"}
    if case == "unprocessed":
        session["processed_events"] = [end["id"]]
    elif case == "wrong-execution":
        failed["terminal_failure"]["execution_id"] = "input-not-owned"
        session["terminal_failure"] = copy.deepcopy(failed["terminal_failure"])
    elif case == "wrong-event":
        failed["terminal_failure"]["event_id"] = end["id"]
    elif case == "foreign-owner":
        session["message_log"][1]["native_input_attempts"][failed_id] = 0
    elif case == "pending-retry":
        session["pending_retry_event"] = {"id": "retry-pending", "type": "session.error"}
    elif case in {"running", "unknown"}:
        states[failed_id] = case.upper()
    elif case == "no-later-acceptance":
        session["message_log"][1]["remote_event_id"] = None
        session["message_log"][1]["native_input_attempts"] = {}
    elif case == "later-failed":
        session["message_log"][1]["turn_status"] = "FAILED"
    elif case == "legacy-session-proof":
        failed.pop("terminal_failure")
        session["terminal_failure"]["execution_id"] = "input-not-owned"
    elif case == "missing-action":
        session["native_execution_receipts"]["unfinished-action"] = {
            "id": "unfinished-action",
            "type": "session.status_idle",
            "execution_id": failed_id,
            "stop_reason": {"type": "requires_action", "event_ids": ["missing-tool"]},
        }
    else:
        call = {
            "session_id": "s",
            "call_id": "owned-tool",
            "tool": "get_item",
            "input": {},
            "status": "PENDING",
            "native_turn_message_id": "root",
            "native_turn_retry_count": 1,
        }
        if case != "unreturned":
            call.update(status="DONE", remote_event_id="input-tool", returned_at=AT)
            states["input-tool"] = "RUNNING" if case == "running-tool" else "UNKNOWN"
        await repo.put("ops.tool_calls", "s:owned-tool", call)
    await repo.put("ops.agent_sessions", "s", session)
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(native_reader(states, [end], requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        result = await service.execution_barrier(session, verify_remote=True)
        assert not result["complete"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["active_message_id"] == "milestone"
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["message_log"][1]["turn_status"] != "DONE"
        assert "root" not in current.get("retired_message_ids", [])
        assert not any(request.method == "POST" for request in requests)
        await service.close()


async def test_scoped_legacy_terminal_failure_can_settle_with_completed_owned_tool():
    repo = MemoryRepository()
    session, end = await seed_reopened_failure(repo)
    session["message_log"][0].pop("terminal_failure")
    await repo.put("ops.agent_sessions", "s", session)
    await repo.put(
        "ops.tool_calls",
        "s:owned-tool",
        {
            "session_id": "s",
            "call_id": "owned-tool",
            "tool": "get_item",
            "input": {},
            "status": "DONE",
            "native_turn_message_id": "root",
            "native_turn_retry_count": 1,
            "returned_at": AT,
            "remote_event_id": "input-tool",
        },
    )
    states = {"input-failed-retry": "FAILED", "input-milestone": "COMPLETED", "input-tool": "COMPLETED"}
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(native_reader(states, [end], requests))
    ) as http:
        service = SessionService(repo, settings(), None, http=http)
        result = await service.completed_turn("s")
        assert result["complete"]
        assert "input-tool" in result["accepted_input_ids"]
        assert "input-failed-retry" not in result["accepted_input_ids"]
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][0]["turn_status"] == "FAILED"
        assert current["terminal_failure"] == session["terminal_failure"]
        assert current["active_message_id"] is None
        await service.close()


@pytest.mark.parametrize("terminal", ["FAILED", "ENDED"])
async def test_acceptance_reply_cannot_reopen_concurrent_terminal_session(terminal):
    repo = MemoryRepository()
    session = await seed(repo)
    pending = message("pending", None, status="ACCEPTING")
    pending.pop("sent_at")
    session["message_log"].append(pending)
    await repo.put("ops.agent_sessions", "s", session)

    async def handler(request):
        assert request.method == "POST"
        async with repo.lock("session:s"):
            current = await repo.get("ops.agent_sessions", "s")
            current["status"] = terminal
            current["message_log"][1]["turn_status"] = "FAILED"
            await repo.put("ops.agent_sessions", "s", current)
        return httpx.Response(200, json={"data": [{"id": "input-pending"}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http)
        await service.recover_outbound_inputs("s")
        current = await repo.get("ops.agent_sessions", "s")
        assert current["status"] == terminal
        assert current["message_log"][1]["turn_status"] == "FAILED"
        assert current["message_log"][1]["remote_event_id"] == "input-pending"
        await service.close()

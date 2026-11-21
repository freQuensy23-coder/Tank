from types import SimpleNamespace

import httpx
import pytest

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@pytest.mark.parametrize("cancelled_input", ["source-input", "callback-input"])
@pytest.mark.parametrize("independent_input", [False, True])
async def test_settled_cancellation_fails_only_its_owner(cancelled_input, independent_input):
    repo = MemoryRepository()
    source = {
        "message_id": "source",
        "remote_event_id": "source-input",
        "body": {"type": "user.message", "content": []},
        "payload": {"kind": "NEW_ITEM", "item_id": "item"},
        "turn_status": "RUNNING",
    }
    queued = {
        "message_id": "independent",
        "remote_event_id": "independent-input",
        "payload": {"kind": "SCOUT_REVIEW"},
        "turn_status": "PENDING",
        "retry_count": 1,
    }
    await repo.put(
        "ops.agent_sessions",
        "s",
        {
            "session_id": "s",
            "status": "OPEN",
            "runtime_status": "RUNNING",
            "active_message_id": "source",
            "message_log": [source, queued] if independent_input else [source],
        },
    )
    await repo.put("ingest.source_items", "item", {"item_id": "item", "extraction_status": "PENDING"})
    await repo.put(
        "ops.tool_calls",
        "s:unreturned",
        {"call_id": "unreturned", "session_id": "s", "native_turn_message_id": "source"},
    )
    if cancelled_input == "callback-input":
        await repo.put(
            "ops.tool_calls",
            "s:returned",
            {
                "call_id": "returned",
                "session_id": "s",
                "native_turn_message_id": "source",
                "remote_event_id": "callback-input",
                "returned_at": "2026-10-19T21:23:00Z",
            },
        )
    states = {"source-input": "COMPLETED", cancelled_input: "CANCELLED", "independent-input": "QUEUED"}
    if independent_input:
        await repo.put(
            "ops.tool_calls",
            "s:old-callback",
            {
                "call_id": "old-callback",
                "session_id": "s",
                "native_turn_message_id": "independent",
                "native_turn_retry_count": 0,
                "remote_event_id": "old-callback-input",
                "returned_at": "2026-10-19T21:23:00Z",
            },
        )
        states["old-callback-input"] = "CANCELLED"
    history = []

    def handler(request):
        assert request.method == "GET", "Cancelled work must not be resubmitted"
        if request.url.path == "/v1/sessions/s/executions":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": identity,
                            "state": states[identity],
                            "settled_outcome": None if states[identity] == "QUEUED" else states[identity],
                        }
                        for identity in request.url.params.get_list("ids[]")
                    ]
                },
            )
        assert request.url.path == "/v1/sessions/s/events"
        return httpx.Response(200, json={"data": history})

    settings = SimpleNamespace(
        openma_url="http://native", openma_api_key="test", replay=False, load_config=lambda name: {}
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings, None, http=http)
        try:
            assert not (await service.completed_turn("s"))["complete"]
            current = await repo.get("ops.agent_sessions", "s")
            assert current["retired_message_ids"] == ["source"]
            failed = current["message_log"][0]
            assert failed["turn_status"] == "FAILED"
            failure = failed["terminal_failure"]
            assert failure["reason"] == "EXECUTION_CANCELLED"
            assert failure["execution_id"] == cancelled_input
            assert failure["queue_status"]["settled_outcome"] == "CANCELLED"
            assert not failure["queue_status"].get("recovery_refused")
            item = await repo.get("ingest.source_items", "item")
            assert (item["extraction_status"], item["extraction_reason"]) == ("FAILED", "SESSION_FAILED")

            if independent_input:
                assert current["status"] == "OPEN"
                assert current["message_log"][1] == queued
                superseded = current["superseded_native_inputs"]["old-callback-input"]
                assert superseded["reason"] == "CANCELLED_PRIOR_TURN_ATTEMPT"
                assert superseded["replacement_input_id"] == "independent-input"
                barrier = await service.execution_barrier(current)
                assert barrier["accepted_input_ids"] == ["independent-input"]
                assert not barrier["unidentified_inputs"]
                states["independent-input"] = "COMPLETED"
                history.append(
                    {
                        "id": "independent-ended",
                        "type": "session.status_idle",
                        "execution_id": "independent-input",
                        "stop_reason": {"type": "end_turn"},
                        "processed_at": "2026-10-19T21:23:20Z",
                    }
                )
                await service.process_event("s", history[0])
                assert await service.finish_if_idle("s")
                current = await repo.get("ops.agent_sessions", "s")
                assert [row["turn_status"] for row in current["message_log"]] == ["FAILED", "DONE"]
                assert current["verified_native_completion"]["accepted_input_ids"] == ["independent-input"]
            else:
                assert current["status"] == "FAILED"
                assert current["terminal_failure"] == failure
            assert current["runtime_status"] == "IDLE"
            assert current["active_message_id"] is None
        finally:
            await service.close()

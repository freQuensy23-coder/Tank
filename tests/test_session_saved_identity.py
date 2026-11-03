import httpx
import pytest
from test_session_acceptance_recovery import native_peer
from test_session_binding_concurrency import fixture_repo
from test_session_event_merge import AT, message, seed, settings

from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
@pytest.mark.parametrize("attempt", [0, 1])
async def test_saved_identity_restores_without_post_and_releases_completed_slot(
    backend, postgres_template, attempt
):
    async with fixture_repo(backend, postgres_template) as repo:
        session = await seed(repo)
        intent = session["message_log"][0]
        intent.update(
            remote_event_id=None,
            retry_count=attempt,
            native_input_attempts={"native-accepted": attempt},
        )
        await repo.put("ops.agent_sessions", "s", session)
        item = await repo.get("ingest.source_items", "i")
        item["extraction_status"] = "DONE"
        await repo.put("ingest.source_items", "i", item)
        pending = {
            "session_id": "s",
            "call_id": "pending",
            "tool": "get_item",
            "arguments": {},
            "input": {},
            "status": "PENDING",
            "native_turn_message_id": "root",
            "native_turn_retry_count": attempt,
        }
        await repo.put("ops.tool_calls", "s:pending", pending)
        async with native_peer() as (url, requests, effects):
            opts = settings()
            opts.openma_url = url
            async with httpx.AsyncClient() as client:
                service = SessionService(repo, opts, None, http=client)
                await service.recover_outbound_inputs("s")
                current = await repo.get("ops.agent_sessions", "s")
                assert current["message_log"][0]["remote_event_id"] == "native-accepted"
                assert await service.native_input_owner(current, "native-accepted") == ("root", attempt)
                assert not (await service.completed_turn("s"))["complete"]
                assert (await service.slot_state())[0] == {"s"}
                result = {
                    "type": "user.custom_tool_result",
                    "custom_tool_use_id": "pending",
                    "content": [],
                }
                accepted = await service.request(
                    "POST", "sessions/s/events", json={"events": [result]}, idem="tool:s:pending"
                )
                pending.update(
                    status="DONE", returned_at=AT, remote_event_id=service.accepted_input_id(accepted)
                )
                await repo.put("ops.tool_calls", "s:pending", pending)
                assert (await service.completed_turn("s"))["complete"]
                assert (await service.slot_state())[0] == set()
                assert len(requests) == 1
                assert requests[0][0] == "tool:s:pending"
                assert effects == [result]


@pytest.mark.parametrize(
    "conflict",
    [
        "ambiguous",
        "foreign",
        "tool",
        "superseded",
        "bool",
        "bool-count",
        "retired",
        "terminal",
        "archived",
    ],
)
async def test_saved_identity_conflicts_remain_unidentified_without_post(conflict):
    repo = MemoryRepository()
    session = await seed(repo)
    intent = session["message_log"][0]
    intent.update(remote_event_id=None, native_input_attempts={"saved": 0})
    if conflict == "ambiguous":
        intent["native_input_attempts"]["other"] = 0
    elif conflict == "foreign":
        session["message_log"].append(message("foreign", "saved"))
    elif conflict == "tool":
        await repo.put(
            "ops.tool_calls", "s:t", {"session_id": "s", "call_id": "t", "remote_event_id": "saved"}
        )
    elif conflict == "superseded":
        session["superseded_native_inputs"] = {"saved": {}}
    elif conflict == "bool":
        intent["native_input_attempts"]["saved"] = False
    elif conflict == "bool-count":
        intent["retry_count"] = False
    elif conflict == "retired":
        session["retired_message_ids"] = ["root"]
    elif conflict == "terminal":
        intent["turn_status"] = "DONE"
    elif conflict == "archived":
        session["archived_at"] = AT
    await repo.put("ops.agent_sessions", "s", session)

    def refuse(request):
        pytest.fail(f"Unexpected HTTP request: {request.method}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as client:
        service = SessionService(repo, settings(), None, http=client)
        await service.recover_outbound_inputs("s")
        current = await repo.get("ops.agent_sessions", "s")
        assert current["message_log"][0] == intent
        assert current["message_log"][0]["remote_event_id"] is None


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
@pytest.mark.parametrize("missing", [None, "body", "failure", "cutoff"])
async def test_legacy_committed_retry_replays_only_exact_saved_retry(backend, postgres_template, missing):
    async with fixture_repo(backend, postgres_template) as repo:
        session = await seed(repo)
        intent = session["message_log"][0]
        body = intent["body"]
        intent.update(
            retry_count=1,
            remote_event_id=None,
            native_input_attempts={"input-root": 0},
            superseded_native_input_ids=["input-root"],
            retry_call_ordinal_cutoffs={"0": 0},
        )
        session.update(
            pending_retry_event=None,
            processed_events=["failure"],
            superseded_native_inputs={
                "input-root": {
                    "reason": "TURN_FAILED_AND_RETRIED",
                    "failure_event_id": "failure",
                    "replacement_input_id": None,
                }
            },
        )
        if missing == "body":
            intent.pop("body")
        elif missing == "failure":
            session["processed_events"] = []
        elif missing == "cutoff":
            intent.pop("retry_call_ordinal_cutoffs")
        await repo.put("ops.agent_sessions", "s", session)
        async with native_peer() as (url, requests, effects):
            opts = settings()
            opts.openma_url = url
            async with httpx.AsyncClient() as client:
                service = SessionService(repo, opts, None, http=client)
                if not missing:
                    saved = await repo.get("ops.agent_sessions", "s")
                    assert saved is not None
                    assert saved["message_log"][0]["body"] == body
                    body = saved["message_log"][0]["body"]
                await service.request(
                    "POST", "sessions/s/events", json={"events": [body]}, idem="turn-retry:s:root"
                )
                await service.recover_outbound_inputs("s")
                current = await repo.get("ops.agent_sessions", "s")
                if missing:
                    assert current["message_log"][0] == intent
                    assert len(requests) == 1
                    assert not (await service.execution_barrier(current))["complete"]
                else:
                    assert requests[0] == requests[1]
                    assert requests[1][0] == "turn-retry:s:root"
                    assert current["message_log"][0]["remote_event_id"] == "native-accepted"
                    assert current["message_log"][0]["retry_count"] == 1
                    replacement = current["superseded_native_inputs"]["input-root"]
                    assert replacement["replacement_input_id"] == "native-accepted"
                    await service.recover_outbound_inputs("s")
                    assert len(requests) == 2
                assert effects == [body]


@pytest.mark.parametrize("turn_status", ["PENDING", "RUNNING"])
async def test_failed_session_does_not_recover_separate_legacy_missing_identity(turn_status):
    repo = MemoryRepository()
    session = await seed(repo)
    session["status"] = "FAILED"
    session["active_message_id"] = None
    session["message_log"][0]["turn_status"] = "FAILED"
    session["message_log"].append(message("later", None, status=turn_status))
    await repo.put("ops.agent_sessions", "s", session)

    def refuse(request):
        pytest.fail(f"Unexpected HTTP request: {request.method}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as client:
        service = SessionService(repo, settings(), None, http=client)
        await service.recover_outbound_inputs("s")
        assert await repo.get("ops.agent_sessions", "s") == session

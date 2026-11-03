import asyncio
import json

import httpx
import pytest
from model_fixture import process_declared_event
from test_agents_execution_barrier import config, idle
from test_session_binding_concurrency import fixture_repo

from tank.agents.context import CONTEXT_VERSION
from tank.agents.sessions import SessionService
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import sha256

AT = "2026-10-27T10:00:00Z"


def settings():
    value = config()
    load_config = value.load_config
    value.load_config = lambda name: {"max_running_sessions": 1} if name == "openma" else load_config(name)
    return value


def message(key, identity, *, status="RUNNING", attempt=0):
    return {
        "message_id": key,
        "body": {"type": "user.message", "content": [{"type": "text", "text": key}]},
        "payload": {"kind": "ITEM_ADDED", "item_id": "i", "session_id": "s"},
        "requested_at": AT,
        "sent_at": AT,
        "remote_event_id": identity,
        "retry_count": attempt,
        "turn_status": status,
        "native_input_attempts": {identity: attempt} if identity else {},
    }


async def seed(repo):
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "source_id": "S01",
            "feed": "fixture",
            "external_id": "i",
            "text": "Product announced",
            "source_class": "PRIMARY",
            "observed_at": AT,
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": AT,
            "published_prec": "EXACT",
            "raw_ref": "/fixture/raw",
            "raw_sha256": sha256("Product announced"),
            "content_hash": sha256("Product announced"),
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        },
    )
    session = {
        "session_id": "s",
        "status": "OPEN",
        "runtime_status": "IDLE",
        "item_id": "i",
        "opened_by_item": "i",
        "opened_at": AT,
        "opened_at_prec": "COLLECTION_ONLY",
        "last_item_at": AT,
        "last_item_at_prec": "COLLECTION_ONLY",
        "last_event_seq": 0,
        "agent_version": "1",
        "prompt_version": "a" * 64,
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "model_id": "fixture-model",
        "config_version": "test",
        "code_version": "test",
        "model_context_version": CONTEXT_VERSION,
        "active_message_id": "root",
        "messages": ["root"],
        "message_log": [message("root", "input-root")],
    }
    await repo.put("ops.agent_sessions", "s", session)
    return session


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
@pytest.mark.parametrize("ack_before_reload", [False, True])
async def test_event_reload_cannot_overwrite_ack_of_message_absent_from_initial_snapshot(
    backend, postgres_template, monkeypatch, ack_before_reload
):
    async with fixture_repo(backend, postgres_template) as repo:
        await seed(repo)
        tool_started = asyncio.Event()
        release_tool = asyncio.Event()
        message_posted = asyncio.Event()
        release_ack = asyncio.Event()
        stale_reload = asyncio.Event()
        release_merge = asyncio.Event()
        original_get = repo.get
        event_task = None
        send_task = None
        requests = []

        async def get(collection, key):
            row = await original_get(collection, key)
            if (
                collection == "ops.tool_calls"
                and key == "s:old-tool"
                and row
                and row.get("returned_at")
                and asyncio.current_task() is event_task
            ):
                stale_reload.set()
                await release_merge.wait()
            return row

        monkeypatch.setattr(repo, "get", get)

        async def dispatch(tool, arguments, context):
            assert tool == "web_search"
            assert arguments["query"] == "Product announcement"
            tool_started.set()
            await release_tool.wait()
            return {"results": []}

        async def handler(request):
            requests.append(request)
            assert request.method == "POST"
            body = json.loads(request.content)["events"][0]
            if body["type"] == "user.message":
                message_posted.set()
                await release_ack.wait()
                return httpx.Response(200, json={"data": [{"id": "input-new"}]})
            assert body["type"] == "user.custom_tool_result"
            assert body["custom_tool_use_id"] == "old-tool"
            return httpx.Response(200, json={"data": [{"id": "input-tool"}]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            service = SessionService(repo, settings(), dispatch, http=http)
            service.start_stream = lambda sid: None
            service.clock_timestamp = lambda: collection_clock(AT)
            payload = {"kind": "ITEM_ADDED", "item_id": "i", "session_id": "s"}
            try:
                async with asyncio.timeout(5):
                    event_task = asyncio.create_task(
                        process_declared_event(service, 
                            "s",
                            {
                                "id": "old-tool",
                                "type": "agent.custom_tool_use",
                                "execution_id": "input-root",
                                "name": "web_search",
                                "input": {"query": "Product announcement"},
                                "processed_at": AT,
                            },
                            seq=17,
                        )
                    )
                    await tool_started.wait()
                    send_task = asyncio.create_task(service.send_message(payload))
                    await message_posted.wait()
                    if ack_before_reload:
                        release_ack.set()
                        await send_task
                    release_tool.set()
                    await stale_reload.wait()
                    release_ack.set()
                    await send_task
                    async with repo.lock("session:s"):
                        concurrent = await original_get("ops.agent_sessions", "s")
                        concurrent["message_log"][0].update(
                            retry_count=1,
                            turn_status="DONE",
                            remote_event_id="input-root-retry",
                            native_input_attempts={"input-root": 0, "input-root-retry": 1},
                        )
                        concurrent["native_execution_receipts"] = {
                            "concurrent-end": idle("concurrent-end", "input-root-retry")
                        }
                        concurrent["superseded_native_inputs"] = {
                            "input-root": {"reason": "TURN_FAILED_AND_RETRIED"}
                        }
                        concurrent["processed_events"] = ["concurrent-event"]
                        concurrent["runtime_status"] = "IDLE"
                        await repo.put("ops.agent_sessions", "s", concurrent)
                    acknowledged = await original_get("ops.agent_sessions", "s")
                    accepted = next(
                        row for row in acknowledged["message_log"] if row["message_id"] == sha256(payload)
                    )
                    assert accepted["remote_event_id"] == "input-new"
                    assert accepted["turn_status"] == "PENDING"
                    release_merge.set()
                    await event_task
                current = await original_get("ops.agent_sessions", "s")
                row = next(row for row in current["message_log"] if row["message_id"] == sha256(payload))
                assert row["remote_event_id"] == "input-new"
                assert row["turn_status"] == "PENDING"
                assert row["sent_at"] == accepted["sent_at"]
                assert row["native_input_attempts"] == {"input-new": 0}
                assert current["messages"] == ["root", sha256(payload)]
                assert current["processed_events"] == ["concurrent-event", "old-tool"]
                assert current["message_log"][0] == acknowledged["message_log"][0]
                assert current["native_execution_receipts"] == acknowledged["native_execution_receipts"]
                assert current["superseded_native_inputs"] == acknowledged["superseded_native_inputs"]
                assert current["runtime_status"] == "IDLE"
                assert current["last_event_seq"] == 17
                assert (await original_get("ops.tool_calls", "s:old-tool"))["remote_event_id"] == "input-tool"
                assert [json.loads(request.content)["events"][0]["type"] for request in requests] == [
                    "user.message",
                    "user.custom_tool_result",
                ]
            finally:
                for task in (event_task, send_task):
                    if task is not None and not task.done():
                        task.cancel()
                await asyncio.gather(
                    *(task for task in (event_task, send_task) if task is not None), return_exceptions=True
                )
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

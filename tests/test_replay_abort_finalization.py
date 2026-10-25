import asyncio
import copy
import json
from pathlib import Path

import httpx
import pytest
from agent_source_fixtures import source_record_fixture
from source_support import fixture_versions

from tank.agents import SessionService
from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import MarketTruthService
from tank.event_tools import EventToolService
from tank.replay import (
    ReplayError,
    ReplayMarketReader,
    load_inputs,
    recorded_page_access,
    replay_agent_session,
)
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import new_id


@pytest.mark.parametrize(
    "failure_kind",
    [
        "missing_capture",
        "missing_snapshot",
        "lost_acceptance",
        "timeout",
        "cancel",
        "native_terminal",
        "native_terminated",
    ],
)
async def test_actual_replay_abort_finalizes_owned_turn_after_stopping_follower(
    tmp_path: Path, failure_kind
) -> None:
    cfg = {
        "registry": {"tokens": [{"token_id": "T", "symbol": "T"}], "entities": [], "instruments": []},
        "sources": {"sources": [{"source_id": "S01", "name": "Synthetic replay abort source"}]},
        "thresholds": Settings(_env_file=None).load_config("thresholds"),
        "semantics": {},
        "dependency_map": {},
        "openma": {
            "agent_id": "synthetic-agent",
            "environment_id": "unused-live",
            "replay_environment_id": "offline",
        },
    }

    class ReplaySettings(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name])

    settings = ReplaySettings(
        replay=True,
        config_version="test",
        code_version="test",
        data_dir=tmp_path,
        openma_url="http://127.0.0.1:9999",
        _env_file=None,
    )
    source = source_record_fixture(
        {
            "item_id": new_id("item"),
            "source_id": "S01",
            "text": "Explicit synthetic abort fixture",
            "observed_at": "2026-09-24T11:59:00Z",
            "observed_at_prec": "COLLECTION_ONLY",
        }
    )
    packet = tmp_path / "packet.json"
    packet.write_text(
        json.dumps(
            {
                "cutoff": "2026-09-24T12:00:00Z",
                "code_version": "test",
                "config_version": "test",
                "registry": cfg["registry"],
                "config": {k: v for k, v in cfg.items() if k != "registry"},
                "records": [
                    {
                        "collection": "ingest.source_items",
                        "key": source["item_id"],
                        "available_at": source["observed_at"],
                        "record": source,
                    }
                ],
                "tool_results": [],
            }
        )
    )
    event_id = new_id("event")
    if failure_kind == "missing_snapshot":
        data = json.loads(packet.read_text())
        ems_id = new_id("ems")
        data["records"].extend(
            [
                {
                    "collection": "evt.events",
                    "key": event_id,
                    "available_at": source["observed_at"],
                    "record": {
                        "event_id": event_id,
                        "statement": "Explicit synthetic major event",
                        "event_state": "APPROVED",
                        "tokens_mentioned": ["T"],
                        "current_ems_id": ems_id,
                        "public_at": source["observed_at"],
                        "public_at_prec": "EXACT",
                        "known_to_us_at": source["observed_at"],
                        "known_to_us_at_prec": "EXACT",
                    },
                },
                {
                    "collection": "evt.ems_assessments",
                    "key": ems_id,
                    "available_at": source["observed_at"],
                    "record": {"ems_id": ems_id, "event_id": event_id, "class": "MAJOR"},
                },
            ]
        )
        packet.write_text(json.dumps(data))
    repo = MemoryRepository()
    queue = asyncio.Queue()
    streaming = asyncio.Event()
    retired = asyncio.Event()
    archives = []
    accepted = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            try:
                while True:
                    event = await queue.get()
                    yield ("data: " + json.dumps(event) + "\n\n").encode()
                    streaming.set()
            finally:
                retired.set()

        async def aclose(self) -> None:
            if failure_kind not in {"native_terminal", "native_terminated", "lost_acceptance"}:
                current = await repo.get("ops.agent_sessions", "owned-session")

                assert current["status"] == "OPEN"
            retired.set()

    async def transport(request: httpx.Request):
        path = request.url.path
        if path.endswith("/archive"):
            if failure_kind != "lost_acceptance":
                assert retired.is_set()
            row = await repo.get("ops.agent_sessions", "owned-session")
            assert row["status"] == "FAILED" and row["runtime_status"] == "IDLE"
            assert row["active_message_id"] is None
            archives.append(path)
            return httpx.Response(200, json={"id": "owned-session", "archived": True})
        if path.endswith("/sessions"):
            return (
                httpx.Response(200, json={"data": []})
                if request.method == "GET"
                else httpx.Response(201, json={"id": "owned-session"})
            )
        if path.endswith("/owned-session"):
            return httpx.Response(
                200,
                json={
                    "id": "owned-session",
                    "status": "running",
                    "agent": {
                        "id": "synthetic-agent",
                        "version": 1,
                        "system": "Synthetic prompt",
                        "skills": [],
                        "tools": [],
                        "model": {"id": "synthetic-model"},
                    },
                },
            )
        if path.endswith("/executions"):
            ids = request.url.params.get_list("ids[]")
            assert set(ids) <= set(accepted)
            return httpx.Response(
                200, json={"data": [{"execution_id": identity, "state": "RUNNING"} for identity in ids]}
            )
        if path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream())
        if path.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": []})
            body = json.loads(request.content)["events"][0]
            if failure_kind == "lost_acceptance":
                raise httpx.ReadError("Synthetic reply lost after acceptance", request=request)
            identity = "accepted-" + str(len(accepted))
            accepted.append(identity)
            if body["type"] == "user.message":
                await queue.put(
                    {"id": "running-" + identity, "type": "session.status_running", "execution_id": identity}
                )
                if failure_kind == "missing_capture":
                    await queue.put(
                        {
                            "id": "missing-page",
                            "type": "agent.custom_tool_use",
                            "name": "fetch_page",
                            "input": {"url": "https://synthetic.invalid/uncaptured"},
                            "execution_id": identity,
                        }
                    )
                elif failure_kind == "missing_snapshot":
                    await queue.put(
                        {
                            "id": "missing-market",
                            "type": "agent.custom_tool_use",
                            "name": "get_market_truth",
                            "input": {"token_ids": ["T"], "cutoff": source["observed_at"]},
                            "execution_id": identity,
                        }
                    )
                elif failure_kind == "native_terminated":
                    await queue.put(
                        {
                            "id": "terminated",
                            "type": "session.status_terminated",
                            "execution_id": identity,
                            "reason": "Explicit synthetic native termination",
                        }
                    )
                elif failure_kind == "native_terminal":
                    await queue.put(
                        {
                            "id": "failure-" + identity,
                            "type": "session.error",
                            "execution_id": identity,
                            "reason": "Explicit synthetic native failure",
                        }
                    )
            return httpx.Response(200, json={"data": [{"id": identity}]})
        raise AssertionError(str(request.url))

    output = tmp_path / "failed.json"
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        truth = MarketTruthService(
            repo, settings, ReplayMarketReader(load_inputs(packet, settings)), versions=fixture_versions()
        )
        reaction_truth = truth
        reaction_scheduler = Scheduler(
            repo,
            settings,
            reaction_truth.market,
            reaction_truth,
            thresholds=settings.thresholds(),
            versions=fixture_versions(),
        )
        tools = EventToolService(
            repo,
            settings,
            reaction_truth,
            DecisionEngine(settings),
            http=http,
            versions=fixture_versions(),
            scheduler=reaction_scheduler,
            pages=recorded_page_access(load_inputs(packet, settings)),
        )
        task = asyncio.create_task(
            replay_agent_session(
                packet,
                repo,
                settings,
                tools,
                http=http,
                initial_message={
                    "kind": "ITEM_ADDED" if failure_kind == "missing_snapshot" else "NEW_ITEM",
                    "item_id": source["item_id"],
                    **({"event_id": event_id} if failure_kind == "missing_snapshot" else {}),
                },
                replay_run_id="unique-owned-run",
                timeout=0.3 if failure_kind == "timeout" else 2,
                output_path=output,
            )
        )
        if failure_kind == "cancel":
            await asyncio.wait_for(streaming.wait(), 1)
            task.cancel()
        expected = {
            "missing_capture": ReplayError,
            "missing_snapshot": ReplayError,
            "lost_acceptance": httpx.ReadError,
            "timeout": TimeoutError,
            "cancel": asyncio.CancelledError,
            "native_terminal": ReplayError,
            "native_terminated": ReplayError,
        }[failure_kind]
        with pytest.raises(expected):
            await task
    row = await repo.get("ops.agent_sessions", "owned-session")
    assert row["status"] == "FAILED" and row["runtime_status"] == "IDLE"
    assert row["active_message_id"] is None
    assert row["replay_abort"]["replay_run_id"] == "unique-owned-run"
    assert all(m["turn_status"] == "FAILED" for m in row["message_log"])
    assert archives and len(archives) == 1
    result = json.loads(output.read_text())
    assert result["status"] == "FAILED" and result["output"] is None
    assert result["cleanup_sessions"][0]["local_finalization"]["finalized"]
    if failure_kind == "native_terminal":
        assert row["terminal_failure"]["reason"] == "Explicit synthetic native failure"
        assert row["message_log"][0]["retry_count"] == 1
    else:
        assert (await repo.get("ingest.source_items", source["item_id"]))["extraction_status"] == "PENDING"
    if failure_kind == "missing_snapshot":
        assert "missing exact market snapshot" in result["failure"]["reason"]
        assert len(await repo.list("evt.ems_assessments")) == 1
        assert not await repo.list("ana.market_truth")
    else:
        assert not await repo.list("evt.ems_assessments")
    assert not await repo.list("out.alerts")


async def test_replay_abort_preserves_completed_state_and_refuses_other_ownership() -> None:
    repo = MemoryRepository()
    cfg = Settings(_env_file=None)
    source = {"item_id": "completed-source", "extraction_status": "DONE", "event_id": "accepted-event"}
    event = {"event_id": "accepted-event", "event_state": "PROPOSED", "statement": "Accepted fact"}
    done = {
        "message_id": "completed",
        "turn_status": "DONE",
        "workflow_completion_verified": {"version": 1, "immutable": "proof"},
    }
    pending = {"message_id": "pending", "turn_status": "ACCEPTING", "remote_event_id": None}
    row = {
        "session_id": "own",
        "replay_run_id": "current",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "pending",
        "message_log": [done, pending],
    }
    foreign = dict(row, session_id="foreign", replay_run_id="other")
    for sid, s in [("own", row), ("foreign", foreign)]:
        await repo.put("ops.agent_sessions", sid, copy.deepcopy(s))
    await repo.put("ingest.source_items", source["item_id"], copy.deepcopy(source))
    await repo.put("evt.events", event["event_id"], copy.deepcopy(event))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: pytest.fail("No network required"))
    ) as http:
        service = SessionService(repo, cfg, None, http=http, versions=fixture_versions())
        refusal = await service.finalize_replay_abort("foreign", "current", {"reason": "fixture unavailable"})
        assert refusal["finalized"] is False
        await service.finalize_replay_abort(
            "own",
            "current",
            {"error_type": "ReplayError", "reason": "insufficient_fixture: missing exact market snapshot"},
        )
        after = await repo.get("ops.agent_sessions", "own")
        assert after["message_log"][0] == done
        assert after["message_log"][1]["turn_status"] == "FAILED"
        await service.process_event("own", {"id": "late-running", "type": "session.status_running"})
        assert await repo.get("ops.agent_sessions", "own") == after
        assert await repo.get("ops.agent_sessions", "foreign") == foreign
        assert await repo.get("ingest.source_items", source["item_id"]) == source
        assert await repo.get("evt.events", event["event_id"]) == event
        await service.close()


async def test_native_terminal_failure_clears_runtime_but_preserves_done_messages_and_source() -> None:
    repo = MemoryRepository()
    done = {
        "message_id": "done",
        "turn_status": "DONE",
        "retry_count": 0,
        "payload": {"kind": "NEW_ITEM", "item_id": "source-done"},
        "immutable_completion": "retained",
    }
    active = {
        "message_id": "active",
        "turn_status": "RUNNING",
        "retry_count": 1,
        "body": {"type": "user.message"},
        "payload": {"kind": "NEW_ITEM", "item_id": "source-done"},
        "remote_event_id": "accepted-retry",
    }
    session = {
        "session_id": "native",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "active",
        "message_log": [done, active],
    }
    source = source_record_fixture(
        {
            "item_id": "source-done",
            "source_id": "S01",
            "extraction_status": "DONE",
            "text": "Preserved accepted synthetic fact",
        }
    )
    await repo.put("ops.agent_sessions", "native", copy.deepcopy(session))
    await repo.put("ingest.source_items", "source-done", copy.deepcopy(source))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: pytest.fail("Terminal failure does not retry"))
    ) as http:
        service = SessionService(repo, Settings(_env_file=None), None, http=http, versions=fixture_versions())
        failure = {
            "id": "failure-final",
            "type": "session.error",
            "execution_id": "accepted-retry",
            "reason": "Native execution failed after retry",
            "error": {"code": "SYNTHETIC_NATIVE_FAILURE", "message": "Precise retained cause"},
        }
        await service.process_event("native", failure)
        actual = await repo.get("ops.agent_sessions", "native")
        assert actual["status"] == "FAILED" and actual["runtime_status"] == "IDLE"
        assert actual["active_message_id"] is None
        assert actual["message_log"][0] == done
        assert actual["message_log"][1]["turn_status"] == "FAILED"
        assert actual["terminal_failure"]["error"] == failure["error"]
        assert actual["terminal_failure"]["reason"] == failure["reason"]
        assert await repo.get("ingest.source_items", "source-done") == source
        await service.process_event(
            "native", {"id": "late", "type": "session.status_running", "execution_id": "accepted-retry"}
        )
        assert await repo.get("ops.agent_sessions", "native") == actual
        assert (await service.finalize_replay_abort("native", None, {"reason": "not owned"}))[
            "finalized"
        ] is False
        assert await repo.get("ops.agent_sessions", "native") == actual
        await service.close()


@pytest.mark.parametrize("turn_status", ["RUNNING", "DONE"])
async def test_native_termination_clears_runtime_without_manufacturing_completion(turn_status) -> None:
    repo = MemoryRepository()
    message = {
        "message_id": "turn",
        "turn_status": turn_status,
        "retry_count": 0,
        "payload": {"kind": "NEW_ITEM", "item_id": "done-source"},
    }
    if turn_status == "DONE":
        message["workflow_completion_verified"] = {"version": 1, "immutable": "proof"}
    session = {
        "session_id": "terminated",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "turn",
        "message_log": [copy.deepcopy(message)],
    }
    source = source_record_fixture(
        {
            "item_id": "done-source",
            "source_id": "S01",
            "extraction_status": "DONE",
            "text": "Already completed synthetic source",
        }
    )
    await repo.put("ops.agent_sessions", "terminated", session)
    await repo.put("ingest.source_items", "done-source", source)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: pytest.fail("No network required"))
    ) as http:
        service = SessionService(repo, Settings(_env_file=None), None, http=http, versions=fixture_versions())
        event = {
            "id": "actual-terminal",
            "type": "session.status_terminated",
            "reason": "Explicit native shutdown",
        }
        await service.process_event("terminated", event)
        result = await repo.get("ops.agent_sessions", "terminated")
        assert result["status"] == "ENDED" and result["runtime_status"] == "IDLE"
        assert result["active_message_id"] is None
        if turn_status == "DONE":
            assert result["message_log"] == [message]
        else:
            assert result["message_log"][0]["turn_status"] == "FAILED"
            assert result["message_log"][0]["terminal_failure"]["reason"] == event["reason"]
        assert await repo.get("ingest.source_items", "done-source") == source
        await service.process_event("terminated", {"id": "late-running", "type": "session.status_running"})
        assert await repo.get("ops.agent_sessions", "terminated") == result
        await service.close()

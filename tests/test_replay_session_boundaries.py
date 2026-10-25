"""Explicit NEW_ITEM starts a fresh replay session; ITEM_ADDED retains its event."""

import copy
import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from source_support import fixture_versions

from tank.agents.sessions import SessionService
from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import MarketTruthService
from tank.event_tools import EventToolService
from tank.replay import ReplayMarketReader, load_inputs, recorded_page_access, replay_agent_session
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.mark.parametrize("second_kind,event_count", [("NEW_ITEM", 2), ("ITEM_ADDED", 1)])
async def test_explicit_replay_session_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, second_kind, event_count
) -> None:
    cfg = {
        "registry": {"tokens": []},
        "thresholds": Settings(_env_file=None).load_config("thresholds"),
        "semantics": {},
        "dependency_map": {},
        "openma": {"agent_id": "analyst", "replay_environment_id": "offline"},
    }

    class Config(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name])

    settings = Config(
        _env_file=None, replay=True, code_version="test", config_version="test", data_dir=tmp_path
    )
    at = "2026-09-18T17:12:00Z"
    path = tmp_path / "input.json"
    path.write_text(
        json.dumps(
            {
                "cutoff": at,
                "code_version": "test",
                "config_version": "test",
                "registry": cfg["registry"],
                "config": {k: v for k, v in cfg.items() if k != "registry"},
                "tool_results": [],
                "records": [
                    {
                        "collection": "ingest.source_items",
                        "key": item,
                        "available_at": at,
                        "record": {
                            "item_id": item,
                            "source_id": "S01",
                            "text": item,
                            "observed_at": at,
                            "source_class": "PRIMARY",
                        },
                    }
                    for item in ["testing", "validator"]
                ],
            }
        )
    )
    repo = MemoryRepository()
    reaction_truth = MarketTruthService(
        repo, settings, ReplayMarketReader(load_inputs(path, settings)), versions=fixture_versions()
    )
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
        versions=fixture_versions(),
        scheduler=reaction_scheduler,
        pages=recorded_page_access(load_inputs(path, settings)),
    )
    sent, archived = [], []

    async def send(service, payload):

        sid = payload.get("session_id")
        if sid is None:
            sid = "native-" + str(len(await repo.list("ops.agent_sessions")) + 1)
            await repo.put(
                "ops.agent_sessions",
                sid,
                {
                    "session_id": sid,
                    "status": "OPEN",
                    "runtime_status": "IDLE",
                    "replay_run_id": payload["replay_run_id"],
                    "message_log": [],
                },
            )
        session = await repo.get("ops.agent_sessions", sid)
        item = payload["item_id"]
        stamp = {"value": at, "precision": "EXACT", "quote": None}
        if payload["kind"] == "NEW_ITEM":
            result = await tools.dispatch(
                "record_event",
                {
                    "item_id": item,
                    "is_event": True,
                    "statement": item,
                    "certainty": "HIGH",
                    "certainty_reason": "Synthetic actor",
                    "entities": [],
                    "token_ids": [],
                    "event_time": stamp,
                    "public_at": {**stamp, "basis": "Synthetic input"},
                    "freshness": "FRESH",
                    "claims": [],
                    "next_milestone": None,
                    "checked_sources": [],
                },
                {"session_id": sid},
            )
        else:
            result = await tools.dispatch(
                "add_observation",
                {
                    "item_id": item,
                    "event_id": session["event_id"],
                    "link": "CORROBORATION",
                    "claims": [],
                    "public_at": {**stamp, "basis": "Synthetic input"},
                    "freshness": "FRESH",
                },
                {"session_id": sid},
            )
        assert not result.get("error"), result
        source = await repo.get("ingest.source_items", item)
        await repo.put("ingest.source_items", item, dict(source, extraction_status="DONE"))
        sent.append((item, sid))
        return await repo.get("ops.agent_sessions", sid)

    async def request(service, method, path, **kwargs):
        assert method == "POST" and path.endswith("/archive")
        archived.append(path.split("/")[1])
        return {}

    monkeypatch.setattr(SessionService, "send_message", send)
    monkeypatch.setattr(SessionService, "completed_turn", AsyncMock(return_value={"complete": True}))
    monkeypatch.setattr(SessionService, "request", request)
    async with httpx.AsyncClient() as http:
        report = await replay_agent_session(
            path,
            repo,
            settings,
            tools,
            http=http,
            initial_message={"kind": "NEW_ITEM", "item_id": "testing"},
            subsequent_messages=[{"kind": second_kind, "item_id": "validator"}],
            timeout=2,
        )
    assert report["status"] == "COMPLETED"
    assert len(report["output"]["events"]) == event_count
    assert len({sid for _, sid in sent}) == event_count
    assert set(archived) == {sid for _, sid in sent}

    observations = report["output"]["observations"]
    assert len(observations) == 2
    assert {o["item_id"]: o["link_type"] for o in observations} == {
        "testing": "ORIGIN",
        "validator": "ORIGIN" if second_kind == "NEW_ITEM" else "CORROBORATION",
    }

import json
from datetime import timedelta
from pathlib import Path
from typing import Literal

import httpx
import pytest
from agent_configuration_fixtures import AgentFixtureSettings, FixtureConfigurations
from agent_source_fixtures import (
    declared_versions_fixture,
    native_tool_arguments_fixture,
    source_record_fixture,
)
from native_session_fixtures import created_native_session
from pydantic import JsonValue
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_http_admission

from tank.agents import ConfigurationError, Pipeline, SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.agents.native_sessions import NativeSessionResolution
from tank.analysis.page_access import LivePageAccess
from tank.contracts.source_items import parse_feed_source_item
from tank.ops_progress import DisabledSourceProgress
from tank.runtime_contracts import ScoutReviewJob
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.telegram_rich import score_summary
from tank.util import clock_iso, iso, sha256, utcnow


def settings(
    *,
    config_version: str = "test",
    code_version: str = "test",
    openma_model: str = "gpt-6.1-sol",
    openma_url: str = "http://oma",
) -> tuple[AgentFixtureSettings, FixtureConfigurations]:
    config: FixtureConfigurations = {
        "thresholds": {"filter_cutoff": 0.7, "routing_cutoff": 0.8},
        "registry": {
            "entities": [
                {"entity_id": "aave", "name": "Aave", "names": ["Aave"], "aliases": ["Aave", "Aave Labs"]}
            ],
            "tokens": [],
            "x_accounts": [],
            "instruments": [],
        },
        "sources": {"sources": []},
        "openma": {
            "agent_id": "agt_1",
            "scout_agent_id": "agt_s",
            "environment_id": "env_1",
            "max_running_sessions": 2,
            "prompt_version": "test",
            "reasoning_effort": "medium",
        },
    }
    config["thresholds"] = {
        **Settings(_env_file=None).thresholds().model_dump(mode="json"),
        **config["thresholds"],
    }
    return AgentFixtureSettings(
        documents=config,
        tool_reader=Settings(_env_file=None).read_config_json,
        openrouter_api_key="secret",
        opencode_api_key=None,
        replay=False,
        ops_progress_enabled=False,
        openma_api_key="secret",
        openma_url=openma_url,
        openma_model=openma_model,
        config_version=config_version,
        code_version=code_version,
        telegram_token=None,
        codex_gateway_key=None,
        database_url=None,
    ), config


async def seed_session(repo, **changes):
    from tank.openma_sync import agent_tools
    from tank.util import sha256

    row = {
        "session_id": "ses_1",
        "model_context_version": CONTEXT_VERSION,
        "tools_sha256": sha256(agent_tools(settings()[0], "tank-event-analyst")),
        "event_id": "evt_1",
        "item_id": "itm_1",
        "status": "OPEN",
        "model_id": "gpt-6.1-sol",
        "prompt_version": "test",
        "last_event_seq": 0,
        "messages": [],
    }
    row.update(changes)
    from tank.util import sha256

    row["prompt_label"] = row["prompt_version"]
    row["prompt_version"] = sha256(
        {"system": "Resolved synthetic fixture prompt", "label": row["prompt_label"]}
    )
    row["prompt_version_basis"] = "RESOLVED_SESSION_AGENT_SNAPSHOT"
    await repo.put("ops.agent_sessions", "ses_1", row)
    return row


@pytest.mark.asyncio
async def test_filter_requires_calibration_and_logs_typed_usage() -> None:
    repo = MemoryRepository()
    opts, config = settings()
    await repo.put(
        "ingest.source_items",
        "i",
        source_record_fixture(
            {"item_id": "i", "source_id": "S30", "text": "Aave release", "source": "issuer"}
        ),
    )
    calls = []

    def handler(request: httpx.Request):
        calls.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "answers": {"relevant": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 100, "output_tokens": 0, "cost": "0.0000042"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        pipe = Pipeline(
            repo, opts, http=http, progress=DisabledSourceProgress(), versions=declared_versions_fixture()
        )
        config["thresholds"]["filter_cutoff"] = None
        with pytest.raises(ConfigurationError):
            await pipe.filter_item_wire({"item_id": "i"})
        assert calls == []
        config["thresholds"]["filter_cutoff"] = 0.7
        await pipe.filter_item_wire({"item_id": "i"})
        await pipe.filter_item_wire({"item_id": "i"})
    assert len(calls) == 1
    assert set(calls[0]["state"]) == {"item"}
    assert set(calls[0]["state"]["item"]) <= {"source", "author", "title", "text", "quoted"}
    jobs = await repo.list("ops.jobs")
    assert len(jobs) == 1 and jobs[0]["type"] == "ROUTE_ITEM"
    usage = await repo.list("ops.llm_usage")
    assert usage[0]["cost_usd"] == "0.0000042"
    assert usage[0]["billing_basis"] == "OPENCODE_REPORTED"
    assert usage[0]["endpoint"] == "https://opencode.ai/inference/systemone/v1/systemone"


@pytest.mark.asyncio
async def test_routing_whole_word_alias_three_day_and_best_score() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await repo.put(
        "ingest.source_items",
        "i",
        source_record_fixture({"item_id": "i", "text": "Aave Labs confirms launch", "deferrable": False}),
    )
    for name, days in [("old", 4), ("new", 1), ("best", 2)]:
        await repo.put(
            "evt.events",
            name,
            {
                "lifecycle": "ANALYZING",
                "tokens_mentioned": [],
                "event_state": "RUMOR",
                "event_id": name,
                "statement": name,
                "entities": ["aave"],
                "tokens": [],
            },
        )
        await repo.put(
            "ops.agent_sessions",
            name,
            {
                "status": "OPEN",
                "session_id": name,
                "event_id": name,
                "last_item_at": iso(utcnow() - timedelta(days=days)),
            },
        )
    seen = []

    def handler(req):
        event = json.loads(req.content)["state"]["event"]["statement"]
        seen.append(event)
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "answers": {"same_event": {"type": "noul", "noul": 0.95 if event == "best" else 0.85}},
                "usage": {"input_tokens": 100},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        pipe = Pipeline(
            repo, opts, http=http, progress=DisabledSourceProgress(), versions=declared_versions_fixture()
        )
        assert (
            pipe.identities(
                parse_feed_source_item(source_record_fixture({"item_id": "fixture", "text": "notAave Labsx"}))
            )
            == set()
        )
        result = await pipe.route_item_wire({"item_id": "i"})
        await pipe.route_item_wire({"item_id": "i"})
    assert result["route_event_id"] == "best" and seen == ["best", "new"]
    job = (await repo.list("ops.jobs"))[0]
    assert job["payload"] == {"kind": "ITEM_ADDED", "item_id": "i", "event_id": "best"}


@pytest.mark.asyncio
async def test_tool_result_lost_reply_restart_reuses_completed_result() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo)
    count = 0
    posts = []

    async def dispatch(name, args, context):
        nonlocal count
        count += 1
        await repo.put(
            "evt.events",
            "evt_1",
            {
                "statement": "Synthetic event",
                "lifecycle": "ANALYZING",
                "tokens_mentioned": [],
                "event_state": "RUMOR",
                "event_id": "evt_1",
                "result": args,
            },
        )
        return {"ok": True}

    def broken(req) -> None:
        posts.append(req)
        raise httpx.ReadError("lost response", request=req)

    event = {
        "id": "call_1",
        "type": "agent.custom_tool_use",
        "name": "record_event",
        "input": native_tool_arguments_fixture("record_event"),
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(broken)) as http:
        service = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        with pytest.raises(httpx.ReadError):
            await service.process_event("ses_1", event, 7)
    assert count == 1
    assert (await repo.get("ops.tool_calls", "ses_1:call_1"))["status"] == "DONE"
    assert (await repo.get("ops.agent_sessions", "ses_1"))["last_event_seq"] == 0

    def works(req):
        posts.append(req)
        return httpx.Response(200, json={"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(works)) as http:
        restarted = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        await restarted.process_event("ses_1", event, 7)
        await restarted.process_event("ses_1", event, 7)
    assert count == 1 and len(posts) == 2
    assert posts[0].headers["Idempotency-Key"] == posts[1].headers["Idempotency-Key"]
    body = json.loads(posts[1].content)["events"][0]
    assert body["custom_tool_use_id"] == "call_1" and body["content"][0]["text"] == '{"ok":true}'
    assert (await repo.get("ops.agent_sessions", "ses_1"))["last_event_seq"] == 7


@pytest.mark.asyncio
async def test_failed_dispatch_rolls_back_and_replays() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo)

    async def bad(*args) -> None:
        await repo.put(
            "evt.events",
            "oops",
            {
                "statement": "Synthetic event",
                "lifecycle": "ANALYZING",
                "tokens_mentioned": [],
                "event_state": "RUMOR",
                "event_id": "oops",
            },
        )
        raise ValueError("bad data")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    ) as http:
        service = SessionService(repo, opts, bad, http=http, versions=declared_versions_fixture())
        await service.process_event(
            "ses_1",
            {
                "id": "c",
                "type": "agent.custom_tool_use",
                "name": "record_event",
                "input": native_tool_arguments_fixture("record_event"),
            },
        )
    assert await repo.get("evt.events", "oops") is None
    assert (await repo.get("ops.agent_sessions", "ses_1"))["processed_events"] == ["c"]
    refused = await repo.get("ops.tool_calls", "ses_1:c")
    assert refused["status"] == "REFUSED" and refused["result"]["error"]["reason"] == "bad data"


@pytest.mark.asyncio
async def test_sse_resume_and_paginated_recovery_dedup_usage() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo, last_event_seq=3)
    event = {
        "id": "usage_1",
        "type": "span.model_request_end",
        "processed_at": clock_iso(),
        "model_usage": {"input_tokens": 1000, "output_tokens": 10},
    }
    requests = []

    def handler(req):
        requests.append(req)
        if req.url.path.endswith("/stream"):
            assert req.headers["Last-Event-ID"] == "3"
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text="id: 4\nevent: span.model_request_end\ndata: " + json.dumps(event) + "\n\n",
            )
        if req.url.params.get("page") == "next":
            return httpx.Response(200, json={"data": [event], "next_page": None})
        return httpx.Response(200, json={"data": [], "next_page": "next"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        await service.stream("ses_1")
        await service.catch_up("ses_1")
    usage = await repo.list("ops.llm_usage")
    assert len(usage) == 1
    assert usage[0]["cost_usd"] == "0"
    assert usage[0]["billing_basis"] == "SUBSCRIPTION"
    assert usage[0]["input_tokens"] == 1000
    assert usage[0]["output_tokens"] == 10
    assert (await repo.get("ops.agent_sessions", "ses_1"))["last_event_seq"] == 4


@pytest.mark.asyncio
async def test_message_running_session_posted_and_retry_failure() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo, status="RUNNING")
    await repo.put(
        "evt.events",
        "evt_1",
        {
            "statement": "Synthetic event",
            "lifecycle": "ANALYZING",
            "tokens_mentioned": [],
            "event_state": "RUMOR",
            "event_id": "evt_1",
        },
    )
    await repo.put(
        "ingest.source_items", "itm_2", source_record_fixture({"item_id": "itm_2", "text": "Update"})
    )
    posts = []

    def handler(req):
        posts.append(json.loads(req.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        service.start_stream = lambda sid: None
        payload = {"kind": "ITEM_ADDED", "event_id": "evt_1", "item_id": "itm_2"}
        await service.send_message(payload)
        await service.send_message(payload)
        assert len(posts) == 1
        assert posts[0]["events"][0]["type"] == "user.message"
        await service.process_event("ses_1", {"id": "err1", "type": "session.error"})
        await service.process_event("ses_1", {"id": "err2", "type": "session.error"})
    assert len(posts) == 2
    assert (await repo.get("ops.agent_sessions", "ses_1"))["status"] == "FAILED"
    assert "status" not in (await repo.get("evt.events", "evt_1"))


@pytest.mark.asyncio
async def test_canonical_event_routing_and_added_message() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await repo.put(
        "ingest.source_items",
        "i",
        source_record_fixture({"item_id": "i", "source_id": "S01", "text": "Aave confirms launch"}),
    )
    await repo.put(
        "evt.events",
        "evt_1",
        {
            "statement": "Synthetic event",
            "event_id": "evt_1",
            "entities": [{"entity_id": "aave", "role": "SUBJECT"}],
            "tokens_mentioned": ["AAVE"],
            "event_state": "LIVE",
            "lifecycle": "ANALYZING",
            "current_ems_id": "ems_1",
        },
    )
    await repo.put("evt.ems_assessments", "ems_1", {"ems_id": "ems_1", "event_id": "evt_1", "class": "MAJOR"})
    await seed_session(repo, last_item_at=clock_iso())
    posted = []

    def handler(req):
        body = json.loads(req.content)
        posted.append(body)
        if req.url.host == "opencode.ai":
            assert req.url.path == "/inference/systemone/v1/systemone"
            assert body["state"]["event"]["stage"] == "live"
            assert body["state"]["event"]["tokens"] == ["AAVE"]
            return httpx.Response(
                200,
                json={
                    "model": "jev-1.13-free",
                    "answers": {"same_event": {"type": "noul", "noul": 0.99}},
                    "usage": {"input_tokens": 100},
                },
            )
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        routed = await Pipeline(
            repo, opts, http=http, progress=DisabledSourceProgress(), versions=declared_versions_fixture()
        ).route_item_wire({"item_id": "i"})
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        service.start_stream = lambda sid: None
        await service.send_message(routed["routing_message"])
    assert routed["route"]["decision"] == "ADDED"
    assert len(posted) == 2


@pytest.mark.asyncio
async def test_new_scout_session_wire_protocol_and_isolation() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await repo.put("scout.expansion_proposals", "p", {"prp_id": "p", "concept_name": "test"})
    posted = []

    def handler(req):
        if req.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        body = json.loads(req.content)
        posted.append(body)
        return httpx.Response(
            200, json=created_native_session(req, "ses_scout") if req.url.path == "/v1/sessions" else {}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        service.start_stream = lambda sid: None
        session = await service.scout_review(ScoutReviewJob(proposal_id="p"))
        await service.scout_review(ScoutReviewJob(proposal_id="p"))
        await service.process_event(
            session.session_id,
            {"id": "call_bad", "type": "agent.custom_tool_use", "name": "publish_card", "input": {}},
        )
    assert len(posted) == 3
    assert posted[2]["events"][0]["is_error"] is True
    assert posted[0] == {
        "agent": "agt_s",
        "environment_id": "env_1",
        "metadata": {
            "tank_identity": sha256(
                {"identity": "p", "scout": True, "model_context_version": CONTEXT_VERSION}
            )
        },
    }
    content = json.loads(posted[1]["events"][0]["content"][0]["text"])
    assert content["proposal"]["concept_name"] == "test"
    assert "prp_id" not in content["proposal"]


@pytest.mark.asyncio
async def test_stream_processing_preserves_concurrent_added_message() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo)
    await repo.put(
        "ingest.source_items", "itm_2", source_record_fixture({"item_id": "itm_2", "text": "fresh"})
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    ) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        service.start_stream = lambda sid: None
        original_usage = service.usage

        async def usage(session, event) -> None:
            await service.send_message({"kind": "ITEM_ADDED", "event_id": "evt_1", "item_id": "itm_2"})
            await original_usage(session, event)

        service.usage = usage
        await service.process_event(
            "ses_1", {"id": "u", "type": "span.model_request_end", "model_usage": {"input_tokens": 10}}, 9
        )
    record = await repo.get("ops.agent_sessions", "ses_1")
    assert len(record["messages"]) == 1 and len(record["message_log"]) == 1
    assert record["last_event_seq"] == 9


@pytest.mark.asyncio
async def test_slow_denial_turn_remains_until_exit_update_delivered(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from tank.analysis import cards
    from tank.analysis.card_contracts import CardInputs
    from tank.analysis.decisions import DecisionEngine
    from tank.event_tools import EventToolService
    from tank.settings import Settings

    repo = MemoryRepository()
    opts = Settings(data_dir=tmp_path)
    tools = EventToolService(
        repo,
        opts,
        None,
        DecisionEngine(),
        versions=declared_versions_fixture(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    now = clock_iso()
    await repo.put(
        "evt.events",
        "evt_1",
        {
            "event_id": "evt_1",
            "statement": "Product announced",
            "certainty": "HIGH",
            "event_state": "DEPLOYED",
            "state_verified": True,
            "lifecycle": "MONITORING",
            "next_milestone": None,
            "contradictions": [],
            "current_ems_id": "ems",
            "public_at": now,
            "known_to_us_at": now,
            "freshness": "FRESH",
            "entities": [],
            "tokens_mentioned": ["world:WLD"],
        },
    )
    await repo.put(
        "evt.ems_assessments",
        "ems",
        {
            "ems_id": "ems",
            "event_id": "evt_1",
            "class": "MAJOR",
            "criterion": "A deployed product announcement is the recorded synthetic major event.",
            "impact_horizon": {"impact_start": "UNKNOWN"},
            "all_dependencies_within_24h": None,
            "dims": {
                key: {"value": None, "reason": "NO_ESTIMATE"}
                for key in (
                    "economic_directness",
                    "magnitude",
                    "permanence",
                    "distribution_expansion",
                    "mechanical_flows",
                    "surprise",
                    "implementation",
                )
            },
        },
    )
    await repo.put(
        "evt.beneficiary_maps",
        "map",
        {
            "event_id": "evt_1",
            "map_version": 1,
            "analysis_set": ["world:WLD"],
            "least_repriced_edge": None,
            "no_credible_secondary_reason": "The synthetic announcement names only the product issuer.",
            "layers": {
                "ISSUER_PRODUCT": {"note": "Only the named product issuer is identified."},
                "VENUE_DEX": {"note": "The announcement identifies no venue relationship."},
                "SETTLEMENT_L1_L2": {"note": "The announcement identifies no settlement relationship."},
                "LIQUIDITY_EXECUTION": {"note": "The announcement identifies no liquidity relationship."},
                "LENDING_COLLATERAL": {"note": "The announcement identifies no lending relationship."},
                "ORACLE_DATA": {"note": "The announcement identifies no oracle relationship."},
                "INTEROPERABILITY": {"note": "The announcement identifies no interoperability relationship."},
                "CUSTODY_COMPLIANCE": {"note": "The announcement identifies no custody relationship."},
            },
        },
    )
    await repo.put(
        "evt.beneficiary_edges",
        "edge",
        {
            "edge_id": "edge",
            "event_id": "evt_1",
            "token_id": "world:WLD",
            "role": "PRIMARY",
            "credible": True,
            "tradable": True,
            "capture_class": "UNKNOWN",
            "exposure_score": None,
            "thesis_link_credible": None,
            "evidence": [],
            "layer": "ISSUER_PRODUCT",
            "causal_chain": "The announced product belongs to the named issuer.",
            "map_version": 1,
        },
    )
    await repo.put(
        "ana.market_truth",
        "truth",
        {
            "mta_id": "truth",
            "event_id": "evt_1",
            "token_id": "world:WLD",
            "cutoff": now,
            "data_confidence": {"class": "LOW", "limiting": None},
            "decisive_facts": [],
            "class_confidence": "LOW",
            "move_class": "UNKNOWN",
            "primary_window": "EVENT",
            "relative": {
                "repricing_band": None,
                "market_context": None,
                "entry_ref_price": None,
                "anchor": {"ref_price": None},
                "windows": {"EVENT": {"r_raw": None, "r_btc_adj": None}},
                "context_inputs": {"return_normal_range": None, "btc_return_24h": None},
            },
            "liquidity": {"window": "EVENT", "start": None, "cutoff": now, "reason": "INCOMPLETE"},
            "spot": {
                "EVENT": {
                    "window": "EVENT",
                    "direction": None,
                    "classification": None,
                    "exact_majority": {"sell": None},
                    "coverage": {"n_complete": None, "excluded": []},
                    "venues": [],
                }
            },
            "derivatives": {
                "window": "W4H",
                "synchronized": None,
                "per_venue": [],
                "windows": {"W4H": {"synchronized": None, "per_venue": []}},
            },
            "supply": {"holders_revenue": {"protocols": []}},
            "divergence": {"state": "UNKNOWN", "unresolved": []},
            "alternatives": [],
        },
    )
    await repo.put(
        "ana.calls",
        "call",
        {
            "call_id": "call",
            "event_id": "evt_1",
            "token_id": "world:WLD",
            "status": "OPEN",
            "opened_at": now,
            "opened_at_prec": "COLLECTION_ONLY",
            "opened_at_prec_reason": None,
            "invalidation_price": "0.9",
        },
    )
    await repo.put(
        "ingest.source_items",
        "denial",
        source_record_fixture(
            {
                "item_id": "denial",
                "text": "Official cancellation",
                "source_id": "S01",
                "source_class": "PRIMARY",
                "observed_at": iso(utcnow() + timedelta(seconds=1)),
            }
        ),
    )
    await seed_session(repo, runtime_status="RUNNING")
    claim = {
        "type": "STATE",
        "subject": "World",
        "predicate": "cancels",
        "object": "Product",
        "state_claim": "CANCELLED",
        "verified": True,
        "directionality": "BOTH",
        "evidence_class": "PRIMARY_FACT",
        "quote": "Official cancellation",
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    ) as http:
        service = SessionService(repo, opts, tools.dispatch, http=http, versions=declared_versions_fixture())

        def original_card(inputs: CardInputs) -> str:
            return "Original product card" + "\n" + score_summary(inputs.assessment.scores)

        monkeypatch.setattr(cards, "compose_card", original_card)
        await service.process_event(
            "ses_1",
            {
                "id": "decision_original",
                "type": "agent.custom_tool_use",
                "name": "get_decision",
                "input": {"token_id": "world:WLD"},
            },
        )
        await service.process_event(
            "ses_1",
            {
                "id": "publish_original",
                "type": "agent.custom_tool_use",
                "name": "publish_card",
                "input": {
                    "kind": "FULL",
                    "what": "Product announcement",
                    "verdict": "Product announcement",
                    "why": ["Official publication", "Tradable beneficiary"],
                },
            },
        )
        original = (await repo.get("ops.tool_calls", "ses_1:publish_original"))["result"]
        assert original["published"]
        original_alert = await repo.get("out.alerts", f"{original['alert_id']}:{original['version']}")
        assert original_alert["shown_tokens"] == ["world:WLD"]
        assert original_alert["decisions"] == [
            (await repo.get("ops.tool_calls", "ses_1:decision_original"))["result"]["dec_id"]
        ]
        stored_session = await repo.get("ops.agent_sessions", "ses_1")
        stored_session["last_payload"] = {"kind": "ITEM_ADDED", "item_id": "denial"}
        await repo.put("ops.agent_sessions", "ses_1", stored_session)
        await service.process_event(
            "ses_1",
            {
                "id": "denial_call",
                "type": "agent.custom_tool_use",
                "name": "add_observation",
                "input": {
                    "link": "DENIAL",
                    "certainty": "HIGH",
                    "certainty_reason": "Official cancellation",
                    "claims": [claim],
                    "public_at": {
                        "value": now,
                        "precision": "EXACT",
                        "basis": "Official denial",
                        "quote": None,
                    },
                    "freshness": "FRESH",
                },
            },
        )
        invalidated = await repo.get("evt.events", "evt_1")
        assert invalidated["lifecycle"] == "INVALIDATED"

        assert not await service.can_archive(await repo.get("ops.agent_sessions", "ses_1"), invalidated)
        await service.process_event(
            "ses_1",
            {
                "id": "decision_exit",
                "type": "agent.custom_tool_use",
                "name": "get_decision",
                "input": {"token_id": "world:WLD"},
            },
        )
        decision = (await repo.get("ops.tool_calls", "ses_1:decision_exit"))["result"]
        assert decision["action"] == "EXIT"

        def exit_card(inputs: CardInputs) -> str:
            return "EXIT: Official cancellation" + "\n" + score_summary(inputs.assessment.scores)

        monkeypatch.setattr(cards, "compose_card", exit_card)
        await service.process_event(
            "ses_1",
            {
                "id": "publish_exit",
                "type": "agent.custom_tool_use",
                "name": "publish_card",
                "input": {
                    "kind": "UPDATE",
                    "what": "Official cancellation",
                    "verdict": "Official cancellation",
                    "why": ["Official denial", "Exit prior call"],
                },
            },
        )
        published = (await repo.get("ops.tool_calls", "ses_1:publish_exit"))["result"]
        assert published["published"]
        alert = await repo.get("out.alerts", f"{published['alert_id']}:{published['version']}")
        assert alert["decisions"] == [decision["dec_id"]]

        invalidated = await repo.get("evt.events", "evt_1")
        invalidated["session_finalized_at"] = clock_iso()
        await repo.put("evt.events", "evt_1", invalidated)
        await service.process_event(
            "ses_1", {"id": "idle", "type": "session.status_idle", "stop_reason": {"type": "end_turn"}}
        )
        assert not await service.can_archive(await repo.get("ops.agent_sessions", "ses_1"), invalidated)
        call = await repo.get("ana.calls", "call")
        call["status"] = "CLOSED"
        await repo.put("ana.calls", "call", call)
        for job in await repo.list("ops.jobs"):
            job["status"] = "DONE"
            await repo.put("ops.jobs", job["job_id"], job)
        await repo.put(
            "out.deliveries",
            "sent",
            {
                "delivery_id": "sent",
                "alert_id": published["alert_id"],
                "version": published["version"],
                "status": "SENT",
            },
            immutable=True,
        )
        armed = await repo.list("ana.watch_conditions", filters={"event_id": "evt_1", "status": "ARMED"})
        assert armed
        assert not await service.can_archive(await repo.get("ops.agent_sessions", "ses_1"), invalidated)
        for condition in armed:
            await repo.put("ana.watch_conditions", condition["cond_id"], {**condition, "status": "EXPIRED"})
        assert await service.can_archive(await repo.get("ops.agent_sessions", "ses_1"), invalidated)


@pytest.mark.asyncio
async def test_unpublished_invalidated_event_can_end_when_idle_without_marker() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    event = {"event_id": "evt_1", "lifecycle": "INVALIDATED", "current_ems_id": "ems"}
    await repo.put("evt.events", "evt_1", event)
    await repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": "evt_1", "class": "LOW"})
    session = await seed_session(repo, runtime_status="RUNNING")
    async with httpx.AsyncClient() as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        assert not await service.can_archive(session, event)
        await service.process_event(
            "ses_1", {"id": "idle", "type": "session.status_idle", "stop_reason": {"type": "end_turn"}}
        )
        session = await repo.get("ops.agent_sessions", "ses_1")
        assert await service.can_archive(session, event)
        await repo.put("ana.calls", "call", {"call_id": "call", "event_id": "evt_1", "status": "OPEN"})
        assert not await service.can_archive(session, event)


@pytest.mark.asyncio
async def test_concurrent_items_without_event_candidates_open_distinct_idempotent_sessions() -> None:
    import asyncio

    repo = MemoryRepository()
    opts, _ = settings()
    for iid in ("first", "second"):
        await repo.put(
            "ingest.source_items",
            iid,
            source_record_fixture({"item_id": iid, "source_id": "S01", "text": "Aave Labs news"}),
        )
    creates, messages = [], []

    def handler(request: httpx.Request):
        assert request.url.host != "opencode.ai"
        if request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        body = json.loads(request.content)
        if request.url.path == "/v1/sessions":
            creates.append(body)
            return httpx.Response(
                200, json=created_native_session(request, f"pending_session_{len(creates)}")
            )
        messages.append(body)
        return httpx.Response(200, json={"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        pipe = Pipeline(
            repo, opts, http=http, progress=DisabledSourceProgress(), versions=declared_versions_fixture()
        )
        first, second = await asyncio.gather(
            pipe.route_item_wire({"item_id": "first"}), pipe.route_item_wire({"item_id": "second"})
        )
        assert await repo.list("evt.events") == []
        assert first["routing_message"]["kind"] == second["routing_message"]["kind"] == "NEW_ITEM"
        assert all(
            "reservation_id" not in item[field]
            for item in (first, second)
            for field in ("route", "routing_message")
        )
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        service.start_stream = lambda sid: None
        one = await service.send_message(first["routing_message"])
        two = await service.send_message(second["routing_message"])
        assert one["session_id"] != two["session_id"]
        repeated = await pipe.route_item_wire({"item_id": "second"})
        await service.send_message(repeated["routing_message"])
    assert len(creates) == len(messages) == 2


@pytest.mark.asyncio
async def test_live_slow_and_recovered_tool_context_keep_session_provenance() -> None:
    repo = MemoryRepository()
    opts, _ = settings(config_version="new_config", code_version="new_code", openma_model="new_model")
    session = await seed_session(
        repo,
        model_id="pinned_model",
        prompt_version="pinned_prompt",
        agent_version="agent_version_7",
        config_version="old_config",
        code_version="old_code",
    )
    contexts = []

    async def dispatch(name, args, context):
        contexts.append(context)
        return {"ok": True}

    def transport(req):
        return httpx.Response(200, json={"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        live = SessionService(
            repo, opts, dispatch, http=http, versions=declared_versions_fixture("new_config", "new_code")
        )
        await live.process_event(
            "ses_1", {"id": "live", "type": "agent.custom_tool_use", "name": "get_item", "input": {}}
        )
        await live.process_event(
            "ses_1",
            {
                "id": "slow",
                "type": "agent.custom_tool_use",
                "name": "get_market_truth",
                "input": native_tool_arguments_fixture("get_market_truth"),
            },
        )
        await repo.put(
            "ops.tool_calls",
            "ses_1:recover",
            {
                "call_id": "recover",
                "session_id": "ses_1",
                "tool": "get_item",
                "input": {},
                "arguments": {},
                "status": "PENDING",
            },
        )
        recovered = SessionService(
            repo, opts, dispatch, http=http, versions=declared_versions_fixture("new_config", "new_code")
        )
        await recovered.process_event(
            "ses_1", {"id": "recover", "type": "agent.custom_tool_use", "name": "get_item", "input": {}}
        )
    assert len(contexts) == 3
    for context in contexts:
        assert {key: context[key] for key in ("model_id", "prompt_version", "agent_version")} == {
            key: session[key] for key in ("model_id", "prompt_version", "agent_version")
        }
        assert context["config_version"] == "new_config"
        assert context["code_version"] == "new_code"
        assert context["session_config_version"] == "old_config"
        assert context["session_code_version"] == "old_code"


@pytest.mark.asyncio
async def test_market_truth_wire_omits_internal_metadata_full_result_survives_recovery() -> None:
    from tank.analysis.truth import compute_market_truth
    from tank.util import canonical_json

    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo)
    truth = compute_market_truth({"token_id": "world:WLD", "cutoff": "2026-10-16T12:00:00Z", "flow_bars": []})
    truth.update(mta_id="MTA-wire", event_id="evt_1")
    truth["evidence"] = {"measurement_metadata": {"canonical_source_key": "must remain"}}
    full = {"market_truth": [truth]}
    await repo.put("ana.market_truth", "MTA-wire", truth, immutable=True)
    dispatched = 0
    requests = []

    async def dispatch(name, args, context):
        nonlocal dispatched
        dispatched += 1
        return full

    def lost(request: httpx.Request) -> None:
        requests.append(json.loads(request.content))
        raise httpx.ReadError("lost response", request=request)

    event = {
        "id": "mt_call",
        "type": "agent.custom_tool_use",
        "name": "get_market_truth",
        "input": native_tool_arguments_fixture("get_market_truth"),
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(lost)) as http:
        first = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        with pytest.raises(httpx.ReadError):
            await first.process_event("ses_1", event, 7)
    stored = await repo.get("ops.tool_calls", "ses_1:mt_call")
    assert stored["result"] == full and "measurement_metadata" in stored["result"]["market_truth"][0]

    def accepted(request: httpx.Request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as http:
        recovered = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        await recovered.process_event("ses_1", event, 7)
    assert dispatched == 1 and requests[0] == requests[1]
    wire = json.loads(requests[1]["events"][0]["content"][0]["text"])
    assert wire["market_truth"][0]["cutoff"] == truth["cutoff"]
    assert wire["market_truth"][0]["spot"] == truth["spot"]
    assert "measurement_metadata_access" not in wire
    assert "mta_id" not in canonical_json(wire)
    assert "session_id" not in canonical_json(wire)
    assert "measurement_metadata" not in wire["market_truth"][0]
    assert "measurement_metadata" not in wire["market_truth"][0]["evidence"]
    assert len(canonical_json(wire)) < len(canonical_json(full))
    assert (await repo.get("ana.market_truth", "MTA-wire")) == truth
    assert (await repo.get("ops.tool_calls", "ses_1:mt_call"))["result"] == full


@pytest.mark.asyncio
async def test_routing_and_resume_find_late_history_with_one_row_pages() -> None:
    import asyncio

    class TinyPages(MemoryRepository):
        async def list(self, collection, **kwargs):
            if collection in {"evt.events", "ops.agent_sessions"}:
                raise AssertionError("Permanent history must use scoped paged reads")
            return await super().list(collection, **kwargs)

        def iter_all(self, collection, **kwargs):
            kwargs["batch_size"] = 1
            return super().iter_all(collection, **kwargs)

        def iter_range(self, collection, time_field, **kwargs):
            kwargs["batch_size"] = 1
            return super().iter_range(collection, time_field, **kwargs)

    repo = TinyPages()
    opts, _ = settings()
    for index in range(5):
        eid = f"old_{index}"
        await repo.put(
            "evt.events",
            eid,
            {
                "lifecycle": "ANALYZING",
                "tokens_mentioned": [],
                "event_state": "RUMOR",
                "event_id": eid,
                "statement": "old",
                "entities": [{"entity_id": "aave", "role": "SUBJECT"}],
            },
        )
        await repo.put(
            "ops.agent_sessions",
            eid,
            {
                "session_id": eid,
                "event_id": eid,
                "status": "ENDED",
                "last_item_at": iso(utcnow() - timedelta(days=10)),
            },
        )
    await repo.put(
        "evt.events",
        "zz_event",
        {
            "event_state": "RUMOR",
            "event_id": "zz_event",
            "statement": "late event",
            "entities": [{"entity_id": "aave", "role": "SUBJECT"}],
            "tokens_mentioned": [],
            "lifecycle": "TRACKED",
        },
    )
    await repo.put(
        "ops.agent_sessions",
        "zz_session",
        {"session_id": "zz_session", "event_id": "zz_event", "status": "OPEN", "last_item_at": clock_iso()},
    )
    await repo.put(
        "ingest.source_items",
        "pending_item",
        source_record_fixture({"item_id": "pending_item", "text": "Aave pending candidate"}),
    )
    await repo.put(
        "ingest.source_items",
        "incoming",
        source_record_fixture({"item_id": "incoming", "text": "Aave confirms late event"}),
    )
    seen = []

    def handler(req):
        body = json.loads(req.content)
        seen.append(body["state"])
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "answers": {
                    "same_event": {"type": "noul", "noul": 0.99 if "event" in body["state"] else 0.85}
                },
                "usage": {"input_tokens": 10},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await Pipeline(
            repo, opts, http=http, progress=DisabledSourceProgress(), versions=declared_versions_fixture()
        ).route_item_wire({"item_id": "incoming"})
        assert result["route_event_id"] == "zz_event"
        assert len(seen) == 1
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        assert (
            await service.resolve(
                NativeSessionResolution.from_wire({"event_id": "zz_event"}, role="ANALYST", replay=False)
            )
        )["session_id"] == "zz_session"
        stop = asyncio.Event()
        resumed = []

        def resume(sid) -> None:
            resumed.append(sid)
            stop.set()

        service.start_stream = resume
        await service.run(stop)
        assert resumed == ["zz_session"]


@pytest.mark.asyncio
async def test_session_acceptance_and_tools_keep_clock_and_source_precision_separate() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    await repo.put(
        "ingest.source_items",
        "item",
        source_record_fixture(
            {
                "item_id": "item",
                "text": "Captured source",
                "observed_at": "2026-10-01T00:00:00Z",
                "observed_at_prec": "COLLECTION_ONLY",
            }
        ),
    )

    def transport(request: httpx.Request):
        if request.url.path.endswith("/sessions"):
            return (
                httpx.Response(200, json={"data": [], "next_page": None})
                if request.method == "GET"
                else httpx.Response(201, json=created_native_session(request, "fresh"))
            )
        return httpx.Response(200, json={})

    async def dispatch(*args):
        return {"ok": True}

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        service.start_stream = lambda sid: None
        session = await service.send_message({"kind": "NEW_ITEM", "item_id": "item"})
        await service.process_event(
            "fresh",
            {
                "id": "call",
                "type": "agent.custom_tool_use",
                "name": "get_item",
                "input": {"item_id": "item"},
                "processed_at": "2026-10-02T10:00:00Z",
                "processed_at_prec": "BUCKET(1s)",
            },
        )
        await service.close()
    session = await repo.get("ops.agent_sessions", "fresh")
    assert (
        session["opened_at_prec"]
        == session["created_at_prec"]
        == session["last_item_at_prec"]
        == "COLLECTION_ONLY"
    )
    assert session["last_item_observed_at"] == "2026-10-01T00:00:00Z"
    assert session["last_item_observed_at_prec"] == "COLLECTION_ONLY"
    message = session["message_log"][0]
    assert message["sent_at_prec"] == "COLLECTION_ONLY"
    assert message["item_observed_at_prec"] == "COLLECTION_ONLY"
    assert session["last_event_at_prec"] == "BUCKET(1s)"
    tool = await repo.get("ops.tool_calls", "fresh:call")
    for field in ("requested_at", "answered_at", "completed_at", "returned_at"):
        assert tool[field + "_prec"] == "COLLECTION_ONLY"
    assert tool["processed_at_prec"] == "BUCKET(1s)"
    assert tool["answered_at"] == tool["completed_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("precision", [None, "BUCKET(1s)"])
async def test_usage_propagates_provider_processed_precision_without_inference(precision) -> None:
    repo, (opts, _) = MemoryRepository(), settings()
    session = await seed_session(repo)
    event = {
        "id": "usage",
        "processed_at": "2026-10-02T10:00:00.123456Z",
        "model_usage": {"input_tokens": 1, "output_tokens": 1},
    }
    if precision:
        event["processed_at_prec"] = precision
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        await service.usage(session, event)
    row = await repo.get("ops.llm_usage", "ses_1:usage")
    assert row["at"] == event["processed_at"]
    assert row["at_prec"] == row["processed_at_prec"] == precision
    assert row["at_prec_reason"] == (None if precision else "INCOMPLETE")
    assert row["created_at_prec"] == "COLLECTION_ONLY"


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["fetch_page", "get_market_truth"])
@pytest.mark.parametrize("failure_kind", ["http", "type"])
async def test_tool_execution_error_is_durable_returned_and_never_redispatched_after_lost_reply(
    tool_name, failure_kind
) -> None:
    repo, (opts, _) = MemoryRepository(), settings()
    await seed_session(repo)
    invocations, responses = 0, []

    async def dispatch(*args) -> None:
        nonlocal invocations
        invocations += 1
        if failure_kind == "type":
            raise TypeError("FAKE_CREDENTIAL_IN_EXCEPTION must never enter ledger")
        request = httpx.Request("GET", "https://source.invalid/?apikey=FAKE_CREDENTIAL_IN_EXCEPTION")
        raise httpx.HTTPStatusError(
            "FAKE_CREDENTIAL_IN_EXCEPTION", request=request, response=httpx.Response(500, request=request)
        )

    def control(request: httpx.Request):
        responses.append(json.loads(request.content)["events"][0])
        if len(responses) == 1:
            raise httpx.ConnectError("Lost tool response acknowledgement", request=request)
        return httpx.Response(200, json={})

    event = {
        "id": "failure",
        "type": "agent.custom_tool_use",
        "name": tool_name,
        "input": native_tool_arguments_fixture(tool_name),
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(control)) as http:
        first = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        with pytest.raises(httpx.ConnectError):
            await first.process_event("ses_1", event)
        failed = await repo.get("ops.tool_calls", "ses_1:failure")
        assert failed["status"] == "ERROR" and not failed.get("returned_at")
        assert "failure" not in (await repo.get("ops.agent_sessions", "ses_1")).get("processed_events", [])
        resumed = SessionService(repo, opts, dispatch, http=http, versions=declared_versions_fixture())
        await resumed.process_event("ses_1", event)
        await resumed.process_event("ses_1", event)
    stored = await repo.get("ops.tool_calls", "ses_1:failure")
    assert invocations == 1
    assert stored["status"] == "ERROR" and stored["returned_at"]
    assert responses[0] == responses[1] and responses[1]["is_error"] is True
    assert "FAKE_CREDENTIAL_IN_EXCEPTION" not in json.dumps(stored)


@pytest.mark.asyncio
async def test_new_session_prompt_version_binds_actual_resolved_agent_not_static_label() -> None:
    import copy

    repo, (opts, config) = MemoryRepository(), settings()
    snapshot = {
        "id": "agt_1",
        "version": 7,
        "system": "Actual registered instructions",
        "skills": [{"type": "custom", "skill_id": "skill", "version": "1"}],
        "tools": [],
        "model": {"id": "gpt-6.1-sol"},
    }
    created = 0

    def control(request: httpx.Request):
        nonlocal created
        if request.url.path.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            created += 1
            return httpx.Response(
                201,
                json=created_native_session(
                    request, "fresh-" + str(created), agent_snapshot=copy.deepcopy(snapshot)
                ),
            )
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(control)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        service.start_stream = lambda sid: None
        for item_id in ("one", "two"):
            await repo.put(
                "ingest.source_items",
                item_id,
                source_record_fixture({"item_id": item_id, "text": "Synthetic item"}),
            )
        first = await service.send_message({"kind": "NEW_ITEM", "item_id": "one"})
        snapshot["system"] = "Changed actual registered instructions"
        second = await service.send_message({"kind": "NEW_ITEM", "item_id": "two"})
    assert first["agent_version"] == second["agent_version"] == "7"
    assert first["prompt_label"] == second["prompt_label"] == config["openma"]["prompt_version"]
    assert first["prompt_version"] != second["prompt_version"]
    assert first["prompt_version_basis"] == "RESOLVED_SESSION_AGENT_SNAPSHOT"
    assert first["prompt_binding"]["resolved_skill_versions"] == [
        {"type": "custom", "skill_id": "skill", "version": "1"}
    ]
    incomplete = service.agent_provenance({"agent": {"id": "agt_1", "version": 8}}, config["openma"], "agt_1")
    assert incomplete.prompt_version is None
    assert incomplete.prompt_label == "test" and "INCOMPLETE" in incomplete.prompt_version_reason


def test_jev_source_view_does_not_substitute_internal_source_codes() -> None:
    view = Pipeline.item_view(
        parse_feed_source_item(
            source_record_fixture(
                {
                    "item_id": "internal",
                    "source_id": "S29",
                    "title": "Protocol launch",
                    "text": "New crypto protocol launches",
                }
            )
        )
    )
    assert view.source == "Synthetic Publisher"
    assert "S29" not in json.dumps(view.to_wire()) and "item_id" not in view.to_wire()


@pytest.mark.parametrize(
    "defect", ["empty_verdict", "blank_verdict", "empty_why", "single_why", "blank_bullet"]
)
async def test_publish_card_requires_declared_verdict_and_two_factual_bullets(
    defect: Literal["empty_verdict", "blank_verdict", "empty_why", "single_why", "blank_bullet"],
) -> None:

    from tank.contracts.json import decode_json

    repo = MemoryRepository()
    await seed_session(repo)
    opts, _ = settings()
    arguments: dict[str, JsonValue] = {
        "kind": "PARTIAL",
        "what": "A synthetic product shipped.",
        "verdict": "Synthetic launch confirmed.",
        "why": ["The release is published.", "The feature is available."],
    }
    if defect == "empty_verdict":
        arguments["verdict"] = ""
    elif defect == "blank_verdict":
        arguments["verdict"] = "   "
    elif defect == "empty_why":
        arguments["why"] = []
    elif defect == "single_why":
        arguments["why"] = ["One fact."]
    else:
        arguments["why"] = ["First fact.", "   "]
    dispatched: list[str] = []
    responses: list[JsonValue] = []

    async def dispatch(tool: str, payload: dict[str, JsonValue], context: dict[str, JsonValue]) -> JsonValue:
        dispatched.append(tool)
        return {"published": True}

    def native(request: httpx.Request) -> httpx.Response:
        responses.append(decode_json(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as client:
        service = SessionService(
            repo, opts, dispatch, http=client, stream_http=client, versions=declared_versions_fixture()
        )
        await service.process_event(
            "ses_1",
            {
                "id": "publish-structure",
                "type": "agent.custom_tool_use",
                "name": "publish_card",
                "input": arguments,
            },
        )
    capture = await repo.get("ops.tool_calls", "ses_1:publish-structure")
    assert capture is not None and capture["status"] == "REFUSED"
    assert capture["is_error"] is True and capture["returned_at"]
    assert dispatched == [] and len(responses) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("effort", ["high", None])
async def test_new_native_session_records_observed_effort_without_relabeling_intent(
    effort: str | None,
) -> None:
    repo = MemoryRepository()
    opts, _ = settings()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        snapshot: dict[str, JsonValue] = {"model": {"id": "gpt-6.1-sol"}}
        model = snapshot["model"]
        assert isinstance(model, dict)
        if effort is not None:
            model["effort"] = {"type": effort}
        return httpx.Response(201, json=created_native_session(request, "observed", agent_snapshot=snapshot))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        result = await service.resolve(
            NativeSessionResolution.from_wire({"item_id": "item"}, role="ANALYST", replay=False)
        )
    assert result["configured_effort"] == "medium"
    assert result["effort"] == effort
    assert result["effort_basis"] == ("RESOLVED_SESSION_AGENT" if effort is not None else None)
    assert result["effort_reason"] == (
        "Native resolved agent did not report reasoning effort" if effort is None else None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["agent", "environment", "identity", "replay"])
async def test_native_creation_binding_failure_never_commits_a_local_session(defect: str) -> None:
    repo = MemoryRepository()
    opts, configuration = settings()
    payload: dict[str, JsonValue] = {"kind": "NEW_ITEM", "item_id": "input"}
    if defect == "replay":
        opts.replay = True
        configuration["openma"]["replay_environment_id"] = "replay-environment"
        payload.update(
            replay_run_id="run",
            replay_source_session_id="original",
            replay_evidence_sha256="1" * 64,
            replay_cutoff="2026-10-01T00:00:00Z",
        )
    item = source_record_fixture({"item_id": "input", "text": "Declared synthetic source"})
    await repo.put("ingest.source_items", "input", item)
    calls: list[tuple[str, str]] = []
    private_value = "synthetic-private-provider-metadata"

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
        assert request.url.path == "/v1/sessions"
        wire = created_native_session(request, "unconfirmed")
        metadata = wire["metadata"]
        assert isinstance(metadata, dict)
        metadata["provider_private"] = private_value
        if defect == "agent":
            agent = wire["agent"]
            assert isinstance(agent, dict)
            agent["id"] = "different-agent"
        elif defect == "environment":
            wire["environment_id"] = "different-environment"
        elif defect == "identity":
            metadata["tank_identity"] = "different-identity"
        else:
            del metadata["tank_replay_evidence_sha256"]
        return httpx.Response(201, json=wire)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        with pytest.raises(ValueError) as caught:
            await service.send_message(payload)
        await service.close()
    assert private_value not in str(caught.value)
    assert calls == [("GET", "/v1/sessions"), ("POST", "/v1/sessions")]
    assert await repo.list("ops.agent_sessions") == []
    assert await repo.list("ops.tool_calls") == []
    assert sha256(await repo.get("ingest.source_items", "input")) == sha256(item)


@pytest.mark.asyncio
async def test_live_native_creation_cannot_borrow_a_replay_only_opening_source() -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    original: dict[str, JsonValue] = {
        "session_id": "replay-only",
        "event_id": "event",
        "item_id": "replayed-item",
        "opened_by_item": "replayed-item",
        "replay_run_id": "previous-run",
        "status": "OPEN",
    }
    await repo.put("ops.agent_sessions", "replay-only", original)
    calls: list[str] = []

    def unexpected(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise AssertionError("An undeclared replay source cannot initiate native creation")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        with pytest.raises(ValueError, match="durable opening source"):
            await service.resolve(
                NativeSessionResolution.from_wire({"event_id": "event"}, role="ANALYST", replay=False)
            )
        await service.close()
    assert not calls
    assert sha256(await repo.get("ops.agent_sessions", "replay-only")) == sha256(original)


@pytest.mark.asyncio
@pytest.mark.parametrize("creation_role", ["ANALYST", "SCOUT"])
@pytest.mark.parametrize("actual_scout", [False, True])
async def test_explicit_native_continuation_preserves_the_actual_stored_context(
    creation_role: Literal["ANALYST", "SCOUT"], actual_scout: bool
) -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    original: dict[str, JsonValue] = {
        "session_id": "existing",
        "event_id": None if actual_scout else "actual-event",
        "item_id": None if actual_scout else "actual-item",
        "proposal_id": "actual-proposal" if actual_scout else None,
        "status": "OPEN",
        "opaque": {"integer": 1, "float": 1.0, "flag": True},
    }
    await repo.put("ops.agent_sessions", "existing", original)
    calls: list[str] = []

    def unexpected(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise AssertionError("Existing native session lookup must not create a fresh resource")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        resolved = await service.resolve(
            NativeSessionResolution.from_wire({"session_id": "existing"}, role=creation_role, replay=False)
        )
        await service.close()
    assert not calls
    assert sha256(resolved) == sha256(original)
    assert sha256(await repo.get("ops.agent_sessions", "existing")) == sha256(original)


@pytest.mark.asyncio
@pytest.mark.parametrize("creation_role", ["ANALYST", "SCOUT"])
async def test_explicit_missing_native_session_never_falls_back_to_creation(
    creation_role: Literal["ANALYST", "SCOUT"],
) -> None:
    repo = MemoryRepository()
    opts, _ = settings()
    calls: list[str] = []

    def unexpected(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise AssertionError("An explicit missing session must not become a fresh role")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        with pytest.raises(KeyError, match="missing"):
            await service.resolve(
                NativeSessionResolution.from_wire({"session_id": "missing"}, role=creation_role, replay=False)
            )
        await service.close()
    assert not calls
    assert await repo.list("ops.agent_sessions") == []


@pytest.mark.asyncio
async def test_missing_native_followup_records_absence_without_termination_or_input() -> None:
    from tank.telegram_inbox import GONE_TEXT

    repo = MemoryRepository()
    opts, _ = settings()
    await seed_session(repo)
    await repo.put(
        "out.user_questions",
        "question",
        {
            "question_id": "question",
            "session_id": "ses_1",
            "status": "PENDING",
            "answer_status": None,
            "text": "Can the analysis continue?",
        },
    )
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        return httpx.Response(404, json={"error": "synthetic-private-provider-message"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, opts, None, http=http, versions=declared_versions_fixture())
        result = await service.send_message(
            {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": "question"}
        )
        await service.close()
    assert result is None
    assert calls == [("GET", "/v1/sessions/ses_1")]
    stored = await repo.get("ops.agent_sessions", "ses_1")
    assert stored is not None
    remote = stored["remote_state"]
    assert isinstance(remote, dict)
    assert remote["kind"] == "MISSING" and remote["missing"] is True
    assert "status" not in remote and "termination_receipt" not in stored
    assert remote["checked_at_prec"] == "COLLECTION_ONLY"
    answer = await repo.get("out.user_questions", "question")
    assert answer is not None
    assert answer["answer_status"] == "NO_SESSION" and answer["reason_text"] == GONE_TEXT
    assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1

"""Scout isolation on real PostgreSQL (03 §10, 01 §6.9).

The analyst's propose_pattern creates a proposal and a lowest-priority SCOUT_REVIEW
job; the scout session writes only expansion_proposals and its own session logs, and
nothing the operator is told changes.
"""

import json
import os
import uuid
from pathlib import Path

import httpx
import pytest

from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.util import iso, sha256

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

SCHEMAS = ("reg", "ingest", "evt", "mkt", "ana", "out", "ops", "scout", "qa")
OWN_LOGS = {"ops.agent_sessions", "ops.tool_calls", "ops.llm_usage"}
OPERATOR_FACING = ("out.alerts", "out.deliveries", "out.ops_notices", "out.evidence_bundles", "ana.decisions")


@pytest.fixture
async def repo(postgres_template):
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Disposable administrator PostgreSQL DSN required")
    async with postgres_template.repository(url) as repository:
        yield repository, repository.database_url


async def snapshot(dsn):
    """Row count and content hash of every Tank table."""
    import psycopg

    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        tables = await (
            await conn.execute(
                "SELECT table_schema || '.' || table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' AND table_schema = ANY(%s) ORDER BY 1",
                (list(SCHEMAS),),
            )
        ).fetchall()
        state = {}
        for (name,) in tables:
            schema, table = name.split(".")
            row = await (
                await conn.execute(
                    f"SELECT count(*), md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) "
                    f'FROM "{schema}"."{table}" t'
                )
            ).fetchone()
            state[name] = tuple(row)
        return state


def native_session(session_id):
    return {
        "id": session_id,
        "agent": {
            "id": "native-scout-fixture",
            "version": 1,
            "system": "Resolved synthetic instructions",
            "skills": [],
            "tools": [],
            "model": {"id": "gpt-6.1-sol"},
        },
    }


class FakeOpenMA:
    def __init__(self) -> None:
        self.posted = []

    def __call__(self, request: pytest.FixtureRequest):
        if request.method == "GET" and request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"data": [], "next_page": None})
        if request.method == "GET":
            return httpx.Response(200, json=native_session(request.url.path.split("/")[3]))
        body = json.loads(request.content)
        self.posted.append((request.url.path, body))
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"id": "ses_scout_" + uuid.uuid4().hex[:8]})
        return httpx.Response(200, json={"data": [{"id": "in_" + uuid.uuid4().hex[:8]}]})


def scout_settings(tmp_path: Path):
    settings = Settings(data_dir=tmp_path, openma_url="http://openma:8787", openma_api_key="test")
    config = settings.load_config

    def load_config(name):
        if name == "openma":
            return {
                "agent_id": "agt_analyst",
                "scout_agent_id": "agt_scout",
                "environment_id": "env_analysis",
                "prompt_version": "test",
            }
        return config(name)

    object.__setattr__(settings, "load_config", load_config)
    return settings


async def seed_event(tools, sessions):
    at = iso()
    await tools.repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "source_id": "S01",
            "feed": "fixture",
            "external_id": "i",
            "text": "Product announced",
            "source_class": "PRIMARY",
            "observed_at": at,
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": at,
            "published_prec": "EXACT",
            "raw_ref": "/fixture/raw",
            "raw_sha256": sha256("Product announced"),
            "content_hash": sha256("Product announced"),
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        },
    )
    analyst = {
        "session_id": "ses_analyst",
        "model_context_version": CONTEXT_VERSION,
        "event_id": None,
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "opened_by_item": "i",
        "opened_at": at,
        "opened_at_prec": "COLLECTION_ONLY",
        "last_item_at": at,
        "last_item_at_prec": "COLLECTION_ONLY",
        "last_event_seq": 0,
        "agent_version": "1",
        "prompt_version": "a" * 64,
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "model_id": "gpt-6.1-sol",
        "active_message_id": "m",
        "message_log": [
            {
                "message_id": "m",
                "remote_event_id": "input",
                "retry_count": 0,
                "payload": {"kind": "NEW_ITEM", "item_id": "i"},
            }
        ],
    }
    await tools.repo.put("ops.agent_sessions", "ses_analyst", analyst)
    extraction = {
        "item_id": "i",
        "is_event": True,
        "statement": "Product announced",
        "certainty": "HIGH",
        "certainty_reason": "Official primary",
        "entities": [],
        "token_ids": [],
        "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
        "public_at": {"value": at, "precision": "EXACT", "basis": "Primary publication", "quote": None},
        "freshness": "FRESH",
        "claims": [
            {
                "verified": True,
                "type": "STATE",
                "subject": "World",
                "predicate": "announces",
                "object": "Product",
                "state_claim": "LIVE",
                "directionality": "BOTH",
                "evidence_class": "PRIMARY_FACT",
                "quote": "Product announced",
            }
        ],
        "next_milestone": None,
        "checked_sources": [],
    }
    extraction.pop("item_id")
    await sessions.tool(analyst, {"id": "call_record", "name": "record_event", "input": extraction})
    recorded = await tools.repo.get("ops.tool_calls", "ses_analyst:call_record")
    assert recorded["status"] == "DONE", recorded["result"]
    return await tools.repo.get("ops.agent_sessions", "ses_analyst"), recorded["result"]


@pytest.mark.asyncio
async def test_scout_writes_only_proposals_and_its_logs_on_postgres(repo, tmp_path: Path) -> None:
    repository, dsn = repo
    settings = scout_settings(tmp_path)
    tools = EventToolService(repository, settings, None, None)
    openma = FakeOpenMA()
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma)) as http:
        sessions = SessionService(repository, settings, tools.dispatch, http=http)
        sessions.start_stream = lambda sid: None
        analyst, recorded = await seed_event(tools, sessions)
        eid = analyst["event_id"]
        assert eid == recorded["event_id"]
        claim = recorded["accepted_claims"][0]["claim_id"]

        await sessions.tool(
            analyst,
            {
                "id": "call_propose",
                "name": "propose_pattern",
                "input": {
                    "hook": "MARKET_TRUTH_STATE",
                    "subject": "Venue-led drift",
                    "evidence": [{"claim_id": claim}],
                },
            },
        )
        proposed = await repository.get("ops.tool_calls", "ses_analyst:call_propose")
        assert proposed["status"] == "DONE", proposed["result"]
        proposals = await repository.list("scout.expansion_proposals")
        assert len(proposals) == 1 and proposals[0]["hook"] == "MARKET_TRUTH_STATE"
        prp_id = proposals[0]["prp_id"]
        jobs = await repository.list("ops.jobs", filters={"type": "SCOUT_REVIEW"})
        assert [(job["priority"], job["payload"]) for job in jobs] == [(4, {"prp_id": prp_id})]
        assert max(job["priority"] for job in await repository.list("ops.jobs")) == 4

        openma.posted.clear()
        scout = await sessions.scout_review(jobs[0]["payload"])
        assert scout["proposal_id"] == prp_id
        create = openma.posted[0]
        assert create[0] == "/v1/sessions" and create[1]["agent"] == "agt_scout"
        message = json.loads(openma.posted[1][1]["events"][0]["content"][0]["text"])
        assert message["proposal"]["concept_name"] == "Venue-led drift"
        assert [row["concept_name"] for row in message["open_proposals"]] == ["Venue-led drift"]

        before = await snapshot(dsn)
        attempts = [
            ("publish_card", {"event_id": eid, "kind": "FULL", "what": "x", "verdict": "x", "why": ["y"]}),
            ("set_assessment", {"event_id": eid, "assessment": {}}),
            ("record_event", {"item_id": "i"}),
            ("get_decision", {"event_id": eid, "token_id": "world:WLD"}),
            (
                "propose_pattern",
                {"hook": "MARKET_TRUTH_STATE", "subject": "x", "evidence": [{"claim_id": claim}]},
            ),
            (
                "record_review",
                {
                    "verdict": "Already covered by existing move classes",
                    "reason": "No decision-relevant difference shown",
                    "evidence": [{"claim_id": claim}],
                    "scores": {"overlap": 8, "observability": 5, "reusability": 3, "decision_value": 2},
                    "recommendation": "REJECT",
                    "duplicate_of": None,
                },
            ),
        ]
        for number, (name, arguments) in enumerate(attempts):
            await sessions.process_event(
                scout["session_id"],
                {
                    "id": f"scout_call_{number}",
                    "type": "agent.custom_tool_use",
                    "name": name,
                    "input": arguments,
                },
            )
        after = await snapshot(dsn)

    calls = {
        row["tool"]: row
        for row in await repository.list("ops.tool_calls", filters={"session_id": scout["session_id"]})
    }
    for name in ("publish_card", "set_assessment", "record_event", "get_decision", "propose_pattern"):
        assert calls[name]["status"] == "REFUSED", name
        assert calls[name]["returned_at"]
    assert calls["record_review"]["status"] == "DONE", calls["record_review"]["result"]
    reviewed = await repository.get("scout.expansion_proposals", prp_id)
    assert reviewed["recommendation"] == "REJECT"
    assert reviewed["adversarial_review"]["scores"]["overlap"] == 8
    assert not reviewed.get("human_action")

    changed = {name for name in before if before[name] != after[name]}
    assert changed <= {"scout.expansion_proposals"} | OWN_LOGS, changed
    assert "scout.expansion_proposals" in changed
    for name in OPERATOR_FACING:
        assert before[name] == after[name]

    assert (await repository.get("ops.agent_sessions", "ses_analyst"))["session_id"] == "ses_analyst"
    assert all(
        row["session_id"] == scout["session_id"]
        for row in await repository.list("ops.tool_calls")
        if row["call_id"].startswith("scout_call_")
    )

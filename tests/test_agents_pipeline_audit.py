import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from tank.agents.pipeline import FILTER, ROUTE, PendingRouting, Pipeline
from tank.storage import MemoryRepository
from tank.util import iso


def options():
    config = {
        "registry": yaml.safe_load(Path("config/registry.yaml").read_text()),
        "thresholds": {"filter_cutoff": 0.7, "routing_cutoff": 0.8},
    }
    return SimpleNamespace(
        load_config=config.__getitem__, typesafe_api_key="test", config_version="test", code_version="test"
    ), config


def test_audit_2_10_11_20_exact_filter_title_outlet():
    design = Path("03_Tank_V0_Technical_Design.md").read_text()
    assert f'"instructions": "{FILTER}"' in design
    state = Pipeline.state({"source_name": "Outlet", "title": "Aave approved", "body": "Details"})
    assert state == {
        "source": "Outlet",
        "author": "Outlet",
        "text": "Aave approved\n\nDetails",
        "quoted": None,
    }


@pytest.mark.parametrize("text", ["near the world curve quant cow lit plasma", "lighter", "linear unrelated"])
def test_audit_2_12_generic_words_do_not_resolve(text):
    opts, _ = options()
    assert not Pipeline(MemoryRepository(), opts, http=object()).identities({"text": text})


@pytest.mark.parametrize(
    "text,entity,token",
    [
        ("$NEAR", "near", "near:NEAR"),
        ("Lighter.xyz", "lighter", "lighter:LIT"),
        ("Lighter announced LIT", "lighter", "lighter:LIT"),
        ("Plasma chain", "plasma", "plasma:XPL"),
        ("World ID", "world", "world:WLD"),
        ("CoW Swap", "cow", "cow:COW"),
    ],
)
def test_audit_2_12_canonical_aliases(text, entity, token):
    opts, _ = options()
    ids = Pipeline(MemoryRepository(), opts, http=object()).identities({"title": text})
    assert {entity, token} <= ids


@pytest.mark.asyncio
@pytest.mark.parametrize("same,decision", [(0.95, "ADDED"), (0.1, "NEW_SESSION")])
async def test_audit_2_13_pending_extraction_defers_then_uses_event_question(same, decision):
    opts, config = options()
    repo = MemoryRepository()
    for key in ("first", "next"):
        await repo.put("ingest.source_items", key, {"item_id": key, "title": "Aave product approved"})
    calls = []

    def handler(request):
        body = json.loads(request.content)
        calls.append(body)
        assert body["questions"]["same_event"]["instructions"] == ROUTE
        assert "event" in body["state"] and "pending_item" not in body["state"]
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "usage": {},
                "answers": {"same_event": {"type": "noul", "noul": same}},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        pipe = Pipeline(repo, opts, http=http)
        config["thresholds"]["routing_cutoff"] = None
        first = await pipe.route_item({"item_id": "first"})
        config["thresholds"]["routing_cutoff"] = 0.8
        with pytest.raises(PendingRouting) as blocked:
            await pipe.route_item({"item_id": "next"})
        assert blocked.value.run_after and not calls
        rid = first["route"]["reservation_id"]
        reservation = await repo.get("ops.routing_reservations", rid)
        reservation.update(status="BOUND", event_id="evt")
        await repo.put("ops.routing_reservations", rid, reservation)
        await repo.put(
            "ops.agent_sessions", "ses", {"session_id": "ses", "event_id": "evt", "last_item_at": iso()}
        )
        await repo.put(
            "evt.events",
            "evt",
            {"event_id": "evt", "entities": ["aave"], "statement": "Aave product approved"},
        )
        result = await pipe.route_item({"item_id": "next"})
        assert result["route"]["decision"] == decision
        assert result["route"]["event_id"] == ("evt" if decision == "ADDED" else None)
        await pipe.route_item({"item_id": "next"})
        assert len(calls) == 1


@pytest.mark.parametrize("status", ["ENDED", "FAILED"])
@pytest.mark.asyncio
async def test_terminal_pending_session_releases_identity_barrier(status):
    opts, _ = options()
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "i", {"item_id": "i", "title": "Aave other event"})
    await repo.put(
        "ops.routing_reservations",
        "old",
        {
            "reservation_id": "old",
            "status": "PENDING",
            "last_item_at": iso(),
            "entities": ["aave"],
            "session_id": "ses",
        },
    )
    await repo.put("ops.agent_sessions", "ses", {"session_id": "ses", "status": status})
    result = await Pipeline(repo, opts, http=object()).route_item({"item_id": "i"})
    assert result["route"]["decision"] == "NEW_SESSION"
    assert (await repo.get("ops.routing_reservations", "old"))["status"] == "CLOSED"


def test_explicit_token_identity_expands_to_its_entity():
    opts, _ = options()
    assert {"aave", "aave:AAVE"} <= Pipeline(MemoryRepository(), opts, http=object()).identities(
        {"tokens": ["aave:AAVE"]}
    )


@pytest.mark.asyncio
async def test_official_repost_promotion_during_filter_request_preserves_latest_item():
    opts, _ = options()
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "source_id": "S29",
            "text": "News",
            "author": "original",
            "author_role": "OTHER",
            "deferrable": True,
            "relevance": {"decision": "PENDING"},
        },
    )

    async def handler(request):
        # Taking this same lock proves the model request does not block ingestion.
        async with repo.lock("ingest-dedup"):
            item = await repo.get("ingest.source_items", "i")
            item.update(
                monitored_author="Aave",
                reposted_by="Aave",
                author_role="ORGANIZATION",
                deferrable=False,
                relevance={"decision": "ALWAYS"},
                status="NEW",
            )
            await repo.put("ingest.source_items", "i", item)
        return httpx.Response(
            200,
            json={"model": "jev-1.13.0", "usage": {}, "answers": {"relevant": {"type": "noul", "noul": 0.1}}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await Pipeline(repo, opts, http=http).filter_item({"item_id": "i"})
    stored = await repo.get("ingest.source_items", "i")
    assert result == stored
    assert stored["relevance"] == {"decision": "ALWAYS"}
    assert stored["status"] == "NEW" and "extraction_status" not in stored
    assert stored["author"] == "original" and stored["monitored_author"] == "Aave"
    assert stored["author_role"] == "ORGANIZATION" and not stored["deferrable"]
    jobs = await repo.list("ops.jobs")
    assert len(jobs) == 1 and jobs[0]["type"] == "ROUTE_ITEM" and jobs[0]["priority"] == 1


@pytest.mark.parametrize("field", ["author", "monitored_author", "reposted_by"])
def test_organization_author_and_repost_resolve_canonical_entity_and_token(field):
    opts, _ = options()
    ids = Pipeline(MemoryRepository(), opts, http=object()).identities(
        {"author": "unknown_original", field: "@Aave"}
    )
    assert {"aave", "aave:AAVE"} <= ids


@pytest.mark.asyncio
async def test_failed_event_is_not_a_routing_candidate():
    opts, _ = options()
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "i", {"item_id": "i", "text": "Aave new event"})
    await repo.put(
        "evt.events",
        "failed",
        {
            "event_id": "failed",
            "lifecycle": "FAILED",
            "entities": ["aave"],
        },
    )
    await repo.put(
        "ops.agent_sessions",
        "s",
        {
            "session_id": "s",
            "event_id": "failed",
            "last_item_at": iso(),
        },
    )
    # object() deliberately cannot issue HTTP: terminal events are never scored.
    routed = await Pipeline(repo, opts, http=object()).route_item({"item_id": "i"})
    assert routed["route"]["decision"] == "NEW_SESSION"
    assert routed["routing_scores"] == {}

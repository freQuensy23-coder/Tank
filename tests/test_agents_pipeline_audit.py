import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from tank.agents.pipeline import FILTER, ROUTE, Pipeline
from tank.storage import MemoryRepository
from tank.util import iso


def options():
    config = {
        "registry": yaml.safe_load(Path("config/registry.yaml").read_text()),
        "thresholds": {"filter_cutoff": 0.7, "routing_cutoff": 0.8},
    }
    return SimpleNamespace(
        load_config=config.__getitem__, openrouter_api_key="test", config_version="test", code_version="test"
    ), config


def test_filter_question_asks_the_03_3_1_condition_about_the_named_item():
    design = Path("03_Tank_V0_Technical_Design.md").read_text()
    # Same condition and project list as 03 §3.1, phrased for Jev's literal reading (user decision 2026-10-20).
    projects = FILTER.split("one of these projects: ")[1].rstrip("?")
    assert f"one of these projects: {projects}?" in design and FILTER.startswith("Is `item` about crypto")
    state = Pipeline.state({"source_name": "Outlet", "title": "Aave approved", "body": "Details"})
    assert state == {"item": {"source": "Outlet", "title": "Aave approved", "text": "Details"}}


@pytest.mark.parametrize(
    "text",
    [
        "near the world curve quant cow lit plasma",
        "lighter",
        "linear unrelated",
        "all this hype",
        "EUL CRV ZRX",
    ],
)
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
async def test_scored_match_joins_running_event_despite_other_pending_extraction():
    opts, _ = options()
    repo = MemoryRepository()
    at = iso()
    await repo.put("ingest.source_items", "item", {"item_id": "item", "title": "Aave product approved"})
    await repo.put(
        "evt.events", "event", {"event_id": "event", "entities": ["aave"], "lifecycle": "ANALYZING"}
    )
    await repo.put(
        "ops.agent_sessions",
        "running",
        {"session_id": "running", "event_id": "event", "status": "RUNNING", "last_item_at": at},
    )
    pipe = Pipeline(repo, opts, http=object())

    async def score(item, name, state):
        assert name == "same_event"
        return 0.95

    pipe.score = score
    routed = await pipe.route_item({"item_id": "item"})
    assert routed["route"]["decision"] == "ADDED"
    assert routed["route_event_id"] == "event"
    assert routed["routing_message"] == {"kind": "ITEM_ADDED", "item_id": "item", "event_id": "event"}


@pytest.mark.asyncio
@pytest.mark.parametrize("precision", ["BUCKET(1d)", None])
async def test_filter_job_expiry_preserves_source_observation_precision(precision):
    opts, _ = options()
    repo = MemoryRepository()
    item = {
        "item_id": "promoted",
        "title": "Official item",
        "relevance": {"decision": "ALWAYS"},
        "observed_at": "2026-10-18T00:00:00.123456Z",
    }
    if precision:
        item["observed_at_precision"] = precision
    await repo.put("ingest.source_items", item["item_id"], item)
    await Pipeline(repo, opts, http=object()).filter_item({"item_id": item["item_id"]})
    jobs = await repo.list("ops.jobs")
    assert len(jobs) == 1
    assert jobs[0]["expires_at"] == "2026-10-18T06:00:00.123456Z"
    assert jobs[0]["expires_at_prec"] == precision
    assert jobs[0]["expires_at_prec_reason"] == (None if precision else "INCOMPLETE")


@pytest.mark.asyncio
async def test_new_item_routes_without_reservation_state():
    opts, _ = options()
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "new", {"item_id": "new", "title": "Aave announcement"})
    routed = await Pipeline(repo, opts, http=object()).route_item({"item_id": "new"})
    assert routed["routing_message"] == {"kind": "NEW_ITEM", "item_id": "new"}
    assert routed["route"] == {"decision": "NEW_SESSION", "event_id": None, "p_same": None}


@pytest.mark.asyncio
@pytest.mark.parametrize("same,decision", [(0.95, "ADDED"), (0.1, "NEW_SESSION")])
async def test_pending_extraction_is_not_a_candidate_then_real_event_is_scored(same, decision):
    opts, config = options()
    repo = MemoryRepository()
    for key in ("first", "pending", "next"):
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
                "model": "jev-1.13-free",
                "usage": {},
                "answers": {"same_event": {"type": "noul", "noul": same}},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        pipe = Pipeline(repo, opts, http=http)
        config["thresholds"]["routing_cutoff"] = None
        first = await pipe.route_item({"item_id": "first"})
        config["thresholds"]["routing_cutoff"] = 0.8
        pending = await pipe.route_item({"item_id": "pending"})
        assert pending["routing_message"]["kind"] == "NEW_ITEM" and not calls
        assert first["routing_message"] == {"kind": "NEW_ITEM", "item_id": "first"}
        assert "reservation_id" not in pending["route"]
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
async def test_other_pending_session_never_blocks_unmatched_item(status):
    opts, _ = options()
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "i", {"item_id": "i", "title": "Aave other event"})
    await repo.put("ops.agent_sessions", "ses", {"session_id": "ses", "status": status})
    result = await Pipeline(repo, opts, http=object()).route_item({"item_id": "i"})
    assert result["route"]["decision"] == "NEW_SESSION"


def test_identity_matching_uses_registry_aliases_and_qualifiers_without_code_overrides():
    opts, config = options()
    config["registry"]["entities"] = [
        {
            "entity_id": "lighter",
            "names": ["Generic"],
            "aliases": ["New Venue"],
            "qualified_aliases": [{"alias": "Registered", "requires_any": ["Qualifier"]}],
            "tokens": ["lighter:LIT"],
        }
    ]
    pipe = Pipeline(MemoryRepository(), opts, http=object())
    assert "lighter" in pipe.identities({"text": "NEW VENUE"})
    assert "lighter" in pipe.identities({"quoted": "Registered with qualifier"})
    for text in ("Generic", "Registered", "Lighter DEX", "xNew Venuex"):
        assert "lighter" not in pipe.identities({"text": text})
    assert "lighter" in pipe.identities({"text": "$LIT"})


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
            json={
                "model": "jev-1.13-free",
                "usage": {},
                "answers": {"relevant": {"type": "noul", "noul": 0.1}},
            },
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
            "lifecycle": "DETECTED",
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
            "status": "FAILED",
        },
    )
    # object() deliberately cannot issue HTTP: terminal events are never scored.
    routed = await Pipeline(repo, opts, http=object()).route_item({"item_id": "i"})
    assert routed["route"]["decision"] == "NEW_SESSION"
    assert routed["routing_scores"] == {}


@pytest.mark.asyncio
async def test_candidate_questions_overlap_and_reduce_in_stable_order():
    import asyncio

    opts, _ = options()
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "i", {"item_id": "i", "text": "Aave"})
    events = [{"event_id": key, "statement": key} for key in ("a", "b", "c")]
    pipe = Pipeline(repo, opts, http=object())

    async def candidates(*args):
        return events

    pipe._candidate_events = candidates
    arrived, ready = set(), asyncio.Event()

    async def score(item, name, state):
        key = state["event"]["statement"]
        arrived.add(key)
        if len(arrived) == 3:
            ready.set()
        await ready.wait()
        return {"a": 0.95, "b": 0.95, "c": 0.9}[key]

    pipe.score = score
    async with asyncio.timeout(2):
        result = await pipe.route_item({"item_id": "i"})
    assert result["route_event_id"] == "a"
    assert list(result["routing_scores"]) == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_candidate_window_includes_equal_clock_and_lower_boundary_only():
    from datetime import timedelta

    from tank.util import utcnow

    opts, _ = options()
    repo = MemoryRepository()
    now = utcnow()
    since = now - timedelta(days=3)
    for key, at in (
        ("z_equal", now),
        ("a_lower", since),
        ("future", now + timedelta(microseconds=1)),
        ("old", since - timedelta(microseconds=1)),
    ):
        await repo.put("evt.events", key, {"event_id": key, "entities": ["aave"]})
        await repo.put(
            "ops.agent_sessions", key, {"session_id": key, "event_id": key, "last_item_at": iso(at)}
        )
    events = await Pipeline(repo, opts, http=object())._candidate_events({"aave"}, iso(since), now)
    assert [event["event_id"] for event in events] == ["a_lower", "z_equal"]


@pytest.mark.parametrize(
    ("title", "body", "expected"),
    [
        ("OKX lists QNT", "OKX lists QNT", {"title": "OKX lists QNT"}),
        ("OKX lists QNT", "OKX  lists\nQNT ", {"title": "OKX lists QNT"}),
        (
            "Judge rejects bid - OKX",
            "Judge rejects bid OKX",
            {"title": "Judge rejects bid"},
        ),
        (
            "OKX lists QNT",
            "Deposits open at 10:00 UTC.\nTrading opens at 12:00 UTC.",
            {"title": "OKX lists QNT", "text": "Deposits open at 10:00 UTC. Trading opens at 12:00 UTC."},
        ),
        ("OKX lists QNT", "", {"title": "OKX lists QNT"}),
    ],
    ids=["identical", "whitespace", "outlet-suffix", "body-kept", "empty-body"],
)
def test_item_view_sends_title_and_body_once_without_mutating_capture(title, body, expected):
    from copy import deepcopy

    item = {
        "source_id": "S10",
        "source": "OKX",
        "author": "OKX",
        "title": title,
        "text": body,
        "quoted": "Original quoted material",
        "raw_ref": "immutable-original.zst",
        "published_at": "2026-10-20T10:00:00Z",
    }
    original = deepcopy(item)
    assert Pipeline.state(item) == {
        "item": {"source": "OKX", **expected, "quoted": "Original quoted material"}
    }
    assert item == original


def test_long_body_is_cut_at_a_sentence():
    body = "First sentence is here. " * 1000
    text = Pipeline.item_view({"source": "SEC", "title": "Speech", "text": body})["text"]
    assert len(text) <= 12002 and text.endswith(". …")


def test_routing_state_names_item_and_event_with_readable_identities():
    opts, _ = options()
    state = Pipeline(MemoryRepository(), opts, http=object()).routing_state(
        {"source": "Reuters", "author": "Jane Doe", "title": "Uniswap fee switch passes"},
        {
            "statement": "Uniswap governance approved the fee switch.",
            "entities": [{"entity_id": "uniswap", "role": "SUBJECT"}, {"entity_id": "UNRESOLVED:Gauntlet"}],
            "tokens_mentioned": ["uniswap:UNI", "UNRESOLVED:FOO"],
            "event_state": "APPROVED",
            "known_to_us_at": "2026-10-20T10:00:00.123456Z",
        },
    )
    assert state == {
        "item": {"source": "Reuters", "author": "Jane Doe", "title": "Uniswap fee switch passes"},
        "event": {
            "statement": "Uniswap governance approved the fee switch.",
            "entities": ["Uniswap", "Gauntlet"],
            "tokens": ["UNI", "FOO"],
            "stage": "approved",
        },
    }


@pytest.mark.parametrize("text", ["Uniswap governance passes fee switch", "$UNI fee switch passes"])
async def test_event_with_resolved_token_id_is_a_candidate_for_alias_or_ticker(text):
    # W5: event tools store tokens as registry token_ids ("uniswap:UNI"), never bare tickers.
    from tank.util import utcnow

    opts, _ = options()
    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "e", {"event_id": "e", "entities": [], "tokens_mentioned": ["uniswap:UNI"]})
    await repo.put("ops.agent_sessions", "s", {"session_id": "s", "event_id": "e", "last_item_at": iso(now)})
    pipe = Pipeline(repo, opts, http=object())
    assert "uniswap:UNI" in pipe.identities({"title": text})
    events = await pipe._candidate_events(pipe.identities({"title": text}), iso(now), now)
    assert [event["event_id"] for event in events] == ["e"]


def test_bare_item_ticker_resolves_to_registry_token_id():
    opts, _ = options()
    ids = Pipeline(MemoryRepository(), opts, http=object()).identities(
        {"title": "fee switch", "tokens": ["UNI"]}
    )
    assert {"uniswap:UNI", "uniswap"} <= ids

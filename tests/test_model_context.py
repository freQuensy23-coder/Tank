"""The model boundary: text context out, validated record IDs in."""

import copy
import json
from pathlib import Path
from typing import Literal

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_http_admission

from tank.agents.context import CONTEXT_VERSION, ID_FIELDS, ModelContext, identifier_field
from tank.agents.sessions import SessionService
from tank.analysis.page_access import LivePageAccess
from tank.event_tools import EventToolService, validate_schema
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import clock_iso, iso, new_id, sha256, utcnow


@pytest.fixture
async def context(tmp_path: Path):
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, _env_file=None)
    stamp = clock_iso()
    item = {
        "item_id": new_id("itm"),
        "url": "https://example.org/launch",
        "title": "Product launch",
        "text": "Product is live.",
        "observed_at": stamp,
        "source_id": "S01",
        "source_class": "PRIMARY",
        "raw_sha256": "a" * 64,
        "code_version": "b" * 40,
        "raw_ref": "/data/private/source.json",
        "routing_scores": {new_id("evt"): 0.2},
    }
    session = {
        "session_id": "session_private",
        "model_context_version": CONTEXT_VERSION,
        "item_id": item["item_id"],
        "event_id": None,
        "prompt_version": sha256({"prompt": "test"}),
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "model_id": "test",
        "config_version": "test",
        "code_version": "test",
        "status": "OPEN",
        "last_payload": {"kind": "NEW_ITEM", "item_id": item["item_id"]},
    }
    await repo.put("ingest.source_items", item["item_id"], item)
    await repo.put("ops.agent_sessions", session["session_id"], session)
    return repo, settings, session, item


def assert_public(value) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert not identifier_field(key), key
            assert_public(item)
    elif isinstance(value, list):
        for item in value:
            assert_public(item)


async def bound_event(repo, session, **changes):
    event = {
        "event_id": new_id("evt"),
        "statement": "World product is live",
        "lifecycle": "SCORED",
        **changes,
    }
    await repo.put("evt.events", event["event_id"], event)
    session["event_id"] = event["event_id"]
    await repo.put("ops.agent_sessions", session["session_id"], session)
    return event


async def claim_of(repo, event_id, quote="Product is live."):
    claim = {
        "claim_id": new_id("clm"),
        "event_id": event_id,
        "quote": quote,
        "accepted": True,
        "verified": True,
    }
    await repo.put("evt.claims", claim["claim_id"], claim)
    return claim


def recorded(item, **changes):
    return {
        "is_event": True,
        "statement": "World product is live",
        "certainty": "HIGH",
        "certainty_reason": "Official launch",
        "entities": [{"entity_id": "world", "role": "ACTOR"}],
        "token_ids": ["world:WLD"],
        "event_time": {"value": item["observed_at"], "precision": "EXACT", "quote": None},
        "public_at": {
            "value": item["observed_at"],
            "precision": "EXACT",
            "basis": "Primary source",
            "quote": None,
        },
        "freshness": "FRESH",
        "next_milestone": None,
        "checked_sources": [item["url"]],
        "claims": [
            {
                "type": "STATE",
                "subject": "World",
                "predicate": "launched",
                "object": "product",
                "state_claim": "LIVE",
                "verified": True,
                "directionality": "BOTH",
                "evidence_class": "PRIMARY_FACT",
                "quote": item["text"],
                "evidence": [{"item_id": item["item_id"], "quote": item["text"]}],
            }
        ],
        **changes,
    }


def observation(**changes):
    return {
        "link": "CORROBORATION",
        "claims": [],
        "freshness": "FRESH",
        "public_at": {"value": clock_iso(), "precision": "EXACT", "basis": "Source", "quote": None},
        **changes,
    }


async def test_initial_message_is_text_and_the_incoming_item_stays_implicit(context) -> None:
    repo, settings, session, item = context
    original = copy.deepcopy(item)
    boundary = ModelContext(repo, settings, session)
    message = await boundary.message(session["last_payload"], item)
    assert_public(message)
    assert message["item"]["text"] == item["text"]
    assert message["item"]["observed_at"] == item["observed_at"]
    serialized = json.dumps(message)
    for secret in (item["item_id"], item["raw_sha256"], item["code_version"], item["raw_ref"]):
        assert secret not in serialized
    assert await repo.get("ingest.source_items", item["item_id"]) == original
    assert await boundary.arguments("get_item", {}) == {"item_id": item["item_id"]}
    assert await boundary.arguments("get_item", {"item_id": item["item_id"]}) == {"item_id": item["item_id"]}
    with pytest.raises(ValueError, match=r"\$\.document is no longer accepted: pass item_id"):
        await boundary.arguments("get_item", {"document": message["item"]["document"]})


async def test_real_record_and_followup_tools_take_ids_and_reply_without_them(context) -> None:
    repo, settings, session, item = context
    tools = EventToolService(
        repo,
        settings,
        None,
        None,
        versions=declared_versions_fixture(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    requests = []

    def receive(request: pytest.FixtureRequest):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        service = SessionService(
            repo, settings, tools.dispatch, http=http, versions=declared_versions_fixture()
        )
        await service.tool(session, {"id": "call_record", "name": "record_event", "input": recorded(item)})
    ledger = await repo.get("ops.tool_calls", "session_private:call_record")
    assert ledger["status"] == "DONE", ledger.get("result")
    assert ledger["arguments"]["item_id"] == item["item_id"]
    event = ledger["result"]
    public = json.loads(requests[-1]["events"][0]["content"][0]["text"])
    assert_public(public)
    for raw in (
        event["event_id"],
        event["accepted_claims"][0]["claim_id"],
        event["accepted_claims"][0]["obs_id"],
    ):
        assert raw not in json.dumps(public)
    assert public["accepted_claims"][0]["claim"]["quote"] == item["text"]
    claim_id = event["accepted_claims"][0]["claim_id"]
    assert event["accepted_claims"][0]["evidence"] == [{"item_id": item["item_id"], "quote": item["text"]}]
    boundary = ModelContext(repo, settings, await repo.get("ops.agent_sessions", session["session_id"]))
    assert await boundary.arguments("get_decision", {"token_id": "world:WLD"}) == {
        "token_id": "world:WLD",
        "event_id": event["event_id"],
    }
    edges = {
        "edges": [
            {
                "entity_id": "world",
                "token_id": "world:WLD",
                "role": "PRIMARY",
                "layer": "ISSUER_PRODUCT",
                "direction": "POSITIVE",
                "causal_chain": "World runs the product",
                "directionality": "BOTH",
                "exposure_score": 8,
                "edge_certainty": "HIGH",
                "evidence": [claim_id],
            }
        ]
    }
    assert (await boundary.arguments("set_beneficiaries", edges))["edges"][0]["evidence"] == [claim_id]
    foreign = await claim_of(repo, new_id("evt"))
    edges["edges"][0]["evidence"] = [claim_id, foreign["claim_id"]]
    with pytest.raises(
        ValueError, match=r"\$\.edges\[0\]\.evidence\[1\]: this is not a claim of the current event"
    ):
        await boundary.arguments("set_beneficiaries", edges)


@pytest.mark.parametrize(
    "tool,arguments,message",
    [
        (
            "get_item",
            {"item_id": "ITM-01M5QZZZZZZZZZZZZZZZZZZZZZ"},
            r"\$\.item_id: no source document has this ID",
        ),
        ("get_event", {"event_id": "EVT-01M5QZZZZZZZZZZZZZZZZZZZZZ"}, r"\$\.event_id: no event has this ID"),
        ("get_token_facts", {"token_id": "BTC"}, r"\$\.token_id: not a registry token_id"),
        ("get_token_facts", {"token_id": "Bitcoin (BTC)"}, r"\$\.token_id: not a registry token_id"),
        ("list_open_events", {"entity_id": "Uniswap Labs"}, r"\$\.entity_id: not a registry entity_id"),
        (
            "search_tank_items",
            {"query": "x", "source_ids": ["SEC"]},
            r"\$\.source_ids\[0\]: not a registered source_id",
        ),
    ],
)
async def test_unknown_ids_are_refused_with_the_lookup_query(context, tool, arguments, message) -> None:
    repo, settings, session, _ = context
    with pytest.raises(ValueError, match=message) as refused:
        await ModelContext(repo, settings, session).arguments(tool, arguments)
    assert "Finding record IDs" in str(refused.value)
    registry_key = "registry" in message or "registered" in message
    assert ("shown after « · » next to the name" in str(refused.value)) is registry_key
    assert ("Look it up with read-only SQL" in str(refused.value)) is not registry_key


async def test_an_id_of_the_wrong_kind_is_named_as_such(context) -> None:
    repo, settings, session, item = context
    event = await bound_event(repo, session)
    claim = await claim_of(repo, event["event_id"])
    boundary = ModelContext(repo, settings, session)
    with pytest.raises(ValueError, match=r"\$\.event_id: this is the ID of a claim, not of an event"):
        await boundary.arguments("get_event", {"event_id": claim["claim_id"]})
    with pytest.raises(ValueError, match="this is the ID of a source document, not of a watch condition"):
        await boundary.arguments("set_watch_condition", {"action": "DISARM", "cond_id": item["item_id"]})
    other = await repo.get("evt.events", event["event_id"])
    assert await boundary.arguments("get_event", {"event_id": other["event_id"]}) == {
        "event_id": event["event_id"]
    }


@pytest.mark.parametrize(
    "tool,arguments,replacement",
    [
        ("get_item", {"document": {"url": "https://example.org/launch"}}, "item_id"),
        ("get_event", {"event": {"statement": "World product is live"}}, "event_id"),
        ("merge_into", {"event": {"statement": "World product is live"}}, "event_id"),
        ("get_decision", {"token": "World (WLD)"}, "token_id"),
        ("get_token_facts", {"token": "BTC"}, "token_id"),
        ("get_market_truth", {"tokens": ["World (WLD)"]}, "token_ids"),
        ("get_registry", {"entity_or_token": "Uniswap"}, "token_id or entity_id"),
        ("list_open_events", {"entity_or_token": "Uniswap"}, "token_id or entity_id"),
        ("search_tank_items", {"query": "x", "sources": ["SEC"]}, "source_ids"),
        (
            "schedule_milestone",
            {"milestone": "Vote", "due_at": "2026-10-30", "source": "Snapshot"},
            "source_id",
        ),
        ("set_watch_condition", {"action": "DISARM", "condition": {"text": "Coverage high"}}, "cond_id"),
        ("set_watch_condition", {"action": "ARM", "token": "World (WLD)", "conditions": []}, "token_id"),
        (
            "add_observation",
            observation(claims=[{"supersedes_claims": [{"quote": "x"}]}]),
            "supersedes_claim_ids",
        ),
        ("add_observation", observation(document={"url": "https://example.org/launch"}), "item_id"),
        ("set_beneficiaries", {"edges": [{"entity": "World", "token": "World (WLD)"}]}, "entity_id"),
    ],
)
async def test_old_description_references_are_refused_and_name_the_id_field(
    context, tool, arguments, replacement
) -> None:
    repo, settings, session, _ = context
    await bound_event(repo, session)
    with pytest.raises(ValueError, match="no longer accepted: pass " + replacement) as refused:
        await ModelContext(repo, settings, session).arguments(tool, arguments)
    assert "read-only SQL" in str(refused.value)


@pytest.mark.parametrize(
    "path",
    [
        ["dims", "magnitude", "evidence"],
        ["structural_gates", 0, "evidence"],
    ],
)
async def test_claim_descriptions_in_assessment_evidence_are_refused(context, path) -> None:
    repo, settings, session, _ = context
    await bound_event(repo, session)
    dimensions = (
        "economic_directness",
        "magnitude",
        "permanence",
        "distribution_expansion",
        "mechanical_flows",
        "surprise",
        "implementation",
    )
    args = {
        "assessment": {
            "dims": {
                name: {"value": None, "reason": "NOT_ESTIMABLE", "rationale": "x", "evidence": []}
                for name in dimensions
            },
            "class": "MATERIAL",
            "criterion": "MATERIAL",
            "criterion_claim_id": None,
            "impact_horizon": {
                "impact_start": "WEEKS",
                "ramp_shape": "GRADUAL",
                "dependencies": [],
                "time_to_materiality": "Unknown",
                "timing_confidence": "LOW",
            },
            "structural_gates": [{"gate": "G1", "satisfied": False, "evidence": []}],
        }
    }
    target = args["assessment"]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = [{"quote": "Product is live.", "subject": "World"}]
    with pytest.raises(ValueError, match="evidence\\[0\\].*name records only by their IDs"):
        await ModelContext(repo, settings, session).arguments("set_assessment", args)


async def test_scoped_tools_bind_the_current_event_and_refuse_an_explicit_one(context) -> None:
    repo, settings, session, _ = context
    boundary = ModelContext(repo, settings, session)
    with pytest.raises(ValueError, match="works on the current event; record it with record_event first"):
        await boundary.arguments("get_decision", {"token_id": "world:WLD"})
    event = await bound_event(repo, session)
    boundary = ModelContext(repo, settings, session)
    card = {"kind": "FULL", "what": "x", "verdict": "y", "why": ["First fact.", "Second fact."]}
    assert await boundary.arguments("publish_card", card) == {**card, "event_id": event["event_id"]}
    with pytest.raises(ValueError, match=r"unknown fields \['event_id'\]"):
        await boundary.arguments("publish_card", {**card, "event_id": new_id("evt")})
    assert await boundary.arguments("web_search", {"query": "Uniswap announcement"}) == {
        "query": "Uniswap announcement"
    }


async def test_authored_text_cannot_carry_internal_ids_but_quotes_and_queries_can(context) -> None:
    repo, settings, session, item = context
    text = "The release includes session_notes.csv and checksum " + "a" * 64
    boundary = ModelContext(repo, settings, session)
    assert await boundary.arguments("web_search", {"query": text}) == {"query": text}
    args = recorded(item, claims=[{**recorded(item)["claims"][0], "quote": text}])
    args["claims"][0]["evidence"] = [{"item_id": item["item_id"], "quote": text}]
    restored = await boundary.arguments("record_event", args)
    assert restored["claims"][0]["evidence"] == [{"item_id": item["item_id"], "quote": text}]
    args["statement"] = "Matches " + new_id("evt")
    with pytest.raises(ValueError, match=r"\$\.statement: write this text without internal record IDs"):
        await boundary.arguments("record_event", args)


async def test_merge_into_by_event_id_works_when_the_session_never_displayed_the_target(context) -> None:
    repo, settings, session, item = context
    target = {
        "event_id": new_id("evt"),
        "statement": "An Argentine judge reportedly dismissed the LIBRA criminal case.",
        "public_at": "2026-10-23T00:00:00Z",
        "known_to_us_at": "2026-10-23T08:00:00Z",
        "lifecycle": "SCORED",
    }
    await repo.put("evt.events", target["event_id"], target)
    current = await bound_event(repo, session, known_to_us_at="2026-10-24T14:00:00Z")
    observation_id = new_id("obs")
    await repo.put(
        "evt.event_observations",
        observation_id,
        {"obs_id": observation_id, "event_id": current["event_id"], "item_id": item["item_id"]},
    )
    assert "model_catalog" not in session
    boundary = ModelContext(repo, settings, session)
    assert await boundary.arguments("merge_into", {"event_id": target["event_id"]}) == {
        "event_id": target["event_id"]
    }
    with pytest.raises(ValueError, match="this is the current event"):
        await boundary.arguments("merge_into", {"event_id": current["event_id"]})
    posts = []

    def receive(request: pytest.FixtureRequest):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={"data": []})

    tools = EventToolService(
        repo,
        settings,
        None,
        None,
        versions=declared_versions_fixture(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        service = SessionService(
            repo, settings, tools.dispatch, http=http, versions=declared_versions_fixture()
        )
        await service.tool(
            session, {"id": "merge", "name": "merge_into", "input": {"event_id": target["event_id"]}}
        )
    call = await repo.get("ops.tool_calls", session["session_id"] + ":merge")
    assert call["status"] == "DONE", call.get("result")
    assert (await repo.get("evt.events", current["event_id"]))["lifecycle"] == "MERGED"
    assert (await repo.get("evt.event_observations", observation_id))["event_id"] == target["event_id"]
    text = posts[-1]["events"][0]["content"][0]["text"]
    assert target["statement"] in text and target["event_id"] not in text
    merged = {**target, "event_id": new_id("evt"), "lifecycle": "MERGED"}
    await repo.put("evt.events", merged["event_id"], merged)
    with pytest.raises(ValueError, match="that event is MERGED; merge into an open event"):
        await ModelContext(repo, settings, session).arguments("merge_into", {"event_id": merged["event_id"]})


async def test_market_results_keep_values_units_coverage_and_discard_audit_companions(context) -> None:
    repo, settings, session, _ = context
    raw = {
        "market_truth": [
            {
                "mta_id": new_id("mta"),
                "event_id": new_id("evt"),
                "token_id": "uniswap:UNI",
                "spot": {"cvd": "-42.125", "unit": "NATIVE_TOKEN", "complete": False, "excluded": ["kraken"]},
                "measurement_metadata": {"hash": "c" * 64},
                "provenance": {"session_id": session["session_id"]},
            }
        ]
    }
    public = await ModelContext(repo, settings, session).result("get_market_truth", raw)
    assert_public(public)
    assert public["market_truth"][0]["spot"] == {
        **raw["market_truth"][0]["spot"],
        "excluded": ["Kraken · kraken"],
    }
    assert public["market_truth"][0]["token"] == "Uniswap (UNI) · uniswap:UNI"
    assert "measurement_metadata" not in json.dumps(public)
    assert raw["market_truth"][0]["mta_id"]


def test_every_id_field_of_the_model_contracts_is_validated() -> None:
    root = Path(__file__).resolve().parents[1] / "config/skills"
    seen = set()

    def visit(schema) -> None:
        for key, value in schema.get("properties", {}).items():
            if identifier_field(key):
                assert key in ID_FIELDS, key
                seen.add(key)
            visit(value)
        if isinstance(schema.get("items"), dict):
            visit(schema["items"])
        for kind in ("oneOf", "anyOf", "allOf"):
            for branch in schema.get(kind, []):
                visit(branch)

    for path in root.glob("*/tools.json"):
        for tool in json.loads(path.read_text()):
            visit(tool["input_schema"])
    assert {"item_id", "event_id", "claim_id", "cond_id", "token_id", "entity_id", "source_id"} <= seen
    assert {"instrument_id", "claim_ids", "token_ids", "source_ids", "supersedes_claim_ids"} <= seen
    assert 'psql "$TANK_DB_URL"' in (root / "tank-data/SKILL.md").read_text()


async def test_registry_objects_are_shown_with_their_key_and_the_key_is_what_tools_take(context) -> None:
    repo, settings, session, _ = context
    boundary = ModelContext(repo, settings, session)
    projected = await boundary.result("get_registry", settings.load_config("registry"))
    assert_public(projected)
    text = json.dumps(projected, ensure_ascii=False)
    for label in (
        "Uniswap (UNI) · uniswap:UNI",
        "Uniswap · uniswap",
        "binance SPOT BTCUSDT · binance:spot:BTCUSDT",
    ):
        assert label in text
    uniswap = next(row for row in projected["entities"] if row["entity"] == "Uniswap · uniswap")
    assert "X · S29" in uniswap["official_sources"] and "Uniswap · S15" in uniswap["official_sources"]
    assert await boundary.arguments("get_registry", {"token_id": "uniswap:UNI"}) == {
        "token_id": "uniswap:UNI"
    }
    assert await boundary.arguments("list_open_events", {"entity_id": "uniswap"}) == {"entity_id": "uniswap"}
    assert await boundary.arguments("search_tank_items", {"query": "x", "source_ids": ["S14"]}) == {
        "query": "x",
        "source_ids": ["S14"],
    }


async def test_a_source_registered_only_in_the_database_is_accepted(context) -> None:
    repo, settings, session, _ = context
    await repo.put("reg.sources", "S99", {"source_id": "S99", "name": "Replay-only source"})
    arguments = {"query": "x", "source_ids": ["S99"]}
    assert await ModelContext(repo, settings, session).arguments("search_tank_items", arguments) == arguments


async def test_no_id_guards_strip_record_ids_but_keep_registry_keys(context) -> None:
    repo, settings, session, item = context
    boundary = ModelContext(repo, settings, session)
    event_id = new_id("evt")
    public = await boundary.result(
        "test",
        {
            "note": f"World (WLD) · world:WLD and S14 moved; see {event_id}",
            "token_id": "world:WLD",
            "error": {"reason": f"S14 and {event_id} disagree"},
        },
    )
    assert public == {
        "note": "World (WLD) · world:WLD and S14 moved; see [internal reference omitted]",
        "token": "World (WLD) · world:WLD",
        "error": {"reason": "Venue instrument lists · S14 and [internal reference omitted] disagree"},
    }
    args = recorded(item, statement="world:WLD integrates with binance:spot:BTCUSDT via S14")
    assert (await boundary.arguments("record_event", args))["statement"] == args["statement"]


async def test_scout_review_binds_its_proposal_and_names_the_duplicate_by_id(context) -> None:
    repo, settings, session, item = context
    event = {"event_id": new_id("evt"), "statement": "A separate launch"}
    await repo.put("evt.events", event["event_id"], event)
    proposal = {"prp_id": new_id("prp"), "concept_name": "New fee mechanism"}
    other = {"prp_id": new_id("prp"), "concept_name": "Existing fee mechanism"}
    decided = {"prp_id": new_id("prp"), "concept_name": "Old fee mechanism", "human_action": "REJECT"}
    for row in (proposal, other, decided):
        await repo.put("scout.expansion_proposals", row["prp_id"], row)
    session.update(proposal_id=proposal["prp_id"], event_id=None)
    boundary = ModelContext(repo, settings, session)
    message = await boundary.message({"kind": "SCOUT_REVIEW", "proposal_id": proposal["prp_id"]}, None)
    assert_public(message)
    review = {
        "verdict": "Duplicate",
        "reason": "Same mechanism",
        "evidence": [{"event_id": event["event_id"]}, {"item_id": item["item_id"]}],
        "scores": {"overlap": 9, "observability": 5, "reusability": 3, "decision_value": 2},
        "recommendation": "REJECT",
        "duplicate_of": other["prp_id"],
    }
    restored = await boundary.arguments("record_review", review)
    assert restored["proposal_id"] == proposal["prp_id"] and restored["duplicate_of"] == other["prp_id"]
    for duplicate, message in (
        (proposal["prp_id"], "this is the current proposal"),
        (decided["prp_id"], "that proposal is already decided"),
    ):
        with pytest.raises(ValueError, match=message):
            await boundary.arguments("record_review", {**review, "duplicate_of": duplicate})
    with pytest.raises(
        ValueError, match=r"\$\.evidence\[0\]\.claim_id: this is the ID of an event, not of a claim"
    ):
        await boundary.arguments("record_review", {**review, "evidence": [{"claim_id": event["event_id"]}]})
    with pytest.raises(ValueError, match="duplicate_of.*name records only by their IDs"):
        await boundary.arguments(
            "record_review", {**review, "duplicate_of": {"subject": other["concept_name"]}}
        )


async def test_source_url_with_external_hash_remains_a_working_citation(context) -> None:
    repo, settings, session, item = context
    item["url"] = "https://example.org/release/" + "a" * 40
    public = await ModelContext(repo, settings, session).result("fetch_page", item)
    assert public["document"]["url"] == item["url"] and public["url"] == item["url"]
    assert_public(public)


async def test_active_input_binds_origin_even_when_another_source_is_already_queued(context) -> None:
    repo, settings, session, item = context
    session.update(
        active_message_id="active",
        last_payload={"kind": "ITEM_ADDED", "item_id": "queued-source"},
        message_log=[{"message_id": "active", "payload": {"kind": "NEW_ITEM", "item_id": item["item_id"]}}],
    )
    result = await ModelContext(repo, settings, session).arguments(
        "record_event", recorded(item, is_event=False, no_event_reason="Opinion only", claims=[])
    )
    assert result["item_id"] == item["item_id"]


async def test_document_without_url_or_title_uses_its_text_and_clock(context) -> None:
    repo, settings, session, item = context
    item.pop("url")
    item.pop("title")
    await repo.put("ingest.source_items", item["item_id"], item)
    public = await ModelContext(repo, settings, session).result("get_item", item)
    assert public["document"] == {"excerpt": item["text"], "observed_at": item["observed_at"]}


async def test_verbatim_source_content_survives_identifier_shaped_prose(context) -> None:
    repo, settings, session, item = context
    text = "The release includes session_notes.csv and checksum " + "a" * 64
    item.update(text=text, title="agent_status update")
    await repo.put("ingest.source_items", item["item_id"], item)
    public = await ModelContext(repo, settings, session).result("get_item", item)
    assert public["text"] == text and public["title"] == item["title"]
    assert "raw_sha256" not in public and "item_id" not in public


async def test_real_domain_refusal_uses_display_names_and_keeps_raw_audit(context) -> None:
    repo, settings, session, _ = context
    event = await bound_event(repo, session, current_ems_id="ems")
    await repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": event["event_id"]})
    await repo.put(
        "evt.beneficiary_maps",
        "map",
        {"event_id": event["event_id"], "map_version": 1, "analysis_set": ["uniswap:UNI"]},
    )
    posts = []

    def receive(request: pytest.FixtureRequest):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={"data": []})

    tools = EventToolService(
        repo,
        settings,
        None,
        None,
        versions=declared_versions_fixture(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        service = SessionService(
            repo, settings, tools.dispatch, http=http, versions=declared_versions_fixture()
        )
        await service.tool(
            session, {"id": "decision_error", "name": "get_decision", "input": {"token_id": "uniswap:UNI"}}
        )
    call = await repo.get("ops.tool_calls", session["session_id"] + ":decision_error")
    assert call["status"] == "REFUSED"
    assert "uniswap:UNI" in call["result"]["error"]["reason"]
    text = posts[-1]["events"][0]["content"][0]["text"]
    assert "Uniswap (UNI) · uniswap:UNI" in text and "token_id" in text


async def test_diagnostics_map_registry_keys_to_names_without_rewriting_quotes_or_urls(context) -> None:
    repo, settings, session, _ = context
    boundary = ModelContext(repo, settings, session)
    text = "Use registry token_id uniswap:UNI, entity_id bitcoin, instrument_id binance:spot:BTCUSDT and source_id S01."
    url = "https://example.org/uniswap:UNI?source_id=S01"
    public = await boundary.result(
        "test",
        {
            "error": {"reason": text, "details": [text, url], "quote": text},
            "quote": text,
            "text": text,
            "url": url,
        },
    )
    reason = public["error"]["reason"]
    for label in (
        "Uniswap (UNI) · uniswap:UNI",
        "Bitcoin · bitcoin",
        "binance SPOT BTCUSDT · binance:spot:BTCUSDT",
        "SEC · S01",
        "token_id",
        "source_id",
    ):
        assert label in reason
    assert reason.count("uniswap:UNI") == 1
    assert public["error"]["details"] == [reason, url]
    assert public["error"]["quote"] == text
    assert public["quote"] == public["text"] == text and public["url"] == url


@pytest.mark.parametrize("token", ["uniswap:UNI", "cow:COW"])
async def test_identical_watches_on_different_tokens_are_disarmed_by_cond_id(context, token) -> None:
    repo, settings, session, _ = context
    event = await bound_event(repo, session)
    from datetime import timedelta

    from tank.analysis.watch_contracts import WatchCondition
    from tank.time_precision import collection_clock, offset_schedule

    service = EventToolService(
        repo,
        settings,
        None,
        None,
        versions=declared_versions_fixture(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    created = collection_clock()
    expires = offset_schedule(created, timedelta(days=7))
    provenance = service.provenance({"session_id": session["session_id"], "role": "analyst"})
    watches = [
        WatchCondition.model_validate(
            {
                "cond_id": new_id("CND"),
                "event_id": event["event_id"],
                "token_id": value,
                "role": "AGENT",
                "human_text": "Data confidence becomes high",
                "expr": {
                    "metric": "dc.class",
                    "window": "W1H",
                    "op": "==",
                    "value": "HIGH",
                    "persist": {"count": 1, "unit": "consecutive_windows"},
                },
                "status": "ARMED",
                "created_by": "analyst",
                **created.fields("created_at"),
                **expires.fields("expires_at"),
                **provenance,
            }
        ).model_dump(mode="json", exclude_none=False)
        for value in ("uniswap:UNI", "cow:COW")
    ]
    for row in watches:
        await repo.put("ana.watch_conditions", row["cond_id"], row)
    boundary = ModelContext(repo, settings, session)
    public = await boundary.result("get_decision", {"conditions": watches})
    assert_public(public)
    assert {row["condition"]["token"] for row in public["conditions"]} == {
        "Uniswap (UNI) · uniswap:UNI",
        "CoW Protocol (COW) · cow:COW",
    }
    selected = next(row for row in watches if row["token_id"] == token)
    args = {"action": "DISARM", "cond_id": selected["cond_id"]}
    validate_schema(
        args, settings.read_config_json("skills/tank-event-analyst/set_watch_condition.schema.json")
    )
    restored = await boundary.arguments("set_watch_condition", args)
    assert restored == {**args, "event_id": event["event_id"]}
    result = await EventToolService(
        repo,
        settings,
        None,
        None,
        versions=declared_versions_fixture(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    ).dispatch("set_watch_condition", restored, {"session_id": session["session_id"], "role": "analyst"})
    assert result["accepted"]
    for row in watches:
        expected = "CANCELLED" if row["token_id"] == token else "ARMED"
        assert (await repo.get("ana.watch_conditions", row["cond_id"]))["status"] == expected


async def test_watch_of_another_event_cannot_be_disarmed(context) -> None:
    repo, settings, session, _ = context
    await bound_event(repo, session)
    foreign = {
        "cond_id": new_id("CND"),
        "event_id": new_id("evt"),
        "token_id": "uniswap:UNI",
        "status": "ARMED",
    }
    await repo.put("ana.watch_conditions", foreign["cond_id"], foreign)
    with pytest.raises(ValueError, match=r"\$\.cond_id: this watch condition belongs to another event"):
        await ModelContext(repo, settings, session).arguments(
            "set_watch_condition", {"action": "DISARM", "cond_id": foreign["cond_id"]}
        )


async def test_watch_audit_relationships_are_removed_entirely_without_mutating_storage(context) -> None:
    repo, settings, session, _ = context
    row = {
        "cond_id": new_id("CND"),
        "token_id": "uniswap:UNI",
        "human_text": "Coverage becomes high",
        "expr": {
            "metric": "dc.class",
            "window": "EVENT",
            "op": "==",
            "value": "HIGH",
            "persist": {"count": 1, "unit": "consecutive_windows"},
        },
        "created_at": "2026-10-23T10:00:00Z",
        "status": "CANCELLED",
        "created_by": new_id("DEC"),
        "updatedBy": new_id("DEC"),
        "last_shown_by": new_id("DEC"),
        "rearmed_from": new_id("CND"),
        "cancelled_by": "AGENT",
        "cancel_provenance": {"session_id": session["session_id"], "code_version": "old"},
        "producingSessionProvenance": {"session_id": session["session_id"]},
    }
    await repo.put("ana.watch_conditions", row["cond_id"], row)
    public = await ModelContext(repo, settings, session).result("get_decision", {"conditions": [row]})
    assert public == {
        "conditions": [
            {
                "condition": {
                    "text": row["human_text"],
                    "conditions": row["expr"],
                    "token": "Uniswap (UNI) · uniswap:UNI",
                    "created_at": row["created_at"],
                },
                "token": "Uniswap (UNI) · uniswap:UNI",
                "human_text": row["human_text"],
                "expr": row["expr"],
                "created_at": row["created_at"],
                "status": row["status"],
            }
        ]
    }
    assert await repo.get("ana.watch_conditions", row["cond_id"]) == row


@pytest.mark.parametrize("kind", ["MILESTONE", "CONDITION_FIRED", "CALL_EXPIRED", "USER_QUESTION"])
async def test_callback_without_source_requires_an_explicit_document_id(context, kind) -> None:
    repo, settings, session, item = context
    await bound_event(repo, session)
    session.update(
        active_message_id="active",
        message_log=[{"message_id": "active", "payload": {"kind": kind}}],
        last_payload={"kind": "ITEM_ADDED", "item_id": item["item_id"]},
    )
    boundary = ModelContext(repo, settings, session)
    with pytest.raises(ValueError, match=r"\$\.item_id: this callback has no incoming source"):
        await boundary.arguments("add_observation", observation())
    restored = await boundary.arguments("add_observation", observation(item_id=item["item_id"]))
    assert restored["item_id"] == item["item_id"] and restored["event_id"] == session["event_id"]


async def test_incoming_source_binding_rejects_override_but_allows_other_claim_evidence(context) -> None:
    repo, settings, session, item = context
    await bound_event(repo, session)
    other = {**item, "item_id": new_id("itm"), "url": "https://example.org/other"}
    await repo.put("ingest.source_items", other["item_id"], other)
    boundary = ModelContext(repo, settings, session)
    with pytest.raises(ValueError, match="bound to the incoming source"):
        await boundary.arguments("add_observation", observation(item_id=other["item_id"]))
    claim = {
        **recorded(item)["claims"][0],
        "evidence": [{"item_id": other["item_id"], "quote": other["text"]}],
    }
    args = await boundary.arguments("add_observation", observation(claims=[claim]))
    assert args["item_id"] == item["item_id"]
    assert args["claims"][0]["evidence"][0]["item_id"] == other["item_id"]
    claim["evidence"][0]["item_id"] = "web-" + "f" * 32
    with pytest.raises(
        ValueError, match=r"claims\[0\]\.evidence\[0\]\.item_id: no source document has this ID"
    ):
        await boundary.arguments("add_observation", observation(claims=[claim]))


async def test_superseded_claims_must_belong_to_the_current_event(context) -> None:
    repo, settings, session, item = context
    event = await bound_event(repo, session)
    own = await claim_of(repo, event["event_id"])
    foreign = await claim_of(repo, new_id("evt"))
    claim = {**recorded(item)["claims"][0], "supersedes_claim_ids": [own["claim_id"]]}
    boundary = ModelContext(repo, settings, session)
    restored = await boundary.arguments("add_observation", observation(claims=[claim]))
    assert restored["claims"][0]["supersedes_claim_ids"] == [own["claim_id"]]
    claim["supersedes_claim_ids"] = [foreign["claim_id"]]
    with pytest.raises(
        ValueError, match=r"supersedes_claim_ids\[0\]: this is not a claim of the current event"
    ):
        await boundary.arguments("add_observation", observation(claims=[claim]))


async def test_milestone_callback_confirms_from_the_fetched_official_document_by_id(context) -> None:
    from datetime import timedelta

    repo, settings, session, opening = context
    opening.update(source_id="S29", observed_at=iso(utcnow() - timedelta(days=2)))
    await repo.put("ingest.source_items", opening["item_id"], opening)
    session.update(
        event_id=new_id("evt"),
        active_message_id="milestone-callback",
        message_log=[{"message_id": "milestone-callback", "payload": {"kind": "MILESTONE"}}],
        last_payload={"kind": "ITEM_ADDED", "item_id": opening["item_id"]},
    )
    await repo.put("ops.agent_sessions", session["session_id"], session)
    await repo.put(
        "evt.events",
        session["event_id"],
        {
            "event_id": session["event_id"],
            "certainty": "HIGH",
            "event_state": "PROPOSED",
            "statement": "Application awaits approval",
            "lifecycle": "SCORED",
        },
    )
    posts = []
    quote = "The SEC approved the application."
    url = "https://www.sec.gov/news/approval"

    def receive(request: pytest.FixtureRequest):
        if request.method == "GET" and str(request.url) == url:
            return httpx.Response(
                200, text=f"<html><body>{quote}</body></html>", headers={"content-type": "text/html"}
            )
        posts.append(json.loads(request.content))
        return httpx.Response(200, json={"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        domain = EventToolService(
            repo,
            settings,
            None,
            None,
            http=http,
            versions=declared_versions_fixture(),
            scheduler=RecordingReactionScheduling(),
            pages=LivePageAccess(fixture_http_admission()),
        )
        due = clock_iso()
        await domain.dispatch(
            "schedule_milestone",
            {"event_id": session["event_id"], "source_id": "S01", "milestone": "Approval", "due_at": due},
            {},
        )
        sessions = SessionService(
            repo, settings, domain.dispatch, http=http, versions=declared_versions_fixture()
        )
        await sessions.tool(session, {"id": "fetch-proof", "name": "fetch_page", "input": {"url": url}})
        fetched_call = await repo.get("ops.tool_calls", session["session_id"] + ":fetch-proof")
        assert fetched_call["status"] == "DONE", fetched_call.get("result")
        public = json.loads(posts[-1]["events"][0]["content"][0]["text"])
        assert_public(public)
        assert public["url"] == url
        found = [row for row in await repo.list("ingest.source_items") if row.get("url") == url]
        assert len(found) == 1
        fetched_item = found[0]["item_id"]
        args = observation(
            item_id=fetched_item,
            public_at={"value": due, "precision": "EXACT", "basis": "Official release", "quote": None},
            milestone_outcome={"outcome": "Application approved", "quote": quote},
            claims=[
                {
                    "type": "MILESTONE",
                    "subject": "SEC",
                    "predicate": "approved",
                    "object": "application",
                    "state_claim": "APPROVED",
                    "directionality": "BOTH",
                    "evidence_class": "PRIMARY_FACT",
                    "verified": True,
                    "quote": quote,
                    "evidence": [{"item_id": fetched_item, "quote": quote}],
                }
            ],
        )
        validate_schema(
            args, settings.read_config_json("skills/tank-event-analyst/add_observation.schema.json")
        )
        await sessions.tool(session, {"id": "observe-proof", "name": "add_observation", "input": args})
    call = await repo.get("ops.tool_calls", session["session_id"] + ":observe-proof")
    assert call["status"] == "DONE", call.get("result")
    assert call["arguments"]["item_id"] == fetched_item != opening["item_id"]
    observations = await repo.list("evt.event_observations")
    assert len(observations) == 1 and observations[0]["item_id"] == fetched_item
    claim = call["result"]["accepted_claims"][0]
    assert claim["obs_id"] == observations[0]["obs_id"]
    assert claim["evidence"] == [{"item_id": fetched_item, "quote": quote}]
    event = await repo.get("evt.events", session["event_id"])
    assert event["next_milestone"] is None and event["milestone_resolved"]
    assert event["milestone_resolutions"][-1]["quotes"] == [quote]
    escalations = await repo.list("ops.source_escalations")
    assert escalations and all(row["status"] == "CONFIRMED" for row in escalations)
    assert await repo.get("ingest.source_items", opening["item_id"]) == opening


@pytest.mark.parametrize("tool", ["record_event", "get_item"])
async def test_active_sourceless_callback_cannot_record_or_implicitly_read_the_opening_document(
    context: tuple[MemoryRepository, Settings, dict[str, JsonValue], dict[str, JsonValue]],
    tool: Literal["record_event", "get_item"],
) -> None:
    repo, settings, session, item = context
    session.update(
        active_message_id="callback",
        message_log=[
            {
                "message_id": "callback",
                "payload": {"kind": "CONDITION_FIRED"},
                "retry_count": 0,
                "turn_status": "RUNNING",
            }
        ],
        last_payload={"kind": "ITEM_ADDED", "item_id": "stale-source"},
    )
    await repo.put("ops.agent_sessions", "session_private", session)
    item_id = item["item_id"]
    assert isinstance(item_id, str)
    before = copy.deepcopy(await repo.get("ingest.source_items", item_id))
    dispatches: list[str] = []
    requests: list[httpx.Request] = []

    async def dispatch(
        name: str,
        arguments: dict[str, JsonValue],
        context: dict[str, JsonValue],
    ) -> dict[str, JsonValue]:
        dispatches.append(name)
        assert name == "get_item" and arguments["item_id"] == item["item_id"]
        return item

    def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"data": []})

    arguments: dict[str, JsonValue] = recorded(item) if tool == "record_event" else {}
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        service = SessionService(
            repo, settings, dispatch, http=http, stream_http=http, versions=declared_versions_fixture()
        )
        await service.tool(session, {"id": "callback-call", "name": tool, "input": arguments})
        refused = await repo.get("ops.tool_calls", "session_private:callback-call")
        assert refused is not None and refused["status"] == "REFUSED"
        failure = refused["result"]
        assert isinstance(failure, dict) and isinstance(failure["error"], dict)
        reason = failure["error"]["reason"]
        assert isinstance(reason, str) and "source" in reason
        assert dispatches == []
        assert len(requests) == 1
        await service.tool(
            session,
            {"id": "explicit-origin-read", "name": "get_item", "input": {"item_id": item["item_id"]}},
        )
    assert dispatches == ["get_item"] and len(requests) == 2
    explicit = await repo.get("ops.tool_calls", "session_private:explicit-origin-read")
    assert explicit is not None and explicit["status"] == "DONE"
    assert await repo.get("ingest.source_items", item_id) == before

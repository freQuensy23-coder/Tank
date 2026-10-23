"""Exercise the actual model boundary while retaining host-side evidence lineage."""

import copy
import json
from pathlib import Path

import httpx
import pytest

from tank.agents.context import ModelContext, identifier_field
from tank.agents.sessions import SessionService
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, new_id, sha256


@pytest.fixture
async def context(tmp_path):
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, _env_file=None)
    stamp = iso()
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
        "model_context_version": 1,
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


def assert_public(value):
    if isinstance(value, dict):
        for key, item in value.items():
            assert not identifier_field(key), key
            assert_public(item)
    elif isinstance(value, list):
        for item in value:
            assert_public(item)


async def test_initial_message_keeps_the_article_and_clocks_without_storage_metadata(context):
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
    restored = await boundary.arguments("get_item", {"document": message["item"]["document"]})
    assert restored == {"item_id": item["item_id"]}


async def test_real_record_and_followup_tools_bind_the_session_and_keep_exact_evidence(context):
    repo, settings, session, item = context
    boundary = ModelContext(repo, settings, session)
    visible = await boundary.message(session["last_payload"], item)
    tools = EventToolService(repo, settings, None, None)
    requests = []

    def receive(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"data": []})

    args = {
        "is_event": True,
        "statement": "World product is live",
        "certainty": "HIGH",
        "certainty_reason": "Official launch",
        "entities": [{"name": "World", "role": "ACTOR"}],
        "tokens": ["World (WLD)"],
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
                "evidence": [{"document": visible["item"]["document"], "quote": item["text"]}],
            }
        ],
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        service = SessionService(repo, settings, tools.dispatch, http=http)
        await service.tool(session, {"id": "call_record", "name": "record_event", "input": args})
    ledger = await repo.get("ops.tool_calls", "session_private:call_record")
    assert ledger["status"] == "DONE", ledger.get("result")
    event = ledger["result"]
    public = json.loads(requests[-1]["events"][0]["content"][0]["text"])
    assert_public(public)
    for raw in (
        event["event_id"],
        event["accepted_claims"][0]["claim_id"],
        event["accepted_claims"][0]["obs_id"],
    ):
        assert raw not in json.dumps(public)
    claim = public["accepted_claims"][0]["claim"]
    fresh = await repo.get("ops.agent_sessions", session["session_id"])
    boundary = ModelContext(repo, settings, fresh)
    restored = await boundary.arguments("get_decision", {"token": "World (WLD)"})
    assert restored == {"event_id": event["event_id"], "token": "world:WLD"}
    raw = boundary.restore({"dims": {"economic_directness": {"evidence": [claim]}}})
    assert raw["dims"]["economic_directness"]["evidence"] == [event["accepted_claims"][0]["claim_id"]]
    assert event["accepted_claims"][0]["evidence"][0]["item_id"] == item["item_id"]


async def test_duplicate_url_requires_observation_time_and_ambiguous_claim_is_rejected(context):
    repo, settings, session, item = context
    boundary = ModelContext(repo, settings, session)
    second = {**item, "item_id": new_id("itm"), "observed_at": "2026-10-23T00:00:00Z"}
    for row in (item, second):
        await repo.put("ingest.source_items", row["item_id"], row)
    public = await boundary.result("search_tank_items", {"items": [item, second]})
    with pytest.raises(ValueError, match="ambiguous"):
        await boundary.arguments("get_item", {"document": {"url": item["url"]}})
    selected = await boundary.arguments("get_item", {"document": public["items"][1]["document"]})
    assert selected["item_id"] == second["item_id"]
    for subject in ("A", "B"):
        await boundary.project({"claim_id": new_id("clm"), "quote": "Launch", "subject": subject})
    with pytest.raises(ValueError, match="ambiguous"):
        boundary.claim_key({"quote": "Launch"})
    assert boundary.claim_key({"quote": "Launch", "subject": "A"})


async def test_identifiers_cannot_override_current_event_but_search_query_is_allowed(context):
    repo, settings, session, _ = context
    boundary = ModelContext(repo, settings, session)
    for args in ({"event_id": new_id("evt")}, {"nested": {"claimId": "x"}}, {"note": "a" * 64}):
        with pytest.raises(ValueError, match="internal"):
            await boundary.arguments("publish_card", args)
    assert await boundary.arguments("web_search", {"query": "Uniswap announcement"}) == {
        "query": "Uniswap announcement"
    }


async def test_market_results_keep_values_units_coverage_and_discard_audit_companions(context):
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
    assert public["market_truth"][0]["spot"] == {**raw["market_truth"][0]["spot"], "excluded": ["Kraken"]}
    assert public["market_truth"][0]["token"] == "Uniswap (UNI)"
    assert "measurement_metadata" not in json.dumps(public)
    assert raw["market_truth"][0]["mta_id"]


def test_model_tool_schemas_do_not_request_internal_identifiers():
    root = Path(__file__).resolve().parents[1] / "config/skills"

    def visit(schema):
        for key, value in schema.get("properties", {}).items():
            assert not identifier_field(key), key
            visit(value)
        if isinstance(schema.get("items"), dict):
            visit(schema["items"])
        for kind in ("oneOf", "anyOf", "allOf"):
            for branch in schema.get(kind, []):
                visit(branch)

    for path in root.glob("*/tools.json"):
        for tool in json.loads(path.read_text()):
            visit(tool["input_schema"])
    assert 'psql "$TANK_DB_URL"' in (root / "tank-data/SKILL.md").read_text()


async def test_other_events_watches_and_scout_proposals_use_meaningful_descriptions(context):
    repo, settings, session, _ = context
    event = {"event_id": new_id("evt"), "statement": "A separate launch", "public_at": "2026-10-22T10:00:00Z"}
    await repo.put("evt.events", event["event_id"], event)
    session["event_id"] = new_id("evt")
    boundary = ModelContext(repo, settings, session)
    await boundary.result("list_open_events", {"events": [event]})
    assert await boundary.arguments(
        "merge_into", {"event": {"statement": event["statement"], "public_at": event["public_at"]}}
    ) == {"event_id": event["event_id"]}
    condition = {
        "cond_id": new_id("cond"),
        "event_id": session["event_id"],
        "human_text": "Spot coverage becomes complete",
        "expr": {"metric": "dc.class", "op": "==", "value": "HIGH"},
    }
    public = await boundary.result("set_watch_condition", {"condition": condition})
    restored = await boundary.arguments(
        "set_watch_condition", {"action": "DISARM", "condition": public["condition"]["condition"]}
    )
    assert restored == {"action": "DISARM", "event_id": session["event_id"], "cond_id": condition["cond_id"]}
    proposal = {"prp_id": new_id("prp"), "concept_name": "New fee mechanism"}
    other = {"prp_id": new_id("prp"), "concept_name": "Existing fee mechanism"}
    for row in (proposal, other):
        await repo.put("scout.expansion_proposals", row["prp_id"], row)
    session["proposal_id"] = proposal["prp_id"]
    boundary = ModelContext(repo, settings, session)
    message = await boundary.message({"kind": "SCOUT_REVIEW", "proposal_id": proposal["prp_id"]}, None)
    assert_public(message)
    restored = await boundary.arguments(
        "record_review", {"duplicate_of": {"subject": other["concept_name"]}, "evidence": []}
    )
    assert restored["proposal_id"] == proposal["prp_id"] and restored["duplicate_of"] == other["prp_id"]


async def test_registry_uses_names_in_lists_and_arguments(context):
    repo, settings, session, _ = context
    boundary = ModelContext(repo, settings, session)
    projected = await boundary.result("get_registry", settings.load_config("registry"))
    assert_public(projected)
    assert "uniswap:UNI" not in json.dumps(projected)
    assert '"S29"' not in json.dumps(projected)
    assert await boundary.arguments("get_registry", {"entity_or_token": "Uniswap (UNI)"}) == {
        "entity_or_token": "uniswap:UNI"
    }


async def test_source_url_with_external_hash_remains_a_working_citation(context):
    repo, settings, session, item = context
    item["url"] = "https://example.org/release/" + "a" * 40
    boundary = ModelContext(repo, settings, session)
    public = await boundary.result("fetch_page", item)
    assert public["document"]["url"] == item["url"]
    args = await boundary.arguments("get_item", {"document": public["document"]})
    assert args["item_id"] == item["item_id"]


async def test_active_input_binds_origin_even_when_another_source_is_already_queued(context):
    repo, settings, session, item = context
    session.update(
        active_message_id="active",
        last_payload={"kind": "ITEM_ADDED", "item_id": "queued-source"},
        message_log=[{"message_id": "active", "payload": {"kind": "NEW_ITEM", "item_id": item["item_id"]}}],
    )
    result = await ModelContext(repo, settings, session).arguments("record_event", {"is_event": False})
    assert result["item_id"] == item["item_id"]


async def test_identical_claim_wording_can_be_distinguished_by_source_capture(context):
    repo, settings, session, item = context
    boundary = ModelContext(repo, settings, session)
    claims = []
    for observed in ("2026-10-21T10:00:00Z", "2026-10-22T10:00:00Z"):
        document = {**item, "item_id": new_id("itm"), "observed_at": observed}
        await repo.put("ingest.source_items", document["item_id"], document)
        claims.append(
            {
                "claim_id": new_id("clm"),
                "quote": "Product is live.",
                "subject": "Project",
                "predicate": "launched",
                "object": "Product",
                "verified": True,
                "state_claim": "LIVE",
                "evidence": [{"item_id": document["item_id"], "quote": "Product is live."}],
            }
        )
    exposed = await boundary.result("record_event", {"accepted_claims": claims})
    with pytest.raises(ValueError, match="ambiguous"):
        boundary.claim_key({"quote": "Product is live."})
    for original, public in zip(claims, exposed["accepted_claims"], strict=True):
        assert_public(public)
        assert boundary.claim_key(public["claim"]) == original["claim_id"]


async def test_document_without_url_or_title_uses_its_text_and_clock(context):
    repo, settings, session, item = context
    item.pop("url")
    item.pop("title")
    await repo.put("ingest.source_items", item["item_id"], item)
    boundary = ModelContext(repo, settings, session)
    public = await boundary.result("get_item", item)
    assert public["document"] == {"excerpt": item["text"], "observed_at": item["observed_at"]}
    assert await boundary.arguments("get_item", {"document": public["document"]}) == {
        "item_id": item["item_id"]
    }


async def test_verbatim_source_content_survives_identifier_shaped_prose(context):
    repo, settings, session, item = context
    text = "The release includes session_notes.csv and checksum " + "a" * 64
    item.update(text=text, title="agent_status update")
    await repo.put("ingest.source_items", item["item_id"], item)
    boundary = ModelContext(repo, settings, session)
    public = await boundary.result("get_item", item)
    assert public["text"] == text and public["title"] == item["title"]
    assert "raw_sha256" not in public and "item_id" not in public
    await boundary.arguments("web_search", {"query": text})
    restored = await boundary.arguments(
        "record_event",
        {"claims": [{"quote": text, "evidence": [{"document": public["document"], "quote": text}]}]},
    )
    assert restored["claims"][0]["quote"] == text
    assert restored["claims"][0]["evidence"] == [{"item_id": item["item_id"], "quote": text}]

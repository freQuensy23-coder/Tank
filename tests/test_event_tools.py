from datetime import timedelta

import pytest

from tank.event_tools import EventToolService, freshness, validate_schema
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.fixture
async def service(tmp_path):
    repo = MemoryRepository()
    await repo.open()
    settings = Settings(data_dir=tmp_path)
    return EventToolService(repo, settings, None, None)


def extraction():
    now = iso()
    return dict(
        item_id="i",
        is_event=True,
        statement="Product announced",
        certainty="HIGH",
        certainty_reason="Official primary",
        entities=[],
        tokens=["WLD"],
        event_time={"value": now, "precision": "EXACT", "quote": None},
        public_at={"value": now, "precision": "EXACT", "basis": "Primary publication", "quote": None},
        freshness="FRESH",
        claims=[
            dict(
                type="STATE",
                subject="World",
                predicate="announces",
                object="Product",
                state_claim="LIVE",
                directionality="BOTH",
                evidence_class="PRIMARY_FACT",
                quote="Product announced",
            )
        ],
        next_milestone=None,
        checked_sources=[],
    )


async def seed(service):
    x = extraction()
    await service.repo.put(
        "ingest.source_items",
        "i",
        dict(
            item_id="i",
            source_id="S01",
            text="Product announced",
            source_class="PRIMARY",
            observed_at=iso(utcnow() + timedelta(seconds=1)),
        ),
    )
    return x


async def test_quote_and_stage_validation(service):
    x = await seed(service)
    result = await service.dispatch("record_event", x, {"session_id": "s"})
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["event_state"] == "APPROVED"
    assert event["event_time"]["precision"] == "BUCKET_1D"
    assert result["accepted_claims"][0]["verified"]
    assert len(await service.repo.list("evt.state_changes")) == 1


async def test_fabricated_quote_dropped_and_future_time_refused(service):
    x = await seed(service)
    x["claims"][0]["quote"] = "invented evidence"
    result = await service.dispatch("record_event", x, {"session_id": "s"})
    assert not result["accepted_claims"]
    assert result["dropped_claims"][0]["reason"].startswith("V1")
    x["public_at"]["value"] = iso(utcnow() + timedelta(days=1))
    with pytest.raises(ValueError, match="V7"):
        await service.dispatch("record_event", x, {})


async def test_unknown_fields_and_freshness(service):
    with pytest.raises(ValueError, match="unknown fields"):
        await service.dispatch("get_item", {"item_id": "i", "extra": True}, {})
    assert [freshness(n * 3600) for n in [1, 4, 8, 24, 48, 49]] == [
        "FRESH",
        "NORMAL",
        "MEDIUM",
        "POOR",
        "OLD",
        "STALE",
    ]


async def test_denial_rollback_requires_primary(service):
    x = await seed(service)
    result = await service.dispatch("record_event", x, {})
    eid = result["event_id"]
    await service.repo.put(
        "ingest.source_items",
        "secondary",
        dict(
            item_id="secondary",
            source_id="S02",
            text="Product announced",
            source_class="SECONDARY",
            observed_at=iso(utcnow() + timedelta(seconds=1)),
        ),
    )
    args = dict(
        event_id=eid,
        item_id="secondary",
        link="DENIAL",
        claims=x["claims"],
        public_at=x["public_at"],
        freshness="FRESH",
    )
    with pytest.raises(ValueError):
        await service.add_observation(args, {})


async def test_publication_freezes_reproducible_bundle_and_queues_only(service, monkeypatch):
    import tank.analysis.cards as cards
    from tank.util import sha256

    monkeypatch.setattr(cards, "alert_gate", lambda *a, **k: (True, "INITIAL"))
    monkeypatch.setattr(cards, "compose_card", lambda *a, **k: "Frozen card")
    monkeypatch.setattr(cards, "lint_card", lambda *a, **k: [])
    x = await seed(service)
    result = await service.dispatch("record_event", x, {})
    eid = result["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": eid, "class": "STRUCTURAL_SHOCK"}
    )
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "PARTIAL",
            "verdict": "Material partnership",
            "why": ["Defined access", "Relevant actor"],
        },
        {"session_id": "s", "event_id": eid, "call_id": "c"},
    )
    assert result["published"]
    bundle = await service.repo.get("out.evidence_bundles", result["evidence_bundle_id"])
    assert sha256(bundle["payload"]) == result["evidence_bundle_id"]
    assert bundle["payload"]["source_items"][0]["text"] == "Product announced"
    jobs = await service.repo.list("ops.jobs")
    assert len(jobs) == 1
    assert jobs[0]["type"] == "DELIVER"
    assert jobs[0]["payload"]["channel"] == "telegram:alerts"


async def test_fetched_page_retains_original_bytes_before_parse(service):
    from pathlib import Path

    import httpx

    from tank.util import sha256

    original = b"<html><script>hidden code</script><body>Official   evidence</body></html>"

    async def respond(request):
        return httpx.Response(200, content=original, headers={"content-type": "text/html"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service.http = client
        item = await service.dispatch(
            "fetch_page", {"url": "https://example.com/evidence"}, {"session_id": "s"}
        )
    assert Path(item["raw_ref"]).read_bytes() == original
    assert item["raw_sha256"] == sha256(original)
    assert "Official" in item["text"] and "hidden code" not in item["text"]
    assert item["readable_text_sha256"] == sha256(item["text"])
    assert (await service.repo.get("ingest.source_items", item["item_id"]))["raw_sha256"] == sha256(original)


def test_nullable_scores_skip_numeric_constraints():
    validate_schema(None, {"type": ["number", "null"], "minimum": 0, "maximum": 10})


async def test_official_fetched_domain_requires_matching_identity(service):
    assert service.primary_source(
        {"source_id": "WEB", "url": "https://www.sec.gov/news/statement", "source_class": "SECONDARY"},
        {"subject": "SEC"},
    )
    assert not service.primary_source(
        {"source_id": "WEB", "url": "https://www.sec.gov.evil.test/news", "source_class": "SECONDARY"},
        {"subject": "SEC"},
    )
    assert not service.primary_source(
        {"source_id": "WEB", "url": "https://www.sec.gov/news/statement", "source_class": "SECONDARY"},
        {"subject": "World"},
    )
    assert not service.primary_source(
        {"source_id": "WEB", "url": "https://example.org", "source_class": "SECONDARY"}, {"subject": "SEC"}
    )


@pytest.mark.parametrize("backend", ["memory", "postgres"])
async def test_numeric_map_version_ten_removes_old_token(service, backend):
    import os

    from tank.storage import PostgresRepository
    from tank.util import new_id

    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        repo = PostgresRepository(url)
        await repo.open()
        await repo.migrate()
        service.repo = repo
    eid = new_id("evt")
    try:
        await service.repo.put(
            "evt.events", eid, complete_event_fixture(eid, current_ems_id=eid + "ems", event_state="LIVE")
        )
        await service.repo.put(
            "evt.ems_assessments", eid + "ems", complete_assessment_fixture(eid + "ems", eid)
        )
        for version in [9, 10]:
            await service.repo.put(
                "evt.beneficiary_maps",
                f"{eid}:{version}",
                {
                    "event_id": eid,
                    "map_version": version,
                    "analysis_set": ["world:WLD"] if version == 9 else [],
                },
            )
        edgeid = new_id("edge")
        await service.repo.put(
            "evt.beneficiary_edges",
            edgeid,
            {"edge_id": edgeid, "event_id": eid, "map_version": 9, "token_id": "world:WLD"},
        )
        await service.repo.put(
            "ana.market_truth", eid + "mta", complete_truth_fixture(eid + "mta", eid, "world:WLD", iso())
        )
        with pytest.raises(ValueError, match="current-map"):
            await service.get_decision({"event_id": eid, "token": "world:WLD"}, {})
        with pytest.raises(ValueError, match="outside analysis set"):
            await service.get_market_truth({"event_id": eid, "tokens": ["world:WLD"]}, {"call_id": "c"})
    finally:
        if backend == "postgres":
            await service.repo.close()


async def test_real_decision_condition_reuse_expiry_and_consumption(service):
    from tank.analysis.decisions import DecisionEngine

    service.engine = DecisionEngine()
    eid = "condition-event"
    token = "T"
    event = {
        "event_id": eid,
        "current_ems_id": "condition-ems",
        "event_state": "APPROVED",
        "state_verified": True,
        "freshness": "FRESH",
    }
    assessment = {
        "ems_id": "condition-ems",
        "event_id": eid,
        "class": "MAJOR",
        "certainty": "HIGH",
        "impact_horizon": {"impact_start": "NOW"},
    }
    edge = {
        "edge_id": "condition-edge",
        "event_id": eid,
        "map_version": 1,
        "token_id": token,
        "role": "PRIMARY",
        "credible": True,
        "capture_class": "DIRECT",
    }
    truth = {
        "mta_id": "condition-truth",
        "event_id": eid,
        "token_id": token,
        "cutoff": iso(),
        "move_class": "SPOT_LED_ACCUMULATION",
        "data_confidence": {"class": "HIGH", "critical_semantics_verified": True},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL", "anchor": {"ref_price": "10"}},
        "liquidity": {"liquidity_state": "NORMAL"},
        "spot": {},
        "derivatives": {},
    }
    for table, key, row in [
        ("evt.events", eid, event),
        ("evt.ems_assessments", "condition-ems", assessment),
        (
            "evt.beneficiary_maps",
            "condition-map",
            {"event_id": eid, "map_version": 1, "analysis_set": [token]},
        ),
        ("evt.beneficiary_edges", "condition-edge", edge),
        ("ana.market_truth", "condition-truth", truth),
        (
            "ana.calls",
            "condition-call",
            {
                "call_id": "condition-call",
                "event_id": eid,
                "token_id": token,
                "status": "OPEN",
                "opened_at": iso(),
                "invalidation_price": "9",
            },
        ),
    ]:
        await service.repo.put(table, key, row)
    first = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert all(condition["expires_at"] and condition["created_at"] for condition in first["conditions"])
    assert first["action"] == "HOLD"
    armed = next(c for c in first["conditions"] if c["role"] == "UPGRADE")
    armed.update(counters={"root": {"count": 1, "window": 42}})
    await service.repo.put("ana.watch_conditions", armed["cond_id"], armed)
    second = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    same = next(c for c in second["conditions"] if c["role"] == "UPGRADE")
    assert same["cond_id"] == armed["cond_id"]
    assert same["counters"] == armed["counters"]
    same.update(status="FIRED", fired_at=iso())
    await service.repo.put("ana.watch_conditions", same["cond_id"], same)
    fired = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert fired["action"] == "ADD"
    consumed = await service.repo.get("ana.watch_conditions", same["cond_id"])
    assert consumed["consumed_by"] == fired["dec_id"]
    again = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert again["action"] == "HOLD"
    assert len(await service.repo.list("ana.watch_conditions")) == len(first["conditions"])


async def test_primary_confirmation_promotes_certainty_and_preserves_checks(service):
    x = await seed(service)
    x["certainty"] = "MEDIUM"
    x["checked_sources"] = ["Official newsroom searched: NOT FOUND"]
    result = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["certainty"] == "MEDIUM"
    assert event["checked_sources"] == x["checked_sources"]
    confirmation = dict(
        event_id=event["event_id"],
        item_id="i",
        link="CORROBORATION",
        claims=x["claims"],
        public_at=x["public_at"],
        freshness="FRESH",
        certainty="HIGH",
    )
    confirmed = await service.dispatch("add_observation", confirmation, {})
    event = await service.repo.get("evt.events", event["event_id"])
    assert event["certainty"] == "HIGH"
    assert confirmed["accepted_claims"][0]["claim_id"] in event["certainty_reason"]


async def test_numeric_ems_requires_accepted_evidence(service):
    x = await seed(service)
    result = await service.dispatch("record_event", x, {})
    dims = {
        name: {"value": 8, "rationale": "Claimed effect", "evidence": []}
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]
    }
    assessment = {
        "class": "MAJOR",
        "criterion": "M2",
        "criterion_quote": "Product announced",
        "dims": dims,
        "impact_horizon": {"impact_start": "WEEKS"},
    }
    with pytest.raises(ValueError, match="requires accepted claim evidence"):
        await service.set_assessment({"event_id": result["event_id"], "assessment": assessment}, {})


async def test_milestone_exact_escalation_window(service):
    from tank.util import parse_time

    x = await seed(service)
    result = await service.dispatch("record_event", x, {})
    await service.repo.put("reg.sources", "S01", {"source_id": "S01"})
    due = iso(utcnow() + timedelta(days=1))
    await service.dispatch(
        "schedule_milestone",
        {"event_id": result["event_id"], "milestone": "Decision", "due_at": due, "source": "S01"},
        {},
    )
    source = (await service.repo.list("ops.source_escalations", filters={"source_id": "S01"}))[0]
    assert await service.repo.get("reg.sources", "S01") == {"source_id": "S01"}
    assert parse_time(source["starts_at"]) == parse_time(due) - timedelta(minutes=5)
    assert parse_time(source["expires_at"]) == parse_time(due) + timedelta(hours=2)


async def test_assessment_escalates_all_official_sources_in_consumer_table(service):
    from tank.sources import SourceService

    x = await seed(service)
    x["tokens"] = ["world:WLD"]
    x["entities"] = [{"name": "World", "entity_id": "world", "role": "ACTOR"}]
    result = await service.dispatch("record_event", x, {})
    dims = {
        name: {
            "value": None,
            "reason": "NOT_ESTIMABLE",
            "rationale": "Unknown magnitude",
            "evidence": [],
            "proposed_by": "LLM",
            "caps_applied": [],
        }
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]
    }
    assessment = {
        "class": "MAJOR",
        "criterion": "M2",
        "criterion_quote": "Product announced",
        "dims": dims,
        "impact_horizon": {
            "impact_start": "WEEKS",
            "ramp_shape": "GRADUAL",
            "dependencies": [],
            "time_to_materiality": "Unknown",
            "timing_confidence": "LOW",
        },
    }
    scored = await service.dispatch(
        "set_assessment", {"event_id": result["event_id"], "assessment": assessment}, {}
    )
    assert scored["class"] == "MAJOR"
    expected = next(
        e for e in service.settings.load_config("registry")["entities"] if e["entity_id"] == "world"
    )["official_sources"]
    sources = SourceService(service.repo, service.settings)
    for source_id in expected:
        assert await sources.is_escalated(source_id)
    assert not await sources.is_escalated("S01")


@pytest.mark.parametrize("is_event,status", [(True, "BOUND"), (False, "CLOSED")])
async def test_extraction_binds_or_closes_shared_routing_reservation(service, is_event, status):
    x = await seed(service)
    x["is_event"] = is_event
    await service.repo.put(
        "ops.routing_reservations",
        "reservation",
        {"reservation_id": "reservation", "status": "PENDING", "item_ids": ["i", "i2"], "first_item_id": "i"},
    )
    result = await service.dispatch("record_event", x, {"reservation_id": "reservation"})
    reservation = await service.repo.get("ops.routing_reservations", "reservation")
    assert reservation["status"] == status
    assert reservation["item_ids"] == ["i", "i2"]
    if is_event:
        assert reservation["event_id"] == result["event_id"]


async def test_structural_assessment_requires_agent_partial_without_parallel_publisher(service):
    x = await seed(service)
    x["tokens"] = ["world:WLD"]
    x["statement"] = "World access approved"
    x["claims"][0].update(state_claim="APPROVED", quote="World access approved")
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = "World access approved"
    await service.repo.put("ingest.source_items", "i", item)
    extraction = await service.dispatch("record_event", x, {})
    dims = {
        name: {
            "value": None,
            "reason": "NOT_ESTIMABLE",
            "rationale": "Unknown magnitude",
            "evidence": [],
            "proposed_by": "LLM",
            "caps_applied": [],
        }
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]
    }
    assessment = {
        "class": "STRUCTURAL_SHOCK",
        "criterion": "G5",
        "criterion_quote": "World access approved",
        "dims": dims,
        "impact_horizon": {
            "impact_start": "WEEKS",
            "ramp_shape": "GRADUAL",
            "dependencies": [],
            "time_to_materiality": "Unknown",
            "timing_confidence": "LOW",
        },
    }
    scored = await service.dispatch(
        "set_assessment", {"event_id": extraction["event_id"], "assessment": assessment}, {}
    )
    assert scored["class"] == "STRUCTURAL_SHOCK"
    assert scored["partial_required"] and scored["first_card_deadline"]
    assert not await service.repo.list("ops.jobs", filters={"type": "PUBLISH_PARTIAL"})
    args = {
        "event_id": extraction["event_id"],
        "kind": "PARTIAL",
        "verdict": "World access approved",
        "why": [],
    }
    result = await service.dispatch(
        "publish_card", args, {"event_id": extraction["event_id"], "role": "analyst"}
    )
    assert result["published"], result
    alert = await service.repo.get("out.alerts", result["alert_id"] + ":1")
    assert alert["kind"] == "PARTIAL"
    assert alert["linter"]["passed"]
    assert not alert["decisions"]
    assert (
        await service.dispatch("publish_card", args, {"event_id": extraction["event_id"], "role": "analyst"})
    )["reason"] == "FIRST_CARD_ALREADY_GENERATED"


async def test_real_session_tool_refuses_cross_event_mutation(service):
    from tank.agents import SessionService

    await service.repo.put("evt.events", "A", {"event_id": "A", "lifecycle": "DETECTED"})
    original = {"event_id": "B", "lifecycle": "DETECTED", "statement": "Untouched"}
    await service.repo.put("evt.events", "B", original)
    session = {"session_id": "scope-session", "event_id": "A", "item_id": "i"}
    await service.repo.put("ops.agent_sessions", session["session_id"], session)
    sessions = SessionService(service.repo, service.settings, service.dispatch)
    replies = []

    async def request(*args, **kwargs):
        replies.append(kwargs)
        return {}

    sessions.request = request
    await sessions.tool(
        session,
        {
            "id": "scope-call",
            "name": "schedule_milestone",
            "input": {"event_id": "B", "milestone": "Wrong event", "due_at": iso(), "source": "S01"},
        },
    )
    assert await service.repo.get("evt.events", "B") == original
    stored = await service.repo.get("ops.tool_calls", "scope-session:scope-call")
    assert stored["result"]["reason"] == "EVENT_SCOPE"
    assert stored["returned_at"]
    assert replies
    assert not await service.repo.list("ops.source_escalations")
    # Other-event reads are allowed through the same real session wrapper.
    await sessions.tool(session, {"id": "scope-read", "name": "get_event", "input": {"event_id": "B"}})
    assert (await service.repo.get("ops.tool_calls", "scope-session:scope-read"))["result"][
        "event"
    ] == original


async def test_novel_layer_proposal_survives_real_session_transaction(service):
    from tank.agents import SessionService

    x = await seed(service)
    extracted = await service.dispatch("record_event", x, {})
    eid = extracted["event_id"]
    proof = extracted["accepted_claims"][0]["claim_id"]
    session = {"session_id": "novel-session", "event_id": eid, "item_id": "i"}
    await service.repo.put("ops.agent_sessions", session["session_id"], session)
    sessions = SessionService(service.repo, service.settings, service.dispatch)

    async def request(*args, **kwargs):
        return {}

    sessions.request = request
    await sessions.tool(
        session,
        {
            "id": "novel-call",
            "name": "set_beneficiaries",
            "input": {"event_id": eid, "edges": [{"layer": "NOVEL_LAYER", "evidence": [proof]}]},
        },
    )
    ledger = await service.repo.get("ops.tool_calls", "novel-session:novel-call")
    assert ledger["returned_at"]
    assert ledger["result"]["accepted"] is False
    proposals = await service.repo.list("scout.expansion_proposals")
    assert len(proposals) == 1
    assert proposals[0]["hook"] == "BENEFICIARY_LAYER"
    assert proposals[0]["evidence"] == [proof]
    assert len(await service.repo.list("ops.jobs", filters={"type": "SCOUT_REVIEW"})) == 1
    assert not await service.repo.list("evt.beneficiary_edges")


async def test_unbound_analyst_cannot_mutate_before_extraction(service):
    refused = await service.dispatch(
        "set_beneficiaries", {"event_id": "other", "edges": []}, {"role": "analyst", "session_id": "unbound"}
    )
    assert refused["reason"] == "EVENT_SCOPE"


async def test_sama_authority_verifies_world_and_openai_but_not_unrelated_org(service):
    quote = "OpenAI and World approved the integration."
    await service.repo.put(
        "ingest.source_items",
        "sama-post",
        {
            "item_id": "sama-post",
            "source_id": "S29",
            "author": "sama",
            "source_class": "SOCIAL_LEAD",
            "text": quote,
            "observed_at": iso(utcnow() + timedelta(seconds=1)),
        },
    )
    x = extraction()
    x.update(
        item_id="sama-post",
        statement=quote,
        certainty="HIGH",
        certainty_reason="Registered CEO/co-founder speaking about both authorized organizations",
        tokens=["world:WLD"],
        entities=[
            {"name": "World", "entity_id": "world", "role": "ACTOR"},
            {"name": "OpenAI", "entity_id": "openai", "role": "COUNTERPARTY"},
        ],
    )
    x["claims"] = [
        {
            "type": "STATE",
            "subject": subject,
            "predicate": "approved",
            "object": "the integration",
            "state_claim": "APPROVED",
            "directionality": "BOTH",
            "evidence_class": "PRIMARY_FACT",
            "quote": quote,
        }
        for subject in ["World", "OpenAI"]
    ]
    result = await service.dispatch("record_event", x, {})
    assert len(result["accepted_claims"]) == 2
    assert all(claim["verified"] for claim in result["accepted_claims"])
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["certainty"] == "HIGH"
    assert event["state_verified"]
    assert event["event_state"] == "APPROVED"
    assert not service.primary_source(
        {"source_class": "SOCIAL_LEAD", "author": "sama"}, {"subject": "Binance"}
    )


async def test_future_availability_does_not_fabricate_live_or_deployment(service):
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = "Trading opens tomorrow at 09:00 UTC."
    await service.repo.put("ingest.source_items", "i", item)
    x["claims"][0].update(quote=item["text"], state_claim="LIVE")
    result = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["event_state"] == "APPROVED"
    assert event["state_verified"]


async def test_full_requires_entire_analysis_then_pins_diverse_three_tokens(service, monkeypatch):
    import tank.analysis.cards as cards

    monkeypatch.setattr(cards, "alert_gate", lambda *a, **kw: (True, "TEST"))
    monkeypatch.setattr(cards, "compose_card", lambda *a, **kw: "Validated full card")
    monkeypatch.setattr(cards, "lint_card", lambda *a, **kw: [])
    x = await seed(service)
    result = await service.dispatch("record_event", x, {})
    eid = result["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "full-ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments", "full-ems", {"ems_id": "full-ems", "event_id": eid, "class": "MAJOR"}
    )
    tokens = ["P1", "P2", "P3", "S1", "S2", "S3", "L1"]
    await service.repo.put(
        "evt.beneficiary_maps",
        "full-map",
        {"event_id": eid, "map_version": 1, "analysis_set": tokens, "least_repriced_edge": "edge-S2"},
    )
    for index, token in enumerate(tokens):
        role = "PRIMARY" if token.startswith("P") else "SECONDARY" if token.startswith("S") else "LOSER"
        await service.repo.put(
            "evt.beneficiary_edges",
            "edge-" + token,
            {
                "edge_id": "edge-" + token,
                "event_id": eid,
                "map_version": 1,
                "token_id": token,
                "role": role,
                "tradable": True,
                "credible": True,
                "exposure_score": 10 - index,
            },
        )
        if token == "L1":
            continue
        await service.repo.put(
            "ana.market_truth",
            "truth-" + token,
            {
                "mta_id": "truth-" + token,
                "event_id": eid,
                "token_id": token,
                "cutoff": iso(),
                "data_confidence": {"class": "LOW"},
            },
        )
        await service.repo.put(
            "ana.decisions",
            "dec-" + token,
            {"dec_id": "dec-" + token, "event_id": eid, "token_id": token, "cutoff": iso(), "action": "WAIT"},
        )
    args = {
        "event_id": eid,
        "kind": "FULL",
        "verdict": "Approved fact",
        "why": ["Defined consequence", "Evidence available"],
    }
    refusal = await service.dispatch("publish_card", args, {})
    assert refusal["reason"] == "ANALYSIS_INCOMPLETE"
    assert refusal["missing_market_truth"] == ["L1"]
    assert not await service.repo.list("out.alerts")
    await service.repo.put(
        "ana.market_truth",
        "truth-L1",
        {
            "mta_id": "truth-L1",
            "event_id": eid,
            "token_id": "L1",
            "cutoff": iso(),
            "data_confidence": {"class": "LOW"},
        },
    )
    await service.repo.put(
        "ana.decisions",
        "dec-L1",
        {"dec_id": "dec-L1", "event_id": eid, "token_id": "L1", "cutoff": iso(), "action": "WAIT"},
    )
    first = await service.dispatch("publish_card", args, {})
    alert = await service.repo.get("out.alerts", first["alert_id"] + ":1")
    assert alert["shown_tokens"] == ["P1", "S2", "S1"]
    assert set(alert["decisions"]) == {"dec-P1", "dec-S1", "dec-S2"}
    frozen = await service.repo.get("out.evidence_bundles", alert["evidence_bundle_id"])
    assert len(frozen["payload"]["decisions"]) == 7
    await service.repo.put(
        "evt.beneficiary_maps",
        "new-map",
        {"event_id": eid, "map_version": 2, "analysis_set": ["P2"], "least_repriced_edge": None},
    )
    await service.repo.put(
        "evt.beneficiary_edges",
        "new-P2",
        {
            "edge_id": "new-P2",
            "event_id": eid,
            "map_version": 2,
            "token_id": "P2",
            "role": "PRIMARY",
            "tradable": True,
            "credible": True,
            "exposure_score": 10,
        },
    )
    args["kind"] = "UPDATE"
    update = await service.dispatch("publish_card", args, {})
    second = await service.repo.get("out.alerts", update["alert_id"] + ":2")
    assert second["shown_tokens"] == alert["shown_tokens"]
    assert "P2" not in second["shown_tokens"]
    assert {"P1", "S1", "S2"} <= await service.managed_tokens(eid)


async def test_real_expiry_to_update_delivery_never_reopens_call(service, monkeypatch):
    import httpx

    from tank.analysis.decisions import DecisionEngine
    from tank.delivery import DeliveryService
    from tank.scheduler import Scheduler

    service.engine = DecisionEngine()
    eid = "expiry-event"
    token = "T"
    now = utcnow()
    event = {
        "event_id": eid,
        "statement": "Access approved",
        "current_ems_id": "expiry-ems",
        "event_state": "APPROVED",
        "state_verified": True,
        "freshness": "FRESH",
        "certainty": "HIGH",
        "lifecycle": "MONITORING",
        "public_at": iso(now),
        "known_to_us_at": iso(now),
        "event_time": {"value": None, "precision": "UNKNOWN"},
    }
    assessment = {
        "ems_id": "expiry-ems",
        "event_id": eid,
        "class": "MAJOR",
        "criterion": "M2",
        "certainty": "HIGH",
        "impact_horizon": {"impact_start": "WEEKS"},
    }
    edge = {
        "edge_id": "expiry-edge",
        "event_id": eid,
        "map_version": 1,
        "entity_id": "world",
        "token_id": token,
        "role": "PRIMARY",
        "tradable": True,
        "credible": True,
        "capture_class": "DIRECT",
        "exposure_score": 8,
    }
    truth = {
        "mta_id": "expiry-truth",
        "event_id": eid,
        "token_id": token,
        "cutoff": iso(now),
        "move_class": "SPOT_LED_ACCUMULATION",
        "data_confidence": {"class": "HIGH", "critical_semantics_verified": True},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL", "anchor": {"ref_price": "10"}},
        "liquidity": {"liquidity_state": "NORMAL"},
        "spot": {},
        "derivatives": {},
    }
    call = {
        "call_id": "expiry-call",
        "event_id": eid,
        "token_id": token,
        "status": "OPEN",
        "opened_at": iso(now - timedelta(days=8)),
        "invalidation_price": "9",
        "entry_ref_price": "10",
    }
    for table, key, row in [
        ("evt.events", eid, event),
        ("evt.ems_assessments", "expiry-ems", assessment),
        ("evt.beneficiary_maps", "expiry-map", {"event_id": eid, "map_version": 1, "analysis_set": [token]}),
        ("evt.beneficiary_edges", "expiry-edge", edge),
        ("ana.market_truth", "expiry-truth", truth),
        ("ana.calls", "expiry-call", call),
    ]:
        await service.repo.put(table, key, row)
    first_decision = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert first_decision["action"] == "HOLD"
    first = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
            "verdict": "Access approved",
            "why": ["Defined access", "Primary event confirmed"],
        },
        {},
    )
    assert first["published"], first
    service.settings.telegram_token = "test"
    service.settings.telegram_alerts_chat = "1"
    service.settings.telegram_ops_chat = "2"
    sent = []

    async def telegram(request):
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        delivery = DeliveryService(service.repo, service.settings, http=http)
        await delivery.deliver({"alert_id": first["alert_id"], "version": 1})
        stale_event = await service.repo.get("evt.events", eid)
        stale_event["freshness"] = "STALE"
        await service.repo.put("evt.events", eid, stale_event)
        scheduler = Scheduler(service.repo, service.settings, None, None)
        await scheduler.expire_calls(now)
        assert (await service.repo.get("evt.events", eid))["call_expired"]
        lifecycle = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
        assert lifecycle["tracking_ended"] and lifecycle["actionable"] is False
        assert lifecycle["action"] == "HOLD"
        assert not lifecycle["conditions"]
        correction = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "UPDATE",
                "verdict": "Tracking window ended",
                "why": ["Call window expired", "Tracking ended"],
            },
            {},
        )
        assert correction["published"] and correction["deferred"], correction
        repeated = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "UPDATE",
                "verdict": "Tracking window ended",
                "why": ["Call window expired", "Tracking ended"],
            },
            {},
        )
        assert repeated["reason"] == "IDENTICAL_PENDING" and repeated["version"] == correction["version"]
        queued = [
            row
            for row in await service.repo.list("ops.jobs", filters={"type": "DELIVER"})
            if row["payload"]["version"] == 2
        ]
        assert len(queued) == 1 and queued[0]["run_after"] == correction["delivery_not_before"]
        alert = await service.repo.get("out.alerts", correction["alert_id"] + ":2")
        assert alert["rate_limited"] and not alert["delivery_immediate"]
        assert "No longer following" in alert["card_text"]
        assert alert["expired_call_ids"] == ["expiry-call"]
        assert (await service.repo.get("evt.events", eid))["call_expired"]
        from tank.delivery import DeliveryDeferred

        before = len(sent)
        with pytest.raises(DeliveryDeferred) as pending:
            await delivery.deliver({"alert_id": correction["alert_id"], "version": 2})
        assert len(sent) == before
        assert (await service.repo.get("evt.events", eid))["call_expired"]
        import tank.delivery as delivery_module
        from tank.util import parse_time

        monkeypatch.setattr(
            delivery_module, "utcnow", lambda: parse_time(pending.value.run_after) + timedelta(seconds=1)
        )
        await delivery.deliver({"alert_id": correction["alert_id"], "version": 2})
    calls = await service.repo.list("ana.calls", filters={"event_id": eid})
    assert len(calls) == 1
    assert calls[0]["status"] == "CLOSED" and calls[0]["closed_by"] == "EXPIRED"
    assert not (await service.repo.get("evt.events", eid)).get("call_expired")
    again = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert again["tracking_ended"] and not again["actionable"]
    assert (await service.repo.get("ana.decisions", first_decision["dec_id"]))["action"] == "HOLD"


async def test_material_update_deferred_from_last_sent_not_new_pending(service, monkeypatch):
    import httpx

    from tank.analysis.decisions import DecisionEngine
    from tank.delivery import DeliveryDeferred, DeliveryService
    from tank.util import parse_time

    service.engine = DecisionEngine()
    eid = "material-event"
    token = "T"
    now = utcnow()
    records = [
        (
            "evt.events",
            eid,
            {
                "event_id": eid,
                "statement": "Access approved",
                "current_ems_id": "material-ems",
                "event_state": "APPROVED",
                "state_verified": True,
                "freshness": "FRESH",
                "certainty": "HIGH",
                "lifecycle": "MONITORING",
                "public_at": iso(now),
                "known_to_us_at": iso(now),
            },
        ),
        (
            "evt.ems_assessments",
            "material-ems",
            {
                "ems_id": "material-ems",
                "event_id": eid,
                "class": "MAJOR",
                "criterion": "M2",
                "certainty": "HIGH",
                "impact_horizon": {"impact_start": "WEEKS"},
            },
        ),
        (
            "evt.beneficiary_maps",
            "material-map",
            {"event_id": eid, "map_version": 1, "analysis_set": [token]},
        ),
        (
            "evt.beneficiary_edges",
            "material-edge",
            {
                "edge_id": "material-edge",
                "event_id": eid,
                "map_version": 1,
                "entity_id": "world",
                "token_id": token,
                "role": "PRIMARY",
                "tradable": True,
                "credible": True,
                "capture_class": "DIRECT",
                "exposure_score": 8,
            },
        ),
        (
            "ana.calls",
            "material-call",
            {
                "call_id": "material-call",
                "event_id": eid,
                "token_id": token,
                "status": "OPEN",
                "opened_at": iso(now),
                "invalidation_price": "9",
                "entry_ref_price": "10",
            },
        ),
    ]
    truth = {
        "mta_id": "material-truth",
        "event_id": eid,
        "token_id": token,
        "cutoff": iso(now),
        "move_class": "SPOT_LED_ACCUMULATION",
        "data_confidence": {"class": "HIGH", "critical_semantics_verified": True},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL", "anchor": {"ref_price": "10"}},
        "liquidity": {"liquidity_state": "NORMAL"},
        "spot": {},
        "derivatives": {},
    }
    records.append(("ana.market_truth", "material-truth", truth))
    for table, key, row in records:
        await service.repo.put(table, key, row)
    first = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert first["action"] == "HOLD"
    full = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
            "verdict": "Access approved",
            "why": ["Defined access", "Primary event confirmed"],
        },
        {},
    )
    assert full["published"], full
    service.settings.telegram_token = "test"
    service.settings.telegram_alerts_chat = "1"
    service.settings.telegram_ops_chat = "2"
    sent = []

    async def telegram(request):
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        delivery = DeliveryService(service.repo, service.settings, http=http)
        await delivery.deliver({"alert_id": full["alert_id"], "version": 1})
        low = dict(
            truth,
            mta_id="material-low",
            cutoff=iso(),
            data_confidence={"class": "LOW", "critical_semantics_verified": False},
        )
        await service.repo.put("ana.market_truth", "material-low", low)
        reduced = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
        assert reduced["action"] == "REDUCE"
        args = {
            "event_id": eid,
            "kind": "UPDATE",
            "verdict": "Data coverage limited",
            "why": ["Coverage decreased", "Execution confidence lower"],
        }
        pending = await service.dispatch("publish_card", args, {})
        assert pending["published"] and pending["deferred"], pending
        args["verdict"] = "Data coverage remains limited"
        newer = await service.dispatch("publish_card", args, {})
        assert newer["published"] and newer["deferred"] and newer["version"] == 3, newer
        assert newer["delivery_not_before"] == pending["delivery_not_before"]
        await delivery.deliver({"alert_id": pending["alert_id"], "version": 2})
        assert len(sent) == 1
        with pytest.raises(DeliveryDeferred):
            await delivery.deliver({"alert_id": newer["alert_id"], "version": 3})
        import tank.delivery as delivery_module

        monkeypatch.setattr(
            delivery_module, "utcnow", lambda: parse_time(newer["delivery_not_before"]) + timedelta(seconds=1)
        )
        await delivery.deliver({"alert_id": newer["alert_id"], "version": 3})
    assert len(sent) == 2
    assert len(await service.repo.list("ana.calls", filters={"event_id": eid})) == 1


async def test_bundle_keeps_producing_model_and_separate_session_config(service):
    x = await seed(service)
    x["tokens"] = ["world:WLD"]
    x["statement"] = "World access approved"
    x["claims"][0].update(state_claim="APPROVED", quote="World access approved")
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = "World access approved"
    await service.repo.put("ingest.source_items", "i", item)
    session = {
        "session_id": "prior-session",
        "event_id": None,
        "item_id": "i",
        "model_id": "prior-model",
        "prompt_version": "prior-prompt",
        "agent_version": "prior-agent",
        "config_version": "prior-config",
        "code_version": "prior-code",
    }
    await service.repo.put("ops.agent_sessions", "prior-session", session)
    context = dict(session, role="analyst")
    result = await service.dispatch("record_event", x, context)
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["config_version"] == service.settings.config_version
    assert event["session_config_version"] == "prior-config"
    assert event["model_id"] == "prior-model"
    dims = {
        name: {
            "value": None,
            "reason": "NOT_ESTIMABLE",
            "rationale": "Unknown magnitude",
            "evidence": [],
            "proposed_by": "LLM",
            "caps_applied": [],
        }
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]
    }
    assessment = {
        "class": "STRUCTURAL_SHOCK",
        "criterion": "G5",
        "criterion_quote": "World access approved",
        "dims": dims,
        "impact_horizon": {
            "impact_start": "WEEKS",
            "ramp_shape": "GRADUAL",
            "dependencies": [],
            "time_to_materiality": "Unknown",
            "timing_confidence": "LOW",
        },
    }
    await service.dispatch(
        "set_assessment", {"event_id": event["event_id"], "assessment": assessment}, context
    )
    published = await service.dispatch(
        "publish_card",
        {"event_id": event["event_id"], "kind": "PARTIAL", "verdict": "World access approved", "why": []},
        {"event_id": event["event_id"], "role": "system"},
    )
    assert published["published"], published
    bundle = (await service.repo.get("out.evidence_bundles", published["evidence_bundle_id"]))["payload"]
    assert bundle["model_ids"] == ["prior-model"]
    assert service.settings.openma_model not in bundle["model_ids"]
    assert bundle["prompt_versions"] == ["prior-prompt"]
    assert bundle["agent_versions"] == ["prior-agent"]
    assert {"prior-config", service.settings.config_version} <= set(bundle["config_versions"])


async def test_tool_schema_uses_frozen_runtime_snapshot_after_disk_edit(service, tmp_path):
    import shutil

    from tank.settings import resolve_versions

    frozen_dir = tmp_path / "frozen-config"
    shutil.copytree(service.settings.config_dir, frozen_dir)
    service.settings = resolve_versions(Settings(config_dir=frozen_dir, data_dir=tmp_path / "snapshot-data"))
    await seed(service)
    schema = frozen_dir / "skills" / "tank-event-analyst" / "get_item.schema.json"
    schema.write_text("{invalid edited JSON")
    item = await service.dispatch("get_item", {"item_id": "i"}, {})
    assert item["item_id"] == "i"
    with pytest.raises(ValueError, match="unknown fields"):
        await service.dispatch("get_item", {"item_id": "i", "injected": True}, {})


async def test_market_truth_domain_result_and_evidence_bundle_keep_full_metadata(service):
    import asyncio

    from tank.analysis.truth import compute_market_truth

    cutoff = "2026-10-16T12:00:00Z"
    x = await seed(service)
    extracted = await service.dispatch("record_event", x, {})
    eid = extracted["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "metadata-ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "metadata-ems",
        {"ems_id": "metadata-ems", "event_id": eid, "class": "STRUCTURAL_SHOCK", "criterion": "G5"},
    )
    await service.repo.put(
        "evt.beneficiary_maps", "metadata-map", {"event_id": eid, "map_version": 1, "analysis_set": ["T"]}
    )
    future = {
        "instrument_id": "binance:spot:T",
        "minute_start": "2026-10-17T12:00:00Z",
        "buy_qty": 1,
        "sell_qty": 0,
        "sum_sq_qty": 1,
        "complete": True,
    }
    expected = compute_market_truth(
        {"token_id": "T", "event_id": eid, "cutoff": cutoff, "flow_bars": [future]}
    )
    expected["mta_id"] = "metadata-mta"
    task = asyncio.create_task(
        service.dispatch(
            "get_market_truth",
            {"event_id": eid, "tokens": ["T"], "cutoff": cutoff},
            {"call_id": "metadata-call"},
        )
    )
    try:
        while not task.done():
            job = await service.repo.claim_job()
            if job:
                if job["type"] == "RUN_MARKET_TRUTH":
                    await service.repo.put("ana.market_truth", "metadata-mta", expected)
                    stored = await service.repo.get("ops.jobs", job["job_id"])
                    stored["result"] = expected
                    await service.repo.put("ops.jobs", job["job_id"], stored)
                await service.repo.finish_job(job["job_id"])
            await asyncio.sleep(0.01)
        full = (await task)["market_truth"][0]
    finally:
        if not task.done():
            task.cancel()
    assert full == expected and full["measurement_metadata"]
    assert full["spot"]["W1H"]["coverage"]["n_complete"] == 0
    published = await service.dispatch(
        "publish_card", {"event_id": eid, "kind": "PARTIAL", "verdict": "Approved fact", "why": []}, {}
    )
    assert published["published"], published
    bundle = (await service.repo.get("out.evidence_bundles", published["evidence_bundle_id"]))["payload"]
    assert bundle["market_truth"][0] == expected
    assert (await service.repo.get("ana.market_truth", "metadata-mta")) == expected


@pytest.mark.parametrize("backend", ["memory", "postgres"])
async def test_long_history_uses_bounded_current_snapshots_and_exact_refs(service, backend):
    import os

    from tank.analysis.decisions import DecisionEngine
    from tank.storage import PostgresRepository
    from tank.util import new_id

    base = service.repo
    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        base = PostgresRepository(url)
        await base.open()
        await base.migrate()

    class Guarded:
        def __init__(self, repo):
            self.repo = repo
            self.latest = []
            self.exact = []

        def __getattr__(self, key):
            return getattr(self.repo, key)

        async def list(self, table, **kwargs):
            assert table not in {"ana.market_truth", "ana.decisions"}, "Whole heavy-history scan forbidden"
            return await self.repo.list(table, **kwargs)

        def iter_all(self, table, **kwargs):
            assert table not in {"ana.market_truth", "ana.decisions", "out.alerts"}, (
                "Heavy history must use bounded selectors"
            )
            if table in {"ops.tool_calls", "ops.agent_sessions", "out.deliveries"}:
                assert kwargs.get("filters"), "Global audit/session scan forbidden"
            return self.repo.iter_all(table, **kwargs)

        async def latest_before(self, table, time_field, cutoff, **kwargs):
            self.latest.append((table, kwargs.get("filters")))
            return await self.repo.latest_before(table, time_field, cutoff, **kwargs)

        async def get(self, table, key):
            self.exact.append((table, key))
            return await self.repo.get(table, key)

    guarded = Guarded(base)
    service.repo = guarded
    service.engine = DecisionEngine()
    eid = new_id("bounded")
    token = "T"
    now = utcnow()
    ems = eid + "-ems"
    current = eid + "-current"
    try:
        await base.put("evt.events", eid, complete_event_fixture(eid, current_ems_id=ems))
        await base.put("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid))
        await base.put(
            "evt.beneficiary_maps", eid + "-map", {"event_id": eid, "map_version": 1, "analysis_set": [token]}
        )
        await base.put(
            "evt.beneficiary_edges",
            eid + "-edge",
            {
                "edge_id": eid + "-edge",
                "event_id": eid,
                "map_version": 1,
                "entity_id": "world",
                "token_id": token,
                "role": "PRIMARY",
                "tradable": True,
                "credible": True,
                "capture_class": "DIRECT",
                "exposure_score": 8,
            },
        )
        historic_truth = complete_truth_fixture(eid + "-history", eid, token, iso(now - timedelta(days=2)))
        historic_decision = complete_decision_fixture(
            eid + "-history", eid, token, iso(now - timedelta(days=2)), eid + "-history"
        )
        async with base.transaction():
            for index in range(1001):
                key = eid + f"-historic-{index}"
                await base.put(
                    "ana.market_truth",
                    key,
                    dict(historic_truth, mta_id=key, cutoff=iso(now - timedelta(days=2, seconds=index))),
                )
                await base.put(
                    "ana.decisions",
                    key,
                    dict(
                        historic_decision,
                        dec_id=key,
                        mta_id=key,
                        market_truth_id=key,
                        cutoff=iso(now - timedelta(days=2, seconds=index)),
                    ),
                )
        truth = {
            "mta_id": current,
            "event_id": eid,
            "token_id": token,
            "cutoff": iso(now),
            "move_class": "SPOT_LED_ACCUMULATION",
            "data_confidence": {"class": "HIGH", "critical_semantics_verified": True},
            "relative": {
                "repricing_band": "EARLY",
                "market_context": "NEUTRAL",
                "anchor": {"ref_price": "10"},
            },
            "liquidity": {"liquidity_state": "NORMAL"},
            "spot": {},
            "derivatives": {},
        }
        truth = dict(complete_truth_fixture(current, eid, token, iso(now)), **truth)
        await base.put("ana.market_truth", current, truth)
        await base.put(
            "ana.market_truth",
            eid + "-future",
            dict(
                truth, mta_id=eid + "-future", cutoff=iso(now + timedelta(days=1)), move_class="DISTRIBUTION"
            ),
        )
        decision = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
        assert decision["market_truth_id"] == current
        published = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "FULL",
                "verdict": "Access approved",
                "why": ["Defined access", "Primary event confirmed"],
            },
            {},
        )
        assert published["published"], published
        assert ("ana.market_truth", current) in guarded.exact
        assert sum(table == "ana.market_truth" for table, _ in guarded.latest) == 1
        bundle = (await base.get("out.evidence_bundles", published["evidence_bundle_id"]))["payload"]
        assert [row["mta_id"] for row in bundle["market_truth"]] == [current]
        assert [row["dec_id"] for row in bundle["decisions"]] == [decision["dec_id"]]
    finally:
        if backend == "postgres":
            await base.close()


async def test_duckduckgo_results_are_durable_free_search_evidence(service):
    from pathlib import Path

    from tank.search import SearchResponse
    from tank.util import canonical_json, sha256

    raw = {
        "title": "Official release",
        "href": "https://www.sec.gov/news/release",
        "body": "The SEC approved the filing.",
    }

    class Search:
        async def search(self, query, *, on_attempt):
            assert query == "SEC filing"
            await on_attempt(1)
            await on_attempt(2)
            return SearchResponse([raw], 2)

    service.search_service = Search()
    assert not hasattr(service.settings, "tavily_api_key")
    result = await service.dispatch(
        "web_search", {"query": "SEC filing"}, {"session_id": "search-session", "call_id": "search-call"}
    )
    item = result["items"][0]
    assert result["provider"] == "duckduckgo" and result["attempts"] == 2
    assert item["url"] == raw["href"] and item["title"] == raw["title"] and item["text"] == raw["body"]
    assert Path(item["raw_ref"]).read_bytes() == canonical_json(raw).encode()
    assert item["raw_sha256"] == sha256(canonical_json(raw).encode())
    assert item["evidence_kind"] == "SEARCH_RESULT"
    assert not service.primary_source(item, {"subject": "SEC"})
    usage = await service.repo.list("ops.llm_usage")
    assert len(usage) == 2 and all(
        row["billing_basis"] == "FREE" and row["cost_usd"] == "0" and row["web_searches"] == 1
        for row in usage
    )


async def test_search_failure_does_not_mutate_medium_event_certainty(service):
    from tank.search import SearchError

    x = await seed(service)
    x["certainty"] = "MEDIUM"
    created = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", created["event_id"])

    class Search:
        async def search(self, query, *, on_attempt):
            await on_attempt(1)
            raise SearchError("BLOCKED", "DuckDuckGo blocked request", retryable=True, attempts=1)

    service.search_service = Search()
    result = await service.dispatch(
        "web_search", {"query": "official confirmation"}, {"event_id": created["event_id"]}
    )
    assert (
        result["items"] == []
        and result["search"]["certainty"] == "MEDIUM"
        and result["search"]["verification"] == "UNCONFIRMED"
    )
    assert await service.repo.get("evt.events", created["event_id"]) == event
    assert len(await service.repo.list("evt.event_observations")) == 1


def unknown_assessment(criterion="M2", klass="MAJOR", quote="Product announced"):
    dims = {
        name: {
            "value": None,
            "reason": "NOT_ESTIMABLE",
            "rationale": "Not estimable from available evidence",
            "evidence": [],
            "proposed_by": "LLM",
            "caps_applied": [],
        }
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]
    }
    return {
        "class": klass,
        "criterion": criterion,
        "criterion_quote": quote,
        "dims": dims,
        "impact_horizon": {
            "impact_start": "WEEKS",
            "ramp_shape": "GRADUAL",
            "dependencies": [],
            "time_to_materiality": "Unknown",
            "timing_confidence": "LOW",
        },
    }


async def test_primary_approved_edge_is_credible_and_exposure_layer_reasons_checked(service):
    from tank.event_tools import LAYERS

    x = await seed(service)
    x["tokens"] = ["world:WLD"]
    result = await service.dispatch("record_event", x, {})
    eid = result["event_id"]
    proof = result["accepted_claims"][0]["claim_id"]
    await service.repo.put(
        "evt.capture_chains",
        "credible-capture",
        {
            "event_id": eid,
            "token_id": "world:WLD",
            "version": 1,
            "capture_score": 8,
            "capture_class": "DIRECT",
            "token_mechanism": {"evidence": [proof]},
        },
    )
    edge = {
        "entity_id": "world",
        "token_id": "world:WLD",
        "role": "PRIMARY",
        "layer": "ISSUER_PRODUCT",
        "direction": "POSITIVE",
        "causal_chain": "Required direct rail",
        "directionality": "BOTH",
        "exposure_score": 6,
        "edge_certainty": "HIGH",
        "evidence": [proof],
    }
    args = {
        "event_id": eid,
        "edges": [edge],
        "layer_reasons": {layer: "No supported intermediary" for layer in LAYERS},
        "layer_statuses": {layer: "NONE" for layer in LAYERS},
        "no_credible_secondary_reason": "No adopted intermediary",
    }
    mapping = await service.dispatch("set_beneficiaries", args, {})
    stored = (await service.repo.list("evt.beneficiary_edges", filters={"event_id": eid}))[0]
    assert stored["credible"] and (await service.repo.get("evt.events", eid))["event_state"] == "APPROVED"
    assert mapping["layers"]["ISSUER_PRODUCT"]["status"] == "FOUND"
    assert mapping["layers"]["ORACLE_DATA"]["status"] == "NONE"
    assert mapping["no_credible_secondary_reason"] == "No adopted intermediary"
    args["edges"][0]["exposure_score"] = 11
    with pytest.raises(ValueError):
        await service.dispatch("set_beneficiaries", args, {})


async def test_material_criterion_cannot_pass_major_and_measured_headline_only_caps(service):
    x = await seed(service)
    x["tokens"] = ["world:WLD"]
    created = await service.dispatch("record_event", x, {})
    assessment = unknown_assessment("MATERIAL")
    scored = await service.dispatch(
        "set_assessment", {"event_id": created["event_id"], "assessment": assessment}, {}
    )
    assert scored["class"] == "MATERIAL"
    measured = {
        "value": None,
        "unit": "USD",
        "window": "EVENT",
        "observed_at": None,
        "precision": None,
        "coverage": None,
        "reason": "NOT_ESTIMABLE",
    }
    chain = {
        name: dict(measured)
        for name in [
            "headline_activity",
            "eligible_activity",
            "effective_fees",
            "protocol_capture",
            "token_flow_annual_usd",
            "token_flow_pct_mcap",
        ]
    }
    chain["headline_activity"].update(value="1000000", reason=None)
    chain.update(
        token_id="world:WLD",
        capture_score=10,
        token_mechanism={"type": "UNKNOWN", "active": False, "evidence": []},
    )
    assessment = unknown_assessment()
    assessment["capture_chains"] = [chain]
    await service.dispatch("set_assessment", {"event_id": created["event_id"], "assessment": assessment}, {})
    capture = (await service.repo.list("evt.capture_chains"))[0]
    assert capture["capture_score"] == 3 and "C1" in capture["caps_applied"]


async def test_other_tank_item_quote_read_through_real_session_is_admissible(service):
    from tank.agents import SessionService

    x = await seed(service)
    await service.repo.put(
        "ingest.source_items",
        "other-primary",
        {
            "item_id": "other-primary",
            "source_id": "S01",
            "source_class": "PRIMARY",
            "text": "Fee switch approved.",
            "observed_at": iso(),
        },
    )
    session = {"session_id": "item-proof-session", "event_id": None, "item_id": "i"}
    await service.repo.put("ops.agent_sessions", session["session_id"], session)
    sessions = SessionService(service.repo, service.settings, service.dispatch)

    async def request(*a, **kw):
        return {}

    sessions.request = request
    await sessions.tool(
        session, {"id": "read-other", "name": "get_item", "input": {"item_id": "other-primary"}}
    )
    x["claims"][0].update(quote="Fee switch approved.", state_claim="APPROVED")
    await sessions.tool(session, {"id": "extract", "name": "record_event", "input": x})
    result = (await service.repo.get("ops.tool_calls", "item-proof-session:extract"))["result"]
    assert result["accepted_claims"][0]["evidence"] == [
        {"item_id": "other-primary", "quote": "Fee switch approved."}
    ]
    assert result["accepted_claims"][0]["verified"]


async def test_milestone_official_outcome_quote_clears_pending_and_preserves_history(service):
    x = await seed(service)
    created = await service.dispatch("record_event", x, {})
    eid = created["event_id"]
    due = iso()
    await service.dispatch(
        "schedule_milestone", {"event_id": eid, "milestone": "Approval", "due_at": due, "source": "S01"}, {}
    )
    quote = "The SEC approved the application."
    await service.repo.put(
        "ingest.source_items",
        "milestone-proof",
        {
            "item_id": "milestone-proof",
            "source_id": "S01",
            "source_class": "PRIMARY",
            "text": quote,
            "observed_at": iso(utcnow() + timedelta(seconds=1)),
        },
    )
    claim = dict(x["claims"][0], quote=quote, state_claim="APPROVED", type="MILESTONE")
    await service.dispatch(
        "add_observation",
        {
            "event_id": eid,
            "item_id": "milestone-proof",
            "link": "CORROBORATION",
            "claims": [claim],
            "certainty": "HIGH",
            "public_at": {"value": due, "precision": "EXACT", "basis": "Official release", "quote": None},
            "freshness": "FRESH",
            "milestone_outcome": {"outcome": "Application approved", "quote": quote},
        },
        {},
    )
    event = await service.repo.get("evt.events", eid)
    assert event["next_milestone"] is None and event["milestone_resolved"]
    assert event["milestone_resolutions"][-1]["outcome"] == "Application approved"
    assert event["milestone_resolutions"][-1]["quotes"] == [quote]
    assert all(row["status"] == "CONFIRMED" for row in await service.repo.list("ops.source_escalations"))


async def test_scout_review_records_four_scores_and_merges_named_duplicate(service):
    first = await service.propose_pattern(
        {"hook": "BENEFICIARY_LAYER", "subject": "Concept A", "evidence": ["claim-A"]}, {}
    )
    second = await service.propose_pattern(
        {"hook": "BENEFICIARY_LAYER", "subject": "Equivalent wording", "evidence": ["claim-B"]}, {}
    )
    review = {
        "proposal_id": second["prp_id"],
        "verdict": "Already represented",
        "reason": "Same observable mechanism",
        "evidence": ["claim-B"],
        "scores": {"overlap": 10, "observability": 8, "reusability": 4, "decision_value": 2},
        "recommendation": "REJECT",
        "duplicate_of": first["prp_id"],
    }
    result = await service.dispatch(
        "record_review", review, {"role": "scout", "proposal_id": second["prp_id"]}
    )
    assert result["adversarial_review"]["scores"] == review["scores"] and result["recommendation"] == "REJECT"
    target = await service.repo.get("scout.expansion_proposals", first["prp_id"])
    assert target["occurrences"] == 2 and set(target["evidence"]) == {"claim-A", "claim-B"}


@pytest.mark.parametrize("lifecycle", ["CLOSED", "MERGED"])
async def test_terminal_event_cannot_publish_new_card(service, lifecycle):
    x = await seed(service)
    created = await service.dispatch("record_event", x, {})
    eid = created["event_id"]
    event = await service.repo.get("evt.events", eid)
    event.update(lifecycle=lifecycle, current_ems_id="terminal-ems")
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "terminal-ems",
        {"ems_id": "terminal-ems", "event_id": eid, "class": "STRUCTURAL_SHOCK"},
    )
    result = await service.dispatch(
        "publish_card", {"event_id": eid, "kind": "PARTIAL", "verdict": "Approved fact", "why": []}, {}
    )
    assert not result["published"] and result["reason"] == "TERMINAL_EVENT"


async def test_credential_evidence_rejected_before_network_or_raw_persistence(service):
    import httpx

    sentinel = "AUDIT_FAKE_QUERY_KEY"
    service.settings.etherscan_api_key = sentinel
    calls = []

    async def respond(request):
        calls.append(request)
        return httpx.Response(200, text="Public response")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service.http = client
        with pytest.raises(ValueError, match="Credential-bearing URLs") as error:
            await service.dispatch(
                "fetch_page",
                {"url": "https://api.etherscan.io/v2/api?apikey=" + sentinel + "&module=account"},
                {},
            )
    assert sentinel not in str(error.value) and not calls
    assert not await service.repo.list("ingest.source_items")
    assert not (service.settings.data_dir / "raw").exists()
    with pytest.raises(ValueError, match="Credential-bearing content"):
        await service.save_web(
            "https://example.com", sentinel, "Public page", {}, raw_bytes=sentinel.encode()
        )


async def test_snapshot_vote_end_creates_single_canonical_milestone_after_extraction(service):
    x = await seed(service)
    due = iso(utcnow() + timedelta(days=2))
    item = await service.repo.get("ingest.source_items", "i")
    item.update(source_id="S24", end=due, proposal_id="snapshot-proposal", state="active")
    await service.repo.put("ingest.source_items", "i", item)
    created = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", created["event_id"])
    assert event["next_milestone"] == {"kind": "Snapshot vote ends", "due_at": due, "source_id": "S24"}
    jobs = await service.repo.list("ops.jobs", filters={"type": "MILESTONE_CHECK"})
    assert len(jobs) == 1
    await service.dispatch(
        "schedule_milestone",
        {"event_id": event["event_id"], "milestone": "Snapshot vote ends", "due_at": due, "source": "S24"},
        {},
    )
    assert len(await service.repo.list("ops.jobs", filters={"type": "MILESTONE_CHECK"})) == 1


def complete_event_fixture(event_id, **changes):
    now = iso()
    row = {
        "event_id": event_id,
        "statement": "Access approved",
        "theme_tags": [],
        "entities": [],
        "tokens_mentioned": ["T"],
        "public_at": now,
        "public_at_precision": "EXACT",
        "public_at_basis": "Synthetic fixture primary publication",
        "known_to_us_at": now,
        "event_state": "APPROVED",
        "state_verified": True,
        "certainty": "HIGH",
        "timing_confidence": "HIGH",
        "best_source_class": "PRIMARY",
        "freshness": "FRESH",
        "lifecycle": "MONITORING",
        "config_version": "fixture",
        "code_version": "fixture",
    }
    return dict(row, **changes)


def complete_assessment_fixture(ems_id, event_id, **changes):
    row = dict(
        unknown_assessment(),
        ems_id=ems_id,
        event_id=event_id,
        version=1,
        cutoff=iso(),
        structural_gates=[],
        class_reasons=["Synthetic fixture primary fact"],
        config_version="fixture",
        code_version="fixture",
        model_id="fixture-model",
        prompt_version="fixture-prompt",
    )
    return dict(row, **changes)


def complete_truth_fixture(mta_id, event_id, token_id, cutoff, **changes):
    from tank.analysis.truth import compute_market_truth

    row = compute_market_truth({"token_id": token_id, "event_id": event_id, "cutoff": cutoff})
    row.pop("measurement_metadata", None)
    row.update(mta_id=mta_id, config_version="fixture", code_version="fixture")
    return dict(row, **changes)


def complete_decision_fixture(dec_id, event_id, token_id, cutoff, mta_id):
    from tank.analysis.decisions import DecisionEngine

    event = complete_event_fixture(event_id)
    assessment = complete_assessment_fixture("fixture-ems", event_id)
    truth = complete_truth_fixture(mta_id, event_id, token_id, cutoff)
    edge = {"token_id": token_id, "role": "PRIMARY", "credible": False, "capture_class": "UNKNOWN"}
    row = DecisionEngine().evaluate(event, assessment, edge, truth)
    row.update(
        dec_id=dec_id,
        mta_id=mta_id,
        market_truth_id=mta_id,
        trade_reasons=[row["trade_rule"]],
        config_version="fixture",
        code_version="fixture",
    )
    return row


async def test_real_event_tool_producers_satisfy_postgres_typed_contract(service, tmp_path):
    import os

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import compute_market_truth
    from tank.event_tools import LAYERS
    from tank.storage import PostgresRepository
    from tank.util import new_id, sha256

    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
    repo = PostgresRepository(url)
    await repo.open()
    await repo.migrate()
    service.repo = repo
    service.engine = DecisionEngine()
    item_id = new_id("item")
    session_id = new_id("session")
    stamp = iso()
    quote = "World access approved"
    raw = tmp_path / (item_id + ".txt")
    raw.write_text(quote)
    try:
        await repo.put(
            "ingest.source_items",
            item_id,
            {
                "item_id": item_id,
                "source_id": "S01",
                "feed": "fixture:official",
                "external_id": item_id,
                "published_at": stamp,
                "published_prec": "EXACT",
                "observed_at": stamp,
                "raw_ref": str(raw),
                "raw_sha256": sha256(quote),
                "content_hash": sha256(quote),
                "source_class": "PRIMARY",
                "text": quote,
                "title": quote,
                "extraction_status": "PENDING",
                "relevance": {"decision": "ALWAYS"},
            },
        )
        x = extraction()
        x.update(
            item_id=item_id,
            statement=quote,
            tokens=["world:WLD"],
            entities=[{"name": "World", "entity_id": "world", "role": "SUBJECT"}],
            public_at={
                "value": stamp,
                "precision": "EXACT",
                "basis": "Synthetic primary fixture publication",
                "quote": quote,
            },
        )
        x["claims"][0].update(quote=quote, state_claim="APPROVED")
        context = {
            "session_id": session_id,
            "model_id": "fixture-model",
            "prompt_version": "fixture-prompt",
            "role": "analyst",
        }
        created = await service.dispatch("record_event", x, context)
        eid = created["event_id"]
        context["event_id"] = eid
        await service.dispatch(
            "set_assessment", {"event_id": eid, "assessment": unknown_assessment(quote=quote)}, context
        )
        proof = created["accepted_claims"][0]["claim_id"]
        edge = {
            "entity_id": "world",
            "token_id": "world:WLD",
            "role": "PRIMARY",
            "layer": "ISSUER_PRODUCT",
            "direction": "POSITIVE",
            "causal_chain": "Primary approved access",
            "directionality": "BOTH",
            "exposure_score": 6,
            "edge_certainty": "HIGH",
            "evidence": [proof],
        }
        await service.dispatch(
            "set_beneficiaries",
            {
                "event_id": eid,
                "edges": [edge],
                "layer_reasons": {layer: "No intermediary evidence" for layer in LAYERS},
            },
            context,
        )
        truth = compute_market_truth({"event_id": eid, "token_id": "world:WLD", "cutoff": iso()})
        truth["mta_id"] = new_id("mta")
        await repo.put("ana.market_truth", truth["mta_id"], truth)
        decision = await service.dispatch("get_decision", {"event_id": eid, "token": "world:WLD"}, context)
        assert decision["mta_id"] == truth["mta_id"] and decision["trade_reasons"]
        published = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "FULL",
                "verdict": quote,
                "why": ["Access approved", "Execution data unavailable"],
            },
            context,
        )
        assert published["published"], published
        assert (await repo.get("out.alerts", published["alert_id"] + ":1"))["shown_tokens"] == ["world:WLD"]
        assert (await repo.get("evt.events", eid))[
            "public_at_basis"
        ] == "Synthetic primary fixture publication"
    finally:
        await repo.close()


async def test_official_search_snippet_cannot_high_but_fetched_same_fact_can(service):
    import httpx

    from tank.search import SearchResponse

    quote = "SEC approved the application."

    class Search:
        async def search(self, query, *, on_attempt):
            await on_attempt(1)
            return SearchResponse(
                [{"title": "SEC release", "href": "https://www.sec.gov/news/release", "body": quote}], 1
            )

    service.search_service = Search()
    found = await service.dispatch(
        "web_search", {"query": "SEC approval"}, {"session_id": "certainty-session"}
    )
    x = extraction()
    x.update(
        item_id=found["items"][0]["item_id"],
        statement=quote,
        entities=[{"name": "SEC", "entity_id": "sec", "role": "REGULATOR"}],
    )
    x["public_at"]["value"] = found["items"][0]["observed_at"]
    x["claims"] = [
        {
            "type": "STATE",
            "subject": "SEC",
            "predicate": "approved",
            "object": "the application",
            "state_claim": "APPROVED",
            "directionality": "BOTH",
            "evidence_class": "PRIMARY_FACT",
            "quote": quote,
        }
    ]
    created = await service.dispatch("record_event", x, {"session_id": "certainty-session"})
    event = await service.repo.get("evt.events", created["event_id"])
    assert event["certainty"] == "MEDIUM" and "HIGH not verified" in event["certainty_reason"]
    assert not created["accepted_claims"][0]["verified"]

    async def page(request):
        return httpx.Response(
            200, text="<html><body>" + quote + "</body></html>", headers={"content-type": "text/html"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(page)) as http:
        service.http = http
        fetched = await service.dispatch(
            "fetch_page", {"url": "https://www.sec.gov/news/release"}, {"session_id": "certainty-session"}
        )
    await service.dispatch(
        "add_observation",
        {
            "event_id": created["event_id"],
            "item_id": fetched["item_id"],
            "link": "CORROBORATION",
            "claims": x["claims"],
            "certainty": "HIGH",
            "public_at": x["public_at"],
            "freshness": "FRESH",
        },
        {"session_id": "certainty-session", "event_id": created["event_id"]},
    )
    verified = await service.repo.get("evt.events", created["event_id"])
    assert verified["certainty"] == "HIGH" and verified["state_verified"]


async def test_irrelevant_verified_fact_cannot_raise_event_certainty(service):
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = "SEC was founded in 1934."
    await service.repo.put("ingest.source_items", "i", item)
    x.update(
        statement="SEC approved the application.",
        entities=[{"name": "SEC", "entity_id": "sec", "role": "REGULATOR"}],
    )
    x["claims"] = [
        {
            "type": "FACT",
            "subject": "SEC",
            "predicate": "was founded",
            "object": "1934",
            "state_claim": None,
            "directionality": "NONE",
            "evidence_class": "PRIMARY_FACT",
            "quote": item["text"],
        }
    ]
    created = await service.dispatch("record_event", x, {})
    assert created["accepted_claims"][0]["verified"]
    assert (await service.repo.get("evt.events", created["event_id"]))["certainty"] == "MEDIUM"


@pytest.mark.parametrize("backend", ["memory", "postgres"])
async def test_truth_job_exact_spec_keys_priorities_and_cross_call_idempotency(service, backend):
    import asyncio
    import os

    from tank.analysis.truth import compute_market_truth
    from tank.storage import PostgresRepository
    from tank.util import canonical_json, new_id, parse_time

    base = service.repo
    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        base = PostgresRepository(url)
        await base.open()
        await base.migrate()
        service.repo = base
    eid = new_id("event")
    ems = eid + "-ems"
    tokens = ["world:WLD", "uniswap:UNI"]
    cutoff = "2026-10-16T12:00:00Z"
    tasks = []

    async def jobs(kind):
        return [
            row
            async for row in base.iter_all("ops.jobs", filters={"type": kind})
            if row["payload"].get("event_id") == eid
        ]

    async def wait_jobs(kind, count):
        async with asyncio.timeout(5):
            while len(found := await jobs(kind)) < count:
                await asyncio.sleep(0.01)
        return found

    async def finish_truth(job):
        payload = job["payload"]
        truth = compute_market_truth(
            {"event_id": eid, "token_id": payload["token_id"], "cutoff": payload["cutoff"]}
        )
        truth["mta_id"] = new_id("mta")
        await base.put("ana.market_truth", truth["mta_id"], truth)
        current = await base.get("ops.jobs", job["job_id"])
        current.update(status="RUNNING", attempts=current["attempts"] + 1, result=truth)
        await base.put("ops.jobs", job["job_id"], current)
        await base.finish_job(job["job_id"])
        return truth

    async def finish_escalation(job):
        current = await base.get("ops.jobs", job["job_id"])
        current.update(status="RUNNING", attempts=current["attempts"] + 1)
        await base.put("ops.jobs", job["job_id"], current)
        await base.finish_job(job["job_id"])

    try:
        await base.put("evt.events", eid, complete_event_fixture(eid, current_ems_id=ems))
        await base.put("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid))
        await base.put(
            "evt.beneficiary_maps",
            eid + ":1",
            {"event_id": eid, "map_version": 1, "analysis_set": tokens, "cutoff": iso()},
        )
        started = utcnow()
        for call in ["model-call-one"]:
            tasks.append(
                asyncio.create_task(
                    service.dispatch(
                        "get_market_truth",
                        {"event_id": eid, "tokens": [tokens[0]], "cutoff": cutoff},
                        {"call_id": call},
                    )
                )
            )
        runs = await wait_jobs("RUN_MARKET_TRUTH", 1)
        escalation = (await wait_jobs("ESCALATE_TOKENS", 1))[0]
        queued_at = utcnow()
        assert escalation["idem_key"] == tokens[0] + ":" + eid
        assert escalation["priority"] == 0 and escalation["expires_at"] is None
        run = runs[0]
        assert run["idem_key"] == eid + ":" + tokens[0] + ":" + cutoff
        assert run["priority"] == 1 and run["max_attempts"] == 2
        assert (
            started + timedelta(minutes=30)
            <= parse_time(run["expires_at"])
            <= queued_at + timedelta(minutes=30)
        )
        assert run["payload"]["_depends_on"] == [escalation["job_id"]]
        await finish_escalation(escalation)
        actual = await finish_truth(run)
        results = await asyncio.wait_for(asyncio.gather(*tasks), 5)
        assert all(result["market_truth"][0] == actual for result in results)
        second = await asyncio.wait_for(
            service.dispatch(
                "get_market_truth",
                {"event_id": eid, "tokens": [tokens[0]], "cutoff": cutoff},
                {"call_id": "model-call-two"},
            ),
            5,
        )
        assert second["market_truth"][0] == actual
        assert len(await jobs("RUN_MARKET_TRUTH")) == 1
        completed = await base.get("ops.jobs", escalation["job_id"])
        frozen = canonical_json(completed)
        hot_key = eid + ":" + tokens[0]
        await base.put(
            "mkt.hot_tokens", hot_key, {"token_id": tokens[0], "event_id": eid, "priority": 1, "since": iso()}
        )
        await base.delete("mkt.hot_tokens", hot_key)
        reused = await service.dispatch(
            "get_market_truth",
            {"event_id": eid, "tokens": [tokens[0]], "cutoff": cutoff},
            {"call_id": "third-call-after-demotion"},
        )
        assert reused["market_truth"][0] == actual
        assert await base.get("mkt.hot_tokens", hot_key) is None
        assert canonical_json(await base.get("ops.jobs", escalation["job_id"])) == frozen
        later = iso(parse_time(cutoff) + timedelta(minutes=1))
        later_task = asyncio.create_task(
            service.dispatch(
                "get_market_truth",
                {"event_id": eid, "tokens": [tokens[0]], "cutoff": later},
                {"call_id": "later-model-call"},
            )
        )
        tasks.append(later_task)
        later_runs = await wait_jobs("RUN_MARKET_TRUTH", 2)
        new_run = next(row for row in later_runs if row["payload"]["cutoff"] == later)
        assert new_run["idem_key"] == eid + ":" + tokens[0] + ":" + later
        assert new_run["payload"]["_depends_on"] == [escalation["job_id"]]
        later_truth = await finish_truth(new_run)
        assert (await later_task)["market_truth"][0] == later_truth
        token_task = asyncio.create_task(
            service.dispatch(
                "get_market_truth",
                {"event_id": eid, "tokens": [tokens[1]], "cutoff": cutoff},
                {"call_id": "other-token-call"},
            )
        )
        tasks.append(token_task)
        other_escalations = await wait_jobs("ESCALATE_TOKENS", 2)
        other_escalation = next(row for row in other_escalations if row["payload"]["token_id"] == tokens[1])
        await finish_escalation(other_escalation)
        other_run = next(
            row for row in await wait_jobs("RUN_MARKET_TRUTH", 3) if row["payload"]["token_id"] == tokens[1]
        )
        assert other_run["payload"]["_depends_on"] == [other_escalation["job_id"]]
        other_truth = await finish_truth(other_run)
        assert (await token_task)["market_truth"][0] == other_truth
        assert len(await jobs("ESCALATE_TOKENS")) == 2 and len(await jobs("RUN_MARKET_TRUTH")) == 3
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if backend == "postgres":
            await base.close()

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
        tokens=["world:WLD"],
        event_time={"value": now, "precision": "EXACT", "quote": None},
        public_at={"value": now, "precision": "EXACT", "basis": "Primary publication", "quote": None},
        freshness="FRESH",
        claims=[
            dict(
                verified=True,
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


async def test_agent_declared_quote_stage_and_time_are_preserved(service):
    x = await seed(service)
    result = await service.dispatch("record_event", x, {"session_id": "s"})
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["event_state"] == "LIVE"
    assert event["event_time"]["precision"] == "EXACT"
    assert result["accepted_claims"][0]["verified"]
    assert len(await service.repo.list("evt.state_changes")) == 1


async def test_agent_quote_and_public_time_are_stored_without_host_semantics(service):
    x = await seed(service)
    x["claims"][0]["quote"] = "Agent selected evidence"
    x["public_at"]["value"] = iso(utcnow() + timedelta(days=1))
    result = await service.dispatch("record_event", x, {})
    assert result["dropped_claims"] == []
    assert result["accepted_claims"][0]["quote"] == "Agent selected evidence"
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["public_at"] == x["public_at"]["value"]


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


async def test_denial_requires_agent_verified_claim_and_invalidates(service):
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
        certainty="HIGH",
        certainty_reason="Agent observation judgment",
        claims=x["claims"],
        public_at=x["public_at"],
        freshness="FRESH",
    )
    args["claims"][0]["verified"] = False
    with pytest.raises(ValueError):
        await service.add_observation(args, {})
    args["claims"][0]["verified"] = True
    await service.add_observation(args, {})
    assert (await service.repo.get("evt.events", eid))["lifecycle"] == "INVALIDATED"


async def test_publication_freezes_reproducible_bundle_and_queues_only(service, monkeypatch):
    import tank.analysis.cards as cards
    from tank.util import sha256

    monkeypatch.setattr(cards, "alert_gate", lambda *a, **k: (True, "INITIAL"))
    monkeypatch.setattr(cards, "compose_card", lambda *a, **k: "Frozen card")
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
        # Serialization-only system call, not an invented agent tool receipt.
        {"event_id": eid, "call_id": "c", "role": "system"},
    )
    assert result["published"]
    bundle = await service.repo.get("out.evidence_bundles", result["evidence_bundle_id"])
    assert sha256(bundle["payload"]) == result["evidence_bundle_id"]
    from pathlib import Path

    import zstandard

    physical = Path(service.settings.data_dir) / "bundles" / (result["evidence_bundle_id"] + ".json.zst")
    assert bundle["path"] == str(physical)
    assert (
        sha256(zstandard.ZstdDecompressor().decompress(physical.read_bytes())) == result["evidence_bundle_id"]
    )
    assert bundle["payload"]["source_items"][0]["text"] == "Product announced"
    jobs = await service.repo.list("ops.jobs")
    assert len(jobs) == 1
    assert jobs[0]["type"] == "DELIVER"
    assert jobs[0]["payload"]["channel"] == "telegram:alerts"


async def test_fetched_page_retains_original_bytes_before_parse(service):
    from pathlib import Path

    import httpx
    import zstandard

    from tank.util import sha256

    original = b"<html><script>hidden code</script><body>Official   evidence</body></html>"

    async def respond(request):
        return httpx.Response(200, content=original, headers={"content-type": "text/html"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service.http = client
        item = await service.dispatch(
            "fetch_page", {"url": "https://example.com/evidence"}, {"session_id": "s"}
        )
    assert zstandard.ZstdDecompressor().decompress(Path(item["raw_ref"]).read_bytes()) == original
    assert item["raw_sha256"] == sha256(original)
    from tank.util import parse_time

    date = parse_time(item["observed_at"])
    assert Path(item["raw_ref"]).relative_to(service.settings.data_dir).parts == (
        "raw",
        "items",
        "WEB",
        date.strftime("%Y"),
        date.strftime("%m"),
        date.strftime("%d"),
        item["item_id"] + ".json.zst",
    )
    assert "Official" in item["text"] and "hidden code" not in item["text"]
    assert item["readable_text_sha256"] == sha256(item["text"])
    assert (await service.repo.get("ingest.source_items", item["item_id"]))["raw_sha256"] == sha256(original)


def test_nullable_scores_skip_numeric_constraints():
    validate_schema(None, {"type": ["number", "null"], "minimum": 0, "maximum": 10})


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
        "cutoff_prec": "BUCKET(1m)",
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
                "opened_at_prec": "BUCKET(1h)",
                "invalidation_price": "9",
            },
        ),
    ]:
        await service.repo.put(table, key, row)
    first = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert all(condition["expires_at"] and condition["created_at"] for condition in first["conditions"])
    assert first["action"] == "HOLD"
    assert first["cutoff_prec"] == "BUCKET(1m)"
    assert all(c["created_at_prec"] == "COLLECTION_ONLY" for c in first["conditions"])
    assert all(c["expires_at_prec"] == "BUCKET(1h)" for c in first["conditions"])
    armed = next(c for c in first["conditions"] if c["role"] == "UPGRADE")
    armed.update(counters={"root": {"count": 1, "window": 42}})
    await service.repo.put("ana.watch_conditions", armed["cond_id"], armed)
    second = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    same = next(c for c in second["conditions"] if c["role"] == "UPGRADE")
    assert same["cond_id"] == armed["cond_id"]
    assert same["counters"] == armed["counters"]
    same.update(status="FIRED", fired_at=iso())
    await service.repo.put("ana.watch_conditions", same["cond_id"], same)
    # This unit exercises consumption with already refreshed market truth; the
    # actual asynchronous refresh/worker boundary is covered separately.
    await service.repo.put(
        "ana.market_truth",
        "condition-truth-fresh",
        {**truth, "mta_id": "condition-truth-fresh", "cutoff": same["fired_at"]},
    )
    fired = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert fired["action"] == "ADD"
    consumed = await service.repo.get("ana.watch_conditions", same["cond_id"])
    assert consumed["consumed_by"] == fired["dec_id"]
    again = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert again["action"] == "HOLD"
    # W1 (03 §6.6, §7.2): the spent watch is not reused or shown again; the same
    # expression is re-armed afresh, so a later upgrade can still produce ADD.
    rearmed = next(c for c in again["conditions"] if c["role"] == "UPGRADE")
    assert rearmed["cond_id"] != same["cond_id"] and rearmed["status"] == "ARMED"
    assert rearmed["rearmed_from"] == same["cond_id"] and "counters" not in rearmed
    assert same["cond_id"] not in again["conditions_shown"]
    assert (await service.repo.get("ana.watch_conditions", same["cond_id"]))["status"] == "FIRED"
    assert len(await service.repo.list("ana.watch_conditions")) == len(first["conditions"]) + 1
    rearmed.update(status="FIRED", fired_at=iso())
    await service.repo.put("ana.watch_conditions", rearmed["cond_id"], rearmed)
    await service.repo.put(
        "ana.market_truth",
        "condition-truth-second",
        {**truth, "mta_id": "condition-truth-second", "cutoff": rearmed["fired_at"]},
    )
    second_add = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert second_add["action"] == "ADD"


async def test_start_condition_is_rearmed_for_the_open_call(service):
    """W1: the upgrade that led to START is spent; once the call is open the
    same expression is armed again, so ADD stays reachable."""
    from tank.analysis.decisions import DecisionEngine

    service.engine = DecisionEngine()
    eid, token = "start-event", "T"
    await service.repo.put(
        "evt.events",
        eid,
        {"event_id": eid, "current_ems_id": "start-ems", "event_state": "APPROVED", "state_verified": True},
    )
    await service.repo.put(
        "evt.ems_assessments",
        "start-ems",
        {"ems_id": "start-ems", "event_id": eid, "class": "MAJOR", "certainty": "HIGH"},
    )
    await service.repo.put(
        "evt.beneficiary_maps", "start-map", {"event_id": eid, "map_version": 1, "analysis_set": [token]}
    )
    await service.repo.put(
        "evt.beneficiary_edges",
        "start-edge",
        {
            "edge_id": "start-edge",
            "event_id": eid,
            "map_version": 1,
            "token_id": token,
            "role": "PRIMARY",
            "credible": True,
            "capture_class": "DIRECT",
        },
    )
    truth = {
        "mta_id": "start-truth",
        "event_id": eid,
        "token_id": token,
        "cutoff": iso(),
        "move_class": "ORGANIC_MIXED",
        "data_confidence": {"class": "MEDIUM"},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL", "anchor": {"ref_price": "10"}},
        "liquidity": {"liquidity_state": "NORMAL"},
    }
    await service.repo.put("ana.market_truth", "start-truth", truth)
    watch = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert watch["action"] == "WATCH_TO_START"
    upgrade = next(c for c in watch["conditions"] if c["role"] == "UPGRADE")
    upgrade.update(status="FIRED", fired_at=iso())
    await service.repo.put("ana.watch_conditions", upgrade["cond_id"], upgrade)
    await service.repo.put(
        "ana.market_truth",
        "start-truth-2",
        {**truth, "mta_id": "start-truth-2", "cutoff": iso(), "data_confidence": {"class": "HIGH"}},
    )
    start = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert start["action"] == "START"
    await service.repo.put(
        "ana.calls",
        "start-call",
        {
            "call_id": "start-call",
            "event_id": eid,
            "token_id": token,
            "status": "OPEN",
            "opened_at": iso(),
            "invalidation_price": "9",
        },
    )
    hold = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert hold["action"] == "HOLD"
    add_watch = next(c for c in hold["conditions"] if c["role"] == "UPGRADE")
    assert add_watch["status"] == "ARMED" and add_watch["cond_id"] != upgrade["cond_id"]
    assert add_watch["expr"] == upgrade["expr"]


async def test_agent_confirmation_certainty_and_reason_are_preserved(service):
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
        certainty_reason="Agent observation judgment",
        claims=x["claims"],
        public_at=x["public_at"],
        freshness="FRESH",
        certainty="HIGH",
    )
    await service.dispatch("add_observation", confirmation, {})
    event = await service.repo.get("evt.events", event["event_id"])
    assert event["certainty"] == "HIGH"
    assert event["certainty_reason"] == confirmation["certainty_reason"]


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
    assert not alert["decisions"]
    assert (
        await service.dispatch("publish_card", args, {"event_id": extraction["event_id"], "role": "analyst"})
    )["reason"] == "FIRST_CARD_ALREADY_GENERATED"


def resolved_native_domain_fixture(session_id):
    return {
        "id": session_id,
        "agent": {
            "id": "native-domain-fixture",
            "version": 1,
            "system": "Actual resolved synthetic domain integration instructions",
            "skills": [],
            "tools": [],
            "model": {"id": "gpt-6.1-sol"},
        },
    }


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
        if args and args[0] == "GET":
            return resolved_native_domain_fixture(session["session_id"])
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
        if args and args[0] == "GET":
            return resolved_native_domain_fixture(session["session_id"])
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


async def test_agent_verification_of_world_and_openai_is_persisted(service):
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
            "verified": True,
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


async def test_future_wording_does_not_override_agent_stage(service):
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = "Trading opens tomorrow at 09:00 UTC."
    await service.repo.put("ingest.source_items", "i", item)
    x["claims"][0].update(quote=item["text"], state_claim="LIVE")
    result = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["event_state"] == "LIVE"
    assert event["state_verified"]


async def test_full_requires_entire_analysis_then_pins_diverse_three_tokens(service, monkeypatch):
    import tank.analysis.cards as cards

    monkeypatch.setattr(cards, "alert_gate", lambda *a, **kw: (True, "TEST"))
    monkeypatch.setattr(cards, "compose_card", lambda *a, **kw: "Validated full card")
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


@pytest.mark.parametrize("freshness", ["FRESH", "STALE"])
async def test_real_expiry_to_update_delivery_never_reopens_call(service, monkeypatch, freshness):
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
        stale_event["freshness"] = freshness
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
        assert len(sent) == 1
        if freshness == "STALE":
            assert not correction["published"] and correction["reason"] == "STALE_STATE_CHANGE"
            assert len(await service.repo.list("out.alerts")) == 1
            assert (await service.repo.get("ana.calls", "expiry-call"))["status"] == "CLOSED"
            return
        assert correction["published"] and correction["reason"] == "QUEUED_RATE_LIMIT"
        assert len(await service.repo.list("out.alerts")) == 2
        later = now + timedelta(minutes=16)
        for module in ("tank.analysis.cards", "tank.event_tools", "tank.delivery"):
            monkeypatch.setattr(module + ".utcnow", lambda: later)
        assert correction["published"] and correction["version"] == 2
        alert = await service.repo.get("out.alerts", correction["alert_id"] + ":2")
        assert not alert["delivery_immediate"]
        assert "No longer following" in alert["card_text"]
        assert alert["expired_call_ids"] == ["expiry-call"]
        await delivery.deliver({"alert_id": correction["alert_id"], "version": 2})
        assert len(sent) == 2

    calls = await service.repo.list("ana.calls", filters={"event_id": eid})
    assert len(calls) == 1
    assert calls[0]["status"] == "CLOSED" and calls[0]["closed_by"] == "EXPIRED"
    assert not (await service.repo.get("evt.events", eid)).get("call_expired")
    again = await service.dispatch("get_decision", {"event_id": eid, "token": token}, {})
    assert again["tracking_ended"] and not again["actionable"]
    assert (await service.repo.get("ana.decisions", first_decision["dec_id"]))["action"] == "HOLD"


async def test_material_update_is_queued_once_and_delivered_after_window(service, monkeypatch):
    import httpx

    from tank.analysis.decisions import DecisionEngine
    from tank.delivery import DeliveryService

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
        for _ in range(2):
            published = await service.dispatch("publish_card", args, {})
            assert published["published"] and published["version"] == 2
            assert published["publication_receipt"]["disposition"] == "PUBLISHED"
        assert len(await service.repo.list("out.alerts")) == 2
        assert len(await service.repo.list("out.evidence_bundles")) == 2
        assert len(await service.repo.list("ops.jobs", filters={"type": "DELIVER"})) == 2
        later = now + timedelta(minutes=16)
        for module in ("tank.analysis.cards", "tank.event_tools", "tank.delivery"):
            monkeypatch.setattr(module + ".utcnow", lambda: later)
        # No new item or second publication call is needed at the due time.
        await delivery.deliver({"alert_id": published["alert_id"], "version": 2})

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
    import zstandard

    assert (
        zstandard.ZstdDecompressor().decompress(Path(item["raw_ref"]).read_bytes())
        == canonical_json(raw).encode()
    )
    assert item["raw_sha256"] == sha256(canonical_json(raw).encode())
    assert item["evidence_kind"] == "SEARCH_RESULT"
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
    assert stored["credible"] and (await service.repo.get("evt.events", eid))["event_state"] == "LIVE"
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
        if a and a[0] == "GET":
            return resolved_native_domain_fixture(session["session_id"])
        return {}

    sessions.request = request
    await sessions.tool(
        session, {"id": "read-other", "name": "get_item", "input": {"item_id": "other-primary"}}
    )
    x["claims"][0].update(quote="Fee switch approved.", state_claim="APPROVED")
    await sessions.tool(session, {"id": "extract", "name": "record_event", "input": x})
    result = (await service.repo.get("ops.tool_calls", "item-proof-session:extract"))["result"]
    assert result["accepted_claims"][0]["evidence"] == [{"item_id": "i", "quote": "Fee switch approved."}]
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
    claim = dict(x["claims"][0], verified=True, quote=quote, state_claim="APPROVED", type="MILESTONE")
    await service.dispatch(
        "add_observation",
        {
            "event_id": eid,
            "item_id": "milestone-proof",
            "link": "CORROBORATION",
            "certainty_reason": "Agent observation judgment",
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
    assert event["next_milestone"]["due_at"] == due
    assert event["next_milestone"]["kind"] == "Snapshot vote ends"
    assert event["next_milestone"]["source_id"] == "S24"
    assert event["next_milestone"]["due_at_prec"] is None
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
            # Typed domain producer/serialization coverage; no native session
            # or per-message workflow is claimed by this direct-dispatch unit.
            "model_id": "fixture-model",
            "prompt_version": "fixture-prompt",
            "role": "system",
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


@pytest.mark.parametrize("media_type", ["text/html", "application/json"])
async def test_search_and_fetched_evidence_keep_agent_certainty(service, media_type):
    import httpx

    from tank.search import SearchResponse
    from tank.util import canonical_json

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
            "verified": True,
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
    assert event["certainty"] == x["certainty"] and event["certainty_reason"] == x["certainty_reason"]
    assert created["accepted_claims"][0]["verified"] is x["claims"][0]["verified"]

    async def page(request):
        text = (
            canonical_json(
                {"title": "SEC release", "href": "https://www.sec.gov/news/release", "body": quote}
            )
            if media_type == "application/json"
            else "<html><body>" + quote + "</body></html>"
        )
        return httpx.Response(200, text=text, headers={"content-type": media_type})

    async with httpx.AsyncClient(transport=httpx.MockTransport(page)) as http:
        service.http = http
        fetched = await service.dispatch(
            "fetch_page", {"url": "https://www.sec.gov/news/release"}, {"session_id": "certainty-session"}
        )
    assert found["items"][0]["evidence_kind"] == "SEARCH_RESULT"
    assert fetched["evidence_kind"] == "FETCHED_PAGE" and fetched["feed"] == "web:page"
    assert fetched["item_id"] != found["items"][0]["item_id"]
    await service.dispatch(
        "add_observation",
        {
            "event_id": created["event_id"],
            "item_id": fetched["item_id"],
            "link": "CORROBORATION",
            "certainty_reason": "Agent observation judgment",
            "claims": x["claims"],
            "certainty": "HIGH",
            "public_at": x["public_at"],
            "freshness": "FRESH",
        },
        {"session_id": "certainty-session", "event_id": created["event_id"]},
    )
    verified = await service.repo.get("evt.events", created["event_id"])
    assert verified["certainty"] == "HIGH" and verified["state_verified"]


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


async def test_persistent_fatal_and_exit_publish_delivery_repeat_is_silent(service, monkeypatch):
    """Use real publication gate, frozen snapshots and delivery; isolate card layout."""
    import httpx

    import tank.analysis.cards as cards
    from tank.delivery import DeliveryService

    monkeypatch.setattr(cards, "compose_card", lambda *args, **kwargs: "Synthetic safety update")
    eid = "repeat-safety"
    await service.repo.put(
        "evt.events",
        eid,
        {
            "event_id": eid,
            "current_ems_id": "repeat-ems",
            "event_state": "APPROVED",
            "state_verified": True,
            "freshness": "FRESH",
            "certainty": "HIGH",
            "lifecycle": "MONITORING",
            "known_to_us_at": iso(),
            "tokens_mentioned": ["T"],
        },
    )
    await service.repo.put(
        "evt.ems_assessments",
        "repeat-ems",
        {"ems_id": "repeat-ems", "event_id": eid, "class": "MAJOR", "certainty": "HIGH"},
    )
    await service.repo.put(
        "evt.beneficiary_maps", "repeat-map", {"event_id": eid, "map_version": 1, "analysis_set": ["T"]}
    )
    await service.repo.put(
        "evt.beneficiary_edges",
        "repeat-edge",
        {
            "edge_id": "repeat-edge",
            "event_id": eid,
            "map_version": 1,
            "token_id": "T",
            "role": "PRIMARY",
            "tradable": True,
            "credible": True,
            "exposure_score": 8,
        },
    )
    await service.repo.put(
        "ana.market_truth",
        "repeat-truth",
        {
            "mta_id": "repeat-truth",
            "event_id": eid,
            "token_id": "T",
            "cutoff": iso(),
            "data_confidence": {"class": "LOW"},
        },
    )
    await service.repo.put(
        "ana.decisions",
        "repeat-wait",
        {"dec_id": "repeat-wait", "event_id": eid, "token_id": "T", "action": "WAIT", "cutoff": iso()},
    )
    args = {
        "event_id": eid,
        "kind": "FULL",
        "verdict": "Synthetic fact",
        "why": ["Primary fixture", "Safety fixture"],
    }
    first = await service.dispatch("publish_card", args, {})
    assert first["published"], first
    service.settings.telegram_token = "fixture"
    service.settings.telegram_alerts_chat = "1"
    service.settings.telegram_ops_chat = "2"
    sent = []

    def telegram(request):
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        delivery = DeliveryService(service.repo, service.settings, http=http)
        await delivery.deliver({"alert_id": first["alert_id"], "version": 1})
        event = await service.repo.get("evt.events", eid)
        await service.repo.put("evt.events", eid, {**event, "freshness": "FRESH", "lifecycle": "INVALIDATED"})
        await service.repo.put(
            "ana.contradictions",
            "repeat-fatal",
            {
                "contradiction_id": "repeat-fatal",
                "event_id": eid,
                "type": "FATAL",
                "description": "Synthetic official denial",
            },
        )
        await service.repo.put(
            "ana.decisions",
            "repeat-exit",
            {"dec_id": "repeat-exit", "event_id": eid, "token_id": "T", "action": "EXIT", "cutoff": iso()},
        )
        args["kind"] = "UPDATE"
        correction = await service.dispatch("publish_card", args, {})
        assert correction["published"], correction
        await delivery.deliver({"alert_id": correction["alert_id"], "version": 2})
        for _ in range(2):
            repeat = await service.dispatch("publish_card", args, {})
            assert not repeat["published"], repeat
        assert len(sent) == 2
        assert len(await service.repo.list("out.alerts", filters={"event_id": eid})) == 2
        # Persisted FATAL/EXIT cannot make an unrelated class change immediate.
        event = await service.repo.get("evt.events", eid)
        await service.repo.put("evt.events", eid, {**event, "freshness": "FRESH", "lifecycle": "MONITORING"})
        await service.repo.put(
            "evt.ems_assessments",
            "repeat-ems",
            {"ems_id": "repeat-ems", "event_id": eid, "class": "STRUCTURAL_SHOCK", "certainty": "HIGH"},
        )
        queued = await service.dispatch("publish_card", args, {})
        assert queued["published"] and queued["reason"] == "QUEUED_RATE_LIMIT"
        assert queued["version"] == 3
        assert len(await service.repo.list("out.alerts", filters={"event_id": eid})) == 3
        assert len(sent) == 2
        await service.repo.put(
            "ana.decisions",
            "repeat-start",
            {
                "dec_id": "repeat-start",
                "event_id": eid,
                "token_id": "T",
                "action": "START",
                "cutoff": iso(),
                "entry_ref_price": "10",
                "invalidation_price": "9",
            },
        )
        started = await service.dispatch("publish_card", args, {})
        assert started["published"], started
        assert started["version"] == 4
        frozen = await service.repo.get("out.alerts", started["alert_id"] + ":4")
        assert frozen["delivery_immediate"]
        await delivery.deliver({"alert_id": started["alert_id"], "version": 4})
        assert len(sent) == 3


async def seed_economic_event(service, *, capture_score=8, negative_relation=True, approval_dependency=False):
    from tank.event_tools import LAYERS
    from tank.util import new_id

    quote = "World access approved."
    mechanism_quote = "The World token is required to pay protocol gas fees."
    relation_quote = "World route is built. Replacement lowers World fee revenue."
    dependency_quote = "World token demand depends on SEC application approval."
    text = " ".join([quote, mechanism_quote, relation_quote, dependency_quote])
    item_id = new_id("item")
    stamp = iso()
    await service.repo.put(
        "ingest.source_items",
        item_id,
        {
            "item_id": item_id,
            "source_id": "S01",
            "source_class": "PRIMARY",
            "text": text,
            "observed_at": stamp,
        },
    )
    x = extraction()
    x.update(
        item_id=item_id,
        statement=quote,
        tokens=["world:WLD"],
        entities=[{"name": "World", "entity_id": "world", "role": "ACTOR"}],
        public_at={
            "value": stamp,
            "precision": "EXACT",
            "basis": "Synthetic primary release",
            "quote": quote,
        },
    )
    x["claims"] = [
        {
            "type": "STATE",
            "verified": True,
            "subject": "world",
            "predicate": "access approved",
            "object": "access",
            "state_claim": "APPROVED",
            "directionality": "BOTH",
            "evidence_class": "PRIMARY_FACT",
            "quote": quote,
        },
        {
            "type": "MECHANISM",
            "verified": True,
            "subject": "world:WLD",
            "predicate": "required for",
            "object": "protocol gas fees",
            "state_claim": None,
            "directionality": "BOTH",
            "evidence_class": "PRIMARY_FACT",
            "quote": mechanism_quote,
        },
    ]
    if negative_relation:
        x["claims"].append(
            {
                "type": "RELATION",
                "verified": True,
                "subject": "world",
                "predicate": "lowers",
                "object": "World fee revenue",
                "state_claim": "BUILT",
                "directionality": "BOTH",
                "evidence_class": "PRIMARY_FACT",
                "quote": relation_quote,
            }
        )
    if approval_dependency:
        x["claims"].append(
            {
                "type": "RELATION",
                "verified": True,
                "subject": "world:WLD",
                "predicate": "depends on",
                "object": "SEC application approval",
                "state_claim": None,
                "directionality": "NONE",
                "evidence_class": "PRIMARY_FACT",
                "quote": dependency_quote,
            }
        )
    created = await service.dispatch("record_event", x, {})
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
    chain.update(
        token_id="world:WLD",
        capture_score=capture_score,
        token_mechanism={
            "type": "GAS",
            "active": True,
            "evidence": [created["accepted_claims"][1]["claim_id"]],
        },
    )
    assessment = unknown_assessment(quote=quote)
    assessment["capture_chains"] = [chain]
    await service.dispatch("set_assessment", {"event_id": created["event_id"], "assessment": assessment}, {})
    return created, relation_quote, {layer: "No further adopted link is established" for layer in LAYERS}


@pytest.mark.parametrize("capture_score,expected_grade", [(8, "B"), (4, "C")])
async def test_produced_loser_has_supported_negative_grade_without_long_action(
    service, capture_score, expected_grade
):
    from tank.analysis.decisions import DecisionEngine

    service.engine = DecisionEngine()
    created, negative_quote, notes = await seed_economic_event(service, capture_score=capture_score)
    eid = created["event_id"]
    claims = created["accepted_claims"]
    negative_id = claims[2]["claim_id"]
    edge = {
        "entity_id": "world",
        "token_id": "world:WLD",
        "role": "LOSER",
        "layer": "ISSUER_PRODUCT",
        "direction": "NEGATIVE",
        "causal_chain": "Replacement lowers World fee revenue.",
        "directionality": "BOTH",
        "exposure_score": 6,
        "edge_certainty": "HIGH",
        "evidence": [negative_id],
        "negative_thesis": {
            "mechanism": "Replacement lowers World fee revenue.",
            "quote": negative_quote,
            "claim_ids": [negative_id],
        },
    }
    await service.dispatch(
        "set_beneficiaries", {"event_id": eid, "edges": [edge], "layer_reasons": notes}, {}
    )
    produced = (await service.repo.list("evt.beneficiary_edges", filters={"event_id": eid}))[0]
    assert produced["credible"] is False and produced["thesis_link_credible"] is True
    truth = complete_truth_fixture("loser-" + eid, eid, "world:WLD", iso())
    await service.repo.put("ana.market_truth", truth["mta_id"], truth)
    decision = await service.dispatch("get_decision", {"event_id": eid, "token": "world:WLD"}, {})
    assert decision["thesis_grade"] == expected_grade
    # The actual empty market snapshot remains UNKNOWN; a supported negative thesis does not fabricate market direction.
    assert decision["outlook"]["direction"] == "UNKNOWN"
    assert decision["action"] not in {"START", "ADD"}


async def test_loser_cannot_reuse_only_positive_capture_proof(service):
    from tank.analysis.decisions import DecisionEngine

    service.engine = DecisionEngine()
    created, _, notes = await seed_economic_event(service, negative_relation=False)
    eid = created["event_id"]
    proof = created["accepted_claims"][1]["claim_id"]
    edge = {
        "entity_id": "world",
        "token_id": "world:WLD",
        "role": "LOSER",
        "layer": "ISSUER_PRODUCT",
        "direction": "NEGATIVE",
        "causal_chain": "A negative mechanism has not been evidenced",
        "directionality": "BOTH",
        "exposure_score": 6,
        "edge_certainty": "HIGH",
        "evidence": [created["accepted_claims"][0]["claim_id"], proof],
    }
    await service.dispatch(
        "set_beneficiaries", {"event_id": eid, "edges": [edge], "layer_reasons": notes}, {}
    )
    produced = (await service.repo.list("evt.beneficiary_edges", filters={"event_id": eid}))[0]
    assert produced["thesis_link_credible"] is False
    truth = complete_truth_fixture("unsupported-loser-" + eid, eid, "world:WLD", iso())
    await service.repo.put("ana.market_truth", truth["mta_id"], truth)
    assert (await service.dispatch("get_decision", {"event_id": eid, "token": "world:WLD"}, {}))[
        "thesis_grade"
    ] == "D"
    edge["negative_thesis"] = {
        "mechanism": "Claimed negative relation",
        "quote": "The World token is required to pay protocol gas fees.",
        "claim_ids": [proof],
    }
    with pytest.raises(ValueError, match="causal relation"):
        await service.dispatch(
            "set_beneficiaries", {"event_id": eid, "edges": [edge], "layer_reasons": notes}, {}
        )


async def test_rejected_approval_only_exits_call_with_explicit_evidenced_dependency(service):
    from tank.analysis.decisions import DecisionEngine
    from tank.util import new_id

    service.engine = DecisionEngine()
    created, _, notes = await seed_economic_event(service, negative_relation=False, approval_dependency=True)
    eid = created["event_id"]
    claims = created["accepted_claims"]
    edge = {
        "entity_id": "world",
        "token_id": "world:WLD",
        "role": "PRIMARY",
        "layer": "ISSUER_PRODUCT",
        "direction": "POSITIVE",
        "causal_chain": "Approved access uses the required token rail",
        "directionality": "BOTH",
        "exposure_score": 6,
        "edge_certainty": "HIGH",
        "evidence": [claims[0]["claim_id"], claims[1]["claim_id"]],
    }
    await service.dispatch(
        "set_beneficiaries", {"event_id": eid, "edges": [edge], "layer_reasons": notes}, {}
    )
    truth = complete_truth_fixture("approval-" + eid, eid, "world:WLD", iso())
    await service.repo.put("ana.market_truth", truth["mta_id"], truth)
    await service.repo.put(
        "ana.calls",
        "approval-call-" + eid,
        {
            "call_id": "approval-call-" + eid,
            "event_id": eid,
            "token_id": "world:WLD",
            "status": "OPEN",
            "opened_at": iso(),
            "entry_ref_price": "10",
            "invalidation_price": "9",
        },
    )
    before = await service.dispatch("get_decision", {"event_id": eid, "token": "world:WLD"}, {})

    async def rejected(object_text, dependency=None):
        stamp = iso()
        item_id = new_id("item")
        quote = object_text + " was rejected."
        await service.repo.put(
            "ingest.source_items",
            item_id,
            {
                "item_id": item_id,
                "source_id": "S01",
                "source_class": "PRIMARY",
                "text": quote,
                "observed_at": stamp,
            },
        )
        claim = {
            "type": "STATE",
            "verified": True,
            "subject": "sec",
            "predicate": "rejected",
            "object": object_text,
            "state_claim": "REJECTED",
            "directionality": "NONE",
            "evidence_class": "PRIMARY_FACT",
            "quote": quote,
        }
        if dependency is not None:
            claim["approval_dependency"] = dependency
        return await service.dispatch(
            "add_observation",
            {
                "event_id": eid,
                "item_id": item_id,
                "link": "CORROBORATION",
                "certainty_reason": "Agent observation judgment",
                "claims": [claim],
                "certainty": "HIGH",
                "public_at": {
                    "value": stamp,
                    "precision": "EXACT",
                    "basis": "Official rejection fixture",
                    "quote": quote,
                },
                "freshness": "FRESH",
            },
            {},
        )

    await rejected("An unrelated SEC motion")
    assert not await service.repo.list("ana.contradictions", filters={"event_id": eid})
    unrelated = await service.dispatch("get_decision", {"event_id": eid, "token": "world:WLD"}, {})
    assert unrelated["action"] == before["action"] and unrelated["action"] != "EXIT"
    dependency_id = claims[2]["claim_id"]
    await rejected(
        "SEC application approval",
        {"scope": "TOKEN", "token_ids": ["world:WLD"], "claim_ids": [dependency_id]},
    )
    contradictions = await service.repo.list("ana.contradictions", filters={"event_id": eid})
    assert len(contradictions) == 1 and contradictions[0]["type"] == "FATAL"
    assert contradictions[0]["token_id"] == "world:WLD" and dependency_id in contradictions[0]["evidence"]
    dependent = await service.dispatch("get_decision", {"event_id": eid, "token": "world:WLD"}, {})
    assert dependent["action"] == "EXIT"


async def test_integration_cap_uses_actual_stage_claim_not_unrelated_counterparty_fact(service):
    from tank.event_tools import LAYERS

    x = await seed(service)
    x["tokens"] = ["world:WLD"]
    x["statement"] = "World route is now live."
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = "World route is now live. A different counterparty announced an investment."
    await service.repo.put("ingest.source_items", "i", item)
    x["claims"] = [
        {
            "type": "STATE",
            "verified": True,
            "subject": "world",
            "predicate": "supports",
            "object": "World route",
            "state_claim": "LIVE",
            "directionality": "SOURCE_SIDE_SUPPORT",
            "evidence_class": "PRIMARY_FACT",
            "quote": "World route is now live.",
        },
        {
            "type": "FACT",
            "verified": True,
            "subject": "Other company",
            "predicate": "announced",
            "object": "investment",
            "state_claim": None,
            "directionality": "COUNTERPARTY_ADOPTION",
            "evidence_class": "PRIMARY_FACT",
            "quote": "A different counterparty announced an investment.",
        },
    ]
    created = await service.dispatch("record_event", x, {})
    edge = {
        "entity_id": "world",
        "token_id": "world:WLD",
        "role": "SECONDARY",
        "layer": "ISSUER_PRODUCT",
        "direction": "POSITIVE",
        "causal_chain": "Source-side compatibility only",
        "directionality": "BOTH",
        "exposure_score": 4,
        "edge_certainty": "HIGH",
        "evidence": [claim["claim_id"] for claim in created["accepted_claims"]],
    }
    await service.dispatch(
        "set_beneficiaries",
        {
            "event_id": created["event_id"],
            "edges": [edge],
            "layer_reasons": {layer: "No independent integration evidence" for layer in LAYERS},
        },
        {},
    )
    produced = (await service.repo.list("evt.beneficiary_edges", filters={"event_id": created["event_id"]}))[
        0
    ]
    assert produced["integration_state"] == "BUILT"
    assert produced["integration_state_evidence"] == [created["accepted_claims"][0]["claim_id"]]
    assert set(produced["evidence"]) == {claim["claim_id"] for claim in created["accepted_claims"]}


async def test_magnitude_agent_judgment_is_preserved_with_or_without_ratio_metadata(service):
    created, _, _ = await seed_economic_event(service, negative_relation=False)
    eid = created["event_id"]
    proof = created["accepted_claims"][0]["claim_id"]
    assessment = unknown_assessment(quote="World access approved")
    assessment["dims"]["magnitude"].update(value=10, rationale="V4 is significant", evidence=[proof])
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    stored = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert stored["dims"]["magnitude"]["value"] == 10
    # Actual quoted NUMBER claims supply a denominator of the prescribed kind and unit.
    for key, quote in [
        ("numerator", "World token flow is 10 USD."),
        ("denominator", "World token market cap is 100 USD."),
    ]:
        await service.repo.put(
            "evt.claims",
            key,
            {
                "claim_id": key,
                "event_id": eid,
                "accepted": True,
                "verified": True,
                "type": "NUMBER",
                "evidence": [{"item_id": "ratio-primary", "quote": quote}],
            },
        )
    assessment["magnitude_basis"] = {
        "kind": "TOKEN_MECHANICAL",
        "denominator_window": "SPOT",
        "numerator": {
            "value": 10,
            "unit": "USD",
            "quote": "World token flow is 10 USD.",
            "claim_id": "numerator",
        },
        "denominator": {
            "value": 100,
            "unit": "USD",
            "quote": "World token market cap is 100 USD.",
            "claim_id": "denominator",
        },
    }
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    stored = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert stored["dims"]["magnitude"]["value"] == 10  # Agent owns the rubric judgment.
    marketing = "World marketing budget is 100 USD."
    await service.repo.put(
        "evt.claims",
        "denominator",
        {
            "claim_id": "denominator",
            "event_id": eid,
            "accepted": True,
            "verified": True,
            "type": "NUMBER",
            "evidence": [{"item_id": "ratio-primary", "quote": marketing}],
        },
    )
    assessment["magnitude_basis"]["denominator"]["quote"] = marketing
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    stored = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert stored["dims"]["magnitude"]["value"] == 10


async def test_proposed_timing_requires_primary_dated_decision(service):
    from tank.util import parse_time

    created, _, _ = await seed_economic_event(service, negative_relation=False)
    eid = created["event_id"]
    event = await service.repo.get("evt.events", eid)
    await service.repo.put("evt.events", eid, {**event, "event_state": "PROPOSED"})
    assessment = unknown_assessment(quote="World access approved")
    assessment["impact_horizon"].update(impact_start="DAYS", timing_confidence="HIGH")
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    stored = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert stored["impact_horizon"]["impact_start"] == "WEEKS"
    assert stored["impact_horizon"]["timing_confidence"] == "HIGH"
    due = iso(utcnow() + timedelta(days=2))
    quote = "World decision scheduled " + due
    await service.repo.put(
        "evt.claims",
        "timing-primary",
        {
            "claim_id": "timing-primary",
            "event_id": eid,
            "accepted": True,
            "verified": True,
            "type": "MILESTONE",
            "evidence": [{"item_id": "primary", "quote": quote}],
        },
    )
    assessment["impact_horizon"].update(
        timing_evidence=[{"claim_id": "timing-primary", "due_at": due, "quote": quote}],
        dependencies=["World approval decision"],
    )
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    stored = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert stored["impact_horizon"]["impact_start"] == "DAYS"
    assert stored["impact_horizon"]["ramp_shape"] == "CONDITIONAL"
    assert stored["impact_horizon"]["timing_confidence"] == "HIGH"
    assessment["impact_horizon"]["timing_evidence"][0]["due_at"] = iso(parse_time(due) + timedelta(hours=1))
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    assert (
        out["impact_horizon"]["timing_evidence"][0]["due_at"]
        == assessment["impact_horizon"]["timing_evidence"][0]["due_at"]
    )


async def test_immediate_market_access_allows_effective_primary_change_without_prior_trade(service):
    from tank.util import new_id

    stamp = iso()
    quote = "World access approved and effective " + stamp + " for the World instrument."
    item_id = new_id("item")
    await service.repo.put(
        "ingest.source_items",
        item_id,
        {
            "item_id": item_id,
            "source_id": "S01",
            "source_class": "PRIMARY",
            "text": quote,
            "observed_at": stamp,
        },
    )
    x = extraction()
    x.update(
        item_id=item_id,
        statement=quote,
        tokens=["world:WLD"],
        entities=[{"name": "World", "entity_id": "world", "role": "ACTOR"}],
        public_at={
            "value": stamp,
            "precision": "EXACT",
            "basis": "Primary effective access release",
            "quote": quote,
        },
    )
    x["claims"] = [
        {
            "type": "STATE",
            "verified": True,
            "subject": "world",
            "predicate": "approved effective access",
            "object": "World instrument",
            "state_claim": "APPROVED",
            "directionality": "BOTH",
            "evidence_class": "PRIMARY_FACT",
            "quote": quote,
        }
    ]
    created = await service.dispatch("record_event", x, {})
    eid = created["event_id"]
    proof = created["accepted_claims"][0]["claim_id"]
    await service.repo.put(
        "reg.instruments",
        "world-access",
        {"instrument_id": "world-access", "token_id": "world:WLD", "active": True},
    )
    assessment = unknown_assessment(quote=quote)
    assessment["impact_horizon"].update(
        impact_start="NOW",
        immediate_market_access={
            "claim_id": proof,
            "instrument_id": "world-access",
            "effective_at": stamp,
            "quote": quote,
        },
    )
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    assert (await service.repo.get("evt.ems_assessments", out["ems_id"]))["impact_horizon"][
        "impact_start"
    ] == "NOW"
    from tank.util import parse_time

    original_latest = service.repo.latest_before

    async def typed_only(collection, *args, **kwargs):
        assert collection != "mkt.trades", "Raw trades must never be queried from PostgreSQL"
        return await original_latest(collection, *args, **kwargs)

    service.repo.latest_before = typed_only
    minute = iso(parse_time(stamp).replace(second=0, microsecond=0))
    bar = {
        "instrument_id": "world-access",
        "minute_start": minute,
        "minute_start_prec": "BUCKET(60s)",
        "close": "999",
        "last_trade": None,
    }
    await service.repo.put("mkt.flow_bars_1m", "world-access:" + minute, bar)
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    frozen = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert "trade_evidence" not in frozen["impact_horizon"]["immediate_market_access"]
    bar["last_trade"] = {"trade_id": "actual-trade", "price": "10", "ts": stamp, "ts_prec": "EXACT"}
    await service.repo.put("mkt.flow_bars_1m", "world-access:" + minute, bar)
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    frozen = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert "trade_evidence" not in frozen["impact_horizon"]["immediate_market_access"]


async def test_primary_natural_calendar_and_usd_ratio_dispatch(service):
    created, _, _ = await seed_economic_event(service, negative_relation=False)
    eid = created["event_id"]
    for key, quote, kind in [
        ("fees", "Annual fees were $100.", "NUMBER"),
        ("revenue", "Annual protocol revenue was $500.", "NUMBER"),
        ("vote", "The vote ends on October 20, 2026.", "MILESTONE"),
    ]:
        await service.repo.put(
            "evt.claims",
            key,
            {
                "claim_id": key,
                "event_id": eid,
                "accepted": True,
                "verified": True,
                "type": kind,
                "evidence": [{"item_id": "primary-quote", "quote": quote}],
            },
        )
    assessment = unknown_assessment(quote="World access approved")
    assessment["dims"]["magnitude"].update(value=10, evidence=["fees", "revenue"])
    assessment["magnitude_basis"] = {
        "kind": "USAGE_REVENUE",
        "denominator_window": "ANNUALIZED",
        "numerator": {"value": 100, "unit": "USD", "quote": "Annual fees were $100.", "claim_id": "fees"},
        "denominator": {
            "value": 500,
            "unit": "USD",
            "quote": "Annual protocol revenue was $500.",
            "claim_id": "revenue",
        },
    }
    assessment["impact_horizon"].update(
        timing_confidence="HIGH",
        timing_evidence=[
            {
                "claim_id": "vote",
                "due_at": "2026-10-20T00:00:00Z",
                "quote": "The vote ends on October 20, 2026.",
            }
        ],
    )
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    stored = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert stored["dims"]["magnitude"]["value"] == 10
    assert stored["impact_horizon"]["timing_evidence"] == assessment["impact_horizon"]["timing_evidence"]
    assert stored["impact_horizon"]["timing_confidence"] == "HIGH"


async def test_event_source_time_provenance_and_milestone_offsets(service):
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["observed_at_prec"] = "BUCKET(5m)"
    await service.repo.put("ingest.source_items", "i", item)
    x["public_at"]["precision"] = "DAY"
    made = await service.dispatch("record_event", x, {})
    eid = made["event_id"]
    event = await service.repo.get("evt.events", eid)
    assert event["known_to_us_at_prec"] == "BUCKET(5m)"
    assert event["public_at_prec"] == "BUCKET(1d)"
    observations = await service.repo.list("evt.event_observations", filters={"event_id": eid})
    assert observations[0]["observed_at_prec"] == "BUCKET(5m)"
    assert observations[0]["public_at_prec"] == "BUCKET(1d)"
    due = iso(utcnow() + timedelta(days=2))
    await service.dispatch(
        "schedule_milestone",
        {
            "event_id": eid,
            "milestone": "Quoted day milestone",
            "due_at": due,
            "due_at_prec": "BUCKET(1d)",
            "source": "S01",
        },
        {},
    )
    event = await service.repo.get("evt.events", eid)
    assert event["next_milestone"]["due_at_prec"] == "BUCKET(1d)"
    escalation = (await service.repo.list("ops.source_escalations", filters={"event_id": eid}))[0]
    assert escalation["starts_at_prec"] == escalation["expires_at_prec"] == "BUCKET(1d)"


async def test_event_merge_copies_selected_earlier_time_precision(service):
    source = {
        "event_id": "source-time",
        "known_to_us_at": "2026-10-18T00:00:00Z",
        "known_to_us_at_prec": "EXACT",
    }
    target = {
        "event_id": "target-time",
        "known_to_us_at": "2026-10-17T00:00:00Z",
        "known_to_us_at_prec": "BUCKET(1d)",
    }
    await service.repo.put("evt.events", source["event_id"], source)
    await service.repo.put("evt.events", target["event_id"], target)
    result = await service.dispatch(
        "merge_into", {"event_id": target["event_id"]}, {"event_id": source["event_id"]}
    )
    assert result["known_to_us_at"] == target["known_to_us_at"]
    assert result["known_to_us_at_prec"] == "BUCKET(1d)"


async def test_market_analysis_boundary_rejects_collection_clock_precision(service):
    made, _, _ = await seed_economic_event(service, negative_relation=False)
    with pytest.raises(ValueError, match="Chosen boundaries"):
        await service.dispatch(
            "get_market_truth",
            {
                "event_id": made["event_id"],
                "tokens": ["world:WLD"],
                "cutoff": iso(),
                "cutoff_prec": "COLLECTION_ONLY",
            },
            {},
        )
    assert not await service.repo.list("ops.jobs", filters={"kind": "RUN_MARKET_TRUTH"})


async def test_assessment_defers_reaction_ownership_to_scheduler_after_commit(service):
    import asyncio

    class SchedulerObserver:
        def __init__(self):
            self.events = []

        async def ensure_reactions(self, event):
            async def independent_read():
                async with service.repo.transaction(), service.repo.lock("event:" + event["event_id"]):
                    stored = await service.repo.get("evt.events", event["event_id"])
                    return await service.repo.get("evt.ems_assessments", stored["current_ems_id"])

            assessment = await asyncio.wait_for(asyncio.create_task(independent_read()), 1)
            assert assessment["class"] == "MAJOR"
            self.events.append(event["event_id"])

    service.scheduler = SchedulerObserver()
    created, _, _ = await seed_economic_event(service, negative_relation=False)
    assert service.scheduler.events == [created["event_id"]]
    assert not await service.repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})


async def test_full_card_blocks_only_analysis_tokens_with_decisions(service, monkeypatch):
    from tank.analysis import cards

    created, _, _ = await seed_economic_event(service, negative_relation=False)
    eid = created["event_id"]
    token = "world:WLD"
    await service.repo.put(
        "evt.beneficiary_maps", "block-map", {"event_id": eid, "map_version": 1, "analysis_set": [token]}
    )
    for edge_id, edge_token, exposure in [("selected", token, 7), ("not-in-analysis", "ethereum:ETH", 10)]:
        await service.repo.put(
            "evt.beneficiary_edges",
            edge_id,
            {
                "edge_id": edge_id,
                "event_id": eid,
                "map_version": 1,
                "token_id": edge_token,
                "role": "PRIMARY",
                "tradable": True,
                "credible": True,
                "exposure_score": exposure,
            },
        )
    truth = complete_truth_fixture("block-truth", eid, token, iso())
    await service.repo.put("ana.market_truth", "block-truth", truth)
    await service.repo.put(
        "ana.decisions",
        "block-decision",
        {
            "dec_id": "block-decision",
            "event_id": eid,
            "token_id": token,
            "action": "WAIT",
            "cutoff": iso(),
            "market_truth_id": "block-truth",
        },
    )
    captured = {}

    def compose(event, assessment, beneficiaries, *args, **kwargs):
        captured["blocks"] = [edge["token_id"] for edge in beneficiaries]
        captured["who"] = [edge["token_id"] for edge in event["beneficiary_edges"]]
        return "Isolated token selection fixture"

    monkeypatch.setattr(cards, "compose_card", compose)
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
            "verdict": "World approved access",
            "why": ["Quoted approval", "Supported capture"],
        },
        {},
    )
    assert result["published"], result
    assert captured["blocks"] == [token]
    assert set(captured["who"]) == {token, "ethereum:ETH"}


async def test_web_raw_canonical_file_exists_even_when_parser_fails(service, monkeypatch):
    from pathlib import Path

    import httpx
    import selectolax.parser
    import zstandard

    original = b"<html><body>Saved before parser</body></html>"

    def fail_parser(body):
        raise RuntimeError("Synthetic parser failure")

    monkeypatch.setattr(selectolax.parser, "HTMLParser", fail_parser)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=original, headers={"content-type": "text/html"})
        )
    ) as client:
        service.http = client
        with pytest.raises(RuntimeError, match="Synthetic parser failure"):
            await service.dispatch(
                "fetch_page", {"url": "https://example.com/raw-first"}, {"session_id": "raw-session"}
            )
    files = list((Path(service.settings.data_dir) / "raw" / "items" / "WEB").rglob("*.json.zst"))
    assert len(files) == 1
    assert zstandard.ZstdDecompressor().decompress(files[0].read_bytes()) == original
    assert not await service.repo.list("ingest.source_items")


async def test_bundle_directory_sync_failure_never_creates_database_reference(service, monkeypatch):
    import os
    import stat

    from tank.analysis import cards

    monkeypatch.setattr(cards, "alert_gate", lambda *args, **kwargs: (True, "INITIAL"))
    monkeypatch.setattr(cards, "compose_card", lambda *args, **kwargs: "Durability fixture")
    extraction_args = await seed(service)
    made = await service.dispatch("record_event", extraction_args, {})
    eid = made["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "durability-ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "durability-ems",
        {"ems_id": "durability-ems", "event_id": eid, "class": "STRUCTURAL_SHOCK"},
    )
    original_sync = os.fsync
    seen = []

    def fail_directory_sync(descriptor):
        kind = "directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file"
        seen.append(kind)
        if kind == "directory":
            raise OSError("Synthetic bundle directory fsync failure")
        original_sync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_directory_sync)
    with pytest.raises(OSError, match="Synthetic bundle directory fsync failure"):
        await service.dispatch(
            "publish_card",
            {"event_id": eid, "kind": "PARTIAL", "verdict": "Approved source fact", "why": []},
            {},
        )
    assert seen[-1] == "directory" and "file" in seen[:-1]
    assert not await service.repo.list("out.evidence_bundles")
    assert not await service.repo.list("out.alerts")
    assert not await service.repo.list("out.deliveries")
    assert not await service.repo.list("ops.jobs")


@pytest.mark.parametrize("backend", ["memory", "postgres"])
@pytest.mark.parametrize("resolved_snapshot", [True, False])
async def test_native_prompt_snapshot_producer_reaches_versioned_event_tools(
    service, backend, resolved_snapshot
):
    import os

    import httpx

    from tank.agents import SessionService
    from tank.storage import PostgresRepository
    from tank.util import new_id

    original_repo = service.repo
    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        service.repo = PostgresRepository(url)
        await service.repo.open()
        await service.repo.migrate()
    repo = service.repo
    sid = new_id("native-session")
    stamp = iso()
    quote = "SEC approved the World application at " + stamp + "."
    config = service.settings.load_config("openma")
    snapshot = {
        "id": config["agent_id"],
        "version": 42,
        "system": "Actual resolved native fixture instructions",
        "skills": [{"type": "custom", "skill_id": "native-fixture-skill", "version": "3"}],
        "tools": [],
        "model": {"id": "gpt-6.1-sol"},
    }
    requested = []
    dispatched = []

    def control(request):
        requested.append((request.method, request.url.path))
        if request.url.host == "www.sec.gov":
            return httpx.Response(
                200, text="<html><body>" + quote + "</body></html>", headers={"content-type": "text/html"}
            )
        if request.url.path.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": []})
            return httpx.Response(201, json={"id": sid, "agent": {"id": config["agent_id"], "version": 42}})
        if request.method == "GET" and request.url.path.endswith("/sessions/" + sid):
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": sid,
                        "agent": snapshot if resolved_snapshot else {"id": config["agent_id"], "version": 42},
                    }
                },
            )
        return httpx.Response(200, json={})

    async def dispatch(name, args, context):
        dispatched.append(name)
        return await service.dispatch(name, args, context)

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(control)) as http:
            service.http = http
            item = await service.dispatch(
                "fetch_page", {"url": "https://www.sec.gov/news/native-provenance-fixture"}, {}
            )
            native = SessionService(repo, service.settings, dispatch, http=http)
            session = await native.resolve({"kind": "NEW_ITEM", "item_id": item["item_id"]})
            # This is the real incomplete POST snapshot, not a hand-seeded prompt version.
            assert session["prompt_version"] is None and session["prompt_label"] == config["prompt_version"]
            args = extraction()
            args.update(
                item_id=item["item_id"],
                statement=quote,
                tokens=["world:WLD"],
                entities=[{"name": "SEC", "entity_id": "sec", "role": "REGULATOR"}],
                event_time={"value": stamp, "precision": "EXACT", "quote": quote},
                public_at={
                    "value": stamp,
                    "precision": "EXACT",
                    "basis": "Quoted synthetic primary release time",
                    "quote": stamp,
                },
            )
            args["claims"] = [
                {
                    "type": "STATE",
                    "verified": True,
                    "subject": "SEC",
                    "predicate": "approved",
                    "object": "the World application",
                    "state_claim": "APPROVED",
                    "directionality": "BOTH",
                    "evidence_class": "PRIMARY_FACT",
                    "quote": quote,
                }
            ]
            await native.tool(session, {"id": "native-record", "name": "record_event", "input": args})
            ledger = await repo.get("ops.tool_calls", sid + ":native-record")
            if not resolved_snapshot:
                assert ledger["status"] == "REFUSED" and ledger["result"]["error"]["category"] == "PROVENANCE"
                assert not dispatched
                assert not await repo.list("evt.events", filters={"session_id": sid})
                assert not await repo.list("evt.ems_assessments", filters={"session_id": sid})
            else:
                assert ledger["status"] == "DONE", ledger
                eid = ledger["result"]["event_id"]
                session = await repo.get("ops.agent_sessions", sid)
                await native.tool(
                    session,
                    {
                        "id": "native-assessment",
                        "name": "set_assessment",
                        "input": {"event_id": eid, "assessment": unknown_assessment(quote=quote)},
                    },
                )
                assessed = await repo.get("ops.tool_calls", sid + ":native-assessment")
                assert assessed["status"] == "DONE", assessed
                ems = await repo.get("evt.ems_assessments", assessed["result"]["ems_id"])
                assert (
                    ems["prompt_version"] == session["prompt_version"]
                    and ems["prompt_version"] != config["prompt_version"]
                )
                assert ems["prompt_version_basis"] == "RESOLVED_SESSION_AGENT_SNAPSHOT"
                assert (
                    ems["prompt_label"] == config["prompt_version"] and ems["prompt_version_reason"] is None
                )
                assert ems["agent_version"] == "42" and ems["agent_version_basis"] == "RESOLVED_SESSION_AGENT"
                assert ems["prompt_binding"]["resolved_skill_versions"] == [
                    {"type": "custom", "skill_id": "native-fixture-skill", "version": "3"}
                ]
                assert dispatched == ["record_event", "set_assessment"]
            assert requested.count(("GET", "/v1/sessions/" + sid)) == 1
    finally:
        if backend == "postgres":
            await repo.close()
            service.repo = original_repo


@pytest.mark.parametrize("http_status", [404, 503])
@pytest.mark.parametrize("outcome", ["continue_to_card", "native_error_failed"])
async def test_actual_fetch_http_error_returns_durably_then_card_or_native_failed(
    service, http_status, outcome
):
    import asyncio
    import json

    import httpx

    from tank.agents import SessionService
    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import compute_market_truth
    from tank.event_tools import LAYERS
    from tank.util import new_id

    sid = new_id("http-session")
    replay_sid = new_id("http-replay-session")
    iid = new_id("item")
    stamp = iso()
    approved = "World access approved."
    mechanism = "The World token is required to pay protocol gas fees."
    await service.repo.put(
        "ingest.source_items",
        iid,
        {
            "item_id": iid,
            "source_id": "S01",
            "source_class": "PRIMARY",
            "text": approved + " Primary publication timestamp " + stamp + ". " + mechanism,
            "observed_at": stamp,
            "observed_at_prec": "COLLECTION_ONLY",
            "extraction_status": "PENDING",
        },
    )
    fetched = []
    native_posts = []
    replay_posts = []
    lose_reply = True

    def control(request):
        nonlocal lose_reply
        if request.url.host == "evidence.invalid":
            fetched.append(str(request.url))
            return httpx.Response(http_status, text="Synthetic unavailable source")
        if request.url.path.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": []})
            body = json.loads(request.content)
            target = replay_sid if body.get("metadata", {}).get("tank_replay_run_id") else sid
            return httpx.Response(201, json=resolved_native_domain_fixture(target))
        if request.method == "POST" and request.url.path.endswith("/events"):
            payload = json.loads(request.content)
            (native_posts if "/sessions/" + sid + "/" in request.url.path else replay_posts).append(payload)
            item = payload["events"][0]
            if (
                item.get("type") == "user.custom_tool_result"
                and item.get("custom_tool_use_id") == "http-failure"
                and lose_reply
            ):
                lose_reply = False
                raise httpx.ConnectError("Synthetic lost acknowledgement", request=request)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(control)) as http:
        service.http = http
        service.engine = DecisionEngine()
        first = SessionService(service.repo, service.settings, service.dispatch, http=http)
        first.start_stream = lambda session_id: None
        session = await first.send_message({"kind": "NEW_ITEM", "item_id": iid})
        failed_call = {
            "id": "http-failure",
            "type": "agent.custom_tool_use",
            "name": "fetch_page",
            "input": {"url": "https://evidence.invalid/unavailable"},
        }
        with pytest.raises(httpx.ConnectError, match="Synthetic lost acknowledgement"):
            await first.process_event(sid, failed_call)
        ledger = await service.repo.get("ops.tool_calls", sid + ":http-failure")
        assert ledger["status"] == "ERROR" and not ledger.get("returned_at")
        resumed = SessionService(service.repo, service.settings, service.dispatch, http=http)
        resumed.start_stream = lambda session_id: None
        await resumed.process_event(sid, failed_call)
        await resumed.process_event(sid, failed_call)
        ledger = await service.repo.get("ops.tool_calls", sid + ":http-failure")
        assert ledger["status"] == "ERROR" and ledger["returned_at"]
        assert ledger["result"]["error"]["category"] == "SOURCE_HTTP"
        assert str(http_status) in ledger["result"]["error"]["reason"]
        error_replies = [
            p["events"][0] for p in native_posts if p["events"][0].get("custom_tool_use_id") == "http-failure"
        ]
        assert len(fetched) == 1 and len(error_replies) == 2 and error_replies[0] == error_replies[1]
        assert error_replies[0]["is_error"] is True
        assert len(await service.repo.list("ingest.source_items")) == 1
        assert (await service.repo.get("ops.agent_sessions", sid))["status"] == "OPEN"
        replay_options = service.settings.model_copy(update={"replay": True})
        replay = SessionService(service.repo, replay_options, service.dispatch, http=http)
        replay.start_stream = lambda session_id: None
        await replay.send_message(
            {
                "kind": "NEW_ITEM",
                "item_id": iid,
                "replay_run_id": new_id("replay-run"),
                "replay_source_session_id": sid,
            }
        )
        await replay.process_event(replay_sid, failed_call)
        await replay.process_event(replay_sid, failed_call)
        replayed = await service.repo.get("ops.tool_calls", replay_sid + ":http-failure")
        assert replayed["status"] == "ERROR" and replayed["is_error"] is True and replayed["returned_at"]
        assert replayed["result"] == ledger["result"]
        replay_errors = [
            p["events"][0] for p in replay_posts if p["events"][0].get("custom_tool_use_id") == "http-failure"
        ]
        assert replay_errors == [error_replies[-1]]
        assert len(fetched) == 1

        async def tool(call_id, name, args):
            await resumed.process_event(
                sid, {"id": call_id, "type": "agent.custom_tool_use", "name": name, "input": args}
            )
            stored = await service.repo.get("ops.tool_calls", sid + ":" + call_id)
            assert stored["status"] == "DONE", stored
            return stored["result"]

        x = extraction()
        x.update(
            item_id=iid,
            statement=approved,
            tokens=["world:WLD"],
            entities=[{"name": "World", "entity_id": "world", "role": "ACTOR"}],
            public_at={
                "value": stamp,
                "precision": "EXACT",
                "basis": "Quoted synthetic primary publication timestamp",
                "quote": stamp,
            },
        )
        x["claims"] = [
            {
                "type": "STATE",
                "verified": True,
                "subject": "World",
                "predicate": "approved",
                "object": "access",
                "state_claim": "APPROVED",
                "directionality": "BOTH",
                "evidence_class": "PRIMARY_FACT",
                "quote": approved,
            },
            {
                "type": "MECHANISM",
                "verified": True,
                "subject": "world:WLD",
                "predicate": "required for",
                "object": "protocol gas fees",
                "state_claim": None,
                "directionality": "BOTH",
                "evidence_class": "PRIMARY_FACT",
                "quote": mechanism,
            },
        ]
        created = await tool("known-facts", "record_event", x)
        eid = created["event_id"]
        if outcome == "native_error_failed":
            await resumed.process_event(sid, {"id": "native-error-1", "type": "session.error"})
            assert (await service.repo.get("ops.agent_sessions", sid))["status"] == "OPEN"
            await resumed.process_event(sid, {"id": "native-error-2", "type": "session.error"})
            await resumed.process_event(sid, {"id": "native-error-2", "type": "session.error"})
            await resumed.process_event(sid, failed_call)
            session = await service.repo.get("ops.agent_sessions", sid)
            event = await service.repo.get("evt.events", eid)
            assert session["status"] == "FAILED" and session["terminal_failure"]["event_type"] == "session.error"
            assert event["lifecycle"] != "FAILED" and "failure_reason" not in event
            messages = [p["events"][0] for p in native_posts if p["events"][0].get("type") == "user.message"]
            assert len(messages) == 2 and messages[0] == messages[1]
            assert not await service.repo.list("out.alerts")
        else:
            unknown = {
                "value": None,
                "unit": "USD",
                "window": "EVENT",
                "observed_at": None,
                "precision": None,
                "coverage": None,
                "reason": "NOT_ESTIMABLE",
            }
            chain = {
                name: dict(unknown)
                for name in [
                    "headline_activity",
                    "eligible_activity",
                    "effective_fees",
                    "protocol_capture",
                    "token_flow_annual_usd",
                    "token_flow_pct_mcap",
                ]
            }
            chain.update(
                token_id="world:WLD",
                capture_score=8,
                token_mechanism={
                    "type": "GAS",
                    "active": True,
                    "evidence": [created["accepted_claims"][1]["claim_id"]],
                },
            )
            assessment = unknown_assessment(quote=approved)
            assessment["capture_chains"] = [chain]
            await tool("assessment", "set_assessment", {"event_id": eid, "assessment": assessment})
            await tool(
                "beneficiaries",
                "set_beneficiaries",
                {
                    "event_id": eid,
                    "edges": [
                        {
                            "entity_id": "world",
                            "token_id": "world:WLD",
                            "role": "PRIMARY",
                            "layer": "ISSUER_PRODUCT",
                            "direction": "POSITIVE",
                            "causal_chain": mechanism,
                            "directionality": "BOTH",
                            "exposure_score": 6,
                            "edge_certainty": "HIGH",
                            "evidence": [c["claim_id"] for c in created["accepted_claims"]],
                        }
                    ],
                    "layer_reasons": {layer: "No additional adopted link is established" for layer in LAYERS},
                },
            )

            async def settle_owned_missing_input_jobs():
                # This declared fixture has no retained market observations.
                # Complete only this tool's durable rendezvous with the real
                # empty-input formula (UNKNOWN), never a fabricated tool result.
                deadline = asyncio.get_running_loop().time() + 5
                while asyncio.get_running_loop().time() < deadline:
                    jobs = [
                        job
                        for job in await service.repo.list("ops.jobs")
                        if job["type"] in {"ESCALATE_TOKENS", "RUN_MARKET_TRUTH"}
                        and job["payload"].get("event_id") == eid
                        and job["payload"].get("token_id") == "world:WLD"
                    ]
                    for job in sorted(jobs, key=lambda row: row["priority"]):
                        if job["status"] != "QUEUED":
                            continue
                        dependencies = [
                            await service.repo.get("ops.jobs", ref)
                            for ref in job["payload"].get("_depends_on", [])
                        ]
                        if any(not row or row["status"] != "DONE" for row in dependencies):
                            continue
                        job.update(status="RUNNING", attempts=job["attempts"] + 1)
                        if job["type"] == "RUN_MARKET_TRUTH":
                            truth = compute_market_truth(
                                {
                                    "event_id": eid,
                                    "token_id": "world:WLD",
                                    "cutoff": job["payload"]["cutoff"],
                                }
                            )
                            truth["mta_id"] = new_id("mta")
                            await service.repo.put("ana.market_truth", truth["mta_id"], truth)
                            job["result"] = truth
                        await service.repo.put("ops.jobs", job["job_id"], job)
                        await service.repo.finish_job(job["job_id"])
                        if job["type"] == "RUN_MARKET_TRUTH":
                            return
                    await asyncio.sleep(0)
                raise AssertionError("Owned market-truth job was not created or dependency did not settle")

            market_call = asyncio.create_task(
                tool(
                    "market-truth",
                    "get_market_truth",
                    {"event_id": eid, "tokens": ["world:WLD"], "cutoff": iso(), "cutoff_prec": "EXACT"},
                )
            )
            market_worker = asyncio.create_task(settle_owned_missing_input_jobs())
            try:
                truth_result, _ = await asyncio.gather(market_call, market_worker)
            finally:
                for task in (market_call, market_worker):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(market_call, market_worker, return_exceptions=True)
            assert truth_result["market_truth"][0]["token_id"] == "world:WLD"
            receipt = await service.repo.get("ops.tool_calls", sid + ":market-truth")
            assert receipt["status"] == "DONE" and receipt["returned_at"]
            assert (
                receipt["native_turn_message_id"]
                == (await service.repo.get("ops.agent_sessions", sid))["active_message_id"]
            )
            decision = await tool("decision", "get_decision", {"event_id": eid, "token": "world:WLD"})
            assert decision["action"] == "WAIT" and decision["outlook"]["direction"] == "UNKNOWN"
            card = await tool(
                "card",
                "publish_card",
                {
                    "event_id": eid,
                    "kind": "FULL",
                    "verdict": approved,
                    "why": ["What changed: World access approved.", "Economic effect: Not estimable."],
                },
            )
            assert card["published"], card
            assert len(await service.repo.list("out.alerts")) == 1
            assert any(j["type"] == "DELIVER" for j in await service.repo.list("ops.jobs"))
            assert (await service.repo.get("ops.agent_sessions", sid))["status"] == "OPEN"
        assert len(fetched) == 1
        assert all(
            row["status"] != "PENDING" and row.get("returned_at")
            for row in await service.repo.list("ops.tool_calls", filters={"session_id": sid})
        )


async def test_trusted_reference_clock_changes_logical_age_only(service, tmp_path, monkeypatch):
    import copy
    import json

    from tank.fixture_reference import TrustedFixtureClock
    from tank.replay import digest, load_inputs
    from tank.util import parse_time, sha256

    physical = "2026-10-19T00:00:00Z"
    public = "2022-09-21T00:00:00Z"
    logical = "2022-09-22T00:00:00Z"
    body = "Product announced"
    raw = b"<p>Product announced</p>"
    raw_path = tmp_path / "original.html"
    raw_path.write_bytes(raw)
    item = dict(
        item_id="i",
        source_id="S01",
        feed="fixture:official",
        external_id="original-v1",
        text=body,
        source_class="PRIMARY",
        observed_at=physical,
        observed_at_prec="COLLECTION_ONLY",
        published_at=public,
        published_at_precision="BUCKET(1d)",
        raw_ref=str(raw_path),
        raw_sha256=sha256(raw),
        content_hash=sha256(body),
        extraction_status="PENDING",
        relevance={"decision": "ALWAYS"},
    )
    packet = dict(
        replay_mode="FIXTURE_REFERENCE_RECONSTRUCTION",
        fixture_reference=dict(
            schema="tank.fixture-reference.v1",
            fixture_id="F-18",
            preregistered_at=physical,
            cutoff_spec="21 Sep 2022",
            cutoff_policy="UTC_DAY_END_EXCLUSIVE",
            cutoff_day="2022-09-21",
            cutoff_timezone="UTC",
            historical_live_run=False,
        ),
        cutoff=logical,
        assembled_at=physical,
        config_version="test",
        code_version="test",
        registry={},
        config={
            "replay_messages": [
                dict(
                    kind="NEW_ITEM",
                    item_id="i",
                    origin="FIXTURE_AUTHORED_REFERENCE",
                    historical=False,
                    replay_at=logical,
                )
            ]
        },
        tool_results=[],
        records=[
            dict(
                collection="ingest.source_items",
                key="i",
                available_at=physical,
                simulated_available_at=logical,
                simulated_availability_basis="CONSERVATIVE_PUBLICATION_UPPER_BOUND",
                as_of_evidence=dict(
                    kind="IMMUTABLE_ORIGINAL_PUBLICATION",
                    version_id="original-v1",
                    source_url="https://fixture.invalid/original",
                    raw_ref=str(raw_path),
                    raw_sha256=sha256(raw),
                    body_sha256=sha256(body),
                ),
                record=item,
            )
        ],
    )
    packet["input_sha256"] = digest(packet)
    path = tmp_path / "reference.json"
    path.write_text(json.dumps(packet))
    options = service.settings.model_copy(
        update={"replay": True, "config_version": "test", "code_version": "test"}
    )
    clock = TrustedFixtureClock(load_inputs(path, options))
    with pytest.raises(ValueError, match="isolated fixture"):
        EventToolService(service.repo, service.settings, None, None, reference_clock=clock)
    with pytest.raises(ValueError, match="isolated fixture"):
        EventToolService(service.repo, options, None, None, reference_clock=object())
    reference = EventToolService(service.repo, options, None, None, reference_clock=clock)
    x = extraction()
    x["public_at"] = dict(value=public, precision="DAY", basis="Original publication", quote=None)
    x["event_time"] = dict(value=public, precision="BUCKET_1D", quote=None)
    x["freshness"] = "POOR"
    assert service.validate_time(x, item) == public
    assert reference.validate_time(x, item) == public
    await service.repo.put("ingest.source_items", "i", item)
    stored_before = await service.repo.get("ingest.source_items", "i")
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: parse_time("2022-09-22T01:00:00Z"))
    view = await reference.dispatch("get_item", {"item_id": "i"}, {})
    assert view["observed_at"] == physical
    assert view["freshness_reference"]["value"] == logical
    assert view["freshness_reference"]["clock_domain"] == "FIXTURE_SIMULATED_AS_OF"
    assert view["analysis_cutoff"] == "2022-09-22T01:00:00Z"
    assert view["simulated_available_at"] == logical
    assert await service.dispatch("get_item", {"item_id": "i"}, {}) == stored_before
    assert reference.validate_time(dict(x, freshness="STALE"), item) == public
    found = await reference.dispatch("search_tank_items", {"query": "Product", "days": 1}, {})
    assert [entry["item_id"] for entry in found["items"]] == ["i"]
    assert found["items"][0] == view
    assert not (await service.dispatch("search_tank_items", {"query": "Product", "days": 1}, {}))["items"]
    assert not (await reference.dispatch("search_tank_items", {"query": "Product", "sources": ["S99"]}, {}))[
        "items"
    ]
    assert await service.repo.get("ingest.source_items", "i") == stored_before
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: parse_time(logical) - timedelta(seconds=1))
    assert not (await reference.dispatch("search_tank_items", {"query": "Product"}, {}))["items"]
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: parse_time(logical))
    assert (await reference.dispatch("search_tank_items", {"query": "Product"}, {}))["items"][0][
        "item_id"
    ] == "i"
    # Even a sealed source is invisible until the prefix importer admits it.
    unimported = EventToolService(MemoryRepository(), options, None, None, reference_clock=clock)
    assert not (await unimported.dispatch("search_tank_items", {"query": "Product"}, {}))["items"]
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: parse_time("2022-09-22T01:00:00Z"))
    result = await reference.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", result["event_id"])
    assert result["accepted_claims"][0]["evidence"][0]["quote"] == body
    observations = await service.repo.list("evt.event_observations")
    assert event["known_to_us_at"] == logical
    assert event["fixture_reference_provenance"]["clock_domain"] == "FIXTURE_SIMULATED_AS_OF"
    assert observations[0]["observed_at"] == physical
    assert observations[0]["observed_at_prec"] == "COLLECTION_ONLY"
    assert observations[0]["simulated_available_at"] == logical
    assert (await service.repo.get("ingest.source_items", "i"))["observed_at"] == physical
    await reference.dispatch(
        "add_observation",
        dict(
            event_id=event["event_id"],
            item_id="i",
            link="CORROBORATION",
            certainty_reason="Agent observation judgment",
            claims=x["claims"],
            public_at=x["public_at"],
            freshness=x["freshness"],
            certainty="HIGH",
        ),
        {},
    )
    observations = await service.repo.list("evt.event_observations")
    assert len(observations) == 2
    assert all(row["observed_at"] == physical for row in observations)
    assert all(row["simulated_available_at"] == logical for row in observations)
    live_x = copy.deepcopy(x)
    live_x["freshness"] = "STALE"
    assert service.validate_time(live_x, item) == public
    assert service.item_known_at(item).value == physical
    tampered = dict(item, text="Changed content")
    with pytest.raises(ValueError, match="sealed fixture"):
        reference.validate_time(x, tampered)
    future = copy.deepcopy(x)
    future["public_at"]["value"] = "2022-09-23T00:00:00Z"
    assert reference.validate_time(future, item) == future["public_at"]["value"]


@pytest.mark.parametrize("explicit_stage", [False, True])
async def test_quoted_proposal_requires_explicit_stage_claim_not_fact_keyword(service, explicit_stage):
    quote = "The Securities and Exchange Commission is proposing new custody rules."
    x = extraction()
    x.update(statement="SEC proposes new custody rules", tokens=[], entities=[])
    x["claims"] = [
        dict(
            verified=True,
            type="STATE" if explicit_stage else "FACT",
            subject="SEC",
            predicate="proposes",
            object="new custody rules",
            state_claim="PROPOSED" if explicit_stage else None,
            directionality="NONE",
            evidence_class="PRIMARY_FACT",
            quote=quote,
        )
    ]
    await service.repo.put(
        "ingest.source_items",
        "i",
        dict(
            item_id="i",
            source_id="S03",
            source_class="PRIMARY",
            text=quote,
            observed_at=iso(),
            observed_at_prec="COLLECTION_ONLY",
        ),
    )
    result = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", result["event_id"])
    assert result["accepted_claims"][0]["evidence"][0]["quote"] == quote
    assert event["event_state"] == ("PROPOSED" if explicit_stage else "RUMOR")
    assert event["state_verified"] is explicit_stage
    assert not await service.repo.list("ana.market_truth")
    assert not await service.repo.list("ana.decisions")


@pytest.mark.parametrize("paraphrase", [False, True])
async def test_primary_event_fact_certainty_allows_neutral_paraphrase(service, paraphrase):
    quote = "The SEC announced approval of the application."
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = quote
    await service.repo.put("ingest.source_items", "i", item)
    x.update(
        statement="SEC approved the application." if paraphrase else quote,
        entities=[{"name": "SEC", "entity_id": "sec", "role": "REGULATOR"}],
        tokens=[],
    )
    x["claims"] = [
        dict(
            verified=True,
            type="STATE",
            subject="SEC",
            predicate="approved",
            object="the application",
            state_claim="APPROVED",
            directionality="BOTH",
            evidence_class="PRIMARY_FACT",
            quote=quote,
        )
    ]
    created = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", created["event_id"])
    assert event["certainty"] == "HIGH" and event["state_verified"]
    assert event["certainty_evidence_claims"] == [created["accepted_claims"][0]["claim_id"]]


async def test_primary_selection_paraphrase_preserves_fact_certainty(service):
    # Actual retained primary selection wording; no class or token-capture expectation.
    quote = (
        "The Clearing House announced the selection of Quant, a leading provider of programmable money "
        "infrastructure, to power its On-Chain Money Initiative, a new interoperable payments network "
        "that will enable financial institutions of all sizes to clear and settle tokenized deposit transactions."
    )
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item.update(source_id="S27", text=quote)
    await service.repo.put("ingest.source_items", "i", item)
    x.update(
        statement="The Clearing House selected Quant for its planned On-Chain Money Initiative.",
        entities=[
            {"name": "The Clearing House", "entity_id": "the-clearing-house", "role": "ACTOR"},
            {"name": "Quant", "entity_id": "quant", "role": "COUNTERPARTY"},
        ],
        tokens=["quant:QNT"],
    )
    x["claims"] = [
        dict(
            verified=True,
            type="STATE",
            subject="the-clearing-house",
            predicate="selected",
            object="Quant to power the planned On-Chain Money Initiative",
            state_claim="APPROVED",
            directionality="COUNTERPARTY_ADOPTION",
            evidence_class="PRIMARY_FACT",
            quote=quote,
        )
    ]
    created = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", created["event_id"])
    assert event["certainty"] == "HIGH" and event["event_state"] == "APPROVED"
    assert event["certainty_evidence_claims"]


@pytest.mark.parametrize("scope", ["registered_alias", "token_entity", "unresolved_name", "unresolved_id"])
async def test_primary_fact_scope_uses_declared_identity_not_word_overlap(service, scope):
    x = await seed(service)
    if scope.startswith("unresolved"):
        name = "Northstar Lab"
        entities = [{"name": name, "entity_id": "UNRESOLVED:" + name, "role": "ACTOR"}]
        tokens = []
        subject = name if scope == "unresolved_name" else "UNRESOLVED:" + name
    else:
        name, subject = "World", "World"
        entities = [] if scope == "token_entity" else [{"name": name, "entity_id": "world", "role": "ACTOR"}]
        tokens = ["world:WLD"]
    quote = name + " announced a planned product launch."
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = quote
    await service.repo.put("ingest.source_items", "i", item)
    x.update(statement=name + " plans to launch a product.", entities=entities, tokens=tokens)
    x["claims"] = [
        dict(
            verified=True,
            type="STATE",
            subject=subject,
            predicate="announced",
            object="a future product",
            state_claim="APPROVED",
            directionality="BOTH",
            evidence_class="PRIMARY_FACT",
            quote=quote,
        )
    ]
    created = await service.dispatch("record_event", x, {})
    assert (await service.repo.get("evt.events", created["event_id"]))["certainty"] == "HIGH"


@pytest.mark.parametrize("failure", ["no_quote", "secondary_source", "other_entity", "empty_subject"])
async def test_event_certainty_is_not_host_semantic_scope_judgment(service, failure):
    x = await seed(service)
    quote = "SEC approved the application."
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = quote
    if failure == "secondary_source":
        item.update(source_id="S33", source_class="SECONDARY", author="CoinDesk")
    await service.repo.put("ingest.source_items", "i", item)
    x.update(statement=quote, entities=[{"name": "SEC", "entity_id": "sec", "role": "REGULATOR"}], tokens=[])
    subject = "World" if failure == "other_entity" else ("" if failure == "empty_subject" else "SEC")
    x["claims"] = [
        dict(
            verified=True,
            type="STATE",
            subject=subject,
            predicate="approved",
            object="the application",
            state_claim="APPROVED",
            directionality="BOTH",
            evidence_class="PRIMARY_FACT",
            quote="Absent from source" if failure == "no_quote" else quote,
        )
    ]
    created = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", created["event_id"])
    assert event["certainty"] == x["certainty"] and event["certainty_reason"] == x["certainty_reason"]


async def test_explicit_agent_corroboration_updates_certainty(service):
    x = await seed(service)
    quote = "World announced approval of a new product."
    item = await service.repo.get("ingest.source_items", "i")
    item.update(text=quote, source_id="S33", source_class="SECONDARY", author="CoinDesk")
    await service.repo.put("ingest.source_items", "i", item)
    x.update(
        statement="World approved a new product.",
        entities=[{"name": "World", "entity_id": "world", "role": "ACTOR"}],
    )
    x["claims"] = [
        dict(
            verified=True,
            type="STATE",
            subject="World",
            predicate="approved",
            object="a new product",
            state_claim="APPROVED",
            directionality="BOTH",
            evidence_class="PRIMARY_FACT",
            quote=quote,
        )
    ]
    created = await service.dispatch("record_event", x, {})
    assert (await service.repo.get("evt.events", created["event_id"]))["certainty"] == x["certainty"]
    primary = dict(
        item, item_id="primary-corroboration", source_id="S15", source_class="PRIMARY", author="World"
    )
    await service.repo.put("ingest.source_items", primary["item_id"], primary)
    await service.dispatch(
        "add_observation",
        dict(
            event_id=created["event_id"],
            item_id=primary["item_id"],
            link="CORROBORATION",
            certainty_reason="Agent observation judgment",
            claims=x["claims"],
            certainty="HIGH",
            public_at=x["public_at"],
            freshness="FRESH",
        ),
        {},
    )
    event = await service.repo.get("evt.events", created["event_id"])
    assert event["certainty"] == "HIGH" and event["state_verified"]
    assert event["certainty_reason"] == "Agent observation judgment"


@pytest.mark.parametrize(
    "case",
    [
        "verbatim",
        "paraphrase",
        "same_entity_annual_report",
        "wrong_claim",
        "unrelated_entity",
        "inactive_instrument",
        "wrong_token",
        "missing_quote",
        "future_date",
    ],
)
async def test_immediate_access_uses_agent_structured_fact_and_effective_time(service, case):
    from tank.util import new_id

    stamp = iso()
    quote = "World access approved and effective " + stamp + " for the World instrument."
    unrelated = "SEC registration approved and effective " + stamp + " for a different instrument."
    report_quote = "World published its annual report on " + stamp + "."
    item_id = new_id("item")
    await service.repo.put(
        "ingest.source_items",
        item_id,
        dict(
            item_id=item_id,
            source_id="S01",
            source_class="PRIMARY",
            text=quote + " " + unrelated + " " + report_quote,
            observed_at=stamp,
        ),
    )
    x = extraction()
    x.update(
        item_id=item_id,
        statement=quote if case == "verbatim" else "World approved access to its instrument.",
        tokens=["world:WLD"],
        entities=[{"name": "World", "entity_id": "world", "role": "ACTOR"}],
        public_at={
            "value": stamp,
            "precision": "EXACT",
            "basis": "Primary access announcement",
            "quote": quote,
        },
    )
    x["claims"] = [
        dict(
            verified=True,
            type="STATE",
            subject=subject,
            predicate="approved effective access",
            object="the announced instrument",
            state_claim="APPROVED",
            directionality="BOTH",
            evidence_class="PRIMARY_FACT",
            quote=evidence,
        )
        for subject, evidence in [("world", quote), ("sec", unrelated)]
    ]
    if case == "same_entity_annual_report":
        x["claims"].append(
            dict(
                verified=True,
                type="FACT",
                subject="world",
                predicate="published",
                object="annual report",
                state_claim=None,
                directionality="NONE",
                evidence_class="PRIMARY_FACT",
                quote=report_quote,
            )
        )
    created = await service.dispatch("record_event", x, {})
    eid = created["event_id"]
    proof = created["accepted_claims"][0]["claim_id"]
    await service.repo.put(
        "reg.instruments",
        "access-contract-test",
        dict(
            instrument_id="access-contract-test",
            token_id="quant:QNT" if case == "wrong_token" else "world:WLD",
            active=case != "inactive_instrument",
        ),
    )
    assessment = unknown_assessment(quote=quote)
    assessment["impact_horizon"].update(
        impact_start="NOW",
        immediate_market_access={
            "claim_id": "not-an-event-claim"
            if case == "wrong_claim"
            else (created["accepted_claims"][1]["claim_id"] if case == "unrelated_entity" else proof),
            "instrument_id": "access-contract-test",
            "effective_at": iso(utcnow() + timedelta(days=1)) if case == "future_date" else stamp,
            "quote": "" if case == "missing_quote" else (unrelated if case == "unrelated_entity" else quote),
        },
    )
    if case == "same_entity_annual_report":
        report_claim = created["accepted_claims"][-1]
        assert report_claim["accepted"] and report_claim["verified"]
        assessment["impact_horizon"]["immediate_market_access"].update(
            claim_id=report_claim["claim_id"], quote=report_quote
        )
    if case == "missing_quote":
        # The declared schema requires a non-empty quote (minLength 1).
        with pytest.raises(ValueError, match="immediate_market_access.quote: shorter than 1"):
            await service.dispatch("set_assessment", dict(event_id=eid, assessment=assessment), {})
    elif case not in {"wrong_claim", "future_date"}:
        result = await service.dispatch("set_assessment", dict(event_id=eid, assessment=assessment), {})
        assert (await service.repo.get("evt.ems_assessments", result["ems_id"]))["impact_horizon"][
            "impact_start"
        ] == "NOW"
    else:
        with pytest.raises(ValueError, match="immediately effective market access"):
            await service.dispatch("set_assessment", dict(event_id=eid, assessment=assessment), {})


@pytest.mark.parametrize("case", ["valid", "omitted", "nonexistent", "cross_event", "self"])
async def test_observation_explicit_claim_replacement_preserves_history(service, case):
    import copy

    x = await seed(service)
    created = await service.dispatch("record_event", x, {"session_id": "s"})
    await service.repo.put("ops.agent_sessions", "s", {"session_id": "s", "event_id": created["event_id"]})
    old = created["accepted_claims"][0]
    original = copy.deepcopy(await service.repo.get("evt.claims", old["claim_id"]))
    await service.repo.put(
        "ingest.source_items",
        "confirmation",
        {
            "item_id": "confirmation",
            "source_id": "S01",
            "text": "Confirmed",
            "observed_at": iso(),
            "source_class": "PRIMARY",
        },
    )
    claim = copy.deepcopy(x["claims"][0])
    if case != "omitted":
        ref = old["claim_id"]
        if case == "nonexistent":
            ref = "missing"
        elif case == "cross_event":
            ref = "foreign"
            await service.repo.put("evt.claims", ref, dict(original, claim_id=ref, event_id="other"))
        elif case == "self":
            ref = "self"
            claim["claim_id"] = ref
        claim["supersedes_claim_ids"] = [ref]
    args = dict(
        event_id=created["event_id"],
        item_id="confirmation",
        link="STATE_UPDATE",
        certainty="HIGH",
        certainty_reason="Agent confirmation",
        claims=[claim],
        public_at=x["public_at"],
        freshness="FRESH",
    )
    if case in {"nonexistent", "cross_event", "self"}:
        with pytest.raises(ValueError):
            await service.add_observation(args, {})
    else:
        result = await service.dispatch("add_observation", args, {"session_id": "s"})
        assert result
        claims = await service.rows("evt.claims", created["event_id"])
        assert len(claims) == 2, result
        added = next(c for c in claims if c["claim_id"] != old["claim_id"])
        assert added.get("supersedes_claim_ids", []) == ([old["claim_id"]] if case == "valid" else [])
    assert await service.repo.get("evt.claims", old["claim_id"]) == original


async def test_secondary_default_summary_does_not_duplicate_layer_notes(service):
    from tank.event_tools import LAYERS

    created = await service.dispatch("record_event", await seed(service), {})
    args = {
        "event_id": created["event_id"],
        "edges": [],
        "layer_reasons": {layer: "No supported rail: " + layer for layer in LAYERS},
    }
    result = await service.dispatch("set_beneficiaries", args, {})
    assert (
        result["no_credible_secondary_reason"]
        == "No SECONDARY edge satisfies the credibility requirements in §4.7."
    )
    assert {k: v["note"] for k, v in result["layers"].items()} == args["layer_reasons"]
    args["no_credible_secondary_reason"] = "Agent-specific uncertainty retained verbatim"
    result = await service.dispatch("set_beneficiaries", args, {})
    assert result["no_credible_secondary_reason"] == args["no_credible_secondary_reason"]


@pytest.fixture
async def live_update_database(service, backend):
    """The queue consumer must never see jobs owned by another PostgreSQL test."""
    if backend == "memory":
        yield None
        return
    import os
    import uuid

    from psycopg import AsyncConnection, sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    source = os.getenv("TANK_TEST_DATABASE_URL")
    if not source:
        pytest.skip("Requires isolated PostgreSQL with CREATE DATABASE permission")
    admin = await AsyncConnection.connect(source, autocommit=True)
    name = "tank_publication_" + uuid.uuid4().hex
    created = False
    try:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        created = True
        parameters = conninfo_to_dict(source)
        parameters["dbname"] = name
        yield make_conninfo(**parameters)
    finally:
        try:
            await service.repo.close()
        finally:
            try:
                if created:
                    await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
            finally:
                await admin.close()


@pytest.mark.parametrize("backend", ["memory", "postgres"])
async def test_live_update_survives_restart_and_delivers_at_boundary_with_retry(
    service, monkeypatch, backend, live_update_database
):
    """Exercise real publication, immutable storage, durable queue and transport ledger."""
    import copy
    import json

    import httpx

    from tank.delivery import DeliveryError, DeliveryService
    from tank.storage import PostgresRepository
    from tank.util import new_id, parse_time

    database_url = live_update_database
    if backend == "postgres":
        service.repo = PostgresRepository(database_url)
        await service.repo.open()
        await service.repo.migrate()
    repo = service.repo
    # PostgreSQL's queue uses its actual clock. The simulated due boundary is
    # just before that clock so the due worker can claim without changing jobs.
    base = utcnow() - timedelta(minutes=15, seconds=3)
    clock = [base]
    for module in ("tank.util", "tank.event_tools", "tank.analysis.cards", "tank.delivery", "tank.storage"):
        monkeypatch.setattr(module + ".utcnow", lambda: clock[0])
    eid = new_id("event")
    token = "world:WLD"
    ems = eid + "-ems"
    decision = complete_decision_fixture(eid + "-wait", eid, token, iso(base), eid + "-truth")
    assert decision["action"] == "WAIT"
    for table, key, value in [
        (
            "evt.events",
            eid,
            complete_event_fixture(
                eid, current_ems_id=ems, event_state="APPROVED", freshness="FRESH", known_to_us_at=iso(base)
            ),
        ),
        ("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid)),
        ("evt.beneficiary_maps", eid + ":1", {"event_id": eid, "map_version": 1, "analysis_set": [token]}),
        (
            "evt.beneficiary_edges",
            eid + "-edge",
            {
                "edge_id": eid + "-edge",
                "event_id": eid,
                "map_version": 1,
                "token_id": token,
                "role": "PRIMARY",
                "tradable": True,
                "credible": True,
                "exposure_score": 8,
            },
        ),
        ("ana.market_truth", eid + "-truth", complete_truth_fixture(eid + "-truth", eid, token, iso(base))),
        ("ana.decisions", decision["dec_id"], decision),
    ]:
        await repo.put(table, key, value)
    args = {
        "event_id": eid,
        "kind": "FULL",
        "verdict": "Access approved",
        "why": ["Official approval", "Defined access"],
    }
    requests = []
    reject_next = [False]

    def telegram(request):
        requests.append(json.loads(request.content))
        if reject_next[0]:
            reject_next[0] = False
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 1}})
        return httpx.Response(
            200, json={"ok": True, "result": {"message_id": 42 if len(requests) == 1 else 43}}
        )

    service.settings.telegram_token = "fixture"
    service.settings.telegram_alerts_chat = "1"
    service.settings.telegram_ops_chat = "2"
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
            original = await service.dispatch("publish_card", args, {})
            first_job = await repo.claim_job()
            assert first_job["payload"]["alert_id"] == original["alert_id"]
            await DeliveryService(repo, service.settings, http=http).deliver(first_job["payload"])
            await repo.finish_job(first_job["job_id"])
            assert len(requests) == 1
            clock[0] = base + timedelta(minutes=5)
            event = await repo.get("evt.events", eid)
            await repo.put("evt.events", eid, {**event, "event_state": "LIVE", "lifecycle": "ANALYZING"})
            args.update(kind="UPDATE", verdict="Access is now live")
            accepted = await service.dispatch("publish_card", args, {})
            assert accepted["published"] and accepted["reason"] == "QUEUED_RATE_LIMIT"
            assert parse_time(accepted["delivery_not_before"]) == base + timedelta(minutes=15)
            frozen = copy.deepcopy(await repo.get("out.alerts", original["alert_id"] + ":2"))
            repeated = await service.dispatch("publish_card", args, {})
            assert repeated["version"] == 2 and repeated["reason"] == "IDENTICAL_PENDING"
            assert len(await repo.list("out.alerts", filters={"event_id": eid})) == 2
            update_jobs = await repo.list(
                "ops.jobs",
                filters={"type": "DELIVER", "idem_key": original["alert_id"] + ":2:telegram:alerts"},
            )
            assert len(update_jobs) == 1
            assert update_jobs[0]["run_after"] == accepted["delivery_not_before"]
            with pytest.raises(DeliveryError):
                await DeliveryService(repo, service.settings, http=http).deliver(update_jobs[0]["payload"])
            assert len(requests) == 1
            if backend == "memory":
                assert await repo.claim_job() is None
            else:
                await repo.close()
                repo = PostgresRepository(database_url)
                await repo.open()
                service.repo = repo
            # Recreate services after restart. No new source item/tool invocation.
            clock[0] = base + timedelta(minutes=15)
            job = await repo.claim_job()
            assert job["job_id"] == update_jobs[0]["job_id"]
            reject_next[0] = True
            with pytest.raises(DeliveryError) as rejected:
                await DeliveryService(repo, service.settings, http=http).deliver(job["payload"])
            assert rejected.value.retry_after == 1
            await repo.fail_job(job["job_id"], rejected.value, retry_after=1)
            repeated_after_failure = await service.dispatch("publish_card", args, {})
            assert repeated_after_failure["version"] == 2
            assert repeated_after_failure["reason"] == "IDENTICAL_PENDING"
            assert len(await repo.list("out.alerts", filters={"event_id": eid})) == 2
            clock[0] += timedelta(seconds=1)
            retry = await repo.get("ops.jobs", job["job_id"])
            assert retry["status"] == "QUEUED"
            assert parse_time(retry["run_after"]) == clock[0]
            retry = await repo.claim_job()
            assert retry["job_id"] == job["job_id"] and retry["attempts"] == 2
            await DeliveryService(repo, service.settings, http=http).deliver(job["payload"])
            await repo.finish_job(job["job_id"])
            await DeliveryService(repo, service.settings, http=http).deliver(job["payload"])
            assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "DONE"
            assert len(requests) == 3
            assert requests[1]["reply_parameters"] == requests[2]["reply_parameters"] == {"message_id": 42}
            assert await repo.get("out.alerts", original["alert_id"] + ":2") == frozen
            ledger = await repo.list(
                "out.deliveries", filters={"alert_id": original["alert_id"], "version": 2}
            )
            assert [row["status"] for row in ledger].count("FAILED") == 1
            assert [row["status"] for row in ledger].count("SENT") == 1
            assert all(not row.get("suppressed") for row in ledger)
    finally:
        if backend == "postgres":
            await repo.close()


async def test_old_trigger_updates_stage_but_does_not_publish_or_resurface_on_timer(service, monkeypatch):
    import tank.analysis.cards as cards

    monkeypatch.setattr(cards, "compose_card", lambda *args, **kwargs: "Stored stage update")
    extracted = await seed(service)
    extracted["claims"][0]["state_claim"] = "APPROVED"
    created = await service.dispatch("record_event", extracted, {})
    eid = created["event_id"]
    ems = eid + "-ems"
    event = await service.repo.get("evt.events", eid)
    await service.repo.put("evt.events", eid, {**event, "current_ems_id": ems})
    await service.repo.put("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid))
    await service.repo.put(
        "evt.beneficiary_maps", eid + ":1", {"event_id": eid, "map_version": 1, "analysis_set": []}
    )
    args = {
        "event_id": eid,
        "kind": "FULL",
        "verdict": "Approved",
        "why": ["Official fact", "Defined access"],
    }
    first = await service.dispatch("publish_card", args, {"trigger_kind": "NEW_ITEM", "trigger_item_id": "i"})
    assert first["published"]
    await service.repo.put(
        "out.deliveries",
        "stale-original-sent",
        {
            "alert_id": first["alert_id"],
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "at": iso(utcnow() - timedelta(minutes=20)),
            "at_prec": "COLLECTION_ONLY",
            "message_ref": "42",
            "attempt": 1,
        },
        immutable=True,
    )
    public = iso(utcnow() - timedelta(days=3))
    await service.repo.put(
        "ingest.source_items",
        "old-reprint",
        {"item_id": "old-reprint", "source_id": "S01", "text": "Now live", "observed_at": iso()},
    )
    claim = dict(extracted["claims"][0], state_claim="LIVE", quote="Now live")
    await service.dispatch(
        "add_observation",
        {
            "event_id": eid,
            "item_id": "old-reprint",
            "link": "STATE_UPDATE",
            "claims": [claim],
            "certainty": "HIGH",
            "certainty_reason": "Official old release",
            "public_at": {
                "value": public,
                "precision": "EXACT",
                "basis": "Source publication",
                "quote": public,
            },
            "freshness": "STALE",
        },
        {},
    )
    assert (await service.repo.get("evt.events", eid))["event_state"] == "LIVE"
    assert (await service.repo.get("evt.events", eid))["freshness"] == "FRESH"
    args.update(kind="UPDATE", verdict="Now live")
    for context in [{"trigger_kind": "ITEM_ADDED", "trigger_item_id": "old-reprint"}, {}]:
        refused = await service.dispatch("publish_card", args, context)
        assert not refused["published"] and refused["reason"] == "STALE_STATE_CHANGE"
    timer = await service.dispatch("publish_card", args, {"trigger_kind": "CONDITION_MET"})
    assert not timer["published"] and timer["reason"] == "NO_MATERIAL_CHANGE"
    assert len(await service.repo.list("out.alerts", filters={"event_id": eid})) == 1
    assert len(await service.repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1
    stored = await service.repo.get("evt.events", eid)
    assert "publication_trigger_freshness" not in stored
    assert "publication_stage_freshness" not in stored


@pytest.mark.parametrize("role", ["PRIMARY", "SECONDARY", "NONE"])
async def test_each_layer_needs_agent_reason_when_primary_does_not_supply_a_secondary(service, role):
    from tank.event_tools import LAYERS

    created = await service.dispatch("record_event", await seed(service), {})
    eid = created["event_id"]
    proof = created["accepted_claims"][0]["claim_id"]
    await service.repo.put(
        "evt.capture_chains",
        "layer-reason-capture",
        {
            "event_id": eid,
            "token_id": "world:WLD",
            "version": 1,
            "capture_score": 8,
            "capture_class": "DIRECT",
            "token_mechanism": {"evidence": [proof]},
        },
    )
    edges = (
        []
        if role == "NONE"
        else [
            {
                "entity_id": "world",
                "token_id": "world:WLD",
                "role": role,
                "layer": "ISSUER_PRODUCT",
                "direction": "POSITIVE",
                "causal_chain": "Required direct rail",
                "directionality": "BOTH",
                "exposure_score": 6,
                "edge_certainty": "HIGH",
                "evidence": [proof],
            }
        ]
    )
    notes = {
        layer: "No adopted provider evidenced: " + layer for layer in LAYERS if layer != "ISSUER_PRODUCT"
    }
    args = {"event_id": eid, "edges": edges, "layer_reasons": notes}
    if role != "SECONDARY":
        # A real primary benefit does not explain the absence of intermediaries.
        with pytest.raises(ValueError, match="ISSUER_PRODUCT.*SECONDARY"):
            await service.dispatch("set_beneficiaries", args, {})
        notes["ISSUER_PRODUCT"] = "   "
        with pytest.raises(ValueError, match="ISSUER_PRODUCT.*SECONDARY"):
            await service.dispatch("set_beneficiaries", args, {})
        notes["ISSUER_PRODUCT"] = "World issues the product; no additional issuer earns token-linked fees."
    result = await service.dispatch("set_beneficiaries", args, {})
    layer = result["layers"]["ISSUER_PRODUCT"]
    assert layer["note"] == notes.get("ISSUER_PRODUCT", "")
    assert "Evidence-backed edges" not in {row["note"] for row in result["layers"].values()}
    stored = await service.rows("evt.beneficiary_edges", eid)
    if role in {"PRIMARY", "SECONDARY"}:
        submitted = [row for row in stored if row["role"] == role and row.get("credible")]
        assert submitted and all(row["evidence"] == [proof] for row in submitted)
        assert layer["status"] == "FOUND"
        assert "world:WLD" in result["analysis_set"]
    if role == "SECONDARY":
        assert result["no_credible_secondary_reason"] is None


async def test_tokens_and_entities_resolve_to_registry_ids(service):
    """W5 (03 §2.11, V3): tickers, $TICKER and registered aliases resolve to the
    token id the universe check and routing use; other names stay UNRESOLVED."""
    from tank.event_tools import resolve_entity, resolve_token

    registry = service.settings.load_config("registry")
    assert resolve_token(registry, "UNI") == "uniswap:UNI"
    assert resolve_token(registry, "$uni") == "uniswap:UNI"
    assert resolve_token(registry, "Uniswap Labs") == "uniswap:UNI"
    assert resolve_token(registry, "uniswap:UNI") == "uniswap:UNI"
    assert resolve_token(registry, "litentry:LIT") == "litentry:LIT"
    assert resolve_token(registry, "Lighter") == "UNRESOLVED:Lighter"
    assert resolve_token(registry, "Lighter", context="Lighter (LIT) lists") == "lighter:LIT"
    assert resolve_token(registry, "Foo") == "UNRESOLVED:Foo"
    assert resolve_entity(registry, "Plasma") == "UNRESOLVED:Plasma"
    assert resolve_entity(registry, "Plasma", context="XPL") == "plasma"
    assert resolve_entity(registry, "Ava Labs") == "avalanche"
    assert resolve_entity(registry, "", None) == "UNRESOLVED:"
    x = await seed(service)
    x.update(
        tokens=["UNI", "$UNI", "Foo"],
        entities=[{"name": "Uniswap Labs", "entity_id": None, "role": "ACTOR"}],
    )
    eid = (await service.dispatch("record_event", x, {}))["event_id"]
    event = await service.repo.get("evt.events", eid)
    assert event["tokens_mentioned"] == ["uniswap:UNI", "UNRESOLVED:Foo"]
    assert event["entities"] == [{"entity_id": "uniswap", "role": "ACTOR"}]


async def test_least_repriced_ranks_all_credible_tokens_on_partial_refresh(service):
    """W6 (03 §4.7 step 9): refreshing one token's truth keeps ranking every
    credible beneficiary from its latest stored truth."""
    import asyncio

    eid = "least-event"
    await service.repo.put("evt.events", eid, {"event_id": eid, "current_ems_id": "least-ems"})
    await service.repo.put(
        "evt.ems_assessments", "least-ems", {"ems_id": "least-ems", "event_id": eid, "class": "MAJOR"}
    )
    await service.repo.put(
        "evt.beneficiary_maps",
        f"{eid}:1",
        {"event_id": eid, "map_version": 1, "analysis_set": ["A", "B"]},
    )
    for token in ["A", "B"]:
        await service.repo.put(
            "evt.beneficiary_edges",
            "edge-" + token,
            {
                "edge_id": "edge-" + token,
                "event_id": eid,
                "map_version": 1,
                "token_id": token,
                "role": "SECONDARY",
                "credible": True,
                "exposure_score": 6,
            },
        )

    def truth(token, band, pctile, cutoff):
        return {
            "mta_id": "mta-" + token + cutoff,
            "event_id": eid,
            "token_id": token,
            "cutoff": cutoff,
            "liquidity": {"liquidity_state": "NORMAL"},
            "relative": {"repricing_band": band, "windows": {"EVENT": {"pctile": pctile}}},
        }

    earlier = iso(utcnow() - timedelta(minutes=5))
    await service.repo.put("ana.market_truth", "mta-B", truth("B", "UNPRICED", 0.2, earlier))
    fresh = truth("A", "HEAVY", 0.9, iso())
    task = asyncio.create_task(service.dispatch("get_market_truth", {"event_id": eid, "tokens": ["A"]}, {}))
    try:
        while not task.done():
            job = await service.repo.claim_job()
            if job:
                if job["type"] == "RUN_MARKET_TRUTH":
                    stored = await service.repo.get("ops.jobs", job["job_id"])
                    stored["result"] = fresh
                    await service.repo.put("ops.jobs", job["job_id"], stored)
                await service.repo.finish_job(job["job_id"])
            await asyncio.sleep(0.01)
        await task
    finally:
        if not task.done():
            task.cancel()
    mapping = await service.repo.get("evt.beneficiary_maps", f"{eid}:1")
    assert mapping["least_repriced_edge"] == "edge-B"


async def test_list_open_events_skips_events_whose_session_failed(service):
    """03 §3.2 Failures: a failed session ends the event's analysis, so the agent
    never sees it as open and never merges later news into it."""
    for eid, status in [("live-event", "OPEN"), ("failed-event", "FAILED")]:
        await service.repo.put(
            "evt.events", eid, {"event_id": eid, "lifecycle": "ANALYZING", "statement": "World news"}
        )
        await service.repo.put(
            "ops.agent_sessions", "session-" + eid, {"session_id": "session-" + eid, "event_id": eid, "status": status}
        )
    listed = await service.dispatch("list_open_events", {"entity_or_token": "world"}, {})
    assert [row["event_id"] for row in listed["events"]] == ["live-event"]


async def test_token_facts_show_unlocks_before_the_cutoff_and_nothing_after(service):
    """F-02 / 03 §3.2: get_token_facts returns supply and unlocks known at the
    cutoff, including a recent past unlock; rows observed later stay hidden."""
    now = utcnow()
    rows = {
        "past": {"date": iso(now - timedelta(days=3)), "observed_at": iso(now - timedelta(days=10))},
        "next": {"date": iso(now + timedelta(days=5)), "observed_at": iso(now - timedelta(days=1))},
        "future-capture": {"date": iso(now + timedelta(days=6)), "observed_at": iso(now + timedelta(hours=1))},
    }
    for label, row in rows.items():
        await service.repo.put(
            "mkt.unlocks", "layerzero:ZRO:" + label, {"token_id": "layerzero:ZRO", "label": label, **row}
        )
    facts = await service.dispatch("get_token_facts", {"token": "layerzero:ZRO"}, {})
    assert [(u["label"], u["relative_to_cutoff"]) for u in facts["unlocks"]] == [("past", "PAST"), ("next", "UPCOMING")]
    # A late-receipt reconstruction is known from the end of its schedule_as_of
    # bucket, as replay admits it (lead rulings F-02, F-04).
    from tank.event_tools import known_at

    day = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    reconstructed = {
        "observed_at": iso(now + timedelta(days=1)),
        "evidence_mode": "HISTORICAL_RECONSTRUCTION",
        "schedule_as_of": iso(day),
        "schedule_as_of_prec": "BUCKET(1d)",
    }
    assert known_at(reconstructed) == day + timedelta(days=1)
    assert known_at({"observed_at": iso(day)}) == day
    assert facts["analysis_cutoff"] and "supply_obs" in facts and "capture_chains" in facts


def test_schema_refusal_names_allowed_cutoff_precisions():
    """A precision outside the declared schema is refused at the schema with the
    allowed forms, not deep in the tool (an undeclared value such as "FORTNIGHT")."""
    import json
    from pathlib import Path

    schema = json.loads(
        (Path(__file__).resolve().parents[1] / "config/skills/tank-event-analyst/get_market_truth.schema.json").read_text()
    )
    args = {"event_id": "e", "tokens": ["T"], "cutoff": "2026-10-20T19:07:00Z"}
    validate_schema(dict(args, cutoff_prec="BUCKET(1m)"), schema)
    validate_schema(dict(args, cutoff_prec="EXACT"), schema)
    with pytest.raises(ValueError) as refused:
        validate_schema(dict(args, cutoff_prec="FORTNIGHT"), schema)
    assert "$.cutoff_prec" in str(refused.value) and "EXACT" in str(refused.value) and "BUCKET" in str(refused.value)


async def test_token_tools_resolve_tickers_and_refuse_unknown_names_explicitly(service):
    """E2E msg 15/16: get_decision("BTC") must reach bitcoin:BTC, and an
    unknown name gets an error that states the expected form."""
    resolved = service.resolve_token_arguments("get_decision", {"event_id": "e", "token": "BTC"})
    assert resolved["token"] == "bitcoin:BTC"
    many = service.resolve_token_arguments("get_market_truth", {"event_id": "e", "tokens": ["$WLD", "uniswap:UNI"]})
    assert many["tokens"] == ["world:WLD", "uniswap:UNI"]
    assert service.resolve_token_arguments("get_token_facts", {"token": "Bitcoinz"}) == {"token": "Bitcoinz"}
    assert service.resolve_token_arguments("get_event", {"event_id": "e"}) == {"event_id": "e"}
    await service.repo.put("evt.events", "e", {"event_id": "e", "current_ems_id": "ems"})
    await service.repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": "e", "class": "MAJOR"})
    await service.repo.put(
        "evt.beneficiary_maps", "e:1", {"event_id": "e", "map_version": 1, "analysis_set": ["bitcoin:BTC"]}
    )
    with pytest.raises(ValueError, match=r"'Bitcoinz'.*analysis set: \['bitcoin:BTC'\]"):
        await service.dispatch("get_decision", {"event_id": "e", "token": "Bitcoinz"}, {})


def test_publish_card_schema_bounds_agent_text():
    """E2E msg 16: agent text is bounded at the schema, before the card is built."""
    import json
    from pathlib import Path

    schema = json.loads(
        (Path(__file__).resolve().parents[1] / "config/skills/tank-event-analyst/publish_card.schema.json").read_text()
    )
    ok = {"event_id": "e", "kind": "FULL", "verdict": "v" * 160, "why": ["w" * 220, "x"]}
    validate_schema(ok, schema)
    with pytest.raises(ValueError, match=r"\$\.verdict: 161 characters, at most 160"):
        validate_schema(dict(ok, verdict="v" * 161), schema)
    with pytest.raises(ValueError, match=r"\$\.why\[0\]: 221 characters"):
        validate_schema(dict(ok, why=["w" * 221]), schema)


async def test_token_facts_for_btc_fit_the_context(service):
    """r107 P0: get_token_facts("bitcoin:BTC") returned 6.7 MB (143 raw supply
    rows with daily charts since 2011) and ended the session on the context
    window. It now returns the latest known row per kind, series shortened,
    under 40 KB, with the documented figures kept."""
    from tank.event_tools import TOOL_RESULT_MAX_BYTES
    from tank.util import canonical_json

    now = utcnow()
    chart = [[1296432000 + day * 86400, str(day)] for day in range(5500)]
    for n in range(143):
        kind = ["FUNDAMENTALS", "LENDING", "EXCHANGE_INVENTORY", "CHAIN_LIQUIDITY", "UNLOCKS"][n % 5]
        row = {
            "token_id": "bitcoin:BTC",
            "kind": kind,
            "ts": iso(now - timedelta(hours=143 - n)),
            "available": True,
        }
        if kind == "FUNDAMENTALS":
            daily = {"complete": True, "total24h": str(200000 + n), "total30d": "7277898", "available": True,
                     "totalDataChart": chart}
            row.update(metrics={"dailyFees": daily, "dailyRevenue": daily},
                       protocols={"bitcoin": {"dailyFees": dict(daily, source_url="https://api.llama.fi/x")}})
        await service.repo.put("mkt.supply_obs", f"btc:{n}", row)
    facts = await service.dispatch("get_token_facts", {"token": "BTC"}, {})
    size = len(canonical_json(facts).encode())
    assert size < TOOL_RESULT_MAX_BYTES, size
    kinds = [row["kind"] for row in facts["supply_obs"]]
    assert sorted(kinds) == ["CHAIN_LIQUIDITY", "EXCHANGE_INVENTORY", "FUNDAMENTALS", "LENDING", "UNLOCKS"]
    fundamentals = next(row for row in facts["supply_obs"] if row["kind"] == "FUNDAMENTALS")
    fees = fundamentals["metrics"]["dailyFees"]
    assert fees["total24h"] == "200140" and fees["totalDataChart"]["count"] == 5500
    assert len(fees["totalDataChart"]["last"]) == 7 and "_truncated" not in facts


async def test_read_tool_results_are_bounded_and_say_so(service):
    """Any read tool result above 40 KB is shortened with a note; list_open_events
    returns its compact view (03 §3.2: statements, stages, known_to_us_at, sessions)."""
    from tank.event_tools import TOOL_RESULT_MAX_BYTES, fit_result
    from tank.util import canonical_json

    big = {"text": "x" * 200_000, "hits": [{"title": "t", "text": "y" * 5000} for _ in range(40)]}
    fitted = fit_result(big)
    assert len(canonical_json(fitted).encode()) <= TOOL_RESULT_MAX_BYTES
    assert fitted["_truncated"]["original_bytes"] > 200_000 and "more characters stored" in fitted["text"]
    assert fit_result({"small": 1}) == {"small": 1}
    await service.repo.put(
        "evt.events",
        "open",
        {"event_id": "open", "lifecycle": "ANALYZING", "statement": "World news", "claims_blob": "z" * 50_000},
    )
    await service.repo.put("ops.agent_sessions", "s1", {"session_id": "s1", "event_id": "open", "status": "OPEN"})
    listed = await service.dispatch("list_open_events", {"entity_or_token": "world"}, {})
    event = listed["events"][0]
    assert "claims_blob" not in event and event["sessions"] == [{"session_id": "s1", "status": "OPEN"}]


async def test_daily_vesting_unlocks_stay_visible_around_the_cutoff(service):
    """F-04 (r108): with daily vesting (4 labels x many days) the 40 KB cap
    kept only the newest rows; the agent must see the tranches within 14 days
    of the cutoff, per label, at any schedule size."""
    from tank.util import canonical_json, parse_time

    now = utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    for label in ("Team", "Investors", "Ecosystem", "Treasury"):
        for day in range(-30, 400):
            await service.repo.put(
                "mkt.unlocks",
                f"asset-k:{label}:{day}",
                {
                    "token_id": "ethena:ENA",
                    "label": label,
                    "kind": "POTENTIAL_SUPPLY",
                    "date": iso(now + timedelta(days=day, hours=1)),
                    "qty": "1000",
                    "observed_at": iso(now - timedelta(days=40)),
                },
            )
    facts = await service.dispatch("get_token_facts", {"token": "ethena:ENA"}, {})
    assert len(canonical_json(facts).encode()) < 40_000
    summary = facts["unlock_summary"]
    assert summary["count"] == 4 * 430 and summary["window_days"] == 14
    team = next(row for row in summary["by_label"] if row["label"] == "Team")
    assert team["past_14d"] == {"count": 14, "qty": "14000"}
    assert team["upcoming_14d"]["count"] == 14
    assert team["nearest_past"]["date"] == iso(now - timedelta(days=1) + timedelta(hours=1))
    assert team["nearest_upcoming"]["date"] == iso(now + timedelta(hours=1))
    dates = [parse_time(row["date"]) for row in facts["unlocks"]]
    assert len(dates) == 14 and all(abs(d - now) <= timedelta(days=3) for d in dates)
    assert {row["relative_to_cutoff"] for row in facts["unlocks"]} == {"PAST", "UPCOMING"}


async def test_token_facts_give_the_covered_daily_volume(service):
    """03 §4.2 base of M (access and distribution): the token's covered daily
    volume per spot venue and in total, native and USD, with exclusions."""
    now = utcnow()
    rows = {
        "binance:spot:BTCUSDT": {"volume_native": "12000.5", "volume_usd": "900000000", "ts": iso(now - timedelta(minutes=2))},
        "coinbase:spot:BTC-USD": {"volume_native": "8000", "volume_usd": "600000000", "ts": iso(now - timedelta(minutes=1))},
        "kraken:spot:XBT/USD": {"volume_native": "100", "volume_usd": "7500000", "ts": iso(now - timedelta(hours=3))},
        "okx:spot:BTC-USDT": {"volume_native": "500", "volume_usd": None, "usd_reason": "NO_USD_RATE", "ts": iso(now)},
    }
    for iid, row in rows.items():
        await service.repo.put("mkt.tickers_24h", iid + ":t", {"instrument_id": iid, "token_id": "bitcoin:BTC", **row})
    facts = await service.dispatch("get_token_facts", {"token": "BTC"}, {})
    volume = facts["covered_daily_volume"]
    # Native over every fresh venue; USD only where the venue reports it.
    assert volume["total_native"] == "20500.5" and volume["total_usd"] == "1500000000"
    assert {v["instrument_id"] for v in volume["venues"]} == {
        "binance:spot:BTCUSDT", "coinbase:spot:BTC-USD", "okx:spot:BTC-USDT"
    }
    assert volume["total_usd_excludes"] == [{"instrument_id": "okx:spot:BTC-USDT", "reason": "NO_USD_RATE"}]
    reasons = {e["instrument_id"]: e["reason"] for e in volume["excluded"]}
    assert reasons["kraken:spot:XBT/USD"] == "TICKER_STALE" and reasons["bybit:spot:BTCUSDT"] == "NO_STORED_24H_TICKER"
    assert volume["market_cap"] is None and volume["market_cap_reason"].startswith("NOT_STORED")


async def test_card_generated_at_is_the_version_write_and_deliver_is_runnable_then(service, monkeypatch):
    """E2E #5 (r113): generated_at was stamped before the 8 s evidence bundle,
    so delivery looked late. generated_at is now when the immutable version is
    written (after the bundle is durable) and the DELIVER job runs from then."""
    import tank.analysis.cards as cards
    import tank.event_tools as event_tools
    from tank.util import parse_time

    monkeypatch.setattr(cards, "alert_gate", lambda *a, **k: (True, "INITIAL"))
    monkeypatch.setattr(cards, "compose_card", lambda *a, **k: "Frozen card")
    written = {}
    original = event_tools.write_evidence_bundle

    def timed(bundle, data_dir):
        result = original(bundle, data_dir)
        written["at"] = utcnow()
        return result

    monkeypatch.setattr(event_tools, "write_evidence_bundle", timed)
    x = await seed(service)
    eid = (await service.dispatch("record_event", x, {}))["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": eid, "class": "STRUCTURAL_SHOCK"}
    )
    result = await service.dispatch(
        "publish_card",
        {"event_id": eid, "kind": "PARTIAL", "verdict": "v", "why": []},
        {"event_id": eid, "call_id": "c", "role": "system"},
    )
    alert = await service.repo.get("out.alerts", f"{result['alert_id']}:1")
    assert parse_time(alert["generated_at"]) >= written["at"]
    job = (await service.repo.list("ops.jobs", filters={"type": "DELIVER"}))[0]
    assert parse_time(job["run_after"]) == parse_time(alert["generated_at"])
    assert alert["delivery_not_before"] == alert["generated_at"]


async def test_named_cutoff_precisions_are_accepted_as_buckets(service):
    """Live (22:51, 23:45, 05:05): cutoff_prec "MINUTE" was refused each time;
    named units now validate and normalize to their bucket, recorded in the result."""
    import asyncio
    import json
    from pathlib import Path

    schema = json.loads(
        (Path(__file__).resolve().parents[1] / "config/skills/tank-event-analyst/get_market_truth.schema.json").read_text()
    )
    for name in ("SECOND", "MINUTE", "HOUR"):
        validate_schema({"event_id": "e", "tokens": ["T"], "cutoff": "2026-10-21T05:05:00Z", "cutoff_prec": name}, schema)
    eid = "named-prec"
    await service.repo.put("evt.events", eid, {"event_id": eid, "current_ems_id": "ems", "tokens_mentioned": ["T"]})
    await service.repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": eid, "class": "MAJOR"})
    await service.repo.put("evt.beneficiary_maps", f"{eid}:1", {"event_id": eid, "map_version": 1, "analysis_set": ["T"]})
    task = asyncio.create_task(
        service.dispatch(
            "get_market_truth",
            {"event_id": eid, "tokens": ["T"], "cutoff": "2026-10-21T05:05:00Z", "cutoff_prec": "MINUTE"},
            {},
        )
    )
    try:
        while not task.done():
            job = await service.repo.claim_job()
            if job:
                if job["type"] == "RUN_MARKET_TRUTH":
                    assert job["payload"]["cutoff_prec"] == "BUCKET(1m)"
                    stored = await service.repo.get("ops.jobs", job["job_id"])
                    stored["result"] = {"token_id": "T", "mta_id": "m"}
                    await service.repo.put("ops.jobs", job["job_id"], stored)
                await service.repo.finish_job(job["job_id"])
            await asyncio.sleep(0.01)
        result = await task
    finally:
        if not task.done():
            task.cancel()
    assert result["cutoff_prec_normalized"] == {"from": "MINUTE", "to": "BUCKET(1m)"}

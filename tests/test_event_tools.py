from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue
from scheduling_support import RecordingReactionScheduling
from score_fixtures import unassessed_score_dimensions
from source_support import fixture_http_admission, fixture_request_budgets, fixture_versions

from tank.agents.context import CONTEXT_VERSION
from tank.analysis.page_access import LivePageAccess
from tank.contracts.json import JsonDocument
from tank.event_tools import EventToolService, freshness, validate_schema
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.telegram_rich import score_summary
from tank.util import JsonInput, canonical_json, clock_iso, iso, utcnow


@pytest.fixture
async def service(tmp_path: Path):
    repo = MemoryRepository()
    await repo.open()
    settings = Settings(data_dir=tmp_path)
    return EventToolService(
        repo,
        settings,
        None,
        None,
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )


@pytest.fixture
async def repository_lifetime():
    """Register lazy PostgreSQL contexts only after each test's backend/DSN guard."""
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as resources:
        yield resources


@pytest.fixture
def truth_job_enqueued(service, monkeypatch: pytest.MonkeyPatch):
    """Signal actual RUN_MARKET_TRUTH admission without wall-clock polling."""
    import asyncio

    ready = asyncio.Event()
    enqueue = service.repo.enqueue

    async def observed_enqueue(kind, *args, **kwargs):
        job = await enqueue(kind, *args, **kwargs)
        if kind == "RUN_MARKET_TRUTH":
            ready.set()
        return job

    monkeypatch.setattr(service.repo, "enqueue", observed_enqueue)
    return ready


def extraction() -> dict[str, JsonValue]:
    now = clock_iso()
    return dict(
        item_id="i",
        is_event=True,
        statement="Product announced",
        certainty="HIGH",
        certainty_reason="Official primary",
        entities=[],
        token_ids=["world:WLD"],
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


async def seed(service: EventToolService) -> dict[str, JsonValue]:
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


async def test_agent_declared_quote_stage_and_time_are_preserved(service) -> None:
    x = await seed(service)
    result = await service.dispatch("record_event", x, {"session_id": "s"})
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["event_state"] == "LIVE"
    assert event["event_time"]["precision"] == "EXACT"
    assert result["accepted_claims"][0]["verified"]
    assert len(await service.repo.list("evt.state_changes")) == 1


async def test_agent_quote_and_public_time_are_stored_without_host_semantics(service) -> None:
    x = await seed(service)
    x["claims"][0]["quote"] = "Agent selected evidence"
    x["public_at"]["value"] = iso(utcnow() + timedelta(days=1))
    result = await service.dispatch("record_event", x, {})
    assert result["dropped_claims"] == []
    assert result["accepted_claims"][0]["quote"] == "Agent selected evidence"
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["public_at"] == x["public_at"]["value"]


async def test_unknown_fields_and_freshness(service) -> None:
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


async def test_denial_requires_agent_verified_claim_and_invalidates(service) -> None:
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


async def test_publication_freezes_reproducible_bundle_and_queues_only(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tank.analysis.cards as cards
    from tank.util import sha256

    monkeypatch.setattr(
        cards, "compose_card", lambda *a, **k: "Frozen card" + "\n" + score_summary(a[0].assessment.scores)
    )
    x = await seed(service)
    result = await service.dispatch("record_event", x, {})
    eid = result["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "ems",
        {
            "dims": unassessed_score_dimensions(),
            "ems_id": "ems",
            "event_id": eid,
            "class": "STRUCTURAL_SHOCK",
            "criterion": "G5",
        },
    )
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "PARTIAL",
            "what": "Material partnership",
            "verdict": "Material partnership",
            "why": ["Defined access", "Relevant actor"],
        },
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
    jobs = [job for job in await service.repo.list("ops.jobs") if job["payload"].get("alert_id")]
    assert len(jobs) == 1
    assert jobs[0]["type"] == "DELIVER"
    assert jobs[0]["payload"]["channel"] == "telegram:alerts"


async def test_fetched_page_retains_original_bytes_before_parse(service) -> None:
    from pathlib import Path

    import zstandard

    from tank.util import sha256

    original = b"<html><script>hidden code</script><body>Official   evidence</body></html>"

    async def respond(request: httpx.Request):
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


def test_nullable_scores_skip_numeric_constraints() -> None:
    validate_schema(None, {"type": ["number", "null"], "minimum": 0, "maximum": 10})


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_numeric_map_version_ten_removes_old_token(
    service, backend, postgres_template, repository_lifetime
) -> None:
    import os

    from tank.util import new_id

    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        repo = await repository_lifetime.enter_async_context(postgres_template.repository(url))
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
            "ana.market_truth",
            eid + "mta",
            complete_truth_fixture(eid + "mta", eid, "world:WLD", clock_iso()),
        )
        with pytest.raises(ValueError, match="current-map"):
            await service.get_decision({"event_id": eid, "token_id": "world:WLD"}, {})
        with pytest.raises(ValueError, match="outside analysis set"):
            await service.get_market_truth({"event_id": eid, "token_ids": ["world:WLD"]}, {"call_id": "c"})
    finally:
        if backend == "postgres":
            await service.repo.close()


async def test_real_decision_condition_reuse_expiry_and_consumption(service) -> None:
    from test_decision_contracts import decision_wire_fixture

    from tank.analysis.decisions import DecisionEngine

    service.engine = DecisionEngine()
    authored_event, authored_assessment, authored_edge, authored_truth = decision_wire_fixture()
    eid = "condition-event"
    token = "world:WLD"
    event = {
        **authored_event,
        "event_id": eid,
        "current_ems_id": "condition-ems",
        "event_state": "APPROVED",
        "state_verified": True,
        "freshness": "FRESH",
    }
    assessment = {
        **authored_assessment,
        "dims": unassessed_score_dimensions(),
        "ems_id": "condition-ems",
        "event_id": eid,
        "class": "MAJOR",
        "certainty": "HIGH",
        "impact_horizon": {"impact_start": "NOW"},
    }
    edge = {
        **authored_edge,
        "edge_id": "condition-edge",
        "event_id": eid,
        "map_version": 1,
        "token_id": token,
        "role": "PRIMARY",
        "credible": True,
        "capture_class": "DIRECT",
    }
    truth = dict(
        authored_truth,
        mta_id="condition-truth",
        event_id=eid,
        token_id=token,
        cutoff=clock_iso(),
        cutoff_prec="BUCKET(1m)",
    )
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
                "opened_at": clock_iso(),
                "opened_at_prec": "BUCKET(1h)",
                "invalidation_price": "9",
            },
        ),
    ]:
        await service.repo.put(table, key, row)
    first = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert all(condition["expires_at"] and condition["created_at"] for condition in first["conditions"])
    assert first["action"] == "HOLD"
    assert first["cutoff_prec"] == "BUCKET(1m)"
    assert all(c["created_at_prec"] == "COLLECTION_ONLY" for c in first["conditions"])
    assert all(c["expires_at_prec"] == "BUCKET(1h)" for c in first["conditions"])
    armed = next(c for c in first["conditions"] if c["role"] == "UPGRADE")
    armed.update(
        counters={
            "root": {
                "count": 1,
                "window": 42,
                "value": "BUY",
                "data_available": True,
                "unavailable_reason": None,
                "cutoff": truth["cutoff"],
                "cutoff_prec": truth["cutoff_prec"],
                "cutoff_prec_reason": None,
            }
        }
    )
    await service.repo.put("ana.watch_conditions", armed["cond_id"], armed)
    second = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    same = next(c for c in second["conditions"] if c["role"] == "UPGRADE")
    assert same["cond_id"] == armed["cond_id"]
    assert same["counters"] == armed["counters"]
    same.update(status="FIRED", fired_at=clock_iso())
    await service.repo.put("ana.watch_conditions", same["cond_id"], same)

    await service.repo.put(
        "ana.market_truth",
        "condition-truth-fresh",
        {**truth, "mta_id": "condition-truth-fresh", "cutoff": same["fired_at"]},
    )
    fired = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert fired["action"] == "ADD"
    consumed = await service.repo.get("ana.watch_conditions", same["cond_id"])
    assert consumed["consumed_by"] == fired["dec_id"]
    again = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert again["action"] == "HOLD"

    rearmed = next(c for c in again["conditions"] if c["role"] == "UPGRADE")
    assert rearmed["cond_id"] != same["cond_id"] and rearmed["status"] == "ARMED"
    assert rearmed["rearmed_from"] == same["cond_id"] and "counters" not in rearmed
    assert same["cond_id"] not in again["conditions_shown"]
    assert (await service.repo.get("ana.watch_conditions", same["cond_id"]))["status"] == "FIRED"
    assert len(await service.repo.list("ana.watch_conditions")) == len(first["conditions"]) + 1
    rearmed.update(status="FIRED", fired_at=clock_iso())
    await service.repo.put("ana.watch_conditions", rearmed["cond_id"], rearmed)
    await service.repo.put(
        "ana.market_truth",
        "condition-truth-second",
        {**truth, "mta_id": "condition-truth-second", "cutoff": rearmed["fired_at"]},
    )
    second_add = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert second_add["action"] == "ADD"


async def test_start_condition_is_rearmed_for_the_open_call(service) -> None:
    """W1: the upgrade that led to START is spent; once the call is open the
    same expression is armed again, so ADD stays reachable."""
    from test_decision_contracts import decision_wire_fixture

    from tank.analysis.decisions import DecisionEngine

    service.engine = DecisionEngine()
    authored_event, authored_assessment, authored_edge, authored_truth = decision_wire_fixture()
    eid, token = "start-event", "world:WLD"
    await service.repo.put(
        "evt.events",
        eid,
        {
            **authored_event,
            "event_id": eid,
            "current_ems_id": "start-ems",
            "event_state": "APPROVED",
            "state_verified": True,
        },
    )
    await service.repo.put(
        "evt.ems_assessments",
        "start-ems",
        {
            **authored_assessment,
            "dims": unassessed_score_dimensions(),
            "ems_id": "start-ems",
            "event_id": eid,
            "class": "MAJOR",
            "certainty": "HIGH",
        },
    )
    await service.repo.put(
        "evt.beneficiary_maps", "start-map", {"event_id": eid, "map_version": 1, "analysis_set": [token]}
    )
    await service.repo.put(
        "evt.beneficiary_edges",
        "start-edge",
        {
            **authored_edge,
            "edge_id": "start-edge",
            "event_id": eid,
            "map_version": 1,
            "token_id": token,
            "role": "PRIMARY",
            "credible": True,
            "capture_class": "DIRECT",
        },
    )
    truth = dict(
        authored_truth,
        mta_id="start-truth",
        event_id=eid,
        token_id=token,
        cutoff=clock_iso(),
        move_class="ORGANIC_MIXED",
        data_confidence={"class": "MEDIUM"},
    )
    await service.repo.put("ana.market_truth", "start-truth", truth)
    watch = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert watch["action"] == "WATCH_TO_START"
    upgrade = next(c for c in watch["conditions"] if c["role"] == "UPGRADE")
    upgrade.update(status="FIRED", fired_at=clock_iso())
    await service.repo.put("ana.watch_conditions", upgrade["cond_id"], upgrade)
    await service.repo.put(
        "ana.market_truth",
        "start-truth-2",
        {**truth, "mta_id": "start-truth-2", "cutoff": clock_iso(), "data_confidence": {"class": "HIGH"}},
    )
    start = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert start["action"] == "START"
    await service.repo.put(
        "ana.calls",
        "start-call",
        {
            "call_id": "start-call",
            "event_id": eid,
            "token_id": token,
            "status": "OPEN",
            "opened_at": clock_iso(),
            "invalidation_price": "9",
        },
    )
    hold = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert hold["action"] == "HOLD"
    add_watch = next(c for c in hold["conditions"] if c["role"] == "UPGRADE")
    assert add_watch["status"] == "ARMED" and add_watch["cond_id"] != upgrade["cond_id"]
    assert add_watch["expr"] == upgrade["expr"]


async def test_agent_confirmation_certainty_and_reason_are_preserved(service) -> None:
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


async def test_numeric_ems_requires_accepted_evidence(service) -> None:
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
        "criterion_claim_id": result["accepted_claims"][0]["claim_id"],
        "dims": dims,
        "impact_horizon": {"impact_start": "WEEKS"},
    }
    with pytest.raises(ValueError, match="requires accepted claim evidence"):
        await service.set_assessment({"event_id": result["event_id"], "assessment": assessment}, {})


async def test_milestone_exact_escalation_window(service) -> None:
    from tank.util import parse_time

    x = await seed(service)
    result = await service.dispatch("record_event", x, {})
    await service.repo.put("reg.sources", "S01", {"source_id": "S01"})
    due = iso(utcnow() + timedelta(days=1))
    await service.dispatch(
        "schedule_milestone",
        {"event_id": result["event_id"], "milestone": "Decision", "due_at": due, "source_id": "S01"},
        {},
    )
    source = (await service.repo.list("ops.source_escalations", filters={"source_id": "S01"}))[0]
    assert await service.repo.get("reg.sources", "S01") == {"source_id": "S01"}
    assert parse_time(source["starts_at"]) == parse_time(due) - timedelta(minutes=5)
    assert parse_time(source["expires_at"]) == parse_time(due) + timedelta(hours=2)


async def test_assessment_escalates_all_official_sources_in_consumer_table(service) -> None:
    from tank.sources import SourceService

    x = await seed(service)
    x["token_ids"] = ["world:WLD"]
    x["entities"] = [{"entity_id": "world", "role": "ACTOR"}]
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
        "criterion_claim_id": result["accepted_claims"][0]["claim_id"],
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
    sources = SourceService(
        service.repo, service.settings, versions=fixture_versions(), budgets=fixture_request_budgets()
    )
    for source_id in expected:
        assert await sources.is_escalated(source_id)
    assert not await sources.is_escalated("S01")


async def test_structural_assessment_requires_agent_partial_without_parallel_publisher(service) -> None:
    x = await seed(service)
    x["token_ids"] = ["world:WLD"]
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
        "criterion_claim_id": extraction["accepted_claims"][0]["claim_id"],
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
        "what": "World access approved",
        "verdict": "World access approved",
        "why": ["World access approved", "The publication does not quantify price or liquidity effects."],
    }
    result = await service.dispatch(
        "publish_card", args, {"event_id": extraction["event_id"], "role": "analyst"}
    )
    assert result["published"], result
    alert = await service.repo.get("out.alerts", result["alert_id"] + ":1")
    assert alert["kind"] == "PARTIAL"
    assert not alert["decisions"]
    assert alert["rich_message"]["blocks"][1]["text"] == [
        {"type": "bold", "text": "WHAT:"},
        " " + args["what"],
    ]
    assert alert["card_struct"]["what"] == args["what"]
    assert alert["rich_message"]["blocks"][2]["type"] == "table"
    bundle = await service.repo.get("out.evidence_bundles", alert["evidence_bundle_id"])
    assert bundle["payload"]["rich_message"] == alert["rich_message"]
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


async def test_real_session_tool_refuses_cross_event_mutation(service) -> None:
    from tank.agents import SessionService

    await service.repo.put("evt.events", "A", {"event_id": "A", "lifecycle": "DETECTED"})
    original = {"event_id": "B", "lifecycle": "DETECTED", "statement": "Untouched"}
    await service.repo.put("evt.events", "B", original)
    session = {
        "session_id": "scope-session",
        "model_context_version": CONTEXT_VERSION,
        "event_id": "A",
        "item_id": "i",
    }
    await service.repo.put("ops.agent_sessions", session["session_id"], session)
    sessions = SessionService(service.repo, service.settings, service.dispatch, versions=fixture_versions())
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
            "input": {"event_id": "B", "milestone": "Wrong event", "due_at": clock_iso(), "source_id": "S01"},
        },
    )
    assert await service.repo.get("evt.events", "B") == original
    stored = await service.repo.get("ops.tool_calls", "scope-session:scope-call")
    assert stored["status"] == "REFUSED"
    assert stored["result"]["error"]["category"] == "VALIDATION"
    assert stored["returned_at"]
    assert replies
    assert not await service.repo.list("ops.source_escalations")

    from tank.agents.context import ModelContext

    await ModelContext(service.repo, service.settings, session).result(
        "list_open_events", {"events": [original]}
    )
    await sessions.tool(
        session, {"id": "scope-old", "name": "get_event", "input": {"event": {"statement": "Untouched"}}}
    )
    old = await service.repo.get("ops.tool_calls", "scope-session:scope-old")
    assert old["status"] == "REFUSED" and "pass event_id" in old["result"]["error"]["reason"]
    await sessions.tool(session, {"id": "scope-read", "name": "get_event", "input": {"event_id": "B"}})
    assert (await service.repo.get("ops.tool_calls", "scope-session:scope-read"))["result"][
        "event"
    ] == original


async def test_novel_layer_proposal_survives_real_session_transaction(service) -> None:
    from tank.agents import SessionService

    x = await seed(service)
    extracted = await service.dispatch("record_event", x, {})
    eid = extracted["event_id"]
    proof = extracted["accepted_claims"][0]["claim_id"]
    session = {
        "session_id": "novel-session",
        "model_context_version": CONTEXT_VERSION,
        "event_id": eid,
        "item_id": "i",
    }
    await service.repo.put("ops.agent_sessions", session["session_id"], session)
    sessions = SessionService(service.repo, service.settings, service.dispatch, versions=fixture_versions())

    async def request(*args, **kwargs):
        if args and args[0] == "GET":
            return resolved_native_domain_fixture(session["session_id"])
        return {}

    sessions.request = request
    edge = {
        "entity_id": "world",
        "token_id": "world:WLD",
        "role": "SECONDARY",
        "layer": "NOVEL_LAYER",
        "direction": "POSITIVE",
        "causal_chain": "A new kind of rail",
        "directionality": "SOURCE_SIDE_SUPPORT",
        "exposure_score": 4,
        "edge_certainty": "MEDIUM",
        "evidence": [proof],
    }
    await sessions.tool(
        session, {"id": "novel-call", "name": "set_beneficiaries", "input": {"edges": [edge]}}
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


async def test_unbound_analyst_cannot_mutate_before_extraction(service) -> None:
    refused = await service.dispatch(
        "set_beneficiaries", {"event_id": "other", "edges": []}, {"role": "analyst", "session_id": "unbound"}
    )
    assert refused["reason"] == "EVENT_SCOPE"


async def test_agent_verification_of_world_and_openai_is_persisted(service) -> None:
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
        token_ids=["world:WLD"],
        entities=[
            {"entity_id": "world", "role": "ACTOR"},
            {"entity_id": "openai", "role": "COUNTERPARTY"},
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


async def test_future_wording_does_not_override_agent_stage(service) -> None:
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = "Trading opens tomorrow at 09:00 UTC."
    await service.repo.put("ingest.source_items", "i", item)
    x["claims"][0].update(quote=item["text"], state_claim="LIVE")
    result = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", result["event_id"])
    assert event["event_state"] == "LIVE"
    assert event["state_verified"]


@pytest.mark.parametrize("least_exposure", [None, 0, 6])
async def test_full_requires_entire_analysis_then_pins_diverse_three_tokens(
    service, least_exposure: int | None
) -> None:
    from tank.analysis.decision_codec import decode_decision, encode_outlook
    from tank.event_tools import LAYERS

    extracted = await seed(service)
    created = await service.dispatch("record_event", extracted, {})
    eid = created["event_id"]
    claim_id = created["accepted_claims"][0]["claim_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "full-ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "full-ems",
        complete_assessment_fixture("full-ems", eid, **{"class": "MAJOR", "criterion": "M2"}),
    )
    named = {
        "P1": "world:WLD",
        "P2": "ethereum:ETH",
        "P3": "bitcoin:BTC",
        "S1": "uniswap:UNI",
        "S2": "cow:COW",
        "S3": "lighter:LIT",
        "L1": "curve:CRV",
    }
    tokens = list(named.values())
    layers = {layer: {"note": "No additional role is asserted by this synthetic map."} for layer in LAYERS}
    await service.repo.put(
        "evt.beneficiary_maps",
        "full-map",
        {
            "event_id": eid,
            "map_version": 1,
            "analysis_set": tokens,
            "least_repriced_edge": "edge-" + named["S2"],
            "no_credible_secondary_reason": None,
            "layers": layers,
        },
    )

    async def store_calculation(token: str) -> None:
        cutoff = clock_iso()
        truth_id, decision_id = "truth-" + token, "dec-" + token
        await service.repo.put(
            "ana.market_truth", truth_id, complete_truth_fixture(truth_id, eid, token, cutoff)
        )
        row = complete_decision_fixture(decision_id, eid, token, cutoff, truth_id)
        record = decode_decision(row)
        await service.repo.put("ana.outlooks", record.outlook_id, encode_outlook(record))
        await service.repo.put("ana.decisions", decision_id, row)

    for index, (label, token) in enumerate(named.items()):
        role = "PRIMARY" if label.startswith("P") else "SECONDARY" if label.startswith("S") else "LOSER"
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
                "exposure_score": least_exposure if label == "S2" else 10 - index,
                "capture_class": "DIRECT",
                "causal_chain": "Authored synthetic token exposure to the approved product.",
                "layer": "LIQUIDITY_EXECUTION" if role == "SECONDARY" else "ISSUER_PRODUCT",
                "entity_id": None,
                "entity_name": None,
                "evidence": [claim_id],
                "integration_state": None,
                "integration_state_evidence": [],
            },
        )
        if label != "L1":
            await store_calculation(token)
    args = {
        "event_id": eid,
        "kind": "FULL",
        "what": "Approved fact",
        "verdict": "Approved fact",
        "why": ["Defined consequence", "Evidence available"],
    }
    refusal = await service.dispatch("publish_card", args, {})
    assert refusal["reason"] == "ANALYSIS_INCOMPLETE"
    assert refusal["missing_market_truth"] == [named["L1"]]
    assert refusal["missing_decisions"] == [named["L1"]]
    assert not await service.repo.list("out.alerts")
    await store_calculation(named["L1"])
    first = await service.dispatch("publish_card", args, {})
    alert = await service.repo.get("out.alerts", first["alert_id"] + ":1")
    expected = [named["P1"], named["S2"], named["S1"]]
    assert alert["shown_tokens"] == expected
    assert set(alert["decisions"]) == {"dec-" + token for token in expected}
    frozen = await service.repo.get("out.evidence_bundles", alert["evidence_bundle_id"])
    assert len(frozen["payload"]["decisions"]) == 7
    await service.repo.put(
        "evt.beneficiary_maps",
        "new-map",
        {
            "event_id": eid,
            "map_version": 2,
            "analysis_set": [named["P2"]],
            "least_repriced_edge": None,
            "no_credible_secondary_reason": "Current map has only the primary; prior card participants stay pinned.",
            "layers": layers,
        },
    )
    old_edge = await service.repo.get("evt.beneficiary_edges", "edge-" + named["P2"])
    await service.repo.put(
        "evt.beneficiary_edges",
        "new-P2",
        {**old_edge, "edge_id": "new-P2", "map_version": 2, "exposure_score": 10},
    )
    current_event = await service.repo.get("evt.events", eid)
    await service.repo.put(
        "evt.events", eid, {**current_event, "event_state": "MONEY_FLOWING", "state_verified": True}
    )
    assert current_event["event_state"] == "LIVE"
    args["kind"] = "UPDATE"
    update = await service.dispatch("publish_card", args, {})
    assert update["published"], update
    second = await service.repo.get("out.alerts", update["alert_id"] + ":2")
    assert second["shown_tokens"] == expected
    assert named["P2"] not in second["shown_tokens"]
    assert set(expected) <= await service.managed_tokens(eid)


@pytest.mark.integration
@pytest.mark.parametrize("freshness", ["FRESH", "STALE"])
async def test_real_expiry_to_update_delivery_never_reopens_call(
    service, monkeypatch: pytest.MonkeyPatch, freshness
) -> None:

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
        "dims": unassessed_score_dimensions(),
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
    first_decision = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert first_decision["action"] == "HOLD"
    first = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
            "what": "Access approved",
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

    async def telegram(request: httpx.Request):
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        delivery = DeliveryService(service.repo, service.settings, http=http)
        await delivery.deliver({"channel": "telegram:alerts", "alert_id": first["alert_id"], "version": 1})
        stale_event = await service.repo.get("evt.events", eid)
        stale_event["freshness"] = freshness
        await service.repo.put("evt.events", eid, stale_event)
        scheduler = Scheduler(service.repo, service.settings, None, None, versions=fixture_versions())
        await scheduler.expire_calls(now)
        assert (await service.repo.get("evt.events", eid))["call_expired"]
        lifecycle = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
        assert lifecycle["tracking_ended"] and lifecycle["actionable"] is False
        assert lifecycle["action"] == "HOLD"
        assert not lifecycle["conditions"]
        correction = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "UPDATE",
                "what": "Tracking window ended",
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
        await delivery.deliver(
            {"channel": "telegram:alerts", "alert_id": correction["alert_id"], "version": 2}
        )
        assert len(sent) == 2

    calls = await service.repo.list("ana.calls", filters={"event_id": eid})
    assert len(calls) == 1
    assert calls[0]["status"] == "CLOSED" and calls[0]["closed_by"] == "EXPIRED"
    assert not (await service.repo.get("evt.events", eid)).get("call_expired")
    again = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert again["tracking_ended"] and not again["actionable"]
    assert (await service.repo.get("ana.decisions", first_decision["dec_id"]))["action"] == "HOLD"


@pytest.mark.integration
async def test_material_update_is_queued_once_and_delivered_after_window(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:

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
                "dims": unassessed_score_dimensions(),
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
    first = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
    assert first["action"] == "HOLD"
    full = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
            "what": "Access approved",
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

    async def telegram(request: httpx.Request):
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        delivery = DeliveryService(service.repo, service.settings, http=http)
        await delivery.deliver({"channel": "telegram:alerts", "alert_id": full["alert_id"], "version": 1})
        low = dict(
            truth,
            mta_id="material-low",
            cutoff=clock_iso(),
            data_confidence={"class": "LOW", "critical_semantics_verified": False},
        )
        await service.repo.put("ana.market_truth", "material-low", low)
        reduced = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
        assert reduced["action"] == "REDUCE"
        args = {
            "event_id": eid,
            "kind": "UPDATE",
            "what": "Data coverage limited",
            "verdict": "Data coverage limited",
            "why": ["Coverage decreased", "Execution confidence lower"],
        }
        for _ in range(2):
            published = await service.dispatch("publish_card", args, {})
            assert published["published"] and published["version"] == 2
            assert published["publication_receipt"]["disposition"] == "PUBLISHED"
        assert len(await service.repo.list("out.alerts")) == 2
        assert len(await service.repo.list("out.evidence_bundles")) == 2
        assert (
            len(
                [
                    job
                    for job in await service.repo.list("ops.jobs", filters={"type": "DELIVER"})
                    if job["payload"].get("alert_id")
                ]
            )
            == 2
        )
        later = now + timedelta(minutes=16)
        for module in ("tank.analysis.cards", "tank.event_tools", "tank.delivery"):
            monkeypatch.setattr(module + ".utcnow", lambda: later)

        await delivery.deliver(
            {"channel": "telegram:alerts", "alert_id": published["alert_id"], "version": 2}
        )

    assert len(sent) == 2
    assert len(await service.repo.list("ana.calls", filters={"event_id": eid})) == 1


async def test_bundle_keeps_producing_model_and_separate_session_config(service) -> None:
    x = await seed(service)
    x["token_ids"] = ["world:WLD"]
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
    assert event["config_version"] == service.versions.config_version
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
        "criterion_claim_id": result["accepted_claims"][0]["claim_id"],
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
        {
            "event_id": event["event_id"],
            "kind": "PARTIAL",
            "what": "World access approved",
            "verdict": "World access approved",
            "why": ["World access approved", "The publication does not quantify price or liquidity effects."],
        },
        {"event_id": event["event_id"], "role": "system"},
    )
    assert published["published"], published
    bundle = (await service.repo.get("out.evidence_bundles", published["evidence_bundle_id"]))["payload"]
    assert bundle["model_ids"] == ["prior-model"]
    assert service.settings.openma_model not in bundle["model_ids"]
    assert bundle["prompt_versions"] == ["prior-prompt"]
    assert bundle["agent_versions"] == ["prior-agent"]
    assert {"prior-config", service.versions.config_version} <= set(bundle["config_versions"])


async def test_tool_schema_uses_frozen_runtime_snapshot_after_disk_edit(service, tmp_path: Path) -> None:
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


async def test_market_truth_domain_result_and_evidence_bundle_keep_full_metadata(
    service, truth_job_enqueued
) -> None:
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
        {
            "dims": unassessed_score_dimensions(),
            "ems_id": "metadata-ems",
            "event_id": eid,
            "class": "STRUCTURAL_SHOCK",
            "criterion": "G5",
        },
    )
    await service.repo.put(
        "evt.beneficiary_maps",
        "metadata-map",
        {
            "event_id": eid,
            "map_version": 1,
            "analysis_set": ["T"],
            "layers": {},
            "no_credible_secondary_reason": "No beneficiary is asserted in this isolated capture fixture.",
        },
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
            {"event_id": eid, "token_ids": ["T"], "cutoff": cutoff},
            {"call_id": "metadata-call"},
        )
    )
    try:
        async with asyncio.timeout(2):
            await truth_job_enqueued.wait()
            for kind in ("ESCALATE_TOKENS", "RUN_MARKET_TRUTH"):
                job = await service.repo.claim_job()
                assert job is not None and job["type"] == kind
                if kind == "RUN_MARKET_TRUTH":
                    await service.repo.put("ana.market_truth", "metadata-mta", expected)
                    stored = await service.repo.get("ops.jobs", job["job_id"])
                    stored["result"] = expected
                    await service.repo.put("ops.jobs", job["job_id"], stored)
                await service.repo.finish_job(job["job_id"])
            full = (await task)["market_truth"][0]
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert full == expected and full["measurement_metadata"]
    assert full["spot"]["W1H"]["coverage"]["n_complete"] == 0
    published = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "PARTIAL",
            "what": "Product announced",
            "verdict": "Product announced",
            "why": ["Product announced", "The publication does not quantify price or liquidity effects."],
        },
        {},
    )
    assert published["published"], published
    bundle = (await service.repo.get("out.evidence_bundles", published["evidence_bundle_id"]))["payload"]
    assert bundle["market_truth"][0] == expected
    assert (await service.repo.get("ana.market_truth", "metadata-mta")) == expected


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_long_history_uses_bounded_current_snapshots_and_exact_refs(
    service, backend, postgres_template, repository_lifetime
) -> None:
    import os

    from tank.analysis.decisions import DecisionEngine
    from tank.event_tools import LAYERS
    from tank.storage import TABLE_COLUMNS
    from tank.util import new_id

    base = service.repo
    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        base = await repository_lifetime.enter_async_context(postgres_template.repository(url))

    class Guarded:
        def __init__(self, repo) -> None:
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
    token = "world:WLD"
    now = utcnow()
    ems = eid + "-ems"
    current = eid + "-current"
    try:
        await base.put("evt.events", eid, complete_event_fixture(eid, current_ems_id=ems))
        await base.put("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid))
        await base.put(
            "evt.beneficiary_maps",
            eid + "-map",
            {
                "event_id": eid,
                "map_version": 1,
                "analysis_set": [token],
                "least_repriced_edge": None,
                "no_credible_secondary_reason": "Only the authored primary is supported in this fixture.",
                "layers": {
                    layer: {"note": "No additional role is asserted by this synthetic map."}
                    for layer in LAYERS
                },
            },
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
                "causal_chain": "Authored synthetic current-map primary link.",
                "layer": "ISSUER_PRODUCT",
                "evidence": [],
                "entity_name": None,
                "integration_state": None,
                "integration_state_evidence": [],
            },
        )
        historic_truth = complete_truth_fixture(eid + "-history", eid, token, iso(now - timedelta(days=2)))
        historic_decision = complete_decision_fixture(
            eid + "-history", eid, token, iso(now - timedelta(days=2)), eid + "-history"
        )

        historic_truth = {
            key: value for key, value in historic_truth.items() if key in TABLE_COLUMNS["ana.market_truth"]
        }
        historic_decision = {
            key: value
            for key, value in historic_decision.items()
            if key in TABLE_COLUMNS["ana.decisions"] or key == "market_truth_id"
        }
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
        truth = complete_truth_fixture(current, eid, token, iso(now))
        await base.put("ana.market_truth", current, truth)
        await base.put(
            "ana.market_truth",
            eid + "-future",
            dict(
                truth, mta_id=eid + "-future", cutoff=iso(now + timedelta(days=1)), move_class="DISTRIBUTION"
            ),
        )
        decision = await service.dispatch("get_decision", {"event_id": eid, "token_id": token}, {})
        assert decision["market_truth_id"] == current
        published = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "FULL",
                "what": "Access approved",
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


async def test_duckduckgo_results_are_durable_free_search_evidence(service) -> None:
    from pathlib import Path

    from tank.search import SearchHit, SearchResponse
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
            return SearchResponse(results=[SearchHit.model_validate(raw)], attempts=2)

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


async def test_search_failure_does_not_mutate_medium_event_certainty(service) -> None:
    from tank.search import SearchError

    x = await seed(service)
    x["certainty"] = "MEDIUM"
    created = await service.dispatch("record_event", x, {})
    event = await service.repo.get("evt.events", created["event_id"])

    class Search:
        async def search(self, query, *, on_attempt) -> None:
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


def unknown_assessment(criterion="M2", klass="MAJOR", claim=None):
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
        "criterion_claim_id": claim,
        "dims": dims,
        "impact_horizon": {
            "impact_start": "WEEKS",
            "ramp_shape": "GRADUAL",
            "dependencies": [],
            "time_to_materiality": "Unknown",
            "timing_confidence": "LOW",
        },
    }


async def test_primary_approved_edge_is_credible_and_exposure_layer_reasons_checked(service) -> None:
    from tank.event_tools import LAYERS

    x = await seed(service)
    x["token_ids"] = ["world:WLD"]
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


async def test_material_criterion_cannot_pass_major_and_measured_headline_only_caps(service) -> None:
    x = await seed(service)
    x["token_ids"] = ["world:WLD"]
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


async def test_other_tank_item_quote_read_through_real_session_is_admissible(service) -> None:
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
            "url": "https://example.org/fee-switch",
            "observed_at": clock_iso(),
        },
    )
    session = {
        "session_id": "item-proof-session",
        "model_context_version": CONTEXT_VERSION,
        "event_id": None,
        "item_id": "i",
    }
    await service.repo.put("ops.agent_sessions", session["session_id"], session)
    sessions = SessionService(service.repo, service.settings, service.dispatch, versions=fixture_versions())

    async def request(*a, **kw):
        if a and a[0] == "GET":
            return resolved_native_domain_fixture(session["session_id"])
        return {}

    sessions.request = request
    await sessions.tool(
        session, {"id": "search-other", "name": "search_tank_items", "input": {"query": "Fee switch"}}
    )
    await sessions.tool(
        session, {"id": "read-other", "name": "get_item", "input": {"item_id": "other-primary"}}
    )
    assert (await service.repo.get("ops.tool_calls", "item-proof-session:read-other"))["status"] == "DONE"
    x["claims"][0].update(quote="Fee switch approved.", state_claim="APPROVED")
    x.pop("item_id")
    x["token_ids"] = ["world:WLD"]
    await sessions.tool(session, {"id": "extract", "name": "record_event", "input": x})
    result = (await service.repo.get("ops.tool_calls", "item-proof-session:extract"))["result"]
    assert result["accepted_claims"][0]["evidence"] == [{"item_id": "i", "quote": "Fee switch approved."}]
    assert result["accepted_claims"][0]["verified"]


async def test_milestone_official_outcome_quote_clears_pending_and_preserves_history(service) -> None:
    x = await seed(service)
    created = await service.dispatch("record_event", x, {})
    eid = created["event_id"]
    due = clock_iso()
    await service.dispatch(
        "schedule_milestone",
        {"event_id": eid, "milestone": "Approval", "due_at": due, "source_id": "S01"},
        {},
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


async def test_scout_review_records_four_scores_and_merges_named_duplicate(service) -> None:
    first = await service.propose_pattern(
        {"hook": "BENEFICIARY_LAYER", "subject": "Concept A", "evidence": [{"claim_id": "claim-A"}]}, {}
    )
    second = await service.propose_pattern(
        {"hook": "BENEFICIARY_LAYER", "subject": "Equivalent wording", "evidence": [{"claim_id": "claim-B"}]},
        {},
    )
    review = {
        "proposal_id": second["prp_id"],
        "verdict": "Already represented",
        "reason": "Same observable mechanism",
        "evidence": [{"claim_id": "claim-B"}],
        "scores": {"overlap": 10, "observability": 8, "reusability": 4, "decision_value": 2},
        "recommendation": "REJECT",
        "duplicate_of": first["prp_id"],
    }
    result = await service.dispatch(
        "record_review", review, {"role": "scout", "proposal_id": second["prp_id"]}
    )
    assert result["adversarial_review"]["scores"] == review["scores"] and result["recommendation"] == "REJECT"
    assert result["adversarial_review"]["evidence"] == ["claim-B"]
    target = await service.repo.get("scout.expansion_proposals", first["prp_id"])
    assert target["occurrences"] == 2 and set(target["evidence"]) == {"claim-A", "claim-B"}


@pytest.mark.parametrize("lifecycle", ["CLOSED", "MERGED"])
async def test_terminal_event_cannot_publish_new_card(service, lifecycle) -> None:
    x = await seed(service)
    created = await service.dispatch("record_event", x, {})
    eid = created["event_id"]
    event = await service.repo.get("evt.events", eid)
    event.update(lifecycle=lifecycle, current_ems_id="terminal-ems")
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "terminal-ems",
        {
            "dims": unassessed_score_dimensions(),
            "ems_id": "terminal-ems",
            "event_id": eid,
            "class": "STRUCTURAL_SHOCK",
        },
    )
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "PARTIAL",
            "what": "Product announced",
            "verdict": "Product announced",
            "why": ["Product announced", "The publication does not quantify price or liquidity effects."],
        },
        {},
    )
    assert not result["published"] and result["reason"] == "TERMINAL_EVENT"


async def test_credential_evidence_rejected_before_network_or_raw_persistence(service) -> None:

    sentinel = "AUDIT_FAKE_QUERY_KEY"
    service.settings.etherscan_api_key = sentinel
    calls = []

    async def respond(request: httpx.Request):
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


async def test_snapshot_vote_end_creates_single_canonical_milestone_after_extraction(service) -> None:
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
        {"event_id": event["event_id"], "milestone": "Snapshot vote ends", "due_at": due, "source_id": "S24"},
        {},
    )
    assert len(await service.repo.list("ops.jobs", filters={"type": "MILESTONE_CHECK"})) == 1


def complete_event_fixture(event_id, **changes):
    now = clock_iso()
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
        "next_milestone": None,
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
        cutoff=clock_iso(),
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
    from tank.analysis.decision_codec import decision_inputs_from_records, encode_decision
    from tank.analysis.decision_contracts import (
        ActiveDecisionTracking,
        DecisionIdentity,
        DecisionMaterialization,
        DecisionMetadata,
    )
    from tank.analysis.decision_watches import (
        DecisionWatchCommand,
        DecisionWatchPolicy,
        reconcile_decision_watches,
    )
    from tank.analysis.decisions import DecisionEngine, finalize_decision, materialize_decision
    from tank.contracts.provenance import GenerationProvenance
    from tank.time_precision import collection_clock, propagate_timestamp
    from tank.util import parse_time

    event = complete_event_fixture(event_id, contradictions=[], next_milestone=None)
    assessment = complete_assessment_fixture("fixture-ems", event_id)
    truth = complete_truth_fixture(mta_id, event_id, token_id, cutoff)
    edge = {
        "event_id": event_id,
        "token_id": token_id,
        "role": "PRIMARY",
        "credible": False,
        "capture_class": "UNKNOWN",
        "exposure_score": None,
    }
    evaluated = DecisionEngine().evaluate(
        decision_inputs_from_records(event, assessment, edge, truth, open_call=None, conditions=())
    )
    versions = fixture_versions()
    generation = GenerationProvenance(
        config_version=versions.config_version,
        code_version=versions.code_version,
        session_config_version=None,
        session_code_version=None,
        model_id=None,
        model_id_basis=None,
        model_id_reason=None,
        prompt_version=None,
        prompt_version_basis=None,
        prompt_version_reason=None,
        prompt_label=None,
        prompt_binding=None,
        agent_version=None,
        agent_version_basis=None,
        agent_version_reason=None,
        session_id=None,
    )
    draft = materialize_decision(
        evaluated,
        DecisionMaterialization(
            metadata=DecisionMetadata(
                identity=DecisionIdentity(
                    decision_id=dec_id,
                    event_id=event_id,
                    token_id=token_id,
                    cutoff=propagate_timestamp(truth, "cutoff"),
                    thresholds_version=Settings().thresholds().version,
                    versions=versions,
                    generation=generation,
                    assessment_id=assessment["ems_id"],
                    market_truth_id=mta_id,
                ),
                calculated_at=collection_clock(cutoff),
                management="NORMAL",
                contradiction_ids=(),
                prices=None,
                tracking=ActiveDecisionTracking(kind="ACTIVE"),
            ),
            condition_ids=tuple(f"{dec_id}:watch:{index}" for index in range(len(evaluated.conditions))),
        ),
    )
    watches = reconcile_decision_watches(
        DecisionWatchCommand.from_draft(draft),
        [],
        [],
        now=parse_time(cutoff),
        policy=DecisionWatchPolicy(lifetime=timedelta(days=7), milestone_grace=timedelta(hours=1)),
        prepared_condition_ids=None,
    )
    return encode_decision(
        finalize_decision(
            draft, conditions=watches.active, conditions_shown=watches.shown, outlook_id=f"{dec_id}:outlook"
        )
    )


@pytest.mark.integration
@pytest.mark.postgres
async def test_real_event_tool_producers_satisfy_postgres_typed_contract(
    service, tmp_path: Path, postgres_template, repository_lifetime
) -> None:
    import os

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import compute_market_truth
    from tank.event_tools import LAYERS
    from tank.util import new_id, sha256

    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
    repo = await repository_lifetime.enter_async_context(postgres_template.repository(url))
    service = EventToolService(
        repo,
        service.settings,
        service.truth,
        DecisionEngine(),
        http=service.http,
        search=service.search_service,
        reference_clock=service.reference_clock,
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    item_id = new_id("item")
    stamp = clock_iso()
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
            token_ids=["world:WLD"],
            entities=[{"entity_id": "world", "role": "SUBJECT"}],
            public_at={
                "value": stamp,
                "precision": "EXACT",
                "basis": "Synthetic primary fixture publication",
                "quote": quote,
            },
        )
        x["claims"][0].update(quote=quote, state_claim="APPROVED")
        context = {
            "model_id": "fixture-model",
            "prompt_version": "fixture-prompt",
            "role": "system",
        }
        created = await service.dispatch("record_event", x, context)
        eid = created["event_id"]
        context["event_id"] = eid
        await service.dispatch(
            "set_assessment",
            {
                "event_id": eid,
                "assessment": unknown_assessment(claim=created["accepted_claims"][0]["claim_id"]),
            },
            context,
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
        truth = compute_market_truth({"event_id": eid, "token_id": "world:WLD", "cutoff": clock_iso()})
        truth["mta_id"] = new_id("mta")
        await repo.put("ana.market_truth", truth["mta_id"], truth)
        decision = await service.dispatch("get_decision", {"event_id": eid, "token_id": "world:WLD"}, context)
        assert decision["mta_id"] == truth["mta_id"] and decision["trade_reasons"]
        published = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "FULL",
                "what": quote,
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
async def test_search_and_fetched_evidence_keep_agent_certainty(service, media_type) -> None:

    from tank.search import SearchHit, SearchResponse
    from tank.util import canonical_json

    quote = "SEC approved the application."

    class Search:
        async def search(self, query, *, on_attempt):
            await on_attempt(1)
            return SearchResponse(
                results=[SearchHit(title="SEC release", href="https://www.sec.gov/news/release", body=quote)],
                attempts=1,
            )

    service.search_service = Search()
    found = await service.dispatch(
        "web_search", {"query": "SEC approval"}, {"session_id": "certainty-session"}
    )
    x = extraction()
    x.update(
        item_id=found["items"][0]["item_id"],
        statement=quote,
        entities=[{"entity_id": "sec", "role": "REGULATOR"}],
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

    async def page(request: httpx.Request):
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


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_truth_job_exact_spec_keys_priorities_and_cross_call_idempotency(
    service, backend, monkeypatch: pytest.MonkeyPatch, postgres_template, repository_lifetime
) -> None:
    import asyncio
    import os

    from tank.util import canonical_json, new_id, parse_time

    base = service.repo
    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        base = await repository_lifetime.enter_async_context(postgres_template.repository(url))
        service.repo = base
    eid = new_id("event")
    ems = eid + "-ems"
    tokens = ["world:WLD", "uniswap:UNI"]
    cutoff = "2026-10-16T12:00:00Z"
    tasks = []
    from contextlib import asynccontextmanager

    jobs_changed = asyncio.Event()
    enqueue = base.enqueue
    transaction = base.transaction
    transaction_depth = {}
    pending_notification = set()

    @asynccontextmanager
    async def observed_transaction():
        task = asyncio.current_task()
        transaction_depth[task] = transaction_depth.get(task, 0) + 1
        try:
            async with transaction() as repository:
                yield repository
        finally:
            transaction_depth[task] -= 1
            if transaction_depth[task] == 0:
                del transaction_depth[task]
                if task in pending_notification:
                    pending_notification.remove(task)

                    jobs_changed.set()

    async def observed_enqueue(kind, payload, *args, **kwargs):
        job = await enqueue(kind, payload, *args, **kwargs)
        if kind in {"ESCALATE_TOKENS", "RUN_MARKET_TRUTH"} and payload.get("event_id") == eid:
            task = asyncio.current_task()
            if transaction_depth.get(task, 0):
                pending_notification.add(task)
            else:
                jobs_changed.set()
        return job

    monkeypatch.setattr(base, "transaction", observed_transaction)
    monkeypatch.setattr(base, "enqueue", observed_enqueue)

    async def jobs(kind):
        return [
            row
            async for row in base.iter_all("ops.jobs", filters={"type": kind})
            if row["payload"].get("event_id") == eid
        ]

    async def wait_jobs(kind, count):
        async with asyncio.timeout(5):
            while True:
                jobs_changed.clear()
                found = await jobs(kind)
                if len(found) >= count:
                    return found
                await jobs_changed.wait()

    async def finish_truth(job):
        payload = job["payload"]

        truth = complete_truth_fixture(new_id("mta"), eid, payload["token_id"], payload["cutoff"])
        await base.put("ana.market_truth", truth["mta_id"], truth)
        current = await base.get("ops.jobs", job["job_id"])
        current.update(status="RUNNING", attempts=current["attempts"] + 1, result=truth)
        await base.put("ops.jobs", job["job_id"], current)
        await base.finish_job(job["job_id"])
        return truth

    async def finish_escalation(job) -> None:
        current = await base.get("ops.jobs", job["job_id"])
        current.update(status="RUNNING", attempts=current["attempts"] + 1)
        await base.put("ops.jobs", job["job_id"], current)
        await base.finish_job(job["job_id"])

    try:
        async with asyncio.timeout(5):
            await base.put("evt.events", eid, complete_event_fixture(eid, current_ems_id=ems))
            await base.put("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid))
            await base.put(
                "evt.beneficiary_maps",
                eid + ":1",
                {"event_id": eid, "map_version": 1, "analysis_set": tokens, "cutoff": clock_iso()},
            )
            started = utcnow()
            for call in ["model-call-one"]:
                tasks.append(
                    asyncio.create_task(
                        service.dispatch(
                            "get_market_truth",
                            {"event_id": eid, "token_ids": [tokens[0]], "cutoff": cutoff},
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
                    {"event_id": eid, "token_ids": [tokens[0]], "cutoff": cutoff},
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
                "mkt.hot_tokens",
                hot_key,
                {"token_id": tokens[0], "event_id": eid, "priority": 1, "since": clock_iso()},
            )
            await base.delete("mkt.hot_tokens", hot_key)
            reused = await service.dispatch(
                "get_market_truth",
                {"event_id": eid, "token_ids": [tokens[0]], "cutoff": cutoff},
                {"call_id": "third-call-after-demotion"},
            )
            assert reused["market_truth"][0] == actual
            assert await base.get("mkt.hot_tokens", hot_key) is None
            assert canonical_json(await base.get("ops.jobs", escalation["job_id"])) == frozen
            later = iso(parse_time(cutoff) + timedelta(minutes=1))
            later_task = asyncio.create_task(
                service.dispatch(
                    "get_market_truth",
                    {"event_id": eid, "token_ids": [tokens[0]], "cutoff": later},
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
                    {"event_id": eid, "token_ids": [tokens[1]], "cutoff": cutoff},
                    {"call_id": "other-token-call"},
                )
            )
            tasks.append(token_task)
            other_escalations = await wait_jobs("ESCALATE_TOKENS", 2)
            other_escalation = next(
                row for row in other_escalations if row["payload"]["token_id"] == tokens[1]
            )
            await finish_escalation(other_escalation)
            other_run = next(
                row
                for row in await wait_jobs("RUN_MARKET_TRUTH", 3)
                if row["payload"]["token_id"] == tokens[1]
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


@pytest.mark.integration
async def test_persistent_fatal_and_exit_publish_delivery_repeat_is_silent(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use real publication gate, frozen snapshots and delivery; isolate card layout."""

    import tank.analysis.cards as cards
    from tank.delivery import DeliveryService

    monkeypatch.setattr(
        cards,
        "compose_card",
        lambda *args, **kwargs: "Synthetic safety update" + "\n" + score_summary(args[0].assessment.scores),
    )
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
            "known_to_us_at": clock_iso(),
            "tokens_mentioned": ["T"],
        },
    )
    await service.repo.put(
        "evt.ems_assessments",
        "repeat-ems",
        {
            "dims": unassessed_score_dimensions(),
            "ems_id": "repeat-ems",
            "event_id": eid,
            "class": "MAJOR",
            "certainty": "HIGH",
        },
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
            "cutoff": clock_iso(),
            "data_confidence": {"class": "LOW"},
        },
    )
    await service.repo.put(
        "ana.decisions",
        "repeat-wait",
        {"dec_id": "repeat-wait", "event_id": eid, "token_id": "T", "action": "WAIT", "cutoff": clock_iso()},
    )
    args = {
        "event_id": eid,
        "kind": "FULL",
        "what": "Synthetic fact",
        "verdict": "Synthetic fact",
        "why": ["Primary fixture", "Safety fixture"],
    }
    first = await service.dispatch("publish_card", args, {})
    assert first["published"], first
    service.settings.telegram_token = "fixture"
    service.settings.telegram_alerts_chat = "1"
    service.settings.telegram_ops_chat = "2"
    sent = []

    def telegram(request: httpx.Request):
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        delivery = DeliveryService(service.repo, service.settings, http=http)
        await delivery.deliver({"channel": "telegram:alerts", "alert_id": first["alert_id"], "version": 1})
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
            {
                "dec_id": "repeat-exit",
                "event_id": eid,
                "token_id": "T",
                "action": "EXIT",
                "cutoff": clock_iso(),
            },
        )
        args["kind"] = "UPDATE"
        correction = await service.dispatch("publish_card", args, {})
        assert correction["published"], correction
        await delivery.deliver(
            {"channel": "telegram:alerts", "alert_id": correction["alert_id"], "version": 2}
        )
        for _ in range(2):
            repeat = await service.dispatch("publish_card", args, {})
            assert not repeat["published"], repeat
        assert len(sent) == 2
        assert len(await service.repo.list("out.alerts", filters={"event_id": eid})) == 2

        event = await service.repo.get("evt.events", eid)
        await service.repo.put("evt.events", eid, {**event, "freshness": "FRESH", "lifecycle": "MONITORING"})
        await service.repo.put(
            "evt.ems_assessments",
            "repeat-ems",
            {
                "dims": unassessed_score_dimensions(),
                "ems_id": "repeat-ems",
                "event_id": eid,
                "class": "STRUCTURAL_SHOCK",
                "criterion": "G5",
                "certainty": "HIGH",
            },
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
                "cutoff": clock_iso(),
                "entry_ref_price": "10",
                "invalidation_price": "9",
            },
        )
        started = await service.dispatch("publish_card", args, {})
        assert started["published"], started
        assert started["version"] == 4
        frozen = await service.repo.get("out.alerts", started["alert_id"] + ":4")
        assert frozen["delivery_immediate"]
        await delivery.deliver({"channel": "telegram:alerts", "alert_id": started["alert_id"], "version": 4})
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
    stamp = clock_iso()
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
        token_ids=["world:WLD"],
        entities=[{"entity_id": "world", "role": "ACTOR"}],
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
    assessment = unknown_assessment(claim=created["accepted_claims"][0]["claim_id"])
    assessment["capture_chains"] = [chain]
    await service.dispatch("set_assessment", {"event_id": created["event_id"], "assessment": assessment}, {})
    return created, relation_quote, {layer: "No further adopted link is established" for layer in LAYERS}


@pytest.mark.parametrize("capture_score,expected_grade", [(8, "B"), (4, "C")])
async def test_produced_loser_has_supported_negative_grade_without_long_action(
    service, capture_score, expected_grade
) -> None:
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
    truth = complete_truth_fixture("loser-" + eid, eid, "world:WLD", clock_iso())
    await service.repo.put("ana.market_truth", truth["mta_id"], truth)
    decision = await service.dispatch("get_decision", {"event_id": eid, "token_id": "world:WLD"}, {})
    assert decision["thesis_grade"] == expected_grade

    assert decision["outlook"]["direction"] == "UNKNOWN"
    assert decision["action"] not in {"START", "ADD"}


async def test_loser_cannot_reuse_only_positive_capture_proof(service) -> None:
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
    truth = complete_truth_fixture("unsupported-loser-" + eid, eid, "world:WLD", clock_iso())
    await service.repo.put("ana.market_truth", truth["mta_id"], truth)
    assert (await service.dispatch("get_decision", {"event_id": eid, "token_id": "world:WLD"}, {}))[
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


async def test_rejected_approval_only_exits_call_with_explicit_evidenced_dependency(service) -> None:
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
    truth = complete_truth_fixture("approval-" + eid, eid, "world:WLD", clock_iso())
    await service.repo.put("ana.market_truth", truth["mta_id"], truth)
    await service.repo.put(
        "ana.calls",
        "approval-call-" + eid,
        {
            "call_id": "approval-call-" + eid,
            "event_id": eid,
            "token_id": "world:WLD",
            "status": "OPEN",
            "opened_at": clock_iso(),
            "entry_ref_price": "10",
            "invalidation_price": "9",
        },
    )
    before = await service.dispatch("get_decision", {"event_id": eid, "token_id": "world:WLD"}, {})

    async def rejected(object_text, dependency=None):
        stamp = clock_iso()
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
    unrelated = await service.dispatch("get_decision", {"event_id": eid, "token_id": "world:WLD"}, {})
    assert unrelated["action"] == before["action"] and unrelated["action"] != "EXIT"
    dependency_id = claims[2]["claim_id"]
    await rejected(
        "SEC application approval",
        {"scope": "TOKEN", "token_ids": ["world:WLD"], "claim_ids": [dependency_id]},
    )
    contradictions = await service.repo.list("ana.contradictions", filters={"event_id": eid})
    assert len(contradictions) == 1 and contradictions[0]["type"] == "FATAL"
    assert contradictions[0]["token_id"] == "world:WLD" and dependency_id in contradictions[0]["evidence"]
    dependent = await service.dispatch("get_decision", {"event_id": eid, "token_id": "world:WLD"}, {})
    assert dependent["action"] == "EXIT"


async def test_integration_cap_uses_actual_stage_claim_not_unrelated_counterparty_fact(service) -> None:
    from tank.event_tools import LAYERS

    x = await seed(service)
    x["token_ids"] = ["world:WLD"]
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


async def test_magnitude_agent_judgment_is_preserved_with_or_without_ratio_metadata(service) -> None:
    created, _, _ = await seed_economic_event(service, negative_relation=False)
    eid = created["event_id"]
    proof = created["accepted_claims"][0]["claim_id"]
    assessment = unknown_assessment(claim=created["accepted_claims"][0]["claim_id"])
    assessment["dims"]["magnitude"].update(value=10, rationale="V4 is significant", evidence=[proof])
    out = await service.dispatch("set_assessment", {"event_id": eid, "assessment": assessment}, {})
    stored = await service.repo.get("evt.ems_assessments", out["ems_id"])
    assert stored["dims"]["magnitude"]["value"] == 10

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
    assert stored["dims"]["magnitude"]["value"] == 10
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


async def test_proposed_timing_requires_primary_dated_decision(service) -> None:
    from tank.util import parse_time

    created, _, _ = await seed_economic_event(service, negative_relation=False)
    eid = created["event_id"]
    event = await service.repo.get("evt.events", eid)
    await service.repo.put("evt.events", eid, {**event, "event_state": "PROPOSED"})
    assessment = unknown_assessment(claim=created["accepted_claims"][0]["claim_id"])
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


async def test_immediate_market_access_allows_effective_primary_change_without_prior_trade(service) -> None:
    from tank.util import new_id

    stamp = clock_iso()
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
        token_ids=["world:WLD"],
        entities=[{"entity_id": "world", "role": "ACTOR"}],
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
    assessment = unknown_assessment(claim=proof)
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
        assert collection != "mkt.trades", "Prints never feed a typed latest read"
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


async def test_primary_natural_calendar_and_usd_ratio_dispatch(service) -> None:
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
    assessment = unknown_assessment(claim=created["accepted_claims"][0]["claim_id"])
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


async def test_event_source_time_provenance_and_milestone_offsets(service) -> None:
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
            "source_id": "S01",
        },
        {},
    )
    event = await service.repo.get("evt.events", eid)
    assert event["next_milestone"]["due_at_prec"] == "BUCKET(1d)"
    escalation = (await service.repo.list("ops.source_escalations", filters={"event_id": eid}))[0]
    assert escalation["starts_at_prec"] == escalation["expires_at_prec"] == "BUCKET(1d)"


async def test_event_merge_copies_selected_earlier_time_precision(service) -> None:
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


async def test_market_analysis_boundary_rejects_collection_clock_precision(service) -> None:
    made, _, _ = await seed_economic_event(service, negative_relation=False)
    with pytest.raises(ValueError, match="Chosen boundaries"):
        await service.dispatch(
            "get_market_truth",
            {
                "event_id": made["event_id"],
                "token_ids": ["world:WLD"],
                "cutoff": clock_iso(),
                "cutoff_prec": "COLLECTION_ONLY",
            },
            {},
        )
    assert not await service.repo.list("ops.jobs", filters={"type": "RUN_MARKET_TRUTH"})


async def test_assessment_defers_reaction_ownership_to_scheduler_after_commit(service) -> None:
    import asyncio

    class SchedulerObserver:
        def __init__(self) -> None:
            self.events: list[str] = []

        async def ensure_event_reactions(self, event_id: str) -> None:
            async def independent_read():
                async with service.repo.transaction(), service.repo.lock("event:" + event_id):
                    stored = await service.repo.get("evt.events", event_id)
                    return await service.repo.get("evt.ems_assessments", stored["current_ems_id"])

            assessment = await asyncio.wait_for(asyncio.create_task(independent_read()), 1)
            assert assessment["class"] == "MAJOR"
            self.events.append(event_id)

    service.scheduler = SchedulerObserver()
    created, _, _ = await seed_economic_event(service, negative_relation=False)
    assert service.scheduler.events == [created["event_id"]]
    assert not await service.repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})


async def test_assessment_dispatch_creates_real_reaction_jobs(tmp_path: Path) -> None:
    from contextlib import AsyncExitStack

    from scheduler_support import operational_reaction_services
    from source_support import fixture_request_budgets

    from tank.analysis.decisions import DecisionEngine

    repo = MemoryRepository()
    await repo.open()
    settings = Settings(data_dir=tmp_path, _env_file=None)
    async with AsyncExitStack() as resources:
        budgets = fixture_request_budgets()
        resources.push_async_callback(budgets.shutdown)
        services = await resources.enter_async_context(
            operational_reaction_services(repo, settings, versions=fixture_versions(), budgets=budgets)
        )
        tools = EventToolService(
            repo,
            settings,
            services.truth,
            DecisionEngine(),
            versions=fixture_versions(),
            scheduler=services.scheduler,
            pages=LivePageAccess(services.admission),
        )
        created, _, _ = await seed_economic_event(tools, negative_relation=False)
        jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        assert jobs
        assert {job["payload"]["event_id"] for job in jobs} == {created["event_id"]}
        assert {job["payload"]["token_id"] for job in jobs} == {"bitcoin:BTC", "ethereum:ETH"}
        assert {job["payload"]["checkpoint"] for job in jobs} == {
            "POST_15M",
            "POST_1H",
            "POST_4H",
            "POST_24H",
            "POST_72H",
        }
        assert len(jobs) == 10
        assert all(job["payload"]["retro"] is False for job in jobs)
    assert services.scheduler.closing and not services.scheduler.background
    assert services.market.http.is_closed
    assert budgets.backend.closed
    await repo.close()


async def test_web_raw_canonical_file_exists_even_when_parser_fails(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pathlib import Path

    import selectolax.parser
    import zstandard

    original = b"<html><body>Saved before parser</body></html>"

    def fail_parser(body) -> None:
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


async def test_bundle_directory_sync_failure_never_creates_database_reference(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os
    import stat

    from tank.analysis import cards

    monkeypatch.setattr(
        cards,
        "compose_card",
        lambda *args, **kwargs: "Durability fixture" + "\n" + score_summary(args[0].assessment.scores),
    )
    extraction_args = await seed(service)
    made = await service.dispatch("record_event", extraction_args, {})
    eid = made["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "durability-ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "durability-ems",
        {
            "dims": unassessed_score_dimensions(),
            "ems_id": "durability-ems",
            "event_id": eid,
            "class": "STRUCTURAL_SHOCK",
            "criterion": "G5",
        },
    )
    original_sync = os.fsync
    seen = []

    def fail_directory_sync(descriptor) -> None:
        kind = "directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file"
        seen.append(kind)
        if kind == "directory":
            raise OSError("Synthetic bundle directory fsync failure")
        original_sync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_directory_sync)
    with pytest.raises(OSError, match="Synthetic bundle directory fsync failure"):
        await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "PARTIAL",
                "what": "Product announced",
                "verdict": "Product announced",
                "why": ["Product announced", "The publication does not quantify price or liquidity effects."],
            },
            {},
        )
    assert seen[-1] == "directory" and "file" in seen[:-1]
    assert not await service.repo.list("out.evidence_bundles")
    assert not await service.repo.list("out.alerts")
    assert not await service.repo.list("out.deliveries")
    assert all(job["payload"].get("notice_id") for job in await service.repo.list("ops.jobs"))


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.parametrize("resolved_snapshot", [True, False])
async def test_native_prompt_snapshot_producer_reaches_versioned_event_tools(
    service, backend, resolved_snapshot, postgres_template, repository_lifetime
) -> None:
    import os

    from tank.agents import SessionService
    from tank.util import new_id

    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        repo = await repository_lifetime.enter_async_context(postgres_template.repository(url))
        service = EventToolService(
            repo,
            service.settings,
            service.truth,
            service.engine,
            http=service.http,
            search=service.search_service,
            reference_clock=service.reference_clock,
            versions=fixture_versions(),
            scheduler=RecordingReactionScheduling(),
            pages=LivePageAccess(fixture_http_admission()),
        )
    repo = service.repo
    sid = new_id("native-session")
    stamp = clock_iso()
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

    def control(request: httpx.Request):
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
            native = SessionService(repo, service.settings, dispatch, http=http, versions=fixture_versions())
            session = await native.resolve({"kind": "NEW_ITEM", "item_id": item["item_id"]})

            assert session["prompt_version"] is None and session["prompt_label"] == config["prompt_version"]
            args = extraction()
            args.update(
                item_id=item["item_id"],
                statement=quote,
                token_ids=["world:WLD"],
                entities=[{"entity_id": "sec", "role": "REGULATOR"}],
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
            args.pop("item_id")
            await native.tool(session, {"id": "native-record", "name": "record_event", "input": args})
            ledger = await repo.get("ops.tool_calls", sid + ":native-record")
            if not resolved_snapshot:
                assert ledger["status"] == "REFUSED" and ledger["result"]["error"]["category"] == "PROVENANCE"
                assert not dispatched
                assert not await repo.list("evt.events", filters={"session_id": sid})
                assert not await repo.list("evt.ems_assessments", filters={"session_id": sid})
            else:
                assert ledger["status"] == "DONE", ledger
                session = await repo.get("ops.agent_sessions", sid)
                await native.tool(
                    session,
                    {
                        "id": "native-assessment",
                        "name": "set_assessment",
                        "input": {
                            "assessment": unknown_assessment(
                                claim=ledger["result"]["accepted_claims"][0]["claim_id"]
                            )
                        },
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


@pytest.mark.integration
@pytest.mark.parametrize("http_status", [404, 503])
@pytest.mark.parametrize("outcome", ["continue_to_card", "native_error_failed"])
async def test_actual_fetch_http_error_returns_durably_then_card_or_native_failed(
    service, http_status, outcome, truth_job_enqueued
) -> None:
    import asyncio
    import json

    from tank.agents import SessionService
    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import compute_market_truth
    from tank.event_tools import LAYERS
    from tank.util import new_id

    sid = new_id("http-session")
    replay_sid = new_id("http-replay-session")
    iid = new_id("item")
    stamp = clock_iso()
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

    def control(request: httpx.Request):
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
        first = SessionService(
            service.repo, service.settings, service.dispatch, http=http, versions=fixture_versions()
        )
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
        resumed = SessionService(
            service.repo, service.settings, service.dispatch, http=http, versions=fixture_versions()
        )
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
        replay = SessionService(
            service.repo, replay_options, service.dispatch, http=http, versions=fixture_versions()
        )
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
            from model_fixture import model_arguments

            args = model_arguments(name, args)
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
            token_ids=["world:WLD"],
            entities=[{"entity_id": "world", "role": "ACTOR"}],
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
            assert (
                session["status"] == "FAILED" and session["terminal_failure"]["event_type"] == "session.error"
            )
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
            assessment = unknown_assessment(claim=created["accepted_claims"][0]["claim_id"])
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

            async def settle_owned_missing_input_jobs() -> None:

                await truth_job_enqueued.wait()
                for kind in ("ESCALATE_TOKENS", "RUN_MARKET_TRUTH"):
                    jobs = [
                        job
                        for job in await service.repo.list("ops.jobs")
                        if job["type"] == kind
                        and job["payload"].get("event_id") == eid
                        and job["payload"].get("token_id") == "world:WLD"
                    ]
                    assert len(jobs) == 1 and jobs[0]["status"] == "QUEUED"
                    job = jobs[0]
                    dependencies = [
                        await service.repo.get("ops.jobs", ref)
                        for ref in job["payload"].get("_depends_on", [])
                    ]
                    assert all(row and row["status"] == "DONE" for row in dependencies)
                    job.update(status="RUNNING", attempts=job["attempts"] + 1)
                    if kind == "RUN_MARKET_TRUTH":
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

            market_call = asyncio.create_task(
                tool(
                    "market-truth",
                    "get_market_truth",
                    {
                        "event_id": eid,
                        "token_ids": ["world:WLD"],
                        "cutoff": clock_iso(),
                        "cutoff_prec": "EXACT",
                    },
                )
            )
            market_worker = asyncio.create_task(settle_owned_missing_input_jobs())
            try:
                async with asyncio.timeout(5):
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
            decision = await tool("decision", "get_decision", {"event_id": eid, "token_id": "world:WLD"})
            assert decision["action"] == "WAIT" and decision["outlook"]["direction"] == "UNKNOWN"
            card = await tool(
                "card",
                "publish_card",
                {
                    "event_id": eid,
                    "kind": "FULL",
                    "what": approved,
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


async def test_trusted_reference_clock_changes_logical_age_only(
    service, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
        EventToolService(
            service.repo,
            service.settings,
            None,
            None,
            reference_clock=clock,
            versions=fixture_versions(),
            scheduler=RecordingReactionScheduling(),
            pages=LivePageAccess(fixture_http_admission()),
        )
    with pytest.raises(ValueError, match="isolated fixture"):
        EventToolService(
            service.repo,
            options,
            None,
            None,
            reference_clock=object(),
            versions=fixture_versions(),
            scheduler=RecordingReactionScheduling(),
            pages=LivePageAccess(fixture_http_admission()),
        )
    reference = EventToolService(
        service.repo,
        options,
        None,
        None,
        reference_clock=clock,
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
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
    assert not (
        await reference.dispatch("search_tank_items", {"query": "Product", "source_ids": ["S99"]}, {})
    )["items"]
    assert await service.repo.get("ingest.source_items", "i") == stored_before
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: parse_time(logical) - timedelta(seconds=1))
    assert not (await reference.dispatch("search_tank_items", {"query": "Product"}, {}))["items"]
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: parse_time(logical))
    assert (await reference.dispatch("search_tank_items", {"query": "Product"}, {}))["items"][0][
        "item_id"
    ] == "i"

    unimported = EventToolService(
        MemoryRepository(),
        options,
        None,
        None,
        reference_clock=clock,
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
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
async def test_quoted_proposal_requires_explicit_stage_claim_not_fact_keyword(
    service, explicit_stage
) -> None:
    quote = "The Securities and Exchange Commission is proposing new custody rules."
    x = extraction()
    x.update(statement="SEC proposes new custody rules", token_ids=[], entities=[])
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
            observed_at=clock_iso(),
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
async def test_primary_event_fact_certainty_allows_neutral_paraphrase(service, paraphrase) -> None:
    quote = "The SEC announced approval of the application."
    x = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = quote
    await service.repo.put("ingest.source_items", "i", item)
    x.update(
        statement="SEC approved the application." if paraphrase else quote,
        entities=[{"entity_id": "sec", "role": "REGULATOR"}],
        token_ids=[],
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


async def test_primary_selection_paraphrase_preserves_fact_certainty(service) -> None:

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
            {"entity_id": "the-clearing-house", "role": "ACTOR"},
            {"entity_id": "quant", "role": "COUNTERPARTY"},
        ],
        token_ids=["quant:QNT"],
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
async def test_primary_fact_scope_uses_declared_identity_not_word_overlap(service, scope) -> None:
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
    x.update(statement=name + " plans to launch a product.", entities=entities, token_ids=tokens)
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
async def test_event_certainty_is_not_host_semantic_scope_judgment(service, failure) -> None:
    x = await seed(service)
    quote = "SEC approved the application."
    item = await service.repo.get("ingest.source_items", "i")
    item["text"] = quote
    if failure == "secondary_source":
        item.update(source_id="S33", source_class="SECONDARY", author="CoinDesk")
    await service.repo.put("ingest.source_items", "i", item)
    x.update(statement=quote, entities=[{"entity_id": "sec", "role": "REGULATOR"}], token_ids=[])
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


async def test_explicit_agent_corroboration_updates_certainty(service) -> None:
    x = await seed(service)
    quote = "World announced approval of a new product."
    item = await service.repo.get("ingest.source_items", "i")
    item.update(text=quote, source_id="S33", source_class="SECONDARY", author="CoinDesk")
    await service.repo.put("ingest.source_items", "i", item)
    x.update(
        statement="World approved a new product.",
        entities=[{"entity_id": "world", "role": "ACTOR"}],
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
async def test_immediate_access_uses_agent_structured_fact_and_effective_time(service, case) -> None:
    from tank.util import new_id

    stamp = clock_iso()
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
        token_ids=["world:WLD"],
        entities=[{"entity_id": "world", "role": "ACTOR"}],
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
    assessment = unknown_assessment(claim=proof)
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
async def test_observation_explicit_claim_replacement_preserves_history(service, case) -> None:
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
            "observed_at": clock_iso(),
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


async def test_secondary_default_summary_does_not_duplicate_layer_notes(service) -> None:
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
async def live_update_database(service, backend, postgres_template):
    """Keep the reconnect test on one child database owned by the lazy template."""
    if backend == "memory":
        yield None
        return
    import os

    source = os.getenv("TANK_TEST_DATABASE_URL")
    if not source:
        pytest.skip("Requires isolated PostgreSQL with CREATE DATABASE permission")
    async with postgres_template.repository(source) as repository:
        yield repository


@pytest.mark.integration
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_live_update_survives_restart_and_delivers_at_boundary_with_retry(
    service, monkeypatch: pytest.MonkeyPatch, backend, live_update_database
) -> None:
    """Exercise real publication, immutable storage, durable queue and transport ledger."""
    import copy
    import json

    from tank.delivery import DeliveryError, DeliveryService
    from tank.storage import PostgresRepository
    from tank.util import new_id, parse_time

    database_url = None
    if backend == "postgres":
        service.repo = live_update_database
        database_url = service.repo.database_url
    repo = service.repo

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
        "what": "Access approved",
        "verdict": "Access approved",
        "why": ["Official approval", "Defined access"],
    }
    requests = []
    reject_next = [False]

    def telegram(request: httpx.Request):
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


async def test_old_trigger_updates_stage_but_does_not_publish_or_resurface_on_timer(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tank.analysis.cards as cards

    monkeypatch.setattr(
        cards,
        "compose_card",
        lambda *args, **kwargs: "Stored stage update" + "\n" + score_summary(args[0].assessment.scores),
    )
    extracted = await seed(service)
    extracted["claims"][0]["state_claim"] = "APPROVED"
    created = await service.dispatch("record_event", extracted, {})
    eid = created["event_id"]
    ems = eid + "-ems"
    event = await service.repo.get("evt.events", eid)
    await service.repo.put("evt.events", eid, {**event, "current_ems_id": ems})
    await service.repo.put("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid))
    from tank.event_tools import LAYERS

    await service.dispatch(
        "set_beneficiaries",
        {
            "event_id": eid,
            "edges": [],
            "layer_reasons": {
                layer: "No supported token link is asserted by this fixture." for layer in LAYERS
            },
        },
        {},
    )
    args = {
        "event_id": eid,
        "kind": "FULL",
        "what": "Approved",
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
        {"item_id": "old-reprint", "source_id": "S01", "text": "Now live", "observed_at": clock_iso()},
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
    assert (
        len(
            [
                job
                for job in await service.repo.list("ops.jobs", filters={"type": "DELIVER"})
                if job["payload"].get("alert_id")
            ]
        )
        == 1
    )
    stored = await service.repo.get("evt.events", eid)
    assert "publication_trigger_freshness" not in stored
    assert "publication_stage_freshness" not in stored


@pytest.mark.parametrize("role", ["PRIMARY", "SECONDARY", "NONE"])
async def test_each_layer_needs_agent_reason_when_primary_does_not_supply_a_secondary(service, role) -> None:
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


async def test_registry_ids_pass_and_only_names_outside_the_registry_stay_unresolved(service) -> None:
    """W5 (03 §2.11, V3): entities and tokens are named by registry ID; a name is kept,
    UNRESOLVED, only when the registry does not resolve it, and a registered name is refused."""
    from tank.event_tools import resolve_entity, resolve_token

    registry = service.settings.load_config("registry")
    assert resolve_token(registry, "UNI") == "uniswap:UNI"
    assert resolve_token(registry, "$uni") == "uniswap:UNI"
    assert resolve_token(registry, "Uniswap Labs") == "uniswap:UNI"
    assert resolve_token(registry, "Lighter") == "UNRESOLVED:Lighter"
    assert resolve_token(registry, "Lighter", context="Lighter (LIT) lists") == "lighter:LIT"
    assert resolve_token(registry, "Foo") == "UNRESOLVED:Foo"
    assert resolve_entity(registry, "Plasma") == "UNRESOLVED:Plasma"
    assert resolve_entity(registry, "Plasma", context="XPL") == "plasma"
    assert resolve_entity(registry, "Ava Labs") == "avalanche"
    x = await seed(service)
    x.update(
        token_ids=["uniswap:UNI"],
        token_names=["Foo"],
        entities=[
            {"entity_id": "uniswap", "role": "ACTOR"},
            {"name": "Northstar Lab", "role": "COUNTERPARTY"},
        ],
    )
    eid = (await service.dispatch("record_event", x, {}))["event_id"]
    event = await service.repo.get("evt.events", eid)
    assert event["tokens_mentioned"] == ["uniswap:UNI", "UNRESOLVED:Foo"]
    assert event["entities"] == [
        {"entity_id": "uniswap", "role": "ACTOR"},
        {"entity_id": "UNRESOLVED:Northstar Lab", "role": "COUNTERPARTY"},
    ]
    for names in ({"token_names": ["$UNI"]}, {"entities": [{"name": "Uniswap Labs", "role": "ACTOR"}]}):
        with pytest.raises(ValueError, match="is the registry (token|entity)"):
            await service.dispatch("record_event", {**x, **names}, {})


async def test_least_repriced_ranks_all_credible_tokens_on_partial_refresh(
    service, truth_job_enqueued
) -> None:
    """W6 (03 §4.7 step 9): refreshing one token's truth keeps ranking every
    credible beneficiary from its latest stored truth."""
    import asyncio

    eid = "least-event"
    await service.repo.put("evt.events", eid, {"event_id": eid, "current_ems_id": "least-ems"})
    await service.repo.put(
        "evt.ems_assessments",
        "least-ems",
        {"dims": unassessed_score_dimensions(), "ems_id": "least-ems", "event_id": eid, "class": "MAJOR"},
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
            "cutoff_prec": "EXACT",
            "cutoff_prec_reason": None,
            "liquidity": {"liquidity_state": "NORMAL"},
            "relative": {"repricing_band": band, "windows": {"EVENT": {"pctile": pctile}}},
        }

    earlier = iso(utcnow() - timedelta(minutes=5))
    await service.repo.put("ana.market_truth", "mta-B", truth("B", "UNPRICED", 0.2, earlier))
    fresh = truth("A", "HEAVY", 0.9, clock_iso())
    task = asyncio.create_task(
        service.dispatch("get_market_truth", {"event_id": eid, "token_ids": ["A"]}, {})
    )
    try:
        async with asyncio.timeout(2):
            await truth_job_enqueued.wait()
            for kind in ("ESCALATE_TOKENS", "RUN_MARKET_TRUTH"):
                job = await service.repo.claim_job()
                assert job is not None and job["type"] == kind
                if kind == "RUN_MARKET_TRUTH":
                    stored = await service.repo.get("ops.jobs", job["job_id"])
                    await service.repo.put("ana.market_truth", fresh["mta_id"], fresh)
                    stored["result"] = fresh
                    await service.repo.put("ops.jobs", job["job_id"], stored)
                await service.repo.finish_job(job["job_id"])
            await task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    mapping = await service.repo.get("evt.beneficiary_maps", f"{eid}:1")
    assert mapping["least_repriced_edge"] == "edge-B"


async def test_list_open_events_skips_events_whose_session_failed(service) -> None:
    """03 §3.2 Failures: a failed session ends the event's analysis, so the agent
    never sees it as open and never merges later news into it."""
    for eid, status in [("live-event", "OPEN"), ("failed-event", "FAILED")]:
        await service.repo.put(
            "evt.events",
            eid,
            {
                "event_id": eid,
                "lifecycle": "ANALYZING",
                "statement": "World news",
                "entities": [{"entity_id": "world", "role": "SUBJECT"}],
            },
        )
        await service.repo.put(
            "ops.agent_sessions",
            "session-" + eid,
            {"session_id": "session-" + eid, "event_id": eid, "status": status},
        )
    listed = await service.dispatch("list_open_events", {"entity_id": "world"}, {})
    assert [row["event_id"] for row in listed["events"]] == ["live-event"]


async def test_token_facts_show_unlocks_before_the_cutoff_and_nothing_after(service) -> None:
    """F-02 / 03 §3.2: get_token_facts returns supply and unlocks known at the
    cutoff, including a recent past unlock; rows observed later stay hidden."""
    now = utcnow()
    rows = {
        "past": {"date": iso(now - timedelta(days=3)), "observed_at": iso(now - timedelta(days=10))},
        "next": {"date": iso(now + timedelta(days=5)), "observed_at": iso(now - timedelta(days=1))},
        "future-capture": {
            "date": iso(now + timedelta(days=6)),
            "observed_at": iso(now + timedelta(hours=1)),
        },
    }
    for label, row in rows.items():
        await service.repo.put(
            "mkt.unlocks", "layerzero:ZRO:" + label, {"token_id": "layerzero:ZRO", "label": label, **row}
        )
    facts = await service.dispatch("get_token_facts", {"token_id": "layerzero:ZRO"}, {})
    assert [(u["label"], u["relative_to_cutoff"]) for u in facts["unlocks"]] == [
        ("past", "PAST"),
        ("next", "UPCOMING"),
    ]

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


def test_schema_refusal_names_allowed_cutoff_precisions() -> None:
    """A precision outside the declared schema is refused at the schema with the
    allowed forms, not deep in the tool (an undeclared value such as "FORTNIGHT")."""
    import json
    from pathlib import Path

    schema = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "config/skills/tank-event-analyst/get_market_truth.schema.json"
        ).read_text()
    )
    args = {"token_ids": ["T"], "cutoff": "2026-10-20T19:07:00Z"}
    validate_schema(dict(args, cutoff_prec="BUCKET(1m)"), schema)
    validate_schema(dict(args, cutoff_prec="EXACT"), schema)
    with pytest.raises(ValueError) as refused:
        validate_schema(dict(args, cutoff_prec="FORTNIGHT"), schema)
    assert (
        "$.cutoff_prec" in str(refused.value)
        and "EXACT" in str(refused.value)
        and "BUCKET" in str(refused.value)
    )


async def test_decision_for_a_token_outside_the_analysis_set_names_the_set(service) -> None:
    """E2E msg 15/16: a token_id outside the analysis set gets an error that names the set."""
    await service.repo.put("evt.events", "e", {"event_id": "e", "current_ems_id": "ems"})
    await service.repo.put(
        "evt.ems_assessments",
        "ems",
        {"dims": unassessed_score_dimensions(), "ems_id": "ems", "event_id": "e", "class": "MAJOR"},
    )
    await service.repo.put(
        "evt.beneficiary_maps", "e:1", {"event_id": "e", "map_version": 1, "analysis_set": ["bitcoin:BTC"]}
    )
    with pytest.raises(ValueError, match=r"'Bitcoinz'.*analysis set: \['bitcoin:BTC'\]"):
        await service.dispatch("get_decision", {"event_id": "e", "token_id": "Bitcoinz"}, {})


def test_publish_card_schema_bounds_agent_text() -> None:
    """E2E msg 16: agent text is bounded at the schema, before the card is built."""
    import json
    from pathlib import Path

    schema = json.loads(
        (
            Path(__file__).resolve().parents[1] / "config/skills/tank-event-analyst/publish_card.schema.json"
        ).read_text()
    )
    ok = {"kind": "FULL", "what": "я" * 500, "verdict": "v" * 160, "why": ["w" * 220, "x"]}
    validate_schema(ok, schema)
    with pytest.raises(ValueError, match=r"\$\.verdict: 161 characters, at most 160"):
        validate_schema(dict(ok, verdict="v" * 161), schema)
    with pytest.raises(ValueError, match=r"\$\.why\[0\]: 221 characters"):
        validate_schema(dict(ok, why=["w" * 221, "Second authored fact"]), schema)

    manifest = json.loads(
        (Path(__file__).resolve().parents[1] / "config/skills/tank-event-analyst/tools.json").read_text()
    )
    tool = next(tool for tool in manifest if tool["name"] == "publish_card")
    assert tool["description"] == schema["description"]
    assert tool["input_schema"] == {
        key: value for key, value in schema.items() if key not in {"$schema", "title", "description"}
    }
    for kind in ("PARTIAL", "FULL", "UPDATE"):
        payload = dict(ok, kind=kind)
        validate_schema(payload, schema)
        with pytest.raises(ValueError, match="what"):
            validate_schema({key: value for key, value in payload.items() if key != "what"}, schema)
        for invalid in ("", " \n\t", None, 42, "я" * 501):
            with pytest.raises(ValueError, match=r"\$\.what"):
                validate_schema(dict(payload, what=invalid), schema)


async def test_token_facts_for_btc_fit_the_context(service) -> None:
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
            daily = {
                "complete": True,
                "total24h": str(200000 + n),
                "total30d": "7277898",
                "available": True,
                "totalDataChart": chart if n == 140 else [[1296432000, str(n)]],
            }
            row.update(
                metrics={"dailyFees": daily, "dailyRevenue": daily},
                protocols={"bitcoin": {"dailyFees": dict(daily, source_url="https://api.llama.fi/x")}},
            )
        await service.repo.put("mkt.supply_obs", f"btc:{n}", row)
    facts = await service.dispatch("get_token_facts", {"token_id": "bitcoin:BTC"}, {})
    size = len(canonical_json(facts).encode())
    assert size < TOOL_RESULT_MAX_BYTES, size
    kinds = [row["kind"] for row in facts["supply_obs"]]
    assert sorted(kinds) == ["CHAIN_LIQUIDITY", "EXCHANGE_INVENTORY", "FUNDAMENTALS", "LENDING", "UNLOCKS"]
    fundamentals = next(row for row in facts["supply_obs"] if row["kind"] == "FUNDAMENTALS")
    fees = fundamentals["metrics"]["dailyFees"]
    assert fees["total24h"] == "200140" and fees["totalDataChart"]["count"] == 5500
    assert len(fees["totalDataChart"]["last"]) == 7 and "_truncated" not in facts


async def test_read_tool_results_are_bounded_and_say_so(service) -> None:
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
        {
            "event_id": "open",
            "lifecycle": "ANALYZING",
            "statement": "World news",
            "tokens_mentioned": ["world:WLD"],
            "claims_blob": "z" * 50_000,
        },
    )
    await service.repo.put(
        "ops.agent_sessions", "s1", {"session_id": "s1", "event_id": "open", "status": "OPEN"}
    )
    listed = await service.dispatch("list_open_events", {"entity_id": "world"}, {})
    event = listed["events"][0]
    assert "claims_blob" not in event and event["sessions"] == [{"session_id": "s1", "status": "OPEN"}]


async def test_daily_vesting_unlocks_stay_visible_around_the_cutoff(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F-04 (r108): with daily vesting (4 labels x many days) the 40 KB cap
    kept only the newest rows; the agent must see the tranches within 14 days
    of the cutoff, per label, at any schedule size."""
    from tank.util import canonical_json, parse_time

    now = utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: now)
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
    facts = await service.dispatch("get_token_facts", {"token_id": "ethena:ENA"}, {})
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


async def test_token_facts_give_the_covered_daily_volume(service) -> None:
    """03 §4.2 base of M (access and distribution): the token's covered daily
    volume per spot venue and in total, native and USD, with exclusions."""
    now = utcnow()
    rows = {
        "binance:spot:BTCUSDT": {
            "volume_native": "12000.5",
            "volume_usd": "900000000",
            "ts": iso(now - timedelta(minutes=2)),
        },
        "coinbase:spot:BTC-USD": {
            "volume_native": "8000",
            "volume_usd": "600000000",
            "ts": iso(now - timedelta(minutes=1)),
        },
        "kraken:spot:XBT/USD": {
            "volume_native": "100",
            "volume_usd": "7500000",
            "ts": iso(now - timedelta(hours=3)),
        },
        "okx:spot:BTC-USDT": {
            "volume_native": "500",
            "volume_usd": None,
            "usd_reason": "NO_USD_RATE",
            "ts": iso(now),
        },
    }
    for iid, row in rows.items():
        await service.repo.put(
            "mkt.tickers_24h", iid + ":t", {"instrument_id": iid, "token_id": "bitcoin:BTC", **row}
        )
    facts = await service.dispatch("get_token_facts", {"token_id": "bitcoin:BTC"}, {})
    volume = facts["covered_daily_volume"]

    assert volume["total_native"] == "20500.5" and volume["total_usd"] == "1500000000"
    assert {v["instrument_id"] for v in volume["venues"]} == {
        "binance:spot:BTCUSDT",
        "coinbase:spot:BTC-USD",
        "okx:spot:BTC-USDT",
    }
    assert volume["total_usd_excludes"] == [{"instrument_id": "okx:spot:BTC-USDT", "reason": "NO_USD_RATE"}]
    reasons = {e["instrument_id"]: e["reason"] for e in volume["excluded"]}
    assert (
        reasons["kraken:spot:XBT/USD"] == "TICKER_STALE"
        and reasons["bybit:spot:BTCUSDT"] == "NO_STORED_24H_TICKER"
    )
    assert volume["market_cap"] is None and volume["market_cap_reason"].startswith("NOT_STORED")


async def test_card_generated_at_is_the_version_write_and_deliver_is_runnable_then(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    """E2E #5 (r113): generated_at was stamped before the 8 s evidence bundle,
    so delivery looked late. generated_at is now when the immutable version is
    written (after the bundle is durable) and the DELIVER job runs from then."""
    import tank.analysis.cards as cards
    import tank.event_tools as event_tools
    from tank.util import parse_time

    monkeypatch.setattr(
        cards, "compose_card", lambda *a, **k: "Frozen card" + "\n" + score_summary(a[0].assessment.scores)
    )
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
        "evt.ems_assessments",
        "ems",
        {
            "dims": unassessed_score_dimensions(),
            "ems_id": "ems",
            "event_id": eid,
            "class": "STRUCTURAL_SHOCK",
            "criterion": "G5",
        },
    )
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "PARTIAL",
            "what": "Product announced",
            "verdict": "Product announced",
            "why": ["Product announced", "The publication does not quantify price or liquidity effects."],
        },
        {"event_id": eid, "call_id": "c", "role": "system"},
    )
    alert = await service.repo.get("out.alerts", f"{result['alert_id']}:1")
    assert parse_time(alert["generated_at"]) >= written["at"]
    job = next(
        job
        for job in await service.repo.list("ops.jobs", filters={"type": "DELIVER"})
        if job["payload"].get("alert_id")
    )
    assert parse_time(job["run_after"]) == parse_time(alert["generated_at"])
    assert alert["delivery_not_before"] == alert["generated_at"]


async def test_named_cutoff_precisions_are_accepted_as_buckets(service, truth_job_enqueued) -> None:
    """Live (22:51, 23:45, 05:05): cutoff_prec "MINUTE" was refused each time;
    named units now validate and normalize to their bucket, recorded in the result."""
    import asyncio
    import json
    from pathlib import Path

    schema = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "config/skills/tank-event-analyst/get_market_truth.schema.json"
        ).read_text()
    )
    for name in ("SECOND", "MINUTE", "HOUR"):
        validate_schema({"token_ids": ["T"], "cutoff": "2026-10-21T05:05:00Z", "cutoff_prec": name}, schema)
    eid = "named-prec"
    await service.repo.put(
        "evt.events", eid, {"event_id": eid, "current_ems_id": "ems", "tokens_mentioned": ["T"]}
    )
    await service.repo.put(
        "evt.ems_assessments",
        "ems",
        {"dims": unassessed_score_dimensions(), "ems_id": "ems", "event_id": eid, "class": "MAJOR"},
    )
    await service.repo.put(
        "evt.beneficiary_maps", f"{eid}:1", {"event_id": eid, "map_version": 1, "analysis_set": ["T"]}
    )
    task = asyncio.create_task(
        service.dispatch(
            "get_market_truth",
            {"event_id": eid, "token_ids": ["T"], "cutoff": "2026-10-21T05:05:00Z", "cutoff_prec": "MINUTE"},
            {},
        )
    )
    try:
        async with asyncio.timeout(2):
            await truth_job_enqueued.wait()
            for kind in ("ESCALATE_TOKENS", "RUN_MARKET_TRUTH"):
                job = await service.repo.claim_job()
                assert job is not None and job["type"] == kind
                if kind == "RUN_MARKET_TRUTH":
                    assert job["payload"]["cutoff_prec"] == "BUCKET(1m)"
                    stored = await service.repo.get("ops.jobs", job["job_id"])
                    persisted = complete_truth_fixture(
                        "m",
                        eid,
                        "T",
                        job["payload"]["cutoff"],
                        cutoff_prec=job["payload"]["cutoff_prec"],
                        cutoff_prec_reason=None,
                    )
                    await service.repo.put("ana.market_truth", "m", persisted)
                    stored["result"] = persisted
                    await service.repo.put("ops.jobs", job["job_id"], stored)
                await service.repo.finish_job(job["job_id"])
            result = await task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert result["cutoff_prec_normalized"] == {"from": "MINUTE", "to": "BUCKET(1m)"}


async def test_publish_card_keeps_market_truth_once_and_bounds_evidence_and_ops_reads(
    service, monkeypatch: pytest.MonkeyPatch
) -> None:
    """E2E #6 (r114): publish_card took 22 s. The card's card_struct copied the
    1.8 MB Market Truth, the bundle's request_log repeated it, and collections
    were re-read. Now card_struct references the immutable truth row, an exact
    request_log copy is a reference, and each collection is read once."""
    import tank.analysis.cards as cards
    from tank.event_tools import dedupe_request_log
    from tank.util import sha256

    truth = {"mta_id": "mta-1", "token_id": "T", "big": "x" * 10_000}
    calls = [
        {
            "tool": "get_market_truth",
            "call_id": "c1",
            "result": {"market_truth": [truth, {"mta_id": "other", "v": 1}]},
        },
        {"tool": "get_decision", "call_id": "c2", "result": {"action": "WAIT"}},
    ]
    deduped = dedupe_request_log(calls, [sha256(truth)])
    assert deduped[0]["result"]["market_truth"][0] == {
        "same_as_bundle_market_truth": "mta-1",
        "sha256": sha256(truth),
    }
    assert deduped[0]["result"]["market_truth"][1] == {"mta_id": "other", "v": 1}
    assert deduped[1] == calls[1] and calls[0]["result"]["market_truth"][0] is truth

    monkeypatch.setattr(
        cards, "compose_card", lambda *a, **k: "Frozen card" + "\n" + score_summary(a[0].assessment.scores)
    )
    x = await seed(service)
    eid = (await service.dispatch("record_event", x, {}))["event_id"]
    event = await service.repo.get("evt.events", eid)
    event["current_ems_id"] = "ems"
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "ems",
        {
            "dims": unassessed_score_dimensions(),
            "ems_id": "ems",
            "event_id": eid,
            "class": "STRUCTURAL_SHOCK",
            "criterion": "G5",
        },
    )
    reads = []
    original = service.repo.iter_all

    def counting(collection, **kwargs):
        reads.append(collection)
        return original(collection, **kwargs)

    monkeypatch.setattr(service.repo, "iter_all", counting)
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "PARTIAL",
            "what": "Product announced",
            "verdict": "Product announced",
            "why": ["Product announced", "The publication does not quantify price or liquidity effects."],
        },
        {"event_id": eid, "call_id": "c", "role": "system"},
    )
    assert result["published"]
    for collection in (
        "evt.beneficiary_maps",
        "evt.beneficiary_edges",
        "evt.claims",
        "evt.event_observations",
        "ana.contradictions",
    ):
        expected = 2 if collection == "evt.event_observations" else 1
        assert reads.count(collection) == expected, (collection, reads.count(collection))


@pytest.mark.parametrize("operation", ["record_event", "add_observation"])
async def test_successful_extraction_clears_prior_no_event_metadata(service, operation: str) -> None:
    arguments = await seed(service)
    context = {}
    if operation == "add_observation":
        event = await service.record_event(arguments, context)
        item = await service.repo.get("ingest.source_items", arguments["item_id"])
        item.update(
            item_id="retry-observation",
            extraction_status="SKIPPED",
            extraction_decision="NO_EVENT",
            extraction_reason="Earlier classification rejected the item",
        )
        await service.repo.put("ingest.source_items", item["item_id"], item)
        target = item["item_id"]
        await service.add_observation(
            {
                "event_id": event["event_id"],
                "item_id": target,
                "link": "CORROBORATION",
                "certainty": "HIGH",
                "certainty_reason": "Official source retained in the retry",
                "claims": arguments["claims"],
                "public_at": arguments["public_at"],
                "freshness": "FRESH",
            },
            context,
        )
    else:
        target = arguments["item_id"]
        item = await service.repo.get("ingest.source_items", target)
        item.update(
            extraction_status="SKIPPED",
            extraction_decision="NO_EVENT",
            extraction_reason="Earlier classification rejected the item",
        )
        await service.repo.put("ingest.source_items", target, item)
        await service.record_event(arguments, context)
    completed = await service.repo.get("ingest.source_items", target)
    assert completed["extraction_status"] == "DONE"
    assert completed.get("extraction_decision") is None
    assert completed.get("extraction_reason") is None


async def test_accepted_completion_and_native_failure_preserve_done_without_deadlock(
    service: EventToolService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    from tank.agents import SessionService

    arguments = await seed(service)
    repo = service.repo
    assert isinstance(repo, MemoryRepository)
    original = await repo.get("ingest.source_items", "i")
    assert original is not None
    original.update(
        extraction_status="PENDING",
        retained_capture={"origin": "explicit historical fixture", "lexical": "0001", "flags": [False, None]},
    )
    await repo.put("ingest.source_items", "i", original)
    await repo.put(
        "ops.agent_sessions",
        "completion-race",
        {
            "session_id": "completion-race",
            "status": "OPEN",
            "runtime_status": "RUNNING",
            "active_message_id": "turn",
            "message_log": [
                {
                    "message_id": "turn",
                    "turn_status": "RUNNING",
                    "retry_count": 1,
                    "payload": {"kind": "NEW_ITEM", "item_id": "i"},
                    "remote_event_id": "accepted-execution",
                    "body": {"type": "user.message"},
                }
            ],
        },
    )
    entered, release, native_started, start_native = (asyncio.Event() for _ in range(4))
    put = repo.put

    async def held_event_write(
        collection: str,
        key: str,
        document: Mapping[str, JsonInput],
        *,
        immutable: bool = False,
    ) -> dict[str, JsonValue]:
        result = await put(collection, key, document, immutable=immutable)
        if collection == "evt.events":
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(repo, "put", held_event_write)

    def no_network(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Native terminal failure must not retry the provider")

    async with httpx.AsyncClient(transport=httpx.MockTransport(no_network)) as http:
        native = SessionService(repo, service.settings, None, http=http, versions=fixture_versions())

        async def accept_fact() -> JsonValue:
            return JsonDocument.validate_value(
                await service.dispatch("record_event", arguments, {"session_id": "completion-race"})
            )

        async def fail_native() -> None:
            await start_native.wait()
            native_started.set()
            await native.process_event(
                "completion-race",
                {
                    "id": "concurrent-native-failure",
                    "type": "session.error",
                    "execution_id": "accepted-execution",
                    "reason": "Actual concurrent terminal fixture",
                },
            )

        accepted, failed = asyncio.create_task(accept_fact()), asyncio.create_task(fail_native())
        try:
            async with asyncio.timeout(2):
                await entered.wait()
            start_native.set()
            await native_started.wait()
            await asyncio.sleep(0)
            assert not failed.done()
            release.set()
            async with asyncio.timeout(2):
                result, _ = await asyncio.gather(accepted, failed)
            assert isinstance(result, dict)
            event_id = result["event_id"]
            assert isinstance(event_id, str)
            current = await repo.get("ingest.source_items", "i")
            assert current is not None and current["extraction_status"] == "DONE"
            assert "url" not in current and "author" not in current
            owned = {"extraction_status", "extraction_decision", "extraction_reason"}
            assert canonical_json(
                {key: value for key, value in current.items() if key not in owned}
            ) == canonical_json({key: value for key, value in original.items() if key not in owned})
            assert await repo.get("evt.events", event_id) is not None
            assert len(await repo.list("evt.event_observations")) == 1
            final = await repo.get("ops.agent_sessions", "completion-race")
            assert final is not None and final["status"] == "FAILED"
            terminal = final["terminal_failure"]
            assert isinstance(terminal, dict) and terminal["reason"] == "Actual concurrent terminal fixture"
        finally:
            release.set()
            start_native.set()
            for task in (accepted, failed):
                if not task.done():
                    task.cancel()
            await asyncio.gather(accepted, failed, return_exceptions=True)
            await native.close()


@pytest.mark.parametrize(
    "backend",
    ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])],
)
async def test_expired_tracking_status_preserves_the_frozen_financial_calculation(
    service, backend, postgres_template, repository_lifetime, monkeypatch: pytest.MonkeyPatch
) -> None:
    from copy import deepcopy

    from foundation_helpers import object_field

    from tank.analysis.decision_codec import decode_decision, encode_decision, encode_outlook
    from tank.util import parse_time

    if backend == "postgres":
        import os

        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
        service.repo = await repository_lifetime.enter_async_context(postgres_template.repository(url))

    fixed = parse_time("2026-10-29T00:00:00Z")
    for module in ("tank.util", "tank.event_tools"):
        monkeypatch.setattr(module + ".utcnow", lambda: fixed)
    frozen = decode_decision(
        complete_decision_fixture(
            "DEC-historical", "EVT-historical", "T", "2026-10-25T00:00:00Z", "MTA-historical"
        )
    )
    identity = frozen.metadata.identity
    original_outlook = encode_outlook(frozen)
    original_wire = encode_decision(frozen)
    object_field(original_wire, "outlook")["scenarios"] = {
        "base": None,
        "upside": None,
        "failure": "The captured failure scenario remains available.",
    }
    object_field(original_wire, "outlook")["scenario_reasons"] = {"upside": "ORIGINAL_CAUSE_AS_CAPTURED"}
    original_outlook["scenarios"] = object_field(original_wire, "outlook")["scenarios"]
    original_outlook["scenario_reasons"] = {"upside": "ORIGINAL_CAUSE_AS_CAPTURED"}
    literal_historical = deepcopy(original_wire)
    literal_outlook = deepcopy(original_outlook)
    expected = decode_decision(original_wire)
    prior_truth = complete_truth_fixture(
        identity.market_truth_id,
        identity.event_id,
        identity.token_id,
        identity.cutoff.value,
    )
    eid, token = identity.event_id, identity.token_id
    closed_at = "2026-10-28T00:00:00Z"
    for table, key, value in (
        (
            "evt.events",
            eid,
            complete_event_fixture(eid, current_ems_id=identity.assessment_id),
        ),
        (
            "evt.ems_assessments",
            identity.assessment_id,
            complete_assessment_fixture(identity.assessment_id, eid),
        ),
        (
            "evt.beneficiary_maps",
            "expired-map",
            {"event_id": eid, "map_version": 1, "analysis_set": [token]},
        ),
        (
            "evt.beneficiary_edges",
            "expired-edge",
            {
                "event_id": eid,
                "token_id": token,
                "edge_id": "expired-edge",
                "map_version": 1,
                "role": "PRIMARY",
                "credible": False,
                "exposure_score": None,
            },
        ),
        (
            "ana.market_truth",
            "newer-truth",
            complete_truth_fixture("newer-truth", eid, token, clock_iso()),
        ),
        ("ana.market_truth", identity.market_truth_id, prior_truth),
        ("ana.outlooks", frozen.outlook_id, original_outlook),
        ("ana.decisions", identity.decision_id, original_wire),
        (
            "ana.calls",
            "expired-call",
            {
                "call_id": "expired-call",
                "event_id": eid,
                "token_id": token,
                "status": "CLOSED",
                "opened_by_alert": "ALERT-historical-position",
                "opened_by_version": 1,
                "opened_at": "2026-10-25T00:00:00Z",
                "opened_at_prec": "EXACT",
                "opened_at_prec_reason": None,
                "entry_ref_price": "1",
                "invalidation_price": "0.9",
                "closed_by": "EXPIRED",
                "closed_at": closed_at,
                "closed_at_prec": "COLLECTION_ONLY",
                "closed_at_prec_reason": None,
            },
        ),
    ):
        await service.repo.put(table, key, value)
    wire = await service.get_decision({"event_id": eid, "token_id": token}, {})
    ended = decode_decision(wire)
    assert ended.calculation == expected.calculation
    assert ended.metadata.identity.cutoff == frozen.metadata.identity.cutoff
    assert ended.metadata.calculated_at == frozen.metadata.calculated_at
    assert ended.metadata.identity.generation == frozen.metadata.identity.generation
    assert ended.metadata.identity.assessment_id == identity.assessment_id
    assert ended.metadata.identity.market_truth_id == identity.market_truth_id
    assert ended.metadata.identity.thresholds_version == identity.thresholds_version
    assert wire["previous_decision_id"] == identity.decision_id
    assert wire["outlook_id"] == frozen.outlook_id and wire["conditions"] == []
    assert wire["tracking_recorded_at"] is not None
    assert wire["tracking_writer"]["versions"] == service.versions.model_dump(mode="json")
    assert object_field(wire, "outlook")["scenario_reasons"] == {"upside": "ORIGINAL_CAUSE_AS_CAPTURED"}
    assert ended.calculation.outlook.scenario_explanations.base.state == "EXPLANATION_NOT_CAPTURED"
    assert await service.repo.get("ana.decisions", identity.decision_id) == literal_historical
    assert await service.repo.get("ana.outlooks", frozen.outlook_id) == literal_outlook

    from tank.analysis.decision_state import DecisionScope, DecisionStateConflict, DecisionStateRepository

    states = DecisionStateRepository(service.repo)
    scope = DecisionScope(event_id=eid, token_id=token)
    current = await states.current(scope)
    assert current is not None and current[1] == ended and current[0].revision == 1
    conflicting = ended.model_copy(
        update={
            "metadata": ended.metadata.model_copy(
                update={"contradiction_ids": ("different-captured-evidence",)}
            )
        }
    )
    attempted_effects: list[str] = []

    async def effects() -> None:
        attempted_effects.append("must not run")

    with pytest.raises(DecisionStateConflict, match="conflicting content"):
        await states.advance(
            scope,
            expected=current[0],
            candidate=conflicting,
            recorded_at=current[0].recorded_at,
            effects=effects,
        )
    assert attempted_effects == []
    assert (await states.current(scope)) == current
    assert await service.repo.get("ana.decisions", ended.metadata.identity.decision_id) == wire

    fresh = decode_decision(
        complete_decision_fixture("DEC-new-active", eid, token, clock_iso(), "newer-truth")
    )

    async def no_writes() -> None:
        """This authored calculation has no separate watch write set."""

    fresh_head = await states.advance(
        scope,
        expected=current[0],
        candidate=fresh,
        recorded_at=current[0].recorded_at,
        effects=no_writes,
    )
    assert fresh_head.revision == 2
    assert fresh_head.previous_head_dec_id == ended.metadata.identity.decision_id
    assert fresh_head.recorded_at == current[0].recorded_at
    assert (await service.latest_record("ana.decisions", eid, token))["dec_id"] == "DEC-new-active"

    rollback_candidate = decode_decision(
        complete_decision_fixture("DEC-rollback", eid, token, clock_iso(), "newer-truth")
    )
    assert rollback_candidate.conditions
    watch = rollback_candidate.conditions[0]
    original_put = service.repo.put

    async def fail_head(collection, key, document, *, immutable=False):
        if collection == "ana.decision_state_heads":
            raise RuntimeError("injected final head failure")
        return await original_put(collection, key, document, immutable=immutable)

    async def write_watch() -> None:
        await service.repo.put(
            "ana.watch_conditions",
            watch.cond_id,
            watch.model_dump(mode="json", exclude_unset=True),
        )

    monkeypatch.setattr(service.repo, "put", fail_head)
    try:
        with pytest.raises(RuntimeError, match="injected final head failure"):
            await states.advance(
                scope,
                expected=fresh_head,
                candidate=rollback_candidate,
                recorded_at=fresh_head.recorded_at,
                effects=write_watch,
            )
    finally:
        monkeypatch.setattr(service.repo, "put", original_put)
    assert await service.repo.get("ana.watch_conditions", watch.cond_id) is None
    assert await service.repo.get("ana.decisions", "DEC-rollback") is None
    assert await service.repo.get("ana.outlooks", rollback_candidate.outlook_id) is None
    final = await states.current(scope)
    assert final is not None and final[0] == fresh_head and final[1] == fresh
    assert await service.repo.get("ana.decisions", identity.decision_id) == literal_historical
    assert await service.repo.get("ana.outlooks", frozen.outlook_id) == literal_outlook

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from test_event_tools import extraction, seed

from tank.analysis.cards import preliminary_gate
from tank.delivery import DeliveryService
from tank.event_tools import EventToolService
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.fixture
async def service(tmp_path):
    repo = MemoryRepository()
    await repo.open()
    return EventToolService(repo, Settings(data_dir=tmp_path), None, None)


async def lead(service):
    args = await seed(service)
    item = await service.repo.get("ingest.source_items", "i")
    item["url"] = "https://example.org/commit/123"
    await service.repo.put("ingest.source_items", "i", item)
    args.update(certainty="LOW", certainty_reason="Integration is not confirmed")
    args["claims"][0].update(verified=False, verification_missing="No partner confirmation")
    result = await service.dispatch("record_event", args, {"session_id": "s"})
    eid = result["event_id"]
    event = await service.repo.get("evt.events", eid)
    event.update(current_ems_id="ems", lifecycle="TRACKED")
    await service.repo.put("evt.events", eid, event)
    await service.repo.put(
        "evt.ems_assessments",
        "ems",
        {"ems_id": "ems", "event_id": eid, "class": "MATERIAL", "certainty": "LOW", "criterion": "MATERIAL"},
    )
    return {
        "event_id": eid,
        "kind": "PRELIMINARY",
        "what": "The repository contains an integration adapter.",
        "verdict": "An integration adapter appeared",
        "why": ["It could connect the protocol to the counterparty.", "Actual use remains unknown."],
        "unconfirmed": "The counterparty has not confirmed adoption or a launch date.",
    }


async def deliver(repo, http, result):
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    service = DeliveryService(repo, settings, http=http)
    await service.deliver({"alert_id": result["alert_id"], "version": result["version"]})


async def test_preliminary_without_market_analysis_preserves_ordinary_gate(service, monkeypatch):
    args = await lead(service)
    denied = await service.dispatch("publish_card", {**args, "kind": "FULL"}, {})
    assert denied["reason"] == "BELOW_MAJOR"
    analysis_read = AsyncMock(side_effect=AssertionError("Preliminary cards must not read analysis records"))
    monkeypatch.setattr(service, "latest_record", analysis_read)
    result = await service.dispatch("publish_card", args, {})
    assert result["published"]
    row = await service.repo.get("out.alerts", f"{result['alert_id']}:1")
    assert row["kind"] == "PRELIMINARY"
    assert row["card_struct"]["assessment"]["class"] == "MATERIAL"
    assert row["card_struct"]["event"]["certainty"] == "LOW"
    assert row["decisions"] == row["shown_tokens"] == []
    assert row["card_struct"]["market_truths"] == []
    assert row["card_text"].startswith("PRELIMINARY · MATERIAL")
    assert "VERIFIED:" in row["card_text"]
    assert "NOT VERIFIED:" in row["card_text"]
    assert "WHY IT MATTERS" in row["card_text"]
    assert "Market analysis pending" in row["card_text"]
    assert all(label not in row["card_text"] for label in ("SOURCE:", "POSSIBLE EFFECT:", "NOT CONFIRMED:"))
    bundle = await service.repo.get("out.evidence_bundles", row["evidence_bundle_id"])
    assert bundle["payload"]["source_items"][0]["url"] == "https://example.org/commit/123"
    assert args["unconfirmed"] in row["card_text"]
    assert "WHAT TO DO" not in row["card_text"]
    assert sum(block["type"] == "table" for block in row["rich_message"]["blocks"]) == 1
    assert not await service.repo.list("ana.market_truth")
    assert not await service.repo.list("ana.decisions")
    assert not await service.repo.list("ana.calls")
    again = await service.dispatch("publish_card", args, {})
    assert again["reason"] == "NO_MATERIAL_CHANGE"
    assert len(await service.repo.list("out.alerts")) == 1
    analysis_read.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("freshness", "STALE", "STALE"),
        ("lifecycle", "INVALIDATED", "TERMINAL_EVENT"),
        ("lifecycle", "CLOSED", "TERMINAL_EVENT"),
        ("lifecycle", "MERGED", "TERMINAL_EVENT"),
    ],
)
async def test_preliminary_refuses_terminal_and_stale_events(service, field, value, reason):
    args = await lead(service)
    event = await service.repo.get("evt.events", args["event_id"])
    event[field] = value
    await service.repo.put("evt.events", args["event_id"], event)
    result = await service.dispatch("publish_card", args, {})
    assert result["reason"] == reason
    assert not await service.repo.list("out.alerts")


@pytest.mark.parametrize(
    "event,assessment,reason",
    [
        ({"claims": []}, {"class": "MATERIAL"}, "PRELIMINARY_REQUIRES_EVIDENCE"),
        ({}, {"class": "LOW"}, "PRELIMINARY_REQUIRES_MATERIAL"),
        ({}, {"class": "MAJOR"}, "PRELIMINARY_REQUIRES_MATERIAL"),
    ],
)
def test_preliminary_requires_material_and_source_evidence(event, assessment, reason):
    assert preliminary_gate(event, assessment) == (False, reason)


@pytest.mark.parametrize(
    "key,value", [("unconfirmed", ""), ("unconfirmed", None), ("why", []), ("what", " ")]
)
async def test_preliminary_requires_explanation_and_uncertainty(service, key, value):
    args = await lead(service)
    if value is None:
        args.pop(key)
    else:
        args[key] = value
    with pytest.raises(ValueError):
        await service.dispatch("publish_card", args, {})
    assert not await service.repo.list("out.alerts")


async def test_preliminary_then_full_uses_ordinary_requirements_and_same_thread(service):
    args = await lead(service)
    eid = args["event_id"]
    preliminary = await service.dispatch("publish_card", args, {})
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 40 + len(requests)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        await deliver(service.repo, http, preliminary)
        event = await service.repo.get("evt.events", eid)
        assert "first_card_delivered_at" not in event
        assert event["lifecycle"] == "TRACKED"
        event.update(certainty="MEDIUM")
        await service.repo.put("evt.events", eid, event)
        assessment = await service.repo.get("evt.ems_assessments", "ems")
        assessment.update({"class": "MAJOR", "certainty": "MEDIUM", "criterion": "M2"})
        await service.repo.put("evt.ems_assessments", "ems", assessment)
        with pytest.raises(ValueError, match="UPDATE requires"):
            await service.dispatch("publish_card", {**args, "kind": "UPDATE"}, {})
        incomplete = await service.dispatch("publish_card", {**args, "kind": "FULL"}, {})
        assert incomplete["reason"] == "ANALYSIS_INCOMPLETE"
        await service.repo.put(
            "evt.beneficiary_maps",
            "map",
            {
                "event_id": eid,
                "map_version": 1,
                "analysis_set": ["world:WLD"],
            },
        )
        incomplete = await service.dispatch("publish_card", {**args, "kind": "FULL"}, {})
        assert incomplete["missing_market_truth"] == ["world:WLD"]
        assert incomplete["missing_decisions"] == ["world:WLD"]
        await service.repo.put(
            "ana.market_truth",
            "truth",
            {
                "mta_id": "truth",
                "event_id": eid,
                "token_id": "world:WLD",
                "cutoff": iso(),
                "data_confidence": {"class": "LOW"},
            },
        )
        await service.repo.put(
            "ana.decisions",
            "decision",
            {
                "dec_id": "decision",
                "event_id": eid,
                "token_id": "world:WLD",
                "market_truth_id": "truth",
                "cutoff": iso(),
                "action": "WAIT",
            },
        )
        full = await service.dispatch("publish_card", {**args, "kind": "FULL"}, {})
        assert full["published"]
        assert full["alert_id"] == preliminary["alert_id"]
        assert full["version"] == 2
        await deliver(service.repo, http, full)
        event = await service.repo.get("evt.events", eid)
        assert event["first_card_delivered_at"]
    assert len(requests) == 2
    assert requests[0].get("reply_parameters") is None
    assert requests[1]["reply_parameters"]["message_id"] == 41
    assert not await service.repo.list("ana.calls")
    denied = await service.dispatch("publish_card", args, {})
    assert denied["reason"] == "ORDINARY_CARD_EXISTS"


async def test_superseded_preliminary_is_not_delivered(service):
    args = await lead(service)
    first = await service.dispatch("publish_card", args, {})
    second = await service.dispatch(
        "publish_card",
        {
            **args,
            "unconfirmed": "The counterparty denied involvement; the code remains unexplained.",
        },
        {},
    )
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        await deliver(service.repo, http, first)
        assert requests == []
        await deliver(service.repo, http, second)
    assert len(requests) == 1


async def arm(service, eid, due, **extra):
    return await service.dispatch(
        "set_watch_condition",
        {
            "event_id": eid,
            "action": "ARM",
            "review_at": iso(due),
            "review_question": "Has the counterparty confirmed the integration?",
            **extra,
        },
        {"session_id": "s", "event_id": eid},
    )


async def test_timed_review_fires_once_in_original_session_without_market_reads(service):
    args = await lead(service)
    now = utcnow()
    due = now + timedelta(hours=6)
    result = await arm(service, args["event_id"], due)
    existing = await arm(service, args["event_id"], due)
    assert existing["existing"]
    condition = result["condition"]
    assert condition["token_id"] is None
    market = SimpleNamespace(assess=AsyncMock(side_effect=AssertionError("No market read expected")))
    scheduler = Scheduler(service.repo, service.settings, None, market)
    await scheduler.eval_conditions({"cutoff": iso(now)})
    assert (await service.repo.get("ana.watch_conditions", condition["cond_id"]))["status"] == "ARMED"
    await scheduler.eval_conditions({"cutoff": iso(due)})
    await scheduler.eval_conditions({"cutoff": iso(due + timedelta(minutes=1))})
    jobs = await service.repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    reviews = [job for job in jobs if job["payload"].get("kind") == "CONDITION_FIRED"]
    assert len(reviews) == 1
    payload = reviews[0]["payload"]
    assert payload["session_id"] == "s"
    assert payload["event_id"] == args["event_id"]
    assert payload["review_question"] == condition["expr"]["review_question"]
    assert payload["review_at"] == iso(due)
    market.assess.assert_not_awaited()


@pytest.mark.parametrize(
    "hours,extra",
    [
        (-1, {}),
        (80, {}),
        (1, {"expires_in_hours": 1}),
        (1, {"token_id": "world:WLD"}),
        (1, {"conditions": [{"metric": "event.state", "op": "==", "value": "LIVE"}]}),
    ],
)
async def test_timed_review_rejects_invalid_deadlines_or_metric_mix(service, hours, extra):
    args = await lead(service)
    with pytest.raises(ValueError):
        await arm(service, args["event_id"], utcnow() + timedelta(hours=hours, seconds=1), **extra)
    assert not await service.repo.list("ana.watch_conditions")


@pytest.mark.parametrize("outcome", ["DISARM", "INVALIDATED", "EXPIRED"])
async def test_timed_review_cancellation_and_expiry(service, outcome):
    args = await lead(service)
    now = utcnow()
    result = await arm(service, args["event_id"], now + timedelta(hours=1), expires_in_hours=2)
    cid = result["condition"]["cond_id"]
    if outcome == "DISARM":
        await service.dispatch(
            "set_watch_condition",
            {
                "event_id": args["event_id"],
                "action": "DISARM",
                "cond_id": cid,
            },
            {},
        )
    elif outcome == "INVALIDATED":
        event = await service.repo.get("evt.events", args["event_id"])
        event["lifecycle"] = "INVALIDATED"
        await service.repo.put("evt.events", args["event_id"], event)
    scheduler = Scheduler(service.repo, service.settings, None, None)
    await scheduler.eval_conditions({"cutoff": iso(now + timedelta(hours=3 if outcome == "EXPIRED" else 1))})
    row = await service.repo.get("ana.watch_conditions", cid)
    assert row["status"] == ("EXPIRED" if outcome == "EXPIRED" else "CANCELLED")
    assert not await service.repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})


async def test_pending_review_preserves_tracked_event_until_disarmed(service):
    args = await lead(service)
    eid = args["event_id"]
    now = utcnow()
    result = await arm(service, eid, now + timedelta(hours=1))
    event = await service.repo.get("evt.events", eid)
    event["known_to_us_at"] = iso(now - timedelta(days=8))
    await service.repo.put("evt.events", eid, event)
    for row in await service.repo.list("evt.event_observations"):
        row["observed_at"] = iso(now - timedelta(days=8))
        await service.repo.put("evt.event_observations", row["obs_id"], row)
    scheduler = Scheduler(service.repo, service.settings, None, None)
    await scheduler._advance_lifecycle(event, now)
    assert (await service.repo.get("evt.events", eid))["lifecycle"] == "TRACKED"
    await service.dispatch(
        "set_watch_condition",
        {
            "event_id": eid,
            "action": "DISARM",
            "cond_id": result["condition"]["cond_id"],
        },
        {},
    )
    await scheduler._advance_lifecycle(event, now)
    assert (await service.repo.get("evt.events", eid))["lifecycle"] == "CLOSED"


def test_superseded_evidence_cannot_authorize_preliminary():
    event = {
        "claims": [
            {"claim_id": "old", "accepted": True, "quote": "Old assertion", "evidence": [{"item_id": "i"}]},
            {
                "claim_id": "new",
                "accepted": True,
                "supersedes_claim_ids": ["old"],
                "quote": "",
                "evidence": [],
            },
        ]
    }
    assert preliminary_gate(event, {"class": "MATERIAL"}) == (False, "PRELIMINARY_REQUIRES_EVIDENCE")


async def test_failed_preliminary_can_be_republished_without_new_evidence(service):
    args = await lead(service)
    first = await service.dispatch("publish_card", args, {})
    await service.repo.put(
        "out.deliveries",
        "failed",
        {
            "alert_id": first["alert_id"],
            "version": first["version"],
            "channel": "telegram:alerts",
            "status": "FAILED",
            "terminal": True,
            "attempt": 5,
            "at": iso(),
        },
        immutable=True,
    )
    retry = await service.dispatch("publish_card", args, {})
    assert retry["published"]
    assert retry["version"] == 2


@pytest.mark.parametrize("state", ["TRACKED", "COOLDOWN"])
async def test_fired_review_keeps_old_event_open_for_queued_analysis(service, state):
    args = await lead(service)
    eid = args["event_id"]
    now = utcnow()
    due = now + timedelta(hours=1)
    event = await service.repo.get("evt.events", eid)
    known = iso(now - timedelta(days=8))
    event.update(lifecycle=state, known_to_us_at=known, cooldown_at=known)
    await service.repo.put("evt.events", eid, event)
    for row in await service.repo.list("evt.event_observations"):
        row["observed_at"] = known
        await service.repo.put("evt.event_observations", row["obs_id"], row)
    result = await arm(service, eid, due)
    scheduler = Scheduler(service.repo, service.settings, None, None)
    await scheduler._advance_lifecycle(event, now)
    assert (await service.repo.get("evt.events", eid))["lifecycle"] == state
    await scheduler.eval_conditions({"cutoff": iso(due)})
    event = await service.repo.get("evt.events", eid)
    await scheduler._advance_lifecycle(event, due)
    event = await service.repo.get("evt.events", eid)
    assert event["lifecycle"] == state
    assert event["known_to_us_at"] == known
    assert event["review_requested_at"] == iso(due)
    assert (await service.repo.get("ana.watch_conditions", result["condition"]["cond_id"]))[
        "status"
    ] == "FIRED"


async def deny(service, eid):
    await service.repo.put(
        "ingest.source_items",
        "denial",
        dict(
            item_id="denial",
            source_id="S01",
            text="The partner denies any integration",
            source_class="PRIMARY",
            observed_at=iso(utcnow() + timedelta(seconds=2)),
        ),
    )
    x = extraction()
    x["claims"][0].update(verified=True, quote="The partner denies any integration")
    await service.dispatch(
        "add_observation",
        dict(
            event_id=eid,
            item_id="denial",
            link="DENIAL",
            certainty="HIGH",
            certainty_reason="Official statement of the named partner",
            claims=x["claims"],
            public_at=x["public_at"],
            freshness="FRESH",
        ),
        {},
    )
    return await service.repo.get("evt.events", eid)


async def test_denied_preliminary_gets_one_immediate_correction_in_its_thread(service):
    from test_agents import settings

    from tank.agents.sessions import SessionService

    args = await lead(service)
    eid = args["event_id"]
    first = await service.dispatch("publish_card", args, {})
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 40 + len(requests)}})

    opts, _ = settings()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        await deliver(service.repo, http, first)
        event = await deny(service, eid)
        assert event["lifecycle"] == "INVALIDATED"
        session = {"session_id": "s", "event_id": eid, "runtime_status": "IDLE"}
        sessions = SessionService(service.repo, opts, None, http=http)
        for job in await service.repo.list("ops.jobs"):
            await service.repo.put("ops.jobs", job["job_id"], {**job, "status": "DONE"})
        assert not await sessions.can_archive(session, event)
        update = await service.dispatch("publish_card", {**args, "kind": "UPDATE"}, {})
        assert update["reason"] == "BELOW_MAJOR"
        correction = {
            **{key: value for key, value in args.items() if key != "unconfirmed"},
            "verdict": "The partner denied the integration",
            "why": ["The partner officially denied any integration."],
        }
        result = await service.dispatch("publish_card", correction, {})
        assert result["published"]
        assert (result["alert_id"], result["version"]) == (first["alert_id"], 2)
        row = await service.repo.get("out.alerts", f"{first['alert_id']}:2")
        assert row["card_text"].startswith("PRELIMINARY · DENIED · MATERIAL")
        assert row["terminal_correction"]
        assert row["delivery_immediate"]
        again = await service.dispatch("publish_card", correction, {})
        assert again["reason"] == "TERMINAL_EVENT"
        await deliver(service.repo, http, result)
        event = await service.repo.get("evt.events", eid)
        assert event["session_finalized_at"]
        for job in await service.repo.list("ops.jobs"):
            await service.repo.put("ops.jobs", job["job_id"], {**job, "status": "DONE"})
        assert await sessions.can_archive(session, event)
    assert len(requests) == 2
    assert requests[1]["reply_parameters"]["message_id"] == 41


async def test_invalidated_event_without_preliminary_card_refuses_correction(service):
    args = await lead(service)
    await deny(service, args["event_id"])
    args.pop("unconfirmed")
    result = await service.dispatch("publish_card", args, {})
    assert result["reason"] == "TERMINAL_EVENT"
    assert not await service.repo.list("out.alerts")


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_preliminary_and_timed_review_roundtrip(tmp_path, postgres_template):
    import os

    from test_event_tools import extraction, unknown_assessment

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import compute_market_truth
    from tank.event_tools import LAYERS
    from tank.util import new_id, sha256

    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
    async with postgres_template.repository(url) as repo:
        service = EventToolService(repo, Settings(data_dir=tmp_path), None, DecisionEngine())
        item_id = new_id("item")
        stamp = iso()
        quote = "World access approved"
        raw = tmp_path / "source.txt"
        raw.write_text(quote)
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
                "url": "https://example.org/commit/123",
            },
        )
        x = extraction()
        x.update(
            item_id=item_id,
            statement=quote,
            certainty="LOW",
            certainty_reason="Unconfirmed",
            entities=[{"entity_id": "world", "role": "SUBJECT"}],
        )
        x["claims"][0].update(quote=quote, state_claim="APPROVED", verified=False)
        context = {"model_id": "fixture-model", "prompt_version": "fixture-prompt", "role": "system"}
        created = await service.dispatch("record_event", x, context)
        eid = created["event_id"]
        proof = created["accepted_claims"][0]["claim_id"]
        assessment = await service.dispatch(
            "set_assessment",
            {
                "event_id": eid,
                "assessment": unknown_assessment(claim=proof),
            },
            context,
        )
        assert assessment["class"] == "MATERIAL"
        card = {
            "event_id": eid,
            "kind": "PRELIMINARY",
            "what": quote,
            "verdict": quote,
            "why": ["Potential new access", "The proposed scope remains unknown"],
            "unconfirmed": "Counterparty confirmation is missing",
        }
        first = await service.dispatch("publish_card", card, context)
        assert first["published"]
        stored = await repo.get("out.alerts", f"{first['alert_id']}:{first['version']}")
        assert stored["kind"] == "PRELIMINARY" and stored["decisions"] == []
        requests = []

        def respond(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(requests) + 40}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            await deliver(repo, http, first)
            event = await repo.get("evt.events", eid)
            assert not event.get("first_card_delivered_at")
            event.update(certainty="MEDIUM", event_state="APPROVED")
            await repo.put("evt.events", eid, event)
            assessment = await service.dispatch(
                "set_assessment",
                {
                    "event_id": eid,
                    "assessment": unknown_assessment(claim=proof),
                },
                context,
            )
            assert assessment["class"] == "MAJOR"
            await service.dispatch(
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
                            "causal_chain": "Primary approved access",
                            "directionality": "BOTH",
                            "exposure_score": 6,
                            "edge_certainty": "MEDIUM",
                            "evidence": [proof],
                        }
                    ],
                    "layer_reasons": {layer: "No intermediary evidence" for layer in LAYERS},
                },
                context,
            )
            truth = compute_market_truth({"event_id": eid, "token_id": "world:WLD", "cutoff": iso()})
            truth["mta_id"] = new_id("mta")
            await repo.put("ana.market_truth", truth["mta_id"], truth)
            await service.dispatch("get_decision", {"event_id": eid, "token_id": "world:WLD"}, context)
            full = await service.dispatch("publish_card", {**card, "kind": "FULL"}, context)
            assert full["published"] and full["version"] == 2 and full["alert_id"] == first["alert_id"]
            await deliver(repo, http, full)
        assert requests[1]["reply_parameters"]["message_id"] == 41
        now = utcnow()
        result = await service.dispatch(
            "set_watch_condition",
            {
                "event_id": eid,
                "action": "ARM",
                "review_at": iso(now + timedelta(minutes=5)),
                "review_question": "Has adoption been confirmed?",
            },
            context,
        )
        cid = result["condition"]["cond_id"]
        assert (await repo.get("ana.watch_conditions", cid))["token_id"] is None
        scheduler = Scheduler(
            repo, service.settings, None, SimpleNamespace(assess=AsyncMock(return_value=truth))
        )
        await scheduler.eval_conditions({"cutoff": iso(now + timedelta(minutes=5))})
        assert (await repo.get("ana.watch_conditions", cid))["status"] == "FIRED"
        jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
        assert any(job["payload"].get("review_question") for job in jobs)


@pytest.mark.parametrize("phase", ["before_due", "due", "expired"])
async def test_disarm_wins_while_timed_review_waits_for_event_lock(service, monkeypatch, phase):
    import asyncio
    from contextlib import asynccontextmanager

    args = await lead(service)
    eid = args["event_id"]
    now = utcnow()
    result = await arm(service, eid, now + timedelta(hours=1), expires_in_hours=2)
    cid = result["condition"]["cond_id"]
    waiting, resume = asyncio.Event(), asyncio.Event()
    original_lock = service.repo.lock

    @asynccontextmanager
    async def pause_review(key):
        if key == "event:" + eid and asyncio.current_task().get_name() == "timed-review":
            waiting.set()
            await resume.wait()
        async with original_lock(key):
            yield

    monkeypatch.setattr(service.repo, "lock", pause_review)
    scheduler = Scheduler(service.repo, service.settings, None, None)
    cutoff = now + timedelta(hours={"before_due": 0, "due": 1, "expired": 3}[phase])
    task = asyncio.create_task(scheduler.eval_conditions({"cutoff": iso(cutoff)}), name="timed-review")
    try:
        await asyncio.wait_for(waiting.wait(), timeout=2)
        cancelled = await service.dispatch(
            "set_watch_condition",
            {
                "event_id": eid,
                "action": "DISARM",
                "cond_id": cid,
            },
            {},
        )
        assert cancelled["accepted"] and cancelled["status"] == "CANCELLED"
    finally:
        resume.set()
        await asyncio.wait_for(task, timeout=2)
    assert (await service.repo.get("ana.watch_conditions", cid))["status"] == "CANCELLED"
    assert not await service.repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert not (await service.repo.get("evt.events", eid)).get("review_requested_at")


async def test_metric_and_timed_watches_share_deduplication_and_capacity(service):
    from tank.event_tools import MAX_AGENT_WATCHES

    args = await lead(service)
    eid = args["event_id"]
    metric = {
        "event_id": eid,
        "action": "ARM",
        "token_id": "world:WLD",
        "conditions": [{"metric": "event.state", "op": "==", "value": "APPROVED"}],
        "text": "Check event approval",
    }
    first = await service.dispatch("set_watch_condition", metric, {})
    due = utcnow() + timedelta(hours=6)
    timed = await arm(service, eid, due)
    for index in range(MAX_AGENT_WATCHES - 2):
        await arm(service, eid, due + timedelta(minutes=index + 1))
    assert (await service.dispatch("set_watch_condition", metric, {}))["existing"]
    assert (await arm(service, eid, due))["condition"]["cond_id"] == timed["condition"]["cond_id"]
    with pytest.raises(ValueError, match="agent rules"):
        await arm(service, eid, due + timedelta(hours=1))
    await service.dispatch(
        "set_watch_condition",
        {
            "event_id": eid,
            "action": "DISARM",
            "cond_id": first["condition"]["cond_id"],
        },
        {},
    )
    assert (await arm(service, eid, due + timedelta(hours=1)))["accepted"]
    active = await service.repo.list("ana.watch_conditions", filters={"event_id": eid, "status": "ARMED"})
    assert len(active) == MAX_AGENT_WATCHES

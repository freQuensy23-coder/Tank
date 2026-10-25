import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from test_agents_pipeline_audit import options
from test_event_tools import seed
from test_session_binding_concurrency import fixture_repo
from test_sources import captured
from test_sources import settings as source_settings

from tank.agents.pipeline import Pipeline
from tank.delivery import DeliveryError, DeliveryService
from tank.event_tools import EventToolService
from tank.ops_progress import item_progress, session_progress
from tank.runtime import Runtime
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import MemoryRepository
from tank.telegram_rich import score_summary
from tank.util import iso, utcnow


async def notices(repo):
    return await repo.list("out.ops_notices", filters={"kind": "ITEM_PROGRESS"})


def delivery_settings():
    return SimpleNamespace(
        ops_progress_enabled=True,
        ops_progress_delay_s=0,
        telegram_token="test",
        telegram_alerts_chat="alerts",
        telegram_ops_chat="ops",
    )


@pytest.mark.parametrize("source_id,decision", [("S30", None), ("S01", "ALWAYS")])
async def test_every_ingested_material_reaches_ops_before_filtering_and_duplicates_are_silent(
    tmp_path, source_id, decision
):
    repo = MemoryRepository()
    source = {"source_id": source_id, "name": "Fixture source"}
    settings = source_settings(tmp_path, [source])
    settings.ops_progress_enabled = True
    service = SourceService(repo, settings)
    try:
        item = captured(
            service,
            source_id,
            {
                "external_id": "first",
                "title": "A new item",
                "text": "All input",
                "url": "https://example.test/item",
            },
        )
        result = await service.ingest(source, item)
        await service.ingest(source, item)
        await service.ingest(
            source,
            captured(
                service, source_id, {"external_id": "duplicate", "title": "A new item", "text": "All input"}
            ),
        )
    finally:
        await service.close()
    rows = await notices(repo)
    expected = {"RECEIVED", "JEV_BYPASSED"} if decision == "ALWAYS" else {"RECEIVED"}
    assert len(rows) == len(expected) and {row["stage"] for row in rows} == expected
    assert result["relevance"]["decision"] == decision
    assert all(row["format_version"] == 2 for row in rows)
    assert await repo.list("out.alerts") == []
    assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == len(expected)
    if decision == "ALWAYS":
        assert await repo.list("ops.jobs", filters={"type": "FILTER_ITEM"}) == []
        await Pipeline(repo, settings, http=object()).route_item({"item_id": result["item_id"]})
        await Pipeline(repo, settings, http=object()).route_item({"item_id": result["item_id"]})
        assert {row["stage"] for row in await notices(repo)} == expected | {"AGENT_QUEUED"}
        assert len(await notices(repo)) == 3


@pytest.mark.parametrize(
    "score,expected", [(0.1, "JEV_REJECTED"), (0.9, "JEV_PASSED"), (None, "JEV_BYPASSED")]
)
async def test_filter_outcomes_and_routing_are_visible_without_publishing(score, expected):
    settings, _ = options()
    settings.ops_progress_enabled = True
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "title": "Aave announcement",
            "relevance": {"decision": "ALWAYS" if score is None else None},
            "observed_at": iso(),
        },
    )
    pipeline = Pipeline(repo, settings, http=object())
    pipeline._score_record = AsyncMock(
        return_value={
            "score": score,
            "resolved_model_id": "fixture",
            "classifier_provider": "fixture",
            "requested_model_id": "fixture",
            "semantic_model_id": "fixture",
            "model_pin_evidence": {},
            "usage_id": "usage",
        }
    )
    await pipeline.filter_item({"item_id": "i"})
    await pipeline.filter_item({"item_id": "i"})
    assert [row["stage"] for row in await notices(repo)] == [expected]
    if expected == "JEV_REJECTED":
        assert await repo.list("ops.jobs", filters={"type": "ROUTE_ITEM"}) == []
        assert "порог=0.7000" in (await notices(repo))[0]["reason"]
    else:
        await pipeline.route_item({"item_id": "i"})
        await pipeline.route_item({"item_id": "i"})
        assert len([row for row in await notices(repo) if row["stage"] == "AGENT_QUEUED"]) == 1
        assert len(await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})) == 1
    assert await repo.list("out.alerts") == []


@pytest.mark.parametrize("is_event", [False, True])
async def test_agent_extraction_and_publication_gate_explain_silence(tmp_path, is_event):
    repo = MemoryRepository()
    service = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
    extraction = await seed(service)
    extraction.update(
        is_event=is_event,
        certainty_reason="Official source",
        no_event_reason="No relevant event in this material",
    )
    result = await service.dispatch("record_event", extraction, {})
    if not is_event:
        rows = await notices(repo)
        assert rows[0]["stage"] == "AGENT_REJECTED"
        assert rows[0]["reason"] == "No relevant event in this material"
    else:
        eid = result["event_id"]
        event = await repo.get("evt.events", eid)
        await repo.put("evt.events", eid, {**event, "current_ems_id": "ems"})
        await repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": eid, "class": "LOW"})
        result = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "FULL",
                "what": "Low significance",
                "verdict": "Low significance",
                "why": ["Below threshold"],
            },
            {"event_id": eid, "role": "system"},
        )
        assert result["published"] is False and result["reason"] == "BELOW_MAJOR"
        rows = await notices(repo)
        assert {row["stage"] for row in rows} == {"EVENT_RECORDED", "PUBLICATION_BLOCKED"}
        assert next(row["reason"] for row in rows if row["stage"] == "PUBLICATION_BLOCKED") == "BELOW_MAJOR"
    assert await repo.list("out.alerts") == []


@pytest.mark.parametrize("success", [True, False])
async def test_generated_card_only_becomes_sent_after_telegram_receipt(tmp_path, monkeypatch, success):
    import tank.analysis.cards as cards

    repo = MemoryRepository()
    service = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
    extraction = await seed(service)
    eid = (await service.dispatch("record_event", extraction, {}))["event_id"]
    event = await repo.get("evt.events", eid)
    await repo.put("evt.events", eid, {**event, "current_ems_id": "ems"})
    await repo.put(
        "evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": eid, "class": "STRUCTURAL_SHOCK"}
    )
    monkeypatch.setattr(cards, "alert_gate", lambda *a, **k: (True, "INITIAL"))
    monkeypatch.setattr(cards, "compose_card", lambda *a, **k: "Frozen card" + "\n" + score_summary(a[1]))
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "PARTIAL",
            "what": "A material event",
            "verdict": "A material event",
            "why": ["Official release"],
        },
        {"event_id": eid, "role": "system"},
    )
    assert "ALERT_QUEUED" in {row["stage"] for row in await notices(repo)}
    assert "ALERT_SENT" not in {row["stage"] for row in await notices(repo)}
    payload = {"alert_id": result["alert_id"], "version": result["version"]}
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return (
            httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
            if success
            else httpx.Response(500, json={"ok": False})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        delivery = DeliveryService(repo, delivery_settings(), http=http)
        if success:
            await delivery.deliver(payload)
            await delivery.deliver(payload)
        else:
            for _ in range(5):
                with pytest.raises(DeliveryError):
                    await delivery.deliver(payload)
            await delivery.terminal_failure(payload, "EXPIRED", 5)
    stages = [row["stage"] for row in await notices(repo)]
    assert stages.count("ALERT_SENT" if success else "ALERT_FAILED") == 1
    assert ("ALERT_FAILED" if success else "ALERT_SENT") not in stages
    assert all(request["chat_id"] == "alerts" for request in requests)


async def test_agent_acceptance_completion_and_failure_are_not_confused_with_approval():
    repo = MemoryRepository()
    settings = delivery_settings()
    await repo.put("ingest.source_items", "i", {"item_id": "i", "extraction_status": "PENDING"})
    session = {
        "session_id": "s",
        "message_log": [
            {
                "message_id": "m",
                "payload": {"kind": "NEW_ITEM", "item_id": "i"},
                "sent_at": iso(),
                "turn_status": "PENDING",
            }
        ],
    }
    await session_progress(repo, settings, session)
    session["message_log"][0]["turn_status"] = "DONE"
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "extraction_status": "SKIPPED",
            "extraction_reason": "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION",
        },
    )
    await session_progress(repo, settings, session)
    session["message_log"][0]["turn_status"] = "FAILED"
    await session_progress(repo, settings, session)
    await session_progress(repo, settings, session)
    assert {row["stage"] for row in await notices(repo)} == {
        "AGENT_ACCEPTED",
        "AGENT_REJECTED",
        "PROCESSING_FAILED",
    }
    assert await repo.list("out.alerts") == []


@pytest.mark.parametrize("kind", ["FILTER_ITEM", "ROUTE_ITEM", "AGENT_MESSAGE"])
@pytest.mark.parametrize("status", ["FAILED", "EXPIRED"])
async def test_terminal_jobs_and_restart_reconciliation_report_where_processing_stopped(kind, status):
    repo = MemoryRepository()
    runtime = object.__new__(Runtime)
    runtime.repo, runtime.settings = repo, delivery_settings()
    await repo.put("ingest.source_items", "i", {"item_id": "i"})
    job = await repo.enqueue(kind, {"item_id": "i", "kind": "NEW_ITEM"}, "failed")
    job.update(status=status, last_error="TimeoutError")
    await repo.put("ops.jobs", job["job_id"], job)
    await runtime.reconcile_delivery_failures()
    await runtime.terminal_delivery_failure(job, "TimeoutError")
    rows = await notices(repo)
    assert len(rows) == 1
    assert rows[0]["stage"] == ("PROCESSING_EXPIRED" if status == "EXPIRED" else "PROCESSING_FAILED")
    assert kind in rows[0]["reason"]


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_outbox_is_transactional_and_concurrent_retries_create_one_notice(backend, postgres_template):
    async with fixture_repo(backend, postgres_template) as repo:
        at = iso(utcnow() - timedelta(seconds=1))
        await repo.put(
            "ingest.source_items",
            "i",
            {
                "item_id": "i",
                "source_id": "S01",
                "feed": "fixture",
                "external_id": "i",
                "text": "Material",
                "observed_at": at,
                "published_prec": "COLLECTION_ONLY",
                "raw_ref": "/fixture",
                "raw_sha256": "a" * 64,
                "content_hash": "b" * 64,
                "source_class": "PRIMARY",
                "relevance": {"decision": "ALWAYS"},
                "extraction_status": "PENDING",
            },
        )
        settings = delivery_settings()
        with pytest.raises(ValueError, match="rollback"):
            async with repo.transaction():
                await item_progress(repo, settings, "i", "RECEIVED")
                raise ValueError("rollback")
        assert await notices(repo) == []
        assert await repo.list("ops.jobs") == []
        await asyncio.gather(*(item_progress(repo, settings, "i", "RECEIVED") for _ in range(4)))
        assert len(await notices(repo)) == 1
        assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1


@pytest.mark.parametrize("flag,replay", [(False, False), (True, True)])
async def test_disabled_or_replay_does_not_create_live_ops_notices(flag, replay):
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "i", {"item_id": "i"})
    await item_progress(repo, SimpleNamespace(ops_progress_enabled=flag, replay=replay), "i", "RECEIVED")
    assert await notices(repo) == [] and await repo.list("ops.jobs") == []

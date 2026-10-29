import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from test_session_binding_concurrency import fixture_repo

from tank.delivery import DeliveryDeferred, DeliveryError, DeliveryService
from tank.ops_cards import render, snapshot
from tank.ops_progress import item_progress
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import sha256, utcnow


@pytest.fixture
def settings():
    return SimpleNamespace(
        ops_progress_enabled=True,
        ops_progress_delay_s=0,
        telegram_token="test",
        telegram_ops_chat="-100123",
        telegram_alerts_chat="-100456",
    )


async def source(repo, **updates):
    item = {
        "item_id": "ITM-private",
        "source_id": "S16",
        "source_name": "Robinhood",
        "feed": "press",
        "external_id": "news",
        "title": "Robinhood chooses DIFC as regional headquarters to drive Middle East expansion",
        "text": "Robinhood has received authorization from the DFSA to introduce its retail investing platform in the Middle East. A long quotation that should not occupy the entire message.",
        "url": "https://robinhood.com/newsroom/robinhood-chooses-difc-regional-headquarters-middle-east-expansion",
        "source_class": "PRIMARY",
        "published_prec": "COLLECTION_ONLY",
        "raw_ref": "/test/evidence",
        "raw_sha256": "a" * 64,
        "content_hash": "b" * 64,
        "extraction_status": "PENDING",
        "relevance": {"decision": "ALWAYS"},
        **collection_clock().fields("observed_at"),
        **updates,
    }
    await repo.put("ingest.source_items", item["item_id"], item)
    return item


async def analysis(repo, settings, *, klass=None, completed=True):
    item = await source(repo, extraction_status="DONE")
    event = {
        "event_id": "EVT-private",
        "statement": "Robinhood получила разрешение DFSA для запуска инвестиционной платформы на Ближнем Востоке.",
        "lifecycle": "ANALYZING",
    }
    if klass:
        event["current_ems_id"] = "assessment"
        await repo.put(
            "evt.ems_assessments",
            "assessment",
            {"ems_id": "assessment", "event_id": event["event_id"], "class": klass, "class_reasons": []},
        )
    await repo.put("evt.events", event["event_id"], event)
    for stage in ("RECEIVED", "JEV_BYPASSED", "AGENT_QUEUED", "AGENT_ACCEPTED", "EVENT_RECORDED"):
        await item_progress(repo, settings, item["item_id"], stage, event_id=event["event_id"])
    if klass:
        await item_progress(repo, settings, item["item_id"], "ASSESSED", event_id=event["event_id"])
    if completed:
        await item_progress(repo, settings, item["item_id"], "AGENT_COMPLETED", event_id=event["event_id"])
    return item, event


async def test_robinhood_completion_without_assessment_reports_the_missing_result(settings):
    repo = MemoryRepository()
    item, _ = await analysis(repo, settings)
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "INCOMPLETE"
    assert "Анализ не завершён" in card["text"]
    assert "Не выполнено: оценка значимости события" in card["text"]
    assert "Alerts: публикация не запрошена" in card["text"]
    assert "Robinhood получила разрешение DFSA" in card["text"]
    for unwanted in (
        "это не подтверждение",
        "по политике источника",
        "Этап зафиксирован",
        "ITM-private",
        "EVT-private",
        "A long quotation",
    ):
        assert unwanted not in card["text"]
    assert len(card["text"]) < 800


@pytest.mark.parametrize("klass", ["LOW", "MATERIAL"])
async def test_completion_preserves_the_actual_assessment_outcome(settings, klass):
    repo = MemoryRepository()
    item, _ = await analysis(repo, settings, klass=klass)
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "BELOW_MAJOR"
    assert "Ниже порога Alerts" in card["text"] and klass in card["text"]
    assert "не отправлено — ниже порога MAJOR" in card["text"]
    assert "завершил обработку" not in card["text"]


async def test_completed_major_analysis_names_missing_decisions(settings):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR")
    await repo.put(
        "evt.beneficiary_maps",
        "map",
        {"event_id": event["event_id"], "map_version": 1, "analysis_set": ["robinhood:HOOD"]},
    )
    await repo.put("ana.market_truth", "truth", {"event_id": event["event_id"], "token_id": "robinhood:HOOD"})
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "INCOMPLETE"
    assert "данные: 1/1 токенов; решения: 0/1" in card["text"]
    assert "Не выполнено: решение по токенам: HOOD" in card["text"]
    assert "robinhood:HOOD" not in card["text"]


async def test_running_major_analysis_shows_current_work_instead_of_completion(settings):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR", completed=False)
    await repo.put(
        "evt.beneficiary_maps",
        "map",
        {"event_id": event["event_id"], "map_version": 1, "analysis_set": ["world:WLD"]},
    )
    card = render(await snapshot(repo, item["item_id"]))
    assert card["status"] == "Ожидает рыночного анализа"
    assert "данные: 0/1 токенов; решения: 0/1" in card["text"]


async def test_legacy_no_observation_row_is_missing_agent_decision_not_rejection(settings):
    repo = MemoryRepository()
    item = await source(
        repo, extraction_status="SKIPPED", extraction_reason="NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION"
    )
    await item_progress(
        repo, settings, item["item_id"], "AGENT_REJECTED", "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION"
    )
    card = render(await snapshot(repo, item["item_id"]))
    assert card["status"] == "Агент не оставил решения"
    assert "Агент не записал ни событие, ни решение об отклонении" in card["text"]
    assert "Отклонено агентом" not in card["text"]


async def test_legacy_certainty_reason_is_not_presented_as_a_rejection_explanation(settings):
    repo = MemoryRepository()
    item = await source(repo, extraction_status="SKIPPED", extraction_reason="Официальный источник")
    await item_progress(repo, settings, item["item_id"], "AGENT_REJECTED", "Официальный источник")
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "AGENT_REJECTED"
    assert "Причина отказа не записана" in card["text"]
    assert "Официальный источник" not in card["text"]


async def test_jev_rejection_shows_score_and_original_threshold(settings):
    repo = MemoryRepository()
    item = await source(
        repo, relevance={"decision": "SKIP", "p_relevant": 0.12, "cutoff": 0.7}, extraction_status="SKIPPED"
    )
    await item_progress(repo, settings, item["item_id"], "JEV_REJECTED")
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "JEV_REJECTED"
    assert "релевантность 12%, порог 70%" in card["text"]
    assert "Агент: ещё не получил материал" in card["text"]


async def test_instrument_change_is_readable_without_raw_json(settings):
    repo = MemoryRepository()
    before = {"venue": "okx", "symbol": "XGS-USDT", "market_type": "SPOT", "status": "preopen"}
    after = {**before, "status": "live"}
    item = await source(
        repo,
        source_id="S14",
        source_name="Venue instrument lists",
        title="XGS-USDT instrument change",
        text=json.dumps({"before": before, "after": after}),
    )
    await item_progress(repo, settings, item["item_id"], "RECEIVED")
    card = render(await snapshot(repo, item["item_id"]))
    assert "OKX · XGS-USDT · спот" in card["text"]
    assert "статус: до открытия торгов → торги открыты" in card["text"]
    assert not any(value in card["text"] for value in ('{"', '"before"', '"after"', '"status"'))


async def test_snapshot_markdown_is_cleaned_and_title_is_not_repeated(settings):
    repo = MemoryRepository()
    title = "[ARFC] Onboard mWIN to Aave Horizon"
    item = await source(
        repo,
        title=title,
        source_name="Snapshot",
        text=f"**Title:** {title} **Author:** AaveLabs **Date:** 2026-10-22 ## Summary This proposal adds mWIN as collateral in Aave Horizon. "
        + "Motivation and source text. " * 100,
    )
    await item_progress(repo, settings, item["item_id"], "RECEIVED")
    card = render(await snapshot(repo, item["item_id"]))
    assert card["text"].count(title) == 1
    assert "This proposal adds mWIN as collateral in Aave Horizon." in card["text"]
    assert "**" not in card["text"] and "##" not in card["text"]
    assert "Motivation and source text" not in card["text"]


async def test_delivery_receipt_beats_later_generic_completion(settings):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR")
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": event["event_id"],
            **collection_clock().fields("generated_at"),
        },
        immutable=True,
    )
    await repo.put(
        "out.deliveries",
        "sent",
        {
            "alert_id": "a",
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "message_ref": "42",
            "chat_id": "-100456",
            **collection_clock().fields("at"),
        },
        immutable=True,
    )
    await item_progress(repo, settings, item["item_id"], "AGENT_COMPLETED", key="later")
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "ALERT_SENT"
    assert "Alerts: доставлено" in card["text"]
    assert "https://t.me/c/456/42" in card["html"]


async def test_publication_refusal_contains_exact_missing_analysis(settings):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR")
    await item_progress(
        repo,
        settings,
        item["item_id"],
        "PUBLICATION_BLOCKED",
        "ANALYSIS_INCOMPLETE",
        event_id=event["event_id"],
        details={"missing_market_truth": ["world:WLD"], "missing_decisions": ["world:WLD"]},
    )
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "PUBLICATION_BLOCKED"
    assert "рыночные данные: WLD" in card["text"] and "решения: WLD" in card["text"]


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_burst_is_one_message_and_later_change_edits_it_across_restart(
    settings, monkeypatch, backend, postgres_template
):
    async with fixture_repo(backend, postgres_template) as repo:
        item = await source(repo)
        for index in range(20):
            await item_progress(repo, settings, item["item_id"], "AGENT_ACCEPTED", key=str(index))
        jobs = await repo.list("ops.jobs", filters={"type": "DELIVER"})
        requests = []
        clock = utcnow() + timedelta(minutes=1)
        monkeypatch.setattr("tank.delivery.utcnow", lambda: clock)

        def respond(request):
            requests.append((request.url.path.rsplit("/", 1)[-1], json.loads(request.content)))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 77}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = DeliveryService(repo, settings, http=http)
            await asyncio.gather(*(service.deliver(job["payload"]) for job in reversed(jobs)))
            assert len(requests) == 1 and requests[0][0] == "sendMessage"
            assert requests[0][1]["disable_notification"] is True
            item.update(
                extraction_status="SKIPPED",
                extraction_decision="NO_EVENT",
                extraction_reason="Материал повторяет ранее опубликованную новость",
            )
            await repo.put("ingest.source_items", item["item_id"], item)
            await item_progress(repo, settings, item["item_id"], "AGENT_REJECTED", item["extraction_reason"])
            newer = (await repo.list("ops.jobs", filters={"type": "DELIVER"}))[-1]
            clock += timedelta(seconds=10)
            service = DeliveryService(repo, settings, http=http)
            await service.deliver(newer["payload"])
            await asyncio.gather(*(service.deliver(job["payload"]) for job in jobs))
        assert len(requests) == 2
        assert requests[1][0] == "editMessageText" and requests[1][1]["message_id"] == 77
        assert "Отклонено агентом" in requests[1][1]["text"]
        assert len(await repo.list("out.ops_cards")) == 1


async def test_legacy_backlog_updates_existing_message_instead_of_replaying_stages(settings, monkeypatch):
    repo = MemoryRepository()
    item = await source(repo)
    for index, stage in enumerate(("RECEIVED", "JEV_BYPASSED", "AGENT_ACCEPTED")):
        await repo.put(
            "out.ops_notices",
            str(index),
            {
                "notice_id": str(index),
                "kind": "ITEM_PROGRESS",
                "subject_id": item["item_id"],
                "stage": stage,
                "text": "OLD RAW MESSAGE",
                "created_by": "legacy",
                **collection_clock(utcnow() - timedelta(minutes=5 - index)).fields("created_at"),
            },
            immutable=True,
        )
    await repo.put(
        "out.deliveries",
        "old-sent",
        {
            "notice_id": "0",
            "channel": "telegram:ops",
            "status": "SENT",
            "message_ref": "66",
            "chat_id": settings.telegram_ops_chat,
            **collection_clock(utcnow() - timedelta(minutes=4)).fields("at"),
        },
        immutable=True,
    )
    requests = []
    monkeypatch.setattr("tank.delivery.utcnow", lambda: utcnow() + timedelta(minutes=1))

    def respond(request):
        requests.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 66}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = DeliveryService(repo, settings, http=http)
        await service.deliver({"notice_id": "2"})
        await service.deliver({"notice_id": "1"})
    assert len(requests) == 1 and requests[0][0].endswith("/editMessageText")
    assert requests[0][1]["message_id"] == 66 and "OLD RAW MESSAGE" not in requests[0][1]["text"]


async def test_debounce_sends_no_intermediate_network_request(settings):
    repo = MemoryRepository()
    item = await source(repo)
    settings.ops_progress_delay_s = 15
    await item_progress(repo, settings, item["item_id"], "RECEIVED")
    notice = (await repo.list("out.ops_notices"))[0]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("no send before debounce"))
    ) as http:
        with pytest.raises(DeliveryDeferred):
            await DeliveryService(repo, settings, http=http).deliver({"notice_id": notice["notice_id"]})
    assert await repo.list("out.deliveries") == []


async def test_auxiliary_agent_evidence_stays_in_audit_without_spamming_ops(settings):
    repo = MemoryRepository()
    item = await source(repo, source_id="WEB", evidence_kind="FETCHED_PAGE")
    await item_progress(repo, settings, item["item_id"], "EVIDENCE_FETCHED")
    assert len(await repo.list("out.ops_notices")) == 1
    assert await repo.list("ops.jobs") == []


async def test_missing_legacy_message_does_not_create_a_new_spam_message(settings, monkeypatch):
    repo = MemoryRepository()
    item = await source(repo)
    await item_progress(repo, settings, item["item_id"], "RECEIVED")
    notice = (await repo.list("out.ops_notices"))[0]
    key = sha256({"item_id": item["item_id"], "chat_id": settings.telegram_ops_chat})
    await repo.put(
        "out.ops_cards",
        key,
        {"item_id": item["item_id"], "chat_id": settings.telegram_ops_chat, "message_ref": "88"},
    )
    requests = []
    monkeypatch.setattr("tank.delivery.utcnow", lambda: utcnow() + timedelta(minutes=1))

    def respond(request):
        requests.append(request.url.path)
        return httpx.Response(
            400, json={"ok": False, "description": "Bad Request: message to edit not found"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = DeliveryService(repo, settings, http=http)
        with pytest.raises(DeliveryError):
            await service.deliver({"notice_id": notice["notice_id"]})
        await service.deliver({"notice_id": notice["notice_id"]})
    assert len(requests) == 1 and requests[0].endswith("/editMessageText")
    assert (await repo.get("out.ops_cards", key))["delivery_status"] == "FAILED"


async def test_unicode_markdown_and_source_html_fit_telegram_without_leaking_markup(settings):
    repo = MemoryRepository()
    item = await source(repo, title="**A <b>title</b> & text**", text="😀" * 5000)
    await item_progress(repo, settings, item["item_id"], "RECEIVED")
    card = render(await snapshot(repo, item["item_id"]))
    assert "**" not in card["html"] and "&amp;" in card["html"]
    assert len(card["text"].encode("utf-16-le")) <= 4096 * 2


async def test_latest_refused_update_is_not_presented_as_a_new_success(settings):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR")
    old = utcnow() - timedelta(minutes=10)
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": event["event_id"],
            **collection_clock(old).fields("generated_at"),
        },
        immutable=True,
    )
    await repo.put(
        "out.deliveries",
        "sent",
        {
            "alert_id": "a",
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "message_ref": "42",
            "chat_id": "-100456",
            **collection_clock(old).fields("at"),
        },
        immutable=True,
    )
    await item_progress(
        repo,
        settings,
        item["item_id"],
        "PUBLICATION_BLOCKED",
        "NO_MATERIAL_CHANGE",
        event_id=event["event_id"],
    )
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "PUBLICATION_BLOCKED"
    assert "Обновление не опубликовано" in card["text"]
    assert "Нет существенных изменений" in card["text"]
    assert "Предыдущая карточка в Alerts" in card["text"]


async def test_final_class_cap_is_explained_before_positive_agent_rationales(settings):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="LOW")
    await repo.put(
        "evt.ems_assessments",
        "assessment",
        {
            "ems_id": "assessment",
            "event_id": event["event_id"],
            "class": "LOW",
            "class_reasons": ["Large licensed platform", "New region", "NO_UNIVERSE_CONCERN"],
        },
    )
    card = render(await snapshot(repo, item["item_id"]))
    assert "Событие не затрагивает токены из реестра Tank" in card["text"]
    assert "NO_UNIVERSE_CONCERN" not in card["text"]
    assert "Для публикации требуется" not in card["text"]


async def test_rate_limit_defers_other_items_without_extra_http_attempts(settings):
    repo = MemoryRepository()
    first = await source(repo)
    second = await source(repo, item_id="second", external_id="second")
    for item in (first, second):
        await item_progress(repo, settings, item["item_id"], "RECEIVED")
    notices = await repo.list("out.ops_notices")
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 30}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = DeliveryService(repo, settings, http=http)
        with pytest.raises(DeliveryError):
            await service.deliver({"notice_id": notices[0]["notice_id"]})
        with pytest.raises(DeliveryDeferred):
            await service.deliver({"notice_id": notices[1]["notice_id"]})
    assert len(requests) == 1
    assert not await repo.list("out.deliveries", filters={"notice_id": notices[1]["notice_id"]})


async def test_edit_not_modified_is_successful_recovery_without_another_message(settings):
    repo = MemoryRepository()
    item = await source(repo)
    await item_progress(repo, settings, item["item_id"], "RECEIVED")
    notice = (await repo.list("out.ops_notices"))[0]
    key = sha256({"item_id": item["item_id"], "chat_id": settings.telegram_ops_chat})
    await repo.put(
        "out.ops_cards",
        key,
        {"item_id": item["item_id"], "chat_id": settings.telegram_ops_chat, "message_ref": "88"},
    )
    requests = []

    def respond(request):
        requests.append(request.url.path)
        return httpx.Response(400, json={"ok": False, "description": "Bad Request: message is not modified"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = DeliveryService(repo, settings, http=http)
        await service.deliver({"notice_id": notice["notice_id"]})
        await service.deliver({"notice_id": notice["notice_id"]})
    assert len(requests) == 1 and requests[0].endswith("/editMessageText")
    assert (await repo.get("out.ops_cards", key))["delivery_status"] == "SENT"


@pytest.mark.parametrize("delivered", [False, True])
async def test_previous_alert_does_not_hide_new_incomplete_analysis(settings, delivered):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR")
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": event["event_id"],
            **collection_clock(utcnow() - timedelta(minutes=10)).fields("generated_at"),
        },
        immutable=True,
    )
    if delivered:
        await repo.put(
            "out.deliveries",
            "sent",
            {
                "alert_id": "a",
                "version": 1,
                "channel": "telegram:alerts",
                "status": "SENT",
                "message_ref": "42",
                "chat_id": "-100456",
                **collection_clock().fields("at"),
            },
            immutable=True,
        )
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "INCOMPLETE"
    assert "Не выполнено: определение затронутых токенов" in card["text"]


async def test_new_assessment_replaces_an_old_publication_refusal(settings):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR", completed=False)
    await item_progress(
        repo, settings, item["item_id"], "PUBLICATION_BLOCKED", "BELOW_MAJOR", event_id=event["event_id"]
    )
    await repo.put(
        "evt.ems_assessments",
        "assessment",
        {
            "ems_id": "assessment",
            "event_id": event["event_id"],
            "class": "MAJOR",
            **collection_clock().fields("cutoff"),
        },
    )
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == "ANALYZING"
    assert card["status"] == "Ожидает определения затронутых токенов"
    assert "Значимость ниже порога" not in card["text"]


async def test_followup_job_is_ready_without_another_initial_batch_delay(settings):
    repo = MemoryRepository()
    settings.ops_progress_delay_s = 15
    item = await source(repo)
    key = sha256({"item_id": item["item_id"], "chat_id": settings.telegram_ops_chat})
    await repo.put(
        "out.ops_cards",
        key,
        {"item_id": item["item_id"], "chat_id": settings.telegram_ops_chat, "message_ref": "77"},
    )
    await item_progress(repo, settings, item["item_id"], "AGENT_ACCEPTED")
    job = (await repo.list("ops.jobs", filters={"type": "DELIVER"}))[0]
    notice = (await repo.list("out.ops_notices"))[0]
    assert job["run_after"] == notice["created_at"]


@pytest.mark.parametrize("update_kind", ["notice", "artifact"])
async def test_later_analysis_clears_an_obsolete_missing_data_refusal(settings, update_kind):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR", completed=False)
    await item_progress(
        repo,
        settings,
        item["item_id"],
        "PUBLICATION_BLOCKED",
        "ANALYSIS_INCOMPLETE",
        event_id=event["event_id"],
        details={"missing_market_truth": ["world:WLD"], "missing_decisions": ["world:WLD"]},
    )
    stamp = collection_clock().fields("cutoff") if update_kind == "artifact" else {}
    await repo.put(
        "evt.beneficiary_maps",
        "map",
        {"event_id": event["event_id"], "map_version": 1, "analysis_set": ["world:WLD"], **stamp},
    )
    await repo.put(
        "ana.market_truth", "truth", {"event_id": event["event_id"], "token_id": "world:WLD", **stamp}
    )
    await repo.put(
        "ana.decisions", "decision", {"event_id": event["event_id"], "token_id": "world:WLD", **stamp}
    )
    if update_kind == "notice":
        await item_progress(repo, settings, item["item_id"], "ANALYSIS_UPDATED", event_id=event["event_id"])
    card = render(await snapshot(repo, item["item_id"]))
    assert card["status"] == "Ожидает решения о публикации"
    assert "данные: 1/1 токенов; решения: 1/1" in card["text"]
    assert "Не завершён анализ" not in card["text"]


@pytest.mark.parametrize("same_alert_id", [False, True])
@pytest.mark.parametrize("failed", [False, True])
async def test_latest_delivery_state_keeps_link_to_earlier_success(settings, same_alert_id, failed):
    repo = MemoryRepository()
    item, event = await analysis(repo, settings, klass="MAJOR")
    old = utcnow() - timedelta(minutes=10)
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": event["event_id"],
            **collection_clock(old).fields("generated_at"),
        },
        immutable=True,
    )
    await repo.put(
        "out.deliveries",
        "sent",
        {
            "alert_id": "a",
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "message_ref": "42",
            "chat_id": "-100456",
            **collection_clock(old).fields("at"),
        },
        immutable=True,
    )
    latest = "a" if same_alert_id else "b"
    await repo.put(
        "out.alerts",
        latest + ":2",
        {
            "alert_id": latest,
            "version": 2,
            "event_id": event["event_id"],
            **collection_clock().fields("generated_at"),
        },
        immutable=True,
    )
    if failed:
        await repo.put(
            "out.deliveries",
            "failed",
            {
                "alert_id": latest,
                "version": 2,
                "channel": "telegram:alerts",
                "status": "FAILED",
                "terminal": True,
                "attempt": 5,
                **collection_clock().fields("at"),
            },
            immutable=True,
        )
    card = render(await snapshot(repo, item["item_id"]))
    assert card["code"] == ("ALERT_FAILED" if failed else "ALERT_QUEUED")
    assert "Предыдущая карточка в Alerts" in card["text"]
    assert "https://t.me/c/456/42" in card["html"]


async def test_historical_jev_scores_are_not_relabelled_as_decision_1(settings):
    repo = MemoryRepository()
    item = await source(
        repo,
        relevance={
            "decision": "SKIP",
            "p_relevant": 0.12,
            "cutoff": 0.7,
            "model": "jev-1.13",
            "semantic_model_id": "jev-1.13.0",
        },
        extraction_status="SKIPPED",
    )
    await item_progress(repo, settings, item["item_id"], "JEV_REJECTED")
    card = render(await snapshot(repo, item["item_id"]))
    assert "Decision-1" not in card["text"]
    assert "релевантность 12%, порог 70%" in card["text"]

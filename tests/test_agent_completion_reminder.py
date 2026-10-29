"""An agent turn that ends without the source item's outcome is reminded, never silently skipped."""

import copy
import json
from types import SimpleNamespace

import httpx

from tank.agents.context import ModelContext
from tank.agents.sessions import SessionService
from tank.ops_cards import render, snapshot
from tank.ops_progress import item_progress, session_progress
from tank.settings import Settings
from tank.storage import MemoryRepository

AT = "2026-10-27T10:00:00Z"
REAL = Settings(_env_file=None)
BODY = "Full article body that is already in the session. " * 200


def settings(limit=20):
    return SimpleNamespace(
        openma_url="http://native",
        openma_api_key="fake",
        replay=False,
        config_version="test",
        code_version="test",
        ops_progress_enabled=True,
        ops_progress_delay_s=0,
        telegram_token="test",
        telegram_ops_chat="-100123",
        telegram_alerts_chat="-100456",
        read_config_json=REAL.read_config_json,
        load_config=lambda name: (
            {"agent_completion_reminders_per_session": limit}
            if name == "thresholds"
            else REAL.load_config(name)
            if name in {"registry", "sources"}
            else {}
        ),
    )


def message(key, payload, status="DONE"):
    return {"message_id": key, "payload": payload, "turn_status": status, "sent_at": AT, "retry_count": 0}


async def seed(repo, items=("i",), kind="NEW_ITEM"):
    for item_id in items:
        await repo.put(
            "ingest.source_items",
            item_id,
            {
                "item_id": item_id,
                "source_id": "S01",
                "title": "Exchange lists a token",
                "text": BODY,
                "url": "https://example.test/listing",
                "observed_at": AT,
                "relevance": {"decision": "ALWAYS"},
                "extraction_status": "PENDING",
            },
        )
    session = {
        "session_id": "s",
        "status": "OPEN",
        "message_log": [
            message("m-" + item_id, {"kind": kind, "item_id": item_id, "session_id": "s"})
            for item_id in items
        ],
    }
    await repo.put("ops.agent_sessions", "s", session)
    return session


async def service(repo, config):
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500)))
    return SessionService(repo, config, lambda *args: None, http=http)


async def reminders(repo):
    jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    return sorted(
        (job for job in jobs if job["payload"]["kind"] == "COMPLETION_REMINDER"),
        key=lambda job: job["payload"]["reminder"],
    )


async def settle(repo, sessions):
    session = await repo.get("ops.agent_sessions", "s")
    await sessions.remind_incomplete(session)
    await repo.put("ops.agent_sessions", "s", session)
    return session


async def deliver(repo, job, status="DONE"):
    """The reminder job arrives in the session as its own message, as send_message records it."""
    session = await repo.get("ops.agent_sessions", "s")
    session["message_log"].append(message("r-" + job["idem_key"], copy.deepcopy(job["payload"]), status))
    await repo.put("ops.agent_sessions", "s", session)


async def test_missing_outcome_reminds_same_session_with_format_and_refusals():
    repo = MemoryRepository()
    config = settings()
    await seed(repo)
    await repo.put(
        "ops.tool_calls",
        "s:call",
        {
            "session_id": "s",
            "call_id": "call",
            "tool": "record_event",
            "status": "REFUSED",
            "native_turn_message_id": "m-i",
            "native_turn_call_index": 0,
            "result": {"ok": False, "error": {"reason": "$.event_time.quote: required field missing"}},
        },
    )
    sessions = await service(repo, config)
    session = await settle(repo, sessions)
    [job] = await reminders(repo)
    assert job["payload"] == {
        "kind": "COMPLETION_REMINDER",
        "session_id": "s",
        "item_id": "i",
        "reminded_kind": "NEW_ITEM",
        "attempt": 1,
        "reminder": 1,
        "reminder_limit": 20,
        "turn_message_id": "m-i",
    }
    assert session["completion_reminders_sent"] == 1
    item = await repo.get("ingest.source_items", "i")
    assert (item["extraction_status"], item["extraction_reason"]) == ("PENDING", "AGENT_REMINDED")
    content = await ModelContext(repo, config, session).message(job["payload"], item)
    text = content["reminder"]
    assert "TASK NOT COMPLETE (reminder 1 of 20" in text
    assert '"is_event": false' in text and '"no_event_reason": "<non-empty string>"' in text
    assert set(content) == {"message", "source", "reminder"}
    assert content["source"] == {
        "title": "Exchange lists a token",
        "source": "SEC · S01",
        "url": "https://example.test/listing",
        "observed_at": AT,
    }
    assert "Full article body" not in json.dumps(content, ensure_ascii=False)
    assert len(json.dumps(content, ensure_ascii=False)) < 6000
    assert '"is_event": true' in text and '"checked_sources": ["<string>"]' in text
    assert "merge_into arguments" in text
    assert "record_event: $.event_time.quote: required field missing" in text
    assert "never blocks recording" in text
    assert "item_id" not in content["message"]
    await sessions.close()


async def test_item_added_reminder_names_add_observation():
    repo = MemoryRepository()
    config = settings()
    await seed(repo, kind="ITEM_ADDED")
    sessions = await service(repo, config)
    session = await settle(repo, sessions)
    [job] = await reminders(repo)
    assert job["payload"]["reminded_kind"] == "ITEM_ADDED"
    item = await repo.get("ingest.source_items", "i")
    content = await ModelContext(repo, config, session).message(job["payload"], item)
    text = content["reminder"]
    assert "Full article body" not in json.dumps(content, ensure_ascii=False)
    assert len(json.dumps(content, ensure_ascii=False)) < 6000
    assert '"is_event": true' in text
    assert 'add_observation arguments: {"link": "<CORROBORATION|STATE_UPDATE|DENIAL|CONTRADICTION>"' in text
    assert "merge_into" not in text
    await sessions.close()


async def test_outcome_after_reminder_completes_normally():
    repo = MemoryRepository()
    config = settings()
    await seed(repo)
    sessions = await service(repo, config)
    await settle(repo, sessions)
    [job] = await reminders(repo)
    await deliver(repo, job)
    item = await repo.get("ingest.source_items", "i")
    item["extraction_status"] = "DONE"
    await repo.put("ingest.source_items", "i", item)
    session = await settle(repo, sessions)
    assert len(await reminders(repo)) == 1
    assert (await repo.get("ingest.source_items", "i"))["extraction_status"] == "DONE"
    assert all(row["completion_checked"] for row in session["message_log"])
    await session_progress(repo, config, session)
    stages = {row["stage"] for row in await repo.list("out.ops_notices")}
    assert "AGENT_COMPLETED" in stages and "AGENT_REJECTED" not in stages
    await sessions.close()


async def test_reminder_turn_without_outcome_is_another_attempt():
    repo = MemoryRepository()
    config = settings()
    await seed(repo)
    sessions = await service(repo, config)
    await settle(repo, sessions)
    [first] = await reminders(repo)
    await deliver(repo, first)
    session = await settle(repo, sessions)
    second = (await reminders(repo))[1]["payload"]
    assert (second["attempt"], second["reminder"], second["reminded_kind"]) == (2, 2, "NEW_ITEM")
    assert second["turn_message_id"] == "r-" + first["idem_key"]
    assert session["completion_reminder_attempts"] == {"i": 2}
    await sessions.close()


async def test_session_budget_spans_items_then_fails_explicitly():
    repo = MemoryRepository()
    config = settings(limit=3)
    await seed(repo, items=("a", "b"))
    sessions = await service(repo, config)
    await settle(repo, sessions)
    for job in await reminders(repo):
        await deliver(repo, job)
    await settle(repo, sessions)
    assert [job["payload"]["reminder"] for job in await reminders(repo)] == [1, 2, 3]
    for job in (await reminders(repo))[2:]:
        await deliver(repo, job)
    session = await settle(repo, sessions)
    assert session["completion_reminders_sent"] == 3
    assert len(await reminders(repo)) == 3
    failed = [await repo.get("ingest.source_items", key) for key in ("a", "b")]
    assert {
        (row["extraction_status"], row["extraction_reason"], row["extraction_failed_session_id"])
        for row in failed
    } == {("FAILED", "AGENT_INCOMPLETE", "s")}
    assert session["agent_incomplete"] is True
    card = render(await snapshot(repo, "a"))
    assert card["code"] == "PROCESSING_FAILED"
    assert card["status"] == "Ошибка обработки"
    assert "3/3" in card["text"] and "Отклонено агентом" not in card["text"]
    await session_progress(repo, config, session)
    await session_progress(repo, config, session)
    failures = [
        (row["subject_id"], row["reason"])
        for row in await repo.list("out.ops_notices")
        if row["stage"] == "PROCESSING_FAILED"
    ]
    assert sorted(failures) == [("a", "AGENT_INCOMPLETE"), ("b", "AGENT_INCOMPLETE")]
    card = render(await snapshot(repo, "a"))
    assert card["code"] == "PROCESSING_FAILED" and "повторный анализ" in card["text"]
    await sessions.close()


async def test_default_budget_is_twenty_per_session():
    repo = MemoryRepository()
    config = settings()
    config.load_config = lambda name: REAL.load_config(name) if name == "thresholds" else {}
    await seed(repo)
    session = await repo.get("ops.agent_sessions", "s")
    session["completion_reminders_sent"] = 20
    await repo.put("ops.agent_sessions", "s", session)
    sessions = await service(repo, config)
    await settle(repo, sessions)
    assert not await reminders(repo)
    assert (await repo.get("ingest.source_items", "i"))["extraction_reason"] == "AGENT_INCOMPLETE"
    await sessions.close()


async def test_restart_before_session_write_does_not_duplicate_reminder():
    repo = MemoryRepository()
    config = settings()
    await seed(repo)
    sessions = await service(repo, config)
    lost = await repo.get("ops.agent_sessions", "s")
    await sessions.remind_incomplete(lost)
    item = await repo.get("ingest.source_items", "i")
    item["extraction_status"] = "PENDING"
    await repo.put("ingest.source_items", "i", item)
    restarted = await service(repo, config)
    session = await settle(repo, restarted)
    await settle(repo, restarted)
    assert len(await reminders(repo)) == 1
    assert session["completion_reminders_sent"] == 1
    await sessions.close()
    await restarted.close()


async def test_ops_shows_reminder_stage_with_session_counter():
    repo = MemoryRepository()
    config = settings()
    await seed(repo, items=("a", "b"))
    sessions = await service(repo, config)
    session = await settle(repo, sessions)
    await session_progress(repo, config, session)
    card = render(await snapshot(repo, "b"))
    assert card["code"] == "AGENT_REMINDED"
    assert card["status"] == "Агент не вернул результат, напоминание 2/20"
    notices = await repo.list("out.ops_notices", filters={"subject_id": "b"})
    assert {row["stage"] for row in notices} == {"AGENT_REMINDED"}
    assert notices[0]["reason"] == "Агент не вернул результат, напоминание 2/20"
    await sessions.close()


async def test_deliberate_no_event_still_renders_as_rejection():
    repo = MemoryRepository()
    config = settings()
    await seed(repo)
    item = await repo.get("ingest.source_items", "i")
    item.update(
        extraction_status="SKIPPED",
        extraction_decision="NO_EVENT",
        extraction_reason="Мнение без нового факта",
    )
    await repo.put("ingest.source_items", "i", item)
    await item_progress(repo, config, "i", "AGENT_REJECTED", "Мнение без нового факта")
    sessions = await service(repo, config)
    await settle(repo, sessions)
    assert not await reminders(repo)
    card = render(await snapshot(repo, "i"))
    assert card["code"] == "AGENT_REJECTED"
    assert card["status"] == "Отклонено агентом"
    assert "Мнение без нового факта" in card["text"]
    await sessions.close()

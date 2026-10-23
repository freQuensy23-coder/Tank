"""Durable, idempotent source processing notices for the operator channel."""

from datetime import timedelta

from tank.time_precision import collection_clock, offset_schedule
from tank.util import sha256

STAGES = {
    "ANALYSIS_UPDATED": "Обновлён результат анализа",
    "RECEIVED": "Получен материал",
    "EVIDENCE_FETCHED": "Агент получил дополнительный источник",
    "JEV_PASSED": "Фильтр Jev пройден",
    "JEV_REJECTED": "Остановлен фильтром Jev",
    "JEV_BYPASSED": "Фильтр Jev не требуется",
    "AGENT_QUEUED": "Ожидает передачи агенту",
    "AGENT_ACCEPTED": "Передан агенту",
    "EVENT_RECORDED": "Агент зарегистрировал событие",
    "OBSERVATION_ADDED": "Агент добавил материал к событию",
    "AGENT_REJECTED": "Агент не выделил событие",
    "ASSESSED": "Агент оценил значимость",
    "PUBLICATION_BLOCKED": "Карточка не допущена к публикации",
    "ALERT_QUEUED": "Карточка подготовлена, ожидает отправки в Alerts",
    "ALERT_SENT": "Карточка доставлена в Alerts",
    "ALERT_FAILED": "Карточка не доставлена в Alerts",
    "AGENT_COMPLETED": "Агент завершил обработку материала",
    "PROCESSING_FAILED": "Ошибка обработки",
    "PROCESSING_EXPIRED": "Истёк срок обработки",
}


def enabled(settings):
    return bool(getattr(settings, "ops_progress_enabled", False)) and not getattr(settings, "replay", False)


def clipped(value, units):
    text = " ".join(str(value or "").split())
    encoded = text.encode("utf-16-le")
    return (
        text if len(encoded) <= units * 2 else encoded[: (units - 1) * 2].decode("utf-16-le", "ignore") + "…"
    )


async def item_progress(repo, settings, item_id, stage, reason="", *, event_id=None, key=None, details=None):
    if not enabled(settings) or not item_id:
        return
    item = await repo.get("ingest.source_items", item_id)
    if item is None:
        return
    identity = {
        "item_id": item_id,
        "stage": stage,
        "event_id": None if key else event_id,
        "key": key or reason,
    }
    notice_id = "progress_" + sha256(identity)
    async with repo.lock("ops-progress:" + notice_id), repo.transaction():
        if await repo.get("out.ops_notices", notice_id):
            return
        stamp = collection_clock()
        await repo.put(
            "out.ops_notices",
            notice_id,
            {
                "notice_id": notice_id,
                "kind": "ITEM_PROGRESS",
                "subject_id": item_id,
                "stage": stage,
                "event_id": event_id,
                "reason": reason,
                "text": STAGES[stage],
                "format_version": 2,
                "details": details or {},
                **stamp.fields("created_at"),
                "created_by": "ops-progress",
            },
            immutable=True,
        )
        if item.get("source_id") == "WEB" and item.get("evidence_kind"):
            return
        await repo.enqueue(
            "DELIVER",
            {"notice_id": notice_id, "channel": "telegram:ops"},
            notice_id,
            priority=2,
            run_after=offset_schedule(
                stamp, timedelta(seconds=getattr(settings, "ops_progress_delay_s", 15))
            ),
            max_attempts=5,
            expires_at=offset_schedule(stamp, timedelta(days=7)),
        )


async def event_progress(repo, settings, event_id, stage, reason="", *, key=None, details=None):
    if not enabled(settings) or not event_id:
        return
    items = {
        row["item_id"]
        async for row in repo.iter_metadata(
            "evt.event_observations", ["item_id"], filters={"event_id": event_id}
        )
    }
    for item_id in sorted(items):
        await item_progress(
            repo, settings, item_id, stage, reason, event_id=event_id, key=key, details=details
        )


async def tool_progress(repo, settings, tool, arguments, context, result):
    if not enabled(settings) or not isinstance(result, dict):
        return
    event_id = arguments.get("event_id") or context.get("event_id")
    if tool == "publish_card":
        published = result.get("published")
        reason = result.get("reason") or "UNKNOWN"
        if published:
            reason += f"; карточка {result['alert_id']} v{result['version']}"
            if result.get("delivery_not_before"):
                reason += f"; отправка не раньше {result['delivery_not_before']}"
        await event_progress(
            repo,
            settings,
            event_id,
            "ALERT_QUEUED" if published else "PUBLICATION_BLOCKED",
            reason,
            key=f"{result['alert_id']}:{result['version']}" if published else None,
            details={
                name: result[name]
                for name in ("missing_market_truth", "missing_decisions", "publication_receipt")
                if name in result
            },
        )
    elif tool in {"set_beneficiaries", "get_market_truth", "get_decision"}:
        await event_progress(
            repo, settings, event_id, "ANALYSIS_UPDATED", tool, key=context.get("call_id") or sha256(result)
        )


async def session_progress(repo, settings, session):
    if not enabled(settings):
        return
    for message in session.get("message_log", []):
        payload = message.get("payload") or {}
        if payload.get("kind") not in {"NEW_ITEM", "ITEM_ADDED"}:
            continue
        item_id = payload.get("item_id")
        key = f"{session['session_id']}:{message['message_id']}:{message.get('retry_count', 0)}"
        status = message.get("turn_status")
        if status == "FAILED":
            stage, reason = "PROCESSING_FAILED", "AGENT_EXECUTION_FAILED"
        elif status == "DONE":
            item = await repo.get("ingest.source_items", item_id)
            if item and item.get("extraction_status") == "SKIPPED":
                stage = "AGENT_REJECTED"
                reason = item.get("extraction_reason") or ""
                key = None
            else:
                stage, reason = "AGENT_COMPLETED", ""
        elif message.get("sent_at"):
            stage, reason = "AGENT_ACCEPTED", "Ожидается результат анализа"
        else:
            continue
        await item_progress(repo, settings, item_id, stage, reason, event_id=session.get("event_id"), key=key)


async def job_failure_progress(repo, settings, job, reason):
    if not enabled(settings):
        return
    kind = job["type"]
    if kind not in {"FILTER_ITEM", "ROUTE_ITEM", "AGENT_MESSAGE", "RUN_MARKET_TRUTH", "ESCALATE_TOKENS"}:
        return
    payload = job["payload"]
    if kind == "AGENT_MESSAGE" and payload.get("kind") == "USER_QUESTION":
        return
    stage = (
        "PROCESSING_EXPIRED" if job.get("status") == "EXPIRED" or reason == "EXPIRED" else "PROCESSING_FAILED"
    )
    detail = f"Этап {kind}; {reason}"
    if payload.get("item_id"):
        await item_progress(repo, settings, payload["item_id"], stage, detail, key=job["job_id"])
    else:
        await event_progress(repo, settings, payload.get("event_id"), stage, detail, key=job["job_id"])

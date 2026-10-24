"""Durable, idempotent source processing notices for the operator channel."""

from datetime import timedelta

from pydantic import JsonValue

from tank.contracts.progress import (
    ProgressDetails,
    ProgressIdentity,
    ProgressNotice,
    ProgressRepository,
    ProgressSettings,
    ProgressStage,
    ProgressSubject,
)
from tank.time_precision import collection_clock, offset_schedule
from tank.util import iso, sha256

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


def enabled(settings: ProgressSettings) -> bool:
    return settings.ops_progress_enabled and not settings.replay


def clipped(value: JsonValue, units: int) -> str:
    if units <= 0:
        raise ValueError("Text clipping requires a positive UTF-16 unit limit")
    text = " ".join(str(value if value is not None else "").split())
    encoded = text.encode("utf-16-le")
    return (
        text if len(encoded) <= units * 2 else encoded[: (units - 1) * 2].decode("utf-16-le", "ignore") + "…"
    )


async def item_progress(
    repo: ProgressRepository,
    settings: ProgressSettings,
    item_id: str,
    stage: ProgressStage,
    reason: str = "",
    *,
    event_id: str | None = None,
    key: str | None = None,
    details: ProgressDetails | None = None,
) -> None:
    if not enabled(settings):
        return
    if key is not None and not key.strip():
        raise ValueError("Progress deduplication key must be nonempty when supplied")
    identity = ProgressIdentity(
        item_id=item_id,
        stage=stage,
        event_id=None if key is not None else event_id,
        key=key if key is not None else reason,
    )
    record = await repo.get("ingest.source_items", item_id)
    if record is None:
        return
    subject = ProgressSubject.model_validate(
        {field: record[field] for field in ("item_id", "source_id", "evidence_kind") if field in record}
    )
    if subject.item_id != item_id:
        raise ValueError("Progress source identity conflicts with its stored subject")
    notice_id = "progress_" + sha256(identity.model_dump(mode="json"))
    async with repo.lock("ops-progress:" + notice_id), repo.transaction():
        if await repo.get("out.ops_notices", notice_id) is not None:
            return
        created_at = iso()
        stamp = collection_clock(created_at)
        notice = ProgressNotice(
            notice_id=notice_id,
            subject_id=item_id,
            stage=stage,
            event_id=event_id,
            reason=reason,
            text=STAGES[stage],
            details=details if details is not None else ProgressDetails(),
            created_at=created_at,
        )
        await repo.put("out.ops_notices", notice_id, notice.storage_record(), immutable=True)
        if subject.source_id == "WEB" and subject.evidence_kind is not None:
            return
        await repo.enqueue(
            "DELIVER",
            {"notice_id": notice_id, "channel": "telegram:ops"},
            notice_id,
            priority=2,
            run_after=stamp,
            max_attempts=5,
            expires_at=offset_schedule(stamp, timedelta(days=7)),
        )


class RepositorySourceProgress:
    def __init__(self, repo: ProgressRepository, settings: ProgressSettings) -> None:
        self.repo, self.settings = repo, settings

    async def record(
        self,
        item_id: str,
        stage: ProgressStage,
        reason: str = "",
        *,
        event_id: str | None = None,
        key: str | None = None,
    ) -> None:
        await item_progress(self.repo, self.settings, item_id, stage, reason, event_id=event_id, key=key)


class DisabledSourceProgress:
    """Explicit side-effect-free reporting port for a composition that disables notices."""

    async def record(
        self,
        item_id: str,
        stage: ProgressStage,
        reason: str = "",
        *,
        event_id: str | None = None,
        key: str | None = None,
    ) -> None:
        return None


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
            details=ProgressDetails.model_validate(
                {
                    name: result[name]
                    for name in ("missing_market_truth", "missing_decisions", "publication_receipt")
                    if name in result
                }
            ),
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

"""Durable, idempotent source processing notices for the operator channel."""

from collections.abc import Mapping
from datetime import timedelta

from pydantic import JsonValue

from tank.contracts.progress import (
    EventObservationSubject,
    EventProgressRepository,
    ExtractionProgressView,
    FailureProgressTarget,
    JobFailureProgressView,
    ProgressDetails,
    ProgressIdentity,
    ProgressNotice,
    ProgressRepository,
    ProgressSettings,
    ProgressStage,
    ProgressSubject,
    PublicationProgressResult,
    SessionProgressView,
    SourceMessageProgress,
    ToolProgressContext,
)
from tank.time_precision import collection_clock, offset_schedule
from tank.util import clock_iso, sha256

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
        created_at = clock_iso()
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


async def event_progress(
    repo: EventProgressRepository,
    settings: ProgressSettings,
    event_id: str | None,
    stage: ProgressStage,
    reason: str = "",
    *,
    key: str | None = None,
    details: ProgressDetails | None = None,
) -> None:
    if not enabled(settings) or event_id is None:
        return
    if not event_id or any(char.isspace() for char in event_id):
        raise ValueError("Event progress requires a nonempty event identity")
    items = {
        EventObservationSubject.model_validate(row).item_id
        async for row in repo.iter_metadata(
            "evt.event_observations", ["item_id"], filters={"event_id": event_id}
        )
    }
    for item_id in sorted(items):
        await item_progress(
            repo, settings, item_id, stage, reason, event_id=event_id, key=key, details=details
        )


async def tool_progress(
    repo: EventProgressRepository,
    settings: ProgressSettings,
    tool: str,
    arguments: Mapping[str, JsonValue],
    context: Mapping[str, JsonValue],
    result: JsonValue,
) -> None:
    if not enabled(settings) or tool not in {
        "publish_card",
        "set_beneficiaries",
        "get_market_truth",
        "get_decision",
    }:
        return
    if not isinstance(result, dict):
        raise ValueError("Reported tool results must be JSON objects")
    argument_context = ToolProgressContext.model_validate(
        {name: arguments[name] for name in ("event_id",) if name in arguments}
    )
    call_context = ToolProgressContext.model_validate(
        {name: context[name] for name in ("event_id", "call_id") if name in context}
    )
    event_id = argument_context.event_id
    if event_id is None:
        event_id = call_context.event_id
    if tool == "publish_card":
        detail = ProgressDetails.model_validate(
            {
                name: result[name]
                for name in ("missing_market_truth", "missing_decisions", "publication_receipt")
                if name in result
            }
        )
        output = PublicationProgressResult.model_validate(
            {
                **{
                    name: result[name]
                    for name in ("published", "reason", "alert_id", "version", "delivery_not_before")
                    if name in result
                },
                "details": detail,
            }
        )
        reason = output.reason
        if output.published:
            reason += f"; карточка {output.alert_id} v{output.version}"
            if output.delivery_not_before is not None:
                reason += f"; отправка не раньше {output.delivery_not_before}"
        await event_progress(
            repo,
            settings,
            event_id,
            "ALERT_QUEUED" if output.published else "PUBLICATION_BLOCKED",
            reason,
            key=f"{output.alert_id}:{output.version}" if output.published else None,
            details=output.details,
        )
    else:
        await event_progress(
            repo,
            settings,
            event_id,
            "ANALYSIS_UPDATED",
            tool,
            key=call_context.call_id if call_context.call_id is not None else sha256(result),
        )


def source_message_progress(message: JsonValue) -> SourceMessageProgress | None:
    if not isinstance(message, dict):
        raise ValueError("Session progress messages must be JSON objects")
    payload = message["payload"]
    if not isinstance(payload, dict) or not isinstance(payload["kind"], str):
        raise ValueError("Session message payload requires its declared kind")
    if payload["kind"] not in {"NEW_ITEM", "ITEM_ADDED"}:
        return None
    return SourceMessageProgress.model_validate(
        {
            **{
                name: message[name]
                for name in ("message_id", "retry_count", "turn_status", "sent_at")
                if name in message
            },
            "payload": {
                name: payload[name]
                for name in ("kind", "item_id", "event_id", "session_id")
                if name in payload
            },
        }
    )


async def session_progress(
    repo: ProgressRepository,
    settings: ProgressSettings,
    session: Mapping[str, JsonValue],
) -> None:
    if not enabled(settings):
        return
    log = session["message_log"]
    if not isinstance(log, list):
        raise ValueError("Session progress requires its durable message log")
    messages = [source_message_progress(message) for message in log]
    view = SessionProgressView.model_validate(
        {
            **{name: session[name] for name in ("session_id", "event_id") if name in session},
            "source_messages": [message for message in messages if message is not None],
        }
    )
    for message in view.source_messages:
        item_id = message.payload.item_id
        key: str | None = f"{view.session_id}:{message.message_id}:{message.retry_count}"
        stage: ProgressStage
        if message.turn_status == "FAILED":
            stage, reason = "PROCESSING_FAILED", "AGENT_EXECUTION_FAILED"
        elif message.turn_status == "DONE":
            item = await repo.get("ingest.source_items", item_id)
            extraction = (
                ExtractionProgressView.model_validate(
                    {name: item[name] for name in ("extraction_status", "extraction_reason") if name in item}
                )
                if item is not None
                else None
            )
            if extraction is not None and extraction.extraction_status == "SKIPPED":
                stage = "AGENT_REJECTED"
                reason = extraction.extraction_reason if extraction.extraction_reason is not None else ""
                key = None
            else:
                stage, reason = "AGENT_COMPLETED", ""
        elif message.sent_at is not None:
            stage, reason = "AGENT_ACCEPTED", "Ожидается результат анализа"
        else:
            continue
        await item_progress(repo, settings, item_id, stage, reason, event_id=view.event_id, key=key)


async def job_failure_progress(
    repo: EventProgressRepository,
    settings: ProgressSettings,
    job: Mapping[str, JsonValue],
    reason: str,
) -> None:
    if not enabled(settings):
        return
    kind = job["type"]
    if not isinstance(kind, str):
        raise ValueError("Progress job type must be a declared string")
    if kind not in {"FILTER_ITEM", "ROUTE_ITEM", "AGENT_MESSAGE", "RUN_MARKET_TRUTH", "ESCALATE_TOKENS"}:
        return
    payload = job["payload"]
    if not isinstance(payload, dict):
        raise ValueError("Source-processing job payload must be a JSON object")
    if kind == "AGENT_MESSAGE":
        message_kind = payload["kind"]
        if not isinstance(message_kind, str):
            raise ValueError("Agent progress payload requires its declared kind")
        if message_kind in {"USER_QUESTION", "SCOUT_REVIEW"}:
            return
    target = FailureProgressTarget.model_validate(
        {name: payload[name] for name in ("item_id", "event_id") if name in payload}
    )
    view = JobFailureProgressView.model_validate(
        {
            **{name: job[name] for name in ("job_id", "type", "status") if name in job},
            "target": target,
        }
    )
    stage: ProgressStage = (
        "PROCESSING_EXPIRED" if view.status == "EXPIRED" or reason == "EXPIRED" else "PROCESSING_FAILED"
    )
    detail = f"Этап {view.type}; {reason}"
    if view.target.item_id is not None:
        await item_progress(repo, settings, view.target.item_id, stage, detail, key=view.job_id)
    else:
        await event_progress(repo, settings, view.target.event_id, stage, detail, key=view.job_id)

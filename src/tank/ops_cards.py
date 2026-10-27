"""Readable operator outcomes derived from persisted pipeline results."""

import html
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from urllib.parse import urlsplit

from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.contracts.delivery import (
    DeliveryStore,
    OpsAlertView,
    OpsAssessmentView,
    OpsCardOutcome,
    OpsCardSnapshot,
    OpsDeliveryView,
    OpsEventView,
    OpsInstrumentView,
    OpsMapView,
    OpsObservationView,
    OpsPresentationClocks,
    OpsProgressDetails,
    OpsProgressNotice,
    OpsRelevance,
    OpsSourceItem,
    OpsTokenView,
    RenderedOpsCard,
    UnknownOpsMap,
    select_ops_map,
)
from tank.contracts.json import decode_unique_json
from tank.ops_progress import clipped
from tank.time_precision import propagate_timestamp
from tank.util import parse_time, utcnow

REASONS = {
    "BELOW_MAJOR": "Значимость ниже порога публикации MAJOR",
    "LOW_CERTAINTY": "Недостаточно подтверждений события",
    "STALE": "Событие устарело",
    "INVALIDATED": "Событие опровергнуто",
    "NO_MATERIAL_CHANGE": "Нет существенных изменений для новой карточки",
    "STALE_STATE_CHANGE": "Изменение события уже устарело",
    "TERMINAL_EVENT": "Событие закрыто или объединено с другим",
    "FIRST_CARD_ALREADY_GENERATED": "Карточка этого события уже подготовлена",
    "ANALYSIS_INCOMPLETE": "Не завершён анализ токенов",
    "AGENT_EXECUTION_FAILED": "Ошибка выполнения агента",
    "NO_UNIVERSE_CONCERN": "Событие не затрагивает токены из реестра Tank",
    "PROPOSED": "Событие остаётся на стадии предложения",
    "MAJOR_EVIDENCE": "Не выполнен критерий высокой значимости",
    "M1_VENUE": "Площадка не входит в список основных бирж Tank",
    "STRUCTURAL_GATE": "Не выполнены условия структурного изменения",
    "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION": "Агент не записал ни событие, ни решение об отклонении",
}
CLASSES = {
    "LOW": "низкая",
    "MATERIAL": "существенная",
    "MAJOR": "высокая",
    "STRUCTURAL_SHOCK": "структурное изменение",
}
STATES = {
    "preopen": "до открытия торгов",
    "live": "торги открыты",
    "suspend": "торги приостановлены",
    "suspended": "торги приостановлены",
    "trading": "торги открыты",
}
LABELS = {
    "status": "статус",
    "base": "базовый актив",
    "quote": "валюта котировки",
    "market_type": "рынок",
    "contract_type": "тип контракта",
    "symbol": "инструмент",
}
EPOCH = datetime.min.replace(tzinfo=UTC)


def plain(value: str | int | float | bool | None, limit: int = 400) -> str:
    text = html.unescape("" if value is None else str(value))
    text = re.sub(r"</?[A-Za-z][^>]*>", " ", text)
    text = re.sub(r"!\[[^]]*\]\([^)]*\)", "", text)
    text = re.sub(r"\[([^]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"(?m)^\s{0,3}#{1,6}\s*", "", text)
    text = re.sub(r"[*`_]{1,3}", "", text)
    return clipped(text, limit)


def source_summary(item: OpsSourceItem) -> tuple[str, str]:
    title = plain(item.title, 180)
    raw = item.text or item.body or item.quoted or ""
    data = None
    if item.source_id == "S14":
        try:
            data = decode_unique_json(raw)
        except (TypeError, ValueError):
            data = None
    if isinstance(data, dict):
        before, after = data.get("before"), data.get("after")
        if isinstance(before, dict) or isinstance(after, dict):
            if (
                not isinstance(before, dict)
                and before is not None
                or not isinstance(after, dict)
                and after is not None
            ):
                raise ValueError("Instrument presentation requires object revisions")
            previous = presentation_view(OpsInstrumentView, before) if isinstance(before, dict) else None
            following = presentation_view(OpsInstrumentView, after) if isinstance(after, dict) else None
            current = following if following is not None else previous
            if current is None:
                raise ValueError("Instrument presentation lacks an observed revision")
            symbol = current.symbol or title
            venue = (current.venue or "Биржа").upper()
            market = {
                "SPOT": "спот",
                "PERP": "рынок деривативов",
                "PERP_LINEAR": "рынок деривативов",
            }.get((current.reported_market_type or "").upper(), "")
            title = plain(f"{venue} · {symbol}" + (f" · {market}" if market else ""), 180)
            if before is None:
                return title, "Инструмент добавлен в список биржи"
            if after is None:
                return title, "Инструмент удалён из списка биржи"
            if previous is None or following is None:
                raise ValueError("Instrument update lacks one of its actual revisions")
            before_fields, after_fields = previous.displayed_fields(), following.displayed_fields()
            changes = [
                f"{label}: {STATES.get(str(before_fields[key]), plain(before_fields[key], 60))} → {STATES.get(str(after_fields[key]), plain(after_fields[key], 60))}"
                for key, label in LABELS.items()
                if before_fields[key] != after_fields[key]
            ]
            return title, "; ".join(changes[:3]) or "Изменились параметры инструмента"
        return title, ""
    text = str(raw)
    text = re.sub(r"(?is)^.*?#{1,6}\s*Summary\s*", "", text, count=1)
    cleaned = plain(text, 2000)
    if title and cleaned.startswith(title):
        cleaned = cleaned[len(title) :].lstrip(" —:-")
    if cleaned.lower().startswith("title:"):
        return title, ""
    first = re.split(r"(?<=[.!?])\s+(?=[A-ZА-Я])", cleaned, maxsplit=1)[0]
    return title, clipped(first, 320)


def time_of(record: OpsPresentationClocks | None, field: str = "created_at") -> datetime:
    if record is None:
        return EPOCH
    match field:
        case "created_at":
            value = record.created_at
        case "at":
            value = record.at
        case "cutoff":
            value = record.cutoff
        case "generated_at":
            value = record.generated_at
        case "observed_at":
            value = record.observed_at
        case _:
            raise ValueError("Unsupported Ops presentation clock")
    return parse_time(value) if value is not None else EPOCH


def recorded_time_of(record: Mapping[str, JsonValue], field: str = "created_at") -> datetime:
    return time_of(presentation_view(OpsPresentationClocks, record), field)


def reason_text(reason: str | None) -> str:
    return REASONS.get(str(reason), plain(reason, 350))


def telegram_link(chat: str | None, message: str | int | None) -> str | None:
    chat = str(chat or "")
    if chat.startswith("-100") and chat[4:].isdigit():
        return f"https://t.me/c/{chat[4:]}/{message}"
    if chat.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]+", chat):
        return f"https://t.me/{chat[1:]}/{message}"
    return None


def safe_link(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = urlsplit(value)
        return (
            value
            if parsed.scheme in {"http", "https"}
            and parsed.hostname
            and not parsed.username
            and not parsed.password
            else None
        )
    except ValueError:
        return None


def presentation_view[T: DomainModel](schema: type[T], record: Mapping[str, JsonValue]) -> T:
    return schema.model_validate(
        {
            field.alias if field.alias is not None else name: record[
                field.alias if field.alias is not None else name
            ]
            for name, field in schema.model_fields.items()
            if (field.alias if field.alias is not None else name) in record
        }
    )


def progress_view(record: Mapping[str, JsonValue]) -> OpsProgressNotice:
    values = {
        name: record[name]
        for name in OpsProgressNotice.model_fields
        if name in record
        and name not in {"details", "created_precision", "created_precision_reason", "created_bucket_anchor"}
    }
    clock = propagate_timestamp(record, "created_at")
    clock_fields = {
        "created_precision": clock.precision,
        "created_precision_reason": clock.precision_reason,
        "created_bucket_anchor": clock.bucket_anchor,
    }
    details = record.get("details")
    if details is not None:
        if not isinstance(details, dict):
            raise ValueError("Ops progress details require an object")
        return OpsProgressNotice.model_validate(
            {**values, **clock_fields, "details": presentation_view(OpsProgressDetails, details)}
        )
    return OpsProgressNotice.model_validate({**values, **clock_fields})


async def snapshot(repo: DeliveryStore, item_id: str) -> OpsCardSnapshot | None:
    raw_item = await repo.get("ingest.source_items", item_id)
    if raw_item is None:
        return None
    item_fields = {
        name: raw_item[name]
        for name in OpsSourceItem.model_fields
        if name in raw_item and name != "relevance"
    }
    relevance = raw_item.get("relevance")
    if relevance is not None:
        if not isinstance(relevance, dict):
            raise ValueError("Ops item relevance requires an object")
        item = OpsSourceItem.model_validate(
            {**item_fields, "relevance": presentation_view(OpsRelevance, relevance)}
        )
    else:
        item = OpsSourceItem.model_validate(item_fields)
    if item.item_id != item_id:
        raise ValueError("Ops presentation item differs from its requested identity")
    history = [
        progress_view(row)
        async for row in repo.iter_all(
            "out.ops_notices", filters={"kind": "ITEM_PROGRESS", "subject_id": item_id}
        )
    ]
    if any(notice.subject_id != item_id for notice in history):
        raise ValueError("Ops progress history differs from its actual source item")
    history.sort(key=lambda row: (time_of(row), row.notice_id))
    stages = {row.stage: row for row in history}
    observations = [
        presentation_view(OpsObservationView, row)
        async for row in repo.iter_all("evt.event_observations", filters={"item_id": item_id})
    ]
    event_id = next((row.event_id for row in reversed(observations) if row.event_id is not None), None)
    if event_id is None:
        event_id = next((row.event_id for row in reversed(history) if row.event_id is not None), None)
    if event_id is None:
        event_id = item.route_event_id
    raw_event = await repo.get("evt.events", event_id) if event_id is not None else None
    event = presentation_view(OpsEventView, raw_event) if raw_event is not None else None
    raw_assessment = (
        await repo.get("evt.ems_assessments", event.current_ems_id)
        if event is not None and event.current_ems_id is not None
        else None
    )
    assessment = presentation_view(OpsAssessmentView, raw_assessment) if raw_assessment is not None else None
    maps: list[OpsMapView] = []
    truths: list[OpsTokenView] = []
    decisions: list[OpsTokenView] = []
    alerts: list[OpsAlertView] = []
    deliveries: list[OpsDeliveryView] = []
    prior_receipt: OpsDeliveryView | None = None
    if event_id is not None:
        maps = [
            presentation_view(OpsMapView, row)
            async for row in repo.iter_metadata(
                "evt.beneficiary_maps",
                ["map_version", "analysis_set", "cutoff"],
                filters={"event_id": event_id},
            )
        ]
        truths = [
            presentation_view(OpsTokenView, row)
            async for row in repo.iter_metadata(
                "ana.market_truth", ["token_id", "cutoff", "action"], filters={"event_id": event_id}
            )
        ]
        decisions = [
            presentation_view(OpsTokenView, row)
            async for row in repo.iter_metadata(
                "ana.decisions", ["token_id", "cutoff", "action"], filters={"event_id": event_id}
            )
        ]
        alerts = [
            presentation_view(OpsAlertView, row)
            for row in await repo.list_latest(
                "out.alerts", "generated_at", filters={"event_id": event_id}, limit=1
            )
        ]
        if alerts:
            alert = alerts[0]
            deliveries = [
                presentation_view(OpsDeliveryView, row)
                async for row in repo.iter_all(
                    "out.deliveries",
                    filters={
                        "alert_id": alert.alert_id,
                        "version": alert.version,
                        "channel": "telegram:alerts",
                    },
                )
            ]
            alert_ids = [
                row
                async for row in repo.iter_metadata(
                    "out.alerts", ["alert_id"], filters={"event_id": event_id}
                )
            ]
            for identity in alert_ids:
                alert_id = identity.get("alert_id")
                if not isinstance(alert_id, str) or not alert_id.strip():
                    raise ValueError("Ops publication requires its actual alert identity")
                receipts = [
                    presentation_view(OpsDeliveryView, row)
                    for row in await repo.list_latest(
                        "out.deliveries",
                        "at",
                        filters={"alert_id": alert_id, "channel": "telegram:alerts", "status": "SENT"},
                        limit=1,
                    )
                ]
                if receipts and time_of(receipts[0], "at") > time_of(prior_receipt, "at"):
                    prior_receipt = receipts[0]
    return OpsCardSnapshot(
        item=item,
        history=history,
        stages=stages,
        event=event,
        assessment=assessment,
        maps=maps,
        truths=truths,
        decisions=decisions,
        alert=alerts[0] if alerts else None,
        deliveries=deliveries,
        prior_receipt=prior_receipt,
        observations=observations,
    )


def outcome(data: OpsCardSnapshot) -> OpsCardOutcome:
    item, stages, event, assessment = data.item, data.stages, data.event, data.assessment
    relevance = item.relevance
    decision = relevance.decision if relevance is not None else None
    if decision is None and "JEV_REJECTED" in stages:
        decision = "SKIP"
    score = relevance.p_relevant if relevance is not None else None
    jev = (
        {"ALWAYS": "пропущен", "SEND": "пройден", "SKIP": "отсеяно"}.get(decision, "ожидает проверки")
        if decision is not None
        else "ожидает проверки"
    )
    if score is not None:
        jev += f" · релевантность {score:.0%}"
        cutoff = relevance.cutoff if relevance is not None else None
        if cutoff is not None:
            jev += f", порог {cutoff:.0%}"
    fields = [("Фильтр", jev)]
    finished = "AGENT_COMPLETED" in stages or "AGENT_REJECTED" in stages
    if time_of(stages.get("AGENT_ACCEPTED")) > max(
        time_of(stages.get("AGENT_COMPLETED")), time_of(stages.get("AGENT_REJECTED"))
    ):
        finished = False
    klass = assessment.classification if assessment is not None else None
    if assessment:
        fields.append(
            (
                "Агент",
                f"значимость — {CLASSES.get(klass, klass) if klass is not None else None} ({klass})",
            )
        )
    elif event:
        fields.append(
            (
                "Агент",
                "событие выделено; оценка значимости отсутствует"
                if finished
                else "событие выделено; оценивает значимость",
            )
        )
    elif finished:
        fields.append(("Агент", "работа остановлена"))
    elif "AGENT_ACCEPTED" in stages:
        fields.append(("Агент", "материал передан, решение ещё не получено"))
    elif "AGENT_QUEUED" in stages or decision in {"SEND", "ALWAYS"}:
        fields.append(("Агент", "ожидает в очереди"))
    else:
        fields.append(("Агент", "ещё не получил материал"))
    sent = [row for row in data.deliveries if row.status == "SENT"]
    alert = data.alert
    blocked = stages.get("PUBLICATION_BLOCKED")
    if blocked and time_of(blocked) < time_of(assessment, "cutoff"):
        blocked = None
    if blocked and blocked.reason == "ANALYSIS_INCOMPLETE":
        updated = max(
            time_of(stages.get("ANALYSIS_UPDATED")),
            max(
                (time_of(row, "cutoff") for rows in (data.maps, data.truths, data.decisions) for row in rows),
                default=EPOCH,
            ),
        )
        if time_of(blocked) < updated:
            blocked = None
    receipt = max(sent, key=lambda row: time_of(row, "at")) if sent else None
    previous_publication = bool(alert) and (
        time_of(alert, "generated_at") < time_of(item, "observed_at")
        or time_of(assessment, "cutoff") > time_of(alert, "generated_at")
        or time_of(blocked) > time_of(alert, "generated_at")
        or max(time_of(stages.get("PROCESSING_FAILED")), time_of(stages.get("PROCESSING_EXPIRED")))
        > time_of(alert, "generated_at")
    )
    if receipt and not previous_publication:
        fields.append(("Alerts", "доставлено"))
        return OpsCardOutcome(
            code="ALERT_SENT",
            status="Опубликовано в Alerts",
            fields=fields,
            reason="",
            link=telegram_link(receipt.chat_id, receipt.message_ref),
        )
    if alert and not receipt and not previous_publication:
        failed = any(row.terminal and row.status == "FAILED" for row in data.deliveries)
        fields.append(
            (
                "Alerts",
                "доставка завершилась ошибкой" if failed else "карточка подготовлена, ожидает отправки",
            )
        )
        errors = [row for row in data.deliveries if row.status == "FAILED"]
        last_error = max(errors, key=lambda row: time_of(row, "at"), default=None)
        reason = ""
        if failed:
            reported_attempts = [row.attempt for row in errors if row.attempt is not None]
            if any(row.attempt is None for row in errors):
                reason = "Доставка не выполнена; число попыток не полностью записано"
                if reported_attempts:
                    reason += f"; известный максимум: {max(reported_attempts)}"
            else:
                reason = f"Доставка не выполнена; попыток: {max(reported_attempts)}"
            http_status = next((row.http_status for row in reversed(errors) if row.http_status), None)
            if http_status:
                reason += f"; Telegram HTTP {http_status}"
            elif last_error is not None and last_error.error:
                reason += "; " + plain(last_error.error, 100)
        elif alert.delivery_not_before and parse_time(alert.delivery_not_before) > utcnow():
            reason = "Отправка запланирована на " + parse_time(alert.delivery_not_before).strftime(
                "%H:%M UTC"
            )
        elif errors:
            reason = "Ожидает повторной отправки после ошибки Telegram"
        return OpsCardOutcome(
            code="ALERT_FAILED" if failed else "ALERT_QUEUED",
            status="Ошибка доставки в Alerts" if failed else "Ожидает отправки в Alerts",
            fields=fields,
            reason=reason,
        )
    failure = max(
        (row for row in data.history if row.stage in {"PROCESSING_FAILED", "PROCESSING_EXPIRED"}),
        key=time_of,
        default=None,
    )
    recoveries = [
        row
        for row in data.history
        if row.stage
        not in {"PROCESSING_FAILED", "PROCESSING_EXPIRED", "AGENT_COMPLETED", "RECEIVED", "EVIDENCE_FETCHED"}
    ]
    if failure and time_of(failure) >= max((time_of(row) for row in recoveries), default=EPOCH):
        fields.append(("Alerts", "не отправлено"))
        reason = reason_text(failure.reason)
        for code, label in {
            "FILTER_ITEM": "фильтр",
            "ROUTE_ITEM": "маршрутизация",
            "AGENT_MESSAGE": "передача агенту",
            "RUN_MARKET_TRUTH": "рыночный анализ",
            "ESCALATE_TOKENS": "подготовка рыночных данных",
            "TimeoutError": "превышено время ожидания",
        }.items():
            reason = reason.replace(code, label)
        return OpsCardOutcome(
            code=failure.stage,
            status="Истёк срок обработки" if failure.stage == "PROCESSING_EXPIRED" else "Ошибка обработки",
            fields=fields,
            reason=reason,
        )
    if decision == "SKIP":
        fields.append(("Alerts", "не отправлено — материал отсеян до анализа"))
        return OpsCardOutcome(
            code="JEV_REJECTED", status="Отсеяно фильтром", fields=fields, reason="Релевантность ниже порога"
        )
    if item.extraction_status == "SKIPPED":
        rejection = stages.get("AGENT_REJECTED")
        rejection_reason = (
            item.extraction_reason
            if item.extraction_reason
            else rejection.reason
            if rejection is not None
            else None
        )
        explicit_rejection = item.extraction_decision == "NO_EVENT"
        missing_observation = rejection_reason == "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION" or (
            not rejection_reason and not explicit_rejection
        )
        fields[-1] = (
            "Агент",
            "решение по материалу отсутствует" if missing_observation else "не признал материал событием",
        )
        fields.append(("Alerts", "не отправлено"))
        return OpsCardOutcome(
            code="INCOMPLETE" if missing_observation else "AGENT_REJECTED",
            status="Агент не оставил решения" if missing_observation else "Отклонено агентом",
            fields=fields,
            reason=reason_text(rejection_reason)
            if rejection_reason and (explicit_rejection or missing_observation)
            else "Отсутствует запись события или явного отклонения"
            if missing_observation
            else "Причина отказа не записана",
        )
    if blocked:
        fields.append(("Alerts", "обновление отклонено" if receipt else "публикация отклонена"))
        reason = reason_text(blocked.reason)
        details = blocked.details
        missing = []
        for values, label in (
            (details.missing_market_truth if details is not None else None, "рыночные данные"),
            (details.missing_decisions if details is not None else None, "решения"),
        ):
            if values:
                missing.append(label + ": " + ", ".join(value.rsplit(":", 1)[-1] for value in values))
        if blocked.reason == "ANALYSIS_INCOMPLETE" and not data.maps:
            missing.append("не определены затронутые токены")
        if missing:
            reason += ": " + "; ".join(missing)
        return OpsCardOutcome(
            code="PUBLICATION_BLOCKED",
            status="Обновление не опубликовано в Alerts" if receipt else "Не опубликовано в Alerts",
            fields=fields,
            reason=reason,
        )
    if (
        receipt
        and finished
        and any(row.link_type == "CORROBORATION" for row in data.observations)
        and time_of(assessment, "cutoff") <= time_of(alert, "generated_at")
    ):
        fields[1] = ("Агент", "добавил подтверждение к существующему событию")
        fields.append(("Alerts", "новая карточка не создавалась"))
        return OpsCardOutcome(
            code="CORROBORATION", status="Подтверждение существующего события", fields=fields, reason=""
        )
    if assessment and klass not in {"MAJOR", "STRUCTURAL_SHOCK"}:
        fields.append(("Alerts", "не отправлено — ниже порога MAJOR"))
        reasons = list(
            dict.fromkeys(assessment.class_reasons if assessment.class_reasons is not None else [])
        )
        caps = {
            "NO_UNIVERSE_CONCERN",
            "PROPOSED",
            "LOW_CERTAINTY",
            "MAJOR_EVIDENCE",
            "M1_VENUE",
            "STRUCTURAL_GATE",
        }
        reasons.sort(key=lambda value: value not in caps)
        reasons = [reason_text(value) for value in reasons]
        return OpsCardOutcome(
            code="BELOW_MAJOR", status="Ниже порога Alerts", fields=fields, reason="; ".join(reasons[:2])
        )
    missing = []
    reached = "анализ материала"
    if not event:
        missing.append("решение, является ли материал событием")
    elif not assessment:
        missing.append("оценка значимости события")
        reached = "оценка значимости"
    elif not data.maps:
        missing.append("определение затронутых токенов")
        reached = "определение затронутых токенов"
    else:
        mapping = select_ops_map(data.maps)
        if isinstance(mapping, UnknownOpsMap):
            missing.append(
                "версия карты не записана"
                if mapping.reason == "MAP_VERSION_UNREPORTED"
                else "состав токенов не записан"
            )
            reached = "определение затронутых токенов"
        else:
            tokens = set(mapping.analysis_set)
            truth_tokens = {row.token_id for row in data.truths}
            decision_tokens = {row.token_id for row in data.decisions}
            fields.append(
                (
                    "Рынок",
                    f"данные: {len(tokens & truth_tokens)}/{len(tokens)} токенов; решения: {len(tokens & decision_tokens)}/{len(tokens)}",
                )
            )
            if tokens - truth_tokens:
                missing.append(
                    "рыночный анализ: "
                    + ", ".join(token.rsplit(":", 1)[-1] for token in sorted(tokens - truth_tokens))
                )
                reached = "рыночный анализ"
            elif tokens - decision_tokens:
                missing.append(
                    "решение по токенам: "
                    + ", ".join(token.rsplit(":", 1)[-1] for token in sorted(tokens - decision_tokens))
                )
                reached = "решение по токенам"
            else:
                missing.append("решение о публикации")
                reached = "решение о публикации"
    fields.append(("Alerts", "публикация не запрошена"))
    if finished:
        return OpsCardOutcome(
            code="INCOMPLETE",
            status="Анализ не завершён",
            fields=fields,
            reason="Не выполнено: " + "; ".join(missing),
        )
    if "AGENT_ACCEPTED" in stages or event:
        return OpsCardOutcome(
            code="ANALYZING",
            status={
                "анализ материала": "Ожидает решения агента",
                "оценка значимости": "Ожидает оценки значимости",
                "определение затронутых токенов": "Ожидает определения затронутых токенов",
                "рыночный анализ": "Ожидает рыночного анализа",
                "решение по токенам": "Ожидает решения по токенам",
                "решение о публикации": "Ожидает решения о публикации",
            }[reached],
            fields=fields,
            reason="",
        )
    return OpsCardOutcome(
        code="QUEUED",
        status="В очереди к агенту" if decision in {"SEND", "ALWAYS"} else "В очереди на фильтр",
        fields=fields,
        reason="",
    )


def render(data: OpsCardSnapshot) -> RenderedOpsCard:
    result = outcome(data)
    item, event = data.item, data.event
    title, summary = source_summary(item)
    if item.source_id != "S14" and event is not None and event.statement:
        title, summary = plain(event.statement, 450), ""
    if summary == title:
        summary = ""
    lines = [f"<b>{html.escape(result.status)}</b>"]
    if title:
        lines.append(f"<b>{html.escape(title)}</b>")
    if summary:
        lines.append(html.escape(summary))
    lines.append("")
    lines.extend(f"<b>{label}:</b> {html.escape(plain(value, 400))}" for label, value in result.fields)
    if result.reason:
        lines.append("<b>Причина:</b> " + html.escape(plain(result.reason, 450)))
    source = plain(item.source_name or item.source or item.source_id, 100)
    url = safe_link(item.url)
    lines.extend(
        [
            "",
            f'<a href="{html.escape(url, quote=True)}">{html.escape(source)} · источник</a>'
            if url
            else html.escape(source),
        ]
    )
    link = result.link
    label = "Карточка в Alerts"
    if not link:
        receipt = data.prior_receipt
        if receipt:
            link = telegram_link(receipt.chat_id, receipt.message_ref)
            label = "Предыдущая карточка в Alerts"
    if link:
        lines.append(f'<a href="{html.escape(link, quote=True)}">{label}</a>')
    rendered = "\n".join(lines).strip()
    return RenderedOpsCard(
        code=result.code,
        status=result.status,
        fields=result.fields,
        reason=result.reason,
        link=result.link,
        html=rendered,
        text=html.unescape(re.sub(r"<[^>]*>", "", rendered)),
    )

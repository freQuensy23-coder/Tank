"""Readable operator outcomes derived from persisted pipeline results."""

import html
import json
import re
from datetime import UTC, datetime
from urllib.parse import urlsplit

from tank.ops_progress import clipped
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


def plain(value, limit=400):
    text = html.unescape(str(value or ""))
    text = re.sub(r"</?[A-Za-z][^>]*>", " ", text)
    text = re.sub(r"!\[[^]]*\]\([^)]*\)", "", text)
    text = re.sub(r"\[([^]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"(?m)^\s{0,3}#{1,6}\s*", "", text)
    text = re.sub(r"[*`_]{1,3}", "", text)
    return clipped(text, limit)


def source_summary(item):
    title = plain(item.get("title"), 180)
    raw = item.get("text") or item.get("body") or item.get("quoted") or ""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        data = None
    if isinstance(data, dict):
        before, after = data.get("before"), data.get("after")
        if isinstance(before, dict) or isinstance(after, dict):
            current = after or before
            symbol = current.get("symbol") or title
            venue = str(current.get("venue") or "Биржа").upper()
            market = {"SPOT": "спот", "PERP": "бессрочный контракт"}.get(
                str(current.get("market_type", "")).upper(), ""
            )
            title = plain(f"{venue} · {symbol}" + (f" · {market}" if market else ""), 180)
            if before is None:
                return title, "Инструмент добавлен в список биржи"
            if after is None:
                return title, "Инструмент удалён из списка биржи"
            changes = [
                f"{label}: {STATES.get(str(before.get(key)), plain(before.get(key), 60))} → {STATES.get(str(after.get(key)), plain(after.get(key), 60))}"
                for key, label in LABELS.items()
                if before.get(key) != after.get(key)
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


def time_of(record, field="created_at"):
    value = record.get(field) if record else None
    return parse_time(value) if value else EPOCH


def reason_text(reason):
    return REASONS.get(str(reason), plain(reason, 350))


def telegram_link(chat, message):
    chat = str(chat or "")
    if chat.startswith("-100") and chat[4:].isdigit():
        return f"https://t.me/c/{chat[4:]}/{message}"
    if chat.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]+", chat):
        return f"https://t.me/{chat[1:]}/{message}"
    return None


def safe_link(value):
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


async def snapshot(repo, item_id):
    item = await repo.get("ingest.source_items", item_id)
    if item is None:
        return None
    history = [
        row
        async for row in repo.iter_all(
            "out.ops_notices", filters={"kind": "ITEM_PROGRESS", "subject_id": item_id}
        )
    ]
    history.sort(key=lambda row: (time_of(row), row["notice_id"]))
    stages = {row["stage"]: row for row in history}
    observations = [
        row async for row in repo.iter_all("evt.event_observations", filters={"item_id": item_id})
    ]
    event_id = next((row.get("event_id") for row in reversed(observations) if row.get("event_id")), None)
    event_id = (
        event_id
        or next((row.get("event_id") for row in reversed(history) if row.get("event_id")), None)
        or item.get("route_event_id")
    )
    event = await repo.get("evt.events", event_id) if event_id else None
    event = event or {}
    assessment = (
        await repo.get("evt.ems_assessments", event["current_ems_id"])
        if event.get("current_ems_id")
        else None
    )
    maps, truths, decisions, alerts, deliveries = [], [], [], [], []
    prior_receipt = None
    if event_id:
        for collection, target in (
            ("evt.beneficiary_maps", maps),
            ("ana.market_truth", truths),
            ("ana.decisions", decisions),
        ):
            fields = (
                ["map_version", "analysis_set", "cutoff"]
                if collection == "evt.beneficiary_maps"
                else ["token_id", "cutoff", "action"]
            )
            target.extend(
                [row async for row in repo.iter_metadata(collection, fields, filters={"event_id": event_id})]
            )
        alerts = await repo.list_latest("out.alerts", "generated_at", filters={"event_id": event_id}, limit=1)
        if alerts:
            deliveries = [
                row
                async for row in repo.iter_all(
                    "out.deliveries",
                    filters={
                        "alert_id": alerts[0]["alert_id"],
                        "version": alerts[0]["version"],
                        "channel": "telegram:alerts",
                    },
                )
            ]
            alert_ids = {
                row["alert_id"]
                async for row in repo.iter_metadata(
                    "out.alerts", ["alert_id"], filters={"event_id": event_id}
                )
            }
            for alert_id in alert_ids:
                receipts = await repo.list_latest(
                    "out.deliveries",
                    "at",
                    filters={"alert_id": alert_id, "channel": "telegram:alerts", "status": "SENT"},
                    limit=1,
                )
                if receipts and time_of(receipts[0], "at") > time_of(prior_receipt, "at"):
                    prior_receipt = receipts[0]
    return {
        "item": item,
        "history": history,
        "stages": stages,
        "event": event,
        "assessment": assessment,
        "maps": maps,
        "truths": truths,
        "decisions": decisions,
        "alert": alerts[0] if alerts else None,
        "deliveries": deliveries,
        "prior_receipt": prior_receipt,
        "observations": observations,
    }


def outcome(data):
    item, stages, event, assessment = (data[key] for key in ("item", "stages", "event", "assessment"))
    relevance = item.get("relevance") or {}
    decision = relevance.get("decision")
    if decision is None and "JEV_REJECTED" in stages:
        decision = "SKIP"
    score = relevance.get("p_relevant")
    jev = {"ALWAYS": "пропущен", "SEND": "пройден", "SKIP": "отсеяно"}.get(decision, "ожидает проверки")
    if score is not None:
        jev += f" · релевантность {score:.0%}"
        cutoff = relevance.get("cutoff")
        if cutoff is not None:
            jev += f", порог {cutoff:.0%}"
    fields = [("Jev", jev)]
    finished = "AGENT_COMPLETED" in stages or "AGENT_REJECTED" in stages
    if time_of(stages.get("AGENT_ACCEPTED")) > max(
        time_of(stages.get("AGENT_COMPLETED")), time_of(stages.get("AGENT_REJECTED"))
    ):
        finished = False
    klass = (assessment or {}).get("class")
    if assessment:
        fields.append(("Агент", f"значимость — {CLASSES.get(klass, klass)} ({klass})"))
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
    sent = [row for row in data["deliveries"] if row["status"] == "SENT"]
    alert = data["alert"]
    blocked = stages.get("PUBLICATION_BLOCKED")
    if blocked and time_of(blocked) < time_of(assessment, "cutoff"):
        blocked = None
    if blocked and blocked.get("reason") == "ANALYSIS_INCOMPLETE":
        updated = max(
            time_of(stages.get("ANALYSIS_UPDATED")),
            max(
                (time_of(row, "cutoff") for name in ("maps", "truths", "decisions") for row in data[name]),
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
        return {
            "code": "ALERT_SENT",
            "status": "Опубликовано в Alerts",
            "fields": fields,
            "reason": "",
            "link": telegram_link(receipt.get("chat_id"), receipt["message_ref"]),
        }
    if alert and not receipt and not previous_publication:
        failed = any(row.get("terminal") and row["status"] == "FAILED" for row in data["deliveries"])
        fields.append(
            (
                "Alerts",
                "доставка завершилась ошибкой" if failed else "карточка подготовлена, ожидает отправки",
            )
        )
        errors = [row for row in data["deliveries"] if row["status"] == "FAILED"]
        last_error = max(errors, key=lambda row: time_of(row, "at"), default={})
        reason = ""
        if failed:
            reason = (
                f"Доставка не выполнена; попыток: {max((row.get('attempt', 0) for row in errors), default=0)}"
            )
            http_status = next(
                (row.get("http_status") for row in reversed(errors) if row.get("http_status")), None
            )
            if http_status:
                reason += f"; Telegram HTTP {http_status}"
            elif last_error.get("error"):
                reason += "; " + plain(last_error["error"], 100)
        elif alert.get("delivery_not_before") and parse_time(alert["delivery_not_before"]) > utcnow():
            reason = "Отправка запланирована на " + parse_time(alert["delivery_not_before"]).strftime(
                "%H:%M UTC"
            )
        elif errors:
            reason = "Ожидает повторной отправки после ошибки Telegram"
        return {
            "code": "ALERT_FAILED" if failed else "ALERT_QUEUED",
            "status": "Ошибка доставки в Alerts" if failed else "Ожидает отправки в Alerts",
            "fields": fields,
            "reason": reason,
        }
    failure = max(
        (row for row in data["history"] if row["stage"] in {"PROCESSING_FAILED", "PROCESSING_EXPIRED"}),
        key=time_of,
        default=None,
    )
    recoveries = [
        row
        for row in data["history"]
        if row["stage"]
        not in {"PROCESSING_FAILED", "PROCESSING_EXPIRED", "AGENT_COMPLETED", "RECEIVED", "EVIDENCE_FETCHED"}
    ]
    if failure and time_of(failure) >= max((time_of(row) for row in recoveries), default=EPOCH):
        fields.append(("Alerts", "не отправлено"))
        reason = reason_text(failure.get("reason", ""))
        for code, label in {
            "FILTER_ITEM": "фильтр Jev",
            "ROUTE_ITEM": "маршрутизация",
            "AGENT_MESSAGE": "передача агенту",
            "RUN_MARKET_TRUTH": "рыночный анализ",
            "ESCALATE_TOKENS": "подготовка рыночных данных",
            "TimeoutError": "превышено время ожидания",
        }.items():
            reason = reason.replace(code, label)
        return {
            "code": failure["stage"],
            "status": "Истёк срок обработки"
            if failure["stage"] == "PROCESSING_EXPIRED"
            else "Ошибка обработки",
            "fields": fields,
            "reason": reason,
        }
    if decision == "SKIP":
        fields.append(("Alerts", "не отправлено — материал отсеян до анализа"))
        return {
            "code": "JEV_REJECTED",
            "status": "Отсеяно Jev",
            "fields": fields,
            "reason": "Релевантность ниже порога",
        }
    if item.get("extraction_status") == "SKIPPED":
        reason = item.get("extraction_reason") or (stages.get("AGENT_REJECTED") or {}).get("reason")
        explicit_rejection = item.get("extraction_decision") == "NO_EVENT"
        missing = reason == "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION" or (
            not reason and not explicit_rejection
        )
        fields[-1] = (
            "Агент",
            "решение по материалу отсутствует" if missing else "не признал материал событием",
        )
        fields.append(("Alerts", "не отправлено"))
        return {
            "code": "INCOMPLETE" if missing else "AGENT_REJECTED",
            "status": "Агент не оставил решения" if missing else "Отклонено агентом",
            "fields": fields,
            "reason": reason_text(reason)
            if reason and (explicit_rejection or missing)
            else "Отсутствует запись события или явного отклонения"
            if missing
            else "Причина отказа не записана",
        }
    if blocked:
        fields.append(("Alerts", "обновление отклонено" if receipt else "публикация отклонена"))
        reason = reason_text(blocked.get("reason"))
        details = blocked.get("details") or {}
        missing = []
        for name, label in (("missing_market_truth", "рыночные данные"), ("missing_decisions", "решения")):
            if details.get(name):
                missing.append(
                    label + ": " + ", ".join(str(value).rsplit(":", 1)[-1] for value in details[name])
                )
        if blocked.get("reason") == "ANALYSIS_INCOMPLETE" and not data["maps"]:
            missing.append("не определены затронутые токены")
        if missing:
            reason += ": " + "; ".join(missing)
        return {
            "code": "PUBLICATION_BLOCKED",
            "status": "Обновление не опубликовано в Alerts" if receipt else "Не опубликовано в Alerts",
            "fields": fields,
            "reason": reason,
        }
    if (
        receipt
        and finished
        and any(row.get("link_type") == "CORROBORATION" for row in data["observations"])
        and time_of(assessment, "cutoff") <= time_of(alert, "generated_at")
    ):
        fields[1] = ("Агент", "добавил подтверждение к существующему событию")
        fields.append(("Alerts", "новая карточка не создавалась"))
        return {
            "code": "CORROBORATION",
            "status": "Подтверждение существующего события",
            "fields": fields,
            "reason": "",
        }
    if assessment and klass not in {"MAJOR", "STRUCTURAL_SHOCK"}:
        fields.append(("Alerts", "не отправлено — ниже порога MAJOR"))
        reasons = list(dict.fromkeys(assessment.get("class_reasons", [])))
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
        return {
            "code": "BELOW_MAJOR",
            "status": "Ниже порога Alerts",
            "fields": fields,
            "reason": "; ".join(reasons[:2]),
        }
    missing = []
    reached = "анализ материала"
    if not event:
        missing.append("решение, является ли материал событием")
    elif not assessment:
        missing.append("оценка значимости события")
        reached = "оценка значимости"
    elif not data["maps"]:
        missing.append("определение затронутых токенов")
        reached = "определение затронутых токенов"
    else:
        mapping = max(data["maps"], key=lambda row: row.get("map_version", 0))
        tokens = set(mapping.get("analysis_set", []))
        truth_tokens = {row["token_id"] for row in data["truths"]}
        decision_tokens = {row["token_id"] for row in data["decisions"]}
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
        return {
            "code": "INCOMPLETE",
            "status": "Анализ не завершён",
            "fields": fields,
            "reason": "Не выполнено: " + "; ".join(missing),
        }
    if "AGENT_ACCEPTED" in stages or event:
        return {
            "code": "ANALYZING",
            "status": {
                "анализ материала": "Ожидает решения агента",
                "оценка значимости": "Ожидает оценки значимости",
                "определение затронутых токенов": "Ожидает определения затронутых токенов",
                "рыночный анализ": "Ожидает рыночного анализа",
                "решение по токенам": "Ожидает решения по токенам",
                "решение о публикации": "Ожидает решения о публикации",
            }[reached],
            "fields": fields,
            "reason": "",
        }
    return {
        "code": "QUEUED",
        "status": "В очереди к агенту" if decision in {"SEND", "ALWAYS"} else "В очереди на Jev",
        "fields": fields,
        "reason": "",
    }


def render(data):
    result = outcome(data)
    item, event = data["item"], data["event"]
    title, summary = source_summary(item)
    if item.get("source_id") != "S14" and event.get("statement"):
        title, summary = plain(event["statement"], 450), ""
    if summary == title:
        summary = ""
    lines = [f"<b>{html.escape(result['status'])}</b>"]
    if title:
        lines.append(f"<b>{html.escape(title)}</b>")
    if summary:
        lines.append(html.escape(summary))
    lines.append("")
    lines.extend(f"<b>{label}:</b> {html.escape(plain(value, 400))}" for label, value in result["fields"])
    if result.get("reason"):
        lines.append("<b>Причина:</b> " + html.escape(plain(result["reason"], 450)))
    source = plain(item.get("source_name") or item.get("source") or item.get("source_id"), 100)
    url = safe_link(item.get("url"))
    lines.extend(
        [
            "",
            f'<a href="{html.escape(url, quote=True)}">{html.escape(source)} · источник</a>'
            if url
            else html.escape(source),
        ]
    )
    link = result.get("link")
    label = "Карточка в Alerts"
    if not link:
        receipt = data.get("prior_receipt")
        if receipt:
            link = telegram_link(receipt.get("chat_id"), receipt["message_ref"])
            label = "Предыдущая карточка в Alerts"
    if link:
        lines.append(f'<a href="{html.escape(link, quote=True)}">{label}</a>')
    result["html"] = "\n".join(lines).strip()
    result["text"] = html.unescape(re.sub(r"<[^>]*>", "", result["html"]))
    return result

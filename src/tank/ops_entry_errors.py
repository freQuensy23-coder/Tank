"""Tank Ops reports of source entries that a poll could not turn into items.

A poll that leaves entry errors in the feed checkpoint reports the errors not seen
before in one ``ENTRY_ERRORS`` notice for that source poll. Each reported error is
remembered by an ``ENTRY_ERROR`` marker whose id is derived from the source, the
feed URL, the reason and the entry identity (the digest of the wire entry when the
parser had one), never from the poll time. Feeds that keep returning the same broken
entry therefore produce no further messages. A failed S29 run-sync call is keyed by its
HTTP status and the feed's count of successful reads before it, so it is reported once
per failure episode and status. A post referenced by a new X post whose page X answered
with an HTTP error is keyed by that post. Markers, the notice and its ``DELIVER`` job are
written in one transaction.

An HTTP error is reported in full: the failed S29 run-sync response, and every request the
worker made to X for a failed account or referenced post, with the status, every header and
the whole body of each response. When that text does not fit one Telegram message the notice
keeps it in ``document`` and its ``text`` is a short caption; delivery sends both in one
``sendDocument`` call.
"""

import html
import re
from datetime import timedelta

from tank.ops_cards import plain
from tank.ops_progress import enabled
from tank.time_precision import collection_clock, offset_schedule
from tank.util import parse_time, sha256

REASONS = {
    "ENTRY_WITHOUT_ID_OR_URL": "Запись без идентификатора и ссылки",
    "INVALID_PUBLISHER_ENTRY": "Запись не соответствует формату источника",
    "MALFORMED_FEED_DOCUMENT": "Документ ленты повреждён, записи разобраны частично",
    "PENDING_PROPOSAL_UNAVAILABLE": "Отслеживаемое предложение не вернулось в ответе источника",
    "MISSING_FROM_RUN": "Аккаунт X отсутствует в результате сбора",
    "NO_CURSOR": "Для аккаунта X не получен курсор",
    "APIFY_RUN_HTTP_ERROR": "Запуск сбора X в Apify завершился HTTP-ошибкой",
}
MESSAGE_LIMIT = 3900
CAPTION_LIMIT = 1000
RELATIONS = {
    "quoted": "процитированный пост",
    "replied_to": "пост, на который дан ответ",
    "linked": "пост по ссылке",
}


def identity(error):
    if error.get("entry_sha256"):
        return {"entry_sha256": error["entry_sha256"]}
    if error.get("proposal_id"):
        return {"proposal_id": error["proposal_id"]}
    if error.get("reason") == "APIFY_RUN_HTTP_ERROR":
        return {"status": error["response"]["status"], "reads_ok_before": error.get("reads_ok_before")}
    if error.get("referenced_post"):
        return {"referenced_post": error["referenced_post"]["id"]}
    if "account" in error:
        return {"account": str(error.get("account") or "").lower()}
    return {"error": re.sub(r"\d+", "#", str(error.get("error") or ""))}


def error_key(source_id, feed_url, error):
    return sha256(
        {
            "source_id": source_id,
            "feed": feed_url,
            "reason": re.sub(r"\d+", "#", str(error.get("reason") or "")),
            **identity(error),
        }
    )


def units(text):
    return len(text.encode("utf-16-le")) // 2


def response_lines(status, headers, body, size):
    """An HTTP response in full: the status line, every header on its own line, the body as received."""
    lines = [f"Ответ: HTTP {status}"]
    pairs = headers.items() if isinstance(headers, dict) else headers or []
    if pairs:
        lines.append("Заголовки:")
        lines.extend(f"{name}: {value}" for name, value in pairs)
    if not body:
        return [*lines, f"Тело ответа ({size}): пусто"]
    return [*lines, f"Тело ответа ({size}):", str(body)]


def run_response_lines(response, full):
    """The response of a failed S29 run-sync call."""
    status = f"{response.get('status')} {response.get('reason_phrase') or ''}".rstrip()
    if not full:
        return [f"Ответ: HTTP {status}"]
    return response_lines(
        status, response.get("headers"), response.get("body"), f"{response.get('body_bytes')} байт"
    )


def diagnostics_lines(diagnostics, full):
    """The worker's requests to X for one failure: the request, then each attempt and X's response."""
    request = diagnostics.get("request") or {}
    attempts = diagnostics.get("attempts") or []
    lines = [
        f"Запрос к X: {request.get('method')} {request.get('url')}",
        f"Попыток: {len(attempts)}; итог: {diagnostics.get('outcome')}",
    ]
    if not full:
        return [*lines, "Ответы: " + ", ".join(str(a.get("status") or a.get("error")) for a in attempts)]
    for attempt in attempts:
        lines.append(f"Попытка {attempt.get('attempt')} · {attempt.get('at')} · {attempt.get('error')}")
        response = attempt.get("response")
        if not response:
            continue
        size = f"{response.get('bodyBytes')} байт"
        if response.get("bodyEncoding") == "base64":
            size += ", base64"
        if response.get("decodeError"):
            size += ", gzip не распакован"
        if response.get("bodyTruncated"):
            size += ", получено не полностью"
        if response.get("omittedWireBytes"):
            size += f", не хватает {response['omittedWireBytes']} байт"
        if response.get("readError"):
            size += f", {response['readError']}"
        if response.get("redacted"):
            size += ", учётные данные скрыты"
        lines.extend(
            response_lines(response.get("status"), response.get("headers"), response.get("body"), size)
        )
    return lines


def seen_label(value):
    return parse_time(value).strftime("%Y-%m-%d %H:%M:%S UTC")


def entry_block(number, error, payload_shown, full):
    reason = str(error.get("reason") or "UNKNOWN")
    label = REASONS.get(reason)
    lines = [f"{number}. {label} ({reason})" if label else f"{number}. {plain(reason, 200)}"]
    entry = error.get("entry") or {}
    parts = []
    if error.get("entry_index") is not None:
        parts.append(f"№{error['entry_index']} в ответе")
    if entry.get("title"):
        parts.append("«" + plain(entry["title"], 160) + "»")
    for name in ("id", "link"):
        if entry.get(name):
            parts.append(plain(entry[name], 200))
    if error.get("proposal_id"):
        parts.append("предложение " + plain(error["proposal_id"], 120))
    if error.get("account"):
        parts.append("аккаунт @" + plain(error["account"], 60))
        if reference := error.get("referenced_post"):
            relation = RELATIONS.get(reference.get("relation"), "пост")
            parts.append(f"{relation} {reference.get('url')} в посте {reference.get('in')}")
    elif "account" in error:
        parts.append("весь запуск сбора X")
    if parts:
        lines.append("Запись: " + " · ".join(parts))
    if entry.get("excerpt"):
        lines.append("Фрагмент: " + plain(entry["excerpt"], 200))
    if error.get("error"):
        lines.append("Ошибка: " + plain(error["error"], 300))
    if error.get("response"):
        lines.extend(run_response_lines(error["response"], full))
    if error.get("diagnostics"):
        lines.extend(diagnostics_lines(error["diagnostics"], full))
    if error.get("entry_sha256"):
        lines.append("sha256 записи: " + error["entry_sha256"])
    if error.get("payload_sha256") and not payload_shown:
        lines.append("sha256 ответа: " + error["payload_sha256"])
    return "\n".join(lines)


def render(notice, limit=MESSAGE_LIMIT):
    """The notice text within limit units, or all of it when limit is None.

    A notice with a ``document`` gets the caption: each error in brief, the responses left to the file."""
    full = not notice.get("document")
    rest_in = "в приложенном файле" if not full else f"полный список: out.ops_notices {notice['notice_id']}"
    source = notice["source_id"] + (f" · {notice['source_name']}" if notice.get("source_name") else "")
    errors = notice["entry_errors"]
    payloads = {error.get("payload_sha256") for error in errors}
    shared = len(payloads) == 1 and None not in payloads
    head = [
        "TANK OPS · Записи источника не разобраны",
        "Источник: " + plain(source, 200),
        "Лента: " + notice["feed_url"],
        "Обнаружено: " + seen_label(notice["seen_at"]),
        f"Новых записей с ошибкой: {len(errors)}",
    ]
    if shared:
        head.append("sha256 ответа: " + payloads.pop())
    if not full:
        head.append("Запросы и ответы HTTP целиком, с заголовками и телами: в приложенном файле")
    text = "\n".join(head)
    for index, error in enumerate(errors):
        block = entry_block(index + 1, error, shared, full)
        rest = len(errors) - index - 1
        tail = f"\n\n… и ещё {rest}; {rest_in}" if rest else ""
        candidate = text + "\n\n" + block
        if limit is not None and units(html.escape(candidate + tail)) > limit:
            return text + f"\n\n… и ещё {len(errors) - index}; {rest_in}"
        text = candidate
    return text


async def report_entry_errors(repo, settings, source, feed, errors, seen_at):
    if not enabled(settings) or not errors:
        return None
    source_id = source["source_id"]
    feed_url = feed["url"]
    pending = {}
    for error in errors:
        pending.setdefault(error_key(source_id, feed_url, error), error)
    async with repo.lock(f"ops-entry-errors:{source_id}:{sha256(feed_url)}"), repo.transaction():
        fresh = {
            key: error
            for key, error in pending.items()
            if not await repo.get("out.ops_notices", "entry_error_" + key)
        }
        if not fresh:
            return None
        notice_id = "entry_errors_" + sha256(
            {"source_id": source_id, "feed": feed_url, "keys": sorted(fresh)}
        )
        stamp = collection_clock()
        for key, error in fresh.items():
            await repo.put(
                "out.ops_notices",
                "entry_error_" + key,
                {
                    "notice_id": "entry_error_" + key,
                    "kind": "ENTRY_ERROR",
                    "subject_id": source_id,
                    "text": str(error.get("reason") or "UNKNOWN"),
                    "group_notice_id": notice_id,
                    "feed_url": feed_url,
                    "entry_error": error,
                    **stamp.fields("created_at"),
                    "created_by": "source-poller",
                },
                immutable=True,
            )
        notice = {
            "notice_id": notice_id,
            "kind": "ENTRY_ERRORS",
            "subject_id": source_id,
            "source_id": source_id,
            "source_name": source.get("name"),
            "feed_url": feed_url,
            "seen_at": seen_at,
            "entry_error_keys": sorted(fresh),
            "entry_errors": list(fresh.values()),
            **stamp.fields("created_at"),
            "created_by": "source-poller",
        }
        text = render(notice, None)
        if units(html.escape(text)) > MESSAGE_LIMIT:
            if any(error.get("response") or error.get("diagnostics") for error in fresh.values()):
                notice["document"] = {"filename": notice_id + ".txt", "text": text}
                text = render(notice, CAPTION_LIMIT)
            else:
                text = render(notice)
        notice["text"] = text
        await repo.put("out.ops_notices", notice_id, notice, immutable=True)
        await repo.enqueue(
            "DELIVER",
            {"notice_id": notice_id, "channel": "telegram:ops"},
            notice_id,
            priority=2,
            run_after=stamp,
            max_attempts=5,
            expires_at=offset_schedule(stamp, timedelta(days=7)),
        )
    return notice_id

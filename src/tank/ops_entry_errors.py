"""Tank Ops reports of source entries that a poll could not turn into items.

A poll that leaves entry errors in the feed checkpoint reports the errors not seen
before in one ``ENTRY_ERRORS`` notice for that source poll. Each reported error is
remembered by an ``ENTRY_ERROR`` marker whose id is derived from the source, the
feed URL, the reason and the entry identity (the digest of the wire entry when the
parser had one), never from the poll time. Feeds that keep returning the same broken
entry therefore produce no further messages. Markers, the notice and its ``DELIVER``
job are written in one transaction.
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
}
MESSAGE_LIMIT = 3900


def identity(error):
    if error.get("entry_sha256"):
        return {"entry_sha256": error["entry_sha256"]}
    if error.get("proposal_id"):
        return {"proposal_id": error["proposal_id"]}
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


def seen_label(value):
    return parse_time(value).strftime("%Y-%m-%d %H:%M:%S UTC")


def entry_block(number, error, payload_shown):
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
    elif "account" in error:
        parts.append("весь запуск сбора X")
    if parts:
        lines.append("Запись: " + " · ".join(parts))
    if entry.get("excerpt"):
        lines.append("Фрагмент: " + plain(entry["excerpt"], 200))
    if error.get("error"):
        lines.append("Ошибка: " + plain(error["error"], 300))
    if error.get("entry_sha256"):
        lines.append("sha256 записи: " + error["entry_sha256"])
    if error.get("payload_sha256") and not payload_shown:
        lines.append("sha256 ответа: " + error["payload_sha256"])
    return "\n".join(lines)


def render(notice):
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
    text = "\n".join(head)
    for index, error in enumerate(errors):
        block = entry_block(index + 1, error, shared)
        rest = len(errors) - index - 1
        tail = f"\n\n… и ещё {rest}; полный список: out.ops_notices {notice['notice_id']}" if rest else ""
        candidate = text + "\n\n" + block
        if units(html.escape(candidate + tail)) > MESSAGE_LIMIT:
            return (
                text
                + f"\n\n… и ещё {len(errors) - index}; полный список: out.ops_notices {notice['notice_id']}"
            )
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
        notice["text"] = render(notice)
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

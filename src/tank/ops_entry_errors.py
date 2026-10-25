"""Durable idempotent operator reports of malformed publisher entries."""

import html
import re
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Protocol

from pydantic import JsonValue

from tank.contracts.sources import SourceDefinition, SourceFeed
from tank.ops_cards import plain
from tank.ops_progress import enabled
from tank.settings import Settings
from tank.sources.errors import (
    AccountEntryError,
    EntryErrorMarker,
    EntryErrorNotice,
    EntryIdentity,
    SourceEntryError,
    SourceErrorIdentity,
    XDiagnostics,
    parse_entry_error,
)
from tank.sources.x_diagnostics import documents, sanitize
from tank.time_precision import Timestamp, collection_clock, offset_schedule
from tank.util import TimeInput, clock_iso, parse_time, sha256

REASONS = {
    "ENTRY_WITHOUT_ID_OR_URL": "Запись без идентификатора и ссылки",
    "INVALID_PUBLISHER_ENTRY": "Запись не соответствует формату источника",
    "MALFORMED_FEED_DOCUMENT": "Документ ленты повреждён, записи разобраны частично",
    "PENDING_PROPOSAL_UNAVAILABLE": "Отслеживаемое предложение не вернулось в ответе источника",
    "MISSING_FROM_RUN": "Аккаунт X отсутствует в результате сбора",
    "NO_CURSOR": "Для аккаунта X не получен курсор",
    "CURSOR_REGRESSION": "Источник X вернул курсор раньше уже прочитанной записи",
    "UNREQUESTED_ACCOUNT_RESULT": "Сбор X вернул данные вне запрошенного набора аккаунтов",
}
MESSAGE_LIMIT = 3900


class EntryNoticeRepository(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[None]: ...
    def transaction(self) -> AbstractAsyncContextManager[None]: ...
    async def enqueue(
        self,
        kind: str,
        payload: dict[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        run_after: Timestamp,
        max_attempts: int,
        expires_at: Timestamp,
    ) -> JsonValue: ...


def identity(error: SourceEntryError) -> SourceErrorIdentity:
    return error.identity()


def error_key(source_id: str, feed_url: str, error: SourceEntryError) -> str:
    """Preserve the existing flat canonical hash document for already-issued markers."""
    record: dict[str, JsonValue] = {
        "source_id": source_id,
        "feed": feed_url,
        "reason": re.sub(r"\d+", "#", error.reason),
        **identity(error).model_dump(mode="json"),
    }
    return sha256(record)


def units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def seen_label(value: TimeInput) -> str:
    return parse_time(value).strftime("%Y-%m-%d %H:%M:%S UTC")


def entry_block(number: int, error: SourceEntryError, payload_shown: bool) -> str:
    reason = error.reason
    label = REASONS.get(reason)
    lines = [f"{number}. {label} ({reason})" if label else f"{number}. {plain(reason, 200)}"]
    entry = error.entry if error.entry is not None else EntryIdentity()
    parts: list[str] = []
    if error.entry_index is not None:
        parts.append(f"№{error.entry_index} в ответе")
    if entry.title:
        parts.append("«" + plain(entry.title, 160) + "»")
    for value in (entry.id, entry.link):
        if value:
            parts.append(plain(value, 200))
    error_identity = error.identity()
    from tank.sources.errors import ProposalIdentity

    if isinstance(error_identity, ProposalIdentity):
        parts.append("предложение " + plain(error_identity.proposal_id, 120))
    if isinstance(error, AccountEntryError):
        parts.append("аккаунт @" + plain(error.account, 60) if error.account else "весь запуск сбора X")
    if parts:
        lines.append("Запись: " + " · ".join(parts))
    if entry.excerpt:
        lines.append("Фрагмент: " + plain(entry.excerpt, 200))
    if error.error:
        lines.append("Ошибка: " + plain(error.error, 300))
    from tank.sources.errors import DigestIdentity

    if isinstance(error_identity, DigestIdentity):
        lines.append("sha256 записи: " + error_identity.entry_sha256)
    if error.payload_sha256 is not None and not payload_shown:
        lines.append("sha256 ответа: " + error.payload_sha256)
    if isinstance(error, AccountEntryError) and error.diagnostics is not None:
        diagnostics = error.diagnostics
        url = diagnostics.request.url
        outcome = diagnostics.outcome
        attempts = diagnostics.attemptCount
        lines.extend(
            [
                "HTTP fetch failure: GET " + plain(url, 300),
                "Тело запроса GET: отсутствует (не тело HTTP-ответа)",
                f"Попыток: {attempts}; исход: {plain(outcome, 120)}",
                "Тела HTTP-ответов и заголовки: приложенные документы; усечённые ответы обозначены bodyTruncated.",
            ]
        )
    return "\n".join(lines)


def render(notice: EntryErrorNotice) -> str:
    source = notice.source_id + (f" · {notice.source_name}" if notice.source_name else "")
    errors = notice.entry_errors
    shared_payload = errors[0].payload_sha256 if errors else None
    shared = shared_payload is not None and all(error.payload_sha256 == shared_payload for error in errors)
    head = [
        "TANK OPS · Ошибка HTTP-сбора X"
        if any(isinstance(error, AccountEntryError) and error.diagnostics is not None for error in errors)
        else "TANK OPS · Записи источника не разобраны",
        "Источник: " + plain(source, 200),
        "Лента: " + notice.feed_url,
        "Обнаружено: " + seen_label(notice.seen_at),
        f"Новых записей с ошибкой: {len(errors)}",
    ]
    if shared and shared_payload is not None:
        head.append("sha256 ответа: " + shared_payload)
    text = "\n".join(head)
    if any(isinstance(error, AccountEntryError) and error.diagnostics is not None for error in errors):
        text += "\nHTTP response bodies / headers: документы к этому сообщению. GET request body: empty."
    for index, error in enumerate(errors):
        block = entry_block(index + 1, error, shared)
        rest = len(errors) - index - 1
        tail = f"\n\n… и ещё {rest}; полный список: out.ops_notices {notice.notice_id}" if rest else ""
        candidate = text + "\n\n" + block
        if units(html.escape(candidate + tail)) > MESSAGE_LIMIT:
            return (
                text + f"\n\n… и ещё {len(errors) - index}; полный список: out.ops_notices {notice.notice_id}"
            )
        text = candidate
    return text


async def report_entry_errors(
    repo: EntryNoticeRepository,
    settings: Settings,
    source: SourceDefinition,
    feed: SourceFeed,
    errors: Sequence[SourceEntryError | Mapping[str, JsonValue]],
    seen_at: str,
) -> str | None:
    if not enabled(settings) or not errors:
        return None
    source_id, feed_url = source.source_id, feed.url
    pending: dict[str, SourceEntryError] = {}
    for error in errors:
        if isinstance(error, Mapping):
            wire = dict(error)
            if "diagnostics" in wire and wire["diagnostics"] is not None:
                diagnostics = wire["diagnostics"]
                if not isinstance(diagnostics, dict):
                    raise ValueError("X diagnostics require a recorded object")
                wire["diagnostics"] = XDiagnostics.model_validate(
                    sanitize(diagnostics, settings.apify_api_token)
                ).model_dump(mode="json")
            parsed = parse_entry_error(wire)
        else:
            parsed = parse_entry_error(error)
            if isinstance(parsed, AccountEntryError) and parsed.diagnostics is not None:
                parsed = AccountEntryError.model_validate(
                    parsed.model_copy(
                        update={
                            "diagnostics": XDiagnostics.model_validate(
                                sanitize(parsed.diagnostics.model_dump(mode="json"), settings.apify_api_token)
                            )
                        }
                    )
                )
        pending.setdefault(error_key(source_id, feed_url, parsed), parsed)
    async with repo.lock(f"ops-entry-errors:{source_id}:{sha256(feed_url)}"), repo.transaction():
        fresh: dict[str, SourceEntryError] = {}
        for key, error in pending.items():
            if await repo.get("out.ops_notices", "entry_error_" + key) is None:
                fresh[key] = error
        if not fresh:
            return None
        poll_identity: dict[str, JsonValue] = {
            "source_id": source_id,
            "feed": feed_url,
            "keys": [key for key in sorted(fresh)],
        }
        notice_id = "entry_errors_" + sha256(poll_identity)
        created_at = clock_iso()
        stamp = collection_clock(created_at)
        for key, error in fresh.items():
            marker = EntryErrorMarker(
                notice_id="entry_error_" + key,
                subject_id=source_id,
                text=error.reason,
                group_notice_id=notice_id,
                feed_url=feed_url,
                entry_error=error,
                created_at=created_at,
            )
            await repo.put("out.ops_notices", marker.notice_id, marker.storage_record(), immutable=True)
        notice = EntryErrorNotice(
            notice_id=notice_id,
            subject_id=source_id,
            source_id=source_id,
            source_name=source.name,
            feed_url=feed_url,
            seen_at=seen_at,
            entry_error_keys=sorted(fresh),
            entry_errors=list(fresh.values()),
            created_at=created_at,
            text="",
        )
        notice = notice.model_copy(update={"text": render(notice)})
        if attachments := documents(
            [error.model_dump(mode="json", exclude_unset=True) for error in notice.entry_errors]
        ):
            notice = EntryErrorNotice.model_validate(
                notice.model_copy(
                    update={
                        "document_manifest": [
                            {
                                "filename": row["filename"],
                                "sha256": row["sha256"],
                                "bytes": len(row["content"]),
                            }
                            for row in attachments
                        ]
                    }
                )
            )
        await repo.put("out.ops_notices", notice_id, notice.storage_record(), immutable=True)
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

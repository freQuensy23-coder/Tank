"""Durable idempotent operator reports of malformed publisher entries."""

import html
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from datetime import timedelta
from typing import Protocol, Self

from pydantic import JsonValue

from tank.contracts.progress import ProgressSettings
from tank.contracts.source_errors import (
    AccountEntryError,
    EntryErrorMarker,
    EntryErrorNotice,
    EntryErrorRenderContent,
    EntryIdentity,
    SourceEntryError,
    SourceErrorIdentity,
    parse_entry_error,
)
from tank.contracts.source_reports import (
    SourceReportDeduplicated,
    SourceReportingPolicy,
    SourceReportMarkerReference,
    SourceReportNoticeWitness,
    SourceReportNotRequested,
    SourceReportOutcome,
    SourceReportRecorded,
    SourceReportRequest,
    SourceReportScope,
    source_report_marker_reference,
    source_report_notice_id,
    source_report_notice_witness,
)
from tank.contracts.source_reports import source_error_key as error_key
from tank.diagnostic_documents import documents
from tank.ops_cards import plain
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
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
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
    from tank.contracts.source_errors import ProposalIdentity

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
    from tank.contracts.source_errors import DigestIdentity

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


def render(notice: EntryErrorRenderContent) -> str:
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


async def _report_references(
    repo: EntryNoticeRepository, scope: SourceReportScope, keys: Sequence[str]
) -> list[SourceReportMarkerReference]:
    references: list[SourceReportMarkerReference] = []
    notices: dict[str, SourceReportNoticeWitness] = {}
    for key in keys:
        raw_marker = await repo.get("out.ops_notices", "entry_error_" + key)
        if raw_marker is None:
            raise ValueError("Source report marker is absent after recording")
        marker = EntryErrorMarker.model_validate(raw_marker)
        if marker.group_notice_id not in notices:
            raw_notice = await repo.get("out.ops_notices", marker.group_notice_id)
            if raw_notice is None:
                raise ValueError("Source report marker has no durable grouped notice")
            notices[marker.group_notice_id] = source_report_notice_witness(scope, raw_notice)
        references.append(
            source_report_marker_reference(scope, key, raw_marker, notices[marker.group_notice_id])
        )
    return references


async def report_entry_errors(
    repo: EntryNoticeRepository, settings: ProgressSettings, request: SourceReportRequest
) -> SourceReportOutcome:
    request = SourceReportRequest.model_validate(request)
    policy = SourceReportingPolicy(ops_progress_enabled=settings.ops_progress_enabled, replay=settings.replay)
    subject = request.subject
    if policy.mode != "ENABLED":
        return SourceReportNotRequested(kind="NOT_REQUESTED", scope=subject.scope, reason=policy.mode)
    pending: dict[str, SourceEntryError] = {}
    for error in request.errors:
        if isinstance(error, Mapping):
            raise ValueError("Source error reporting requires admitted named records")
        parsed = parse_entry_error(error)
        pending.setdefault(error_key(subject.source_id, subject.feed_url, parsed), parsed)
    keys = sorted(pending)
    notice_id: str | None = None
    outcome: SourceReportOutcome
    async with (
        repo.lock(f"ops-entry-errors:{subject.source_id}:{sha256(subject.feed_url)}"),
        AsyncExitStack() as locks,
    ):
        for key in keys:
            await locks.enter_async_context(repo.lock("record:out.ops_notices:entry_error_" + key))
        fresh: dict[str, SourceEntryError] = {}
        groups: set[str] = set()
        for key in keys:
            raw = await repo.get("out.ops_notices", "entry_error_" + key)
            if raw is None:
                fresh[key] = pending[key]
            else:
                groups.add(EntryErrorMarker.model_validate(raw).group_notice_id)
        if fresh:
            notice_id = source_report_notice_id(subject.scope, sorted(fresh))
            groups.add(notice_id)
        for group in sorted(groups):
            await locks.enter_async_context(repo.lock("record:out.ops_notices:" + group))
        if notice_id is not None:
            await locks.enter_async_context(repo.lock("queue:DELIVER:" + notice_id))
        async with repo.transaction():
            if fresh:
                if notice_id is None:
                    raise ValueError("Source report has no planned group identity")
                created_at = clock_iso()
                stamp = collection_clock(created_at)
                for key, error in fresh.items():
                    marker = EntryErrorMarker(
                        notice_id="entry_error_" + key,
                        subject_id=subject.source_id,
                        text=error.reason,
                        group_notice_id=notice_id,
                        feed_url=subject.feed_url,
                        entry_error=error,
                        created_at=created_at,
                    )
                    await repo.put(
                        "out.ops_notices", marker.notice_id, marker.storage_record(), immutable=True
                    )
                content = EntryErrorRenderContent(
                    notice_id=notice_id,
                    source_id=subject.source_id,
                    source_name=subject.source_name,
                    feed_url=subject.feed_url,
                    seen_at=request.seen_at,
                    entry_errors=list(fresh.values()),
                )
                attachments = documents(content.entry_errors)
                notice = EntryErrorNotice(
                    notice_id=content.notice_id,
                    subject_id=subject.source_id,
                    source_id=content.source_id,
                    source_name=content.source_name,
                    feed_url=content.feed_url,
                    seen_at=content.seen_at,
                    entry_error_keys=sorted(fresh),
                    entry_errors=content.entry_errors,
                    created_at=created_at,
                    text=render(content),
                    document_manifest=[row.reference() for row in attachments] if attachments else None,
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
            references = await _report_references(repo, subject.scope, keys)
            outcome = (
                SourceReportDeduplicated(kind="DEDUPLICATED", scope=subject.scope, markers=references)
                if notice_id is None
                else SourceReportRecorded(
                    kind="RECORDED", scope=subject.scope, markers=references, notice_id=notice_id
                )
            )
    return outcome

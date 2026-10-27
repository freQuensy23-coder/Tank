"""Real report delivery resumes only with the consumed recipient and credential facts."""

from pathlib import Path

import httpx
import pytest
from delivery_support import delivery_job
from pydantic_settings import SettingsConfigDict
from source_support import source_definition

from tank.contracts.source_errors import (
    AccountEntryError,
    XDiagnosticAttempt,
    XDiagnosticRequest,
    XDiagnosticResponse,
    XDiagnostics,
)
from tank.delivery import DeliveryError, DeliveryService
from tank.ops_entry_errors import report_entry_errors
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import new_id, sha256


class ResumeSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def configuration(tmp_path: Path, token: str | None, chat: str | None) -> Settings:
    return ResumeSettings(
        data_dir=tmp_path / "data",
        backup_dir=tmp_path / "backup",
        telegram_token=token,
        telegram_ops_chat=chat,
        ops_progress_enabled=True,
    )


async def reported_delivery(repo: MemoryRepository, tmp_path: Path, *, complete: bool) -> str:
    source = source_definition(
        {
            "source_id": "S29",
            "name": "X",
            "feeds": [{"kind": "x", "url": "https://x.com/alice"}],
        }
    )
    body = "Actual diagnostic body"
    error = AccountEntryError(
        account="alice",
        reason="PUBLIC_PROFILE_HTTP_503",
        diagnostics=XDiagnostics(
            request=XDiagnosticRequest(method="GET", url="https://x.com/alice", body=None),
            outcome="exhausted",
            attemptCount=1,
            attempts=[
                XDiagnosticAttempt(
                    attempt=1,
                    at="2026-10-27T00:00:00Z",
                    status=503,
                    retryable=True,
                    error="PUBLIC_PROFILE_HTTP_503",
                    response=XDiagnosticResponse(
                        status=503,
                        headers={},
                        body=body,
                        bodyEncoding="utf-8",
                        bodyBytes=len(body.encode()),
                        bodySha256=sha256(body),
                        bodyTruncated=False,
                        redacted=False,
                    ),
                )
            ],
        ),
    )
    settings = configuration(tmp_path, "test", "10")
    notice_id = await report_entry_errors(
        repo, settings, source, source.feeds[0], [error], "2026-10-27T00:00:00Z"
    )
    assert notice_id is not None
    count = 0

    def send(request: httpx.Request) -> httpx.Response:
        nonlocal count
        assert request.url.host == "api.telegram.org"
        count += 1
        if request.url.path.endswith("sendDocument") and not complete:
            return httpx.Response(503, json={"ok": False, "parameters": {"retry_after": 1}})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": count}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(repo, settings, http=http)
        if complete:
            await service.deliver(delivery_job({"notice_id": notice_id}))
        else:
            with pytest.raises(DeliveryError):
                await service.deliver(delivery_job({"notice_id": notice_id}))
    return notice_id


async def restore_without_parent_fact(
    original: MemoryRepository, notice_id: str, missing_field: str | None
) -> MemoryRepository:
    restored = MemoryRepository()
    notice = await original.get("out.ops_notices", notice_id)
    assert notice is not None
    await restored.put("out.ops_notices", notice_id, notice, immutable=True)
    for stored in await original.list("out.deliveries"):
        row = dict(stored)
        if row.get("method") != "sendDocument":
            if missing_field is None:
                continue
            row.pop(missing_field, None)
        await restored.put("out.deliveries", new_id("delivery"), row, immutable=True)
    return restored


@pytest.mark.parametrize("mode", ["TOKEN", "CHAT", "BOTH", "CHANGED_CHAT", "UNREPORTED_PARENT_CHAT"])
async def test_pending_document_resume_refuses_before_new_attempt(mode: str, tmp_path: Path) -> None:
    repo = MemoryRepository()
    notice_id = await reported_delivery(repo, tmp_path, complete=False)
    if mode == "UNREPORTED_PARENT_CHAT":
        repo = await restore_without_parent_fact(repo, notice_id, "chat_id")
    before = await repo.list("out.deliveries")
    token = None if mode in {"TOKEN", "BOTH"} else "test"
    chat = None if mode in {"CHAT", "BOTH"} else "20" if mode == "CHANGED_CHAT" else "10"
    requests: list[httpx.Request] = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 20 + len(requests)}})

    captured: Exception | None = None
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(repo, configuration(tmp_path, token, chat), http=http)
        try:
            await service.deliver(delivery_job({"notice_id": notice_id}))
        except Exception as error:
            captured = error
    assert requests == []
    assert await repo.list("out.deliveries") == before
    assert isinstance(captured, ValueError)


async def test_all_documents_sent_needs_no_unused_parent_or_credentials(tmp_path: Path) -> None:
    original = MemoryRepository()
    notice_id = await reported_delivery(original, tmp_path, complete=True)
    repo = await restore_without_parent_fact(original, notice_id, None)
    before = await repo.list("out.deliveries")
    assert len(before) == 4

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Already completed document delivery must not use HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        await DeliveryService(repo, configuration(tmp_path, None, None), http=http).deliver(
            delivery_job({"notice_id": notice_id})
        )
    assert await repo.list("out.deliveries") == before

"""Outbound Telegram receipts require actual unambiguous message identity."""

import os
from pathlib import Path
from typing import Literal

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic_settings import SettingsConfigDict

from tank.question_answers import record_answer
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_api import TelegramProtocolError, TelegramResponseError
from tank.telegram_inbox import TelegramInbox


class AmbiguitySettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


type SendKind = Literal["text", "document"]


async def seed_question(repo: MemoryRepository | PostgresRepository) -> None:
    await repo.put(
        "out.user_questions",
        "q",
        {
            "question_id": "q",
            "session_id": "s",
            "status": "QUEUED",
            "chat_id": "42",
            "reply_to_message_id": 17,
            "attachments": [],
        },
    )
    await record_answer(repo, "q", "actual answer", status="ANSWERED")


async def send(inbox: TelegramInbox, kind: SendKind) -> None:
    if kind == "text":
        await inbox.answer({"question_id": "q"})
    else:
        await inbox.send_session_file("q", "tool-call", "report.txt", b"actual bytes")


@pytest.mark.parametrize("kind", ["text", "document"])
@pytest.mark.parametrize(
    "payload",
    [
        b'{"ok":true,"result":{"message_id":101,"message_id":102}}',
        b'{"ok":true,"result":{"message_id":101,"message_\\u0069d":102}}',
        b'{"ok":true,"result":{}}',
        b'{"ok":true,"result":{"message_id":0}}',
        b'{"ok":true,"result":{"message_id":true}}',
        b'{"ok":true,"result":{"message_id":1.0}}',
        b'{"ok":true,"result":{"message_id":"101"}}',
        b'{"ok":true,"result":null}',
    ],
)
async def test_ambiguous_or_invalid_receipt_never_authorizes_sent_or_blind_resend(
    tmp_path: Path, kind: SendKind, payload: bytes
) -> None:
    repo = MemoryRepository()
    await seed_question(repo)
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path.rsplit("/", 1)[-1])
        return httpx.Response(200, content=payload)

    settings = AmbiguitySettings(data_dir=tmp_path, telegram_token="synthetic-private-token")
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, settings, http=http)
        with pytest.raises(TelegramProtocolError) as error:
            await send(inbox, kind)
        assert "synthetic-private-token" not in str(error.value)
        deliveries = await repo.list("out.deliveries")
        assert [row["status"] for row in deliveries] == ["GENERATED"]
        await send(inbox, kind)
    assert len(calls) == 1
    assert await repo.list("out.deliveries") == deliveries
    question = await repo.get("out.user_questions", "q")
    assert question is not None and question["status"] == "ANSWERED"


@pytest.mark.parametrize("kind", ["text", "document"])
async def test_explicit_provider_failure_allows_retry_then_actual_receipt_once(
    tmp_path: Path, kind: SendKind
) -> None:
    repo = MemoryRepository()
    await seed_question(repo)
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if len(calls) == 1:
            return httpx.Response(
                429,
                json={
                    "ok": False,
                    "error_code": 429,
                    "description": "Too Many Requests",
                    "parameters": {"retry_after": 17},
                },
            )
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 101, "date": 1800000000}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(
            repo, AmbiguitySettings(data_dir=tmp_path, telegram_token="synthetic-private-token"), http=http
        )
        with pytest.raises(TelegramResponseError) as error:
            await send(inbox, kind)
        assert error.value.status == 429 and error.value.retry_after == 17
        await send(inbox, kind)
        await send(inbox, kind)
    assert len(calls) == 2
    rows = await repo.list("out.deliveries")
    statuses = [row["status"] for row in rows]
    assert len(statuses) == 4 and statuses.count("GENERATED") == 2
    assert statuses.count("FAILED") == 1 and statuses.count("SENT") == 1
    sent_rows = [row for row in rows if row["status"] == "SENT"]
    assert len(sent_rows) == 1 and sent_rows[0]["message_ref"] == "101"


@pytest.mark.parametrize("kind", ["text", "document"])
async def test_read_timeout_after_post_remains_uncertain(tmp_path: Path, kind: SendKind) -> None:
    repo = MemoryRepository()
    await seed_question(repo)
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("response not received", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(
            repo, AmbiguitySettings(data_dir=tmp_path, telegram_token="synthetic-private-token"), http=http
        )
        with pytest.raises(httpx.ReadTimeout):
            await send(inbox, kind)
        await send(inbox, kind)
    assert calls == 1
    rows = await repo.list("out.deliveries")
    assert [row["status"] for row in rows] == ["GENERATED"]
    question = await repo.get("out.user_questions", "q")
    assert question is not None and question["status"] == "ANSWERED"


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_unknown_receipt_preserves_generated_and_never_claims_delivery(
    postgres_template: PostgresTemplate, tmp_path: Path
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await seed_question(repo)
        calls = 0

        def respond(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            assert request.url.path.endswith("/sendMessage")
            return httpx.Response(200, content=b'{"ok":true,"result":{}}')

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            inbox = TelegramInbox(
                repo,
                AmbiguitySettings(data_dir=tmp_path, telegram_token="synthetic-private-token"),
                http=http,
            )
            with pytest.raises(TelegramProtocolError):
                await inbox.answer({"question_id": "q"})
            await inbox.answer({"question_id": "q"})
        assert calls == 1
        rows = await repo.list("out.deliveries")
        assert len(rows) == 1 and rows[0]["status"] == "GENERATED"
        question = await repo.get("out.user_questions", "q")
        assert question is not None and question["status"] == "ANSWERED"

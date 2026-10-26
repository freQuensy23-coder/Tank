"""Local Telegram credential failure never becomes a possible POST receipt."""

import os
from pathlib import Path
from typing import Literal

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.contracts.session_followups import FollowupRefusalTarget
from tank.question_answers import record_answer
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_api import TelegramConfigurationError
from tank.telegram_inbox import TelegramInbox


class PreflightSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


type Operation = Literal["answer", "document", "notice", "prompt"]


async def seed(repo: MemoryRepository | PostgresRepository, inbox: TelegramInbox) -> dict[str, JsonValue]:
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
    await inbox.store(
        {
            "chat": {"id": 42, "type": "private"},
            "from": {"id": 42},
            "message_id": 17,
            "date": 1800000000,
            "text": "actual input",
        },
        1,
    )
    row = await repo.get("ops.telegram_messages", "42:17")
    assert row is not None
    return row


async def operate(inbox: TelegramInbox, operation: Operation, row: dict[str, JsonValue]) -> None:
    if operation == "answer":
        await inbox.answer({"question_id": "q"})
    elif operation == "document":
        await inbox.send_session_file(
            FollowupRefusalTarget(question_id="q", session_id="s"), "call", "report.txt", b"actual bytes"
        )
    elif operation == "notice":
        await inbox.notice(row, "actual notice", "IGNORED", reason="NO_REFERENCE")
    else:
        await inbox.prompt(row, {"kind": "SESSION", "session_id": "s", "text": "actual reference"})


@pytest.mark.parametrize("token", [None, "", " \t"])
@pytest.mark.parametrize("operation", ["answer", "document", "notice", "prompt"])
async def test_new_outbound_intent_requires_credential_before_any_marker(
    tmp_path: Path, token: str | None, operation: Operation
) -> None:
    repo = MemoryRepository()

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Local credential failure cannot issue HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=token), http=http)
        row = await seed(repo, inbox)
        questions = await repo.list("out.user_questions")
        with pytest.raises(TelegramConfigurationError):
            await operate(inbox, operation, row)
    assert await repo.list("out.deliveries") == []
    assert await repo.list("out.user_questions") == questions
    assert await repo.get("ops.telegram_messages", "42:17") == row


@pytest.mark.parametrize("operation", ["answer", "document", "notice", "prompt"])
@pytest.mark.parametrize("uncertain", [False, True])
async def test_existing_receipt_or_uncertainty_reconciles_without_new_credential(
    tmp_path: Path, operation: Operation, uncertain: bool
) -> None:
    repo = MemoryRepository()
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if uncertain:
            raise httpx.ReadTimeout("response unavailable", request=request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 101, "date": 1800000000}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        active = TelegramInbox(
            repo, PreflightSettings(data_dir=tmp_path, telegram_token="synthetic-private-token"), http=http
        )
        row = await seed(repo, active)
        if uncertain:
            with pytest.raises(httpx.ReadTimeout):
                await operate(active, operation, row)
        else:
            await operate(active, operation, row)
        ledger = await repo.list("out.deliveries")
        inactive = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
        await operate(inactive, operation, row)
    assert calls == 1 and await repo.list("out.deliveries") == ledger
    if uncertain:
        assert all(delivery["status"] == "GENERATED" for delivery in ledger)


@pytest.mark.parametrize("operation", ["getMe", "getUpdates"])
async def test_direct_api_call_has_no_fabricated_missing_token_url(tmp_path: Path, operation: str) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Missing credential cannot issue HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(
            MemoryRepository(), PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http
        )
        with pytest.raises(TelegramConfigurationError):
            await inbox.call(operation, {})


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_unconfigured_outbound_operations_leave_no_new_marker_or_prompt(
    postgres_template: PostgresTemplate, tmp_path: Path
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:

        def refuse(request: httpx.Request) -> httpx.Response:
            raise AssertionError("Unconfigured operation must not issue HTTP")

        async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
            inbox = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
            row = await seed(repo, inbox)
            questions = await repo.list("out.user_questions")
            operations: tuple[Operation, ...] = ("answer", "document", "notice", "prompt")
            for operation in operations:
                with pytest.raises(TelegramConfigurationError):
                    await operate(inbox, operation, row)
                assert await repo.list("out.deliveries") == []
                assert await repo.list("out.user_questions") == questions
                assert await repo.get("ops.telegram_messages", "42:17") == row

"""Stored answer receipts cannot acquire authority from another recipient's history."""

import os
from pathlib import Path

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue, ValidationError
from pydantic_settings import SettingsConfigDict

from tank.question_answers import record_answer
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_inbox import TelegramInbox
from tank.time_precision import collection_clock
from tank.util import utcnow


class StateSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


async def subject(repo: MemoryRepository | PostgresRepository) -> None:
    await repo.put(
        "out.user_questions",
        "q",
        {"question_id": "q", "session_id": "s", "chat_id": "42", "status": "QUEUED"},
    )
    await record_answer(repo, "q", "actual answer", status="ANSWERED")


@pytest.mark.parametrize(
    ("field", "value"),
    [("session_id", "foreign"), ("chat_id", "99"), ("message_ref", "0"), ("message_ref", True)],
)
async def test_foreign_or_invalid_sent_receipt_never_settles_question(field: str, value: JsonValue) -> None:
    repo = MemoryRepository()
    await subject(repo)
    receipt: dict[str, JsonValue] = {
        "question_id": "q",
        "session_id": "s",
        "chat_id": "42",
        "purpose": "ANSWER",
        "channel": "telegram:answer",
        "chunk": 0,
        "attempt": 1,
        "status": "SENT",
        "message_ref": "17",
    }
    receipt[field] = value
    await repo.put("out.deliveries", "d", receipt)
    before = await repo.get("out.user_questions", "q")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("An invalid stored receipt must refuse before HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, StateSettings(telegram_token=None), http=http)
        with pytest.raises((ValueError, ValidationError)):
            await inbox.answer({"question_id": "q"})
    assert await repo.get("out.user_questions", "q") == before
    assert await repo.get("out.deliveries", "d") == receipt


@pytest.mark.parametrize("other_purpose", ["ATTACHMENT", "NOTICE", "PROMPT"])
async def test_matching_sent_receipt_settles_without_new_http_or_credential(other_purpose: str) -> None:
    repo = MemoryRepository()
    await subject(repo)
    await repo.put(
        "out.deliveries",
        "d",
        {
            "question_id": "q",
            "session_id": "s",
            "chat_id": 42,
            "purpose": "ANSWER",
            "channel": "telegram:answer",
            "chunk": 0,
            "attempt": 1,
            "status": "SENT",
            "message_ref": 17,
        },
    )

    await repo.put(
        "out.deliveries",
        "separate-purpose",
        {
            "question_id": "q",
            "session_id": "s",
            "chat_id": "42",
            "channel": "telegram:answer",
            "purpose": other_purpose,
            "status": "GENERATED",
            "attempt": 1,
        },
    )

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Verified prior delivery needs no POST")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, StateSettings(telegram_token=None), http=http)
        await inbox.answer({"question_id": "q"})
    row = await repo.get("out.user_questions", "q")
    assert row is not None and row["status"] == "DELIVERED"


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_foreign_receipt_refuses_then_actual_receipt_reconciles(
    postgres_template: PostgresTemplate, tmp_path: Path
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await subject(repo)
        receipt: dict[str, JsonValue] = {
            "question_id": "q",
            "session_id": "foreign",
            "chat_id": "42",
            "purpose": "ANSWER",
            "channel": "telegram:answer",
            "chunk": 0,
            "attempt": 1,
            "status": "SENT",
            "message_ref": "17",
        }
        receipt.update(collection_clock(utcnow()).fields("at"))
        await repo.put("out.deliveries", "d", receipt)
        before = await repo.get("out.user_questions", "q")

        def refuse(request: httpx.Request) -> httpx.Response:
            raise AssertionError("Stored receipt reconciliation must not issue HTTP")

        async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
            inbox = TelegramInbox(repo, StateSettings(data_dir=tmp_path, telegram_token=None), http=http)
            with pytest.raises(ValueError, match="actual recipient"):
                await inbox.answer({"question_id": "q"})
            assert await repo.get("out.user_questions", "q") == before
            assert await repo.get("out.deliveries", "d") == receipt
            await repo.put(
                "out.user_questions",
                "q2",
                {"question_id": "q2", "session_id": "s", "chat_id": "42", "status": "QUEUED"},
            )
            await record_answer(repo, "q2", "actual second answer", status="ANSWERED")
            await repo.put("out.deliveries", "d2", {**receipt, "question_id": "q2", "session_id": "s"})
            await inbox.answer({"question_id": "q2"})
        question = await repo.get("out.user_questions", "q2")
        assert question is not None and question["status"] == "DELIVERED"
        assert await repo.get("out.deliveries", "d") == receipt
        assert await repo.get("out.user_questions", "q") == before


@pytest.mark.parametrize("status", ["GENERATED", "SENT"])
@pytest.mark.parametrize("missing_fact", ["chunk", "null_chunk", "purpose", "null_purpose"])
async def test_ambiguous_answer_history_refuses_before_new_post(status: str, missing_fact: str) -> None:
    repo = MemoryRepository()
    await subject(repo)
    receipt: dict[str, JsonValue] = {
        "question_id": "q",
        "session_id": "s",
        "chat_id": "42",
        "purpose": "ANSWER",
        "channel": "telegram:answer",
        "chunk": 0,
        "attempt": 1,
        "status": status,
        "message_ref": "17",
    }
    if missing_fact.startswith("null_"):
        receipt[missing_fact.removeprefix("null_")] = None
    else:
        del receipt[missing_fact]
    await repo.put("out.deliveries", "d", receipt)
    before = await repo.get("out.user_questions", "q")
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 18}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, StateSettings(telegram_token="synthetic-test-token"), http=http)
        with pytest.raises(ValueError):
            await inbox.answer({"question_id": "q"})
    assert calls == 0
    assert await repo.get("out.user_questions", "q") == before
    assert await repo.list("out.deliveries") == [receipt]


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("missing_fact", ["chunk", "null_chunk", "purpose", "null_purpose"])
async def test_postgres_ambiguous_attempt_preserves_history_and_refuses_new_send(
    postgres_template: PostgresTemplate, tmp_path: Path, missing_fact: str
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await subject(repo)
        receipt: dict[str, JsonValue] = {
            "question_id": "q",
            "session_id": "s",
            "chat_id": "42",
            "purpose": "ANSWER",
            "channel": "telegram:answer",
            "chunk": 0,
            "attempt": 1,
            "status": "GENERATED",
            **collection_clock(utcnow()).fields("at"),
        }
        if missing_fact.startswith("null_"):
            receipt[missing_fact.removeprefix("null_")] = None
        else:
            del receipt[missing_fact]
        await repo.put("out.deliveries", "d", receipt)
        before = await repo.get("out.user_questions", "q")
        calls = 0

        def respond(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 18}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            inbox = TelegramInbox(
                repo, StateSettings(data_dir=tmp_path, telegram_token="synthetic-test-token"), http=http
            )
            with pytest.raises(ValueError):
                await inbox.answer({"question_id": "q"})
        assert calls == 0
        assert await repo.get("out.user_questions", "q") == before
        assert await repo.list("out.deliveries") == [receipt]

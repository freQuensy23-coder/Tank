"""File delivery admits both authoritative identities and every prior attempt."""

import asyncio
from pathlib import Path

import httpx
import pytest
from psycopg.errors import CheckViolation
from pydantic import JsonValue, ValidationError
from pydantic_settings import SettingsConfigDict
from test_storage import repo as repo

from tank.contracts.session_followups import FollowupRefusalTarget
from tank.contracts.telegram_state import TelegramFileAlreadySent, TelegramFileSent, TelegramFileUncertain
from tank.settings import Settings
from tank.storage import PostgresRepository, StorageBackend
from tank.telegram_inbox import TelegramInbox
from tank.time_precision import collection_clock


class FileSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def question() -> dict[str, JsonValue]:
    return {"question_id": "q", "session_id": "s", "chat_id": "42", "reply_to_message_id": 17}


def attempt() -> dict[str, JsonValue]:
    return {
        "question_id": "q",
        "session_id": "s",
        "channel": "telegram:answer",
        "chat_id": "42",
        "call_id": "call",
        "purpose": "ATTACHMENT",
        "filename": "actual.txt",
        "status": "SENT",
        "attempt": 1,
        "message_ref": "101",
        **collection_clock().fields("at"),
    }


@pytest.mark.parametrize("field", ["question_id", "session_id"])
async def test_foreign_stored_question_refuses_before_http_or_attempt(
    repo: StorageBackend, tmp_path: Path, field: str
) -> None:
    row = question() | {field: "foreign"}
    await repo.put("out.user_questions", "q", row)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Foreign question cannot send a file")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, FileSettings(data_dir=tmp_path, telegram_token="test"), http=http)
        with pytest.raises(ValueError, match="target"):
            await inbox.send_session_file(
                FollowupRefusalTarget(question_id="q", session_id="s"), "call", "actual.txt", b"bytes"
            )
    assert await repo.get("out.user_questions", "q") == row
    assert await repo.list("out.deliveries") == []


@pytest.mark.parametrize(
    "fault",
    ["session_id", "chat_id", "purpose", "filename", "missing-message", "invalid-message", "unknown-phase"],
)
async def test_cached_attempt_cannot_authorize_wrong_target_or_unknown_receipt(
    repo: StorageBackend, tmp_path: Path, fault: str
) -> None:
    await repo.put("out.user_questions", "q", question())
    row = attempt()
    if fault == "missing-message":
        del row["message_ref"]
    elif fault == "invalid-message":
        row["message_ref"] = "0"
    elif fault == "unknown-phase":
        row["status"] = "UNKNOWN"
    else:
        row[fault] = "foreign"
    if fault == "unknown-phase" and isinstance(repo, PostgresRepository):
        with pytest.raises(CheckViolation, match="valid_status"):
            await repo.put("out.deliveries", "old", row, immutable=True)
        assert await repo.list("out.deliveries") == []
        return
    await repo.put("out.deliveries", "old", row, immutable=True)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Unknown prior receipt cannot send again")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, FileSettings(data_dir=tmp_path), http=http)
        with pytest.raises((ValueError, ValidationError)):
            await inbox.send_session_file(
                FollowupRefusalTarget(question_id="q", session_id="s"), "call", "actual.txt", b"bytes"
            )
    assert await repo.list("out.deliveries") == [row]


async def test_matching_cached_sent_and_generated_need_no_new_credential_or_http(
    repo: StorageBackend, tmp_path: Path
) -> None:
    await repo.put("out.user_questions", "q", question())
    sent = attempt()
    await repo.put("out.deliveries", "sent", sent, immutable=True)
    generated = attempt() | {"status": "GENERATED", "call_id": "pending"}
    del generated["message_ref"]
    await repo.put("out.deliveries", "pending", generated, immutable=True)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Cached reconciliation cannot issue HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, FileSettings(data_dir=tmp_path), http=http)
        target = FollowupRefusalTarget(question_id="q", session_id="s")
        result = await inbox.send_session_file(target, "call", "actual.txt", b"bytes")
        pending = await inbox.send_session_file(target, "pending", "actual.txt", b"bytes")
    assert isinstance(result, TelegramFileAlreadySent) and result.message_id == "101"
    assert isinstance(pending, TelegramFileUncertain)
    assert len(await repo.list("out.deliveries")) == 2


async def test_concurrent_same_call_uses_one_actual_post_and_locked_scope(
    repo: StorageBackend, tmp_path: Path
) -> None:
    await repo.put("out.user_questions", "q", question())
    calls: list[bytes] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        calls.append(await request.aread())
        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 101}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, FileSettings(data_dir=tmp_path, telegram_token="test"), http=http)
        target = FollowupRefusalTarget(question_id="q", session_id="s")
        first, second = await asyncio.gather(
            inbox.send_session_file(target, "call", "actual.txt", b"actual bytes"),
            inbox.send_session_file(target, "call", "actual.txt", b"actual bytes"),
        )
    assert isinstance(first, TelegramFileSent) and isinstance(second, TelegramFileAlreadySent)
    assert first.message_id == second.message_id == "101"
    assert first.bytes == len(b"actual bytes")
    assert len(calls) == 1 and b"actual bytes" in calls[0]
    assert [row["status"] for row in await repo.list("out.deliveries")] == ["GENERATED", "SENT"]


async def test_question_binding_is_read_after_waiting_for_its_lock(
    repo: StorageBackend, tmp_path: Path
) -> None:
    await repo.put("out.user_questions", "q", question())
    entered = asyncio.Event()

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Changed authoritative binding cannot send")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, FileSettings(data_dir=tmp_path, telegram_token="test"), http=http)

        async def deliver() -> None:
            entered.set()
            await inbox.send_session_file(
                FollowupRefusalTarget(question_id="q", session_id="s"), "call", "actual.txt", b"bytes"
            )

        async with repo.lock("question:q"):
            task = asyncio.create_task(deliver())
            await entered.wait()
            await asyncio.sleep(0)
            assert not task.done()
            changed = question() | {"session_id": "different-current-owner"}
            await repo.put("out.user_questions", "q", changed)
        with pytest.raises(ValueError, match="target"):
            await task
    assert await repo.get("out.user_questions", "q") == changed
    assert await repo.list("out.deliveries") == []

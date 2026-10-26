"""Notice reconciliation must admit the actual stored delivery authority."""

from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue
from test_storage import repo as repo
from test_telegram_preflight import PreflightSettings

from tank.storage import MemoryRepository, StorageBackend
from tank.telegram_inbox import ANSWER_CHANNEL, TelegramInbox, question_id_for
from tank.time_precision import collection_clock


async def invalid_notice_receipt(repo: StorageBackend, tmp_path: Path, fault: str) -> None:

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Invalid retained receipt cannot authorize a new POST")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
        await inbox.store(
            {
                "chat": {"id": 42, "type": "private"},
                "message_id": 17,
                "date": 1800000000,
                "text": "Actual input",
            },
            1,
        )
        row = await repo.get("ops.telegram_messages", "42:17")
        assert row is not None
        receipt: dict[str, JsonValue] = {
            "question_id": question_id_for("42", 17),
            "channel": ANSWER_CHANNEL,
            "purpose": "NOTICE",
            "chat_id": "42",
            "attempt": 1,
            "status": "SENT",
            "message_ref": "101",
            "text": "Actual notice",
            **collection_clock().fields("at"),
        }
        if fault == "foreign-chat":
            receipt["chat_id"] = "99"
        elif fault == "missing-message-ref":
            receipt.pop("message_ref")
        elif fault == "zero-message-ref":
            receipt["message_ref"] = "0"
        elif fault == "missing-attempt":
            receipt.pop("attempt")
        elif fault == "missing-purpose":
            receipt.pop("purpose")
        else:
            receipt["purpose"] = "UNATTESTED"
        await repo.put("out.deliveries", "malformed-notice", receipt, immutable=True)
        before = await repo.list("out.deliveries")
        with pytest.raises(ValueError):
            await inbox.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
    assert await repo.get("ops.telegram_messages", "42:17") == row
    assert await repo.list("out.deliveries") == before


@pytest.mark.parametrize(
    "fault", ["foreign-chat", "missing-message-ref", "zero-message-ref", "missing-purpose", "unknown-purpose"]
)
async def test_malformed_notice_receipt_cannot_settle_the_actual_input(
    repo: StorageBackend, tmp_path: Path, fault: str
) -> None:
    await invalid_notice_receipt(repo, tmp_path, fault)


async def test_unreported_notice_attempt_is_not_fabricated_for_an_unconstrained_old_row(
    tmp_path: Path,
) -> None:
    await invalid_notice_receipt(MemoryRepository(), tmp_path, "missing-attempt")


@pytest.mark.parametrize("uncertain", [False, True])
async def test_actual_notice_phase_reconciles_without_new_token_or_post(
    repo: StorageBackend, tmp_path: Path, uncertain: bool
) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if uncertain:
            raise httpx.ReadTimeout("Synthetic unavailable receipt", request=request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 101, "date": 1800000001}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        active = TelegramInbox(
            repo, PreflightSettings(data_dir=tmp_path, telegram_token="synthetic-private-token"), http=http
        )
        await active.store(
            {
                "chat": {"id": 42, "type": "private"},
                "message_id": 17,
                "date": 1800000000,
                "text": "Actual input",
            },
            1,
        )
        row = await repo.get("ops.telegram_messages", "42:17")
        assert row is not None
        if uncertain:
            with pytest.raises(httpx.ReadTimeout):
                await active.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
        else:
            await active.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
        before = await repo.list("out.deliveries")
        inactive = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
        await inactive.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
        assert await repo.list("out.deliveries") == before
    assert calls == 1
    current = await repo.get("ops.telegram_messages", "42:17")
    assert current is not None and current["status"] == "IGNORED"
    if uncertain:
        assert {receipt["status"] for receipt in before} == {"GENERATED"}
    else:
        assert {receipt["status"] for receipt in before} == {"GENERATED", "SENT"}

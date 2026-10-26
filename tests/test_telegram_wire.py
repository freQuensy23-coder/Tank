"""Actual Telegram intake refuses ambiguous or incomplete ownership facts before effects."""

import json
from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue, ValidationError
from pydantic_settings import SettingsConfigDict

from tank.contracts.telegram_wire import TelegramInaccessibleMessage, TelegramUnsupportedUpdate
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.telegram_api import TelegramProtocolError, TelegramResponseError, response_result, update_receipts
from tank.telegram_inbox import CURSOR_KEY, TelegramInbox


class TelegramSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def incoming() -> dict[str, JsonValue]:
    return {
        "message_id": 17,
        "date": 1_800_000_000,
        "chat": {"id": -1001, "type": "supergroup"},
        "text": "actual text",
    }


async def poll_payload(payload: bytes, tmp_path: Path, repo: MemoryRepository) -> MemoryRepository:

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/getUpdates")
        return httpx.Response(200, content=payload)

    settings = TelegramSettings(data_dir=tmp_path, telegram_token="synthetic-test-token")
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, settings, http=http)
        await inbox.poll(timeout=0)
    return repo


@pytest.mark.parametrize("field", ["date", "chat", "message_id"])
async def test_missing_required_message_fact_refuses_entire_batch_before_storage(
    tmp_path: Path, field: str
) -> None:
    first = incoming()
    broken = incoming()
    del broken[field]
    repo = MemoryRepository()
    payload = json.dumps(
        {"ok": True, "result": [{"update_id": 8, "message": first}, {"update_id": 9, "message": broken}]}
    ).encode()

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, TelegramSettings(data_dir=tmp_path), http=http)
        with pytest.raises(TelegramProtocolError, match="batch is invalid"):
            await inbox.poll(timeout=0)
    assert await repo.list("ops.telegram_messages") == []
    assert await repo.get("ops.source_checkpoints", CURSOR_KEY) is None


@pytest.mark.parametrize("bad_date", [0, -1, True, 1.0, "1800000000", None])
def test_ordinary_message_date_requires_reported_positive_integer(bad_date: JsonValue) -> None:
    message = incoming()
    message["date"] = bad_date
    with pytest.raises(TelegramProtocolError):
        update_receipts([{"update_id": 1, "message": message}])


def test_inaccessible_message_has_separate_zero_date_contract() -> None:
    value = TelegramInaccessibleMessage.model_validate(
        {"message_id": 17, "date": 0, "chat": {"id": -1001, "type": "supergroup"}}
    )
    assert value.date == 0
    with pytest.raises(ValidationError):
        TelegramInaccessibleMessage.model_validate(
            {"message_id": 17, "date": False, "chat": {"id": -1001, "type": "supergroup"}}
        )


async def test_sender_absence_preserves_unknown_and_original_message(tmp_path: Path) -> None:
    message = incoming()
    message["provider_extension"] = {"opaque": "unchanged"}
    repo = await poll_payload(
        json.dumps({"ok": True, "result": [{"update_id": 8, "message": message}]}).encode(),
        tmp_path,
        MemoryRepository(),
    )
    row = await repo.get("ops.telegram_messages", "-1001:17")
    assert row is not None and row["user_id"] is None
    assert row["date"] == 1_800_000_000 and row["message"] == message
    state = await repo.get("ops.source_checkpoints", CURSOR_KEY)
    assert state is not None and state["cursor"] == "9"


async def test_scheduled_unusable_message_and_unsupported_update_advance_without_message_effects(
    tmp_path: Path,
) -> None:
    message = incoming()
    message["message_id"] = 0
    updates: list[JsonValue] = [
        {"update_id": 8, "message": message},
        {"update_id": 9, "edited_message": incoming()},
    ]
    receipts = update_receipts(updates)
    assert isinstance(receipts[1].payload, TelegramUnsupportedUpdate)
    assert receipts[1].payload.kind == "edited_message"
    repo = await poll_payload(
        json.dumps({"ok": True, "result": updates}).encode(), tmp_path, MemoryRepository()
    )
    assert await repo.list("ops.telegram_messages") == []
    state = await repo.get("ops.source_checkpoints", CURSOR_KEY)
    assert state is not None and state["cursor"] == "10"


@pytest.mark.parametrize("fragment", [b'"update_id":8,"update_id":9', b'"update_id":8,"upda\\u0074e_id":9'])
async def test_duplicate_wire_keys_refuse_before_any_cursor_or_message_effect(
    tmp_path: Path, fragment: bytes
) -> None:
    payload = (
        b'{"ok":true,"result":[{'
        + fragment
        + b',"message":{"message_id":17,"date":1800000000,"chat":{"id":-1001,"type":"supergroup"}}}]}'
    )
    repo = MemoryRepository()
    with pytest.raises(TelegramProtocolError) as error:
        await poll_payload(payload, tmp_path, repo)
    assert "1800000000" not in str(error.value)
    assert await repo.list("ops.telegram_messages") == []
    assert await repo.get("ops.source_checkpoints", CURSOR_KEY) is None


def test_reported_retry_delay_and_http_status_are_actual_facts() -> None:
    payload = b'{"ok":false,"error_code":429,"description":"synthetic private description","parameters":{"retry_after":17}}'
    with pytest.raises(TelegramResponseError) as error:
        response_result(payload, method="getUpdates", status=429)
    assert error.value.status == 429 and error.value.retry_after == 17
    assert "private" not in str(error.value)


@pytest.mark.parametrize("update_id", [None, True, -1, 1.0, "8"])
def test_update_identity_is_required_without_numeric_coercion(update_id: JsonValue) -> None:
    with pytest.raises(TelegramProtocolError):
        update_receipts([{"update_id": update_id, "message": incoming()}])


@pytest.mark.parametrize("sender", [None, {}, {"id": True}, {"id": 0}, "unknown"])
def test_present_sender_cannot_be_replaced_with_unknown_identity(sender: JsonValue) -> None:
    message = incoming()
    message["from"] = sender
    with pytest.raises(TelegramProtocolError):
        update_receipts([{"update_id": 8, "message": message}])


def test_supported_message_null_and_multiple_update_payloads_refuse() -> None:
    values: list[dict[str, JsonValue]] = [
        {"update_id": 8, "message": None},
        {"update_id": 8, "message": incoming(), "edited_message": incoming()},
    ]
    for value in values:
        with pytest.raises(TelegramProtocolError):
            update_receipts([value])


async def test_two_messages_with_unreported_sender_do_not_gain_pairing_authority(tmp_path: Path) -> None:
    first = incoming()
    first["forward_origin"] = {"type": "hidden_user", "date": 1_800_000_000, "sender_user_name": "unknown"}
    second = incoming()
    second["message_id"] = 18
    repo = await poll_payload(
        json.dumps(
            {"ok": True, "result": [{"update_id": 8, "message": first}, {"update_id": 9, "message": second}]}
        ).encode(),
        tmp_path,
        MemoryRepository(),
    )
    row = await repo.get("ops.telegram_messages", "-1001:17")
    assert row is not None

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Pairing cannot issue HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, TelegramSettings(data_dir=tmp_path), http=http)
        assert await inbox.companion(row, forwarded=True) is None


@pytest.mark.parametrize(
    "state",
    [
        {"source_id": "telegram:bot-updates"},
        {"source_id": "foreign", "cursor": "9"},
        {"source_id": "telegram:bot-updates", "cursor": ""},
    ],
)
async def test_existing_invalid_cursor_does_not_reset_to_first_update(
    tmp_path: Path, state: dict[str, JsonValue]
) -> None:
    repo = MemoryRepository()
    await repo.put("ops.source_checkpoints", CURSOR_KEY, state)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Invalid retained cursor cannot request updates")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, TelegramSettings(data_dir=tmp_path), http=http)
        with pytest.raises((KeyError, ValidationError)):
            await inbox.poll(timeout=0)
    assert await repo.get("ops.source_checkpoints", CURSOR_KEY) == state

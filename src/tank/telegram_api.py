"""Telegram response admission before application writes or offset confirmation."""

from pydantic import JsonValue, ValidationError

from tank.contracts.json import decode_unique_json
from tank.contracts.telegram_wire import (
    TelegramChatIdentity,
    TelegramFailure,
    TelegramMessageIdentity,
    TelegramMessageReceipt,
    TelegramSenderIdentity,
    TelegramSuccess,
    TelegramUnsupportedUpdate,
    TelegramUpdateReceipt,
)


class TelegramProtocolError(RuntimeError):
    """An API response cannot establish the required Telegram facts."""


class TelegramResponseError(RuntimeError):
    def __init__(self, method: str, status: int, *, retry_after: int | None) -> None:
        super().__init__(f"Telegram {method} rejected (HTTP {status})")
        self.status = status
        self.retry_after = retry_after


def response_result(payload: bytes, *, method: str, status: int) -> JsonValue:
    try:
        document = decode_unique_json(payload)
        if not isinstance(document, dict):
            raise ValueError("Telegram envelope requires an object")
        if document.get("ok") is False:
            failure = TelegramFailure.model_validate(document)
            retry_after = failure.parameters.retry_after if failure.parameters is not None else None
            raise TelegramResponseError(method, status, retry_after=retry_after)
        success = TelegramSuccess.model_validate(document)
    except TelegramResponseError:
        raise
    except (ValueError, ValidationError):
        raise TelegramProtocolError(f"Telegram {method} response is invalid (HTTP {status})") from None
    if status != 200:
        raise TelegramResponseError(method, status, retry_after=None)
    return success.result


def message_receipt(document: dict[str, JsonValue]) -> TelegramMessageReceipt:
    """Project required intake identities without relabelling unconsumed wire fields."""
    chat = document["chat"]
    if not isinstance(chat, dict):
        raise ValueError("Telegram message chat requires an object")
    sender = document.get("from")
    if sender is None:
        if "from" in document:
            raise ValueError("Telegram sender requires an object")
    elif not isinstance(sender, dict):
        raise ValueError("Telegram sender requires an object")
    identity = TelegramMessageIdentity.model_validate(
        {
            "message_id": document["message_id"],
            "date": document["date"],
            "chat": TelegramChatIdentity.model_validate({"id": chat["id"], "type": chat["type"]}),
            "sender": TelegramSenderIdentity.model_validate({"id": sender["id"]})
            if sender is not None
            else None,
        }
    )
    return TelegramMessageReceipt(identity=identity, source_message=document)


def update_receipts(document: JsonValue) -> list[TelegramUpdateReceipt]:
    """Validate the complete batch before any update or cursor becomes durable."""
    try:
        if not isinstance(document, list):
            raise ValueError("Telegram updates require an array")
        receipts: list[TelegramUpdateReceipt] = []
        payload: TelegramMessageReceipt | TelegramUnsupportedUpdate
        for item in document:
            if not isinstance(item, dict):
                raise ValueError("Telegram update requires an object")
            kinds = [key for key in item if key != "update_id"]
            if len(kinds) != 1:
                raise ValueError("Telegram update requires exactly one payload kind")
            if "message" in item:
                message = item["message"]
                if not isinstance(message, dict):
                    raise ValueError("Telegram update message requires an object")
                payload = message_receipt(message)
            else:
                if not isinstance(item[kinds[0]], dict):
                    raise ValueError("Telegram unsupported update requires its actual payload kind")
                payload = TelegramUnsupportedUpdate(kind=kinds[0])
            receipts.append(
                TelegramUpdateReceipt.model_validate({"update_id": item["update_id"], "payload": payload})
            )
        return receipts
    except (KeyError, ValueError, ValidationError):
        raise TelegramProtocolError("Telegram update batch is invalid") from None

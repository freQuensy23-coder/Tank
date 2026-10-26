"""Telegram response admission before application writes or offset confirmation."""

import re
from collections.abc import Mapping
from urllib.parse import quote

import httpx
from pydantic import JsonValue, ValidationError

from tank.contracts.json import decode_unique_json
from tank.contracts.telegram_wire import (
    TelegramAuthorizationSubject,
    TelegramBotIdentity,
    TelegramChatIdentity,
    TelegramDownloadedFile,
    TelegramEmptyUpdate,
    TelegramFailure,
    TelegramFileInfo,
    TelegramFileReference,
    TelegramKnownMembership,
    TelegramMediaKind,
    TelegramMediaSelection,
    TelegramMembership,
    TelegramMessageIdentity,
    TelegramMessageReceipt,
    TelegramNamedFile,
    TelegramPhotoSize,
    TelegramRestrictedMembership,
    TelegramSenderIdentity,
    TelegramSentMessageReceipt,
    TelegramSuccess,
    TelegramUnsupportedUpdate,
    TelegramUpdateReceipt,
)


class TelegramProtocolError(ValueError):
    """An API response cannot establish the required Telegram facts."""


class TelegramConfigurationError(ValueError):
    """An active API operation lacks its actual configured credential."""


def require_telegram_token(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TelegramConfigurationError("Telegram active operation requires a nonblank token")
    return value


class TelegramResponseError(RuntimeError):
    def __init__(
        self, method: str, status: int, *, retry_after: int | None, failure: TelegramFailure | None = None
    ) -> None:
        super().__init__(f"Telegram {method} rejected (HTTP {status})")
        self.status = status
        self.retry_after = retry_after
        self.failure = failure


def response_result(payload: bytes, *, method: str, status: int) -> JsonValue:
    try:
        document = decode_unique_json(payload)
        if not isinstance(document, dict):
            raise ValueError("Telegram envelope requires an object")
        if document.get("ok") is False:
            failure = TelegramFailure.model_validate(document)
            retry_after = failure.parameters.retry_after if failure.parameters is not None else None
            raise TelegramResponseError(method, status, retry_after=retry_after, failure=failure)
        success = TelegramSuccess.model_validate(document)
    except TelegramResponseError:
        raise
    except (ValueError, ValidationError):
        raise TelegramProtocolError(f"Telegram {method} response is invalid (HTTP {status})") from None
    if status != 200:
        raise TelegramProtocolError(f"Telegram {method} success conflicts with HTTP {status}")
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
    if identity.message_id != 0:
        media_selection(document)
    return TelegramMessageReceipt(identity=identity, source_message=document)


def update_receipts(document: JsonValue) -> list[TelegramUpdateReceipt]:
    """Validate the complete batch before any update or cursor becomes durable."""
    try:
        if not isinstance(document, list):
            raise ValueError("Telegram updates require an array")
        receipts: list[TelegramUpdateReceipt] = []
        payload: TelegramMessageReceipt | TelegramUnsupportedUpdate | TelegramEmptyUpdate
        for item in document:
            if not isinstance(item, dict):
                raise ValueError("Telegram update requires an object")
            kinds = [key for key in item if key != "update_id"]
            if len(kinds) > 1:
                raise ValueError("Telegram update permits at most one payload kind")
            if not kinds:
                payload = TelegramEmptyUpdate(reason="NO_OPTIONAL_PAYLOAD")
            elif "message" in item:
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


def pairing_sender_id(record: Mapping[str, JsonValue]) -> str | None:
    """Read old intake's empty sentinel only when its retained wire actually omitted sender."""
    stored = record["user_id"]
    message = record["message"]
    if not isinstance(message, dict):
        raise ValueError("Retained Telegram message requires its original object")
    if "from" not in message:
        if stored not in (None, ""):
            raise ValueError("Retained Telegram sender contradicts absent wire sender")
        return None
    sender = message["from"]
    if not isinstance(sender, dict):
        raise ValueError("Retained Telegram sender requires its actual object")
    identity = TelegramSenderIdentity.model_validate({"id": sender["id"]})
    if stored != str(identity.id):
        raise ValueError("Retained Telegram sender identity conflicts with its wire")
    return stored


def sent_message_receipt(result: JsonValue) -> TelegramSentMessageReceipt:
    """Admit consumed receipt identity before any caller records delivery success."""
    try:
        if not isinstance(result, dict):
            raise ValueError("Telegram sent message result requires an object")
        return TelegramSentMessageReceipt.model_validate(
            {field: result[field] for field in TelegramSentMessageReceipt.model_fields if field in result}
        )
    except (ValueError, ValidationError):
        raise TelegramProtocolError("Telegram sent message receipt is invalid") from None


MEDIA_KINDS: tuple[TelegramMediaKind, ...] = ("document", "photo", "video", "audio", "voice", "animation")


def media_selection(message: Mapping[str, JsonValue]) -> TelegramMediaSelection | None:
    """Select actual file identity and declared metadata; unknown size stays unknown."""
    for kind in MEDIA_KINDS:
        if kind not in message:
            continue
        value = message[kind]
        item: TelegramFileReference
        file_name: str | None = None
        reported_mime_type: str | None = None
        if kind == "photo":
            if not isinstance(value, list) or not value:
                raise ValueError("Telegram photo requires declared sizes")
            sizes: list[TelegramPhotoSize] = []
            for candidate in value:
                if not isinstance(candidate, dict):
                    raise ValueError("Telegram photo size requires an object")
                sizes.append(
                    TelegramPhotoSize.model_validate(
                        {
                            field: candidate[field]
                            for field in TelegramPhotoSize.model_fields
                            if field in candidate
                        }
                    )
                )
            item = max(sizes, key=lambda photo: photo.width * photo.height)
        else:
            if not isinstance(value, dict):
                raise ValueError("Telegram media requires its file object")
            named = TelegramNamedFile.model_validate(
                {field: value[field] for field in TelegramNamedFile.model_fields if field in value}
            )
            item = named
            file_name = named.file_name
            reported_mime_type = named.mime_type
        message_id = message["message_id"]
        if not isinstance(message_id, int) or isinstance(message_id, bool) or message_id <= 0:
            raise ValueError("Telegram attachment requires its usable message identity")
        name = file_name if file_name is not None and file_name else f"{kind}-{message_id}"
        filename = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
        if not filename:
            filename = f"file-{message_id}"
        return TelegramMediaSelection(
            kind=kind,
            file_id=item.file_id,
            file_unique_id=item.file_unique_id,
            filename=filename[:120],
            reported_mime_type=reported_mime_type,
            declared_size=item.file_size,
        )
    return None


class FileTooLarge(ValueError):
    """The actual declared or received file bytes exceed the Bot API download limit."""


async def download_file(
    http: httpx.AsyncClient, *, token: str, media: TelegramMediaSelection, limit: int
) -> TelegramDownloadedFile:
    token = require_telegram_token(token)
    if limit <= 0:
        raise ValueError("Telegram file byte limit must be positive")
    if media.declared_size is not None and media.declared_size > limit:
        raise FileTooLarge("Telegram declared file size exceeds download byte limit")
    try:
        response = await http.post(
            f"https://api.telegram.org/bot{token}/getFile",
            json={"file_id": media.file_id},
            follow_redirects=False,
        )
        try:
            result = response_result(response.content, method="getFile", status=response.status_code)
        except TelegramResponseError as error:
            if (
                error.status == 400
                and error.failure is not None
                and error.failure.error_code == 400
                and error.failure.description == "Bad Request: file is too big"
            ):
                raise FileTooLarge("Telegram refused the oversized file download") from None
            raise
        try:
            info = TelegramFileInfo.model_validate(result)
        except ValidationError:
            raise TelegramProtocolError("Telegram getFile result is invalid") from None
        if info.file_unique_id != media.file_unique_id:
            raise TelegramProtocolError("Telegram getFile identity conflicts with the selected attachment")
        if info.file_size is not None and info.file_size > limit:
            raise FileTooLarge("Telegram getFile declared size exceeds download byte limit")
        if (
            media.declared_size is not None
            and info.file_size is not None
            and media.declared_size != info.file_size
        ):
            raise TelegramProtocolError("Telegram getFile size conflicts with the selected attachment")
        if info.file_path is None:
            raise TelegramProtocolError("Telegram getFile did not provide a download path")
        if (
            info.file_path.startswith("/")
            or "\\" in info.file_path
            or any(part in (".", "..") for part in info.file_path.split("/"))
        ):
            raise TelegramProtocolError("Telegram getFile download path is invalid")
        url = f"https://api.telegram.org/file/bot{token}/" + quote(info.file_path, safe="/")
        chunks: list[bytes] = []
        received = 0
        async with http.stream("GET", url, follow_redirects=False) as file_response:
            if file_response.status_code != 200:
                raise TelegramResponseError("file download", file_response.status_code, retry_after=None)
            content_type = file_response.headers.get("content-type")
            async for chunk in file_response.aiter_bytes():
                received += len(chunk)
                if received > limit:
                    raise FileTooLarge("Received Telegram file exceeds download byte limit")
                chunks.append(chunk)
        if (info.file_size is not None and received != info.file_size) or (
            media.declared_size is not None and received != media.declared_size
        ):
            raise TelegramProtocolError("Received Telegram file bytes conflict with declared size")
        return TelegramDownloadedFile(
            content=b"".join(chunks), response_content_type=content_type, telegram=info
        )
    except httpx.HTTPError:
        raise TelegramProtocolError("Telegram file transport failed") from None


class TelegramAuthorizationUnavailable(RuntimeError):
    """Required authorization evidence is unreported, not a negative membership fact."""


def authorization_subject(message: Mapping[str, JsonValue]) -> TelegramAuthorizationSubject:
    try:
        chat = message["chat"]
        if not isinstance(chat, dict):
            raise ValueError("Telegram authorization requires its reported chat")
        sender = message.get("from")
        if sender is None:
            if "from" in message:
                raise ValueError("A reported Telegram sender requires its object")
        elif not isinstance(sender, dict):
            raise ValueError("A reported Telegram sender requires its object")
        return TelegramAuthorizationSubject(
            chat=TelegramChatIdentity.model_validate({"id": chat["id"], "type": chat["type"]}),
            sender=TelegramSenderIdentity.model_validate({"id": sender["id"]})
            if sender is not None
            else None,
        )
    except (KeyError, ValueError, ValidationError):
        raise TelegramProtocolError("Telegram authorization subject is invalid") from None


def require_membership_selector(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TelegramConfigurationError("Telegram membership lookup requires its configured chat selector")
    return value


def bot_identity(result: JsonValue) -> TelegramBotIdentity:
    try:
        if not isinstance(result, dict):
            raise ValueError("Telegram bot identity requires an object")
        return TelegramBotIdentity.model_validate(
            {field: result[field] for field in TelegramBotIdentity.model_fields if field in result}
        )
    except (ValueError, ValidationError):
        raise TelegramProtocolError("Telegram getMe identity is invalid") from None


def membership_observation(result: JsonValue, requested_user_id: int) -> TelegramMembership:
    try:
        requested = TelegramSenderIdentity(id=requested_user_id)
        if not isinstance(result, dict) or not isinstance(result["user"], dict):
            raise ValueError("Telegram membership requires its actual user")
        user = TelegramSenderIdentity.model_validate({"id": result["user"]["id"]})
        if user.id != requested.id:
            raise ValueError("Telegram membership user conflicts with its requested identity")
        if result.get("status") == "restricted":
            return TelegramRestrictedMembership.model_validate(
                {
                    "user": user,
                    **{field: result[field] for field in ("status", "is_member") if field in result},
                }
            )
        return TelegramKnownMembership.model_validate(
            {"user": user, **{field: result[field] for field in ("status",) if field in result}}
        )
    except (KeyError, ValueError, ValidationError):
        raise TelegramProtocolError(
            "Telegram membership observation is invalid or belongs to another user"
        ) from None

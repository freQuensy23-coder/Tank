"""Validated Telegram wire facts consumed by update intake and file acquisition."""

from typing import Annotated, Literal

from pydantic import AfterValidator, Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.scalars import FalseFlag, TrueFlag, ZeroInt

type PositiveId = Annotated[int, Field(gt=0)]


def nonzero_chat_id(value: int) -> int:
    if value == 0:
        raise ValueError("Telegram chat identity must be nonzero")
    return value


type ChatId = Annotated[int, AfterValidator(nonzero_chat_id)]


class TelegramChatIdentity(DomainModel):
    id: ChatId
    type: Literal["private", "group", "supergroup", "channel"]


class TelegramSenderIdentity(DomainModel):
    id: PositiveId


class TelegramMessageIdentity(DomainModel):
    message_id: Annotated[int, Field(ge=0)]
    date: PositiveId
    chat: TelegramChatIdentity
    sender: TelegramSenderIdentity | None


class TelegramInaccessibleMessage(DomainModel):
    message_id: PositiveId
    date: ZeroInt
    chat: TelegramChatIdentity


class TelegramMessageReceipt(DomainModel):
    """Original wire bytes' decoded message retained only at the storage boundary."""

    identity: TelegramMessageIdentity
    source_message: dict[str, JsonValue]


class TelegramUnsupportedUpdate(DomainModel):
    kind: Annotated[str, Field(min_length=1)]


class TelegramUpdateReceipt(DomainModel):
    update_id: Annotated[int, Field(ge=0)]
    payload: TelegramMessageReceipt | TelegramUnsupportedUpdate


class TelegramRetryParameters(DomainModel):
    retry_after: Annotated[int, Field(gt=0)] | None = None
    migrate_to_chat_id: ChatId | None = None


class TelegramSuccess(DomainModel):
    ok: TrueFlag
    result: JsonValue


class TelegramFailure(DomainModel):
    ok: FalseFlag
    error_code: PositiveId
    description: str
    parameters: TelegramRetryParameters | None = None


class TelegramUpdateCursor(DomainModel):
    source_id: Literal["telegram:bot-updates"]
    cursor: Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)$")]

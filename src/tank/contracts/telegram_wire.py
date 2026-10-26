"""Validated Telegram wire facts consumed by update intake and file acquisition."""

from typing import Annotated, Literal, Self

from pydantic import AfterValidator, Field, JsonValue, model_validator

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


class TelegramEmptyUpdate(DomainModel):
    reason: Literal["NO_OPTIONAL_PAYLOAD"]


class TelegramUpdateReceipt(DomainModel):
    update_id: Annotated[int, Field(ge=0)]
    payload: TelegramMessageReceipt | TelegramUnsupportedUpdate | TelegramEmptyUpdate


class TelegramRetryParameters(DomainModel):
    retry_after: Annotated[int, Field(gt=0)] | None = None
    migrate_to_chat_id: ChatId | None = None


class TelegramSuccess(DomainModel):
    ok: TrueFlag
    result: JsonValue
    description: str | None = None

    @model_validator(mode="after")
    def reported_description_is_text(self) -> Self:
        if "description" in self.model_fields_set and self.description is None:
            raise ValueError("A reported Telegram description requires text")
        return self


class TelegramFailure(DomainModel):
    ok: FalseFlag
    error_code: PositiveId
    description: str
    parameters: TelegramRetryParameters | None = None


class TelegramUpdateCursor(DomainModel):
    source_id: Literal["telegram:bot-updates"]
    cursor: Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)$")]

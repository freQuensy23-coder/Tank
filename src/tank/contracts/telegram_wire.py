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


class TelegramSentMessageReceipt(DomainModel):
    """Only returned identity authorizes SENT; an omitted ancillary date stays unreported."""

    message_id: PositiveId
    date: PositiveId | None = None

    @model_validator(mode="after")
    def reported_date_is_positive(self) -> Self:
        if "date" in self.model_fields_set and self.date is None:
            raise ValueError("A reported Telegram sent-message date requires an integer")
        return self


type TelegramMediaKind = Literal["document", "photo", "video", "audio", "voice", "animation"]
type TelegramMimeBasis = Literal["SENDER_REPORTED", "DOWNLOAD_HEADER", "STORAGE_DEFAULT"]
type FileIdentifier = Annotated[str, Field(min_length=1, pattern=r"\S")]


class TelegramFileReference(DomainModel):
    file_id: FileIdentifier
    file_unique_id: FileIdentifier
    file_size: Annotated[int, Field(ge=0)] | None = None

    @model_validator(mode="after")
    def reported_size_is_integer(self) -> Self:
        if "file_size" in self.model_fields_set and self.file_size is None:
            raise ValueError("A reported Telegram file size requires an integer")
        return self


class TelegramPhotoSize(TelegramFileReference):
    width: Annotated[int, Field(gt=0)]
    height: Annotated[int, Field(gt=0)]


class TelegramNamedFile(TelegramFileReference):
    file_name: str | None = None
    mime_type: str | None = None

    @model_validator(mode="after")
    def reported_metadata_is_text(self) -> Self:
        if "file_name" in self.model_fields_set and self.file_name is None:
            raise ValueError("Reported Telegram filename requires text")
        if "mime_type" in self.model_fields_set and self.mime_type is None:
            raise ValueError("Reported Telegram MIME type requires text")
        return self


class TelegramMediaSelection(DomainModel):
    kind: TelegramMediaKind
    file_id: FileIdentifier
    file_unique_id: FileIdentifier
    filename: Annotated[str, Field(min_length=1)]
    reported_mime_type: str | None
    declared_size: Annotated[int, Field(ge=0)] | None


class TelegramFileInfo(TelegramFileReference):
    file_path: Annotated[str, Field(min_length=1)] | None = None

    @model_validator(mode="after")
    def reported_path_is_text(self) -> Self:
        if "file_path" in self.model_fields_set and self.file_path is None:
            raise ValueError("A reported Telegram file path requires text")
        return self


class TelegramDownloadedFile(DomainModel):
    content: bytes
    response_content_type: str | None
    telegram: TelegramFileInfo


class TelegramAttachmentReference(DomainModel):
    file_key: Annotated[str, Field(min_length=1)]
    filename: Annotated[str, Field(min_length=1)]
    mime: Annotated[str, Field(min_length=1)]
    size: Annotated[int, Field(ge=0)]


class TelegramCachedFile(TelegramAttachmentReference):
    question_id: Annotated[str, Field(min_length=1)]
    session_id: Annotated[str, Field(min_length=1)]
    file_unique_id: FileIdentifier
    file_identity_basis: Literal["TELEGRAM_FILE_UNIQUE_ID"]
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    declared_size: Annotated[int, Field(ge=0)] | None
    reported_mime_type: str | None
    mime_basis: TelegramMimeBasis
    response_content_type: str | None

    @model_validator(mode="after")
    def metadata_matches_its_receipt(self) -> Self:
        effective = TelegramMimeSelection.from_facts(self.reported_mime_type, self.response_content_type)
        if self.mime != effective.value or self.mime_basis != effective.basis:
            raise ValueError("Telegram cached MIME provenance conflicts with its facts")
        if self.declared_size is not None and self.size != self.declared_size:
            raise ValueError("Telegram cached bytes conflict with declared source size")
        return self


class TelegramFilePayload(DomainModel):
    file_key: Annotated[str, Field(min_length=1)]
    content_base64: str


class TelegramMimeSelection(DomainModel):
    value: Annotated[str, Field(min_length=1)]
    basis: TelegramMimeBasis

    @classmethod
    def from_facts(cls, reported: str | None, response_header: str | None) -> Self:
        if reported is not None and reported.strip():
            return cls(value=reported, basis="SENDER_REPORTED")
        if response_header is not None and response_header.strip():
            return cls(value=response_header, basis="DOWNLOAD_HEADER")
        return cls(value="application/octet-stream", basis="STORAGE_DEFAULT")

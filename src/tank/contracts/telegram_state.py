"""Consumed Telegram application state and its persistence/configuration ports."""

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.contracts.telegram_wire import ChatId, PositiveId
from tank.time_precision import Timestamp

type AnswerStatus = Literal["ANSWERED", "FAILED", "NO_SESSION", "TERMINATED", "ARCHIVED"]
type MessageReference = PositiveId | Annotated[str, Field(pattern=r"^[1-9][0-9]*$")]
type ChatReference = ChatId | Annotated[str, Field(pattern=r"^-?[1-9][0-9]*$")]
type NonblankText = Annotated[str, Field(pattern=r"\S")]


class TelegramInboxSettings(Protocol):
    @property
    def telegram_token(self) -> str | None: ...
    @property
    def telegram_alerts_chat(self) -> str | None: ...
    @property
    def telegram_ops_chat(self) -> str | None: ...


class TelegramInboxStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...
    async def delete(self, collection: str, key: str) -> None: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    def iter_all(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    def iter_metadata(
        self, collection: str, fields: Sequence[str], *, filters: dict[str, JsonValue] | None = None
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    def iter_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
        filters: dict[str, JsonValue] | None = None,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    async def enqueue(
        self,
        kind: str,
        payload: dict[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        expires_at: Timestamp,
        run_after: Timestamp | None = None,
        max_attempts: int = 3,
    ) -> dict[str, JsonValue]: ...


class TelegramQuestionRecipient(FollowupRefusalTarget):
    chat_id: ChatReference
    reply_to_message_id: MessageReference | None = None

    @property
    def chat_key(self) -> str:
        return str(self.chat_id)

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], target: FollowupRefusalTarget) -> Self:
        target = FollowupRefusalTarget.model_validate(target)
        result = cls.model_validate({field: record[field] for field in cls.model_fields if field in record})
        if result.question_id != target.question_id or result.session_id != target.session_id:
            raise ValueError("Telegram question recipient conflicts with its actual target")
        return result


class TelegramAnsweredQuestion(TelegramQuestionRecipient):
    answer_status: Literal["ANSWERED"]
    answer_text: NonblankText | None


class TelegramFailedQuestion(TelegramQuestionRecipient):
    answer_status: Literal["FAILED"]
    reason_text: NonblankText | None


class TelegramClosedQuestion(TelegramQuestionRecipient):
    answer_status: Literal["NO_SESSION", "TERMINATED", "ARCHIVED"]
    reason_text: NonblankText


type TelegramSettledQuestion = TelegramAnsweredQuestion | TelegramFailedQuestion | TelegramClosedQuestion


def settled_question(
    record: Mapping[str, JsonValue], target: FollowupRefusalTarget
) -> TelegramSettledQuestion:
    status = record.get("answer_status")
    if status == "ANSWERED":
        return TelegramAnsweredQuestion.from_record(record, target)
    if status == "FAILED":
        return TelegramFailedQuestion.from_record(record, target)
    if status in ("NO_SESSION", "TERMINATED", "ARCHIVED"):
        return TelegramClosedQuestion.from_record(record, target)
    raise ValueError("Telegram question has no admitted answer outcome")


class TelegramChunkIdentity(FollowupRefusalTarget):
    channel: Literal["telegram:answer"]
    purpose: Literal["ANSWER"]
    chat_id: ChatReference
    chunk: Annotated[int, Field(ge=0)]


class TelegramGeneratedChunk(TelegramChunkIdentity):
    status: Literal["GENERATED"]


class TelegramFailedChunk(TelegramChunkIdentity):
    status: Literal["FAILED"]


class TelegramSentChunk(TelegramChunkIdentity):
    status: Literal["SENT"]
    message_ref: MessageReference


type TelegramAnswerChunk = TelegramGeneratedChunk | TelegramFailedChunk | TelegramSentChunk


def answer_chunk(
    record: Mapping[str, JsonValue], recipient: TelegramQuestionRecipient
) -> TelegramAnswerChunk:
    recipient = TelegramQuestionRecipient.model_validate(
        recipient.model_dump(include=set(TelegramQuestionRecipient.model_fields))
    )
    status = record.get("status")
    match status:
        case "GENERATED":
            result: TelegramAnswerChunk = TelegramGeneratedChunk.model_validate(
                {field: record[field] for field in TelegramGeneratedChunk.model_fields if field in record}
            )
        case "FAILED":
            result = TelegramFailedChunk.model_validate(
                {field: record[field] for field in TelegramFailedChunk.model_fields if field in record}
            )
        case "SENT":
            result = TelegramSentChunk.model_validate(
                {field: record[field] for field in TelegramSentChunk.model_fields if field in record}
            )
        case _:
            raise ValueError("Telegram answer chunk has no admitted attempt phase")
    if (
        result.question_id != recipient.question_id
        or result.session_id != recipient.session_id
        or str(result.chat_id) != recipient.chat_key
    ):
        raise ValueError("Telegram answer chunk conflicts with its actual recipient")
    return result


class TelegramFileSent(DomainModel):
    sent: TrueFlag
    message_id: Annotated[str, Field(pattern=r"^[1-9][0-9]*$")]
    filename: NonblankText
    bytes: Annotated[int, Field(ge=0)]


class TelegramFileAlreadySent(DomainModel):
    sent: TrueFlag
    message_id: Annotated[str, Field(pattern=r"^[1-9][0-9]*$")]
    duplicate: TrueFlag


class TelegramFileUncertain(DomainModel):
    sent: Literal["UNKNOWN"]
    reason: NonblankText


type TelegramFileDeliveryOutcome = TelegramFileSent | TelegramFileAlreadySent | TelegramFileUncertain


class QuestionReference(DomainModel):
    kind: Literal["CARD", "ANSWER", "PROMPT", "USER_MESSAGE"]
    text: str | None


class QuestionOpsReference(DomainModel):
    kind: Literal["OPS_CARD"]
    item_id: NonblankText
    text: str | None


class QuestionUnreportedReference(DomainModel):
    kind: None
    text: str | None


type QuestionPromptReference = QuestionReference | QuestionOpsReference | QuestionUnreportedReference


class QuestionPromptAttachment(DomainModel):
    filename: NonblankText
    size: Annotated[int, Field(ge=0)]
    path: NonblankText | None


class QuestionPrompt(DomainModel):
    text: str | None
    reference: QuestionPromptReference | None
    attachments: list[QuestionPromptAttachment] | None


type KnownTelegramFileDelivery = TelegramFileAlreadySent | TelegramFileUncertain


class TelegramAttachmentAttempt(FollowupRefusalTarget):
    channel: Literal["telegram:answer"]
    purpose: Literal["ATTACHMENT"]
    chat_id: ChatReference
    call_id: NonblankText
    filename: NonblankText


class TelegramFileDeliveryContext(DomainModel):
    recipient: TelegramQuestionRecipient
    attempt: TelegramAttachmentAttempt
    known: KnownTelegramFileDelivery | None


class TelegramGeneratedAttachment(TelegramAttachmentAttempt):
    status: Literal["GENERATED"]


class TelegramFailedAttachment(TelegramAttachmentAttempt):
    status: Literal["FAILED"]


class TelegramSentAttachment(TelegramAttachmentAttempt):
    status: Literal["SENT"]
    message_ref: MessageReference


type TelegramAttachmentDelivery = (
    TelegramGeneratedAttachment | TelegramFailedAttachment | TelegramSentAttachment
)


def attachment_delivery(
    record: Mapping[str, JsonValue],
    recipient: TelegramQuestionRecipient,
    *,
    call_id: str,
    filename: str,
) -> TelegramAttachmentDelivery:
    match record.get("status"):
        case "GENERATED":
            result: TelegramAttachmentDelivery = TelegramGeneratedAttachment.model_validate(
                {
                    field: record[field]
                    for field in TelegramGeneratedAttachment.model_fields
                    if field in record
                }
            )
        case "FAILED":
            result = TelegramFailedAttachment.model_validate(
                {field: record[field] for field in TelegramFailedAttachment.model_fields if field in record}
            )
        case "SENT":
            result = TelegramSentAttachment.model_validate(
                {field: record[field] for field in TelegramSentAttachment.model_fields if field in record}
            )
        case _:
            raise ValueError("Telegram file delivery has no admitted attempt phase")
    if (
        result.question_id != recipient.question_id
        or result.session_id != recipient.session_id
        or str(result.chat_id) != recipient.chat_key
        or result.call_id != call_id
        or result.filename != filename
    ):
        raise ValueError("Telegram file delivery conflicts with its actual target")
    return result


class TelegramInputTarget(DomainModel):
    chat_id: ChatReference
    message_id: MessageReference

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({field: record[field] for field in cls.model_fields if field in record})

    @property
    def storage_key(self) -> str:
        return f"{self.chat_id}:{self.message_id}"

    def same_message(self, other: Self) -> bool:
        return str(self.chat_id) == str(other.chat_id) and int(self.message_id) == int(other.message_id)


class TelegramInputPatch(DomainModel):
    reason: NonblankText | None = None
    item_id: NonblankText | None = None
    paired_with: MessageReference | None = None
    question_id: NonblankText | None = None
    session_id: NonblankText | None = None
    event_id: NonblankText | None = None


class TelegramNoticeSubject(DomainModel):
    item_id: NonblankText


class TelegramNoticeIdentity(DomainModel):
    question_id: NonblankText
    channel: Literal["telegram:answer"]
    purpose: Literal["NOTICE"]
    chat_id: ChatReference
    attempt: Annotated[int, Field(ge=1)]


class TelegramGeneratedNotice(TelegramNoticeIdentity):
    status: Literal["GENERATED"]


class TelegramFailedNotice(TelegramNoticeIdentity):
    status: Literal["FAILED"]


class TelegramSentNotice(TelegramNoticeIdentity):
    status: Literal["SENT"]
    message_ref: MessageReference


type TelegramNoticeAttempt = TelegramGeneratedNotice | TelegramFailedNotice | TelegramSentNotice


def notice_attempt(
    record: Mapping[str, JsonValue], target: TelegramInputTarget, question_id: str
) -> TelegramNoticeAttempt:
    match record.get("status"):
        case "GENERATED":
            result: TelegramNoticeAttempt = TelegramGeneratedNotice.model_validate(
                {field: record[field] for field in TelegramGeneratedNotice.model_fields if field in record}
            )
        case "FAILED":
            result = TelegramFailedNotice.model_validate(
                {field: record[field] for field in TelegramFailedNotice.model_fields if field in record}
            )
        case "SENT":
            result = TelegramSentNotice.model_validate(
                {field: record[field] for field in TelegramSentNotice.model_fields if field in record}
            )
        case _:
            raise ValueError("Telegram notice has no admitted attempt phase")
    if result.question_id != question_id or str(result.chat_id) != str(target.chat_id):
        raise ValueError("Telegram notice receipt conflicts with its actual input")
    return result


type TelegramInputStatus = Literal["IGNORED", "UNMATCHED", "PAIRED", "PROMPTED", "QUESTION", "NO_SESSION"]
type TelegramNoticeStatus = Literal["IGNORED", "UNMATCHED", "NO_SESSION"]


class TelegramInputDisposition(DomainModel):
    status: TelegramInputStatus


class TelegramNoticeRequest(DomainModel):
    text: NonblankText
    status: TelegramNoticeStatus

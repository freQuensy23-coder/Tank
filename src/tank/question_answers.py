"""Locked question binding and idempotent durable answer/delivery requests."""

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.time_precision import Timestamp, collection_clock, offset_schedule
from tank.util import utcnow

type AnswerStatus = Literal["ANSWERED", "FAILED", "NO_SESSION", "TERMINATED", "ARCHIVED"]


class QuestionAnswerRepository(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    async def enqueue(
        self,
        kind: str,
        payload: dict[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        run_after: Timestamp,
        max_attempts: int,
        expires_at: Timestamp,
    ) -> JsonValue: ...


class QuestionAnswerKey(DomainModel):
    question_id: str = Field(min_length=1, pattern=r"\S")


class QuestionAnswerState(QuestionAnswerKey):
    """Only consumed answer facts are projected; the stored question is preserved verbatim."""

    session_id: str | None = Field(default=None, min_length=1)
    answer_status: AnswerStatus | None = None


def validate_question_identity(
    record: Mapping[str, JsonValue], target: QuestionAnswerKey | FollowupRefusalTarget
) -> QuestionAnswerState:
    state = QuestionAnswerState.model_validate(
        {field: record[field] for field in QuestionAnswerState.model_fields if field in record}
    )
    if state.question_id != target.question_id:
        raise ValueError("Stored answer question identity conflicts with its key")
    if isinstance(target, FollowupRefusalTarget) and state.session_id != target.session_id:
        raise ValueError("Follow-up refusal question belongs to another session")
    return state


class QuestionAnswerUpdate(DomainModel):
    status: AnswerStatus
    text: str | None
    answered_at: Timestamp

    def storage_patch(self) -> dict[str, JsonValue]:
        if self.answered_at.value is None:
            raise ValueError("A recorded answer requires its actual collection timestamp")
        return {
            "status": self.status,
            "answer_status": self.status,
            "answer_text": self.text if self.status == "ANSWERED" and self.text else None,
            "reason_text": self.text if self.status != "ANSWERED" and self.text else None,
            **self.answered_at.fields("answered_at"),
        }


class QuestionAnswerReceipt(QuestionAnswerKey):
    status: AnswerStatus


async def record_answer(
    repo: QuestionAnswerRepository,
    question_id: str | FollowupRefusalTarget,
    text: str | None,
    *,
    status: AnswerStatus,
) -> QuestionAnswerReceipt | None:
    """Re-read identity under the question lock and request idempotent delivery."""
    target = (
        FollowupRefusalTarget.model_validate(question_id)
        if isinstance(question_id, FollowupRefusalTarget)
        else QuestionAnswerKey(question_id=question_id)
    )
    async with repo.lock("question:" + target.question_id), repo.transaction():
        question = await repo.get("out.user_questions", target.question_id)
        if question is None:
            return None
        state = validate_question_identity(question, target)
        answer_status = state.answer_status
        if answer_status is None:
            transition = QuestionAnswerUpdate(
                status=status, text=text, answered_at=collection_clock(utcnow())
            )
            question.update(transition.storage_patch())
            await repo.put("out.user_questions", target.question_id, question)
            answer_status = transition.status
        scheduled = collection_clock(utcnow())
        await repo.enqueue(
            "TELEGRAM_ANSWER",
            {"question_id": target.question_id},
            target.question_id,
            priority=0,
            run_after=offset_schedule(scheduled, timedelta(0)),
            max_attempts=5,
            expires_at=offset_schedule(scheduled, timedelta(days=1)),
        )
    return QuestionAnswerReceipt(question_id=target.question_id, status=answer_status)

"""Observed native answer history and narrow durable question-settlement facts."""

from collections.abc import Mapping, Sequence
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.agents.input_ownership import NativeInputOwner, NativeOwnershipSnapshot
from tank.agents.native_transport import ManagedEventPage
from tank.agents.tool_contracts import NativeText
from tank.contracts import DomainModel
from tank.contracts.session_followups import FollowupRefusalTarget

type AnswerIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type TurnPhase = Literal["ACCEPTING", "PENDING", "RUNNING", "DONE", "FAILED"]


class HistoryIdentity(DomainModel):
    event_id: AnswerIdentity
    event_type: AnswerIdentity


class HistoryRunning(HistoryIdentity):
    event_type: Literal["session.status_running"]
    execution_id: AnswerIdentity | None


class HistoryRedacted(DomainModel):
    type: Literal["redacted"]


class HistoryMessage(HistoryIdentity):
    event_type: Literal["agent.message"]
    content: list[NativeText | HistoryRedacted]

    def text(self) -> str | None:
        text = "".join(block.text for block in self.content if isinstance(block, NativeText)).strip()
        return text if text else None


class HistoryIdle(HistoryIdentity):
    event_type: Literal["session.status_idle"]
    execution_id: AnswerIdentity | None
    stop_reason: Literal["end_turn", "requires_action", "retries_exhausted", "budget_reached"]


type AnswerHistoryEvent = HistoryRunning | HistoryMessage | HistoryIdle | HistoryIdentity


def answer_history_event(raw: Mapping[str, JsonValue]) -> AnswerHistoryEvent:
    identity = HistoryIdentity.model_validate({"event_id": raw["id"], "event_type": raw["type"]})
    values: dict[str, JsonValue] = {
        "event_id": identity.event_id,
        "event_type": identity.event_type,
    }
    if identity.event_type == "session.status_running":
        return HistoryRunning.model_validate({**values, "execution_id": raw.get("execution_id")})
    if identity.event_type == "agent.message":
        return HistoryMessage.model_validate({**values, "content": raw["content"]})
    if identity.event_type == "session.status_idle":
        reason = raw["stop_reason"]
        if not isinstance(reason, dict):
            raise ValueError("Native idle history requires its reported stop reason")
        return HistoryIdle.model_validate(
            {**values, "execution_id": raw.get("execution_id"), "stop_reason": reason["type"]}
        )
    return identity


class AnswerHistoryPage(DomainModel):
    events: tuple[AnswerHistoryEvent, ...]
    next_page: str | None = Field(min_length=1)

    @classmethod
    def from_wire(cls, raw: dict[str, JsonValue]) -> Self:
        page = ManagedEventPage.model_validate(raw)
        return cls(events=tuple(answer_history_event(event) for event in page.data), next_page=page.next_page)


class NativeTurnAnswer(DomainModel):
    message_id: AnswerIdentity
    attempt: int = Field(ge=0)
    text: str | None = Field(pattern=r"\S")
    phase: Literal["UNSETTLED", "COMPLETED"]

    @property
    def ended(self) -> bool:
        return self.phase == "COMPLETED"


def collect_turn_answers(
    history: Sequence[AnswerHistoryEvent], owners: Mapping[str, NativeInputOwner]
) -> dict[str, NativeTurnAnswer]:
    texts: dict[str, str] = {}
    ended: dict[str, bool] = {}
    execution_order: list[str] = []
    current: str | None = None
    for event in history:
        if isinstance(event, HistoryRunning):
            current = event.execution_id
            if current is not None and current not in execution_order:
                execution_order.append(current)
        elif isinstance(event, HistoryMessage) and current is not None:
            text = event.text()
            if text is not None:
                texts[current] = text
        elif isinstance(event, HistoryIdle) and event.execution_id is not None:
            ended[event.execution_id] = event.stop_reason == "end_turn"
    answers: dict[str, NativeTurnAnswer] = {}
    for execution_id in execution_order:
        owner = owners.get(execution_id)
        if owner is None:
            continue
        previous = answers.get(owner.message_id)
        if previous is not None and owner.attempt < previous.attempt:
            continue
        text = texts.get(execution_id)
        if text is None and previous is not None and previous.attempt == owner.attempt:
            text = previous.text
        answers[owner.message_id] = NativeTurnAnswer(
            message_id=owner.message_id,
            attempt=owner.attempt,
            text=text,
            phase="COMPLETED" if ended.get(execution_id) is True else "UNSETTLED",
        )
    return answers


class QuestionSettlement(DomainModel):
    message_id: AnswerIdentity
    question_id: AnswerIdentity
    input_id: AnswerIdentity | None
    turn_status: TurnPhase | None
    answer_recorded: bool | None

    def target(self, session_id: str) -> FollowupRefusalTarget:
        return FollowupRefusalTarget(question_id=self.question_id, session_id=session_id)


class QuestionSettlementSession(DomainModel):
    ownership: NativeOwnershipSnapshot
    status: Literal["OPEN", "FAILED", "ENDED"] | None
    questions: tuple[QuestionSettlement, ...]

    @model_validator(mode="after")
    def questions_have_distinct_journal_owners(self) -> Self:
        messages = {lineage.message_id for lineage in self.ownership.lineages}
        ids: set[str] = set()
        for question in self.questions:
            if question.message_id not in messages or question.message_id in ids:
                raise ValueError("Question settlement conflicts with its actual journal owner")
            ids.add(question.message_id)
        return self

    @classmethod
    def from_record(cls, raw: dict[str, JsonValue]) -> Self:
        ownership = NativeOwnershipSnapshot.from_record(raw)
        journal = raw.get("message_log", [])
        if not isinstance(journal, list):
            raise ValueError("Native question journal must be an array")
        questions: list[QuestionSettlement] = []
        for row in journal:
            if not isinstance(row, dict):
                raise ValueError("Native question journal message must be an object")
            payload = row.get("payload")
            if not isinstance(payload, dict) or payload.get("kind") != "USER_QUESTION":
                continue
            if payload["session_id"] != ownership.session_id:
                raise ValueError("Question payload belongs to another native session")
            questions.append(
                QuestionSettlement.model_validate(
                    {
                        "message_id": row["message_id"],
                        "question_id": payload["question_id"],
                        "input_id": row.get("remote_event_id"),
                        "turn_status": row.get("turn_status"),
                        "answer_recorded": row.get("answer_recorded"),
                    }
                )
            )
        return cls.model_validate({"ownership": ownership, "status": raw.get("status"), "questions": tuple(questions)})

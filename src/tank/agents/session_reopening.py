"""Typed reopening policy and sparse mutation of the actual locked session journal."""

from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.agents.answer_settlement import TurnPhase
from tank.agents.outbound_inputs import MessageIdentity
from tank.contracts import DomainModel
from tank.runtime_contracts import UserQuestionMessage
from tank.time_precision import Timestamp

type ReopenIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]


class ReopenInput(DomainModel):
    message_id: ReopenIdentity
    turn_status: TurnPhase | None
    question: bool


class ReopenState(DomainModel):
    session_id: ReopenIdentity
    status: Literal["OPEN", "FAILED", "ENDED"]
    retired: list[ReopenIdentity] | None
    journal: tuple[ReopenInput, ...]

    @classmethod
    def from_record(cls, session: dict[str, JsonValue]) -> Self:
        journal: list[ReopenInput] = []
        if "message_log" in session:
            entries = session["message_log"]
            if not isinstance(entries, list):
                raise ValueError("Session reopening requires an actual journal array")
            for entry in entries:
                if not isinstance(entry, dict):
                    raise ValueError("Session reopening journal entries must be objects")
                payload = entry.get("payload")
                if "payload" in entry and not isinstance(payload, dict):
                    raise ValueError("Session reopening input payload must be an object when reported")
                journal.append(
                    ReopenInput.model_validate(
                        {
                            "message_id": entry["message_id"],
                            "turn_status": entry.get("turn_status"),
                            "question": isinstance(payload, dict) and payload.get("kind") == "USER_QUESTION",
                        }
                    )
                )
        if len({entry.message_id for entry in journal}) != len(journal):
            raise ValueError("Session reopening journal repeats an input identity")
        if "retired_message_ids" in session and session["retired_message_ids"] is None:
            raise ValueError("A reported retired-message index cannot be null")
        return cls.model_validate(
            {
                "session_id": session["session_id"],
                "status": session["status"],
                "retired": session.get("retired_message_ids"),
                "journal": tuple(journal),
            }
        )


class ReopenedQuestion(DomainModel):
    question_id: ReopenIdentity
    previous_status: Literal["FAILED"]
    at: Timestamp

    @model_validator(mode="after")
    def observed_reopening(self) -> Self:
        if self.at.value is None:
            raise ValueError("Session reopening requires its actual controller clock")
        return self

    def storage_record(self) -> dict[str, JsonValue]:
        return {
            "question_id": self.question_id,
            "previous_status": self.previous_status,
            **self.at.fields("at"),
        }


def reopen_question_session(
    session: dict[str, JsonValue],
    request: UserQuestionMessage,
    message_id: str,
    at: Timestamp,
) -> None:
    state = ReopenState.from_record(session)
    if state.session_id != request.session_id:
        raise ValueError("Question reopening target differs from its actual session")
    current = MessageIdentity(message_id=message_id)
    retired = set(state.retired) if state.retired is not None else set()
    if state.status == "FAILED":
        history = session.get("reopened", [])
        if not isinstance(history, list):
            raise ValueError("Session reopening history must be an array")
        observation = ReopenedQuestion(question_id=request.question_id, previous_status="FAILED", at=at)
        unfinished = {
            row.message_id
            for row in state.journal
            if row.turn_status != "DONE" and row.message_id != current.message_id
        }
        failed_questions = {
            row.message_id for row in state.journal if row.question and row.message_id in unfinished
        }
        retired.update(unfinished)
        if "message_log" in session:
            journal = session["message_log"]
            if not isinstance(journal, list):
                raise ValueError("Session reopening journal changed during planning")
            for row in journal:
                if not isinstance(row, dict):
                    raise ValueError("Session reopening journal changed during planning")
                if row["message_id"] in failed_questions:
                    row["turn_status"] = "FAILED"
        updated_history: list[JsonValue] = [*history, observation.storage_record()]
        session["reopened"] = updated_history
        session.update(status="OPEN", runtime_status="IDLE", active_message_id=None, pending_retry_event=None)
    retired.discard(current.message_id)
    if retired or state.retired is not None:
        session["retired_message_ids"] = list(sorted(retired))

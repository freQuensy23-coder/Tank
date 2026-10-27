"""Consumed immutable native-call ownership and journal trigger admission."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel

type AgentJournalInputKind = Literal[
    "NEW_ITEM",
    "ITEM_ADDED",
    "USER_QUESTION",
    "SCOUT_REVIEW",
    "CONDITION_FIRED",
    "CALL_EXPIRED",
    "MILESTONE",
    "CONDITION_MET",
]
type ToolDispatchIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]


class ToolJournalTrigger(DomainModel):
    kind: AgentJournalInputKind
    item_id: ToolDispatchIdentity | None


class ToolCallOwner(DomainModel):
    session_id: ToolDispatchIdentity
    call_id: ToolDispatchIdentity
    tool: ToolDispatchIdentity
    message_id: ToolDispatchIdentity
    retry_count: int | None = Field(ge=0)

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "session_id": record.get("session_id"),
                "call_id": record.get("call_id"),
                "tool": record.get("tool"),
                "message_id": record.get("native_turn_message_id"),
                "retry_count": record.get("native_turn_retry_count"),
            }
        )

    def trigger(
        self, session: Mapping[str, JsonValue], *, session_id: str, call_id: str, tool: str
    ) -> ToolJournalTrigger:
        if (self.session_id, self.call_id, self.tool) != (session_id, call_id, tool):
            raise ValueError("Tool call ownership differs from requested dispatch")
        if session.get("session_id") != self.session_id:
            raise ValueError("Tool call journal belongs to another session")
        journal = session.get("message_log")
        if not isinstance(journal, list):
            raise ValueError("Tool call journal is unreported")
        matches: list[Mapping[str, JsonValue]] = []
        for row in journal:
            if not isinstance(row, dict):
                raise ValueError("Tool call journal contains a malformed input")
            if row.get("message_id") == self.message_id:
                matches.append(row)
        if len(matches) != 1:
            raise ValueError("Tool call requires one actual owning journal input")
        owner = matches[0]
        count = owner.get("retry_count")
        if count is not None:
            admitted = ToolJournalAttempt.model_validate({"retry_count": count})
            if self.retry_count is not None and admitted.retry_count != self.retry_count:
                raise ValueError("Tool call attempt differs from its journal owner")
        payload = owner.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("Tool call owning input payload is unreported")
        return ToolJournalTrigger.model_validate(
            {"kind": payload.get("kind"), "item_id": payload.get("item_id")}
        )


class ToolJournalAttempt(DomainModel):
    retry_count: int = Field(ge=0)

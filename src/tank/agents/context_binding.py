"""Immutable input-owner projection for model tool scope and implicit source binding."""

from typing import Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel


class ContextTrigger(DomainModel):
    active_message: bool
    has_payload: bool
    item_id: str | None = Field(min_length=1)


class ContextScope(DomainModel):
    session_id: str = Field(min_length=1)
    event_id: str | None = Field(min_length=1)
    item_id: str | None = Field(min_length=1)
    proposal_id: str | None = Field(min_length=1)
    active_message_id: str | None = Field(min_length=1)


class ContextInputOwner(DomainModel):
    message_id: str = Field(min_length=1)


class ContextBinding(ContextScope):
    trigger: ContextTrigger

    @classmethod
    def from_session(cls, session: dict[str, JsonValue]) -> Self:
        scope = ContextScope.model_validate(
            {
                "session_id": session["session_id"],
                "event_id": session.get("event_id"),
                "item_id": session.get("item_id"),
                "proposal_id": session.get("proposal_id"),
                "active_message_id": session.get("active_message_id"),
            }
        )
        messages = session.get("message_log", [])
        if not isinstance(messages, list):
            raise ValueError("Session input journal must be an array")
        active: dict[str, JsonValue] | None = None
        for message in messages:
            if not isinstance(message, dict):
                raise ValueError("Session input journal must contain objects")
            owner = ContextInputOwner.model_validate({"message_id": message["message_id"]})
            if owner.message_id == scope.active_message_id:
                if active is not None:
                    raise ValueError("Session input journal repeats the active owner")
                active = message
        if scope.active_message_id is not None and active is None:
            raise ValueError("Active input has no authoritative journal owner")
        payload = active.get("payload") if active is not None else session.get("last_payload")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("Session input payload must be an object")
        item_id = payload.get("item_id") if isinstance(payload, dict) else None
        return cls(
            session_id=scope.session_id,
            event_id=scope.event_id,
            item_id=scope.item_id,
            proposal_id=scope.proposal_id,
            active_message_id=scope.active_message_id,
            trigger=ContextTrigger.model_validate(
                {
                    "active_message": active is not None,
                    "has_payload": bool(payload),
                    "item_id": item_id,
                }
            ),
        )

    def observation_source(self) -> str | None:
        if self.trigger.item_id is not None:
            return self.trigger.item_id
        if not self.trigger.active_message and not self.trigger.has_payload:
            return self.item_id
        return None

"""Exact acknowledged-input lineage without reconstructed retry ordinals."""

from typing import Annotated, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel

type InputIdentity = Annotated[str, Field(min_length=1)]
type RetryAttempt = Annotated[int, Field(ge=0)]


class UnprovedInputOwnership(ValueError):
    pass


class NativeInputOwner(DomainModel):
    message_id: InputIdentity
    attempt: RetryAttempt


class JournalInputLineage(DomainModel):
    message_id: InputIdentity
    current_input_id: InputIdentity | None
    current_attempt: RetryAttempt | None
    attempts: dict[InputIdentity, RetryAttempt] | None
    superseded_inputs: list[InputIdentity | None]

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "message_id": record["message_id"],
                "current_input_id": record.get("remote_event_id"),
                "current_attempt": record.get("retry_count"),
                "attempts": record.get("native_input_attempts"),
                "superseded_inputs": record.get("superseded_native_input_ids", []),
            }
        )

    @model_validator(mode="after")
    def current_attempt_agrees_with_acknowledged_lineage(self) -> Self:
        if (
            self.current_input_id is not None
            and self.current_attempt is not None
            and self.attempts is not None
            and self.current_input_id in self.attempts
            and self.attempts[self.current_input_id] != self.current_attempt
        ):
            raise ValueError("Current native input contradicts its recorded attempt")
        return self

    def owner(self, input_id: str) -> NativeInputOwner | None:
        if self.attempts is not None and input_id in self.attempts:
            return NativeInputOwner(message_id=self.message_id, attempt=self.attempts[input_id])
        if input_id == self.current_input_id:
            if self.current_attempt is None:
                raise UnprovedInputOwnership("Current native input has no recorded retry attempt")
            return NativeInputOwner(message_id=self.message_id, attempt=self.current_attempt)
        if input_id in self.superseded_inputs:
            raise UnprovedInputOwnership("Superseded native input has no recorded retry attempt")
        return None


class ToolInputLineage(DomainModel):
    input_id: InputIdentity | None
    message_id: InputIdentity | None
    attempt: RetryAttempt | None

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "input_id": record.get("remote_event_id"),
                "message_id": record.get("native_turn_message_id"),
                "attempt": record.get("native_turn_retry_count"),
            }
        )

    def owner(self, input_id: str, message_ids: frozenset[str]) -> NativeInputOwner | None:
        if input_id != self.input_id:
            return None
        if self.message_id is None or self.message_id not in message_ids:
            raise UnprovedInputOwnership("Acknowledged tool input has no verified journal owner")
        if self.attempt is None:
            raise UnprovedInputOwnership("Acknowledged tool input has no recorded retry attempt")
        return NativeInputOwner(message_id=self.message_id, attempt=self.attempt)

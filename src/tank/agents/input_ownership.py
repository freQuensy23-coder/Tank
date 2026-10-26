"""Exact acknowledged-input lineage without reconstructed retry ordinals."""

from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel

type InputIdentity = Annotated[str, Field(min_length=1)]
type RetryAttempt = Annotated[int, Field(ge=0)]


type UnprovedOwnershipReason = Literal[
    "CURRENT_INPUT_ATTEMPT_NOT_RETAINED",
    "SUPERSEDED_INPUT_ATTEMPT_NOT_RETAINED",
    "TOOL_INPUT_OWNER_NOT_RETAINED",
    "TOOL_INPUT_ATTEMPT_NOT_RETAINED",
]


class UnprovedInputOwnership(ValueError):
    def __init__(self, reason: UnprovedOwnershipReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason


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
                raise UnprovedInputOwnership(
                    "CURRENT_INPUT_ATTEMPT_NOT_RETAINED", "Current native input has no recorded retry attempt"
                )
            return NativeInputOwner(message_id=self.message_id, attempt=self.current_attempt)
        if input_id in self.superseded_inputs:
            raise UnprovedInputOwnership(
                "SUPERSEDED_INPUT_ATTEMPT_NOT_RETAINED",
                "Superseded native input has no recorded retry attempt",
            )
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
            raise UnprovedInputOwnership(
                "TOOL_INPUT_OWNER_NOT_RETAINED", "Acknowledged tool input has no verified journal owner"
            )
        if self.attempt is None:
            raise UnprovedInputOwnership(
                "TOOL_INPUT_ATTEMPT_NOT_RETAINED", "Acknowledged tool input has no recorded retry attempt"
            )
        return NativeInputOwner(message_id=self.message_id, attempt=self.attempt)


class NativeOwnershipSnapshot(DomainModel):
    session_id: InputIdentity
    lineages: tuple[JournalInputLineage, ...] | None
    unselected_message_ids: tuple[InputIdentity, ...]

    @model_validator(mode="after")
    def distinct_message_owners(self) -> Self:
        if self.lineages is None:
            if self.unselected_message_ids:
                raise ValueError("Unobserved native journal cannot declare message scope")
            return self
        ids = [lineage.message_id for lineage in self.lineages] + list(self.unselected_message_ids)
        if len(set(ids)) != len(ids):
            raise ValueError("Native input journal repeats a message identity")
        return self

    @classmethod
    def from_record(cls, record: dict[str, JsonValue], *, message_ids: frozenset[str] | None = None) -> Self:
        if "message_log" not in record:
            return cls.model_validate(
                {"session_id": record["session_id"], "lineages": None, "unselected_message_ids": ()}
            )
        journal = record["message_log"]
        if not isinstance(journal, list):
            raise ValueError("Native input journal must be an array")
        lineages: list[JournalInputLineage] = []
        unselected: list[InputIdentity] = []
        for raw in journal:
            if not isinstance(raw, dict):
                raise ValueError("Native input journal message must be an object")
            identity = JournalMessageIdentity.model_validate({"message_id": raw["message_id"]})
            if message_ids is not None and identity.message_id not in message_ids:
                unselected.append(identity.message_id)
            else:
                lineages.append(JournalInputLineage.from_record(raw))
        return cls.model_validate(
            {
                "session_id": record["session_id"],
                "lineages": tuple(lineages),
                "unselected_message_ids": tuple(unselected),
            }
        )


class JournalMessageIdentity(DomainModel):
    message_id: InputIdentity

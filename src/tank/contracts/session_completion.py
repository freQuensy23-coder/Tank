"""Consumed source-completion facts and evidence for retiring a failed prior attempt."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import require_integer
from tank.contracts.source_items import ExtractionStatus

type CompletionIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type CompletionAttempt = Annotated[int, Field(ge=0)]
type SourceCompletionOutcome = ExtractionStatus | Literal["MISSING_SOURCE_ITEM", "NOT_APPLICABLE"] | None


class CompletionMessage(DomainModel):
    message_id: CompletionIdentity
    item_id: CompletionIdentity | None
    native_input_id: CompletionIdentity | None
    retry_attempt: CompletionAttempt | None
    source_message: bool | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        payload = record.get("payload")
        if payload is None:
            kind = None
            item_id = None
        elif isinstance(payload, dict):
            kind = payload.get("kind")
            item_id = payload.get("item_id")
            if kind is not None and not isinstance(kind, str):
                raise ValueError("Completion message kind must be text")
        else:
            raise ValueError("Completion message payload must be an object")
        return cls.model_validate(
            {
                "message_id": record.get("message_id"),
                "item_id": item_id,
                "native_input_id": record.get("remote_event_id"),
                "retry_attempt": record.get("retry_count"),
                "source_message": None if kind is None else kind in {"NEW_ITEM", "ITEM_ADDED"},
            }
        )

    @model_validator(mode="after")
    def source_message_has_actual_item(self) -> Self:
        if self.source_message is True and self.item_id is None:
            raise ValueError("Source completion requires its actual item identity")
        return self


class SourceCompletionEntry(DomainModel):
    version: Annotated[Literal[4], BeforeValidator(require_integer)]
    item_id: CompletionIdentity | None
    native_input_id: CompletionIdentity | None
    retry_attempt: CompletionAttempt | None
    outcome: SourceCompletionOutcome


class SourceCompletionSnapshot(DomainModel):
    entries: dict[CompletionIdentity, SourceCompletionEntry]

    def storage_record(self) -> dict[str, JsonValue]:
        return {key: entry.model_dump(mode="json") for key, entry in self.entries.items()}


class CompletionAttemptOwner(DomainModel):
    message_id: CompletionIdentity
    attempt: CompletionAttempt | None
    replacement_input_id: CompletionIdentity | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "message_id": record.get("message_id"),
                "attempt": record.get("retry_count"),
                "replacement_input_id": record.get("remote_event_id"),
            }
        )


class FailedToolAttempt(DomainModel):
    input_id: CompletionIdentity
    message_id: CompletionIdentity | None
    attempt: CompletionAttempt | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "input_id": record.get("remote_event_id"),
                "message_id": record.get("native_turn_message_id"),
                "attempt": record.get("native_turn_retry_count"),
            }
        )

    def is_prior_to(self, owner: CompletionAttemptOwner) -> bool:
        return (
            self.message_id == owner.message_id
            and self.attempt is not None
            and owner.attempt is not None
            and owner.replacement_input_id is not None
            and self.input_id != owner.replacement_input_id
            and self.attempt < owner.attempt
        )

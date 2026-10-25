"""Declared host retry budgets and durable native retry phases."""

from typing import Literal

from pydantic import Field, JsonValue

from tank.agents.native_inputs import NativeInputAcknowledgment, NativeUserMessage
from tank.contracts import DomainModel
from tank.contracts.scalars import OneInt, ZeroInt
from tank.time_precision import Timestamp

type UnknownRetryReason = Literal[
    "RETRY_COUNT_NOT_RETAINED",
    "CURRENT_INPUT_OWNER_NOT_RETAINED",
    "RETRY_BODY_NOT_RETAINED",
    "TOOL_ORDINAL_NOT_RETAINED",
]


class UnknownNativeRetryState(ValueError):
    def __init__(self, reason: UnknownRetryReason) -> None:
        super().__init__("Native retry refused: " + reason)
        self.reason = reason


class UnknownRetryState(DomainModel):
    kind: Literal["UNKNOWN"]
    reason: UnknownRetryReason


class JournalRetryOwner(DomainModel):
    kind: Literal["JOURNAL"]
    message_id: str = Field(min_length=1)


class OriginRetryOwner(DomainModel):
    kind: Literal["ORIGIN"]
    session_id: str = Field(min_length=1)


type RetryOwner = JournalRetryOwner | OriginRetryOwner


class KnownRetryState(DomainModel):
    kind: Literal["KNOWN"]
    owner: RetryOwner
    attempt: int = Field(ge=0)
    body: NativeUserMessage | None = Field(repr=False)
    previous_input_id: str | None = Field(min_length=1)


class RetryCount(DomainModel):
    attempt: int | None = Field(ge=0)


class PendingNativeRetry(DomainModel):
    phase: Literal["PENDING"]
    owner: RetryOwner
    body: NativeUserMessage = Field(repr=False)
    previous_input_id: str | None = Field(min_length=1)
    previous_attempt: ZeroInt
    next_attempt: OneInt
    failure_event_id: str = Field(min_length=1)
    tool_call_ordinal_cutoff: int = Field(ge=0)

    def idempotency_key(self, session_id: str) -> str:
        selector = self.owner.message_id if isinstance(self.owner, JournalRetryOwner) else "legacy"
        return "turn-retry:" + session_id + ":" + selector

    def accepted(self, receipt: NativeInputAcknowledgment, at: Timestamp) -> "AcceptedNativeRetry":
        return AcceptedNativeRetry(phase="ACCEPTED", intent=self, receipt=receipt, accepted_at=at)


class AcceptedNativeRetry(DomainModel):
    phase: Literal["ACCEPTED"]
    intent: PendingNativeRetry
    receipt: NativeInputAcknowledgment
    accepted_at: Timestamp


def retry_state(
    session: dict[str, JsonValue], active: dict[str, JsonValue] | None
) -> KnownRetryState | UnknownRetryState:
    if active is not None:
        record = active
        owner: RetryOwner = JournalRetryOwner.model_validate(
            {"kind": "JOURNAL", "message_id": active["message_id"]}
        )
        body = record.get("body")
    else:
        journal = session.get("message_log", [])
        if not isinstance(journal, list):
            raise ValueError("Native retry journal must be an array")
        if journal:
            return UnknownRetryState(kind="UNKNOWN", reason="CURRENT_INPUT_OWNER_NOT_RETAINED")
        record = session
        owner = OriginRetryOwner.model_validate({"kind": "ORIGIN", "session_id": session["session_id"]})
        body = record.get("last_message")
    count = RetryCount.model_validate({"attempt": record.get("retry_count")}).attempt
    if count is None:
        return UnknownRetryState(kind="UNKNOWN", reason="RETRY_COUNT_NOT_RETAINED")
    return KnownRetryState.model_validate(
        {
            "kind": "KNOWN",
            "owner": owner,
            "attempt": count,
            "body": body,
            "previous_input_id": record.get("remote_event_id") if active is not None else None,
        }
    )

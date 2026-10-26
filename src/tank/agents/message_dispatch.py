"""Observed phases of a session message, without guessing native acceptance."""

from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, RootModel, model_validator

from tank.agents.native_inputs import AcceptedNativeInput
from tank.agents.outbound_inputs import (
    AcceptedOutboundInput,
    MessageIdentity,
    PendingOutboundInput,
    RecordedInputIdentity,
    parse_outbound_input,
    retained_outbound_acceptance,
)
from tank.contracts import DomainModel
from tank.contracts.session_followups import FollowupRefusalTarget


class MessageDispatchIdentity(DomainModel):
    session_id: str = Field(min_length=1, pattern=r"\S")
    message_id: str = Field(min_length=1, pattern=r"\S")


class AcceptedMessageDispatch(MessageDispatchIdentity):
    phase: Literal["ACCEPTED"]
    acceptance: AcceptedOutboundInput

    @model_validator(mode="after")
    def actual_input(self) -> Self:
        if self.acceptance.message_id != self.message_id:
            raise ValueError("Message dispatch receipt differs from its actual input owner")
        return self


class PendingMessageDispatch(MessageDispatchIdentity):
    phase: Literal["PENDING"]
    intent: PendingOutboundInput

    @model_validator(mode="after")
    def actual_input(self) -> Self:
        if self.intent.message_id != self.message_id:
            raise ValueError("Message dispatch intent differs from its actual input owner")
        return self


class IndexedMessageDispatch(MessageDispatchIdentity):
    phase: Literal["INDEXED_UNREPORTED"]
    reason: Literal["NATIVE_ACCEPTANCE_RECEIPT_NOT_RETAINED"]


class DeclinedQuestionDispatch(DomainModel):
    phase: Literal["NOT_CONTINUABLE"]
    target: FollowupRefusalTarget


type RecordedMessageDispatch = Annotated[
    AcceptedMessageDispatch | PendingMessageDispatch | IndexedMessageDispatch,
    Field(discriminator="phase"),
]


type MessageDispatchOutcome = Annotated[
    AcceptedMessageDispatch | PendingMessageDispatch | IndexedMessageDispatch | DeclinedQuestionDispatch,
    Field(discriminator="phase"),
]


class MessageDispatchOutcomeRecord(RootModel[MessageDispatchOutcome]):
    model_config = {"strict": True, "hide_input_in_errors": True, "revalidate_instances": "always"}


class _StoredMessageIndex(DomainModel):
    messages: list[Annotated[str, Field(min_length=1, pattern=r"\S")]]


def find_message_dispatch(session: dict[str, JsonValue], message_id: str) -> RecordedMessageDispatch | None:
    identity = MessageDispatchIdentity.model_validate(
        {"session_id": session["session_id"], "message_id": message_id}
    )
    row: dict[str, JsonValue] | None = None
    if "message_log" in session:
        journal = session["message_log"]
        if not isinstance(journal, list):
            raise ValueError("Session message journal must be an array")
        for entry in journal:
            if not isinstance(entry, dict):
                raise ValueError("Session message journal entries must be objects")
            owner = MessageIdentity.model_validate({"message_id": entry["message_id"]})
            if owner.message_id == identity.message_id:
                if row is not None:
                    raise ValueError("Session message journal repeats the requested input owner")
                row = entry
    if row is not None:
        accepted = retained_outbound_acceptance(row)
        if accepted is not None:
            if isinstance(accepted.receipt, AcceptedNativeInput) and "remote_event_id" in row:
                native = RecordedInputIdentity.model_validate({"event_id": row["remote_event_id"]})
                if native.event_id != accepted.receipt.event_id:
                    raise ValueError("Observed message acceptance contradicts its retained native identity")
            return AcceptedMessageDispatch(
                phase="ACCEPTED",
                session_id=identity.session_id,
                message_id=identity.message_id,
                acceptance=accepted,
            )
    if "messages" in session:
        index = _StoredMessageIndex.model_validate({"messages": session["messages"]})
        if identity.message_id in index.messages:
            return IndexedMessageDispatch(
                phase="INDEXED_UNREPORTED",
                session_id=identity.session_id,
                message_id=identity.message_id,
                reason="NATIVE_ACCEPTANCE_RECEIPT_NOT_RETAINED",
            )
    if row is not None:
        pending = parse_outbound_input(row)
        if isinstance(pending, AcceptedOutboundInput):
            raise ValueError("Outbound message acceptance projection disagrees")
        return PendingMessageDispatch(
            phase="PENDING",
            session_id=identity.session_id,
            message_id=identity.message_id,
            intent=pending,
        )
    return None


def observe_message_dispatch(session: dict[str, JsonValue], message_id: str) -> RecordedMessageDispatch:
    result = find_message_dispatch(session, message_id)
    if result is None:
        raise ValueError("Session has no actual recorded message or acceptance index")
    return result

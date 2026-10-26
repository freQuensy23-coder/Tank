"""Phase-specific initial input intents and retained acceptance evidence."""

from typing import Literal

from pydantic import Field, JsonValue

from tank.agents.context_binding import ContextBinding
from tank.agents.native_inputs import (
    AcceptedNativeInput,
    NativeInputAcknowledgment,
    NativeUserMessage,
    UnreportedNativeInput,
)
from tank.contracts import DomainModel
from tank.contracts.scalars import ZeroInt
from tank.time_precision import Timestamp, propagate_timestamp


class OutboundSource(DomainModel):
    item_id: str | None = Field(min_length=1)
    observed_at: Timestamp


class InitialInputDefinition(DomainModel):
    message_id: str = Field(min_length=1)
    body: NativeUserMessage = Field(repr=False)
    attempt: ZeroInt
    source: OutboundSource


class PendingOutboundInput(InitialInputDefinition):
    phase: Literal["PENDING"]
    turn_status: Literal["ACCEPTING"]

    def definition(self) -> InitialInputDefinition:
        return InitialInputDefinition(
            message_id=self.message_id, body=self.body, attempt=self.attempt, source=self.source
        )

    def accepted(self, receipt: NativeInputAcknowledgment, sent_at: Timestamp) -> "AcceptedOutboundInput":
        return AcceptedOutboundInput(
            phase="ACCEPTED", message_id=self.message_id, receipt=receipt, sent_at=sent_at
        )


class LegacyUnreportedAcceptance(DomainModel):
    status: Literal["LEGACY_UNREPORTED"]
    reason: Literal["RECEIPT_NOT_RETAINED"]


class AcceptedOutboundInput(DomainModel):
    phase: Literal["ACCEPTED"]
    message_id: str = Field(min_length=1)
    receipt: AcceptedNativeInput | UnreportedNativeInput | LegacyUnreportedAcceptance
    sent_at: Timestamp | None

    @property
    def event_id(self) -> str | None:
        return self.receipt.event_id if isinstance(self.receipt, AcceptedNativeInput) else None


type OutboundInput = PendingOutboundInput | AcceptedOutboundInput


def parse_outbound_input(record: dict[str, JsonValue]) -> OutboundInput:
    identity = RecordedInputIdentity.model_validate({"event_id": record.get("remote_event_id")}).event_id
    receipt = record.get("initial_input_acceptance")
    if "initial_input_acceptance" in record and receipt is None:
        raise ValueError("Retained initial input acceptance cannot be null")
    attempt = MessageAttempt.model_validate({"retry_count": record.get("retry_count")}).retry_count
    if identity is not None or record.get("sent_at") is not None or receipt is not None:
        if receipt is None:
            evidence: NativeInputAcknowledgment | LegacyUnreportedAcceptance = (
                AcceptedNativeInput.model_validate({"status": "ACCEPTED", "event_id": identity})
                if identity is not None and attempt == 0
                else LegacyUnreportedAcceptance(status="LEGACY_UNREPORTED", reason="RECEIPT_NOT_RETAINED")
            )
        else:
            evidence = RecordedReceipt.model_validate({"receipt": receipt}).receipt
        observed = AcceptedOutboundInput(
            phase="ACCEPTED",
            message_id=MessageIdentity.model_validate({"message_id": record["message_id"]}).message_id,
            receipt=evidence,
            sent_at=propagate_timestamp(record, "sent_at") if "sent_at" in record else None,
        )
        if attempt == 0 and observed.event_id != identity:
            raise ValueError("Retained native acceptance contradicts its input identity")
        return observed
    definition = initial_input_definition(record)
    return PendingOutboundInput.model_validate(
        {
            "phase": "PENDING",
            "message_id": definition.message_id,
            "body": definition.body,
            "attempt": definition.attempt,
            "turn_status": record["turn_status"],
            "source": definition.source,
        }
    )


def initial_input_definition(record: dict[str, JsonValue]) -> InitialInputDefinition:
    payload = record["payload"]
    if not isinstance(payload, dict):
        raise ValueError("Outbound input payload must be an object")
    return InitialInputDefinition.model_validate(
        {
            "message_id": record["message_id"],
            "body": record["body"],
            "attempt": record["retry_count"],
            "source": {
                "item_id": payload.get("item_id"),
                "observed_at": propagate_timestamp(record, "item_observed_at"),
            },
        }
    )


class MessageIdentity(DomainModel):
    message_id: str = Field(min_length=1)


class RecordedReceipt(DomainModel):
    receipt: NativeInputAcknowledgment


class MessageAttempt(DomainModel):
    retry_count: int | None = Field(ge=0)


def outbound_message_record(session: dict[str, JsonValue], message_id: str) -> dict[str, JsonValue]:
    journal = session["message_log"]
    if not isinstance(journal, list):
        raise ValueError("Outbound input journal must be an array")
    matches: list[dict[str, JsonValue]] = []
    for record in journal:
        if not isinstance(record, dict):
            raise ValueError("Outbound input journal entries must be objects")
        identity = MessageIdentity.model_validate({"message_id": record["message_id"]}).message_id
        if identity == message_id:
            matches.append(record)
    if len(matches) != 1:
        raise ValueError("Outbound message identity must name exactly one journal entry")
    return matches[0]


class RecordedInputIdentity(DomainModel):
    event_id: str | None = Field(min_length=1)


def recoverable_outbound_inputs(session: dict[str, JsonValue]) -> tuple[PendingOutboundInput, ...]:
    ContextBinding.from_session(session)
    journal = session.get("message_log", [])
    if not isinstance(journal, list):
        raise ValueError("Outbound input journal must be an array")
    pending: list[PendingOutboundInput] = []
    seen: set[str] = set()
    for record in journal:
        if not isinstance(record, dict):
            raise ValueError("Outbound input journal entries must be objects")
        identity = MessageIdentity.model_validate({"message_id": record["message_id"]}).message_id
        if identity in seen:
            raise ValueError("Outbound input journal repeats a message identity")
        seen.add(identity)
        if record.get("turn_status") == "ACCEPTING":
            phase = parse_outbound_input(record)
            if isinstance(phase, PendingOutboundInput):
                pending.append(phase)
    return tuple(pending)


def retained_outbound_acceptance(record: dict[str, JsonValue]) -> AcceptedOutboundInput | None:
    """Read prior acceptance for retirement without consuming unused body or retry budget."""
    if "initial_input_acceptance" in record:
        receipt: NativeInputAcknowledgment | LegacyUnreportedAcceptance = RecordedReceipt.model_validate(
            {"receipt": record["initial_input_acceptance"]}
        ).receipt
    else:
        identity = RecordedInputIdentity.model_validate({"event_id": record.get("remote_event_id")}).event_id
        if identity is None and record.get("sent_at") is None:
            return None
        receipt = LegacyUnreportedAcceptance(status="LEGACY_UNREPORTED", reason="RECEIPT_NOT_RETAINED")
    return AcceptedOutboundInput(
        phase="ACCEPTED",
        message_id=MessageIdentity.model_validate({"message_id": record["message_id"]}).message_id,
        receipt=receipt,
        sent_at=propagate_timestamp(record, "sent_at") if "sent_at" in record else None,
    )

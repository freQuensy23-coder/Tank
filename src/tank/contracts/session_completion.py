"""Consumed source-completion facts and evidence for retiring a failed prior attempt."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import require_integer
from tank.contracts.source_items import ExtractionStatus
from tank.util import parse_time

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
            if kind is not None and (not isinstance(kind, str) or not kind.strip()):
                raise ValueError("Completion message kind must be nonblank text")
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


type NativeExecutionIdentity = Annotated[str, Field(min_length=1, max_length=191)]
type NativeExecutionPhase = Literal["QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"]
type NativeSettledOutcome = Literal["COMPLETED", "FAILED", "CANCELLED"]


class NativeExecutionState(DomainModel):
    execution_id: NativeExecutionIdentity
    state: NativeExecutionPhase
    settled_outcome: NativeSettledOutcome | None

    @model_validator(mode="after")
    def outcome_matches_observed_state(self) -> Self:
        expected = self.state if self.state in {"COMPLETED", "FAILED", "CANCELLED"} else None
        if self.settled_outcome != expected:
            raise ValueError("Native execution outcome contradicts its state")
        return self


class NativeExecutionQuery(DomainModel):
    execution_ids: list[NativeExecutionIdentity] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def identities_are_distinct(self) -> Self:
        if len(set(self.execution_ids)) != len(self.execution_ids):
            raise ValueError("Native execution query repeats an identity")
        return self


class NativeExecutionPage(DomainModel):
    data: list[NativeExecutionState]

    def correlate(self, query: NativeExecutionQuery) -> list[NativeExecutionState]:
        admitted = NativeExecutionPage.model_validate(self)
        requested = NativeExecutionQuery.model_validate(query)
        if [row.execution_id for row in admitted.data] != requested.execution_ids:
            raise ValueError("Native execution response differs from its requested identities")
        return admitted.data


class NativeCompletionStop(DomainModel):
    type: Literal["end_turn", "retries_exhausted", "budget_reached"]


class NativeActionStop(DomainModel):
    type: Literal["requires_action"]
    event_ids: list[CompletionIdentity]


type NativeStopReason = NativeCompletionStop | NativeActionStop


class NativeExecutionReceipt(DomainModel):
    id: CompletionIdentity
    type: Literal[
        "session.status_running",
        "session.status_idle",
        "session.status_rescheduled",
        "session.status_terminated",
    ]
    execution_id: CompletionIdentity
    processed_at: str | None = None
    stop_reason: NativeStopReason | None = None

    @model_validator(mode="after")
    def reported_clock_is_aware(self) -> Self:
        if self.processed_at is not None:
            parse_time(self.processed_at)
        return self

    @property
    def ended_turn(self) -> bool:
        return (
            self.type == "session.status_idle"
            and isinstance(self.stop_reason, NativeCompletionStop)
            and self.stop_reason.type == "end_turn"
        )


class NativeExecutionBarrier(DomainModel):
    session_id: CompletionIdentity
    complete: bool
    accepted_input_ids: list[CompletionIdentity]
    pending_input_ids: list[CompletionIdentity]
    failed_input_ids: list[CompletionIdentity]
    unidentified_inputs: list[CompletionIdentity]
    pending_action_ids: list[CompletionIdentity]
    terminal_receipts: dict[CompletionIdentity, NativeExecutionReceipt]
    execution_states: dict[CompletionIdentity, NativeExecutionState]

    @model_validator(mode="after")
    def identities_and_completion_agree(self) -> Self:
        for identities in (
            self.accepted_input_ids,
            self.pending_input_ids,
            self.failed_input_ids,
            self.unidentified_inputs,
            self.pending_action_ids,
        ):
            if len(set(identities)) != len(identities):
                raise ValueError("Native completion repeats an identity")
        accepted = set(self.accepted_input_ids)
        if not set(self.pending_input_ids) <= accepted or not set(self.failed_input_ids) <= set(
            self.pending_input_ids
        ):
            raise ValueError("Native completion references an unrelated pending input")
        expected_pending = {
            identity
            for identity in accepted
            if identity not in self.execution_states or self.execution_states[identity].state != "COMPLETED"
        }
        expected_failed = {
            identity
            for identity in accepted
            if identity in self.execution_states
            and self.execution_states[identity].state in {"FAILED", "CANCELLED"}
        }
        if set(self.pending_input_ids) != expected_pending or set(self.failed_input_ids) != expected_failed:
            raise ValueError("Native completion pending facts contradict its observed states")
        if any(key != receipt.id for key, receipt in self.terminal_receipts.items()):
            raise ValueError("Native completion receipt identity differs from its key")
        if any(key != state.execution_id for key, state in self.execution_states.items()):
            raise ValueError("Native completion execution identity differs from its key")
        if self.complete and (
            not accepted
            or self.pending_input_ids
            or self.failed_input_ids
            or self.unidentified_inputs
            or self.pending_action_ids
            or not any(
                receipt.execution_id in accepted and receipt.ended_turn
                for receipt in self.terminal_receipts.values()
            )
            or any(
                identity not in self.execution_states or self.execution_states[identity].state != "COMPLETED"
                for identity in accepted
            )
        ):
            raise ValueError("Native completion lacks its required terminal facts")
        return self

    def incomplete(self) -> Self:
        return type(self).model_validate(self.model_copy(update={"complete": False}))

    def storage_record(self) -> dict[str, JsonValue]:
        admitted = NativeExecutionBarrier.model_validate(self)
        return admitted.model_dump(mode="json", exclude={"session_id"}, exclude_unset=True)


class CompletionJournalMessage(CompletionMessage):
    turn_status: Literal["ACCEPTING", "PENDING", "RUNNING", "DONE", "FAILED"] | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        admitted = CompletionMessage.from_record(record)
        return cls.model_validate({**admitted.model_dump(), "turn_status": record.get("turn_status")})


class CompletionToolInput(DomainModel):
    session_id: CompletionIdentity
    call_id: CompletionIdentity
    input_id: CompletionIdentity | None
    message_id: CompletionIdentity | None
    attempt: CompletionAttempt | None
    returned_at: str | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "session_id": record.get("session_id"),
                "call_id": record.get("call_id"),
                "input_id": record.get("remote_event_id"),
                "message_id": record.get("native_turn_message_id"),
                "attempt": record.get("native_turn_retry_count"),
                "returned_at": record.get("returned_at"),
            }
        )

    @model_validator(mode="after")
    def returned_clock_is_aware(self) -> Self:
        if self.returned_at is not None:
            parse_time(self.returned_at)
        return self


class RetriedInputReceipt(DomainModel):
    reason: Literal["TURN_FAILED_AND_RETRIED"]
    failure_event_id: CompletionIdentity | None = None
    replacement_input_id: CompletionIdentity | None = None


class PriorToolInputReceipt(DomainModel):
    reason: Literal["FAILED_PRIOR_TURN_ATTEMPT"]
    queue_state: NativeExecutionState | None = None
    replacement_input_id: CompletionIdentity | None = None
    message_id: CompletionIdentity | None = None

    @model_validator(mode="after")
    def reported_failure_is_actual(self) -> Self:
        if self.queue_state is not None and self.queue_state.state != "FAILED":
            raise ValueError("Prior tool retirement contradicts its observed queue state")
        return self


type SupersededInputReceipt = RetriedInputReceipt | PriorToolInputReceipt


class CompletionSessionSnapshot(DomainModel):
    session_id: CompletionIdentity
    active_message_id: CompletionIdentity | None
    journal: list[CompletionJournalMessage]
    retired_message_ids: list[CompletionIdentity]
    terminal_receipts: dict[CompletionIdentity, NativeExecutionReceipt]
    execution_states: dict[CompletionIdentity, NativeExecutionState]
    superseded_inputs: dict[CompletionIdentity, SupersededInputReceipt]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        raw = record.get("message_log", [])
        if not isinstance(raw, list):
            raise ValueError("Completion message journal must be an array")
        journal: list[CompletionJournalMessage] = []
        for message in raw:
            if not isinstance(message, dict):
                raise ValueError("Completion journal message must be an object")
            journal.append(CompletionJournalMessage.from_record(message))
        return cls.model_validate(
            {
                "session_id": record.get("session_id"),
                "active_message_id": record.get("active_message_id"),
                "journal": journal,
                "retired_message_ids": record.get("retired_message_ids", []),
                "terminal_receipts": record.get("native_execution_receipts", {}),
                "execution_states": record.get("native_execution_states", {}),
                "superseded_inputs": record.get("superseded_native_inputs", {}),
            }
        )

    @model_validator(mode="after")
    def snapshot_identities_are_bound(self) -> Self:
        identities = [message.message_id for message in self.journal]
        if len(set(identities)) != len(identities) or len(set(self.retired_message_ids)) != len(
            self.retired_message_ids
        ):
            raise ValueError("Completion session repeats a message identity")
        if self.active_message_id is not None and self.active_message_id not in identities:
            raise ValueError("Completion active owner is absent from its journal")
        if any(key != receipt.id for key, receipt in self.terminal_receipts.items()):
            raise ValueError("Completion receipt identity differs from its key")
        if any(key != state.execution_id for key, state in self.execution_states.items()):
            raise ValueError("Completion execution identity differs from its key")
        for identity, receipt in self.superseded_inputs.items():
            if receipt.replacement_input_id == identity:
                raise ValueError("Superseded native input cannot replace itself")
            if (
                isinstance(receipt, PriorToolInputReceipt)
                and receipt.queue_state is not None
                and receipt.queue_state.execution_id != identity
            ):
                raise ValueError("Prior tool retirement references another execution")
        return self


def execution_barrier(
    snapshot: CompletionSessionSnapshot, calls: list[CompletionToolInput]
) -> NativeExecutionBarrier:
    session = CompletionSessionSnapshot.model_validate(snapshot)
    admitted = [CompletionToolInput.model_validate(call) for call in calls]
    if len({call.call_id for call in admitted}) != len(admitted):
        raise ValueError("Completion repeats a tool call identity")
    if any(call.session_id != session.session_id for call in admitted):
        raise ValueError("Completion tool input belongs to another session")
    retired = set(session.retired_message_ids)
    required: set[str] = set()
    unknown: list[str] = []
    for message in session.journal:
        if message.message_id in retired:
            continue
        if message.native_input_id is None:
            unknown.append("message:" + message.message_id)
        else:
            required.add(message.native_input_id)
    active_calls = [call for call in admitted if call.message_id not in retired]
    for call in active_calls:
        if call.returned_at is None:
            unknown.append("unreturned-tool:" + call.call_id)
        elif call.input_id is None:
            unknown.append("tool:" + call.call_id)
        else:
            required.add(call.input_id)
    required.difference_update(session.superseded_inputs)
    returned = {call.call_id for call in active_calls if call.returned_at is not None}
    actions = sorted(
        {
            identity
            for receipt in session.terminal_receipts.values()
            if receipt.execution_id in required and isinstance(receipt.stop_reason, NativeActionStop)
            for identity in receipt.stop_reason.event_ids
            if identity not in returned
        }
    )
    pending = sorted(
        identity
        for identity in required
        if identity not in session.execution_states or session.execution_states[identity].state != "COMPLETED"
    )
    failed = sorted(
        identity
        for identity in required
        if identity in session.execution_states
        and session.execution_states[identity].state in {"FAILED", "CANCELLED"}
    )
    ended = any(
        receipt.execution_id in required and receipt.ended_turn
        for receipt in session.terminal_receipts.values()
    )
    return NativeExecutionBarrier(
        session_id=session.session_id,
        complete=bool(required) and not unknown and not pending and not actions and ended,
        accepted_input_ids=sorted(required),
        pending_input_ids=pending,
        failed_input_ids=failed,
        unidentified_inputs=unknown,
        pending_action_ids=actions,
        terminal_receipts=session.terminal_receipts,
        execution_states=session.execution_states,
    )


class SessionCompletionStatus(DomainModel):
    session_id: CompletionIdentity
    status: Literal["OPEN", "FAILED", "ENDED"]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({"session_id": record.get("session_id"), "status": record.get("status")})

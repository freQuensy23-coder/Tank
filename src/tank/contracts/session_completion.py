"""Consumed source-completion facts and evidence for retiring a failed prior attempt."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.scalars import FalseFlag, TrueFlag, require_integer
from tank.contracts.source_items import ExtractionStatus
from tank.time_precision import Timestamp
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


class HostExecutionObservation(DomainModel):
    source: Literal["HOST_MANAGED_EXECUTION_STATUS"]
    observation_id: CompletionIdentity
    session_id: CompletionIdentity
    execution_ids: list[NativeExecutionIdentity] = Field(min_length=1)
    recorded_at: Timestamp
    versions: ResolvedVersionIdentity

    @model_validator(mode="after")
    def actual_observation_scope_and_clock(self) -> Self:
        if len(set(self.execution_ids)) != len(self.execution_ids):
            raise ValueError("Execution observation repeats an input identity")
        if self.recorded_at.value is None:
            raise ValueError("Execution observation requires its actual recorded time")
        return self


class CompletionSessionSnapshot(DomainModel):
    session_id: CompletionIdentity
    active_message_id: CompletionIdentity | None
    journal: list[CompletionJournalMessage]
    retired_message_ids: list[CompletionIdentity]
    terminal_receipts: dict[CompletionIdentity, NativeExecutionReceipt]
    execution_states: dict[CompletionIdentity, NativeExecutionState]
    execution_observation: HostExecutionObservation | None
    superseded_inputs: dict[CompletionIdentity, SupersededInputReceipt]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        if "native_execution_observation" in record and record["native_execution_observation"] is None:
            raise ValueError("Reported execution observation cannot be null")
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
                "execution_observation": record.get("native_execution_observation"),
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
        if self.execution_observation is not None:
            if self.execution_observation.session_id != self.session_id:
                raise ValueError("Execution observation belongs to another session")
            if not set(self.execution_observation.execution_ids) <= self.execution_states.keys():
                raise ValueError("Execution observation lacks its reported state rows")
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


class ReplayAbortTarget(DomainModel):
    session_id: CompletionIdentity
    replay_run_id: CompletionIdentity | None


class ReplayAbortFailure(DomainModel):
    error_type: CompletionIdentity
    reason: str


class ReplayAbortOwnership(DomainModel):
    session_id: CompletionIdentity
    replay_run_id: CompletionIdentity | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], session_id: str) -> Self:
        result = cls.model_validate(
            {"session_id": record["session_id"], "replay_run_id": record.get("replay_run_id")}
        )
        if result.session_id != session_id:
            raise ValueError("Replay abort session identity differs from its actual storage key")
        return result

    def matches(self, target: ReplayAbortTarget) -> bool:
        target = ReplayAbortTarget.model_validate(target)
        return (
            target.replay_run_id is not None
            and self.session_id == target.session_id
            and self.replay_run_id == target.replay_run_id
        )


class ReplayAbortMessage(DomainModel):
    message_id: CompletionIdentity
    turn_status: Literal["ACCEPTING", "PENDING", "RUNNING", "DONE", "FAILED"] | None

    @property
    def unfinished(self) -> bool:
        return self.turn_status not in {"DONE", "FAILED"}


class ReplayAbortJournal(DomainModel):
    messages: tuple[ReplayAbortMessage, ...] | None
    active_message_id: CompletionIdentity | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        messages: list[ReplayAbortMessage] | None = None
        if "message_log" in record:
            raw = record["message_log"]
            if not isinstance(raw, list):
                raise ValueError("Replay abort journal must be an actual array")
            messages = []
            for row in raw:
                if not isinstance(row, dict):
                    raise ValueError("Replay abort message must be an actual object")
                messages.append(
                    ReplayAbortMessage.model_validate(
                        {"message_id": row["message_id"], "turn_status": row.get("turn_status")}
                    )
                )
        return cls.model_validate(
            {
                "messages": tuple(messages) if messages is not None else None,
                "active_message_id": record.get("active_message_id"),
            }
        )

    @model_validator(mode="after")
    def actual_message_ownership(self) -> Self:
        identities = [row.message_id for row in self.messages] if self.messages is not None else []
        if len(identities) != len(set(identities)):
            raise ValueError("Replay abort journal contains duplicate message identities")
        if self.active_message_id is not None and self.active_message_id not in identities:
            raise ValueError("Replay abort active pointer has no actual journal owner")
        return self


class HostReplayAbortReceipt(ReplayAbortFailure):
    origin: Literal["HOST_REPLAY_ABORT"] = "HOST_REPLAY_ABORT"
    replay_run_id: CompletionIdentity
    recorded_at: Timestamp

    @model_validator(mode="after")
    def actual_controller_clock(self) -> Self:
        if self.recorded_at.value is None:
            raise ValueError("Replay abort requires its actual controller clock")
        return self

    def storage_record(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "origin": self.origin,
            "replay_run_id": self.replay_run_id,
            "error_type": self.error_type,
            "reason": self.reason,
            **self.recorded_at.fields("recorded_at"),
        }
        return result


class ReplayAbortRefused(DomainModel):
    session_id: CompletionIdentity
    finalized: FalseFlag = False
    reason: Literal["NOT_OWNED_REPLAY_RUN"] = "NOT_OWNED_REPLAY_RUN"


class ReplayAbortFinalized(DomainModel):
    session_id: CompletionIdentity
    finalized: TrueFlag = True
    failed_message_ids: tuple[CompletionIdentity, ...]


type ReplayAbortOutcome = ReplayAbortRefused | ReplayAbortFinalized

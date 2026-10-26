"""Claimed reaction queue coordination, separate from measured capture evidence."""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from typing import Protocol, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.jobs import JobClaimRevision, JobClaimState, JobEnvelope
from tank.contracts.json import decode_json
from tank.contracts.scheduler_commands import (
    InitialReactionCommand,
    ManualReactionRevisionCommand,
    ReactionAnchorFacts,
    ReactionCaptureCommand,
    ReactionTarget,
    RevisedReactionCommand,
    reaction_command_from_wire,
)
from tank.contracts.scheduler_results import (
    AppliedReactionChange,
    CapturedReaction,
    CoalescedReaction,
    HistoricalCapturedReaction,
    ReactionCaptureResult,
    ReactionFingerprint,
    SchedulerIdentity,
    UnchangedReaction,
    reaction_capture_result,
)
from tank.delivery import DeliveryDeferred
from tank.runtime_contracts import JobPayload
from tank.time_precision import Timestamp, offset_schedule
from tank.util import canonical_json, parse_time, sha256

log = logging.getLogger(__name__)


class ReactionPayload(JobPayload):
    depends_on: list[SchedulerIdentity] = Field(default_factory=list, alias="_depends_on")


class InitialReactionJob(InitialReactionCommand, ReactionPayload):
    cutoff: str = Field(...)
    retro: bool = Field(...)


class ManualReactionJob(ManualReactionRevisionCommand, ReactionPayload):
    anchor: ReactionAnchorFacts = Field(...)
    cutoff: str = Field(...)
    retro: bool = Field(...)


class ChangedReactionJob(RevisedReactionCommand, ReactionPayload):
    pass


type ReactionInput = ChangedReactionJob | ManualReactionJob | InitialReactionJob


class ReactionCapturePort(Protocol):
    async def reaction_capture(self, request: ReactionCaptureCommand) -> ReactionCaptureResult: ...


class ReactionQueue(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    def iter_all(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...


class ReactionClaimLost(RuntimeError):
    pass


class ReactionCoalescingDeferred(DeliveryDeferred):
    def __init__(self, job_id: str, not_before: Timestamp) -> None:
        self.pending = CoalescedReaction(kind="COALESCED", coalesced_into=job_id)
        super().__init__(not_before)


def _reaction_input(payload: JsonValue) -> ReactionInput:
    if not isinstance(payload, dict):
        raise ValueError("Reaction queue payload requires an object")
    revision = payload.get("revision")
    if "automatic_revision" in payload:
        return ChangedReactionJob.model_validate(payload)
    if isinstance(revision, int) and not isinstance(revision, bool) and revision > 1:
        return ManualReactionJob.model_validate(payload)
    return InitialReactionJob.model_validate(payload)


def _command(value: ReactionInput) -> ReactionCaptureCommand:
    return reaction_command_from_wire(
        value.model_dump(mode="json", exclude_unset=True, exclude={"depends_on"})
    )


def reaction_job_command(payload: dict[str, JsonValue]) -> ReactionCaptureCommand:
    return _command(_reaction_input(payload))


class ReactionClaimContext(DomainModel):
    claim: JobClaimState
    original_payload: JsonValue
    payload_sha256: ReactionFingerprint

    @model_validator(mode="after")
    def actual_wire_identity(self) -> Self:
        if self.claim.type != "REACTION_CAPTURE" or not self.claim.job_id.strip():
            raise ValueError("Reaction execution requires its actual queue identity")
        if self.payload_sha256 != sha256(self.original_payload):
            raise ValueError("Reaction claim payload evidence conflicts with its digest")
        return self

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        if "payload" not in record:
            raise ValueError("Reaction claim requires its actual wire payload")
        payload = record["payload"]
        return cls(
            claim=JobClaimState.from_document(record),
            original_payload=payload,
            payload_sha256=sha256(payload),
        )

    @classmethod
    def from_envelope(cls, envelope: JobEnvelope) -> Self:
        return cls(
            claim=JobClaimState(
                job_id=envelope.job_id,
                type=envelope.type,
                status=envelope.status,
                priority=envelope.priority,
                run_after=envelope.run_after,
                expires_at=envelope.expires_at,
                attempts=envelope.attempts,
                max_attempts=envelope.max_attempts,
                recovery_reclaim=envelope.recovery_reclaim,
                recovery_count=envelope.recovery_count,
            ),
            original_payload=envelope.payload,
            payload_sha256=sha256(envelope.payload),
        )

    def same_execution(self, other: Self) -> bool:
        return (
            self.claim.job_id == other.claim.job_id
            and self.claim.attempts == other.claim.attempts
            and self.claim.recovery_count == other.claim.recovery_count
            and self.payload_sha256 == other.payload_sha256
        )


class ReactionJobInvocation(DomainModel):
    context: ReactionClaimContext
    queue_input: ReactionInput

    @model_validator(mode="after")
    def bind_actual_claim_and_input(self) -> Self:
        if self.context.claim.status != "RUNNING":
            raise ValueError("Reaction execution requires its actual running claim")
        admitted = _reaction_input(self.context.original_payload)
        supplied = self.queue_input.model_dump(mode="json", by_alias=True, exclude_unset=True)
        expected = admitted.model_dump(mode="json", by_alias=True, exclude_unset=True)
        if type(admitted) is not type(self.queue_input) or canonical_json(expected) != canonical_json(
            supplied
        ):
            raise ValueError("Reaction command conflicts with its actual claimed payload")
        return self

    @classmethod
    def from_context(cls, context: ReactionClaimContext) -> Self:
        return cls(context=context, queue_input=_reaction_input(context.original_payload))

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.from_context(ReactionClaimContext.from_record(record))

    @classmethod
    def from_envelope(cls, envelope: JobEnvelope) -> Self:
        return cls.from_context(ReactionClaimContext.from_envelope(envelope))

    @property
    def command(self) -> ReactionCaptureCommand:
        return _command(self.queue_input)


class QueuedReactionPeer(DomainModel):
    context: ReactionClaimContext
    queue_input: ChangedReactionJob

    @model_validator(mode="after")
    def actual_queued_payload(self) -> Self:
        if self.context.claim.status != "QUEUED":
            raise ValueError("Reaction arbitration requires a queued peer")
        admitted = ChangedReactionJob.model_validate(self.context.original_payload)
        if canonical_json(
            admitted.model_dump(mode="json", by_alias=True, exclude_unset=True)
        ) != canonical_json(self.queue_input.model_dump(mode="json", by_alias=True, exclude_unset=True)):
            raise ValueError("Reaction peer conflicts with its actual queue payload")
        return self


async def _dependencies_ready(queue: ReactionQueue, dependencies: list[str]) -> bool:
    for identity in dependencies:
        record = await queue.get("ops.jobs", identity)
        if record is None:
            return False
        try:
            state = JobClaimRevision.from_document(record)
        except (KeyError, TypeError, ValueError):
            return False
        if state.job_id != identity or state.status != "DONE":
            return False
    return True


def _edf(claim: JobClaimState, now: datetime) -> tuple[int, bool, datetime, datetime, str]:
    return (
        claim.priority,
        claim.expires_at is None,
        now if claim.expires_at is None else parse_time(claim.expires_at),
        parse_time(claim.run_after),
        claim.job_id,
    )


async def reaction_leader(
    queue: ReactionQueue, invocation: ReactionJobInvocation, now: datetime
) -> JobClaimState:
    leader = invocation.context.claim
    command = invocation.command
    if not isinstance(command, RevisedReactionCommand):
        return leader
    async for record in queue.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE", "status": "QUEUED"}):
        try:
            context = ReactionClaimContext.from_record(record)
            payload = context.original_payload
            if not isinstance(payload, dict) or "automatic_revision" not in payload:
                continue
            peer = QueuedReactionPeer(context=context, queue_input=ChangedReactionJob.model_validate(payload))
            state, request = peer.context.claim, peer.queue_input
            if (request.event_id, request.token_id, request.checkpoint) != (
                command.event_id,
                command.token_id,
                command.checkpoint,
            ):
                continue
            if state.job_id == invocation.context.claim.job_id:
                continue
            if parse_time(state.run_after) > now:
                continue
            if state.expires_at is not None and parse_time(state.expires_at) <= now:
                continue
            if state.attempts >= state.max_attempts and state.recovery_reclaim is not True:
                continue
            if not await _dependencies_ready(queue, request.depends_on):
                continue
            if _edf(state, now) < _edf(leader, now):
                leader = state
        except (KeyError, TypeError, ValueError) as failure:
            log.warning("Rejected queued reaction arbitration candidate: %s", type(failure).__name__)
    return leader


async def _owned_capture(
    scheduler: ReactionCapturePort, command: ReactionCaptureCommand
) -> ReactionCaptureResult:
    capture = asyncio.create_task(scheduler.reaction_capture(command), name="reaction-capture-effect")
    try:
        return await asyncio.shield(capture)
    except asyncio.CancelledError as cancelled:
        capture.cancel()
        while not capture.done():
            try:
                await asyncio.shield(capture)
            except asyncio.CancelledError:
                continue
            except Exception as failure:
                cancelled.add_note("Capture drain raised " + type(failure).__name__)
        if not capture.cancelled():
            drain_error = capture.exception()
            if drain_error is not None:
                cancelled.add_note("Capture drain ended with " + type(drain_error).__name__)
        raise


def validate_reaction_receipt(
    command: ReactionCaptureCommand, outcome: ReactionCaptureResult
) -> ReactionCaptureResult:
    receipt = reaction_capture_result(outcome)
    match receipt:
        case CapturedReaction() | HistoricalCapturedReaction():
            expected_key = f"{receipt.event_id}:{receipt.token_id}:{receipt.checkpoint}:{receipt.revision}"
            if receipt.checkpoint_key != expected_key:
                raise ValueError("Reaction receipt conflicts with its checkpoint key")
            if not isinstance(command, RevisedReactionCommand) and receipt.revision != command.revision:
                raise ValueError("Reaction receipt conflicts with its requested revision")
        case AppliedReactionChange() | UnchangedReaction():
            if not isinstance(command, RevisedReactionCommand) or receipt.change_id != command.change_id:
                raise ValueError("Reaction receipt conflicts with its change command")
    if (
        receipt.event_id != command.event_id
        or receipt.token_id != command.token_id
        or receipt.checkpoint != command.checkpoint
    ):
        raise ValueError("Reaction receipt conflicts with its requested subject")
    return receipt


async def capture_reaction_job(
    scheduler: ReactionCapturePort,
    queue: ReactionQueue,
    invocation: ReactionJobInvocation,
    *,
    clock: Callable[[], Timestamp],
    retry_delay: timedelta,
) -> dict[str, JsonValue]:
    invocation = ReactionJobInvocation.model_validate(invocation)
    command = invocation.command
    target = ReactionTarget(
        event_id=command.event_id, token_id=command.token_id, checkpoint=command.checkpoint
    )
    async with queue.lock("reaction-queue:" + sha256(target.model_dump(mode="json"))):
        record = await queue.get("ops.jobs", invocation.context.claim.job_id)
        if record is None:
            raise ReactionClaimLost("Reaction queue claim is no longer available")
        actual_context = ReactionClaimContext.from_record(record)
        if actual_context.claim.status != "RUNNING" or not invocation.context.same_execution(actual_context):
            raise ReactionClaimLost("Reaction execution claim changed before capture")
        active = ReactionJobInvocation.from_context(actual_context)
        stamp = Timestamp.model_validate(clock())
        if stamp.value is None:
            raise ValueError("Reaction arbitration requires its actual clock")
        leader = await reaction_leader(queue, active, parse_time(stamp.value))
        if leader.job_id != active.context.claim.job_id:
            raise ReactionCoalescingDeferred(leader.job_id, offset_schedule(stamp, retry_delay))
        receipt = validate_reaction_receipt(command, await _owned_capture(scheduler, command))
    serialized = decode_json(receipt.model_dump_json())
    if not isinstance(serialized, dict):
        raise TypeError("Reaction receipt must serialize as an object")
    return serialized

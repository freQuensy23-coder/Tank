"""Queue admission around the concrete reaction capture application port."""

from typing import Protocol

from pydantic import JsonValue

from tank.contracts.json import decode_json
from tank.contracts.scheduler_commands import (
    InitialReactionCommand,
    ManualReactionRevisionCommand,
    ReactionCaptureCommand,
    RevisedReactionCommand,
    reaction_command_from_wire,
)
from tank.contracts.scheduler_results import (
    AppliedReactionChange,
    CapturedReaction,
    CoalescedReaction,
    HistoricalCapturedReaction,
    ReactionCaptureResult,
    UnchangedReaction,
    reaction_capture_result,
)
from tank.runtime_contracts import JobPayload


class InitialReactionJob(InitialReactionCommand, JobPayload):
    pass


class ManualReactionJob(ManualReactionRevisionCommand, JobPayload):
    pass


class ChangedReactionJob(RevisedReactionCommand, JobPayload):
    pass


class ReactionCapturePort(Protocol):
    async def reaction_capture(self, request: ReactionCaptureCommand) -> ReactionCaptureResult: ...


def reaction_job_command(payload: dict[str, JsonValue]) -> ReactionCaptureCommand:
    job: ChangedReactionJob | ManualReactionJob | InitialReactionJob
    revision = payload.get("revision")
    if "automatic_revision" in payload:
        job = ChangedReactionJob.model_validate(payload)
    elif isinstance(revision, int) and not isinstance(revision, bool) and revision > 1:
        job = ManualReactionJob.model_validate(payload)
    else:
        job = InitialReactionJob.model_validate(payload)
    return reaction_command_from_wire(job.model_dump(mode="json", exclude_unset=True, exclude={"depends_on"}))


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
        case CoalescedReaction():
            if not isinstance(command, RevisedReactionCommand):
                raise ValueError("Only a revised reaction can be coalesced")
            return receipt
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
    scheduler: ReactionCapturePort, payload: dict[str, JsonValue]
) -> dict[str, JsonValue]:
    command = reaction_job_command(payload)
    receipt = validate_reaction_receipt(command, await scheduler.reaction_capture(command))
    serialized = decode_json(receipt.model_dump_json())
    if not isinstance(serialized, dict):
        raise TypeError("Reaction receipt must serialize as an object")
    return serialized

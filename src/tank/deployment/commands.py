"""Closed publisher operations and their existing plain-text command output."""

from typing import Literal, assert_never

from tank.contracts import DomainModel
from tank.deployment.contracts import WorkerRestored
from tank.deployment.identities import CommitSHA
from tank.deployment.publisher import Publisher, PublisherStateError

type PublisherAction = Literal[
    "prepare",
    "activate",
    "commit",
    "finish",
    "rollback",
    "rollback_finish",
    "previous_release",
    "recovery_release",
]


class PublisherCommand(DomainModel):
    action: PublisherAction
    release_sha: CommitSHA


class CommandOutput(DomainModel):
    lines: tuple[str, ...]


def execute_command(command: PublisherCommand, publisher: Publisher) -> CommandOutput:
    if command.release_sha != publisher.sha:
        raise PublisherStateError("Publisher command release mismatch")
    action = command.action
    match action:
        case "prepare":
            prepared = publisher.prepare()
            candidate = prepared.candidate
            return CommandOutput(
                lines=(
                    f"worker prepared release={prepared.release_sha} digest={candidate.source_digest} build={candidate.build_id}",
                )
            )
        case "activate":
            publisher.activate()
            return CommandOutput(lines=())
        case "commit":
            committed = publisher.commit()
            return CommandOutput(
                lines=(f"worker committed release={committed.release_sha} build={committed.build_id}",)
            )
        case "finish":
            publisher.finish()
            return CommandOutput(lines=())
        case "rollback":
            outcome = publisher.rollback()
            if isinstance(outcome, WorkerRestored):
                return CommandOutput(lines=(f"worker restored build={outcome.build_id}",))
            return CommandOutput(lines=())
        case "rollback_finish":
            publisher.rollback_finish()
            return CommandOutput(lines=())
        case "previous_release":
            previous = publisher.previous_release()
            return CommandOutput(lines=("" if previous is None else previous,))
        case "recovery_release":
            return CommandOutput(lines=(publisher.recovery_release(),))
        case _:
            assert_never(action)

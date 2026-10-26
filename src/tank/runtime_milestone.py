"""Actual milestone queue admission before the typed scheduler use case."""

from typing import Protocol

from pydantic import JsonValue

from tank.contracts.milestone_checks import MilestoneCheckCommand
from tank.runtime_contracts import MilestoneCheckJob


class MilestoneChecker(Protocol):
    async def milestone_check(self, command: MilestoneCheckCommand) -> None: ...


async def check_milestone_job(checker: MilestoneChecker, payload: dict[str, JsonValue]) -> None:
    job = MilestoneCheckJob.model_validate(payload)
    await checker.milestone_check(job.command())

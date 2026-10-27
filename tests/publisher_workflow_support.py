"""Execute only local archive/package workflow commands, never the production step."""

import subprocess
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict

ROOT = Path(__file__).resolve().parents[1]


class WorkflowStep(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    id: str | None = None
    name: str | None = None
    run: str | None = None


class DeploymentJob(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    steps: list[WorkflowStep]


class DeploymentJobs(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    deploy: DeploymentJob


class DeploymentWorkflow(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    jobs: DeploymentJobs


def step(*, identifier: str | None, name: str | None) -> str:
    raw: object = yaml.safe_load((ROOT / ".github/workflows/deploy.yml").read_text())
    workflow = DeploymentWorkflow.model_validate(raw)
    selected = [
        item
        for item in workflow.jobs.deploy.steps
        if (identifier is not None and item.id == identifier) or (name is not None and item.name == name)
    ]
    assert len(selected) == 1
    command = selected[0].run
    assert command is not None
    return command


def execute(
    command: str, repository: Path, environment: dict[str, str]
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", command],
        cwd=repository,
        env=environment,
        capture_output=True,
        check=False,
    )

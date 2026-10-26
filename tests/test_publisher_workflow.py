"""Execute only local archive/package workflow commands, never the production step."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel, ConfigDict

from tank.deployment.artifact_contracts import PublisherArtifactRuntime
from tank.deployment.contracts import HistoricalRelease

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_RELEASES = (
    "ec4f1e1ff6785d1e4aa39f13efefef0cfa4390ff",
    "002605350ca05fe3ea52d82dbdf74d169ebc954d",
    "36731eba17d50cdb462bc1fb03b104640ff1f145",
    "8d2dd4431e6dd40afc2328f44a2a175ee912d179",
    "fb66082796bed342116c04710c2bccd921312404",
)


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


@pytest.mark.integration(reason="Actual selected historical Git bytes and current archived publisher CLI")
@pytest.mark.parametrize("sha", HISTORICAL_RELEASES)
def test_current_archive_packages_historical_selection_without_executing_its_code(
    tmp_path: Path,
    sha: str,
) -> None:
    repository = tmp_path / "repository"
    subprocess.run(
        ["git", "clone", "--shared", "--no-checkout", str(ROOT), str(repository)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "checkout", "-q", "--detach", "HEAD"], cwd=repository, check=True, capture_output=True
    )
    orchestrator = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    subprocess.run(
        ["git", "update-ref", "refs/remotes/origin/main", sha],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    runner = tmp_path / "runner"
    runner.mkdir()
    output = tmp_path / "target-output"
    environment = dict(os.environ)
    environment.update(INPUT_SHA=sha, RUNNER_TEMP=str(runner), GITHUB_OUTPUT=str(output))
    targeted = execute(step(identifier="target", name=None), repository, environment)
    assert targeted.returncode == 0, targeted.stderr
    assert output.read_text() == f"sha={sha}\norchestrator_sha={orchestrator}\n"
    archive = runner / "tank-orchestrator"
    environment.update(
        SHA=sha,
        ORCHESTRATOR_SHA=orchestrator,
        GITHUB_WORKSPACE=str(repository),
        GITHUB_REPOSITORY="freQuensy23-coder/Tank",
        UV_PROJECT_ENVIRONMENT=sys.prefix,
        PYTHONPATH=str(archive / "src"),
        PYTHONDONTWRITEBYTECODE="1",
    )
    packaged = execute(
        step(identifier=None, name="Package the selected X worker with the main orchestrator"),
        repository,
        environment,
    )
    assert packaged.returncode == 0, packaged.stderr
    release = HistoricalRelease.model_validate_json(packaged.stdout)
    assert release.release_sha == sha
    assert release.worker_mode == ("unused" if sha == HISTORICAL_RELEASES[-1] else "legacy")
    package = runner / sha
    assert not (package / "worker").exists()
    runtime = PublisherArtifactRuntime.model_validate_json((package / "publisher-runtime.json").read_bytes())
    assert runtime.runtime.orchestrator_sha == orchestrator
    assert runtime.runtime.selected_release_sha == sha
    assert orchestrator != sha
    assert (package / "publisher-image").read_text() == runtime.image_reference + "\n"
    assert not (tmp_path / "apify-state").exists()
    assert (archive / "scripts/deploy/publisher_worker.sh").read_bytes() == (
        ROOT / "scripts/deploy/publisher_worker.sh"
    ).read_bytes()

"""Bind an early aggregate to this attempt and preserve its total verification minute."""

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ValidationError,
    field_validator,
    model_validator,
)

from tank.contracts.json import decode_unique_json
from tank.contracts.scalars import FalseFlag

SHA = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
Positive = Annotated[int, Field(gt=0)]
Nonblank = Annotated[str, Field(min_length=1, pattern=r"\S")]
MATRIX_NAMES: tuple[
    Literal["test / Python integration (0)"],
    Literal["test / Python integration (1)"],
    Literal["test / Python integration (2)"],
] = (
    "test / Python integration (0)",
    "test / Python integration (1)",
    "test / Python integration (2)",
)
POST_READY_SECONDS = 60
POLL_SECONDS = 2


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class ApiRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")


class GitRef(ApiRecord):
    sha: SHA


class PullRequest(ApiRecord):
    head: GitRef
    base: GitRef


class PullEvent(ApiRecord):
    pull_request: PullRequest


class Context(StrictModel):
    repository: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")]
    run_id: Positive
    attempt: Positive
    source_head: SHA
    checkout_sha: SHA
    event_name: Nonblank
    workflow_path: Literal[".github/workflows/ci.yml"]
    workflow_name: Nonblank
    runner_name: Nonblank
    job_name: Nonblank


class WaitArguments(StrictModel):
    command: Literal["wait"]
    receipt: Path


class VerifyArguments(StrictModel):
    command: Literal["verify"]
    receipt: Path
    artifacts: Path


class Repository(ApiRecord):
    full_name: str


class Workflow(ApiRecord):
    id: Positive
    path: str


class Run(ApiRecord):
    id: Positive
    workflow_id: Positive
    run_attempt: Positive
    head_sha: SHA
    path: str
    name: str
    event: str
    repository: Repository


class MatrixJob(ApiRecord):
    id: Positive
    name: Literal[
        "test / Python integration (0)",
        "test / Python integration (1)",
        "test / Python integration (2)",
    ]
    run_id: Positive
    run_attempt: Positive
    head_sha: SHA
    workflow_name: str
    status: str = Field(min_length=1)
    conclusion: str | None


class OtherJob(ApiRecord):
    name: str

    @field_validator("name")
    @classmethod
    def unrelated(cls, value: str) -> str:
        if value in MATRIX_NAMES:
            raise ValueError("a matrix job cannot escape required metadata")
        return value


class JobPage(ApiRecord):
    jobs: list[MatrixJob | OtherJob]
    total_count: int = Field(ge=0)


class ArtifactRun(ApiRecord):
    id: Positive
    head_sha: SHA


class Artifact(ApiRecord):
    id: Positive
    name: str
    expired: bool
    size_in_bytes: int = Field(ge=0)
    workflow_run: ArtifactRun | None


class ArtifactPage(ApiRecord):
    artifacts: list[Artifact]
    total_count: int = Field(ge=0)


class SuccessfulJob(MatrixJob):
    status: Literal["completed"]
    conclusion: Literal["success"]


class UsableArtifact(Artifact):
    expired: FalseFlag
    size_in_bytes: Positive
    workflow_run: ArtifactRun


class Receipt(StrictModel):
    context: Context
    workflow_id: Positive
    jobs: tuple[SuccessfulJob, SuccessfulJob, SuccessfulJob]
    artifacts: tuple[UsableArtifact, UsableArtifact, UsableArtifact]
    verification_started_ns: Positive

    @model_validator(mode="after")
    def bound(self) -> Self:
        context = self.context
        if {job.name for job in self.jobs} != set(MATRIX_NAMES) or len({job.id for job in self.jobs}) != 3:
            raise ValueError("exactly three distinct successful matrix jobs are required")
        for job in self.jobs:
            bind_job(context, job)
        names = {artifact_name(context.attempt, index) for index in range(3)}
        if {artifact.name for artifact in self.artifacts} != names or len(
            {a.id for a in self.artifacts}
        ) != 3:
            raise ValueError("exactly three distinct current-attempt artifacts are required")
        for artifact in self.artifacts:
            bind_artifact(context, artifact)
        return self


class ReadApi(Protocol):
    def __call__(self, suffix: str) -> JsonValue: ...


def artifact_name(attempt: int, index: int) -> str:
    if attempt < 1 or index not in range(3):
        raise ValueError("invalid integration artifact scope")
    return f"python-integration-results-attempt-{attempt}-{index}"


def read_context(environment: Mapping[str, str], worktree: Path) -> Context:
    repository = environment["GITHUB_REPOSITORY"]
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("invalid current repository")
    reference = environment["GITHUB_WORKFLOW_REF"]
    prefix = repository + "/"
    if not reference.startswith(prefix):
        raise ValueError("foreign workflow reference")
    path, separator, ref = reference.removeprefix(prefix).partition("@")
    if not separator or not ref or path != ".github/workflows/ci.yml":
        raise ValueError("unsupported current workflow")
    checkout = subprocess.check_output(["git", "-C", str(worktree), "rev-parse", "HEAD"], text=True).strip()
    if checkout != environment["GITHUB_SHA"]:
        raise ValueError("checkout does not match current workflow SHA")
    event_name = environment["GITHUB_EVENT_NAME"]
    source = checkout
    if event_name == "pull_request":
        event = PullEvent.model_validate(
            decode_unique_json(Path(environment["GITHUB_EVENT_PATH"]).read_bytes())
        )
        source = event.pull_request.head.sha
        header = subprocess.check_output(["git", "-C", str(worktree), "cat-file", "-p", "HEAD"], text=True)
        parents = [
            line.removeprefix("parent ")
            for line in header.split("\n\n", 1)[0].splitlines()
            if line.startswith("parent ")
        ]
        if parents != [event.pull_request.base.sha, source]:
            raise ValueError("pull-request checkout has foreign merge parents")
    return Context(
        repository=repository,
        run_id=int(environment["GITHUB_RUN_ID"]),
        attempt=int(environment["GITHUB_RUN_ATTEMPT"]),
        source_head=source,
        checkout_sha=checkout,
        event_name=event_name,
        workflow_path=".github/workflows/ci.yml",
        workflow_name=environment["GITHUB_WORKFLOW"],
        runner_name=environment["RUNNER_NAME"],
        job_name=environment["GITHUB_JOB"],
    )


def bind_run(context: Context, workflow: Workflow, run: Run) -> None:
    if (
        run.id != context.run_id
        or run.run_attempt != context.attempt
        or run.head_sha != context.source_head
        or run.repository.full_name != context.repository
        or run.workflow_id != workflow.id
        or run.path != context.workflow_path
        or workflow.path != context.workflow_path
        or run.name != context.workflow_name
        or run.event != context.event_name
    ):
        raise ValueError("workflow/run/attempt/head identity changed")


def bind_job(context: Context, job: MatrixJob) -> None:
    if (
        job.run_id != context.run_id
        or job.run_attempt != context.attempt
        or job.head_sha != context.source_head
        or job.workflow_name != context.workflow_name
    ):
        raise ValueError("foreign or stale matrix job")


def bind_artifact(context: Context, artifact: UsableArtifact) -> None:
    if artifact.workflow_run.id != context.run_id or artifact.workflow_run.head_sha != context.source_head:
        raise ValueError("foreign or stale integration artifact")


def all_jobs(context: Context, api: ReadApi) -> list[MatrixJob | OtherJob]:
    jobs: list[MatrixJob | OtherJob] = []
    page = 1
    while True:
        response = JobPage.model_validate(
            api(f"/actions/runs/{context.run_id}/attempts/{context.attempt}/jobs?per_page=100&page={page}")
        )
        jobs.extend(response.jobs)
        if len(jobs) >= response.total_count:
            return jobs
        if not response.jobs:
            raise ValueError("matrix jobs pagination made no progress")
        page += 1


def all_artifacts(context: Context, api: ReadApi) -> list[Artifact]:
    artifacts: list[Artifact] = []
    page = 1
    while True:
        response = ArtifactPage.model_validate(
            api(f"/actions/runs/{context.run_id}/artifacts?per_page=100&page={page}")
        )
        artifacts.extend(response.artifacts)
        if len(artifacts) >= response.total_count:
            return artifacts
        if not response.artifacts:
            raise ValueError("integration artifacts pagination made no progress")
        page += 1


def successful_jobs(
    context: Context, jobs: list[MatrixJob | OtherJob]
) -> tuple[SuccessfulJob, SuccessfulJob, SuccessfulJob] | None:
    selected: dict[str, MatrixJob] = {}
    for job in jobs:
        if not isinstance(job, MatrixJob):
            continue
        bind_job(context, job)
        if job.name in selected:
            raise ValueError("duplicate current matrix job")
        selected[job.name] = job
        if job.status == "completed" and job.conclusion != "success":
            raise ValueError("an integration matrix job did not finish successfully")
        if job.status != "completed" and job.conclusion is not None:
            raise ValueError("unfinished matrix job reported a terminal conclusion")
    if set(selected) != set(MATRIX_NAMES) or any(job.status != "completed" for job in selected.values()):
        return None
    values = [SuccessfulJob.model_validate(selected[name].model_dump()) for name in MATRIX_NAMES]
    return values[0], values[1], values[2]


def usable_artifacts(
    context: Context, artifacts: list[Artifact]
) -> tuple[UsableArtifact, UsableArtifact, UsableArtifact] | None:
    expected = tuple(artifact_name(context.attempt, index) for index in range(3))
    selected: dict[str, UsableArtifact] = {}
    for artifact in artifacts:
        if artifact.name not in expected:
            continue
        if artifact.name in selected:
            raise ValueError("duplicate current-attempt integration artifact")
        admitted = UsableArtifact.model_validate(artifact.model_dump())
        bind_artifact(context, admitted)
        selected[artifact.name] = admitted
    if set(selected) != set(expected):
        return None
    values = [selected[name] for name in expected]
    if len({artifact.id for artifact in values}) != 3:
        raise ValueError("integration artifact identities are not distinct")
    return values[0], values[1], values[2]


def wait(
    context: Context,
    api: ReadApi,
    sleep: Callable[[float], None] = time.sleep,
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
) -> Receipt:
    workflow = Workflow.model_validate(api("/actions/workflows/ci.yml"))
    run_path = f"/actions/runs/{context.run_id}"
    bind_run(context, workflow, Run.model_validate(api(run_path)))
    bind_run(context, workflow, Run.model_validate(api(f"{run_path}/attempts/{context.attempt}")))
    while True:
        jobs = successful_jobs(context, all_jobs(context, api))
        if jobs is not None:
            artifacts = usable_artifacts(context, all_artifacts(context, api))
            if artifacts is not None:
                bind_run(context, workflow, Run.model_validate(api(run_path)))
                bind_run(context, workflow, Run.model_validate(api(f"{run_path}/attempts/{context.attempt}")))
                return Receipt(
                    context=context,
                    workflow_id=workflow.id,
                    jobs=jobs,
                    artifacts=artifacts,
                    verification_started_ns=monotonic_ns(),
                )
        sleep(POLL_SECONDS)


def remaining_seconds(receipt: Receipt, now_ns: int) -> float:
    elapsed = now_ns - receipt.verification_started_ns
    if elapsed < 0 or elapsed >= POST_READY_SECONDS * 1_000_000_000:
        raise TimeoutError("total post-readiness verification minute expired")
    return (POST_READY_SECONDS * 1_000_000_000 - elapsed) / 1_000_000_000


def verify(
    receipt: Receipt, context: Context, artifacts: Path, now_ns: Callable[[], int] = time.monotonic_ns
) -> None:
    if receipt.context != context:
        raise ValueError("verification receipt belongs to another job/run/attempt/head/runner")
    expected = {artifact.name for artifact in receipt.artifacts}
    if {path.name for path in artifacts.iterdir()} != expected:
        raise ValueError("downloaded artifact directories do not match admitted identities")
    reports = sorted(path.resolve() for path in artifacts.glob("*/integration.xml"))
    collections = sorted(path.resolve() for path in artifacts.glob("*/integration-collection.json"))
    if len(reports) != 3 or len(collections) != 3:
        raise ValueError("exactly three integration report/collection pairs are required")
    remaining = remaining_seconds(receipt, now_ns())
    command = [
        sys.executable,
        "-m",
        "scripts.ci.check_integration_results",
        *(str(path) for path in reports),
        "--collection",
        *(str(path) for path in collections),
    ]
    result = subprocess.run(command, cwd=Path(__file__).resolve().parents[2], check=False, timeout=remaining)
    if result.returncode:
        raise ValueError("complete integration union checker failed")
    remaining_seconds(receipt, now_ns())


def read_api(context: Context, token: str) -> ReadApi:
    if not token:
        raise ValueError("read-only Actions token is required")
    base = "https://api.github.com/repos/" + context.repository

    def get(suffix: str) -> JsonValue:
        if not suffix.startswith("/actions/"):
            raise ValueError("unsupported read-only Actions resource")
        request = urllib.request.Request(
            base + suffix,
            method="GET",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return decode_unique_json(response.read())

    return get


def write_receipt(path: Path, receipt: Receipt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.parent.stat().st_uid != os.geteuid():
        raise ValueError("readiness receipt directory is not owned")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(receipt.model_dump_json() + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    waiting = commands.add_parser("wait")
    waiting.add_argument("--receipt", type=Path, required=True)
    checking = commands.add_parser("verify")
    checking.add_argument("--receipt", type=Path, required=True)
    checking.add_argument("--artifacts", type=Path, required=True)
    raw_arguments = vars(parser.parse_args())
    try:
        context = read_context(os.environ, Path.cwd())
        if raw_arguments["command"] == "wait":
            args = WaitArguments.model_validate(raw_arguments)
            receipt = wait(context, read_api(context, os.environ["GH_TOKEN"]))
            write_receipt(args.receipt, receipt)
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
                output.write("artifact_ids=" + ",".join(str(a.id) for a in receipt.artifacts) + "\n")
            print(receipt.model_dump_json())
        else:
            checking_args = VerifyArguments.model_validate(raw_arguments)
            raw_receipt = checking_args.receipt.read_bytes()
            decode_unique_json(raw_receipt)
            receipt = Receipt.model_validate_json(raw_receipt)
            verify(receipt, context, checking_args.artifacts)
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, ValidationError) as error:
        print(f"integration completion refused: {type(error).__name__}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()

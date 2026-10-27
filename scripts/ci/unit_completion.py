"""Bind an early aggregate to this attempt and preserve its total verification minute."""

import argparse
import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Literal, Self

from pydantic import (
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from scripts.ci.integration_completion import (
    SHA,
    ApiRecord,
    Artifact,
    Context,
    Positive,
    ReadApi,
    Run,
    StrictModel,
    UsableArtifact,
    VerifyArguments,
    WaitArguments,
    Workflow,
    all_artifacts,
    bind_run,
    read_api,
    read_context,
)
from tank.contracts.json import decode_unique_json

MATRIX_NAMES: tuple[
    Literal["test / Python unit (0)"],
    Literal["test / Python unit (1)"],
    Literal["test / Python unit (2)"],
] = (
    "test / Python unit (0)",
    "test / Python unit (1)",
    "test / Python unit (2)",
)
POST_READY_SECONDS = 60
POLL_SECONDS = 2


class MatrixJob(ApiRecord):
    id: Positive
    name: Literal[
        "test / Python unit (0)",
        "test / Python unit (1)",
        "test / Python unit (2)",
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


class SuccessfulJob(MatrixJob):
    status: Literal["completed"]
    conclusion: Literal["success"]


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


def artifact_name(attempt: int, index: int) -> str:
    if attempt < 1 or index not in range(3):
        raise ValueError("invalid unit artifact scope")
    return f"python-unit-results-attempt-{attempt}-{index}"


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
        raise ValueError("foreign or stale unit artifact")


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
            raise ValueError("a unit matrix job did not finish successfully")
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
            raise ValueError("duplicate current-attempt unit artifact")
        admitted = UsableArtifact.model_validate(artifact.model_dump())
        bind_artifact(context, admitted)
        selected[artifact.name] = admitted
    if set(selected) != set(expected):
        return None
    values = [selected[name] for name in expected]
    if len({artifact.id for artifact in values}) != 3:
        raise ValueError("unit artifact identities are not distinct")
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
    bundles = sorted(path.resolve() for path in artifacts.iterdir())
    if len(bundles) != 3 or any(
        not (bundle / name).is_file()
        for bundle in bundles
        for name in ("unit.xml", "unit-collection.json", "unit-run.json")
    ):
        raise ValueError("exactly three complete unit result bundles are required")
    remaining = remaining_seconds(receipt, now_ns())
    root = Path(__file__).resolve().parents[2]
    command = [
        sys.executable,
        "-m",
        "scripts.ci.check_unit_results",
        "--bundle",
        *(str(path) for path in bundles),
        "--plan",
        str(root / "scripts/ci/unit_shards.json"),
        "--source-sha",
        context.checkout_sha,
    ]
    result = subprocess.run(command, cwd=root, check=False, timeout=remaining)
    if result.returncode:
        raise ValueError("complete unit union checker failed")
    remaining_seconds(receipt, now_ns())


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
        print(f"unit completion refused: {type(error).__name__}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()

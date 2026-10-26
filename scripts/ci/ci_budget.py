"""Reject late CI results and cancel only this workflow when its wall-clock budget expires."""

import os
import time
import urllib.request
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, JsonValue, RootModel

BUDGET_SECONDS = 240
FINISH_RESERVE_SECONDS = 5
REQUIRED_JOBS = {
    "lint-python",
    "lint-go",
    "mypy",
    "test / Python unit (0)",
    "test / Python unit (1)",
    "test / Python unit complete",
    "test / Python integration complete",
    "test / Node local units and contracts",
    "test / Node persistence contracts",
    "capture / go",
    "capture / parity",
}
REQUIRED_IMAGES = {"tank", "tank-capture", "tank-openma", "tank-postgres", "tank-clickhouse"}


class GitHubRecord(BaseModel):
    """Project the required fields from GitHub's extensible API records."""

    model_config = ConfigDict(strict=True, extra="ignore")


class PullRequest(GitHubRecord):
    created_at: str


class Event(GitHubRecord):
    action: str | None = None
    pull_request: PullRequest | None = None


class Run(GitHubRecord):
    created_at: str
    run_attempt: int = Field(ge=1)
    run_started_at: str | None = None


class Job(GitHubRecord):
    name: str
    status: str
    conclusion: str | None
    completed_at: str | None


class JobPage(GitHubRecord):
    jobs: list[Job]
    total_count: int = Field(ge=0)


class Outcome(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    budget_seconds: int
    elapsed_seconds: float


class JsonPayload(RootModel[JsonValue]):
    pass


class Api(Protocol):
    def __call__(self, suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue: ...


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def origin(event: Event, run: Run) -> float:
    if run.run_attempt > 1:
        if run.run_started_at is None:
            raise ValueError("a rerun has no run-start timestamp")
        return min(timestamp(run.created_at), timestamp(run.run_started_at))
    if event.action == "opened" and event.pull_request is not None:
        return timestamp(event.pull_request.created_at)
    return timestamp(run.created_at)


def result(jobs: list[Job], deadline: float) -> bool:
    required: list[Job] = []
    names: set[str] = set()
    images: set[str] = set()
    for job in jobs:
        name = job.name
        if name in REQUIRED_JOBS:
            names.add(name)
            required.append(job)
        elif name.startswith("image ("):
            images.add(name.removeprefix("image (").split(",", 1)[0].rstrip(")"))
            required.append(job)
    if names != REQUIRED_JOBS or images != REQUIRED_IMAGES:
        return False
    if any(job.status != "completed" for job in required):
        return False
    if any(job.conclusion != "success" for job in required):
        raise ValueError("a required CI job did not pass")
    for job in required:
        if job.completed_at is None:
            raise ValueError("a completed CI job has no completion timestamp")
        if timestamp(job.completed_at) > deadline:
            raise ValueError("a required CI job completed after the four-minute deadline")
    return True


def watch(
    event: Event, api: Api, now: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep
) -> Outcome:
    run = Run.model_validate(api("", "GET", None))
    attempt = run.run_attempt
    attempt_path = f"/attempts/{attempt}" if attempt > 1 else ""
    if attempt_path:
        run = Run.model_validate(api(attempt_path, "GET", None))
        if run.run_attempt != attempt:
            raise ValueError("GitHub returned a different run attempt")
    start = origin(event, run)
    deadline = start + BUDGET_SECONDS
    while now() < deadline - FINISH_RESERVE_SECONDS:
        jobs: list[Job] = []
        page = 1
        while True:
            response = JobPage.model_validate(
                api(f"{attempt_path}/jobs?per_page=100&page={page}", "GET", deadline)
            )
            jobs.extend(response.jobs)
            if len(jobs) >= response.total_count:
                break
            if not response.jobs:
                raise ValueError("GitHub jobs pagination made no progress")
            page += 1
        if result(jobs, deadline) and now() < deadline - FINISH_RESERVE_SECONDS:
            return Outcome(budget_seconds=BUDGET_SECONDS, elapsed_seconds=round(now() - start, 2))
        sleep(min(2, max(0, deadline - FINISH_RESERVE_SECONDS - now())))
    print("::error::CI exceeded its 240-second wall-clock budget, including queue and setup", flush=True)
    api("/force-cancel", "POST", None)
    raise TimeoutError("CI did not finish within four minutes")


def main() -> None:
    repository = os.environ["GITHUB_REPOSITORY"]
    run_id = int(os.environ["GITHUB_RUN_ID"])
    token = os.environ["GH_TOKEN"]
    base = f"https://api.github.com/repos/{repository}/actions/runs/{run_id}"

    def api(suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue:
        timeout = 5 if deadline is None else max(0.1, min(5, deadline - time.time()))
        request = urllib.request.Request(
            base + suffix,
            data=b"{}" if method == "POST" else None,
            method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
        return JsonPayload.model_validate_json(body).root if body else None

    event = Event.model_validate_json(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    try:
        outcome = watch(event, api)
    except (ValueError, TimeoutError, OSError) as error:
        print(f"::error::{error}", flush=True)
        raise SystemExit(1) from error
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
        output.write(f"All required checks passed in {outcome.elapsed_seconds} s; limit: 240 s.\n")
    print(outcome.model_dump_json())


if __name__ == "__main__":
    main()

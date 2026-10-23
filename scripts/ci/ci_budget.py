"""Reject late CI results and cancel only this workflow when its wall-clock budget expires."""

import json
import os
import time
import urllib.request
from datetime import datetime
from pathlib import Path

BUDGET_SECONDS = 240
FINISH_RESERVE_SECONDS = 5
REQUIRED_JOBS = {
    "lint-python",
    "lint-go",
    "mypy",
    "test / Python unit",
    "test / Python integration complete",
    "test / Node local units and contracts",
    "capture / go",
    "capture / parity",
}
REQUIRED_IMAGES = {"tank", "tank-capture", "tank-openma", "tank-postgres", "tank-clickhouse"}


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def origin(event: dict, run: dict) -> float:
    if event.get("action") == "opened" and "pull_request" in event:
        return timestamp(event["pull_request"]["created_at"])
    return timestamp(run["created_at"])


def result(jobs: list[dict], deadline: float) -> bool:
    required = []
    names = set()
    images = set()
    for job in jobs:
        name = job["name"]
        if name in REQUIRED_JOBS:
            names.add(name)
            required.append(job)
        elif name.startswith("image ("):
            images.add(name.removeprefix("image (").split(",", 1)[0].rstrip(")"))
            required.append(job)
    if names != REQUIRED_JOBS or images != REQUIRED_IMAGES:
        return False
    if any(job["status"] != "completed" for job in required):
        return False
    if any(job["conclusion"] != "success" for job in required):
        raise ValueError("a required CI job did not pass")
    if any(timestamp(job["completed_at"]) > deadline for job in required):
        raise ValueError("a required CI job completed after the four-minute deadline")
    return True


def watch(event: dict, api, now=time.time, sleep=time.sleep) -> dict:
    run = api("", "GET", None)
    start = origin(event, run)
    deadline = start + BUDGET_SECONDS
    while now() < deadline - FINISH_RESERVE_SECONDS:
        jobs = []
        page = 1
        while True:
            response = api(f"/jobs?per_page=100&page={page}", "GET", deadline)
            jobs.extend(response["jobs"])
            if len(jobs) >= response["total_count"]:
                break
            page += 1
        if result(jobs, deadline) and now() < deadline - FINISH_RESERVE_SECONDS:
            return {"budget_seconds": BUDGET_SECONDS, "elapsed_seconds": round(now() - start, 2)}
        sleep(min(2, max(0, deadline - FINISH_RESERVE_SECONDS - now())))
    print("::error::CI exceeded its 240-second wall-clock budget, including queue and setup", flush=True)
    api("/force-cancel", "POST", None)
    raise TimeoutError("CI did not finish within four minutes")


def main() -> None:
    repository = os.environ["GITHUB_REPOSITORY"]
    run_id = int(os.environ["GITHUB_RUN_ID"])
    token = os.environ["GH_TOKEN"]
    base = f"https://api.github.com/repos/{repository}/actions/runs/{run_id}"

    def api(suffix, method, deadline):
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
        return json.loads(body) if body else {}

    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    try:
        outcome = watch(event, api)
    except (ValueError, TimeoutError, OSError) as error:
        print(f"::error::{error}", flush=True)
        raise SystemExit(1) from error
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
        output.write(f"All required checks passed in {outcome['elapsed_seconds']} s; limit: 240 s.\n")
    print(json.dumps(outcome))


if __name__ == "__main__":
    main()

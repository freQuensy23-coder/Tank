import importlib
import importlib.util
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Literal

import pytest
from pydantic import JsonValue

CI_ROOT = Path(__file__).resolve().parents[1] / "scripts/ci"

if TYPE_CHECKING:
    from scripts.ci import ci_budget as budget
else:
    spec = importlib.util.spec_from_file_location("ci_budget", CI_ROOT / "ci_budget.py")
    assert spec is not None
    assert spec.loader is not None
    budget = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = budget
    spec.loader.exec_module(budget)


@pytest.fixture
def integration_checks(monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, ModuleType]:
    monkeypatch.syspath_prepend(str(CI_ROOT))
    return importlib.import_module("integration_shard"), importlib.import_module("check_integration_results")


def test_shards_preserve_every_file_and_are_stable(integration_checks: tuple[ModuleType, ModuleType]) -> None:
    shard, _ = integration_checks
    paths = [f"tests/test_integration_{index}.py" for index in range(500)]
    partitions = [{path for path in paths if shard.shard_for(path, 3) == index} for index in range(3)]
    assert set.union(*partitions) == set(paths)
    assert sum(map(len, partitions)) == len(paths)
    assert all(partitions)
    before = {path: shard.shard_for(path, 3) for path in paths}
    assert {path: shard.shard_for(path, 3) for path in reversed(paths)} == before
    assert {path: shard.shard_for(path, 3) for path in [*paths, "tests/new.py"] if path in before} == before


@pytest.mark.parametrize("value", ["-1/3", "3/3", "0/0", "0", "a/3", "0/1/2"])
def test_invalid_shard_is_rejected(integration_checks: tuple[ModuleType, ModuleType], value: str) -> None:
    shard, _ = integration_checks
    with pytest.raises(ValueError):
        shard.parse_shard(value)


def reports(tmp_path: Path) -> tuple[Path, list[Path]]:
    expected = [
        {"classname": "tests.test_a", "name": "first", "file": "tests/test_a.py"},
        {"classname": "tests.test_b", "name": "second", "file": "tests/test_b.py"},
    ]
    manifest = tmp_path / "collection.json"
    manifest.write_text(json.dumps(expected))
    paths = []
    for index, case in enumerate(expected):
        root = ET.Element("testsuite")
        ET.SubElement(root, "testcase", classname=case["classname"], name=case["name"])
        path = tmp_path / f"shard-{index}.xml"
        ET.ElementTree(root).write(path)
        paths.append(path)
    return manifest, paths


def test_full_union_requires_every_case_once(
    integration_checks: tuple[ModuleType, ModuleType], tmp_path: Path
) -> None:
    _, check = integration_checks
    manifest, paths = reports(tmp_path)
    assert check.verify(paths, [manifest]) == {"cases": 2, "passed": 2, "skipped": 0}
    for invalid in (paths[:1], paths + paths[:1]):
        with pytest.raises(ValueError, match="full collected"):
            check.verify(invalid, [manifest])


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_union_rejects_unsuccessful_tests(
    integration_checks: tuple[ModuleType, ModuleType], tmp_path: Path, outcome: str
) -> None:
    _, check = integration_checks
    manifest, paths = reports(tmp_path)
    root = ET.parse(paths[0])
    case = root.getroot().find("testcase")
    assert case is not None
    ET.SubElement(case, outcome)
    root.write(paths[0])
    with pytest.raises(ValueError, match="zero failures"):
        check.verify(paths, [manifest])


def test_union_rejects_different_collections(
    integration_checks: tuple[ModuleType, ModuleType], tmp_path: Path
) -> None:
    _, check = integration_checks
    manifest, paths = reports(tmp_path)
    other = tmp_path / "other.json"
    other.write_text("[]")
    with pytest.raises(ValueError, match="different full"):
        check.verify(paths, [manifest, other])


def passed_jobs(completed: str = "2026-10-23T12:03:20Z") -> list[budget.Job]:
    names = sorted(budget.REQUIRED_JOBS)
    names += [f"image ({name}, ., Dockerfile)" for name in sorted(budget.REQUIRED_IMAGES)]
    return [
        budget.Job(name=name, status="completed", conclusion="success", completed_at=completed)
        for name in names
    ]


def test_budget_starts_at_mr_creation_including_workflow_delay() -> None:
    event = budget.Event(action="opened", pull_request=budget.PullRequest(created_at="2026-10-23T12:00:00Z"))
    run = budget.Run(created_at="2026-10-23T12:01:00Z", run_attempt=1)
    assert budget.origin(event, run) == budget.timestamp("2026-10-23T12:00:00Z")


def test_new_commit_gets_its_own_feedback_budget() -> None:
    event = budget.Event(
        action="synchronize", pull_request=budget.PullRequest(created_at="2026-10-22T12:00:00Z")
    )
    run = budget.Run(created_at="2026-10-23T12:00:00Z", run_attempt=1)
    assert budget.origin(event, run) == budget.timestamp(run.created_at)


@pytest.mark.parametrize("action", ["opened", "synchronize"])
def test_rerun_uses_its_attempt_creation_before_queue_and_setup(action: str) -> None:
    calls: list[tuple[str, str, float | None]] = []
    event = budget.Event.model_validate(
        {"action": action, "pull_request": {"created_at": "2026-10-22T12:00:00Z"}}
    )

    def api(suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue:
        calls.append((suffix, method, deadline))
        if not suffix:
            return {"created_at": "2026-10-23T12:00:00Z", "run_attempt": 2}
        if suffix == "/attempts/2":
            return {
                "created_at": "2026-10-23T12:10:01Z",
                "run_started_at": "2026-10-23T12:10:00Z",
                "run_attempt": 2,
            }
        assert suffix == "/attempts/2/jobs?per_page=100&page=1"
        return {
            "jobs": [job.model_dump(mode="json") for job in passed_jobs("2026-10-23T12:11:40Z")],
            "total_count": len(passed_jobs()),
        }

    outcome = budget.watch(
        event,
        api,
        now=lambda: budget.timestamp("2026-10-23T12:11:41Z"),
        sleep=lambda _: pytest.fail("the current attempt is complete"),
    )
    assert outcome.budget_seconds == 240
    assert outcome.elapsed_seconds == 101
    assert calls == [
        ("", "GET", None),
        ("/attempts/2", "GET", None),
        ("/attempts/2/jobs?per_page=100&page=1", "GET", budget.timestamp("2026-10-23T12:14:00Z")),
    ]


def test_rerun_queued_past_its_own_budget_is_cancelled() -> None:
    calls: list[tuple[str, str]] = []

    def api(suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue:
        calls.append((suffix, method))
        if not suffix:
            return {"created_at": "2026-10-23T12:00:00Z", "run_attempt": 2}
        if suffix == "/attempts/2":
            return {
                "created_at": "2026-10-23T12:10:00Z",
                "run_started_at": "2026-10-23T12:13:30Z",
                "run_attempt": 2,
            }
        assert (suffix, method) == ("/force-cancel", "POST")
        return {}

    with pytest.raises(TimeoutError):
        budget.watch(
            budget.Event(),
            api,
            now=lambda: budget.timestamp("2026-10-23T12:14:01Z"),
            sleep=lambda _: pytest.fail("queue and setup must count"),
        )
    assert calls == [("", "GET"), ("/attempts/2", "GET"), ("/force-cancel", "POST")]


def test_first_attempt_does_not_drop_queue_time() -> None:
    run = budget.Run(
        created_at="2026-10-23T12:00:00Z",
        run_started_at="2026-10-23T12:03:30Z",
        run_attempt=1,
    )
    assert budget.origin(budget.Event(), run) == budget.timestamp("2026-10-23T12:00:00Z")


def test_budget_waits_for_all_lanes_and_images() -> None:
    deadline = budget.timestamp("2026-10-23T12:04:00Z")
    jobs = passed_jobs()
    assert budget.result(jobs, deadline)
    assert not budget.result(jobs[:-1], deadline)
    jobs[0].status = "in_progress"
    assert not budget.result(jobs, deadline)


@pytest.mark.parametrize("conclusion", ["failure", "skipped", "cancelled"])
def test_budget_cannot_be_green_when_required_lane_did_not_pass(conclusion: str) -> None:
    jobs = passed_jobs()
    jobs[0].conclusion = conclusion
    with pytest.raises(ValueError, match="did not pass"):
        budget.result(jobs, budget.timestamp("2026-10-23T12:04:00Z"))


def test_budget_rejects_late_success() -> None:
    with pytest.raises(ValueError, match="after"):
        budget.result(
            passed_jobs("2026-10-23T12:04:01Z"),
            budget.timestamp("2026-10-23T12:04:00Z"),
        )


def test_queued_past_deadline_is_cancelled_without_accepting_results() -> None:
    calls: list[tuple[str, str]] = []

    def api(suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue:
        calls.append((suffix, method))
        return {"created_at": "2026-10-23T12:00:00Z", "run_attempt": 1} if not suffix else {}

    def now() -> float:
        return budget.timestamp("2026-10-23T12:04:01Z")

    with pytest.raises(TimeoutError):
        budget.watch(budget.Event(), api, now=now, sleep=lambda _: pytest.fail("must not wait"))
    assert calls == [("", "GET"), ("/force-cancel", "POST")]


def test_watcher_accepts_only_complete_results_within_budget() -> None:
    def api(suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue:
        if not suffix:
            return {"created_at": "2026-10-23T12:00:00Z", "run_attempt": 1}
        assert method == "GET"
        return {
            "jobs": [job.model_dump(mode="json") for job in passed_jobs()],
            "total_count": len(passed_jobs()),
        }

    def now() -> float:
        return budget.timestamp("2026-10-23T12:03:21Z")

    outcome = budget.watch(budget.Event(), api, now=now, sleep=lambda _: pytest.fail("already complete"))
    assert outcome.elapsed_seconds == 201


def test_watcher_rejects_empty_nonterminal_page() -> None:
    calls: list[str] = []

    def api(suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue:
        calls.append(suffix)
        if not suffix:
            return {"created_at": "2026-10-23T12:00:00Z", "run_attempt": 1}
        return {"jobs": [], "total_count": 1}

    with pytest.raises(ValueError, match="pagination made no progress"):
        budget.watch(
            budget.Event(),
            api,
            now=lambda: budget.timestamp("2026-10-23T12:00:01Z"),
            sleep=lambda _: pytest.fail("invalid pagination must not wait"),
        )
    assert calls == ["", "/jobs?per_page=100&page=1"]


def test_completed_job_requires_completion_evidence() -> None:
    jobs = passed_jobs()
    jobs[0].completed_at = None
    with pytest.raises(ValueError, match="no completion timestamp"):
        budget.result(jobs, budget.timestamp("2026-10-23T12:04:00Z"))


@pytest.mark.parametrize("total_count", ["1", -1, True])
def test_github_page_rejects_invalid_count(total_count: str | int) -> None:
    with pytest.raises(ValueError):
        budget.JobPage.model_validate({"jobs": [], "total_count": total_count})


def test_budget_workflow_provisions_frozen_runtime_before_launch() -> None:
    import yaml

    workflow = yaml.safe_load((CI_ROOT.parents[1] / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["budget"]["steps"]
    setup = next(
        index for index, step in enumerate(steps) if step.get("uses", "").startswith("astral-sh/setup-uv@")
    )
    sync = next(
        index
        for index, step in enumerate(steps)
        if step.get("run") == "uv sync --python 3.13.13 --frozen --extra dev"
    )
    launch = next(
        index for index, step in enumerate(steps) if "scripts/ci/ci_budget.py" in step.get("run", "")
    )
    assert setup < sync < launch
    assert steps[setup]["with"]["version"] == "0.11.7"
    assert steps[setup]["with"]["python-version"] == "3.13.13"
    assert steps[launch]["run"] == "uv run --frozen --extra dev python scripts/ci/ci_budget.py"


def test_missing_run_attempt_refuses_before_origin_or_jobs() -> None:
    calls: list[str] = []

    def api(suffix: str, method: Literal["GET", "POST"], deadline: float | None) -> JsonValue:
        calls.append(suffix)
        return {"created_at": "2026-10-23T12:00:00Z"}

    def now() -> float:
        pytest.fail("An unreported attempt must not select budget-origin semantics")

    with pytest.raises(ValueError, match="run_attempt"):
        budget.watch(budget.Event(), api, now=now, sleep=lambda _: pytest.fail("must not wait"))
    assert calls == [""]


@pytest.mark.parametrize("attempt", [0, -1, True, "1", None])
def test_run_attempt_requires_positive_integer(attempt: int | str | None) -> None:
    with pytest.raises(ValueError):
        budget.Run.model_validate({"created_at": "2026-10-23T12:00:00Z", "run_attempt": attempt})

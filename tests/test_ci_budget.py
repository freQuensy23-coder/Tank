import importlib
import json
import runpy
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

CI_ROOT = Path(__file__).resolve().parents[1] / "scripts/ci"
BUDGET = runpy.run_path(str(CI_ROOT / "ci_budget.py"))


@pytest.fixture
def integration_checks(monkeypatch):
    monkeypatch.syspath_prepend(str(CI_ROOT))
    return importlib.import_module("integration_shard"), importlib.import_module("check_integration_results")


def test_shards_preserve_every_file_and_are_stable(integration_checks):
    shard, _ = integration_checks
    paths = [f"tests/test_integration_{index}.py" for index in range(500)]
    partitions = [{path for path in paths if shard.shard_for(path, 3) == index} for index in range(3)]
    assert set.union(*partitions) == set(paths)
    assert sum(map(len, partitions)) == len(paths)
    assert all(partitions)
    assert [shard.shard_for(path, 3) for path in paths] == [
        shard.shard_for(path, 3) for path in reversed(list(reversed(paths)))
    ]


@pytest.mark.parametrize("value", ["-1/3", "3/3", "0/0", "0", "a/3", "0/1/2"])
def test_invalid_shard_is_rejected(integration_checks, value):
    shard, _ = integration_checks
    with pytest.raises(ValueError):
        shard.parse_shard(value)


def reports(tmp_path):
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


def test_full_union_requires_every_case_once(integration_checks, tmp_path):
    _, check = integration_checks
    manifest, paths = reports(tmp_path)
    assert check.verify(paths, [manifest]) == {"cases": 2, "passed": 2, "skipped": 0}
    for invalid in (paths[:1], paths + paths[:1]):
        with pytest.raises(ValueError, match="full collected"):
            check.verify(invalid, [manifest])


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_union_rejects_unsuccessful_tests(integration_checks, tmp_path, outcome):
    _, check = integration_checks
    manifest, paths = reports(tmp_path)
    root = ET.parse(paths[0])
    ET.SubElement(root.getroot().find("testcase"), outcome)
    root.write(paths[0])
    with pytest.raises(ValueError, match="zero failures"):
        check.verify(paths, [manifest])


def test_union_rejects_different_collections(integration_checks, tmp_path):
    _, check = integration_checks
    manifest, paths = reports(tmp_path)
    other = tmp_path / "other.json"
    other.write_text("[]")
    with pytest.raises(ValueError, match="different full"):
        check.verify(paths, [manifest, other])


def passed_jobs(completed="2026-10-23T12:03:20Z"):
    names = sorted(BUDGET["REQUIRED_JOBS"])
    names += [f"image ({name}, ., Dockerfile)" for name in sorted(BUDGET["REQUIRED_IMAGES"])]
    return [
        {"name": name, "status": "completed", "conclusion": "success", "completed_at": completed}
        for name in names
    ]


def test_budget_starts_at_mr_creation_including_workflow_delay():
    event = {"action": "opened", "pull_request": {"created_at": "2026-10-23T12:00:00Z"}}
    run = {"created_at": "2026-10-23T12:01:00Z"}
    assert BUDGET["origin"](event, run) == BUDGET["timestamp"]("2026-10-23T12:00:00Z")


def test_new_commit_gets_its_own_feedback_budget():
    event = {"action": "synchronize", "pull_request": {"created_at": "2026-10-22T12:00:00Z"}}
    run = {"created_at": "2026-10-23T12:00:00Z"}
    assert BUDGET["origin"](event, run) == BUDGET["timestamp"](run["created_at"])


def test_budget_waits_for_all_lanes_and_images():
    deadline = BUDGET["timestamp"]("2026-10-23T12:04:00Z")
    jobs = passed_jobs()
    assert BUDGET["result"](jobs, deadline)
    assert not BUDGET["result"](jobs[:-1], deadline)
    jobs[0]["status"] = "in_progress"
    assert not BUDGET["result"](jobs, deadline)


@pytest.mark.parametrize("conclusion", ["failure", "skipped", "cancelled"])
def test_budget_cannot_be_green_when_required_lane_did_not_pass(conclusion):
    jobs = passed_jobs()
    jobs[0]["conclusion"] = conclusion
    with pytest.raises(ValueError, match="did not pass"):
        BUDGET["result"](jobs, BUDGET["timestamp"]("2026-10-23T12:04:00Z"))


def test_budget_rejects_late_success():
    with pytest.raises(ValueError, match="after"):
        BUDGET["result"](
            passed_jobs("2026-10-23T12:04:01Z"),
            BUDGET["timestamp"]("2026-10-23T12:04:00Z"),
        )


def test_queued_past_deadline_is_cancelled_without_accepting_results():
    calls = []

    def api(suffix, method, deadline):
        calls.append((suffix, method))
        return {"created_at": "2026-10-23T12:00:00Z"} if not suffix else {}

    def now():
        return BUDGET["timestamp"]("2026-10-23T12:04:01Z")

    with pytest.raises(TimeoutError):
        BUDGET["watch"]({}, api, now=now, sleep=lambda _: pytest.fail("must not wait"))
    assert calls == [("", "GET"), ("/force-cancel", "POST")]


def test_watcher_accepts_only_complete_results_within_budget():
    def api(suffix, method, deadline):
        if not suffix:
            return {"created_at": "2026-10-23T12:00:00Z"}
        assert method == "GET"
        return {"jobs": passed_jobs(), "total_count": len(passed_jobs())}

    def now():
        return BUDGET["timestamp"]("2026-10-23T12:03:21Z")

    outcome = BUDGET["watch"]({}, api, now=now, sleep=lambda _: pytest.fail("already complete"))
    assert outcome["elapsed_seconds"] == 201

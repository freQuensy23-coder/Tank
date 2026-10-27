"""Attempt-bound API admission and one total download/checker budget."""

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest
from pydantic import JsonValue, ValidationError

from scripts.ci.integration_completion import Artifact, Context
from scripts.ci.unit_completion import (
    MATRIX_NAMES,
    JobPage,
    MatrixJob,
    Receipt,
    artifact_name,
    remaining_seconds,
    successful_jobs,
    usable_artifacts,
    verify,
    wait,
)


@pytest.fixture
def context() -> Context:
    return Context(
        repository="synthetic/tank",
        run_id=17,
        attempt=2,
        source_head="a" * 40,
        checkout_sha="b" * 40,
        event_name="pull_request",
        workflow_path=".github/workflows/ci.yml",
        workflow_name="ci",
        runner_name="owned-runner",
        job_name="python-unit-complete",
    )


def jobs(context: Context) -> list[MatrixJob]:
    return [
        MatrixJob.model_validate(
            {
                "id": i + 1,
                "name": name,
                "run_id": context.run_id,
                "run_attempt": context.attempt,
                "head_sha": context.source_head,
                "workflow_name": "ci",
                "status": "completed",
                "conclusion": "success",
            }
        )
        for i, name in enumerate(MATRIX_NAMES)
    ]


def artifacts(context: Context) -> list[Artifact]:
    return [
        Artifact.model_validate(
            {
                "id": i + 101,
                "name": artifact_name(context.attempt, i),
                "expired": False,
                "size_in_bytes": 99,
                "workflow_run": {"id": context.run_id, "head_sha": context.source_head},
            }
        )
        for i in range(2)
    ]


def receipt(context: Context) -> Receipt:
    admitted_jobs = successful_jobs(context, list(jobs(context)))
    admitted_artifacts = usable_artifacts(context, artifacts(context))
    assert admitted_jobs is not None and admitted_artifacts is not None
    return Receipt(
        context=context,
        workflow_id=31,
        jobs=admitted_jobs,
        artifacts=admitted_artifacts,
        verification_started_ns=1_000_000_000,
    )


@pytest.mark.parametrize(
    "field,value", [("run_id", 18), ("run_attempt", 1), ("head_sha", "c" * 40), ("workflow_name", "other")]
)
def test_foreign_job_refuses(context: Context, field: str, value: JsonValue) -> None:
    rows = jobs(context)
    payload = rows[0].model_dump()
    payload[field] = value
    rows[0] = MatrixJob.model_validate(payload)
    with pytest.raises(ValueError, match="foreign or stale"):
        successful_jobs(context, list(rows))


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "skipped", None])
def test_completed_unsuccessful_job_refuses(context: Context, conclusion: str | None) -> None:
    rows = jobs(context)
    rows[0] = MatrixJob.model_validate(rows[0].model_dump() | {"conclusion": conclusion})
    with pytest.raises(ValueError, match="did not finish"):
        successful_jobs(context, list(rows))


def test_missing_pending_and_duplicate_are_not_success(context: Context) -> None:
    rows = jobs(context)
    assert successful_jobs(context, list(rows[:1])) is None
    pending = MatrixJob.model_validate(rows[0].model_dump() | {"status": "in_progress", "conclusion": None})
    assert successful_jobs(context, [pending, *rows[1:]]) is None
    with pytest.raises(ValueError, match="duplicate"):
        successful_jobs(context, [*rows, rows[0]])
    with pytest.raises(ValidationError):
        JobPage.model_validate({"total_count": 1, "jobs": [{"name": MATRIX_NAMES[0]}]})


@pytest.mark.parametrize(
    "change",
    [
        {"expired": True},
        {"size_in_bytes": 0},
        {"workflow_run": None},
        {"workflow_run": {"id": 18, "head_sha": "a" * 40}},
    ],
)
def test_unusable_artifact_refuses(context: Context, change: dict[str, JsonValue]) -> None:
    rows = artifacts(context)
    rows[0] = Artifact.model_validate(rows[0].model_dump() | change)
    with pytest.raises((ValueError, ValidationError)):
        usable_artifacts(context, rows)


def test_artifacts_require_current_attempt_unique_ids(context: Context) -> None:
    rows = artifacts(context)
    old = Artifact.model_validate(rows[0].model_dump() | {"name": artifact_name(1, 0)})
    assert usable_artifacts(context, [old, *rows[1:]]) is None
    with pytest.raises(ValueError, match="duplicate"):
        usable_artifacts(context, [*rows, rows[0]])
    rows[1] = Artifact.model_validate(rows[1].model_dump() | {"id": rows[0].id})
    with pytest.raises(ValueError, match="distinct"):
        usable_artifacts(context, rows)


class SyntheticApi:
    def __init__(self, context: Context, *, race: bool = False) -> None:
        self.context = context
        self.race = race
        self.run_reads = 0
        self.paths: list[str] = []

    def __call__(self, suffix: str) -> JsonValue:
        self.paths.append(suffix)
        if suffix == "/actions/workflows/ci.yml":
            return {"id": 31, "path": self.context.workflow_path}
        if suffix.endswith("/jobs?per_page=100&page=1"):
            return {"total_count": 2, "jobs": [row.model_dump(mode="json") for row in jobs(self.context)]}
        if suffix.endswith("/artifacts?per_page=100&page=1"):
            return {
                "total_count": 2,
                "artifacts": [row.model_dump(mode="json") for row in artifacts(self.context)],
            }
        self.run_reads += 1
        return {
            "id": self.context.run_id,
            "workflow_id": 31,
            "run_attempt": 3 if self.race and self.run_reads > 2 else 2,
            "head_sha": self.context.source_head,
            "path": self.context.workflow_path,
            "name": "ci",
            "event": self.context.event_name,
            "repository": {"full_name": self.context.repository},
        }


def test_readiness_rechecks_attempt_after_artifact_publication(context: Context) -> None:
    api = SyntheticApi(context)
    ready = wait(context, api, monotonic_ns=lambda: 1_000_000_000)
    assert [a.id for a in ready.artifacts] == [101, 102]
    assert api.run_reads == 4
    assert any("/attempts/2/jobs" in path for path in api.paths)
    with pytest.raises(ValueError, match="identity changed"):
        wait(context, SyntheticApi(context, race=True))


def prepare_downloads(directory: Path, ready: Receipt) -> None:
    for artifact in ready.artifacts:
        target = directory / artifact.name
        target.mkdir(parents=True)
        (target / "unit.xml").write_text("actual checker receives this file")
        (target / "unit-collection.json").write_text("[]")
        (target / "unit-run.json").write_text("{}")


def test_slow_download_consumes_same_minute(context: Context, tmp_path: Path) -> None:
    ready = receipt(context)
    downloads = tmp_path / "downloads"
    prepare_downloads(downloads, ready)
    with pytest.raises(TimeoutError, match="total post-readiness"):
        verify(ready, context, downloads, now_ns=lambda: 61_000_000_000)
    assert remaining_seconds(ready, 60_000_000_000) == 1


def test_checker_failure_and_late_success_refuse(
    context: Context, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready = receipt(context)
    downloads = tmp_path / "downloads"
    prepare_downloads(downloads, ready)
    timeouts: list[float] = []

    def failed(
        command: list[str], *, cwd: Path, check: bool, timeout: float
    ) -> subprocess.CompletedProcess[bytes]:
        assert command[:4] == [sys.executable, "-m", "scripts.ci.check_unit_results", "--bundle"]
        assert cwd == Path(__file__).resolve().parents[1]
        assert command[-4:] == [
            "--plan",
            str(cwd / "scripts/ci/unit_shards.json"),
            "--source-sha",
            context.checkout_sha,
        ]
        timeouts.append(timeout)
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(subprocess, "run", failed)
    with pytest.raises(ValueError, match="union checker failed"):
        verify(ready, context, downloads, now_ns=lambda: 31_000_000_000)
    assert timeouts == [30.0]

    def succeeded(
        command: list[str], *, cwd: Path, check: bool, timeout: float
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", succeeded)
    clocks = iter([31_000_000_000, 61_000_000_000])
    with pytest.raises(TimeoutError):
        verify(ready, context, downloads, now_ns=lambda: next(clocks))


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.timeout(10)
def test_actual_unit_checker_binds_checkout_plan_and_complete_downloads(
    context: Context, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    import hashlib
    import xml.etree.ElementTree as ET

    from scripts.ci.unit_contracts import (
        UnitCase,
        UnitCollection,
        UnitDeclaration,
        UnitPlan,
        UnitShard,
        measured_assignments,
        read_model,
        weight_coverage,
    )

    root = Path(__file__).resolve().parents[1]
    checkout = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    context = context.model_copy(update={"checkout_sha": checkout})
    assert context.source_head != context.checkout_sha
    payload = receipt(context).model_dump() | {"verification_started_ns": time.monotonic_ns()}
    ready = Receipt.model_validate(payload)
    downloads = Path("downloads")
    plan_path = Path(__file__).resolve().parents[1] / "scripts/ci/unit_shards.json"
    plan = read_model(plan_path, UnitPlan)
    measured = measured_assignments(plan.files)
    files = [next(path for path, assigned in measured.items() if assigned == index) for index in range(2)]
    collection = UnitCollection(
        schema_version="tank-unit-collection-v1",
        source_head=context.checkout_sha,
        cases=[
            UnitCase(classname="synthetic.unit_download", name=f"case_{index}", file=file)
            for index, file in enumerate(files)
        ],
    )
    for index, artifact in enumerate(ready.artifacts):
        directory = downloads / artifact.name
        directory.mkdir(parents=True)
        (directory / "unit-collection.json").write_text(collection.model_dump_json())
        declaration = UnitDeclaration(
            schema_version="tank-unit-declaration-v1",
            source_head=context.checkout_sha,
            shard=UnitShard(index=index, count=2),
            collection_sha256=collection.signature,
            plan_sha256=hashlib.sha256(plan_path.read_bytes()).hexdigest(),
            coverage=weight_coverage(collection, plan),
        )
        (directory / "unit-run.json").write_text(declaration.model_dump_json())
        report = ET.Element("testsuite")
        ET.SubElement(
            report, "testcase", classname="synthetic.unit_download", name=f"case_{index}", time="0.01"
        )
        ET.ElementTree(report).write(directory / "unit.xml")
    verify(ready, context, downloads)
    verify(ready, context, downloads.resolve())
    receipt_path = tmp_path / "unit-ready.json"
    push_context = context.model_copy(update={"source_head": checkout, "event_name": "push"})
    push_payload = receipt(push_context).model_dump() | {
        "verification_started_ns": ready.verification_started_ns
    }
    receipt_path.write_text(Receipt.model_validate(push_payload).model_dump_json())
    environment = {
        "GITHUB_REPOSITORY": context.repository,
        "GITHUB_RUN_ID": str(context.run_id),
        "GITHUB_RUN_ATTEMPT": str(context.attempt),
        "GITHUB_SHA": context.checkout_sha,
        "GITHUB_EVENT_NAME": "push",
        "GITHUB_WORKFLOW": context.workflow_name,
        "GITHUB_WORKFLOW_REF": context.repository + "/.github/workflows/ci.yml@refs/heads/synthetic",
        "RUNNER_NAME": context.runner_name,
        "GITHUB_JOB": context.job_name,
    }
    for key, value in environment.items():
        monkeypatch.setenv(key, value)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.ci.unit_completion",
            "verify",
            "--receipt",
            str(receipt_path),
            "--artifacts",
            str(downloads.resolve()),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=8,
    )
    last = downloads / ready.artifacts[1].name / "unit.xml"
    last.write_bytes((downloads / ready.artifacts[0].name / "unit.xml").read_bytes())
    with pytest.raises(ValueError, match="union checker failed"):
        verify(ready, context, downloads)
    last.write_text("<testsuite/>")
    with pytest.raises(ValueError, match="union checker failed"):
        verify(ready, context, downloads)
    last.unlink()
    with pytest.raises(ValueError, match="complete unit result bundles"):
        verify(ready, context, downloads)


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.timeout(10)
def test_actual_merge_and_nonpr_checkout_identity(tmp_path: Path) -> None:
    from scripts.ci.integration_completion import read_context

    root = tmp_path / "checkout"
    root.mkdir()

    def git(*arguments: str, input_text: str = "") -> str:
        return subprocess.check_output(
            [
                "git",
                "-c",
                "user.name=Synthetic CI",
                "-c",
                "user.email=ci@example.invalid",
                "-C",
                str(root),
                *arguments,
            ],
            text=True,
            input=input_text,
        ).strip()

    git("init", "--quiet")
    tree = git("mktree")
    base = git("commit-tree", tree, input_text="synthetic base\n")
    head = git("commit-tree", tree, "-p", base, input_text="synthetic head\n")
    merge = git("commit-tree", tree, "-p", base, "-p", head, input_text="synthetic PR merge\n")
    git("update-ref", "HEAD", merge)
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"pull_request": {"base": {"sha": base}, "head": {"sha": head}}}))
    environment = {
        "GITHUB_REPOSITORY": "synthetic/tank",
        "GITHUB_RUN_ID": "17",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_WORKFLOW_REF": "synthetic/tank/.github/workflows/ci.yml@refs/pull/1/merge",
        "GITHUB_SHA": merge,
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_EVENT_PATH": str(event),
        "GITHUB_WORKFLOW": "ci",
        "RUNNER_NAME": "synthetic",
        "GITHUB_JOB": "python-unit-complete",
    }
    admitted = read_context(environment, root)
    assert admitted.source_head == head and admitted.checkout_sha == merge
    event.write_text(json.dumps({"pull_request": {"base": {"sha": head}, "head": {"sha": base}}}))
    with pytest.raises(ValueError, match="foreign merge parents"):
        read_context(environment, root)
    environment["GITHUB_EVENT_NAME"] = "push"
    assert read_context(environment, root).source_head == merge
    environment["GITHUB_SHA"] = base
    with pytest.raises(ValueError, match="does not match"):
        read_context(environment, root)


def test_missing_artifact_waits_without_admitting_partial_result(context: Context) -> None:
    class PublishingApi(SyntheticApi):
        publications = 0

        def __call__(self, suffix: str) -> JsonValue:
            if "/artifacts?" in suffix:
                self.publications += 1
                rows = artifacts(context)
                if self.publications == 1:
                    rows = rows[:1]
                return {"total_count": len(rows), "artifacts": [row.model_dump(mode="json") for row in rows]}
            return super().__call__(suffix)

    api = PublishingApi(context)
    sleeps: list[float] = []
    ready = wait(context, api, sleep=sleeps.append, monotonic_ns=lambda: 1_000_000_000)
    assert sleeps == [2]
    assert len(ready.artifacts) == 2

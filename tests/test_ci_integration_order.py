"""Measured slots preserve unknown work and the actual configured native scheduler."""

import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path, PosixPath

import pytest
from pydantic import ValidationError

from scripts.ci.integration_order import (
    IntegrationOrderPlan,
    measured_file_order,
    native_file_order,
    read_collection,
)
from scripts.ci.integration_partition import shard_for
from scripts.ci.unit_contracts import FileTiming, UnitRecord
from tank.contracts.scalars import FalseFlag


def plan(files: list[FileTiming]) -> IntegrationOrderPlan:
    return IntegrationOrderPlan(
        schema_version="tank-integration-order-v1",
        source_head="a" * 40,
        checkout_sha="b" * 40,
        source_tree="c" * 40,
        collection_sha256="d" * 64,
        report_sha256="e" * 64,
        shard_index=2,
        shard_count=3,
        cases=sum(file.cases for file in files),
        files=files,
    )


def test_measured_slots_keep_unknown_and_changed_count_native_positions() -> None:
    files = (
        ["tests/test_short.py"] * 3
        + ["tests/test_unknown.py"] * 2
        + ["tests/test_changed.py"] * 2
        + ["tests/test_long.py"]
        + ["tests/test_zero.py"]
    )
    calibration = plan(
        [
            FileTiming(file="tests/test_short.py", cases=3, seconds=Decimal(1)),
            FileTiming(file="tests/test_long.py", cases=1, seconds=Decimal(20)),
            FileTiming(file="tests/test_zero.py", cases=1, seconds=Decimal(0)),
            FileTiming(file="tests/test_changed.py", cases=1, seconds=Decimal(50)),
            FileTiming(file="tests/test_absent.py", cases=1, seconds=Decimal(40)),
        ]
    )
    native = native_file_order(files)
    order = measured_file_order(files, calibration)
    assert order.files == (
        "tests/test_long.py",
        "tests/test_unknown.py",
        "tests/test_changed.py",
        "tests/test_short.py",
        "tests/test_zero.py",
    )
    assert order.files.index("tests/test_unknown.py") == native.index("tests/test_unknown.py")
    assert order.files.index("tests/test_changed.py") == native.index("tests/test_changed.py")
    assert order.unmeasured_files == ("tests/test_unknown.py",)
    assert order.changed_count_files == ("tests/test_changed.py",)
    assert order.absent_calibration_files == ("tests/test_absent.py",)
    assert "tests/test_zero.py" in order.measured_files
    assert Counter(order.files) == Counter(native)


def test_equal_observed_cost_keeps_actual_native_tie_order() -> None:
    files = ["tests/test_b.py", "tests/test_a.py"]
    calibration = plan([FileTiming(file=file, cases=1, seconds=Decimal(0)) for file in reversed(files)])
    assert measured_file_order(files, calibration).files == tuple(files)


@pytest.mark.parametrize(
    "change",
    [
        {"cases": False},
        {"cases": 1.0},
        {"shard_count": 2},
        {"shard_index": 0},
        {"source_head": "unknown"},
        {"cases": 99},
    ],
)
def test_invalid_calibration_metadata_refuses(change: dict[str, str | bool | int | float]) -> None:
    calibration = plan([FileTiming(file="tests/test_one.py", cases=1, seconds=Decimal(1))])
    with pytest.raises(ValidationError):
        IntegrationOrderPlan.model_validate(calibration.model_dump() | change)


def test_duplicate_files_and_nonfinite_durations_refuse() -> None:
    timing = FileTiming(file="tests/test_one.py", cases=1, seconds=Decimal(1))
    with pytest.raises(ValidationError):
        plan([timing, timing])
    with pytest.raises(ValidationError):
        FileTiming(file="tests/test_one.py", cases=1, seconds=Decimal("NaN"))


class Dispatch(UnitRecord):
    file: str
    node: str
    scheduler: str
    loadscope_reorder: FalseFlag
    automatic_nodes: int


class Execution(UnitRecord):
    nodeid: str
    worker: str


class ProbeAttempt(UnitRecord):
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    dispatches_jsonl: str | None
    executions_jsonl: str | None
    workers_jsonl: str | None


PROBE = """from collections.abc import Generator, Iterator
import os
from pathlib import Path
import pytest
from pydantic import BaseModel, ConfigDict
from xdist.remote import Producer
from xdist.scheduler.loadfile import LoadFileScheduling
from xdist.workermanage import WorkerController

class Dispatch(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    file: str
    node: str
    scheduler: str
    loadscope_reorder: bool
    automatic_nodes: int

class Execution(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    nodeid: str
    worker: str

class WorkerState(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    worker: str
    exitstatus: int
    testscollected: int
    testsfailed: int
    collect_only: bool
    shouldstop: bool | str
    shouldfail: bool | str
    items: tuple[str, ...]

def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if "PYTEST_XDIST_WORKER" not in os.environ:
        return
    record = WorkerState(worker=os.environ["PYTEST_XDIST_WORKER"], exitstatus=int(exitstatus), testscollected=session.testscollected, testsfailed=session.testsfailed, collect_only=session.config.getoption("collectonly"), shouldstop=session.shouldstop, shouldfail=session.shouldfail, items=tuple(item.nodeid for item in session.items))
    with Path(os.environ["WORKER_LOG"]).open("a") as stream:
        stream.write(record.model_dump_json()+"\\n")


@pytest.hookimpl(wrapper=True)
def pytest_xdist_make_scheduler(config: pytest.Config, log: Producer) -> Generator[None, LoadFileScheduling, LoadFileScheduling]:
    scheduler = yield
    assert type(scheduler) is LoadFileScheduling
    assert config.getoption("dist") == "loadfile"
    assert config.getoption("loadscopereorder") is False
    original = scheduler._assign_work_unit
    def observed(node: WorkerController) -> None:
        file = next(iter(scheduler.workqueue))
        record = Dispatch(file=file, node=node.gateway.id, scheduler=type(scheduler).__module__+"."+type(scheduler).__name__, loadscope_reorder=config.getoption("loadscopereorder"), automatic_nodes=scheduler.numnodes)
        with Path(os.environ["DISPATCH_LOG"]).open("a") as stream:
            stream.write(record.model_dump_json()+"\\n")
        original(node)
    setattr(scheduler, "_assign_work_unit", observed)
    return scheduler

@pytest.fixture(autouse=True)
def record_case(request: pytest.FixtureRequest) -> Iterator[None]:
    yield
    assert isinstance(request.node, pytest.Item)
    record = Execution(nodeid=request.node.nodeid, worker=os.environ["PYTEST_XDIST_WORKER"])
    with Path(os.environ["EXECUTION_LOG"]).open("a") as stream:
        stream.write(record.model_dump_json()+"\\n")
"""


@pytest.mark.integration(reason="Actual Git and pinned pytest-xdist console composition")
@pytest.mark.process
@pytest.mark.timeout(10)
def test_actual_console_native_loadfile_scheduler_honors_measured_order(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    root = tmp_path / "synthetic-repository"
    (root / "tests").mkdir(parents=True)
    source = Path(__file__).resolve().parents[1]
    names = [
        f"tests/test_probe_{index}.py"
        for index in range(100)
        if shard_for(f"tests/test_probe_{index}.py", 3) == 2
    ][:2]
    costly, numerous = names
    for file, count in [(costly, 1), (numerous, 3)]:
        (root / file).write_text(
            "import pytest\npytestmark=pytest.mark.integration\n"
            + "\n".join(f"def test_case_{index}() -> None:\n    return None\n" for index in range(count))
        )
    (root / "conftest.py").write_text(PROBE)
    (root / "pyproject.toml").write_text(
        "[tool.pytest.ini_options]\npythonpath = "
        + json.dumps([str(source), str(source / "src")])
        + '\nmarkers = ["integration"]\n'
    )
    subprocess.run(["git", "init", "--quiet", str(root)], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(root), "add", "tests", "conftest.py", "pyproject.toml"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Synthetic CI",
            "-c",
            "user.email=ci@example.invalid",
            "-C",
            str(root),
            "commit",
            "--quiet",
            "-m",
            "synthetic scheduler source",
        ],
        check=True,
        capture_output=True,
    )
    calibration = plan(
        [
            FileTiming(file=costly, cases=1, seconds=Decimal(20)),
            FileTiming(file=numerous, cases=3, seconds=Decimal(1)),
        ]
    )
    (root / "plan.json").write_text(calibration.model_dump_json())
    environment = dict(os.environ)
    environment.pop("PYTEST_XDIST_WORKER", None)
    environment.pop("PYTEST_CURRENT_TEST", None)
    environment["PYTHONPATH"] = str(source / "scripts/ci")
    environment["DISPATCH_LOG"] = str(root / "dispatch.jsonl")
    environment["EXECUTION_LOG"] = str(root / "execution.jsonl")
    environment["WORKER_LOG"] = str(root / "workers.jsonl")
    executable = Path(sys.executable).with_name("pytest")
    common = [
        str(executable),
        "-c",
        str(root / "pyproject.toml"),
        "--confcutdir",
        str(root),
        "-q",
        "-m",
        "integration",
    ]
    collection = subprocess.run(
        [*common, "-p", "collect_integration", "--collect-only"],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
    )
    assert collection.returncode == 0, collection.stdout + collection.stderr
    captured = root / ".test-results/integration-collection.json"
    signature = hashlib.sha256(captured.read_bytes()).hexdigest()
    command = [
        *common,
        "-p",
        "integration_shard",
        "--ci-shard",
        "2/3",
        "--integration-order-plan",
        str(root / "plan.json"),
        "-n",
        "auto",
        "--dist",
        "loadfile",
        "--no-loadscope-reorder",
        "--junitxml=" + str(root / "actual.xml"),
    ]
    result = subprocess.run(
        command, cwd=root, env=environment, capture_output=True, text=True, timeout=8, check=False
    )
    attempt = ProbeAttempt(
        argv=tuple(command),
        returncode=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
        dispatches_jsonl=(root / "dispatch.jsonl").read_text()
        if (root / "dispatch.jsonl").exists()
        else None,
        executions_jsonl=(root / "execution.jsonl").read_text()
        if (root / "execution.jsonl").exists()
        else None,
        workers_jsonl=(root / "workers.jsonl").read_text() if (root / "workers.jsonl").exists() else None,
    )
    attempt_path = Path(".test-results/integration-order-probe-attempt.json")
    attempt_path.parent.mkdir(parents=True, exist_ok=True)
    attempt_path.write_text(attempt.model_dump_json(indent=2) + "\n")
    assert result.returncode == 0, attempt.model_dump_json()
    dispatches = [
        Dispatch.model_validate_json(line) for line in (root / "dispatch.jsonl").read_text().splitlines()
    ]
    executions = [
        Execution.model_validate_json(line) for line in (root / "execution.jsonl").read_text().splitlines()
    ]
    assert [record.file for record in dispatches] == [costly, numerous]
    assert all(
        record.scheduler == "xdist.scheduler.loadfile.LoadFileScheduling" and record.automatic_nodes > 0
        for record in dispatches
    )
    expected = {
        f"{file}::test_case_{index}" for file, count in [(costly, 1), (numerous, 3)] for index in range(count)
    }
    assert Counter(record.nodeid for record in executions) == Counter(expected)
    assert len({record.worker for record in executions if record.nodeid.startswith(numerous + "::")}) == 1
    assert hashlib.sha256(captured.read_bytes()).hexdigest() == signature
    assert isinstance(request.node, pytest.Item)
    request.node.user_properties.append(
        ("native_scheduler_dispatches", json.dumps([record.model_dump() for record in dispatches]))
    )
    request.node.user_properties.append(
        ("native_scheduler_executions", json.dumps([record.model_dump() for record in executions]))
    )
    request.node.user_properties.append(("native_scheduler_argv", json.dumps(command)))
    request.node.user_properties.append(("complete_collection_sha256", signature))
    output = Path(".test-results/integration-order-probe.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "argv": command,
                "dispatches": [record.model_dump() for record in dispatches],
                "executions": [record.model_dump() for record in executions],
                "collection_sha256": signature,
            },
            indent=2,
        )
        + "\n"
    )


def test_collection_digest_binds_exact_admitted_bytes_during_retarget(tmp_path: Path) -> None:
    first = b'[{"classname":"tests.test_before","name":"test_case","file":"tests/test_before.py"}]'
    replacement = b'[{"classname":"tests.test_after","name":"test_case","file":"tests/test_after.py"}]'
    target = tmp_path / "collection.json"
    target.write_bytes(first)

    class RetargetedCollection(PosixPath):
        def read_bytes(self) -> bytes:
            admitted = super().read_bytes()
            target.write_bytes(replacement)
            return admitted

    collection, digest = read_collection(RetargetedCollection(target))
    assert collection.root[0].file == "tests/test_before.py"
    assert digest == hashlib.sha256(first).hexdigest()
    assert target.read_bytes() == replacement
    assert digest != hashlib.sha256(replacement).hexdigest()

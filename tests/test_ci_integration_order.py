"""Measured slots preserve unknown work and the actual configured native scheduler."""

import base64
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path, PosixPath
from typing import Literal

import pytest
from pydantic import ValidationError

from scripts.ci.check_integration_results import verify as verify_integration_results
from scripts.ci.integration_order import (
    IntegrationCollection,
    IntegrationOrderPlan,
    measured_file_order,
    native_file_order,
    read_collection,
)
from scripts.ci.integration_partition import shard_for
from scripts.ci.unit_contracts import FileTiming, UnitCase, UnitRecord
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
    autoload_disabled: bool
    required_xdist: bool
    unrelated_plugins: tuple[str, ...]


class Execution(UnitRecord):
    nodeid: str
    worker: str


class WorkerState(UnitRecord):
    worker: str
    exitstatus: int
    testscollected: int
    testsfailed: int
    collect_only: bool
    shouldstop: bool | str
    shouldfail: bool | str
    items: tuple[str, ...]
    autoload_disabled: bool
    required_xdist: bool
    unrelated_plugins: tuple[str, ...]


class ProbeAttempt(UnitRecord):
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    dispatches_jsonl: str | None
    executions_jsonl: str | None
    workers_jsonl: str | None


class TimedOutProbeAttempt(UnitRecord):
    kind: Literal["TIMED_OUT"] = "TIMED_OUT"
    argv: tuple[str, ...]
    timeout_seconds: float
    stdout_base64: str | None
    stderr_base64: str | None
    dispatches_jsonl: str | None
    executions_jsonl: str | None
    workers_jsonl: str | None
    actual_xml: str | None


def captured_timeout_stream(value: str | bytes | None) -> str | None:
    if value is None:
        return None
    raw = value.encode("utf-8", errors="surrogatepass") if isinstance(value, str) else value
    return base64.b64encode(raw).decode("ascii")


def persist_probe_timeout(root: Path, command: list[str], error: subprocess.TimeoutExpired) -> Path:
    def retained(name: str) -> str | None:
        path = root / name
        return path.read_text() if path.exists() else None

    attempt = TimedOutProbeAttempt(
        argv=tuple(command),
        timeout_seconds=float(error.timeout),
        stdout_base64=captured_timeout_stream(error.stdout),
        stderr_base64=captured_timeout_stream(error.stderr),
        dispatches_jsonl=retained("dispatch.jsonl"),
        executions_jsonl=retained("execution.jsonl"),
        workers_jsonl=retained("workers.jsonl"),
        actual_xml=retained("actual.xml"),
    )
    target = Path(".test-results/integration-order-probe-timeout.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(attempt.model_dump_json(indent=2) + "\n")
    return target


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
    autoload_disabled: bool
    required_xdist: bool
    unrelated_plugins: tuple[str, ...]

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
    autoload_disabled: bool
    required_xdist: bool
    unrelated_plugins: tuple[str, ...]

def unrelated_plugins(config: pytest.Config) -> tuple[str, ...]:
    return tuple(name for name in ("asyncio", "anyio", "timeout", "respx", "xdist.looponfail", "pytest_asyncio.plugin", "anyio.pytest_plugin", "pytest_timeout", "respx.plugin") if config.pluginmanager.hasplugin(name))

def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if "PYTEST_XDIST_WORKER" not in os.environ:
        return
    record = WorkerState(worker=os.environ["PYTEST_XDIST_WORKER"], exitstatus=int(exitstatus), testscollected=session.testscollected, testsfailed=session.testsfailed, collect_only=session.config.getoption("collectonly"), shouldstop=session.shouldstop, shouldfail=session.shouldfail, items=tuple(item.nodeid for item in session.items), autoload_disabled=os.environ.get("PYTEST_DISABLE_PLUGIN_AUTOLOAD")=="1", required_xdist=session.config.pluginmanager.hasplugin("xdist.plugin"), unrelated_plugins=unrelated_plugins(session.config))
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
        record = Dispatch(file=file, node=node.gateway.id, scheduler=type(scheduler).__module__+"."+type(scheduler).__name__, loadscope_reorder=config.getoption("loadscopereorder"), automatic_nodes=scheduler.numnodes, autoload_disabled=os.environ.get("PYTEST_DISABLE_PLUGIN_AUTOLOAD")=="1", required_xdist=config.pluginmanager.hasplugin("xdist.plugin"), unrelated_plugins=unrelated_plugins(config))
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
    environment.pop("PYTEST_PLUGINS", None)
    environment.pop("PYTEST_ADDOPTS", None)
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
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
        "-p",
        "xdist.plugin",
        "-m",
        "integration",
    ]
    declared = IntegrationCollection(
        root=[
            UnitCase(
                classname=file.removesuffix(".py").replace("/", "."),
                name=f"test_case_{index}",
                file=file,
            )
            for file, count in sorted([(costly, 1), (numerous, 3)])
            for index in range(count)
        ]
    )
    captured = root / ".test-results/integration-collection.json"
    captured.parent.mkdir()
    captured.write_text(declared.model_dump_json())
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
    try:
        result = subprocess.run(
            command, cwd=root, env=environment, capture_output=True, text=True, timeout=8, check=False
        )
    except subprocess.TimeoutExpired as error:
        assert isinstance(request.node, pytest.Item)
        try:
            retained_timeout = persist_probe_timeout(root, command, error)
            request.node.user_properties.append(("native_scheduler_timeout", retained_timeout.read_text()))
        except (OSError, UnicodeError, ValidationError) as retention_error:
            request.node.user_properties.append(
                ("native_scheduler_timeout_retention_error", type(retention_error).__name__)
            )
        raise
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
        record.scheduler == "xdist.scheduler.loadfile.LoadFileScheduling"
        and record.automatic_nodes > 0
        and record.autoload_disabled
        and record.required_xdist
        and not record.unrelated_plugins
        for record in dispatches
    )
    expected = {
        f"{file}::test_case_{index}" for file, count in [(costly, 1), (numerous, 3)] for index in range(count)
    }
    assert Counter(record.nodeid for record in executions) == Counter(expected)
    workers = [
        WorkerState.model_validate_json(line) for line in (root / "workers.jsonl").read_text().splitlines()
    ]
    assert len({worker.worker for worker in workers}) == dispatches[0].automatic_nodes == len(workers)
    assert all(
        worker.exitstatus == 0
        and worker.testscollected == 4
        and worker.testsfailed == 0
        and worker.collect_only is False
        and worker.shouldstop is False
        and worker.shouldfail is False
        and Counter(worker.items) == Counter(expected)
        and worker.autoload_disabled
        and worker.required_xdist
        and not worker.unrelated_plugins
        for worker in workers
    )
    assert verify_integration_results([root / "actual.xml"], [captured]) == {
        "cases": 4,
        "passed": 4,
        "skipped": 0,
    }
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
    request.node.user_properties.append(
        ("native_worker_admission", json.dumps([worker.model_dump() for worker in workers]))
    )
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


@pytest.mark.parametrize("output", [None, b"partial\xff", "partial text"])
def test_probe_timeout_retains_actual_partial_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: str | bytes | None
) -> None:
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "synthetic-probe"
    root.mkdir()
    partial = {
        "dispatch.jsonl": '{"file":"tests/test_one.py"}\n',
        "execution.jsonl": "",
        "workers.jsonl": '{"worker":"gw0"}\n',
        "actual.xml": "<testsuites>",
    }
    for name, text in partial.items():
        (root / name).write_text(text)
    command = ["pytest", "-n", "auto"]
    error = subprocess.TimeoutExpired(command, 8, output=output, stderr=b"stderr\x00")
    path = persist_probe_timeout(root, command, error)
    actual = TimedOutProbeAttempt.model_validate_json(path.read_bytes())
    assert actual.kind == "TIMED_OUT"
    assert actual.argv == tuple(command)
    assert actual.timeout_seconds == 8
    assert actual.stdout_base64 == captured_timeout_stream(output)
    assert actual.stderr_base64 is not None
    assert base64.b64decode(actual.stderr_base64) == b"stderr\x00"
    assert actual.dispatches_jsonl == partial["dispatch.jsonl"]
    assert actual.executions_jsonl == ""
    assert actual.workers_jsonl == partial["workers.jsonl"]
    assert actual.actual_xml == "<testsuites>"
    assert error.output is output
    assert error.timeout == 8


def test_probe_timeout_preserves_absent_partial_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    error = subprocess.TimeoutExpired(["pytest"], 8)
    actual = TimedOutProbeAttempt.model_validate_json(
        persist_probe_timeout(tmp_path, ["pytest"], error).read_bytes()
    )
    assert actual.dispatches_jsonl is None
    assert actual.executions_jsonl is None
    assert actual.workers_jsonl is None
    assert actual.actual_xml is None
    assert actual.stdout_base64 is actual.stderr_base64 is None

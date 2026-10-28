import asyncio
import gzip
import hashlib
import json
import os
import threading
from contextlib import asynccontextmanager
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType, SimpleNamespace
from urllib.parse import urlsplit
from uuid import uuid4

import market_truth_full_inputs as full_inputs
import market_truth_latency_worker as worker
import pytest
import test_market_truth_latency_comparison as comparison

from tank.clickhouse_market import ClickHouse, ddl


def source_module(name, *, file=None, paths=()):
    module = ModuleType(name)
    module.__file__ = str(file) if file is not None else None
    module.__spec__ = ModuleSpec(name, loader=None, is_package=file is None)
    if file is None:
        module.__path__ = [str(path) for path in paths]
        module.__spec__.submodule_search_locations = module.__path__
    else:
        module.__spec__.origin = str(file)
        module.__spec__.has_location = True
    return module


@pytest.fixture
def isolated_sources(tmp_path, monkeypatch):
    root = tmp_path / "original"
    package = root / "src" / "tank"
    namespace = package / "market"
    namespace.mkdir(parents=True)
    initializer = package / "__init__.py"
    initializer.write_text("value = 1\n")
    utility = package / "util.py"
    utility.write_text("value = 2\n")
    modules = {
        "tank": source_module("tank", file=initializer),
        "tank.util": source_module("tank.util", file=utility),
        "tank.market": source_module("tank.market", paths=[namespace]),
    }
    monkeypatch.setattr(worker, "sys", SimpleNamespace(modules=modules))
    monkeypatch.setattr(worker, "tank", modules["tank"])
    return root, modules


@pytest.mark.parametrize("namespace_root", [False, True])
def test_imported_sources_records_namespaces_and_files(isolated_sources, monkeypatch, namespace_root):
    root, modules = isolated_sources
    if namespace_root:
        modules["tank"] = source_module("tank", paths=[root / "src" / "tank"])
        monkeypatch.setattr(worker, "tank", modules["tank"])
    report = worker.imported_sources(root)
    assert set(report) == set(modules)
    assert report["tank.util"]["file"] == "src/tank/util.py"
    assert report["tank.util"]["sha256"] == hashlib.sha256(b"value = 2\n").hexdigest()
    assert "src/tank/market" in json.dumps(report["tank.market"])
    assert str(root) not in json.dumps(report)


@pytest.mark.parametrize("foreign_kind", ["file", "namespace", "mixed_namespace", "symlink_namespace"])
def test_imported_sources_rejects_foreign_files_and_namespace_paths(isolated_sources, tmp_path, foreign_kind):
    root, modules = isolated_sources
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    if foreign_kind == "file":
        path = foreign / "util.py"
        path.write_text("value = 3\n")
        modules["tank.injected"] = source_module("tank.injected", file=path)
    else:
        paths = [foreign]
        if foreign_kind == "mixed_namespace":
            paths.insert(0, root / "src" / "tank" / "market")
        elif foreign_kind == "symlink_namespace":
            link = root / "src" / "tank" / "linked"
            link.symlink_to(foreign, target_is_directory=True)
            paths = [link]
        modules["tank.injected"] = source_module("tank.injected", paths=paths)
    with pytest.raises((AssertionError, ValueError)):
        worker.imported_sources(root)


def sample_arguments(tool, execute):
    async def enqueue(*args, **kwargs):
        return {"job_id": "owned-job"}

    async def finish(job_id):
        return None

    repo = SimpleNamespace(enqueue=enqueue, finish_job=finish)
    meter = worker.Measurements()
    runtime = SimpleNamespace(truth=SimpleNamespace(_assessment_baselines={}), worker=execute)
    arguments = (
        "cold",
        repo,
        {},
        None,
        SimpleNamespace(tool=tool),
        runtime,
        meter,
        {},
        [],
        Path("."),
        {"semantic_cutoff": "2026-10-28T10:00:00Z"},
        {},
        {},
    )
    return arguments, repo, meter, (enqueue, finish)


@pytest.mark.parametrize("after_admission", [False, True])
async def test_sample_preserves_failure_and_settles_tasks_before_restoring_hooks(after_admission):
    failure = LookupError("private exception text must not be exported")
    started, settled = asyncio.Event(), asyncio.Event()
    owned = []

    async def tool(*args):
        owned.append(asyncio.current_task())
        if after_admission:
            await repo.enqueue("RUN_MARKET_TRUTH")
            await started.wait()
        raise failure

    async def execute(stop):
        owned.append(asyncio.current_task())
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            settled.set()
            raise RuntimeError("secondary cleanup failure")

    arguments, repo, meter, originals = sample_arguments(tool, execute)
    with pytest.raises(LookupError) as raised:
        await worker.run_sample(*arguments)
    assert raised.value is failure
    assert (repo.enqueue, repo.finish_job) == originals
    assert all(task.done() for task in owned)
    assert settled.is_set() is after_admission
    assert meter.cleanup["hooks_restored"] is True
    assert meter.cleanup["settled_tasks"] == meter.cleanup["owned_tasks"] == (3 if after_admission else 2)
    assert meter.active is False


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_owned_repository_exits_after_sample_hooks_are_restored(tmp_path, capsys, cleanup_fails):
    failure = LookupError("primary sample failure")
    teardown = []

    async def tool(*args):
        raise failure

    async def execute(stop):
        pytest.fail("No worker is admitted")

    arguments, repo, meter, originals = sample_arguments(tool, execute)
    name = "tank_test_fixture_" + "a" * 32
    repo.database_url = "dbname=" + name
    repo.clickhouse = SimpleNamespace(database="mkt_" + name)

    @asynccontextmanager
    async def repository(route):
        try:
            yield repo
        finally:
            assert (repo.enqueue, repo.finish_job) == originals
            assert meter.cleanup["settled_tasks"] == meter.cleanup["owned_tasks"] == 2
            teardown.append(name)
            if cleanup_fails:
                raise ValueError("private cleanup error")

    ownership = {"repository_cleanup": "NOT_ACQUIRED"}
    with pytest.raises(LookupError) as raised:
        async with worker.owned_repository(
            SimpleNamespace(repository=repository), "owned-route", ownership, tmp_path
        ):
            await worker.run_sample(*arguments)
    assert raised.value is failure and teardown == [name]
    receipt = json.loads((tmp_path / "ownership.json").read_text())
    assert receipt["postgres_database"] == name
    assert receipt["clickhouse_database"] == "mkt_" + name
    assert receipt["repository_cleanup"] == ("NOT_CONFIRMED" if cleanup_fails else "CONTEXT_EXIT_COMPLETED")
    if cleanup_fails:
        assert receipt["cleanup_failure"]["type"] == "ValueError"
    assert all(json.loads(line)["kind"] == "ownership" for line in capsys.readouterr().out.splitlines())


async def test_cancelled_sample_waits_for_real_executor_work_and_repeated_cancellation():
    entered, cancelled = asyncio.Event(), asyncio.Event()
    release, completed = threading.Event(), threading.Event()
    loop = asyncio.get_running_loop()

    def compute():
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release.wait(2), "Test failed to release its owned thread"
        finally:
            completed.set()

    async def tool(*args):
        await repo.enqueue("RUN_MARKET_TRUTH")
        await asyncio.Event().wait()

    async def execute(stop):
        try:
            await worker.cancel_safe_thread(meter, asyncio.to_thread(compute))
        finally:
            cancelled.set()

    arguments, repo, meter, originals = sample_arguments(tool, execute)
    sample = asyncio.create_task(worker.run_sample(*arguments))
    try:
        await entered.wait()
        sample.cancel("primary cancellation")
        await cancelled.wait()
        assert not completed.is_set() and not sample.done()
        sample.cancel("secondary cancellation")
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await sample
    assert raised.value.args == ("primary cancellation",)
    assert completed.is_set()
    assert (repo.enqueue, repo.finish_job) == originals
    assert meter.cleanup["settled_tasks"] == meter.cleanup["owned_tasks"] == 3
    assert meter.cleanup["executor_submissions_settled"] == meter.cleanup["executor_submissions"] == 1


@pytest.mark.parametrize("shutdown", ["close", "eof", "cancel"])
async def test_control_interrupts_active_sample_and_awaits_its_finalizer(shutdown):
    reader = asyncio.StreamReader()
    entered, finalizing, release, settled = (asyncio.Event() for _ in range(4))
    owned = []
    emitted = []

    async def sample(state):
        owned.append(asyncio.current_task())
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalizing.set()
            await release.wait()
            settled.set()

    controller = asyncio.create_task(worker.serve_commands(reader, sample, emitted.append))
    reader.feed_data(b'{"operation":"cold"}\n')
    await entered.wait()
    if shutdown == "close":
        reader.feed_data(b'{"operation":"close"}\n')
    elif shutdown == "eof":
        reader.feed_eof()
    else:
        controller.cancel("outer shutdown")
    await finalizing.wait()
    assert not controller.done() and not settled.is_set()
    release.set()
    if shutdown == "cancel":
        with pytest.raises(asyncio.CancelledError, match="outer shutdown"):
            await controller
    else:
        await controller
    assert settled.is_set() and all(task.done() for task in owned)
    assert emitted == []


async def test_control_retains_sample_failure_identity():
    reader = asyncio.StreamReader()
    failure = RuntimeError("sample failed")

    async def sample(state):
        raise failure

    reader.feed_data(b'{"operation":"cold"}\n')
    with pytest.raises(RuntimeError) as raised:
        await worker.serve_commands(reader, sample, lambda result: None)
    assert raised.value is failure
    reader.feed_eof()
    assert await reader.readline() == b""


async def test_sigterm_callback_awaits_cleanup_and_ignores_repeated_signal(monkeypatch, capsys):
    entered, finalizing, release, settled = (asyncio.Event() for _ in range(4))
    handlers = {}
    loop = asyncio.get_running_loop()

    def register(signum, callback):
        handlers[signum] = callback

    monkeypatch.setattr(loop, "add_signal_handler", register)
    monkeypatch.setattr(loop, "remove_signal_handler", lambda signum: handlers.pop(signum))

    async def run_worker(ownership):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalizing.set()
            await release.wait()
            ownership["repository_cleanup"] = "CONTEXT_EXIT_COMPLETED"
            settled.set()

    monkeypatch.setattr(worker, "run_worker", run_worker)
    task = asyncio.create_task(worker.main())
    await entered.wait()
    handlers[worker.signal.SIGTERM]()
    await finalizing.wait()
    handlers[worker.signal.SIGTERM]()
    assert not settled.is_set() and not task.done()
    release.set()
    await task
    assert settled.is_set() and handlers == {}
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["shutdown_signal"] == "SIGTERM"
    assert receipt["repository_cleanup"] == "CONTEXT_EXIT_COMPLETED"


async def test_parent_close_drains_receipt_after_active_sample_cleanup(tmp_path):
    commands, output = asyncio.StreamReader(), asyncio.StreamReader()
    entered, finalizing, release, settled = (asyncio.Event() for _ in range(4))
    lifecycle = {"receipts": [], "shutdown_actions": []}
    ownership = {
        "postgres_database": "tank_test_fixture_" + "a" * 32,
        "clickhouse_database": "mkt_tank_test_fixture_" + "a" * 32,
    }

    async def sample(state):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalizing.set()
            await release.wait()
            settled.set()

    async def child():
        await worker.serve_commands(commands, sample, lambda result: None)
        assert settled.is_set()
        receipt = {"kind": "shutdown", **ownership, "repository_cleanup": "CONTEXT_EXIT_COMPLETED"}
        output.feed_data(json.dumps(receipt).encode() + b"\n")
        output.feed_eof()
        process.returncode = 0

    async def drain():
        return None

    def force():
        pytest.fail("Cooperative close must not terminate or kill its child")

    process = SimpleNamespace(
        pid=123,
        returncode=None,
        stdout=output,
        stdin=SimpleNamespace(write=commands.feed_data, drain=drain, close=commands.feed_eof),
        terminate=force,
        kill=force,
    )
    child_task = asyncio.create_task(child())

    async def wait():
        await asyncio.shield(child_task)
        return process.returncode

    process.wait = wait
    commands.feed_data(b'{"operation":"cold"}\n')
    await entered.wait()
    checkpoint_path = tmp_path / "closing-progress.json"

    def checkpoint():
        comparison.checkpoint_progress(checkpoint_path, {}, {}, {"original": lifecycle})

    close = asyncio.create_task(comparison.close_worker(process, lifecycle, checkpoint))
    await finalizing.wait()
    assert not close.done() and process.returncode is None
    release.set()
    await close
    assert child_task.done() and settled.is_set()
    assert lifecycle["shutdown_actions"] == ["CLOSE_AND_STDIN_EOF"]
    assert lifecycle["cleanup_confirmed"] is True
    assert lifecycle["ownership"]["postgres_database"] == ownership["postgres_database"]
    assert lifecycle["ownership"]["clickhouse_database"] == ownership["clickhouse_database"]
    assert lifecycle["exit_code"] == 0
    current = json.loads(checkpoint_path.read_bytes())["lifecycles"]["original"]
    assert current == lifecycle and current["cleanup_confirmed"] is True


def test_public_export_keeps_exact_derived_files_and_binds_bytes(tmp_path):
    source = tmp_path / "original-run"
    source.mkdir()
    raw = b'{"relative":{"entry_ref_price":"0.20290000"}}'
    (source / "cold-result.json").write_bytes(raw)
    for name in ("stderr.log", "application.log", "environment.json"):
        (source / name).write_text("private-operator-secret")
    destination = tmp_path / "export"
    receipt = comparison.export_evidence(tmp_path, destination, {"comparison_id": "owned"}, {}, {})
    assert {path.name for path in destination.iterdir()} == {
        "original-cold-result.json",
        "receipts.json",
        "manifest.json",
    }
    manifest = json.loads((destination / "manifest.json").read_text())
    entry = manifest["files"]["original-cold-result.json"]
    assert entry["source_bytes_sha256"] == hashlib.sha256(raw).hexdigest()
    exported = (destination / "original-cold-result.json").read_bytes()
    assert entry["export_sha256"] == hashlib.sha256(exported).hexdigest()
    manifest_bytes = (destination / "manifest.json").read_bytes()
    assert receipt["manifest_sha256"] == hashlib.sha256(manifest_bytes).hexdigest()
    assert all(b"private-operator-secret" not in path.read_bytes() for path in destination.iterdir())


@pytest.mark.parametrize(
    "payload",
    [
        {"result": [{"password": "hidden"}]},
        {"content": [{"text": '{"nested":{"access_token":"hidden"}}'}]},
        {"result": "postgresql://private:secret@localhost/database"},
        {"result": "operator@example.invalid"},
    ],
)
def test_public_export_rejects_private_nested_content_before_creating_bundle(tmp_path, payload):
    source = tmp_path / "original-run"
    source.mkdir()
    (source / "cold-result.json").write_text(json.dumps(payload))
    destination = tmp_path / "export"
    with pytest.raises(ValueError, match="Forbidden public evidence"):
        comparison.export_evidence(tmp_path, destination, {}, {}, {})
    assert not destination.exists()


def test_public_export_enforces_file_and_bundle_bounds(tmp_path, monkeypatch):
    source = tmp_path / "candidate-run"
    source.mkdir()
    (source / "cold-result.json").write_text(json.dumps({"result": "x" * 100}))
    monkeypatch.setattr(comparison, "PUBLIC_FILE_LIMIT", 32)
    with pytest.raises(ValueError, match="Unbounded"):
        comparison.export_evidence(tmp_path, tmp_path / "file-limit", {}, {}, {})
    monkeypatch.setattr(comparison, "PUBLIC_FILE_LIMIT", 8000)
    monkeypatch.setattr(comparison, "PUBLIC_TOTAL_LIMIT", 32)
    with pytest.raises(ValueError, match="bundle exceeds"):
        comparison.export_evidence(tmp_path, tmp_path / "bundle-limit", {}, {}, {})
    assert not (tmp_path / "file-limit").exists() and not (tmp_path / "bundle-limit").exists()


def test_failure_receipt_has_source_frames_but_no_exception_text():
    try:
        raise RuntimeError("postgresql://operator:private-secret@localhost/private")
    except RuntimeError as error:
        receipt = worker.safe_failure(error)
    assert receipt["type"] == "RuntimeError"
    assert receipt["source_frames"][-1]["file"] == "tests/test_market_truth_latency_lifecycle.py"
    frame = receipt["source_frames"][-1]
    assert frame["function"] == "test_failure_receipt_has_source_frames_but_no_exception_text"
    assert frame["source_sha256"] == hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    assert "private-secret" not in json.dumps(receipt)
    comparison.validate_public(receipt)


def test_setup_progress_records_stage_and_elapsed_wall_and_cpu_without_callback_cost(monkeypatch):
    clock = {"wall": 10.0, "cpu": 2.0}
    emitted = []
    monkeypatch.setattr(full_inputs, "perf_counter", lambda: clock["wall"])
    monkeypatch.setattr(full_inputs, "process_time", lambda: clock["cpu"])

    def emit(receipt):
        emitted.append(receipt)
        clock["wall"] += 100
        clock["cpu"] += 50

    progress = full_inputs.StageProgress(emit)
    clock.update(wall=13.0, cpu=3.5)
    progress.record("source_preflight_complete")
    clock.update(wall=117.0, cpu=54.0)
    progress.record("collection_native_import_complete", collection="mkt.klines_1m", rows=7)
    first, second = emitted
    assert first["wall_seconds"] == 3.0 and first["process_cpu_seconds"] == 1.5
    assert second["wall_seconds"] == 4.0 and second["process_cpu_seconds"] == 0.5
    assert second["elapsed_wall_seconds"] == 107.0
    assert second["elapsed_process_cpu_seconds"] == 52.0
    assert second["rows"] == 7 and second["collection"] == "mkt.klines_1m"
    assert all(item["completion"] == "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME" for item in emitted)
    for receipt in emitted:
        comparison.public_bytes(receipt)


@pytest.mark.parametrize(
    ("stage", "details"),
    [
        ("postgresql://private:secret@localhost/private", {}),
        ("ready", {"exception": "private text"}),
        ("sample_start", {"state": "operator@example.invalid"}),
        ("collection_native_import_complete", {"collection": "private.table", "rows": 1}),
        ("collection_native_import_complete", {"collection": "mkt.klines_1m", "rows": -1}),
    ],
)
def test_setup_progress_refuses_nonallowlisted_stages_fields_and_values(stage, details):
    emitted = []
    progress = full_inputs.StageProgress(emitted.append)
    with pytest.raises(ValueError):
        progress.record(stage, **details)
    assert emitted == []


def test_progress_checkpoint_atomically_replaces_current_public_receipts(tmp_path):
    destination = tmp_path / "progress" / "owned.json"
    identities = {"comparison_id": "owned", "completion": "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME"}
    lifecycle = {"receipts": [], "shutdown_actions": []}
    comparison.checkpoint_progress(destination, identities, {}, {"original": lifecycle})
    before = destination.read_bytes()
    comparison.retain_message(
        {"kind": "ownership", "repository_cleanup": "ACQUIRED_PENDING_CLEANUP"}, lifecycle
    )
    comparison.checkpoint_progress(destination, identities, {}, {"original": lifecycle})
    raw, current = comparison.read_public_file(destination)
    assert raw != before
    assert current["lifecycles"]["original"]["receipts"] == lifecycle["receipts"]
    assert current["identities"] == identities
    assert current["completion"] == "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME"
    assert list(destination.parent.iterdir()) == [destination]
    identities["completion"] = (
        "MATCHED_FULL_DENSE_PATH_RECONSTRUCTION_COMPARISON_COMPLETED_NOT_PERFORMANCE_ACCEPTANCE"
    )
    comparison.checkpoint_progress(destination, identities, {}, {"original": lifecycle})
    assert json.loads(destination.read_bytes())["completion"] == identities["completion"]


@pytest.mark.parametrize(
    "private",
    [
        {"receipts": [{"content": [{"text": '{"nested":{"access_token":"hidden"}}'}]}]},
        {"receipts": [{"error": "postgresql://private:secret@localhost/private"}]},
    ],
)
def test_progress_checkpoint_privacy_refusal_preserves_previous_bytes_and_creates_no_artifact(
    tmp_path, private
):
    destination = tmp_path / "progress" / "owned.json"
    comparison.checkpoint_progress(destination, {}, {}, {})
    before = destination.read_bytes()
    with pytest.raises(ValueError, match="Forbidden public evidence"):
        comparison.checkpoint_progress(destination, {}, {}, private)
    assert destination.read_bytes() == before
    missing = tmp_path / "refused" / "owned.json"
    with pytest.raises(ValueError, match="Forbidden public evidence"):
        comparison.checkpoint_progress(missing, {}, {}, private)
    assert not missing.parent.exists()
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize("boundary", ["fsync", "replace"])
def test_progress_checkpoint_write_failure_preserves_previous_bytes(tmp_path, monkeypatch, boundary):
    destination = tmp_path / "progress" / "owned.json"
    comparison.checkpoint_progress(destination, {}, {}, {})
    before = destination.read_bytes()

    def refuse(*args, **kwargs):
        raise OSError("private write failure must not be retained")

    monkeypatch.setattr(comparison.os, boundary, refuse)
    with pytest.raises(OSError):
        comparison.checkpoint_progress(destination, {"comparison_id": "new"}, {}, {})
    assert destination.read_bytes() == before
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize("indirect", ["file", "parent", "ancestor"])
def test_progress_checkpoint_refuses_symlink_targets_and_directories(tmp_path, indirect):
    real = tmp_path / "real"
    real.mkdir()
    destination = real / "owned.json"
    comparison.checkpoint_progress(destination, {}, {}, {})
    before = destination.read_bytes()
    link = tmp_path / "link"
    if indirect == "file":
        link.symlink_to(destination)
        target = link
    else:
        link.symlink_to(real, target_is_directory=True)
        target = link / "owned.json" if indirect == "parent" else link / "nested" / "owned.json"
    with pytest.raises(ValueError, match="indirect"):
        comparison.checkpoint_progress(target, {"comparison_id": "new"}, {}, {})
    assert destination.read_bytes() == before
    assert list(real.iterdir()) == [destination]


@pytest.mark.parametrize("bound", ["file", "bundle"])
def test_progress_checkpoint_bounds_preserve_previous_bytes(tmp_path, monkeypatch, bound):
    destination = tmp_path / "progress" / "owned.json"
    comparison.checkpoint_progress(destination, {}, {}, {})
    before = destination.read_bytes()
    name = "PUBLIC_FILE_LIMIT" if bound == "file" else "PUBLIC_TOTAL_LIMIT"
    monkeypatch.setattr(comparison, name, 128 if bound == "file" else 32)
    with pytest.raises(ValueError, match="exceeds its bound"):
        comparison.checkpoint_progress(destination, {"comparison_id": "x" * 100}, {}, {})
    assert destination.read_bytes() == before
    assert list(destination.parent.iterdir()) == [destination]


async def test_parent_checkpoints_ownership_progress_and_ready_in_order_before_child_exit(tmp_path):
    reader = asyncio.StreamReader()
    lifecycle = {"receipts": [], "shutdown_actions": []}
    destination = tmp_path / "progress.json"
    observed = []

    def checkpoint():
        comparison.checkpoint_progress(destination, {}, {}, {"original": lifecycle})
        observed.append(json.loads(destination.read_bytes()))

    messages = [{"kind": "ownership", "repository_cleanup": "ACQUIRED_PENDING_CLEANUP"}]
    progress = full_inputs.StageProgress(messages.append)
    progress.record("source_preflight_complete")
    messages.append({"kind": "ready", "pid": 123})
    for message in messages:
        reader.feed_data(json.dumps(message).encode() + b"\n")
    process = SimpleNamespace(stdout=reader, returncode=None)
    ready = await comparison.worker_message(process, lifecycle, checkpoint)
    assert ready == messages[-1]
    assert [len(item["lifecycles"]["original"]["receipts"]) for item in observed] == [1, 2, 3]
    assert lifecycle["receipts"] == messages
    assert lifecycle["ownership"] == messages[0]
    assert process.returncode is None and not reader.at_eof()
    assert all(item["completion"] == "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME" for item in observed)


def test_progress_checkpoint_retains_safe_failure_without_raw_exception(tmp_path):
    destination = tmp_path / "progress.json"
    try:
        raise RuntimeError("postgresql://operator:private-secret@localhost/private")
    except RuntimeError as error:
        message = {"kind": "error", **worker.safe_failure(error)}
    lifecycle = {"receipts": [], "shutdown_actions": []}
    comparison.retain_message(message, lifecycle)
    comparison.checkpoint_progress(destination, {}, {}, {"original": lifecycle})
    raw, current = comparison.read_public_file(destination)
    assert current["lifecycles"]["original"]["receipts"][0]["type"] == "RuntimeError"
    assert b"private-secret" not in raw and b"postgresql" not in raw


def test_complete_worker_progress_fits_unchanged_receipt_budget_and_stays_incomplete(tmp_path):
    lifecycle = {"receipts": [], "shutdown_actions": []}
    destination = tmp_path / "progress.json"

    def retain(message):
        comparison.retain_message(message, lifecycle)
        comparison.checkpoint_progress(destination, {}, {}, {"original": lifecycle})

    progress = full_inputs.StageProgress(retain)
    progress.record("source_preflight_complete")
    retain({"kind": "ownership", "repository_cleanup": "ACQUIRED_PENDING_CLEANUP"})
    progress.record("repository_acquired")
    progress.record("seed_start")
    progress.record("fixture_preflight_complete", rows=10, collections=10, dimensions=2, peers=1)
    for collection in sorted(full_inputs.PROGRESS_COLLECTIONS):
        progress.record("collection_native_import_complete", collection=collection, rows=1)
    progress.record("native_cache_installed", dimensions=2)
    progress.record("sessions_seeded", sessions=2)
    progress.record(
        "native_cache_complete",
        dimensions=2,
        peers=1,
        configured_instruments=2,
        captured_resolved_instruments=1,
    )
    progress.record("ready")
    retain({"kind": "ready"})
    for state in ("cold", "warm"):
        progress.record("sample_start", state=state)
        progress.record("sample_completed", state=state)
        retain({"kind": "sample", "state": state})
    retain({"kind": "ownership", "repository_cleanup": "CONTEXT_EXIT_COMPLETED"})
    progress.record("completed")
    retain({"kind": "shutdown", "repository_cleanup": "CONTEXT_EXIT_COMPLETED"})
    assert len(lifecycle["receipts"]) == 29
    assert lifecycle["receipt_bytes"] == sum(
        len(comparison.public_bytes(item)) for item in lifecycle["receipts"]
    )
    current = json.loads(destination.read_bytes())
    assert current["lifecycles"]["original"] == lifecycle
    assert current["completion"] == "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME"
    assert all(
        item["completion"] == current["completion"]
        for item in lifecycle["receipts"]
        if item["kind"] == "progress"
    )


def test_progress_emission_and_parent_receipt_limits_refuse_without_replacing_checkpoint(tmp_path):
    emitted = []
    progress = full_inputs.StageProgress(emitted.append)
    for _ in range(24):
        progress.record("ready")
    with pytest.raises(ValueError, match="Progress receipts exceed"):
        progress.record("ready")
    assert len(emitted) == 24
    lifecycle = {"receipts": [], "shutdown_actions": []}
    destination = tmp_path / "progress.json"

    def checkpoint():
        comparison.checkpoint_progress(destination, {}, {}, {"original": lifecycle})

    for _ in range(32):
        comparison.retain_message({"kind": "ready"}, lifecycle, checkpoint)
    before = destination.read_bytes()
    with pytest.raises(ValueError, match="Worker receipts exceed"):
        comparison.retain_message({"kind": "ready"}, lifecycle, checkpoint)
    assert len(lifecycle["receipts"]) == 32
    assert destination.read_bytes() == before


def test_final_export_accounts_for_separately_retained_progress_bytes(tmp_path, monkeypatch):
    baseline = tmp_path / "baseline"
    comparison.export_evidence(tmp_path, baseline, {}, {}, {})
    limit = sum(path.stat().st_size for path in baseline.iterdir())
    monkeypatch.setattr(comparison, "PUBLIC_TOTAL_LIMIT", limit)
    accepted = tmp_path / "accepted"
    comparison.export_evidence(tmp_path, accepted, {}, {}, {}, progress_bytes=0)
    assert {path.name: path.read_bytes() for path in accepted.iterdir()} == {
        path.name: path.read_bytes() for path in baseline.iterdir()
    }
    destination = tmp_path / "final"
    with pytest.raises(ValueError, match="bundle exceeds"):
        comparison.export_evidence(tmp_path, destination, {}, {}, {}, progress_bytes=1)
    assert not destination.exists()


async def test_parent_refuses_invalid_progress_without_replacing_checkpoint(tmp_path):
    reader = asyncio.StreamReader()
    lifecycle = {"receipts": [], "shutdown_actions": []}
    destination = tmp_path / "progress.json"

    def checkpoint():
        comparison.checkpoint_progress(destination, {}, {}, {"original": lifecycle})

    checkpoint()
    before = destination.read_bytes()
    reader.feed_data(b'{"kind":"progress","stage":"private exception text"}\n')
    with pytest.raises(ValueError, match="Unexpected progress stage"):
        await comparison.worker_message(SimpleNamespace(stdout=reader), lifecycle, checkpoint)
    assert lifecycle["receipts"] == []
    assert destination.read_bytes() == before


def test_parent_receipt_byte_limit_preserves_checkpoint(tmp_path, monkeypatch):
    lifecycle = {"receipts": [], "shutdown_actions": []}
    destination = tmp_path / "progress.json"

    def checkpoint():
        comparison.checkpoint_progress(destination, {}, {}, {"original": lifecycle})

    comparison.retain_message({"kind": "ready"}, lifecycle, checkpoint)
    before = destination.read_bytes()
    payload = {"kind": "sample", "state": "cold"}
    limit = lifecycle["receipt_bytes"] + len(comparison.public_bytes(payload)) - 1
    monkeypatch.setattr(comparison, "PUBLIC_FILE_LIMIT", limit)
    with pytest.raises(ValueError, match="Worker receipts exceed"):
        comparison.retain_message(payload, lifecycle, checkpoint)
    assert lifecycle["receipts"] == [{"kind": "ready"}]
    assert destination.read_bytes() == before


@pytest.fixture
def export_inputs(tmp_path):
    expected = {}
    for name in ("original", "candidate"):
        source = tmp_path / (name + "-run")
        source.mkdir()
        for filename in (
            "ownership.json",
            "cold-result.json",
            "cold-projection.json",
            "cold-http.json",
            "warm-result.json",
            "warm-projection.json",
            "warm-http.json",
        ):
            output = name + "-" + filename
            value = {"derived_file": output, "value": "0.202900000000000000001", "missing": None}
            if output == "original-cold-result.json":
                value["content"] = [
                    {
                        "text": json.dumps(
                            {
                                "points": [
                                    {
                                        "ts": "2026-10-28T10:00:00.123456Z",
                                        "price": "0.202900000000000000001",
                                        "reason": "RETAINED_PUBLIC_OBSERVATION_" * 4,
                                        "value": index,
                                        "missing": None,
                                    }
                                    for index in range(256)
                                ]
                            }
                        )
                    }
                ]
            raw = json.dumps(value, separators=(",", ":")).encode()
            (source / filename).write_bytes(raw)
            expected[output] = (raw, value)
        for filename in ("stderr.log", "environment.json", "config.json", "application.log"):
            (source / filename).write_text("private-operator-secret")
    return expected


def test_export_validates_and_serializes_each_full_file_once_without_trimming(
    tmp_path, monkeypatch, export_inputs
):
    validations, serializations = [], []
    validate, serialize = comparison.validate_public, comparison.json.dumps

    def validate_once(value):
        if isinstance(value, dict) and "derived_file" in value:
            validations.append(value["derived_file"])
        return validate(value)

    def serialize_once(value, *args, **kwargs):
        if isinstance(value, dict) and "derived_file" in value:
            serializations.append(value["derived_file"])
        return serialize(value, *args, **kwargs)

    monkeypatch.setattr(comparison, "validate_public", validate_once)
    monkeypatch.setattr(comparison.json, "dumps", serialize_once)
    destination = tmp_path / "export"
    receipt = comparison.export_evidence(tmp_path, destination, {}, {}, {})
    assert receipt["files"] == 16
    assert {path.name for path in destination.iterdir()} == {
        *export_inputs,
        "receipts.json",
        "manifest.json",
    }
    manifest = json.loads((destination / "manifest.json").read_bytes())
    for output, (raw, value) in export_inputs.items():
        exported = (destination / output).read_bytes()
        assert exported == serialize(value, sort_keys=True, allow_nan=False).encode()
        assert json.loads(exported) == value
        assert manifest["files"][output] == {
            "source_bytes_sha256": hashlib.sha256(raw).hexdigest(),
            "export_sha256": hashlib.sha256(exported).hexdigest(),
            "bytes": len(exported),
        }
    assert validations == list(export_inputs)
    assert serializations == list(export_inputs)
    assert all(b"private-operator-secret" not in path.read_bytes() for path in destination.iterdir())


def test_canonical_public_read_revalidates_changed_sources_and_preserves_default_result(tmp_path):
    source = tmp_path / "cold-result.json"
    raw = b'{"price":"0.20290000","missing":null}'
    source.write_bytes(raw)
    assert comparison.read_public_file(source) == (raw, {"price": "0.20290000", "missing": None})
    assert comparison.read_public_file(source, canonical=True) == (
        raw,
        b'{"missing": null, "price": "0.20290000"}',
    )
    source.write_text('{"content":[{"text":"{\\"nested\\":{\\"stderr\\":\\"hidden\\"}}"}]}')
    with pytest.raises(ValueError, match="Forbidden public evidence"):
        comparison.read_public_file(source, canonical=True)


def test_export_checkpoints_all_parent_read_stages_and_bundle_without_claiming_export_success(
    tmp_path, monkeypatch, export_inputs
):
    clock = {"wall": 10.0, "cpu": 2.0}
    monkeypatch.setattr(comparison, "perf_counter", lambda: clock["wall"])
    monkeypatch.setattr(comparison, "process_time", lambda: clock["cpu"])
    read = comparison.read_public_file

    def timed_read(*args, **kwargs):
        result = read(*args, **kwargs)
        clock["wall"] += 2
        clock["cpu"] += 1
        return result

    monkeypatch.setattr(comparison, "read_public_file", timed_read)
    progress_path = tmp_path / "progress.json"
    destination = tmp_path / "export"
    identities = {
        "completion": "MATCHED_FULL_DENSE_PATH_RECONSTRUCTION_COMPARISON_COMPLETED_NOT_PERFORMANCE_ACCEPTANCE"
    }
    lifecycle = {"original": {"receipts": [], "exit_code": 0, "cleanup_confirmed": True}}
    observed = []

    def checkpoint(export_progress):
        assert not destination.exists()
        comparison.checkpoint_progress(
            progress_path, identities, {}, lifecycle, export_progress=export_progress
        )
        observed.append(json.loads(progress_path.read_bytes()))
        clock["wall"] += 0.5
        clock["cpu"] += 0.25
        return progress_path.stat().st_size

    receipt = comparison.export_evidence(
        tmp_path, destination, identities, {}, lifecycle, checkpoint=checkpoint
    )
    assert receipt["files"] == 16
    stages = observed[-1]["export_progress"]
    assert len(observed) == len(stages) == 29
    assert [(item["stage"], item["filename"]) for item in stages] == [
        pair for filename in export_inputs for pair in (("read_start", filename), ("read_complete", filename))
    ] + [("bundle_assembled", "")]
    assert stages[0]["elapsed_wall_seconds"] == stages[0]["elapsed_process_cpu_seconds"] == 0
    assert stages[1]["elapsed_wall_seconds"] == 2.5
    assert stages[1]["elapsed_process_cpu_seconds"] == 1.25
    assert stages[-1]["files"] == 16
    assert stages[-1]["bytes"] == sum(path.stat().st_size for path in destination.iterdir())
    for index, snapshot in enumerate(observed):
        assert len(snapshot["export_progress"]) == index + 1
        assert snapshot["completion"] == identities["completion"]
        assert snapshot["lifecycles"] == lifecycle
    assert json.loads(progress_path.read_bytes()) == observed[-1]
    assert b"private-operator-secret" not in progress_path.read_bytes()


@pytest.mark.parametrize("boundary", ["read", "write"])
def test_export_interruption_retains_safe_parent_stage_and_cleanup(tmp_path, monkeypatch, boundary):
    source = tmp_path / "original-run"
    source.mkdir()
    (source / "ownership.json").write_text('{"repository_cleanup":"CONTEXT_EXIT_COMPLETED"}')
    destination, progress_path = tmp_path / "export", tmp_path / "progress.json"
    lifecycle = {"original": {"cleanup_confirmed": True, "exit_code": 0, "receipts": []}}
    failure = OSError("postgresql://operator:private-secret@localhost/private")

    def checkpoint(export_progress):
        comparison.checkpoint_progress(progress_path, {}, {}, lifecycle, export_progress=export_progress)
        return progress_path.stat().st_size

    if boundary == "read":

        def refuse_read(*args, **kwargs):
            raise failure

        monkeypatch.setattr(comparison, "read_public_file", refuse_read)
    else:
        write = Path.write_bytes

        def refuse_write(path, data):
            if path.parent == destination:
                raise failure
            return write(path, data)

        monkeypatch.setattr(Path, "write_bytes", refuse_write)
    with pytest.raises(OSError) as raised:
        comparison.export_evidence(tmp_path, destination, {}, {}, lifecycle, checkpoint=checkpoint)
    assert raised.value is failure
    current = json.loads(progress_path.read_bytes())
    assert current["lifecycles"] == lifecycle
    assert current["completion"] == "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME"
    assert current["export_progress"][-1]["stage"] == (
        "read_start" if boundary == "read" else "bundle_assembled"
    )
    assert b"private-secret" not in progress_path.read_bytes()
    assert not (destination / "manifest.json").exists()
    assert list(tmp_path.glob(".*.tmp")) == []


@pytest.mark.parametrize("indirect", ["file", "parent"])
def test_export_refuses_symlink_sources_before_bundle_creation(tmp_path, indirect):
    source, real = tmp_path / "original-run", tmp_path / "real"
    real.mkdir()
    (real / "ownership.json").write_text('{"safe":true}')
    if indirect == "parent":
        source.symlink_to(real, target_is_directory=True)
    else:
        source.mkdir()
        (source / "ownership.json").symlink_to(real / "ownership.json")
    destination = tmp_path / "export"
    with pytest.raises(ValueError, match="indirect"):
        comparison.export_evidence(tmp_path, destination, {}, {}, {})
    assert not destination.exists()
    assert (real / "ownership.json").read_text() == '{"safe":true}'


@pytest.mark.parametrize(
    "payload",
    [
        {"value": float("nan")},
        {"value": float("inf")},
        {"content": [{"text": json.dumps({"nested": {"config": {"safe": True}}})}]},
        {"content": [{"text": json.dumps({"nested": {"stderr": "hidden"}})}]},
    ],
)
def test_export_refuses_nonfinite_and_embedded_private_json_with_parent_checkpoint(tmp_path, payload):
    source = tmp_path / "original-run"
    source.mkdir()
    (source / "ownership.json").write_text(json.dumps(payload))
    destination, progress_path = tmp_path / "export", tmp_path / "progress.json"
    comparison.checkpoint_progress(progress_path, {}, {}, {})

    def checkpoint(export_progress):
        comparison.checkpoint_progress(progress_path, {}, {}, {}, export_progress=export_progress)
        return progress_path.stat().st_size

    with pytest.raises(ValueError):
        comparison.export_evidence(tmp_path, destination, {}, {}, {}, checkpoint=checkpoint)
    current = json.loads(progress_path.read_bytes())
    assert current["export_progress"][-1]["stage"] == "read_start"
    assert "hidden" not in progress_path.read_text()
    assert not destination.exists()


@pytest.mark.parametrize(
    "mutation",
    [
        {"stage": "private text"},
        {"filename": "stderr.log"},
        {"filename": "../ownership.json"},
        {"extra": "private text"},
        {"files": 17},
        {"bytes": 64_000_001},
        {"elapsed_wall_seconds": -1},
        {"elapsed_process_cpu_seconds": float("nan")},
        {"elapsed_wall_seconds": float("inf")},
        {"files": True},
    ],
)
def test_export_progress_refuses_nonallowlisted_receipts_without_replacing_checkpoint(tmp_path, mutation):
    progress_path = tmp_path / "progress.json"
    comparison.checkpoint_progress(progress_path, {}, {}, {})
    before = progress_path.read_bytes()
    entries_before = set(tmp_path.iterdir())
    receipt = {
        "stage": "read_start",
        "filename": "original-ownership.json",
        "files": 0,
        "bytes": 0,
        "elapsed_wall_seconds": 0.0,
        "elapsed_process_cpu_seconds": 0.0,
        **mutation,
    }
    with pytest.raises(ValueError):
        comparison.checkpoint_progress(progress_path, {}, {}, {}, export_progress=[receipt])
    assert progress_path.read_bytes() == before
    assert set(tmp_path.iterdir()) == entries_before


def test_export_progress_count_bound_preserves_previous_checkpoint(tmp_path):
    progress_path = tmp_path / "progress.json"
    comparison.checkpoint_progress(progress_path, {}, {}, {})
    before = progress_path.read_bytes()
    receipt = {
        "stage": "read_start",
        "filename": "original-ownership.json",
        "files": 0,
        "bytes": 0,
        "elapsed_wall_seconds": 0.0,
        "elapsed_process_cpu_seconds": 0.0,
    }
    with pytest.raises(ValueError):
        comparison.checkpoint_progress(progress_path, {}, {}, {}, export_progress=[receipt] * 30)
    assert progress_path.read_bytes() == before


def test_export_reserves_latest_parent_checkpoint_bytes_before_creating_bundle(tmp_path, monkeypatch):
    monkeypatch.setattr(comparison, "perf_counter", lambda: 0.0)
    monkeypatch.setattr(comparison, "process_time", lambda: 0.0)
    progress_path = tmp_path / "progress.json"
    comparison.checkpoint_progress(progress_path, {}, {}, {})
    initial_size = progress_path.stat().st_size

    def checkpoint(export_progress):
        comparison.checkpoint_progress(progress_path, {}, {}, {}, export_progress=export_progress)
        return progress_path.stat().st_size

    baseline = tmp_path / "baseline"
    comparison.export_evidence(tmp_path, baseline, {}, {}, {}, initial_size, checkpoint=checkpoint)
    final_size = progress_path.stat().st_size
    output_size = sum(path.stat().st_size for path in baseline.iterdir())
    assert final_size > initial_size
    monkeypatch.setattr(comparison, "PUBLIC_TOTAL_LIMIT", output_size + final_size)
    accepted = tmp_path / "accepted"
    comparison.export_evidence(tmp_path, accepted, {}, {}, {}, initial_size, checkpoint=checkpoint)
    assert {path.name: path.read_bytes() for path in accepted.iterdir()} == {
        path.name: path.read_bytes() for path in baseline.iterdir()
    }
    monkeypatch.setattr(comparison, "PUBLIC_TOTAL_LIMIT", output_size + final_size - 1)
    refused = tmp_path / "refused"
    with pytest.raises(ValueError, match="bundle exceeds"):
        comparison.export_evidence(tmp_path, refused, {}, {}, {}, initial_size, checkpoint=checkpoint)
    assert not refused.exists()
    assert json.loads(progress_path.read_bytes())["export_progress"][-1]["stage"] == "bundle_assembled"


def test_canonical_public_read_preserves_postserialization_file_bound(tmp_path, monkeypatch):
    value = {str(index): index for index in range(20)}
    raw = json.dumps(value, separators=(",", ":")).encode()
    canonical = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    assert len(raw) < len(canonical)
    source = tmp_path / "cold-result.json"
    source.write_bytes(raw)
    monkeypatch.setattr(comparison, "PUBLIC_FILE_LIMIT", len(canonical) - 1)
    with pytest.raises(ValueError, match="file exceeds"):
        comparison.read_public_file(source, canonical=True)


@pytest.mark.integration
@pytest.mark.postgres
async def test_native_fixture_copies_are_exact_independent_and_survive_source_removal(tmp_path, monkeypatch):
    assert urlsplit(os.environ["CLICKHOUSE_TEST_URL"]).hostname in {"127.0.0.1", "localhost", "::1"}
    names = ["mkt_tank_test_fixture_" + uuid4().hex for _ in range(3)]
    clients = [ClickHouse(os.environ["CLICKHOUSE_TEST_URL"], database=name) for name in names]
    source, first, second = clients
    table = "mkt.klines_1m"
    row = {
        "storage_key": "actual:tiny:key",
        "instrument_id": "binance:spot:ENAUSDT",
        "open_time": "2026-10-28 12:01:00.123456",
        "recorded_at": "2026-10-28 12:02:00.654321",
        "c": "0.123456789012345678901234567890123456789000",
        "h": None,
        "n_trades": 9007199254740993,
        "complete": True,
        "interval_s": 60,
        "_version": 1,
    }
    try:
        for client in clients:
            for statement in ddl():
                await client.execute(statement)
        funding = {
            "storage_key": "actual:funding:key",
            "instrument_id": row["instrument_id"],
            "kind": "CURRENT_ESTIMATE",
            "as_of": row["open_time"],
            "semantic_fingerprint": "literal mkt",
            "source_observations": '{"value":9007199254740993,"missing":null}',
        }
        tickers = {
            "storage_key": "actual:ticker:key",
            "instrument_id": row["instrument_id"],
            "ts": row["open_time"],
            "capture_sources": ["PUBLIC_REST", "PUBLIC_WS"],
        }
        fixtures = tmp_path / "tiny"
        fixtures.mkdir()
        entries = []
        for collection, captured in (
            (table, {key: value for key, value in row.items() if key != "_version"}),
            ("mkt.funding_obs", funding),
            ("mkt.tickers_24h", tickers),
        ):
            payload = json.dumps(captured).encode() + b"\n"
            name = collection.split(".")[1] + ".gz"
            archive = gzip.compress(payload)
            (fixtures / name).write_bytes(archive)
            entries.append(
                {
                    "name": name,
                    "table": collection,
                    "rows": 1,
                    "columns": list(captured),
                    "bytes": len(payload),
                    "compressed_bytes": len(archive),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "archive_sha256": hashlib.sha256(archive).hexdigest(),
                }
            )
        cache = {
            "version": 1,
            "saved_at": "2026-10-29T00:00:00Z",
            "counts": [[row["instrument_id"], "2026-10-27T00:00:00Z", 1]],
        }
        (fixtures / "kline_day_counts.json").write_text(json.dumps(cache))
        (fixtures / "privacy.json").write_text(
            json.dumps(
                {
                    "omitted_occurrences": {},
                    "unapproved_fields_or_values": 0,
                    "all_urls_omitted": True,
                    "source_archive_not_selected": True,
                    "top_level_allowlist": {entry["table"]: entry["columns"] for entry in entries},
                    "recursive_key_allowlist": {"mkt.funding_obs": ["value", "missing"]},
                }
            )
        )
        manifest = {
            "files": entries,
            "auxiliary_files": [
                {
                    "name": name,
                    "bytes": (fixtures / name).stat().st_size,
                    "sha256": full_inputs.digest(fixtures / name),
                }
                for name in ("kline_day_counts.json", "privacy.json")
            ],
            "source_config_git_objects": {},
            "privacy_omitted_occurrences": {},
            "history_start": "2026-10-26T00:00:00Z",
            "semantic_cutoff": "2026-10-28T00:00:00Z",
            "peer_spot_instrument_ids": [row["instrument_id"]],
            "native_cache": {
                "saved_at": cache["saved_at"],
                "entries": 1,
                "eligible_whole_day_rows": 1,
                "positive_entries": 1,
                "zero_entries": 0,
            },
            "confirmed_empty_tables": ["mkt.book_summaries", "mkt.liq_obs"],
            "qualification": "TINY_NATIVE_FIXTURE",
            "peer_coverage_counts": {},
            "dense_projections": [],
            "limitations": [],
        }
        (fixtures / "manifest.json").write_text(json.dumps(manifest))
        monkeypatch.setattr(full_inputs, "MANIFEST_SHA256", full_inputs.digest(fixtures / "manifest.json"))
        streams = []
        retained = full_inputs.retained_rows

        def observe_stream(*args):
            streams.append(args[1]["table"])
            yield from retained(*args)

        monkeypatch.setattr(full_inputs, "retained_rows", observe_stream)

        def repository(client):
            return SimpleNamespace(
                database_url="dbname=" + client.database.removeprefix("mkt_"), clickhouse=client
            )

        def settings(name):
            return SimpleNamespace(data_dir=tmp_path / name)

        setup = await full_inputs.seed_full(
            repository(source), settings("source"), fixtures, clickhouse_only=True
        )
        seed = {
            "comparison_id": "c" * 32,
            "admission": full_inputs.fixture_admission(settings("source"), fixtures),
            "source_database": source.database,
            "inputs": setup["inputs"],
            "copy_tables": setup["copy_tables"],
        }
        assert seed["copy_tables"] == {table: True, "mkt.funding_obs": False, "mkt.tickers_24h": True}
        for index, target in enumerate((first, second)):
            await full_inputs.seed_full(
                repository(target),
                settings(str(index)),
                fixtures,
                clickhouse_seed=seed,
                comparison_id="c" * 32,
            )
            assert await target.rows("SELECT capture_sources FROM mkt.tickers_24h FINAL") == [
                {"capture_sources": ["PUBLIC_REST", "PUBLIC_WS"]}
            ]
            assert await target.rows(
                "SELECT semantic_fingerprint,source_observations FROM mkt.funding_obs FINAL"
            ) == [
                {
                    "semantic_fingerprint": "literal " + target.database,
                    "source_observations": '{"value":9007199254740993,"missing":null}',
                }
            ]
        assert streams == [
            table,
            "mkt.funding_obs",
            "mkt.tickers_24h",
            "mkt.funding_obs",
            "mkt.funding_obs",
        ]
        archive_path = fixtures / entries[0]["name"]
        original_archive = archive_path.read_bytes()
        archive_path.write_bytes(original_archive + b"changed")
        with pytest.raises(ValueError, match="Fixture admission hash mismatch"):
            full_inputs.fixture_admission(settings("source"), fixtures)
        archive_path.write_bytes(original_archive)
        expected = {
            **row,
            "open_time": "2026-10-28T12:01:00.123456Z",
            "recorded_at": "2026-10-28T12:02:00.654321Z",
            "o": None,
        }
        columns = ",".join(f"`{key}`" for key in expected)
        for client in clients:
            assert await client.rows(f"SELECT {columns} FROM {table} FINAL") == [expected]
        with pytest.raises(ValueError, match="not empty"):
            await full_inputs.copy_clickhouse(first, source.database, table, 1)
        await first.execute(f"TRUNCATE TABLE {table}")
        execute = first.execute

        async def corrupt_copy(query, params=None):
            if query.startswith("INSERT INTO"):
                prefix, selected = query.split(" SELECT ", 1)
                query = prefix + " SELECT " + selected.replace("`c`", "'wrong' AS `c`")
            await execute(query, params)

        with monkeypatch.context() as patch:
            patch.setattr(first, "execute", corrupt_copy)
            with pytest.raises(ValueError, match="full-column copy mismatch"):
                await full_inputs.copy_clickhouse(first, source.database, table, 1)
        await source.execute("DROP DATABASE mkt SYNC")
        assert (
            await first.rows(
                "SELECT name FROM system.databases WHERE name={source:String}",
                {"source": (names[0], "String")},
            )
            == []
        )
        await first.execute(f"TRUNCATE TABLE {table}")
        assert await first.rows(f"SELECT count() AS n FROM {table} FINAL") == [{"n": 0}]
        assert await second.rows(f"SELECT {columns} FROM {table} FINAL") == [expected]
        with pytest.raises(ValueError, match="physical market tables"):
            await full_inputs.copy_clickhouse(first, source.database, table, 1)
    finally:
        for client in clients:
            try:
                await client.execute("DROP DATABASE IF EXISTS mkt SYNC")
            finally:
                await client.close()
    verifier = ClickHouse(os.environ["CLICKHOUSE_TEST_URL"])
    try:
        for name in names:
            assert (
                await verifier.rows(
                    "SELECT name FROM system.databases WHERE name={name:String}",
                    {"name": (name, "String")},
                )
                == []
            )
    finally:
        await verifier.close()


@pytest.mark.parametrize(
    ("source", "table", "rows"),
    [
        ("mkt", "mkt.klines_1m", 1),
        ("mkt_tank_test_fixture_" + "a" * 32, "mkt.trades", 1),
        ("mkt_tank_test_fixture_" + "a" * 32, "mkt.klines_1m;DROP", 1),
        ("mkt_tank_test_fixture_" + "a" * 32, "mkt.klines_1m", -1),
        ("mkt_tank_test_fixture_" + "a" * 32, "mkt.klines_1m", True),
    ],
)
async def test_native_copy_refuses_unsafe_contract_before_transport(source, table, rows):
    client = ClickHouse("http://localhost:8123", database="mkt_tank_test_fixture_" + "b" * 32)
    try:
        with pytest.raises(ValueError):
            await full_inputs.copy_clickhouse(client, source, table, rows)
    finally:
        await client.close()


@pytest.mark.parametrize("mutation", ["run", "admission", "source", "partial", "table", "rows", "wire"])
def test_seed_descriptor_refuses_stale_partial_or_foreign_inputs(mutation):
    manifest = {"files": [{"table": "mkt.klines_1m", "rows": 1, "name": "tiny.gz"}]}
    admission = {"manifest_sha256": "a" * 64, "loader_sha256": "b" * 64}
    seed = {
        "comparison_id": "c" * 32,
        "admission": dict(admission),
        "source_database": "mkt_tank_test_fixture_" + "d" * 32,
        "inputs": [{"collection": "mkt.klines_1m", "rows": 1, "name": "tiny.gz"}],
        "copy_tables": {"mkt.klines_1m": True},
    }
    if mutation == "run":
        seed["comparison_id"] = "e" * 32
    elif mutation == "admission":
        seed["admission"]["loader_sha256"] = "0" * 64
    elif mutation == "source":
        seed["source_database"] = "mkt"
    elif mutation == "partial":
        seed["inputs"] = []
    elif mutation == "table":
        seed["copy_tables"] = {"mkt.trades": True}
    elif mutation == "rows":
        seed["inputs"][0]["rows"] = 2
    else:
        seed["copy_tables"]["mkt.klines_1m"] = "true"
    with pytest.raises(ValueError):
        full_inputs.validate_clickhouse_seed(seed, "c" * 32, admission, manifest)


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_canonical_seed_cancellation_settles_import_before_owned_cleanup(
    tmp_path, monkeypatch, cleanup_fails
):
    entered, release = asyncio.Event(), asyncio.Event()
    actions = []
    name = "tank_test_fixture_" + "a" * 32
    repo = SimpleNamespace(database_url="dbname=" + name, clickhouse=SimpleNamespace(database="mkt_" + name))

    class Template:
        def __init__(self, *, schema_template):
            assert schema_template == "tank_test_external"

        @asynccontextmanager
        async def repository(self, route):
            try:
                yield repo
            finally:
                actions.append("repository_closed")
                if cleanup_fails:
                    raise ValueError("Secondary cleanup failure")

        async def close(self):
            actions.append("template_closed")

    async def seed(*args, **kwargs):
        entered.set()
        await release.wait()
        actions.append("import_settled")
        return {"inputs": [], "copy_tables": {}}

    async def absent(route, url, database):
        assert database == name
        actions.append("absence_read_back")

    monkeypatch.setenv("TANK_TEST_DATABASE_URL", "dbname=tank_test_admin")
    monkeypatch.setenv("CLICKHOUSE_TEST_URL", "http://localhost:8123")
    monkeypatch.setattr(comparison, "PostgresTemplate", Template)
    monkeypatch.setattr(comparison, "seed_full", seed)
    monkeypatch.setattr(comparison, "Settings", lambda **kwargs: SimpleNamespace())
    monkeypatch.setattr(comparison, "confirm_source_absent", absent)
    lifecycle = {"receipts": [], "shutdown_actions": []}

    async def prepare():
        async with comparison.prepared_clickhouse_seed(
            tmp_path, "tank_test_external", "c" * 32, lifecycle, lambda: None
        ):
            pytest.fail("Cancelled import must not publish a ready seed")

    task = asyncio.create_task(prepare())
    await entered.wait()
    task.cancel("primary cancellation")
    task.cancel("repeated cancellation")
    assert actions == []
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert actions == ["import_settled", "repository_closed", "template_closed", "absence_read_back"]
    assert "seed" not in lifecycle and lifecycle["cleanup_confirmed"] is not cleanup_fails
    if cleanup_fails:
        assert lifecycle["cleanup_failure_type"] == "ValueError"


def test_clickhouse_fixture_contract_refuses_wire_or_ddl_changes(tmp_path):
    source = (comparison.ROOT / "src/tank/clickhouse_market.py").read_text()
    original = tmp_path / "original.py"
    original.write_text(source)
    baseline = full_inputs.clickhouse_fixture_contract(original)
    for replacement in (
        source.replace('r"\\bmkt\\b"', 'r"mkt"'),
        source.replace('"numeric": "String"', '"numeric": "Float64"'),
    ):
        original.write_text(replacement)
        assert full_inputs.clickhouse_fixture_contract(original) != baseline

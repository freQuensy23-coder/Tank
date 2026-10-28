import asyncio
import hashlib
import json
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import market_truth_latency_worker as worker
import pytest
import test_market_truth_latency_comparison as comparison


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


async def test_parent_close_drains_receipt_after_active_sample_cleanup():
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
    close = asyncio.create_task(comparison.close_worker(process, lifecycle))
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

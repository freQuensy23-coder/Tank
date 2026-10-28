import asyncio
import json
import logging
import threading
from pathlib import Path
from runpy import run_path

import httpx
import pytest

from tank import clickhouse_market as chm
from tank import logging as telemetry
from tank.agents import SessionService
from tank.analysis import truth as truth_module
from tank.analysis.truth import compute_market_truth
from tank.storage import MemoryRepository


def events(caplog):
    return [
        json.loads(record.getMessage()) for record in caplog.records if record.name == "tank.market_timing"
    ]


def test_compute_reports_owned_stages_without_changing_result(caplog):
    snapshot = {"token_id": "test:T", "cutoff": "2026-10-28T12:00:00Z"}
    expected = compute_market_truth(snapshot)
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    actual = compute_market_truth(snapshot)
    assert actual == expected
    records = events(caplog)
    ended = [row for row in records if row["event"] == "stage_end"]
    stages = {row["stage"] for row in ended}
    assert {
        "compute",
        "compute.lock_wait",
        "compute.prepare",
        "compute.spot",
        "compute.relative",
        "compute.derivatives",
        "compute.liquidity",
        "compute.supply",
        "compute.evidence",
        "compute.confidence",
        "compute.selection",
        "compute.timestamps",
        "compute.metadata",
        "compute.jsonable",
    } <= stages
    assert len({row["trace_id"] for row in records}) == 1
    assert all(row["status"] == "ok" and row["wall_ms"] >= 0 for row in ended)
    assert "thread_cpu_ms" in next(row for row in ended if row["stage"] == "compute.metadata")
    assert "thread_cpu_ms" not in next(row for row in ended if row["stage"] == "compute.lock_wait")


def test_spans_use_monotonic_clock_and_emit_one_terminal(caplog, monkeypatch):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    ticks = iter([1_000_000, 2_000_000, 5_000_000, 9_000_000])
    monkeypatch.setattr(telemetry.time, "monotonic_ns", lambda: next(ticks))
    with telemetry.timing_span("outer", root=True), telemetry.timing_span("inner"):
        pass
    records = events(caplog)
    ends = {row["stage"]: row for row in records if row["event"] == "stage_end"}
    assert ends["outer"]["wall_ms"] == 8
    assert ends["inner"]["wall_ms"] == 3
    assert ends["inner"]["parent_span_id"] == ends["outer"]["span_id"]
    assert len(records) == 4


@pytest.mark.parametrize("failure", [ValueError("private-payload"), asyncio.CancelledError()])
def test_span_closes_and_does_not_log_exception_payload(caplog, failure):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    with (
        pytest.raises(type(failure)),
        telemetry.timing_span("failure", root=True, password="private-password"),
    ):
        raise failure
    records = events(caplog)
    assert [row["event"] for row in records] == ["stage_start", "stage_end"]
    assert records[-1]["status"] == ("cancelled" if isinstance(failure, asyncio.CancelledError) else "error")
    assert "private-" not in caplog.text


async def test_parallel_and_thread_contexts_are_isolated(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    ready = asyncio.Event()
    entered = 0

    async def run():
        nonlocal entered
        with telemetry.timing_span("request", root=True):
            entered += 1
            if entered == 2:
                ready.set()
            await ready.wait()
            await asyncio.to_thread(compute_market_truth, {"cutoff": "2026-10-28T12:00:00Z"})

    await asyncio.gather(run(), run())
    records = events(caplog)
    roots = [row for row in records if row["stage"] == "request" and row["event"] == "stage_start"]
    assert len({row["trace_id"] for row in roots}) == 2
    for root in roots:
        compute = [
            row
            for row in records
            if row["stage"] == "compute"
            and row["event"] == "stage_end"
            and row["trace_id"] == root["trace_id"]
        ]
        assert len(compute) == 1
        assert compute[0]["parent_span_id"] == root["span_id"]
    count = len(records)
    with telemetry.timing_span("inactive"):
        pass
    assert len(events(caplog)) == count


def test_timing_sink_failure_does_not_replace_application_result(monkeypatch):
    def broken(*args, **kwargs):
        raise OSError("private-sink-failure")

    monkeypatch.setattr(logging.getLogger("tank.market_timing"), "info", broken)
    with telemetry.timing_span("sink", root=True):
        result = 7
    assert result == 7


async def test_cancelled_lock_wait_is_terminal_without_releasing_another_owner(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    lock = asyncio.Lock()
    await lock.acquire()
    started = asyncio.Event()

    async def wait():
        with telemetry.timing_span("request", root=True):
            started.set()
            async with telemetry.timing_lock(lock, "lock.wait"):
                raise AssertionError("cancelled waiter acquired lock")

    task = asyncio.create_task(wait())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert lock.locked()
    lock.release()
    ended = [row for row in events(caplog) if row["event"] == "stage_end"]
    assert {row["stage"] for row in ended} == {"request", "lock.wait"}
    assert all(row["status"] == "cancelled" for row in ended)
    async with telemetry.timing_lock(lock, "inactive"):
        assert lock.locked()
    assert not lock.locked()


def test_decode_error_preserves_error_and_closes_stage(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    with pytest.raises(json.JSONDecodeError), telemetry.timing_span("decode", root=True):
        chm._json_lines(b"private-invalid-json")
    records = events(caplog)
    ended = [row for row in records if row["event"] == "stage_end"]
    assert {row["stage"] for row in ended} == {"decode", "clickhouse.json_decode"}
    assert all(row["status"] == "error" for row in ended)
    assert "private-invalid-json" not in caplog.text


async def test_threadpool_queue_span_precedes_real_compute(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    with telemetry.timing_span("request", root=True):
        result = await telemetry.timing_thread(compute_market_truth, {"cutoff": "2026-10-28T12:00:00Z"})
    assert result["data_confidence"]["class"] == "LOW"
    ended = [row for row in events(caplog) if row["event"] == "stage_end"]
    queue = next(row for row in ended if row["stage"] == "threadpool.wait")
    work = next(row for row in ended if row["stage"] == "threadpool.work")
    compute = next(row for row in ended if row["stage"] == "compute")
    assert queue["trace_id"] == compute["trace_id"]
    assert queue["parent_span_id"] == work["parent_span_id"]
    assert compute["parent_span_id"] == work["span_id"]
    assert queue["ended_mono_ns"] <= compute["started_mono_ns"]
    assert "thread_cpu_ms" not in queue
    assert len(ended) < 300


async def test_real_compute_lock_wait_has_no_compute_work_until_owner_releases(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    lock = threading.RLock()
    entered = threading.Event()

    class ObservedLock:
        def __enter__(self):
            entered.set()
            return lock.__enter__()

        def __exit__(self, *args):
            return lock.__exit__(*args)

    monkeypatch.setattr(truth_module, "_COMPUTE_SLOT", ObservedLock())
    lock.acquire()
    task = asyncio.create_task(
        telemetry.timing_thread(compute_market_truth, {"cutoff": "2026-10-28T12:00:00Z"})
    )
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        assert not task.done()
        records = events(caplog)
        assert any(row["stage"] == "compute.lock_wait" and row["event"] == "stage_start" for row in records)
        assert not any(row["stage"] == "compute.prepare" for row in records)
        assert not any(row["stage"] == "compute.lock_wait" and row["event"] == "stage_end" for row in records)
    finally:
        lock.release()
    result = await task
    assert result["data_confidence"]["class"] == "LOW"
    ended = [row for row in events(caplog) if row["event"] == "stage_end"]
    waiting = next(row for row in ended if row["stage"] == "compute.lock_wait")
    prepare = next(row for row in ended if row["stage"] == "compute.prepare")
    assert waiting["ended_mono_ns"] <= prepare["started_mono_ns"]
    assert "thread_cpu_ms" not in waiting
    assert prepare["thread_cpu_ms"] >= 0


def test_span_metadata_is_allowlisted_and_bounded_independent_of_payload_size(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    with telemetry.timing_span(
        "safe",
        root=True,
        rows=3,
        bytes=float("nan"),
        collection="https://private.example/token",
        job_id="private-job",
        password="private-password" * 100000,
    ):
        pass
    records = events(caplog)
    assert len(records) == 2
    assert all(row["rows"] == 3 for row in records)
    assert all(not {"bytes", "collection", "job_id", "password"} & row.keys() for row in records)
    assert "private" not in caplog.text
    assert len(caplog.text) < 2000


async def test_session_audit_wire_and_openma_acceptance_close_with_same_call_context(caplog):
    helpers = run_path(str(Path(__file__).with_name("test_agents.py")))
    settings, _ = helpers["settings"]()
    repo = MemoryRepository()
    session = await helpers["seed_session"](repo)
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    requests = []

    async def dispatch(name, arguments, context):
        truth = compute_market_truth({"token_id": "world:WLD", "cutoff": "2026-10-28T12:00:00Z"})
        truth["mta_id"] = "MTA-TIMING"
        await repo.put("ana.market_truth", truth["mta_id"], truth, immutable=True)
        return {"market_truth": [truth]}

    def accept(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accept)) as http:
        service = SessionService(repo, settings, dispatch, http=http)
        await service.tool(session, {"name": "get_market_truth", "id": "call-timing", "input": {}})
    stored = await repo.get("ops.tool_calls", "ses_1:call-timing")
    assert stored["status"] == "DONE"
    assert stored["returned_at"] is not None
    assert stored["result"]["market_truth"][0] == await repo.get("ana.market_truth", "MTA-TIMING")
    delivered = json.loads(requests[0]["events"][0]["content"][0]["text"])
    assert delivered["market_truth"][0]["cutoff"] == "2026-10-28T12:00:00Z"
    assert "measurement_metadata" not in delivered["market_truth"][0]
    ended = [row for row in events(caplog) if row["event"] == "stage_end"]
    assert {
        "tool",
        "tool.audit",
        "tool.projection",
        "tool.wire_json",
        "tool.wire_audit",
        "tool.openma_acceptance",
        "tool.return_audit",
    } <= {row["stage"] for row in ended}
    assert len({row["trace_id"] for row in ended}) == 1
    assert all(row["call_id"] == telemetry.timing_identity("call-timing") for row in ended)
    assert all(row["session_id"] == telemetry.timing_identity("ses_1") for row in ended)

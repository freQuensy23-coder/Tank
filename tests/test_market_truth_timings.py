import asyncio
import json
import logging

import pytest

from tank import logging as telemetry
from tank.analysis.truth import compute_market_truth


def events(caplog):
    return [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == "tank.market_timing"
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
    with telemetry.timing_span("outer", root=True):
        with telemetry.timing_span("inner"):
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
    with pytest.raises(type(failure)):
        with telemetry.timing_span("failure", root=True, password="private-password"):
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
            if row["stage"] == "compute" and row["event"] == "stage_end" and row["trace_id"] == root["trace_id"]
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

import asyncio
import json
import logging
import threading
import time
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest

from tank import clickhouse_market as chm
from tank import logging as telemetry
from tank.storage import MemoryRepository
from tank.util import iso, parse_time


def timing_events(caplog, event=None):
    records = [
        json.loads(record.getMessage()) for record in caplog.records if record.name == "tank.market_timing"
    ]
    return [record for record in records if event is None or record["event"] == event]


@pytest.mark.parametrize("count", [1, 1000, 1001])
async def test_compact_projection_owns_conversion_at_the_existing_offload_boundary(
    count, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    base = parse_time("2026-10-28T00:00:00Z")
    fields = ["open_time", "open_time_prec", "c", "volume_native", "complete"]
    raw = [
        {
            "open_time": iso(base + timedelta(minutes=index)),
            "open_time_prec": "BUCKET(60s)",
            "c": "1.2345678901234567890123456789",
            "volume_native": "3.00000000000000000001",
            "complete": True,
            "_cursor_time": iso(base + timedelta(minutes=index)),
            "_cursor_key": f"bar-{index:04d}",
        }
        for index in range(count)
    ]
    payload = "\n".join(json.dumps(row) for row in raw).encode()
    entered = threading.Event()
    release = threading.Event()
    conversions = []
    decimal = chm.Decimal
    main_thread = threading.get_ident()

    def observed_decimal(value):
        if not conversions:
            entered.set()
            if count > 1000 and not release.wait(2):
                raise TimeoutError("projection conversion barrier was not released")
        conversions.append((time.monotonic_ns(), threading.get_ident()))
        return decimal(value)

    monkeypatch.setattr(chm, "Decimal", observed_decimal)

    async def read_projection(repo):
        with telemetry.timing_span("projection_request", root=True):
            return [
                row
                async for row in repo.iter_market_projection(
                    "mkt.klines_1m",
                    fields,
                    "open_time",
                    start=iso(base),
                    end=iso(base + timedelta(minutes=count)),
                    filters={"complete": True},
                    instrument_ids=["test:spot:T"],
                    assembled_at=iso(base + timedelta(days=2)),
                    batch_size=count + 1,
                )
            ]

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=payload))
    ) as http:
        repo = MemoryRepository()
        repo.clickhouse = chm.ClickHouse("http://clickhouse.test", http=http)
        task = asyncio.create_task(read_projection(repo))
        try:
            if count > 1000:
                assert await asyncio.to_thread(entered.wait, 2)
                records = timing_events(caplog)
                assert any(
                    row["stage"] == "clickhouse.projection_convert" and row["event"] == "stage_start"
                    for row in records
                ), "dense projection conversion has no owned live span"
                assert not any(
                    row["stage"] == "clickhouse.projection_convert" and row["event"] == "stage_end"
                    for row in records
                )
        finally:
            release.set()
            result = await task
    assert result == [{key: row[key] for key in fields} for row in raw]
    assert len(conversions) == count * 2
    assert {thread for _, thread in conversions} == (
        {main_thread} if count <= 1000 else {conversions[0][1]}
    )
    if count > 1000:
        assert conversions[0][1] != main_thread
    ended = timing_events(caplog, "stage_end")
    converted = [row for row in ended if row["stage"] == "clickhouse.projection_convert"]
    assert len(converted) == 1, "compact projection bypassed conversion instrumentation"
    converted = converted[0]
    assert converted["rows"] == count
    assert converted["thread_cpu_ms"] >= 0
    assert converted["started_mono_ns"] <= conversions[0][0]
    assert converted["ended_mono_ns"] >= conversions[-1][0]
    fetched = next(row for row in ended if row["stage"] == "clickhouse.http")
    assert converted["query_id"] == fetched["query_id"]
    waits = [row for row in ended if row["stage"] == "threadpool.wait"]
    work = [row for row in ended if row["stage"] == "threadpool.work"]
    assert len(waits) == len(work) == int(count > 1000)
    if work:
        assert work[0]["started_mono_ns"] <= conversions[0][0]
        assert work[0]["ended_mono_ns"] >= conversions[-1][0]
        assert waits[0]["ended_mono_ns"] <= work[0]["started_mono_ns"]
        assert waits[0]["query_id"] == work[0]["query_id"] == fetched["query_id"]


async def test_executor_queue_end_is_worker_entry_before_the_timing_sink(monkeypatch):
    clock = [1_000_000]
    records = []
    worker_entry = []
    real_to_thread = asyncio.to_thread

    async def observed_to_thread(function, *args, **kwargs):
        def enter():
            clock[0] = 4_000_000
            worker_entry.append(clock[0])
            return function(*args, **kwargs)

        return await real_to_thread(enter)

    def sink(message):
        record = json.loads(message)
        records.append(record)
        if record["stage"] == "threadpool.wait" and record["event"] == "stage_start":
            clock[0] += 10_000_000

    monkeypatch.setattr(
        telemetry,
        "time",
        SimpleNamespace(monotonic_ns=lambda: clock[0], thread_time_ns=time.thread_time_ns),
    )
    monkeypatch.setattr(asyncio, "to_thread", observed_to_thread)
    monkeypatch.setattr(telemetry._TIMING_LOG, "info", sink)
    with telemetry.timing_span("request", root=True):
        result = await telemetry.timing_thread(sum, [2, 3])
    assert result == 5
    queue = next(
        row for row in records if row["stage"] == "threadpool.wait" and row["event"] == "stage_end"
    )
    work = next(
        row for row in records if row["stage"] == "threadpool.work" and row["event"] == "stage_end"
    )
    assert queue["started_mono_ns"] == 1_000_000
    assert queue["ended_mono_ns"] == worker_entry[0] == 4_000_000
    assert queue["wall_ms"] == 3
    assert "thread_cpu_ms" not in queue
    assert work["started_mono_ns"] >= 14_000_000
    assert work["parent_span_id"] == queue["parent_span_id"]


@pytest.mark.parametrize("count", [1, 1001])
async def test_full_record_conversion_retains_the_full_row_query_identity(count, caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    requests = []
    at = "2026-10-28T11:59:00Z"
    rows = [
        {
            "storage_key": f"bar-{index:04d}",
            "instrument_id": "test:spot:T",
            "token_id": "T",
            "open_time": at,
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "c": "1.2345678901234567890123456789",
            "complete": True,
        }
        for index in range(count)
    ]

    def respond(request):
        requests.append(request.url.params["query_id"])
        if len(requests) == 1:
            selected = [
                {"storage_key": row["storage_key"], "instrument_id": row["instrument_id"], "_day": at}
                for row in rows
            ]
        else:
            selected = rows
        return httpx.Response(200, content="\n".join(json.dumps(row) for row in selected))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = chm.ClickHouse("http://clickhouse.test", http=http)
        with telemetry.timing_span("range_request", root=True):
            page = await chm.range_page(
                client,
                "mkt.klines_1m",
                "open_time",
                start="2026-10-28T11:00:00Z",
                end="2026-10-28T12:00:00Z",
                limit=count + 1,
            )
            with telemetry.timing_span("after_range"):
                pass
    assert len(page) == count
    assert [cursor for _, cursor in page] == [(at, row["storage_key"]) for row in rows]
    assert all(
        record["c"] == "1.2345678901234567890123456789"
        and record["open_time_prec"] == "BUCKET(60s)"
        and record["complete"] is True
        for record, _ in page
    )
    ended = timing_events(caplog, "stage_end")
    http_spans = [row for row in ended if row["stage"] == "clickhouse.http"]
    assert [row["query_id"] for row in http_spans] == requests
    assert len(set(requests)) == 2
    typed = next(row for row in ended if row["stage"] == "clickhouse.typed_decode")
    assert typed.get("query_id") == requests[-1], "typed conversion lost ownership of the full-row query"
    assert typed["query_id"] != requests[0]
    assert typed["rows"] == count
    assert typed["thread_cpu_ms"] >= 0
    assert "query_id" not in next(row for row in ended if row["stage"] == "after_range")
    if count > 1000:
        executor = [row for row in ended if row["stage"] in {"threadpool.wait", "threadpool.work"}]
        assert len(executor) == 2
        assert all(row.get("query_id") == requests[-1] for row in executor)

import asyncio
import cProfile
import json
import logging
import pstats
from pathlib import Path
from runpy import run_path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from tank import clickhouse_market as chm
from tank import logging as telemetry
from tank.analysis.truth import MarketTruthService, compute_market_truth
from tank.event_tools import EventToolService
from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.util import sha256

pytestmark = pytest.mark.integration


async def test_durable_tool_worker_snapshot_compute_and_persistence_are_linked(setup, monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    repo, settings, instrument = setup
    token = instrument["token_id"]
    event_id = "EVT-TIMING"
    anchor = "2026-10-28T11:00:00Z"
    cutoff = "2026-10-28T12:00:00Z"
    await repo.put(
        "evt.events",
        event_id,
        {
            "event_id": event_id,
            "current_ems_id": "EMS-TIMING",
            "tokens_mentioned": [token],
            "known_to_us_at": anchor,
            "known_to_us_at_prec": "EXACT",
        },
    )
    await repo.put("evt.ems_assessments", "EMS-TIMING", {"class": "MAJOR"})
    backfill_id = "BF-" + sha256({"event_id": event_id, "token_id": token, "anchor": anchor})
    await repo.put(
        "mkt.backfill_runs",
        backfill_id,
        {
            "backfill_id": backfill_id,
            "event_id": event_id,
            "token_id": token,
            "anchor": anchor,
            "started_at": anchor,
            "status": "COMPLETE",
        },
    )
    await repo.put(
        "mkt.klines_1m",
        "one-close",
        {
            "instrument_id": instrument["instrument_id"],
            "token_id": token,
            "open_time": "2026-10-28T11:59:00Z",
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "c": "2",
            "volume_native": "3",
            "complete": True,
        },
    )
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)
        truth = MarketTruthService(repo, settings, market)
        tools = EventToolService(repo, settings, truth, None)
        runtime = object.__new__(Runtime)
        runtime.repo, runtime.settings, runtime.market, runtime.truth = repo, settings, market, truth
        runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate, "RUN_MARKET_TRUTH": runtime.assess}
        stop = asyncio.Event()
        finish = repo.finish_job

        async def observe_finish(job_id):
            await finish(job_id)
            row = await repo.get("ops.jobs", job_id)
            if row["type"] == "RUN_MARKET_TRUTH":
                stop.set()

        monkeypatch.setattr(repo, "finish_job", observe_finish)
        async with asyncio.timeout(5):
            result, _ = await asyncio.gather(
                tools.get_market_truth({"event_id": event_id, "token_ids": [token], "cutoff": cutoff}, {}),
                runtime.worker(stop),
            )
        record = result["market_truth"][0]
        assert await repo.get("ana.market_truth", record["mta_id"]) == record
        jobs = await repo.list("ops.jobs", filters={"type": "RUN_MARKET_TRUTH"})
        assert len(jobs) == 1
        assert jobs[0]["status"] == "DONE"
        assert jobs[0]["result"] == record
        assert record["cutoff"] == cutoff
        assert record["data_confidence"]["class"] == "LOW"
        with telemetry.timing_span("warm_snapshot", root=True):
            await market.snapshot(token, cutoff, anchor={"type": "KNOWN_TO_US_AT", "ts": anchor})
    records = [json.loads(r.getMessage()) for r in caplog.records if r.name == "tank.market_timing"]
    ends = [r for r in records if r["event"] == "stage_end"]
    by_stage = {r["stage"] for r in ends}
    assert {
        "get_market_truth", "tool.validation", "tool.enqueue", "tool.jobs_wait", "job.link", "job.claimed",
        "job.execution", "job.capture_readiness", "job.repair", "snapshot", "snapshot.collection",
        "snapshot.series", "snapshot.fingerprints", "assessment.lock_wait", "threadpool.wait",
        "compute.lock_wait", "compute.metadata", "assessment.persist", "job.persist",
    } <= by_stage
    job_key = telemetry.timing_identity(jobs[0]["job_id"])
    for stage in ("job.link", "job.claimed", "job.execution", "assessment.persist"):
        assert any(r["stage"] == stage and r.get("job_id") == job_key for r in ends)
    assert {r["cache_hit"] for r in ends if r["stage"] == "snapshot.range_cache"} == {False, True}
    starts = {r["span_id"] for r in records if r["event"] == "stage_start"}
    assert starts == {r["span_id"] for r in ends}
    assert len(starts) == len(ends)
    assert "trace_id" not in json.dumps(record)
    assert all(r["status"] == "ok" for r in ends)


async def test_http_decode_and_real_compute_share_trace_without_payloads(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    requests = []
    tasks = set()
    payload = b'{"instrument_id":"test:spot:T","open_time":"2026-10-28T11:59:00Z","c":"2","complete":true}\n'

    async def serve(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            headers = head.decode().split("\r\n")
            length = int(next(line.split(":", 1)[1] for line in headers if line.lower().startswith("content-length:")))
            await reader.readexactly(length)
            requests.append(parse_qs(urlsplit(headers[0].split()[1]).query))
            writer.write(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                + str(len(payload)).encode()
                + b"\r\n\r\n"
                + payload
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient() as http:
            client = chm.ClickHouse(f"http://127.0.0.1:{port}", password="private-password", http=http)
            with telemetry.timing_span("integration", root=True):
                rows = await client.rows("SELECT private_sql", {"private_param": ("private-value", "String")})
                decoded = await chm._decode("mkt.klines_1m", rows)
                result = await asyncio.to_thread(
                    compute_market_truth,
                    {"token_id": "test:T", "cutoff": "2026-10-28T12:00:00Z", "klines": decoded},
                )
    finally:
        server.close()
        if tasks:
            await asyncio.gather(*tasks)
        await server.wait_closed()
    assert result["cutoff"] == "2026-10-28T12:00:00Z"
    assert result["data_confidence"]["class"] == "LOW"
    records = [json.loads(r.getMessage()) for r in caplog.records if r.name == "tank.market_timing"]
    ended = [r for r in records if r["event"] == "stage_end"]
    assert len({r["trace_id"] for r in records}) == 1
    assert {"clickhouse.http", "clickhouse.json_decode", "clickhouse.typed_decode", "compute"} <= {
        r["stage"] for r in ended
    }
    http_span = next(r for r in ended if r["stage"] == "clickhouse.http")
    assert requests[0]["query_id"] == [http_span["query_id"]]
    assert http_span["bytes"] == len(payload)
    assert next(r for r in ended if r["stage"] == "clickhouse.json_decode")["rows"] == 1
    assert "private-" not in "\n".join(r.getMessage() for r in caplog.records if r.name == "tank.market_timing")


def test_bounded_real_compute_profile_retains_hot_functions(record_property):
    fixture = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))
    snapshot = fixture["compact_start_snapshot"]()
    profiler = cProfile.Profile()
    result = profiler.runcall(compute_market_truth, snapshot)
    assert result["move_class"] == "SPOT_LED_ACCUMULATION"
    stats = pstats.Stats(profiler)
    functions = [
        {
            "file": Path(filename).name,
            "line": line,
            "function": function,
            "calls": values[1],
            "self_s": values[2],
            "cumulative_s": values[3],
        }
        for (filename, line, function), values in stats.stats.items()
        if "/tank/" in filename
    ]
    top = sorted(functions, key=lambda row: row["cumulative_s"], reverse=True)[:20]
    assert any(row["function"] == "compute_market_truth" for row in top)
    assert any(row["function"] == "_compute_market_truth" for row in top)
    record_property("controlled_fixture_profile_top", json.dumps(top, sort_keys=True))
    record_property("controlled_fixture_profile_scope", "synthetic compact START; not historical ENA reproduction")

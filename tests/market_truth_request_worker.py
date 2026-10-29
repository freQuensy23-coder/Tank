import asyncio
import cProfile
import gc
import hashlib
import json
import logging
import pstats
import re
import signal
import sys
import threading
from collections import Counter
from contextlib import suppress
from contextvars import copy_context
from datetime import datetime
from decimal import Decimal
from functools import wraps
from pathlib import Path
from time import perf_counter, process_time
from urllib.parse import urlsplit

import httpx
from market_truth_request_data import ORIGINAL, seed_data

from tank import clickhouse_market as chm
from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION, ModelContext
from tank.analysis import compute_workers, truth
from tank.clickhouse_market import ClickHouse
from tank.event_tools import EventToolService
from tank.market import service as market_module
from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import PostgresRepository
from tank.util import iso, parse_time, sha256

TOKEN = "ethena:ENA"
ASSEMBLED = "2026-10-28T12:08:24.291857Z"


def emit(value):
    print(json.dumps(value, sort_keys=True), flush=True)


def provenance(root):
    modules = {}
    for name, module in tuple(sys.modules.items()):
        if name == "tank" or name.startswith("tank."):
            filename = getattr(module, "__file__", None)
            paths = [filename] if filename else list(module.__spec__.submodule_search_locations or [])
            assert paths
            resolved = [Path(path).resolve() for path in paths]
            assert all(path.is_relative_to(root / "src/tank") for path in resolved)
            if not filename:
                assert set(resolved) == {Path(path).resolve() for path in module.__path__}
            modules[name] = [str(path.relative_to(root)) for path in resolved]
    assert {
        "tank.storage",
        "tank.analysis.truth",
        "tank.market.service",
        "tank.agents.sessions",
    } <= modules.keys()
    return modules


def encode(value):
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    return list(value)


def save(directory, name, value):
    body = json.dumps(value, sort_keys=True, default=encode).encode()
    (directory / name).write_bytes(body)
    return hashlib.sha256(body).hexdigest()


def clear_timestamp_cache():
    parser = getattr(sys.modules["tank.util"], "_parse_text_time", None)
    if parser is not None:
        parser.cache_clear()
        assert parser.cache_info().currsize == 0


def timed_process_compute(parts, config, baseline_key):
    original = truth.compute_market_truth
    assert original.__module__ == "tank.analysis.truth"
    assert compute_workers._compute is not timed_process_compute
    before = compute_workers._baselines.get(baseline_key)
    receipt = {
        "baseline_present_before": baseline_key in compute_workers._baselines,
        "baseline_entries_before": len(before) if before is not None else 0,
        "compute_seconds": [],
        "compute_cpu_seconds": [],
    }

    @wraps(original)
    def measured(*args, **kwargs):
        started, cpu = perf_counter(), process_time()
        try:
            return original(*args, **kwargs)
        finally:
            receipt["compute_seconds"].append(perf_counter() - started)
            receipt["compute_cpu_seconds"].append(process_time() - cpu)

    truth.compute_market_truth = measured
    try:
        result = compute_workers._compute(parts, config, baseline_key)
    finally:
        truth.compute_market_truth = original
    assert len(receipt["compute_seconds"]) == 1
    after = compute_workers._baselines.get(baseline_key)
    receipt["baseline_present_after"] = baseline_key in compute_workers._baselines
    receipt["baseline_entries_after"] = len(after) if after is not None else 0
    root = Path(sys.modules["tank.util"].__file__).resolve().parents[2]
    receipt["modules"] = provenance(root)
    receipt["artifacts"] = {
        name: {
            "path": str(Path(sys.modules[name].__file__).resolve().relative_to(root)),
            "sha256": hashlib.sha256(Path(sys.modules[name].__file__).read_bytes()).hexdigest(),
            "loader": type(sys.modules[name].__loader__).__name__,
        }
        for name in ("tank.analysis.truth", "tank.clickhouse_market", "tank.util")
    }
    return result, receipt


def profile_retained_call(call, *, gc_receipt=None):
    context, function, args, kwargs = call
    if "baseline_cache" in kwargs:
        kwargs = dict(kwargs, baseline_cache={})
    assert sys.getprofile() is None
    profiler = cProfile.Profile()
    starts = {}
    parser = getattr(sys.modules["tank.util"], "_parse_text_time", None)

    def observe_gc(phase, info):
        generation = info["generation"]
        if phase == "start":
            starts[generation] = perf_counter()
            return
        elapsed = perf_counter() - starts.pop(generation)
        counts = gc_receipt["by_generation"].setdefault(
            str(generation), {"collections": 0, "collected": 0, "uncollectable": 0, "seconds": 0.0}
        )
        for name, value in (
            ("collections", 1),
            ("collected", info["collected"]),
            ("uncollectable", info["uncollectable"]),
            ("seconds", elapsed),
        ):
            counts[name] += value
            gc_receipt[name] += value

    if gc_receipt is not None:
        gc_receipt.update(
            collections=0,
            collected=0,
            uncollectable=0,
            seconds=0.0,
            by_generation={},
            timestamp_cache_before=parser.cache_info()._asdict() if parser is not None else None,
        )
        gc.callbacks.append(observe_gc)
    try:
        profiler.runcall(context.run, function, *args, **kwargs)
    finally:
        if gc_receipt is not None:
            gc.callbacks.remove(observe_gc)
            gc_receipt["timestamp_cache_after"] = (
                parser.cache_info()._asdict() if parser is not None else None
            )
    assert sys.getprofile() is None
    return profile_rows(profiler)


async def profile_retained_async(call):
    context, function, args, kwargs = call
    assert sys.getprofile() is None
    profiler = cProfile.Profile()
    profiler.enable()
    try:
        async with asyncio.timeout(10):
            await asyncio.create_task(function(*args, **kwargs), context=context)
    finally:
        profiler.disable()
    return profile_rows(profiler)


async def sample_retained_async(call):
    context, function, args, kwargs = call
    stopped = threading.Event()
    samples = Counter()
    ticks = 0
    deadline = perf_counter() + 10

    def sample():
        nonlocal ticks
        while True:
            for ident, frame in sys._current_frames().items():
                if ident != threading.get_ident():
                    code = frame.f_code
                    samples[(Path(code.co_filename).name, code.co_firstlineno, code.co_name)] += 1
            ticks += 1
            if stopped.wait(0.005) or perf_counter() >= deadline:
                return

    sampler = threading.Thread(target=sample, name="market-truth-sampler", daemon=True)
    sampler.start()
    try:
        async with asyncio.timeout(10):
            await asyncio.create_task(function(*args, **kwargs), context=context)
    finally:
        stopped.set()
        sampler.join(timeout=1)
        assert not sampler.is_alive()
    return {
        "kind": "statistical_thread_samples",
        "period_s": 0.005,
        "ticks": ticks,
        "rows": [
            {"module": module, "line": line, "function": name, "samples": count}
            for (module, line, name), count in samples.most_common(40)
        ],
    }


def profile_rows(profiler):
    stats = pstats.Stats(profiler).stats
    return [
        {
            "module": Path(filename).name,
            "line": line,
            "function": name,
            "calls": values[1],
            "self_seconds": values[2],
            "inclusive_seconds": values[3],
            "callers": [
                {
                    "module": Path(caller_file).name,
                    "line": caller_line,
                    "function": caller_name,
                    "calls": counts[1],
                    "self_seconds": counts[2],
                    "inclusive_seconds": counts[3],
                }
                for (caller_file, caller_line, caller_name), counts in sorted(
                    values[4].items(), key=lambda item: item[1][3], reverse=True
                )[:5]
            ],
        }
        for (filename, line, name), values in sorted(
            stats.items(), key=lambda item: item[1][2], reverse=True
        )[:40]
    ]


async def seed_session(repo, state, anchor):
    event, sid, ems, item = "event-" + state, "s-" + state, "ems-" + state, "item-" + state
    await repo.put("reg.sources", "S01", {"source_id": "S01", "name": "Public replay"})
    await repo.put(
        "ingest.source_items",
        item,
        {
            "item_id": item,
            "source_id": "S01",
            "feed": "fixture",
            "external_id": state,
            "text": "Public retained ENA workload",
            "source_class": "PRIMARY",
            "observed_at": anchor,
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": anchor,
            "published_prec": "EXACT",
            "raw_ref": "/fixture/raw",
            "raw_sha256": sha256("public"),
            "content_hash": sha256("public"),
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        },
    )
    await repo.put(
        "evt.events",
        event,
        {
            "event_id": event,
            "statement": "Public retained ENA workload",
            "theme_tags": [],
            "entities": [],
            "tokens_mentioned": [TOKEN],
            "public_at": anchor,
            "public_at_prec": "EXACT",
            "public_at_basis": "TEST",
            "known_to_us_at": anchor,
            "known_to_us_at_prec": "EXACT",
            "event_state": "APPROVED",
            "state_verified": True,
            "certainty": "HIGH",
            "timing_confidence": "HIGH",
            "freshness": "FRESH",
            "best_source_class": "PRIMARY",
            "lifecycle": "MONITORING",
            "current_ems_id": ems,
            "config_version": "fixture",
            "code_version": "fixture",
        },
    )
    await repo.put(
        "evt.ems_assessments",
        ems,
        {
            "ems_id": ems,
            "event_id": event,
            "version": 1,
            "cutoff": anchor,
            "class": "MAJOR",
            "criterion": "M2",
            "dims": {},
            "gates": [],
            "structural_gates": [],
            "class_reasons": ["Seeded replay"],
            "impact_horizon": {},
            "config_version": "fixture",
            "code_version": "fixture",
            "model_id": "fixture",
            "prompt_version": "fixture",
        },
    )
    session = {
        "session_id": sid,
        "event_id": event,
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "opened_by_item": item,
        "opened_at": anchor,
        "opened_at_prec": "COLLECTION_ONLY",
        "last_item_at": anchor,
        "last_item_at_prec": "COLLECTION_ONLY",
        "last_event_seq": 0,
        "agent_version": "1",
        "prompt_version": "a" * 64,
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "model_id": "fixture",
        "active_message_id": "m",
        "model_context_version": CONTEXT_VERSION,
        "message_log": [
            {
                "message_id": "m",
                "remote_event_id": "input",
                "retry_count": 0,
                "payload": {"kind": "NEW_ITEM", "item_id": item},
            }
        ],
    }
    await repo.put("ops.agent_sessions", sid, session)
    backfill = "BF-" + sha256({"event_id": event, "token_id": TOKEN, "anchor": anchor})
    await repo.put(
        "mkt.backfill_runs",
        backfill,
        {
            "backfill_id": backfill,
            "event_id": event,
            "token_id": TOKEN,
            "anchor": anchor,
            "anchor_prec": "EXACT",
            "started_at": anchor,
            "started_at_prec": "COLLECTION_ONLY",
            "finished_at": ASSEMBLED,
            "finished_at_prec": "COLLECTION_ONLY",
            "status": "COMPLETE",
            "config_version": "fixture",
            "code_version": "fixture",
        },
    )
    return session


async def run(root, directory, fixtures, revision, reader):
    import os

    provenance(root)
    assert urlsplit(os.environ["CLICKHOUSE_TEST_URL"]).hostname in {"127.0.0.1", "localhost", "::1"}
    descriptor = json.loads(await reader.readline())
    repo = PostgresRepository(descriptor["database_url"])
    await repo.open()
    repo.clickhouse = ClickHouse(
        os.environ["CLICKHOUSE_TEST_URL"], database=descriptor["clickhouse_database"]
    )
    errors, received, connections = [], {}, set()

    async def receive_http(reader, writer):
        connections.add(asyncio.current_task())
        try:
            async with asyncio.timeout(10):
                lines = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
                headers = dict(line.lower().split(": ", 1) for line in lines[1:] if line)
                size = int(headers["content-length"])
                assert 0 < size < 8_000_000
                body = json.loads(await reader.readexactly(size))
                native = body["events"][0]
                state = native["custom_tool_use_id"]
                assert lines[0] == f"POST /v1/sessions/s-{state}/events HTTP/1.1"
                assert headers["anthropic-beta"] == "managed-agents-2026-04-01"
                assert headers["idempotency-key"] == "tool:" + sha256(f"s-{state}:{state}")
                audit = await repo.get("ops.tool_calls", f"s-{state}:{state}")
                assert audit["status"] == "DONE" and not audit["is_error"] and not audit.get("returned_at")
                assert audit["native_result_body"] == native
                result = audit["result"]["market_truth"][0]
                assert await repo.get("ana.market_truth", result["mta_id"]) == result
                received[state] = json.loads(native["content"][0]["text"])
                response = json.dumps({"data": [{"id": "accepted-" + state}]}).encode()
                writer.write(
                    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: "
                    + str(len(response)).encode()
                    + b"\r\n\r\n"
                    + response
                )
                await writer.drain()
        except Exception as error:
            errors.append(failure_receipt(error))
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            connections.discard(asyncio.current_task())

    server = await asyncio.start_server(receive_http, "127.0.0.1", 0)
    try:
        async with httpx.AsyncClient(trust_env=False) as http:
            config_dir = Path(descriptor["input_config_dir"])
            assert not config_dir.is_symlink()
            assert config_dir.resolve() == (root.parent / "retained-inputs" / "config").resolve()
            settings = Settings(
                _env_file=None,
                config_dir=config_dir,
                data_dir=directory / "data",
                replay=True,
                openma_url=f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                openma_api_key=None,
                code_version=revision,
            )
            manifest = json.loads((fixtures / "manifest.json").read_bytes())
            started = perf_counter()
            native = await seed_data(repo, settings, fixtures, manifest)
            market = MarketService(repo, settings, http=http)
            clock = {"value": ASSEMBLED}
            market_module.utcnow = lambda: parse_time(ASSEMBLED)
            market_module.iso = lambda value=None: iso(clock["value"] if value is None else value)
            assert len(market.instruments) == 181
            assert await market._load_day_counts() == manifest["native_cache"]["entries"] == 7209
            assert not market.__dict__.get("_condition_series")
            service = truth.MarketTruthService(repo, settings, market)
            tools = EventToolService(repo, settings, service, None)
            sessions = SessionService(repo, settings, tools.dispatch, http=http)
            seeded = {
                state: await seed_session(repo, state, manifest["anchor"]) for state in ("cold", "warm")
            }
            runtime = object.__new__(Runtime)
            runtime.repo, runtime.settings, runtime.market, runtime.truth = repo, settings, market, service
            runtime.scheduler = Scheduler(repo, settings, market, service)
            runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate, "RUN_MARKET_TRUTH": runtime.assess}
            snapshots, dense, phases, phase_calls = [], {}, {}, {}
            diagnostic_call = snapshot_call = None
            diagnostic_seconds = 0.0
            original_snapshot, original_heads, original_rows = (
                market.snapshot,
                market._snapshot_heads,
                repo.clickhouse.rows,
            )

            def heads(*args):
                started = perf_counter()
                original_heads(*args)
                phases["snapshot.heads"] = perf_counter() - started
                clock["value"] = ASSEMBLED

            async def snapshot(*args, **kwargs):
                nonlocal snapshot_call
                snapshot_call = (copy_context(), original_snapshot, args, kwargs)
                clock["value"] = manifest["availability_cutoff"]
                started = perf_counter()
                value = await original_snapshot(*args, **kwargs)
                phases["snapshot"] = perf_counter() - started
                snapshots.append(value)
                return value

            async def rows(query, params=None):
                value = await original_rows(query, params)
                selected = set(re.findall(r"`([a-z_]+)` AS `\1`", query.split(" FROM ")[0]))
                common = {"open_time", "open_time_prec", "recorded_at", "complete", "interval_s"}
                for entry in manifest["dense_projections"]:
                    field, iid = entry["field"], entry["instrument_id"]
                    if selected == common | {field} and f"instrument_id IN ('{iid}')" in query:
                        dense[iid, field] = dense.get((iid, field), 0) + len(value)
                return value

            market.snapshot, market._snapshot_heads, repo.clickhouse.rows = snapshot, heads, rows
            emit(
                {
                    "kind": "ready",
                    "revision": revision,
                    "input_configuration_revision": ORIGINAL,
                    "modules": provenance(root),
                    "native": native,
                    "setup_seconds": perf_counter() - started,
                }
            )
            if hasattr(chm, "dense_projection_page"):
                original_dense = chm.dense_projection_page

                async def dense_page(client, **kwargs):
                    value = await original_dense(client, **kwargs)
                    key = kwargs["instrument_id"], kwargs["field"]
                    dense[key] = dense.get(key, 0) + len(value["open_us"])
                    return value

                chm.dense_projection_page = dense_page

            def measure(name, function):
                @wraps(function)
                def measured(*args, **kwargs):
                    nonlocal diagnostic_call, diagnostic_seconds
                    context = copy_context()
                    started = perf_counter()
                    try:
                        return function(*args, **kwargs)
                    finally:
                        elapsed = perf_counter() - started
                        if elapsed > diagnostic_seconds:
                            diagnostic_call = (context, function, args, kwargs)
                            diagnostic_seconds = elapsed
                        phases[name] = phases.get(name, 0) + elapsed
                        phase_calls[name] = phase_calls.get(name, 0) + 1

                return measured

            def measure_async(name, function):
                @wraps(function)
                async def measured(*args, **kwargs):
                    started = perf_counter()
                    try:
                        return await function(*args, **kwargs)
                    finally:
                        phases[name] = phases.get(name, 0) + perf_counter() - started
                        phase_calls[name] = phase_calls.get(name, 0) + 1

                return measured

            for name, handler in tuple(runtime.handlers.items()):
                runtime.handlers[name] = measure_async(f"request.{name}", handler)
            ModelContext.result = measure_async("return.project", ModelContext.result)
            original_put = repo.put

            async def put(collection, *args, **kwargs):
                name = "request.repo.put." + collection
                return await measure_async(name, original_put)(collection, *args, **kwargs)

            repo.put = measure_async("request.repo.put", put)
            sessions.request = measure_async("return.request", sessions.request)

            for name in (
                "_ranged_read",
                "_ranged_cached",
                "_condition_kline_series",
                "_condition_heads",
                "_series_counts",
                "_capture_metadata",
            ):
                setattr(market, name, measure_async("snapshot." + name, getattr(market, name)))

            for name in (
                "_compute_market_truth",
                "spot_window_graph",
                "relative_truth",
                "derivatives_truth",
                "support_features",
                "spot_truth",
                "data_confidence",
                "_price_groups",
                "_oi_rows",
                "_oi_stats",
                "synchronized_oi",
                "_oi_combined_timeline",
                "_native_volume_arrays",
                "_history_decimal_arrays",
                "_history_fixed_decimal_column",
                "_synchronized_oi",
                "_composite_history",
                "_oi_history_returns",
                "_composite_history_vector",
                "_relative_history_arrays",
                "_relative_history_returns",
                "btc_baseline",
                "enrich_evidence",
                "timestamp_companions",
                "measurement_metadata",
                "bounded_market_snapshot",
                "liquidity_truth",
                "supply_truth",
                "available_field_freshness",
            ):
                if hasattr(truth, name):
                    setattr(truth, name, measure(name, getattr(truth, name)))
            original_compute = truth.compute_market_truth
            original_worker_compute = compute_workers.compute_market_truth
            compute_workers._compute = timed_process_compute
            compute_seconds, worker_receipts = [], []

            async def compute_in_worker(lane, snapshot, *, config, baseline_key=None):
                nonlocal diagnostic_call, diagnostic_seconds
                context = copy_context()
                started = perf_counter()
                result, receipt = await original_worker_compute(
                    lane, snapshot, config=config, baseline_key=baseline_key
                )
                assert lane == compute_workers.ASSESSMENT and baseline_key == TOKEN
                assert len(receipt["compute_seconds"]) == 1
                parent_modules = provenance(root)
                assert all(parent_modules.get(name) == paths for name, paths in receipt["modules"].items())
                for name, artifact in receipt["artifacts"].items():
                    module = sys.modules[name]
                    filename = Path(module.__file__).resolve()
                    assert artifact == {
                        "path": str(filename.relative_to(root)),
                        "sha256": hashlib.sha256(filename.read_bytes()).hexdigest(),
                        "loader": type(module.__loader__).__name__,
                    }
                compute_seconds.extend(receipt["compute_seconds"])
                worker_receipts.append(receipt)
                diagnostic_call = (
                    context,
                    original_compute,
                    (snapshot,),
                    {"config": config, "baseline_cache": {}},
                )
                diagnostic_seconds = max(diagnostic_seconds, perf_counter() - started)
                return result

            compute_workers.compute_market_truth = measure_async("compute.worker_boundary", compute_in_worker)
            while command := await reader.readline():
                command = json.loads(command)
                if command.get("stop"):
                    break
                if command.get("diagnose"):
                    assert diagnostic_call is not None
                    assert snapshot_call is not None
                    cold_market = MarketService(repo, settings, http=http)
                    assert await cold_market._load_day_counts() == manifest["native_cache"]["entries"]
                    cold_heads = cold_market._snapshot_heads

                    def cold_snapshot_heads(result, cutoff_dt):
                        cold_heads(result, cutoff_dt)
                        clock["value"] = ASSEMBLED

                    cold_market._snapshot_heads = cold_snapshot_heads
                    clock["value"] = manifest["availability_cutoff"]
                    clear_timestamp_cache()
                    cold_profile = await sample_retained_async(
                        (copy_context(), cold_market.snapshot, snapshot_call[2], snapshot_call[3])
                    )
                    clock["value"] = manifest["availability_cutoff"]
                    phases.clear()
                    phase_calls.clear()
                    gc_receipt = {}
                    rows = profile_retained_call(diagnostic_call, gc_receipt=gc_receipt)
                    emit(
                        {
                            "kind": "post_measurement_profile",
                            "stage": diagnostic_call[1].__name__,
                            "rows": rows,
                            "garbage_collection": gc_receipt,
                            "inclusive_nonadditive_phase_seconds": dict(phases),
                            "phase_call_counts": dict(phase_calls),
                            "snapshot": await profile_retained_async(snapshot_call),
                            "cold_snapshot": cold_profile,
                        }
                    )
                    continue
                state = command["sample"]
                diagnostic_seconds = 0.0
                phases.clear()
                phase_calls.clear()
                compute_seconds.clear()
                worker_receipts.clear()
                stop, admitted = asyncio.Event(), asyncio.Event()
                enqueue, finish = repo.enqueue, repo.finish_job

                async def observe_enqueue(kind, *args, **kwargs):
                    value = await enqueue(kind, *args, **kwargs)
                    if kind == "RUN_MARKET_TRUTH":
                        admitted.set()
                    return value

                async def observe_finish(job_id):
                    await finish(job_id)
                    if (await repo.get("ops.jobs", job_id))["type"] == "RUN_MARKET_TRUTH":
                        stop.set()

                repo.enqueue, repo.finish_job = observe_enqueue, observe_finish
                if state == "cold":
                    clear_timestamp_cache()
                assert sys.getprofile() is None
                started, cpu = perf_counter(), process_time()
                caller = worker = None
                try:
                    async with asyncio.timeout(90):
                        caller = asyncio.create_task(
                            sessions.tool(
                                seeded[state],
                                {
                                    "name": "get_market_truth",
                                    "id": state,
                                    "input": {
                                        "token_ids": [TOKEN],
                                        "cutoff": manifest["semantic_cutoff"],
                                        "cutoff_prec": "BUCKET(1m)",
                                    },
                                },
                            )
                        )
                        await admitted.wait()
                        worker = asyncio.create_task(runtime.worker(stop))
                        try:
                            await caller
                        except Exception as error:
                            if errors:
                                raise ReturnBoundaryError(errors) from error
                            raise
                        await worker
                    wall, used = perf_counter() - started, process_time() - cpu
                finally:
                    stop.set()
                    for task in (caller, worker):
                        if task and not task.done():
                            task.cancel()
                    await asyncio.gather(*(task for task in (caller, worker) if task), return_exceptions=True)
                    repo.enqueue, repo.finish_job = enqueue, finish
                assert not errors, errors
                assert len(worker_receipts) == len(compute_seconds) == 1
                worker_receipt = worker_receipts[0]
                baseline_before = worker_receipt["baseline_entries_before"]
                assert (baseline_before == 0) == (state == "cold")
                assert worker_receipt["baseline_present_before"] == (state != "cold")
                assert worker_receipt["baseline_present_after"]
                assert worker_receipt["baseline_entries_after"] > 0
                assert compute_seconds[0] > 0
                audit = await repo.get("ops.tool_calls", f"s-{state}:{state}")
                assert audit["returned_at"] and audit["remote_event_id"] == "accepted-" + state
                result = audit["result"]["market_truth"][0]
                assert await repo.get("ana.market_truth", result["mta_id"]) == result
                jobs = await repo.list("ops.jobs", filters={"status": "DONE"}, limit=100)
                jobs = [job for job in jobs if job["payload"]["event_id"] == seeded[state]["event_id"]]
                assert sorted(job["type"] for job in jobs) == ["ESCALATE_TOKENS", "RUN_MARKET_TRUTH"]
                assert all(job["attempts"] == 1 for job in jobs)
                assert next(job for job in jobs if job["type"] == "RUN_MARKET_TRUTH")["result"] == result
                assert result["relative"]["ref_venue"] == "binance:spot:ENAUSDT"
                assert Decimal(result["relative"]["entry_ref_price"]) == Decimal("0.20290000")
                assert Decimal(result["relative"]["anchor"]["ref_price"]) == Decimal("0.20330000")
                assert result["detail"]["capture_status"]["status"] == "COMPLETE"
                assert result["assembled_at"] == ASSEMBLED and result["cutoff"] == manifest["semantic_cutoff"]
                value = snapshots[-1]
                assert len(snapshots) == (1 if state == "cold" else 2)
                assert value["series_coverage_counts"] == manifest["peer_coverage_counts"]
                assert {
                    key: len(value[key]) for key in ("flow_bars", "oi", "funding", "tickers", "supply")
                } == {"flow_bars": 4346, "oi": 57399, "funding": 3260, "tickers": 156, "supply": 764}
                assert sum(row["oi_usd"] is None for row in value["oi"]) == 25721
                assert value["books"] == value["liquidations"] == []
                if state == "cold":
                    assert dense == {
                        (row["instrument_id"], row["field"]): row["rows"]
                        for row in manifest["dense_projections"]
                    }
                full = {key: item for key, item in result.items() if key not in {"mta_id", "code_version"}}
                snapshot_value = {key: item for key, item in value.items() if key != "code_version"}
                digests = {
                    name: save(directory, f"{state}-{name}.json", item)
                    for name, item in (
                        ("result", full),
                        ("projection", received[state]),
                        ("snapshot", snapshot_value),
                    )
                }
                emit(
                    {
                        "kind": "sample",
                        "state": state,
                        "wall_seconds": wall,
                        "process_cpu_seconds": used,
                        "digests": digests,
                        "baseline_entries_before": baseline_before,
                        "compute_seconds": compute_seconds[-1],
                        "compute_cpu_seconds": worker_receipt["compute_cpu_seconds"][0],
                        "compute_worker": worker_receipt,
                        "inclusive_nonadditive_phase_seconds": dict(phases),
                        "phase_call_counts": dict(phase_calls),
                        "modules": provenance(root),
                    }
                )
    finally:
        server.close()
        pending_connections = tuple(connections)
        for task in pending_connections:
            task.cancel()
        await asyncio.gather(*pending_connections, return_exceptions=True)
        await server.wait_closed()
        await repo.clickhouse.close()
        await repo.close()
    emit({"kind": "stopped"})


class ReturnBoundaryError(Exception):
    pass


def failure_receipt(error):
    frames, trace = [], error.__traceback__
    while trace:
        frames.append([Path(trace.tb_frame.f_code.co_filename).name, trace.tb_lineno])
        trace = trace.tb_next
    result = {
        "failure": type(error).__name__,
        "frames": frames if len(frames) <= 16 else frames[:8] + frames[-8:],
        "frames_omitted": max(0, len(frames) - 16),
    }
    if type(error) is ReturnBoundaryError:
        result["return_boundary_failures"] = error.args[0]
    return result


async def main():
    root, directory, fixtures = (Path(value).resolve() for value in sys.argv[1:4])
    loop, task = asyncio.get_running_loop(), asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    reader = asyncio.StreamReader()
    transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    logging.getLogger().addHandler(logging.NullHandler())
    try:
        await run(root, directory, fixtures, sys.argv[4], reader)
    except BaseException as error:
        emit(failure_receipt(error))
        raise SystemExit(1) from None
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import cProfile
import hashlib
import json
import logging
import pstats
import re
import signal
import sys
from contextlib import suppress
from datetime import datetime
from decimal import Decimal
from functools import wraps
from pathlib import Path
from time import perf_counter, process_time
from urllib.parse import urlsplit

import httpx
from market_truth_request_data import seed_data

from tank import clickhouse_market as chm
from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.analysis import truth
from tank.clickhouse_market import ClickHouse
from tank.event_tools import EventToolService
from tank.market import service as market_module
from tank.market.service import MarketService
from tank.runtime import Runtime
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
            errors.append(type(error).__name__)
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            connections.discard(asyncio.current_task())

    server = await asyncio.start_server(receive_http, "127.0.0.1", 0)
    try:
        async with httpx.AsyncClient(trust_env=False) as http:
            settings = Settings(
                _env_file=None,
                config_dir=root / "config",
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
            runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate, "RUN_MARKET_TRUTH": runtime.assess}
            snapshots, dense, phases, phase_calls = [], {}, {}, {}
            original_snapshot, original_heads, original_rows = (
                market.snapshot,
                market._snapshot_heads,
                repo.clickhouse.rows,
            )

            def heads(*args):
                original_heads(*args)
                clock["value"] = ASSEMBLED

            async def snapshot(*args, **kwargs):
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

            profiler = cProfile.Profile() if hasattr(chm, "dense_projection_page") else None
            profiled, profiling = set(), False
            profile_roots = {
                "derivatives_truth",
                "support_features",
                "enrich_evidence",
                "timestamp_companions",
                "bounded_market_snapshot",
                "liquidity_truth",
                "supply_truth",
                "data_confidence",
            }

            def measure(name, function):
                @wraps(function)
                def measured(*args, **kwargs):
                    nonlocal profiling
                    started = perf_counter()
                    selected = (
                        profiler is not None
                        and name in profile_roots
                        and name not in profiled
                        and not profiling
                    )
                    if selected:
                        profiled.add(name)
                        profiling = True
                        profiler.enable()
                    try:
                        return function(*args, **kwargs)
                    finally:
                        if selected:
                            profiler.disable()
                            profiling = False
                        phases[name] = phases.get(name, 0) + perf_counter() - started
                        phase_calls[name] = phase_calls.get(name, 0) + 1

                return measured

            for name in (
                "relative_truth",
                "derivatives_truth",
                "support_features",
                "spot_truth",
                "data_confidence",
                "_price_groups",
                "_oi_rows",
                "_oi_stats",
                "_synchronized_oi",
                "_composite_history",
                "_oi_history_returns",
                "_composite_history_vector",
                "_relative_history_arrays",
                "_relative_history_returns",
                "btc_baseline",
                "enrich_evidence",
                "timestamp_companions",
                "bounded_market_snapshot",
                "liquidity_truth",
                "supply_truth",
                "available_field_freshness",
            ):
                if hasattr(truth, name):
                    setattr(truth, name, measure(name, getattr(truth, name)))
            original_compute = truth.compute_market_truth
            compute_seconds = []

            def compute(*args, **kwargs):
                started = perf_counter()
                try:
                    return original_compute(*args, **kwargs)
                finally:
                    compute_seconds.append(perf_counter() - started)

            truth.compute_market_truth = compute
            while command := await reader.readline():
                command = json.loads(command)
                if command.get("stop"):
                    break
                state = command["sample"]
                phases.clear()
                phase_calls.clear()
                baseline_before = len(service._assessment_baselines.get(TOKEN, {}))
                assert (baseline_before == 0) == (state == "cold")
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
                        await caller
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
                        "inclusive_nonadditive_phase_seconds": dict(phases),
                        "phase_call_counts": dict(phase_calls),
                        "diagnostic_profiled_first_roots": sorted(profiled) if state == "cold" else [],
                        "diagnostic_profile": [
                            {
                                "file": Path(key[0]).name,
                                "line": key[1],
                                "function": key[2],
                                "calls": value[1],
                                "self_seconds": value[2],
                                "inclusive_seconds": value[3],
                            }
                            for key, value in sorted(
                                pstats.Stats(profiler).stats.items(),
                                key=lambda item: item[1][3],
                                reverse=True,
                            )[:40]
                        ] if profiler is not None and state == "cold" else [],
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
        frames, trace = [], error.__traceback__
        while trace:
            frames.append([Path(trace.tb_frame.f_code.co_filename).name, trace.tb_lineno])
            trace = trace.tb_next
        emit({"failure": type(error).__name__, "frames": frames[-8:]})
        raise SystemExit(1) from None
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())

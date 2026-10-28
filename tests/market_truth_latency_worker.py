import asyncio
import gzip
import hashlib
import inspect
import json
import logging
import os
import re
import shutil
import signal
import sys
from collections import Counter, defaultdict
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from decimal import Decimal, Inexact, Rounded, getcontext, localcontext
from functools import wraps
from importlib.metadata import version
from pathlib import Path
from time import perf_counter, process_time, thread_time
from urllib.parse import urlsplit

import httpx
import yaml
from postgres_helpers import PostgresTemplate, _finish
from psycopg.conninfo import conninfo_to_dict

import tank
from tank import clickhouse_market
from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.analysis import truth
from tank.event_tools import EventToolService
from tank.market import service as market_module
from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.settings import Settings
from tank.util import parse_time, sha256

TOKEN = "ethena:ENA"
REFERENCE = "binance:spot:ENAUSDT"
ASSEMBLED = "2026-10-28T12:08:24.291857Z"
PRIVATE_CONTENT = re.compile(
    rb"://|[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|-----BEGIN|"
    rb"(?:password|passwd|api_key|access_token|authorization|telegram_id|chat_id|user_id|email)\s*[\"=:]",
    re.IGNORECASE,
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def decimal_state():
    context = getcontext()
    return {
        "precision": context.prec,
        "rounding": context.rounding,
        "Emin": context.Emin,
        "Emax": context.Emax,
        "capitals": context.capitals,
        "clamp": context.clamp,
        "flags": {signal.__name__: value for signal, value in context.flags.items()},
        "traps": {signal.__name__: value for signal, value in context.traps.items()},
    }


def public_nested(value, fields):
    if isinstance(value, dict):
        assert set(value) <= fields, "Unexpected nested public fixture field"
        for child in value.values():
            public_nested(child, fields)
    elif isinstance(value, list):
        for child in value:
            public_nested(child, fields)


def retained_rows(directory, entry, policy):
    archive = (directory / entry["name"]).read_bytes()
    assert hashlib.sha256(archive).hexdigest() == policy["ARCHIVE_HASHES"][entry["name"]]
    assert hashlib.sha256(archive).hexdigest() == entry["archive_sha256"]
    assert len(archive) == entry["compressed_bytes"]
    payload = gzip.decompress(archive)
    assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
    assert len(payload) == entry["bytes"] and not PRIVATE_CONTENT.search(payload)
    rows = [json.loads(line) for line in payload.splitlines()]
    assert len(rows) == entry["rows"]
    family = (
        "PRICE_FIELDS" if "price" in entry["name"] else "OI_FIELDS" if "-oi." in entry["name"] else "FLOW_FIELDS"
    )
    assert set(entry["columns"]) == set(policy[family])
    for row in rows:
        assert set(row) == set(policy[family]), "Unexpected public fixture field"
        if family == "FLOW_FIELDS":
            for field in ("first_trade", "last_trade", "boundary_segments", "reference_points"):
                public_nested(
                    json.loads(row[field]) if row[field] is not None else None, set(policy["NESTED_FIELDS"])
                )
    return rows


class Measurements:
    def __init__(self):
        self.active = False
        self.phases = []
        self.inventory = []
        self.decimals = []
        self.phase_decimals = []
        self.series = []
        self.snapshot = None
        self.snapshot_invariants = None
        self.query_inventory = []
        self.executor_dispatch = []
        self.executors = []
        self.cleanup = {}

    def record(self, phase, started, cpu, thread_cpu):
        self.phases.append(
            {
                "phase": phase,
                "wall_seconds": perf_counter() - started,
                "process_cpu_seconds": process_time() - cpu,
                "calling_thread_cpu_seconds": thread_time() - thread_cpu,
            }
        )

    def instrument(self, owner, name, phase):
        original = getattr(owner, name)

        @wraps(original)
        async def asynchronous(*args, **kwargs):
            if not self.active:
                return await original(*args, **kwargs)
            started, cpu, thread_cpu = perf_counter(), process_time(), thread_time()
            try:
                result = await original(*args, **kwargs)
            finally:
                self.record(phase, started, cpu, thread_cpu)
            if name == "_condition_kline_series":
                instrument = args[0]
                self.series.append((instrument["instrument_id"], kwargs["kind"], result))
            if name == "rows":
                query = args[0]
                collections = sorted(set(re.findall(r"\bmkt\.[a-z0-9_]+", query)))
                self.query_inventory.append(
                    {
                        "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
                        "collections": collections,
                        "rows": len(result),
                        "select_kind": "FULL_TYPED" if query.startswith("SELECT * ") else "PROJECTION_OR_INDEX",
                    }
                )
            return result

        @wraps(original)
        def synchronous(*args, **kwargs):
            if not self.active:
                return original(*args, **kwargs)
            before = (
                decimal_state()
                if name in {"compute_market_truth", "relative_truth", "derivatives_truth"}
                else None
            )
            started, cpu, thread_cpu = perf_counter(), process_time(), thread_time()
            try:
                return original(*args, **kwargs)
            finally:
                self.record(phase, started, cpu, thread_cpu)
                if before is not None:
                    after = decimal_state()
                    if name == "compute_market_truth":
                        self.decimals.append({"before": before, "after": after})
                        assert before == after, "Full compute changed its caller's Decimal context"
                    else:
                        self.phase_decimals.append({"phase": name, "before": before, "after": after})
                        assert before["precision"] >= 128, "Full compute lost adaptive Decimal precision"

        setattr(owner, name, asynchronous if inspect.iscoroutinefunction(original) else synchronous)

    def summary(self):
        output = defaultdict(
            lambda: {
                "calls": 0,
                "wall_seconds": 0,
                "process_cpu_seconds": 0,
                "calling_thread_cpu_seconds": 0,
            }
        )
        for phase in self.phases:
            aggregate = output[phase["phase"]]
            aggregate["calls"] += 1
            for field in ("wall_seconds", "process_cpu_seconds", "calling_thread_cpu_seconds"):
                aggregate[field] += phase[field]
        return dict(output)


async def seed_market(repo, directory, fixtures, policy, manifest):
    started, cpu = perf_counter(), process_time()
    registry = yaml.safe_load((directory / "registry.yaml").read_text())
    entries = {entry["name"]: entry for entry in manifest["files"]}
    assert set(entries) == set(policy["ARCHIVE_HASHES"])
    instruments = {row["instrument_id"]: row for row in registry["instruments"]}
    admitted = {f"binance:spot:{symbol}USDT" for symbol in ("ENA", "BTC", "ETH")}
    inventory = []
    expected_spot = {}
    for name, entry in entries.items():
        rows = retained_rows(fixtures, entry, policy)
        if "price" in name:
            collection = "mkt.klines_1m"
            instrument = instruments[f"binance:spot:{entry['symbol']}USDT"]
            for row in rows:
                row.pop("_cursor_time")
                row["storage_key"] = row.pop("_cursor_key")
                row.update(
                    {field: instrument[field] for field in ("instrument_id", "token_id", "venue", "market_type")}
                )
        elif "flow" in name:
            collection = "mkt.flow_bars_1m"
            end = parse_time(manifest["semantic_cutoff"])
            selected = [
                row
                for row in rows
                if end - timedelta(hours=1) <= parse_time(row["minute_start"])
                and parse_time(row["minute_start"]) + timedelta(minutes=1) <= end
            ]
            assert len(selected) == 60 and all(row["complete"] is True for row in selected)
            with localcontext() as context:
                context.prec = 128
                expected_spot = {
                    "buy_qty": str(sum((Decimal(row["buy_qty"]) for row in selected), Decimal(0))),
                    "sell_qty": str(sum((Decimal(row["sell_qty"]) for row in selected), Decimal(0))),
                }
        else:
            collection = "mkt.oi_obs"
            admitted.update(row["instrument_id"] for row in rows)
        counts = Counter(row["instrument_id"] for row in rows)
        for index, row in enumerate(rows):
            row.setdefault("storage_key", f"retained:{name}:{index}")
            row["_version"] = 1
        for offset in range(0, len(rows), 10000):
            await repo.clickhouse.insert(collection, rows[offset : offset + 10000])
        inventory.append(
            {
                "name": name,
                "collection": collection,
                "rows": len(rows),
                "by_instrument": dict(counts),
                "sha256": entry["sha256"],
                "archive_sha256": entry["archive_sha256"],
                "columns": entry["columns"],
            }
        )
    assert len(admitted) == 7
    controls = []
    for symbol in ("ENA", "BTC", "ETH"):
        instrument = instruments[f"binance:spot:{symbol}USDT"]
        controls.append(
            {
                "storage_key": "synthetic-future-exclusion:" + instrument["instrument_id"],
                "_version": 1,
                **{field: instrument[field] for field in ("instrument_id", "token_id", "venue", "market_type")},
                "open_time": parse_time(manifest["semantic_cutoff"]).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "open_time_prec": "BUCKET(60s)",
                "recorded_at": parse_time(manifest["availability_cutoff"]).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "interval_s": 60,
                "complete": True,
                "c": "999999999.00000000000000000000000000001",
            }
        )
    await repo.clickhouse.insert("mkt.klines_1m", controls)
    for row in registry["tokens"]:
        if row["token_id"] in {"ethena:ENA", "bitcoin:BTC", "ethereum:ETH"}:
            await repo.put("reg.tokens", row["token_id"], row)
    for row in registry["instruments"]:
        if row["instrument_id"] in admitted:
            await repo.put("reg.instruments", row["instrument_id"], row)
    await repo.put("reg.sources", "S01", {"source_id": "S01", "name": "Public retained replay fixture"})
    for collection, total in (("mkt.klines_1m", 389163), ("mkt.flow_bars_1m", 1444), ("mkt.oi_obs", 57399)):
        counted = await repo.clickhouse.rows(f"SELECT count() AS n FROM {collection} FINAL")
        assert counted == [{"n": total}], "Seeded rows were lost or deduplicated"
    return {
        "wall_seconds": perf_counter() - started,
        "process_cpu_seconds": process_time() - cpu,
        "inputs": inventory,
        "qualification": "REDUCED_INPUT_DIAGNOSTIC_NOT_FULL_ENA_ACCEPTANCE",
        "historical_original_dense_projection_rows": 785571,
        "retained_dense_projection_rows": 389160,
        "retained_dense_projection_dimensions": 3,
        "historical_dense_projection_dimensions": 16,
        "synthetic_controls": {
            "collection": "mkt.klines_1m",
            "rows": 3,
            "purpose": "Explicit next-minute exclusion sentinels, NOT captured historical data",
        },
        "expected_spot": expected_spot,
        "registry_sha256": digest(directory / "registry.yaml"),
        "semantics_sha256": digest(directory / "semantics.yaml"),
        "thresholds_sha256": digest(directory / "thresholds.yaml"),
        "missing_dimensions": [
            "90-day native-volume histories for every ENA instrument",
            "non-Binance ENA spot price histories",
            "ENA perpetual price histories",
            "funding",
            "books",
            "supply",
            "liquidations",
            "live capture and repair",
            "original row versions",
        ],
        "loader": "Direct sparse typed ClickHouse fixture insertion, not collector ingestion. Missing OHLC/volume fields remain NULL. No schema, required-field policy, registry semantics or product configuration is relaxed.",
    }


async def seed_session(repo, state, anchor):
    event_id, sid = "latency-" + state, "s-" + state
    assessment_id = "ems-" + state
    await repo.put(
        "evt.events",
        event_id,
        {
            "event_id": event_id,
            "statement": "Public retained ENA workload",
            "theme_tags": [],
            "entities": [],
            "tokens_mentioned": [TOKEN],
            "public_at": anchor,
            "public_at_prec": "EXACT",
            "public_at_basis": "Seeded public-data benchmark boundary",
            "known_to_us_at": anchor,
            "known_to_us_at_prec": "EXACT",
            "event_state": "APPROVED",
            "state_verified": True,
            "certainty": "HIGH",
            "timing_confidence": "HIGH",
            "best_source_class": "PRIMARY",
            "freshness": "FRESH",
            "lifecycle": "MONITORING",
            "config_version": "fixture",
            "code_version": "fixture",
            "current_ems_id": assessment_id,
        },
    )
    await repo.put(
        "evt.ems_assessments",
        assessment_id,
        {
            "ems_id": assessment_id,
            "event_id": event_id,
            "version": 1,
            "cutoff": anchor,
            "class": "MAJOR",
            "criterion": "M2",
            "criterion_claim_id": None,
            "dims": {
                name: {
                    "value": None,
                    "reason": "NOT_ESTIMABLE",
                    "rationale": "Seeded replay",
                    "evidence": [],
                    "proposed_by": "LLM",
                    "caps_applied": [],
                }
                for name in (
                    "economic_directness",
                    "magnitude",
                    "permanence",
                    "distribution_expansion",
                    "mechanical_flows",
                    "surprise",
                    "implementation",
                )
            },
            "impact_horizon": {
                "impact_start": "WEEKS",
                "ramp_shape": "GRADUAL",
                "dependencies": [],
                "time_to_materiality": "Unknown",
                "timing_confidence": "LOW",
            },
            "structural_gates": [],
            "class_reasons": ["Seeded MAJOR for ready workload"],
            "config_version": "fixture",
            "code_version": "fixture",
            "model_id": "fixture-model",
            "prompt_version": "fixture-prompt",
        },
    )
    await repo.put(
        "ingest.source_items",
        "item-" + state,
        {
            "item_id": "item-" + state,
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
            "raw_sha256": sha256("Public retained ENA workload"),
            "content_hash": sha256("Public retained ENA workload"),
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        },
    )
    session = {
        "session_id": sid,
        "event_id": event_id,
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "opened_by_item": "item-" + state,
        "opened_at": anchor,
        "opened_at_prec": "COLLECTION_ONLY",
        "last_item_at": anchor,
        "last_item_at_prec": "COLLECTION_ONLY",
        "last_event_seq": 0,
        "agent_version": "1",
        "prompt_version": "a" * 64,
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "model_id": "fixture-model",
        "active_message_id": "m",
        "message_log": [
            {
                "message_id": "m",
                "remote_event_id": "input",
                "retry_count": 0,
                "payload": {"kind": "NEW_ITEM", "item_id": "item-" + state},
            }
        ],
        "model_context_version": CONTEXT_VERSION,
    }
    await repo.put("ops.agent_sessions", sid, session)
    backfill_id = "BF-" + sha256({"event_id": event_id, "token_id": TOKEN, "anchor": anchor})
    await repo.put(
        "mkt.backfill_runs",
        backfill_id,
        {
            "backfill_id": backfill_id,
            "event_id": event_id,
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
    return session, backfill_id


async def load_native_day_counts(market, fixtures, manifest):
    entry = manifest.get("native_day_counts")
    if entry is None:
        return {"state": "COLD_NATIVE_DAY_COUNTS", "restored": 0}
    archive = (fixtures / entry["name"]).read_bytes()
    assert hashlib.sha256(archive).hexdigest() == entry["archive_sha256"]
    assert len(archive) == entry["compressed_bytes"]
    payload = gzip.decompress(archive) if entry["name"].endswith(".gz") else archive
    assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
    assert len(payload) == entry["bytes"] and not PRIVATE_CONTENT.search(payload)
    saved = json.loads(payload)
    assert set(saved) == {"version", "saved_at", "counts"} and saved["version"] == 1
    parse_time(saved["saved_at"])
    assert len(saved["counts"]) == entry["rows"]
    admitted = set(entry["instrument_ids"])
    assert len(admitted) == 81
    for instrument_id, day, count in saved["counts"]:
        assert instrument_id in admitted and instrument_id in market.instruments
        assert parse_time(day).hour == parse_time(day).minute == parse_time(day).second == 0
        assert isinstance(count, int) and not isinstance(count, bool) and count >= 0
    path = market._day_counts_path()
    assert not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    restored = await market._load_day_counts()
    assert 0 < restored <= len(saved["counts"]), "Real cache load restored no eligible day aggregates"
    return {
        "state": "WARM_NATIVE_DAY_COUNTS_COLD_DENSE_AND_ANALYSIS",
        "restored": restored,
        "input_rows": len(saved["counts"]),
        "sha256": entry["sha256"],
        "saved_at": saved["saved_at"],
        "semantics": "Actual captured aggregates loaded by the unchanged native disk-cache API and its SQL invalidation; NOT all application caches cold",
    }


@asynccontextmanager
async def receiver(repo, directory, meter):
    pending, receipts, errors = set(), {}, []

    async def receive(reader, writer):
        task = asyncio.current_task()
        pending.add(task)
        try:
            async with asyncio.timeout(10):
                lines = (await reader.readuntil(b"\r\n\r\n")).decode("ascii").split("\r\n")
                match = re.fullmatch(r"POST /v1/sessions/(s-(?:cold|warm))/events HTTP/1.1", lines[0])
                assert match
                headers = {
                    key.lower(): value.strip()
                    for line in lines[1:]
                    if line
                    for key, value in [line.split(":", 1)]
                }
                assert headers["content-type"] == "application/json"
                assert headers["anthropic-beta"] == "managed-agents-2026-04-01"
                size = int(headers["content-length"])
                assert 0 < size < 8_000_000
                payload = await reader.readexactly(size)
                body = json.loads(payload)
                assert set(body) == {"events"} and len(body["events"]) == 1
                native = body["events"][0]
                call_id, sid = native["custom_tool_use_id"], match[1]
                assert call_id in {"cold", "warm"} and sid == "s-" + call_id
                assert native["type"] == "user.custom_tool_result" and "is_error" not in native
                assert headers["idempotency-key"] == "tool:" + sha256(sid + ":" + call_id)
                audit = await repo.get("ops.tool_calls", sid + ":" + call_id)
                assert audit["status"] == "DONE" and audit["native_result_body"] == native
                assert audit.get("returned_at") is None
                result = audit["result"]["market_truth"][0]
                assert await repo.get("ana.market_truth", result["mta_id"]) == result
                path = directory / (call_id + "-http.json")
                with path.open("wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                receipts[call_id] = {
                    "bytes": size,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "wall_until_durable_receipt_seconds": perf_counter() - meter.request_started,
                }
                response = json.dumps({"data": [{"id": "protocol-" + call_id}]}).encode()
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                    + str(len(response)).encode()
                    + b"\r\n\r\n"
                    + response
                )
                await writer.drain()
        except Exception as error:
            errors.append(type(error).__name__)
            writer.write(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            with suppress(ConnectionError):
                await writer.drain()
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            pending.discard(task)

    server = await asyncio.start_server(receive, "127.0.0.1", 0)
    try:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}", receipts, errors
    finally:
        server.close()
        await server.wait_closed()
        for task in tuple(pending):
            task.cancel()
        if pending:
            await asyncio.gather(*tuple(pending), return_exceptions=True)


def assert_snapshot(snapshot, manifest):
    series = snapshot["stat_price_series"]
    cutoff = parse_time(manifest["semantic_cutoff"])
    epoch = parse_time("1970-01-01T00:00:00Z")
    retained = {"binance:spot:ENAUSDT", "binance:spot:BTCUSDT", "binance:spot:ETHUSDT"}
    assert retained <= set(series)
    for key, value in series.items():
        if key not in retained:
            assert not value["times_us"], "Unexpected unretained price history"
            continue
        assert len(value["times_us"]) == len(value["values"]) == 129600
        assert all(value["complete"])
        assert value["precision"] is None and value["precision_reason"] == "INCOMPLETE"
        assert value["time_basis"] == "CLOSE_TIME"
        assert epoch + timedelta(microseconds=value["times_us"][-1]) == cutoff
        first_close = cutoff - timedelta(days=90) + timedelta(minutes=1)
        assert epoch + timedelta(microseconds=value["times_us"][0]) == first_close
        assert all(price < 999999999 for price in value["values"])
    assert all(parse_time(row["open_time"]) + timedelta(minutes=1) <= cutoff for row in snapshot["klines"])
    assert all(parse_time(row["minute_start"]) + timedelta(minutes=1) <= cutoff for row in snapshot["flow_bars"])
    assert all(parse_time(row["ts"]) <= cutoff for row in snapshot["oi"])
    assert len(snapshot["flow_bars"]) == 1444
    assert len(snapshot["oi"]) == 57399
    assert all(parse_time(row["recorded_at"]) <= parse_time(ASSEMBLED) for row in snapshot["oi"])
    assert snapshot["funding"] == snapshot["books"] == snapshot["supply"] == snapshot["liquidations"] == []
    return {
        "price_points": {key: len(value["values"]) for key, value in series.items()},
        "volume_points": {
            key: {"rows": len(value["values"]), "valid_rows": sum(value["complete"])}
            for key, value in snapshot["stat_volume_series"].items()
        },
        "collections": {
            key: len(snapshot[key])
            for key in ("flow_bars", "klines", "oi", "funding", "books", "supply", "liquidations")
        },
        "oi_by_instrument": dict(Counter(row["instrument_id"] for row in snapshot["oi"])),
        "cutoff_exclusions": "ALL_KLINES_AND_FLOW_CLOSE_AT_OR_BEFORE_CUTOFF_AND_OI_AT_OR_BEFORE_CUTOFF",
    }


def assert_result(result, manifest, expected_spot):
    assert result["token_id"] == TOKEN and result["cutoff"] == manifest["semantic_cutoff"]
    assert result["cutoff_prec"] == result["requested_cutoff_prec"] == "BUCKET(1m)"
    assert result["requested_cutoff"] == manifest["semantic_cutoff"] and result["assembled_at"] == ASSEMBLED
    assert result["assembled_at_prec"] == "COLLECTION_ONLY"
    assert result["time_policy"] == "SEMANTIC_MARKET_TIME_WITH_ASSEMBLY_AVAILABILITY"
    assert result["detail"]["capture_status"]["status"] == "COMPLETE"
    assert result["detail"]["prerequisite_pending"] is False
    assert result["measurement_metadata"]
    relative = result["relative"]
    assert relative["ref_venue"] == REFERENCE and relative["reference_reason"] is None
    assert Decimal(relative["entry_ref_price"]) == Decimal("0.20290000")
    assert Decimal(relative["anchor"]["ref_price"]) == Decimal("0.20330000")
    assert relative["anchor"]["ts"] == manifest["anchor"]
    assert relative["entry_price_source"] == relative["anchor"]["price_source"] == "LAST_TRADE"
    assert relative["beta_flags"] == {"btc": None, "eth": None}
    assert set(relative["normal_ranges"]) == {"W1H", "W4H", "W12H", "W24H", "W3D", "W7D"}
    diagnostics = {}
    for window in list(relative["windows"].values()) + list(relative["normal_ranges"].values()):
        diagnostic = window["historical_measurement"]
        diagnostics[diagnostic["target_window_duration_s"]] = diagnostic
        assert diagnostic["resolution_s"] == 60
        assert 100000 <= diagnostic["sample_count"] <= 129720
        assert parse_time(diagnostic["latest_observed_at"]) < parse_time(manifest["anchor"])
    assert len(diagnostics) == 8
    assert 1000000 <= sum(value["sample_count"] for value in diagnostics.values()) <= 1050000
    spot = result["spot"]["W1H"]["venues"]
    assert len(spot) == 1
    with localcontext() as context:
        context.prec = 128
        buy, sell = Decimal(expected_spot["buy_qty"]), Decimal(expected_spot["sell_qty"])
        assert Decimal(spot[0]["buy_qty"]) == buy and Decimal(spot[0]["sell_qty"]) == sell
        assert Decimal(spot[0]["cvd"]) == buy - sell and Decimal(spot[0]["volume_native"]) == buy + sell
    windows = result["derivatives"]["windows"]
    assert set(windows) == {"W15M", "W1H", "W4H", "W24H", "EVENT"}
    assert windows["W1H"]["synchronized"] is not None
    assert set(windows["W1H"]["synchronized"]["venues"]) == {"binance", "okx", "bybit"}
    for value in windows.values():
        assert {row["venue"] for row in value["per_venue"]} == {"binance", "bybit", "okx", "hyperliquid"}
        hyperliquid = next(row for row in value["per_venue"] if row["venue"] == "hyperliquid")
        assert hyperliquid["oi_native_start"] is None and hyperliquid["oi_class"] is None
    return diagnostics


def assert_expected_components(result, expected):
    def compare(actual, original):
        if isinstance(original, dict):
            assert isinstance(actual, dict)
            for key, value in original.items():
                if key.endswith(("_prec", "_prec_reason")) or key == "precision":
                    continue
                compare(actual[key], value)
        elif isinstance(original, list):
            assert isinstance(actual, list) and len(actual) == len(original)
            for left, right in zip(actual, original, strict=True):
                compare(left, right)
        elif isinstance(original, float):
            assert actual is not None and abs(actual - original) <= max(1e-12, abs(original) * 1e-10)
        elif isinstance(original, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", original):
            assert actual is not None and Decimal(actual) == Decimal(original)
        else:
            assert actual == original

    for field in ("ref_venue", "beta_btc", "beta_eth", "normal_ranges", "market_context", "context_inputs"):
        compare(result["relative"][field], expected["relative"][field])
    for name, original in expected["oi_windows"].items():
        actual = result["derivatives"]["windows"][name]
        synchronized = original["synchronized"]
        if synchronized is None:
            assert actual["synchronized"] is None
        else:
            compare(
                actual["synchronized"],
                {
                    key: value
                    for key, value in synchronized.items()
                    if key not in {"decomposition", "coverage", "venues"}
                },
            )
            assert sorted(actual["synchronized"]["venues"]) == sorted(synchronized["venues"])
        by_instrument = {row["instrument_id"]: row for row in actual["per_venue"]}
        for row in original["per_venue"]:
            compare(by_instrument[row["instrument_id"]], row)


async def settle_owned(tasks):
    owned = tuple(task for task in tasks if task is not None)
    for task in owned:
        if not task.done():
            task.cancel()
    if owned:
        await _finish(asyncio.gather(*owned, return_exceptions=True))


async def finish_sample(repo, originals, tasks, stop, meter):
    primary = sys.exception()
    stop.set()
    try:
        try:
            await settle_owned(tasks)
        finally:
            if meter.executors:
                await _finish(asyncio.gather(*meter.executors, return_exceptions=True))
    except BaseException:
        if primary is None:
            raise
    finally:
        repo.enqueue, repo.finish_job = originals
        meter.cleanup = {
            "owned_tasks": sum(task is not None for task in tasks),
            "settled_tasks": sum(task is not None and task.done() for task in tasks),
            "executor_submissions": len(meter.executors),
            "executor_submissions_settled": sum(task.done() for task in meter.executors),
            "executor_cancellation": "Await cancellation does not stop synchronous work; tracked submissions are awaited before repository teardown",
            "hooks_restored": repo.enqueue == originals[0] and repo.finish_job == originals[1],
        }


async def serve_commands(reader, sample, emit):
    seen, active, command = [], None, asyncio.create_task(reader.readline())
    try:
        while True:
            waiting = (command,) if active is None else (command, active)
            await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            if command.done():
                line = command.result()
                operation = json.loads(line)["operation"] if line else "close"
                if operation == "close":
                    return
                assert active is None, "Overlapping samples are not permitted"
                assert operation == ("cold" if not seen else "warm") and len(seen) < 2
                active = asyncio.create_task(sample(operation))
                command = asyncio.create_task(reader.readline())
            if active is not None and active.done():
                result = active.result()
                emit(result)
                seen.append(result["state"])
                active = None
    finally:
        primary = sys.exception()
        try:
            await settle_owned((active, command))
        except BaseException:
            if primary is None:
                raise


async def cancel_safe_thread(meter, operation):
    task = asyncio.create_task(operation)
    meter.executors.append(task)
    return await asyncio.shield(task)


def safe_failure(error):
    roots = (
        (Path.cwd().resolve() / "src" / "tank", "src/tank"),
        (Path(__file__).resolve().parent, "tests"),
    )
    frames, trace = [], error.__traceback__
    while trace is not None:
        path = Path(trace.tb_frame.f_code.co_filename).resolve()
        for root, label in roots:
            if path.is_relative_to(root) and path.is_file():
                relative = str(path.relative_to(root))
                function = trace.tb_frame.f_code.co_name
                if re.fullmatch(r"[A-Za-z0-9_./<>-]{1,240}", relative) and re.fullmatch(
                    r"[A-Za-z0-9_<>]{1,100}", function
                ):
                    frames.append(
                        {
                            "file": label + "/" + relative,
                            "line": trace.tb_lineno,
                            "function": function,
                            "source_sha256": digest(path),
                        }
                    )
                break
        trace = trace.tb_next
    return {"type": type(error).__name__, "source_frames": frames[-16:]}


def emit_receipt(value):
    print(json.dumps(value, sort_keys=True), flush=True)


@asynccontextmanager
async def owned_repository(template, route, ownership, directory):
    context = template.repository(route)
    repo = await context.__aenter__()
    try:
        postgres = conninfo_to_dict(repo.database_url)["dbname"]
        clickhouse = repo.clickhouse.database
        assert re.fullmatch(r"tank_test_fixture_[a-f0-9]{32}", postgres)
        assert clickhouse == "mkt_" + postgres
        ownership.update(
            postgres_database=postgres,
            clickhouse_database=clickhouse,
            repository_cleanup="ACQUIRED_PENDING_CLEANUP",
        )
        (directory / "ownership.json").write_text(json.dumps(ownership, sort_keys=True))
        emit_receipt({"kind": "ownership", **ownership})
        yield repo
    finally:
        failure = sys.exc_info()
        try:
            await context.__aexit__(*failure)
        except BaseException as error:
            ownership["repository_cleanup"] = "NOT_CONFIRMED"
            ownership["cleanup_failure"] = safe_failure(error)
            if failure[1] is None:
                raise
        else:
            ownership["repository_cleanup"] = "CONTEXT_EXIT_COMPLETED"
        finally:
            (directory / "ownership.json").write_text(json.dumps(ownership, sort_keys=True))
            emit_receipt({"kind": "ownership", **ownership})


async def run_sample(
    state, repo, session, backfill, sessions, runtime, meter, receipts, errors, directory, manifest, setup, expected
):
    assert state not in receipts
    cache_before = len(runtime.truth._assessment_baselines.get(TOKEN, {}))
    if state == "cold":
        assert cache_before == 0
    else:
        assert cache_before > 0
    stop, admitted = asyncio.Event(), asyncio.Event()
    enqueue, finish = repo.enqueue, repo.finish_job

    async def observe_enqueue(kind, *args, **kwargs):
        job = await enqueue(kind, *args, **kwargs)
        if kind == "RUN_MARKET_TRUTH":
            admitted.set()
        return job

    async def observe_finish(job_id):
        await finish(job_id)
        row = await repo.get("ops.jobs", job_id)
        if row["type"] == "RUN_MARKET_TRUTH":
            stop.set()

    repo.enqueue, repo.finish_job = observe_enqueue, observe_finish
    meter.phases, meter.inventory, meter.decimals, meter.series = [], [], [], []
    meter.phase_decimals = []
    meter.query_inventory, meter.executor_dispatch = [], []
    meter.executors, meter.cleanup = [], {}
    caller = worker = admission = None
    with localcontext() as context:
        context.prec = 37
        context.clear_flags()
        context.flags[Inexact], context.flags[Rounded] = False, True
        decimal_before = decimal_state()
        meter.active = True
        meter.request_started, cpu, thread_cpu = perf_counter(), process_time(), thread_time()
        try:
            async with asyncio.timeout(90):
                caller = asyncio.create_task(
                    sessions.tool(
                        session,
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
                admission = asyncio.create_task(admitted.wait())
                await asyncio.wait((caller, admission), return_when=asyncio.FIRST_COMPLETED)
                if caller.done():
                    await caller
                assert admitted.is_set(), "Session/tool returned without admitting the full Market Truth job"
                worker = asyncio.create_task(runtime.worker(stop))
                await caller
                await worker
        finally:
            try:
                meter.record("request", meter.request_started, cpu, thread_cpu)
            finally:
                meter.active = False
                await finish_sample(repo, (enqueue, finish), (caller, worker, admission), stop, meter)
        assert decimal_state() == decimal_before, "Request changed its caller's Decimal context"
    assert not errors, errors
    audit = await repo.get("ops.tool_calls", session["session_id"] + ":" + state)
    assert audit["status"] == "DONE" and audit["is_error"] is False
    assert audit["returned_at"] is not None and audit["returned_at_prec"] == "COLLECTION_ONLY"
    assert audit["remote_event_id"] == "protocol-" + state
    result = audit["result"]["market_truth"][0]
    assert await repo.get("ana.market_truth", result["mta_id"]) == result
    diagnostics = assert_result(result, manifest, setup["expected_spot"])
    assert_expected_components(result, expected)
    assert any(row["backfill_id"] == backfill for row in result["detail"]["backfill_runs"])
    jobs = [
        row
        for row in await repo.list("ops.jobs", limit=100)
        if row["payload"]["event_id"] == session["event_id"]
    ]
    assert Counter(row["type"] for row in jobs) == {"ESCALATE_TOKENS": 1, "RUN_MARKET_TRUTH": 1}
    assert all(row["status"] == "DONE" and row["attempts"] == 1 for row in jobs)
    assessment = next(row for row in jobs if row["type"] == "RUN_MARKET_TRUTH")
    escalation = next(row for row in jobs if row["type"] == "ESCALATE_TOKENS")
    assert assessment["payload"]["_depends_on"] == [escalation["job_id"]]
    assert assessment["result"] == result
    assert await repo.list("mkt.capture_requests") == []
    wire = json.loads((directory / (state + "-http.json")).read_bytes())
    assert wire["events"] == [audit["native_result_body"]]
    native = wire["events"][0]
    assert len(native["content"]) == 1 and native["content"][0]["type"] == "text"
    projected = json.loads(native["content"][0]["text"])
    assert projected["requested_cutoff"] == manifest["semantic_cutoff"]
    assert projected["requested_cutoff_prec"] == "BUCKET(1m)"
    assert len(projected["market_truth"]) == 1
    projected_truth = projected["market_truth"][0]
    assert "mta_id" not in projected_truth and "code_version" not in projected_truth
    assert projected_truth["relative"]["entry_ref_price"] == result["relative"]["entry_ref_price"]
    assert (
        projected_truth["spot"]["W1H"]["venues"][0]["buy_qty"]
        == result["spot"]["W1H"]["venues"][0]["buy_qty"]
    )
    full = dict(result)
    full.pop("mta_id")
    full.pop("code_version")

    for name, output in (("result", full), ("projection", projected)):
        (directory / f"{state}-{name}.json").write_text(json.dumps(output, sort_keys=True))
    phases = meter.summary()
    meter.snapshot_invariants = assert_snapshot(meter.snapshot, manifest)
    meter.inventory = [
        {
            "instrument_id": instrument,
            "kind": kind,
            "rows": len(value["times_us"]),
            "valid_rows": sum(value["complete"]),
            "precision": value.get("precision"),
            "precision_reason": value.get("precision_reason"),
            "series_sha256": hashlib.sha256(
                value["times_us"].tobytes()
                + (value["values"].tobytes() if kind == "price" else json.dumps(value["values"]).encode())
            ).hexdigest(),
        }
        for instrument, kind, value in meter.series
    ]
    assert phases["compute"]["calls"] == phases["snapshot"]["calls"] == 1
    assert phases["compute.derivatives"]["calls"] == 5
    assert phases["compute.relative"]["calls"] == 1
    assert phases["compute.support_features"]["calls"] == 5
    assert phases["compute.timestamp_companions"]["calls"] == 1
    assert phases["compute.jsonable"]["calls"] >= 1
    assert meter.decimals and all(item["before"]["precision"] == 37 for item in meter.decimals)
    return {
        "kind": "sample",
        "state": state,
        "pid": os.getpid(),
        "request": phases["request"],
        "phases_inclusive_not_additive": phases,
        "phase_semantics": "Unprofiled same-boundary delegating wrappers on both versions. "
        "Process CPU includes all child threads; calling-thread CPU for async phases is event-loop CPU, "
        "not awaited worker CPU. PostgreSQL/ClickHouse server CPU is not included. "
        "Nested/overlapping phases must not be summed.",
        "cache": {
            "baseline_entries_before": cache_before,
            "baseline_entries_after": len(runtime.truth._assessment_baselines.get(TOKEN, {})),
        },
        "snapshot_invariants": meter.snapshot_invariants,
        "series_requests": meter.inventory,
        "query_inventory": meter.query_inventory,
        "executor_dispatch": meter.executor_dispatch,
        "decimal_semantics": {
            "request_context": decimal_before,
            "full_compute": meter.decimals,
            "full_compute_phases": meter.phase_decimals,
        },
        "historical_measurements": diagnostics,
        "receipt": receipts[state],
        "cleanup": meter.cleanup,
        "result_sha256": digest(directory / f"{state}-result.json"),
        "projection_sha256": digest(directory / f"{state}-projection.json"),
        "durability": "SQL job/result/tool-call equality checked before HTTP200; response receipt fsynced; returned audit read back",
    }


def imported_sources(root):
    modules = {}
    for name, module in tuple(sys.modules.items()):
        if name == "tank" or name.startswith("tank."):
            path = Path(module.__file__).resolve()
            assert path.is_relative_to(root / "src" / "tank"), "Mixed original/candidate product imports"
            modules[name] = {"file": str(path.relative_to(root)), "sha256": digest(path)}
    assert Path(tank.__file__).resolve().is_relative_to(root / "src" / "tank")
    return modules


async def run_worker(ownership):
    setup_started, setup_cpu = perf_counter(), process_time()
    root, directory, fixtures, policy_path = map(Path, sys.argv[1:5])
    revision = sys.argv[5]
    assert root.resolve() == Path.cwd().resolve()
    assert urlsplit(os.environ["CLICKHOUSE_TEST_URL"]).hostname in {"127.0.0.1", "localhost", "::1"}
    imported_sources(root)
    logging.basicConfig(level=logging.INFO, filename=directory / "application.log")
    policy = json.loads(policy_path.read_text())
    manifest = json.loads((fixtures / "manifest.json").read_text())
    expected_path = fixtures / manifest["expected_output"]["name"]
    assert digest(expected_path) == manifest["expected_output"]["sha256"]
    expected = json.loads(expected_path.read_text())
    config = directory / "config"
    shutil.copytree(root / "config", config)
    template = PostgresTemplate(schema_template=sys.argv[6])
    try:
        async with owned_repository(template, os.environ["TANK_TEST_DATABASE_URL"], ownership, directory) as repo:
            assert repo.clickhouse.database.startswith("mkt_tank_test_")
            setup = await seed_market(repo, config, fixtures, policy, manifest)
            setup.update(
                manifest_sha256=digest(fixtures / "manifest.json"),
                expected_components_sha256=digest(expected_path),
                public_policy_sha256=digest(policy_path),
                availability_cutoff=manifest["availability_cutoff"],
                final_assembled_at=ASSEMBLED,
            )
            seeded = {
                state: await seed_session(repo, state, manifest["anchor"]) for state in ("cold", "warm")
            }
            meter = Measurements()
            original_iso = market_module.iso
            frozen_time = {"value": ASSEMBLED}
            market_module.utcnow = lambda: parse_time(ASSEMBLED)
            market_module.iso = lambda value=None: original_iso(frozen_time["value"] if value is None else value)
            async with (
                receiver(repo, directory, meter) as (url, receipts, errors),
                httpx.AsyncClient(trust_env=False) as http,
            ):
                settings = Settings(
                    _env_file=None,
                    config_dir=config,
                    data_dir=directory / "data",
                    replay=True,
                    openma_url=url,
                    openma_api_key=None,
                    code_version=revision,
                )
                market = MarketService(repo, settings, http=http)
                setup["native_day_counts"] = await load_native_day_counts(market, fixtures, manifest)
                service = truth.MarketTruthService(repo, settings, market)
                tools = EventToolService(repo, settings, service, None)
                sessions = SessionService(repo, settings, tools.dispatch, http=http)
                runtime = object.__new__(Runtime)
                runtime.repo, runtime.settings, runtime.market, runtime.truth = repo, settings, market, service
                runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate, "RUN_MARKET_TRUTH": runtime.assess}
                expected_series = {
                    (instrument["instrument_id"], kind)
                    for instrument in market.instruments.values()
                    if instrument["token_id"] == TOKEN
                    for kind in (("price", "volume") if instrument["market_type"] == "SPOT" else ("volume",))
                } | {
                    ("binance:spot:BTCUSDT", "price"),
                    ("binance:spot:ETHUSDT", "price"),
                }
                setup["expected_dense_series_requests"] = sorted(expected_series)
                setup["configured_ena_instruments"] = sorted(
                    instrument["instrument_id"]
                    for instrument in market.instruments.values()
                    if instrument["token_id"] == TOKEN
                )
                original_to_thread = asyncio.to_thread

                async def observed_to_thread(function, *args, **kwargs):
                    if not meter.active:
                        return await original_to_thread(function, *args, **kwargs)
                    submitted = perf_counter()

                    def work():
                        entered, cpu = perf_counter(), thread_time()
                        try:
                            return function(*args, **kwargs)
                        finally:
                            meter.executor_dispatch.append(
                                {
                                    "dispatch_wall_seconds": entered - submitted,
                                    "work_wall_seconds": perf_counter() - entered,
                                    "work_thread_cpu_seconds": thread_time() - cpu,
                                }
                            )

                    return await cancel_safe_thread(meter, original_to_thread(work))

                asyncio.to_thread = observed_to_thread
                for name in (
                    "compute_market_truth",
                    "relative_truth",
                    "spot_window_graph",
                    "derivatives_truth",
                    "liquidity_truth",
                    "supply_truth",
                    "support_features",
                    "enrich_evidence",
                    "data_confidence",
                    "measurement_metadata",
                    "timestamp_companions",
                    "jsonable",
                ):
                    phase = (
                        "compute" if name == "compute_market_truth" else "compute." + name.removesuffix("_truth")
                    )
                    meter.instrument(truth, name, phase)
                for name in ("_records", "_json_lines"):
                    meter.instrument(clickhouse_market, name, "clickhouse." + name.removeprefix("_"))
                for name in ("capture_readiness", "repair_windows", "_condition_kline_series"):
                    meter.instrument(market, name, "market." + name.removeprefix("_"))
                snapshot = market.snapshot
                snapshot_heads = market._snapshot_heads

                def final_snapshot_clock(*args, **kwargs):
                    result = snapshot_heads(*args, **kwargs)
                    frozen_time["value"] = ASSEMBLED
                    return result

                market._snapshot_heads = final_snapshot_clock

                async def observe_snapshot(*args, **kwargs):
                    frozen_time["value"] = manifest["availability_cutoff"]
                    try:
                        result = await snapshot(*args, **kwargs)
                    finally:
                        frozen_time["value"] = ASSEMBLED
                    meter.snapshot = result
                    return result

                market.snapshot = observe_snapshot
                meter.instrument(market, "snapshot", "snapshot")
                meter.instrument(repo.clickhouse, "rows", "clickhouse.rows_http_and_json")
                meter.instrument(service, "assess", "assessment")
                meter.instrument(tools, "get_market_truth", "tool.domain")
                meter.instrument(sessions, "request", "tool.http_request")
                meter.instrument(repo, "put", "postgres.put")
                reader = asyncio.StreamReader()
                protocol = asyncio.StreamReaderProtocol(reader)
                transport, _ = await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin)
                try:
                    setup["total_wall_seconds"] = perf_counter() - setup_started
                    setup["total_process_cpu_seconds"] = process_time() - setup_cpu
                    print(
                        json.dumps(
                            {
                                "kind": "ready",
                                "pid": os.getpid(),
                                "revision": revision,
                                "interpreter": {
                                    "implementation": sys.implementation.name,
                                    "version": sys.version,
                                },
                                "dependencies": {
                                    name: version(name) for name in ("httpx", "psycopg", "PyYAML")
                                },
                                "setup": setup,
                                "modules": imported_sources(root),
                            }
                        ),
                        flush=True,
                    )

                    async def sample(operation):
                        session, backfill = seeded[operation]
                        try:
                            result = await run_sample(
                                operation,
                                repo,
                                session,
                                backfill,
                                sessions,
                                runtime,
                                meter,
                                receipts,
                                errors,
                                directory,
                                manifest,
                                setup,
                                expected,
                            )
                            observed_series = {
                                (row["instrument_id"], row["kind"]) for row in result["series_requests"]
                            }
                            assert observed_series == expected_series
                        except BaseException as error:
                            emit_receipt(
                                {
                                    "kind": "error",
                                    "state": operation,
                                    **safe_failure(error),
                                    "completion": "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME",
                                    "timeout": (
                                        {"origin": "sample_harness", "limit_seconds": 90}
                                        if isinstance(error, TimeoutError)
                                        else None
                                    ),
                                    "phases_inclusive_not_additive": meter.summary(),
                                    "query_inventory": meter.query_inventory,
                                    "cleanup": meter.cleanup,
                                }
                            )
                            raise
                        result["modules"] = imported_sources(root)
                        return result

                    await serve_commands(reader, sample, emit_receipt)
                finally:
                    transport.close()
    finally:
        await template.close()


async def main():
    ownership = {"pid": os.getpid(), "repository_cleanup": "NOT_ACQUIRED"}
    task, loop = asyncio.current_task(), asyncio.get_running_loop()
    assert task is not None

    def terminate():
        ownership["shutdown_signal"] = "SIGTERM"
        if not task.cancelling():
            task.cancel()

    loop.add_signal_handler(signal.SIGTERM, terminate)
    original_to_thread = asyncio.to_thread
    original_iso, original_utcnow = market_module.iso, market_module.utcnow
    try:
        await run_worker(ownership)
    except asyncio.CancelledError:
        if ownership.get("shutdown_signal") != "SIGTERM":
            raise
    finally:
        asyncio.to_thread = original_to_thread
        market_module.iso, market_module.utcnow = original_iso, original_utcnow
        loop.remove_signal_handler(signal.SIGTERM)
        emit_receipt({"kind": "shutdown", **ownership})


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as failure:
        emit_receipt(
            {
                "kind": "error",
                **safe_failure(failure),
                "completion": "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME",
            }
        )
        raise

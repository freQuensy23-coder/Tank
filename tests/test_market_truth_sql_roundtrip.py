import asyncio
import json
import logging
import os
from collections import Counter
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from decimal import Decimal, localcontext

import httpx
import pytest
from test_event_tools import complete_assessment_fixture, complete_event_fixture
from test_session_binding_concurrency import seed as seed_typed_session

from tank import logging as telemetry
from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.analysis.truth import MarketTruthService
from tank.event_tools import EventToolService
from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.settings import Settings
from tank.util import iso, parse_time, sha256

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ANCHOR = "2026-10-28T11:00:00Z"
AS_OF = "2026-10-28T12:05:00Z"
TOKEN = "test:ABC"
INSTRUMENT = "binance:spot:ABCUSD"
ENTRY = "110.00000000000000000000000000001"
NEXT_ENTRY = "112.00000000000000000000000000001"
BUY = "3.00000000000000000001"


@pytest.fixture
async def sql_roundtrip_repo(postgres_template):
    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "Owned disposable TANK_TEST_DATABASE_URL is required"
    assert os.environ.get("CLICKHOUSE_TEST_URL"), "Owned loopback CLICKHOUSE_TEST_URL is required"
    async with postgres_template.repository(dsn) as repo:
        assert repo.clickhouse.database.startswith("mkt_tank_test_")
        yield repo


@asynccontextmanager
async def protocol_receiver(repo, directory):
    pending = set()
    receipts = []
    errors = []

    async def receive(reader, writer):
        task = asyncio.current_task()
        pending.add(task)
        try:
            async with asyncio.timeout(10):
                head = (await reader.readuntil(b"\r\n\r\n")).decode("ascii").split("\r\n")
                assert head[0] == "POST /v1/sessions/s/events HTTP/1.1"
                headers = {
                    key.lower(): value.strip()
                    for line in head[1:]
                    if line
                    for key, value in [line.split(":", 1)]
                }
                assert headers["content-type"] == "application/json"
                assert headers["anthropic-beta"] == "managed-agents-2026-04-01"
                length = int(headers["content-length"])
                assert 0 < length < 8_000_000
                raw = await reader.readexactly(length)
                body = json.loads(raw)
                assert set(body) == {"events"}
                assert len(body["events"]) == 1
                native = body["events"][0]
                call_id = native["custom_tool_use_id"]
                assert call_id in {"sql-first", "sql-next"}
                assert native["type"] == "user.custom_tool_result"
                assert "is_error" not in native
                assert headers["idempotency-key"] == "tool:" + sha256("s:" + call_id)
                audited = await repo.get("ops.tool_calls", "s:" + call_id)
                assert audited["status"] == "DONE"
                assert audited["native_result_body"] == native
                assert audited.get("returned_at") is None
                path = directory / (call_id + ".json")
                with path.open("wb") as receipt:
                    receipt.write(raw)
                    receipt.flush()
                    os.fsync(receipt.fileno())
                receipts.append(path)
                response = json.dumps({"data": [{"id": "protocol-" + call_id}]}).encode()
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
                    b"Content-Length: " + str(len(response)).encode() + b"\r\n\r\n" + response
                )
                await writer.drain()
        except Exception as error:
            errors.append(error)
            writer.write(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            with suppress(ConnectionError):
                await writer.drain()
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            pending.discard(task)

    server = await asyncio.start_server(receive, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}", receipts, errors
    finally:
        server.close()
        await server.wait_closed()
        for task in tuple(pending):
            task.cancel()
        if pending:
            await asyncio.gather(*tuple(pending), return_exceptions=True)


async def seed_roundtrip(repo, config_dir):
    instruments = [
        {
            "instrument_id": iid,
            "token_id": token,
            "venue": "binance",
            "market_type": "SPOT",
            "venue_symbol": symbol + "USD",
            "quote_asset": "USD",
            "active": True,
            "contract_multiplier": "1",
            "tick_size": "0.01",
            "lot_size": "0.01",
        }
        for iid, token, symbol in (
            (INSTRUMENT, TOKEN, "ABC"),
            ("binance:spot:BTCUSD", "bitcoin:BTC", "BTC"),
        )
    ]
    tokens = [
        {"token_id": TOKEN, "symbol": "ABC", "sector": "FIXTURE"},
        {"token_id": "bitcoin:BTC", "symbol": "BTC", "sector": "BENCHMARK"},
    ]
    for name, value in {
        "registry": {"tokens": tokens, "instruments": instruments},
        "semantics": {"metrics": []},
        "thresholds": {},
        "sources": {"sources": []},
    }.items():
        (config_dir / (name + ".yaml")).write_text(json.dumps(value))
    for row in tokens:
        await repo.put("reg.tokens", row["token_id"], row)
    for row in instruments:
        await repo.put("reg.instruments", row["instrument_id"], row)
    await repo.put("reg.sources", "S01", {"source_id": "S01", "name": "Public protocol fixture"})
    event = complete_event_fixture(
        "e",
        tokens_mentioned=[TOKEN],
        public_at=ANCHOR,
        public_at_prec="EXACT",
        known_to_us_at=ANCHOR,
        known_to_us_at_prec="EXACT",
    )
    await repo.put("evt.events", "e", event)
    await repo.put(
        "evt.ems_assessments",
        "ems-sql",
        complete_assessment_fixture("ems-sql", "e", **{"class": "MAJOR", "cutoff": ANCHOR}),
    )
    await repo.put("evt.events", "e", dict(event, current_ems_id="ems-sql"))
    session, _ = await seed_typed_session(repo)
    session.update(event_id="e", model_context_version=CONTEXT_VERSION)
    await repo.put("ops.agent_sessions", "s", session)
    backfill_id = "BF-" + sha256({"event_id": "e", "token_id": TOKEN, "anchor": ANCHOR})
    await repo.put(
        "mkt.backfill_runs",
        backfill_id,
        {
            "backfill_id": backfill_id,
            "event_id": "e",
            "token_id": TOKEN,
            "anchor": ANCHOR,
            "anchor_prec": "EXACT",
            "started_at": ANCHOR,
            "started_at_prec": "COLLECTION_ONLY",
            "finished_at": AS_OF,
            "finished_at_prec": "COLLECTION_ONLY",
            "status": "COMPLETE",
            "config_version": "fixture",
            "code_version": "fixture",
        },
    )
    end = parse_time("2026-10-28T12:00:00Z")
    bars = []
    klines = []
    for minute in range(1442):
        opened = end - timedelta(minutes=1440) + timedelta(minutes=minute)
        close = "100" if opened < parse_time(ANCHOR) else ENTRY
        if opened == end:
            close = NEXT_ENTRY
        if opened > end:
            close = "999999"
        common = {
            "instrument_id": INSTRUMENT,
            "token_id": TOKEN,
            "venue": "binance",
            "market_type": "SPOT",
            "quote_asset": "USD",
            "complete": True,
            "recorded_at": AS_OF,
            "recorded_at_prec": "COLLECTION_ONLY",
        }
        klines.append(
            dict(
                common,
                open_time=iso(opened),
                open_time_prec="BUCKET(60s)",
                interval_s=60,
                o=close,
                h=close,
                l=close,
                c=close,
                volume_native="4.00000000000000000001",
            )
        )
        if minute < 1441:
            bar = dict(
                common,
                minute_start=iso(opened),
                minute_start_prec="BUCKET(60s)",
                buy_qty=BUY,
                sell_qty="1",
                buy_usd="300",
                sell_usd="100",
                sum_sq_qty="10",
                n_buy=3,
                n_sell=1,
                size_buckets={},
            )
            if opened + timedelta(minutes=1) == parse_time(ANCHOR):
                bar["last_trade"] = {
                    "ts": iso(opened + timedelta(seconds=59)),
                    "ts_prec": "EXACT",
                    "price": "100",
                }
            bars.append(bar)
    for opened in (end - timedelta(minutes=1), end):
        klines.append(
            {
                "instrument_id": "binance:spot:BTCUSD",
                "token_id": "bitcoin:BTC",
                "venue": "binance",
                "market_type": "SPOT",
                "open_time": iso(opened),
                "open_time_prec": "BUCKET(60s)",
                "interval_s": 60,
                "o": "50000.00000000000000000000000000001",
                "h": "50000.00000000000000000000000000001",
                "l": "50000.00000000000000000000000000001",
                "c": "50000.00000000000000000000000000001",
                "volume_native": "1",
                "complete": True,
                "recorded_at": AS_OF,
                "recorded_at_prec": "COLLECTION_ONLY",
            }
        )
    assert len(await repo.put_market_batch("mkt.flow_bars_1m", bars)) == 1441
    assert len(await repo.put_market_batch("mkt.klines_1m", klines)) == 1444
    return session, backfill_id


def assert_numeric_result(result, cutoff, entry, event_minutes):
    assert result["cutoff"] == cutoff
    assert result["cutoff_prec"] == "BUCKET(1m)"
    assert result["requested_cutoff"] == cutoff
    assert result["requested_cutoff_prec"] == "BUCKET(1m)"
    assert result["assembled_at"] == AS_OF
    assert result["assembled_at_prec"] == "COLLECTION_ONLY"
    assert result["relative"]["entry_ref_price"] == entry
    assert result["relative"]["entry_price_source"] == "KLINE_CLOSE"
    assert result["relative"]["entry_price_observed_at"] == cutoff
    assert result["relative"]["anchor"]["ref_price"] == "100"
    assert result["relative"]["anchor"]["ts"] == ANCHOR
    btc_reference = result["relative"]["outcome_btc_reference"]
    assert btc_reference["entry_ref_price"] == "50000.00000000000000000000000000001"
    with localcontext() as context:
        context.prec = 128
        assert result["relative"]["windows"]["EVENT"]["r_raw"] == pytest.approx(
            float(Decimal(entry) / Decimal("100") - 1), rel=1e-12, abs=1e-12
        )
        for window, minutes in (("W1H", 60), ("EVENT", event_minutes)):
            venues = result["spot"][window]["venues"]
            assert len(venues) == 1
            assert Decimal(venues[0]["buy_qty"]) == Decimal(BUY) * minutes
            assert Decimal(venues[0]["sell_qty"]) == Decimal("1") * minutes
            assert Decimal(venues[0]["cvd"]) == (Decimal(BUY) - 1) * minutes
            assert Decimal(venues[0]["volume_native"]) == (Decimal(BUY) + 1) * minutes


async def test_sql_market_truth_returns_over_http_protocol_fixture(
    sql_roundtrip_repo, tmp_path, monkeypatch, caplog, record_property
):
    repo = sql_roundtrip_repo
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    session, backfill_id = await seed_roundtrip(repo, config_dir)
    monkeypatch.setattr("tank.market.service.utcnow", lambda: parse_time(AS_OF))
    monkeypatch.setattr("tank.market.service.iso", lambda value=None: iso(AS_OF if value is None else value))
    requests = {}

    async def observe_request(request):
        query_id = request.url.params["query_id"]
        assert query_id not in requests
        requests[query_id] = request.content.decode()

    repo.clickhouse.http.event_hooks["request"].append(observe_request)
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    stop = asyncio.Event()
    admitted = asyncio.Event()
    enqueue = repo.enqueue
    finish = repo.finish_job

    async def observed_enqueue(kind, *args, **kwargs):
        job = await enqueue(kind, *args, **kwargs)
        if kind == "RUN_MARKET_TRUTH":
            admitted.set()
        return job

    async def observed_finish(job_id):
        await finish(job_id)
        if (await repo.get("ops.jobs", job_id))["type"] == "RUN_MARKET_TRUTH":
            stop.set()

    monkeypatch.setattr(repo, "enqueue", observed_enqueue)
    monkeypatch.setattr(repo, "finish_job", observed_finish)
    async with (
        protocol_receiver(repo, tmp_path) as (url, receipts, errors),
        httpx.AsyncClient(trust_env=False) as http,
    ):
        settings = Settings(
            _env_file=None,
            config_dir=config_dir,
            data_dir=tmp_path / "data",
            replay=True,
            openma_url=url,
            openma_api_key=None,
        )
        market = MarketService(repo, settings, http=http)
        truth = MarketTruthService(repo, settings, market)
        tools = EventToolService(repo, settings, truth, None)
        sessions = SessionService(repo, settings, tools.dispatch, http=http)
        runtime = object.__new__(Runtime)
        runtime.repo, runtime.settings, runtime.market, runtime.truth = repo, settings, market, truth
        runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate, "RUN_MARKET_TRUTH": runtime.assess}
        results = []
        for call_id, cutoff, entry, minutes in (
            ("sql-first", "2026-10-28T12:00:00Z", ENTRY, 60),
            ("sql-next", "2026-10-28T12:01:00Z", NEXT_ENTRY, 61),
        ):
            stop.clear()
            admitted.clear()
            caller = worker = None
            try:
                async with asyncio.timeout(30):
                    caller = asyncio.create_task(
                        sessions.tool(
                            session,
                            {
                                "name": "get_market_truth",
                                "id": call_id,
                                "input": {
                                    "token_ids": [TOKEN],
                                    "cutoff": cutoff,
                                    "cutoff_prec": "BUCKET(1m)",
                                },
                            },
                        )
                    )
                    await admitted.wait()
                    worker = asyncio.create_task(runtime.worker(stop))
                    await caller
                    await worker
            finally:
                stop.set()
                for task in (caller, worker):
                    if task is not None:
                        if not task.done():
                            task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task
            assert not errors
            stored = await repo.get("ops.tool_calls", "s:" + call_id)
            assert stored["status"] == "DONE"
            assert stored["returned_at"] is not None
            assert stored["returned_at_prec"] == "COLLECTION_ONLY"
            assert stored["remote_event_id"] == "protocol-" + call_id
            result = stored["result"]["market_truth"][0]
            assert_numeric_result(result, cutoff, entry, minutes)
            persisted = await repo.get("ana.market_truth", result["mta_id"])
            assert persisted == result
            assert persisted["token_id"] == TOKEN
            assert persisted["event_id"] == "e"
            assert persisted["relative"]["ref_venue"] == INSTRUMENT
            assert persisted["detail"]["capture_status"]["status"] == "COMPLETE"
            assert persisted["detail"]["capture_status"]["backfill_id"] == backfill_id
            assert persisted["detail"]["prerequisite_pending"] is False
            wire = json.loads((tmp_path / (call_id + ".json")).read_bytes())
            assert wire["events"] == [stored["native_result_body"]]
            native = wire["events"][0]
            assert len(native["content"]) == 1
            assert native["content"][0]["type"] == "text"
            projected = json.loads(native["content"][0]["text"])
            assert projected["requested_cutoff"] == cutoff
            assert projected["requested_cutoff_prec"] == "BUCKET(1m)"
            assert len(projected["market_truth"]) == 1
            assert_numeric_result(projected["market_truth"][0], cutoff, entry, minutes)
            results.append(result)
        assert len(receipts) == 2
    assert results[0]["mta_id"] != results[1]["mta_id"]
    jobs = await repo.list("ops.jobs", limit=100)
    assert Counter(row["type"] for row in jobs) == {"ESCALATE_TOKENS": 1, "RUN_MARKET_TRUTH": 2}
    escalation = next(row for row in jobs if row["type"] == "ESCALATE_TOKENS")
    assessments = [row for row in jobs if row["type"] == "RUN_MARKET_TRUTH"]
    assert all(row["status"] == "DONE" and row["attempts"] == 1 for row in jobs)
    assert all(row["payload"]["_depends_on"] == [escalation["job_id"]] for row in assessments)
    assert {row["result"]["mta_id"] for row in assessments} == {row["mta_id"] for row in results}
    assert (await repo.get("mkt.hot_tokens", TOKEN + ":e"))["priority"] == 3
    assert (await repo.get("mkt.backfill_runs", backfill_id))["status"] == "COMPLETE"
    assert await repo.list("mkt.capture_requests") == []
    records = [json.loads(row.getMessage()) for row in caplog.records if row.name == "tank.market_timing"]
    starts = {row["span_id"]: row for row in records if row["event"] == "stage_start"}
    ends = {row["span_id"]: row for row in records if row["event"] == "stage_end"}
    assert len(starts) == len(ends) == len(records) // 2
    assert starts.keys() == ends.keys()
    assert all(row["status"] == "ok" for row in ends.values())
    for span_id, row in ends.items():
        assert row["started_mono_ns"] == starts[span_id]["started_mono_ns"]
        assert row["ended_mono_ns"] >= row["started_mono_ns"]
        assert row["wall_ms"] >= 0
        if row["parent_span_id"] is not None:
            parent = ends[row["parent_span_id"]]
            assert parent["trace_id"] == row["trace_id"]
            assert parent["started_mono_ns"] <= row["started_mono_ns"]
            assert row["ended_mono_ns"] <= parent["ended_mono_ns"]
    stages = {row["stage"] for row in ends.values()}
    assert {
        "tool",
        "tool.provenance",
        "tool.arguments",
        "get_market_truth",
        "tool.validation",
        "tool.enqueue",
        "tool.jobs_wait",
        "tool.audit",
        "tool.projection",
        "tool.wire_json",
        "tool.wire_audit",
        "tool.openma_acceptance",
        "tool.return_audit",
        "job.link",
        "job.claimed",
        "job.dependencies",
        "job.execution",
        "job.capture_readiness",
        "capture.readiness",
        "job.repair",
        "capture.repair",
        "capture.hot_set_lock_wait",
        "capture.escalate_prepare",
        "snapshot",
        "snapshot.collection",
        "snapshot.references",
        "snapshot.fingerprints",
        "clickhouse.http",
        "clickhouse.json_decode",
        "clickhouse.typed_decode",
        "clickhouse.projection_convert",
        "assessment.lock_wait",
        "threadpool.wait",
        "threadpool.work",
        "compute",
        "compute.lock_wait",
        "compute.relative",
        "compute.spot_window",
        "compute.metadata",
        "assessment.persist",
        "job.persist",
    } <= stages
    for call_id in ("sql-first", "sql-next"):
        root = next(
            row
            for row in ends.values()
            if row["stage"] == "tool" and row.get("call_id") == telemetry.timing_identity(call_id)
        )
        linked = [row for row in ends.values() if row["trace_id"] == root["trace_id"]]
        assert all(row.get("session_id") == telemetry.timing_identity("s") for row in linked)
        assert all(row.get("call_id") == telemetry.timing_identity(call_id) for row in linked)
        audit = await repo.get("ops.tool_calls", "s:" + call_id)
        assessment = next(
            job for job in assessments if job["payload"]["cutoff"] == audit["arguments"]["cutoff"]
        )
        assert {row["job_id"] for row in linked if row["stage"] == "job.link"} == {
            telemetry.timing_identity(escalation["job_id"]),
            telemetry.timing_identity(assessment["job_id"]),
        }
    for job in assessments:
        owned = [
            row for row in ends.values() if row.get("job_id") == telemetry.timing_identity(job["job_id"])
        ]
        assert {"job.execution", "snapshot", "compute", "assessment.persist", "job.persist"} <= {
            row["stage"] for row in owned
        }
    conversions = [
        row
        for row in ends.values()
        if row["stage"] in {"clickhouse.typed_decode", "clickhouse.projection_convert"} and row["rows"]
    ]
    assert sum(row["rows"] for row in conversions if row["stage"] == "clickhouse.typed_decode") >= 1441
    assert any(row["stage"] == "clickhouse.projection_convert" and row["rows"] > 1000 for row in conversions)
    for row in conversions:
        query_id = row["query_id"]
        query = requests[query_id]
        assert query.startswith("SELECT * " if row["stage"] == "clickhouse.typed_decode" else "SELECT `")
        if row["stage"] == "clickhouse.projection_convert":
            assert "_cursor_time" in query
            if row["rows"] > 1000:
                assert ends[row["parent_span_id"]]["stage"] == "threadpool.work"
        else:
            assert "_day" not in query
        assert row["thread_cpu_ms"] >= 0
        siblings = [span for span in ends.values() if span.get("query_id") == query_id]
        assert {"clickhouse.query", "clickhouse.http", "clickhouse.json_decode"} <= {
            span["stage"] for span in siblings
        }
        assert all(span["trace_id"] == row["trace_id"] for span in siblings)
        http_span = next(span for span in siblings if span["stage"] == "clickhouse.http")
        assert http_span["bytes"] > 0
        assert http_span["ended_mono_ns"] <= row["started_mono_ns"]
    assert {row["cache_hit"] for row in ends.values() if row["stage"] == "snapshot.range_cache"} == {
        False,
        True,
    }
    safe_logs = json.dumps(records)
    assert all(value not in safe_logs for value in (TOKEN, INSTRUMENT, ENTRY, BUY, "SELECT", "http://"))
    assert all("trace_id" not in row["payload"] for row in jobs)
    record_property(
        "qualification_scope",
        "Real isolated PostgreSQL+ClickHouse and loopback HTTP protocol fixture; deterministic public-shaped "
        "market data; not native OpenMA, model continuation, historical incident replay, or deployment",
    )
    record_property(
        "roundtrip_timing_stage_counts", json.dumps(Counter(row["stage"] for row in ends.values()))
    )

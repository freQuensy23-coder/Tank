"""tank-capture owns exchange capture; this process escalates and reads.

The table contract with tank-capture (capture/README.md): mkt.backfill_runs and
mkt.capture_requests are created PENDING here and finished there; mkt.capture_streams is
written there and read here; ops.request_budget_ledger is shared by both.
"""

import asyncio
import time
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import yaml
from pydantic import JsonValue
from source_support import FixtureBudgetClock, fixture_http_admission, fixture_versions

from tank.market.normalize import timestamp_fields
from tank.market.service import CAPTURE_REQUEST_GRACE_S, EXCHANGE_REST_OWNER, MarketService
from tank.request_budget import BudgetConstraint, RequestBudgetScope, RequestLedger, SharedRequestBudgets
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.time_precision import Timestamp, collection_clock
from tank.util import iso, parse_time, sha256, utcnow

SPOT, PERP, HL = "binance:spot:ABCUSDT", "binance:usdm:ABCUSDT", "hyperliquid:perp:ABC"
TOKEN = "test:ABC"


def instrument(iid, venue, market_type, symbol):
    return {
        "instrument_id": iid,
        "token_id": TOKEN,
        "venue": venue,
        "market_type": market_type,
        "venue_symbol": symbol,
        "quote_asset": "USDT",
        "active": True,
        "contract_multiplier": "1",
    }


def make_settings(tmp_path: Path, **values):
    config = tmp_path / "config"
    config.mkdir(exist_ok=True)
    (config / "thresholds.yaml").write_bytes(Settings().read_config_bytes("thresholds.yaml"))
    registry = {
        "tokens": [{"token_id": TOKEN, "symbol": "ABC", "sector": "A"}],
        "instruments": [
            instrument(SPOT, "binance", "SPOT", "ABCUSDT"),
            instrument(PERP, "binance", "PERP_LINEAR", "ABCUSDT"),
            instrument(HL, "hyperliquid", "PERP_LINEAR", "ABC"),
        ],
    }
    (config / "registry.yaml").write_text(yaml.safe_dump(registry))
    (config / "semantics.yaml").write_text(yaml.safe_dump({"metrics": []}))
    return Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", **values)


@pytest.fixture
def settings(tmp_path: Path):
    return make_settings(tmp_path)


def capture_event() -> dict[str, JsonValue]:
    return {
        "event_id": "EVT-1",
        "statement": "An official approval requires historical market capture",
        "theme_tags": ["CAPTURE_TEST"],
        "public_at": "2026-10-18T12:00:00Z",
        "public_at_prec": "EXACT",
        "public_at_basis": "Recorded official fixture publication",
        "known_to_us_at": "2026-10-18T12:00:00Z",
        "known_to_us_at_prec": "EXACT",
        "freshness": "STALE",
        "event_state": "APPROVED",
        "state_verified": True,
        "certainty": "HIGH",
        "timing_confidence": "HIGH",
        "best_source_class": "PRIMARY",
        "lifecycle": "MONITORING",
    }


async def put(repo, collection, row, key) -> None:
    await repo.put_market_batch(collection, [row], keys=[key])


async def update(repo, collection, key, **fields) -> None:
    """What tank-capture writes into a row this process created."""
    await put(repo, collection, {**await repo.get(collection, key), **fields}, key)


async def finish(repo, collection, key, **fields) -> None:
    await update(repo, collection, key, **collection_clock(utcnow()).fields("finished_at"), **fields)


async def test_external_escalation_writes_the_obligation_and_starts_nothing(settings) -> None:
    repo = MemoryRepository()
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    service._stop = asyncio.Event()
    await repo.put(
        "evt.events",
        "EVT-1",
        capture_event(),
    )
    await service.escalate("EVT-1", [TOKEN])
    assert [row["token_id"] for row in await repo.list("mkt.hot_tokens")] == [TOKEN]
    (run,) = await repo.list("mkt.backfill_runs")
    assert run["status"] == "PENDING" and run["reason"] == "PRE_EVENT_CAPTURE_PENDING"
    assert run["backfill_id"] == "BF-" + sha256(
        {"event_id": "EVT-1", "token_id": TOKEN, "anchor": run["anchor"]}
    )
    assert (
        await service.capture_readiness(
            "EVT-1", TOKEN, anchor=Timestamp(run["anchor"], run["anchor_prec"], run["anchor_prec_reason"])
        )
    ).status == "PENDING"

    await update(repo, "mkt.backfill_runs", run["backfill_id"], status="RUNNING")
    restarted = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    await restarted._schedule_hot_backfills()
    assert (await repo.get("mkt.backfill_runs", run["backfill_id"]))["status"] == "RUNNING"
    await finish(repo, "mkt.backfill_runs", run["backfill_id"], status="FAILED", reason="venue down")
    again = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    await again._schedule_hot_backfills()
    assert (await repo.get("mkt.backfill_runs", run["backfill_id"]))["status"] == "PENDING"
    for market in (service, restarted, again):
        await market.close()


async def capture_worker(repo, outcome, seen) -> None:
    """A tank-capture stand-in: claims the first PENDING request and finishes it."""
    while not (rows := await repo.list("mkt.capture_requests", filters={"status": "PENDING"})):
        await asyncio.sleep(0.01)
    seen.append(rows[0])
    key = rows[0]["request_id"]
    await update(repo, "mkt.capture_requests", key, status="RUNNING", writer="tank-capture")
    await finish(repo, "mkt.capture_requests", key, **outcome)


async def repair_round_trip(repo, service) -> None:
    result = {SPOT: {"incomplete_before": 3, "incomplete_after": 1}}
    seen = []
    worker = asyncio.create_task(
        capture_worker(repo, {"status": "COMPLETE", "result": {"instruments": result}}, seen)
    )
    repaired = await service.repair_windows(
        TOKEN, "2026-10-20T10:00:30Z", "2026-10-21T10:00:00Z", budget_s=10
    )
    await worker
    assert repaired.status == "COMPLETE"
    assert {iid: counts.model_dump() for iid, counts in repaired.instruments.items()} == result
    (request,) = seen
    assert request["kind"] == "REPAIR_WINDOWS" and request["token_id"] == TOKEN
    assert request["start"] == "2026-10-20T10:00:00Z" and request["start_prec"] == "EXACT"
    assert request["end"] == "2026-10-21T10:00:00Z" and request["end_prec"] == "EXACT"
    assert float(request["budget_s"]) == 10
    assert request["requested_at_prec"] == request["deadline_prec"] == "COLLECTION_ONLY"
    assert parse_time(request["deadline"]) - parse_time(request["requested_at"]) == timedelta(seconds=10)
    assert request["request_id"] == "CR-" + sha256(
        {
            "token_id": TOKEN,
            "start": request["start"],
            "end": request["end"],
            "requested_at": request["requested_at"],
        }
    )

    worker = asyncio.create_task(capture_worker(repo, {"status": "FAILED", "reason": "venue down"}, []))
    failed = await service.repair_windows(TOKEN, "2026-10-20T10:00:00Z", "2026-10-21T10:00:00Z", budget_s=10)
    assert failed.status == "FAILED" and failed.reason == "venue down"
    await worker
    started = time.monotonic()
    timed_out = await service.repair_windows(
        TOKEN, "2026-10-20T10:00:00Z", "2026-10-21T10:00:00Z", budget_s=0.2
    )
    assert timed_out.status == "INCOMPLETE" and timed_out.reason == "BOUNDED_CAPTURE_REQUEST_WAIT_EXPIRED"
    assert time.monotonic() - started < 0.2 + CAPTURE_REQUEST_GRACE_S + 1


async def test_external_repair_windows_is_a_capture_request(settings) -> None:
    repo = MemoryRepository()
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    await repair_round_trip(repo, service)
    await service.close()


def stream_rows(now, *, perp_updated, spot_updated):
    connected = now - timedelta(minutes=5)
    return [
        {
            "instrument_id": PERP,
            "stream": "WS",
            "token_id": TOKEN,
            "venue": "binance",
            "market_type": "PERP_LINEAR",
            "state": "CONNECTED",
            **timestamp_fields("connected_since", connected, "COLLECTION_ONLY"),
            **timestamp_fields("watermark_ts", now - timedelta(seconds=2), "EXACT"),
            **timestamp_fields("last_message_at", now - timedelta(seconds=1), "COLLECTION_ONLY"),
            **timestamp_fields("last_trade_ts", now - timedelta(seconds=3), "EXACT"),
            "last_trade_id": "42",
            **timestamp_fields("updated_at", perp_updated, "COLLECTION_ONLY"),
        },
        {
            "instrument_id": SPOT,
            "stream": "WS",
            "token_id": TOKEN,
            "venue": "binance",
            "market_type": "SPOT",
            "state": "CONNECTED",
            **timestamp_fields("connected_since", connected, "COLLECTION_ONLY"),
            **timestamp_fields("watermark_ts", now - timedelta(seconds=2), "EXACT"),
            **timestamp_fields("last_trade_ts", now - timedelta(seconds=9), "EXACT"),
            **timestamp_fields("updated_at", spot_updated, "COLLECTION_ONLY"),
        },
        {
            "instrument_id": SPOT,
            "stream": "KLINES_POLL",
            "token_id": TOKEN,
            "venue": "binance",
            "market_type": "SPOT",
            "recent_polls": [iso(now - timedelta(seconds=120)), iso(now - timedelta(seconds=60))],
            **timestamp_fields("updated_at", now, "COLLECTION_ONLY"),
        },
    ]


async def check_stream_state(repo, service) -> None:
    now = utcnow()
    rows = stream_rows(now, perp_updated=now, spot_updated=now - timedelta(seconds=60))
    await repo.put_market_batch(
        "mkt.capture_streams", rows, keys=[row["instrument_id"] + ":" + row["stream"] for row in rows]
    )
    metadata = await service._capture_metadata(TOKEN, iso(now))
    liquidations = metadata["collections"]["liquidations"]
    assert liquidations["coverage_status_by_instrument"] == {PERP: "CONNECTED", HL: "NOT_SUPPORTED"}
    assert liquidations["observed_through"] == {PERP: iso(now - timedelta(seconds=2))}
    assert liquidations["coverage_start"] == {PERP: iso(now - timedelta(minutes=5))}
    assert metadata["collections"]["klines"]["observed_through"] == {SPOT: iso(now - timedelta(seconds=60))}
    earlier = await service._capture_metadata(TOKEN, iso(now - timedelta(seconds=90)))
    assert earlier["collections"]["klines"]["observed_through"] == {SPOT: iso(now - timedelta(seconds=120))}
    assert earlier["collections"]["liquidations"]["observed_through"] == {
        PERP: iso(now - timedelta(seconds=90))
    }
    coverage = {row["instrument_id"]: row for row in await service.coverage()}
    assert coverage[PERP]["stream_connected"] and coverage[PERP]["last_trade_ts"] == iso(
        now - timedelta(seconds=3)
    )

    assert not coverage[SPOT]["stream_connected"]
    assert coverage[SPOT]["last_trade_ts"] == iso(now - timedelta(seconds=9))

    stale = stream_rows(now, perp_updated=now - timedelta(seconds=31), spot_updated=now)
    await repo.put_market_batch(
        "mkt.capture_streams", [dict(stale[0], last_trade_id="43")], keys=[PERP + ":WS"]
    )
    liquidations = (await service._capture_metadata(TOKEN, iso(now)))["collections"]["liquidations"]
    assert liquidations["coverage_status_by_instrument"][PERP] == "DISCONNECTED"
    assert liquidations["observed_through"] == {} and liquidations["coverage_start"] == {}


async def test_external_stream_state_comes_from_capture_streams(settings) -> None:
    repo = MemoryRepository()
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    await check_stream_state(repo, service)
    await service.close()


async def test_external_capture_never_calls_exchange_rest(settings) -> None:
    hosts = []

    def respond(request: pytest.FixtureRequest):
        hosts.append(request.url.host)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = MarketService(
            MemoryRepository(),
            settings,
            http=http,
            versions=fixture_versions(),
            admission=fixture_http_admission(),
        )
        for method, url, extra in (
            ("GET", "https://api.binance.com/api/v3/klines", {"params": {"symbol": "ABCUSDT", "limit": 3}}),
            ("GET", "https://www.okx.com/api/v5/market/candles", {"params": {"instId": "ABC-USDT"}}),
            ("POST", "https://api.hyperliquid.xyz/info", {"json": {"type": "metaAndAssetCtxs"}}),
            ("GET", "https://api.kraken.com/0/public/Trades", {"params": {"pair": "ABCUSD"}}),
        ):
            with pytest.raises(RuntimeError, match="tank-capture"):
                await service._request(method, url, **extra)

        assert await service._request("GET", "https://api.llama.fi/summary/fees/abc") == {"ok": True}
        assert hosts == ["api.llama.fi"]
        assert EXCHANGE_REST_OWNER.startswith("exchange REST")
        await service.close()


async def test_external_run_keeps_tiers_and_obligations_and_captures_nothing(
    settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = MemoryRepository()
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    since = "2026-10-21T10:00:00Z"
    await repo.put("evt.events", "EVT-1", {**capture_event(), "public_at": since})
    await put(
        repo,
        "mkt.hot_tokens",
        {"token_id": TOKEN, "event_id": "EVT-1", "priority": 2, **timestamp_fields("since", since, "EXACT")},
        TOKEN + ":EVT-1",
    )

    async def idle() -> None:
        await service._stop.wait()

    monkeypatch.setattr(service, "_supply_loop", idle)
    stop = asyncio.Event()
    task = asyncio.create_task(service.run(stop))
    backfill_id = "BF-" + sha256({"event_id": "EVT-1", "token_id": TOKEN, "anchor": since})
    for _ in range(500):
        if await repo.get("mkt.backfill_runs", backfill_id):
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 10)
    assert (await repo.get("mkt.backfill_runs", backfill_id))["status"] == "PENDING"
    tiers = [row async for row in repo.iter_all("ops.capture_tier_transitions")]
    assert [(row["token_id"], row["tier"]) for row in tiers] == [(TOKEN, "HOT")]
    assert (await repo.get("mkt.hot_tokens", TOKEN + ":EVT-1"))["expires_at"] is not None
    await service.close()


@pytest.fixture
async def migrated(typed_repo):
    await typed_repo.migrate()
    return typed_repo


async def ledger_rows(repo):
    async with repo.transaction():
        return await (
            await repo._conn().execute("SELECT bucket, owner FROM ops.request_budget_ledger ORDER BY at")
        ).fetchall()


@pytest.mark.integration
@pytest.mark.postgres
async def test_request_ledger_is_one_budget_for_every_process(migrated) -> None:
    first_ledger = RequestLedger(migrated.database_url, "first")
    second_ledger = RequestLedger(migrated.database_url, "second")
    first, second = SharedRequestBudgets(first_ledger), SharedRequestBudgets(second_ledger)
    try:
        started = time.monotonic()
        await first.acquire("kraken", 1, 1, host="api.kraken.com")
        await second.acquire("kraken", 1, 1, priority=-1, host="api.kraken.com")
        assert time.monotonic() - started >= 0.9
        await first.acquire("binance.spot", 3000, 60, weight=2, host="api.binance.com")
        await second.acquire("binance.spot", 3000, 60, weight=4, host="api.binance.com")

        assert [(row["bucket"], row["owner"]) for row in await ledger_rows(migrated)] == [
            ("kraken|1.0|1.0", "second"),
            ("binance.spot|60.0|3000.0", "first"),
            ("binance.spot|60.0|3000.0", "second"),
        ]
        await first.block("api.binance.com", utcnow() + timedelta(seconds=1), "HTTP 429")
        started = time.monotonic()
        await second.acquire("binance.spot", 3000, 60, weight=2, host="api.binance.com")
        assert time.monotonic() - started >= 0.8
        started = time.monotonic()
        await second.acquire("binance.usdm", 1200, 60, weight=1, host="fapi.binance.com")
        assert time.monotonic() - started < 0.5
    finally:
        await first.shutdown()
        await second.shutdown()
    assert first_ledger.closed and second_ledger.closed
    assert first_ledger.connection is None and second_ledger.connection is None


@pytest.mark.integration
@pytest.mark.postgres
async def test_external_contract_on_postgres(migrated, tmp_path: Path) -> None:
    service = MarketService(
        migrated, make_settings(tmp_path), versions=fixture_versions(), admission=fixture_http_admission()
    )
    await repair_round_trip(migrated, service)
    await check_stream_state(migrated, service)
    await migrated.put(
        "evt.events",
        "EVT-1",
        capture_event(),
    )
    await service.escalate("EVT-1", [TOKEN])
    (run,) = await migrated.list("mkt.backfill_runs")
    assert run["status"] == "PENDING"
    assert run["started_at"] is None and run["started_at_prec"] is None
    assert run["started_at_prec_reason"] == "PENDING"
    assert run["recorded_at"] is not None and run["recorded_at_prec"] == "COLLECTION_ONLY"
    await service.close()


@pytest.mark.integration
@pytest.mark.postgres
async def test_ledger_is_explicitly_bound_for_every_postgres_process(migrated: PostgresRepository) -> None:
    scope = RequestBudgetScope()
    constraints = [
        BudgetConstraint(
            key="coinbase", capacity=5.0, window_s=1.0, weight=1.0, host="api.exchange.coinbase.com"
        )
    ]
    with pytest.raises(RuntimeError, match="not been bound"):
        await scope.acquire_many(constraints)
    ledger = RequestLedger(migrated.database_url, "explicit-process")
    binding = scope.bind(ledger)
    await scope.acquire_many(constraints)
    assert [(row["bucket"], row["owner"]) for row in await ledger_rows(migrated)] == [
        ("coinbase|1.0|5.0", "explicit-process")
    ]
    await scope.release(binding)
    with pytest.raises(RuntimeError, match="not been bound"):
        await scope.acquire_many(constraints)
    with pytest.raises(RuntimeError, match="closed"):
        await ledger.wait_many(constraints)


async def test_same_injected_backend_enforces_429_and_418_without_clamping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from source_support import fixture_request_budgets

    clock = FixtureBudgetClock()
    admission = fixture_http_admission(budgets=fixture_request_budgets(clock=clock))
    replies: list[httpx.Response] = []
    starts: list[float] = []

    def respond(request: httpx.Request) -> httpx.Response:
        starts.append(clock.elapsed)
        return replies.pop(0)

    monkeypatch.setattr("tank.market.service.utcnow", clock.now)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = MarketService(
            MemoryRepository(),
            make_settings(tmp_path),
            http=http,
            versions=fixture_versions(),
            admission=admission,
        )
        url, params = "https://api.llama.fi/summary/fees/abc", {"dataType": "dailyFees"}
        replies[:] = [httpx.Response(418, headers={"Retry-After": "120"})]
        with pytest.raises(RuntimeError, match="418"):
            await service._request_once("GET", url, params=params)
        assert not replies
        assert clock.elapsed == 0.0
        replies[:] = [
            httpx.Response(429, headers={"Retry-After": "600"}),
            httpx.Response(200, json=[]),
        ]
        assert await service._request_once("GET", url, params=params) == []
        assert starts == [0.0, 120.0, 720.0]
        assert clock.elapsed == 720.0 and not replies
        await service.close()

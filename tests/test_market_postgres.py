"""Actual Go capture writes and Python readers on isolated PostgreSQL clones."""

import asyncio
import os
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import yaml

from tank.analysis.truth import compute_condition_values
from tank.market.service import MarketService
from tank.settings import Settings
from tank.util import iso, new_id, utcnow

pytestmark = [pytest.mark.integration, pytest.mark.postgres]
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
async def postgres_repo(postgres_template):
    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TANK_TEST_DATABASE_URL required for isolated PostgreSQL")
    async with postgres_template.repository(dsn) as repo:
        yield repo


@pytest.fixture(scope="module")
def capture_store_binary(tmp_path_factory):
    """Build once; selected integrations require the prepared offline Go lane."""
    go = shutil.which("go")
    if go is None:
        pytest.fail("Go capture integrations require the prepared Go toolchain")
    for name in ("GOMODCACHE", "GOCACHE", "GOPATH"):
        if not os.environ.get(name):
            pytest.fail(f"Go capture integrations require explicit {name} before environment isolation")
    binary = tmp_path_factory.mktemp("capture-store") / "store.test"
    result = subprocess.run(
        [go, "test", "-c", "-o", str(binary), "./internal/store"],
        cwd=ROOT / "capture",
        env=dict(os.environ, GOPROXY="off", GOSUMDB="off", GOTOOLCHAIN="local"),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return binary


async def run_go_producer(binary, repo, test, prefix):
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "-test.run=^" + test + "$",
        "-test.v",
        "-test.count=1",
        "-test.timeout=60s",
        cwd=ROOT / "capture",
        env=dict(
            os.environ,
            TANK_TEST_DATABASE_URL=repo.database_url,
            TANK_CAPTURE_REGRESSION_ID=prefix,
            GOPROXY="off",
            GOSUMDB="off",
            GOTOOLCHAIN="local",
        ),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        output, _ = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    text = output.decode()
    assert process.returncode == 0, text
    assert any(line.startswith("--- PASS: " + test + " ") for line in text.splitlines()), text


@pytest.mark.asyncio
async def test_real_postgres_capture_tier_history_survives_demotion_and_restart(tmp_path, postgres_repo):
    repo = postgres_repo
    token = new_id("TIER-TOKEN")
    event = new_id("TIER-EVENT")
    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text(
        yaml.safe_dump({"tokens": [{"token_id": token, "symbol": "TEST"}], "instruments": []})
    )
    (config / "semantics.yaml").write_text("metrics: []\n")
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", replay=True)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        before = utcnow()
        await service._record_capture_tier(token, "BASE", "CAPTURE_POLICY_STARTED")
        baseline = utcnow()
        await repo.put(
            "evt.events",
            event,
            {
                "event_id": event,
                "public_at": iso(baseline),
                "public_at_precision": "EXACT",
                "public_at_basis": "ISOLATED_TEST_FIXTURE",
                "known_to_us_at": iso(baseline),
                "statement": "Isolated capture tier fixture",
                "theme_tags": [],
                "freshness": "FRESH",
                "event_state": "RUMOR",
                "state_verified": False,
                "certainty": "HIGH",
                "timing_confidence": "HIGH",
                "best_source_class": "PRIMARY",
                "lifecycle": "DETECTED",
            },
        )
        await service.escalate(event, [token])
        active = utcnow()
        await service.demote(token, event)
        after = utcnow()
        restarted = MarketService(repo, settings, http=http)
        assert (await restarted._capture_metadata(token, before))["tier"] == "UNKNOWN"
        assert (await restarted._capture_metadata(token, baseline))["tier"] == "BASE"
        hot = await restarted._capture_metadata(token, active)
        assert hot["tier"] == "HOT" and hot["collections"]["oi"]["cadence_s"] == 60
        base = await restarted._capture_metadata(token, after)
        assert base["tier"] == "BASE" and base["collections"]["oi"]["cadence_s"] == 300
        rows = [
            row async for row in repo.iter_all("ops.capture_tier_transitions", filters={"token_id": token})
        ]
        assert len(rows) == 3 and all(row["effective_at_prec"] == "COLLECTION_ONLY" for row in rows)


@pytest.mark.process
@pytest.mark.asyncio
async def test_real_postgres_market_precision_funding_and_missing_usd_contract(
    postgres_repo, capture_store_binary
):
    prefix = new_id("PG-MARKET")
    await run_go_producer(
        capture_store_binary,
        postgres_repo,
        "TestProducerPrecisionFundingAndMissingUSD",
        prefix,
    )

    expected = {
        "binance:current": "EXACT",
        "binance:history": "BUCKET(300s)",
        "okx:current": "EXACT",
        "bybit:history": "EXACT",
        "hyperliquid:current": "COLLECTION_ONLY",
    }
    rows = [
        row
        async for row in postgres_repo.iter_all("mkt.oi_obs")
        if row["instrument_id"].startswith(prefix + ":")
    ]
    assert len(rows) == 5
    for row in rows:
        assert (
            row["ts_prec"] == row["timestamp_precision"] == expected[row["instrument_id"][len(prefix) + 1 :]]
        )
        assert row["received_ts"] == "2026-10-21T12:01:02.123000Z"
        if row["instrument_id"].endswith(":hyperliquid:current"):
            assert row["ts"] == row["received_ts"]
            assert row["sidedness"] == "UNKNOWN" and not row["synchronized_eligible"]
    liquidation = await postgres_repo.get("mkt.liq_obs", prefix + ":liq")
    assert liquidation["notional_usd"] is None
    assert liquidation["null_reasons"]["notional_usd"] == "INCOMPLETE"


@pytest.mark.process
@pytest.mark.asyncio
async def test_real_postgres_cached_ticker_and_funding_point_identity_survives_restart(
    tmp_path, postgres_repo, capture_store_binary
):
    prefix = new_id("PG-IDENTITY")
    instrument = {
        "instrument_id": prefix + ":HL",
        "token_id": prefix + ":BTC",
        "venue": "hyperliquid",
        "venue_symbol": "BTC",
        "market_type": "PERP_LINEAR",
        "quote_asset": "USDC",
        "active": True,
    }
    legacy = {**instrument, "instrument_id": prefix + ":legacy"}
    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text(
        yaml.safe_dump(
            {
                "tokens": [{"token_id": instrument["token_id"], "symbol": "BTC"}],
                "instruments": [instrument, legacy],
            }
        )
    )
    (config / "semantics.yaml").write_text("metrics: []\n")
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", replay=True)
    await run_go_producer(
        capture_store_binary,
        postgres_repo,
        "TestProducerTickerFundingIdentityAfterRestart",
        prefix,
    )
    async with httpx.AsyncClient() as client:
        reader = MarketService(postgres_repo, settings, http=client)
        snapshot = await reader.snapshot(instrument["token_id"], iso())
    assert len([row for row in snapshot["funding"] if row["kind"] == "SETTLED"]) == 1
    confirmation = snapshot["funding_confirmations"][0]
    assert confirmation["rate"] == "0.001" and confirmation["primary_source_path"].endswith(
        ":websocket:funding"
    )
    async with postgres_repo.transaction():
        ticker_rows = await (
            await postgres_repo._conn().execute(
                "SELECT instrument_id,ts,count(*) AS n FROM mkt.tickers_24h WHERE instrument_id=ANY(%s) GROUP BY instrument_id,ts",
                [[instrument["instrument_id"], legacy["instrument_id"]]],
            )
        ).fetchall()
        fund_rows = await (
            await postgres_repo._conn().execute(
                "SELECT kind,count(*) AS n FROM mkt.funding_obs WHERE instrument_id=%s GROUP BY kind",
                [instrument["instrument_id"]],
            )
        ).fetchall()
    assert len(ticker_rows) == 3 and all(row["n"] == 1 for row in ticker_rows)
    assert len(fund_rows) == 2 and all(row["n"] == 1 for row in fund_rows)


@pytest.mark.asyncio
async def test_real_postgres_loader_to_condition_compute_selective_read_footprint(
    setup,
    postgres_repo,
    monkeypatch,
):
    _, settings, original_inst = setup
    inst = dict(original_inst, instrument_id=new_id("condition-pg"))
    end = (utcnow() - timedelta(hours=2)).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=15)

    rows = [
        {
            "instrument_id": inst["instrument_id"],
            "token_id": inst["token_id"],
            "venue": inst["venue"],
            "market_type": "SPOT",
            "quote_asset": "USD",
            "minute_start": iso(start + timedelta(minutes=minute)),
            "minute_start_prec": "BUCKET(60s)",
            "buy_qty": "2",
            "sell_qty": "0",
            "buy_notional": "24",
            "sell_notional": "0",
            "sum_sq_qty": "4",
            "size_buckets": {},
            "n_buy": 1,
            "n_sell": 0,
            "complete": True,
            "recorded_at": iso(),
            "recorded_at_prec": "COLLECTION_ONLY",
        }
        for minute in range(15)
    ]
    await postgres_repo.put_market_batch("mkt.flow_bars_1m", rows)
    calls = []
    original_range = postgres_repo.iter_range

    async def ranges(collection, field, **kwargs):
        calls.append((collection, kwargs))
        async for row in original_range(collection, field, **kwargs):
            yield row

    monkeypatch.setattr(postgres_repo, "iter_range", ranges)
    async with httpx.AsyncClient() as client:
        reader = MarketService(postgres_repo, settings, http=client)
        reader.instruments = {inst["instrument_id"]: inst}
        reader.snapshot = AsyncMock(side_effect=AssertionError("No full snapshot"))
        reader.flush_market = AsyncMock(side_effect=AssertionError("No capture flush"))
        requirements = [{"metric": "spot.cohort.direction", "window": "W15M"}]
        packet = await reader.condition_snapshot(inst["token_id"], end, requirements=requirements)
        result = compute_condition_values(packet, requirements)
    assert len(packet["flow_bars"]) == 15
    assert result["spot"]["W15M"]["direction"] == "BUY"
    assert {collection for collection, _ in calls} == {"mkt.flow_bars_1m"}
    assert calls[0][1]["start"] == start


@pytest.mark.process
@pytest.mark.asyncio
async def test_real_go_writer_live_chunks_preempt_backfill_and_retry_only_failed_tail(
    postgres_repo, capture_store_binary
):
    await run_go_producer(
        capture_store_binary,
        postgres_repo,
        "TestWriterLiveChunksAndFailureRetry",
        new_id("PG-LIVE-WRITER"),
    )

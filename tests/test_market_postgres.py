"""Real normalized producer writes against the isolated QA PostgreSQL contract."""

import os

import httpx
import pytest
import yaml

from tank.market import normalize as norm
from tank.market.service import MarketService
from tank.settings import Settings
from tank.storage import PostgresRepository
from tank.util import iso, new_id, utcnow


@pytest.mark.asyncio
async def test_real_postgres_capture_tier_history_survives_demotion_and_restart(tmp_path):
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires isolated QA55437/tank_market_live_write_test")
    destination = conninfo_to_dict(dsn)
    if destination.get("port") != "55437" or not destination.get("dbname", "").startswith("tank_"):
        pytest.skip("Only dedicated isolated QA database is allowed")
    repo = PostgresRepository(dsn)
    await repo.open()
    await repo.migrate()
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
    await repo.close()


@pytest.mark.asyncio
async def test_real_postgres_market_precision_funding_and_missing_usd_contract(tmp_path):
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires isolated QA PostgreSQL on55437/tank_market_live_write_test")
    destination = conninfo_to_dict(dsn)
    if destination.get("port") != "55437" or not destination.get("dbname", "").startswith("tank_"):
        pytest.skip("This regression only writes dedicated QA55437/tank_market_live_write_test")
    repo = PostgresRepository(dsn)
    await repo.open()
    await repo.migrate()
    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text(yaml.safe_dump({"tokens": [], "instruments": []}))
    (config / "semantics.yaml").write_text("metrics: []\n")
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", replay=True)
    now = utcnow()
    received = iso(now)
    timestamp = int(now.timestamp() * 1000)
    bucket = timestamp // 300000 * 300000
    marker = new_id("PG-MARKET")
    specimens = [
        ("binance", "current", {"openInterest": "123.000000000000000001", "time": timestamp}, "EXACT"),
        ("binance", "history", {"sumOpenInterest": "124", "timestamp": bucket}, "BUCKET(300s)"),
        ("okx", "current", {"oiCcy": "125", "ts": str(timestamp)}, "EXACT"),
        ("bybit", "history", {"singleOpenInterest": "126", "timestamp": str(bucket)}, "EXACT"),
        (
            "hyperliquid",
            "current",
            {"openInterest": "127", "_capture_observed_at": received},
            "COLLECTION_ONLY",
        ),
    ]
    instruments = []
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ) as client:
            service = MarketService(repo, settings, http=client)
            for venue, sample_kind, raw, precision in specimens:
                inst = {
                    "instrument_id": f"{marker}:{venue}:{sample_kind}",
                    "token_id": "test:PRECISION",
                    "venue": venue,
                    "venue_symbol": "BTCUSDT",
                    "market_type": "PERP_LINEAR",
                    "quote_asset": "USDT",
                    "contract_multiplier": "1",
                }
                instruments.append(inst)
                record = norm.oi(inst, raw, received_ts=received)
                assert record["ts_prec"] == record["timestamp_precision"] == precision
                await service._put("mkt.oi_obs", inst["instrument_id"] + ":" + record["ts"], record)
                await service.flush_market()
                saved = await repo.get("mkt.oi_obs", inst["instrument_id"] + ":" + record["ts"])
                assert saved["received_ts"] == received
                if venue == "hyperliquid":
                    assert saved["ts"] == received and saved["sidedness"] == "UNKNOWN"
                    assert not saved["synchronized_eligible"]
            async with repo.transaction():
                rows = await (
                    await repo._conn().execute(
                        "SELECT instrument_id,ts_prec,timestamp_precision AS companion FROM mkt.oi_obs WHERE instrument_id = ANY(%s)",
                        [[i["instrument_id"] for i in instruments]],
                    )
                ).fetchall()
            assert len(rows) == 5 and all(r["ts_prec"] == r["companion"] for r in rows)
            contradictory = norm.oi(instruments[0], specimens[0][2], received_ts=received)
            contradictory["ts_prec"] = "COLLECTION_ONLY"
            with pytest.raises(ValueError, match="conflict|Conflicting"):
                await repo.put_market_batch("mkt.oi_obs", [contradictory])
            funding_raw = [
                (
                    {"fundingRate": ".0001", "fundingTime": timestamp},
                    {"lastFundingRate": ".0002", "time": timestamp},
                ),
                (
                    {"fundingRate": ".0001", "fundingTime": timestamp},
                    {"fundingRate": ".0002", "ts": timestamp},
                ),
                ({"fundingRate": ".0001", "fundingRateTimestamp": timestamp}, {"fundingRate": ".0002"}),
                (
                    {"fundingRate": ".0001", "time": timestamp},
                    {"funding": ".0002", "_capture_observed_at": received},
                ),
            ]
            for inst, (settled, current) in zip([instruments[0], *instruments[2:]], funding_raw, strict=True):
                for kind, raw in [("SETTLED", settled), ("CURRENT_ESTIMATE", current)]:
                    row = norm.funding(inst, raw, kind=kind, interval_h="8", received_ts=received)
                    assert row["as_of"]
                    if kind == "SETTLED":
                        assert row["as_of"] == row["funding_time"] == row["settlement_time"]
                    await service._put("mkt.funding_obs", inst["instrument_id"] + ":" + kind, row)
            liq = norm.liquidation(
                instruments[0], {"S": "SELL", "q": "1", "ap": "100", "T": timestamp}, received_ts=received
            )
            assert liq["notional_usd"] is None
            assert liq["null_reasons"]["notional_usd"] == "INCOMPLETE"
            await service._put("mkt.liq_obs", marker + ":liq", liq)
            await service.close()
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_real_postgres_cached_ticker_and_funding_point_identity_survives_restart(tmp_path):
    from datetime import timedelta

    from psycopg.conninfo import conninfo_to_dict

    from tank.market.streams import save_mark
    from tank.util import parse_time

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires dedicated QA55437/tank_market_live_write_test")
    destination = conninfo_to_dict(dsn)
    if destination.get("port") != "55437" or not destination.get("dbname", "").startswith("tank_"):
        pytest.skip("Only dedicated QA database is permitted")
    repo = await PostgresRepository(dsn).open()
    await repo.migrate()
    marker = new_id("PG-IDENTITY")
    instrument = {
        "instrument_id": marker + ":HL",
        "token_id": marker + ":BTC",
        "venue": "hyperliquid",
        "venue_symbol": "BTC",
        "market_type": "PERP_LINEAR",
        "quote_asset": "USDC",
        "active": True,
    }
    legacy = {**instrument, "instrument_id": marker + ":legacy"}
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
    point = iso(utcnow() - timedelta(seconds=10))
    raw = {
        "markPx": "100",
        "oraclePx": "99",
        "dayNtlVlm": "1000",
        "funding": ".001",
        "_capture_observed_at": point,
    }
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ) as client:
            service = MarketService(repo, settings, http=client)
            await service._ticker(instrument, raw)
            await save_mark(
                service,
                instrument,
                "100",
                point,
                index="99",
                index_ts=point,
                source="PUBLIC_REST",
                received_ts=point,
            )
            key = instrument["instrument_id"] + ":" + point
            await service.flush_market()
            original = await repo.get("mkt.tickers_24h", key)
            for _ in range(2):
                await service._ticker(instrument, dict(raw))
                await save_mark(
                    service,
                    instrument,
                    "100",
                    point,
                    index="99",
                    index_ts=point,
                    source="PUBLIC_REST",
                    received_ts=point,
                )
            await service.flush_market()
            assert (await repo.get("mkt.tickers_24h", key))["recorded_at"] == original["recorded_at"]
            restarted = MarketService(repo, settings, http=client)
            older = iso(parse_time(point) - timedelta(seconds=1))
            await save_mark(
                restarted,
                instrument,
                "90",
                older,
                index="99",
                index_ts=point,
                source="PUBLIC_REST",
                received_ts=point,
            )
            await restarted._ticker(instrument, dict(raw))
            await service.flush_market()
            await restarted.flush_market()
            stored = await repo.get("mkt.tickers_24h", key)
            assert stored["mark_price"] == "100" and stored["mark_as_of"] == point
            assert stored["index_price"] == "99" and stored["quote_volume"] == "1000"
            assert stored["ts"] == stored["received_ts"] == point
            assert stored["recorded_at"] == original["recorded_at"]
            assert restarted._price_contexts[instrument["instrument_id"]]["mark_ts"] == point
            later_index = iso(parse_time(point) + timedelta(seconds=1))
            await save_mark(
                restarted,
                instrument,
                "80",
                older,
                index="101",
                index_ts=later_index,
                source="PUBLIC_REST",
                received_ts=point,
            )
            await save_mark(
                restarted,
                instrument,
                "70",
                older,
                index="98",
                index_ts=older,
                source="PUBLIC_REST",
                received_ts=point,
            )
            await service.flush_market()
            await restarted.flush_market()
            newest = await repo.get("mkt.tickers_24h", instrument["instrument_id"] + ":" + later_index)
            assert newest["mark_price"] == "100" and newest["index_price"] == "101"
            assert newest["mark_as_of"] == point and newest["index_as_of"] == later_index
            legacy_key = legacy["instrument_id"] + ":mark:" + point
            await service._put(
                "mkt.tickers_24h", legacy_key, {**original, "instrument_id": legacy["instrument_id"]}
            )
            await restarted._ticker(legacy, dict(raw))
            await service.flush_market()
            await restarted.flush_market()
            assert (await repo.get("mkt.tickers_24h", legacy["instrument_id"] + ":" + point))[
                "index_price"
            ] == "99"
            clocked = {**instrument, "instrument_id": marker + ":clock-provenance"}
            await save_mark(
                service,
                clocked,
                "200",
                older,
                index="201",
                index_ts=point,
                source="WEBSOCKET",
                received_ts=point,
            )
            clock_key = clocked["instrument_id"] + ":" + point
            await service._ticker(clocked, {**raw, "markPx": "199"})
            await service.flush_market()
            await restarted.flush_market()
            clock_row = await repo.get("mkt.tickers_24h", clock_key)
            assert clock_row["mark_price"] == "199" and clock_row["mark_as_of"] == point
            assert clock_row["index_price"] == "201" and clock_row["index_as_of"] == point
            assert clock_row["field_sources"]["mark_price"]["clock_basis"] == "COLLECTION_TIME"
            assert clock_row["field_sources"]["mark_price"]["capture_source"] == "PUBLIC_REST"
            assert clock_row["field_sources"]["index_price"]["capture_source"] == "WEBSOCKET"
            assert clock_row["capture_source"] is None
            assert clock_row["capture_sources"] == ["PUBLIC_REST", "WEBSOCKET"]
            await service.save_ticker_observation(
                clocked,
                {
                    "instrument_id": clocked["instrument_id"],
                    "token_id": clocked["token_id"],
                    "ts": point,
                    "mark_price": "198",
                    "capture_source": "PUBLIC_REST",
                    "received_ts": point,
                },
            )
            await service.flush_market()
            await restarted.flush_market()
            unclocked = await repo.get("mkt.tickers_24h", clock_key)
            assert unclocked["mark_price"] == "199" and unclocked["mark_as_of"] == point
            assert unclocked["unclocked_components"]["mark_price"]["value"] == "198"
            assert unclocked["unclocked_components"]["mark_price"]["as_of"] is None
            assert unclocked["unclocked_components"]["mark_price"]["reason"] == "SOURCE_CLOCK_UNAVAILABLE"
            assert unclocked["field_sources"]["index_price"] == clock_row["field_sources"]["index_price"]
            await save_mark(
                service,
                clocked,
                "190",
                older,
                index="191",
                index_ts=older,
                source="WEBSOCKET",
                received_ts=point,
            )
            await service.flush_market()
            await restarted.flush_market()
            retained = await repo.get("mkt.tickers_24h", clock_key)
            assert retained["mark_price"] == "199" and retained["index_price"] == "201"
            assert retained["field_sources"]["mark_price"] == clock_row["field_sources"]["mark_price"]
            assert retained["field_sources"]["index_price"] == clock_row["field_sources"]["index_price"]
            await service._funding(instrument, raw, "CURRENT_ESTIMATE", "1", source="PUBLIC_REST")
            ws_received = iso(parse_time(point) + timedelta(milliseconds=1))
            ws = {"funding": ".002", "time": point, "_capture_observed_at": ws_received}
            await service._funding(instrument, ws, "CURRENT_ESTIMATE", "1", source="WEBSOCKET")
            fund_key = instrument["instrument_id"] + ":CURRENT_ESTIMATE:" + point
            await service.flush_market()
            await restarted.flush_market()
            funding = await repo.get("mkt.funding_obs", fund_key)
            assert funding["source_disagreement"]["present"]
            observations = funding["source_observations"]
            assert observations["PUBLIC_REST"]["rate"] == "0.001"
            assert observations["WEBSOCKET"]["rate"] == "0.002"
            assert observations["PUBLIC_REST"]["received_ts"] == point
            assert observations["WEBSOCKET"]["received_ts"] == ws_received
            assert all(value["recorded_at"] for value in observations.values())
            await restarted._funding(instrument, ws, "CURRENT_ESTIMATE", "1", source="WEBSOCKET")
            await service.flush_market()
            await restarted.flush_market()
            assert (await repo.get("mkt.funding_obs", fund_key))["recorded_at"] == funding["recorded_at"]
            settled = {"fundingRate": ".0001", "time": point, "_capture_observed_at": point}
            await service._funding(instrument, settled, "SETTLED", "1", source="PUBLIC_REST")
            await restarted._funding(instrument, settled, "SETTLED", "1", source="WEBSOCKET")
            snapshot = await restarted.snapshot(instrument["token_id"], iso())
            assert len([row for row in snapshot["funding"] if row["kind"] == "SETTLED"]) == 1
            confirmation = snapshot["funding_confirmations"][0]
            assert confirmation["rate"] == "0.001" and confirmation["primary_source_path"].endswith(
                ":websocket:funding"
            )
            async with repo.transaction():
                ticker_rows = await (
                    await repo._conn().execute(
                        "SELECT instrument_id,ts,count(*) AS n FROM mkt.tickers_24h WHERE instrument_id=ANY(%s) GROUP BY instrument_id,ts",
                        [[instrument["instrument_id"], legacy["instrument_id"]]],
                    )
                ).fetchall()
                fund_rows = await (
                    await repo._conn().execute(
                        "SELECT kind,count(*) AS n FROM mkt.funding_obs WHERE instrument_id=%s GROUP BY kind",
                        [instrument["instrument_id"]],
                    )
                ).fetchall()
            assert len(ticker_rows) == 3 and all(row["n"] == 1 for row in ticker_rows)
            assert len(fund_rows) == 2 and all(row["n"] == 1 for row in fund_rows)
            await service.close()
            await restarted.close()
    finally:
        await repo.close()

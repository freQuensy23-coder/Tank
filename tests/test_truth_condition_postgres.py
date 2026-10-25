"""Disposable QA PostgreSQL proof: selective market loader → truth, bounded SQL and no MTA writes."""

import json
import os
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from math import sin
from pathlib import Path
from runpy import run_path

import httpx
import pytest

from tank.analysis.truth import MarketTruthService, compute_market_truth
from tank.market.cadences import supply_cadences
from tank.market.service import MarketService
from tank.settings import Settings
from tank.util import iso, parse_time

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.mark.asyncio
async def test_disposable_postgres_selective_condition_inputs_and_sql_footprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, postgres_template
) -> None:
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires an isolated tank_test* PostgreSQL database with CREATEDB")
    if not conninfo_to_dict(dsn).get("dbname", "").startswith("tank_test"):
        pytest.fail("TANK_TEST_DATABASE_URL must select an isolated tank_test* database")
    async with postgres_template.repository(dsn) as repo:
        packet = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
            "synthetic_start_snapshot"
        ]()
        rename = {"A:spot:T": "binance:spot:T", "B:spot:T": "coinbase:spot:T"}
        for values in packet.values():
            if not isinstance(values, list):
                continue
            for row in values:
                if not isinstance(row, dict):
                    continue
                row["instrument_id"] = rename.get(row.get("instrument_id"), row.get("instrument_id"))
                if row.get("venue") == "A":
                    row["venue"] = "binance"
                if row.get("venue") == "B":
                    row["venue"] = "coinbase"

        anchor_time = parse_time(packet["anchor"]["ts"])
        packet["instruments"].append(
            dict(instrument_id="A:spot:BTC", token_id="BTC", venue="binance", market_type="SPOT")
        )
        for iid, token, venue in (
            ("binance:spot:T", "T", "binance"),
            ("coinbase:spot:T", "T", "coinbase"),
            ("A:spot:BTC", "BTC", "binance"),
        ):
            for minute in range(8 * 1440, 0, -1):
                price = "100" if token == "BTC" else str(100 * (1 + 0.02 * sin((1 - minute / 60) / 6)))
                packet["klines"].append(
                    dict(
                        instrument_id=iid,
                        token_id=token,
                        venue=venue,
                        market_type="SPOT",
                        open_time=iso(anchor_time - timedelta(minutes=minute)),
                        interval_s=60,
                        complete=True,
                        o=price,
                        c=price,
                        volume_native="1000",
                        quote_volume="100000",
                        quote_volume_usd="100000",
                    )
                )
        for row in packet["semantics"]:
            venue = row["venue"]
            market = "usdm" if row["market_type"] != "SPOT" else "spot"
            row["metric_key"] = f"{venue}.{market}.{row['metric_family']}"
        for row in packet["oi"]:
            row.update(
                token_id="T",
                metric_key="binance.usdm.oi",
                raw_value=row["oi_native"],
                raw_unit="BASE",
                sidedness="SINGLE_SIDED",
                received_ts=iso(),
            )
        for row in packet["funding"]:
            row.update(
                metric_key="binance.usdm.funding",
                as_of=row["settlement_time"],
                as_of_prec="EXACT",
                funding_time=row["settlement_time"],
                funding_time_prec="EXACT",
                rate_8h_eq=str(Decimal(str(row["rate"])) * 8 / Decimal(str(row["interval_h"]))),
            )
        for row in packet["flow_bars"]:
            row.update(n_buy=20000, n_sell=5000, size_buckets={})
        for row in packet["klines"]:
            row.update(
                h=max(Decimal(str(row["o"])), Decimal(str(row["c"]))),
                l=min(Decimal(str(row["o"])), Decimal(str(row["c"]))),
            )
        for row in packet["books"]:
            row.update(best_bid="101", best_ask="101.2", received_ts=iso())
        for family in ("flow_bars", "klines", "oi", "funding", "books"):
            for row in packet[family]:
                row.update(recorded_at=iso(), recorded_at_prec="COLLECTION_ONLY")
                if row.get("received_ts") is not None:
                    row["received_ts_prec"] = "COLLECTION_ONLY"
                field = {
                    "flow_bars": "minute_start",
                    "klines": "open_time",
                    "oi": "ts",
                    "funding": "settlement_time",
                    "books": "exchange_ts",
                }[family]
                row[field + "_prec"] = (
                    "BUCKET(60s)"
                    if family == "flow_bars"
                    else f"BUCKET({row['interval_s']}s)"
                    if family == "klines"
                    else "EXACT"
                )
        tables = {
            "flow_bars": "mkt.flow_bars_1m",
            "klines": "mkt.klines_1m",
            "oi": "mkt.oi_obs",
            "funding": "mkt.funding_obs",
            "books": "mkt.book_summaries",
        }
        for family, table in tables.items():
            values = [row for row in packet[family] if family != "klines" or row.get("interval_s") == 60]
            await repo.put_market_batch(table, values)
        event = "CONDITION-EVENT"
        await repo.put(
            "evt.events",
            event,
            dict(
                event_id=event,
                statement="Synthetic selective SQL proof",
                public_at=packet["anchor"]["ts"],
                public_at_prec="EXACT",
                public_at_basis="TEST",
                known_to_us_at=packet["anchor"]["ts"],
                event_state="RUMOR",
                state_verified=False,
                certainty="HIGH",
                timing_confidence="HIGH",
                freshness="FRESH",
                best_source_class="PRIMARY",
                lifecycle="ANALYZING",
                theme_tags=[],
            ),
        )
        for index, definition in enumerate(packet["semantics"]):
            iid = next(
                row["instrument_id"]
                for row in packet["instruments"]
                if row["venue"] == definition["venue"] and row["market_type"] == definition["market_type"]
            )
            await repo.put(
                "qa.reviews",
                f"SEM-{index}",
                dict(
                    review_id=f"SEM-{index}",
                    subject="METRIC_SEMANTICS",
                    instrument_id=iid,
                    metric_key=definition["metric_key"],
                    at=iso(),
                    semantics=dict(definition, instrument_id=iid, semantic_confidence="VERIFIED"),
                ),
            )
        await repo.put(
            "ops.capture_tier_transitions",
            "TIER",
            dict(
                transition_id="TIER",
                config_version="test",
                code_version="test",
                transition_seq=1,
                token_id="T",
                tier="BASE",
                effective_at=iso(parse_time(packet["anchor"]["ts"]) - timedelta(days=1)),
                effective_at_prec="COLLECTION_ONLY",
                effective_at_prec_reason=None,
                reason="SYNTHETIC_TEST",
                event_ids=[],
                capture_cadences={
                    "oi_funding_s": 300,
                    "kline_s": 60,
                    "book_s": 10,
                    "supply_by_kind": supply_cadences(False),
                },
            ),
        )
        registry = {
            "instruments": packet["instruments"],
            "tokens": [{"token_id": "T", "symbol": "T"}, {"token_id": "BTC", "symbol": "BTC"}],
        }
        config = tmp_path / "config"
        config.mkdir()
        (config / "registry.yaml").write_text(json.dumps(registry))
        (config / "semantics.yaml").write_text(json.dumps({"metrics": packet["semantics"]}))
        (config / "thresholds.yaml").write_bytes(Settings().read_config_bytes("thresholds.yaml"))
        settings = Settings(config_dir=config, data_dir=tmp_path, config_version="test", code_version="test")
        reads = []
        statements = []
        original_range = repo.iter_range
        original_conn = repo._conn

        async def ranges(collection, field, **kwargs):
            reads.append((collection, kwargs))
            async for row in original_range(collection, field, **kwargs):
                yield row

        repo.iter_range = ranges

        class Probe:
            def __init__(self, conn) -> None:
                self.conn = conn

            async def execute(self, query, *args, **kwargs):
                statements.append(query.as_string(self.conn) if hasattr(query, "as_string") else str(query))
                return await self.conn.execute(query, *args, **kwargs)

            def __getattr__(self, key):
                return getattr(self.conn, key)

        repo._conn = lambda: Probe(original_conn())

        async def forbidden(*args, **kwargs) -> None:
            raise AssertionError("No flush/full snapshot/MTA persistence in condition evaluation")

        monkeypatch.setattr(repo, "put", forbidden)
        monkeypatch.setattr(repo, "put_market_batch", forbidden)
        async with httpx.AsyncClient() as http:
            market = MarketService(repo, settings, http=http)
            monkeypatch.setattr(market, "snapshot", forbidden)
            monkeypatch.setattr(market, "_put", forbidden)
            monkeypatch.setattr(market, "flush_market", forbidden)
            truth = MarketTruthService(repo, settings, market)
            ordinary = [
                dict(metric=m, window="W1H") for m in ("spot.cohort.direction", "price.close_vs_level")
            ]
            values = await truth.condition_values(event, "T", packet["cutoff"], ordinary)
            assert values["spot"]["W1H"]["direction"] == "BUY"
            assert Decimal(values["relative"]["close_1h_price"]) == Decimal("101.1")
            assert {table for table, _ in reads} == {"mkt.flow_bars_1m", "mkt.klines_1m"}
            assert not any(
                '"mkt"."oi_obs"' in query
                or '"mkt"."funding_obs"' in query
                or '"mkt"."book_summaries"' in query
                for query in statements
            )
            reads.clear()
            statements.clear()
            requirements = [dict(metric=m, window="W1H") for m in ("dc.class", "liq.depth_p1_vs_pre")]
            selective = await truth.condition_values(event, "T", packet["cutoff"], requirements)
            loaded = await market.condition_snapshot(
                "T",
                packet["cutoff"],
                anchor={"type": "T1", "ts": packet["anchor"]["ts"], "ts_prec": "EXACT"},
                requirements=requirements,
            )
            full = compute_market_truth(deepcopy(loaded))
            assert selective["data_confidence"]["class"] == full["data_confidence"]["class"] == "HIGH"
            assert Decimal(selective["liquidity"]["aggregate_depth_p1_usd"]) == Decimal(
                full["liquidity"]["aggregate_depth_p1_usd"]
            )
            assert Decimal(selective["liquidity"]["pre_event_depth_p1_usd"]) == Decimal(
                full["liquidity"]["pre_event_depth_p1_usd"]
            )
            assert not selective["_condition_evaluation"]["requires_market_truth"]
            assert all(
                parse_time(packet["cutoff"]) - bounds["start"] <= timedelta(days=1, minutes=1)
                for _, bounds in reads
                if bounds.get("start")
            )
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )
            reads.clear()
            statements.clear()
            composite_requirements = [
                dict(metric=m, window="W1H")
                for m in (
                    "mt.move_class",
                    "mt.divergence_state",
                    "price.repricing_band",
                    "price.vs_anchor_pct",
                )
            ]
            composite = await truth.condition_values(event, "T", packet["cutoff"], composite_requirements)
            assert composite["move_class"] == "SPOT_LED_ACCUMULATION", str(
                dict(
                    dc=composite["data_confidence"]["class"],
                    primary=composite.get("primary_window"),
                    relative=composite.get("relative", {}).get("windows", {}).get("EVENT"),
                    ref=composite.get("relative", {}).get("anchor"),
                    spot=composite.get("spot", {}).get("EVENT", {}).get("classification"),
                    cohorts=composite.get("spot", {}).get("EVENT", {}).get("cohort_valid"),
                    selected=composite.get("move_reasons"),
                    liquidity=composite.get("liquidity", {}).get("elasticity"),
                    oi=composite["derivatives"]["windows"]["EVENT"].get("synchronized"),
                    funding=[
                        (p.get("funding_vs_normal"), p.get("funding_start"))
                        for p in composite["derivatives"]["windows"]["EVENT"].get("per_venue", [])
                    ],
                )
            )
            assert composite["relative"]["windows"]["EVENT"]["r_raw"] > 0
            assert composite["relative"]["repricing_band"] is not None
            assert not composite["_condition_evaluation"]["requires_market_truth"]
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )

            repeated = await truth.condition_values(event, "T", packet["cutoff"], composite_requirements)
            assert repeated["move_class"] == composite["move_class"]
            assert all(p["reused"] for p in repeated["_condition_evaluation"]["proofs"])
        repo._conn = original_conn
        async with repo.transaction():
            assert (
                await (await repo._conn().execute("SELECT COUNT(*) AS n FROM ana.market_truth")).fetchone()
            )["n"] == 0

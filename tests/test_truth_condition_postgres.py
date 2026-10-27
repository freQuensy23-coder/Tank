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
from source_support import fixture_http_admission, fixture_versions
from truth_support import fixture_truth_config

from tank.analysis import funding_history_cache
from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.analysis.condition_graph_contracts import ConditionWindow
from tank.analysis.funding_history import (
    FundingHistoricalPopulation,
    FundingHistoryPolicy,
    funding_historical_population,
)
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.analysis.open_interest import OINativeHistoryBaseline, OIWindowRequest, OpenInterestCalculator
from tank.analysis.truth import MarketTruthService, compute_market_truth
from tank.market.cadences import supply_cadences
from tank.market.service import MarketService
from tank.settings import Settings
from tank.util import clock_iso, iso, parse_time

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.mark.asyncio
async def test_disposable_postgres_selective_condition_inputs_and_sql_footprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, postgres_template
) -> None:
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires an isolated tank_test* PostgreSQL database with CREATEDB")
    database_name = conninfo_to_dict(dsn).get("dbname")
    if not isinstance(database_name, str) or not database_name.startswith("tank_test"):
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
                instrument_id = row.get("instrument_id")
                if isinstance(instrument_id, str):
                    row["instrument_id"] = rename.get(instrument_id, instrument_id)
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
                price = (
                    str(100 + 0.02 * sin((1 - minute) / 60 * 1.7))
                    if token == "BTC"
                    else str(100 * (1 + 0.02 * sin((1 - minute / 60) / 6)))
                )
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
                received_ts=clock_iso(),
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
            row.update(best_bid="101", best_ask="101.2", received_ts=clock_iso())
        for family in ("flow_bars", "klines", "oi", "funding", "books"):
            for row in packet[family]:
                row.update(recorded_at=clock_iso(), recorded_at_prec="COLLECTION_ONLY")
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
                    at=clock_iso(),
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

        original_market_batch = repo.put_market_batch
        monkeypatch.setattr(repo, "put", forbidden)
        monkeypatch.setattr(repo, "put_market_batch", forbidden)
        async with httpx.AsyncClient() as http:
            market_service = MarketService(
                repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
            )
            monkeypatch.setattr(market_service, "snapshot", forbidden)
            monkeypatch.setattr(market_service, "_put", forbidden)
            monkeypatch.setattr(market_service, "flush_market", forbidden)
            truth = MarketTruthService(repo, settings, market_service, versions=fixture_versions())
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
            loaded = await market_service.condition_snapshot(
                "T",
                packet["cutoff"],
                anchor={"type": "T1", "ts": packet["anchor"]["ts"], "ts_prec": "EXACT"},
                requirements=requirements,
            )
            full = compute_market_truth(deepcopy(loaded), config=fixture_truth_config())
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
            assert composite["relative"]["beta_btc"] is not None
            assert composite["relative"]["beta_flags"]["btc"] is None
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

            original_native_baseline = OpenInterestCalculator.native_baseline
            population_calls: list[ConditionWindow] = []

            def counted_native_baseline(
                calculator: OpenInterestCalculator, request: OIWindowRequest, *, window: ConditionWindow
            ) -> tuple[OINativeHistoryBaseline, ...]:
                population_calls.append(window)
                return original_native_baseline(calculator, request, window=window)

            monkeypatch.setattr(OpenInterestCalculator, "native_baseline", counted_native_baseline)
            oi_requirements = [
                {"metric": "deriv.native_oi_vs_normal", "window": "W1H"},
                {"metric": "deriv.oi_class", "window": "W1H"},
            ]
            reads.clear()
            statements.clear()
            oi_first = await truth.condition_values(event, "T", packet["cutoff"], oi_requirements)
            initial_oi = oi_first["derivatives"]["windows"]["W1H"]["synchronized"]
            assert Decimal(initial_oi["oi_native_start"]) == Decimal(initial_oi["oi_native_end"]) == 1000
            assert Decimal(initial_oi["oi_usd_start"]) == Decimal(initial_oi["oi_usd_end"]) == 100000
            assert population_calls == ["W1H"]
            assert all(point["available"] for point in oi_first["_condition_evaluation"]["proofs"])
            assert not {"mkt.funding_obs", "mkt.flow_bars_1m", "mkt.klines_1m", "mkt.book_summaries"} & {
                table for table, _ in reads
            }
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )
            oi_repeated = await truth.condition_values(event, "T", packet["cutoff"], oi_requirements)
            assert all(point["reused"] for point in oi_repeated["_condition_evaluation"]["proofs"])
            assert oi_repeated["derivatives"] == oi_first["derivatives"]
            assert population_calls == ["W1H"]

            corrected_oi = dict(
                packet["oi"][-1],
                oi_native="1001",
                raw_value="1001",
                received_ts=clock_iso(),
                recorded_at=clock_iso(),
            )
            await original_market_batch("mkt.oi_obs", [corrected_oi])
            statements.clear()
            oi_corrected = await truth.condition_values(event, "T", packet["cutoff"], oi_requirements)
            current_oi = oi_corrected["derivatives"]["windows"]["W1H"]["synchronized"]
            assert Decimal(current_oi["oi_native_end"]) == 1001
            assert current_oi["oi_class"] == "MIXED"
            assert population_calls == ["W1H"]
            assert [point["reused"] for point in oi_corrected["_condition_evaluation"]["proofs"]] == [
                False,
                True,
            ]
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )

            future_oi = dict(
                corrected_oi,
                oi_native="5000",
                raw_value="5000",
                received_ts=iso(parse_time(clock_iso()) + timedelta(days=1)),
                recorded_at=clock_iso(),
            )
            await original_market_batch("mkt.oi_obs", [future_oi])
            statements.clear()
            oi_invisible = await truth.condition_values(event, "T", packet["cutoff"], oi_requirements)
            assert oi_invisible["derivatives"]["windows"]["W1H"]["synchronized"] is None
            assert not any(point["available"] for point in oi_invisible["_condition_evaluation"]["proofs"])
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )
            funding_population_calls: list[str] = []
            original_funding_population = funding_historical_population

            def counted_funding_population(
                calculator: FundingCalculator,
                request: FundingWindowRequest,
                *,
                instrument: DerivativeInstrument,
                policy: FundingHistoryPolicy,
            ) -> FundingHistoricalPopulation:
                population = original_funding_population(
                    calculator, request, instrument=instrument, policy=policy
                )
                funding_population_calls.append(population.identity())
                return population

            monkeypatch.setattr(
                funding_history_cache, "funding_historical_population", counted_funding_population
            )
            funding_requirements = [{"metric": "deriv.funding_vs_normal", "window": "W1H"}]
            reads.clear()
            statements.clear()
            funding_first = await truth.condition_values(event, "T", packet["cutoff"], funding_requirements)
            first_funding_row = funding_first["derivatives"]["windows"]["W1H"]["per_venue"][0]
            first_funding_proof = funding_first["_condition_evaluation"]["proofs"][0]
            assert first_funding_row["funding_8h_kind"] == "SETTLED"
            assert Decimal(first_funding_row["funding_8h_eq"]) == 0
            assert first_funding_row["funding_vs_normal"] == "WITHIN"
            assert first_funding_proof["available"] is True
            assert len(funding_population_calls) == 1
            assert {table for table, _ in reads} == {"mkt.funding_obs"}
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )
            funding_repeat = await truth.condition_values(event, "T", packet["cutoff"], funding_requirements)
            repeat_funding_proof = funding_repeat["_condition_evaluation"]["proofs"][0]
            assert repeat_funding_proof["reused"] is True
            assert repeat_funding_proof["assembled_at"] != first_funding_proof["assembled_at"]
            assert funding_repeat["derivatives"] == funding_first["derivatives"]
            assert len(funding_population_calls) == 1

            current_funding = dict(
                packet["funding"][-1],
                kind="CURRENT_ESTIMATE",
                rate="0.001",
                rate_8h_eq="0.001",
                as_of=packet["cutoff"],
                received_ts=clock_iso(),
                recorded_at=clock_iso(),
            )
            current_funding.pop("funding_time", None)
            current_funding.pop("funding_time_prec", None)
            current_funding.pop("settlement_time", None)
            await original_market_batch("mkt.funding_obs", [current_funding])
            statements.clear()
            funding_current = await truth.condition_values(event, "T", packet["cutoff"], funding_requirements)
            current_funding_row = funding_current["derivatives"]["windows"]["W1H"]["per_venue"][0]
            assert current_funding_row["funding_8h_kind"] == "CURRENT_ESTIMATE"
            assert Decimal(current_funding_row["funding_8h_eq"]) == Decimal("0.001")
            assert current_funding_row["funding_vs_normal"] == "ABOVE"
            assert (
                current_funding_row["funding_normal_range_8h"] == first_funding_row["funding_normal_range_8h"]
            )
            assert len(funding_population_calls) == 1
            assert funding_current["_condition_evaluation"]["proofs"][0]["reused"] is False
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )

            historical_funding = dict(
                packet["funding"][0],
                rate="-0.0001",
                rate_8h_eq="-0.0001",
                received_ts=clock_iso(),
                recorded_at=clock_iso(),
            )
            await original_market_batch("mkt.funding_obs", [historical_funding])
            statements.clear()
            funding_historical = await truth.condition_values(
                event, "T", packet["cutoff"], funding_requirements
            )
            assert len(funding_population_calls) == 2
            assert funding_population_calls[0] != funding_population_calls[1]
            assert (
                funding_historical["derivatives"]["windows"]["W1H"]["per_venue"][0]["funding_vs_normal"]
                == "ABOVE"
            )
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )

            future_funding = dict(
                current_funding,
                received_ts=iso(parse_time(clock_iso()) + timedelta(days=1)),
                recorded_at=clock_iso(),
            )
            await original_market_batch("mkt.funding_obs", [future_funding])
            statements.clear()
            funding_invisible = await truth.condition_values(
                event, "T", packet["cutoff"], funding_requirements
            )
            invisible_funding_row = funding_invisible["derivatives"]["windows"]["W1H"]["per_venue"][0]
            assert invisible_funding_row["funding_estimate_8h_eq"] is None
            assert invisible_funding_row["funding_8h_kind"] == "SETTLED"
            assert Decimal(invisible_funding_row["funding_8h_eq"]) == 0
            assert not any(
                query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in statements
            )
        repo._conn = original_conn
        async with repo.transaction():
            assert (
                await (await repo._conn().execute("SELECT COUNT(*) AS n FROM ana.market_truth")).fetchone()
            )["n"] == 0

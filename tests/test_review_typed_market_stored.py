"""Actual normalized flow producer, ClickHouse batch storage and ordinary horizon review."""

import os
from contextlib import AsyncExitStack
from datetime import timedelta

import pytest

from tank.market import normalize
from tank.market.flow import FlowAccumulator
from tank.review import ReviewService, price_return
from tank.storage import MemoryRepository
from tank.util import iso, new_id, utcnow


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("subminute", [False, True])
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("database", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_actual_flow_batch_producer_to_stored_review(subminute, backend, postgres_template):
    async with AsyncExitStack() as resources:
        if backend == "database":
            url = os.getenv("TANK_TEST_DATABASE_URL")
            if not url or not os.getenv("CLICKHOUSE_TEST_URL"):
                pytest.skip("requires isolated TANK_TEST_DATABASE_URL and CLICKHOUSE_TEST_URL")
            repo = await resources.enter_async_context(postgres_template.repository(url))
        else:
            repo = await MemoryRepository().open()
            await repo.migrate()
            resources.push_async_callback(repo.close)
        prefix = new_id("typed-review")
        start = (utcnow() - timedelta(hours=2)).replace(second=0, microsecond=0)
        floor = start
        if subminute:
            start += timedelta(seconds=30)
        end = start + timedelta(hours=1)
        instrument = {
            "instrument_id": prefix,
            "token_id": "T",
            "venue": "binance",
            "market_type": "SPOT",
            "venue_symbol": "TUSDT",
            "quote_asset": "USDT",
            "quote_usd_rate": "1",
            "contract_multiplier": "1",
        }
        accumulator = FlowAccumulator()
        first_price = "10000000000000000.01"
        entry = "10000000000000000.00"
        close = "10000000000000000.02"
        accumulator.register_boundaries(prefix, [start, end])
        count = 61 if subminute else 60
        for minute in range(count):
            for index, (second, price) in enumerate([(10, first_price), (50, close)]):
                raw = {
                    "a": minute * 2 + index + 1,
                    "p": price,
                    "q": "1",
                    "T": int((floor + timedelta(minutes=minute, seconds=second)).timestamp() * 1000),
                    "m": False,
                }
                accumulator.add(normalize.trade(instrument, raw))
        accumulator.prove_range(
            prefix, floor, floor + timedelta(minutes=count), complete=True, source="EXHAUSTED_REST_RANGE"
        )
        records = [accumulator.record(key, complete=True) for key in accumulator.bars]
        assert all(
            row["first_trade"]["price"] == first_price and row["last_trade"]["price"] == close
            for row in records
        )
        changed = await repo.put_market_batch("mkt.flow_bars_1m", records)
        assert len(changed) == count
        stored = await repo.list_latest(
            "mkt.flow_bars_1m", "minute_start", filters={"instrument_id": prefix}, limit=1
        )
        assert stored[0]["last_trade"]["price"] == close
        result = await ReviewService(repo).outcome(
            {"alert_id": prefix, "version": 1, "event_id": prefix},
            {
                "dec_id": prefix,
                "token_id": "T",
                "cutoff": iso(start),
                "cutoff_prec": "EXACT",
                "outlook": {"best_horizon": "H1"},
            },
            {"relative": {"ref_venue": prefix, "entry_ref_price": entry}},
            now=iso(),
        )
        assert result["status"] == "COMPLETED"
        assert result["coverage"]["endpoint_source"] == "TYPED_LAST_TRADE"
        assert (
            result["realized_return"] == float(price_return(first_price if subminute else close, entry)) > 0
        )
        assert result["coverage"]["partial_boundary"] is subminute
        if subminute:
            assert all(piece["complete"] for piece in result["coverage"]["boundary_segments"])
        assert result["start_prec"] == result["end_prec"] == "EXACT"
        assert result["computed_at_prec"] == "COLLECTION_ONLY"

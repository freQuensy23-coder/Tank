"""Actual producer-to-PostgreSQL contract for native flow without quote/USD FX."""

import os
from decimal import Decimal

import psycopg
import pytest
import pytest_asyncio

from tank.market.flow import FlowAccumulator
from tank.market.normalize import trade
from tank.util import iso, new_id, utcnow

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest_asyncio.fixture
async def native_flow_repo(postgres_template):
    url = os.environ.get("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires an explicitly isolated PostgreSQL test database")
    async with postgres_template.repository(url) as repo:
        await repo.migrate()
        yield repo


def native_bar():
    at = utcnow().replace(second=0, microsecond=0)
    instrument = {
        "instrument_id": new_id("native-flow-test"),
        "token_id": "native-flow-test",
        "venue": "binance",
        "market_type": "SPOT",
        "quote_asset": "ETH",
    }
    accumulator = FlowAccumulator()
    for number, size, maker in ((1, "3", False), (2, "2", True)):
        normalized = trade(
            instrument,
            {"a": number, "p": "2.5", "q": size, "m": maker, "T": int(at.timestamp() * 1000)},
            received_ts=at,
            source="ARCHIVE",
        )
        accumulator.add(normalized)
    return accumulator.record((instrument["instrument_id"], iso(at)), complete=True)


async def test_native_flow_roundtrip_preserves_quantities_and_unavailable_usd(native_flow_repo):
    repo = native_flow_repo
    row = native_bar()
    key = new_id("native-flow-row")
    await repo.put("mkt.flow_bars_1m", key, row)
    persisted = await repo.get("mkt.flow_bars_1m", key)
    assert {field: persisted[field] for field in row} == row
    async with repo.transaction():
        typed = await (
            await repo._conn().execute(
                "SELECT buy_qty,sell_qty,n_buy,n_sell,sum_sq_qty,buy_usd,sell_usd,size_buckets,complete "
                "FROM mkt.flow_bars_1m WHERE storage_key=%s",
                (key,),
            )
        ).fetchone()
    assert typed == {
        "buy_qty": Decimal(3),
        "sell_qty": Decimal(2),
        "n_buy": 1,
        "n_sell": 1,
        "sum_sq_qty": Decimal(13),
        "buy_usd": None,
        "sell_usd": None,
        "size_buckets": None,
        "complete": True,
    }
    assert row["notional_reason"] == "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"
    assert all(row["null_reasons"][name] == "INCOMPLETE" for name in ("buy_usd", "sell_usd", "size_buckets"))


@pytest.mark.parametrize("field", ["buy_usd", "sell_usd", "size_buckets"])
@pytest.mark.parametrize("reason", [None, "", " ", "made up", {"reason": "INCOMPLETE"}])
async def test_missing_usd_requires_its_own_valid_reason(native_flow_repo, field, reason):
    row = native_bar()
    row["null_reasons"][field] = reason
    with pytest.raises(psycopg.errors.CheckViolation) as failure:
        await native_flow_repo.put("mkt.flow_bars_1m", new_id("invalid-native-flow"), row)
    assert failure.value.diag.constraint_name == "required_" + field


@pytest.mark.parametrize("field", ["buy_qty", "sell_qty", "n_buy", "n_sell", "sum_sq_qty", "complete"])
async def test_usd_reason_does_not_make_native_fields_optional(native_flow_repo, field):
    row = native_bar()
    row[field] = None
    row["null_reasons"][field] = "INCOMPLETE"
    with pytest.raises(psycopg.errors.CheckViolation) as failure:
        await native_flow_repo.put("mkt.flow_bars_1m", new_id("invalid-native-flow"), row)
    assert failure.value.diag.constraint_name == "required_" + field


async def test_json_null_buckets_cannot_bypass_missing_reason(native_flow_repo):
    from psycopg.types.json import Jsonb

    repo = native_flow_repo
    row = native_bar()
    key = new_id("json-null-buckets")
    await repo.put("mkt.flow_bars_1m", key, row)
    with pytest.raises(psycopg.errors.CheckViolation) as failure:
        async with repo.transaction():
            await repo._conn().execute(
                "UPDATE mkt.flow_bars_1m SET size_buckets=%s, "
                "null_reasons=null_reasons - 'size_buckets' WHERE storage_key=%s",
                (Jsonb(None), key),
            )
    assert failure.value.diag.constraint_name == "required_size_buckets"
    persisted = await repo.get("mkt.flow_bars_1m", key)
    assert {field: persisted[field] for field in row} == row

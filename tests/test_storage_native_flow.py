"""Actual producer-to-ClickHouse contract for native flow without quote/USD FX."""

import os
from collections.abc import AsyncIterator
from typing import Literal

import pytest
import pytest_asyncio
from foundation_helpers import object_field, present
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

from tank.clickhouse_market import RequiredFieldError
from tank.contracts.json import JsonDocument
from tank.market.flow import FlowAccumulator
from tank.market.normalize import trade
from tank.market.trade_contracts import FlowBar, FlowInstrument, QuoteUsdValuation
from tank.models import Record
from tank.storage import PostgresRepository
from tank.util import iso, new_id, utcnow

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

type UsdField = Literal["buy_usd", "sell_usd", "size_buckets"]
type NativeField = Literal["buy_qty", "sell_qty", "n_buy", "n_sell", "sum_sq_qty", "complete"]


@pytest_asyncio.fixture
async def native_flow_repo(postgres_template: PostgresTemplate) -> AsyncIterator[PostgresRepository]:
    url = os.environ.get("TANK_TEST_DATABASE_URL")
    if not url or not os.environ.get("CLICKHOUSE_TEST_URL"):
        pytest.fail("Requires isolated TANK_TEST_DATABASE_URL and CLICKHOUSE_TEST_URL")
    async with postgres_template.repository(url) as repo:
        await repo.migrate()
        yield repo


def native_bar() -> FlowBar:
    at = utcnow().replace(second=0, microsecond=0)
    instrument = FlowInstrument(
        instrument_id=new_id("native-flow-test"),
        token_id="native-flow-test",
        venue="binance",
        market_type="SPOT",
        quote_asset="ETH",
        venue_symbol=None,
        contract_multiplier=None,
    )
    valuation = QuoteUsdValuation(
        rate=None,
        as_of=None,
        basis="UNAVAILABLE",
        unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE",
    )
    accumulator = FlowAccumulator(now=utcnow)
    for number, size, maker in ((1, "3", False), (2, "2", True)):
        raw: Record = {"a": number, "p": "2.5", "q": size, "m": maker, "T": int(at.timestamp() * 1000)}
        normalized = trade(instrument, raw, valuation=valuation, received_ts=at, source="ARCHIVE")
        accumulator.add(normalized)
    return accumulator.record((instrument.instrument_id, iso(at)), complete=True)


def storage_wire(bar: FlowBar) -> Record:
    document = JsonDocument.validate_value(bar.model_dump(mode="json", warnings="error"))
    assert isinstance(document, dict)
    return document


async def test_native_flow_roundtrip_preserves_quantities_and_unavailable_usd(
    native_flow_repo: PostgresRepository,
) -> None:
    typed_fields = (
        "buy_qty",
        "sell_qty",
        "n_buy",
        "n_sell",
        "sum_sq_qty",
        "buy_usd",
        "sell_usd",
        "size_buckets",
        "complete",
    )
    repo = native_flow_repo
    bar = native_bar()
    assert bar.buy_notional is None and bar.sell_notional is None and bar.size_buckets is None
    row = storage_wire(bar)
    key = new_id("native-flow-row")
    await repo.put("mkt.flow_bars_1m", key, row)
    persisted = present(await repo.get("mkt.flow_bars_1m", key))
    assert {field: persisted[field] for field in row} == row
    assert {field: persisted[field] for field in typed_fields} == {
        "buy_qty": "3",
        "sell_qty": "2",
        "n_buy": 1,
        "n_sell": 1,
        "sum_sq_qty": "13",
        "buy_usd": None,
        "sell_usd": None,
        "size_buckets": None,
        "complete": True,
    }
    assert row["notional_reason"] == "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"
    assert all(
        object_field(row, "null_reasons")[name] == "INCOMPLETE"
        for name in ("buy_usd", "sell_usd", "size_buckets")
    )


@pytest.mark.parametrize("field", ["buy_usd", "sell_usd", "size_buckets"])
@pytest.mark.parametrize("reason", [None, "", " ", "made up", {"reason": "INCOMPLETE"}])
async def test_missing_usd_requires_its_own_valid_reason(
    native_flow_repo: PostgresRepository, field: UsdField, reason: JsonValue
) -> None:
    row = storage_wire(native_bar())
    object_field(row, "null_reasons")[field] = reason
    with pytest.raises(RequiredFieldError) as failure:
        await native_flow_repo.put("mkt.flow_bars_1m", new_id("invalid-native-flow"), row)
    assert failure.value.constraint == "required_" + field


@pytest.mark.parametrize("field", ["buy_qty", "sell_qty", "n_buy", "n_sell", "sum_sq_qty", "complete"])
async def test_usd_reason_does_not_make_native_fields_optional(
    native_flow_repo: PostgresRepository, field: NativeField
) -> None:
    row = storage_wire(native_bar())
    row[field] = None
    object_field(row, "null_reasons")[field] = "INCOMPLETE"
    with pytest.raises(RequiredFieldError) as failure:
        await native_flow_repo.put("mkt.flow_bars_1m", new_id("invalid-native-flow"), row)
    assert failure.value.constraint == "required_" + field


async def test_null_buckets_cannot_bypass_missing_reason(native_flow_repo: PostgresRepository) -> None:
    repo = native_flow_repo
    row = storage_wire(native_bar())
    key = new_id("null-buckets")
    await repo.put("mkt.flow_bars_1m", key, row)
    broken: Record = dict(
        row,
        size_buckets=None,
        null_reasons={
            key: value for key, value in object_field(row, "null_reasons").items() if key != "size_buckets"
        },
    )
    with pytest.raises(RequiredFieldError) as failure:
        await repo.put("mkt.flow_bars_1m", key, broken)
    assert failure.value.constraint == "required_size_buckets"
    persisted = present(await repo.get("mkt.flow_bars_1m", key))
    assert {field: persisted[field] for field in row} == row

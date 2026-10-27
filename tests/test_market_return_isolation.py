import pytest
from foundation_helpers import object_field, present
from test_clickhouse_market import Both
from test_clickhouse_market import both as both
from test_storage import repo as repo
from test_storage_native_flow import native_bar, storage_wire

from tank.storage import StorageBackend


async def assert_returned_flow_isolated(repository: StorageBackend) -> None:
    original = storage_wire(native_bar())
    expected_trade = dict(object_field(original, "first_trade"))
    returned = await repository.put_market_batch("mkt.flow_bars_1m", [original], keys=["point"])
    assert len(returned) == 1
    object_field(original, "first_trade")["price"] = "888"
    stored = present(await repository.get("mkt.flow_bars_1m", "point"))
    assert object_field(stored, "first_trade") == expected_trade
    object_field(returned[0], "first_trade")["price"] = "999"
    later = present(await repository.get("mkt.flow_bars_1m", "point"))
    assert object_field(later, "first_trade") == expected_trade


async def test_market_batch_return_cannot_mutate_persisted_native_trade(repo: StorageBackend) -> None:
    await assert_returned_flow_isolated(repo)


@pytest.mark.integration
async def test_routed_market_batch_return_cannot_mutate_persisted_native_trade(both: Both) -> None:
    _, routed, _ = both
    await assert_returned_flow_isolated(routed)

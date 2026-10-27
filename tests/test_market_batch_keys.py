import pytest
from foundation_helpers import present
from test_clickhouse_market import Both
from test_clickhouse_market import both as both
from test_storage import repo as repo
from test_storage_native_flow import native_bar, storage_wire

from tank.market_storage import WRITE_CHUNK
from tank.storage import StorageBackend


async def assert_parallel_key_refusal(repository: StorageBackend, difference: int) -> None:
    point = storage_wire(native_bar())
    count = WRITE_CHUNK + 1
    with pytest.raises(ValueError, match="keys and records must have equal lengths"):
        await repository.put_market_batch(
            "mkt.flow_bars_1m", [point] * count, keys=["point"] * (count + difference)
        )
    assert await repository.get("mkt.flow_bars_1m", "point") is None


@pytest.mark.parametrize("difference", [-1, 1])
async def test_parallel_key_cardinality_refuses_before_batch_effects(
    repo: StorageBackend, difference: int
) -> None:
    await assert_parallel_key_refusal(repo, difference)


@pytest.mark.integration
@pytest.mark.parametrize("difference", [-1, 1])
async def test_routed_parallel_key_cardinality_refuses_before_batch_effects(
    both: Both, difference: int
) -> None:
    _, routed, _ = both
    await assert_parallel_key_refusal(routed, difference)


async def test_valid_parallel_keys_preserve_same_identity_coalescing(repo: StorageBackend) -> None:
    point = storage_wire(native_bar())
    count = WRITE_CHUNK + 1
    changed = await repo.put_market_batch("mkt.flow_bars_1m", [point] * count, keys=["point"] * count)
    assert len(changed) == 1
    assert present(await repo.get("mkt.flow_bars_1m", "point"))["buy_qty"] == point["buy_qty"]

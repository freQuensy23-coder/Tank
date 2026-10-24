import asyncio

import pytest
from postgres_helpers import _finish


async def test_cleanup_finishes_before_repeated_cancellation_is_propagated() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    effects: list[str] = []

    async def cleanup() -> None:
        entered.set()
        await release.wait()
        effects.append("resource released")

    owner = asyncio.create_task(_finish(cleanup()))
    await entered.wait()
    owner.cancel()
    await asyncio.sleep(0)
    owner.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await owner
    assert effects == ["resource released"]


async def test_cleanup_failure_is_not_silently_replaced_with_success() -> None:
    async def cleanup() -> None:
        raise RuntimeError("owned resource close failed")

    with pytest.raises(RuntimeError, match="owned resource close failed"):
        await _finish(cleanup())


async def test_cleanup_can_return_the_actual_operation_result() -> None:
    async def cleanup() -> int:
        await asyncio.sleep(0)
        return 73

    assert await _finish(cleanup()) == 73

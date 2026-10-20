"""Memory advisory locks model task-owned key exclusion, not remote transaction scope."""

import asyncio

import pytest

from tank.storage import MemoryRepository


async def test_different_keys_and_child_persistence_progress_during_remote_wait():
    repo = MemoryRepository()
    async with repo.lock("route:item"):

        async def score_and_persist():
            async with repo.lock("score:candidate"):
                await repo.put("reg.tokens", "child", {"token_id": "child"})

        await asyncio.wait_for(asyncio.create_task(score_and_persist()), 1)
        assert await repo.get("reg.tokens", "child") == {"token_id": "child"}
    entered = set()
    both = asyncio.Event()

    async def independent(key):
        async with repo.lock(key):
            entered.add(key)
            if len(entered) == 2:
                both.set()
            await both.wait()

    await asyncio.wait_for(asyncio.gather(independent("A"), independent("B")), 1)
    assert not repo._locks


async def test_same_key_reentry_and_child_does_not_inherit_ownership():
    repo = MemoryRepository()
    started = asyncio.Event()
    acquired = asyncio.Event()

    async def child():
        started.set()
        async with repo.lock("shared"):
            acquired.set()

    async with repo.lock("shared"):
        async with repo.lock("shared"):
            task = asyncio.create_task(child())
            await started.wait()
            await asyncio.sleep(0)
            assert not acquired.is_set()
        await asyncio.sleep(0)
        assert not acquired.is_set()
    await asyncio.wait_for(task, 1)
    assert acquired.is_set() and not repo._locks


async def test_cancelled_waiter_does_not_release_another_tasks_key():
    repo = MemoryRepository()
    started = asyncio.Event()
    acquired = []

    async def contender():
        started.set()
        async with repo.lock("shared"):
            acquired.append(True)

    async with repo.lock("shared"):
        task = asyncio.create_task(contender())
        await started.wait()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with repo.lock("shared"):
            pass
        assert not acquired
    async with repo.lock("shared"):
        acquired.append(True)
    assert acquired == [True] and not repo._locks


async def test_explicit_transaction_rollback_stays_atomic_inside_key_lock():
    repo = MemoryRepository()
    async with repo.lock("mutation"):
        with pytest.raises(ValueError):
            async with repo.transaction():
                await repo.put("reg.tokens", "rollback", {"token_id": "rollback"})
                raise ValueError("rollback explicitly scoped work")
        assert await repo.get("reg.tokens", "rollback") is None
        with pytest.raises(ValueError):
            async with repo.transaction():
                await repo.put("reg.tokens", "outer", {"token_id": "outer"})
                async with repo.transaction():
                    await repo.put("reg.tokens", "nested", {"token_id": "nested"})
                raise ValueError("rollback both levels")
        assert await repo.get("reg.tokens", "outer") is None
        assert await repo.get("reg.tokens", "nested") is None
    with pytest.raises(ValueError):
        async with repo.lock("advisory-only"):
            await repo.put("reg.tokens", "committed", {"token_id": "committed"})
            raise ValueError("an advisory key is not an implicit transaction")
    assert await repo.get("reg.tokens", "committed") == {"token_id": "committed"}


async def test_inverted_contended_lock_order_fails_without_hanging_transaction():
    repo = MemoryRepository()
    held = asyncio.Event()
    release = asyncio.Event()

    async def holder():
        async with repo.lock("shared"):
            held.set()
            await release.wait()

    task = asyncio.create_task(holder())
    await held.wait()
    try:
        with pytest.raises(RuntimeError, match="before explicit transactions"):
            async with repo.transaction():
                await repo.put("reg.tokens", "rollback", {"token_id": "rollback"})
                async with repo.lock("shared"):
                    pass
        assert await repo.get("reg.tokens", "rollback") is None
    finally:
        release.set()
        await task
    assert not repo._locks


async def test_cancelled_owner_releases_key_for_next_task():
    repo = MemoryRepository()
    held = asyncio.Event()

    async def owner():
        async with repo.lock("shared"), repo.lock("shared"):
            held.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(owner())
    await held.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with repo.lock("shared"):
        await repo.put("reg.tokens", "after-cancel", {"token_id": "after-cancel"})
    assert await repo.get("reg.tokens", "after-cancel") == {"token_id": "after-cancel"}
    assert not repo._locks

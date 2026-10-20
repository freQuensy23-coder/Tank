"""Lifecycle upkeep must serialize with accepted event updates (TDD §3.7)."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.mark.asyncio
async def test_lifecycle_rereads_candidate_after_concurrent_state_update(monkeypatch):
    repo = MemoryRepository()
    now = utcnow()
    original = {"event_id": "e", "lifecycle": "TRACKED", "known_to_us_at": iso(now - timedelta(days=8))}
    await repo.put("evt.events", "e", original)
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)

    async def stale_candidates(*args, **kwargs):
        async with repo.lock("event:e"), repo.transaction():
            await repo.put("evt.events", "e", {**original, "lifecycle": "SCORED", "new_fact": "accepted"})
        yield original

    monkeypatch.setattr(scheduler, "active_events", stale_candidates)
    await scheduler.lifecycle(now)
    event = await repo.get("evt.events", "e")
    assert event["lifecycle"] == "SCORED"
    assert event["new_fact"] == "accepted"
    assert "closed_at" not in event


@pytest.mark.asyncio
async def test_lifecycle_cannot_overwrite_update_between_predicate_read_and_write(monkeypatch):
    repo = MemoryRepository()
    now = utcnow()
    original = {"event_id": "e", "lifecycle": "TRACKED", "known_to_us_at": iso(now - timedelta(days=8))}
    await repo.put("evt.events", "e", original)
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    read_started, release_read, update_attempted = (asyncio.Event() for _ in range(3))
    list_latest = repo.list_latest

    async def gated_observations(collection, field, **kwargs):
        rows = await list_latest(collection, field, **kwargs)
        if collection == "evt.event_observations":
            read_started.set()
            await release_read.wait()
        return rows

    async def accepted_update():
        update_attempted.set()
        async with repo.lock("event:e"), repo.transaction():
            current = await repo.get("evt.events", "e")
            await repo.put("evt.events", "e", {**current, "lifecycle": "SCORED", "new_fact": "accepted"})
            await repo.put(
                "evt.event_observations",
                "fresh",
                {"observation_id": "fresh", "event_id": "e", "observed_at": iso(now)},
            )

    monkeypatch.setattr(repo, "list_latest", gated_observations)
    upkeep = asyncio.create_task(scheduler.lifecycle(now))
    await asyncio.wait_for(read_started.wait(), 1)
    update = asyncio.create_task(accepted_update())
    await asyncio.wait_for(update_attempted.wait(), 1)
    # The update either acquired the real event lock or is waiting on it.
    release_read.set()
    await asyncio.wait_for(asyncio.gather(upkeep, update), 1)
    event = await repo.get("evt.events", "e")
    assert event["lifecycle"] == "SCORED"
    assert event["new_fact"] == "accepted"
    assert await repo.get("evt.event_observations", "fresh") is not None


@pytest.mark.asyncio
async def test_fresh_observation_keeps_tracked_event_open():
    repo = MemoryRepository()
    now = utcnow()
    event = {"event_id": "e", "lifecycle": "TRACKED", "known_to_us_at": iso(now - timedelta(days=8))}
    await repo.put("evt.events", "e", event)
    await repo.put(
        "evt.event_observations",
        "fresh",
        {"observation_id": "fresh", "event_id": "e", "observed_at": iso(now)},
    )
    await Scheduler(repo, SimpleNamespace(), None, None).lifecycle(now)
    assert await repo.get("evt.events", "e") == event

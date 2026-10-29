from datetime import timedelta
from types import SimpleNamespace

import pytest

from tank.event_tools import TOOL_RESULT_MAX_BYTES, EventToolService
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import canonical_json, iso, utcnow


@pytest.fixture
async def service(tmp_path):
    repo = MemoryRepository()
    await repo.open()
    return EventToolService(repo, Settings(data_dir=tmp_path), None, None)


async def put_event(
    service, event_id, *, entities=(), tokens=(), statement="News", age=timedelta(hours=1), **extra
):
    await service.repo.put(
        "evt.events",
        event_id,
        {
            "event_id": event_id,
            "lifecycle": "TRACKED",
            "statement": statement,
            "entities": [{"entity_id": entity, "role": "SUBJECT"} for entity in entities],
            "tokens_mentioned": list(tokens),
            "known_to_us_at": iso(utcnow() - age),
            **extra,
        },
    )


async def listed_ids(service, arguments):
    result = await service.dispatch("list_open_events", arguments, {})
    return [row["event_id"] for row in result["events"]]


async def test_token_and_entity_filters_intersect(service):
    """Both filters narrow the list: bitcoin:BTC with okx keeps only events about both."""
    await put_event(service, "both", entities=["okx"], tokens=["bitcoin:BTC"])
    await put_event(service, "btc-only", tokens=["bitcoin:BTC"])
    await put_event(service, "okx-only", entities=["okx"])
    await put_event(service, "other", entities=["world"])
    assert await listed_ids(service, {"token_id": "bitcoin:BTC", "entity_id": "okx"}) == ["both"]
    assert sorted(await listed_ids(service, {"token_id": "bitcoin:BTC"})) == ["both", "btc-only"]
    assert sorted(await listed_ids(service, {"entity_id": "okx"})) == ["both", "okx-only"]
    assert sorted(await listed_ids(service, {})) == ["both", "btc-only", "okx-only", "other"]


async def test_each_filter_still_matches_its_registry_pair(service):
    """A token filter matches its project and a project filter its tokens."""
    await put_event(service, "by-project", entities=["world"])
    await put_event(service, "by-token", tokens=["world:WLD"])
    await put_event(service, "unrelated", tokens=["bitcoin:BTC"])
    assert sorted(await listed_ids(service, {"token_id": "world:WLD"})) == ["by-project", "by-token"]
    assert sorted(await listed_ids(service, {"entity_id": "world"})) == ["by-project", "by-token"]
    assert sorted(await listed_ids(service, {"token_id": "world:WLD", "entity_id": "world"})) == [
        "by-project",
        "by-token",
    ]


async def test_default_window_is_the_routing_window_and_days_widens_it(service):
    """03 §3.1: events without activity in the last 3 days are outside the default list."""
    await put_event(service, "recent", entities=["world"], age=timedelta(days=2))
    await put_event(service, "older", entities=["world"], age=timedelta(days=5))
    await put_event(service, "routed", entities=["world"], age=timedelta(days=6))
    await service.repo.put(
        "ops.agent_sessions",
        "s-routed",
        {
            "session_id": "s-routed",
            "event_id": "routed",
            "status": "OPEN",
            "last_item_at": iso(utcnow() - timedelta(hours=3)),
        },
    )
    await put_event(
        service,
        "reviewed",
        entities=["world"],
        age=timedelta(days=6),
        review_requested_at=iso(utcnow() - timedelta(days=1)),
    )
    default = await service.dispatch("list_open_events", {"entity_id": "world", "days": None}, {})
    assert default["days"] == 3
    assert [row["event_id"] for row in default["events"]] == ["routed", "reviewed", "recent"]
    assert sorted(await listed_ids(service, {"entity_id": "world", "days": 7})) == [
        "older",
        "recent",
        "reviewed",
        "routed",
    ]


@pytest.mark.parametrize("days", [0, 91, -1, True])
async def test_days_outside_the_bounds_are_refused(service, days):
    with pytest.raises(ValueError, match="days"):
        await service.dispatch("list_open_events", {"days": days}, {})


async def test_query_keeps_matching_statements_ranked_by_matched_words(service):
    await put_event(service, "partial", statement="SEC delays a spot ETF decision", age=timedelta(hours=1))
    await put_event(service, "full", statement="SEC approves spot Bitcoin ETFs", age=timedelta(hours=5))
    await put_event(service, "none", statement="Exchange lists a new token", age=timedelta(minutes=5))
    result = await service.dispatch("list_open_events", {"query": "Bitcoin ETF approves"}, {})
    assert [(row["event_id"], row["relevance"]) for row in result["events"]] == [("full", 3), ("partial", 1)]
    assert result["total"] == 2


async def test_pages_return_every_match_once_with_the_total(service):
    for index in range(123):
        await put_event(
            service,
            f"evt-{index:03d}",
            tokens=["bitcoin:BTC"],
            statement=f"Bitcoin development number {index}",
            age=timedelta(minutes=index),
        )
    seen, offset, pages = [], 0, 0
    while offset is not None:
        page = await service.dispatch("list_open_events", {"token_id": "bitcoin:BTC", "offset": offset}, {})
        assert page["total"] == 123 and "_truncated" not in page
        seen += [row["event_id"] for row in page["events"]]
        offset, pages = page["next_offset"], pages + 1
    assert seen == [f"evt-{index:03d}" for index in range(123)]
    assert pages == 3


async def test_fifty_realistic_rows_fit_the_result_limit_uncompacted(service):
    statement = (
        "Binance announces it will list the token on spot markets with USDT and USDC pairs, "
        "deposits open today and trading starts tomorrow at 10:00 UTC according to the official notice."
    )
    for index in range(60):
        await put_event(
            service,
            f"EVT-01M5DFH2Z5N9SFA9CKKRYTG{index:03d}",
            entities=["binance", "world"],
            tokens=["world:WLD", "bitcoin:BTC"],
            statement=statement,
            age=timedelta(minutes=index),
            event_state="ANNOUNCED",
            certainty="HIGH",
            freshness="FRESH",
            public_at=iso(utcnow()),
        )
        await service.repo.put(
            "ops.agent_sessions",
            f"session-{index}",
            {"session_id": f"session_{index:026d}", "event_id": f"EVT-01M5DFH2Z5N9SFA9CKKRYTG{index:03d}"},
        )
    page = await service.dispatch("list_open_events", {"entity_id": "world"}, {})
    assert len(page["events"]) == 50 and page["next_offset"] == 50 and page["total"] == 60
    assert "_truncated" not in page
    assert len(canonical_json(page).encode()) <= TOOL_RESULT_MAX_BYTES
    assert all(row["statement"] == statement for row in page["events"])


async def test_oversized_rows_shrink_the_page_and_keep_the_next_offset(service):
    for index in range(10):
        await put_event(service, f"evt-{index}", statement="x" * 9000, age=timedelta(minutes=index))
    page = await service.dispatch("list_open_events", {}, {})
    assert 0 < len(page["events"]) < 10 and page["next_offset"] == len(page["events"])
    assert "_truncated" not in page


async def test_a_single_row_over_the_limit_still_advances_the_page(service):
    """One statement larger than the whole result limit is shown clipped, and paging moves past it."""
    await put_event(service, "huge", statement="x" * 60_000, age=timedelta(minutes=1))
    await put_event(service, "small", statement="Small", age=timedelta(minutes=2))
    first = await service.dispatch("list_open_events", {}, {})
    assert [row["event_id"] for row in first["events"]] == ["huge"]
    assert first["next_offset"] == 1 and first["total"] == 2
    assert "_truncated" in first and len(canonical_json(first).encode()) <= TOOL_RESULT_MAX_BYTES
    second = await service.dispatch("list_open_events", {"offset": first["next_offset"]}, {})
    assert [row["event_id"] for row in second["events"]] == ["small"] and second["next_offset"] is None


def lifecycle_scheduler(repo, close_days=None):
    settings = SimpleNamespace(
        load_config=lambda name: {} if close_days is None else {"event_close_days": close_days}
    )
    return Scheduler(repo, settings, None, None)


async def tracked(repo, event_id, *, known_days, **extra):
    now = utcnow()
    await repo.put(
        "evt.events",
        event_id,
        {
            "event_id": event_id,
            "lifecycle": "TRACKED",
            "known_to_us_at": iso(now - timedelta(days=known_days)),
            **extra,
        },
    )


async def test_undated_milestone_does_not_keep_a_quiet_tracked_event_open():
    """A next_milestone without due_at can never expire, so it must not block inactivity closure."""
    repo = MemoryRepository()
    now = utcnow()
    await tracked(
        repo, "undated", known_days=8, next_milestone={"kind": "ROLLOUT", "due_at": None, "source_id": "S23"}
    )
    await tracked(
        repo,
        "dated",
        known_days=8,
        next_milestone={"kind": "Vote", "due_at": iso(now + timedelta(days=2)), "source_id": "S23"},
    )
    await tracked(repo, "observed", known_days=8)
    await repo.put(
        "evt.event_observations",
        "obs",
        {"obs_id": "obs", "event_id": "observed", "observed_at": iso(now - timedelta(days=2))},
    )
    scheduler = lifecycle_scheduler(repo)
    for _ in range(2):
        await scheduler.lifecycle(now)
        assert (await repo.get("evt.events", "undated"))["lifecycle"] == "CLOSED"
        assert (await repo.get("evt.events", "dated"))["lifecycle"] == "TRACKED"
        assert (await repo.get("evt.events", "observed"))["lifecycle"] == "TRACKED"
    closed_at = (await repo.get("evt.events", "undated"))["closed_at"]
    await scheduler.lifecycle(now + timedelta(hours=1))
    assert (await repo.get("evt.events", "undated"))["closed_at"] == closed_at


async def test_tracked_close_respects_configured_inactivity_days():
    repo = MemoryRepository()
    await tracked(repo, "quiet", known_days=4)
    await lifecycle_scheduler(repo).lifecycle(utcnow())
    assert (await repo.get("evt.events", "quiet"))["lifecycle"] == "TRACKED"
    await lifecycle_scheduler(repo, close_days=3).lifecycle(utcnow())
    assert (await repo.get("evt.events", "quiet"))["lifecycle"] == "CLOSED"

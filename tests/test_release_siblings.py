import asyncio
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from tank.agents.pipeline import Pipeline, RoutingDeferred, github_repository
from tank.runtime import Runtime
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import iso, parse_time, utcnow

NEARCORE = "https://github.com/near/nearcore/releases.atom"


def options(**thresholds):
    config = {
        "registry": yaml.safe_load(Path("config/registry.yaml").read_text()),
        "thresholds": {"routing_cutoff": 0.5, "release_sibling_wait_s": 300, **thresholds},
    }
    return SimpleNamespace(
        load_config=config.__getitem__, foundry_api_key="test", config_version="test", code_version="test"
    )


def release(item_id, title, at, feed=NEARCORE, **extra):
    return {
        "item_id": item_id,
        "source_id": "S25",
        "feed": feed,
        "url": feed.removesuffix("releases.atom") + "releases/tag/" + title,
        "title": title,
        "author": "GitHub",
        "author_organization": feed.split("/")[3],
        "observed_at": iso(at),
        "extraction_status": "PENDING",
        **extra,
    }


async def near_event(repo, at):
    await repo.put(
        "evt.events",
        "EVT-rc7",
        {
            "event_id": "EVT-rc7",
            "statement": "NEAR published nearcore 2.14.0-rc.7",
            "entities": [{"role": "ACTOR", "entity_id": "near"}],
            "tokens_mentioned": ["near:NEAR"],
            "lifecycle": "TRACKED",
        },
    )
    await repo.put(
        "ops.agent_sessions",
        "session-rc7",
        {"session_id": "session-rc7", "event_id": "EVT-rc7", "status": "OPEN", "last_item_at": iso(at)},
    )


def scored(pipe, value=0.9):
    asked = []

    async def score(item, name, state):
        asked.append(state["event"]["statement"])
        return value

    pipe.score = score
    return asked


def test_every_s25_repository_resolves_one_registry_entity_and_its_token():
    sources = yaml.safe_load(Path("config/sources.yaml").read_text())["sources"]
    feeds = next(row for row in sources if row["source_id"] == "S25")["feeds"]
    registry = yaml.safe_load(Path("config/registry.yaml").read_text())
    pipe = Pipeline(MemoryRepository(), options(), http=object())
    for feed in feeds:
        owners = [
            row["entity_id"]
            for row in registry["entities"]
            if github_repository({"feed": feed["url"]}) in {r.casefold() for r in row.get("github_repos", [])}
        ]
        assert len(owners) == 1, feed["url"]
        entity = next(row for row in registry["entities"] if row["entity_id"] == owners[0])
        assert "S25" in entity["official_sources"]
        assert pipe.identities({"feed": feed["url"], "title": "v1.0.0"}) == {owners[0], *entity["tokens"]}


@pytest.mark.parametrize(
    "feed",
    [
        "https://github.com/bitcoin/bitcoin/releases.atom",
        "https://github.com/ethereum/go-ethereum/releases.atom",
    ],
)
def test_benchmark_l1_client_releases_are_monitored(feed):
    sources = yaml.safe_load(Path("config/sources.yaml").read_text())["sources"]
    feeds = next(row for row in sources if row["source_id"] == "S25")["feeds"]
    assert {"url": feed, "kind": "atom", "interval_s": 600, "subtype": "releases"} in feeds


@pytest.mark.parametrize(
    "feed",
    [
        "https://github.com/someone/nearcore/releases.atom",
        "https://github.com/near/releases.atom",
        "http://github.com/near/nearcore/releases.atom",
        "https://example.org/near/nearcore/releases.atom",
        None,
    ],
)
def test_unmapped_or_foreign_feeds_add_no_identity(feed):
    assert not Pipeline(MemoryRepository(), options(), http=object()).identities(
        {"feed": feed, "title": "2.14.0", "author_organization": "near"}
    )


async def test_release_title_without_project_name_joins_the_repository_event():
    repo = MemoryRepository()
    now = utcnow()
    await near_event(repo, now - timedelta(hours=3))
    await repo.put("ingest.source_items", "final", release("final", "2.14.0", now))
    pipe = Pipeline(repo, options(), http=object())
    asked = scored(pipe)
    routed = await pipe.route_item({"item_id": "final"})
    assert asked == ["NEAR published nearcore 2.14.0-rc.7"]
    assert routed["route"] == {"decision": "ADDED", "event_id": "EVT-rc7", "p_same": 0.9}
    assert routed["routing_message"] == {"kind": "ITEM_ADDED", "item_id": "final", "event_id": "EVT-rc7"}


async def test_same_poll_sibling_waits_for_the_first_tag_event_then_joins_it():
    repo = MemoryRepository()
    now = utcnow()
    await repo.put("ingest.source_items", "core", release("core", "2.14.0-rc.7", now))
    await repo.put("ingest.source_items", "crates", release("crates", "crates-0.38.0-rc.7", now))
    pipe = Pipeline(repo, options(), http=object())
    asked = scored(pipe)

    first = await pipe.route_item({"item_id": "core"})
    assert first["route"]["decision"] == "NEW_SESSION" and asked == []
    with pytest.raises(RoutingDeferred) as deferred:
        await pipe.route_item({"item_id": "crates"})
    wait = parse_time(deferred.value.not_before.value) - utcnow()
    assert timedelta(seconds=10) < wait <= timedelta(seconds=15)
    held = await repo.get("ingest.source_items", "crates")
    assert "route" not in held and "routing_message" not in held
    assert [job["payload"] for job in await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})] == [
        {"kind": "NEW_ITEM", "item_id": "core"}
    ]

    await near_event(repo, now)
    core = await repo.get("ingest.source_items", "core")
    await repo.put("ingest.source_items", "core", {**core, "extraction_status": "DONE"})
    joined = await pipe.route_item({"item_id": "crates"})
    assert joined["route"]["decision"] == "ADDED" and joined["route_event_id"] == "EVT-rc7"


async def test_concurrent_same_poll_siblings_open_one_session():
    repo = MemoryRepository()
    now = utcnow()
    titles = ["v1.15.2-rc.1", "graft/subnet-evm/v1.15.2-rc.1", "graft/evm/v1.15.2-rc.1"]
    feed = "https://github.com/ava-labs/avalanchego/releases.atom"
    for number, title in enumerate(titles):
        await repo.put("ingest.source_items", str(number), release(str(number), title, now, feed=feed))
    pipe = Pipeline(repo, options(), http=object())
    scored(pipe)
    outcomes = await asyncio.gather(
        *(pipe.route_item({"item_id": str(number)}) for number in range(len(titles))), return_exceptions=True
    )
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, RoutingDeferred) for outcome in outcomes) == len(titles) - 1
    assert len(await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})) == 1


@pytest.mark.parametrize("status", ["DONE", "SKIPPED", "FAILED"])
async def test_settled_sibling_without_event_does_not_hold_routing(status):
    repo = MemoryRepository()
    now = utcnow()
    sibling = release("core", "2.14.0-rc.7", now, extraction_status=status)
    sibling["route"] = {"decision": "NEW_SESSION", "event_id": None, "p_same": None}
    await repo.put("ingest.source_items", "core", sibling)
    await repo.put("ingest.source_items", "crates", release("crates", "crates-0.38.0-rc.7", now))
    routed = await Pipeline(repo, options(), http=object()).route_item({"item_id": "crates"})
    assert routed["route"]["decision"] == "NEW_SESSION"


async def test_wait_ends_at_the_bound_after_the_item_observation():
    repo = MemoryRepository()
    now = utcnow()
    sibling = release("core", "2.14.0", now - timedelta(seconds=301))
    sibling["route"] = {"decision": "NEW_SESSION", "event_id": None, "p_same": None}
    await repo.put("ingest.source_items", "core", sibling)
    await repo.put(
        "ingest.source_items", "crates", release("crates", "crates-0.38.0", now - timedelta(seconds=301))
    )
    pipe = Pipeline(repo, options(), http=object())
    routed = await pipe.route_item({"item_id": "crates"})
    assert routed["route"]["decision"] == "NEW_SESSION"


async def test_other_repository_and_news_items_never_wait():
    repo = MemoryRepository()
    now = utcnow()
    sibling = release("core", "2.14.0", now)
    sibling["route"] = {"decision": "NEW_SESSION", "event_id": None, "p_same": None}
    await repo.put("ingest.source_items", "core", sibling)
    cow = "https://github.com/cowprotocol/services/releases.atom"
    await repo.put("ingest.source_items", "cow", release("cow", "v2.385.2", now, feed=cow))
    await repo.put(
        "ingest.source_items",
        "news",
        {
            "item_id": "news",
            "source_id": "S30",
            "feed": "https://example.org/rss",
            "title": "NEAR Protocol 2.14.0",
            "observed_at": iso(now),
            "extraction_status": "PENDING",
        },
    )
    pipe = Pipeline(repo, options(), http=object())
    assert (await pipe.route_item({"item_id": "cow"}))["route"]["decision"] == "NEW_SESSION"
    assert (await pipe.route_item({"item_id": "news"}))["route"]["decision"] == "NEW_SESSION"


async def test_deferred_routing_requeues_without_spending_an_attempt():
    repo = MemoryRepository()
    stop = asyncio.Event()
    queued = await repo.enqueue("ROUTE_ITEM", {"item_id": "crates"}, "crates", max_attempts=2)
    later = utcnow() + timedelta(seconds=15)

    async def defer(payload):
        stop.set()
        raise RoutingDeferred(collection_clock(later), "core")

    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.handlers = {"ROUTE_ITEM": defer}
    await runtime.worker(stop)
    row = await repo.get("ops.jobs", queued["job_id"])
    assert row["status"] == "QUEUED" and row["attempts"] == 0
    assert parse_time(row["run_after"]) == parse_time(iso(later))

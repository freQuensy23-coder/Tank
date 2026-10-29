import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from tank.agents.pipeline import Pipeline, RoutingDeferred, instrument_assets
from tank.storage import MemoryRepository
from tank.util import iso, utcnow

KRAKEN = "https://api.kraken.com/0/public/AssetPairs"


def options():
    config = {
        "registry": yaml.safe_load(Path("config/registry.yaml").read_text()),
        "thresholds": {"routing_cutoff": 0.5, "release_sibling_wait_s": 300, "routing_window_days": 3},
    }
    return SimpleNamespace(
        load_config=config.__getitem__, foundry_api_key="test", config_version="test", code_version="test"
    )


def side(symbol, base, status, venue="kraken"):
    return {
        "venue": venue,
        "market_type": "SPOT",
        "symbol": symbol,
        "base": base,
        "quote": "ZUSD",
        "status": status,
        "contract_type": None,
    }


def change(item_id, symbol, base, at, *, venue="kraken", before="online", after="cancel_only", **extra):
    return {
        "item_id": item_id,
        "source_id": "S14",
        "feed": KRAKEN,
        "title": f"{symbol} instrument change",
        "text": json.dumps(
            {
                "before": side(symbol, base, before, venue),
                "after": side(symbol, base, after, venue),
            },
            sort_keys=True,
        ),
        "observed_at": iso(at),
        "extraction_status": "PENDING",
        **extra,
    }


async def opened(repo, item_id, event_id, at, lifecycle="TRACKED", **event):
    await repo.put(
        "evt.events",
        event_id,
        {"event_id": event_id, "statement": "Kraken delists ELX", "lifecycle": lifecycle, **event},
    )
    await repo.put(
        "ops.agent_sessions",
        "session-" + item_id,
        {
            "session_id": "session-" + item_id,
            "event_id": event_id,
            "opened_by_item": item_id,
            "status": "OPEN",
            "last_item_at": iso(at),
        },
    )


async def settled(repo, item, **fields):
    await repo.put(
        "ingest.source_items",
        item["item_id"],
        {
            **item,
            "extraction_status": "DONE",
            "route": {"decision": "NEW_SESSION", "event_id": None, "p_same": None},
            **fields,
        },
    )


def pipeline(repo):
    pipe = Pipeline(repo, options(), http=object())

    async def score(item, name, state):
        raise AssertionError("Decision-1 must not be asked")

    pipe.score = score
    return pipe


def test_assets_are_venue_and_base_pairs_of_every_reported_symbol():
    now = utcnow()
    assert instrument_assets(change("a", "ELXUSD", "ELX", now)) == {("kraken", "elx")}
    grouped = {
        "item_id": "g",
        "source_id": "S14",
        "text": json.dumps(
            {
                "changes": [
                    {"symbol": "BMBUSD", "before": side("BMBUSD", "BMB", "online"), "after": None},
                    {"symbol": "ELXEUR", "before": side("ELXEUR", "ELX", "online"), "after": None},
                ]
            }
        ),
    }
    assert instrument_assets(grouped) == {("kraken", "bmb"), ("kraken", "elx")}
    swap = change("s", "CEG-USDT-SWAP", "", now, venue="okx", before="preopen", after="live")
    assert instrument_assets(swap) == {("okx", "symbol:ceg-usdt-swap")}
    assert instrument_assets({**change("n", "ELXUSD", "ELX", now), "source_id": "S30"}) == set()
    assert instrument_assets({"source_id": "S14", "text": "not json"}) == set()


async def test_next_pair_of_the_same_asset_joins_the_first_change_event_without_decision_1():
    repo = MemoryRepository()
    now = utcnow()
    first = change("elxusd", "ELXUSD", "ELX", now - timedelta(hours=2))
    await settled(repo, first)
    await opened(repo, "elxusd", "EVT-elx", now - timedelta(hours=2))
    await repo.put("ingest.source_items", "elxeur", change("elxeur", "ELXEUR", "ELX", now))

    routed = await pipeline(repo).route_item({"item_id": "elxeur"})

    assert routed["route"] == {
        "decision": "ADDED",
        "event_id": "EVT-elx",
        "p_same": None,
        "basis": "INSTRUMENT_ASSET",
    }
    assert routed["routing_message"] == {"kind": "ITEM_ADDED", "item_id": "elxeur", "event_id": "EVT-elx"}
    jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert [job["payload"] for job in jobs] == [routed["routing_message"]]


async def test_latest_related_item_wins_and_merged_events_lead_to_their_target():
    repo = MemoryRepository()
    now = utcnow()
    await settled(repo, change("old", "RIZEUSD", "RIZE", now - timedelta(hours=20)))
    await opened(repo, "old", "EVT-old", now - timedelta(hours=20))
    await settled(
        repo,
        change(
            "added", "RIZEUSD", "RIZE", now - timedelta(hours=1), before="cancel_only", after="reduce_only"
        ),
        route={"decision": "ADDED", "event_id": "EVT-dup", "p_same": 0.8},
        route_event_id="EVT-dup",
    )
    await repo.put(
        "evt.events",
        "EVT-dup",
        {"event_id": "EVT-dup", "lifecycle": "MERGED", "merged_into": "EVT-target"},
    )
    await repo.put("evt.events", "EVT-target", {"event_id": "EVT-target", "lifecycle": "TRACKED"})
    await repo.put("ingest.source_items", "new", change("new", "RIZEUSD", "RIZE", now))

    routed = await pipeline(repo).route_item({"item_id": "new"})

    assert routed["route_event_id"] == "EVT-target"


@pytest.mark.parametrize(
    ("other", "lifecycle", "age"),
    [
        ({"venue": "binance"}, "TRACKED", timedelta(hours=1)),
        ({"base": "BMB", "symbol": "BMBUSD"}, "TRACKED", timedelta(hours=1)),
        ({}, "CLOSED", timedelta(hours=1)),
        ({}, "TRACKED", timedelta(days=4)),
    ],
)
async def test_other_venue_other_asset_closed_or_old_events_open_a_session(other, lifecycle, age):
    repo = MemoryRepository()
    now = utcnow()
    prior = change(
        "prior",
        other.get("symbol", "ELXUSD"),
        other.get("base", "ELX"),
        now - age,
        venue=other.get("venue", "kraken"),
    )
    await settled(repo, prior)
    await opened(repo, "prior", "EVT-prior", now - age, lifecycle=lifecycle)
    await repo.put("ingest.source_items", "new", change("new", "ELXEUR", "ELX", now))

    routed = await pipeline(repo).route_item({"item_id": "new"})

    assert routed["route"] == {"decision": "NEW_SESSION", "event_id": None, "p_same": None}


async def test_same_asset_waits_for_the_unsettled_first_change_then_joins_its_event():
    repo = MemoryRepository()
    now = utcnow()
    await repo.put("ingest.source_items", "first", change("first", "EQTUSDT", "EQT", now))
    await repo.put(
        "ingest.source_items",
        "second",
        change("second", "EQTUSDT", "EQT", now, before="PendingOpen", after="Trading"),
    )
    pipe = pipeline(repo)

    first = await pipe.route_item({"item_id": "first"})
    assert first["route"]["decision"] == "NEW_SESSION"
    with pytest.raises(RoutingDeferred):
        await pipe.route_item({"item_id": "second"})

    await opened(repo, "first", "EVT-eqt", now)
    await repo.put("ingest.source_items", "first", {**first, "extraction_status": "DONE"})
    joined = await pipe.route_item({"item_id": "second"})
    assert joined["route_event_id"] == "EVT-eqt"

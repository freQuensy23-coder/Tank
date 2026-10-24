"""Read-only typed routing selection preserves source and event identities."""

from datetime import timedelta

import pytest
from agent_source_fixtures import source_item_fixture
from pydantic import JsonValue

from tank.agents.pipeline_contracts import RoutingRegistry
from tank.agents.routing import RoutingItem, RoutingSelector, item_state
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.mark.asyncio
async def test_routing_selector_reads_required_facts_without_queue_or_http_ports() -> None:
    repo = MemoryRepository()
    item = RoutingItem.from_source(source_item_fixture("source", source_id="S01", text="$ETH announcement"))
    selector = RoutingSelector(
        repo, RoutingRegistry.from_wire({"tokens": [{"token_id": "ethereum", "symbol": "ETH"}]})
    )
    now = utcnow()
    await repo.put(
        "ops.agent_sessions", "session", {"event_id": "event", "status": "OPEN", "last_item_at": iso(now)}
    )
    event_record: dict[str, JsonValue] = {
        "event_id": "event",
        "statement": "Ethereum announcement",
        "lifecycle": "ANALYZING",
        "entities": [],
        "tokens_mentioned": ["ethereum"],
        "event_state": "RUMOR",
    }
    await repo.put("evt.events", "event", event_record)
    identities = selector.identities(item)
    assert identities == {"ethereum"}
    candidates = await selector.candidate_events(identities, iso(now - timedelta(days=1)), now)
    assert [event.event_id for event in candidates] == ["event"]
    state = selector.routing_state(item, candidates[0])
    assert state.item == item_state(item).item
    assert state.event is not None and state.event.tokens == ["ETH"]
    assert state.event.stage == "rumor"
    await repo.put("evt.events", "event", {**event_record, "event_id": "other"})
    with pytest.raises(ValueError, match="repository key"):
        await selector.candidate_events(identities, iso(now - timedelta(days=1)), now)

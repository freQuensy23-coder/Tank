"""Token-scoped fatal evidence stays scoped through actual watch and decision tools."""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from tank.analysis.decisions import DecisionEngine
from tank.event_tools import EventToolService
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fatal_token,resolved,actions",
    [("A", False, ["EXIT", "HOLD"]), (None, False, ["EXIT", "EXIT"]), ("A", True, ["HOLD", "HOLD"])],
)
async def test_scoped_global_and_resolved_fatal_condition_to_actual_decision(
    tmp_path, fatal_token, resolved, actions
):
    from test_decisions import inputs

    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path)
    tools = EventToolService(repo, settings, None, DecisionEngine(settings))
    event, ems, beneficiary, market = inputs()
    now = utcnow()
    event.update(current_ems_id="ems", lifecycle="MONITORING")
    ems.update(ems_id="ems", event_id="e")
    await repo.put("evt.events", "e", event)
    await repo.put("evt.ems_assessments", "ems", ems)
    await repo.put(
        "evt.beneficiary_maps", "map", {"event_id": "e", "map_version": 1, "analysis_set": ["A", "B"]}
    )
    for token in ("A", "B"):
        edge = dict(beneficiary, edge_id=token, event_id="e", map_version=1, token_id=token)
        truth = dict(market, mta_id=token, event_id="e", token_id=token, cutoff=iso(now))
        await repo.put("evt.beneficiary_edges", token, edge)
        await repo.put("ana.market_truth", token, truth)
        await repo.put(
            "ana.calls",
            token,
            {
                "call_id": token,
                "event_id": "e",
                "token_id": token,
                "status": "OPEN",
                "opened_at": iso(now),
                "entry_ref_price": "10",
                "invalidation_price": "9",
            },
        )
        initial = await tools.get_decision({"event_id": "e", "token_id": token}, {})
        assert initial["action"] == "HOLD"
    contradiction = {
        "contradiction_id": "fatal",
        "event_id": "e",
        "type": "FATAL",
        "token_id": fatal_token,
        "detected_at": iso(now),
    }
    if resolved:
        contradiction["resolved_at"] = iso(now)
    await repo.put("ana.contradictions", "fatal", contradiction)

    async def assess(*args):
        return {"relative": {"close_1h_price": "10"}}

    scheduler = Scheduler(repo, settings, None, SimpleNamespace(assess=assess))
    await scheduler.eval_conditions({"cutoff": iso(now + timedelta(minutes=1))})
    for token, expected in zip(("A", "B"), actions, strict=True):
        conditions = await repo.list("ana.watch_conditions", filters={"event_id": "e", "token_id": token})
        fatal_watch = next(c for c in conditions if c["expr"].get("metric") == "event.contradiction_fatal")
        assert fatal_watch["status"] == ("FIRED" if expected == "EXIT" else "ARMED")
        decision = await tools.get_decision({"event_id": "e", "token_id": token}, {})
        assert decision["action"] == expected

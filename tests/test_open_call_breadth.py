"""Actual decision-generated open-call conditions through the durable evaluator."""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from tank.analysis.decisions import DecisionEngine
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, parse_time


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("open_call,expected", [(True, "ARMED"), (False, "FIRED")])
async def test_concentrated_sell_only_fires_weaker_watch_downgrade(open_call, expected) -> None:
    from test_decisions import inputs

    event, assessment, beneficiary, truth = inputs()
    start = parse_time("2026-10-17T10:15:00Z")
    truth["cutoff"] = iso(start)
    if not open_call:
        truth["data_confidence"]["class"] = "MEDIUM"
    call = (
        {"call_id": "call", "status": "OPEN", "entry_ref_price": "10", "invalidation_price": "9"}
        if open_call
        else None
    )
    decision = DecisionEngine().evaluate(event, assessment, beneficiary, truth, open_call=call)
    assert decision["action"] == ("HOLD" if open_call else "WATCH_TO_START")
    condition = next(c for c in decision["conditions"] if c["role"] == "DOWNGRADE")
    repo = MemoryRepository()
    await repo.put("evt.events", "e", event)
    await repo.put(
        "ana.watch_conditions",
        condition["cond_id"],
        dict(
            condition,
            event_id="e",
            token_id="T",
            status="ARMED",
            created_at=iso(start),
            expires_at=iso(start + timedelta(hours=10)),
        ),
    )
    observed = []

    async def assess(event_id, token_id, cutoff):
        observed.append(cutoff)
        return {
            "spot": {
                "W1H": {"direction": "SELL", "classification": "CONCENTRATED", "cw_breadth": {"sell": 0.8}}
            }
        }

    scheduler = Scheduler(
        repo,
        SimpleNamespace(),
        None,
        SimpleNamespace(assess=assess),
        thresholds=Settings(_env_file=None).thresholds(),
    )
    for hour in (11, 12):
        await scheduler.eval_conditions({"cutoff": iso(start.replace(hour=hour, minute=1))})
    persisted = await repo.get("ana.watch_conditions", condition["cond_id"])
    assert persisted["status"] == expected
    if not open_call:
        truth["data_confidence"]["class"] = "HIGH"
        assert (
            DecisionEngine().evaluate(event, assessment, beneficiary, truth, conditions=[persisted])["action"]
            == "WAIT"
        )
    if open_call:

        async def broad(event_id, token_id, cutoff):

            classification = "BROAD" if parse_time(cutoff.value).hour >= 13 else "CONCENTRATED"
            return {
                "spot": {
                    "W1H": {
                        "direction": "SELL",
                        "classification": classification,
                        "cw_breadth": {"sell": 0.8},
                    }
                }
            }

        scheduler.truth = SimpleNamespace(assess=broad)
        for hour, status in [(13, "ARMED"), (14, "FIRED")]:
            await scheduler.eval_conditions({"cutoff": iso(start.replace(hour=hour, minute=1))})
            assert (await repo.get("ana.watch_conditions", condition["cond_id"]))["status"] == status
        jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
        assert len(jobs) == 1
        fired = await repo.get("ana.watch_conditions", condition["cond_id"])
        reduced = DecisionEngine().evaluate(
            event, assessment, beneficiary, truth, open_call=call, conditions=[fired]
        )
        assert reduced["action"] == "REDUCE" and reduced["action_rule"] == "B2"


@pytest.mark.parametrize("capture,grade", [("DIRECT", "B"), ("REVENUE_CLAIM", "B"), ("INDIRECT", "C")])
def test_loser_uses_verified_negative_thesis_link_without_long_eligibility(capture, grade) -> None:
    from test_decisions import inputs

    event, assessment, beneficiary, truth = inputs()
    truth["move_class"] = "ORGANIC_MIXED"
    beneficiary.update(
        role="LOSER", credible=False, thesis_link_credible=True, capture_class=capture, exposure_score=8
    )
    result = DecisionEngine().evaluate(event, assessment, beneficiary, truth)
    assert result["thesis_grade"] == grade
    assert result["outlook"]["direction"] == "BEARISH"
    assert result["action"] == "WAIT" and result["action"] not in {"START", "ADD"}
    beneficiary.update(thesis_link_credible=False, credible=True)
    assert DecisionEngine().evaluate(event, assessment, beneficiary, truth)["thesis_grade"] == "D"
    beneficiary.update(role="SECONDARY", credible=False, thesis_link_credible=True)
    assert DecisionEngine().evaluate(event, assessment, beneficiary, truth)["thesis_grade"] == "D"


async def fire_generated(condition, event, truth_rows):
    repo = MemoryRepository()
    start = parse_time("2026-10-17T10:15:00Z")
    await repo.put("evt.events", "e", event)
    await repo.put(
        "ana.watch_conditions",
        condition["cond_id"],
        dict(
            condition,
            event_id="e",
            token_id="T",
            status="ARMED",
            created_at=iso(start),
            expires_at=iso(start + timedelta(hours=10)),
        ),
    )

    async def assess(*args):
        return truth_rows

    scheduler = Scheduler(
        repo,
        SimpleNamespace(),
        None,
        SimpleNamespace(assess=assess),
        thresholds=Settings(_env_file=None).thresholds(),
    )
    for hour in (11, 12):
        await scheduler.eval_conditions({"cutoff": iso(start.replace(hour=hour, minute=1))})
    return await repo.get("ana.watch_conditions", condition["cond_id"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_watch_anchor_fired_by_actual_evaluator_prevents_new_start() -> None:
    from test_decisions import inputs

    e, a, b, m = inputs()
    m["data_confidence"]["class"] = "MEDIUM"
    engine = DecisionEngine()
    watched = engine.evaluate(e, a, b, m)
    anchor = next(
        c
        for c in watched["conditions"]
        if c["role"] == "DOWNGRADE" and c["expr"].get("metric") == "price.close_vs_level"
    )
    fired = await fire_generated(anchor, e, {"relative": {"close_1h_price": "9"}})
    assert fired["status"] == "FIRED"
    m["data_confidence"]["class"] = "HIGH"
    result = engine.evaluate(e, a, b, m, conditions=[fired])
    assert result["action"] == "WAIT" and result["action_rule"] == "WATCH_ANCHOR_LOST"

    assert (
        engine.evaluate(
            e,
            a,
            b,
            m,
            conditions=[
                {
                    "status": "FIRED",
                    "role": "INVALIDATION",
                    "expr": {"metric": "milestone.outcome", "op": "==", "value": "FAILED"},
                }
            ],
        )["action"]
        == "START"
    )


@pytest.mark.asyncio
async def test_wait_data_upgrade_does_not_require_class_resolution() -> None:
    from test_decisions import inputs

    e, a, b, m = inputs()
    m["data_confidence"]["class"] = "LOW"
    m["move_class"] = "PERP_LED_CHASE"
    engine = DecisionEngine()
    result = engine.evaluate(e, a, b, m)
    assert result["action_rule"] == "A1"
    upgrade = next(c for c in result["conditions"] if c["role"] == "UPGRADE")
    assert upgrade["expr"]["metric"] == "dc.class"
    fired = await fire_generated(
        upgrade, e, {"data_confidence": {"class": "HIGH"}, "move_class": "PERP_LED_CHASE"}
    )
    assert fired["status"] == "FIRED"
    m["data_confidence"]["class"] = "HIGH"
    m["move_class"] = "UNKNOWN"
    unknown = engine.evaluate(e, a, b, m)
    upgrade = next(c for c in unknown["conditions"] if c["role"] == "UPGRADE")
    assert (
        await fire_generated(upgrade, e, {"data_confidence": {"class": "HIGH"}, "move_class": "UNKNOWN"})
    )["status"] == "ARMED"


@pytest.mark.asyncio
@pytest.mark.parametrize("exact,rounded,status", [(True, 0.5, "FIRED"), (False, 0.5000000000000001, "ARMED")])
async def test_generated_majority_uses_truth_exact_flag_over_display_rounding(exact, rounded, status) -> None:
    from test_decisions import inputs

    e, a, b, m = inputs()
    call = {"call_id": "call", "status": "OPEN"}
    condition = next(
        c
        for c in DecisionEngine().evaluate(e, a, b, m, open_call=call)["conditions"]
        if c["role"] == "DOWNGRADE"
    )
    fired = await fire_generated(
        condition,
        e,
        {
            "spot": {
                "W1H": {
                    "direction": "SELL",
                    "classification": "BROAD",
                    "cw_breadth": {"sell": rounded},
                    "exact_majority": {"sell": exact},
                }
            }
        },
    )
    assert fired["status"] == status

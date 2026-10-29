"""EVAL_CONDITIONS against the production condition set of 2026-10-28 (audit finding 4).

49 ARMED conditions on 11 tokens: 29 market conditions whose cold Market Truth read
outlasts the per-read budget (dc.class with mt.move_class, price.close_vs_level and
2-window spot cohort conditions) and 20 event, milestone and timed ones. Time is
scaled 1:200 (15 s batch budget → 75 ms, 8 s read budget → 40 ms, a 60 s minute →
300 ms, a 30-40 s cold read → 150-200 ms, a ~1 s warm read → 5 ms). A read warms the
market caches only when it completes and reads of one token share them under a lock,
as the incremental caches of MarketService do.
"""

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

import tank.scheduler as scheduler_module
from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.time_precision import chosen_boundary
from tank.util import iso, parse_time

SCALE = 200
MINUTE_S = 60 / SCALE
WARM_S = 1 / SCALE

PRODUCTION_MARKET = [
    ("ondo:ONDO", "dc"),
    ("hyperliquid:HYPE", "dc"),
    ("bitcoin:BTC", "dc"),
    ("bitcoin:BTC", "dc"),
    ("hyperliquid:HYPE", "dc"),
    ("ethereum:ETH", "dc"),
    ("bitcoin:BTC", "dc"),
    ("bitcoin:BTC", "dc"),
    ("aerodrome:AERO", "dc"),
    ("ethena:ENA", "dc"),
    ("near:NEAR", "dc"),
    ("uniswap:UNI", "oi"),
    ("ethena:ENA", "close"),
    ("ondo:ONDO", "close"),
    ("curve:CRV", "close"),
    ("bitcoin:BTC", "close"),
    ("bitcoin:BTC", "close"),
    ("ethereum:ETH", "close"),
    ("bitcoin:BTC", "cohort"),
    ("aerodrome:AERO", "cohort"),
    ("ethena:ENA", "cohort"),
    ("hyperliquid:HYPE", "cohort"),
    ("bitcoin:BTC", "cohort"),
    ("ethereum:ETH", "cohort"),
    ("uniswap:UNI", "cohort"),
    ("bitcoin:BTC", "cohort"),
    ("ondo:ONDO", "cohort"),
    ("bitcoin:BTC", "cohort"),
    ("near:NEAR", "cohort"),
]
PRODUCTION_OTHER = [
    ("near:NEAR", "fatal"),
    ("ethereum:ETH", "fatal"),
    ("ethena:ENA", "fatal"),
    ("bitcoin:BTC", "fatal"),
    ("aerodrome:AERO", "fatal"),
    ("uniswap:UNI", "fatal"),
    ("hyperliquid:HYPE", "fatal"),
    ("hyperliquid:HYPE", "fatal"),
    ("curve:CRV", "fatal"),
    ("bitcoin:BTC", "fatal"),
    ("ondo:ONDO", "fatal"),
    ("hyperliquid:HYPE", "milestone"),
    ("hyperliquid:HYPE", "milestone"),
    ("near:NEAR", "milestone"),
    ("near:NEAR", "milestone"),
    ("curve:CRV", "dc"),
    ("curve:CRV", "cohort"),
    ("hyperliquid:HYPE", "cohort"),
    (None, "review"),
    (None, "review"),
]
COLD_S = {
    "bitcoin:BTC": 40 / SCALE,
    "ethereum:ETH": 35 / SCALE,
}


def expression(kind, now):
    if kind == "dc":
        return {
            "all": [
                {"metric": "dc.class", "window": "W1H", "op": "==", "value": "SPOT_LED_ACCUMULATION"},
                {"metric": "mt.move_class", "window": "W1H", "op": "==", "value": "SPOT_LED"},
            ]
        }
    if kind == "oi":
        return {
            "all": [
                {"metric": "spot.cohort.direction", "window": "W1H", "op": "==", "value": "BUY"},
                {"metric": "spot.cohort.cwb_buy", "window": "W1H", "op": ">", "value": 0.5},
                {"metric": "deriv.native_oi_vs_normal", "window": "W1H", "op": "<=", "value": 1},
            ]
        }
    if kind == "close":
        return {"metric": "price.close_vs_level", "window": "W1H", "op": "<", "value": 1}
    if kind == "cohort":
        return {
            "all": [
                {
                    "metric": "spot.cohort.direction",
                    "window": "W1H",
                    "op": "==",
                    "value": "SELL",
                    "persist": {"count": 2},
                },
                {
                    "metric": "spot.cohort.cwb_sell",
                    "window": "W1H",
                    "op": ">",
                    "value": 0.5,
                    "persist": {"count": 2},
                },
            ]
        }
    if kind == "fatal":
        return {"metric": "event.contradiction_fatal", "window": "W1H", "op": "==", "value": True}
    if kind == "milestone":
        return {"metric": "milestone.outcome", "window": "W1H", "op": "==", "value": "VERIFIED"}
    return {"review_at": iso(now + timedelta(days=2)), "review_question": "Still on track?"}


async def production_conditions(repo, now):
    market = []
    for number, (token, kind) in enumerate(PRODUCTION_MARKET + PRODUCTION_OTHER):
        cond_id = f"CND-{number:02d}"
        event_id = "EVT-" + (token or "x:REVIEW").split(":")[1]
        await repo.put("evt.events", event_id, {"event_id": event_id, "status": "OPEN"})
        await repo.put(
            "ana.watch_conditions",
            cond_id,
            {
                "cond_id": cond_id,
                "event_id": event_id,
                "token_id": token,
                "role": "UPGRADE",
                "status": "ARMED",
                "created_at": iso(now - timedelta(hours=6)),
                "expires_at": iso(now + timedelta(days=3)),
                "expr": expression(kind, now),
            },
        )
        if number < len(PRODUCTION_MARKET):
            market.append(cond_id)
    return market


@pytest.mark.asyncio
async def test_production_condition_set_is_evaluated_without_restarting_cold_reads(monkeypatch):
    """Audit finding 4: every cold market read outlasted its 8 s budget, was cancelled
    and restarted from scratch the next minute, so 29 market conditions were never
    evaluated. A read that outlasts the budget now finishes in the background and
    serves the next batch; each token pays its cold read once."""
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITIONS_BUDGET_S", 15 / SCALE)
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITION_BUDGET_S", 8 / SCALE)
    repo = MemoryRepository()
    now = parse_time("2026-10-28T12:01:00Z")
    market = await production_conditions(repo, now)
    warm, cold_reads, locks = set(), {}, {}

    async def condition_values(event_id, token_id, cutoff, requirements):
        async with locks.setdefault(token_id, asyncio.Lock()):
            if token_id in warm:
                await asyncio.sleep(WARM_S)
            else:
                cold_reads[token_id] = cold_reads.get(token_id, 0) + 1
                await asyncio.sleep(COLD_S.get(token_id, 30 / SCALE))
                warm.add(token_id)
        return {"cutoff": cutoff.value, "_condition_evaluation": {"proofs": [], "request_key": token_id}}

    scheduler = Scheduler(repo, SimpleNamespace(), None, SimpleNamespace(condition_values=condition_values))
    loop = asyncio.get_running_loop()
    try:
        for minute in range(15):
            started = loop.time()
            cutoff = chosen_boundary(now + timedelta(minutes=minute), precision="EXACT")
            await asyncio.wait_for(scheduler.eval_conditions(cutoff.fields("cutoff")), 20 / SCALE)
            await asyncio.sleep(max(0, MINUTE_S - (loop.time() - started)))
    finally:
        await scheduler.close()

    rows = {cond_id: await repo.get("ana.watch_conditions", cond_id) for cond_id in market}
    never = sorted(cond_id for cond_id, row in rows.items() if not row.get("last_evaluated_at"))
    assert never == []
    assert all(row.get("counters") for row in rows.values())
    assert cold_reads and set(cold_reads.values()) == {1}


@pytest.mark.asyncio
async def test_background_read_answers_the_next_minute_at_its_own_cutoff(monkeypatch):
    """A one-window market condition whose read outlasts the budget is evaluated the
    next minute from that read, stamped with the cutoff it was read at."""
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITIONS_BUDGET_S", 0.2)
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITION_BUDGET_S", 0.05)
    repo = MemoryRepository()
    now = parse_time("2026-10-28T12:01:00Z")
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.watch_conditions",
        "c",
        {
            "cond_id": "c",
            "event_id": "e",
            "token_id": "T",
            "status": "ARMED",
            "expires_at": iso(now + timedelta(days=1)),
            "expr": {"metric": "price.close_vs_level", "window": "W1H", "op": "<", "value": 1},
        },
    )
    reads = []

    async def condition_values(event_id, token_id, cutoff, requirements):
        reads.append(cutoff.value)
        await asyncio.sleep(0.15 if len(reads) == 1 else 0)
        return {"cutoff": cutoff.value}

    scheduler = Scheduler(repo, SimpleNamespace(), None, SimpleNamespace(condition_values=condition_values))
    try:
        first = chosen_boundary(now, precision="EXACT")
        outcomes = await scheduler.eval_conditions(first.fields("cutoff"))
        assert outcomes["deferred"] == [{"cond_id": "c", "reason": "READ_TIMEOUT"}]
        await asyncio.sleep(0.2)
        second = chosen_boundary(now + timedelta(minutes=1), precision="EXACT")
        outcomes = await scheduler.eval_conditions(second.fields("cutoff"))
        assert outcomes["evaluated"] == ["c"]
        row = await repo.get("ana.watch_conditions", "c")
        assert row["counters"]["root"]["cutoff"] == iso(now)
        assert reads == [iso(now)]
        third = chosen_boundary(now + timedelta(minutes=2), precision="EXACT")
        await scheduler.eval_conditions(third.fields("cutoff"))
        row = await repo.get("ana.watch_conditions", "c")
        assert row["counters"]["root"]["cutoff"] == iso(now + timedelta(minutes=2))
        assert reads == [iso(now), iso(now + timedelta(minutes=2))]
    finally:
        await scheduler.close()


@pytest.mark.asyncio
async def test_window_condition_is_not_served_an_older_read_cached_for_a_one_window_condition(monkeypatch):
    """Codex on #68: at an hour boundary a one-window condition may be answered by an
    older background read; a 2-window condition of the same event and token in the
    same batch must still be evaluated exactly at the boundary."""
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITIONS_BUDGET_S", 1)
    monkeypatch.setattr(scheduler_module, "EVAL_CONDITION_BUDGET_S", 0.05)
    repo = MemoryRepository()
    boundary = parse_time("2026-10-28T12:00:00Z")
    await repo.put("evt.events", "e", {"event_id": "e"})
    window = {"metric": "spot.cohort.direction", "window": "W1H", "op": "==", "value": "SELL"}
    for cond_id, expr in (("one", dict(window)), ("two", dict(window, persist={"count": 2}))):
        await repo.put(
            "ana.watch_conditions",
            cond_id,
            {
                "cond_id": cond_id,
                "event_id": "e",
                "token_id": "T",
                "status": "ARMED",
                "expires_at": iso(boundary + timedelta(days=1)),
                "expr": expr,
            },
        )
    reads = []

    async def condition_values(event_id, token_id, cutoff, requirements):
        reads.append(cutoff.value)
        await asyncio.sleep(0.1 if cutoff.value == iso(boundary - timedelta(minutes=1)) else 0)
        return {"cutoff": cutoff.value}

    scheduler = Scheduler(repo, SimpleNamespace(), None, SimpleNamespace(condition_values=condition_values))
    try:
        before = chosen_boundary(boundary - timedelta(minutes=1), precision="EXACT")
        await scheduler.eval_conditions(before.fields("cutoff"))
        await asyncio.sleep(0.15)
        conditions = {c: await repo.get("ana.watch_conditions", c) for c in ("one", "two")}
        for cond_id, row in conditions.items():
            stamp = "2026-10-28T11:00:00Z" if cond_id == "one" else "2026-10-28T11:59:30Z"
            await repo.put("ana.watch_conditions", cond_id, {**row, "last_evaluated_at": stamp})
        at = chosen_boundary(boundary, precision="EXACT")
        await scheduler.eval_conditions(at.fields("cutoff"))
        one = await repo.get("ana.watch_conditions", "one")
        two = await repo.get("ana.watch_conditions", "two")
        assert one["counters"]["root"]["cutoff"] == iso(boundary - timedelta(minutes=1))
        assert two["counters"]["root"]["cutoff"] == iso(boundary)
        assert all(row["cutoff"] == iso(boundary) for row in two["counters"]["root"]["history"][-1:])
    finally:
        await scheduler.close()

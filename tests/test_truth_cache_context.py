"""Full assessments isolate caches; selective evaluation explicitly owns reusable caches."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from contextvars import copy_context
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from threading import Barrier

import pytest

import tank.analysis.truth as module
from tank.util import iso, parse_time


def _spot_packet():
    """A complete fifteen-minute event, with current source prices and books."""
    end = parse_time("2026-10-16T12:00:00Z")
    packet = {
        "token_id": "T",
        "cutoff": iso(end),
        "anchor": {"type": "T0", "ts": iso(end - timedelta(minutes=15))},
        "instruments": [],
        "flow_bars": [],
        "klines": [],
        "books": [],
        "tickers": [],
        "oi": [],
        "funding": [],
        "semantics": [],
    }
    for venue in ("A", "B"):
        iid = f"{venue}:spot:T"
        packet["instruments"].append(
            {"instrument_id": iid, "token_id": "T", "venue": venue, "market_type": "SPOT"}
        )
        for minute in range(1, 16):
            packet["flow_bars"].append(
                {
                    "instrument_id": iid,
                    "token_id": "T",
                    "minute_start": iso(end - timedelta(minutes=minute)),
                    "buy_qty": 100,
                    "sell_qty": 50,
                    "sum_sq_qty": 1,
                    "buy_notional": 10000,
                    "sell_notional": 5000,
                    "complete": True,
                }
            )
        packet["klines"].append(
            {
                "instrument_id": iid,
                "token_id": "T",
                "open_time": iso(end - timedelta(minutes=1)),
                "interval_s": 60,
                "c": 100,
                "complete": True,
            }
        )
        packet["books"].append(
            {
                "instrument_id": iid,
                "token_id": "T",
                "exchange_ts": iso(end),
                "integrity_ok": True,
                "mid": 100,
                "range_covered_pct": {"bid": 2, "ask": 2},
                "depth": {"bid": {"p1": {"usd": 100000}}, "ask": {"p1": {"usd": 100000}}},
                "bids": [[99, 1000]],
                "asks": [[101, 1000]],
            }
        )
    return packet


def test_concurrent_actual_assessments_isolate_inherited_cache_and_restore_context(monkeypatch):
    packet = _spot_packet()
    first, second = deepcopy(packet), deepcopy(packet)
    first["event_id"], second["event_id"] = "FIRST", "SECOND"
    first["expected_direction"], second["expected_direction"] = "BULLISH", "LOSER"
    for row in second["flow_bars"]:
        row["buy_qty"], row["sell_qty"] = row["sell_qty"], row["buy_qty"]
        row["buy_notional"], row["sell_notional"] = row["sell_notional"], row["buy_notional"]
    outer_cache = {"caller": "untouched"}
    outer_parameters = {"history_min_days": 999}
    outer_inputs = {"caller": "untouched"}
    outer_cache_token = module._BASELINE_CACHE.set(outer_cache)
    outer_parameters_token = module._PARAMETERS.set(outer_parameters)
    outer_dependency_token = module._BASELINE_DEPENDENCY.set("caller-dependency")
    outer_inputs_token = module._ASSESSMENT_INPUTS.set(outer_inputs)
    barrier = Barrier(2)
    observed = {}
    original = module._compute_market_truth

    def inspect(snapshot, *, config):
        cache = module._BASELINE_CACHE.get()
        assert cache is not outer_cache
        assert cache == {}
        assert module._BASELINE_DEPENDENCY.get() is None
        assert module._ASSESSMENT_INPUTS.get() == {}
        assert module._ASSESSMENT_INPUTS.get() is not outer_inputs
        assert module.parameter("history_min_days", 7) == config["history_min_days"]
        cache["assessment-event"] = snapshot["event_id"]
        observed[snapshot["event_id"]] = cache
        barrier.wait(timeout=20)
        assert cache["assessment-event"] == snapshot["event_id"]
        assert module.parameter("history_min_days", 7) == config["history_min_days"]
        return original(snapshot, config=config)

    monkeypatch.setattr(module, "_compute_market_truth", inspect)

    monkeypatch.setattr(module, "_COMPUTE_SLOT", nullcontext())
    try:
        contexts = [copy_context(), copy_context()]
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(
                contexts[0].run, module.compute_market_truth, first, config={"history_min_days": 7}
            )
            b = pool.submit(
                contexts[1].run, module.compute_market_truth, second, config={"history_min_days": 8}
            )
            results = [a.result(timeout=30), b.result(timeout=30)]
        assert observed["FIRST"] is not observed["SECOND"]
        assert {r["event_id"] for r in results} == {"FIRST", "SECOND"}
        assert {r["relative"]["expected_direction"] for r in results} == {"BULLISH", "LOSER"}
        by_event = {result["event_id"]: result for result in results}
        for event_id, direction, cvd, sign in [
            ("FIRST", "BUY", Decimal(1500), 1),
            ("SECOND", "SELL", Decimal(-1500), -1),
        ]:
            actual = by_event[event_id]
            assert actual["primary_window"] == "EVENT"
            assert actual["data_confidence"]["class"] == "HIGH"
            assert actual["spot"]["EVENT"]["direction"] == direction
            assert Decimal(actual["spot"]["EVENT"]["composite_max"]["cvd"]) == cvd
            assert actual["relative"]["repricing_sign"] == sign
        assert outer_cache == {"caller": "untouched"}
        for context in contexts:
            assert context.get(module._BASELINE_CACHE) is outer_cache
            assert context.get(module._PARAMETERS) is outer_parameters
            assert context.get(module._BASELINE_DEPENDENCY) == "caller-dependency"
            assert context.get(module._ASSESSMENT_INPUTS) is outer_inputs
        assert module._BASELINE_CACHE.get() is outer_cache
        assert module._PARAMETERS.get() is outer_parameters
        assert outer_inputs == {"caller": "untouched"}
    finally:
        module._ASSESSMENT_INPUTS.reset(outer_inputs_token)
        module._BASELINE_DEPENDENCY.reset(outer_dependency_token)
        module._PARAMETERS.reset(outer_parameters_token)
        module._BASELINE_CACHE.reset(outer_cache_token)


def test_failed_assessment_restores_all_inherited_context(monkeypatch):
    cache = {"existing": "caller"}
    parameters = {"normal_range_quantiles": [0.1, 0.9]}
    inputs = {"caller": "untouched"}
    tokens = [
        module._BASELINE_CACHE.set(cache),
        module._PARAMETERS.set(parameters),
        module._BASELINE_DEPENDENCY.set("existing"),
        module._ASSESSMENT_INPUTS.set(inputs),
    ]

    def fail(*args, **kwargs):
        assert module._BASELINE_CACHE.get() == {}
        assert module._BASELINE_CACHE.get() is not cache
        assert module._ASSESSMENT_INPUTS.get() == {}
        module._BASELINE_CACHE.get()["failed-assessment"] = True
        raise RuntimeError("Synthetic assessment failure")

    monkeypatch.setattr(module, "_compute_market_truth", fail)
    try:
        with pytest.raises(RuntimeError, match="Synthetic assessment failure"):
            module.compute_market_truth({}, config={"history_min_days": 7})
        assert module._BASELINE_CACHE.get() is cache
        assert cache == {"existing": "caller"}
        assert module._PARAMETERS.get() is parameters
        assert module._BASELINE_DEPENDENCY.get() == "existing"
        assert module._ASSESSMENT_INPUTS.get() is inputs
        assert inputs == {"caller": "untouched"}
    finally:
        module._ASSESSMENT_INPUTS.reset(tokens[3])
        module._BASELINE_DEPENDENCY.reset(tokens[2])
        module._PARAMETERS.reset(tokens[1])
        module._BASELINE_CACHE.reset(tokens[0])

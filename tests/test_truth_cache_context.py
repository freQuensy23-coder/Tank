"""Full assessments isolate caches; selective evaluation explicitly owns reusable caches."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from contextvars import copy_context
from copy import deepcopy
from pathlib import Path
from runpy import run_path
from threading import Barrier

import pytest

import tank.analysis.truth as module


def test_concurrent_actual_assessments_isolate_inherited_cache_and_restore_context(monkeypatch):
    packet = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    first, second = deepcopy(packet), deepcopy(packet)
    first["event_id"], second["event_id"] = "FIRST", "SECOND"
    first["expected_direction"], second["expected_direction"] = "BULLISH", "LOSER"
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

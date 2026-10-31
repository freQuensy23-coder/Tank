import asyncio
import gc
import sys
import threading
from contextvars import ContextVar, copy_context

import pytest
from market_truth_request_worker import profile_retained_async, profile_retained_call, sample_retained_async


def test_postmeasurement_profile_runs_in_retained_context_and_restores_profiler():
    marker = ContextVar("profile-proof", default="outside")
    token = marker.set("retained")
    context = copy_context()
    marker.reset(token)
    observed = []

    def nested_work():
        observed.append(marker.get())

    def retained_work():
        nested_work()

    rows = profile_retained_call((context, retained_work, (), {}))
    assert observed == ["retained"]
    assert marker.get() == "outside"
    assert sys.getprofile() is None
    selected = [row for row in rows if row["function"] == "retained_work"]
    assert len(selected) == 1
    assert selected[0]["calls"] == 1
    assert selected[0]["module"] == "test_market_truth_profile_receipts.py"
    fields = {"module", "line", "function", "calls", "self_seconds", "inclusive_seconds", "callers"}
    assert all(set(row) == fields for row in rows)
    nested = next(row for row in rows if row["function"] == "nested_work")
    assert any(caller["function"] == "retained_work" and caller["calls"] == 1 for caller in nested["callers"])


def test_postmeasurement_profile_does_not_reuse_or_mutate_the_accepted_baseline_cache():
    baseline = {"already-prepared": 1}
    options = {"baseline_cache": baseline}
    seen = []

    def retained_work(*, baseline_cache):
        seen.append(dict(baseline_cache))
        baseline_cache["diagnostic-only"] = 2

    profile_retained_call((copy_context(), retained_work, (), options))
    assert seen == [{}]
    assert baseline == {"already-prepared": 1}
    assert options == {"baseline_cache": baseline}


def test_postmeasurement_profile_preserves_callback_error_and_restores_profiler():
    failure = ValueError("profile-proof")

    def retained_work():
        raise failure

    with pytest.raises(ValueError) as caught:
        profile_retained_call((copy_context(), retained_work, (), {}))
    assert caught.value is failure
    assert sys.getprofile() is None


def test_diagnostic_profile_records_real_gc_and_restores_callbacks():
    callbacks = list(gc.callbacks)
    receipt = {}

    def retained_work():
        cycle = []
        cycle.append(cycle)
        del cycle
        gc.collect()

    rows = profile_retained_call((copy_context(), retained_work, (), {}), gc_receipt=receipt)
    assert rows and receipt["collections"] >= 1 and receipt["collected"] >= 1
    assert receipt["seconds"] >= 0
    assert receipt["collections"] == sum(value["collections"] for value in receipt["by_generation"].values())
    assert gc.callbacks == callbacks and sys.getprofile() is None


def test_diagnostic_gc_restores_callbacks_when_the_kernel_raises():
    callbacks = list(gc.callbacks)
    receipt = {}
    failure = ValueError("gc-profile-proof")

    def retained_work():
        gc.collect()
        raise failure

    with pytest.raises(ValueError) as caught:
        profile_retained_call((copy_context(), retained_work, (), {}), gc_receipt=receipt)
    assert caught.value is failure and receipt["collections"] >= 1
    assert gc.callbacks == callbacks and sys.getprofile() is None


async def test_async_postmeasurement_profile_retains_context_across_task_handoff():
    marker = ContextVar("async-profile-proof", default="outside")
    token = marker.set("retained")
    context = copy_context()
    marker.reset(token)
    observed = []

    async def retained_work():
        await asyncio.sleep(0)
        observed.append(marker.get())

    rows = await profile_retained_async((context, retained_work, (), {}))
    assert observed == ["retained"]
    assert marker.get() == "outside"
    assert sys.getprofile() is None
    assert any(row["function"] == "retained_work" for row in rows)


async def test_async_postmeasurement_profile_preserves_error_and_restores_profiler():
    failure = ValueError("async-profile-proof")

    async def retained_work():
        raise failure

    with pytest.raises(ValueError) as caught:
        await profile_retained_async((copy_context(), retained_work, (), {}))
    assert caught.value is failure
    assert sys.getprofile() is None


async def test_statistical_profile_retains_context_and_stops_its_bounded_sampler():
    marker = ContextVar("sample-proof", default="outside")
    token = marker.set("retained")
    context = copy_context()
    marker.reset(token)
    observed = []

    async def retained_work():
        observed.append(marker.get())

    receipt = await sample_retained_async((context, retained_work, (), {}))
    assert observed == ["retained"] and marker.get() == "outside"
    assert receipt["kind"] == "statistical_thread_samples"
    assert receipt["ticks"] >= 1 and receipt["rows"]
    assert all(set(row) == {"module", "line", "function", "samples"} for row in receipt["rows"])
    assert not any(thread.name == "market-truth-sampler" for thread in threading.enumerate())
    assert sys.getprofile() is None


async def test_statistical_profile_preserves_callback_error_and_stops_sampler():
    failure = ValueError("sample-proof")

    async def retained_work():
        raise failure

    with pytest.raises(ValueError) as caught:
        await sample_retained_async((copy_context(), retained_work, (), {}))
    assert caught.value is failure
    assert not any(thread.name == "market-truth-sampler" for thread in threading.enumerate())
    assert sys.getprofile() is None

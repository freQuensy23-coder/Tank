import sys
from contextvars import ContextVar, copy_context

import pytest
from market_truth_request_worker import profile_retained_call


def test_postmeasurement_profile_runs_in_retained_context_and_restores_profiler():
    marker = ContextVar("profile-proof", default="outside")
    token = marker.set("retained")
    context = copy_context()
    marker.reset(token)
    observed = []

    def retained_work():
        observed.append(marker.get())

    rows = profile_retained_call((context, retained_work, (), {}))
    assert observed == ["retained"]
    assert marker.get() == "outside"
    assert sys.getprofile() is None
    selected = [row for row in rows if row["function"] == "retained_work"]
    assert len(selected) == 1
    assert selected[0]["calls"] == 1
    assert selected[0]["module"] == "test_market_truth_profile_receipts.py"
    fields = {"module", "line", "function", "calls", "self_seconds", "inclusive_seconds"}
    assert all(set(row) == fields for row in rows)


def test_postmeasurement_profile_preserves_callback_error_and_restores_profiler():
    failure = ValueError("profile-proof")

    def retained_work():
        raise failure

    with pytest.raises(ValueError) as caught:
        profile_retained_call((copy_context(), retained_work, (), {}))
    assert caught.value is failure
    assert sys.getprofile() is None

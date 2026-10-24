"""Watch expressions preserve zero values and reject predicates the scheduler cannot evaluate."""

import pytest
from pydantic import ValidationError

from tank.analysis.watch_contracts import WatchAll, WatchLeaf


def leaf(**changes):
    return WatchLeaf.model_validate(
        {
            "metric": "deriv.native_oi_chg_pct",
            "window": "W1H",
            "op": ">",
            "value": 0,
            "persist": {"count": 2, "unit": "consecutive_windows"},
        }
        | changes
    )


def test_zero_watch_threshold_is_a_value_and_recursive_conjunction_is_typed():
    threshold = leaf()
    conjunction = WatchAll.model_validate({"all": [threshold.model_dump(mode="python")]})
    assert isinstance(conjunction.all[0], WatchLeaf)
    assert conjunction.all[0].value == 0
    assert conjunction.all[0].persist.count == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"metric": "unsupported.metric"},
        {"window": "TYPO"},
        {"op": "in", "value": "HIGH"},
        {"op": "in", "value": []},
        {"value": True},
        {"value": "NaN"},
        {"value": None},
        {"metric": "event.state"},
    ],
)
def test_invalid_predicates_are_rejected_before_persistence(changes):
    with pytest.raises((ValidationError, ValueError)):
        leaf(**changes)

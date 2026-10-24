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


def test_monitoring_policy_is_deterministic_and_identity_free() -> None:
    from decimal import Decimal

    from tank.analysis.watch_contracts import WatchPolicyInputs, WatchReferencePrices
    from tank.analysis.watch_policy import build_watch_conditions

    context = WatchPolicyInputs(
        action="DONT_CHASE",
        move="LIQUIDITY_VACUUM",
        band="EARLY",
        prices=WatchReferencePrices(
            anchor_price=Decimal("10"),
            entry_price=Decimal("11"),
            invalidation_price=None,
        ),
        has_open_call=False,
        wait_data=False,
    )
    first = build_watch_conditions(context)
    assert first == build_watch_conditions(context)
    price_rule = next(
        rule
        for rule in first
        if isinstance(rule.expr, WatchLeaf) and rule.expr.metric == "price.close_vs_level"
    )
    assert isinstance(price_rule.expr, WatchLeaf)
    assert price_rule.expr.value == "10"
    assert price_rule.role == "INVALIDATION"
    assert all("cond_id" not in rule.model_dump(mode="json") for rule in first)


@pytest.mark.parametrize("value", ["not-a-price", {"raw": "1"}, [1], True])
def test_reference_prices_reject_malformed_wire_values_as_validation_errors(value) -> None:
    from tank.analysis.watch_contracts import WatchReferencePrices

    with pytest.raises(ValidationError):
        WatchReferencePrices.model_validate(
            {"anchor_price": value, "entry_price": None, "invalidation_price": None}
        )

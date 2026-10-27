"""A non-estimable baseline does not erase a known current funding observation."""

from decimal import Decimal

import pytest
from pydantic import ValidationError
from test_condition_graph_contracts import funding_observation

from tank.analysis.condition_graph_codec import encode_condition_funding
from tank.analysis.condition_graph_contracts import AvailableConditionFunding
from tank.analysis.statistics_contracts import DecimalBounds, DecimalRange


def test_nonestimable_funding_history_preserves_known_current_zero_and_its_clock() -> None:
    observation = funding_observation("CURRENT_ESTIMATE", Decimal(8))
    history = DecimalRange(bounds=None, reason="NOT_ESTIMABLE")
    history = DecimalRange.model_validate_json(history.model_dump_json())
    funding = AvailableConditionFunding(
        status="OBSERVED",
        settled=None,
        estimate=observation,
        start=None,
        selected_kind="CURRENT_ESTIMATE",
        normal_range=history,
        funding_vs_normal=None,
    )
    wire = encode_condition_funding(funding)
    assert wire["funding_8h_eq"] == "0" and wire["funding_reason"] is None
    assert wire["funding_normal_range_8h"] is None
    assert wire["funding_normal_range_reason"] == "NOT_ESTIMABLE"
    assert wire["funding_vs_normal"] is None
    assert wire["funding_estimate_observed_at"] == observation.observed_at.value
    with pytest.raises(ValidationError, match="actual normalized rate and baseline"):
        AvailableConditionFunding.model_validate(funding.model_copy(update={"funding_vs_normal": "WITHIN"}))


def test_observed_zero_range_is_distinct_from_nonestimable_history() -> None:
    known = DecimalRange(bounds=DecimalBounds(lower=Decimal(0), upper=Decimal(0)), reason=None)
    assert known.bounds is not None and known.bounds.lower == known.bounds.upper == 0
    with pytest.raises(ValidationError, match="ranges require a reason"):
        DecimalRange.model_validate(known.model_copy(update={"reason": "NOT_ESTIMABLE"}))
    absent = DecimalRange(bounds=None, reason="NOT_ESTIMABLE")
    with pytest.raises(ValidationError, match="ranges require a reason"):
        DecimalRange.model_validate(absent.model_copy(update={"reason": None}))
    with pytest.raises(ValidationError):
        DecimalRange.model_validate(absent.model_copy(update={"reason": "UNKNOWN"}))

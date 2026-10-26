"""Spread ratios retain venue weighting and actual source windows."""

from datetime import timedelta

import pytest
from pydantic import ValidationError
from test_condition_graph_contracts import AT, single_venue_liquidity

from tank.analysis.condition_graph_contracts import (
    ConditionAnalysisWindowStart,
    ConditionLiquiditySpreadComparison,
    ConditionPreDepthWindow,
    ConditionSpreadSample,
    ConditionVenueSpreadSamples,
)
from tank.time_precision import Timestamp
from tank.util import iso


def samples(venue: str, values: tuple[float, ...], *, current: bool) -> ConditionVenueSpreadSamples:
    end = AT if current else AT - timedelta(hours=1, minutes=20)
    return ConditionVenueSpreadSamples(
        venue=venue,
        instrument_id=f"spot:{venue}",
        samples=tuple(
            ConditionSpreadSample(
                observed_at=Timestamp(
                    value=iso(end - timedelta(minutes=index)),
                    precision=None,
                    precision_reason="SOURCE_PRECISION_NOT_CAPTURED",
                ),
                spread_bps=value,
            )
            for index, value in enumerate(values)
        ),
    )


def comparison() -> ConditionLiquiditySpreadComparison:
    return ConditionLiquiditySpreadComparison(
        current=(samples("a", (4.0,), current=True), samples("b", (8.0,), current=True)),
        prior=(samples("a", (1.0, 3.0), current=False), samples("b", (10.0,), current=False)),
        prior_window=ConditionPreDepthWindow(
            origin=ConditionAnalysisWindowStart(kind="ANALYSIS_WINDOW_START", at=AT - timedelta(hours=1)),
            start=AT - timedelta(hours=2),
            cutoff=AT - timedelta(hours=1),
        ),
        prior_window_reason=None,
    )


def test_spread_ratio_weights_venue_means_instead_of_pooling_every_sample() -> None:
    value = comparison()
    assert value.current[0].mean_bps() == 4
    assert value.prior[0].mean_bps() == 2
    assert value.ratio().value == 1.0 and value.ratio().reason is None
    assert value.prior[0].samples[0].observed_at.precision is None


def test_spread_ratio_preserves_zero_current_and_unavailable_zero_baseline() -> None:
    value = comparison()
    zero_current = type(value).model_validate(
        value.model_copy(update={"current": (samples("a", (0.0,), current=True),)})
    )
    assert zero_current.ratio().value == 0.0 and zero_current.ratio().reason is None
    zero_prior = type(value).model_validate(
        value.model_copy(update={"prior": (samples("a", (0.0,), current=False),)})
    )
    assert zero_prior.ratio().value is None and zero_prior.ratio().reason == "ZERO_BASELINE"
    unknown = single_venue_liquidity().spread
    assert unknown.ratio().value is None and unknown.ratio().reason == "NO_EVENT_ANCHOR"
    missing = type(value).model_validate(value.model_copy(update={"prior": ()}))
    assert missing.ratio().value is None and missing.ratio().reason == "INCOMPLETE"


def test_spread_samples_cannot_borrow_other_book_clocks_or_outside_baseline() -> None:
    value = comparison()
    with pytest.raises(ValidationError, match="baseline window"):
        type(value).model_validate(value.model_copy(update={"prior": (samples("a", (2.0,), current=True),)}))
    with pytest.raises(ValidationError, match="duplicate.*observation"):
        type(value.current[0]).model_validate(
            value.current[0].model_copy(update={"samples": value.current[0].samples * 2})
        )
    basic = single_venue_liquidity()
    changed = type(basic.spread).model_validate(
        basic.spread.model_copy(update={"current": (samples("OTHER", (1.0,), current=True),)})
    )
    with pytest.raises(ValidationError, match="actual selected books"):
        type(basic).model_validate(basic.model_copy(update={"spread": changed}))


@pytest.mark.parametrize("current,prior", [(1e308, 5e-324), (5e-324, 1e308)])
def test_unrepresentable_spread_ratio_is_not_infinity_or_false_known_zero(
    current: float, prior: float
) -> None:
    value = comparison()
    changed = type(value).model_validate(
        value.model_copy(
            update={
                "current": (samples("a", (current,), current=True),),
                "prior": (samples("a", (prior,), current=False),),
            }
        )
    )
    assert changed.ratio().value is None and changed.ratio().reason == "NOT_ESTIMABLE"

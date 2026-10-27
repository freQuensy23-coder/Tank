"""Pure funding phase and independent-component causes are shared with output models."""

from decimal import Decimal

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.funding_normalization_math import (
    FundingComponentNormalization,
    normalize_funding_components,
)
from tank.contracts.derivative_values import ReportedDerivativeNumber
from tank.contracts.funding import FundingNormalizationRequest


@pytest.mark.parametrize(
    "rate,interval,reason",
    [
        (None, "-4", "RATE_NOT_CAPTURED"),
        ("NaN", "4", "RATE_NOT_ESTIMABLE"),
        ("0", None, "INTERVAL_NOT_CAPTURED"),
        ("0.001", "NaN", "INTERVAL_NOT_ESTIMABLE"),
        ("0.001", "-4", "NONPOSITIVE_INTERVAL"),
        ("0.001", "0", "NONPOSITIVE_INTERVAL"),
    ],
)
def test_actual_component_cause_priority(rate: JsonValue, interval: JsonValue, reason: str) -> None:
    value = normalize_funding_components(
        rate=ReportedDerivativeNumber.from_value(rate),
        interval=ReportedDerivativeNumber.from_value(interval),
        request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"),
    )
    assert value.reason == reason
    assert value.value is None
    assert value.calculation is None
    assert value.interval == ReportedDerivativeNumber.from_value(interval)
    assert value.rate == ReportedDerivativeNumber.from_value(rate)


def test_unrequested_current_arithmetic_does_not_invent_an_interval_or_evaluate_partial_inputs() -> None:
    value = normalize_funding_components(
        rate=ReportedDerivativeNumber.from_value(None),
        interval=ReportedDerivativeNumber.from_value("-4"),
        request=FundingNormalizationRequest(mode="RECORDED_ONLY"),
    )
    assert value.reason == "RAW_NORMALIZATION_NOT_REQUESTED"
    assert value.interval.value == -4
    assert value.calculation is None


def test_known_zero_and_calculation_operands_are_retained() -> None:
    value = normalize_funding_components(
        rate=ReportedDerivativeNumber.from_value("0"),
        interval=ReportedDerivativeNumber.from_value("4"),
        request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"),
    )
    assert value.value == Decimal(0)
    assert value.reason is None
    assert value.calculation is not None
    assert value.calculation.operands.interval_h == 4
    with pytest.raises(ValidationError):
        FundingComponentNormalization.model_validate(value.model_copy(update={"reason": "RATE_NOT_CAPTURED"}))

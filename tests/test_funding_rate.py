"""Recorded funding units and requested current calculations preserve independent facts."""

from decimal import ROUND_UP, Decimal, Inexact, localcontext

import pytest
from funding_input_fixtures import funding_record_fixture, funding_scope_fixture
from pydantic import JsonValue, ValidationError

from tank.analysis.condition_funding_input_codec import decode_funding_inputs
from tank.analysis.condition_funding_inputs import FundingInputObservation
from tank.analysis.funding_normalization_math import (
    FundingRateCalculation,
    FundingRateOperands,
    normalize_funding_rate,
)
from tank.analysis.funding_rate import (
    CalculatedFundingEquivalent,
    RecordedFundingEquivalent,
    UnavailableFundingEquivalent,
    funding_equivalent,
)
from tank.contracts.funding import FundingNormalizationRequest


def observation(rate: JsonValue, interval: JsonValue, recorded: JsonValue) -> FundingInputObservation:
    row = funding_record_fixture("CURRENT_ESTIMATE")
    row.update(rate=rate, interval_h=interval, rate_8h_eq=recorded)
    return decode_funding_inputs(
        token_id="T", instruments=[funding_scope_fixture()], records=[row]
    ).observations[0]


def test_recorded_normalized_value_does_not_invent_unread_raw_rate_or_eight_hour_duration() -> None:
    point = observation(None, None, "-0.002")
    value = funding_equivalent(point, request=FundingNormalizationRequest(mode="RECORDED_ONLY"))
    assert isinstance(value, RecordedFundingEquivalent)
    assert value.value == Decimal("-0.002")
    assert value.basis == "RECORDED_EIGHT_HOUR_EQUIVALENT"
    assert value.source.rate.value is None
    assert value.source.interval_h.value is None


def test_recorded_component_preserves_independent_raw_reports_without_asserting_their_derivation() -> None:
    point = observation("0.9", "4", "-0.002")
    value = funding_equivalent(point, request=FundingNormalizationRequest(mode="RECORDED_ONLY"))
    assert isinstance(value, RecordedFundingEquivalent)
    assert value.value == Decimal("-0.002")
    assert value.source.rate.value == Decimal("0.9")
    assert value.source.interval_h.value == 4


def test_requested_current_normalization_has_a_distinct_basis_and_actual_duration() -> None:
    value = funding_equivalent(
        observation("0.001", "4", None), request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
    )
    assert isinstance(value, CalculatedFundingEquivalent)
    assert value.value == Decimal("0.002")
    assert value.basis == "CURRENT_ANALYSIS_RATE_INTERVAL"
    assert value.calculation_precision_digits == 128
    assert value.source.recorded_eight_hour_rate.status == "NOT_CAPTURED"


@pytest.mark.parametrize(
    "rate,interval,reason",
    [
        (None, "4", "RATE_NOT_CAPTURED"),
        ("NaN", "4", "RATE_NOT_ESTIMABLE"),
        ("0.001", None, "INTERVAL_NOT_CAPTURED"),
        ("0.001", "NaN", "INTERVAL_NOT_ESTIMABLE"),
        ("0.001", "0", "NONPOSITIVE_INTERVAL"),
        ("0.001", "-4", "NONPOSITIVE_INTERVAL"),
        ("0", None, "INTERVAL_NOT_CAPTURED"),
    ],
)
def test_missing_or_unusable_raw_normalization_inputs_never_default_to_eight_hours(
    rate: JsonValue, interval: JsonValue, reason: str
) -> None:
    value = funding_equivalent(
        observation(rate, interval, None), request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
    )
    assert isinstance(value, UnavailableFundingEquivalent)
    assert value.value is None
    assert value.reason == reason


def test_recorded_only_phase_does_not_silently_perform_unrequested_raw_calculation() -> None:
    value = funding_equivalent(
        observation("0.001", "4", None), request=FundingNormalizationRequest(mode="RECORDED_ONLY")
    )
    assert isinstance(value, UnavailableFundingEquivalent)
    assert value.reason == "RAW_NORMALIZATION_NOT_REQUESTED"
    assert value.source.rate.value == Decimal("0.001")


def test_current_normalization_is_independent_of_ambient_decimal_context() -> None:
    point = observation("0.00123456789", "4", None)
    with localcontext() as context:
        context.prec = 2
        value = funding_equivalent(point, request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"))
    assert isinstance(value, CalculatedFundingEquivalent)
    assert value.value == Decimal("0.00246913578")


def test_current_normalization_uses_its_own_rounding_and_traps_for_repeating_rates() -> None:
    operands = FundingRateOperands(rate=Decimal(1), interval_h=Decimal(3))
    expected = normalize_funding_rate(operands)
    with localcontext() as context:
        context.prec = 2
        context.rounding = ROUND_UP
        context.traps[Inexact] = True
        actual = normalize_funding_rate(operands)
    assert actual == expected
    assert actual.value is not None
    assert actual.reason is None
    assert len(actual.value.as_tuple().digits) == 128


@pytest.mark.parametrize(
    "rate,interval,expected",
    [("1E1000000000", "1E1000000000", "8"), ("1E-1000000000", "1", "8E-1000000000")],
)
def test_current_normalization_does_not_expand_shared_or_extreme_decimal_exponents(
    rate: str, interval: str, expected: str
) -> None:
    value = funding_equivalent(
        observation(rate, interval, None), request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
    )
    assert isinstance(value, CalculatedFundingEquivalent)
    assert value.value == Decimal(expected)


def test_invalid_recorded_component_is_not_relabelled_as_a_recorded_current_calculation() -> None:
    value = funding_equivalent(
        observation("0.001", "4", "NaN"), request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
    )
    assert isinstance(value, CalculatedFundingEquivalent)
    assert value.value == Decimal("0.002")
    assert value.source.recorded_eight_hour_rate.status == "NOT_ESTIMABLE"


def test_recorded_negative_zero_is_known_and_retains_its_original_numeric_sign() -> None:
    value = funding_equivalent(
        observation(None, None, "-0.000"), request=FundingNormalizationRequest(mode="RECORDED_ONLY")
    )
    assert isinstance(value, RecordedFundingEquivalent)
    assert value.value == 0
    assert value.value.as_tuple().sign == 1


def test_public_calculated_funding_admission_refuses_forged_value_and_float_precision_count() -> None:
    value = funding_equivalent(
        observation("0.001", "4", None), request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
    )
    assert isinstance(value, CalculatedFundingEquivalent)
    with pytest.raises(ValidationError, match="actual rate"):
        CalculatedFundingEquivalent.model_validate(value.model_copy(update={"value": Decimal(1)}))
    with pytest.raises(ValidationError):
        CalculatedFundingEquivalent.model_validate(
            value.model_copy(update={"calculation_precision_digits": 128.0})
        )


def test_public_pure_normalization_refuses_copied_bad_operands_and_changed_result() -> None:
    operands = FundingRateOperands(rate=Decimal("0.001"), interval_h=Decimal(4))
    with pytest.raises(ValidationError):
        normalize_funding_rate(operands.model_copy(update={"interval_h": Decimal(0)}))
    result = normalize_funding_rate(operands)
    with pytest.raises(ValidationError, match="actual rate and positive duration"):
        FundingRateCalculation.model_validate(result.model_copy(update={"value": Decimal(1)}))

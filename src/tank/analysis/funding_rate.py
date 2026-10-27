"""Recorded eight-hour funding and explicitly requested current normalization are distinct."""

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, model_validator

from tank.analysis.condition_funding_inputs import FundingInputObservation
from tank.analysis.funding_normalization_math import FundingRateOperands, normalize_funding_rate
from tank.contracts import DomainModel
from tank.contracts.scalars import require_integer

type FundingNormalizationMode = Literal["RECORDED_ONLY", "CALCULATE_FROM_RAW"]
type FundingNormalizationFailure = Literal[
    "RAW_NORMALIZATION_NOT_REQUESTED",
    "RATE_NOT_CAPTURED",
    "RATE_NOT_ESTIMABLE",
    "INTERVAL_NOT_CAPTURED",
    "INTERVAL_NOT_ESTIMABLE",
    "NONPOSITIVE_INTERVAL",
    "NOT_ESTIMABLE",
]


class FundingNormalizationRequest(DomainModel):
    mode: FundingNormalizationMode


class _FundingArithmetic(DomainModel):
    value: Decimal | None
    reason: FundingNormalizationFailure | None


def _calculate(source: FundingInputObservation) -> _FundingArithmetic:
    rate, interval = source.rate, source.interval_h
    if rate.value is None:
        return _FundingArithmetic(
            value=None,
            reason="RATE_NOT_CAPTURED" if rate.status == "NOT_CAPTURED" else "RATE_NOT_ESTIMABLE",
        )
    if interval.value is None:
        return _FundingArithmetic(
            value=None,
            reason="INTERVAL_NOT_CAPTURED" if interval.status == "NOT_CAPTURED" else "INTERVAL_NOT_ESTIMABLE",
        )
    if interval.value <= 0:
        return _FundingArithmetic(value=None, reason="NONPOSITIVE_INTERVAL")
    result = normalize_funding_rate(FundingRateOperands(rate=rate.value, interval_h=interval.value))
    return _FundingArithmetic(value=result.value, reason=result.reason)


class RecordedFundingEquivalent(DomainModel):
    status: Literal["OBSERVED"]
    basis: Literal["RECORDED_EIGHT_HOUR_EQUIVALENT"]
    value: Decimal
    source: FundingInputObservation

    @model_validator(mode="after")
    def actual_recorded_value(self) -> Self:
        component = self.source.recorded_eight_hour_rate
        if component.status != "OBSERVED" or component.value != self.value:
            raise ValueError("Recorded eight-hour funding must match its actual stored component")
        return self


class CalculatedFundingEquivalent(DomainModel):
    status: Literal["OBSERVED"]
    basis: Literal["CURRENT_ANALYSIS_RATE_INTERVAL"]
    value: Decimal
    source: FundingInputObservation
    calculation_precision_digits: Annotated[Literal[128], BeforeValidator(require_integer)]

    @model_validator(mode="after")
    def actual_calculation(self) -> Self:
        calculated = _calculate(self.source)
        if calculated.reason is not None or calculated.value != self.value:
            raise ValueError("Calculated funding requires the actual rate and positive measured interval")
        return self


class UnavailableFundingEquivalent(DomainModel):
    status: Literal["UNAVAILABLE"]
    value: None
    source: FundingInputObservation
    request: FundingNormalizationRequest
    reason: FundingNormalizationFailure

    @model_validator(mode="after")
    def actual_absence(self) -> Self:
        if self.source.recorded_eight_hour_rate.status == "OBSERVED":
            raise ValueError("Known recorded eight-hour funding cannot become unavailable")
        expected = (
            "RAW_NORMALIZATION_NOT_REQUESTED"
            if self.request.mode == "RECORDED_ONLY"
            else _calculate(self.source).reason
        )
        if expected is None or self.reason != expected:
            raise ValueError("Funding normalization absence must retain its actual requested phase and cause")
        return self


type FundingEquivalent = (
    RecordedFundingEquivalent | CalculatedFundingEquivalent | UnavailableFundingEquivalent
)


def funding_equivalent(
    source: FundingInputObservation, *, request: FundingNormalizationRequest
) -> FundingEquivalent:
    source = FundingInputObservation.model_validate(source)
    request = FundingNormalizationRequest.model_validate(request)
    recorded = source.recorded_eight_hour_rate
    if recorded.status == "OBSERVED" and recorded.value is not None:
        return RecordedFundingEquivalent(
            status="OBSERVED",
            basis="RECORDED_EIGHT_HOUR_EQUIVALENT",
            value=recorded.value,
            source=source,
        )
    if request.mode == "RECORDED_ONLY":
        return UnavailableFundingEquivalent(
            status="UNAVAILABLE",
            value=None,
            source=source,
            request=request,
            reason="RAW_NORMALIZATION_NOT_REQUESTED",
        )
    calculated = _calculate(source)
    if calculated.value is not None:
        return CalculatedFundingEquivalent(
            status="OBSERVED",
            basis="CURRENT_ANALYSIS_RATE_INTERVAL",
            value=calculated.value,
            source=source,
            calculation_precision_digits=128,
        )
    if calculated.reason is None:
        raise ValueError("Unavailable calculated funding requires its exact failure")
    return UnavailableFundingEquivalent(
        status="UNAVAILABLE", value=None, source=source, request=request, reason=calculated.reason
    )

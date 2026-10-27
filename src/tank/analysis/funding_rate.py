"""Recorded eight-hour funding and explicitly requested current normalization are distinct."""

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, model_validator

from tank.analysis.condition_funding_inputs import FundingInputObservation
from tank.analysis.funding_normalization_math import normalize_funding_components
from tank.contracts import DomainModel
from tank.contracts.funding import FundingNormalizationFailure, FundingNormalizationRequest
from tank.contracts.scalars import require_integer


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
        calculated = normalize_funding_components(
            rate=self.source.rate,
            interval=self.source.interval_h,
            request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"),
        )
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
        expected = normalize_funding_components(
            rate=self.source.rate, interval=self.source.interval_h, request=self.request
        ).reason
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
    calculated = normalize_funding_components(rate=source.rate, interval=source.interval_h, request=request)
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

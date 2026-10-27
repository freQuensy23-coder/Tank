"""Historical settled funding populations retain actual evidence and visibility."""

from datetime import datetime, timedelta
from typing import Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.analysis.condition_funding_inputs import FundingInputObservation
from tank.analysis.funding_quantile import funding_quantile
from tank.analysis.funding_rate import (
    FundingEquivalent,
    FundingNormalizationRequest,
    UnavailableFundingEquivalent,
    funding_equivalent,
)
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.analysis.statistics_contracts import DecimalBounds, DecimalRange
from tank.contracts import DomainModel
from tank.contracts.configuration import Thresholds
from tank.time_precision import Timestamp
from tank.util import parse_iso_time, sha256


class FundingHistoryPolicy(DomainModel):
    minimum_history_days: int = Field(gt=0)
    lookback_days: int = Field(gt=0)
    quantiles: tuple[float, float]
    normalization: FundingNormalizationRequest

    @model_validator(mode="after")
    def ordered_probabilities(self) -> Self:
        if not 0 <= self.quantiles[0] < self.quantiles[1] <= 1:
            raise ValueError("Funding history requires actual ordered quantile probabilities")
        return self

    @classmethod
    def from_thresholds(cls, thresholds: Thresholds, *, normalization: FundingNormalizationRequest) -> Self:
        actual = Thresholds.model_validate(thresholds)
        if len(actual.normal_range_quantiles) != 2:
            raise ValueError("Funding history requires exactly two captured quantile probabilities")
        return cls(
            minimum_history_days=actual.history_min_days,
            lookback_days=actual.base_retention_days,
            quantiles=(actual.normal_range_quantiles[0], actual.normal_range_quantiles[1]),
            normalization=normalization,
        )


class FundingHistoricalPopulation(DomainModel):
    instrument: DerivativeInstrument
    before: Timestamp
    policy: FundingHistoryPolicy
    sources: tuple[FundingInputObservation, ...]
    values: tuple[FundingEquivalent, ...]
    ambiguous_clocks: tuple[Timestamp, ...]

    @model_validator(mode="after")
    def actual_historical_values(self) -> Self:
        if self.before.value is None:
            raise ValueError("Funding history requires its actual historical boundary")
        end = parse_iso_time(self.before.value)
        lower = end - timedelta(days=self.policy.lookback_days)
        groups: dict[datetime, list[FundingInputObservation]] = {}
        for source in self.sources:
            groups.setdefault(_historical_time(source), []).append(source)
        representatives: list[FundingInputObservation] = []
        ambiguous: list[Timestamp] = []
        for candidates in groups.values():
            if len({point.evidence.source_projection_sha256 for point in candidates}) > 1:
                ambiguous.append(candidates[0].clock.timestamp)
            else:
                representatives.append(min(candidates, key=lambda point: point.evidence.row_index))
        if tuple(value.source for value in self.values) != tuple(representatives):
            raise ValueError("Funding samples must preserve each unambiguous actual historical observation")
        for value in self.values:
            if value != funding_equivalent(value.source, request=self.policy.normalization):
                raise ValueError("Funding history normalization must retain its actual requested phase")
        if self.ambiguous_clocks != tuple(ambiguous):
            raise ValueError("Funding history must retain actual conflicting source revisions")
        prior = None
        for source in self.sources:
            at = source.clock.timestamp.value
            if (
                source.instrument != self.instrument
                or source.kind != "SETTLED"
                or at is None
                or not lower <= parse_iso_time(at) < end
            ):
                raise ValueError("Funding history must retain its actual settled scope and interval")
            instant = parse_iso_time(at)
            if prior is not None and instant < prior:
                raise ValueError("Funding history must preserve chronological source order")
            prior = instant
        return self

    def normal_range(self) -> DecimalRange:
        if self.ambiguous_clocks:
            return DecimalRange(bounds=None, reason="INCOMPLETE")
        unavailable = tuple(value for value in self.values if isinstance(value, UnavailableFundingEquivalent))
        if unavailable:
            return DecimalRange(
                bounds=None,
                reason="NOT_ESTIMABLE"
                if any(value.reason == "NOT_ESTIMABLE" for value in unavailable)
                else "INCOMPLETE",
            )
        if (
            not self.values
            or (
                _historical_time(self.values[-1].source) - _historical_time(self.values[0].source)
            ).total_seconds()
            < self.policy.minimum_history_days * 86400
        ):
            return DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY")
        rates = tuple(value.value for value in self.values if value.value is not None)
        lower = funding_quantile(rates, self.policy.quantiles[0])
        upper = funding_quantile(rates, self.policy.quantiles[1])
        if lower.value is None or upper.value is None:
            return DecimalRange(bounds=None, reason="NOT_ESTIMABLE")
        return DecimalRange(bounds=DecimalBounds(lower=lower.value, upper=upper.value), reason=None)

    def identity(self) -> str:
        return sha256((self.instrument, self.before, self.policy, self.sources))


def _historical_time(point: FundingInputObservation) -> datetime:
    value = point.clock.timestamp.value
    if value is None:
        raise ValueError("A selected funding history point requires its actual source clock")
    return parse_iso_time(value)


def funding_historical_sources(
    calculator: FundingCalculator,
    request: FundingWindowRequest,
    *,
    instrument: DerivativeInstrument,
    policy: FundingHistoryPolicy,
) -> tuple[FundingInputObservation, ...]:
    request = FundingWindowRequest.model_validate(request)
    instrument = DerivativeInstrument.model_validate(instrument)
    policy = FundingHistoryPolicy.model_validate(policy)
    if instrument not in calculator.source.instruments:
        raise ValueError("Funding history instrument must belong to the actual admitted read")
    if request.start.value is None:
        raise ValueError("Funding history requires an actual window start")
    end = parse_iso_time(request.start.value)
    lower = end - timedelta(days=policy.lookback_days)
    selected = tuple(
        sorted(
            (
                point
                for point in calculator.source.observations
                if point.instrument == instrument
                and point.kind == "SETTLED"
                and point.clock.timestamp.value is not None
                and lower <= parse_iso_time(point.clock.timestamp.value) < end
                and point.visible_at(request.assembled_at) is True
            ),
            key=_historical_time,
        )
    )
    return selected


def funding_historical_population(
    calculator: FundingCalculator,
    request: FundingWindowRequest,
    *,
    instrument: DerivativeInstrument,
    policy: FundingHistoryPolicy,
) -> FundingHistoricalPopulation:
    selected = funding_historical_sources(calculator, request, instrument=instrument, policy=policy)
    groups: dict[datetime, list[FundingInputObservation]] = {}
    for point in selected:
        groups.setdefault(_historical_time(point), []).append(point)
    representatives: list[FundingInputObservation] = []
    ambiguous: list[Timestamp] = []
    for candidates in groups.values():
        if len({point.evidence.source_projection_sha256 for point in candidates}) > 1:
            ambiguous.append(candidates[0].clock.timestamp)
        else:
            representatives.append(min(candidates, key=lambda point: point.evidence.row_index))
    return FundingHistoricalPopulation(
        instrument=instrument,
        before=request.start,
        policy=policy,
        sources=selected,
        values=tuple(funding_equivalent(point, request=policy.normalization) for point in representatives),
        ambiguous_clocks=tuple(ambiguous),
    )

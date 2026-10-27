"""Operational funding baseline cache shares the existing global retention owner."""

from typing import Self

from pydantic import model_validator

from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.analysis.funding_history import (
    FundingHistoricalPopulation,
    FundingHistoryPolicy,
    funding_historical_population,
    funding_historical_sources,
)
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.analysis.statistics_contracts import BaselineCache, BaselineRetention, DecimalRange
from tank.contracts import DomainModel
from tank.util import sha256


class FundingHistoricalBaseline(DomainModel):
    population: FundingHistoricalPopulation
    normal_range: DecimalRange

    @model_validator(mode="after")
    def actual_population_range(self) -> Self:
        if self.normal_range != self.population.normal_range():
            raise ValueError("Funding baseline must retain its exact actual historical population range")
        return self


class FundingHistoryCache:
    """Cache historical facts without assembly; exact read descriptors remain binding."""

    __slots__ = ("_cache",)

    def __init__(self, retention: BaselineRetention) -> None:
        self._cache: BaselineCache[FundingHistoricalBaseline] = BaselineCache("funding_history", retention)

    def baseline(
        self,
        calculator: FundingCalculator,
        request: FundingWindowRequest,
        *,
        instrument: DerivativeInstrument,
        policy: FundingHistoryPolicy,
    ) -> FundingHistoricalBaseline:
        request = FundingWindowRequest.model_validate(request)
        instrument = DerivativeInstrument.model_validate(instrument)
        policy = FundingHistoryPolicy.model_validate(policy)
        sources = funding_historical_sources(calculator, request, instrument=instrument, policy=policy)
        identity = sha256((instrument, request.start, policy, sources))
        cached = self._cache.get(identity)
        if cached is None:
            population = funding_historical_population(
                calculator, request, instrument=instrument, policy=policy
            )
            cached = FundingHistoricalBaseline(population=population, normal_range=population.normal_range())
            self._cache.put(identity, cached)
        return cached

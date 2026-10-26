"""Typed historical OI populations sharing the existing bounded retention owner."""

from typing import Self

from pydantic import model_validator

from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
)
from tank.analysis.condition_graph_contracts import ConditionWindow
from tank.analysis.oi_composite_history import OICompositeHistoryBaseline
from tank.analysis.oi_synchronization import (
    ObservedOISynchronizedSelection,
)
from tank.analysis.open_interest import (
    OINativeHistoryBaseline,
    OIRecordedUsdHistoryBaseline,
    OIWindowRequest,
    OpenInterestCalculator,
)
from tank.analysis.statistics_contracts import BaselineCache, BaselineRetention
from tank.contracts import DomainModel
from tank.util import sha256


class OIIndividualHistoryPopulation(DomainModel):
    native: tuple[OINativeHistoryBaseline, ...]
    recorded_usd: tuple[OIRecordedUsdHistoryBaseline, ...]

    @model_validator(mode="after")
    def actual_instrument_cohort(self) -> Self:
        native = tuple((point.instrument, point.interval) for point in self.native)
        usd = tuple((point.instrument, point.interval) for point in self.recorded_usd)
        if native != usd or len({point.instrument for point in self.native}) != len(self.native):
            raise ValueError(
                "Native and recorded USD histories must preserve their actual instrument universe"
            )
        return self


class OIHistoryCache:
    """Retain closed historical facts without current endpoints or processing assembly."""

    __slots__ = ("_individual", "_composite")

    def __init__(self, retention: BaselineRetention) -> None:
        self._individual: BaselineCache[OIIndividualHistoryPopulation] = BaselineCache(
            "oi_individual_history", retention
        )
        self._composite: BaselineCache[OICompositeHistoryBaseline] = BaselineCache(
            "oi_composite_history", retention
        )

    def individual(
        self, calculator: OpenInterestCalculator, request: OIWindowRequest, *, window: ConditionWindow
    ) -> OIIndividualHistoryPopulation:
        identities = tuple(point.instrument_id for point in calculator.instruments)
        key = sha256((window, calculator.history_identity(request, instrument_ids=identities)))
        cached = self._individual.get(key)
        if cached is None:
            cached = OIIndividualHistoryPopulation(
                native=calculator.native_baseline(request, window=window),
                recorded_usd=calculator.recorded_usd_baseline(request, window=window),
            )
            self._individual.put(key, cached)
        return cached

    def composite_baseline(
        self,
        calculator: OpenInterestCalculator,
        request: OIWindowRequest,
        *,
        selection: ObservedOISynchronizedSelection,
        rules: CapturedOISynchronizationRules,
        window: ConditionWindow,
    ) -> OICompositeHistoryBaseline:
        selection = ObservedOISynchronizedSelection.model_validate(selection)
        identities = tuple(point.instrument.instrument_id for point in selection.sources)
        if selection.token_id != calculator.token_id or selection.interval.window != window:
            raise ValueError("Historical OI cache must preserve its actual selected scope")
        key = sha256(
            (
                window,
                calculator.history_identity(request, instrument_ids=identities),
                selection.native_witness.aggregation_class,
                rules,
            )
        )
        history = self._composite.get(key)
        if history is None:
            history = calculator.history_for_selection(request, selection=selection, rules=rules)
            self._composite.put(key, history)
        return history

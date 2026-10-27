"""Funding-only financial facts retain selected source components independently."""

from typing import Literal, Self

from pydantic import model_validator

from tank.analysis.condition_funding_inputs import FundingInputEvidenceRef
from tank.analysis.condition_graph_contracts import (
    AvailableConditionFunding,
    BasicConditionDerivatives,
    ComputedConditionFundingObservation,
    ConditionDerivativeVenue,
    ConditionFunding,
    ConditionFundingObservationValue,
    ConditionInterval,
    ConditionWindow,
    RecordedConditionFundingObservation,
    UnavailableConditionFunding,
    UnnormalizedConditionFundingObservation,
    UnrequestedConditionBasis,
    UnrequestedConditionFundingDispersion,
    UnrequestedConditionIncrementalVolumes,
    UnrequestedConditionLiquidations,
    UnrequestedConditionOpenInterest,
    UnrequestedConditionSynchronizedOpenInterest,
)
from tank.analysis.funding_history import FundingHistoryPolicy
from tank.analysis.funding_history_cache import FundingHistoryCache
from tank.analysis.funding_normalization_math import normalize_funding_components
from tank.analysis.funding_rate import (
    CalculatedFundingEquivalent,
    FundingEquivalent,
    RecordedFundingEquivalent,
    funding_equivalent,
)
from tank.analysis.funding_selection import (
    FundingCalculator,
    FundingObservationSelection,
    FundingSelectionRole,
    FundingWindowRequest,
    SelectedFundingObservation,
)
from tank.contracts import DomainModel
from tank.contracts.funding import FundingNormalizationRequest
from tank.util import parse_iso_time


class FundingSelectedObservationFacts(DomainModel):
    role: Literal["LATEST_SETTLED", "LATEST_ESTIMATE", "SETTLED_START"]
    equivalent: FundingEquivalent

    @model_validator(mode="after")
    def actual_source_role(self) -> Self:
        expected = "CURRENT_ESTIMATE" if self.role == "LATEST_ESTIMATE" else "SETTLED"
        if self.equivalent.source.kind != expected:
            raise ValueError("Funding financial facts must retain their actual selected source role")
        return self


class FundingConditionCalculation(DomainModel):
    result: BasicConditionDerivatives
    selected: tuple[FundingSelectedObservationFacts, ...]

    @model_validator(mode="after")
    def actual_selected_scope(self) -> Self:
        venues = {point.instrument_id: point.venue for point in self.result.per_venue}
        identities: set[tuple[str, str]] = set()
        for fact in self.selected:
            source = fact.equivalent.source
            instrument = source.instrument
            identity = (instrument.instrument_id, fact.role)
            if (
                instrument.token_id != self.result.token_id
                or venues.get(instrument.instrument_id) != instrument.venue
                or identity in identities
            ):
                raise ValueError("Funding facts must preserve their actual distinct source scope")
            at = source.clock.timestamp.value
            bound = (
                self.result.interval.start if fact.role == "SETTLED_START" else self.result.interval.cutoff
            )
            if at is None or parse_iso_time(at) > bound:
                raise ValueError("Selected funding facts cannot exceed their actual calculation bound")
            identities.add(identity)
        facts = {
            (point.equivalent.source.instrument.instrument_id, point.role): point for point in self.selected
        }
        for venue in self.result.per_venue:
            funding = venue.funding
            if not isinstance(funding, AvailableConditionFunding | UnavailableConditionFunding):
                raise ValueError("Funding-only calculation must evaluate each actual instrument")
            observations: tuple[tuple[FundingSelectionRole, ConditionFundingObservationValue | None], ...] = (
                ("LATEST_SETTLED", funding.settled),
                ("LATEST_ESTIMATE", funding.estimate),
                ("SETTLED_START", funding.start),
            )
            for role, observation in observations:
                if observation != _observation(facts.get((venue.instrument_id, role))):
                    raise ValueError("Funding graph must preserve its actual selected source calculation")
        return self

    def selected_reference(self, instrument_id: str) -> FundingInputEvidenceRef | None:
        venue = next((point for point in self.result.per_venue if point.instrument_id == instrument_id), None)
        if venue is None:
            raise ValueError("Funding selected witness requires an actual calculated instrument")
        if not isinstance(venue.funding, AvailableConditionFunding):
            return None
        role = "LATEST_ESTIMATE" if venue.funding.selected_kind == "CURRENT_ESTIMATE" else "LATEST_SETTLED"
        matches = tuple(
            fact.equivalent.source.evidence
            for fact in self.selected
            if fact.role == role and fact.equivalent.source.instrument.instrument_id == instrument_id
        )
        if len(matches) != 1:
            raise ValueError("Available funding requires its exact selected source witness")
        return matches[0]


def _facts(
    selection: FundingObservationSelection, *, normalization: FundingNormalizationRequest
) -> FundingSelectedObservationFacts | None:
    if not isinstance(selection, SelectedFundingObservation):
        return None
    return FundingSelectedObservationFacts(
        role=selection.role,
        equivalent=funding_equivalent(selection.observation, request=normalization),
    )


def _observation(fact: FundingSelectedObservationFacts | None) -> ConditionFundingObservationValue | None:
    if fact is None:
        return None
    equivalent = fact.equivalent
    source = equivalent.source
    if isinstance(equivalent, RecordedFundingEquivalent):
        return RecordedConditionFundingObservation(
            basis=equivalent.basis,
            kind=source.kind,
            raw_rate=source.rate,
            raw_interval_h=source.interval_h,
            equivalent_eight_hour_rate=equivalent.value,
            observed_at=source.clock.timestamp,
        )
    normalization = normalize_funding_components(
        rate=source.rate,
        interval=source.interval_h,
        request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
        if isinstance(equivalent, CalculatedFundingEquivalent)
        else equivalent.request,
    )
    if normalization.calculation is not None:
        return ComputedConditionFundingObservation(
            basis="CURRENT_ANALYSIS_RATE_INTERVAL",
            kind=source.kind,
            calculation=normalization.calculation,
            observed_at=source.clock.timestamp,
        )
    return UnnormalizedConditionFundingObservation(
        phase="RAW_COMPONENTS_ONLY",
        kind=source.kind,
        normalization=normalization,
        recorded_eight_hour_rate=source.recorded_eight_hour_rate,
        observed_at=source.clock.timestamp,
    )


def condition_funding_family(
    calculator: FundingCalculator,
    request: FundingWindowRequest,
    *,
    history_policy: FundingHistoryPolicy,
    history_cache: FundingHistoryCache,
    window: ConditionWindow,
) -> FundingConditionCalculation:
    request = FundingWindowRequest.model_validate(request)
    history_policy = FundingHistoryPolicy.model_validate(history_policy)
    if request.start.value is None or request.cutoff.value is None:
        raise ValueError("Funding family requires actual request bounds")
    interval = ConditionInterval(
        window=window, start=parse_iso_time(request.start.value), cutoff=parse_iso_time(request.cutoff.value)
    )
    selected: list[FundingSelectedObservationFacts] = []
    venues: list[ConditionDerivativeVenue] = []
    for selection in calculator.select(request):
        facts = tuple(
            _facts(point, normalization=history_policy.normalization)
            for point in (selection.settled, selection.estimate, selection.start)
        )
        selected.extend(point for point in facts if point is not None)
        settled, estimate, start = tuple(_observation(point) for point in facts)
        baseline = history_cache.baseline(
            calculator, request, instrument=selection.instrument, policy=history_policy
        ).normal_range
        current = (
            estimate
            if estimate is not None and estimate.equivalent_eight_hour_rate is not None
            else settled
            if settled is not None and settled.equivalent_eight_hour_rate is not None
            else None
        )
        funding: ConditionFunding
        if current is None:
            funding = UnavailableConditionFunding(
                status="UNAVAILABLE",
                reason="INCOMPLETE",
                settled=settled,
                estimate=estimate,
                start=start,
                normal_range=baseline,
            )
        else:
            value = current.equivalent_eight_hour_rate
            if value is None:
                raise ValueError("Observed funding family requires its actual normalized value")
            bounds = baseline.bounds
            position: Literal["BELOW", "WITHIN", "ABOVE"] | None = (
                None
                if bounds is None
                else "BELOW"
                if value < bounds.lower
                else "ABOVE"
                if value > bounds.upper
                else "WITHIN"
            )
            funding = AvailableConditionFunding(
                status="OBSERVED",
                settled=settled,
                estimate=estimate,
                start=start,
                selected_kind=current.kind,
                normal_range=baseline,
                funding_vs_normal=position,
            )
        venues.append(
            ConditionDerivativeVenue(
                venue=selection.instrument.venue,
                instrument_id=selection.instrument.instrument_id,
                open_interest=UnrequestedConditionOpenInterest(status="NOT_REQUESTED"),
                funding=funding,
                liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
                oi_temporal_exclusions=None,
                basis=UnrequestedConditionBasis(status="NOT_REQUESTED"),
            )
        )
    return FundingConditionCalculation(
        result=BasicConditionDerivatives(
            kind="BASIC",
            token_id=calculator.source.token_id,
            interval=interval,
            per_venue=tuple(venues),
            synchronized=UnrequestedConditionSynchronizedOpenInterest(status="NOT_REQUESTED"),
            funding_dispersion=UnrequestedConditionFundingDispersion(status="NOT_REQUESTED"),
            liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
            incremental_volume=UnrequestedConditionIncrementalVolumes(status="NOT_REQUESTED"),
        ),
        selected=tuple(selected),
    )

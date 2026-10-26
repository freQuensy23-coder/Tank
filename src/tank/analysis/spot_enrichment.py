"""Typed spot-enrichment phases independent of captured-input adapters and pairwise policy."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_graph_contracts import (
    ConditionCohortComparison,
    ConditionWindow,
    EnrichedConditionSpot,
    ObservedConditionCohortComparison,
    ObservedConditionSizeProfile,
    ScopedConditionSpot,
    UnavailableConditionSizeProfile,
    UnrequestedConditionSizeProfile,
    WindowedConditionSpot,
    sum_condition_native,
    validate_condition_spot_pairs,
)
from tank.contracts import DomainModel

WINDOWS: frozenset[ConditionWindow] = frozenset({"W15M", "W1H", "W4H", "W24H", "EVENT"})
FIXED_SECONDS: Mapping[ConditionWindow, float] = MappingProxyType(
    {"W15M": 900.0, "W1H": 3600.0, "W4H": 14400.0, "W24H": 86400.0}
)


class CohortEnrichedConditionSpot(ScopedConditionSpot):
    kind: Literal["COHORT_ENRICHED"]
    enrichment_role: Literal["PRIMARY", "OTHER_WINDOW"]
    cohort_comparisons: dict[ConditionWindow, ConditionCohortComparison]
    size_profile: UnrequestedConditionSizeProfile

    @model_validator(mode="after")
    def actual_pairwise_phase(self) -> Self:
        expected = WINDOWS - {self.interval.window}
        if set(self.cohort_comparisons) != expected or set(self.cohort_consistent) != expected:
            raise ValueError("Cohort enrichment requires each actual other window comparison")
        own = {venue.venue: venue for venue in self.venues}
        for name, comparison in self.cohort_comparisons.items():
            if self.cohort_consistent[name] != isinstance(comparison, ObservedConditionCohortComparison):
                raise ValueError("Cohort consistency must preserve the actual computed comparison state")
            if not set(comparison.cohort) <= set(own):
                raise ValueError("A cohort comparison cannot borrow an unobserved current venue")
            if isinstance(comparison, ObservedConditionCohortComparison) and (
                comparison.current.volume
                != sum_condition_native(own[venue].volume_native for venue in comparison.cohort)
                or comparison.current.cvd
                != sum_condition_native(own[venue].cvd for venue in comparison.cohort)
            ):
                raise ValueError("A cohort comparison must preserve exact current native totals")
        return self

    @classmethod
    def from_windowed(
        cls,
        value: WindowedConditionSpot,
        *,
        comparisons: Mapping[ConditionWindow, ConditionCohortComparison],
        role: Literal["PRIMARY", "OTHER_WINDOW"],
    ) -> Self:
        spot = WindowedConditionSpot.model_validate(value)
        return cls(
            kind="COHORT_ENRICHED",
            token_id=spot.token_id,
            venues=spot.venues,
            composite_max=spot.composite_max,
            breadth=spot.breadth,
            cw_breadth=spot.cw_breadth,
            exact_majority=spot.exact_majority,
            topn=spot.topn,
            concentration=spot.concentration,
            direction=spot.direction,
            direction_reason=spot.direction_reason,
            classification=spot.classification,
            coverage=spot.coverage,
            interval=spot.interval,
            coverage_through=spot.coverage_through,
            composite_cohort=spot.composite_cohort,
            cohort_consistent={
                name: isinstance(comparison, ObservedConditionCohortComparison)
                for name, comparison in comparisons.items()
            },
            enrichment_role=role,
            cohort_comparisons=dict(comparisons),
            size_profile=UnrequestedConditionSizeProfile(status="NOT_REQUESTED"),
        )


class ConditionSpotCohortGraph(DomainModel):
    spots: dict[ConditionWindow, CohortEnrichedConditionSpot]
    durations_s: dict[ConditionWindow, Annotated[float, Field(gt=0)]]
    primary: ConditionWindow

    @model_validator(mode="after")
    def actual_window_and_pair_scope(self) -> Self:
        _validate_window_index(self.spots, self.durations_s, self.primary)
        validate_condition_spot_pairs(self.spots)
        return self


class ConditionEnrichedSpotGraph(DomainModel):
    spots: dict[ConditionWindow, EnrichedConditionSpot]
    durations_s: dict[ConditionWindow, Annotated[float, Field(gt=0)]]
    primary: ConditionWindow

    @model_validator(mode="after")
    def actual_completed_phase(self) -> Self:
        _validate_window_index(self.spots, self.durations_s, self.primary)
        validate_condition_spot_pairs(self.spots)
        return self


def _validate_window_index(
    spots: Mapping[ConditionWindow, CohortEnrichedConditionSpot | EnrichedConditionSpot],
    durations: Mapping[ConditionWindow, float],
    primary: ConditionWindow,
) -> None:
    if set(spots) != WINDOWS or set(durations) != WINDOWS:
        raise ValueError("Enrichment requires the actual five named spot windows")
    if (
        len({spot.token_id for spot in spots.values()}) != 1
        or len({spot.interval.cutoff for spot in spots.values()}) != 1
    ):
        raise ValueError("Enrichment cannot borrow another token or cutoff")
    for name, spot in spots.items():
        if name != spot.interval.window or (spot.enrichment_role == "PRIMARY") != (name == primary):
            raise ValueError("Enrichment must preserve each window and the actual primary role")
        duration = (spot.interval.cutoff - spot.interval.start).total_seconds()
        if duration != durations[name]:
            raise ValueError("Enrichment must preserve actual interval durations")
        if name in FIXED_SECONDS and duration != FIXED_SECONDS[name]:
            raise ValueError("A fixed named enrichment window requires its actual duration")


def finish_spot_enrichment(
    value: ConditionSpotCohortGraph,
    *,
    primary_size_profile: ObservedConditionSizeProfile | UnavailableConditionSizeProfile,
) -> ConditionEnrichedSpotGraph:
    phase = ConditionSpotCohortGraph.model_validate(value)
    profile = type(primary_size_profile).model_validate(primary_size_profile)
    spots: dict[ConditionWindow, EnrichedConditionSpot] = {}
    for name, spot in phase.spots.items():
        spots[name] = EnrichedConditionSpot(
            kind="ENRICHED",
            token_id=spot.token_id,
            venues=spot.venues,
            composite_max=spot.composite_max,
            breadth=spot.breadth,
            cw_breadth=spot.cw_breadth,
            exact_majority=spot.exact_majority,
            topn=spot.topn,
            concentration=spot.concentration,
            direction=spot.direction,
            direction_reason=spot.direction_reason,
            classification=spot.classification,
            coverage=spot.coverage,
            interval=spot.interval,
            coverage_through=spot.coverage_through,
            composite_cohort=spot.composite_cohort,
            cohort_consistent=dict(spot.cohort_consistent),
            enrichment_role=spot.enrichment_role,
            cohort_comparisons=dict(spot.cohort_comparisons),
            size_profile=profile
            if name == phase.primary
            else UnrequestedConditionSizeProfile(status="NOT_REQUESTED"),
        )
    return ConditionEnrichedSpotGraph(spots=spots, durations_s=dict(phase.durations_s), primary=phase.primary)


class ConditionRequestedSpotView(DomainModel):
    requested: WindowedConditionSpot
    state: CohortEnrichedConditionSpot | EnrichedConditionSpot | None

    @model_validator(mode="after")
    def same_consumed_spot_facts(self) -> Self:
        source, state = self.requested, self.state
        if state is None:
            return self
        if (source.token_id, source.interval, source.coverage_through) != (
            state.token_id,
            state.interval,
            state.coverage_through,
        ):
            raise ValueError(
                "Requested and enriched spot views require the same actual subject/window/source clock"
            )
        if (
            source.venues,
            source.composite_max,
            source.breadth,
            source.cw_breadth,
            source.exact_majority,
            source.topn,
            source.concentration,
            source.direction,
            source.direction_reason,
            source.classification,
            source.coverage,
        ) != (
            state.venues,
            state.composite_max,
            state.breadth,
            state.cw_breadth,
            state.exact_majority,
            state.topn,
            state.concentration,
            state.direction,
            state.direction_reason,
            state.classification,
            state.coverage,
        ):
            raise ValueError("Enrichment cannot replace the actual selected spot financial or coverage facts")
        return self

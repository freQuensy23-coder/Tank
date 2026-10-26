"""Like-for-like historical OI composite populations over actual source pairs."""

from bisect import bisect_left
from datetime import datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
    OIInputEvidenceRef,
    OIObservation,
    OIReadSnapshot,
    OISelectionPolicy,
)
from tank.analysis.condition_graph_contracts import ConditionInterval, sum_condition_native
from tank.analysis.oi_ratio import OiEndpoints, OiFractionNotEstimable, exact_oi_endpoint_fraction
from tank.analysis.oi_synchronization import ObservedOISynchronizedSelection, _qualified_rule
from tank.analysis.open_interest import OIWindowRequest, _display_bound, _exclusion, _quantile
from tank.analysis.statistics_contracts import FloatBounds, FloatRange
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import parse_iso_time


class OIHistoricalPairContribution(DomainModel):
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    start_evidence: OIInputEvidenceRef
    end_evidence: OIInputEvidenceRef
    start_observed_at: Timestamp
    end_observed_at: Timestamp
    native_start: Decimal | None = Field(gt=0)
    native_end: Decimal | None = Field(ge=0)
    usd_start: Decimal | None = Field(gt=0)
    usd_end: Decimal | None = Field(ge=0)

    @model_validator(mode="after")
    def actual_pair(self) -> Self:
        if self.start_observed_at.value is None or self.end_observed_at.value is None:
            raise ValueError("Historical OI pairs require actual source endpoint clocks")
        if parse_iso_time(self.start_observed_at.value) >= parse_iso_time(self.end_observed_at.value):
            raise ValueError("Historical OI cannot pair a source observation with itself")
        if (self.native_start is None) != (self.native_end is None) or (self.usd_start is None) != (
            self.usd_end is None
        ):
            raise ValueError("Each historical OI measure requires both actual endpoints")
        if self.native_start is None and self.usd_start is None:
            raise ValueError("Historical OI contributions need an observed measure")
        return self


class OIHistoricalCompositePair(DomainModel):
    start: datetime
    end: datetime
    contributions: tuple[OIHistoricalPairContribution, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def actual_cohort(self) -> Self:
        if self.start.tzinfo is None or self.end.tzinfo is None or self.start >= self.end:
            raise ValueError("Historical OI grid must preserve actual forward UTC bounds")
        identities = [value.instrument_id for value in self.contributions]
        if len(identities) != len(set(identities)):
            raise ValueError("A historical OI pair cannot duplicate an instrument")
        return self

    def endpoints(self, measure: Literal["NATIVE", "RECORDED_USD"]) -> OiEndpoints | None:
        selected = tuple(
            (point.native_start, point.native_end)
            if measure == "NATIVE"
            else (point.usd_start, point.usd_end)
            for point in self.contributions
        )
        starts: list[Decimal] = []
        ends: list[Decimal] = []
        for initial, final in selected:
            if initial is not None and final is not None:
                starts.append(initial)
                ends.append(final)
        return (
            OiEndpoints(start=sum_condition_native(starts), end=sum_condition_native(ends))
            if starts
            else None
        )


class OICompositeHistoryBaseline(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    interval: ConditionInterval
    current_instrument_ids: tuple[str, ...] = Field(min_length=1)
    aggregation_class: str = Field(min_length=1, pattern=r"\S")
    native_normal_range: FloatRange
    usd_normal_range: FloatRange
    pairs: tuple[OIHistoricalCompositePair, ...]
    grid_count: int = Field(ge=0)
    ambiguous_source_times: bool
    policy: OISelectionPolicy
    source_grid: tuple[datetime, ...]

    @model_validator(mode="after")
    def actual_population(self) -> Self:
        if len(set(self.current_instrument_ids)) != len(self.current_instrument_ids):
            raise ValueError("Historical OI scope must preserve distinct actual instruments")
        if (
            self.grid_count != len(self.source_grid)
            or tuple(sorted(set(self.source_grid))) != self.source_grid
        ):
            raise ValueError("Historical OI grid must preserve distinct ordered actual source clocks")
        if any(at.tzinfo is None or at >= self.interval.start for at in self.source_grid):
            raise ValueError("Historical OI grid must remain before the actual requested window")
        if len(self.pairs) > self.grid_count:
            raise ValueError("Historical OI cannot invent source grid points")
        for pair in self.pairs:
            if (
                pair.end not in self.source_grid
                or pair.end >= self.interval.start
                or pair.end - pair.start != self.interval.cutoff - self.interval.start
            ):
                raise ValueError("Historical OI pairs must preserve the actual pre-window duration")
            if any(point.instrument_id not in self.current_instrument_ids for point in pair.contributions):
                raise ValueError("Historical OI cannot borrow an unrelated instrument")
        if self.ambiguous_source_times and (
            self.native_normal_range.bounds is not None or self.usd_normal_range.bounds is not None
        ):
            raise ValueError("Ambiguous source history cannot establish an observed baseline")
        measures: tuple[tuple[Literal["NATIVE", "RECORDED_USD"], FloatRange], ...] = (
            ("NATIVE", self.native_normal_range),
            ("RECORDED_USD", self.usd_normal_range),
        )
        for measure, actual in measures:
            expected = _normal_range(
                self.pairs,
                measure=measure,
                policy=self.policy,
                source_times=self.source_grid,
                duration=self.interval.cutoff - self.interval.start,
                ambiguous=self.ambiguous_source_times,
            )
            if actual != expected:
                raise ValueError("Historical OI range must preserve its actual paired population")
        return self


class ObservedOICompositeWindow(DomainModel):
    status: Literal["OBSERVED"]
    selection: ObservedOISynchronizedSelection
    history: OICompositeHistoryBaseline

    @model_validator(mode="after")
    def actual_selected_scope(self) -> Self:
        if (
            self.history.token_id,
            self.history.interval,
            self.history.aggregation_class,
            self.history.current_instrument_ids,
        ) != (
            self.selection.token_id,
            self.selection.interval,
            self.selection.native_witness.aggregation_class,
            tuple(point.instrument.instrument_id for point in self.selection.sources),
        ):
            raise ValueError("Composite OI statistics must retain their actual selected scope")
        return self


def _normal_range(
    pairs: tuple[OIHistoricalCompositePair, ...],
    *,
    measure: Literal["NATIVE", "RECORDED_USD"],
    policy: OISelectionPolicy,
    source_times: tuple[datetime, ...],
    duration: timedelta,
    ambiguous: bool,
) -> FloatRange:
    if ambiguous:
        return FloatRange(bounds=None, reason="INCOMPLETE")
    changes: list[Fraction] = []
    ends: list[datetime] = []
    unestimable = False
    for pair in pairs:
        points = pair.endpoints(measure)
        if points is None:
            continue
        ends.append(pair.end)
        try:
            change = exact_oi_endpoint_fraction(points)
        except OiFractionNotEstimable:
            unestimable = True
        else:
            if _display_bound(change) is None:
                unestimable = True
            else:
                changes.append(change)
    minimum = timedelta(days=policy.minimum_history_days)
    if (
        not source_times
        or source_times[-1] - source_times[0] < minimum
        or not ends
        or ends[-1] - ends[0] + duration < minimum
    ):
        return FloatRange(bounds=None, reason="INSUFFICIENT_HISTORY")
    if unestimable:
        return FloatRange(bounds=None, reason="NOT_ESTIMABLE")
    ordered = sorted(changes)
    lower, upper = (_quantile(ordered, probability) for probability in policy.quantiles)
    if lower is None or upper is None:
        return FloatRange(bounds=None, reason="NOT_ESTIMABLE")
    return FloatRange(bounds=FloatBounds(lower=lower, upper=upper), reason=None)


def _composite_history(
    source: OIReadSnapshot,
    policy: OISelectionPolicy,
    request: OIWindowRequest,
    *,
    selection: ObservedOISynchronizedSelection,
    rules: CapturedOISynchronizationRules,
) -> OICompositeHistoryBaseline:
    interval = selection.interval
    horizon = interval.start - timedelta(days=policy.lookback_days)
    duration = interval.cutoff - interval.start
    identities = tuple(point.instrument.instrument_id for point in selection.sources)
    histories: dict[str, dict[datetime, OIObservation]] = {iid: {} for iid in identities}
    ambiguous = request.assembled_at.value is None
    for point in source.observations:
        iid = point.instrument.instrument_id
        at = point.reported_time()
        if (
            iid not in histories
            or at is None
            or not horizon <= at < interval.start
            or _exclusion(point, request) is not None
        ):
            continue
        rule = _qualified_rule(point, rules)
        if rule is None or rule.aggregation_class != selection.native_witness.aggregation_class:
            continue
        if at in histories[iid]:
            ambiguous = True
        else:
            histories[iid][at] = point
    times = {iid: sorted(points) for iid, points in histories.items()}
    tolerances: dict[str, float] = {}
    for iid, clocks in times.items():
        gaps = sorted((right - left).total_seconds() for left, right in zip(clocks, clocks[1:], strict=False))
        tolerances[iid] = max(policy.tolerance_s, gaps[len(gaps) // 2] / 2 if gaps else 0.0)
    grid = tuple(sorted({at for clocks in times.values() for at in clocks}))

    def nearest(iid: str, target: datetime) -> OIObservation | None:
        clocks = times[iid]
        index = bisect_left(clocks, target)
        candidates = [
            (abs((clocks[position] - target).total_seconds()), clocks[position])
            for position in (index - 1, index)
            if 0 <= position < len(clocks)
            and abs((clocks[position] - target).total_seconds()) <= tolerances[iid]
        ]
        return histories[iid][min(candidates)[1]] if candidates else None

    pairs: list[OIHistoricalCompositePair] = []
    if not ambiguous:
        for at in grid:
            contributions: list[OIHistoricalPairContribution] = []
            for iid in identities:
                initial, final = nearest(iid, at - duration), nearest(iid, at)
                if initial is None or final is None:
                    continue
                initial_clock, final_clock = initial.reported_time(), final.reported_time()
                if initial_clock is None or final_clock is None or initial_clock >= final_clock:
                    continue
                n0, n1 = initial.native_quantity.value, final.native_quantity.value
                u0, u1 = initial.reported_usd.value, final.reported_usd.value
                native = n0 is not None and n0 > 0 and n1 is not None and n1 >= 0
                usd = u0 is not None and u0 > 0 and u1 is not None and u1 >= 0
                if native or usd:
                    contributions.append(
                        OIHistoricalPairContribution(
                            instrument_id=iid,
                            start_evidence=initial.evidence,
                            end_evidence=final.evidence,
                            start_observed_at=initial.observed_at,
                            end_observed_at=final.observed_at,
                            native_start=n0 if native else None,
                            native_end=n1 if native else None,
                            usd_start=u0 if usd else None,
                            usd_end=u1 if usd else None,
                        )
                    )
            if contributions:
                pairs.append(
                    OIHistoricalCompositePair(start=at - duration, end=at, contributions=tuple(contributions))
                )
    population = tuple(pairs)
    return OICompositeHistoryBaseline(
        token_id=source.token_id,
        interval=interval,
        current_instrument_ids=identities,
        aggregation_class=selection.native_witness.aggregation_class,
        native_normal_range=_normal_range(
            population,
            measure="NATIVE",
            policy=policy,
            source_times=grid,
            duration=duration,
            ambiguous=ambiguous,
        ),
        usd_normal_range=_normal_range(
            population,
            measure="RECORDED_USD",
            policy=policy,
            source_times=grid,
            duration=duration,
            ambiguous=ambiguous,
        ),
        pairs=population,
        grid_count=len(grid),
        ambiguous_source_times=ambiguous,
        policy=policy,
        source_grid=grid,
    )

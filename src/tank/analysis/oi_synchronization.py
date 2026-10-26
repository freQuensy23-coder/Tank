"""Actual common-grid OI source selection with declared scoped comparability."""

from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
    DerivativeInstrument,
    OIObservation,
    OIReadSnapshot,
    OIRuleEvidenceRef,
    OISelectionPolicy,
    UncapturedOISynchronizationRules,
)
from tank.analysis.condition_graph_contracts import (
    ConditionInterval,
    ConditionOISynchronization,
    ConditionOISynchronizationContribution,
    ConditionOISynchronizationEndpoint,
    ConditionWindow,
)
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

if TYPE_CHECKING:
    from tank.analysis.open_interest import OIWindowRequest


class OISynchronizationPolicy(DomainModel):
    eligible_venues: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def actual_venues(self) -> Self:
        if any(not venue.strip() for venue in self.eligible_venues) or len(set(self.eligible_venues)) != len(
            self.eligible_venues
        ):
            raise ValueError("OI synchronization requires distinct actual eligible venue declarations")
        return self


class QualifiedOIInputRule(DomainModel):
    aggregation_class: str = Field(min_length=1, pattern=r"\S")
    evidence: tuple[OIRuleEvidenceRef, ...] = Field(min_length=1)


class OISynchronizationExclusion(DomainModel):
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    reason: Literal[
        "VENUE_NOT_SYNCHRONIZED",
        "NATIVE_WINDOW_UNAVAILABLE",
        "SEMANTICS_UNAVAILABLE",
        "AGGREGATION_CLASS_CHANGED",
    ]


class OISynchronizedSourceContribution(DomainModel):
    instrument: DerivativeInstrument
    start: OIObservation
    end: OIObservation
    start_rule: QualifiedOIInputRule
    end_rule: QualifiedOIInputRule

    @model_validator(mode="after")
    def actual_sources(self) -> Self:
        if self.start.instrument != self.instrument or self.end.instrument != self.instrument:
            raise ValueError("OI synchronization must retain each actual instrument's selected sources")
        if self.start_rule.aggregation_class != self.end_rule.aggregation_class:
            raise ValueError("OI synchronization cannot compare different endpoint aggregation classes")
        return self


class ObservedOISynchronizedSelection(DomainModel):
    status: Literal["OBSERVED"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    interval: ConditionInterval
    witness: ConditionOISynchronization
    sources: tuple[OISynchronizedSourceContribution, ...] = Field(min_length=1)
    excluded: tuple[OISynchronizationExclusion, ...]

    @model_validator(mode="after")
    def actual_contributions(self) -> Self:
        sources = {value.instrument.instrument_id: value for value in self.sources}
        if len(sources) != len(self.sources) or any(
            value.instrument.token_id != self.token_id for value in self.sources
        ):
            raise ValueError(
                "Synchronized OI source contributions require distinct actual subject instruments"
            )
        excluded_ids = [value.instrument_id for value in self.excluded]
        if len(excluded_ids) != len(set(excluded_ids)) or set(excluded_ids) & set(sources):
            raise ValueError("OI synchronization cannot both include and exclude one instrument")
        for endpoint, is_end in ((self.witness.start, False), (self.witness.end, True)):
            target = self.interval.cutoff if is_end else self.interval.start
            if endpoint.at.value is None:
                raise ValueError("Observed OI synchronization requires actual grid bounds")
            grid = parse_iso_time(endpoint.at.value)
            if grid > self.interval.cutoff or abs((grid - target).total_seconds()) > self.witness.tolerance_s:
                raise ValueError("OI synchronization grid must bind its actual requested endpoint tolerance")
            if {value.instrument_id for value in endpoint.contributions} != set(sources):
                raise ValueError("OI synchronization must preserve every actual endpoint source")
            for value in endpoint.contributions:
                source = sources[value.instrument_id]
                point = source.end if is_end else source.start
                if (
                    point.observed_at.value is None
                    or parse_iso_time(point.observed_at.value) > self.interval.cutoff
                ):
                    raise ValueError("OI synchronization cannot borrow a future source observation")
                rule = source.end_rule if is_end else source.start_rule
                if point.reported_usd.status == "NOT_ESTIMABLE":
                    raise ValueError("Observed synchronized OI cannot hide an invalid selected USD source")
                if (value.venue, value.observed_at, value.oi_native, value.oi_usd) != (
                    point.instrument.venue,
                    point.observed_at,
                    point.native_quantity.value,
                    point.reported_usd.value,
                ) or rule.aggregation_class != self.witness.aggregation_class:
                    raise ValueError("OI synchronization witness disagrees with its actual selected source")
        return self


class UnavailableOISynchronizedSelection(DomainModel):
    status: Literal["UNAVAILABLE"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    interval: ConditionInterval
    reason: Literal[
        "OI_SEMANTICS_NOT_CAPTURED",
        "NO_COMPARABLE_INSTRUMENTS",
        "INCOMPATIBLE_AGGREGATION_CLASSES",
        "NO_COMMON_GRID",
        "INVALID_USD_SOURCE",
    ]
    eligible_instrument_ids: tuple[str, ...]
    excluded: tuple[OISynchronizationExclusion, ...]


type OISynchronizedSelection = ObservedOISynchronizedSelection | UnavailableOISynchronizedSelection


class _GridSelection(DomainModel):
    at: Timestamp
    points: tuple[OIObservation, ...]
    rules: tuple[QualifiedOIInputRule, ...]


def _qualified_rule(
    point: OIObservation, rules: CapturedOISynchronizationRules
) -> QualifiedOIInputRule | None:
    if point.synchronized_eligible is False or point.sidedness not in (None, "SINGLE_SIDED"):
        return None
    matching = [entry for entry in rules.entries if entry.rule.applies_to(point.instrument, point.metric_key)]
    specific = [entry for entry in matching if entry.rule.instrument_id is not None]
    selected = specific if specific else matching
    if not selected or len({entry.descriptor.original_projection_sha256 for entry in selected}) != 1:
        return None
    rule = selected[0].rule
    if rule.sidedness != "SINGLE_SIDED" or rule.aggregation_class is None:
        return None
    return QualifiedOIInputRule(
        aggregation_class=rule.aggregation_class, evidence=tuple(entry.reference for entry in selected)
    )


def _select_oi_synchronization(
    source: OIReadSnapshot,
    selection_policy: OISelectionPolicy,
    request: "OIWindowRequest",
    *,
    rules: CapturedOISynchronizationRules | UncapturedOISynchronizationRules,
    policy: OISynchronizationPolicy,
    window: ConditionWindow,
) -> OISynchronizedSelection:
    from tank.analysis.open_interest import SelectedOIEndpoint, _endpoint, _exclusion

    if request.start.value is None or request.cutoff.value is None:
        raise ValueError("OI synchronization requires its actual requested logical bounds")
    start, cutoff = parse_iso_time(request.start.value), parse_iso_time(request.cutoff.value)
    interval = ConditionInterval(window=window, start=start, cutoff=cutoff)
    excluded: list[OISynchronizationExclusion] = []
    series: dict[str, tuple[OIObservation, ...]] = {}
    classes: set[str] = set()
    if isinstance(rules, UncapturedOISynchronizationRules):
        return UnavailableOISynchronizedSelection(
            status="UNAVAILABLE",
            token_id=source.token_id,
            interval=interval,
            reason="OI_SEMANTICS_NOT_CAPTURED",
            eligible_instrument_ids=(),
            excluded=(),
        )
    tolerance = selection_policy.tolerance_s
    for instrument in source.instruments:
        reason: (
            Literal[
                "VENUE_NOT_SYNCHRONIZED",
                "NATIVE_WINDOW_UNAVAILABLE",
                "SEMANTICS_UNAVAILABLE",
                "AGGREGATION_CLASS_CHANGED",
            ]
            | None
        ) = None
        points = tuple(
            point
            for point in source.observations
            if point.instrument == instrument and _exclusion(point, request) is None
        )
        initial = _endpoint(points, request.start, tolerance_s=tolerance, assembled_at=request.assembled_at)
        final = _endpoint(points, request.cutoff, tolerance_s=tolerance, assembled_at=request.assembled_at)
        if instrument.venue not in policy.eligible_venues:
            reason = "VENUE_NOT_SYNCHRONIZED"
        elif not isinstance(initial, SelectedOIEndpoint) or not isinstance(final, SelectedOIEndpoint):
            reason = "NATIVE_WINDOW_UNAVAILABLE"
        else:
            initial_rule, final_rule = (
                _qualified_rule(initial.observation, rules),
                _qualified_rule(final.observation, rules),
            )
            if initial_rule is None or final_rule is None:
                reason = "SEMANTICS_UNAVAILABLE"
            elif initial_rule.aggregation_class != final_rule.aggregation_class:
                reason = "AGGREGATION_CLASS_CHANGED"
            else:
                classes.add(initial_rule.aggregation_class)
                series[instrument.instrument_id] = points
        if reason is not None:
            excluded.append(OISynchronizationExclusion(instrument_id=instrument.instrument_id, reason=reason))
    eligible = tuple(series)
    if not series or len(classes) != 1:
        return UnavailableOISynchronizedSelection(
            status="UNAVAILABLE",
            token_id=source.token_id,
            interval=interval,
            reason="NO_COMPARABLE_INSTRUMENTS" if not series else "INCOMPATIBLE_AGGREGATION_CLASSES",
            eligible_instrument_ids=eligible,
            excluded=tuple(excluded),
        )
    timed = {
        iid: sorted(
            (parse_iso_time(point.observed_at.value), index, point)
            for index, point in enumerate(points)
            if point.observed_at.value is not None
        )
        for iid, points in series.items()
    }
    times = {iid: [value[0] for value in rows] for iid, rows in timed.items()}
    grid_clocks: dict[datetime, list[Timestamp]] = {}
    reach = timedelta(seconds=tolerance)
    for points in series.values():
        for point in points:
            source_at = point.reported_time()
            if source_at is not None and (
                abs((source_at - start).total_seconds()) <= tolerance
                or abs((source_at - cutoff).total_seconds()) <= tolerance
            ):
                grid_clocks.setdefault(source_at, []).append(point.observed_at)
    valid: dict[datetime, _GridSelection] = {}
    for grid, clocks in sorted(grid_clocks.items()):
        grid_stamp = (
            clocks[0]
            if all(clock == clocks[0] for clock in clocks)
            else Timestamp(clocks[0].value, None, "SYNCHRONIZATION_GRID_METADATA_DIFFERS")
        )
        selected_points: list[OIObservation] = []
        selected_rules: list[QualifiedOIInputRule] = []
        for iid in eligible:
            low, high = bisect_left(times[iid], grid - reach), bisect_right(times[iid], grid + reach)
            nearby = tuple(value[2] for value in timed[iid][low:high])
            selected_endpoint = _endpoint(
                nearby, grid_stamp, tolerance_s=tolerance, assembled_at=request.assembled_at
            )
            if not isinstance(selected_endpoint, SelectedOIEndpoint):
                break
            rule = _qualified_rule(selected_endpoint.observation, rules)
            if rule is None or rule.aggregation_class not in classes:
                break
            selected_points.append(selected_endpoint.observation)
            selected_rules.append(rule)
        if len(selected_points) == len(eligible):
            valid[grid] = _GridSelection(
                at=grid_stamp, points=tuple(selected_points), rules=tuple(selected_rules)
            )
    initial_candidates = [value for value in valid if abs((value - start).total_seconds()) <= tolerance]
    final_candidates = [value for value in valid if abs((value - cutoff).total_seconds()) <= tolerance]
    if not initial_candidates or not final_candidates:
        return UnavailableOISynchronizedSelection(
            status="UNAVAILABLE",
            token_id=source.token_id,
            interval=interval,
            reason="NO_COMMON_GRID",
            eligible_instrument_ids=eligible,
            excluded=tuple(excluded),
        )
    initial_grid = valid[min(initial_candidates, key=lambda at: (abs((at - start).total_seconds()), at))]
    final_grid = valid[min(final_candidates, key=lambda at: (abs((at - cutoff).total_seconds()), at))]
    for selected_point in (*initial_grid.points, *final_grid.points):
        usd = selected_point.reported_usd
        if usd.status == "NOT_ESTIMABLE" or usd.value is not None and usd.value < 0:
            return UnavailableOISynchronizedSelection(
                status="UNAVAILABLE",
                token_id=source.token_id,
                interval=interval,
                reason="INVALID_USD_SOURCE",
                eligible_instrument_ids=eligible,
                excluded=tuple(excluded),
            )
    sources = tuple(
        OISynchronizedSourceContribution(
            instrument=initial_point.instrument,
            start=initial_point,
            end=final_point,
            start_rule=initial_rule,
            end_rule=final_rule,
        )
        for initial_point, final_point, initial_rule, final_rule in zip(
            initial_grid.points, final_grid.points, initial_grid.rules, final_grid.rules, strict=True
        )
    )
    endpoints: list[ConditionOISynchronizationEndpoint] = []
    for grid_projection in (initial_grid, final_grid):
        contributions: list[ConditionOISynchronizationContribution] = []
        for point in grid_projection.points:
            native = point.native_quantity.value
            if native is None:
                raise ValueError("Selected OI synchronization lost its actual native contribution")
            contributions.append(
                ConditionOISynchronizationContribution(
                    instrument_id=point.instrument.instrument_id,
                    venue=point.instrument.venue,
                    observed_at=point.observed_at,
                    oi_native=native,
                    oi_usd=point.reported_usd.value,
                )
            )
        endpoints.append(
            ConditionOISynchronizationEndpoint(at=grid_projection.at, contributions=tuple(contributions))
        )
    return ObservedOISynchronizedSelection(
        status="OBSERVED",
        token_id=source.token_id,
        interval=interval,
        witness=ConditionOISynchronization(
            aggregation_class=next(iter(classes)),
            tolerance_s=float(tolerance),
            start=endpoints[0],
            end=endpoints[1],
        ),
        sources=sources,
        excluded=tuple(excluded),
    )

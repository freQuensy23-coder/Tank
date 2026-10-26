"""Pure OI endpoint selection over owned, validated source observations."""

from bisect import bisect_left
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from math import isfinite
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_inputs import (
    DerivativeInstrument,
    OIInputEvidenceRef,
    OIObservation,
    OIReadSnapshot,
    OISelectionPolicy,
)
from tank.analysis.condition_graph_contracts import ConditionInterval, ConditionWindow
from tank.analysis.oi_ratio import NativeOiChange, native_oi_change
from tank.analysis.statistics_contracts import FloatBounds, FloatRange
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

type OIObservationExclusionReason = Literal[
    "ASSEMBLY_CLOCK_NOT_CAPTURED",
    "SOURCE_CLOCK_NOT_CAPTURED",
    "SOURCE_AFTER_CUTOFF",
    "OI_BUCKET_EDGE_UNSPECIFIED",
    "RECEIPT_NOT_CAPTURED",
    "RECEIPT_AFTER_ASSEMBLY",
]


class OIWindowRequest(DomainModel):
    start: Timestamp
    cutoff: Timestamp
    assembled_at: Timestamp

    @model_validator(mode="after")
    def actual_window(self) -> Self:
        if self.start.value is None or self.cutoff.value is None:
            raise ValueError("OI endpoint selection requires actual logical window bounds")
        if parse_iso_time(self.start.value) >= parse_iso_time(self.cutoff.value):
            raise ValueError("OI endpoint selection requires a forward logical window")
        return self


class ExcludedOIObservation(DomainModel):
    evidence: OIInputEvidenceRef
    reason: OIObservationExclusionReason


class SelectedOIEndpoint(DomainModel):
    status: Literal["OBSERVED"]
    at: Timestamp
    observation: OIObservation
    tolerance_s: int = Field(ge=0)

    @model_validator(mode="after")
    def actual_source_endpoint(self) -> Self:
        source = self.observation.reported_time()
        if self.at.value is None or source is None:
            raise ValueError("Selected OI endpoint requires actual grid and source clocks")
        if abs((source - parse_iso_time(self.at.value)).total_seconds()) > self.tolerance_s:
            raise ValueError("Selected OI endpoint must satisfy the declared tolerance")
        if self.observation.explicitly_unresolved_edge():
            raise ValueError("An unresolved OI bucket edge cannot establish a selected endpoint")
        native = self.observation.native_quantity.value
        if native is None or native < 0:
            raise ValueError("Selected OI endpoint requires observed nonnegative native quantity")
        return self


class UnavailableOIEndpoint(DomainModel):
    status: Literal["UNAVAILABLE"]
    at: Timestamp
    reason: Literal[
        "ASSEMBLY_CLOCK_NOT_CAPTURED",
        "NO_ENDPOINT_SAMPLE",
        "AMBIGUOUS_ENDPOINT_SAMPLE",
        "NATIVE_QUANTITY_NOT_CAPTURED",
        "NATIVE_QUANTITY_INVALID",
    ]


type OIEndpoint = SelectedOIEndpoint | UnavailableOIEndpoint


class OIInstrumentWindow(DomainModel):
    instrument: DerivativeInstrument
    start: OIEndpoint
    end: OIEndpoint
    excluded: tuple[ExcludedOIObservation, ...]

    @model_validator(mode="after")
    def selected_instrument(self) -> Self:
        for endpoint in (self.start, self.end):
            if (
                isinstance(endpoint, SelectedOIEndpoint)
                and endpoint.observation.instrument != self.instrument
            ):
                raise ValueError("OI endpoints must retain their actual instrument scope")
        if self.start.at.value is None or self.end.at.value is None:
            raise ValueError("An OI window requires both actual logical bounds")
        if parse_iso_time(self.start.at.value) >= parse_iso_time(self.end.at.value):
            raise ValueError("An OI window requires forward logical bounds")
        if isinstance(self.start, SelectedOIEndpoint) and isinstance(self.end, SelectedOIEndpoint):
            initial = self.start.observation.reported_time()
            final = self.end.observation.reported_time()
            if (
                initial is None
                or final is None
                or initial > final
                or final > parse_iso_time(self.end.at.value)
            ):
                raise ValueError("Selected OI source endpoints cannot run backwards")
        return self

    def native_change(self, *, normal_range: FloatRange) -> NativeOiChange | None:
        selected = type(self).model_validate(self)
        if not isinstance(selected.start, SelectedOIEndpoint) or not isinstance(
            selected.end, SelectedOIEndpoint
        ):
            return None
        initial = selected.start.observation.native_quantity.value
        final = selected.end.observation.native_quantity.value
        if initial is None or final is None:
            raise ValueError("Observed OI endpoints must retain their actual native quantities")
        return native_oi_change(initial, final, normal_range=normal_range)


def _exclusion(point: OIObservation, request: OIWindowRequest) -> OIObservationExclusionReason | None:
    source = point.reported_time()
    if source is None:
        return "SOURCE_CLOCK_NOT_CAPTURED"
    if request.cutoff.value is None:
        raise ValueError("Validated OI request lost its actual cutoff")
    if source > parse_iso_time(request.cutoff.value):
        return "SOURCE_AFTER_CUTOFF"
    if point.explicitly_unresolved_edge():
        return "OI_BUCKET_EDGE_UNSPECIFIED"
    if point.received_at.value is None:
        return "RECEIPT_NOT_CAPTURED"
    if request.assembled_at.value is None:
        return "ASSEMBLY_CLOCK_NOT_CAPTURED"
    if point.visible_at(request.assembled_at) is False:
        return "RECEIPT_AFTER_ASSEMBLY"
    return None


def _endpoint(
    points: tuple[OIObservation, ...], at: Timestamp, *, tolerance_s: int, assembled_at: Timestamp
) -> OIEndpoint:
    if assembled_at.value is None:
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="ASSEMBLY_CLOCK_NOT_CAPTURED")
    if at.value is None:
        raise ValueError("OI endpoint target must retain its actual logical clock")
    target = parse_iso_time(at.value)
    candidates: list[tuple[float, OIObservation]] = []
    for point in points:
        source: datetime | None = point.reported_time()
        if source is not None:
            distance = abs((source - target).total_seconds())
            if distance <= tolerance_s:
                candidates.append((distance, point))
    if not candidates:
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="NO_ENDPOINT_SAMPLE")
    distance = min(value[0] for value in candidates)
    nearest = [point for skew, point in candidates if skew == distance]
    if len(nearest) != 1:
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="AMBIGUOUS_ENDPOINT_SAMPLE")
    selected = nearest[0]
    quantity = selected.native_quantity
    if quantity.status == "NOT_CAPTURED":
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="NATIVE_QUANTITY_NOT_CAPTURED")
    if quantity.value is None or quantity.value < Decimal(0):
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="NATIVE_QUANTITY_INVALID")
    return SelectedOIEndpoint(status="OBSERVED", at=at, observation=selected, tolerance_s=tolerance_s)


class OINativeHistoryBaseline(DomainModel):
    instrument: DerivativeInstrument
    interval: ConditionInterval
    normal_range: FloatRange
    population_basis: Literal["OBSERVED_NATIVE_ENDPOINT_PAIRS"]
    source_observation_count: int = Field(ge=0)
    distinct_source_time_count: int = Field(ge=0)
    pair_count: int = Field(ge=0)
    rejected_pair_count: int = Field(ge=0)
    pair_tolerance_s: float = Field(ge=0)
    minimum_history_days: int = Field(gt=0)
    lookback_days: int = Field(gt=0)
    quantiles: tuple[float, float]
    first_observed_at: Timestamp
    last_observed_at: Timestamp
    first_pair_end_at: Timestamp
    last_pair_end_at: Timestamp

    @model_validator(mode="after")
    def actual_population(self) -> Self:
        if self.distinct_source_time_count > self.source_observation_count:
            raise ValueError("OI history cannot invent more distinct clocks than source observations")
        if self.pair_count + self.rejected_pair_count != self.distinct_source_time_count:
            raise ValueError("OI history cannot invent more evaluated pairs than observed endpoints")
        if not 0 <= self.quantiles[0] < self.quantiles[1] <= 1:
            raise ValueError("OI history quantiles require actual ordered probability bounds")
        source_clocks = (self.first_observed_at.value, self.last_observed_at.value)
        pair_clocks = (self.first_pair_end_at.value, self.last_pair_end_at.value)
        has_sources = self.source_observation_count > 0
        has_pairs = self.pair_count > 0
        if any((value is not None) != has_sources for value in source_clocks):
            raise ValueError("OI history extent requires its actual observed source clocks")
        if any((value is not None) != has_pairs for value in pair_clocks):
            raise ValueError("OI pair extent requires actual qualified pair endpoints")
        if self.source_observation_count:
            initial, final = source_clocks
            if initial is None or final is None:
                raise ValueError("Observed OI history cannot omit either source bound")
            first, last = parse_iso_time(initial), parse_iso_time(final)
            if (
                not self.interval.start - timedelta(days=self.lookback_days)
                <= first
                <= last
                < self.interval.start
            ):
                raise ValueError("OI history extent must remain in its actual historical selection window")
            if self.pair_count:
                initial_pair, final_pair = pair_clocks
                if initial_pair is None or final_pair is None:
                    raise ValueError("Qualified OI pairs cannot omit either endpoint bound")
                pair_first, pair_last = parse_iso_time(initial_pair), parse_iso_time(final_pair)
                if not first <= pair_first <= pair_last <= last:
                    raise ValueError("OI pair extent cannot borrow unrelated source clocks")
                if self.normal_range.bounds is not None and (
                    last - first < timedelta(days=self.minimum_history_days)
                    or pair_last - pair_first + self.interval.cutoff - self.interval.start
                    < timedelta(days=self.minimum_history_days)
                ):
                    raise ValueError("An observed OI baseline must satisfy its actual minimum history")
        if self.normal_range.bounds is not None and not self.pair_count:
            raise ValueError("An observed OI baseline requires actual endpoint pairs")
        return self


def _quantile(values: Sequence[Fraction], probability: float) -> Fraction:
    if not values:
        raise ValueError("OI quantiles require an actual nonempty observed population")
    position = (len(values) - 1) * Fraction(str(probability))
    index = position.numerator // position.denominator
    low, high = values[index], values[min(index + 1, len(values) - 1)]
    return low + (high - low) * (position - index)


def _display_bound(value: Fraction) -> float | None:
    try:
        displayed = float(value)
    except OverflowError:
        return None
    if not isfinite(displayed) or value != 0 and displayed == 0:
        return None
    return displayed


def _native_history(
    source: OIReadSnapshot, policy: OISelectionPolicy, request: OIWindowRequest, *, window: ConditionWindow
) -> tuple[OINativeHistoryBaseline, ...]:
    if request.start.value is None or request.cutoff.value is None:
        raise ValueError("OI history requires the actual requested logical interval")
    start, cutoff = parse_iso_time(request.start.value), parse_iso_time(request.cutoff.value)
    interval = ConditionInterval(window=window, start=start, cutoff=cutoff)
    duration = cutoff - start
    horizon = start - timedelta(days=policy.lookback_days)
    minimum = timedelta(days=policy.minimum_history_days)
    baselines: list[OINativeHistoryBaseline] = []
    for instrument in source.instruments:
        by_time: dict[datetime, OIObservation] = {}
        clocks: dict[datetime, Timestamp] = {}
        observation_count = 0
        ambiguous = False
        for point in source.observations:
            if point.instrument != instrument or _exclusion(point, request) is not None:
                continue
            at = point.reported_time()
            if at is None or not horizon <= at < start:
                continue
            observation_count += 1
            if at in by_time:
                ambiguous = True
                if clocks[at] != point.observed_at:
                    clocks[at] = Timestamp(point.observed_at.value, None, "HISTORY_CLOCK_METADATA_DIFFERS")
            else:
                by_time[at] = point
                clocks[at] = point.observed_at
        times = sorted(by_time)
        gaps = sorted((right - left).total_seconds() for left, right in zip(times, times[1:], strict=False))
        tolerance = max(float(policy.tolerance_s), gaps[len(gaps) // 2] / 2 if gaps else 0.0)
        changes: list[Fraction] = []
        ends: list[datetime] = []
        rejected = 0
        if not ambiguous and request.assembled_at.value is not None:
            for at in times:
                target = at - duration
                index = bisect_left(times, target)
                candidates = [
                    (abs((times[position] - target).total_seconds()), times[position])
                    for position in (index - 1, index)
                    if 0 <= position < len(times)
                    and times[position] < at
                    and abs((times[position] - target).total_seconds()) <= tolerance
                ]
                old = by_time[min(candidates)[1]] if candidates else None
                initial = None if old is None else old.native_quantity.value
                final = by_time[at].native_quantity.value
                if initial is None or initial <= 0 or final is None or final < 0:
                    rejected += 1
                    continue
                changes.append((Fraction(final) - Fraction(initial)) / Fraction(initial))
                ends.append(at)
        else:
            rejected = len(times)
        bounds: FloatBounds | None = None
        reason: Literal["INCOMPLETE", "INSUFFICIENT_HISTORY", "NOT_ESTIMABLE"] | None
        if ambiguous or request.assembled_at.value is None:
            reason = "INCOMPLETE"
        elif (
            not times or times[-1] - times[0] < minimum or not ends or ends[-1] - ends[0] + duration < minimum
        ):
            reason = "INSUFFICIENT_HISTORY"
        else:
            ordered = sorted(changes)
            lower = _display_bound(_quantile(ordered, policy.quantiles[0]))
            upper = _display_bound(_quantile(ordered, policy.quantiles[1]))
            if lower is None or upper is None:
                reason = "NOT_ESTIMABLE"
            else:
                bounds, reason = FloatBounds(lower=lower, upper=upper), None
        baselines.append(
            OINativeHistoryBaseline(
                instrument=instrument,
                interval=interval,
                normal_range=FloatRange(bounds=bounds, reason=reason),
                population_basis="OBSERVED_NATIVE_ENDPOINT_PAIRS",
                source_observation_count=observation_count,
                distinct_source_time_count=len(times),
                pair_count=len(changes),
                rejected_pair_count=rejected,
                pair_tolerance_s=tolerance,
                minimum_history_days=policy.minimum_history_days,
                lookback_days=policy.lookback_days,
                quantiles=policy.quantiles,
                first_observed_at=clocks[times[0]]
                if times
                else Timestamp(None, None, "NO_HISTORY_OBSERVATIONS"),
                last_observed_at=clocks[times[-1]]
                if times
                else Timestamp(None, None, "NO_HISTORY_OBSERVATIONS"),
                first_pair_end_at=clocks[ends[0]]
                if ends
                else Timestamp(None, None, "NO_QUALIFIED_ENDPOINT_PAIRS"),
                last_pair_end_at=clocks[ends[-1]]
                if ends
                else Timestamp(None, None, "NO_QUALIFIED_ENDPOINT_PAIRS"),
            )
        )
    return tuple(baselines)


class OpenInterestCalculator:
    """Own immutable admitted reads once; keep actual assembly in each request."""

    __slots__ = ("_source", "_policy")

    def __init__(self, source: OIReadSnapshot, policy: OISelectionPolicy) -> None:
        self._source = OIReadSnapshot.model_validate(source)
        self._policy = OISelectionPolicy.model_validate(policy)

    def native_baseline(
        self, request: OIWindowRequest, *, window: ConditionWindow
    ) -> tuple[OINativeHistoryBaseline, ...]:
        actual = OIWindowRequest.model_validate(request)
        return _native_history(self._source, self._policy, actual, window=window)

    def select(self, request: OIWindowRequest) -> tuple[OIInstrumentWindow, ...]:
        actual = OIWindowRequest.model_validate(request)
        result: list[OIInstrumentWindow] = []
        for instrument in self._source.instruments:
            points: list[OIObservation] = []
            excluded: list[ExcludedOIObservation] = []
            for point in self._source.observations:
                if point.instrument != instrument:
                    continue
                reason = _exclusion(point, actual)
                if reason is None:
                    points.append(point)
                else:
                    excluded.append(ExcludedOIObservation(evidence=point.evidence, reason=reason))
            admitted = tuple(points)
            result.append(
                OIInstrumentWindow(
                    instrument=instrument,
                    start=_endpoint(
                        admitted,
                        actual.start,
                        tolerance_s=self._policy.tolerance_s,
                        assembled_at=actual.assembled_at,
                    ),
                    end=_endpoint(
                        admitted,
                        actual.cutoff,
                        tolerance_s=self._policy.tolerance_s,
                        assembled_at=actual.assembled_at,
                    ),
                    excluded=tuple(excluded),
                )
            )
        return tuple(result)

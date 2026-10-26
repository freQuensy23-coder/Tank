"""Pure spot arithmetic over qualified inputs; acquisition and evidence bytes stay outside."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, localcontext
from math import isfinite
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_graph_codec import bind_flow_aggregate_evidence, flow_aggregate_projection_sha256
from tank.analysis.condition_graph_contracts import (
    BasicConditionSpot,
    ConditionBoundaryGroup,
    ConditionCohortComparison,
    ConditionComposite,
    ConditionFlowAggregate,
    ConditionFlowAggregateProjection,
    ConditionIncompleteMinuteCoverage,
    ConditionInterval,
    ConditionMissingFlowMetrics,
    ConditionSignHistoryWindow,
    ConditionSpotBreadth,
    ConditionSpotConcentration,
    ConditionSpotCoverage,
    ConditionSpotExclusion,
    ConditionSpotMajority,
    ConditionSpotTopN,
    ConditionSpotVenue,
    ConditionSpotWindowGraph,
    ConditionStatisticalNullReason,
    ConditionTopVenueSignHistory,
    ConditionTradeCounts,
    ConditionUnavailableBoundaryCoverage,
    ConditionVenueSignObservation,
    ConditionWeightedBreadth,
    ConditionWindow,
    FlowAggregateEvidenceRef,
    IncompleteConditionTradeCounts,
    ObservedConditionCohort,
    ObservedConditionCohortComparison,
    ObservedConditionComposite,
    ObservedConditionTradeCounts,
    UnavailableConditionCohort,
    UnavailableConditionCohortComparison,
    UnavailableConditionComposite,
    WindowedConditionSpot,
)
from tank.analysis.condition_inputs import (
    SpotCalculationInputs,
    SpotCalculationPolicy,
    SpotFlowObservation,
    SpotWindowCalculationInputs,
)
from tank.analysis.spot_enrichment import CohortEnrichedConditionSpot, ConditionSpotCohortGraph
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

type FlowSign = Literal[-1, 0, 1]
type NativeFlowField = Literal["buy_qty", "sell_qty", "sum_sq_qty"]


@contextmanager
def _native_arithmetic(values: Sequence[Decimal]) -> Iterator[None]:
    exponents: list[int] = []
    for value in values:
        if not value.is_finite():
            raise ValueError("Spot arithmetic requires finite native amounts")
        exponent = value.as_tuple().exponent
        if not isinstance(exponent, int):
            raise ValueError("Spot arithmetic requires finite Decimal exponents")
        exponents.append(exponent)
    span = max(value.adjusted() for value in values) - min(exponents) + 1 if values else 1
    with localcontext() as context:
        context.prec = max(128, 4 * span + 32)
        yield


class NativeFlowTotals(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    buy_qty: Decimal = Field(ge=0)
    sell_qty: Decimal = Field(ge=0)
    volume_native: Decimal = Field(ge=0)
    cvd: Decimal
    sum_sq_qty: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def actual_native_totals(self) -> Self:
        with _native_arithmetic((self.buy_qty, self.sell_qty, self.volume_native, self.cvd)):
            if self.volume_native != self.buy_qty + self.sell_qty or self.cvd != self.buy_qty - self.sell_qty:
                raise ValueError("Native spot totals must preserve their actual side amounts")
        if (self.volume_native == 0) != (self.sum_sq_qty == 0):
            raise ValueError("Native spot squared quantity cannot invent or erase observed flow")
        return self


class MissingSpotFlowMetrics(DomainModel):
    reason: Literal["INCOMPLETE"]
    missing_metrics: tuple[NativeFlowField, ...] = Field(min_length=1)


def summarize_native_flow(
    token_id: str, rows: Sequence[SpotFlowObservation]
) -> NativeFlowTotals | MissingSpotFlowMetrics:
    missing: list[NativeFlowField] = []
    if not rows or any(row.buy_qty is None for row in rows):
        missing.append("buy_qty")
    if not rows or any(row.sell_qty is None for row in rows):
        missing.append("sell_qty")
    if not rows or any(row.sum_sq_qty is None for row in rows):
        missing.append("sum_sq_qty")
    if any(row.instrument.token_id != token_id for row in rows):
        raise ValueError("Native spot arithmetic cannot combine different token quantities")
    if missing:
        return MissingSpotFlowMetrics(reason="INCOMPLETE", missing_metrics=tuple(missing))
    buys: list[Decimal] = []
    sells: list[Decimal] = []
    squared: list[Decimal] = []
    for row in rows:
        if row.buy_qty is None or row.sell_qty is None or row.sum_sq_qty is None:
            raise ValueError("Unavailable native flow cannot enter observed arithmetic")
        buys.append(row.buy_qty)
        sells.append(row.sell_qty)
        squared.append(row.sum_sq_qty)
    with _native_arithmetic((*buys, *sells, *squared)):
        buy = sum(buys, Decimal(0))
        sell = sum(sells, Decimal(0))
        return NativeFlowTotals(
            token_id=token_id,
            buy_qty=buy,
            sell_qty=sell,
            volume_native=buy + sell,
            cvd=buy - sell,
            sum_sq_qty=sum(squared, Decimal(0)),
        )


def significant_flow(totals: NativeFlowTotals, policy: SpotCalculationPolicy) -> FlowSign:
    totals = NativeFlowTotals.model_validate(totals)
    policy = SpotCalculationPolicy.model_validate(policy)
    with _native_arithmetic((totals.cvd, totals.sum_sq_qty, policy.significant_flow_sigma)):
        if totals.cvd == 0:
            return 0
        if totals.cvd * totals.cvd < policy.significant_flow_sigma**2 * totals.sum_sq_qty:
            return 0
        return 1 if totals.cvd > 0 else -1


def display_ratio(numerator: Decimal, denominator: Decimal) -> float | None:
    if denominator == 0:
        return None
    with _native_arithmetic((numerator, denominator)):
        value = float(numerator / denominator)
    return value if isfinite(value) and (value != 0 or numerator == 0) else None


def aggregate_projection(row: SpotFlowObservation) -> ConditionFlowAggregateProjection:
    if row.complete is not True or row.buy_qty is None or row.sell_qty is None or row.sum_sq_qty is None:
        raise ValueError("An observed aggregate requires actual complete native quantities")
    counts: ConditionTradeCounts
    if row.n_buy is not None and row.n_sell is not None:
        counts = ObservedConditionTradeCounts(status="OBSERVED", n_buy=row.n_buy, n_sell=row.n_sell)
    else:
        counts = IncompleteConditionTradeCounts(
            status="UNAVAILABLE", n_buy=row.n_buy, n_sell=row.n_sell, reason="INCOMPLETE"
        )
    low_reason: Literal["INCOMPLETE", "EMPTY_INTERVAL"] | None = None
    if row.low is None:
        low_reason = "EMPTY_INTERVAL" if counts.total() == 0 else "INCOMPLETE"
    return ConditionFlowAggregateProjection(
        token_id=row.instrument.token_id,
        venue=row.instrument.venue,
        market_type="SPOT",
        instrument_id=row.instrument.instrument_id,
        start=row.start,
        end=row.end,
        complete=True,
        buy_qty=row.buy_qty,
        sell_qty=row.sell_qty,
        sum_sq_qty=row.sum_sq_qty,
        buy_notional=row.buy_notional,
        sell_notional=row.sell_notional,
        notional_reason="INCOMPLETE" if row.buy_notional is None or row.sell_notional is None else None,
        low=row.low,
        low_reason=low_reason,
        counts=counts,
    )


def bound_aggregate(row: SpotFlowObservation) -> ConditionFlowAggregate:
    projection = aggregate_projection(row)
    return bind_flow_aggregate_evidence(
        projection,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(projection),
            source_evidence_sha256=row.evidence.source_evidence_sha256,
        ),
    )


@dataclass(frozen=True, slots=True)
class _Venue:
    name: str
    totals: NativeFlowTotals
    boundaries: tuple[ConditionBoundaryGroup, ...]
    sign: FlowSign
    coverage_endpoints: tuple[Timestamp, ...]


def _partial_intervals(start: datetime, cutoff: datetime) -> tuple[tuple[datetime, datetime], ...]:
    left = start.replace(second=0, microsecond=0)
    right = cutoff.replace(second=0, microsecond=0)
    intervals = []
    if start != left:
        intervals.append((start, min(left + timedelta(minutes=1), cutoff)))
    if cutoff != right and not (start != left and right == left):
        intervals.append((max(start, right), cutoff))
    return tuple(intervals)


def _boundary_cover(
    inputs: SpotCalculationInputs, instrument_id: str, start: datetime, cutoff: datetime
) -> tuple[SpotFlowObservation, ...] | MissingSpotFlowMetrics | None:
    available = [
        row
        for row in inputs.boundaries
        if row.instrument.instrument_id == instrument_id
        and row.complete is True
        and start <= row.start < row.end <= cutoff
        and row.visible_at(inputs.assembled_at) is True
    ]
    cursor = start
    selected = []
    while cursor < cutoff:
        choices = [row for row in available if row.start == cursor]
        if not choices:
            return None
        furthest = max(row.end for row in choices)
        candidates = [row for row in choices if row.end == furthest]
        complete = [
            row
            for row in candidates
            if row.buy_qty is not None and row.sell_qty is not None and row.sum_sq_qty is not None
        ]
        if not complete:
            missing = summarize_native_flow(inputs.token_id, candidates)
            if isinstance(missing, MissingSpotFlowMetrics):
                return missing
            raise ValueError("An unavailable boundary cannot invent observed native totals")
        projection = aggregate_projection(complete[0])
        if any(aggregate_projection(row) != projection for row in complete[1:]):
            raise ValueError("Conflicting eligible boundary financial evidence")
        parent_bound = [row for row in complete if row.parent_document is not None]
        if len(parent_bound) == 1:
            row = parent_bound[0]
        elif len(complete) == 1:
            row = complete[0]
        else:
            raise ValueError("Ambiguous eligible boundary source evidence")
        selected.append(row)
        cursor = row.end
    return tuple(selected)


def _selected_venue(
    inputs: SpotCalculationInputs, venue: str, *, start: datetime, cutoff: datetime
) -> _Venue | ConditionSpotExclusion:
    instruments = tuple(item.instrument_id for item in inputs.instruments if item.venue == venue)
    rows = [
        row
        for row in inputs.minutes
        if row.instrument.instrument_id in instruments and start <= row.start and row.end <= cutoff
    ]
    index: dict[tuple[str, datetime], SpotFlowObservation] = {}
    for row in rows:
        key = row.instrument.instrument_id, row.start
        if key in index:
            raise ValueError("A spot minute cannot identify multiple physical source rows")
        index[key] = row
    first = start.replace(second=0, microsecond=0)
    if first < start:
        first += timedelta(minutes=1)
    last = cutoff.replace(second=0, microsecond=0)
    cursor = first
    while cursor < last:
        for instrument_id in instruments:
            observation = index.get((instrument_id, cursor))
            if (
                observation is None
                or observation.complete is not True
                or observation.visible_at(inputs.assembled_at) is not True
            ):
                return ConditionIncompleteMinuteCoverage(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="FULL_MINUTE_COVERAGE_INCOMPLETE",
                    source_reasons=tuple(sorted({reason for row in rows for reason in row.source_reasons})),
                )
        cursor += timedelta(minutes=1)
    boundaries = []
    for left, right in _partial_intervals(start, cutoff):
        pieces: list[SpotFlowObservation] = []
        for instrument_id in instruments:
            covered = _boundary_cover(inputs, instrument_id, left, right)
            if covered is None:
                return ConditionUnavailableBoundaryCoverage(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="BOUNDARY_AGGREGATE_UNAVAILABLE",
                    source_reasons=tuple(
                        sorted(
                            {
                                reason
                                for row in inputs.boundaries
                                if row.instrument.instrument_id in instruments
                                and row.start < right
                                and row.end > left
                                for reason in row.source_reasons
                            }
                        )
                    ),
                )
            if isinstance(covered, MissingSpotFlowMetrics):
                return ConditionMissingFlowMetrics(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="FLOW_AGGREGATE_FIELDS_MISSING",
                    missing_metrics=covered.missing_metrics,
                )
            pieces.extend(covered)
        rows.extend(pieces)
        boundaries.append(
            ConditionBoundaryGroup(
                start=left, cutoff=right, aggregates=tuple(bound_aggregate(row) for row in pieces)
            )
        )
    totals = summarize_native_flow(inputs.token_id, rows)
    if isinstance(totals, MissingSpotFlowMetrics):
        return ConditionMissingFlowMetrics(
            venue=venue,
            reason="INCOMPLETE",
            detail="FLOW_AGGREGATE_FIELDS_MISSING",
            missing_metrics=totals.missing_metrics,
        )
    endpoints = tuple(row.end_clock for row in rows if row.end == cutoff)
    if len(endpoints) != len(instruments):
        raise ValueError("Complete spot coverage requires the actual endpoint of every instrument")
    return _Venue(venue, totals, tuple(boundaries), significant_flow(totals, inputs.policy), endpoints)


def combine_native_flow(token_id: str, rows: Sequence[NativeFlowTotals]) -> NativeFlowTotals:
    rows = tuple(NativeFlowTotals.model_validate(row) for row in rows)
    if not rows:
        raise ValueError("An unavailable composite cannot become observed zero")
    if any(row.token_id != token_id for row in rows):
        raise ValueError("A native composite cannot combine different token quantities")
    values = tuple(amount for row in rows for amount in (row.buy_qty, row.sell_qty, row.sum_sq_qty))
    with _native_arithmetic(values):
        buy = sum((row.buy_qty for row in rows), Decimal(0))
        sell = sum((row.sell_qty for row in rows), Decimal(0))
        return NativeFlowTotals(
            token_id=token_id,
            buy_qty=buy,
            sell_qty=sell,
            volume_native=buy + sell,
            cvd=buy - sell,
            sum_sq_qty=sum((row.sum_sq_qty for row in rows), Decimal(0)),
        )


def _combined(token_id: str, venues: Sequence[_Venue]) -> NativeFlowTotals:
    return combine_native_flow(token_id, tuple(row.totals for row in venues))


def compose_native_flow(
    totals: NativeFlowTotals | None, venues: tuple[str, ...], policy: SpotCalculationPolicy
) -> ConditionComposite:
    policy = SpotCalculationPolicy.model_validate(policy)
    totals = NativeFlowTotals.model_validate(totals) if totals is not None else None
    if not isinstance(venues, tuple) or any(not isinstance(name, str) or not name.strip() for name in venues):
        raise ValueError("Composite venues require their actual immutable identity tuple")
    if len(venues) != len(set(venues)) or bool(venues) != (totals is not None):
        raise ValueError("A composite must preserve its actual distinct observed venue scope")
    if totals is None:
        return UnavailableConditionComposite(
            status="UNAVAILABLE",
            cvd=None,
            volume=None,
            buy_share=None,
            bs_ratio=None,
            venues=(),
            sign=None,
            reason="INCOMPLETE",
        )
    buy_share = display_ratio(totals.buy_qty, totals.volume_native)
    bs_ratio = display_ratio(totals.buy_qty, totals.sell_qty)
    reasons: dict[
        Literal["buy_share", "bs_ratio"], Literal["ZERO_TOTAL_VOLUME", "ZERO_SELL_QUANTITY", "NOT_ESTIMABLE"]
    ] = {}
    if buy_share is None:
        reasons["buy_share"] = "ZERO_TOTAL_VOLUME" if totals.volume_native == 0 else "NOT_ESTIMABLE"
    if bs_ratio is None:
        reasons["bs_ratio"] = (
            "ZERO_TOTAL_VOLUME"
            if totals.volume_native == 0
            else "ZERO_SELL_QUANTITY"
            if totals.sell_qty == 0
            else "NOT_ESTIMABLE"
        )
    return ObservedConditionComposite(
        status="OBSERVED",
        cvd=totals.cvd,
        volume=totals.volume_native,
        buy_share=buy_share,
        bs_ratio=bs_ratio,
        venues=venues,
        sign=significant_flow(totals, policy),
        null_reasons=reasons,
    )


def _venue_output(row: _Venue, total: Decimal) -> ConditionSpotVenue:
    values = row.totals
    buy_share = display_ratio(values.buy_qty, values.volume_native)
    bs_ratio = display_ratio(values.buy_qty, values.sell_qty)
    volume_share = display_ratio(values.volume_native, total)
    reasons: dict[Literal["buy_share", "bs_ratio", "volume_share"], ConditionStatisticalNullReason] = {}
    if buy_share is None:
        reasons["buy_share"] = "ZERO_TOTAL_VOLUME" if values.volume_native == 0 else "NOT_ESTIMABLE"
    if bs_ratio is None:
        reasons["bs_ratio"] = (
            "ZERO_TOTAL_VOLUME"
            if values.volume_native == 0
            else "ZERO_SELL_QUANTITY"
            if values.sell_qty == 0
            else "NOT_ESTIMABLE"
        )
    if volume_share is None:
        reasons["volume_share"] = "ZERO_TOTAL_VOLUME" if total == 0 else "NOT_ESTIMABLE"
    return ConditionSpotVenue(
        venue=row.name,
        complete=True,
        volume_native=values.volume_native,
        buy_qty=values.buy_qty,
        sell_qty=values.sell_qty,
        cvd=values.cvd,
        buy_share=buy_share,
        bs_ratio=bs_ratio,
        sign=row.sign,
        sum_sq_qty=values.sum_sq_qty,
        boundary_segments=row.boundaries,
        volume_share=volume_share,
        null_reasons=reasons,
    )


def _excluded_daily_volume(
    inputs: SpotCalculationInputs, excluded: Sequence[ConditionSpotExclusion], *, cutoff: datetime
) -> Decimal | None:
    daily: list[Decimal] = []
    excluded_venues = {row.venue for row in excluded}
    for instrument in inputs.instruments:
        if instrument.venue not in excluded_venues:
            continue
        candidates = [
            ticker
            for ticker in inputs.tickers
            if ticker.instrument == instrument
            and ticker.visible_at(inputs.assembled_at) is True
            and ticker.observed_at.value is not None
            and parse_iso_time(ticker.observed_at.value) <= cutoff
        ]
        if not candidates:
            return None
        latest = max(
            parse_iso_time(ticker.observed_at.value)
            for ticker in candidates
            if ticker.observed_at.value is not None
        )
        chosen = [
            ticker
            for ticker in candidates
            if ticker.observed_at.value is not None and parse_iso_time(ticker.observed_at.value) == latest
        ]
        amounts = {ticker.trailing_day_native_volume for ticker in chosen}
        if len(amounts) != 1:
            raise ValueError("Conflicting latest native ticker estimates")
        amount = chosen[0].trailing_day_native_volume
        if amount is None:
            return None
        daily.append(amount)
    return sum(daily, Decimal(0))


def _strict_majority(numerator: Decimal, denominator: Decimal) -> bool | None:
    return None if denominator == 0 else 2 * numerator > denominator


@dataclass(frozen=True, slots=True)
class _SpotQualification:
    observed: tuple[_Venue, ...]
    excluded: tuple[ConditionSpotExclusion, ...]


def _qualify_spot(inputs: SpotCalculationInputs, *, start: datetime, cutoff: datetime) -> _SpotQualification:
    observed: list[_Venue] = []
    excluded: list[ConditionSpotExclusion] = []
    for name in sorted({instrument.venue for instrument in inputs.instruments}):
        selected = _selected_venue(inputs, name, start=start, cutoff=cutoff)
        if isinstance(selected, _Venue):
            observed.append(selected)
        else:
            excluded.append(selected)
    return _SpotQualification(tuple(observed), tuple(excluded))


def _calculate_spot(
    inputs: SpotCalculationInputs, qualification: _SpotQualification, *, start: datetime, cutoff: datetime
) -> BasicConditionSpot:
    observed, excluded = qualification.observed, qualification.excluded
    totals = _combined(inputs.token_id, observed) if observed else None
    composite = compose_native_flow(totals, tuple(row.name for row in observed), inputs.policy)
    volume = Decimal(0) if totals is None else totals.volume_native
    sign = 0 if totals is None else significant_flow(totals, inputs.policy)
    ticker_daily = _excluded_daily_volume(inputs, excluded, cutoff=cutoff)
    duration = cutoff - start
    seconds = Decimal(duration.days * 86400 + duration.seconds) + Decimal(duration.microseconds) / 1000000
    scaled_covered = volume * 86400
    denominator = None if ticker_daily is None else scaled_covered + ticker_daily * seconds
    covered = None if denominator is None else display_ratio(scaled_covered, denominator)
    covered_majority = None if denominator is None else _strict_majority(scaled_covered, denominator)
    volume_by_sign = {
        direction: sum((row.totals.volume_native for row in observed if row.sign == direction), Decimal(0))
        for direction in (-1, 0, 1)
    }
    n_pos = sum(row.sign == 1 for row in observed)
    n_neg = sum(row.sign == -1 for row in observed)
    n_neutral = sum(row.sign == 0 for row in observed)
    pct_pos = None if not observed else n_pos / len(observed)
    pct_neg = None if not observed else n_neg / len(observed)
    breadth_reasons: dict[Literal["pct_pos", "pct_neg"], ConditionStatisticalNullReason] = (
        {"pct_pos": "NO_COMPLETE_VENUES", "pct_neg": "NO_COMPLETE_VENUES"} if not observed else {}
    )
    weighted_buy = display_ratio(volume_by_sign[1], volume)
    weighted_sell = display_ratio(volume_by_sign[-1], volume)
    weighted_neutral = display_ratio(volume_by_sign[0], volume)
    weighted_reasons: dict[Literal["buy", "sell", "neutral"], ConditionStatisticalNullReason] = {}
    weighted_sides: tuple[tuple[Literal["buy", "sell", "neutral"], float | None], ...] = (
        ("buy", weighted_buy),
        ("sell", weighted_sell),
        ("neutral", weighted_neutral),
    )
    for field, value in weighted_sides:
        if value is None:
            weighted_reasons[field] = (
                "NO_COMPLETE_VENUES"
                if not observed
                else "ZERO_TOTAL_VOLUME"
                if volume == 0
                else "NOT_ESTIMABLE"
            )
    majorities = ConditionSpotMajority(
        buy=_strict_majority(volume_by_sign[1], volume),
        sell=_strict_majority(volume_by_sign[-1], volume),
        neutral=_strict_majority(volume_by_sign[0], volume),
        covered=covered_majority,
        null_reasons={
            **(
                {
                    "buy": "NO_COMPLETE_VENUES" if not observed else "ZERO_TOTAL_VOLUME",
                    "sell": "NO_COMPLETE_VENUES" if not observed else "ZERO_TOTAL_VOLUME",
                    "neutral": "NO_COMPLETE_VENUES" if not observed else "ZERO_TOTAL_VOLUME",
                }
                if volume == 0
                else {}
            ),
            **(
                {
                    "covered": "INCOMPLETE"
                    if denominator is None
                    else "NO_COMPLETE_VENUES"
                    if not observed
                    else "ZERO_TOTAL_VOLUME"
                }
                if covered_majority is None
                else {}
            ),
        },
    )
    ranked = sorted(observed, key=lambda row: row.totals.volume_native, reverse=True)
    top: list[_Venue] = []
    top_volume = Decimal(0)
    if volume > 0:
        for row in ranked:
            top.append(row)
            top_volume += row.totals.volume_native
            if 2 * top_volume > volume:
                break
    contributions = [(max(Decimal(0), sign * row.totals.cvd), row) for row in observed]
    directional_total = sum((value for value, _row in contributions), Decimal(0))
    largest = max((value for value, _row in contributions), default=Decimal(0))
    contributor = (
        next((row for value, row in contributions if value == largest), None) if largest > 0 else None
    )
    others = [row for row in observed if contributor is not None and row.name != contributor.name]
    other_volume = sum((row.totals.volume_native for row in others), Decimal(0))
    other_directional = sum((row.totals.volume_native for row in others if row.sign == sign), Decimal(0))
    top_volume_share = None if not ranked else display_ratio(ranked[0].totals.volume_native, volume)
    top_contrib_share = display_ratio(largest, directional_total)
    others_share = None if contributor is None else display_ratio(other_directional, other_volume)
    venue_shares = [display_ratio(row.totals.volume_native, volume) for row in observed]
    hhi = (
        sum(value * value for value in venue_shares if value is not None)
        if volume > 0 and all(value is not None for value in venue_shares)
        else None
    )
    concentration_reasons: dict[
        Literal[
            "hhi",
            "top_volume_venue",
            "top_volume_share",
            "top_contrib_venue",
            "top_contrib_share",
            "others_cwb_dir",
        ],
        ConditionStatisticalNullReason,
    ] = {}
    if hhi is None:
        concentration_reasons["hhi"] = (
            "NO_COMPLETE_VENUES" if not observed else "ZERO_TOTAL_VOLUME" if volume == 0 else "NOT_ESTIMABLE"
        )
    if not ranked or volume == 0:
        concentration_reasons["top_volume_venue"] = (
            "NO_COMPLETE_VENUES" if not observed else "ZERO_TOTAL_VOLUME"
        )
    if top_volume_share is None:
        concentration_reasons["top_volume_share"] = (
            "NO_COMPLETE_VENUES" if not observed else "ZERO_TOTAL_VOLUME" if volume == 0 else "NOT_ESTIMABLE"
        )
    if contributor is None:
        concentration_reasons["top_contrib_venue"] = (
            "NO_COMPLETE_VENUES" if not observed else "ZERO_DIRECTIONAL_CONTRIBUTION"
        )
    if top_contrib_share is None:
        concentration_reasons["top_contrib_share"] = (
            "NO_COMPLETE_VENUES"
            if not observed
            else "ZERO_DIRECTIONAL_CONTRIBUTION"
            if directional_total == 0
            else "NOT_ESTIMABLE"
        )
    if others_share is None:
        concentration_reasons["others_cwb_dir"] = (
            "NO_COMPLETE_VENUES"
            if not observed
            else "ZERO_DIRECTIONAL_CONTRIBUTION"
            if contributor is None
            else "ZERO_OTHER_VOLUME"
            if other_volume == 0
            else "NOT_ESTIMABLE"
        )
    classification: Literal["INSUFFICIENT_DATA", "CONCENTRATED", "BROAD", "DIVERGENT"]
    if len(observed) < inputs.policy.min_spot_venues or covered_majority is not True:
        classification = "INSUFFICIENT_DATA"
    elif (
        directional_total > 0
        and 2 * largest > directional_total
        and other_volume > 0
        and 2 * other_directional < other_volume
    ):
        classification = "CONCENTRATED"
    elif 2 * volume_by_sign[sign] > volume and ranked[0].sign == sign:
        classification = "BROAD"
    else:
        classification = "DIVERGENT"
    coverage_reasons: dict[
        Literal["covered_share_est", "exact_majority"], ConditionStatisticalNullReason
    ] = {}
    if covered is None:
        coverage_reasons["covered_share_est"] = (
            "INCOMPLETE"
            if denominator is None
            else "NO_COMPLETE_VENUES"
            if not observed
            else "ZERO_TOTAL_VOLUME"
            if denominator == 0
            else "NOT_ESTIMABLE"
        )
    if covered_majority is None:
        coverage_reasons["exact_majority"] = (
            "INCOMPLETE"
            if denominator is None
            else "NO_COMPLETE_VENUES"
            if not observed
            else "ZERO_TOTAL_VOLUME"
        )
    return BasicConditionSpot(
        kind="BASIC",
        token_id=inputs.token_id,
        venues=tuple(_venue_output(row, volume) for row in observed),
        composite_max=composite,
        breadth=ConditionSpotBreadth(
            n_pos=n_pos,
            n_neg=n_neg,
            n_neutral=n_neutral,
            pct_pos=pct_pos,
            pct_neg=pct_neg,
            null_reasons=breadth_reasons,
        ),
        cw_breadth=ConditionWeightedBreadth(
            buy=weighted_buy,
            sell=weighted_sell,
            neutral=weighted_neutral,
            null_reasons=weighted_reasons,
        ),
        exact_majority=majorities,
        topn=ConditionSpotTopN(
            n=len(top),
            venues=tuple(row.name for row in top),
            agreement=None if not top else sum(row.sign == sign for row in top) / len(top),
            agreement_reason="NO_COMPLETE_VENUES"
            if not observed
            else "ZERO_TOTAL_VOLUME"
            if not top
            else None,
        ),
        concentration=ConditionSpotConcentration(
            hhi=hhi,
            top_volume_venue=None if not ranked or volume == 0 else ranked[0].name,
            top_volume_share=top_volume_share,
            top_contrib_venue=None if contributor is None else contributor.name,
            top_contrib_share=top_contrib_share,
            others_cwb_dir=others_share,
            null_reasons=concentration_reasons,
        ),
        direction=None if totals is None else "BUY" if sign == 1 else "SELL" if sign == -1 else "NEUTRAL",
        direction_reason="NO_COMPLETE_VENUES" if totals is None else None,
        classification=classification,
        coverage=ConditionSpotCoverage(
            covered_share_est=covered,
            exact_majority=covered_majority,
            n_complete=len(observed),
            excluded=tuple(excluded),
            null_reasons=coverage_reasons,
        ),
    )


def calculate_spot(inputs: SpotCalculationInputs) -> BasicConditionSpot:
    inputs = SpotCalculationInputs.model_validate(inputs)
    values = tuple(
        value
        for row in (*inputs.minutes, *inputs.boundaries)
        for value in (row.buy_qty, row.sell_qty, row.sum_sq_qty)
        if value is not None
    )
    ticker_values = tuple(
        row.trailing_day_native_volume for row in inputs.tickers if row.trailing_day_native_volume is not None
    )
    with _native_arithmetic((*values, *ticker_values, inputs.policy.significant_flow_sigma)):
        qualified = _qualify_spot(inputs, start=inputs.start, cutoff=inputs.cutoff)
        return _calculate_spot(inputs, qualified, start=inputs.start, cutoff=inputs.cutoff)


def calculate_spot_sign_history(
    inputs: SpotCalculationInputs, *, interval: ConditionInterval, selected_venues: tuple[str, ...]
) -> ConditionTopVenueSignHistory:
    source = SpotCalculationInputs.model_validate(inputs)
    interval = ConditionInterval.model_validate(interval)
    if interval.start < source.start or interval.cutoff > source.cutoff:
        raise ValueError("Sign history must remain inside the actual source interval")
    declared = {instrument.venue for instrument in source.instruments}
    if len(set(selected_venues)) != len(selected_venues) or not set(selected_venues) <= declared:
        raise ValueError("Sign history requires distinct actual declared venues")
    amounts = tuple(
        value
        for row in (*source.minutes, *source.boundaries)
        for value in (row.buy_qty, row.sell_qty, row.sum_sq_qty)
        if value is not None
    )
    windows: list[ConditionSignHistoryWindow] = []
    with _native_arithmetic((*amounts, source.policy.significant_flow_sigma)):
        cursor = interval.start
        while selected_venues and cursor + timedelta(minutes=15) <= interval.cutoff:
            cutoff = cursor + timedelta(minutes=15)
            observations: list[ConditionVenueSignObservation] = []
            for venue in selected_venues:
                qualified = _selected_venue(source, venue, start=cursor, cutoff=cutoff)
                observations.append(
                    ConditionVenueSignObservation(
                        venue=venue,
                        value=qualified.sign if isinstance(qualified, _Venue) else None,
                        reason=None if isinstance(qualified, _Venue) else "INCOMPLETE",
                    )
                )
            windows.append(
                ConditionSignHistoryWindow(
                    interval=ConditionInterval(window="W15M", start=cursor, cutoff=cutoff),
                    observations=tuple(observations),
                )
            )
            cursor = cutoff
    return ConditionTopVenueSignHistory(
        token_id=source.token_id,
        interval=interval,
        selected_venues=selected_venues,
        windows=tuple(windows),
    )


def _coverage_through(venues: Sequence[_Venue]) -> Timestamp:
    endpoints = tuple(stamp for venue in venues for stamp in venue.coverage_endpoints)
    if not endpoints:
        return Timestamp(None, None, "NO_COMPLETE_VENUES")
    selected = endpoints[0]
    if any(stamp.value != selected.value for stamp in endpoints):
        raise ValueError("Complete window endpoints must bind the same actual cutoff")
    if all(stamp == selected for stamp in endpoints):
        return selected
    return Timestamp(selected.value, None, "COVERAGE_ENDPOINT_METADATA_DIFFERS")


def _cohort_composite(
    inputs: SpotCalculationInputs,
    current: Sequence[_Venue],
    prior: Sequence[_Venue],
    maximum: BasicConditionSpot,
    cohort: frozenset[str],
) -> ObservedConditionCohort | UnavailableConditionCohort:
    selected = tuple(row for row in current if row.name in cohort)
    if len(selected) < inputs.policy.min_spot_venues:
        return UnavailableConditionCohort(status="UNAVAILABLE", reason="NOT_COHORT_CONSISTENT")
    totals = _combined(inputs.token_id, selected)
    max_composite = maximum.composite_max
    if not isinstance(max_composite, ObservedConditionComposite) or max_composite.volume <= 0:
        return UnavailableConditionCohort(status="UNAVAILABLE", reason="NOT_COHORT_CONSISTENT")
    if not 2 * totals.volume_native > max_composite.volume:
        return UnavailableConditionCohort(status="UNAVAILABLE", reason="NOT_COHORT_CONSISTENT")
    share = display_ratio(totals.volume_native, max_composite.volume)
    if share is None:
        return UnavailableConditionCohort(status="UNAVAILABLE", reason="NOT_COHORT_CONSISTENT")
    composite = compose_native_flow(totals, tuple(row.name for row in selected), inputs.policy)
    if not isinstance(composite, ObservedConditionComposite):
        raise ValueError("Actual complete cohort totals cannot become unavailable")
    previous = tuple(row for row in prior if row.name in cohort)
    velocity: float | None = None
    reason: Literal["INCOMPLETE", "ZERO_BASELINE", "NOT_ESTIMABLE"] | None = "INCOMPLETE"
    if len(previous) == len(cohort):
        baseline = _combined(inputs.token_id, previous).volume_native
        velocity = display_ratio(totals.volume_native, baseline)
        reason = None if velocity is not None else "ZERO_BASELINE" if baseline == 0 else "NOT_ESTIMABLE"
    return ObservedConditionCohort(
        status="OBSERVED",
        cvd=composite.cvd,
        volume=composite.volume,
        buy_share=composite.buy_share,
        bs_ratio=composite.bs_ratio,
        venues=composite.venues,
        sign=composite.sign,
        null_reasons=composite.null_reasons,
        cohort=composite.venues,
        share_of_max=share,
        share_majority=True,
        velocity=velocity,
        velocity_reason=reason,
    )


def calculate_spot_window_graph(inputs: SpotWindowCalculationInputs) -> ConditionSpotWindowGraph:
    plan = SpotWindowCalculationInputs.model_validate(inputs)
    source = plan.source
    amounts = tuple(
        amount
        for row in (*source.minutes, *source.boundaries)
        for amount in (row.buy_qty, row.sell_qty, row.sum_sq_qty)
        if amount is not None
    )
    ticker_amounts = tuple(
        row.trailing_day_native_volume for row in source.tickers if row.trailing_day_native_volume is not None
    )
    with _native_arithmetic((*amounts, *ticker_amounts, source.policy.significant_flow_sigma)):
        qualified = {
            name: _qualify_spot(source, start=interval.start, cutoff=interval.cutoff)
            for name, interval in plan.current.items()
        }
        prior = {
            name: _qualify_spot(source, start=interval.start, cutoff=interval.cutoff)
            for name, interval in plan.previous.items()
        }
        current = {
            name: _calculate_spot(source, qualified[name], start=interval.start, cutoff=interval.cutoff)
            for name, interval in plan.current.items()
        }
        cohort = frozenset.intersection(
            *(frozenset(row.name for row in rows.observed) for rows in qualified.values())
        )
        spots: dict[ConditionWindow, WindowedConditionSpot] = {}
        durations: dict[ConditionWindow, float] = {}
        for name, interval in plan.current.items():
            basic = current[name]
            selected = _cohort_composite(
                source, qualified[name].observed, prior[name].observed, basic, cohort
            )
            consistent = isinstance(selected, ObservedConditionCohort)
            durations[name] = (interval.cutoff - interval.start).total_seconds()
            spots[name] = WindowedConditionSpot(
                kind="WINDOWED",
                token_id=basic.token_id,
                venues=basic.venues,
                composite_max=basic.composite_max,
                breadth=basic.breadth,
                cw_breadth=basic.cw_breadth,
                exact_majority=basic.exact_majority,
                topn=basic.topn,
                concentration=basic.concentration,
                direction=basic.direction,
                direction_reason=basic.direction_reason,
                classification=basic.classification,
                coverage=basic.coverage,
                interval=interval,
                coverage_through=_coverage_through(qualified[name].observed),
                composite_cohort=selected,
                cohort_consistent={other: consistent for other in plan.current if other != name},
            )
        return ConditionSpotWindowGraph(spots=spots, durations_s=durations, primary=plan.primary)


@dataclass(frozen=True, slots=True)
class SpotNativeMeasurement:
    path: str
    value: Decimal
    window: ConditionWindow
    unit: Literal["NATIVE_TOKEN", "NATIVE_TOKEN_SQUARED"]
    observed_at: Timestamp
    venue: str | None


def spot_native_measurements(graph: ConditionSpotWindowGraph) -> tuple[SpotNativeMeasurement, ...]:
    measurements: list[SpotNativeMeasurement] = []
    for window, value in graph.spots.items():
        for name, composite in (
            ("composite_max", value.composite_max),
            ("composite_cohort", value.composite_cohort),
        ):
            if isinstance(composite, ObservedConditionComposite):
                for field, quantity in (("cvd", composite.cvd), ("volume", composite.volume)):
                    measurements.append(
                        SpotNativeMeasurement(
                            f"spot.{window}.{name}.{field}",
                            quantity,
                            window,
                            "NATIVE_TOKEN",
                            value.coverage_through,
                            None,
                        )
                    )
        for index, venue in enumerate(value.venues):
            for field, quantity in (
                ("buy_qty", venue.buy_qty),
                ("sell_qty", venue.sell_qty),
                ("volume_native", venue.volume_native),
                ("cvd", venue.cvd),
                ("sum_sq_qty", venue.sum_sq_qty),
            ):
                measurements.append(
                    SpotNativeMeasurement(
                        f"spot.{window}.venues[{index}].{field}",
                        quantity,
                        window,
                        "NATIVE_TOKEN_SQUARED" if field == "sum_sq_qty" else "NATIVE_TOKEN",
                        value.coverage_through,
                        venue.venue,
                    )
                )
    return tuple(measurements)


def compare_spot_windows(
    graph: ConditionSpotWindowGraph, policy: SpotCalculationPolicy
) -> dict[ConditionWindow, dict[ConditionWindow, ConditionCohortComparison]]:
    validated = ConditionSpotWindowGraph.model_validate(graph)
    captured_policy = SpotCalculationPolicy.model_validate(policy)
    totals: dict[tuple[ConditionWindow, tuple[str, ...]], NativeFlowTotals] = {}
    composites: dict[tuple[ConditionWindow, tuple[str, ...]], ObservedConditionComposite] = {}

    def composite(window: ConditionWindow, cohort: tuple[str, ...]) -> ObservedConditionComposite:
        key = (window, cohort)
        if key not in composites:
            spot = validated.spots[window]
            rows = tuple(
                NativeFlowTotals(
                    token_id=spot.token_id,
                    buy_qty=row.buy_qty,
                    sell_qty=row.sell_qty,
                    volume_native=row.volume_native,
                    cvd=row.cvd,
                    sum_sq_qty=row.sum_sq_qty,
                )
                for row in spot.venues
                if row.venue in cohort
            )
            totals[key] = combine_native_flow(spot.token_id, rows)
            result = compose_native_flow(totals[key], cohort, captured_policy)
            if not isinstance(result, ObservedConditionComposite):
                raise ValueError("An actual complete venue cohort cannot become an unavailable composite")
            composites[key] = result
        return composites[key]

    comparisons: dict[ConditionWindow, dict[ConditionWindow, ConditionCohortComparison]] = {}
    for name, left in validated.spots.items():
        left_names = {row.venue for row in left.venues}
        comparisons[name] = {}
        for other, right in validated.spots.items():
            if name == other:
                continue
            cohort = tuple(sorted(left_names & {row.venue for row in right.venues}))
            share: float | None = None
            majority: bool | None = None
            current: ObservedConditionComposite | None = None
            if cohort:
                current = composite(name, cohort)
                maximum = left.composite_max
                if isinstance(maximum, ObservedConditionComposite) and maximum.volume > 0:
                    share = display_ratio(current.volume, maximum.volume)
                    with _native_arithmetic((current.volume, maximum.volume)):
                        majority = 2 * current.volume > maximum.volume
            if len(cohort) < captured_policy.min_spot_venues or majority is not True or share is None:
                comparisons[name][other] = UnavailableConditionCohortComparison(
                    status="UNAVAILABLE",
                    cohort=cohort,
                    share_of_max=share,
                    share_majority=majority,
                    reason="NOT_COHORT_CONSISTENT",
                )
                continue
            if current is None:
                raise ValueError("A qualified common cohort requires its actual native totals")
            comparisons[name][other] = ObservedConditionCohortComparison(
                status="OBSERVED",
                cohort=cohort,
                current=current,
                other=composite(other, cohort),
                share_of_max=share,
                share_majority=True,
            )
    return comparisons


def enrich_spot_cohorts(
    graph: ConditionSpotWindowGraph, policy: SpotCalculationPolicy
) -> ConditionSpotCohortGraph:
    comparisons = compare_spot_windows(graph, policy)
    return ConditionSpotCohortGraph(
        spots={
            name: CohortEnrichedConditionSpot.from_windowed(
                value,
                comparisons=comparisons[name],
                role="PRIMARY" if name == graph.primary else "OTHER_WINDOW",
            )
            for name, value in graph.spots.items()
        },
        durations_s=graph.durations_s,
        primary=graph.primary,
    )

"""Actual OI-only derivative facts with unrelated calculations unrequested."""

from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
    UncapturedOISynchronizationRules,
)
from tank.analysis.condition_graph_contracts import (
    BasicConditionDerivatives,
    ConditionDerivativeVenue,
    ConditionInterval,
    ConditionOITemporalExclusions,
    ConditionSupplementalOIObservation,
    ConditionSynchronizedOpenInterest,
    ConditionWindow,
    UnavailableConditionSynchronizedOpenInterest,
    UnrequestedConditionBasis,
    UnrequestedConditionFunding,
    UnrequestedConditionFundingDispersion,
    UnrequestedConditionIncrementalVolumes,
    UnrequestedConditionLiquidations,
)
from tank.analysis.oi_composite_history import ObservedOICompositeWindow
from tank.analysis.oi_synchronization import OISynchronizationPolicy
from tank.analysis.oi_usd import RecordedOIUsd, recorded_oi_synchronization, recorded_oi_usd
from tank.analysis.oi_window_statistics import OIStatisticsFacts, observed_oi_statistics, oi_window_statistics
from tank.analysis.open_interest import OIInstrumentWindow, OIWindowRequest, OpenInterestCalculator
from tank.analysis.positioning import MissingOIUsdDetail
from tank.util import parse_iso_time


def _supplemental(
    calculator: OpenInterestCalculator, window: OIInstrumentWindow, request: OIWindowRequest
) -> ConditionOITemporalExclusions | None:
    points = tuple(
        calculator.observation(value.evidence)
        for value in window.excluded
        if value.reason == "OI_BUCKET_EDGE_UNSPECIFIED"
    )
    visible = tuple(point for point in points if point.visible_at(request.assembled_at) is True)
    if not visible:
        return None
    clocks = tuple((point.reported_time(), point) for point in visible)
    latest_at = max(at for at, _ in clocks if at is not None)
    latest = tuple(point for at, point in clocks if at == latest_at)
    if len(latest) != 1:
        raise ValueError("Supplemental OI cannot select ambiguous equal-clock evidence")
    point = latest[0]
    native, usd = point.native_quantity.value, point.reported_usd.value
    return ConditionOITemporalExclusions(
        count=len(visible),
        reason="INCOMPLETE",
        detail="OI_BUCKET_EDGE_UNSPECIFIED",
        latest_observation=ConditionSupplementalOIObservation(
            observed_at=point.observed_at,
            oi_native=native if native is not None and native >= 0 else None,
            oi_usd=usd if usd is not None and usd >= 0 else None,
            scope="SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT",
        ),
    )


def calculate_oi_family(
    calculator: OpenInterestCalculator,
    request: OIWindowRequest,
    *,
    rules: CapturedOISynchronizationRules | UncapturedOISynchronizationRules,
    synchronization_policy: OISynchronizationPolicy,
    window: ConditionWindow,
) -> BasicConditionDerivatives:
    """Compute observed endpoints and like-for-like composite history from owned reads."""
    request = OIWindowRequest.model_validate(request)
    if request.start.value is None or request.cutoff.value is None:
        raise ValueError("OI family requires actual request bounds")
    interval = ConditionInterval(
        window=window, start=parse_iso_time(request.start.value), cutoff=parse_iso_time(request.cutoff.value)
    )
    native = {
        point.instrument.instrument_id: point for point in calculator.native_baseline(request, window=window)
    }
    usd = {
        point.instrument.instrument_id: point
        for point in calculator.recorded_usd_baseline(request, window=window)
    }
    venues = tuple(
        ConditionDerivativeVenue(
            venue=point.instrument.venue,
            instrument_id=point.instrument.instrument_id,
            open_interest=oi_window_statistics(
                point,
                native_baseline=native[point.instrument.instrument_id],
                usd_baseline=usd[point.instrument.instrument_id],
            ),
            funding=UnrequestedConditionFunding(status="NOT_REQUESTED"),
            liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
            oi_temporal_exclusions=_supplemental(calculator, point, request),
            basis=UnrequestedConditionBasis(status="NOT_REQUESTED"),
        )
        for point in calculator.select(request)
    )
    composite = calculator.composite_window(
        request, rules=rules, policy=synchronization_policy, window=window
    )
    synchronized: ConditionSynchronizedOpenInterest | UnavailableConditionSynchronizedOpenInterest
    if isinstance(composite, ObservedOICompositeWindow):
        witness = recorded_oi_synchronization(composite.selection)
        initial, final = witness.start, witness.end
        start_usd, end_usd = initial.usd_total(), final.usd_total()
        detail: MissingOIUsdDetail | None = None
        if start_usd is None or end_usd is None:
            components = tuple(
                recorded_oi_usd(point)
                for source in composite.selection.sources
                for point in (source.start, source.end)
            )
            detail = (
                "REPORTED_USD_INVALID"
                if any(
                    not isinstance(value, RecordedOIUsd) and value.reason == "REPORTED_USD_INVALID"
                    for value in components
                )
                else "RECORDED_USD_NOT_CAPTURED"
            )
        synchronized = ConditionSynchronizedOpenInterest(
            status="OBSERVED",
            venues=tuple(dict.fromkeys(source.instrument.venue for source in composite.selection.sources)),
            values=observed_oi_statistics(
                OIStatisticsFacts(
                    interval=interval,
                    native_start=initial.native_total(),
                    native_end=final.native_total(),
                    usd_start=start_usd,
                    usd_end=end_usd,
                    usd_detail=detail,
                    native_normal_range=composite.history.native_normal_range,
                    usd_normal_range=composite.history.usd_normal_range,
                    start_observed_at=initial.at,
                    end_observed_at=final.at,
                )
            ),
            synchronization=witness,
        )
    else:
        synchronized = UnavailableConditionSynchronizedOpenInterest(status="UNAVAILABLE", reason="INCOMPLETE")
    return BasicConditionDerivatives(
        kind="BASIC",
        token_id=calculator.token_id,
        interval=interval,
        per_venue=venues,
        synchronized=synchronized,
        funding_dispersion=UnrequestedConditionFundingDispersion(status="NOT_REQUESTED"),
        liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
        incremental_volume=UnrequestedConditionIncrementalVolumes(status="NOT_REQUESTED"),
    )

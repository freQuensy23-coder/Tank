"""Endpoint-bound OI statistics over actual native and recorded USD populations."""

from decimal import Decimal

from pydantic import Field

from tank.analysis.condition_graph_contracts import (
    ConditionInterval,
    ObservedConditionOpenInterest,
    UnavailableConditionOpenInterest,
)
from tank.analysis.oi_ratio import native_oi_change, usd_oi_change
from tank.analysis.oi_usd import RecordedOIUsd, recorded_oi_usd
from tank.analysis.open_interest import (
    OIInstrumentWindow,
    OINativeHistoryBaseline,
    OIRecordedUsdHistoryBaseline,
    SelectedOIEndpoint,
)
from tank.analysis.positioning import (
    MissingOIDecomposition,
    MissingOIUsdDetail,
    OiDecompositionResult,
    oi_class_from_comparisons,
)
from tank.analysis.recorded_oi_decomposition import RecordedOiEndpoints, recorded_oi_decomposition
from tank.analysis.statistics_contracts import FloatRange
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import parse_iso_time


def oi_window_statistics(
    window: OIInstrumentWindow,
    *,
    native_baseline: OINativeHistoryBaseline,
    usd_baseline: OIRecordedUsdHistoryBaseline,
) -> ObservedConditionOpenInterest | UnavailableConditionOpenInterest:
    """Preserve native facts when the independently recorded USD component is absent."""
    window = OIInstrumentWindow.model_validate(window)
    native_baseline = OINativeHistoryBaseline.model_validate(native_baseline)
    usd_baseline = OIRecordedUsdHistoryBaseline.model_validate(usd_baseline)
    initial_at, final_at = window.start.at.value, window.end.at.value
    if initial_at is None or final_at is None:
        raise ValueError("OI statistics require the actual logical endpoint clocks")
    initial, final = parse_iso_time(initial_at), parse_iso_time(final_at)
    for baseline in (native_baseline, usd_baseline):
        if baseline.instrument != window.instrument or (
            baseline.interval.start,
            baseline.interval.cutoff,
        ) != (initial, final):
            raise ValueError("OI statistics cannot borrow another instrument or window baseline")
    if not isinstance(window.start, SelectedOIEndpoint) or not isinstance(window.end, SelectedOIEndpoint):
        return UnavailableConditionOpenInterest(
            status="UNAVAILABLE",
            reason="INCOMPLETE",
            detail=(
                "OI_BUCKET_EDGE_UNSPECIFIED"
                if any(point.reason == "OI_BUCKET_EDGE_UNSPECIFIED" for point in window.excluded)
                else None
            ),
        )
    start, end = window.start.observation, window.end.observation
    native_start, native_end = start.native_quantity.value, end.native_quantity.value
    if native_start is None or native_end is None:
        raise ValueError("Selected OI endpoints lost their observed native quantities")
    start_usd, end_usd = recorded_oi_usd(start), recorded_oi_usd(end)
    usd_start = start_usd.value if isinstance(start_usd, RecordedOIUsd) else None
    usd_end = end_usd.value if isinstance(end_usd, RecordedOIUsd) else None
    detail: MissingOIUsdDetail | None = None
    if usd_start is None or usd_end is None:
        detail = (
            "REPORTED_USD_INVALID"
            if any(
                not isinstance(value, RecordedOIUsd) and value.reason == "REPORTED_USD_INVALID"
                for value in (start_usd, end_usd)
            )
            else "RECORDED_USD_NOT_CAPTURED"
        )
    return observed_oi_statistics(
        OIStatisticsFacts(
            interval=native_baseline.interval,
            native_start=native_start,
            native_end=native_end,
            usd_start=usd_start,
            usd_end=usd_end,
            usd_detail=detail,
            native_normal_range=native_baseline.normal_range,
            usd_normal_range=usd_baseline.normal_range,
            start_observed_at=start.observed_at,
            end_observed_at=end.observed_at,
        )
    )


class OIStatisticsFacts(DomainModel):
    interval: ConditionInterval
    native_start: Decimal = Field(ge=0)
    native_end: Decimal = Field(ge=0)
    usd_start: Decimal | None = Field(ge=0)
    usd_end: Decimal | None = Field(ge=0)
    usd_detail: MissingOIUsdDetail | None
    native_normal_range: FloatRange
    usd_normal_range: FloatRange
    start_observed_at: Timestamp
    end_observed_at: Timestamp


def observed_oi_statistics(facts: OIStatisticsFacts) -> ObservedConditionOpenInterest:
    """Use one exact statistics authority for instrument and synchronized endpoints."""
    facts = OIStatisticsFacts.model_validate(facts)
    native_start, native_end = facts.native_start, facts.native_end
    usd_start, usd_end = facts.usd_start, facts.usd_end
    native = native_oi_change(native_start, native_end, normal_range=facts.native_normal_range)
    usd = usd_oi_change(usd_start, usd_end, normal_range=facts.usd_normal_range)
    detail = facts.usd_detail
    decomposition: OiDecompositionResult
    if usd_start is not None and usd_end is not None:
        if detail is not None:
            raise ValueError("Known recorded USD endpoints cannot claim a missing component")
        decomposition = recorded_oi_decomposition(
            RecordedOiEndpoints(
                native_start=native_start, native_end=native_end, usd_start=usd_start, usd_end=usd_end
            )
        )
    else:
        if detail is None:
            raise ValueError("Missing recorded USD endpoints require the actual component cause")
        decomposition = MissingOIDecomposition(reason="INCOMPLETE", detail=detail)
    classification = oi_class_from_comparisons(native.beyond, usd.beyond, decomposition)
    source_start = (
        None if facts.start_observed_at.value is None else parse_iso_time(facts.start_observed_at.value)
    )
    source_end = None if facts.end_observed_at.value is None else parse_iso_time(facts.end_observed_at.value)
    if source_start is None or source_end is None:
        raise ValueError("Selected OI endpoints lost their actual source clocks")
    return ObservedConditionOpenInterest(
        status="OBSERVED",
        requested_window_duration_s=(facts.interval.cutoff - facts.interval.start).total_seconds(),
        observed_window_duration_s=(source_end - source_start).total_seconds(),
        oi_native_start=native_start,
        oi_native_end=native_end,
        oi_native_chg_pct=native.value,
        oi_native_chg_pct_reason=native.reason,
        oi_usd_start=usd_start,
        oi_usd_end=usd_end,
        oi_usd_chg_pct=usd.value,
        oi_usd_chg_pct_reason=usd.reason,
        oi_usd_reason=(
            None if detail is None else "NOT_ESTIMABLE" if detail == "REPORTED_USD_INVALID" else "INCOMPLETE"
        ),
        oi_usd_detail=detail,
        decomposition=decomposition,
        native_normal_range=facts.native_normal_range,
        usd_normal_range=facts.usd_normal_range,
        native_oi_vs_normal=native.position,
        native_beyond=native.beyond,
        usd_beyond=usd.beyond,
        oi_class=classification,
        classification_ready=classification is not None,
        start_observed_at=facts.start_observed_at,
        end_observed_at=facts.end_observed_at,
    )

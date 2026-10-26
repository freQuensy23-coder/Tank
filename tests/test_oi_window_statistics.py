from datetime import timedelta
from decimal import Decimal

import pytest

from tank.analysis.condition_derivative_input_codec import decode_oi_inputs
from tank.analysis.condition_derivative_inputs import OISelectionPolicy
from tank.analysis.condition_graph_contracts import ObservedConditionOpenInterest
from tank.analysis.oi_window_statistics import oi_window_statistics
from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import JsonInput, iso, parse_iso_time


def statistics_source(
    usd_start: str | None, usd_end: str | None
) -> tuple[OpenInterestCalculator, OIWindowRequest]:
    cutoff = parse_iso_time("2026-10-16T12:00:00Z")
    start = cutoff - timedelta(hours=1)
    rows: list[dict[str, JsonInput]] = [
        {
            "instrument_id": "A",
            "ts": iso(start - timedelta(hours=hour)),
            "received_ts": iso(cutoff + timedelta(seconds=1)),
            "oi_native": "3",
            "oi_usd": "1",
        }
        for hour in range(1, 241)
    ]
    rows.extend(
        {
            "instrument_id": "A",
            "ts": iso(at),
            "received_ts": iso(cutoff + timedelta(seconds=1)),
            "oi_native": native,
            "oi_usd": usd,
            "mark_price": "999",
        }
        for at, native, usd in ((start, "3", usd_start), (cutoff, "4", usd_end))
    )
    source = decode_oi_inputs(
        token_id="T",
        instruments=[{"instrument_id": "A", "token_id": "T", "venue": "V", "market_type": "PERP_LINEAR"}],
        observations=rows,
    )
    return OpenInterestCalculator(
        source,
        OISelectionPolicy(tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975)),
    ), OIWindowRequest(
        start=chosen_boundary(start, precision="EXACT"),
        cutoff=chosen_boundary(cutoff, precision="EXACT"),
        assembled_at=collection_clock(cutoff + timedelta(seconds=2)),
    )


def test_recorded_endpoint_boundary_classifies_exact_half_as_mixed() -> None:
    calculator, request = statistics_source("1", "1")
    result = oi_window_statistics(
        calculator.select(request)[0],
        native_baseline=calculator.native_baseline(request, window="W1H")[0],
        usd_baseline=calculator.recorded_usd_baseline(request, window="W1H")[0],
    )
    assert isinstance(result, ObservedConditionOpenInterest)
    assert result.native_beyond is True and result.usd_beyond is False
    assert result.oi_native_start == 3 and result.oi_native_end == 4
    assert result.decomposition.pos_share == 0.5
    assert result.oi_class == "MIXED" and result.classification_ready is True


@pytest.mark.parametrize(
    "value,detail", [(None, "RECORDED_USD_NOT_CAPTURED"), ("-1", "REPORTED_USD_INVALID")]
)
def test_missing_recorded_component_preserves_native_without_mark_fallback(
    value: str | None, detail: str
) -> None:
    calculator, request = statistics_source(value, "1")
    result = oi_window_statistics(
        calculator.select(request)[0],
        native_baseline=calculator.native_baseline(request, window="W1H")[0],
        usd_baseline=calculator.recorded_usd_baseline(request, window="W1H")[0],
    )
    assert isinstance(result, ObservedConditionOpenInterest)
    assert result.oi_native_start == 3 and result.oi_native_end == 4
    assert result.native_beyond is True and result.oi_class is None
    assert result.oi_usd_start is None and result.oi_usd_end == Decimal(1)
    assert result.oi_usd_detail == detail
    assert result.oi_usd_chg_pct_reason == "INCOMPLETE"


def test_statistics_refuses_baseline_for_another_scope() -> None:
    calculator, request = statistics_source("1", "1")
    baseline = calculator.native_baseline(request, window="W1H")[0]
    changed = baseline.model_copy(
        update={"instrument": baseline.instrument.model_copy(update={"token_id": "OTHER"})}
    )
    with pytest.raises(ValueError, match="another instrument or window"):
        oi_window_statistics(
            calculator.select(request)[0],
            native_baseline=changed,
            usd_baseline=calculator.recorded_usd_baseline(request, window="W1H")[0],
        )


def test_recorded_zero_is_known_and_has_separate_ratio_cause() -> None:
    calculator, request = statistics_source("0", "0")
    result = oi_window_statistics(
        calculator.select(request)[0],
        native_baseline=calculator.native_baseline(request, window="W1H")[0],
        usd_baseline=calculator.recorded_usd_baseline(request, window="W1H")[0],
    )
    assert isinstance(result, ObservedConditionOpenInterest)
    assert result.oi_usd_start == result.oi_usd_end == Decimal(0)
    assert result.oi_usd_reason is None and result.oi_usd_detail is None
    assert result.oi_usd_chg_pct is None and result.oi_usd_chg_pct_reason == "ZERO_BASELINE"
    assert result.oi_native_end == 4 and result.native_beyond is True

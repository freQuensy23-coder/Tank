from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_derivative_input_codec import decode_oi_inputs, resolve_oi_observation
from tank.analysis.oi_usd import RecordedOIUsd, UnavailableRecordedOIUsd, recorded_oi_usd
from tank.util import JsonInput


@pytest.mark.parametrize(
    "label", [None, "PROVIDER_REPORTED", "PREVIOUSLY_DERIVED", 7, {"writer": "retained"}]
)
@pytest.mark.parametrize("amount", ["0", "12.500"])
def test_recorded_usd_preserves_known_amount_and_original_source(amount: str, label: JsonInput) -> None:
    row: dict[str, JsonInput] = {
        "instrument_id": "A",
        "token_id": "T",
        "venue": "venue",
        "market_type": "PERP_LINEAR",
        "ts": "2026-10-16T12:00:00Z",
        "oi_native": "10",
        "oi_usd": amount,
        "oi_usd_source": label,
    }
    snapshot = decode_oi_inputs(token_id="T", instruments=[], observations=[row])
    point = snapshot.observations[0]
    result = recorded_oi_usd(point)
    assert isinstance(result, RecordedOIUsd)
    assert result.value == Decimal(amount)
    assert result.source.evidence == point.evidence
    assert result.source.received_at.value is None
    assert result.source.mark_observed_at.value is None
    assert resolve_oi_observation(snapshot, result.source.evidence, expected=result.source) == row
    with pytest.raises(ValidationError, match="actual stored component"):
        RecordedOIUsd.model_validate(result.model_copy(update={"value": Decimal(99)}))


@pytest.mark.parametrize(
    "amount,reason",
    [
        (None, "RECORDED_USD_NOT_CAPTURED"),
        ("-1", "REPORTED_USD_INVALID"),
        ("NaN", "REPORTED_USD_INVALID"),
        ("invalid", "REPORTED_USD_INVALID"),
    ],
)
def test_bad_recorded_usd_never_borrows_mark_or_erases_native(amount: JsonInput, reason: str) -> None:
    row: dict[str, JsonInput] = {
        "instrument_id": "A",
        "token_id": "T",
        "venue": "venue",
        "market_type": "PERP_LINEAR",
        "quote_asset": "USD",
        "ts": "2026-10-16T12:00:00Z",
        "received_ts": "2026-10-16T12:00:01Z",
        "oi_native": "10",
        "oi_usd": amount,
        "mark_price": "2",
        "mark_as_of": "2026-10-16T12:00:00Z",
    }
    snapshot = decode_oi_inputs(token_id="T", instruments=[], observations=[row])
    result = recorded_oi_usd(snapshot.observations[0])
    assert isinstance(result, UnavailableRecordedOIUsd)
    assert result.reason == reason
    assert result.source.native_quantity.value == Decimal(10)
    assert result.source.mark_price.value == Decimal(2)
    assert resolve_oi_observation(snapshot, result.source.evidence, expected=result.source) == row
    with pytest.raises(ValidationError, match="actual stored component"):
        RecordedOIUsd(status="RECORDED_USD", value=Decimal(20), source=result.source)

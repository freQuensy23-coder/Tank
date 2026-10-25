"""Storage evidence and qualified scope remain separate from spot mathematics."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_input_codec import (
    DecodedSpotInputs,
    decode_spot_inputs,
    source_descriptor_digest,
)
from tank.analysis.condition_inputs import SpotCalculationPolicy
from tank.time_precision import Timestamp
from tank.util import JsonInput

AT = datetime(2026, 10, 19, 12, tzinfo=UTC)
INSTRUMENT: dict[str, JsonInput] = {
    "instrument_id": "VENUE:spot:T",
    "token_id": "T",
    "venue": "VENUE",
    "market_type": "SPOT",
}


def minute() -> dict[str, JsonInput]:
    return {
        "instrument_id": "VENUE:spot:T",
        "minute_start": "2026-10-19T12:00:00Z",
        "complete": True,
        "buy_qty": "3",
        "sell_qty": "1",
        "sum_sq_qty": "4",
        "received_ts": "2026-10-19T12:01:02Z",
        "received_ts_prec": "COLLECTION_ONLY",
        "received_ts_prec_reason": None,
    }


def with_boundary(piece: dict[str, JsonInput]) -> dict[str, JsonInput]:
    row = minute()
    parts: list[JsonInput] = [piece]
    row["boundary_segments"] = parts
    return row


def decoded(
    rows: list[dict[str, JsonInput]],
    *,
    pieces: list[dict[str, JsonInput]] | None = None,
    tickers: list[dict[str, JsonInput]] | None = None,
    ticker_field: Literal["volume_native", "volume"] = "volume_native",
) -> DecodedSpotInputs:
    return decode_spot_inputs(
        token_id="T",
        start=AT,
        cutoff=AT + timedelta(minutes=1),
        assembled_at=Timestamp("2026-10-19T12:02:00Z", "COLLECTION_ONLY"),
        instruments=[INSTRUMENT],
        flow_bars=rows,
        flow_segments=[] if pieces is None else pieces,
        tickers=[] if tickers is None else tickers,
        policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2),
        native_ticker_volume_field=ticker_field,
    )


def test_absent_counts_and_precision_are_not_zero_or_exact() -> None:
    result = decoded([minute()])
    row = result.inputs.minutes[0]
    assert row.n_buy is None and row.n_sell is None
    assert row.buy_qty == Decimal(3) and row.sell_qty == Decimal(1)
    assert row.observed_at.value is None and row.observed_at.precision is None
    assert row.received_at.precision == "COLLECTION_ONLY"
    assert row.visible_at(result.inputs.assembled_at) is True
    missing_receipt = decoded([dict(minute(), received_ts=None)])
    assert missing_receipt.inputs.minutes[0].visible_at(result.inputs.assembled_at) is None


def test_embedded_scope_is_qualified_without_rewriting_original_evidence() -> None:
    piece: dict[str, JsonInput] = {
        "start": "2026-10-19T12:00:00Z",
        "end": "2026-10-19T12:00:30Z",
        "complete": True,
        "buy_qty": "1",
        "sell_qty": "0",
        "sum_sq_qty": "1",
        "n_buy": 1,
        "n_sell": 0,
        "captured_extra": {"source": "retained"},
    }
    row = with_boundary(piece)
    result = decoded([row])
    boundary = result.inputs.boundaries[0]
    original = result.source_evidence.original(boundary.evidence)
    effective = result.source_evidence.effective(boundary.evidence)
    assert original == piece and "instrument_id" not in original
    assert effective["instrument_id"] == "VENUE:spot:T" and effective["token_id"] == "T"
    assert effective["captured_extra"] == {"source": "retained"}
    descriptor = result.source_evidence.descriptor(boundary.evidence)
    parent = result.inputs.minutes[0].evidence
    assert descriptor.parent_source_evidence_sha256 == parent.source_evidence_sha256
    assert source_descriptor_digest(descriptor) == boundary.evidence.source_evidence_sha256
    assert descriptor.scope_projection_sha256 != boundary.evidence.source_projection_sha256
    assert (
        result.source_evidence.source_reference(boundary.evidence.source_evidence_sha256) == boundary.evidence
    )
    piece["buy_qty"] = "999"
    assert result.source_evidence.original(boundary.evidence)["buy_qty"] == "1"
    assert boundary.buy_qty == Decimal(1)


def test_conflicting_source_projections_are_preserved_for_actual_as_of_qualification() -> None:
    embedded: dict[str, JsonInput] = {
        "start": "2026-10-19T12:00:00Z",
        "end": "2026-10-19T12:00:30Z",
        "complete": True,
        "buy_qty": "1",
        "sell_qty": "0",
        "sum_sq_qty": "1",
    }
    standalone = dict(embedded, instrument_id="VENUE:spot:T", buy_qty="2", sum_sq_qty="2")
    result = decoded([with_boundary(embedded)], pieces=[standalone])
    assert tuple(row.buy_qty for row in result.inputs.boundaries) == (Decimal(2), Decimal(1))
    originals = [result.source_evidence.original(ref) for ref in result.source_evidence.references()]
    assert embedded in originals and standalone in originals and len(originals) == 3


@pytest.mark.parametrize("field,value", [("n_buy", True), ("n_buy", 1.0), ("complete", 1), ("buy_qty", True)])
def test_wrong_scalar_types_do_not_authorize_financial_inputs(field: str, value: JsonInput) -> None:
    with pytest.raises((ValidationError, ValueError)):
        decoded([dict(minute(), **{field: value})])


def test_unknown_native_amount_remains_an_explicit_missing_input() -> None:
    row = decoded([dict(minute(), buy_qty=None)]).inputs.minutes[0]
    assert row.buy_qty is None and row.sell_qty == Decimal(1)


def test_conflicting_token_or_embedded_market_scope_refuses() -> None:
    with pytest.raises(ValueError, match="scope contradicts"):
        decoded([dict(minute(), token_id="OTHER")])
    with pytest.raises(ValueError, match="actual parent scope"):
        decoded(
            [
                with_boundary(
                    {
                        "instrument_id": "DIFFERENT",
                        "start": "2026-10-19T12:00:00Z",
                        "end": "2026-10-19T12:00:30Z",
                    }
                )
            ]
        )


def test_ticker_native_alias_is_an_explicit_caller_declaration() -> None:
    ticker: dict[str, JsonInput] = {
        "instrument_id": "VENUE:spot:T",
        "ts": "2026-10-19T11:59:00Z",
        "volume": "86400",
    }
    canonical = decoded([], tickers=[ticker]).inputs.tickers[0]
    assert canonical.trailing_day_native_volume is None
    alias = decoded([], tickers=[ticker], ticker_field="volume").inputs.tickers[0]
    assert alias.trailing_day_native_volume == Decimal(86400)
    assert alias.native_volume_field == "volume"
    assert alias.observed_at.precision is None


def test_undated_ticker_volume_is_preserved_without_an_invented_estimate_clock() -> None:
    ticker: dict[str, JsonInput] = {"instrument_id": "VENUE:spot:T", "volume_native": "86400"}
    row = decoded([], tickers=[ticker]).inputs.tickers[0]
    assert row.trailing_day_native_volume == Decimal(86400)
    assert row.observed_at.value is None and row.observed_at.precision is None
    assert row.observed_at.precision_reason is not None


def test_scope_cannot_be_inferred_from_instrument_identifier_spelling() -> None:
    with pytest.raises(ValueError, match="market_type"):
        decode_spot_inputs(
            token_id="T",
            start=AT,
            cutoff=AT + timedelta(minutes=1),
            assembled_at=Timestamp(None, None, "ASSEMBLY_NOT_CAPTURED"),
            instruments=[],
            flow_bars=[minute()],
            flow_segments=[],
            tickers=[],
            policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2),
            native_ticker_volume_field="volume_native",
        )


def test_embedded_piece_cannot_borrow_an_unrelated_parent_minute() -> None:
    with pytest.raises(ValueError, match="actual parent minute"):
        decoded(
            [
                with_boundary(
                    {
                        "start": "2026-10-19T11:59:00Z",
                        "end": "2026-10-19T11:59:30Z",
                        "complete": True,
                        "buy_qty": "1",
                        "sell_qty": "0",
                        "sum_sq_qty": "1",
                    }
                )
            ]
        )


def test_actual_late_receipt_is_not_relabelled_visible_by_the_source_clock() -> None:
    row = dict(minute(), received_ts="2026-10-19T12:02:01Z")
    result = decoded([row])
    assert result.inputs.minutes[0].visible_at(result.inputs.assembled_at) is False
    assert result.inputs.minutes[0].start == AT

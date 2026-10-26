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
from tank.analysis.condition_inputs import SpotCalculationPolicy, SpotPolicyConfiguration
from tank.settings import Settings
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
        flow_parent_bars=[],
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
    assert row.visibility_basis(result.inputs.assembled_at) == "SOURCE_RECEIPT"
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
            flow_parent_bars=[],
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


def test_spot_policy_uses_actual_captured_threshold_values_without_numeric_defaults() -> None:
    thresholds = Settings().thresholds()
    declared = thresholds.model_copy(update={"significant_flow_sigma": 3.0, "min_spot_venues": 4})
    policy = SpotCalculationPolicy.from_thresholds(declared)
    assert policy.significant_flow_sigma == Decimal("3.0") and policy.min_spot_venues == 4
    with pytest.raises(ValidationError):
        SpotCalculationPolicy.model_validate({"significant_flow_sigma": Decimal(2)})


def generated_empty_bar() -> dict[str, JsonInput]:
    from tank.contracts.json import decode_json
    from tank.market.flow import FlowAccumulator
    from tank.market.trade_contracts import FlowIdentity
    from tank.util import iso

    observed = AT + timedelta(seconds=31)
    accumulator = FlowAccumulator(now=lambda: observed)
    identity = FlowIdentity(
        instrument_id="VENUE:spot:T", token_id="T", venue="binance", market_type="SPOT", quote_asset="USD"
    )
    accumulator.empty_minute(identity, AT)
    accumulator.prove_range(
        identity.instrument_id,
        AT,
        AT + timedelta(seconds=30),
        complete=True,
        source="declared-empty-stream-fixture",
        observed_at=observed,
    )
    accumulator.register_boundaries(identity.instrument_id, [AT + timedelta(seconds=30)])
    bar = accumulator.record((identity.instrument_id, iso(AT)), complete=False)
    value = decode_json(bar.model_dump_json())
    if not isinstance(value, dict):
        raise ValueError("Actual flow writer must serialize its bar object")
    return dict(value)


def partial_parent_inputs(parent: dict[str, JsonInput]) -> DecodedSpotInputs:
    scope = dict(INSTRUMENT)
    if "venue" in parent:
        scope["venue"] = parent["venue"]
    return decode_spot_inputs(
        token_id="T",
        start=AT,
        cutoff=AT + timedelta(seconds=30),
        assembled_at=Timestamp("2026-10-19T12:00:40Z", "COLLECTION_ONLY"),
        instruments=[scope],
        flow_bars=[],
        flow_parent_bars=[parent],
        flow_segments=[],
        tickers=[],
        policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2),
        native_ticker_volume_field="volume_native",
    )


def test_actual_generated_empty_piece_preserves_its_own_clock_without_a_trade_receipt() -> None:
    source = generated_empty_bar()
    original = dict(source)
    result = partial_parent_inputs(source)
    assert result.inputs.minutes == ()
    assert len(result.inputs.boundaries) == 1
    piece = result.inputs.boundaries[0]
    assert piece.generated_empty is not None
    assert piece.received_at.value is None
    assert piece.observed_at.value == "2026-10-19T12:00:31Z"
    assert piece.visible_at(result.inputs.assembled_at) is True
    assert piece.visibility_basis(result.inputs.assembled_at) == "GENERATED_EMPTY_PIECE"
    assert piece.parent_document is not None
    assert piece.parent_document.end == AT + timedelta(minutes=1)
    assert piece.evidence.collection == "flow_parent_bars"
    assert result.source_evidence.original(piece.parent_document.evidence) == original
    assert source == original


@pytest.mark.parametrize("field,value", [("observed_at", "2026-10-19T12:00:41Z"), ("observed_at_prec", None)])
def test_empty_piece_future_or_unknown_generation_does_not_gain_visibility(
    field: str,
    value: JsonInput,
) -> None:
    source = generated_empty_bar()
    pieces = source["boundary_segments"]
    assert isinstance(pieces, list)
    first = pieces[0]
    assert isinstance(first, dict)
    first[field] = value
    result = partial_parent_inputs(source)
    piece = result.inputs.boundaries[0]
    if field == "observed_at":
        assert piece.visible_at(result.inputs.assembled_at) is False
    else:
        assert piece.generated_empty is None
        assert piece.visible_at(result.inputs.assembled_at) is None


def test_parent_last_trade_receipt_cannot_authorize_an_unknown_legacy_zero_piece() -> None:
    source = with_boundary(
        {
            "start": "2026-10-19T12:00:00Z",
            "end": "2026-10-19T12:00:30Z",
            "complete": True,
            "buy_qty": "0",
            "sell_qty": "0",
            "sum_sq_qty": "0",
            "n_buy": 0,
            "n_sell": 0,
            "observed_at": "2026-10-19T12:00:20Z",
            "observed_at_prec": "COLLECTION_ONLY",
            "observed_at_prec_reason": None,
        }
    )
    source["received_ts"] = "2026-10-19T12:00:05Z"
    result = partial_parent_inputs(source)
    piece = result.inputs.boundaries[0]
    assert piece.generated_empty is None and piece.received_at.value is None
    assert piece.visible_at(result.inputs.assembled_at) is None


def test_actual_late_parent_restricts_even_an_earlier_generated_piece() -> None:
    source = generated_empty_bar()
    source["received_ts"] = "2026-10-19T12:00:41Z"
    result = partial_parent_inputs(source)
    assert result.inputs.boundaries[0].visible_at(result.inputs.assembled_at) is False


def test_unrequested_parent_money_and_future_piece_money_do_not_enter_the_calculation() -> None:
    source = generated_empty_bar()
    source["buy_qty"] = "future-parent-financial-canary"
    pieces = source["boundary_segments"]
    assert isinstance(pieces, list)
    for piece in pieces:
        assert isinstance(piece, dict)
        if piece["end"] != "2026-10-19T12:00:30Z":
            piece["buy_qty"] = "future-piece-financial-canary"
    result = partial_parent_inputs(source)
    assert result.inputs.minutes == ()
    assert result.inputs.boundaries[0].buy_qty == Decimal(0)


def test_declared_benchmark_scope_is_separate_and_cannot_impersonate_the_subject() -> None:
    benchmark: dict[str, JsonInput] = {
        "instrument_id": "BENCHMARK:spot:B",
        "token_id": "B",
        "venue": "BENCHMARK",
        "market_type": "SPOT",
    }
    foreign = dict(
        minute(), instrument_id=benchmark["instrument_id"], token_id="B", buy_qty="non-subject-canary"
    )

    def actual() -> DecodedSpotInputs:
        return decode_spot_inputs(
            token_id="T",
            start=AT,
            cutoff=AT + timedelta(minutes=1),
            assembled_at=Timestamp("2026-10-19T12:02:00Z", "COLLECTION_ONLY"),
            instruments=[INSTRUMENT, benchmark],
            flow_bars=[minute(), foreign],
            flow_parent_bars=[],
            flow_segments=[],
            tickers=[],
            policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=2),
            native_ticker_volume_field="volume_native",
        )

    result = actual()
    assert tuple(row.token_id for row in result.inputs.instruments) == ("T",)
    assert len(result.inputs.minutes) == 1
    foreign["token_id"] = "T"
    with pytest.raises(ValueError, match="benchmark source contradicts"):
        actual()


def test_unrequested_standalone_money_does_not_change_the_selected_scope_or_values() -> None:
    future = dict(minute(), minute_start="2026-10-19T12:03:00Z", buy_qty="future-native-canary")
    result = decoded(
        [minute(), future],
        pieces=[
            {
                "instrument_id": "VENUE:spot:T",
                "start": "2026-10-19T12:03:00Z",
                "end": "2026-10-19T12:03:30Z",
                "buy_qty": "future-piece-canary",
            }
        ],
    )
    assert len(result.inputs.minutes) == 1
    assert result.inputs.minutes[0].buy_qty == Decimal(3)
    assert result.inputs.boundaries == ()


@pytest.mark.parametrize("sigma", [2, 2.0, 0.05])
def test_captured_spot_policy_retains_numeric_threshold_and_discrete_count(sigma: int | float) -> None:
    policy = SpotPolicyConfiguration(significant_flow_sigma=sigma, min_spot_venues=2).calculation_policy()
    assert policy.significant_flow_sigma == Decimal(str(sigma))
    assert policy.min_spot_venues == 2


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"significant_flow_sigma": 2},
        {"min_spot_venues": 2},
        {"significant_flow_sigma": True, "min_spot_venues": 2},
        {"significant_flow_sigma": "2", "min_spot_venues": 2},
        {"significant_flow_sigma": float("inf"), "min_spot_venues": 2},
        {"significant_flow_sigma": 2, "min_spot_venues": True},
        {"significant_flow_sigma": 2, "min_spot_venues": 2.0},
    ],
)
def test_captured_spot_policy_refuses_absent_or_coerced_facts(values: dict[str, JsonInput]) -> None:
    with pytest.raises(ValidationError):
        SpotPolicyConfiguration.model_validate(values)

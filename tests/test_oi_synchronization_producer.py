from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_derivative_input_codec import decode_oi_inputs, decode_oi_synchronization_rules
from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
    OIReadSnapshot,
    OIRuleReadScope,
    OISelectionPolicy,
    UncapturedOISynchronizationRules,
)
from tank.analysis.oi_synchronization import (
    ObservedOISynchronizedSelection,
    OISynchronizationPolicy,
    UnavailableOISynchronizedSelection,
)
from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import JsonInput, iso, parse_iso_time

START = parse_iso_time("2026-10-16T11:00:00Z")
END = START + timedelta(hours=1)
POLICY = OISelectionPolicy(tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975))
SYNC_POLICY = OISynchronizationPolicy(eligible_venues=("binance", "okx", "bybit"))


def source(*, reported_usd: str | None) -> OIReadSnapshot:
    instruments: list[dict[str, JsonInput]] = [
        {
            "instrument_id": iid,
            "token_id": "T",
            "venue": "binance",
            "market_type": "PERP_LINEAR",
            "quote_asset": "USDT",
        }
        for iid in ("A", "B")
    ]
    rows: list[dict[str, JsonInput]] = [
        {
            "instrument_id": iid,
            "metric_key": "AUTHORED_OI",
            "ts": iso(at),
            "oi_native": quantity,
            "oi_usd": reported_usd,
            "received_ts": iso(END + timedelta(seconds=1)),
        }
        for iid, at, quantity in (
            ("A", START + timedelta(seconds=10), "10"),
            ("B", START + timedelta(seconds=20), "20"),
            ("A", END - timedelta(seconds=10), "12"),
            ("B", END - timedelta(seconds=15), "22"),
        )
    ]
    rows.append(
        {
            **rows[-2],
            "ts": iso(END - timedelta(seconds=1)),
            "oi_native": "100",
            "ts_prec": "BUCKET(60s)",
            "ts_bucket_anchor": "UNSPECIFIED",
        }
    )
    return decode_oi_inputs(token_id="T", instruments=instruments, observations=rows)


def request() -> OIWindowRequest:
    return OIWindowRequest(
        start=chosen_boundary(START, precision="EXACT"),
        cutoff=chosen_boundary(END, precision="EXACT"),
        assembled_at=collection_clock(END + timedelta(seconds=2)),
    )


def semantic_rule() -> dict[str, JsonInput]:
    return {
        "instrument_id": None,
        "venue": "binance",
        "market_type": "PERP_LINEAR",
        "metric_key": "AUTHORED_OI",
        "sidedness": "SINGLE_SIDED",
        "aggregation_class": "AUTHORED_SINGLE_SIDED",
    }


def rules(value: OIReadSnapshot, records: list[dict[str, JsonInput]]) -> CapturedOISynchronizationRules:
    return decode_oi_synchronization_rules(scope=OIRuleReadScope.from_snapshot(value), records=records)


def test_actual_common_grid_keeps_each_contract_and_selected_source_clock() -> None:
    value = source(reported_usd=None)
    declared = rules(value, [semantic_rule()])
    result = OpenInterestCalculator(value, POLICY).synchronize(
        request(), rules=declared, policy=SYNC_POLICY, window="W1H"
    )
    assert isinstance(result, ObservedOISynchronizedSelection)
    assert result.native_witness.start.native_total() == Decimal(30)
    assert result.native_witness.end.native_total() == Decimal(34)
    assert all(
        item.start.reported_usd.value is None and item.end.reported_usd.value is None
        for item in result.sources
    )
    assert result.native_witness.tolerance_s == 60
    assert result.native_witness.start.at.value == iso(START + timedelta(seconds=10))
    assert result.native_witness.end.at.value == iso(END - timedelta(seconds=10))
    assert result.native_witness.end.at.precision is None
    assert {point.instrument_id for point in result.native_witness.end.contributions} == {"A", "B"}
    selected = {point.instrument.instrument_id: point for point in result.sources}
    assert selected["A"].end.observed_at.value == iso(END - timedelta(seconds=10))
    assert selected["A"].end.evidence != value.observations[-1].evidence
    assert selected["B"].end.observed_at.value == iso(END - timedelta(seconds=15))
    assert selected["A"].end_rule.evidence == (declared.entries[0].reference,)


def test_uncaptured_semantics_cannot_become_single_sided_by_default() -> None:
    result = OpenInterestCalculator(source(reported_usd=None), POLICY).synchronize(
        request(),
        rules=UncapturedOISynchronizationRules(
            status="NOT_CAPTURED", token_id="T", reason="OI_SEMANTICS_NOT_CAPTURED"
        ),
        policy=SYNC_POLICY,
        window="W1H",
    )
    assert isinstance(result, UnavailableOISynchronizedSelection)
    assert result.reason == "OI_SEMANTICS_NOT_CAPTURED"


def test_captured_unknown_sidedness_is_not_promoted() -> None:
    value = source(reported_usd=None)
    declared = rules(value, [semantic_rule() | {"sidedness": None}])
    result = OpenInterestCalculator(value, POLICY).synchronize(
        request(), rules=declared, policy=SYNC_POLICY, window="W1H"
    )
    assert isinstance(result, UnavailableOISynchronizedSelection)
    assert result.reason == "NO_COMPARABLE_INSTRUMENTS"
    assert {item.reason for item in result.excluded} == {"SEMANTICS_UNAVAILABLE"}


def test_incompatible_classes_do_not_form_a_synchronized_total() -> None:
    value = source(reported_usd=None)
    declared = rules(
        value, [semantic_rule() | {"instrument_id": iid, "aggregation_class": iid} for iid in ("A", "B")]
    )
    result = OpenInterestCalculator(value, POLICY).synchronize(
        request(), rules=declared, policy=SYNC_POLICY, window="W1H"
    )
    assert isinstance(result, UnavailableOISynchronizedSelection)
    assert result.reason == "INCOMPATIBLE_AGGREGATION_CLASSES"
    assert result.eligible_instrument_ids == ("A", "B")


def test_equal_rule_projection_does_not_overwrite_distinct_source_evidence() -> None:
    value = source(reported_usd=None)
    declared = rules(
        value, [semantic_rule() | {"source_basis_date": date} for date in ("2026-01-01", "2026-10-01")]
    )
    assert declared.entries[0].reference.projection_sha256 == declared.entries[1].reference.projection_sha256
    assert (
        declared.entries[0].reference.source_evidence_sha256
        != declared.entries[1].reference.source_evidence_sha256
    )
    result = OpenInterestCalculator(value, POLICY).synchronize(
        request(), rules=declared, policy=SYNC_POLICY, window="W1H"
    )
    assert isinstance(result, UnavailableOISynchronizedSelection)
    assert result.reason == "NO_COMPARABLE_INSTRUMENTS"


def test_specific_unknown_rule_cannot_fall_through_to_favorable_global_rule() -> None:
    value = source(reported_usd=None)
    declared = rules(
        value, [semantic_rule(), semantic_rule() | {"instrument_id": "B", "sidedness": "UNKNOWN"}]
    )
    result = OpenInterestCalculator(value, POLICY).synchronize(
        request(), rules=declared, policy=SYNC_POLICY, window="W1H"
    )
    assert isinstance(result, ObservedOISynchronizedSelection)
    assert tuple(item.instrument.instrument_id for item in result.sources) == ("A",)
    assert result.excluded[0].instrument_id == "B" and result.excluded[0].reason == "SEMANTICS_UNAVAILABLE"


def test_rule_scope_and_original_projection_are_revalidated_before_calculation() -> None:
    value = source(reported_usd=None)
    with pytest.raises(ValidationError, match="instrument scope"):
        rules(value, [semantic_rule() | {"instrument_id": "A", "venue": "other"}])
    declared = rules(value, [semantic_rule() | {"sidedness": "UNKNOWN"}])
    entry = declared.entries[0]
    changed = entry.model_copy(update={"rule": entry.rule.model_copy(update={"sidedness": "SINGLE_SIDED"})})
    with pytest.raises(ValidationError, match="disagrees"):
        OpenInterestCalculator(value, POLICY).synchronize(
            request(),
            rules=declared.model_copy(update={"entries": (changed,)}),
            policy=SYNC_POLICY,
            window="W1H",
        )


def test_caller_mutation_cannot_change_captured_rule_or_qualified_scope() -> None:
    value = source(reported_usd=None)
    original = semantic_rule()
    declared = rules(value, [original])
    original["sidedness"] = "UNKNOWN"
    result = OpenInterestCalculator(value, POLICY).synchronize(
        request(), rules=declared, policy=SYNC_POLICY, window="W1H"
    )
    assert isinstance(result, ObservedOISynchronizedSelection)
    assert declared.scope.metric_keys == ("AUTHORED_OI",)


@pytest.mark.parametrize("reported,expected", [("20", Decimal(20)), ("NaN", None), ("-1", Decimal(-1))])
def test_raw_usd_quality_does_not_erase_known_native_synchronization(
    reported: str, expected: Decimal | None
) -> None:
    value = source(reported_usd=reported)
    result = OpenInterestCalculator(value, POLICY).synchronize(
        request(), rules=rules(value, [semantic_rule()]), policy=SYNC_POLICY, window="W1H"
    )
    assert isinstance(result, ObservedOISynchronizedSelection)
    assert result.native_witness.start.native_total() == Decimal(30)
    assert result.native_witness.end.native_total() == Decimal(34)
    assert result.sources[0].start.reported_usd.value == expected
    assert "oi_usd" not in result.native_witness.start.contributions[0].model_dump(mode="json")

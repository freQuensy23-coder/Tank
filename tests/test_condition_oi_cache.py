from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import JsonValue
from truth_support import fixture_truth_config

from tank.analysis import oi_composite_history as composite_math
from tank.analysis import truth
from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_derivative_inputs import OISelectionPolicy
from tank.analysis.condition_graph_contracts import (
    ConditionSynchronizedOpenInterest,
    ConditionTopCacheKey,
    ConditionWindow,
)
from tank.analysis.condition_oi_codec import OIConditionLookup
from tank.analysis.oi_composite_history import OIHistoricalCompositePair
from tank.analysis.open_interest import OINativeHistoryBaseline, OIWindowRequest, OpenInterestCalculator
from tank.analysis.statistics_contracts import FloatRange
from tank.contracts.json import JsonDocument
from tank.time_precision import collection_clock
from tank.util import iso, parse_iso_time


def packet(*, future_receipt: bool = False, missing_receipt: bool = False) -> dict[str, JsonValue]:
    end = parse_iso_time("2026-10-16T12:00:00Z")
    start = end - timedelta(hours=1)
    rows: list[JsonValue] = [
        {
            "instrument_id": "A",
            "metric_key": "OI",
            "ts": iso(start - timedelta(hours=hour)),
            "received_ts": iso(end),
            "oi_native": "3",
            "oi_usd": "1",
        }
        for hour in range(1, 241)
    ]
    rows.extend(
        {
            "instrument_id": "A",
            "metric_key": "OI",
            "ts": iso(at),
            "received_ts": iso(end),
            "oi_native": native,
            "oi_usd": "1",
        }
        for at, native in ((start, "3"), (end, "4"))
    )
    assert isinstance(rows[-1], dict)
    if future_receipt:
        rows[-1]["received_ts"] = iso(end + timedelta(seconds=2))
    if missing_receipt:
        del rows[-1]["received_ts"]
    return {
        "token_id": "T",
        "cutoff": iso(end),
        **collection_clock(end + timedelta(seconds=1)).fields("assembled_at"),
        "instruments": [
            {"instrument_id": "A", "token_id": "T", "venue": "binance", "market_type": "PERP_LINEAR"}
        ],
        "oi": rows,
        "semantics": [
            {
                "instrument_id": None,
                "venue": "binance",
                "market_type": "PERP_LINEAR",
                "metric_key": "OI",
                "sidedness": "SINGLE_SIDED",
                "aggregation_class": "SINGLE",
                "cadence_s": 300,
            }
        ],
        "domain_dependency_fingerprints": {"oi": "same-authored-opaque-source-head"},
    }


def evaluate(
    value: dict[str, JsonValue],
    cache: dict[ConditionTopCacheKey, OIConditionLookup],
    baseline: BaselineCaches,
) -> tuple[bool, bool]:
    wire = JsonDocument.validate_value(
        truth.compute_condition_values(
            value,
            [{"metric": "deriv.native_oi_vs_normal", "window": "W1H"}],
            config=fixture_truth_config(),
            cache=cache,
            baseline_cache=baseline,
        )
    )
    assert isinstance(wire, dict)
    evaluation = wire["_condition_evaluation"]
    assert isinstance(evaluation, dict)
    proofs = evaluation["proofs"]
    assert isinstance(proofs, list) and len(proofs) == 1
    proof = proofs[0]
    assert isinstance(proof, dict)
    reused, available = proof["reused"], proof["available"]
    assert isinstance(reused, bool) and isinstance(available, bool)
    return reused, available


def only_fact(cache: dict[ConditionTopCacheKey, OIConditionLookup]) -> OIConditionLookup:
    return next(reversed(cache.values()))


def test_actual_oi_cache_uses_exact_recorded_decomposition_and_fresh_context() -> None:
    value = packet()
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    assert evaluate(value, cache, baseline) == (False, True)
    selected = only_fact(cache).result.synchronized
    assert isinstance(selected, ConditionSynchronizedOpenInterest)
    assert selected.values.oi_native_start == 3 and selected.values.oi_native_end == 4
    assert selected.values.oi_usd_start == selected.values.oi_usd_end == Decimal(1)
    assert selected.values.oi_class == "MIXED"
    later = deepcopy(value)
    later.update(collection_clock(parse_iso_time("2026-10-16T12:00:05Z")).fields("assembled_at"))
    assert evaluate(later, cache, baseline) == (True, True)
    assert len(cache) == 1


def test_current_only_correction_reuses_actual_historical_population_but_history_change_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = OpenInterestCalculator.native_baseline
    original_range = composite_math._normal_range
    calls: list[ConditionWindow] = []
    range_calls: list[Literal["NATIVE", "RECORDED_USD"]] = []

    def counted_range(
        pairs: tuple[OIHistoricalCompositePair, ...],
        *,
        measure: Literal["NATIVE", "RECORDED_USD"],
        policy: OISelectionPolicy,
        source_times: tuple[datetime, ...],
        duration: timedelta,
        ambiguous: bool,
    ) -> FloatRange:
        range_calls.append(measure)
        return original_range(
            pairs,
            measure=measure,
            policy=policy,
            source_times=source_times,
            duration=duration,
            ambiguous=ambiguous,
        )

    def counted(
        calculator: OpenInterestCalculator, request: OIWindowRequest, *, window: ConditionWindow
    ) -> tuple[OINativeHistoryBaseline, ...]:
        calls.append(window)
        return original(calculator, request, window=window)

    monkeypatch.setattr(OpenInterestCalculator, "native_baseline", counted)
    monkeypatch.setattr(composite_math, "_normal_range", counted_range)
    value = packet()
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    assert evaluate(value, cache, baseline) == (False, True)
    assert calls == ["W1H"]
    assert "NATIVE" in range_calls and "RECORDED_USD" in range_calls
    range_count = len(range_calls)
    original_identity = only_fact(cache).facts.source_identity
    rows = value["oi"]
    assert isinstance(rows, list) and isinstance(rows[-1], dict)
    rows[-1]["oi_native"] = "5"
    assert evaluate(value, cache, baseline) == (False, True)
    assert calls == ["W1H"]
    assert len(range_calls) == range_count
    corrected = only_fact(cache).result.synchronized
    assert isinstance(corrected, ConditionSynchronizedOpenInterest)
    assert corrected.values.oi_native_end == 5
    assert only_fact(cache).facts.source_identity != original_identity
    assert isinstance(rows[0], dict)
    rows[0]["oi_native"] = "6"
    evaluate(value, cache, baseline)
    assert calls == ["W1H", "W1H"]
    assert len(range_calls) > range_count


def test_same_opaque_head_late_receipt_changes_eligible_facts() -> None:
    value = packet(future_receipt=True)
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    assert evaluate(value, cache, baseline) == (False, False)
    later = deepcopy(value)
    later.update(collection_clock(parse_iso_time("2026-10-16T12:00:03Z")).fields("assembled_at"))
    assert evaluate(later, cache, baseline) == (False, True)
    selected = only_fact(cache).result.synchronized
    assert isinstance(selected, ConditionSynchronizedOpenInterest)
    assert selected.values.oi_native_end == 4
    assert evaluate(later, cache, baseline) == (True, True)


def test_missing_receipt_never_becomes_available_from_assembly_or_known_numbers() -> None:
    value = packet(missing_receipt=True)
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    assert evaluate(value, cache, baseline) == (False, False)
    value.update(collection_clock(parse_iso_time("2026-10-16T12:00:03Z")).fields("assembled_at"))
    assert evaluate(value, cache, baseline) == (True, False)


def test_caller_mutation_cannot_change_already_cached_owned_facts() -> None:
    value = packet()
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    evaluate(value, cache, baseline)
    admitted = only_fact(cache)
    rows = value["oi"]
    assert isinstance(rows, list) and isinstance(rows[-1], dict)
    rows[-1]["oi_native"] = "9999"
    old = admitted.result.synchronized
    assert isinstance(old, ConditionSynchronizedOpenInterest)
    assert old.values.oi_native_end == 4
    assert evaluate(value, cache, baseline) == (False, True)
    corrected = only_fact(cache).result.synchronized
    assert isinstance(corrected, ConditionSynchronizedOpenInterest)
    assert corrected.values.oi_native_end == 9999


@pytest.mark.parametrize("collection", ["instruments", "oi"])
def test_requested_oi_read_collections_cannot_be_omitted(collection: str) -> None:
    value = packet()
    rows = value["oi"]
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
        row.update(token_id="T", venue="binance", market_type="PERP_LINEAR")
    del value[collection]
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    with pytest.raises(ValueError, match="Requested OI snapshot requires captured"):
        evaluate(value, cache, BaselineCaches.with_capacity(128))
    assert cache == {}


def test_explicit_empty_oi_read_retains_unavailability_without_zero_endpoints() -> None:
    value = packet()
    value["oi"] = []
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    assert evaluate(value, cache, BaselineCaches.with_capacity(128)) == (False, False)
    result = only_fact(cache).result
    assert len(result.per_venue) == 1
    assert result.per_venue[0].open_interest.status == "UNAVAILABLE"
    assert result.synchronized.status == "UNAVAILABLE"


@pytest.mark.parametrize("excluded", ["FUTURE_RECEIPT", "UNRESOLVED_EDGE", "UNREPORTED_RECEIPT"])
def test_freshness_uses_exact_selected_oi_evidence_with_excluded_duplicate(excluded: str) -> None:
    value = packet()
    rows = value["oi"]
    assert isinstance(rows, list) and isinstance(rows[-1], dict)
    duplicate = deepcopy(rows[-1])
    duplicate.update(metric_key="OTHER_OI", oi_usd="999")
    if excluded == "FUTURE_RECEIPT":
        duplicate["received_ts"] = "2026-10-16T12:00:03Z"
    elif excluded == "UNRESOLVED_EDGE":
        duplicate.update(ts_prec="BUCKET(60s)", ts_bucket_anchor="UNSPECIFIED")
    else:
        del duplicate["received_ts"]
    rows.append(duplicate)
    cache: dict[ConditionTopCacheKey, OIConditionLookup] = {}
    assert evaluate(value, cache, BaselineCaches.with_capacity(128)) == (False, True)
    owned = only_fact(cache)
    closing = owned.closing_observations()
    assert len(closing) == 1 and closing[0].metric_key == "OI"
    assert closing[0].reported_usd.value == 1
    assert closing[0].evidence.row_index != len(rows) - 1
    selected = owned.result.synchronized
    assert isinstance(selected, ConditionSynchronizedOpenInterest)
    assert selected.values.oi_usd_end == 1

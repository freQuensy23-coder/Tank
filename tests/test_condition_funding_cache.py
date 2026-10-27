"""Actual selective funding uses typed facts, historical reuse and selected-witness freshness."""

from copy import deepcopy

import pytest
from funding_history_fixtures import rows
from funding_input_fixtures import funding_scope_fixture
from pydantic import JsonValue
from truth_support import fixture_truth_config

from tank.analysis import funding_history_cache, truth
from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.analysis.condition_funding_codec import FundingConditionLookup
from tank.analysis.condition_graph_contracts import ConditionTopCacheKey
from tank.analysis.funding_history import (
    FundingHistoricalPopulation,
    FundingHistoryPolicy,
    funding_historical_population,
)
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.contracts.json import JsonDocument


def packet() -> dict[str, JsonValue]:
    captured_rows: list[JsonValue] = list(rows())
    return {
        "token_id": "T",
        "cutoff": "2026-10-16T12:00:00Z",
        "cutoff_prec": "EXACT",
        "assembled_at": "2026-10-16T12:05:00Z",
        "assembled_at_prec": "COLLECTION_ONLY",
        "instruments": [funding_scope_fixture()],
        "funding": captured_rows,
        "capture_metadata": {"collections": {"funding": {"cadence_s": 300}}},
        "domain_dependency_fingerprints": {"funding": "same-authored-source-head"},
    }


def evaluate(
    snapshot: dict[str, JsonValue],
    cache: dict[ConditionTopCacheKey, FundingConditionLookup],
    baseline: BaselineCaches,
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    value = JsonDocument.validate_value(
        truth.compute_condition_values(
            snapshot,
            [{"metric": "deriv.funding_vs_normal", "window": "W1H"}],
            config=fixture_truth_config(),
            cache=cache,
            baseline_cache=baseline,
        )
    )
    assert isinstance(value, dict)
    evaluation = value["_condition_evaluation"]
    assert isinstance(evaluation, dict)
    proofs = evaluation["proofs"]
    assert isinstance(proofs, list) and len(proofs) == 1 and isinstance(proofs[0], dict)
    derivatives = value["derivatives"]
    assert isinstance(derivatives, dict)
    windows = derivatives["windows"]
    assert isinstance(windows, dict)
    window = windows["W1H"]
    assert isinstance(window, dict)
    venues = window["per_venue"]
    assert isinstance(venues, list) and len(venues) == 1 and isinstance(venues[0], dict)
    return venues[0], proofs[0]


def test_live_assembly_reuses_facts_with_fresh_proof_and_original_source_clock() -> None:
    snapshot = packet()
    cache: dict[ConditionTopCacheKey, FundingConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    first, before = evaluate(snapshot, cache, baseline)
    snapshot["assembled_at"] = "2026-10-16T12:06:00Z"
    second, after = evaluate(snapshot, cache, baseline)
    assert before["available"] is after["available"] is True
    assert before["reused"] is False and after["reused"] is True
    assert before["assembled_at"] == "2026-10-16T12:05:00Z"
    assert after["assembled_at"] == "2026-10-16T12:06:00Z"
    assert first == second
    assert second["funding_estimate_observed_at"] == "2026-10-16T12:00:00Z"
    assert second["funding_vs_normal"] == "BELOW"


def test_current_correction_reuses_historical_calculator_but_historical_correction_invalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    actual = funding_historical_population

    def observe(
        calculator: FundingCalculator,
        request: FundingWindowRequest,
        *,
        instrument: DerivativeInstrument,
        policy: FundingHistoryPolicy,
    ) -> FundingHistoricalPopulation:
        result = actual(calculator, request, instrument=instrument, policy=policy)
        calls.append(result.identity())
        return result

    monkeypatch.setattr(funding_history_cache, "funding_historical_population", observe)
    snapshot = packet()
    cache: dict[ConditionTopCacheKey, FundingConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    first, before = evaluate(snapshot, cache, baseline)
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict) and isinstance(captured[0], dict)
    captured[-1]["rate_8h_eq"] = "0"
    second, corrected = evaluate(snapshot, cache, baseline)
    assert before["reused"] is corrected["reused"] is False
    assert second["funding_8h_eq"] == "0"
    assert first["funding_normal_range_8h"] == second["funding_normal_range_8h"] == ["1.05", "2.95"]
    assert len(calls) == 1
    captured[0]["rate_8h_eq"] = "2"
    third, historical = evaluate(snapshot, cache, baseline)
    assert historical["reused"] is False
    assert third["funding_normal_range_8h"] == ["2.025", "2.975"]
    assert len(calls) == 2


def test_invalid_current_does_not_refresh_historical_settled_observation() -> None:
    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1].update(rate=None, interval_h="-4", rate_8h_eq=None)
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert row["funding_8h_kind"] == "SETTLED"
    assert row["funding_settled_last_observed_at"] == "2026-10-09T08:00:00Z"
    assert row["funding_vs_normal"] is None
    assert proof["available"] is False
    observations = proof["observations"]
    assert isinstance(observations, list) and len(observations) == 1 and isinstance(observations[0], dict)
    assert observations[0]["observed_at"] == "2026-10-09T08:00:00Z"
    assert observations[0]["cadence_s"] == 300


def test_same_opaque_head_later_receipt_can_admit_the_actual_current_witness() -> None:
    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1]["received_ts"] = "2026-10-16T13:00:00Z"
    cache: dict[ConditionTopCacheKey, FundingConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    first, early = evaluate(snapshot, cache, baseline)
    snapshot["assembled_at"] = "2026-10-16T14:00:00Z"
    second, late = evaluate(snapshot, cache, baseline)
    assert early["available"] is False
    assert late["available"] is True and late["reused"] is False
    assert first["funding_8h_kind"] == "SETTLED"
    assert second["funding_8h_kind"] == "CURRENT_ESTIMATE"
    assert second["funding_estimate_observed_at"] == "2026-10-16T12:00:00Z"


@pytest.mark.parametrize("missing", ["funding", "instruments"])
def test_requested_collections_cannot_be_omitted_even_after_a_cache_hit(missing: str) -> None:
    snapshot = packet()
    cache: dict[ConditionTopCacheKey, FundingConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    evaluate(snapshot, cache, baseline)
    broken = deepcopy(snapshot)
    del broken[missing]
    with pytest.raises(ValueError, match="Requested funding snapshot requires captured"):
        evaluate(broken, cache, baseline)


def test_exact_selected_metric_identity_controls_capture_cadence() -> None:
    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1].update(metric_key="fixture.funding.actual", as_of="2026-10-16T11:58:00Z")
    snapshot["capture_metadata"] = {
        "collections": {"funding": {"cadence_s": 300, "cadence_s_by_metric": {"fixture.funding.actual": 60}}}
    }
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert row["funding_vs_normal"] is None
    assert proof["available"] is False
    observations = proof["observations"]
    assert isinstance(observations, list) and len(observations) == 1 and isinstance(observations[0], dict)
    assert observations[0]["cadence_s"] == 60
    assert observations[0]["cadence_source"] == "CAPTURE_METADATA_BY_METRIC"
    assert observations[0]["observed_at"] == "2026-10-16T11:58:00Z"

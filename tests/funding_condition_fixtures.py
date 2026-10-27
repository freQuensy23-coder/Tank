"""Declared funding read packet and real selective wire consumer for focused controls."""

from funding_history_fixtures import rows
from funding_input_fixtures import funding_scope_fixture
from pydantic import JsonValue
from truth_support import fixture_truth_config

from tank.analysis import truth
from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_funding_codec import FundingConditionLookup
from tank.analysis.condition_graph_contracts import ConditionTopCacheKey, ConditionWindow
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


def evaluate_window(
    snapshot: dict[str, JsonValue],
    cache: dict[ConditionTopCacheKey, FundingConditionLookup],
    baseline: BaselineCaches,
    *,
    window: ConditionWindow,
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    value = JsonDocument.validate_value(
        truth.compute_condition_values(
            snapshot,
            [{"metric": "deriv.funding_vs_normal", "window": window}],
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
    selected = windows[window]
    assert isinstance(selected, dict)
    venues = selected["per_venue"]
    assert isinstance(venues, list) and len(venues) == 1 and isinstance(venues[0], dict)
    return venues[0], proofs[0]


def evaluate(
    snapshot: dict[str, JsonValue],
    cache: dict[ConditionTopCacheKey, FundingConditionLookup],
    baseline: BaselineCaches,
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    return evaluate_window(snapshot, cache, baseline, window="W1H")

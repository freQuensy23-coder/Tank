"""Cache comparison identity preserves original source clocks and fresh assembly."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import JsonValue

from tank.analysis.condition_visibility import (
    ConditionVisibilityEvaluation,
    InvalidVisibilityClock,
    VisibilityDomain,
    condition_visibility,
)
from tank.time_precision import Timestamp
from tank.util import iso, sha256

AT = datetime(2026, 10, 16, 12, tzinfo=UTC)


def evaluate(
    consumed: dict[str, JsonValue], *, domain: VisibilityDomain = "state", seconds: int = 0
) -> ConditionVisibilityEvaluation:
    return condition_visibility(
        domain=domain,
        token_id="T",
        assembled_at=Timestamp(iso(AT + timedelta(seconds=seconds)), "COLLECTION_ONLY"),
        content_sha256=sha256(consumed),
        consumed=consumed,
    )


def test_unchanged_visibility_reuses_identity_without_relabeling_receipt_or_assembly() -> None:
    receipt = iso(AT - timedelta(seconds=1))
    inputs: dict[str, JsonValue] = {
        "flow_bars": [{"received_ts": receipt, "received_ts_prec": "COLLECTION_ONLY"}]
    }
    first = evaluate(inputs)
    later = evaluate(inputs, seconds=10)
    assert first.projection.fingerprint() == later.projection.fingerprint()
    assert first.assembled_at != later.assembled_at
    assert first.projection.comparisons[0].source_clock == Timestamp(receipt, "COLLECTION_ONLY")
    assert later.projection.comparisons[0].source_clock == first.projection.comparisons[0].source_clock


@pytest.mark.parametrize(
    "inputs",
    [
        {"oi": [{"received_ts": iso(AT + timedelta(seconds=1))}]},
        {"funding_confirmations": [{"received_ts": iso(AT + timedelta(seconds=1))}]},
        {"flow_segments": [{"observed_at": iso(AT + timedelta(seconds=1))}]},
        {"flow_parent_bars": [{"observed_at": iso(AT + timedelta(seconds=1))}]},
        {"tickers": [{"observed_at": iso(AT + timedelta(seconds=1))}]},
        {"flow_parent_bars": [{"boundary_segments": [{"received_ts": iso(AT + timedelta(seconds=1))}]}]},
        {"reference_points": [{"observed_at": iso(AT + timedelta(seconds=1))}]},
        {"reference_points": [{"last_trade": {"received_ts": iso(AT + timedelta(seconds=1))}}]},
        {"flow_bars": [{"last_trade": {"received_ts": iso(AT + timedelta(seconds=1))}}]},
        {
            "flow_bars": [
                {"reference_points": [{"last_trade": {"received_ts": iso(AT + timedelta(seconds=1))}}]}
            ]
        },
        {"books": [{"independent_rest": {"received_ts": iso(AT + timedelta(seconds=1))}}]},
    ],
)
def test_declared_consumed_clock_gates_change_identity_when_receipt_becomes_eligible(
    inputs: dict[str, JsonValue],
) -> None:
    first = evaluate(inputs)
    later = evaluate(inputs, seconds=1)
    assert first.projection.content_sha256 == later.projection.content_sha256
    assert first.projection.fingerprint() != later.projection.fingerprint()
    assert any(row.relation == "AFTER_ASSEMBLY" for row in first.projection.comparisons)
    assert any(row.relation == "AT_OR_BEFORE_ASSEMBLY" for row in later.projection.comparisons)


@pytest.mark.parametrize("domain", ["depth", "dc", "close"])
def test_independent_rest_clock_is_not_guessed_for_unrelated_builders(domain: VisibilityDomain) -> None:
    inputs: dict[str, JsonValue] = {
        "books": [{"independent_rest": {"received_ts": iso(AT + timedelta(seconds=1))}}]
    }
    assert evaluate(inputs, domain=domain).projection == evaluate(inputs, domain=domain, seconds=2).projection


def test_arbitrary_nested_clock_names_do_not_create_a_visibility_gate() -> None:
    inputs: dict[str, JsonValue] = {
        "books": [{"diagnostic_extension": {"received_ts": iso(AT + timedelta(seconds=1))}}]
    }
    assert evaluate(inputs).projection == evaluate(inputs, seconds=2).projection


def test_corrected_content_changes_identity_even_with_unchanged_visibility() -> None:
    first: dict[str, JsonValue] = {"flow_bars": [{"buy_qty": "1", "received_ts": iso(AT)}]}
    corrected: dict[str, JsonValue] = {"flow_bars": [{"buy_qty": "2", "received_ts": iso(AT)}]}
    a, b = evaluate(first), evaluate(corrected)
    assert a.projection.comparisons == b.projection.comparisons
    assert a.projection.fingerprint() != b.projection.fingerprint()


def test_unknown_and_invalid_clocks_remain_explicit_without_an_invented_receipt() -> None:
    unknown = evaluate({"flow_bars": [{}]})
    assert all(row.relation == "SOURCE_CLOCK_UNCAPTURED" for row in unknown.projection.comparisons)
    invalid = evaluate({"flow_bars": [{"received_ts": "private-invalid-clock-canary"}]})
    receipt = invalid.projection.comparisons[0]
    assert receipt.relation == "INVALID_SOURCE_CLOCK"
    assert isinstance(receipt.source_clock, InvalidVisibilityClock)
    assert "private-invalid-clock-canary" not in invalid.projection.model_dump_json()


def test_unknown_assembly_is_distinct_from_a_known_later_context() -> None:
    inputs: dict[str, JsonValue] = {"flow_bars": [{"received_ts": iso(AT)}]}
    unknown = condition_visibility(
        domain="spot",
        token_id="T",
        assembled_at=Timestamp(None, None, "ASSEMBLY_NOT_CAPTURED"),
        content_sha256=sha256(inputs),
        consumed=inputs,
    )
    known = evaluate(inputs, domain="spot")
    assert unknown.projection.assembly_knowledge == "UNCAPTURED"
    assert unknown.projection.fingerprint() != known.projection.fingerprint()

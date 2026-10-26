"""Consumed identities retain their prior exact representation and actual compact facts."""

from array import array
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tank.analysis.condition_fingerprints import ConditionFingerprintInput, condition_input_fingerprints


def identity_snapshot() -> dict[str, ConditionFingerprintInput]:
    oi: list[ConditionFingerprintInput] = [
        {
            "instrument_id": "A:perp:T",
            "ts": datetime(2026, 10, 1, 2, tzinfo=UTC),
            "oi_native": Decimal("0.00001"),
            "received_ts": "2026-10-01T02:00:00Z",
            "received_ts_prec": "COLLECTION_ONLY",
        }
    ]
    return {
        "token_id": "T",
        "cutoff": "2026-10-01T02:00:00Z",
        "cutoff_prec": "EXACT",
        "assembled_at": "2026-10-01T02:00:01Z",
        "assembled_at_prec": "COLLECTION_ONLY",
        "anchor": {"ts": "2026-10-01T01:00:00Z", "precision": "EXACT"},
        "domain_dependency_fingerprints": {"relative": "opaque-provider-head"},
        "stat_price_series": {
            "A:spot:T": {
                "times_us": array("q", [1790816400000000, 1790820000000000]),
                "values": array("d", [1.25, 1.5]),
                "complete": bytearray([1, 1]),
                "instrument_id": "A:spot:T",
                "venue": "A",
                "market_type": "SPOT",
                "token_id": "T",
                "interval_s": 3600,
                "coverage_count": 2,
                "append_only": True,
            }
        },
        "oi": oi,
    }


def test_exact_pre_extraction_compact_buffer_and_source_clock_identities() -> None:
    assert condition_input_fingerprints(
        identity_snapshot(), ("stat_price_series", "oi", "anchor"), "relative"
    ) == (
        "opaque-provider-head",
        "7f8993f3bac69614f8bb2faebeff82379f30754bb4779e319d85913f2b76fd03",
        "85c88a2272155a1483552e2557252353feb53cd852fb45f2db66cec33e677a8b",
    )


def test_compact_optimization_hints_are_independent_of_consumed_values() -> None:
    snapshot = identity_snapshot()
    original = condition_input_fingerprints(snapshot, ("stat_price_series",), "relative")
    series = snapshot["stat_price_series"]
    assert isinstance(series, Mapping)
    original_row = series["A:spot:T"]
    assert isinstance(original_row, Mapping)
    row: dict[str, ConditionFingerprintInput] = {key: value for key, value in original_row.items()}
    snapshot["stat_price_series"] = {"A:spot:T": row}
    row["append_only"] = False
    row["previous_dependency_fingerprint"] = "different-optimization-hint"
    row["changed_from_us"] = 1790816400000000
    assert condition_input_fingerprints(snapshot, ("stat_price_series",), "relative") == original
    row["values"] = array("d", [1.25, 1.75])
    corrected = condition_input_fingerprints(snapshot, ("stat_price_series",), "relative")
    assert corrected[0] == original[0] and corrected[1] != original[1]


def test_fresh_assembly_keeps_pure_identity_until_receipt_eligibility_changes() -> None:
    snapshot = identity_snapshot()
    fields = ("oi",)
    eligible = condition_input_fingerprints(snapshot, fields, "oi")
    snapshot["assembled_at"] = "2026-10-01T02:00:02Z"
    assert condition_input_fingerprints(snapshot, fields, "oi") == eligible
    snapshot["assembled_at"] = "2026-10-01T01:59:59Z"
    earlier = condition_input_fingerprints(snapshot, fields, "oi")
    assert earlier[:2] == eligible[:2] and earlier[2] != eligible[2]
    with pytest.raises(ValueError, match="fresh evaluation context"):
        condition_input_fingerprints(snapshot, ("assembled_at",), "oi")

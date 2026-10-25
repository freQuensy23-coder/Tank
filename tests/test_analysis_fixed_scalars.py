"""Boolean assertions and integer cache versions retain their actual JSON types."""

import json
from typing import Literal

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.calibration_contracts import BestMeasuredRouting, RoutingMetrics
from tank.contracts.json import decode_json
from tank.fixture_reference_code import CapturedProjectionCodeInventory, UnresolvedProjectionCode
from tank.fixture_reference_sources import RetainedInputOrigin
from tank.market.cache_contracts import SavedKlineDayCounts
from tank.market.capture_contracts import CaptureWaitOutcome
from tank.time_precision import collection_clock


def routing() -> BestMeasuredRouting:
    return BestMeasuredRouting(
        cutoff=0.5,
        metrics=RoutingMetrics(
            cutoff=0.5,
            true_positive=1,
            false_positive=0,
            false_negative=0,
            precision=1.0,
            recall=1.0,
            errors=[],
        ),
        automatic_apply=False,
    )


def code_inventory() -> CapturedProjectionCodeInventory:
    return CapturedProjectionCodeInventory(
        fixed_source_path="src/tank/fixture_reference.py",
        verified_input_packets=1,
        excluded_fixture="F-02_MANIFEST_MISMATCH",
        archived_code_executed=False,
        entries=[
            UnresolvedProjectionCode(
                projection_module_sha256="a" * 64,
                source_path="src/tank/fixture_reference.py",
                status="UNRESOLVED",
                origin_claim="NO_MATCHING_FIXED_PATH_GIT_BLOB_LOCATED",
                origins=[
                    RetainedInputOrigin(
                        fixture_id="F-01",
                        packet_path="authored/input.json.zst",
                        compressed_sha256="b" * 64,
                        field_path="$.projection_module_sha256",
                    )
                ],
            )
        ],
    )


def wait_outcome() -> CaptureWaitOutcome:
    return CaptureWaitOutcome(
        status="INCOMPLETE",
        backfill_id=None,
        reason="BOUNDED_PREREQUISITE_WAIT_EXPIRED",
        bounded_wait_s=0.0,
        observed_at=collection_clock("2026-10-25T00:00:00Z"),
    )


@pytest.mark.parametrize("value", [0, 0.0, "false", None, True])
@pytest.mark.parametrize("target", ["routing", "code"])
def test_false_assertions_refuse_numeric_and_other_nonfalse_wire_values(
    value: JsonValue, target: Literal["routing", "code"]
) -> None:
    if target == "routing":
        wire = decode_json(routing().model_dump_json())
        assert isinstance(wire, dict)
        wire["automatic_apply"] = value
        with pytest.raises(ValidationError):
            BestMeasuredRouting.model_validate_json(json.dumps(wire))
    else:
        wire = decode_json(code_inventory().model_dump_json())
        assert isinstance(wire, dict)
        wire["archived_code_executed"] = value
        with pytest.raises(ValidationError):
            CapturedProjectionCodeInventory.model_validate_json(json.dumps(wire))


@pytest.mark.parametrize("value", [1, 1.0, "true", None, False])
def test_capture_exclusion_assertion_requires_actual_true(value: JsonValue) -> None:
    wire = decode_json(wait_outcome().model_dump_json())
    assert isinstance(wire, dict)
    wire["incomplete_windows_excluded"] = value
    with pytest.raises(ValidationError):
        CaptureWaitOutcome.model_validate_json(json.dumps(wire))


@pytest.mark.parametrize("value", [True, 1.0, "1", 0])
def test_cache_version_is_an_integer_identifier(value: JsonValue) -> None:
    wire = {"version": value, "saved_at": "2026-10-25T00:00:00Z", "counts": []}
    with pytest.raises(ValidationError):
        SavedKlineDayCounts.model_validate_json(json.dumps(wire))


def test_valid_output_flags_and_cache_version_roundtrip_without_type_changes() -> None:
    assert routing().model_dump(mode="json")["automatic_apply"] is False
    assert code_inventory().model_dump(mode="json")["archived_code_executed"] is False
    result = wait_outcome()
    assert result.incomplete_windows_excluded is True
    assert result.to_wire()["incomplete_windows_excluded"] is True
    assert CaptureWaitOutcome.model_validate_json(result.model_dump_json()) == result
    cache = SavedKlineDayCounts(version=1, saved_at="2026-10-25T00:00:00Z", counts=[])
    restored = SavedKlineDayCounts.model_validate_json(cache.model_dump_json())
    assert type(restored.version) is int and restored == cache

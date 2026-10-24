"""Captured tool outcomes preserve error identity and reject contradictory durable metadata."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.tool_contracts import CapturedToolResult, NativeText, NativeToolResult


@pytest.mark.parametrize("status", ["DONE", "ERROR", "REFUSED"])
def test_capture_error_flag_is_derived_from_completed_status(status: str) -> None:
    capture = CapturedToolResult.from_record({"status": status, "result": {"captured": True}})
    assert capture.is_error is (status != "DONE")


@pytest.mark.parametrize("flag", [False, None, "true", 1])
def test_error_capture_refuses_contradictory_or_untyped_flag(flag: JsonValue) -> None:
    with pytest.raises(ValueError, match="inconsistent"):
        CapturedToolResult.from_record({"status": "ERROR", "result": {}, "is_error": flag})


def test_pending_capture_cannot_be_replayed_as_completed() -> None:
    with pytest.raises(ValidationError):
        CapturedToolResult.from_record({"status": "PENDING", "result": {}})


def test_captured_json_is_detached_and_its_signature_includes_outcome() -> None:
    original: dict[str, JsonValue] = {"status": "DONE", "result": {"nested": ["original"]}}
    capture = CapturedToolResult.from_record(original)
    original["result"] = {"nested": ["changed"]}
    assert capture.result == {"nested": ["original"]}
    error = CapturedToolResult(status="ERROR", result=capture.result)
    assert capture.signature() != error.signature()


def test_native_success_omits_error_flag_and_failure_sets_only_true() -> None:
    success = NativeToolResult(
        type="user.custom_tool_result",
        custom_tool_use_id="call",
        content=[NativeText(type="text", text="{}")],
    )
    assert "is_error" not in success.model_dump(exclude_none=True)
    failure = NativeToolResult(
        type="user.custom_tool_result",
        custom_tool_use_id="call",
        content=[NativeText(type="text", text="{}")],
        is_error=True,
    )
    assert failure.model_dump(exclude_none=True)["is_error"] is True

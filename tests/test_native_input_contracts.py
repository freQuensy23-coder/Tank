"""Pinned native acknowledgment shapes and input identity ownership."""

from typing import Literal

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.native_inputs import (
    AcceptedNativeInput,
    NativeInputRequest,
    NativeUserMessage,
    UnreportedNativeInput,
    observe_native_input,
    parse_native_input,
)
from tank.agents.tool_contracts import NativeText, NativeToolResult


def message() -> NativeUserMessage:
    return NativeUserMessage(type="user.message", content=[NativeText(type="text", text="Source update")])


@pytest.mark.parametrize(("wire", "status"), [({}, "UNREPORTED"), ({"data": []}, "EMPTY")])
def test_provider_can_omit_acknowledged_events(
    wire: dict[str, JsonValue], status: Literal["UNREPORTED", "EMPTY"]
) -> None:
    observed = observe_native_input(wire, message())
    assert isinstance(observed, UnreportedNativeInput)
    assert observed.status == status


def test_reported_user_event_binds_the_native_input_identity() -> None:
    observed = observe_native_input(
        {
            "data": [
                {
                    "id": "native-input",
                    "type": "user.message",
                    "content": [{"type": "text", "text": "Source update"}],
                    "processed_at": None,
                }
            ]
        },
        message(),
    )
    assert observed == AcceptedNativeInput(status="ACCEPTED", event_id="native-input")


@pytest.mark.parametrize(
    "wire",
    [
        {"data": None},
        {"data": "native-input"},
        {"data": [{"id": "native-input"}]},
        {"data": [{"id": "", "type": "user.message", "content": []}]},
        {"data": [{"id": "native-input", "type": "unknown"}]},
        {
            "data": [
                {
                    "id": "native-input",
                    "type": "user.custom_tool_result",
                    "custom_tool_use_id": "call",
                    "content": None,
                }
            ]
        },
        {"data": [{"id": "native-input", "type": "user.message", "content": []}], "payload": {}},
        {
            "data": [
                {"id": "one", "type": "user.message", "content": [{"type": "text", "text": "a"}]},
                {"id": "two", "type": "user.message", "content": [{"type": "text", "text": "b"}]},
            ]
        },
    ],
)
def test_malformed_reported_acknowledgment_cannot_claim_unreported_acceptance(
    wire: dict[str, JsonValue],
) -> None:
    with pytest.raises(ValueError):
        observe_native_input(wire, message())


def test_tool_continuation_acknowledgment_binds_the_posted_call() -> None:
    posted = NativeToolResult(
        type="user.custom_tool_result",
        custom_tool_use_id="call",
        content=[NativeText(type="text", text="Tool result")],
    )
    wire: dict[str, JsonValue] = {
        "data": [
            {
                "id": "continuation",
                "type": "user.custom_tool_result",
                "custom_tool_use_id": "call",
                "content": [{"type": "text", "text": "Tool result"}],
                "is_error": False,
                "session_thread_id": None,
                "processed_at": None,
            }
        ]
    }
    assert observe_native_input(wire, posted) == AcceptedNativeInput(
        status="ACCEPTED", event_id="continuation"
    )
    mismatched: dict[str, JsonValue] = {
        "data": [{"id": "continuation", "type": "user.custom_tool_result", "custom_tool_use_id": "other"}]
    }
    with pytest.raises(ValueError, match="posted tool call"):
        observe_native_input(mismatched, posted)
    with pytest.raises(ValueError, match="posted input type"):
        observe_native_input(wire, message())


def test_one_executable_input_is_serialized_without_invented_optional_fields() -> None:
    posted = parse_native_input(
        {"type": "user.message", "content": [{"type": "text", "text": "Source update"}]}
    )
    assert NativeInputRequest(events=[posted]).model_dump(mode="json", exclude_none=True) == {
        "events": [{"type": "user.message", "content": [{"type": "text", "text": "Source update"}]}]
    }
    with pytest.raises(ValidationError):
        NativeInputRequest(events=[])
    with pytest.raises(ValidationError):
        NativeInputRequest(events=[posted, posted])

"""Actual replay annotations retain provenance without authorizing live replay effects."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.native_sessions import NativeSessionResolution
from tank.runtime_contracts import SourceMessage, parse_agent_message


def source_payload() -> dict[str, JsonValue]:
    return {"kind": "NEW_ITEM", "item_id": "source-item", "replay_run_id": "declared-run"}


def test_scalar_replay_clock_preserves_unreported_precision_and_original_wire_presence() -> None:
    payload = source_payload() | {"replay_at": "2026-10-26T10:00:00Z"}
    message = parse_agent_message(payload)
    assert isinstance(message, SourceMessage)
    assert message.replay_at_prec is message.replay_at_prec_reason is None
    wire = message.model_dump(mode="json", by_alias=True, exclude_unset=True)
    assert wire == payload


def test_actual_recorded_clock_facets_and_source_session_are_preserved() -> None:
    payload = source_payload() | {
        "replay_source_session_id": "captured-session",
        "replay_at": "2026-10-26T10:00:00Z",
        "replay_at_prec": "BUCKET(1m)",
        "replay_at_prec_reason": None,
        "replay_at_bucket_anchor": "END",
    }
    assert parse_agent_message(payload).model_dump(mode="json", by_alias=True, exclude_unset=True) == payload


def test_authored_reference_is_explicitly_nonhistorical_and_bound_to_actual_replay() -> None:
    payload = source_payload() | {
        "origin": "FIXTURE_AUTHORED_REFERENCE",
        "historical": False,
        "replay_at": "2026-10-26T10:00:00Z",
    }
    parsed = parse_agent_message(payload)
    assert isinstance(parsed, SourceMessage) and parsed.historical is False
    assert parsed.origin == "FIXTURE_AUTHORED_REFERENCE"
    with pytest.raises(ValueError, match="explicit replay composition"):
        NativeSessionResolution.from_wire(payload, role="ANALYST", replay=False)
    observed = NativeSessionResolution.from_wire(payload, role="ANALYST", replay=True)
    assert observed.replay is not None and observed.replay.run_id == "declared-run"


@pytest.mark.parametrize("historical", [0, 1, True, "false", None])
def test_reference_annotation_cannot_coerce_or_guess_nonhistorical_fact(historical: JsonValue) -> None:
    payload = source_payload() | {
        "origin": "FIXTURE_AUTHORED_REFERENCE",
        "historical": historical,
        "replay_at": "2026-10-26T10:00:00Z",
    }
    with pytest.raises(ValidationError):
        parse_agent_message(payload)


@pytest.mark.parametrize(
    "metadata",
    [
        {"replay_at": "2026-10-26T10:00:00"},
        {"replay_at": "2026-10-26"},
        {"replay_at_prec": "EXACT"},
        {
            "replay_at": "2026-10-26T10:00:00Z",
            "replay_at_prec": "BUCKET(1m)",
            "replay_at_bucket_anchor": "INVALID",
        },
    ],
)
def test_invalid_or_incomplete_actual_replay_clock_refuses(metadata: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        parse_agent_message(source_payload() | metadata)


def test_replay_clock_and_reference_require_actual_run_and_reference_has_no_captured_source() -> None:
    payload = source_payload() | {
        "replay_at": "2026-10-26T10:00:00Z",
        "origin": "FIXTURE_AUTHORED_REFERENCE",
        "historical": False,
    }
    without_run = dict(payload)
    del without_run["replay_run_id"]
    with pytest.raises(ValidationError):
        parse_agent_message(without_run)
    with pytest.raises(ValidationError):
        parse_agent_message(payload | {"replay_source_session_id": "captured-session"})


def test_live_resolution_refuses_unbound_clock_even_when_called_without_message_admission() -> None:
    payload: dict[str, JsonValue] = {
        "kind": "NEW_ITEM",
        "item_id": "source-item",
        "replay_at": "2026-10-26T10:00:00Z",
    }
    with pytest.raises(ValueError, match="explicit replay composition"):
        NativeSessionResolution.from_wire(payload, role="ANALYST", replay=False)

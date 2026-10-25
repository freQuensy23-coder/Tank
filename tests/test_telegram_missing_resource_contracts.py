import copy

import pytest
from pydantic import JsonValue

from tank.telegram_inbox import remote_session_missing


@pytest.mark.parametrize(
    "remote",
    [
        {"kind": "MISSING", "missing": False, "session_id": "session"},
        {"kind": "MISSING", "missing": 1, "session_id": "session"},
        {"kind": "MISSING", "missing": 1.0, "session_id": "session"},
        {"kind": "MISSING", "missing": True},
        {"kind": "MISSING", "session_id": "session"},
        {"missing": True, "session_id": "session"},
        {"kind": "MISSING", "missing": True, "session_id": "another"},
        {"kind": "MISSING", "missing": True, "session_id": "session", "status": "terminated"},
        {"kind": "MISSING", "missing": True, "session_id": "session", "archived_at": None},
        {"kind": "MISSING", "missing": True, "session_id": "session", "telemetry": "private-native-value"},
        [],
    ],
)
def test_missing_observation_requires_its_exact_identity_and_refuses_lifecycle_fabrication(
    remote: JsonValue,
) -> None:
    row: dict[str, JsonValue] = {"session_id": "session", "remote_state": remote}
    before = copy.deepcopy(row)
    with pytest.raises(ValueError) as failure:
        remote_session_missing(row)
    assert "private-native-value" not in str(failure.value)
    assert row == before


def test_actual_missing_observation_ignores_only_separate_collection_clock_facets() -> None:
    row: dict[str, JsonValue] = {
        "session_id": "session",
        "remote_state": {
            "kind": "MISSING",
            "missing": True,
            "session_id": "session",
            "checked_at": "2026-10-25T00:00:00Z",
            "checked_at_prec": "COLLECTION_ONLY",
            "checked_at_prec_reason": None,
        },
    }
    before = copy.deepcopy(row)
    assert remote_session_missing(row)
    assert row == before
    assert not remote_session_missing({"session_id": "session"})
    assert not remote_session_missing({"session_id": "session", "remote_state": None})
    assert not remote_session_missing(
        {
            "session_id": "session",
            "remote_state": {
                "kind": "FOUND",
                "session_id": "session",
                "status": "idle",
                "archived_at": None,
            },
        }
    )

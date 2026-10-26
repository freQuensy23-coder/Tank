"""Historical unknown explanations stay unknown; current writes must explain absence."""

from copy import deepcopy

import pytest
from card_wire_support import card_inputs
from foundation_helpers import object_field
from pydantic import JsonValue
from test_decision_contracts import frozen_decision_fixture, status_writer_fixture

from tank.analysis.card_contracts import ActiveCardDecision, CardInputs, CardScenarios
from tank.analysis.cards import compose_card
from tank.analysis.decision_codec import decode_decision, encode_decision
from tank.analysis.decisions import end_decision_tracking
from tank.time_precision import collection_clock


def historical_wire() -> dict[str, JsonValue]:
    wire = encode_decision(frozen_decision_fixture())
    outlook = object_field(wire, "outlook")
    outlook["scenarios"] = {
        "base": None,
        "upside": None,
        "failure": "The captured failure scenario remains available.",
    }
    outlook["scenario_reasons"] = {"upside": "ORIGINAL_CAUSE_AS_CAPTURED"}
    return wire


def test_literal_missing_historical_reason_has_an_explicit_unknown_classification() -> None:
    wire = historical_wire()
    original = deepcopy(wire)
    record = decode_decision(wire)
    explanations = record.calculation.outlook.scenario_explanations
    assert explanations.base.state == "EXPLANATION_NOT_CAPTURED"
    assert explanations.base.reason is None and explanations.base.text is None
    assert explanations.upside.state == "UNAVAILABLE"
    assert explanations.upside.reason == "ORIGINAL_CAUSE_AS_CAPTURED"
    assert explanations.failure.state == "AVAILABLE"
    assert wire == original


def test_current_active_embedded_outlook_cannot_fill_an_uncaptured_scenario_cause() -> None:
    record = decode_decision(historical_wire())
    with pytest.raises(ValueError, match="actual absence explanation"):
        encode_decision(record)


def test_ended_clone_keeps_literal_missing_reason_and_the_original_captured_cause() -> None:
    wire = historical_wire()
    original = deepcopy(wire)
    frozen = decode_decision(wire)
    ended = end_decision_tracking(
        frozen,
        decision_id="ended-historical",
        expired_call_ids=("expired-call",),
        status_recorded_at=collection_clock("2026-10-27T00:00:00Z"),
        writer=status_writer_fixture(frozen),
    )
    encoded = encode_decision(ended)
    outlook = object_field(encoded, "outlook")
    original_outlook = object_field(original, "outlook")
    assert outlook["scenarios"] == original_outlook["scenarios"]
    assert outlook["scenario_reasons"] == {"upside": "ORIGINAL_CAUSE_AS_CAPTURED"}
    assert ended.calculation == frozen.calculation
    assert wire == original


def test_current_unavailable_scenario_keeps_its_actual_reported_reason() -> None:
    wire = historical_wire()
    object_field(object_field(wire, "outlook"), "scenario_reasons")["base"] = "CAPTURED_BASE_CAUSE"
    record = decode_decision(wire)
    encoded = encode_decision(record)
    assert object_field(object_field(encoded, "outlook"), "scenario_reasons") == {
        "base": "CAPTURED_BASE_CAUSE",
        "upside": "ORIGINAL_CAUSE_AS_CAPTURED",
    }


def test_historical_scenario_presentation_names_the_uncaptured_explanation() -> None:
    values = card_inputs()
    decision = values.decisions_by_token["world:WLD"]
    assert isinstance(decision, ActiveCardDecision)
    scenarios = CardScenarios(
        base=None,
        upside=None,
        failure="The captured failure scenario remains available.",
        base_reason=None,
        upside_reason="ORIGINAL_CAUSE_AS_CAPTURED",
        failure_reason=None,
    )
    outlook = decision.outlook.model_copy(update={"scenarios": scenarios})
    declared = CardInputs.model_validate(
        values.model_copy(
            update={"decisions_by_token": {"world:WLD": decision.model_copy(update={"outlook": outlook})}}
        )
    )
    text = compose_card(declared)
    assert "• Base: n/a (explanation not captured)" in text
    assert "• Upside: n/a (original cause as captured)" in text
    assert "• Failure: The captured failure scenario remains available." in text

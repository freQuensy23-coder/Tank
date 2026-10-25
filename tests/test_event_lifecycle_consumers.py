"""Actual delivery and scheduler phases remain valid active event facts."""

import pytest
from card_wire_support import card_inputs, wire_card_inputs
from test_decision_contracts import decision_wire_fixture

from tank.analysis.card_codec import publication_gate_inputs_from_records
from tank.analysis.card_policy import alert_gate
from tank.analysis.decision_codec import decision_inputs_from_records
from tank.util import parse_time


@pytest.mark.parametrize("lifecycle", ["ALERTED", "COOLDOWN"])
def test_actual_operational_event_phases_reach_decision_and_publication(lifecycle: str) -> None:
    event, assessment, beneficiary, truth = decision_wire_fixture()
    event["lifecycle"] = lifecycle
    decision = decision_inputs_from_records(
        event, assessment, beneficiary, truth, open_call=None, conditions=()
    )
    assert decision.event.invalidated is False and decision.event.has_premise is True

    event, assessment, _, record, _ = wire_card_inputs()
    event["lifecycle"] = lifecycle
    facts = publication_gate_inputs_from_records(
        event,
        assessment,
        [record],
        previous=None,
        policy=card_inputs().policy,
        now=parse_time("2026-10-18T12:32:00Z"),
    )
    assert facts.event.lifecycle == lifecycle
    gate = alert_gate(facts)
    assert gate.allowed is True and gate.reason == "INITIAL"


def test_unknown_operational_phase_is_not_reinterpreted_as_an_active_event() -> None:
    event, assessment, beneficiary, truth = decision_wire_fixture()
    event["lifecycle"] = "NOT_A_PHASE"
    with pytest.raises(ValueError, match="Unsupported decision event lifecycle"):
        decision_inputs_from_records(event, assessment, beneficiary, truth, open_call=None, conditions=())
    event, assessment, _, record, _ = wire_card_inputs()
    event["lifecycle"] = "NOT_A_PHASE"
    with pytest.raises(ValueError, match="lifecycle"):
        publication_gate_inputs_from_records(
            event,
            assessment,
            [record],
            previous=None,
            policy=card_inputs().policy,
            now=parse_time("2026-10-18T12:32:00Z"),
        )

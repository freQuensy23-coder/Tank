import pytest
from card_registry_support import fixture_registry_membership
from card_wire_support import wire_card_inputs
from pydantic import ValidationError
from test_card_policy import event_facts

from tank.analysis.card_codec import card_inputs_from_records
from tank.analysis.card_contracts import (
    PreliminaryGateInputs,
    PreliminaryNarrative,
    PublicationBaseline,
    PublicationPolicy,
)
from tank.analysis.card_policy import preliminary_gate
from tank.analysis.cards import compose_card


def preliminary_inputs() -> PreliminaryGateInputs:
    return PreliminaryGateInputs(
        event=event_facts().model_copy(update={"certainty": "LOW", "lifecycle": "TRACKED"}),
        assessment_class="MATERIAL",
        has_current_sourced_claim=True,
        previous=None,
        previous_failed=False,
        previous_terminal_correction=None,
    )


def test_low_certainty_preliminary_has_no_financial_projection() -> None:
    event, assessment, _, _, _ = wire_card_inputs()
    event.update(lifecycle="TRACKED", certainty="LOW")
    assessment["class"] = "MATERIAL"
    narrative = PreliminaryNarrative(
        kind="PRELIMINARY",
        what="The issuer proposed access.",
        verdict="Access remains proposed.",
        why=("The proposal could change access.",),
        unconfirmed="The counterparty has not confirmed adoption.",
    )
    inputs = card_inputs_from_records(
        event,
        assessment,
        (),
        (),
        (),
        registry=fixture_registry_membership(),
        narrative=narrative,
        policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
    )
    text = compose_card(inputs)
    assert text.startswith("PRELIMINARY · MATERIAL")
    assert narrative.unconfirmed is not None and narrative.unconfirmed in text
    assert "WHAT TO DO:" not in text and "OUTLOOK" not in text
    assert inputs.decisions_by_token == {} and inputs.market_by_token == {}
    assert preliminary_gate(preliminary_inputs()).allowed
    with pytest.raises(ValidationError):
        type(inputs).model_validate(
            inputs.model_copy(update={"narrative": narrative.model_copy(update={"unconfirmed": None})})
        )


def test_denial_uses_generated_predecessor_and_only_retries_failed_correction() -> None:
    inputs = preliminary_inputs()
    previous = PublicationBaseline(
        assessment_class="MATERIAL",
        event=inputs.event.previous_facts(),
        actions_by_token={},
        acknowledged_send=None,
    )
    denied = inputs.event.model_copy(
        update={"lifecycle": "INVALIDATED", "active_fatal_ids": frozenset({"fatal"})}
    )
    assert preliminary_gate(inputs.model_copy(update={"event": denied})).reason == "TERMINAL_EVENT"
    correction = inputs.model_copy(
        update={"event": denied, "previous": previous, "previous_terminal_correction": False}
    )
    assert preliminary_gate(correction).reason == "PRELIMINARY_CORRECTION"
    delivered = correction.model_copy(
        update={
            "previous": previous.model_copy(update={"event": denied.previous_facts()}),
            "previous_terminal_correction": True,
        }
    )
    assert not preliminary_gate(delivered).allowed
    assert preliminary_gate(delivered.model_copy(update={"previous_failed": True})).allowed

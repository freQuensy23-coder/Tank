from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from tank.analysis.card_contracts import (
    AcknowledgedPublicationClock,
    PublicationBaseline,
    PublicationChangeInputs,
    PublicationDecisionStatus,
    PublicationEventFacts,
    PublicationGateInputs,
    PublicationNarrative,
    PublicationPolicy,
)
from tank.analysis.card_policy import alert_gate, publication_immediate
from tank.time_precision import Timestamp, collection_clock

AT = datetime(2026, 10, 17, tzinfo=UTC)


def event_facts() -> PublicationEventFacts:
    return PublicationEventFacts(
        state="APPROVED",
        state_verified=True,
        certainty="HIGH",
        lifecycle="ANALYZING",
        freshness="FRESH",
        trigger_freshness=None,
        stage_freshness=None,
        negative_marker=None,
        active_fatal_ids=frozenset(),
        has_new_milestone_fact=False,
        has_expired_call_fact=False,
    )


def baseline() -> PublicationBaseline:
    return PublicationBaseline(
        assessment_class="MAJOR",
        event=event_facts().previous_facts(),
        actions_by_token={"T": "WATCH_TO_START"},
        acknowledged_send=AcknowledgedPublicationClock(stamp=collection_clock(AT)),
    )


def gate_inputs() -> PublicationGateInputs:
    return PublicationGateInputs(
        event=event_facts(),
        assessment_class="MAJOR",
        decisions=(PublicationDecisionStatus(token_id="T", action="WATCH_TO_START", active=True),),
        previous=baseline(),
        policy=PublicationPolicy(update_rate_limit_s=900),
        now=AT + timedelta(minutes=1),
    )


@pytest.mark.parametrize("field", ["what", "verdict"])
def test_narrative_requires_actual_nonblank_authored_text(field: str) -> None:
    valid = PublicationNarrative(
        kind="PARTIAL",
        what="The issuer approved the product.",
        verdict="The product was approved.",
        why=("The issuer confirmed approval.", "Economic usage remains unmeasured."),
    )
    with pytest.raises(ValidationError):
        PublicationNarrative.model_validate(valid.model_copy(update={field: " "}))
    for why in ((), ("one",), ("one", " ")):
        with pytest.raises(ValidationError):
            PublicationNarrative.model_validate(valid.model_copy(update={"why": why}))


def test_unchanged_or_stale_facts_cannot_bypass_clock_on_a_timer() -> None:
    inputs = gate_inputs()
    unchanged = alert_gate(inputs)
    assert not unchanged.allowed and unchanged.reason == "NO_MATERIAL_CHANGE"
    advanced = inputs.event.model_copy(update={"state": "LIVE", "stage_freshness": "STALE"})
    assert alert_gate(inputs.model_copy(update={"event": advanced})).reason == "NO_MATERIAL_CHANGE"
    stale = inputs.event.model_copy(
        update={"freshness": "STALE", "active_fatal_ids": frozenset({"new-fatal"})}
    )
    assert alert_gate(inputs.model_copy(update={"event": stale})).reason == "STALE_STATE_CHANGE"


def test_ordinary_update_uses_actual_sent_acknowledgement_clock_only() -> None:
    inputs = gate_inputs()
    changed = inputs.event.model_copy(update={"state": "LIVE"})
    inputs = inputs.model_copy(update={"event": changed})
    assert alert_gate(inputs).reason == "UPDATE_RATE_LIMIT"
    elapsed = inputs.model_copy(update={"now": AT + timedelta(minutes=16)})
    assert alert_gate(elapsed).reason == "UPDATE"
    unacknowledged = baseline().model_copy(update={"acknowledged_send": None})
    assert alert_gate(inputs.model_copy(update={"previous": unacknowledged})).reason == "UPDATE"


def test_only_new_safety_and_start_exit_transitions_bypass_cooldown() -> None:
    inputs = gate_inputs()
    start = PublicationDecisionStatus(token_id="T", action="START", active=True)
    assert alert_gate(inputs.model_copy(update={"decisions": (start,)})).reason == "UPDATE"
    inactive = start.model_copy(update={"active": False})
    assert alert_gate(inputs.model_copy(update={"decisions": (inactive,)})).reason == "NO_MATERIAL_CHANGE"
    fatal = inputs.event.model_copy(update={"active_fatal_ids": frozenset({"fatal-1"})})
    assert publication_immediate(PublicationChangeInputs(event=fatal, decisions=(), previous=inputs.previous))
    prior = baseline().model_copy(update={"event": fatal.previous_facts()})
    assert not publication_immediate(PublicationChangeInputs(event=fatal, decisions=(), previous=prior))


def test_delivery_acknowledgement_cannot_be_replaced_with_missing_or_bucket_clock() -> None:
    with pytest.raises(ValidationError, match="actual recorded clock"):
        AcknowledgedPublicationClock(stamp=Timestamp(None, None, "NO_DELIVERY_CLOCK"))
    with pytest.raises(ValidationError, match="not an event bucket"):
        AcknowledgedPublicationClock(stamp=Timestamp("2026-10-17T00:00:00Z", "BUCKET(1d)", None, "START"))
    recorded = AcknowledgedPublicationClock(
        stamp=Timestamp("2026-10-17T00:00:00Z", None, "RECORDED_PRECISION_UNAVAILABLE")
    )
    assert recorded.instant() == AT and recorded.stamp.precision is None
    with pytest.raises(ValidationError):
        PublicationGateInputs.model_validate(gate_inputs().model_copy(update={"now": datetime(2026, 10, 17)}))


def test_ambiguous_duplicate_current_decisions_refuse_change_detection() -> None:
    inputs = gate_inputs()
    duplicate = inputs.decisions[0].model_copy(update={"action": "START"})
    with pytest.raises(ValidationError, match="one current decision"):
        alert_gate(inputs.model_copy(update={"decisions": inputs.decisions + (duplicate,)}))

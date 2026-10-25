"""Deterministic publication eligibility over explicit facts and an observed clock."""

from datetime import timedelta

from tank.analysis.card_contracts import (
    PublicationChangeInputs,
    PublicationGateInputs,
    PublicationGateResult,
)
from tank.analysis.decision_contracts import EventState

_STAGE_LADDER: tuple[EventState, ...] = (
    "RUMOR",
    "ANNOUNCED",
    "PROPOSED",
    "APPROVED",
    "SIGNED",
    "DEPLOYED",
    "LIVE",
    "MONEY_FLOWING",
)


def publication_immediate(inputs: PublicationChangeInputs) -> bool:
    inputs = PublicationChangeInputs.model_validate(inputs)
    previous = inputs.previous
    prior_fatals = previous.event.active_fatal_ids if previous is not None else frozenset()
    if inputs.event.active_fatal_ids - prior_fatals:
        return True
    for decision in inputs.decisions:
        if not decision.active or decision.action not in {"START", "EXIT"}:
            continue
        prior_action = previous.actions_by_token.get(decision.token_id) if previous is not None else None
        if prior_action != decision.action:
            return True
    return False


def alert_gate(inputs: PublicationGateInputs) -> PublicationGateResult:
    inputs = PublicationGateInputs.model_validate(inputs)
    event, previous = inputs.event, inputs.previous
    if inputs.assessment_class not in {"MAJOR", "STRUCTURAL_SHOCK"}:
        return PublicationGateResult(allowed=False, reason="BELOW_MAJOR")
    if event.freshness == "STALE" or event.trigger_freshness == "STALE":
        return PublicationGateResult(
            allowed=False, reason="STALE_STATE_CHANGE" if previous is not None else "STALE"
        )
    if previous is None:
        if event.certainty not in {"MEDIUM", "HIGH"}:
            return PublicationGateResult(allowed=False, reason="LOW_CERTAINTY")
        if event.lifecycle == "INVALIDATED":
            return PublicationGateResult(allowed=False, reason="INVALIDATED")
        return PublicationGateResult(allowed=True, reason="INITIAL")
    action_changed = any(
        decision.active
        and decision.action != "HOLD"
        and previous.actions_by_token.get(decision.token_id) != decision.action
        for decision in inputs.decisions
    )
    fatal = bool(event.active_fatal_ids - previous.event.active_fatal_ids)
    stage_eligible = event.stage_freshness != "STALE" and event.state_verified
    advanced = (
        stage_eligible
        and event.state in _STAGE_LADDER
        and previous.event.state in _STAGE_LADDER
        and _STAGE_LADDER.index(event.state) > _STAGE_LADDER.index(previous.event.state)
    )
    newly_verified = (
        stage_eligible and event.state == previous.event.state and not previous.event.state_verified
    )
    negative = event.negative_marker is not None and event.negative_marker != previous.event.negative_marker
    changed = (
        action_changed
        or fatal
        or inputs.assessment_class != previous.assessment_class
        or advanced
        or newly_verified
        or negative
        or event.has_new_milestone_fact
        or event.has_expired_call_fact
    )
    if not changed:
        return PublicationGateResult(allowed=False, reason="NO_MATERIAL_CHANGE")
    immediate = publication_immediate(
        PublicationChangeInputs(event=event, decisions=inputs.decisions, previous=previous)
    )
    sent = previous.acknowledged_send
    if (
        not immediate
        and sent is not None
        and inputs.now - sent.instant() < timedelta(seconds=inputs.policy.update_rate_limit_s)
    ):
        return PublicationGateResult(allowed=False, reason="UPDATE_RATE_LIMIT")
    return PublicationGateResult(allowed=True, reason="UPDATE")

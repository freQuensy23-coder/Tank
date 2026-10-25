from datetime import timedelta
from decimal import Decimal

import pytest

from tank.analysis.decision_contracts import DecisionCallContext
from tank.analysis.decision_watches import (
    DecisionWatchCommand,
    DecisionWatchPolicy,
    reconcile_decision_watches,
)
from tank.analysis.watch_contracts import (
    WatchCondition,
    WatchLeaf,
    WatchPersistence,
    WatchProposal,
)
from tank.contracts.provenance import GenerationProvenance
from tank.time_precision import collection_clock
from tank.util import iso, parse_iso_time

NOW = parse_iso_time("2026-10-25T12:00:00Z")


def generation() -> GenerationProvenance:
    return GenerationProvenance(
        config_version="declared-fixture",
        code_version="declared-build",
        session_config_version=None,
        session_code_version=None,
        model_id=None,
        model_id_basis=None,
        model_id_reason=None,
        prompt_version=None,
        prompt_version_basis=None,
        prompt_version_reason=None,
        prompt_label=None,
        prompt_binding=None,
        agent_version=None,
        agent_version_basis=None,
        agent_version_reason=None,
        session_id=None,
    )


def expression(state: str = "APPROVED") -> WatchLeaf:
    return WatchLeaf(
        metric="event.state",
        window="W4H",
        op="==",
        value=state,
        persist=WatchPersistence(count=1, unit="consecutive_windows"),
    )


def condition(
    identity: str, *, fired: bool = False, state: str = "APPROVED", agent: bool = False
) -> WatchCondition:
    return WatchCondition.model_validate(
        {
            **generation().model_dump(mode="json"),
            "cond_id": identity,
            "event_id": "EVENT",
            "token_id": "TOKEN",
            "role": "AGENT" if agent else "UPGRADE",
            "human_text": "Await the authored event state",
            "expr": expression(state),
            "status": "FIRED" if fired else "ARMED",
            "created_by": "DEC-prior",
            "created_at": iso(NOW - timedelta(days=1)),
            "created_at_prec": "COLLECTION_ONLY",
            "created_at_prec_reason": None,
            "expires_at": iso(NOW + timedelta(days=1)),
            "expires_at_prec": "COLLECTION_ONLY",
            "expires_at_prec_reason": None,
            "agent_defined": agent,
            "counters": {},
            **(collection_clock(NOW - timedelta(hours=1)).fields("fired_at") if fired else {}),
        }
    )


def command(*proposals: WatchProposal) -> DecisionWatchCommand:
    return DecisionWatchCommand(
        decision_id="DEC-new",
        event_id="EVENT",
        token_id="TOKEN",
        generation=generation(),
        proposals=list(proposals),
    )


def proposal(identity: str = "new-condition") -> WatchProposal:
    return WatchProposal(cond_id=identity, role="UPGRADE", human_text="Await approval", expr=expression())


def policy() -> DecisionWatchPolicy:
    return DecisionWatchPolicy(lifetime=timedelta(days=7), milestone_grace=timedelta(hours=1))


def test_reuses_actual_armed_identity_and_preserves_original_clocks() -> None:
    prior = condition("existing")
    result = reconcile_decision_watches(
        command(proposal()), [prior], [], now=NOW, policy=policy(), prepared_condition_ids=None
    )
    assert [watch.cond_id for watch in result.active] == ["existing"]
    actual = result.active[0]
    assert actual.created_at == prior.created_at and actual.expires_at == prior.expires_at
    assert actual.last_shown_by == "DEC-new" and result.shown == ("existing",)
    assert prior.last_shown_by is None


def test_consumes_only_the_actual_prepared_firing_without_creating_an_active_watch() -> None:
    first, second = condition("first", fired=True), condition("second", fired=True, state="LIVE")
    result = reconcile_decision_watches(
        command(), [first, second], [], now=NOW, policy=policy(), prepared_condition_ids=frozenset({"second"})
    )
    assert result.active == [] and result.shown == ()
    assert [watch.cond_id for watch in result.writes] == ["second"]
    assert result.writes[0].consumed_by == "DEC-new"
    assert result.writes[0].consumed_at == iso(NOW)
    assert first.consumed_by is None and second.consumed_by is None


def test_rearms_actual_fired_predecessor_cancels_obsolete_engine_rule_and_protects_agent_rule() -> None:
    fired = condition("fired", fired=True)
    obsolete = condition("obsolete", state="ANNOUNCED")
    agent = condition("agent", state="LIVE", agent=True)
    result = reconcile_decision_watches(
        command(proposal()),
        [fired, obsolete, agent],
        [],
        now=NOW,
        policy=policy(),
        prepared_condition_ids=None,
    )
    assert result.active[0].rearmed_from == "fired" and result.active[0].cond_id == "new-condition"
    writes = {watch.cond_id: watch for watch in result.writes}
    assert writes["fired"].consumed_by == "DEC-new"
    assert writes["obsolete"].status == "CANCELLED" and writes["obsolete"].superseded_by == "DEC-new"
    assert writes["obsolete"].cancelled_at == iso(NOW)
    assert "agent" not in writes and agent.status == "ARMED"


def test_missing_firing_clock_cannot_be_replaced_by_current_time() -> None:
    fired = WatchCondition.model_validate(
        condition("fired", fired=True).model_copy(
            update={
                "fired_at": None,
                "fired_at_prec": None,
                "fired_at_prec_reason": None,
            }
        )
    )
    with pytest.raises(ValueError, match="actual firing clock"):
        reconcile_decision_watches(
            command(proposal()), [fired], [], now=NOW, policy=policy(), prepared_condition_ids=None
        )
    assert fired.consumed_by is None and fired.fired_at is None


def test_open_call_and_declared_milestone_bound_the_new_watch_expiry() -> None:
    opened = collection_clock(NOW - timedelta(days=6))
    call = DecisionCallContext(
        call_id="CALL",
        event_id="EVENT",
        token_id="TOKEN",
        opened_at=opened,
        invalidation_price=Decimal(9),
        status="OPEN",
    )
    due = iso(NOW + timedelta(hours=1))
    milestone = WatchProposal(
        cond_id="milestone",
        role="DOWNGRADE",
        human_text="Await the actual milestone outcome",
        expr=WatchLeaf(
            metric="milestone.outcome",
            window="EVENT",
            op="==",
            value="FAILED",
            persist=WatchPersistence(count=1, unit="consecutive_windows"),
            due_at=due,
            due_at_prec="EXACT",
            due_at_prec_reason=None,
        ),
    )
    result = reconcile_decision_watches(
        command(proposal(), milestone), [], [call], now=NOW, policy=policy(), prepared_condition_ids=None
    )
    assert result.active[0].expires_at == iso(NOW + timedelta(days=1))
    assert result.active[1].expires_at == iso(NOW + timedelta(hours=2))
    assert result.shown == ("new-condition", "milestone")


def test_rearming_cannot_overwrite_the_durable_firing_identity() -> None:
    prior = condition("spent", fired=True)
    with pytest.raises(ValueError, match="overwrite an existing lifecycle identity"):
        reconcile_decision_watches(
            command(proposal("spent")), [prior], [], now=NOW, policy=policy(), prepared_condition_ids=None
        )
    assert prior.status == "FIRED" and prior.consumed_by is None


def test_past_milestone_grace_cannot_be_recorded_as_a_new_armed_watch() -> None:
    due = iso(NOW - timedelta(hours=2))
    expired = WatchProposal(
        cond_id="expired",
        role="DOWNGRADE",
        human_text="Observe actual milestone failure",
        expr=WatchLeaf(
            metric="milestone.outcome",
            window="EVENT",
            op="==",
            value="FAILED",
            persist=WatchPersistence(count=1, unit="consecutive_windows"),
            due_at=due,
            due_at_prec="EXACT",
            due_at_prec_reason=None,
        ),
    )
    with pytest.raises(ValueError, match="unexpired actual deadline"):
        reconcile_decision_watches(
            command(expired), [], [], now=NOW, policy=policy(), prepared_condition_ids=None
        )

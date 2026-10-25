import re
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from typing import Literal

import pytest
from card_registry_support import fixture_registry_membership
from card_wire_support import card_inputs as template_inputs
from card_wire_support import wire_card_inputs
from foundation_helpers import object_field, present, text_field
from score_fixtures import unassessed_score_dimensions
from test_card_policy import gate_inputs as publication_case
from test_decision_contracts import decision_wire_fixture

from tank.analysis.card_contracts import (
    ActiveCardDecision,
    CardCondition,
    CardDataLimit,
    CardExclusion,
    CardInputs,
    CardOutlook,
    CardReturns,
    CardScenarios,
    CardSubject,
    InactiveCardDecision,
    PublicationDecisionStatus,
)
from tank.analysis.card_policy import alert_gate
from tank.analysis.cards import compose_card
from tank.analysis.decision_codec import decision_inputs_from_records
from tank.analysis.decision_contracts import (
    Action,
    ActionRule,
    DecisionEvaluation,
    DecisionInputs,
    DecisionOpenCall,
    DecisionScenarios,
    DecisionTriggers,
)
from tank.analysis.decisions import POSITIVE_ACTIONS, DecisionEngine
from tank.models import Record


def inputs() -> tuple[Record, Record, Record, Record]:
    event, assessment, beneficiary, market = decision_wire_fixture()
    event.update(event_id="e", freshness="FRESH")
    assessment.update(event_id="e", dims=unassessed_score_dimensions())
    beneficiary.update(event_id="e", exposure_score=6, thesis_link_credible=False)
    market["event_id"] = "e"
    object_field(object_field(market, "relative"), "windows")["EVENT"] = {"r_btc_adj": None}
    return (event, assessment, beneficiary, market)


def policy_inputs(
    event: Record,
    assessment: Record,
    beneficiary: Record,
    market: Record,
    *,
    open_call: DecisionOpenCall | None = None,
    triggers: DecisionTriggers | None = None,
) -> DecisionInputs:
    facts = decision_inputs_from_records(
        event, assessment, beneficiary, market, open_call=None, conditions=()
    )
    return DecisionInputs.model_validate(
        facts.model_copy(
            update={"open_call": open_call, "triggers": facts.triggers if triggers is None else triggers}
        )
    )


def fired_triggers(*roles: str) -> DecisionTriggers:
    return DecisionTriggers.model_validate(
        {"roles": frozenset(roles), "anchor_lost": False, "sell_downgrade": False}
    )


def scenario_texts(scenarios: DecisionScenarios) -> tuple[str, str, str]:
    return (present(scenarios.base), present(scenarios.upside), present(scenarios.failure))


def card_projection(evaluation: DecisionEvaluation, token_id: str) -> ActiveCardDecision:
    outlook = evaluation.outlook
    scenarios = CardScenarios(
        base=outlook.scenarios.base,
        upside=outlook.scenarios.upside,
        failure=outlook.scenarios.failure,
        base_reason=outlook.scenario_reasons.base,
        upside_reason=outlook.scenario_reasons.upside,
        failure_reason=outlook.scenario_reasons.failure,
    )
    return ActiveCardDecision(
        kind="ACTIVE",
        token_id=token_id,
        action=evaluation.action,
        thesis_grade=evaluation.thesis_grade,
        trade_grade=evaluation.trade_grade,
        raw_trade_grade=evaluation.trade_rule,
        trade_reasons=tuple(evaluation.trade_reasons),
        tactical=evaluation.tactical,
        outlook=CardOutlook(
            direction=outlook.direction,
            potential=outlook.potential,
            horizon=outlook.best_horizon,
            confidence=outlook.confidence,
            limitation=outlook.confidence_limiting,
            scenarios=scenarios,
        ),
        conditions=tuple(
            CardCondition(role=condition.role, human_text=condition.human_text)
            for condition in evaluation.conditions
        ),
    )


def evaluated_template(evaluation: DecisionEvaluation) -> CardInputs:
    values = template_inputs()
    return values.model_copy(
        update={"decisions_by_token": {"world:WLD": card_projection(evaluation, "world:WLD")}}
    )


def test_start_and_ordered_blockers() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    assert engine.evaluate(policy_inputs(e, a, b, m)).action == "START"
    cases: list[tuple[Record, Action]] = [
        ({"class": "LOW"}, "WAIT"),
        ({"class": "MEDIUM"}, "WATCH_TO_START"),
        ({"class": "HIGH", "critical_semantics_verified": False}, "START"),
    ]
    for patch, expected in cases:
        n = deepcopy(m)
        object_field(n, "data_confidence").update(patch)
        assert engine.evaluate(policy_inputs(e, a, b, n)).action == expected
    for kind, blocker_action in [("FATAL", "WAIT"), ("UNRESOLVED", "WATCH_TO_START")]:
        n = deepcopy(e)
        n["contradictions"] = [{"type": kind}]
        assert engine.evaluate(policy_inputs(n, a, b, m)).action == blocker_action
    b["role"] = "LOSER"
    assert engine.evaluate(policy_inputs(e, a, b, m)).action == "WAIT"


def test_risk_classes_and_absorption() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    for cls, action in [
        ("PERP_LED_CHASE", "DONT_CHASE"),
        ("SHORT_SQUEEZE", "DONT_CHASE"),
        ("LIQUIDITY_VACUUM", "DONT_CHASE"),
        ("UNKNOWN", "WAIT"),
        ("REAL_MARKET_FRAGMENTATION", "WAIT"),
        ("DISTRIBUTION", "WAIT"),
    ]:
        m["move_class"] = cls
        assert engine.evaluate(policy_inputs(e, a, b, m)).action == action
    m["move_class"] = "PASSIVE_SPOT_ABSORPTION"
    m["divergence"] = {"state": "ABSORPTION_CANDIDATE", "unresolved": []}
    assert engine.evaluate(policy_inputs(e, a, b, m)).action == "WATCH_TO_START"
    object_field(m, "divergence")["state"] = "ABSORPTION_CONFIRMED"
    assert engine.evaluate(policy_inputs(e, a, b, m)).action == "START"


def test_context_lifecycle_safety() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    call = DecisionOpenCall(invalidation_price=Decimal("9"))
    object_field(m, "relative")["market_context"] = "STRESS"
    assert engine.evaluate(policy_inputs(e, a, b, m)).trade_grade == "B"
    a["class"] = "STRUCTURAL_SHOCK"
    object_field(m, "relative")["windows"] = {"EVENT": {"r_btc_adj": "1"}}
    assert engine.evaluate(policy_inputs(e, a, b, m)).trade_grade == "A"
    assert engine.evaluate(policy_inputs(e, a, b, m, open_call=call)).action == "HOLD"
    assert (
        engine.evaluate(policy_inputs(e, a, b, m, open_call=call, triggers=fired_triggers("UPGRADE"))).action
        == "ADD"
    )
    e["contradictions"] = [{"type": "FATAL"}]
    e["freshness"] = "STALE"
    e["lifecycle"] = "INVALIDATED"
    d = engine.evaluate(policy_inputs(e, a, b, m, open_call=call))
    assert d.action == "EXIT"
    case = publication_case()
    event = case.event.model_copy(
        update={"freshness": "STALE", "lifecycle": "INVALIDATED", "active_fatal_ids": frozenset({"fatal"})}
    )
    status = PublicationDecisionStatus(token_id="T", action=d.action, active=True)
    assert not alert_gate(case.model_copy(update={"event": event, "decisions": (status,)})).allowed
    assert not alert_gate(case.model_copy(update={"event": event, "previous": None})).allowed


def test_card_diagnostics_and_probability() -> None:
    e, a, b, m = inputs()
    m["move_class"] = "PERP_LED_CHASE"
    m["divergence"] = {"state": "PERP_LED_MARKUP", "trigger": True, "unresolved": ["ABSORPTION_CANDIDATE"]}
    evaluated = DecisionEngine().evaluate(policy_inputs(e, a, b, m))
    values = evaluated_template(evaluated)
    truth = values.market_by_token["world:WLD"].model_copy(
        update={"move": "PERP_LED_CHASE", "unresolved": ("ABSORPTION_CANDIDATE",)}
    )
    text = compose_card(values.model_copy(update={"market_by_token": {"world:WLD": truth}}))
    assert "ABSORPTION_CANDIDATE" in text and len(text) <= 3500


def test_three_token_template_with_structured_figures() -> None:
    e, a, b, m = inputs()
    values = template_inputs()
    beneficiaries = []
    decisions: dict[str, ActiveCardDecision] = {}
    markets = {}
    for token in ("world:WLD", "ethereum:ETH", "cow:COW"):
        edge = dict(b, token_id=token)
        truth = deepcopy(m)
        truth["token_id"] = token
        evaluated = DecisionEngine().evaluate(policy_inputs(e, a, edge, truth))
        subject = values.beneficiaries[0].subject.model_copy(
            update={"token_id": token, "entity_id": None, "entity_name": None}
        )
        beneficiaries.append(values.beneficiaries[0].model_copy(update={"subject": subject}))
        decisions[token] = card_projection(evaluated, token)
        markets[token] = values.market_by_token["world:WLD"].model_copy(
            update={
                "token_id": token,
                "decisive_facts": ("4h: price 3.2%", "1h: native net flow 12000 tokens"),
            }
        )
    values = values.model_copy(
        update={
            "beneficiaries": tuple(beneficiaries),
            "decisions_by_token": decisions,
            "market_by_token": markets,
        }
    )
    text = compose_card(values)
    assert len(text) <= 3500 and text.count("WHAT TO DO:") == 3


def test_all_no_call_action_rows() -> None:
    engine = DecisionEngine()
    cases: list[tuple[Record, Record, Record, Record, ActionRule, Action]] = [
        ({}, {}, {}, {}, "A5", "START"),
        ({"contradictions": [{"type": "UNRESOLVED"}]}, {}, {}, {}, "A6", "WATCH_TO_START"),
        ({}, {"class": "STRUCTURAL_SHOCK"}, {"capture_class": "INDIRECT"}, {}, "A7", "START"),
        ({}, {}, {"capture_class": "INDIRECT"}, {}, "A8", "WAIT"),
        ({}, {}, {}, {"data_confidence": {"class": "MEDIUM"}}, "A9", "WATCH_TO_START"),
        ({}, {}, {"capture_class": "INDIRECT"}, {"data_confidence": {"class": "MEDIUM"}}, "A10", "WAIT"),
        ({}, {}, {}, {"move_class": "PERP_LED_CHASE"}, "A11", "DONT_CHASE"),
        ({}, {}, {}, {"move_class": "UNKNOWN"}, "A12", "WAIT"),
        ({}, {}, {"credible": False}, {}, "A3", "WAIT"),
        ({}, {}, {"role": "LOSER"}, {}, "A2", "WAIT"),
        (
            {},
            {"class": "STRUCTURAL_SHOCK"},
            {},
            {"relative": {"repricing_band": "HEAVY"}},
            "A0",
            "DONT_CHASE",
        ),
        ({}, {}, {}, {"data_confidence": {"class": "LOW"}}, "A1", "WAIT"),
    ]
    for ep, ap, bp, mp, rule, action in cases:
        e, a, b, m = inputs()
        e.update(ep)
        a.update(ap)
        b.update(bp)
        for field, value in mp.items():
            if field == "relative":
                assert isinstance(value, dict)
                object_field(m, "relative").update(value)
            else:
                m[field] = value
        result = engine.evaluate(policy_inputs(e, a, b, m))
        assert (result.action_rule, result.action) == (rule, action)


def test_start_blocker_combinations_and_secondary_requirements() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    b.update(role="SECONDARY", exposure_score=8)
    assert engine.evaluate(policy_inputs(e, a, b, m)).thesis_grade == "A"
    b["exposure_score"] = 6
    assert engine.evaluate(policy_inputs(e, a, b, m)).thesis_grade == "B"
    b.pop("credible")
    with pytest.raises(ValueError, match="credible"):
        policy_inputs(e, a, b, m)
    b["credible"] = False
    assert engine.evaluate(policy_inputs(e, a, b, m)).thesis_grade == "D"
    b["credible"] = True
    object_field(m, "relative")["repricing_band"] = "HEAVY"
    object_field(m, "data_confidence")["class"] = "LOW"
    a["class"] = "STRUCTURAL_SHOCK"
    b["role"] = "PRIMARY"
    assert engine.evaluate(policy_inputs(e, a, b, m)).action_rule == "A1"
    e["contradictions"] = [{"type": "FATAL"}]
    object_field(m, "data_confidence")["class"] = "HIGH"
    assert engine.evaluate(policy_inputs(e, a, b, m)).action_rule == "A0"


def test_open_call_precedence_and_conditions() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    call = DecisionOpenCall(invalidation_price=Decimal("8"))
    assert (
        engine.evaluate(
            policy_inputs(e, a, b, m, open_call=call, triggers=fired_triggers("DOWNGRADE"))
        ).action
        == "REDUCE"
    )
    assert (
        engine.evaluate(
            policy_inputs(e, a, b, m, open_call=call, triggers=fired_triggers("INVALIDATION", "UPGRADE"))
        ).action
        == "EXIT"
    )
    m["move_class"] = "PERP_LED_CHASE"
    assert engine.evaluate(policy_inputs(e, a, b, m, open_call=call)).action == "HOLD"
    e["next_milestone"] = {"kind": "vote"}
    result = engine.evaluate(policy_inputs(e, a, b, m))
    assert any(c.milestone is not None for c in result.conditions)


@pytest.mark.parametrize("relationship", ["PRIMARY", "LOSER"])
@pytest.mark.parametrize("unresolved", [False, True])
def test_add_uses_literal_b3_without_extra_relationship_or_contradiction_veto(
    relationship: Literal["PRIMARY", "LOSER"], unresolved: bool
) -> None:
    event, assessment, beneficiary, truth = inputs()
    beneficiary.update(role=relationship, thesis_link_credible=True)
    if unresolved:
        event["contradictions"] = [{"type": "UNRESOLVED"}]
    result = DecisionEngine().evaluate(
        policy_inputs(
            event,
            assessment,
            beneficiary,
            truth,
            open_call=DecisionOpenCall(invalidation_price=Decimal("8")),
            triggers=fired_triggers("UPGRADE"),
        )
    )
    assert result.thesis_grade in {"A", "B"}
    assert result.trade_grade == "A"
    assert (result.action, result.action_rule) == ("ADD", "B3")


@pytest.mark.parametrize("higher_priority", ["DOWNGRADE", "INVALIDATION", "FATAL"])
def test_add_never_overrides_literal_b1_b2_precedence(
    higher_priority: Literal["DOWNGRADE", "INVALIDATION", "FATAL"],
) -> None:
    event, assessment, beneficiary, truth = inputs()
    fired: list[Record] = [{"role": "UPGRADE", "status": "FIRED"}]
    if higher_priority == "FATAL":
        event["contradictions"] = [{"type": "FATAL"}]
    else:
        fired.append({"role": higher_priority, "status": "FIRED"})
    result = DecisionEngine().evaluate(
        policy_inputs(
            event,
            assessment,
            beneficiary,
            truth,
            open_call=DecisionOpenCall(invalidation_price=Decimal("8")),
            triggers=fired_triggers(*(text_field(row, "role") for row in fired)),
        )
    )
    assert (result.action, result.action_rule) == (
        ("REDUCE", "B2") if higher_priority == "DOWNGRADE" else ("EXIT", "B1")
    )


def test_update_gate_materiality_rate_limit_and_safety() -> None:
    case = publication_case()
    previous = present(case.previous)
    previous = previous.model_copy(update={"event": previous.event.model_copy(update={"state": "ANNOUNCED"})})
    case = case.model_copy(update={"previous": previous})
    elapsed = case.model_copy(update={"now": case.now + timedelta(minutes=15)})
    assert alert_gate(elapsed).allowed
    assert alert_gate(case).reason == "UPDATE_RATE_LIMIT"
    unverified = case.event.model_copy(update={"state_verified": False})
    assert alert_gate(elapsed.model_copy(update={"event": unverified})).reason == "NO_MATERIAL_CHANGE"
    paused = unverified.model_copy(update={"negative_marker": "PAUSED"})
    assert alert_gate(elapsed.model_copy(update={"event": paused})).allowed
    start = PublicationDecisionStatus(token_id="T", action="START", active=True)
    paused = paused.model_copy(update={"state": "ANNOUNCED"})
    assert alert_gate(case.model_copy(update={"event": paused, "decisions": (start,)})).allowed


def test_horizons_and_scenarios_no_probabilities() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    for impact, horizon in [
        ("NOW", "H12"),
        ("DAYS", "D3"),
        ("WEEKS", "D7"),
        ("MONTHS", "D7"),
        ("YEARS", "D7"),
        ("UNKNOWN", None),
    ]:
        object_field(a, "impact_horizon")["impact_start"] = impact
        d = engine.evaluate(policy_inputs(e, a, b, m))
        assert d.outlook.best_horizon == horizon
        assert not any(re.search("\\d|probability|chance", s) for s in scenario_texts(d.outlook.scenarios))


def test_canonical_claim_evidence_and_verification_update() -> None:
    from tank.analysis.card_codec import card_inputs_from_records

    event, assessment, beneficiary, decision, truth = wire_card_inputs()
    event["evidence_labels"] = {"ITM-24": "Issuer announcement"}
    event["claims"] = [
        {
            "claim_id": "CLM-24",
            "subject": "Issuer",
            "predicate": "signed",
            "object": "agreement",
            "verified": True,
            "accepted": True,
            "evidence": [{"item_id": "ITM-24", "quote": "Issuer signed agreement."}],
        }
    ]
    fixture = template_inputs()
    text = compose_card(
        card_inputs_from_records(
            event,
            assessment,
            [beneficiary],
            [decision],
            [truth],
            registry=fixture_registry_membership(),
            narrative=fixture.narrative,
            policy=fixture.policy,
        )
    )
    assert "Issuer signed agreement [Issuer announcement]" in text
    assert "source unavailable" not in text and "confirmed claim" not in text
    case = publication_case()
    previous = present(case.previous)
    previous = previous.model_copy(
        update={
            "event": previous.event.model_copy(update={"state_verified": False}),
            "acknowledged_send": None,
        }
    )
    case = case.model_copy(update={"previous": previous})
    assert alert_gate(case).allowed
    stale = case.event.model_copy(update={"freshness": "STALE"})
    case = case.model_copy(update={"event": stale})
    assert alert_gate(case).reason == "STALE_STATE_CHANGE"
    stale = stale.model_copy(update={"active_fatal_ids": frozenset({"fatal"})})
    exit_status = PublicationDecisionStatus(token_id="T", action="EXIT", active=True)
    assert (
        alert_gate(case.model_copy(update={"event": stale, "decisions": (exit_status,)})).reason
        == "STALE_STATE_CHANGE"
    )


def test_crowding_uses_fixed_four_hour_window() -> None:
    e, a, b, m = inputs()
    m["derivatives"] = {
        "synchronized": {"native_oi_vs_normal": "WITHIN"},
        "per_venue": [{"funding_vs_normal": "WITHIN"}],
        "windows": {
            "W4H": {
                "synchronized": {
                    "native_oi_vs_normal": "ABOVE",
                    "native_beyond": True,
                    "oi_native_chg_pct": "0.3",
                },
                "per_venue": [{"funding_vs_normal": "ABOVE"}],
            },
            "W1H": {"synchronized": {"native_oi_vs_normal": "WITHIN"}},
        },
    }
    d = DecisionEngine().evaluate(policy_inputs(e, a, b, m))
    assert d.trade_grade == "C" and d.outlook.potential == "LOW"


def test_card_keeps_secondary_layer_explanations_and_least_repriced() -> None:
    from tank.analysis.card_codec import card_inputs_from_records

    event, assessment, beneficiary, decision, truth = wire_card_inputs()
    mapping = object_field(event, "beneficiary_map")
    mapping["least_repriced_edge"] = "EDG-abc"
    mapping["no_credible_secondary_reason"] = "No secondary meets eligibility."
    layers = object_field(mapping, "layers")
    object_field(layers, "SETTLEMENT_L1_L2")["note"] = "No eligible universe rail."
    object_field(layers, "ORACLE_DATA")["note"] = "Dependency not verified."
    beneficiary.update(edge_id="EDG-abc", role="SECONDARY", exposure_score=8)
    fixture = template_inputs()

    def current() -> str:
        return compose_card(
            card_inputs_from_records(
                event,
                assessment,
                [beneficiary],
                [decision],
                [truth],
                registry=fixture_registry_membership(),
                narrative=fixture.narrative,
                policy=fixture.policy,
            )
        )

    text = current()
    assert "least-repriced" in text and "ramp STEP" not in text and "timing HIGH" not in text
    beneficiary["credible"] = False
    text = current()
    assert "secondary none — No secondary meets eligibility." in text
    assert "SETTLEMENT: NONE" not in text and "ORACLE: UNKNOWN" not in text
    assert "• Settlement L1 / L2: No eligible universe rail." in text
    assert "• Oracles / data: Dependency not verified." in text
    assert object_field(layers, "ORACLE_DATA")["note"] == "Dependency not verified."


def test_structural_partial_has_facts_without_market_forecast() -> None:
    values = template_inputs()
    values = values.model_copy(
        update={
            "assessment": values.assessment.model_copy(update={"classification": "STRUCTURAL_SHOCK"}),
            "narrative": values.narrative.model_copy(update={"kind": "PARTIAL"}),
            "beneficiaries": (),
            "decisions_by_token": {},
            "market_by_token": {},
        }
    )
    text = compose_card(values)
    assert "Market analysis pending" in text
    assert "WHAT TO DO:" not in text and "OUTLOOK:" not in text and "──" not in text


def test_class_specific_scenarios_and_unknown() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    seen = set()
    for cls in [
        "UNKNOWN",
        "PERP_LED_CHASE",
        "SHORT_SQUEEZE",
        "LIQUIDITY_VACUUM",
        "REAL_MARKET_FRAGMENTATION",
        "PASSIVE_SPOT_ABSORPTION",
        "SELL_SIDE_ABSORPTION",
        "DISTRIBUTION",
        "ORGANIC_MIXED",
    ]:
        m["move_class"] = cls
        scenarios = engine.evaluate(policy_inputs(e, a, b, m)).outlook.scenarios
        seen.add(tuple(scenario_texts(scenarios)))
        assert not any(re.search("\\d|probability|chance", s) for s in scenario_texts(scenarios))
    assert len(seen) == 9
    object_field(m, "data_confidence")["class"] = "LOW"
    m["move_class"] = "ORGANIC_MIXED"
    assert present(engine.evaluate(policy_inputs(e, a, b, m)).outlook.scenarios.base).startswith("UNKNOWN")


def test_full_who_names_outside_primary_but_omits_unverified_compatibility_candidate() -> None:
    values = template_inputs()
    outside = CardSubject(
        token_id=None,
        entity_id="OutsideNetwork",
        entity_name=None,
        role="PRIMARY",
        credible=False,
        registry_membership="NO_MAPPED_TOKEN",
        outside_evidence_accepted=True,
        layer=None,
        integration_state=None,
        integration_verified=False,
    )
    candidate = CardSubject(
        token_id=None,
        entity_id="Rail",
        entity_name=None,
        role="SECONDARY",
        credible=False,
        registry_membership="NO_MAPPED_TOKEN",
        outside_evidence_accepted=False,
        layer="SETTLEMENT_L1_L2",
        integration_state="COMPATIBLE",
        integration_verified=False,
    )
    truth = values.market_by_token["world:WLD"]
    relative = truth.relative.model_copy(
        update={
            "returns_by_window": {
                "EVENT": CardReturns(
                    raw=Decimal(".021"), btc_adjusted=Decimal(".013"), btc_adjusted_reason=None
                )
            }
        }
    )
    truth = truth.model_copy(update={"relative": relative})
    text = compose_card(
        values.model_copy(
            update={"other_subjects": (outside, candidate), "market_by_token": {"world:WLD": truth}}
        )
    )
    assert "primary OutsideNetwork" in text and "secondary Rail" not in text and "COMPATIBLE" not in text
    assert "2.10% raw; 1.30% net BTC" in text and "WHAT CHANGES THE ACTION:" in text


def test_expired_call_is_tracking_notice_without_action() -> None:
    values = template_inputs()
    inactive = InactiveCardDecision(kind="INACTIVE", token_id="world:WLD", reason="CALL_EXPIRED")
    text = compose_card(values.model_copy(update={"decisions_by_token": {"world:WLD": inactive}}))
    assert "No longer following: call expired." in text
    assert "WHAT TO DO:" not in text and "WHAT CHANGES THE ACTION:" not in text


def test_stale_expiry_never_bypasses_publication_gate() -> None:
    case = publication_case()
    event = case.event.model_copy(update={"freshness": "STALE", "has_expired_call_fact": True})
    frozen = PublicationDecisionStatus(token_id="T", action="START", active=False)
    previous = present(case.previous).model_copy(update={"actions_by_token": {"T": "START"}})
    case = case.model_copy(update={"event": event, "previous": previous, "decisions": (frozen,)})
    assert alert_gate(case).reason == "STALE_STATE_CHANGE"
    assert (
        alert_gate(case.model_copy(update={"now": case.now + timedelta(minutes=15)})).reason
        == "STALE_STATE_CHANGE"
    )
    values = template_inputs()
    inactive = InactiveCardDecision(kind="INACTIVE", token_id="world:WLD", reason="CALL_EXPIRED")
    text = compose_card(values.model_copy(update={"decisions_by_token": {"world:WLD": inactive}}))
    assert "MARKET TRUTH:" not in text and "OUTLOOK:" not in text
    inactive = inactive.model_copy(update={"reason": "REMOVED_FROM_ANALYSIS"})
    text = compose_card(values.model_copy(update={"decisions_by_token": {"world:WLD": inactive}}))
    assert "token removed from current beneficiary map." in text


def test_exact_majority_flag_preserves_tiny_sell_boundary() -> None:
    e, a, b, m = inputs()
    object_field(object_field(m, "spot"), "W4H").update(
        {
            "direction": "SELL",
            "classification": "CONCENTRATED",
            "cw_breadth": {"sell": 0.5},
            "exact_majority": {"sell": True},
        }
    )
    assert DecisionEngine().evaluate(policy_inputs(e, a, b, m)).trade_grade == "D"
    object_field(object_field(object_field(m, "spot"), "W4H"), "exact_majority")["sell"] = False
    assert DecisionEngine().evaluate(policy_inputs(e, a, b, m)).trade_grade == "A"
    object_field(object_field(m, "spot"), "W4H").pop("exact_majority")
    object_field(object_field(object_field(m, "spot"), "W4H"), "cw_breadth")["sell"] = (
        "0.50000000000000000000000000001"
    )
    assert DecisionEngine().evaluate(policy_inputs(e, a, b, m)).trade_grade == "D"


def test_b16_fraction_percent_exclusions_and_header_proof() -> None:
    values = template_inputs()
    truth = values.market_by_token["world:WLD"]
    returns = CardReturns(raw=Decimal(".0705"), btc_adjusted=Decimal(".066"), btc_adjusted_reason=None)
    relative = truth.relative.model_copy(update={"returns_by_window": {"EVENT": returns}})
    excluded = CardExclusion(
        venue="Bybit",
        reason="Current day history incomplete",
        detail=None,
        source_reasons=(),
        missing_metrics=(),
    )
    spot = truth.spot.model_copy(update={"excluded": (excluded,)})
    confidence = truth.confidence.model_copy(
        update={
            "classification": "MEDIUM",
            "limiting": CardDataLimit(condition="spot_coverage", reason="Only partial coverage"),
        }
    )
    truth = truth.model_copy(
        update={
            "relative": relative,
            "spot": spot,
            "confidence": confidence,
            "decisive_facts": ("4h: derivatives.oi native chg pct 8.18%",),
        }
    )
    decision = values.decisions_by_token["world:WLD"]
    assert isinstance(decision, ActiveCardDecision)
    limitation = decision.outlook.limitation.model_copy(update={"input": "data", "text": None})
    outlook = decision.outlook.model_copy(update={"confidence": "MEDIUM", "limitation": limitation})
    decision = decision.model_copy(update={"outlook": outlook})
    text = compose_card(
        values.model_copy(
            update={"market_by_token": {"world:WLD": truth}, "decisions_by_token": {"world:WLD": decision}}
        )
    )
    assert "7.05% raw; 6.60% net BTC" in text and "4h: derivatives.oi native chg pct 8.18%" in text
    assert "Excluded: Bybit — Current day history incomplete" in text
    assert "public 12:30Z · known 12:31Z" in text and "Data MEDIUM" in text
    assert "Spot venue coverage is incomplete: only partial coverage" in text


def test_versioned_card_limits_and_missing_decisions() -> None:
    values = template_inputs()
    policy = values.policy.model_copy(update={"card_max_chars": 10, "update_rate_limit_s": 1800})
    partial = values.model_copy(
        update={"narrative": values.narrative.model_copy(update={"kind": "PARTIAL"}), "policy": policy}
    )
    with pytest.raises(ValueError, match="CARD_LENGTH"):
        compose_card(partial)
    with pytest.raises(ValueError, match="MISSING_DECISION"):
        compose_card(values.model_copy(update={"decisions_by_token": {}}))
    case = publication_case()
    previous = present(case.previous)
    previous = previous.model_copy(update={"event": previous.event.model_copy(update={"state": "ANNOUNCED"})})
    case = case.model_copy(
        update={
            "policy": policy.model_copy(update={"card_max_chars": 3500}),
            "previous": previous,
            "now": case.now + timedelta(minutes=15),
        }
    )
    assert alert_gate(case).reason == "UPDATE_RATE_LIMIT"


def test_dont_chase_conditions_exact_library_rows() -> None:
    e, a, b, m = inputs()
    engine = DecisionEngine()
    m["move_class"] = "PERP_LED_CHASE"
    d = engine.evaluate(policy_inputs(e, a, b, m))
    down = [c for c in d.conditions if c.role == "DOWNGRADE"]
    assert len(down) == 1 and down[0].expr.model_dump(mode="json", exclude_unset=True) == {
        "metric": "price.close_vs_level",
        "window": "W1H",
        "op": "<",
        "value": "10",
        "persist": {"count": 1, "unit": "consecutive_windows"},
    }
    for move, band in [("LIQUIDITY_VACUUM", "EARLY"), ("ORGANIC_MIXED", "HEAVY")]:
        m["move_class"] = move
        object_field(m, "relative")["repricing_band"] = band
        d = engine.evaluate(policy_inputs(e, a, b, m))
        assert d.action == "DONT_CHASE" and (not any(c.role == "DOWNGRADE" for c in d.conditions))


def test_trade_base_a_reason_is_preserved() -> None:
    e, a, b, m = inputs()
    d = DecisionEngine().evaluate(policy_inputs(e, a, b, m))
    assert d.trade_reasons == ["TRADE_BASE_A"]


def test_persistent_safety_is_silent_but_new_fatal_and_action_transition_are_immediate() -> None:
    case = publication_case()
    event = case.event.model_copy(update={"freshness": "NORMAL", "active_fatal_ids": frozenset({"fatal-1"})})
    previous = present(case.previous).model_copy(
        update={"event": event.previous_facts(), "actions_by_token": {"T": "EXIT"}}
    )
    exit_status = PublicationDecisionStatus(token_id="T", action="EXIT", active=True)
    case = case.model_copy(update={"event": event, "previous": previous, "decisions": (exit_status,)})
    assert alert_gate(case).reason == "NO_MATERIAL_CHANGE"
    assert (
        alert_gate(case.model_copy(update={"assessment_class": "STRUCTURAL_SHOCK"})).reason
        == "UPDATE_RATE_LIMIT"
    )
    new_event = event.model_copy(update={"active_fatal_ids": frozenset({"fatal-1", "fatal-2"})})
    assert alert_gate(case.model_copy(update={"event": new_event})).allowed
    start = exit_status.model_copy(update={"action": "START"})
    assert alert_gate(case.model_copy(update={"decisions": (start,)})).allowed


@pytest.mark.parametrize("prior_stage,prior_verified", [("APPROVED", True), ("LIVE", False)])
def test_old_stage_does_not_resurface_on_timer_but_new_action_and_safety_facts_can_publish(
    prior_stage: Literal["APPROVED", "LIVE"], prior_verified: bool
) -> None:
    case = publication_case()
    event = case.event.model_copy(update={"state": "LIVE", "stage_freshness": "STALE"})
    previous = present(case.previous)
    previous = previous.model_copy(
        update={
            "event": previous.event.model_copy(
                update={"state": prior_stage, "state_verified": prior_verified}
            )
        }
    )
    case = case.model_copy(update={"event": event, "previous": previous})
    assert alert_gate(case).reason == "NO_MATERIAL_CHANGE"
    for action in ("START", "EXIT"):
        decision = PublicationDecisionStatus.model_validate(
            {"token_id": "T", "action": action, "active": True}
        )
        assert alert_gate(case.model_copy(update={"decisions": (decision,)})).reason == "UPDATE"
    event = event.model_copy(update={"active_fatal_ids": frozenset({"new"})})
    assert alert_gate(case.model_copy(update={"event": event})).reason == "UPDATE"


@pytest.mark.parametrize("event_class", ["LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"])
@pytest.mark.parametrize("trigger", ["START", "EXIT", "FATAL", "EXPIRY"])
def test_never_sent_rules_apply_before_update_rate_exceptions(
    event_class: Literal["LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"],
    trigger: Literal["START", "EXIT", "FATAL", "EXPIRY"],
) -> None:
    case = publication_case()
    previous = present(case.previous).model_copy(update={"acknowledged_send": None})
    event = case.event.model_copy(
        update={
            "freshness": "STALE",
            "active_fatal_ids": frozenset({"fatal"}) if trigger == "FATAL" else frozenset(),
            "has_expired_call_fact": trigger == "EXPIRY",
        }
    )
    action = trigger if trigger in {"START", "EXIT"} else "WAIT"
    decision = PublicationDecisionStatus.model_validate({"token_id": "T", "action": action, "active": True})
    case = case.model_copy(
        update={
            "event": event,
            "assessment_class": event_class,
            "previous": previous,
            "decisions": (decision,),
        }
    )
    outcome = alert_gate(case)
    assert not outcome.allowed
    assert outcome.reason == ("BELOW_MAJOR" if event_class in {"LOW", "MATERIAL"} else "STALE_STATE_CHANGE")
    outcome = alert_gate(case.model_copy(update={"event": event.model_copy(update={"freshness": "FRESH"})}))
    assert outcome.allowed is (event_class in {"MAJOR", "STRUCTURAL_SHOCK"})


def test_unverified_stage_advance_is_stored_but_does_not_trigger_a_standalone_update() -> None:
    case = publication_case()
    event = case.event.model_copy(update={"state": "LIVE", "state_verified": False})
    wait = PublicationDecisionStatus(token_id="T", action="WAIT", active=True)
    previous = present(case.previous).model_copy(update={"actions_by_token": {"T": "WAIT"}})
    case = case.model_copy(
        update={
            "event": event,
            "previous": previous,
            "decisions": (wait,),
            "now": case.now + timedelta(minutes=15),
        }
    )
    original = case.model_copy(deep=True)
    assert alert_gate(case).reason == "NO_MATERIAL_CHANGE"
    assert case == original


def test_agent_verified_stage_advance_can_trigger_an_update() -> None:
    case = publication_case()
    case = case.model_copy(
        update={
            "event": case.event.model_copy(update={"state": "LIVE"}),
            "now": case.now + timedelta(minutes=15),
        }
    )
    outcome = alert_gate(case)
    assert outcome.allowed and outcome.reason == "UPDATE"


@pytest.mark.parametrize("prior_stage", ["APPROVED", "LIVE"])
def test_unverified_stage_does_not_suppress_an_independent_start(
    prior_stage: Literal["APPROVED", "LIVE"],
) -> None:
    case = publication_case()
    previous = present(case.previous)
    previous = previous.model_copy(
        update={"event": previous.event.model_copy(update={"state": prior_stage, "state_verified": False})}
    )
    start = PublicationDecisionStatus(token_id="T", action="START", active=True)
    case = case.model_copy(
        update={
            "event": case.event.model_copy(update={"state": "LIVE", "state_verified": False}),
            "previous": previous,
            "decisions": (start,),
        }
    )
    outcome = alert_gate(case)
    assert outcome.allowed and outcome.reason == "UPDATE"


@pytest.mark.parametrize("move,expect", [("UNKNOWN", False), ("SPOT_LED_ACCUMULATION", True)])
def test_tactical_only_for_positive_action_on_weak_capture(move: str, expect: bool) -> None:
    """W9 (FR-O-03, 03 §4.8 row C): thesis C labels only a positive action."""
    event, assessment, beneficiary, truth = inputs()
    assessment["class"] = "STRUCTURAL_SHOCK"
    beneficiary["capture_class"] = "INDIRECT"
    truth["move_class"] = move
    result = DecisionEngine().evaluate(policy_inputs(event, assessment, beneficiary, truth))
    assert result.thesis_grade == "C"
    assert result.tactical is (result.action in POSITIVE_ACTIONS)
    assert result.tactical is expect, result.action


@pytest.mark.parametrize("data", ["MEDIUM", "HIGH"])
def test_unknown_move_with_usable_data_names_the_observed_flow(data: str) -> None:
    """03 §5.8/§6.4: an UNKNOWN move with usable data is not "evidence
    incomplete"; the scenarios name the observed flow and the upgrade path
    (live: Telegram msg 15, ALR-01M5DVXXBJGHH8EYRJE5W5PDDD)."""
    event, assessment, beneficiary, truth = inputs()
    event["statement"] = "s"
    truth["move_class"] = "UNKNOWN"
    object_field(truth, "data_confidence")["class"] = data
    cohort = object_field(object_field(truth, "spot"), "W4H")
    cohort.update(direction="NEUTRAL", classification="BROAD")
    object_field(cohort, "coverage")["n_complete"] = 5
    result = DecisionEngine().evaluate(policy_inputs(event, assessment, beneficiary, truth))
    base, upside, failure = scenario_texts(result.outlook.scenarios)
    assert (
        base
        == "No classifiable move yet: spot flow NEUTRAL and BROAD in the primary window on complete venues."
    )
    assert "incomplete" not in base + upside
    assert "BUY and BROAD" in upside and "accumulation" not in upside.lower()
    text = " ".join((base, upside, failure)).lower()
    assert not any(word in text for word in ("chance", "probability", "odds", "%", "$"))
    assert not re.search("\\d", text)


def test_low_data_keeps_the_incomplete_evidence_scenarios() -> None:
    event, assessment, beneficiary, truth = inputs()
    truth["move_class"] = "UNKNOWN"
    object_field(truth, "data_confidence")["class"] = "LOW"
    result = DecisionEngine().evaluate(policy_inputs(event, assessment, beneficiary, truth))
    assert result.outlook.scenarios.base == "UNKNOWN: market evidence incomplete."


@pytest.mark.parametrize("direction,breadth", [("BUY", "BROAD"), ("SELL", "BROAD"), ("NEUTRAL", "NARROW")])
def test_price_markup_scenarios_follow_the_spot_direction(direction: str, breadth: str) -> None:
    """E2E #8 (msg 22): PRICE_MARKUP_FAKE_OI printed the perp-led "spot does not
    confirm" texts while spot was BUY BROAD on all venues. Its scenarios state
    the price markup (L2) and never deny spot buying; no numbers (L7)."""
    event, assessment, beneficiary, truth = inputs()
    event["statement"] = "s"
    truth["move_class"] = "PRICE_MARKUP_FAKE_OI"
    object_field(object_field(truth, "spot"), "W4H").update(direction=direction, classification=breadth)
    result = DecisionEngine().evaluate(policy_inputs(event, assessment, beneficiary, truth))
    base, upside, failure = scenario_texts(result.outlook.scenarios)
    text = " ".join((base, upside, failure))
    assert "price markup" in base
    assert not re.search("leverage|new positions? (?:opened|built)|\\d", text.lower())
    if direction == "BUY":
        assert "spot buying carries the move" in base and "does not confirm" not in text
    else:
        assert "spot does not confirm" in base

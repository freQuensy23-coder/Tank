import re
from copy import deepcopy

from tank.analysis.cards import alert_gate, compose_card, lint_card
from tank.analysis.decisions import DecisionEngine


def inputs():
    return (
        {"event_id": "e", "event_state": "APPROVED", "state_verified": True, "freshness": "FRESH"},
        {"class": "MAJOR", "certainty": "HIGH", "impact_horizon": {"impact_start": "NOW"}},
        {"token_id": "T", "role": "PRIMARY", "credible": True, "capture_class": "DIRECT"},
        {
            "token_id": "T",
            "move_class": "SPOT_LED_ACCUMULATION",
            "data_confidence": {"class": "HIGH", "critical_semantics_verified": True},
            "relative": {
                "repricing_band": "EARLY",
                "market_context": "NEUTRAL",
                "anchor": {"ref_price": "10"},
            },
            "liquidity": {"liquidity_state": "NORMAL"},
            "spot": {},
            "derivatives": {},
        },
    )


def test_start_and_ordered_blockers():
    e, a, b, m = inputs()
    engine = DecisionEngine()
    assert engine.evaluate(e, a, b, m)["action"] == "START"
    cases = [
        ({"class": "LOW"}, "WAIT"),
        ({"class": "MEDIUM"}, "WATCH_TO_START"),
        ({"class": "HIGH", "critical_semantics_verified": False}, "WAIT"),
    ]
    for patch, expected in cases:
        n = deepcopy(m)
        n["data_confidence"].update(patch)
        assert engine.evaluate(e, a, b, n)["action"] == expected
    for kind, expected in [("FATAL", "WAIT"), ("UNRESOLVED", "WATCH_TO_START")]:
        n = deepcopy(e)
        n["contradictions"] = [{"type": kind}]
        assert engine.evaluate(n, a, b, m)["action"] == expected
    b["role"] = "LOSER"
    assert engine.evaluate(e, a, b, m)["action"] == "WAIT"


def test_risk_classes_and_absorption():
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
        assert engine.evaluate(e, a, b, m)["action"] == action
    m["move_class"] = "PASSIVE_SPOT_ABSORPTION"
    m["divergence"] = {"state": "ABSORPTION_CANDIDATE"}
    assert engine.evaluate(e, a, b, m)["action"] == "WATCH_TO_START"
    m["divergence"]["state"] = "ABSORPTION_CONFIRMED"
    assert engine.evaluate(e, a, b, m)["action"] == "START"


def test_context_lifecycle_safety():
    e, a, b, m = inputs()
    engine = DecisionEngine()
    call = {"invalidation_price": "9"}
    m["relative"]["market_context"] = "STRESS"
    assert engine.evaluate(e, a, b, m)["trade_grade"] == "B"
    a["class"] = "STRUCTURAL_SHOCK"
    m["relative"]["windows"] = {"EVENT": {"r_btc_adj": "1"}}
    assert engine.evaluate(e, a, b, m)["trade_grade"] == "A"
    assert engine.evaluate(e, a, b, m, open_call=call)["action"] == "HOLD"
    assert (
        engine.evaluate(e, a, b, m, open_call=call, conditions=[{"status": "FIRED", "role": "UPGRADE"}])[
            "action"
        ]
        == "ADD"
    )
    e["contradictions"] = [{"type": "FATAL"}]
    e["freshness"] = "STALE"
    e["lifecycle"] = "INVALIDATED"
    d = engine.evaluate(e, a, b, m, open_call=call)
    assert d["action"] == "EXIT"
    assert alert_gate(e, a, previous={"decisions": [{"token_id": "T", "action": "START"}]}, decisions=[d])[0]
    assert not alert_gate(e, a)[0]


def test_card_diagnostics_and_probability():
    e, a, b, m = inputs()
    m["move_class"] = "PERP_LED_CHASE"
    m["divergence"] = {"state": "PERP_LED_MARKUP", "trigger": True, "unresolved": ["ABSORPTION_CANDIDATE"]}
    d = DecisionEngine().evaluate(e, a, b, m)
    text = compose_card(e, a, [b], [d], [m], verdict="A verified event changes the thesis.")
    assert "ABSORPTION_CANDIDATE" in text and len(text) <= 3500
    assert not any(
        x["rule"] == "L12" for x in lint_card(text, {"event": e, "beneficiaries": [b], "market_truths": [m]})
    )
    assert any(x["rule"] == "L7" for x in lint_card("High probability of upside.", {}))


def test_three_token_template_with_structured_figures():
    e, a, b, m = inputs()
    bs = []
    ds = []
    ms = []
    for token in ["AAA", "BBB", "CCC"]:
        edge = dict(b, token_id=token)
        truth = deepcopy(m)
        truth["token_id"] = token
        truth["decisive_facts"] = [
            {"metric": "price", "value": "3.2", "unit": "%", "window": "W4H"},
            {"metric": "native net flow", "value": "12000", "unit": "tokens", "window": "W1H"},
        ]
        bs.append(edge)
        ms.append(truth)
        ds.append(DecisionEngine().evaluate(e, a, edge, truth))
    text = compose_card(
        e,
        a,
        bs,
        ds,
        ms,
        verdict="The approval advances a direct token mechanism.",
        why=["What changed: approval verified.", "Economic effect: not estimable."],
    )
    assert len(text) <= 3500 and text.count("WHAT TO DO:") == 3
    assert (
        lint_card(
            text, {"event": e, "assessment": a, "beneficiaries": bs, "decisions": ds, "market_truths": ms}
        )
        == []
    )


def test_all_no_call_action_rows():
    engine = DecisionEngine()
    cases = [
        ({}, {}, {}, {}, "A5", "START"),
        ({"contradictions": [{"type": "UNRESOLVED"}]}, {}, {}, {}, "A6", "WATCH_TO_START"),
        ({}, {"class": "STRUCTURAL_SHOCK"}, {"capture_class": "INDIRECT"}, {}, "A7", "START"),
        ({}, {}, {"capture_class": "INDIRECT"}, {}, "A8", "WAIT"),
        (
            {},
            {},
            {},
            {"data_confidence": {"class": "MEDIUM", "critical_semantics_verified": True}},
            "A9",
            "WATCH_TO_START",
        ),
        (
            {},
            {},
            {"capture_class": "INDIRECT"},
            {"data_confidence": {"class": "MEDIUM", "critical_semantics_verified": True}},
            "A10",
            "WAIT",
        ),
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
        m.update(mp)
        result = engine.evaluate(e, a, b, m)
        assert (result["action_rule"], result["action"]) == (rule, action)


def test_start_blocker_combinations_and_secondary_requirements():
    e, a, b, m = inputs()
    engine = DecisionEngine()
    b.update(role="SECONDARY", exposure_score=8)
    assert engine.evaluate(e, a, b, m)["thesis_grade"] == "A"
    b["exposure_score"] = 6
    assert engine.evaluate(e, a, b, m)["thesis_grade"] == "B"
    b.pop("credible")
    assert engine.evaluate(e, a, b, m)["thesis_grade"] == "D"
    b["credible"] = True
    m["relative"]["repricing_band"] = "HEAVY"
    m["data_confidence"]["class"] = "LOW"
    a["class"] = "STRUCTURAL_SHOCK"
    b["role"] = "PRIMARY"
    assert engine.evaluate(e, a, b, m)["action_rule"] == "A1"
    e["contradictions"] = [{"type": "FATAL"}]
    m["data_confidence"]["class"] = "HIGH"
    # Spec explicitly orders primary-heavy A0 before fatal no-call A3.
    assert engine.evaluate(e, a, b, m)["action_rule"] == "A0"


def test_open_call_precedence_and_conditions():
    e, a, b, m = inputs()
    engine = DecisionEngine()
    call = {"invalidation_price": "8"}
    assert (
        engine.evaluate(e, a, b, m, open_call=call, conditions=[{"role": "DOWNGRADE", "status": "FIRED"}])[
            "action"
        ]
        == "REDUCE"
    )
    assert (
        engine.evaluate(
            e,
            a,
            b,
            m,
            open_call=call,
            conditions=[{"role": "INVALIDATION", "status": "FIRED"}, {"role": "UPGRADE", "status": "FIRED"}],
        )["action"]
        == "EXIT"
    )
    m["move_class"] = "PERP_LED_CHASE"
    assert engine.evaluate(e, a, b, m, open_call=call)["action"] == "HOLD"
    e["next_milestone"] = {"milestone_id": "vote"}
    result = engine.evaluate(e, a, b, m)
    assert any(c.get("milestone_id") == "vote" for c in result["conditions"])


def test_update_gate_materiality_rate_limit_and_safety():
    e, a, b, m = inputs()
    previous = {
        "event_state": "ANNOUNCED",
        "class": "MAJOR",
        "decisions": [{"token_id": "T", "action": "WATCH_TO_START"}],
        "published_at": "2026-10-17T00:00:00Z",
    }
    assert alert_gate(e, a, previous=previous, now="2026-10-17T00:16:00Z")[0]
    assert alert_gate(e, a, previous=previous, now="2026-10-17T00:01:00Z")[1] == "UPDATE_RATE_LIMIT"
    e["state_verified"] = False
    assert alert_gate(e, a, previous=previous, now="2026-10-17T00:16:00Z")[0]
    e["negative_marker"] = "PAUSED"
    assert alert_gate(e, a, previous=previous, now="2026-10-17T00:16:00Z")[0]
    e["event_state"] = "ANNOUNCED"
    d = DecisionEngine().evaluate(e, a, b, m)
    assert alert_gate(e, a, previous=previous, decisions=[d], now="2026-10-17T00:01:00Z")[0]


def test_horizons_and_scenarios_no_probabilities():
    e, a, b, m = inputs()
    engine = DecisionEngine()
    for impact, horizon in [
        ("NOW", "H12"),
        ("DAYS", "D3"),
        ("WEEKS", "D7"),
        ("MONTHS", "D7"),
        ("YEARS", "D7"),
        ("UNKNOWN", "H24"),
    ]:
        a["impact_horizon"]["impact_start"] = impact
        d = engine.evaluate(e, a, b, m)
        assert d["outlook"]["best_horizon"] == horizon
        assert not any(re.search(r"\d|probability|chance", s) for s in d["outlook"]["scenarios"].values())


def test_canonical_claim_evidence_and_verification_update():
    e, a, b, m = inputs()
    e["claims"] = [
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
    d = DecisionEngine().evaluate(e, a, b, m)
    text = compose_card(
        e,
        a,
        [b],
        [d],
        [m],
        verdict="Agreement advances.",
        why=["What changed: signature.", "Economic effect: not estimable."],
    )
    assert "Issuer signed agreement [ITM-24]" in text
    assert "source unavailable" not in text and "confirmed claim" not in text
    assert lint_card(text, {"event": e, "beneficiaries": [b], "market_truths": [m]}) == []
    previous = {"card_struct": {"event": dict(e, state_verified=False)}, "decisions": [d], "class": "MAJOR"}
    assert alert_gate(e, a, previous=previous, decisions=[d])[0]
    e["freshness"] = "STALE"
    assert alert_gate(e, a, previous=previous, decisions=[d])[1] == "STALE_STATE_CHANGE"
    e["contradictions"] = [{"type": "FATAL"}]
    assert (
        alert_gate(e, a, previous=previous, decisions=[dict(d, action="EXIT")])[1] == "UPDATE_SAFETY_OVERRIDE"
    )


def test_crowding_uses_fixed_four_hour_window():
    e, a, b, m = inputs()
    m["derivatives"] = {
        "synchronized": {"native_oi_vs_normal": "WITHIN"},
        "per_venue": [{"funding_vs_normal": "WITHIN"}],
        "windows": {
            "W4H": {
                "synchronized": {"native_oi_vs_normal": "ABOVE"},
                "per_venue": [{"funding_vs_normal": "ABOVE"}],
            },
            "W1H": {"synchronized": {"native_oi_vs_normal": "WITHIN"}},
        },
    }
    d = DecisionEngine().evaluate(e, a, b, m)
    assert d["trade_grade"] == "C" and d["outlook"]["potential"] == "LOW"


def test_impact_layer_reasons_and_least_repriced_are_visible():
    e, a, b, m = inputs()
    a["impact_horizon"].update(
        ramp_shape="STEP",
        dependencies=["verified settlement"],
        timing_confidence="HIGH",
        time_to_materiality="days",
    )
    e["beneficiary_map"] = {
        "layers": {
            "SETTLEMENT": {"status": "NONE", "note": "No eligible universe rail."},
            "ORACLE": {"status": "UNKNOWN", "note": "Dependency not verified."},
        },
        "no_credible_secondary_reason": "No secondary meets eligibility.",
        "least_repriced_edge": "EDG-abc",
    }
    b.update(edge_id="EDG-abc", role="SECONDARY", exposure_score=8)
    d = DecisionEngine().evaluate(e, a, b, m)
    text = compose_card(e, a, [b], [d], [m])
    assert "least-repriced" in text
    assert (
        "start NOW" in text
        and "ramp STEP" in text
        and "verified settlement" in text
        and "timing HIGH" in text
    )
    b["credible"] = False
    text = compose_card(e, a, [b], [d], [m])
    assert "SETTLEMENT: NONE — No eligible universe rail." in text
    assert "ORACLE: UNKNOWN — Dependency not verified." in text
    assert lint_card(text, {"event": e, "beneficiaries": [b], "market_truths": [m]}) == []


def test_structural_partial_has_facts_without_market_forecast():
    e, a, b, m = inputs()
    a["class"] = "STRUCTURAL_SHOCK"
    text = compose_card(e, a, [], [], [], kind="PARTIAL", verdict="Issuer confirms approval.", why=[])
    assert "Market analysis pending" in text
    assert "WHAT TO DO:" not in text and "OUTLOOK:" not in text and "──" not in text
    assert lint_card(text, {"event": e, "assessment": a, "market_truths": []}) == []


def test_class_specific_scenarios_and_unknown():
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
        scenarios = engine.evaluate(e, a, b, m)["outlook"]["scenarios"]
        seen.add(tuple(scenarios.values()))
        assert set(scenarios) == {"base", "upside", "failure"}
        assert not any(re.search(r"\d|probability|chance", s) for s in scenarios.values())
    assert len(seen) == 9
    m["data_confidence"]["class"] = "LOW"
    m["move_class"] = "ORGANIC_MIXED"
    assert engine.evaluate(e, a, b, m)["outlook"]["scenarios"]["base"].startswith("UNKNOWN")


def test_full_who_names_layers_outside_universe_and_net_event_move():
    e, a, b, m = inputs()
    b["token_id"] = "1INCH"
    m["token_id"] = "1INCH"
    e["beneficiary_edges"] = [
        {"entity_id": "OutsideNetwork", "role": "PRIMARY", "token_id": None},
        {"entity_id": "Rail", "role": "SECONDARY", "layer": "SETTLEMENT", "integration_state": "COMPATIBLE"},
    ]
    m["relative"]["windows"] = {"EVENT": {"r_raw": "0.021", "r_btc_adj": "0.013"}}
    text = compose_card(e, a, [b], [DecisionEngine().evaluate(e, a, b, m)], [m])
    assert "OutsideNetwork PRIMARY" in text and "Rail SECONDARY (SETTLEMENT, COMPATIBLE)" in text
    assert "2.10% raw; 1.30% net BTC" in text and "WHAT CHANGES THE ACTION:" in text
    assert lint_card(text, {"event": e, "beneficiaries": [b], "market_truths": [m]}) == []


def test_expired_call_is_tracking_notice_without_action():
    e, a, b, m = inputs()
    d = DecisionEngine().evaluate(e, a, b, m)
    d.update(tracking_ended=True, actionable=False, conditions=[])
    text = compose_card(e, a, [b], [d], [m])
    assert "No longer following: call expired." in text
    assert "WHAT TO DO:" not in text and "WHAT CHANGES THE ACTION:" not in text


def test_expiry_stale_gate_obeys_rate_and_ignores_frozen_action():
    e, a, b, m = inputs()
    e.update(freshness="STALE", call_expired=True)
    frozen = {"token_id": "T", "action": "START", "tracking_ended": True, "actionable": False}
    previous = {
        "event_state": e["event_state"],
        "class": "MAJOR",
        "decisions": [frozen],
        "published_at": "2026-10-17T00:00:00Z",
    }
    assert alert_gate(e, a, previous=previous, decisions=[frozen], now="2026-10-17T00:01:00Z") == (
        False,
        "UPDATE_RATE_LIMIT",
    )
    assert alert_gate(e, a, previous=previous, decisions=[frozen], now="2026-10-17T00:16:00Z") == (
        True,
        "UPDATE_SAFETY_OVERRIDE",
    )
    text = compose_card(e, a, [b], [frozen], [m])
    assert "MARKET TRUTH:" not in text and "OUTLOOK:" not in text
    frozen.update(removed_from_analysis=True)
    text = compose_card(e, a, [b], [frozen], [m])
    assert "token removed from current beneficiary map." in text


def test_exact_majority_flag_preserves_tiny_sell_boundary():
    e, a, b, m = inputs()
    # Display ratio rounds to half; truth computes this flag from exact Decimal venue weights.
    m["spot"] = {
        "W4H": {
            "direction": "SELL",
            "classification": "CONCENTRATED",
            "cw_breadth": {"sell": 0.5},
            "exact_majority": {"sell": True},
        }
    }
    assert DecisionEngine().evaluate(e, a, b, m)["trade_grade"] == "D"
    # Exactly half is not "most" and must not trigger the sell veto.
    m["spot"]["W4H"]["exact_majority"]["sell"] = False
    assert DecisionEngine().evaluate(e, a, b, m)["trade_grade"] == "A"
    # Decimal text legacy fixtures also retain the strict boundary.
    m["spot"]["W4H"].pop("exact_majority")
    m["spot"]["W4H"]["cw_breadth"]["sell"] = "0.50000000000000000000000000001"
    assert DecisionEngine().evaluate(e, a, b, m)["trade_grade"] == "D"


def test_b16_fraction_percent_exclusions_and_header_proof():
    e, a, b, m = inputs()
    e.update(public_at="2026-10-17T12:30:00Z", known_to_us_at="2026-10-17T12:31:00Z")
    m["relative"]["windows"] = {"EVENT": {"r_raw": "0.0705", "r_btc_adj": "0.066"}}
    m["spot"] = {
        "W4H": {"coverage": {"excluded": [{"venue": "Bybit", "reason": "Current day history incomplete"}]}}
    }
    m["data_confidence"]["limiting"] = {"condition": "spot_coverage", "reason": "Only partial coverage"}
    m["decisive_facts"] = [
        {
            "metric": "derivatives.oi_native_chg_pct",
            "value": "0.0818",
            "unit": "DECIMAL_RETURN",
            "window": "W4H",
        }
    ]
    d = DecisionEngine().evaluate(e, a, b, m)
    text = compose_card(e, a, [b], [d], [m])
    assert "7.05% raw; 6.60% net BTC" in text and "8.18% / W4H" in text
    assert "Excluded: Bybit — Current day history incomplete" in text
    assert "public 12:30Z, known 12:31Z" in text and "Data HIGH" in text
    assert "Spot venue coverage is incomplete: Only partial coverage" in text
    assert lint_card(text, {"event": e, "beneficiaries": [b], "market_truths": [m]}) == []


def test_canonical_divergence_trigger_blocks_auto_bearish():
    e, a, b, m = inputs()
    m["divergence"] = {"triggered": True, "state": "UNKNOWN"}
    violations = lint_card("Price shows weakness.", {"event": e, "market_truths": [m]})
    assert any(v["rule"] == "L12" for v in violations)


def test_versioned_card_limits_and_missing_decisions():
    e, a, b, m = inputs()
    e["thresholds"] = {"version": "test-v1", "card_max_chars": 10, "update_rate_limit_s": 1800}
    import pytest

    with pytest.raises(ValueError, match="CARD_LENGTH"):
        compose_card(e, a, [], [], [], kind="PARTIAL")
    assert any(v["rule"] == "LENGTH" for v in lint_card("Longer than ten characters", {"event": e}))
    e["thresholds"]["card_max_chars"] = 3500
    with pytest.raises(ValueError, match="MISSING_DECISION"):
        compose_card(e, a, [b], [], [m])
    previous = {"class": "MAJOR", "event_state": "ANNOUNCED", "published_at": "2026-10-17T00:00:00Z"}
    assert alert_gate(e, a, previous=previous, now="2026-10-17T00:16:00Z") == (False, "UPDATE_RATE_LIMIT")


def test_dont_chase_conditions_exact_library_rows():
    e, a, b, m = inputs()
    engine = DecisionEngine()
    m["move_class"] = "PERP_LED_CHASE"
    d = engine.evaluate(e, a, b, m)
    down = [c for c in d["conditions"] if c["role"] == "DOWNGRADE"]
    assert len(down) == 1 and down[0]["expr"] == {
        "metric": "price.close_vs_level",
        "window": "W1H",
        "op": "<",
        "value": "10",
        "persist": {"count": 1, "unit": "consecutive_windows"},
    }
    for move, band in [("LIQUIDITY_VACUUM", "EARLY"), ("ORGANIC_MIXED", "HEAVY")]:
        m["move_class"] = move
        m["relative"]["repricing_band"] = band
        d = engine.evaluate(e, a, b, m)
        assert d["action"] == "DONT_CHASE" and not any(c["role"] == "DOWNGRADE" for c in d["conditions"])


def test_l8_section_references_are_not_measured_figures():
    e, a, b, m = inputs()
    context = {"event": e, "market_truths": [m]}
    assert lint_card("SECONDARIES: No edge satisfies credibility requirements in §4.7.", context) == []
    assert any(v["rule"] == "L8" for v in lint_card("Net flow 123.", context))
    d = DecisionEngine().evaluate(e, a, b, m)
    assert d["trade_reasons"] == ["TRADE_BASE_A"]

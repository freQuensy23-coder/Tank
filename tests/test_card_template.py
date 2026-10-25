"""Literal PRD §7.1 card layout and detailed exclusion rendering."""

from copy import deepcopy
from pathlib import Path

import pytest
from source_support import fixture_versions

from tank.analysis.cards import compose_card
from tank.util import clock_iso


def card_inputs():
    event = {
        "statement": "Approval changes access.",
        "token_labels": {
            "world:WLD": "World (WLD)",
            "lighter:LIT": "Lighter (LIT)",
            "uniswap:UNI": "Uniswap (UNI)",
            "cow:COW": "CoW Protocol (COW)",
        },
        "event_state": "APPROVED",
        "state_verified": True,
        "freshness": "FRESH",
        "public_at": "2026-10-18T12:30:00Z",
        "public_at_prec": "EXACT",
        "known_to_us_at": "2026-10-18T12:31:00Z",
        "known_to_us_at_prec": "EXACT",
        "claims": [
            {
                "subject": "World",
                "predicate": "approved",
                "object": "product",
                "accepted": True,
                "verified": True,
                "state_claim": "APPROVED",
                "evidence": [{"item_id": "source-proof", "quote": "World approved product"}],
            }
        ],
        "beneficiary_map": {
            "no_credible_secondary_reason": "No eligible dependency rail",
            "layers": {
                "ISSUER_PRODUCT": {"note": "Issuer is the primary"},
                "VENUE_DEX": {"note": "No listing identified"},
                "SETTLEMENT_L1_L2": {"note": "No settlement rail identified"},
                "LIQUIDITY_EXECUTION": {"note": "No execution dependency identified"},
                "LENDING_COLLATERAL": {"note": "No lending pool identified"},
                "ORACLE_DATA": {"note": "No oracle dependency identified"},
                "INTEROPERABILITY": {"note": "No cross-chain dependency identified"},
                "CUSTODY_COMPLIANCE": {"note": "No custody provider identified"},
            },
        },
    }
    assessment = {
        "class": "MAJOR",
        "criterion": "M2",
        "certainty": "HIGH",
        "dims": {
            key: {"value": value}
            for key, value in zip(
                [
                    "economic_directness",
                    "magnitude",
                    "permanence",
                    "distribution_expansion",
                    "mechanical_flows",
                    "surprise",
                    "implementation",
                ],
                [8, 7, 6, 5, 4, 3, 2],
                strict=True,
            )
        },
    }
    beneficiary = {
        "token_id": "world:WLD",
        "entity_name": "World",
        "role": "PRIMARY",
        "credible": True,
        "capture_class": "DIRECT",
        "causal_chain": "Required token gas",
        "mechanism_evidence": ["mechanism"],
    }
    decision = {
        "token_id": "world:WLD",
        "action": "WAIT",
        "thesis_grade": "A",
        "trade_grade": "B",
        "outlook": {
            "direction": "BULLISH",
            "potential": "MEDIUM",
            "best_horizon": "H4",
            "horizon_reason": "event window",
            "confidence": "HIGH",
            "scenarios": {
                "base": "Product remains available",
                "upside": "Deployment verified",
                "failure": "Approval reversed",
            },
        },
        "conditions": [
            {"role": "UPGRADE", "human_text": "Spot BUY persists"},
            {"role": "DOWNGRADE", "human_text": "Spot SELL persists"},
        ],
    }
    truth = {
        "token_id": "world:WLD",
        "cutoff": "2026-10-18T12:31:00Z",
        "move_class": "ORGANIC_MIXED",
        "data_confidence": {"class": "HIGH"},
        "primary_window": "W4H",
        "decisive_facts": [
            {"metric": "price", "value": ".05", "unit": "DECIMAL_RETURN", "window": "W4H"},
            {"metric": "spot.cvd", "value": "2000", "unit": "NATIVE_TOKEN", "window": "W4H"},
        ],
        "relative": {
            "repricing_band": "EARLY",
            "windows": {"EVENT": {"r_raw": ".03", "r_btc_adj": ".01"}},
            "market_context": "NEUTRAL",
            "context_inputs": {"btc_return_24h": 0.01, "return_normal_range": [-0.02, 0.03]},
        },
    }
    return event, assessment, beneficiary, decision, truth


def render(inputs):
    e, a, b, d, m = inputs
    return compose_card(
        e,
        a,
        [b],
        [d],
        [m],
        verdict="Approval changes access.",
        why=["The approval is confirmed", "not estimable"],
    )


def test_telegram_emphasis_preserves_text_and_escapes_untrusted_markup() -> None:
    import html
    import re

    from tank.analysis.cards import telegram_card_html

    values = card_inputs()
    values[0]["claims"][0]["object"] = '<a href="https://example.test">claim & proof</a>'
    text = render(values)
    formatted = telegram_card_html(text)
    assert "<b>MARKET TRUTH:</b>" in formatted
    assert "<b>WHAT TO DO:</b>" in formatted
    assert "<a href=" not in formatted
    assert "&lt;a href=" in formatted
    assert html.unescape(re.sub(r"</?b>", "", formatted)) == text


def test_literal_prd_card_template() -> None:
    values = card_inputs()
    card = render(values)
    assert (
        card
        == """MAJOR (M2) — Approval changes access.
Scores D8 · M7 · P6 · X5 · F4 · S3 · I2
Stage: APPROVED · Certainty: HIGH · Data HIGH
Fresh: FRESH · public 12:30Z · known 12:31Z

VERIFIED: World approved product [source in evidence bundle]
NOT VERIFIED: none recorded

WHY IT MATTERS
• What changed: The approval is confirmed
• Economic effect: not estimable
• Token capture: World (WLD) DIRECT — Required token gas

WHO: primary World (WLD) · secondary none — No eligible dependency rail
• Issuer / product: Issuer is the primary
• Exchanges / DEX: No listing identified
• Settlement L1 / L2: No settlement rail identified
• Liquidity / execution: No execution dependency identified
• Lending / collateral: No lending pool identified
• Oracles / data: No oracle dependency identified
• Interoperability: No cross-chain dependency identified
• Custody / compliance: No custody provider identified

── World (WLD) (PRIMARY) ──

MARKET TRUTH: ORGANIC MIXED
• 4h: price 5.00%
• 4h: spot net flow 2000 WLD
Priced: EARLY · EVENT 3.00% raw; 1.00% net BTC

OUTLOOK: BULLISH · MEDIUM · best 4h
• Base: Product remains available
• Upside: Deployment verified
• Failure: Approval reversed

Market context: NEUTRAL — BTC 24h +1.00% vs normal -2.00% to +3.00%
WHAT TO DO: WAIT — Thesis A · Trade B
WHAT CHANGES THE ACTION:
↑ Spot BUY persists
↓ Spot SELL persists

CONFIDENCE: HIGH — main unknown: none recorded"""
    )
    _e, _a, _b, _d, _m = values
    assert "2026-10-18" not in card and "IMPACT:" not in card and "SECONDARIES:" not in card


@pytest.mark.parametrize(
    "exclusion,expected",
    [
        (
            {"reason": "INCOMPLETE", "detail": "FULL_MINUTE_COVERAGE_INCOMPLETE"},
            "full minute coverage incomplete",
        ),
        (
            {"reason": "INCOMPLETE", "detail": "BOUNDARY_AGGREGATE_UNAVAILABLE"},
            "boundary aggregate unavailable",
        ),
        (
            {
                "reason": "INCOMPLETE",
                "detail": "FULL_MINUTE_COVERAGE_INCOMPLETE",
                "source_reasons": ["CURRENT_DAY_ARCHIVE_UNAVAILABLE"],
            },
            "full minute coverage incomplete; current day archive unavailable",
        ),
        ({"reason": "SOURCE_FAILED", "source_reasons": ["PUBLIC_SOURCE_TIMEOUT"]}, "public source timeout"),
        ({"reason": "INCOMPLETE"}, "coverage incomplete; specific cause unavailable"),
    ],
)
def test_excluded_venue_displays_recorded_cause_without_inventing_one(exclusion, expected) -> None:
    values = card_inputs()
    values[4]["spot"] = {"W4H": {"coverage": {"excluded": [dict(exclusion, venue="Bybit")]}}}
    card = render(values)
    assert "Excluded: Bybit — " + expected in card
    assert "Bybit — INCOMPLETE" not in card
    if "CURRENT_DAY_ARCHIVE_UNAVAILABLE" not in exclusion.get("source_reasons", []):
        assert "current day archive" not in card


def test_impact_and_no_secondary_reasons_stay_in_existing_template_fields() -> None:
    values = card_inputs()
    values[1]["impact_horizon"] = {
        "impact_start": "WEEKS",
        "ramp_shape": "GRADUAL",
        "dependencies": ["licence"],
        "timing_confidence": "LOW",
        "time_to_materiality": "Unknown",
    }
    values[0]["beneficiary_map"]["layers"] = {"SETTLEMENT": {"status": "NONE", "note": "No rail evidence"}}
    card = render(values)
    assert "• Economic effect: not estimable" in card
    assert "No eligible dependency rail" in card
    assert "SETTLEMENT: NONE" not in card
    assert values[1]["impact_horizon"]["impact_start"] == "WEEKS"
    assert values[0]["beneficiary_map"]["layers"]["SETTLEMENT"]["note"] == "No rail evidence"
    assert not any(line.startswith(("IMPACT:", "SECONDARIES:", "SETTLEMENT:")) for line in card.splitlines())


@pytest.mark.parametrize(
    "missing,error",
    [
        ("decision", "MISSING_DECISION"),
        ("action", "MISSING_DECISION_ACTION"),
        ("truth", "MISSING_MARKET_TRUTH"),
    ],
)
def test_selected_token_never_gets_a_fabricated_wait(missing, error) -> None:
    e, a, b, d, m = deepcopy(card_inputs())
    if missing == "action":
        d.pop("action")
    with pytest.raises(ValueError, match=error):
        compose_card(e, a, [b], [] if missing == "decision" else [d], [] if missing == "truth" else [m])


def test_inline_code_conditions_are_not_misrepresented_as_current_market_observations() -> None:
    e, a, b, d, m = card_inputs()
    m["spot"] = {"W4H": {"classification": "SINGLE_VENUE", "coverage": {"n_complete": 1}}}
    d["conditions"][1]["human_text"] = (
        "Spot cohort BROAD SELL with most volume selling for two hourly windows; REDUCE."
    )
    card = render((e, a, b, d, m))
    condition_block = card.split("WHAT CHANGES THE ACTION:", 1)[1]
    assert "Spot cohort BROAD SELL" in condition_block
    assert "Spot cohort BROAD SELL" not in next(
        line for line in card.splitlines() if line.startswith("MARKET TRUTH")
    )


def test_missing_fields_are_explained_and_structural_shock_uses_literal_heading() -> None:
    e, a, b, d, m = card_inputs()
    a["class"] = "STRUCTURAL_SHOCK"
    d["outlook"]["potential"] = None
    m["relative"]["repricing_band"] = None
    card = render((e, a, b, d, m))
    assert card.startswith("STRUCTURAL SHOCK (M2) — ")
    assert "Priced: n/a (not available)" in card
    assert "OUTLOOK: BULLISH · n/a (potential unavailable)" in card
    assert "None" not in card


def test_missing_scores_explain_the_gap_without_inventing_an_estimate() -> None:
    values = card_inputs()
    values[1]["dims"].update(
        magnitude={
            "value": None,
            "reason": "NOT_ESTIMABLE",
            "rationale": "No numeric basis in the source",
        },
        mechanical_flows={"value": None, "reason": "NOT_SUPPORTED"},
        surprise={"value": 0, "rationale": "Already announced"},
    )
    original = deepcopy(values)
    card = render(values)
    scores = next(line for line in card.splitlines() if line.startswith("Scores "))
    assert "M n/a (No numeric basis in the source)" in scores
    assert "F n/a (not supported)" in scores
    assert "S0" in scores and "M—" not in scores
    assert values == original


def test_who_excludes_unjustified_links_and_never_repeats_primary_as_secondary() -> None:
    e, a, primary, d, m = card_inputs()
    primary.update(token_id="uniswap:UNI", entity_id="uniswap")
    d["token_id"] = m["token_id"] = primary["token_id"]
    duplicate = dict(primary, role="SECONDARY", layer="VENUE_DEX", integration_state="LIVE")
    candidate = {
        "entity_id": "candidate",
        "role": "SECONDARY",
        "credible": False,
        "tradable": False,
        "integration_state": "COMPATIBLE",
    }
    loser = {
        "entity_id": "OutsideLoser",
        "role": "LOSER",
        "tradable": False,
    }
    e["beneficiary_edges"] = [duplicate, candidate, loser]
    card = compose_card(e, a, [primary, duplicate], [d], [m])
    who = next(line for line in card.splitlines() if line.startswith("WHO:"))
    assert who.count("primary Uniswap (UNI)") == 1
    assert "secondary Uniswap (UNI)" not in who and "candidate" not in who
    assert "loser OutsideLoser" in who

    secondary = dict(
        primary,
        token_id="cow:COW",
        entity_id="cow",
        role="SECONDARY",
        layer="LIQUIDITY_EXECUTION",
        integration_state="LIVE",
    )
    second_decision, second_truth = dict(d, token_id="cow:COW"), dict(m, token_id="cow:COW")
    card = compose_card(e, a, [primary, secondary], [d, second_decision], [m, second_truth])
    who = next(line for line in card.splitlines() if line.startswith("WHO:"))
    assert "secondary CoW Protocol (COW) (LIQUIDITY_EXECUTION, unverified LIVE)" in who
    assert "secondary none" not in who and "No listing identified" not in card


def test_secondary_outside_universe_is_named_without_market_block() -> None:
    """W7 (PRD §4.1): a beneficiary outside the 22 tokens is named on the card,
    with no market block; dependency-map candidates stay unnamed."""
    e, a, primary, d, m = card_inputs()
    outside = {
        "entity_id": "litentry",
        "token_id": "litentry:LIT",
        "role": "SECONDARY",
        "layer": "ISSUER_PRODUCT",
        "credible": False,
        "tradable": False,
        "integration_state": "BUILT",
        "evidence": ["clm-1"],
    }
    candidate = dict(outside, entity_id="candidate", token_id="other:OTH", evidence=[], dependency_ref="x")
    e["beneficiary_edges"] = [outside, candidate]
    card = compose_card(e, a, [primary], [d], [m], why=["a", "b"])
    who = next(line for line in card.splitlines() if line.startswith("WHO:"))
    assert "secondary litentry (LIT) (ISSUER_PRODUCT, unverified BUILT)" in who
    assert "outside the token universe, no market block" in who
    assert "OTH" not in who
    assert card.count("── ") == 1


def test_tokens_are_named_by_project_never_ticker_alone() -> None:
    """W8 (PRD hard rule 16): Lighter and Litentry never both print as "LIT"."""
    from tank.analysis.cards import token_name

    labels = card_inputs()[0]["token_labels"]
    assert token_name("lighter:LIT", labels) == "Lighter (LIT)"
    assert token_name("litentry:LIT", labels) == "litentry (LIT)"
    assert token_name("UNRESOLVED:Foo", labels) == "Foo (unresolved)"
    card = render(card_inputs())
    assert "── World (WLD) (PRIMARY) ──" in card and "── WLD " not in card


@pytest.mark.parametrize(
    "action,shown", [("START", True), ("WAIT", False), ("DONT_CHASE", False), ("REDUCE", False)]
)
def test_tactical_label_only_on_positive_actions(action, shown) -> None:
    """W9 (FR-O-03, 03 §4.8): "(tactical)" never follows WAIT or DON'T CHASE."""
    values = card_inputs()
    values[3].update(action=action, tactical=True, thesis_grade="C")
    line = next(line for line in render(values).splitlines() if line.startswith("WHAT TO DO:"))
    assert ("(tactical)" in line) is shown


def test_missing_card_fields_print_reason_not_dash() -> None:
    """W13 (03 §7.5): missing fields print n/a (<reason>), never a bare dash."""
    values = card_inputs()
    values[4]["decisive_facts"][0].pop("window")
    values[3].pop("thesis_grade")
    values[3]["outlook"].pop("confidence")
    values[1].pop("criterion")
    card = render(values)
    assert "n/a (window unavailable)" in card
    assert "Thesis n/a (grade unavailable)" in card
    assert "CONFIDENCE: n/a (confidence unavailable)" in card
    assert "MAJOR (n/a (criterion unavailable))" in card
    for line in card.splitlines():
        assert not line.rstrip().endswith("—") and "—)" not in line and ": —" not in line


@pytest.mark.parametrize("confirmation", [None, "ABSORPTION_CANDIDATE", "ABSORPTION_CONFIRMED", "flag"])
def test_absorption_candidate_is_uncertain_and_keeps_recorded_alternatives(confirmation) -> None:
    values = card_inputs()
    truth = values[4]
    truth["move_class"] = "PASSIVE_SPOT_ABSORPTION"
    truth["divergence"] = {"state": confirmation, "unresolved": ["Derivative hedging", "Transfers"]}
    if confirmation == "flag":
        truth["absorption_confirmed"] = True
    original = deepcopy(values)
    card = render(values)
    market = card.split("MARKET TRUTH:", 1)[1].split("OUTLOOK:", 1)[0]
    assert "Unresolved: Derivative hedging, Transfers" in market
    assert ("Possible passive spot absorption — unconfirmed" in market) is (
        confirmation in {None, "ABSORPTION_CANDIDATE"}
    )
    assert values == original


@pytest.mark.parametrize("source_reason", ["CURRENT_DAY_ARCHIVE_UNAVAILABLE", "WEBSOCKET_GAP"])
def test_actual_spot_exclusion_cause_reaches_the_card(source_reason) -> None:
    from datetime import timedelta

    from tank.analysis.truth import spot_truth
    from tank.util import iso, parse_time

    values = card_inputs()
    end = parse_time("2026-10-18T12:30:00Z")
    start = end - timedelta(hours=4)
    spot = spot_truth(
        [
            {
                "instrument_id": "bybit:spot:world:WLD",
                "minute_start": iso(start),
                "buy_qty": 1,
                "sell_qty": 1,
                "sum_sq_qty": 2,
                "complete": False,
                "gap_reason": source_reason,
            }
        ],
        [],
        [],
        start,
        end,
    )
    exclusion = spot["coverage"]["excluded"][0]
    assert exclusion["reason"] == "INCOMPLETE"
    assert exclusion["detail"] == "FULL_MINUTE_COVERAGE_INCOMPLETE"
    assert exclusion["source_reasons"] == [source_reason]
    values[4]["spot"] = {"W4H": spot}
    card = render(values)
    assert source_reason.replace("_", " ").lower() in card
    assert "bybit — INCOMPLETE" not in card


@pytest.mark.integration
@pytest.mark.parametrize(
    "verdict,why",
    [
        ("Approval changes access.", ["The approval is confirmed", "not estimable"]),
        (
            "The institution selected the protocol for its planned network.",
            [
                "Selection is approved, not evidence of a live rollout.",
                "No issuance volumes, effective fees or active value-accrual mechanism are established.",
            ],
        ),
        (
            "The agent's judgment is recorded without a prose checker.",
            ["Live usage and token economics remain unverified.", "Economic effect: not estimable"],
        ),
    ],
)
async def test_current_full_publication_uses_literal_render_and_excludes_undecided_token(
    tmp_path: Path, verdict, why
) -> None:
    """Actual publication, storage and delivery with a mocked Telegram transport."""
    import json
    from types import SimpleNamespace

    import httpx

    from tank.delivery import DeliveryService
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.storage import MemoryRepository
    from tank.telegram_rich import rich_message_text

    event, assessment, beneficiary, decision, truth = deepcopy(card_inputs())
    eid, token = "current-card-contract", "world:WLD"
    what = (
        "World approved access to its product. Approval permits the next step; availability is not confirmed."
    )
    now = clock_iso()
    event.update(
        event_id=eid,
        lifecycle="MONITORING",
        current_ems_id="current-ems",
        tokens_mentioned=[token, "ethereum:ETH"],
        public_at=now,
        known_to_us_at=now,
    )
    assessment.update(ems_id="current-ems", event_id=eid)
    beneficiary.update(edge_id="selected-wld", event_id=eid, map_version=1, tradable=True, exposure_score=7)
    extra = dict(
        beneficiary,
        edge_id="undecided-eth",
        token_id="ethereum:ETH",
        entity_name="Ethereum",
        exposure_score=10,
    )
    truth.update(mta_id="current-truth", event_id=eid, cutoff=now)
    decision.update(dec_id="current-decision", event_id=eid, cutoff=now, market_truth_id="current-truth")
    for index, condition in enumerate(decision["conditions"]):
        condition["cond_id"] = f"current-condition-{index}"
    repo = MemoryRepository()
    await repo.open()
    try:
        await repo.put("evt.events", eid, event)
        await repo.put("evt.ems_assessments", "current-ems", assessment)
        await repo.put(
            "evt.beneficiary_maps",
            "current-map",
            {
                "event_id": eid,
                "map_version": 1,
                "analysis_set": [token],
                "no_credible_secondary_reason": "No eligible dependency rail",
                "layers": event["beneficiary_map"]["layers"],
            },
        )
        await repo.put("evt.beneficiary_edges", "selected-wld", beneficiary)
        await repo.put("evt.beneficiary_edges", "undecided-eth", extra)
        await repo.put("ana.market_truth", "current-truth", truth)
        await repo.put("ana.decisions", "current-decision", decision)
        claim = dict(event["claims"][0], claim_id="current-proof", event_id=eid)
        await repo.put("evt.claims", "current-proof", claim)
        await repo.put(
            "ingest.source_items",
            "source-proof",
            {
                "item_id": "source-proof",
                "source_id": "S01",
                "text": "World approved product",
                "observed_at": now,
                "source_class": "PRIMARY",
            },
        )
        service = EventToolService(repo, Settings(data_dir=tmp_path), None, None, versions=fixture_versions())

        await repo.put("ops.agent_sessions", "card-session", {"session_id": "card-session", "event_id": eid})
        result = await service.dispatch(
            "publish_card",
            {
                "event_id": eid,
                "kind": "FULL",
                "what": what,
                "verdict": verdict,
                "why": why,
            },
            {"role": "analyst", "session_id": "card-session", "event_id": eid, "call_id": "current-render"},
        )
        assert result["published"] is True, result
        alert = await repo.get("out.alerts", result["alert_id"] + ":1")
        text = alert["card_text"]
        assert text.splitlines()[0] == "MAJOR (M2) — " + verdict
        assert why[0] in text
        assert why[1].removeprefix("Economic effect: ") in text
        lines = text.splitlines()
        assert alert["shown_tokens"] == [token]
        assert "primary ETH" not in text
        assert "── ethereum:ETH" not in text
        assert len([line for line in lines if line.startswith("──")]) == 1
        assert lines[1] == "WHAT: " + what
        assert lines[2].startswith("Scores ") and lines[3].startswith("Stage: ")
        assert alert["card_struct"]["what"] == what
        assert rich_message_text(alert["rich_message"]).splitlines()[1] == "WHAT: " + what
        bundle = await repo.get("out.evidence_bundles", alert["evidence_bundle_id"])
        assert bundle["payload"]["what"] == what
        assert bundle["payload"]["rich_message"] == alert["rich_message"]
        assert not any(line.startswith(("IMPACT:", "SECONDARIES:")) for line in lines)
        outlook = text.split("OUTLOOK:", 1)[1].split("WHAT TO DO:", 1)[0]
        assert all("• " + name + ": " in outlook for name in ("Base", "Upside", "Failure"))
        assert not any(now in line for line in lines if line.startswith("MARKET TRUTH:"))
        assert len(text) <= 3500 and "linter" not in alert
        assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1
        assert not any(row.get("status") == "SENT" for row in await repo.list("out.deliveries"))
        sent = []

        def send(request: httpx.Request):
            assert request.url.path.endswith("/sendRichMessage")
            sent.append(json.loads(request.content)["rich_message"])
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

        delivery_settings = SimpleNamespace(
            telegram_token="test",
            telegram_alerts_chat="1",
            telegram_ops_chat="2",
            ops_progress_enabled=False,
            replay=False,
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
            delivery = DeliveryService(repo, delivery_settings, http=http)
            await delivery.deliver(
                {"channel": "telegram:alerts", "alert_id": alert["alert_id"], "version": alert["version"]}
            )
        assert sent == [alert["rich_message"]]
        assert rich_message_text(sent[0]).splitlines()[1] == "WHAT: " + what
    finally:
        await repo.close()


def test_header_preserves_agent_supplied_sentence_and_structured_action() -> None:
    event, assessment, beneficiary, decision, truth = card_inputs()
    event["statement"] = "The institution selected the protocol for its planned network."
    verdict = "The institution chose this protocol as infrastructure for its planned network."
    card = compose_card(event, assessment, [beneficiary], [decision], [truth], verdict=verdict)
    assert card.splitlines()[0] == "MAJOR (M2) — " + verdict
    assert "WHAT TO DO: WAIT — Thesis A · Trade B" in card
    assert event["statement"].endswith("planned network.")


@pytest.mark.parametrize(
    "value,precision,expected",
    [
        ("2026-09-24T00:00:00Z", "BUCKET(1d)", "2026-09-24 (day precision)"),
        ("2026-09-24T00:00:00Z", "DAY", "2026-09-24 (day precision)"),
        ("2026-09-24T12:30:00Z", "EXACT", "12:30Z"),
        ("2026-09-24T12:00:00Z", "BUCKET(1h)", "12:00Z–13:00Z (BUCKET(1h), end exclusive)"),
        ("2026-09-24T12:00:00Z", "BUCKET(900s)", "12:00Z–12:15Z (BUCKET(900s), end exclusive)"),
        (
            "2026-09-24T23:00:00Z",
            "BUCKET(2h)",
            "2026-09-24 23:00Z–2026-09-25 01:00Z (BUCKET(2h), end exclusive)",
        ),
        (
            "2026-09-24T00:00:00Z",
            "BUCKET(1w)",
            "2026-09-24 00:00Z–2026-10-01 00:00Z (BUCKET(1w), end exclusive)",
        ),
        ("2026-09-24T12:00:00Z", "BUCKET(5s)", "12:00:00Z–12:00:05Z (BUCKET(5s), end exclusive)"),
        ("2026-09-24T00:00:00Z", None, "n/a (time precision unknown)"),
        (None, None, "n/a (timestamp unavailable)"),
        ("2026-09-24T20:15:00Z", "COLLECTION_ONLY", "n/a (publication time unavailable; collected 20:15Z)"),
    ],
)
def test_public_clock_preserves_declared_precision_without_inventing_intraday_time(
    value, precision, expected
) -> None:
    inputs = card_inputs()
    inputs[0].update(public_at=value, public_at_prec=precision)
    card = render(inputs)
    assert "public " + expected + " · known 12:31Z" in card
    assert "WHAT TO DO: WAIT" in card
    assert len(card) <= 3500


def test_end_labelled_bucket_and_unknown_edge_are_not_reinterpreted() -> None:
    inputs = card_inputs()
    inputs[0].update(
        public_at="2026-09-25T00:00:00Z", public_at_prec="BUCKET(1d)", public_at_bucket_anchor="END"
    )
    assert "public 2026-09-24 (day precision)" in render(inputs)
    inputs[0]["public_at_bucket_anchor"] = "UNSPECIFIED"
    assert "public n/a (bucket edge unknown; BUCKET(1d))" in render(inputs)


def test_nested_precision_and_known_collection_clock_are_preserved_independently() -> None:
    inputs = card_inputs()
    inputs[0].pop("public_at_prec")
    inputs[0]["public_at"] = {"value": "2026-09-24T00:00:00Z", "precision": "BUCKET_1D"}
    inputs[0].update(known_to_us_at="2026-09-24T20:15:00Z", known_to_us_at_prec="COLLECTION_ONLY")
    assert "public 2026-09-24 (day precision) · known 20:15Z (collection)" in render(inputs)


def test_current_claim_view_uses_only_explicit_replacements_and_preserves_full() -> None:
    inputs = card_inputs()
    event = inputs[0]
    event["claims"] = [
        {
            "claim_id": "lead",
            "subject": "World",
            "predicate": "plans",
            "object": "listing",
            "accepted": True,
            "verified": False,
            "verification_missing": "Official confirmation needed",
        },
        {
            "claim_id": "timing",
            "subject": "World",
            "predicate": "launch time",
            "object": "unknown",
            "accepted": True,
            "verified": False,
            "verification_missing": "Effective time needed",
        },
    ]
    historical_full = render(inputs)
    original = deepcopy(event["claims"])
    event["claims"].append(
        {
            "claim_id": "confirmed",
            "subject": "World",
            "predicate": "confirms",
            "object": "listing",
            "accepted": True,
            "verified": True,
            "supersedes_claim_ids": ["lead"],
        }
    )
    update = render(inputs)
    assert "Official confirmation needed" not in update
    assert "Effective time needed" in update
    assert "World confirms listing" in update
    assert "Official confirmation needed" in historical_full
    assert event["claims"][:2] == original
    event["claims"].append(
        {
            "claim_id": "latest",
            "subject": "World",
            "predicate": "reconfirms",
            "object": "listing",
            "accepted": True,
            "verified": True,
            "supersedes_claim_ids": ["confirmed"],
        }
    )
    assert "World confirms listing" not in render(inputs)
    event["claims"][-1]["accepted"] = False
    assert "World confirms listing" in render(inputs)
    event["claims"][2].pop("supersedes_claim_ids")
    assert "Official confirmation needed" in render(inputs)


def test_each_layer_reason_is_visible_without_generic_rule_reference_or_mutation() -> None:
    inputs = card_inputs()
    event = inputs[0]
    layers = {
        "ISSUER_PRODUCT": {"status": "UNKNOWN", "note": "Issuer proof absent"},
        "VENUE_DEX": {"status": "NONE", "note": "No venue evidence"},
    }
    prefix = "No SECONDARY edge satisfies the credibility requirements in §4.7. "
    summary = prefix.rstrip()
    event["beneficiary_map"] = {"layers": layers, "no_credible_secondary_reason": summary}
    original = deepcopy(inputs)
    text = render(inputs)
    assert text.count("Issuer proof absent") == 1
    assert text.count("No venue evidence") == 1
    assert summary not in text and "§4.7" not in text
    who = text.split("WHO:", 1)[1].split("──", 1)[0]
    assert len([line for line in who.splitlines() if line.startswith("• ")]) == 8
    assert who.count("n/a (layer assessment not supplied)") == 6
    assert inputs == original
    event["beneficiary_map"]["no_credible_secondary_reason"] = "Additional independent uncertainty"
    assert "Additional independent uncertainty" in render(inputs)
    event["beneficiary_map"]["layers"] = {}
    assert "Additional independent uncertainty" in render(inputs)


def test_card_length_diagnostics_preserve_full_preview_and_structured_guidance() -> None:
    import json

    inputs = card_inputs()
    inputs[0]["claims"][0]["object"] = "Unchanged material uncertainty " * 170
    original = deepcopy(inputs)
    with pytest.raises(ValueError, match="CARD_LENGTH") as caught:
        render(inputs)
    diagnostic = json.loads(str(caught.value).split(": ", 1)[1])
    assert diagnostic["max"] == 5000
    assert diagnostic["actual"] == len(diagnostic["preview"])
    assert diagnostic["excess"] == diagnostic["actual"] - 5000
    assert (
        sum(s["chars"] for s in diagnostic["sections"]) + diagnostic["newline_chars"] == diagnostic["actual"]
    )
    assert inputs[0]["claims"][0]["object"] in diagnostic["preview"]
    assert "publish-only shortening may be insufficient" in diagnostic["guidance"]
    assert any("supersedes_claim_ids" in f for f in diagnostic["editable_fields"])
    assert inputs == original
    inputs[0]["thresholds"] = {"card_max_chars": 100000}
    assert render(inputs) == diagnostic["preview"]


def test_market_truth_names_venue_oi_class_markup_and_spot_breadth() -> None:
    """F-11 (03 §11.3): per-venue OI class with the markup component and the
    spot venues on each side are printed, each with venue and window."""
    values = card_inputs()
    truth = values[4]
    truth["derivatives"] = {
        "window": "W4H",
        "per_venue": [
            {
                "coverage": {"venue": "binance"},
                "oi_class": "REAL_NATIVE",
                "oi_native_chg_pct": 0.1037,
                "decomposition": {"markup": "6530100.97", "pos_share": 0.5734},
            },
            {"coverage": {"venue": "bybit"}, "oi_class": None},
        ],
    }
    truth["spot"] = {
        "W4H": {
            "window": "W4H",
            "venues": [
                {"venue": "binance", "complete": True, "sign": 0, "volume_share": 0.656},
                {"venue": "kraken", "complete": True, "sign": 1, "volume_share": 0.028},
            ],
        }
    }
    market = render(values).split("MARKET TRUTH:", 1)[1].split("OUTLOOK:", 1)[0]
    assert "4h: open interest by venue: binance REAL_NATIVE +10.37% in WLD (position share 57.3%" in market
    assert "price-markup component present" in market and "bybit" not in market
    assert "4h: spot venues net buying: kraken 2.8%; net selling: none" in market


def test_no_event_premise_prints_scenario_reason_and_no_accumulation_word() -> None:
    """03 §7.5: the template never names a missing event and never writes
    "accumulation" (L11) in its own condition texts."""
    from tank.analysis.decisions import DecisionEngine

    truth = {
        "token_id": "T",
        "move_class": "UNKNOWN",
        "data_confidence": {"class": "HIGH"},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL"},
    }
    result = DecisionEngine().evaluate({}, {}, {"token_id": "T", "role": "UNKNOWN"}, truth)
    outlook = result["outlook"]
    assert outlook["scenarios"]["failure"] is None
    assert outlook["scenario_reasons"]["failure"] == "no event premise"
    assert all("accumulation" not in c["human_text"].lower() for c in result["conditions"])


def test_market_context_and_trade_notch_are_printed() -> None:
    """F-16 (03 §5.7, §4.9): STRESS with its BTC input, and the notch reason."""
    values = card_inputs()
    values[4]["relative"].update(
        market_context="STRESS",
        context_inputs={"btc_return_24h": -0.0322, "return_normal_range": [-0.0239, 0.0487]},
    )
    values[3].update(trade_rule="B", trade_grade="C", trade_reasons=["TRADE_BASE_B", "CONTEXT_STRESS"])
    card = render(values)
    assert (
        "Market context: STRESS — BTC 24h -3.22% vs normal -2.39% to +4.87% · Trade B → C for market context STRESS"
        in card
    )
    values[3].update(trade_rule="D", trade_grade="D", trade_reasons=["TRADE_BASE_D"])
    assert "· Trade not lowered: already D" in render(values)


def test_falling_native_oi_is_stated_as_positions_closing() -> None:
    """F-14 (03 §11.3): native OI down is positions closing, in native units."""
    values = card_inputs()
    values[4]["derivatives"] = {
        "window": "EVENT",
        "synchronized": {"oi_native_chg_pct": -0.3036, "oi_class": "MIXED", "native_oi_vs_normal": "BELOW"},
        "per_venue": [],
    }
    values[4]["relative"]["windows"]["EVENT"]["r_raw"] = "0.54"
    market = render(values).split("MARKET TRUTH:", 1)[1].split("OUTLOOK:", 1)[0]
    assert (
        "event: open positions (synchronized native OI) -30.36% in WLD while price rose +54.00%: positions closing"
        in market
    )


@pytest.mark.parametrize(
    "vs_normal,ending",
    [("WITHIN", "; within its normal range"), (None, "; normal range n/a (not available)")],
)
def test_native_oi_fall_inside_its_range_is_not_called_positions_closing(vs_normal, ending) -> None:
    """F-09 (r106): a native OI fall within its normal range is printed as a
    figure, not as positions closing (that wording needs BELOW, F-14)."""
    values = card_inputs()
    values[4]["derivatives"] = {
        "window": "W4H",
        "synchronized": {"oi_native_chg_pct": -0.0085, "oi_class": "MIXED", "native_oi_vs_normal": vs_normal},
        "per_venue": [],
    }
    market = render(values).split("MARKET TRUTH:", 1)[1].split("OUTLOOK:", 1)[0]
    line = next(line for line in market.splitlines() if "open positions" in line)
    assert line.endswith(ending) and "positions closing" not in line


def test_btc_priced_line_names_the_benchmark_instead_of_a_missing_net_move() -> None:
    """BTC is the benchmark: Market Truth reports r_btc_adj n/a (NOT_APPLICABLE_BENCHMARK)."""
    values = card_inputs()
    values[4]["relative"]["windows"]["EVENT"] = {
        "r_raw": ".0003",
        "r_btc_adj": None,
        "r_btc_adj_reason": "NOT_APPLICABLE_BENCHMARK",
    }
    card = render(values)
    assert "Priced: EARLY · EVENT 0.03% raw (BTC is the benchmark)\n" in card
    assert "net BTC" not in card
    values[4]["relative"]["windows"]["EVENT"] = {"r_raw": ".03", "r_btc_adj": None}
    assert "Priced: EARLY · EVENT 3.00% raw; n/a (not available) net BTC" in render(values)


@pytest.mark.parametrize(
    "data,certainty,move,expected",
    [
        (
            "HIGH",
            "MEDIUM",
            "SPOT_LED_ACCUMULATION",
            "Event certainty MEDIUM: not confirmed by a HIGH-certainty source",
        ),
        ("HIGH", "MEDIUM", "UNKNOWN", "Move class unresolved: no class eligible (UNKNOWN)"),
        ("HIGH", "HIGH", "SPOT_LED_ACCUMULATION", "none recorded"),
    ],
)
def test_confidence_names_the_limiting_input(data, certainty, move, expected) -> None:
    """E2E #5 msg 19: CONFIDENCE LOW printed "none recorded". §6.5 sets confidence
    as min(data, certainty, class); the card names the input at the minimum
    (data -> certainty -> class), "none recorded" only when HIGH."""
    from tank.analysis.decisions import DecisionEngine

    truth = {
        "token_id": "world:WLD",
        "move_class": move,
        "data_confidence": {"class": data},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL"},
        "liquidity": {"liquidity_state": "NORMAL"},
    }
    decision = DecisionEngine().evaluate(
        {"event_id": "e", "statement": "s", "event_state": "APPROVED", "state_verified": True},
        {"class": "MAJOR", "certainty": certainty},
        {"token_id": "world:WLD", "role": "PRIMARY", "credible": True, "capture_class": "DIRECT"},
        truth,
    )
    values = card_inputs()
    values[3].update(decision)
    values[4].update(truth)
    line = next(line for line in render(values).splitlines() if line.startswith("CONFIDENCE:"))
    assert line.endswith("main unknown: " + expected), line
    assert decision["confidence_limiting"]["input"] == (
        None if expected == "none recorded" else "certainty" if "certainty" in expected else "class"
    )


@pytest.mark.parametrize("kind", ["PARTIAL", "FULL", "UPDATE"])
def test_what_precedes_analysis_and_preserves_every_other_section(kind) -> None:
    import html
    import re

    from tank.analysis.cards import telegram_card_html
    from tank.telegram_rich import card_rich_message, rich_message_text

    event, assessment, beneficiary, decision, truth = card_inputs()
    args = (event, assessment, [beneficiary], [decision], [truth])
    wording = {
        "verdict": "External liquidity is available.",
        "why": ["Access expanded", "Effect not quantified"],
    }
    what = "Uniswap добавил обмены через внешние источники ликвидности. Предложения доступны через приложение & API <v4>."
    original = compose_card(*args, kind=kind, **wording)
    card = compose_card(*args, kind=kind, what=what, **wording)
    assert card.splitlines()[1] == "WHAT: " + what
    assert card.replace("WHAT: " + what + "\n", "", 1) == original
    formatted = telegram_card_html(card)
    assert "<b>WHAT:</b> " + html.escape(what) in formatted
    assert html.unescape(re.sub(r"</?b>", "", formatted)) == card
    rich = card_rich_message(card, assessment)
    assert rich["blocks"][1] == {
        "type": "paragraph",
        "text": [{"type": "bold", "text": "WHAT:"}, " " + what],
    }
    assert rich["blocks"][2]["type"] == "table"
    assert rich_message_text(rich).splitlines()[1] == "WHAT: " + what


def test_what_counts_toward_card_limit_without_truncation() -> None:
    import json

    from tank.telegram_rich import card_rich_message, rich_message_text

    event, assessment, beneficiary, decision, truth = card_inputs()
    args = (event, assessment, [beneficiary], [decision], [truth])
    original = compose_card(*args)
    what = "The protocol now connects users to external trading venues."
    event["thresholds"] = {"card_max_chars": len(original)}
    with pytest.raises(ValueError, match="CARD_LENGTH") as caught:
        compose_card(*args, what=what)
    diagnostics = json.loads(str(caught.value).removeprefix("CARD_LENGTH: "))
    assert diagnostics["excess"] == len("WHAT: " + what + "\n")
    assert "WHAT: " + what in diagnostics["preview"]
    assert "publish_card.what / verdict / why" in diagnostics["editable_fields"]
    assert any(section["section"] == "WHAT" for section in diagnostics["sections"])
    event.pop("thresholds")
    rich_limit = len(rich_message_text(card_rich_message(original, assessment)))
    with pytest.raises(ValueError, match="CARD_LENGTH"):
        card_rich_message(compose_card(*args, what=what), assessment, max_chars=rich_limit)


def test_nested_day_bucket_refuses_uncaptured_timezone() -> None:
    inputs = card_inputs()
    inputs[0].pop("public_at_prec")
    inputs[0]["public_at"] = {"value": "2026-09-24", "precision": "BUCKET_1D"}
    with pytest.raises(ValueError, match="timezone"):
        render(inputs)

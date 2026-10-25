"""Literal PRD §7.1 card layout and detailed exclusion rendering."""

from copy import deepcopy
from pathlib import Path
from typing import Literal

import pytest
from foundation_helpers import array_field, object_field, present, text_field
from pydantic import JsonValue
from source_support import fixture_request_budgets, fixture_versions

from tank.analysis.card_codec import card_inputs_from_records
from tank.analysis.card_contracts import (
    ActiveCardDecision,
    CardInputs,
    CardSubject,
    PublicationKind,
    PublicationNarrative,
    PublicationPolicy,
)
from tank.analysis.cards import compose_card
from tank.analysis.decision_contracts import Action, Confidence, MoveClass
from tank.util import clock_iso

type WireRecord = dict[str, JsonValue]


def wire_card_inputs() -> tuple[WireRecord, WireRecord, WireRecord, WireRecord, WireRecord]:
    event: WireRecord = {
        "statement": "Approval changes access.",
        "certainty": "HIGH",
        "contradictions": [],
        "beneficiary_edges": [],
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
                "claim_id": "approval",
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
            "least_repriced_edge": None,
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
    assessment: WireRecord = {
        "class": "MAJOR",
        "contradictions": [],
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
    beneficiary: WireRecord = {
        "token_id": "world:WLD",
        "entity_name": "World",
        "role": "PRIMARY",
        "credible": True,
        "capture_class": "DIRECT",
        "layer": "ISSUER_PRODUCT",
        "entity_id": "world",
        "evidence": ["approval"],
        "integration_state": None,
        "integration_state_evidence": [],
        "causal_chain": "Required token gas",
        "mechanism_evidence": ["mechanism"],
    }
    decision: WireRecord = {
        "token_id": "world:WLD",
        "action": "WAIT",
        "thesis_grade": "A",
        "trade_grade": "B",
        "trade_rule": "B",
        "trade_reasons": [],
        "tactical": False,
        "outlook": {
            "direction": "BULLISH",
            "potential": "MEDIUM",
            "best_horizon": "H4",
            "horizon_reason": "event window",
            "confidence": "HIGH",
            "confidence_limiting": {"input": None, "text": None},
            "scenario_reasons": {"base": None, "upside": None, "failure": None},
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
    truth: WireRecord = {
        "token_id": "world:WLD",
        "cutoff": "2026-10-18T12:31:00Z",
        "move_class": "ORGANIC_MIXED",
        "data_confidence": {"class": "HIGH", "limiting": None},
        "divergence": {"state": "NONE", "unresolved": []},
        "alternatives": [],
        "derivatives": {"window": "W4H", "per_venue": [], "synchronized": None},
        "spot": {"W4H": {"window": "W4H", "venues": [], "coverage": {"excluded": []}}},
        "supply": {"holders_revenue": {"protocols": []}},
        "primary_window": "W4H",
        "decisive_facts": [
            "4h: price 5.00%",
            "4h: spot net flow 2000 WLD",
        ],
        "relative": {
            "repricing_band": "EARLY",
            "windows": {"EVENT": {"r_raw": ".03", "r_btc_adj": ".01", "r_btc_adj_reason": None}},
            "market_context": "NEUTRAL",
            "context_inputs": {"btc_return_24h": 0.01, "return_normal_range": [-0.02, 0.03]},
        },
    }
    return event, assessment, beneficiary, decision, truth


def card_inputs() -> CardInputs:
    event, assessment, beneficiary, decision, truth = wire_card_inputs()
    return card_inputs_from_records(
        event,
        assessment,
        [beneficiary],
        [decision],
        [truth],
        narrative=PublicationNarrative(
            kind="FULL",
            what="World approved access to the product.",
            verdict="Approval changes access.",
            why=("The approval is confirmed", "not estimable"),
        ),
        policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
    )


def active_decision(values: CardInputs) -> ActiveCardDecision:
    decision = values.decisions_by_token["world:WLD"]
    if not isinstance(decision, ActiveCardDecision):
        raise ValueError("The authored template fixture requires its active decision")
    return decision


def render(inputs: CardInputs) -> str:
    return compose_card(inputs)


def test_telegram_emphasis_preserves_text_and_escapes_untrusted_markup() -> None:
    import html
    import re

    from tank.analysis.cards import telegram_card_html

    values = card_inputs()
    claim = values.event.claims[0].model_copy(
        update={"object_text": '<a href="https://example.test">claim & proof</a>'}
    )
    event = values.event.model_copy(update={"claims": (claim,)})
    text = render(values.model_copy(update={"event": event}))
    formatted = telegram_card_html(text)
    assert "<b>MARKET TRUTH:</b>" in formatted
    assert "<b>WHAT TO DO:</b>" in formatted
    assert "<a href=" not in formatted and "&lt;a href=" in formatted
    assert html.unescape(re.sub(r"</?b>", "", formatted)) == text


def test_literal_prd_card_template() -> None:
    values = card_inputs()
    card = render(values)
    assert (
        card
        == """MAJOR (M2) — Approval changes access.
WHAT: World approved access to the product.
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
def test_excluded_venue_displays_recorded_cause_without_inventing_one(
    exclusion: dict[str, JsonValue], expected: str
) -> None:
    from tank.analysis.card_contracts import CardExclusion
    from tank.util import canonical_json

    values = card_inputs()
    row = CardExclusion.model_validate_json(
        canonical_json(
            {
                "venue": "Bybit",
                "reason": exclusion["reason"],
                "detail": exclusion.get("detail"),
                "source_reasons": exclusion.get("source_reasons", []),
                "missing_metrics": [],
            }
        )
    )
    truth = values.market_by_token["world:WLD"]
    truth = truth.model_copy(update={"spot": truth.spot.model_copy(update={"excluded": (row,)})})
    card = render(values.model_copy(update={"market_by_token": {"world:WLD": truth}}))
    assert "Excluded: Bybit — " + expected in card
    assert "Bybit — INCOMPLETE" not in card
    if "CURRENT_DAY_ARCHIVE_UNAVAILABLE" not in row.source_reasons:
        assert "current day archive" not in card


def test_impact_and_no_secondary_reasons_stay_in_existing_template_fields() -> None:
    values = card_inputs()
    notes = dict(values.mapping.layer_explanations)
    notes["SETTLEMENT_L1_L2"] = "No rail evidence"
    mapping = values.mapping.model_copy(update={"layer_explanations": notes})
    values = values.model_copy(update={"mapping": mapping})
    card = render(values)
    assert "• Economic effect: not estimable" in card
    assert "No eligible dependency rail" in card and "• Settlement L1 / L2: No rail evidence" in card
    assert not any(line.startswith(("IMPACT:", "SECONDARIES:", "SETTLEMENT:")) for line in card.splitlines())
    assert values.mapping.layer_explanations["SETTLEMENT_L1_L2"] == "No rail evidence"


@pytest.mark.parametrize(
    "missing,error",
    [
        ("decision", "MISSING_DECISION"),
        ("action", "action"),
        ("truth", "MISSING_MARKET_TRUTH"),
    ],
)
def test_selected_token_never_gets_a_fabricated_wait(missing: str, error: str) -> None:
    values = card_inputs()
    if missing == "decision":
        values = values.model_copy(update={"decisions_by_token": {}})
    elif missing == "truth":
        values = values.model_copy(update={"market_by_token": {}})
    else:
        decision = active_decision(values).model_copy(update={"action": None})
        values = values.model_copy(update={"decisions_by_token": {"world:WLD": decision}})
    with pytest.raises(ValueError, match=error):
        render(values)


def test_inline_code_conditions_are_not_misrepresented_as_current_market_observations() -> None:
    from tank.analysis.card_contracts import CardCondition

    values = card_inputs()
    decision = active_decision(values).model_copy(
        update={
            "conditions": (
                CardCondition(
                    role="DOWNGRADE",
                    human_text="Spot cohort BROAD SELL with most volume selling for two hourly windows; REDUCE.",
                ),
            )
        }
    )
    card = render(values.model_copy(update={"decisions_by_token": {"world:WLD": decision}}))
    condition_block = card.split("WHAT CHANGES THE ACTION:", 1)[1]
    assert "Spot cohort BROAD SELL" in condition_block
    assert "Spot cohort BROAD SELL" not in next(
        line for line in card.splitlines() if line.startswith("MARKET TRUTH")
    )


def test_missing_fields_are_explained_and_structural_shock_uses_literal_heading() -> None:
    values = card_inputs()
    assessment = values.assessment.model_copy(update={"classification": "STRUCTURAL_SHOCK"})
    truth = values.market_by_token["world:WLD"]
    truth = truth.model_copy(update={"relative": truth.relative.model_copy(update={"repricing": None})})
    card = render(
        values.model_copy(update={"assessment": assessment, "market_by_token": {"world:WLD": truth}})
    )
    assert card.startswith("STRUCTURAL SHOCK (M2) — ")
    assert "Priced: n/a (not available)" in card and "None" not in card


def test_missing_scores_explain_the_gap_without_inventing_an_estimate() -> None:
    from tank.contracts.score import ScoreDimensionView

    values = card_inputs()
    dimensions = values.assessment.scores.dims.model_copy(
        update={
            "magnitude": ScoreDimensionView(
                value=None, reason="NOT_ESTIMABLE", rationale="No numeric basis in the source"
            ),
            "mechanical_flows": ScoreDimensionView(value=None, reason="NOT_SUPPORTED"),
            "surprise": ScoreDimensionView(value=0, rationale="Already announced"),
        }
    )
    scores = values.assessment.scores.model_copy(update={"dims": dimensions})
    values = values.model_copy(update={"assessment": values.assessment.model_copy(update={"scores": scores})})
    original = values.model_copy(deep=True)
    card = render(values)
    summary = next(line for line in card.splitlines() if line.startswith("Scores "))
    assert "M n/a (No numeric basis in the source)" in summary
    assert "F n/a (not supported)" in summary and "S0" in summary and "M—" not in summary
    assert values == original


def test_who_excludes_unjustified_links_and_never_repeats_primary_as_secondary() -> None:
    values = card_inputs()
    primary = values.beneficiaries[0]
    duplicate = primary.subject.model_copy(
        update={"role": "SECONDARY", "layer": "VENUE_DEX", "integration_state": "LIVE"}
    )
    candidate = CardSubject(
        token_id=None,
        entity_id="candidate",
        entity_name=None,
        role="SECONDARY",
        credible=False,
        outside_scope=None,
        layer="VENUE_DEX",
        integration_state="COMPATIBLE",
        integration_verified=False,
    )
    loser = candidate.model_copy(
        update={"entity_id": "OutsideLoser", "role": "LOSER", "integration_state": None}
    )
    values = values.model_copy(update={"other_subjects": (duplicate, candidate, loser)})
    who = next(line for line in render(values).splitlines() if line.startswith("WHO:"))
    assert who.count("primary World (WLD)") == 1
    assert "secondary World (WLD)" not in who and "candidate" not in who and "loser OutsideLoser" in who
    secondary = primary.subject.model_copy(
        update={
            "token_id": "cow:COW",
            "entity_id": "cow",
            "entity_name": "CoW Protocol",
            "role": "SECONDARY",
            "layer": "LIQUIDITY_EXECUTION",
            "integration_state": "LIVE",
        }
    )
    labels = dict(values.event.token_labels)
    labels["cow:COW"] = "CoW Protocol (COW)"
    values = values.model_copy(
        update={
            "event": values.event.model_copy(update={"token_labels": labels}),
            "other_subjects": (secondary, candidate, loser),
        }
    )
    card = render(values)
    who = next(line for line in card.splitlines() if line.startswith("WHO:"))
    assert "secondary CoW Protocol (COW) (LIQUIDITY_EXECUTION, unverified LIVE)" in who
    assert "secondary none" not in who and "No listing identified" not in card


def test_secondary_outside_universe_is_named_without_market_block() -> None:
    values = card_inputs()
    outside = CardSubject(
        token_id="litentry:LIT",
        entity_id="litentry",
        entity_name=None,
        role="SECONDARY",
        credible=False,
        outside_scope="UNREGISTERED_TOKEN",
        layer="ISSUER_PRODUCT",
        integration_state="BUILT",
        integration_verified=False,
    )
    candidate = outside.model_copy(
        update={"token_id": "other:OTH", "entity_id": "candidate", "outside_scope": None}
    )
    values = values.model_copy(update={"other_subjects": (outside, candidate)})
    card = render(values)
    who = next(line for line in card.splitlines() if line.startswith("WHO:"))
    assert "secondary litentry (LIT) (ISSUER_PRODUCT, unverified BUILT)" in who
    assert "outside the token universe, no market block" in who and "OTH" not in who
    assert card.count("── ") == 1


def test_tokens_are_named_by_project_never_ticker_alone() -> None:
    from tank.analysis.cards import token_name

    values = card_inputs()
    labels = dict(values.event.token_labels)
    labels["lighter:LIT"] = "Lighter (LIT)"
    assert token_name("lighter:LIT", labels) == "Lighter (LIT)"
    assert token_name("litentry:LIT", labels) == "litentry (LIT)"
    assert token_name("UNRESOLVED:Foo", labels) == "Foo (unresolved)"
    card = render(values)
    assert "── World (WLD) (PRIMARY) ──" in card and "── WLD " not in card


@pytest.mark.parametrize(
    "action,shown", [("START", True), ("WAIT", False), ("DONT_CHASE", False), ("REDUCE", False)]
)
def test_tactical_label_only_on_positive_actions(action: Action, shown: bool) -> None:
    values = card_inputs()
    decision = active_decision(values).model_copy(
        update={"action": action, "tactical": True, "thesis_grade": "C"}
    )
    line = next(
        line
        for line in render(
            values.model_copy(update={"decisions_by_token": {"world:WLD": decision}})
        ).splitlines()
        if line.startswith("WHAT TO DO:")
    )
    assert ("(tactical)" in line) is shown


def test_missing_card_fields_print_reason_not_dash() -> None:
    values = card_inputs()
    for name in ("thesis_grade", "trade_grade"):
        decision = active_decision(values).model_copy(update={name: None})
        with pytest.raises(ValueError):
            render(values.model_copy(update={"decisions_by_token": {"world:WLD": decision}}))
    with pytest.raises(ValueError):
        render(
            values.model_copy(update={"assessment": values.assessment.model_copy(update={"criterion": ""})})
        )


@pytest.mark.parametrize("confirmation", [None, "ABSORPTION_CANDIDATE", "ABSORPTION_CONFIRMED", "flag"])
def test_absorption_candidate_is_uncertain_and_keeps_recorded_alternatives(confirmation: str | None) -> None:
    values = card_inputs()
    confirmed = confirmation in {"ABSORPTION_CONFIRMED", "flag"}
    truth = values.market_by_token["world:WLD"].model_copy(
        update={
            "move": "PASSIVE_SPOT_ABSORPTION",
            "absorption_confirmed": confirmed,
            "unresolved": ("Derivative hedging", "Transfers"),
        }
    )
    values = values.model_copy(update={"market_by_token": {"world:WLD": truth}})
    original = values.model_copy(deep=True)
    card = render(values)
    market = card.split("MARKET TRUTH:", 1)[1].split("OUTLOOK:", 1)[0]
    assert "Unresolved: Derivative hedging, Transfers" in market
    assert ("Possible passive spot absorption — unconfirmed" in market) is (not confirmed)
    assert values == original


@pytest.mark.parametrize("source_reason", ["CURRENT_DAY_ARCHIVE_UNAVAILABLE", "WEBSOCKET_GAP"])
def test_actual_spot_exclusion_cause_reaches_the_card(source_reason: str) -> None:
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
    from tank.analysis.card_contracts import CardExclusion
    from tank.util import canonical_json

    projected = CardExclusion.model_validate_json(
        canonical_json(
            {
                "venue": exclusion["venue"],
                "reason": exclusion["reason"],
                "detail": exclusion["detail"],
                "source_reasons": exclusion["source_reasons"],
                "missing_metrics": exclusion.get("missing_metrics", []),
            }
        )
    )
    truth = values.market_by_token["world:WLD"]
    truth = truth.model_copy(update={"spot": truth.spot.model_copy(update={"excluded": (projected,)})})
    card = render(values.model_copy(update={"market_by_token": {"world:WLD": truth}}))
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
    tmp_path: Path, verdict: str, why: list[str]
) -> None:
    """Actual publication, storage and delivery with a mocked Telegram transport."""
    from contextlib import AsyncExitStack

    import httpx
    from scheduler_support import operational_reaction_services

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.page_access import LivePageAccess
    from tank.contracts.json import decode_json
    from tank.delivery import DeliveryService
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.storage import MemoryRepository
    from tank.telegram_rich import rich_message_text

    event, assessment, beneficiary, decision, truth = deepcopy(wire_card_inputs())
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
    for index, condition in enumerate(array_field(decision, "conditions")):
        assert isinstance(condition, dict)
        condition["cond_id"] = f"current-condition-{index}"
    repo = MemoryRepository()
    market_http_calls = 0

    async def record_market_request(_request: httpx.Request) -> None:
        nonlocal market_http_calls
        market_http_calls += 1

    async with AsyncExitStack() as resources:
        await repo.open()
        resources.push_async_callback(repo.close)
        settings = Settings(data_dir=tmp_path, _env_file=None)
        versions = fixture_versions()
        budgets = fixture_request_budgets()
        resources.push_async_callback(budgets.close)
        services = await resources.enter_async_context(
            operational_reaction_services(repo, settings, versions=versions, budgets=budgets)
        )
        market, truth_service, scheduler = services.market, services.truth, services.scheduler
        market_http = market.http
        market_http.event_hooks["request"].append(record_market_request)
        await repo.put("evt.events", eid, event)
        await repo.put("evt.ems_assessments", "current-ems", assessment)
        map_record: WireRecord = {
            "event_id": eid,
            "map_version": 1,
            "analysis_set": [token],
            "no_credible_secondary_reason": "No eligible dependency rail",
            "layers": object_field(event, "beneficiary_map")["layers"],
        }
        await repo.put("evt.beneficiary_maps", "current-map", map_record)
        await repo.put("evt.beneficiary_edges", "selected-wld", beneficiary)
        await repo.put("evt.beneficiary_edges", "undecided-eth", extra)
        await repo.put("ana.market_truth", "current-truth", truth)
        await repo.put("ana.decisions", "current-decision", decision)
        first_claim = array_field(event, "claims")[0]
        assert isinstance(first_claim, dict)
        claim: WireRecord = dict(first_claim, claim_id="current-proof", event_id=eid)
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
        service = EventToolService(
            repo,
            settings,
            truth_service,
            DecisionEngine(),
            versions=versions,
            scheduler=scheduler,
            pages=LivePageAccess(services.admission),
            http=market_http,
        )

        await repo.put("ops.agent_sessions", "card-session", {"session_id": "card-session", "event_id": eid})
        result: WireRecord = await service.dispatch(
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
        alert = present(await repo.get("out.alerts", text_field(result, "alert_id") + ":1"))
        text = text_field(alert, "card_text")
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
        assert object_field(alert, "card_struct")["what"] == what
        assert rich_message_text(object_field(alert, "rich_message")).splitlines()[1] == "WHAT: " + what
        bundle = present(await repo.get("out.evidence_bundles", text_field(alert, "evidence_bundle_id")))
        payload = object_field(bundle, "payload")
        assert payload["what"] == what
        assert payload["rich_message"] == alert["rich_message"]
        assert not any(line.startswith(("IMPACT:", "SECONDARIES:")) for line in lines)
        outlook = text.split("OUTLOOK:", 1)[1].split("WHAT TO DO:", 1)[0]
        assert all("• " + name + ": " in outlook for name in ("Base", "Upside", "Failure"))
        assert not any(now in line for line in lines if line.startswith("MARKET TRUTH:"))
        assert len(text) <= 3500 and "linter" not in alert
        assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1
        assert not any(row.get("status") == "SENT" for row in await repo.list("out.deliveries"))
        sent: list[WireRecord] = []

        def send(request: httpx.Request) -> httpx.Response:
            assert request.url.path.endswith("/sendRichMessage")
            wire = decode_json(request.content)
            assert isinstance(wire, dict)
            sent.append(object_field(wire, "rich_message"))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

        delivery_settings = Settings(
            telegram_token="test",
            telegram_alerts_chat="1",
            telegram_ops_chat="2",
            ops_progress_enabled=False,
            replay=False,
            _env_file=None,
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
            delivery = DeliveryService(repo, delivery_settings, http=http)
            await delivery.deliver(
                {"channel": "telegram:alerts", "alert_id": alert["alert_id"], "version": alert["version"]}
            )
        assert sent == [alert["rich_message"]]
        assert rich_message_text(sent[0]).splitlines()[1] == "WHAT: " + what
    assert scheduler.closing and not scheduler.background
    assert market_http.is_closed
    assert market_http_calls == 0


def test_header_preserves_agent_supplied_sentence_and_structured_action() -> None:
    values = card_inputs()
    verdict = "The institution selected the protocol for its planned network."
    values = values.model_copy(update={"narrative": values.narrative.model_copy(update={"verdict": verdict})})
    card = render(values)
    assert card.splitlines()[0] == "MAJOR (M2) — " + verdict
    assert "WHAT TO DO: WAIT — Thesis A · Trade B" in card


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
    value: str | None, precision: str | None, expected: str
) -> None:
    from tank.time_precision import chosen_boundary, propagate_timestamp

    values = card_inputs()
    stamp = (
        chosen_boundary(value, precision=precision)
        if value is not None and precision is not None and precision != "COLLECTION_ONLY"
        else propagate_timestamp({"public_at": value, "public_at_prec": precision}, "public_at")
    )
    values = values.model_copy(update={"event": values.event.model_copy(update={"public_at": stamp})})
    card = render(values)
    assert "public " + expected + " · known 12:31Z" in card
    assert "WHAT TO DO: WAIT" in card and len(card) <= 3500


def test_end_labelled_bucket_and_unknown_edge_are_not_reinterpreted() -> None:
    from tank.time_precision import Timestamp

    values = card_inputs()
    stamp = Timestamp(value="2026-09-25T00:00:00Z", precision="BUCKET(1d)", bucket_anchor="END")
    event = values.event.model_copy(update={"public_at": stamp})
    assert "public 2026-09-24 (day precision)" in render(values.model_copy(update={"event": event}))
    stamp = stamp.model_copy(update={"bucket_anchor": "UNSPECIFIED"})
    event = values.event.model_copy(update={"public_at": stamp})
    assert "public n/a (bucket edge unknown; BUCKET(1d))" in render(
        values.model_copy(update={"event": event})
    )


def test_nested_precision_and_known_collection_clock_are_preserved_independently() -> None:
    from tank.time_precision import propagate_timestamp

    values = card_inputs()
    public = propagate_timestamp(
        {"public_at": {"value": "2026-09-24T00:00:00Z", "precision": "BUCKET_1D", "bucket_anchor": "START"}},
        "public_at",
    )
    known = propagate_timestamp(
        {"known_to_us_at": "2026-09-24T20:15:00Z", "known_to_us_at_prec": "COLLECTION_ONLY"}, "known_to_us_at"
    )
    event = values.event.model_copy(update={"public_at": public, "known_to_us_at": known})
    assert "public 2026-09-24 (day precision) · known 20:15Z (collection)" in render(
        values.model_copy(update={"event": event})
    )


def test_current_claim_view_uses_only_explicit_replacements_and_preserves_full() -> None:
    event, assessment, beneficiary, decision, truth = wire_card_inputs()
    narrative = card_inputs().narrative
    policy = card_inputs().policy
    lead: WireRecord = {
        "claim_id": "lead",
        "subject": "World",
        "predicate": "plans",
        "object": "listing",
        "accepted": True,
        "verified": False,
        "verification_missing": "Official confirmation needed",
        "evidence": [],
    }
    timing: WireRecord = {
        "claim_id": "timing",
        "subject": "World",
        "predicate": "launch time",
        "object": "unknown",
        "accepted": True,
        "verified": False,
        "verification_missing": "Effective time needed",
        "evidence": [],
    }
    claims: list[JsonValue] = [lead, timing]
    event["claims"] = claims

    def current() -> str:
        return render(
            card_inputs_from_records(
                event, assessment, [beneficiary], [decision], [truth], narrative=narrative, policy=policy
            )
        )

    historical_full = current()
    original = deepcopy(claims)
    confirmed: WireRecord = {
        "claim_id": "confirmed",
        "subject": "World",
        "predicate": "confirms",
        "object": "listing",
        "accepted": True,
        "verified": True,
        "evidence": [{"item_id": "official-confirmation"}],
        "supersedes_claim_ids": ["lead"],
    }
    claims.append(confirmed)
    update = current()
    assert "Official confirmation needed" not in update and "Effective time needed" in update
    assert "World confirms listing" in update and "Official confirmation needed" in historical_full
    assert claims[:2] == original
    latest: WireRecord = {
        "claim_id": "latest",
        "subject": "World",
        "predicate": "reconfirms",
        "object": "listing",
        "accepted": True,
        "verified": True,
        "evidence": [{"item_id": "official-reconfirmation"}],
        "supersedes_claim_ids": ["confirmed"],
    }
    claims.append(latest)
    assert "World confirms listing" not in current()
    latest["accepted"] = False
    assert "World confirms listing" in current()
    del confirmed["supersedes_claim_ids"]
    assert "Official confirmation needed" in current()


def test_each_layer_reason_is_visible_without_generic_rule_reference_or_mutation() -> None:
    values = card_inputs()
    notes = dict(values.mapping.layer_explanations)
    notes["ISSUER_PRODUCT"] = "Issuer proof absent"
    notes["VENUE_DEX"] = "No venue evidence"
    summary = "No SECONDARY edge satisfies the credibility requirements in §4.7."
    values = values.model_copy(
        update={
            "mapping": values.mapping.model_copy(
                update={"layer_explanations": notes, "no_credible_secondary_reason": summary}
            )
        }
    )
    original = values.model_copy(deep=True)
    text = render(values)
    assert text.count("Issuer proof absent") == 1 and text.count("No venue evidence") == 1
    assert summary not in text and "§4.7" not in text
    who = text.split("WHO:", 1)[1].split("──", 1)[0]
    assert len([line for line in who.splitlines() if line.startswith("• ")]) == 8
    assert "layer assessment not supplied" not in who and values == original
    mapping = values.mapping.model_copy(
        update={"no_credible_secondary_reason": "Additional independent uncertainty"}
    )
    assert "Additional independent uncertainty" in render(values.model_copy(update={"mapping": mapping}))
    with pytest.raises(ValueError):
        render(values.model_copy(update={"mapping": mapping.model_copy(update={"layer_explanations": {}})}))


def test_card_length_diagnostics_preserve_full_preview_and_structured_guidance() -> None:
    from tank.analysis.card_contracts import CardLengthDiagnostics

    values = card_inputs()
    claim = values.event.claims[0].model_copy(update={"object_text": "Unchanged material uncertainty " * 170})
    values = values.model_copy(update={"event": values.event.model_copy(update={"claims": (claim,)})})
    original = values.model_copy(deep=True)
    with pytest.raises(ValueError, match="CARD_LENGTH") as caught:
        render(values)
    diagnostic = CardLengthDiagnostics.model_validate_json(str(caught.value).removeprefix("CARD_LENGTH: "))
    assert diagnostic.max_chars == 5000 and diagnostic.actual == len(diagnostic.preview)
    assert diagnostic.excess == diagnostic.actual - 5000
    assert (
        sum(section.chars for section in diagnostic.sections) + diagnostic.newline_chars == diagnostic.actual
    )
    assert (
        claim.object_text in diagnostic.preview
        and "publish-only shortening may be insufficient" in diagnostic.guidance
    )
    assert any("supersedes_claim_ids" in field for field in diagnostic.editable_fields) and values == original
    policy = values.policy.model_copy(update={"card_max_chars": 100000})
    assert render(values.model_copy(update={"policy": policy})) == diagnostic.preview


def test_market_truth_names_venue_oi_class_markup_and_spot_breadth() -> None:
    from decimal import Decimal

    from tank.analysis.card_contracts import CardOiAttribution, CardOiVenue, CardSpotVenue

    values = card_inputs()
    truth = values.market_by_token["world:WLD"]
    venue = CardOiVenue(
        venue="binance",
        classification="REAL_NATIVE",
        native_change=Decimal(".1037"),
        attribution=CardOiAttribution(price_markup=Decimal("6530100.97"), position_share=Decimal(".5734")),
    )
    derivatives = truth.derivatives.model_copy(update={"venues": (venue,)})
    spot = truth.spot.model_copy(
        update={
            "complete_venues": (
                CardSpotVenue(venue="binance", sign=0, volume_share=Decimal(".656")),
                CardSpotVenue(venue="kraken", sign=1, volume_share=Decimal(".028")),
            )
        }
    )
    truth = truth.model_copy(update={"derivatives": derivatives, "spot": spot})
    market = (
        render(values.model_copy(update={"market_by_token": {"world:WLD": truth}}))
        .split("MARKET TRUTH:", 1)[1]
        .split("OUTLOOK:", 1)[0]
    )
    assert "4h: open interest by venue: binance REAL_NATIVE +10.37% in WLD (position share 57.3%" in market
    assert "price-markup component present" in market and "bybit" not in market
    assert "4h: spot venues net buying: kraken 2.8%; net selling: none" in market


def test_no_event_premise_prints_scenario_reason_and_no_accumulation_word() -> None:
    from test_decision_contracts import decision_inputs_fixture

    from tank.analysis.decisions import DecisionEngine

    inputs = decision_inputs_fixture()
    inputs = inputs.model_copy(update={"event": inputs.event.model_copy(update={"has_premise": False})})
    result = DecisionEngine().evaluate(inputs)
    assert result.outlook.scenarios.failure is None
    assert result.outlook.scenario_reasons.failure == "no event premise"
    assert all("accumulation" not in condition.human_text.lower() for condition in result.conditions)


def test_market_context_and_trade_notch_are_printed() -> None:
    from decimal import Decimal

    from tank.analysis.card_contracts import CardMarketContext
    from tank.analysis.statistics_contracts import DecimalBounds

    values = card_inputs()
    truth = values.market_by_token["world:WLD"]
    context = CardMarketContext(
        state="STRESS",
        btc_return_24h=Decimal("-.0322"),
        normal_return_range=DecimalBounds(lower=Decimal("-.0239"), upper=Decimal(".0487")),
    )
    truth = truth.model_copy(update={"relative": truth.relative.model_copy(update={"context": context})})
    decision = active_decision(values).model_copy(
        update={
            "raw_trade_grade": "B",
            "trade_grade": "C",
            "trade_reasons": ("TRADE_BASE_B", "CONTEXT_STRESS"),
        }
    )
    values = values.model_copy(
        update={"market_by_token": {"world:WLD": truth}, "decisions_by_token": {"world:WLD": decision}}
    )
    assert (
        "Market context: STRESS — BTC 24h -3.22% vs normal -2.39% to +4.87% · Trade B → C for market context STRESS"
        in render(values)
    )
    decision = decision.model_copy(
        update={"raw_trade_grade": "D", "trade_grade": "D", "trade_reasons": ("TRADE_BASE_D",)}
    )
    assert "· Trade not lowered: already D" in render(
        values.model_copy(update={"decisions_by_token": {"world:WLD": decision}})
    )


def test_falling_native_oi_is_stated_as_positions_closing() -> None:
    from decimal import Decimal

    from tank.analysis.card_contracts import CardReturns, CardSynchronizedOi

    values = card_inputs()
    truth = values.market_by_token["world:WLD"]
    derivatives = truth.derivatives.model_copy(
        update={
            "window": "EVENT",
            "synchronized": CardSynchronizedOi(native_change=Decimal("-.3036"), native_vs_normal="BELOW"),
        }
    )
    returns = CardReturns(raw=Decimal(".54"), btc_adjusted=None, btc_adjusted_reason="NOT_ESTIMABLE")
    truth = truth.model_copy(
        update={
            "derivatives": derivatives,
            "relative": truth.relative.model_copy(update={"returns_by_window": {"EVENT": returns}}),
        }
    )
    market = (
        render(values.model_copy(update={"market_by_token": {"world:WLD": truth}}))
        .split("MARKET TRUTH:", 1)[1]
        .split("OUTLOOK:", 1)[0]
    )
    assert (
        "event: open positions (synchronized native OI) -30.36% in WLD while price rose +54.00%: positions closing"
        in market
    )


@pytest.mark.parametrize(
    "vs_normal,ending",
    [("WITHIN", "; within its normal range"), (None, "; normal range n/a (not available)")],
)
def test_native_oi_fall_inside_its_range_is_not_called_positions_closing(
    vs_normal: Literal["WITHIN"] | None, ending: str
) -> None:
    from decimal import Decimal

    from tank.analysis.card_contracts import CardSynchronizedOi

    values = card_inputs()
    truth = values.market_by_token["world:WLD"]
    truth = truth.model_copy(
        update={
            "derivatives": truth.derivatives.model_copy(
                update={
                    "synchronized": CardSynchronizedOi(
                        native_change=Decimal("-.0085"), native_vs_normal=vs_normal
                    )
                }
            )
        }
    )
    market = (
        render(values.model_copy(update={"market_by_token": {"world:WLD": truth}}))
        .split("MARKET TRUTH:", 1)[1]
        .split("OUTLOOK:", 1)[0]
    )
    line = next(line for line in market.splitlines() if "open positions" in line)
    assert line.endswith(ending) and "positions closing" not in line


def test_btc_priced_line_names_the_benchmark_instead_of_a_missing_net_move() -> None:
    from decimal import Decimal

    from tank.analysis.card_contracts import CardReturns

    values = card_inputs()
    truth = values.market_by_token["world:WLD"]
    point = CardReturns(
        raw=Decimal(".0003"), btc_adjusted=None, btc_adjusted_reason="NOT_APPLICABLE_BENCHMARK"
    )
    truth = truth.model_copy(
        update={"relative": truth.relative.model_copy(update={"returns_by_window": {"EVENT": point}})}
    )
    card = render(values.model_copy(update={"market_by_token": {"world:WLD": truth}}))
    assert "Priced: EARLY · EVENT 0.03% raw (BTC is the benchmark)\n" in card and "net BTC" not in card
    point = point.model_copy(update={"raw": Decimal(".03"), "btc_adjusted_reason": None})
    truth = truth.model_copy(
        update={"relative": truth.relative.model_copy(update={"returns_by_window": {"EVENT": point}})}
    )
    assert "Priced: EARLY · EVENT 3.00% raw; n/a (not available) net BTC" in render(
        values.model_copy(update={"market_by_token": {"world:WLD": truth}})
    )


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
def test_confidence_names_the_limiting_input(
    data: Confidence, certainty: Confidence, move: MoveClass, expected: str
) -> None:
    from test_decision_contracts import decision_inputs_fixture

    from tank.analysis.decisions import DecisionEngine

    inputs = decision_inputs_fixture()
    market = inputs.market.model_copy(
        update={
            "move": move,
            "data_confidence": data,
            "class_confidence": "LOW" if move == "UNKNOWN" else "HIGH",
        }
    )
    inputs = inputs.model_copy(
        update={"event": inputs.event.model_copy(update={"certainty": certainty}), "market": market}
    )
    evaluated = DecisionEngine().evaluate(inputs)
    values = card_inputs()
    decision = active_decision(values)
    decision = decision.model_copy(
        update={
            "outlook": decision.outlook.model_copy(
                update={
                    "confidence": evaluated.outlook.confidence,
                    "limitation": evaluated.confidence_limiting,
                }
            )
        }
    )
    truth = values.market_by_token["world:WLD"]
    truth = truth.model_copy(
        update={"move": move, "confidence": truth.confidence.model_copy(update={"classification": data})}
    )
    values = values.model_copy(
        update={"decisions_by_token": {"world:WLD": decision}, "market_by_token": {"world:WLD": truth}}
    )
    line = next(line for line in render(values).splitlines() if line.startswith("CONFIDENCE:"))
    assert line.endswith("main unknown: " + expected), line
    assert evaluated.confidence_limiting.input == (
        None if expected == "none recorded" else "certainty" if "certainty" in expected else "class"
    )


@pytest.mark.parametrize("kind", ["PARTIAL", "FULL", "UPDATE"])
def test_what_precedes_analysis_and_preserves_every_other_section(kind: PublicationKind) -> None:
    import html
    import re

    from foundation_helpers import array_field

    from tank.analysis.cards import telegram_card_html
    from tank.telegram_rich import card_rich_message, rich_message_text

    values = card_inputs()
    narrative = values.narrative.model_copy(
        update={
            "kind": kind,
            "verdict": "External liquidity is available.",
            "why": ("Access expanded", "Effect not quantified"),
        }
    )
    values = values.model_copy(update={"narrative": narrative})
    original = render(values)
    what = "Uniswap добавил обмены через внешние источники ликвидности. Предложения доступны через приложение & API <v4>."
    card = render(values.model_copy(update={"narrative": narrative.model_copy(update={"what": what})}))
    assert card.splitlines()[1] == "WHAT: " + what
    assert card.replace("WHAT: " + what, "WHAT: " + narrative.what, 1) == original
    formatted = telegram_card_html(card)
    assert "<b>WHAT:</b> " + html.escape(what) in formatted
    assert html.unescape(re.sub(r"</?b>", "", formatted)) == card
    rich = card_rich_message(card, values.assessment.scores)
    blocks = array_field(rich, "blocks")
    assert blocks[1] == {"type": "paragraph", "text": [{"type": "bold", "text": "WHAT:"}, " " + what]}
    table = blocks[2]
    assert isinstance(table, dict) and table["type"] == "table"
    assert rich_message_text(rich).splitlines()[1] == "WHAT: " + what


def test_what_counts_toward_card_limit_without_truncation() -> None:
    from tank.analysis.card_contracts import CardLengthDiagnostics
    from tank.telegram_rich import card_rich_message, rich_message_text

    values = card_inputs()
    original = render(values)
    what = values.narrative.what + " The approved product remains documented."
    narrative = values.narrative.model_copy(update={"what": what})
    policy = values.policy.model_copy(update={"card_max_chars": len(original)})
    with pytest.raises(ValueError, match="CARD_LENGTH") as caught:
        render(values.model_copy(update={"narrative": narrative, "policy": policy}))
    diagnostic = CardLengthDiagnostics.model_validate_json(str(caught.value).removeprefix("CARD_LENGTH: "))
    assert diagnostic.excess == len(what) - len(values.narrative.what)
    assert (
        "WHAT: " + what in diagnostic.preview
        and "publish_card.what / verdict / why" in diagnostic.editable_fields
    )
    assert any(section.section == "WHAT" for section in diagnostic.sections)
    rich_limit = len(rich_message_text(card_rich_message(original, values.assessment.scores)))
    with pytest.raises(ValueError, match="CARD_LENGTH"):
        card_rich_message(
            render(values.model_copy(update={"narrative": narrative})),
            values.assessment.scores,
            max_chars=rich_limit,
        )


def test_nested_day_bucket_refuses_uncaptured_timezone() -> None:
    from tank.time_precision import propagate_timestamp

    with pytest.raises(ValueError, match="timezone"):
        propagate_timestamp({"public_at": {"value": "2026-09-24", "precision": "BUCKET_1D"}}, "public_at")

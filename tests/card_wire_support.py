"""Typed authored card records shared by presentation behavior tests."""

from card_registry_support import fixture_registry_membership
from pydantic import JsonValue

from tank.analysis.card_codec import card_inputs_from_records
from tank.analysis.card_contracts import CardInputs, PublicationNarrative, PublicationPolicy

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
        registry=fixture_registry_membership(),
        narrative=PublicationNarrative(
            kind="FULL",
            what="World approved access to the product.",
            verdict="Approval changes access.",
            why=("The approval is confirmed", "not estimable"),
        ),
        policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
    )

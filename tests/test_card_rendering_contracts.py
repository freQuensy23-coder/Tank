from decimal import Decimal

import pytest
from card_registry_support import fixture_registry_membership
from pydantic import JsonValue, ValidationError

from tank.analysis.card_contracts import (
    CardInputs,
    CardOiAttribution,
    CardOiVenue,
    CardRevenue,
    CardRevenueChartPoint,
    ChartRevenueProtocol,
)
from tank.analysis.cards import compose_card, telegram_card_html
from tank.time_precision import Timestamp
from tank.util import canonical_json


def rendering_inputs() -> CardInputs:
    dimension: JsonValue = {"value": 7, "rationale": None, "reason": None}
    values: dict[str, JsonValue] = {
        "registry": fixture_registry_membership().model_dump(mode="json"),
        "narrative": {
            "kind": "FULL",
            "what": "World approved the product.",
            "verdict": "The product was approved.",
            "why": ["Approval changes access.", "Usage remains unmeasured."],
        },
        "policy": {"card_max_chars": 5000, "update_rate_limit_s": 900},
        "event": {
            "statement": "World approved the product.",
            "state": "APPROVED",
            "state_verified": True,
            "certainty": "HIGH",
            "freshness": "FRESH",
            "public_at": {
                "value": "2026-10-18T12:30:00Z",
                "precision": "EXACT",
                "precision_reason": None,
                "bucket_anchor": None,
            },
            "known_to_us_at": {
                "value": "2026-10-18T12:31:00Z",
                "precision": "COLLECTION_ONLY",
                "precision_reason": None,
                "bucket_anchor": None,
            },
            "claims": [
                {
                    "claim_id": "approval",
                    "subject": "World",
                    "predicate": "approved",
                    "object_text": "the product",
                    "verified": True,
                    "verification_gap": None,
                    "evidence": [{"item_id": "proof", "url": "https://example.com/approval"}],
                }
            ],
            "token_labels": {"world:WLD": "World (WLD)"},
            "evidence_labels": {},
            "has_expired_call_fact": False,
        },
        "assessment": {
            "classification": "MAJOR",
            "criterion": "M2",
            "scores": {
                "dims": {
                    name: dimension
                    for name in (
                        "economic_directness",
                        "magnitude",
                        "permanence",
                        "distribution_expansion",
                        "mechanical_flows",
                        "surprise",
                        "implementation",
                    )
                }
            },
        },
        "beneficiaries": [
            {
                "subject": {
                    "token_id": "world:WLD",
                    "entity_id": "world",
                    "entity_name": "World",
                    "role": "PRIMARY",
                    "credible": True,
                    "registry_membership": "REGISTERED",
                    "outside_evidence_accepted": True,
                    "layer": "ISSUER_PRODUCT",
                    "integration_state": None,
                    "integration_verified": False,
                },
                "capture": "DIRECT",
                "causal_chain": "Required token gas",
                "least_repriced": False,
            }
        ],
        "other_subjects": [],
        "mapping": {
            "layer_explanations": {
                "ISSUER_PRODUCT": "The issuer is primary.",
                "VENUE_DEX": "No listing was identified.",
                "SETTLEMENT_L1_L2": "No settlement dependency was identified.",
                "LIQUIDITY_EXECUTION": "No execution dependency was identified.",
                "LENDING_COLLATERAL": "No lending pool was identified.",
                "ORACLE_DATA": "No oracle dependency was identified.",
                "INTEROPERABILITY": "No cross-chain dependency was identified.",
                "CUSTODY_COMPLIANCE": "No custody dependency was identified.",
            },
            "no_credible_secondary_reason": "No eligible rail was identified.",
        },
        "decisions_by_token": {
            "world:WLD": {
                "kind": "ACTIVE",
                "token_id": "world:WLD",
                "action": "WAIT",
                "thesis_grade": "A",
                "trade_grade": "B",
                "raw_trade_grade": "B",
                "trade_reasons": [],
                "tactical": False,
                "outlook": {
                    "direction": "BULLISH",
                    "potential": "MEDIUM",
                    "horizon": None,
                    "confidence": "HIGH",
                    "limitation": {"input": None, "text": None},
                    "scenarios": {
                        "base": "Product remains available",
                        "upside": "Usage is verified",
                        "failure": "Approval is reversed",
                        "base_reason": None,
                        "upside_reason": None,
                        "failure_reason": None,
                    },
                },
                "conditions": [],
            }
        },
        "market_by_token": {
            "world:WLD": {
                "token_id": "world:WLD",
                "move": "UNKNOWN",
                "absorption_confirmed": False,
                "unresolved": [],
                "confidence": {"classification": "HIGH", "limiting": None},
                "decisive_facts": ["1h: spot net buying was observed"],
                "relative": {
                    "repricing": None,
                    "returns_by_window": {},
                    "context": {"state": None, "btc_return_24h": None, "normal_return_range": None},
                },
                "derivatives": {"window": "W1H", "venues": [], "synchronized": None},
                "spot": {"window": "W1H", "complete_venues": [], "excluded": []},
                "holders_revenue": None,
            }
        },
    }
    return CardInputs.model_validate_json(canonical_json(values))


def test_typed_card_keeps_supplied_prose_and_unavailable_horizon() -> None:
    text = compose_card(rendering_inputs())
    assert text.startswith("MAJOR (M2) — The product was approved.\nWHAT: World approved the product.")
    assert "World approved the product [example.com]" in text
    assert "known 12:31Z (collection)" in text
    assert "best n/a (horizon unavailable)" in text
    assert "WHAT TO DO: WAIT — Thesis A · Trade B" in text
    assert "best 24h" not in text
    assert "<b>WHAT:</b> &lt;unsafe&gt;" in telegram_card_html("headline\nWHAT: <unsafe>")


def test_zero_markup_does_not_claim_price_markup_component() -> None:
    inputs = rendering_inputs()
    market = inputs.market_by_token["world:WLD"]
    row = CardOiVenue(
        venue="Binance",
        classification="REAL_NATIVE",
        native_change=Decimal("0.03"),
        attribution=CardOiAttribution(position_share=Decimal(1), price_markup=Decimal(0)),
    )
    derivative = market.derivatives.model_copy(update={"venues": (row,)})
    market = market.model_copy(update={"derivatives": derivative})
    inputs = inputs.model_copy(update={"market_by_token": {"world:WLD": market}})
    text = compose_card(inputs)
    assert "position share 100.0%" in text
    assert "price-markup component present" not in text
    nonzero = row.model_copy(
        update={"attribution": CardOiAttribution(position_share=Decimal("0.6"), price_markup=Decimal("0.1"))}
    )
    market = market.model_copy(update={"derivatives": derivative.model_copy(update={"venues": (nonzero,)})})
    assert "price-markup component present" in compose_card(
        inputs.model_copy(update={"market_by_token": {"world:WLD": market}})
    )


def test_revenue_uses_declared_seconds_without_inventing_observation_precision() -> None:
    inputs = rendering_inputs()
    market = inputs.market_by_token["world:WLD"]
    revenue = CardRevenue(
        protocols=(
            ChartRevenueProtocol(
                kind="CHART",
                protocol="World",
                unit="USD",
                point=CardRevenueChartPoint(epoch_seconds=Decimal(0), value=Decimal("12.5")),
            ),
        ),
        observed_at=Timestamp(
            value="2026-10-18T12:00:00Z", precision=None, precision_reason="PROVIDER_TIME_PRECISION_UNKNOWN"
        ),
        stale=False,
        complete_24h=False,
    )
    market = market.model_copy(update={"holders_revenue": revenue})
    text = compose_card(inputs.model_copy(update={"market_by_token": {"world:WLD": market}}))
    assert "12.5 USD chart point 1970-01-01 00:00Z (period unknown)" in text
    assert "partial 24h coverage" in text
    assert "observed 2026-10-18" not in text


def test_invalid_nested_model_copy_is_rejected_before_rendering() -> None:
    inputs = rendering_inputs()
    bad = inputs.narrative.model_copy(update={"verdict": " "})
    with pytest.raises(ValidationError):
        compose_card(inputs.model_copy(update={"narrative": bad}))


def publication_wire() -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    event: dict[str, JsonValue] = {
        "statement": "World approved the product.",
        "event_state": "APPROVED",
        "state_verified": True,
        "certainty": "HIGH",
        "freshness": "FRESH",
        "lifecycle": "ANALYZING",
        "contradictions": [],
        "public_at": "2026-10-18T12:30:00Z",
        "public_at_prec": "EXACT",
        "known_to_us_at": "2026-10-18T12:31:00Z",
        "known_to_us_at_prec": "COLLECTION_ONLY",
        "claims": [],
        "token_labels": {"world:WLD": "World (WLD)"},
        "beneficiary_edges": [],
        "beneficiary_map": {"least_repriced_edge": None, "layers": {}, "no_credible_secondary_reason": None},
    }
    assessment: dict[str, JsonValue] = {
        "class": "MAJOR",
        "criterion": "M2",
        "contradictions": [],
        "dims": {
            name: {"value": 7}
            for name in (
                "economic_directness",
                "magnitude",
                "permanence",
                "distribution_expansion",
                "mechanical_flows",
                "surprise",
                "implementation",
            )
        },
    }
    return event, assessment


def test_projection_requires_authored_facts_without_repairing_headline() -> None:
    from tank.analysis.card_codec import card_inputs_from_records
    from tank.analysis.card_contracts import PublicationNarrative, PublicationPolicy

    event, assessment = publication_wire()
    narrative = PublicationNarrative(
        kind="PARTIAL",
        what="World approved the product.",
        verdict="The product was approved.",
        why=("Approval changes access.", "Usage remains unmeasured."),
    )
    policy = PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900)
    projected = card_inputs_from_records(
        event,
        assessment,
        [],
        [],
        [],
        registry=fixture_registry_membership(),
        narrative=narrative,
        policy=policy,
    )
    text = compose_card(projected)
    assert "Market analysis pending" in text
    assert "MAJOR (M2) — The product was approved." in text
    del event["certainty"]
    with pytest.raises(ValueError, match="Missing required card fact: certainty"):
        card_inputs_from_records(
            event,
            assessment,
            [],
            [],
            [],
            registry=fixture_registry_membership(),
            narrative=narrative,
            policy=policy,
        )


def test_projection_never_borrows_rejected_claim_as_verified_evidence() -> None:
    from tank.analysis.card_codec import card_inputs_from_records
    from tank.analysis.card_contracts import PublicationNarrative, PublicationPolicy

    event, assessment = publication_wire()
    event["claims"] = [
        {
            "claim_id": "withdrawn",
            "subject": "World",
            "predicate": "deployed",
            "object": "the product",
            "accepted": False,
            "verified": True,
            "evidence": [{"item_id": "withdrawn-proof"}],
        },
        {
            "claim_id": "current",
            "subject": "World",
            "predicate": "proposed",
            "object": "the product",
            "accepted": True,
            "verified": False,
            "verification_missing": "Deployment is not confirmed.",
            "evidence": [],
        },
    ]
    narrative = PublicationNarrative(
        kind="PARTIAL",
        what="The proposal is recorded.",
        verdict="The product was proposed.",
        why=("The proposal changes intended access.", "Deployment remains unverified."),
    )
    projected = card_inputs_from_records(
        event,
        assessment,
        [],
        [],
        [],
        registry=fixture_registry_membership(),
        narrative=narrative,
        policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
    )
    text = compose_card(projected)
    assert "VERIFIED: n/a (no verified claims)" in text
    assert "World proposed the product — Deployment is not confirmed." in text
    assert "World deployed" not in text


def test_cooldown_projection_requires_exact_acknowledged_revision() -> None:
    from tank.analysis.card_codec import publication_baseline_from_records

    event, assessment = publication_wire()
    alert: dict[str, JsonValue] = {
        "alert_id": "alert",
        "version": 2,
        "card_struct": {"event": event, "assessment": assessment},
        "created_at": "2026-10-18T12:00:00Z",
    }
    absent = publication_baseline_from_records(alert, [], sent_delivery=None)
    assert absent.acknowledged_send is None
    sent: dict[str, JsonValue] = {
        "alert_id": "alert",
        "version": 2,
        "status": "SENT",
        "channel": "telegram:alerts",
        "at": "2026-10-18T12:40:00Z",
        "at_prec": "COLLECTION_ONLY",
    }
    acknowledged = publication_baseline_from_records(alert, [], sent_delivery=sent).acknowledged_send
    assert acknowledged is not None
    assert acknowledged.stamp.value == "2026-10-18T12:40:00Z"
    assert acknowledged.stamp.precision == "COLLECTION_ONLY"
    sent["version"] = 1
    with pytest.raises(ValueError, match="different alert revision"):
        publication_baseline_from_records(alert, [], sent_delivery=sent)
    sent["version"] = 2
    sent["status"] = "PENDING"
    with pytest.raises(ValueError, match="acknowledged Telegram SENT"):
        publication_baseline_from_records(alert, [], sent_delivery=sent)


def test_length_refusal_preserves_full_untruncated_preview() -> None:
    from tank.contracts.json import decode_json

    inputs = rendering_inputs()
    policy = inputs.policy.model_copy(update={"card_max_chars": 100})
    expected = compose_card(inputs)
    with pytest.raises(ValueError, match="CARD_LENGTH") as failure:
        compose_card(inputs.model_copy(update={"policy": policy}))
    diagnostic = decode_json(str(failure.value).removeprefix("CARD_LENGTH: "))
    assert isinstance(diagnostic, dict)
    assert diagnostic["preview"] == expected
    assert diagnostic["actual"] == len(expected)
    assert diagnostic["excess"] == len(expected) - 100


def test_calendar_bucket_and_unknown_precision_do_not_gain_intraday_precision() -> None:
    inputs = rendering_inputs()
    day = Timestamp(value="2026-10-18T00:00:00Z", precision="BUCKET(1d)", bucket_anchor="START")
    event = inputs.event.model_copy(update={"public_at": day})
    text = compose_card(inputs.model_copy(update={"event": event}))
    assert "public 2026-10-18 (day precision)" in text
    unknown = Timestamp(
        value="2026-10-18T12:30:00Z", precision=None, precision_reason="PROVIDER_PRECISION_UNKNOWN"
    )
    event = inputs.event.model_copy(update={"public_at": unknown})
    text = compose_card(inputs.model_copy(update={"event": event}))
    assert "public n/a (time precision unknown)" in text
    assert "public 12:30Z" not in text
    with pytest.raises(ValidationError):
        Timestamp(value="2026-10-18", precision="BUCKET(1d)", bucket_anchor="START")


def test_missing_no_secondary_layer_fact_is_refused_instead_of_padded() -> None:
    inputs = rendering_inputs()
    from tank.analysis.card_contracts import CardMapping

    original = inputs.mapping
    assert isinstance(original, CardMapping)
    notes = dict(original.layer_explanations)
    del notes["LENDING_COLLATERAL"]
    mapping = original.model_copy(update={"layer_explanations": notes})
    with pytest.raises(ValidationError, match="actual eight layer assessments"):
        compose_card(inputs.model_copy(update={"mapping": mapping}))


def test_prior_gate_projection_does_not_require_unused_current_state_facts() -> None:
    from tank.analysis.card_codec import publication_baseline_from_records

    alert: dict[str, JsonValue] = {
        "alert_id": "older-card",
        "version": 1,
        "card_struct": {
            "event": {"event_state": "APPROVED", "state_verified": True, "contradictions": []},
            "assessment": {"class": "MAJOR", "contradictions": []},
        },
    }
    baseline = publication_baseline_from_records(alert, [], sent_delivery=None)
    assert baseline.event.state == "APPROVED" and baseline.event.state_verified
    assert baseline.acknowledged_send is None


def test_duplicate_selected_token_cannot_repeat_a_financial_action_block() -> None:
    inputs = rendering_inputs()
    repeated = inputs.model_copy(update={"beneficiaries": inputs.beneficiaries * 2})
    with pytest.raises(ValidationError, match="repeat a selected beneficiary token"):
        compose_card(repeated)


def test_primary_alias_cannot_mask_missing_secondary_layer_evidence() -> None:
    inputs = rendering_inputs()
    duplicate = inputs.beneficiaries[0].subject.model_copy(update={"role": "SECONDARY"})
    mapping = inputs.mapping.model_copy(update={"layer_explanations": {}})
    invalid = inputs.model_copy(update={"other_subjects": (duplicate,), "mapping": mapping})
    with pytest.raises(ValidationError, match="actual eight layer assessments"):
        compose_card(invalid)


def test_duplicate_claim_identity_is_refused_without_picking_an_authority() -> None:
    from tank.analysis.card_codec import card_inputs_from_records
    from tank.analysis.card_contracts import PublicationNarrative, PublicationPolicy

    event, assessment = publication_wire()
    claim: dict[str, JsonValue] = {
        "claim_id": "same",
        "subject": "World",
        "predicate": "proposed",
        "object": "the product",
        "accepted": True,
        "verified": False,
        "evidence": [],
    }
    event["claims"] = [claim, dict(claim, object="a different product")]
    narrative = PublicationNarrative(
        kind="PARTIAL",
        what="The proposal is recorded.",
        verdict="The product was proposed.",
        why=("The proposal changes intended access.", "Deployment remains unverified."),
    )
    with pytest.raises(ValueError, match="Duplicate displayed claim identity"):
        card_inputs_from_records(
            event,
            assessment,
            [],
            [],
            [],
            registry=fixture_registry_membership(),
            narrative=narrative,
            policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
        )


def test_outside_subject_evidence_requires_a_current_accepted_claim() -> None:
    from tank.analysis.card_codec import card_inputs_from_records
    from tank.analysis.card_contracts import PublicationNarrative, PublicationPolicy

    event, assessment = publication_wire()
    claim: dict[str, JsonValue] = {
        "claim_id": "outside-proof",
        "subject": "Outside",
        "predicate": "proposed",
        "object": "a connection",
        "accepted": False,
        "verified": False,
        "evidence": [],
    }
    event["claims"] = [claim]
    event["beneficiary_edges"] = [
        {
            "token_id": "outside:EXT",
            "entity_id": "outside",
            "entity_name": None,
            "role": "SECONDARY",
            "credible": False,
            "tradable": False,
            "layer": "INTEROPERABILITY",
            "evidence": ["outside-proof"],
            "integration_state": "COMPATIBLE",
        }
    ]
    narrative = PublicationNarrative(
        kind="PARTIAL",
        what="The proposal is recorded.",
        verdict="The product was proposed.",
        why=("The proposal changes intended access.", "Deployment remains unverified."),
    )
    policy = PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900)
    rejected = card_inputs_from_records(
        event,
        assessment,
        [],
        [],
        [],
        registry=fixture_registry_membership(),
        narrative=narrative,
        policy=policy,
    )
    assert rejected.other_subjects[0].registry_membership == "UNREGISTERED_TOKEN"
    assert rejected.other_subjects[0].outside_evidence_accepted is False
    assert rejected.display_subjects() == ()
    claim["accepted"] = True
    accepted = card_inputs_from_records(
        event,
        assessment,
        [],
        [],
        [],
        registry=fixture_registry_membership(),
        narrative=narrative,
        policy=policy,
    )
    assert accepted.other_subjects[0].outside_scope == "UNREGISTERED_TOKEN"
    assert not accepted.other_subjects[0].integration_verified


def test_first_partial_card_requires_no_fabricated_beneficiary_map_or_market_result() -> None:
    from tank.analysis.card_codec import card_inputs_from_records
    from tank.analysis.card_contracts import PendingCardAnalysis, PublicationNarrative, PublicationPolicy

    event, assessment = publication_wire()
    del event["beneficiary_map"]
    del event["beneficiary_edges"]
    assessment["class"] = "STRUCTURAL_SHOCK"
    narrative = PublicationNarrative(
        kind="PARTIAL",
        what="The issuer confirmed approval.",
        verdict="The product was approved.",
        why=("Approval changes access.", "Market analysis remains pending."),
    )
    policy = PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900)
    partial = card_inputs_from_records(
        event,
        assessment,
        [],
        [],
        [],
        registry=fixture_registry_membership(),
        narrative=narrative,
        policy=policy,
    )
    assert isinstance(partial.mapping, PendingCardAnalysis)
    text = compose_card(partial)
    assert text.startswith("STRUCTURAL SHOCK (M2) — The product was approved.")
    assert "Market analysis pending" in text and "WHO:" not in text and "WHAT TO DO:" not in text
    assert "token mechanism unavailable" in text
    with pytest.raises(ValueError, match="beneficiary_map"):
        card_inputs_from_records(
            event,
            assessment,
            [],
            [],
            [],
            registry=fixture_registry_membership(),
            narrative=narrative.model_copy(update={"kind": "FULL"}),
            policy=policy,
        )


def test_pending_partial_projection_cannot_hide_supplied_unscoped_financial_facts() -> None:
    from tank.analysis.card_codec import card_inputs_from_records
    from tank.analysis.card_contracts import PublicationNarrative, PublicationPolicy

    event, assessment = publication_wire()
    del event["beneficiary_map"]
    narrative = PublicationNarrative(
        kind="PARTIAL",
        what="The issuer confirmed approval.",
        verdict="The product was approved.",
        why=("Approval changes access.", "Market analysis remains pending."),
    )
    with pytest.raises(ValueError, match="unscoped financial subjects"):
        card_inputs_from_records(
            event,
            assessment,
            [{"token_id": "T"}],
            [],
            [],
            registry=fixture_registry_membership(),
            narrative=narrative,
            policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
        )


def test_gate_reads_actual_event_contradictions_without_requiring_undeclared_assessment_duplicate() -> None:
    from datetime import UTC, datetime

    from tank.analysis.card_codec import publication_gate_inputs_from_records
    from tank.analysis.card_contracts import PublicationPolicy

    event, assessment = publication_wire()
    del assessment["contradictions"]
    event["contradictions"] = [{"contradiction_id": "actual-fatal", "type": "FATAL"}]
    facts = publication_gate_inputs_from_records(
        event,
        assessment,
        [],
        previous=None,
        policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
        now=datetime(2026, 10, 18, 13, tzinfo=UTC),
    )
    assert facts.event.active_fatal_ids == frozenset({"actual-fatal"})
    del event["contradictions"]
    with pytest.raises(ValueError, match="Missing required card fact: contradictions"):
        publication_gate_inputs_from_records(
            event, assessment, [], previous=None, policy=facts.policy, now=facts.now
        )

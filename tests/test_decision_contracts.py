from decimal import Decimal

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.decision_codec import (
    decision_inputs_from_records,
    decode_decision,
    decode_decision_draft,
    encode_decision,
    encode_decision_draft,
)
from tank.analysis.decision_contracts import (
    ActiveDecisionTracking,
    DecisionAssessment,
    DecisionBeneficiary,
    DecisionDivergence,
    DecisionDraft,
    DecisionEvent,
    DecisionIdentity,
    DecisionInputs,
    DecisionLiquidity,
    DecisionMarket,
    DecisionMaterialization,
    DecisionMetadata,
    DecisionOi,
    DecisionPositioning,
    DecisionPrices,
    DecisionRelative,
    DecisionSpot,
    DecisionTriggers,
    EndedDecisionTracking,
    UnavailableDecisionLiquidity,
)
from tank.analysis.decisions import DecisionEngine, finalize_decision, materialize_decision
from tank.analysis.watch_contracts import WatchCondition
from tank.contracts.provenance import GenerationProvenance, ResolvedVersionIdentity
from tank.time_precision import Timestamp, collection_clock


def decision_metadata_fixture() -> DecisionMetadata:
    versions = ResolvedVersionIdentity(config_version="declared-fixture", code_version="fixture-build")
    generation = GenerationProvenance(
        config_version=versions.config_version,
        code_version=versions.code_version,
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
    return DecisionMetadata(
        identity=DecisionIdentity(
            decision_id="DEC-fixture",
            event_id="EVT-fixture",
            token_id="T",
            cutoff=Timestamp("2026-10-25T00:00:00Z", None, "LEGACY_PRECISION_UNAVAILABLE"),
            thresholds_version="declared-thresholds",
            versions=versions,
            generation=generation,
            assessment_id="EMS-fixture",
            market_truth_id="MTA-fixture",
        ),
        calculated_at=collection_clock("2026-10-25T00:01:00Z"),
        management="NORMAL",
        contradiction_ids=(),
        prices=None,
        tracking=ActiveDecisionTracking(kind="ACTIVE"),
    )


def test_materialized_identity_preserves_actual_unknown_cutoff_precision() -> None:
    metadata = decision_metadata_fixture()
    identity = metadata.identity
    assert identity.cutoff.value == "2026-10-25T00:00:00Z"
    assert identity.cutoff.precision is None
    assert identity.cutoff.precision_reason == "LEGACY_PRECISION_UNAVAILABLE"
    with pytest.raises(ValidationError, match="actual market cutoff"):
        DecisionIdentity.model_validate(
            identity.model_copy(update={"cutoff": Timestamp(None, None, "INCOMPLETE")})
        )


def test_materialization_refuses_relabelled_generation_and_missing_calculation_clock() -> None:
    metadata = decision_metadata_fixture()
    generation = metadata.identity.generation.model_copy(update={"code_version": "other-build"})
    with pytest.raises(ValidationError, match="does not match"):
        DecisionIdentity.model_validate(metadata.identity.model_copy(update={"generation": generation}))
    with pytest.raises(ValidationError, match="actual calculation clock"):
        DecisionMetadata.model_validate(
            metadata.model_copy(update={"calculated_at": Timestamp(None, None, "INCOMPLETE")})
        )


def test_watch_allocation_requires_explicit_unique_nonempty_identities() -> None:
    metadata = decision_metadata_fixture()
    assert DecisionMaterialization(metadata=metadata, condition_ids=()).condition_ids == ()
    for ids in (("CND-one", "CND-one"), ("",), (" ",)):
        with pytest.raises(ValidationError, match="identities"):
            DecisionMaterialization(metadata=metadata, condition_ids=ids)


def test_missing_credibility_is_not_inferred_from_role_or_other_evidence() -> None:
    with pytest.raises(ValidationError):
        DecisionBeneficiary.model_validate(
            {
                "role": "PRIMARY",
                "capture_class": "DIRECT",
                "thesis_link_credible": None,
                "exposure_score": 8,
            }
        )


def test_exact_selected_prices_refuse_nonfinite_or_inverted_protection() -> None:
    price = Decimal("100000000000000000.0000000000000000001")
    references = DecisionPrices(entry_ref_price=price, invalidation_price=price)
    assert references.entry_ref_price == price
    with pytest.raises(ValidationError):
        DecisionPrices(entry_ref_price=price, invalidation_price=Decimal("Infinity"))
    with pytest.raises(ValidationError, match="cannot exceed"):
        DecisionPrices(entry_ref_price=Decimal(1), invalidation_price=Decimal(2))


def decision_inputs_fixture() -> DecisionInputs:
    return DecisionInputs(
        event=DecisionEvent(
            has_premise=True,
            state="APPROVED",
            state_verified=True,
            invalidated=False,
            fatal_contradiction=False,
            unresolved_contradiction=False,
            certainty="HIGH",
            milestone=None,
        ),
        assessment=DecisionAssessment(
            classification="MAJOR", impact_start="NOW", all_dependencies_within_24h=None
        ),
        beneficiary=DecisionBeneficiary(
            role="PRIMARY",
            capture_class="DIRECT",
            credible=True,
            thesis_link_credible=None,
            exposure_score=8,
        ),
        market=DecisionMarket(
            data_confidence="HIGH",
            class_confidence="HIGH",
            move="SPOT_LED_ACCUMULATION",
            relative=DecisionRelative(
                repricing_band="EARLY",
                market_context="NEUTRAL",
                event_btc_adjusted_return=Decimal("0.01"),
                anchor_price=Decimal(10),
                entry_price=Decimal(10),
            ),
            liquidity=DecisionLiquidity(state="NORMAL", compression=None),
            spot=DecisionSpot(direction="BUY", breadth="BROAD", selling_majority=False, complete_venues=3),
            positioning=DecisionPositioning(
                synchronized=DecisionOi(
                    native_beyond=False, native_change=Decimal("0.02"), native_vs_normal="WITHIN"
                ),
                venue_funding=("WITHIN",),
            ),
            divergence=DecisionDivergence(absorption_confirmed=False, unresolved=()),
            seller_exhaustion=None,
            alternatives=(),
        ),
        open_call=None,
        triggers=DecisionTriggers(roles=frozenset(), anchor_lost=False, sell_downgrade=False),
    )


def decision_wire_fixture() -> tuple[
    dict[str, JsonValue], dict[str, JsonValue], dict[str, JsonValue], dict[str, JsonValue]
]:
    return (
        {
            "event_id": "EVT-fixture",
            "event_state": "APPROVED",
            "state_verified": True,
            "lifecycle": "ANALYZING",
            "certainty": "HIGH",
            "next_milestone": None,
            "contradictions": [],
        },
        {
            "event_id": "EVT-fixture",
            "ems_id": "EMS-fixture",
            "class": "MAJOR",
            "impact_horizon": {"impact_start": "NOW"},
        },
        {
            "event_id": "EVT-fixture",
            "token_id": "T",
            "role": "PRIMARY",
            "capture_class": "DIRECT",
            "credible": True,
            "exposure_score": 8,
        },
        {
            "event_id": "EVT-fixture",
            "mta_id": "MTA-fixture",
            "token_id": "T",
            "move_class": "SPOT_LED_ACCUMULATION",
            "data_confidence": {"class": "HIGH"},
            "class_confidence": "HIGH",
            "primary_window": "W4H",
            "relative": {
                "repricing_band": "EARLY",
                "market_context": "NEUTRAL",
                "anchor": {"ref_price": "10"},
                "entry_ref_price": "10",
                "windows": {"EVENT": {"r_btc_adj": "0.01"}},
            },
            "liquidity": {"liquidity_state": "NORMAL", "vol_compression": {"state": None}},
            "spot": {
                "W4H": {
                    "direction": "BUY",
                    "classification": "BROAD",
                    "exact_majority": {"sell": False},
                    "coverage": {"n_complete": 3},
                }
            },
            "derivatives": {
                "windows": {
                    "W4H": {
                        "synchronized": {
                            "native_beyond": False,
                            "oi_native_chg_pct": "0.02",
                            "native_oi_vs_normal": "WITHIN",
                        },
                        "per_venue": [{"funding_vs_normal": "WITHIN"}],
                    }
                }
            },
            "divergence": {"state": "NONE", "unresolved": []},
            "alternatives": [],
        },
    )


def materialized_fixture() -> DecisionDraft:
    result = DecisionEngine().evaluate(decision_inputs_fixture())
    return materialize_decision(
        result,
        DecisionMaterialization(
            metadata=decision_metadata_fixture(),
            condition_ids=tuple(f"CND-{index}" for index in range(len(result.conditions))),
        ),
    )


def reconciled_fixture(draft: DecisionDraft) -> list[WatchCondition]:
    metadata = draft.metadata
    return [
        WatchCondition.model_validate(
            {
                **proposal.model_dump(mode="json", exclude_unset=True),
                **metadata.identity.generation.model_dump(mode="json"),
                "event_id": metadata.identity.event_id,
                "token_id": metadata.identity.token_id,
                "created_by": metadata.identity.decision_id,
                **metadata.calculated_at.fields("created_at"),
                **collection_clock("2026-11-01T00:00:00Z").fields("expires_at"),
            }
        )
        for proposal in draft.proposals
    ]


def test_pure_decision_is_deterministic_without_clock_identity_or_settings() -> None:
    inputs = decision_inputs_fixture()
    first = DecisionEngine().evaluate(inputs)
    assert first == DecisionEngine().evaluate(inputs)
    assert first.action == "START" and first.thesis_grade == "A" and first.trade_grade == "A"
    assert first.outlook.best_horizon == "H12"
    assert first.trade_reasons == ["TRADE_BASE_A"]


def test_unknown_impact_never_invents_a_review_horizon_but_real_move_override_remains() -> None:
    inputs = decision_inputs_fixture()
    inputs = inputs.model_copy(
        update={"assessment": inputs.assessment.model_copy(update={"impact_start": "UNKNOWN"})}
    )
    result = DecisionEngine().evaluate(inputs)
    assert result.outlook.best_horizon is None and result.outlook.horizon_reason == "impact start unavailable"
    overridden = inputs.model_copy(
        update={"market": inputs.market.model_copy(update={"move": "SHORT_SQUEEZE"})}
    )
    assert DecisionEngine().evaluate(overridden).outlook.best_horizon == "H1"


def test_actual_wire_projection_requires_facts_and_checks_event_ownership() -> None:
    event, assessment, beneficiary, market = decision_wire_fixture()
    result = decision_inputs_from_records(
        event, assessment, beneficiary, market, open_call=None, conditions=()
    )
    assert DecisionEngine().evaluate(result).action == "START"
    beneficiary.pop("credible")
    with pytest.raises(ValueError, match="credible"):
        decision_inputs_from_records(event, assessment, beneficiary, market, open_call=None, conditions=())
    beneficiary["credible"] = True
    assessment["event_id"] = "foreign-event"
    with pytest.raises(ValueError, match="different event"):
        decision_inputs_from_records(event, assessment, beneficiary, market, open_call=None, conditions=())


def test_invalid_resolution_clock_cannot_hide_a_fatal_contradiction() -> None:
    event, assessment, beneficiary, market = decision_wire_fixture()
    event["contradictions"] = [{"type": "FATAL", "resolved_at": 0}]
    with pytest.raises(ValueError, match="resolved_at"):
        decision_inputs_from_records(event, assessment, beneficiary, market, open_call=None, conditions=())


def test_materialization_and_full_record_codec_preserve_real_metadata_and_watch_reconciliation() -> None:
    draft = materialized_fixture()
    wire_draft = encode_decision_draft(draft)
    assert encode_decision_draft(decode_decision_draft(wire_draft)) == wire_draft
    assert "metadata" not in wire_draft and wire_draft["config_version"] == "declared-fixture"
    assert (
        wire_draft["cutoff_prec"] is None
        and wire_draft["cutoff_prec_reason"] == "LEGACY_PRECISION_UNAVAILABLE"
    )
    with pytest.raises(ValueError, match="omitted"):
        finalize_decision(draft, conditions=[], conditions_shown=(), outlook_id="OUT-fixture")
    conditions = reconciled_fixture(draft)
    record = finalize_decision(
        draft,
        conditions=conditions,
        conditions_shown=tuple(c.cond_id for c in conditions[:2]),
        outlook_id="OUT-fixture",
    )
    wire = encode_decision(record)
    assert encode_decision(decode_decision(wire)) == wire
    assert wire["market_truth_id"] == wire["mta_id"] == "MTA-fixture"
    assert "tracking_ended" not in wire and "entry_ref_price" not in wire
    wire["mta_id"] = "foreign-truth"
    with pytest.raises(ValueError, match="identities disagree"):
        decode_decision(wire)


def test_final_record_refuses_foreign_reconciled_watch_and_silent_inactivation() -> None:
    draft = materialized_fixture()
    conditions = reconciled_fixture(draft)
    foreign = conditions[0].model_copy(update={"token_id": "foreign-token"})
    with pytest.raises(ValidationError, match="different event or token"):
        finalize_decision(
            draft, conditions=[foreign, *conditions[1:]], conditions_shown=(), outlook_id="OUT-fixture"
        )
    record = finalize_decision(draft, conditions=conditions, conditions_shown=(), outlook_id="OUT-fixture")
    wire = encode_decision(record)
    wire["actionable"] = False
    with pytest.raises(ValueError, match="silently become inactive"):
        decode_decision(wire)


def test_ended_frozen_tracking_requires_actual_lineage_and_has_no_active_watches() -> None:
    draft = materialized_fixture()
    metadata = draft.metadata.model_copy(
        update={
            "tracking": EndedDecisionTracking(
                kind="ENDED", previous_decision_id="DEC-earlier", expired_call_ids=("CALL-expired",)
            )
        }
    )
    ended = DecisionDraft(calculation=draft.calculation, metadata=metadata, proposals=[])
    record = finalize_decision(ended, conditions=[], conditions_shown=(), outlook_id="OUT-earlier")
    wire = encode_decision(record)
    assert wire["tracking_ended"] is True and wire["actionable"] is False
    assert wire["previous_decision_id"] == "DEC-earlier"
    assert encode_decision(decode_decision(wire)) == wire


def test_actual_unanchored_event_liquidity_is_explicitly_unavailable_not_a_filled_object() -> None:
    event, assessment, beneficiary, market = decision_wire_fixture()
    market["primary_window"] = "EVENT"
    market["move_class"] = "UNKNOWN"
    market["class_confidence"] = "LOW"
    market["data_confidence"] = {"class": "LOW"}
    market["spot"] = {
        "EVENT": {
            "direction": None,
            "classification": "INSUFFICIENT_DATA",
            "exact_majority": {"sell": None},
            "coverage": {"n_complete": 0},
        }
    }
    market["liquidity"] = {
        "window": "EVENT",
        "start": None,
        "cutoff": "2026-10-25T00:00:00Z",
        "reason": "INCOMPLETE",
    }
    projected = decision_inputs_from_records(
        event, assessment, beneficiary, market, open_call=None, conditions=()
    )
    assert isinstance(projected.market.liquidity, UnavailableDecisionLiquidity)
    assert DecisionEngine().evaluate(projected).action == "WAIT"
    complete = decision_inputs_fixture()
    unknown = complete.model_copy(
        update={
            "market": complete.market.model_copy(
                update={"liquidity": UnavailableDecisionLiquidity(reason="INCOMPLETE")}
            )
        }
    )
    assert DecisionEngine().evaluate(unknown).trade_grade == "B"
    market["liquidity"] = {"reason": "INCOMPLETE"}
    with pytest.raises(ValueError, match="required"):
        decision_inputs_from_records(event, assessment, beneficiary, market, open_call=None, conditions=())

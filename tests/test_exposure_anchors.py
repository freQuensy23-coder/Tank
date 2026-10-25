"""Evidence anchors for beneficiary exposure ratings."""

import copy
from types import SimpleNamespace

import pytest
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_http_admission, fixture_versions

import tank.event_tools as event_tools
from tank.analysis.page_access import LivePageAccess
from tank.storage import MemoryRepository


async def prepare(
    *,
    role="PRIMARY",
    event_state="APPROVED",
    claim_state="APPROVED",
    direction="COUNTERPARTY_ADOPTION",
    exposure=6,
    claimed_integration="LIVE",
    certainty="HIGH",
    claim_type="STATE",
):
    repo = MemoryRepository()
    await repo.open()
    config = {
        "registry": {
            "tokens": [{"token_id": "world:WLD"}],
            "instruments": [{"token_id": "world:WLD"}],
        },
        "dependency_map": {"edges": []},
    }
    settings = SimpleNamespace(
        config_version="isolated",
        code_version="isolated",
        load_config=lambda name: copy.deepcopy(config[name]),
    )
    service = event_tools.EventToolService(
        repo,
        settings,
        None,
        None,
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "event_state": event_state, "entities": [], "tokens_mentioned": ["world:WLD"]},
    )
    await repo.put(
        "evt.claims",
        "c",
        {
            "claim_id": "c",
            "event_id": "e",
            "accepted": True,
            "verified": certainty == "HIGH",
            "type": claim_type,
            "subject": "world",
            "predicate": "selected",
            "object": "service",
            "state_claim": claim_state,
            "directionality": direction,
            "evidence": [{"item_id": "source", "quote": "The bank selected the service."}],
        },
    )
    await repo.put(
        "evt.capture_chains",
        "capture",
        {
            "event_id": "e",
            "token_id": "world:WLD",
            "version": 1,
            "capture_score": 8,
            "capture_class": "DIRECT",
            "token_mechanism": {"evidence": ["c"]},
        },
    )
    edge = {
        "entity_id": "world",
        "token_id": "world:WLD",
        "role": role,
        "layer": "ISSUER_PRODUCT",
        "direction": "POSITIVE",
        "causal_chain": "Selected rail",
        "directionality": direction,
        "integration_state": claimed_integration,
        "exposure_score": exposure,
        "edge_certainty": certainty,
        "evidence": ["c"],
    }
    args = {
        "event_id": "e",
        "edges": [edge],
        "layer_reasons": {layer: "No further evidenced intermediary" for layer in event_tools.LAYERS},
    }
    return service, args


@pytest.mark.parametrize(
    "stage,exposure", [("PROPOSED", 6), ("RUMOR", 7), ("APPROVED", 8), ("SIGNED", 8), ("DEPLOYED", 9)]
)
async def test_primary_rejects_rating_above_proven_event_anchor(stage, exposure) -> None:
    service, args = await prepare(event_state=stage, claim_state=stage, exposure=exposure)
    with pytest.raises(ValueError, match="requires applicable"):
        await service.set_beneficiaries(args, {})
    assert await service.repo.list("evt.beneficiary_edges") == []


@pytest.mark.parametrize(
    "stage,exposure", [("APPROVED", 6), ("SIGNED", 7), ("LIVE", 8), ("MONEY_FLOWING", 9)]
)
async def test_primary_uses_event_ladder_and_preserves_credible_approved(stage, exposure) -> None:
    service, args = await prepare(event_state=stage, claim_state=stage, exposure=exposure)
    await service.set_beneficiaries(args, {})
    edge = (await service.repo.list("evt.beneficiary_edges"))[0]
    assert edge["exposure_score"] == exposure
    assert edge["integration_state"] is None
    assert edge["credible"] is True


@pytest.mark.parametrize(
    "claim_state,direction,exposure,accepted",
    [
        ("COMPATIBLE", "BOTH", 6, False),
        ("BUILT", "SOURCE_SIDE_SUPPORT", 6, True),
        ("APPROVED", "COUNTERPARTY_ADOPTION", 6, True),
        ("LIVE", "SOURCE_SIDE_SUPPORT", 8, False),
        ("LIVE", "COUNTERPARTY_ADOPTION", 8, True),
        ("ECONOMIC_FLOW", "COUNTERPARTY_ADOPTION", 9, True),
    ],
)
async def test_secondary_uses_derived_integration_evidence_not_submitted_live(
    claim_state, direction, exposure, accepted
) -> None:
    service, args = await prepare(
        role="SECONDARY", claim_state=claim_state, direction=direction, exposure=exposure
    )
    if not accepted:
        with pytest.raises(ValueError, match="requires applicable"):
            await service.set_beneficiaries(args, {})
        assert await service.repo.list("evt.beneficiary_edges") == []
    else:
        await service.set_beneficiaries(args, {})
        edge = (await service.repo.list("evt.beneficiary_edges"))[0]
        assert edge["exposure_score"] == exposure
        assert edge["integration_state"] == claim_state


async def test_exact_ten_rejects_source_side_only_without_quiet_clamp() -> None:
    service, args = await prepare(exposure=10, direction="SOURCE_SIDE_SUPPORT")
    with pytest.raises(ValueError, match="requires accepted counterparty"):
        await service.set_beneficiaries(args, {})
    assert args["edges"][0]["exposure_score"] == 10
    assert await service.repo.list("evt.beneficiary_edges") == []


async def test_exact_ten_does_not_invent_live_or_relation_claim_type_requirement() -> None:
    service, args = await prepare(exposure=10, event_state="APPROVED", claim_type="STATE")

    args["edges"][0]["causal_chain"] = "Counterparty selected this as the sole required rail."
    await service.set_beneficiaries(args, {})
    assert (await service.repo.list("evt.beneficiary_edges"))[0]["exposure_score"] == 10


async def test_accepted_medium_evidence_is_not_silently_required_to_be_high() -> None:
    service, args = await prepare(certainty="MEDIUM", exposure=6)
    await service.set_beneficiaries(args, {})
    assert (await service.repo.list("evt.beneficiary_edges"))[0]["credible"] is True


async def test_approved_selection_rating_rejected_and_supported_six_retained() -> None:
    service, args = await prepare(event_state="APPROVED", exposure=8)
    with pytest.raises(ValueError, match="requires applicable LIVE"):
        await service.set_beneficiaries(args, {})
    assert await service.repo.list("evt.beneficiary_edges") == []
    args["edges"][0]["exposure_score"] = 6
    await service.set_beneficiaries(args, {})
    assert (await service.repo.list("evt.beneficiary_edges"))[0]["exposure_score"] == 6


async def test_primary_own_official_live_does_not_borrow_secondary_source_side_cap() -> None:
    service, args = await prepare(
        event_state="LIVE", claim_state="LIVE", exposure=8, direction="SOURCE_SIDE_SUPPORT"
    )
    await service.set_beneficiaries(args, {})
    edge = (await service.repo.list("evt.beneficiary_edges"))[0]
    assert edge["exposure_score"] == 8
    assert edge["integration_state"] is None


@pytest.mark.parametrize(
    "direction,accepted", [("COUNTERPARTY_ADOPTION", True), ("SOURCE_SIDE_SUPPORT", False)]
)
async def test_proposed_sole_rail_rating_has_no_invented_stage_gate_and_cannot_start(
    direction, accepted
) -> None:
    from tank.analysis.decisions import DecisionEngine

    service, args = await prepare(
        event_state="PROPOSED", claim_state="PROPOSED", exposure=10, direction=direction
    )
    claim = await service.repo.get("evt.claims", "c")
    claim["evidence"][0]["quote"] = "The bank proposes World as the sole required rail."
    await service.repo.put("evt.claims", "c", claim)
    args["edges"][0]["causal_chain"] = "If approved, World would be the bank's sole required rail."
    if not accepted:
        with pytest.raises(ValueError, match="requires accepted counterparty"):
            await service.set_beneficiaries(args, {})
        assert await service.repo.list("evt.beneficiary_edges") == []
        return
    await service.set_beneficiaries(args, {})
    edge = (await service.repo.list("evt.beneficiary_edges"))[0]
    assert edge["exposure_score"] == 10
    assert edge["credible"] is False
    event = {"event_id": "e", "event_state": "PROPOSED", "state_verified": True, "freshness": "FRESH"}
    assessment = {"class": "MAJOR", "certainty": "HIGH", "impact_horizon": {"impact_start": "NOW"}}
    market = {
        "token_id": "world:WLD",
        "move_class": "SPOT_LED_ACCUMULATION",
        "data_confidence": {"class": "HIGH", "critical_semantics_verified": True},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL", "anchor": {"ref_price": "10"}},
        "liquidity": {"liquidity_state": "NORMAL"},
        "spot": {},
        "derivatives": {},
    }
    beneficiary = dict(edge, capture_class="DIRECT")
    engine = DecisionEngine()

    positive = engine.evaluate(
        dict(event, event_state="LIVE"), assessment, dict(beneficiary, credible=True), market
    )
    assert positive["action"] == "START"
    decision = engine.evaluate(event, assessment, beneficiary, market)
    assert decision["thesis_grade"] == "D"
    assert decision["action"] == "WAIT"

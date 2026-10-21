"""Agent semantic decisions survive storage; deterministic policy still applies."""

import copy
from datetime import timedelta

import pytest

from tank.event_tools import LAYERS, EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.fixture
async def owner_service(tmp_path):
    repo = MemoryRepository()
    await repo.open()
    return EventToolService(repo, Settings(data_dir=tmp_path), None, None)


@pytest.mark.parametrize("query", ["sama", "@sama", " @SAMA "])
async def test_registry_handle_notation_preserves_registered_authority(owner_service, query):
    result = await owner_service.get_registry({"entity_or_token": query}, {})
    account = next(row for row in result["x_accounts"] if row["handle"] == "sama")
    assert {row["entity_id"] for row in account["official_authorities"]} == {"world", "openai"}
    assert result == await owner_service.get_registry({"entity_or_token": "sama"}, {})


@pytest.mark.parametrize("query", ["@", "@@sama", "@unregistered_handle"])
async def test_unmatched_handle_does_not_return_registered_accounts(owner_service, query):
    result = await owner_service.get_registry({"entity_or_token": query}, {})
    assert result["x_accounts"] == []


async def create(service, *, verified=True, state="LIVE", certainty="HIGH"):
    now = iso()
    await service.repo.put(
        "ingest.source_items",
        "owned",
        {
            "item_id": "owned",
            "source_id": "S30",
            "source_class": "SECONDARY",
            "text": "A short source paraphrase",
            "observed_at": now,
        },
    )
    args = {
        "item_id": "owned",
        "is_event": True,
        "statement": "Neutral agent interpretation",
        "certainty": certainty,
        "certainty_reason": "Analyst verified the primary artifact",
        "entities": [{"name": "World", "entity_id": None, "role": "ACTOR"}],
        "tokens": ["world:WLD"],
        "event_time": {"value": now, "precision": "EXACT", "quote": "Declared primary time"},
        "public_at": {
            "value": iso(utcnow() + timedelta(days=1)),
            "precision": "EXACT",
            "basis": "Analyst source-time interpretation",
            "quote": "Time quote",
        },
        "freshness": "OLD",
        "next_milestone": None,
        "checked_sources": ["primary artifact"],
        "claims": [
            {
                "type": "STATE",
                "subject": "World",
                "predicate": "operates",
                "object": "SDK repository compatible; will be available tomorrow",
                "state_claim": state,
                "verified": verified,
                "directionality": "BOTH",
                "evidence_class": "PRIMARY_FACT",
                "quote": "Agent supplied exact support",
            }
        ],
    }
    original = copy.deepcopy(args)
    result = await service.record_event(args, {})
    assert args == original
    return result, original


def assessment(proof, *, klass="MATERIAL", criterion="MATERIAL"):
    return {
        "class": klass,
        "criterion": criterion,
        "criterion_quote": "Analyst selected support",
        "dims": {
            name: {"value": 8, "rationale": "Agent dimensional judgment", "evidence": [proof]}
            for name in [
                "economic_directness",
                "magnitude",
                "permanence",
                "distribution_expansion",
                "mechanical_flows",
                "surprise",
                "implementation",
            ]
        },
        "impact_horizon": {"impact_start": "WEEKS", "timing_confidence": "HIGH"},
    }


@pytest.mark.parametrize("verified", [True, False])
async def test_agent_claim_time_certainty_and_stage_are_not_readjudicated(owner_service, verified):
    result, supplied = await create(owner_service, verified=verified)
    event = await owner_service.repo.get("evt.events", result["event_id"])
    claim = result["accepted_claims"][0]
    assert result["dropped_claims"] == []
    for key, value in supplied["claims"][0].items():
        assert claim[key] == value
    assert event["event_state"] == "LIVE"
    assert event["state_verified"] is verified
    assert event["certainty"] == "HIGH"
    assert event["certainty_reason"] == supplied["certainty_reason"]
    assert event["event_time"]["precision"] == "EXACT"
    assert event["public_at"] == supplied["public_at"]["value"]
    assert event["freshness"] == "OLD"
    assert event["entities"] == [{"entity_id": "world", "role": "ACTOR"}]


async def test_observation_uses_explicit_certainty_not_automatic_primary_promotion(owner_service):
    result, args = await create(owner_service, certainty="MEDIUM")
    eid = result["event_id"]
    observation = {
        "event_id": eid,
        "item_id": "owned",
        "link": "CORROBORATION",
        "claims": args["claims"],
        "public_at": args["public_at"],
        "freshness": "STALE",
    }
    await owner_service.add_observation(observation, {})
    assert (await owner_service.repo.get("evt.events", eid))["certainty"] == "MEDIUM"
    observation.update(certainty="LOW", certainty_reason="Agent revised confidence")
    await owner_service.add_observation(observation, {})
    event = await owner_service.repo.get("evt.events", eid)
    assert (event["certainty"], event["certainty_reason"]) == ("LOW", "Agent revised confidence")


async def test_magnitude_is_agent_owned_but_implementation_is_deterministic(owner_service):
    result, _ = await create(owner_service)
    proof = result["accepted_claims"][0]["claim_id"]
    supplied = assessment(proof)
    supplied["magnitude_basis"] = {"kind": "TOKEN_MECHANICAL", "denominator_window": "SPOT"}
    out = await owner_service.set_assessment({"event_id": result["event_id"], "assessment": supplied}, {})
    assert out["dims"]["magnitude"] == supplied["dims"]["magnitude"]
    assert out["magnitude_basis"] == supplied["magnitude_basis"]
    assert out["dims"]["implementation"]["value"] == 8
    assert out["impact_horizon"]["timing_confidence"] == "HIGH"


@pytest.mark.parametrize("klass,criterion", [("MATERIAL", "MATERIAL"), ("MAJOR", "M2"), ("LOW", "LOW")])
async def test_assessment_without_universe_concern_is_low(owner_service, klass, criterion):
    result, _ = await create(owner_service)
    event = await owner_service.repo.get("evt.events", result["event_id"])
    event.update(tokens_mentioned=[], entities=[{"entity_id": "sec", "role": "REGULATOR"}])
    await owner_service.repo.put("evt.events", event["event_id"], event)
    assert not await owner_service.rows("evt.beneficiary_maps", event["event_id"])
    supplied = assessment(result["accepted_claims"][0]["claim_id"], klass=klass, criterion=criterion)
    supplied["class_reasons"] = ["Analyst assessed the scope of the sector-level announcement."]
    out = await owner_service.set_assessment({"event_id": event["event_id"], "assessment": supplied}, {})
    assert out["class"] == "LOW"
    assert out["class_reasons"] == supplied["class_reasons"] + (
        ["NO_UNIVERSE_CONCERN"] if klass != "LOW" else []
    )


@pytest.mark.parametrize(
    "certainty,state,criterion,venue,expected",
    [
        ("LOW", "LIVE", "M2", None, "MATERIAL"),
        ("HIGH", "PROPOSED", "M2", None, "MATERIAL"),
        ("HIGH", "PROPOSED", "M4", None, "MAJOR"),
        ("HIGH", "LIVE", "M1", "uncovered", "MATERIAL"),
        ("HIGH", "LIVE", "M1", "okx", "MAJOR"),
        ("HIGH", "LIVE", "M1", "Coinbase", "MAJOR"),
    ],
)
async def test_explicit_class_caps_still_apply(owner_service, certainty, state, criterion, venue, expected):
    result, _ = await create(owner_service, certainty=certainty, state=state)
    supplied = assessment(result["accepted_claims"][0]["claim_id"], klass="MAJOR", criterion=criterion)
    if venue:
        supplied["criterion_venue"] = venue
    out = await owner_service.set_assessment({"event_id": result["event_id"], "assessment": supplied}, {})
    assert out["class"] == expected


@pytest.mark.parametrize("venue", [None, "", "   "])
async def test_m1_requires_explicit_venue_without_guessing_from_prose(owner_service, venue):
    result, _ = await create(owner_service)
    supplied = assessment(result["accepted_claims"][0]["claim_id"], klass="MAJOR", criterion="M1")
    supplied["criterion_quote"] = "Binance lists this token"
    if venue is not None:
        supplied["criterion_venue"] = venue
    with pytest.raises(ValueError, match="M1 requires criterion_venue"):
        await owner_service.set_assessment({"event_id": result["event_id"], "assessment": supplied}, {})


async def test_observation_link_is_not_reclassified_by_the_host(owner_service):
    result, args = await create(owner_service)
    observation = {
        "event_id": result["event_id"],
        "item_id": "owned",
        "link": "STATE_UPDATE",
        "claims": args["claims"],
        "public_at": args["public_at"],
        "freshness": "OLD",
    }
    update = await owner_service.add_observation(observation, {})
    assert update["link_type"] == "STATE_UPDATE"


async def test_capture_caps_and_source_side_integration_remain_deterministic(owner_service):
    result, _ = await create(owner_service, state="LIVE")
    eid = result["event_id"]
    proof = result["accepted_claims"][0]["claim_id"]
    supplied = assessment(proof)
    supplied["capture_chains"] = [
        {"token_id": "world:WLD", "capture_score": 10, "token_mechanism": {"active": False, "evidence": []}}
    ]
    await owner_service.set_assessment({"event_id": eid, "assessment": supplied}, {})
    chain = (await owner_service.rows("evt.capture_chains", eid))[0]
    assert chain["capture_score"] == 3 and chain["conditional"]

    claim = await owner_service.repo.get("evt.claims", proof)
    claim.update(directionality="SOURCE_SIDE_SUPPORT", state_claim="LIVE")
    await owner_service.repo.put("evt.claims", proof, claim)
    await owner_service.set_beneficiaries(
        {
            "event_id": eid,
            "edges": [
                {
                    "entity_id": "world",
                    "token_id": "world:WLD",
                    "role": "SECONDARY",
                    "layer": "ISSUER_PRODUCT",
                    "direction": "POSITIVE",
                    "causal_chain": "Agent relation",
                    "exposure_score": 4,
                    "edge_certainty": "HIGH",
                    "evidence": [proof],
                }
            ],
            "layer_reasons": {layer: "NONE" for layer in LAYERS},
        },
        {},
    )
    edge = (await owner_service.rows("evt.beneficiary_edges", eid))[0]
    assert edge["integration_state"] == "BUILT"


@pytest.mark.parametrize(
    "binding,expected",
    [
        ("matching", "STRUCTURAL_SHOCK"),
        ("implicit", "STRUCTURAL_SHOCK"),
        ("unrelated_quote", "MAJOR"),
        ("different_gate", "MAJOR"),
        ("unsatisfied", "MAJOR"),
        ("unverified", "MAJOR"),
        ("empty_quote", "MATERIAL"),
    ],
)
async def test_structural_criterion_quote_binds_selected_gate_verified_claim(
    owner_service, binding, expected
):
    import copy

    created, _ = await create(owner_service)
    eid = created["event_id"]
    proof = created["accepted_claims"][0]["claim_id"]
    primary_quote = "The route requires this settlement infrastructure."
    claim = await owner_service.repo.get("evt.claims", proof)
    claim["evidence"][0]["quote"] = primary_quote
    if binding == "unverified":
        claim["verified"] = False
    await owner_service.repo.put("evt.claims", proof, claim)
    unrelated = copy.deepcopy(claim)
    unrelated.update(
        claim_id="unrelated",
        verified=True,
        evidence=[{"item_id": "owned", "quote": "The company opened an office."}],
    )
    await owner_service.repo.put("evt.claims", "unrelated", unrelated)
    supplied = assessment(proof, klass="STRUCTURAL_SHOCK", criterion="G1")
    supplied["criterion_quote"] = "  requires this\n settlement infrastructure. "
    supplied["structural_gates"] = [{"gate": "G1", "satisfied": True, "evidence": [proof]}]
    if binding == "implicit":
        supplied.pop("structural_gates")
    elif binding == "unrelated_quote":
        supplied["criterion_quote"] = unrelated["evidence"][0]["quote"]
    elif binding == "different_gate":
        supplied["structural_gates"][0]["gate"] = "G2"
    elif binding == "unsatisfied":
        supplied["structural_gates"][0]["satisfied"] = False
    elif binding == "empty_quote":
        supplied["criterion_quote"] = "   "
    supplied["impact_horizon"].update(ramp_shape="STEP", dependencies=[], time_to_materiality="Weeks")
    original = copy.deepcopy(supplied)
    result = await owner_service.dispatch("set_assessment", {"event_id": eid, "assessment": supplied}, {})
    assert supplied == original
    assert result["class"] == expected
    assert result["criterion"] == "G1"
    assert result["criterion_quote"] == original["criterion_quote"]
    assert ("STRUCTURAL_GATE" in result["class_reasons"]) == (expected != "STRUCTURAL_SHOCK")

    assert (await owner_service.repo.get("evt.claims", proof))["evidence"][0]["quote"] == primary_quote


async def test_watched_token_beneficiary_preserves_outside_primary_event_class(owner_service):
    created, _ = await create(owner_service)
    eid = created["event_id"]
    event = await owner_service.repo.get("evt.events", eid)
    await owner_service.repo.put("evt.events", eid, {**event, "tokens_mentioned": []})
    await owner_service.repo.put(
        "evt.beneficiary_maps", eid + ":1", {"event_id": eid, "map_version": 1, "analysis_set": ["world:WLD"]}
    )
    await owner_service.repo.put(
        "evt.beneficiary_edges",
        "covered-beneficiary",
        {
            "edge_id": "covered-beneficiary",
            "event_id": eid,
            "map_version": 1,
            "token_id": "world:WLD",
            "role": "SECONDARY",
        },
    )
    supplied = assessment(created["accepted_claims"][0]["claim_id"], klass="MAJOR", criterion="M2")
    supplied["impact_horizon"].update(ramp_shape="STEP", dependencies=[], time_to_materiality="Weeks")
    result = await owner_service.dispatch("set_assessment", {"event_id": eid, "assessment": supplied}, {})
    assert result["class"] == "MAJOR"
    assert "NO_UNIVERSE_CONCERN" not in result["class_reasons"]

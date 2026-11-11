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


@pytest.mark.parametrize("query", [{"entity_id": "world"}, {"token_id": "world:WLD"}])
async def test_registry_filter_preserves_registered_authority(owner_service, query):
    result = await owner_service.get_registry(query, {})
    account = next(row for row in result["x_accounts"] if row["handle"] == "sama")
    assert {row["entity_id"] for row in account["official_authorities"]} == {"world", "openai"}
    assert [row["token_id"] for row in result["tokens"]] == ["world:WLD"]
    assert result == await owner_service.get_registry({"entity_id": "world"}, {})


async def test_registry_filter_returns_only_the_named_identity(owner_service):
    result = await owner_service.get_registry({"entity_id": "openai"}, {})
    assert [row["entity_id"] for row in result["entities"]] == ["openai"]
    assert result["x_accounts"] == [] and result["tokens"] == [] and result["instruments"] == []


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
        "entities": [{"entity_id": "world", "role": "ACTOR"}],
        "token_ids": ["world:WLD"],
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


def assessment(proof, *, klass="MATERIAL"):
    return {
        "class": klass,
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


@pytest.mark.parametrize("klass", ["LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"])
@pytest.mark.parametrize(
    "certainty,state,verified,universe",
    [
        ("HIGH", "LIVE", True, True),
        ("LOW", "LIVE", True, True),
        ("HIGH", "PROPOSED", True, True),
        ("MEDIUM", "LIVE", False, True),
        ("HIGH", "LIVE", True, False),
    ],
    ids=["verified-live", "low-certainty", "proposed", "unverified-medium", "outside-universe"],
)
async def test_stored_level_is_exactly_the_agent_level(
    owner_service, klass, certainty, state, verified, universe
):
    result, _ = await create(owner_service, certainty=certainty, state=state, verified=verified)
    eid = result["event_id"]
    if not universe:
        event = await owner_service.repo.get("evt.events", eid)
        event.update(tokens_mentioned=[], entities=[{"entity_id": "sec", "role": "REGULATOR"}])
        await owner_service.repo.put("evt.events", eid, event)
    supplied = assessment(result["accepted_claims"][0]["claim_id"], klass=klass)
    supplied["class_reasons"] = ["Analyst judged the consequence for the affected token."]
    supplied["impact_horizon"].update(ramp_shape="STEP", dependencies=[], time_to_materiality="Weeks")
    out = await owner_service.dispatch("set_assessment", {"event_id": eid, "assessment": supplied}, {})
    stored = await owner_service.repo.get("evt.ems_assessments", out["ems_id"])
    assert out["class"] == stored["class"] == klass
    assert stored["class_reasons"] == supplied["class_reasons"]
    assert not [key for key in stored if key.startswith("criterion") or key == "structural_gates"]
    event = await owner_service.repo.get("evt.events", eid)
    assert event["current_ems_id"] == out["ems_id"]
    assert event["lifecycle"] == ("ANALYZING" if klass in {"MAJOR", "STRUCTURAL_SHOCK"} else "TRACKED")


async def test_assessment_refuses_the_retired_criterion_fields(owner_service):
    result, _ = await create(owner_service)
    proof = result["accepted_claims"][0]["claim_id"]
    for field, value in [
        ("criterion", "M2"),
        ("criterion_claim_id", proof),
        ("criterion_venue_id", "binance"),
        ("structural_gates", [{"gate": "G1", "satisfied": True, "evidence": [proof]}]),
    ]:
        supplied = {**assessment(proof, klass="MAJOR"), field: value}
        with pytest.raises(ValueError, match=field):
            await owner_service.dispatch(
                "set_assessment", {"event_id": result["event_id"], "assessment": supplied}, {}
            )


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

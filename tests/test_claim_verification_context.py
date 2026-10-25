"""An analyst's verification gap survives the public tools into the card."""

from pathlib import Path

import pytest
from foundation_helpers import array_field
from pydantic import JsonValue
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_http_admission, fixture_versions
from test_card_template import card_inputs, render, wire_card_inputs
from test_event_tools import seed

from tank.analysis.card_codec import card_inputs_from_records
from tank.analysis.page_access import LivePageAccess
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository


def render_claim_wire(
    values: tuple[
        dict[str, JsonValue],
        dict[str, JsonValue],
        dict[str, JsonValue],
        dict[str, JsonValue],
        dict[str, JsonValue],
    ],
) -> str:
    event, assessment, beneficiary, decision, truth = values
    authored = card_inputs()
    return render(
        card_inputs_from_records(
            event,
            assessment,
            [beneficiary],
            [decision],
            [truth],
            narrative=authored.narrative,
            policy=authored.policy,
        )
    )


@pytest.mark.parametrize("tool", ["record_event", "add_observation"])
async def test_agent_verification_gap_reaches_card_through_public_tool(tmp_path: Path, tool) -> None:
    repo = MemoryRepository()
    await repo.open()
    service = EventToolService(
        repo,
        Settings(data_dir=tmp_path),
        None,
        None,
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    args = await seed(service)
    gap = "No announcement from the named venue; the reporter supplies no primary quote."
    args["certainty"] = "MEDIUM"
    args["claims"][0].update(verified=False, verification_missing=gap)
    if tool == "add_observation":
        first = await seed(service)
        event = await service.dispatch("record_event", first, {})
        args = {
            "event_id": event["event_id"],
            "item_id": args["item_id"],
            "link": "CORROBORATION",
            "claims": args["claims"],
            "public_at": args["public_at"],
            "freshness": args["freshness"],
        }
    result = await service.dispatch(tool, args, {})
    claim = result["accepted_claims"][0]
    stored = await repo.get("evt.claims", claim["claim_id"])
    assert stored["verified"] is False
    assert stored["verification_missing"] == gap
    values = wire_card_inputs()
    values[0]["claims"] = [stored]
    line = next(line for line in render_claim_wire(values).splitlines() if line.startswith("NOT VERIFIED:"))
    assert "NOT VERIFIED: World announces Product — " + gap in line


@pytest.mark.parametrize("provided", [False, True])
def test_missing_verification_gap_does_not_invent_a_reason(provided) -> None:
    values = wire_card_inputs()
    claim = array_field(values[0], "claims")[0]
    assert isinstance(claim, dict)
    claim["verified"] = False
    if provided:
        claim["verification_missing"] = None
    line = next(line for line in render_claim_wire(values).splitlines() if line.startswith("NOT VERIFIED:"))
    assert "n/a (verification gap not supplied)" in line
    assert "HIGH-certainty primary confirmation missing" not in line
    assert "None" not in line


@pytest.mark.parametrize("accepted", [True, False])
def test_outside_participant_keeps_current_claim_eligibility_without_a_financial_block(
    accepted: bool,
) -> None:
    values = wire_card_inputs()
    event = values[0]
    claims = array_field(event, "claims")
    claims.append(
        {
            "claim_id": "outside-integration",
            "subject": "Venue X",
            "predicate": "built",
            "object": "World integration",
            "accepted": accepted,
            "verified": True,
            "state_claim": "BUILT",
            "evidence": [{"item_id": "venue-source", "quote": "Venue X built its World integration."}],
        }
    )
    event["beneficiary_edges"] = [
        {
            "entity_id": "venue-x",
            "entity_name": "Venue X",
            "token_id": None,
            "role": "SECONDARY",
            "credible": False,
            "tradable": False,
            "layer": "VENUE_DEX",
            "integration_state": "BUILT",
            "integration_state_evidence": ["outside-integration"],
            "evidence": ["outside-integration"],
        }
    ]
    card = render_claim_wire(values)
    who = next(line for line in card.splitlines() if line.startswith("WHO:"))
    if accepted:
        assert "secondary Venue X" in who and "BUILT" in who
        assert "no market block" in who
    else:
        assert "Venue X" not in who
    assert card.count("── ") == 1


def test_outside_participant_cannot_be_promoted_into_a_selected_financial_block() -> None:
    from foundation_helpers import object_field

    values = wire_card_inputs()
    labels = object_field(values[0], "token_labels")
    del labels["world:WLD"]
    with pytest.raises(ValueError, match="outside-scope participant"):
        render_claim_wire(values)

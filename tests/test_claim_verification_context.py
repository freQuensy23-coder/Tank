"""An analyst's verification gap survives the public tools into the card."""

from pathlib import Path

import pytest
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_versions
from test_card_template import card_inputs, render
from test_event_tools import seed

from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository


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
    values = card_inputs()
    values[0]["claims"] = [stored]
    line = next(line for line in render(values).splitlines() if line.startswith("NOT VERIFIED:"))
    assert "NOT VERIFIED: World announces Product — " + gap in line


@pytest.mark.parametrize("provided", [False, True])
def test_missing_verification_gap_does_not_invent_a_reason(provided) -> None:
    values = card_inputs()
    claim = values[0]["claims"][0]
    claim["verified"] = False
    if provided:
        claim["verification_missing"] = None
    line = next(line for line in render(values).splitlines() if line.startswith("NOT VERIFIED:"))
    assert "n/a (verification gap not supplied)" in line
    assert "HIGH-certainty primary confirmation missing" not in line
    assert "None" not in line

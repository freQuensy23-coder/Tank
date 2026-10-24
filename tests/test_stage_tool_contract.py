"""The uploaded producer contract makes quoted stage evidence explicit."""

import json
from pathlib import Path

import pytest

from tank.event_tools import validate_schema

DIRECTORY = Path(__file__).parents[1] / "config/skills/tank-event-analyst"


@pytest.mark.parametrize("name", ["record_event", "add_observation"])
def test_stage_claim_contract_is_closed_synced_and_preserves_unknown(name):
    schema = json.loads((DIRECTORY / f"{name}.schema.json").read_text())
    tool = next(t for t in json.loads((DIRECTORY / "tools.json").read_text()) if t["name"] == name)
    assert tool["input_schema"] == {
        k: v for k, v in schema.items() if k not in {"$schema", "title", "description"}
    }
    assert tool["description"] == schema["description"]
    claim_schema = schema["properties"]["claims"]["items"]
    claim = {
        "type": "STATE",
        "subject": "SEC",
        "predicate": "proposes",
        "object": "rule",
        "state_claim": "PROPOSED",
        "directionality": "NONE",
        "evidence_class": "PRIMARY_FACT",
        "quote": "The Commission is proposing a rule.",
        "verified": True,
    }
    validate_schema(claim, claim_schema)
    validate_schema({**claim, "type": "FACT", "state_claim": None}, claim_schema)
    for stage in [
        "IN_DEVELOPMENT",
        "BUILT",
        "MATERIAL_USAGE",
        "ECONOMIC_FLOW",
        "CANCELLED",
        "REVERSED",
        "REJECTED",
        "PAUSED",
    ]:
        validate_schema({**claim, "state_claim": stage}, claim_schema)
    with pytest.raises(ValueError):
        validate_schema({**claim, "state_claim": "ANNOUNCEMENT_MEANS_LIVE"}, claim_schema)


@pytest.mark.parametrize("reason", [None, "", "   "])
@pytest.mark.parametrize("public", [True, False])
def test_rejection_contract_requires_a_concrete_reason(reason, public):
    from test_event_tools import extraction

    from tank.event_tools import domain_schema
    from tank.settings import Settings

    schema = (
        json.loads((DIRECTORY / "record_event.schema.json").read_text())
        if public
        else domain_schema(Settings(_env_file=None), "record_event")
    )
    value = extraction()
    if public:
        value.pop("item_id")
    validate_schema(value, schema)
    value["is_event"] = False
    if reason is not None:
        value["no_event_reason"] = reason
    with pytest.raises(ValueError):
        validate_schema(value, schema)
    value["no_event_reason"] = "Мнение без нового фактического утверждения"
    validate_schema(value, schema)

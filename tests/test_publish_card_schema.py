"""Declared publish-card schema surfaces share the existing structural narrative contract."""

from pathlib import Path

import pytest

from tank.contracts.json import decode_json
from tank.contracts.tool_schema import parse_tool_definitions, validate_schema
from tank.models import Record

ROOT = Path(__file__).resolve().parents[1] / "config/skills/tank-event-analyst"


def test_publish_card_schema_and_tool_manifest_share_the_same_narrative_constraints() -> None:
    definitions = parse_tool_definitions(decode_json((ROOT / "tools.json").read_bytes()))
    manifest = next(tool.input_schema for tool in definitions if tool.name == "publish_card")
    standalone = decode_json((ROOT / "publish_card.schema.json").read_bytes())
    assert isinstance(standalone, dict)
    manifest_properties = manifest["properties"]
    standalone_properties = standalone["properties"]
    assert isinstance(manifest_properties, dict) and isinstance(standalone_properties, dict)
    for field in ("verdict", "why"):
        assert manifest_properties[field] == standalone_properties[field]
    verdict = manifest_properties["verdict"]
    why = manifest_properties["why"]
    assert isinstance(verdict, dict) and isinstance(why, dict)
    assert verdict["minLength"] == 1 and verdict["pattern"] == "\\S"
    assert why["minItems"] == 1 and why["maxItems"] == 3
    bullet = why["items"]
    assert isinstance(bullet, dict)
    assert bullet["minLength"] == 1 and bullet["pattern"] == "\\S"
    assert bullet["maxLength"] == 220


def test_publish_card_applies_conditional_narrative_contract_to_actual_requests() -> None:
    definitions = parse_tool_definitions(decode_json((ROOT / "tools.json").read_bytes()))
    schema = next(tool.input_schema for tool in definitions if tool.name == "publish_card")
    preliminary: Record = {
        "kind": "PRELIMINARY",
        "what": "Primary announcement",
        "verdict": "Scope unconfirmed",
        "why": ["Potential access"],
        "unconfirmed": "No confirmed rollout",
    }
    validate_schema(preliminary, schema)
    for kind in ("FULL", "UPDATE", "PARTIAL"):
        ordinary = {key: value for key, value in preliminary.items() if key != "unconfirmed"}
        ordinary["kind"] = kind
        with pytest.raises(ValueError):
            validate_schema(ordinary, schema)
        ordinary["why"] = ["Primary announcement", "Scope is not yet quantified"]
        validate_schema(ordinary, schema)
        with pytest.raises(ValueError):
            validate_schema({**ordinary, "unconfirmed": "Preliminary-only wording"}, schema)

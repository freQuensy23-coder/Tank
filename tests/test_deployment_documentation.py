"""Deployment documentation and skill/tool contracts that the code must keep in sync."""

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_x_list_is_configured_in_sources_not_empty_compose_environment():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    assert "TANK_X_LIST_ID" not in compose["services"]["tank"]["environment"]
    assert "TANK_X_LIST_ID=" not in (ROOT / "deploy/compose.env.example").read_text()
    assert "S29.list_id" in (ROOT / "deploy/compose.env.example").read_text()


def test_changed_tool_descriptions_remain_exactly_synced():
    directory = ROOT / "config/skills/tank-event-analyst"
    tools = {tool["name"]: tool for tool in json.loads((directory / "tools.json").read_text())}
    for name in ("set_assessment", "publish_card"):
        schema = json.loads((directory / f"{name}.schema.json").read_text())
        assert tools[name]["description"] == schema["description"]
        assert tools[name]["input_schema"] == {
            key: value for key, value in schema.items() if key not in {"$schema", "title", "description"}
        }
    skill = (directory / "SKILL.md").read_text()
    # 03 §3.2 playbook step 5, verbatim (the skill carries the spec playbook).
    assert "Repeat coverage → `add_observation` CORROBORATION, and stop." in skill
    assert "NO_UNIVERSE_CONCERN" in skill
    assert "delivery_status: QUEUED" in skill


def test_operations_documents_both_native_architectures():
    # The HLD is frozen spec (COORDINATION.md); tests never pin edits to it.
    operations = (ROOT / "docs/OPERATIONS.md").read_text()
    assert "--platform linux/arm64 --provenance=false" in operations
    assert "--architecture arm64" in operations
    assert "linux/amd64" in operations and "--architecture amd64" in operations

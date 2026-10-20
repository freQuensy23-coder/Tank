"""The deployable Compose contract is fully inventoried in the HLD."""

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def inventory_rows(document):
    return {
        match.group(1): match.group(2)
        for match in re.finditer(r"^\| `([^`]+)` \| (.+)$", document, re.MULTILINE)
    }


def test_hld_inventories_all_compose_secrets_and_consumer_services():
    base = yaml.safe_load((ROOT / "compose.yaml").read_text())
    optional = yaml.safe_load((ROOT / "compose.x.yaml").read_text())
    hld = (ROOT / "02_Tank_V0_HLD.md").read_text().split("### 8.1 Complete deployment inventory", 1)[1]
    rows = inventory_rows(hld)
    for name in set(base["secrets"]) | set(optional["secrets"]):
        assert name in rows, name
        consumers = set()
        for compose in (base, optional):
            consumers.update(
                service
                for service, config in compose["services"].items()
                if name in config.get("secrets", [])
            )
        documented = set(re.findall(r"`(tank|openma|postgres)`", rows[name].split("|", 1)[0]))
        assert documented == consumers, name


def test_hld_inventories_volumes_and_native_architecture_boundary():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    hld = (ROOT / "02_Tank_V0_HLD.md").read_text()
    rows = inventory_rows(hld.split("### 8.1 Complete deployment inventory", 1)[1])
    assert set(compose["volumes"]) <= rows.keys()
    for phrase in (
        "linux/arm64",
        "linux/amd64",
        "UNQUALIFIED_REBUILD",
        "x86-64 deployment requires its own",
        "openma_lifecycle",
        "openma_execution",
        "openma_skill_aliases",
        "openma_file_io",
        "openma_replay_network",
        "openma_continuation",
        "openma_tool_kind",
    ):
        assert phrase in hld


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


def test_source_modules_are_not_documented_as_extra_services():
    hld = (ROOT / "02_Tank_V0_HLD.md").read_text()
    assert "every payload is written to disk" not in hld
    assert "other market responses persist normalized typed fields under TDD §14" in hld
    assert "tank-xsensor" not in hld
    assert "tank-ingest" not in hld
    assert "single `tank` asyncio service" in hld
    operations = (ROOT / "docs/OPERATIONS.md").read_text()
    assert "--platform linux/arm64 --provenance=false" in operations
    assert "--architecture arm64" in operations
    assert "linux/amd64" in operations and "--architecture amd64" in operations

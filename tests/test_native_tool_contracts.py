"""Required managed manifests and semantic native tool readback facts."""

from typing import Literal

import pytest
from pydantic import JsonValue

from tank.contracts.openma import (
    BuiltinToolObservation,
    ManagedBuiltinToolset,
    ManagedToolManifest,
    NativeSessionToolOverride,
    NativeSessionToolUpdate,
    OpenMAContractViolation,
    ToolEnablement,
    observe_openma_tool,
    openma_tools_match,
)
from tank.openma_sync import agent_tools
from tank.util import canonical_json, sha256


class DefinitionReader:
    def read_config_json(self, relative: str) -> JsonValue:
        assert relative in {
            "skills/tank-event-analyst/tools.json",
            "skills/tank-scout-reviewer/tools.json",
        }
        return [
            {
                "name": "synthetic_tool",
                "description": "Declared synthetic call",
                "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
            }
        ]


@pytest.mark.parametrize("agent", ["tank-event-analyst", "tank-scout-reviewer"])
def test_managed_manifest_keeps_the_existing_request_and_fingerprint_wire(
    agent: Literal["tank-event-analyst", "tank-scout-reviewer"],
) -> None:
    reader = DefinitionReader()
    manifest = ManagedToolManifest.from_definitions(
        agent, reader.read_config_json(f"skills/{agent}/tools.json")
    )
    original: list[JsonValue] = [tool for tool in agent_tools(reader, agent)]
    request = NativeSessionToolUpdate(agent=NativeSessionToolOverride(tools=manifest.tools))
    assert canonical_json(request) == canonical_json({"agent": {"tools": original}})
    assert sha256(manifest.tools) == sha256(original)
    assert manifest.agent == agent


def test_unreported_switch_array_cannot_confirm_a_requested_empty_array() -> None:
    desired = ManagedBuiltinToolset(
        type="agent_toolset_20260401", default_config=ToolEnablement(enabled=True), configs=[]
    )
    unreported = observe_openma_tool({"type": "agent_toolset_20260401", "default_config": {"enabled": True}})
    assert isinstance(unreported, BuiltinToolObservation) and unreported.configs is None
    assert not openma_tools_match([unreported], [desired])
    confirmed = observe_openma_tool(
        {"type": "agent_toolset_20260401", "default_config": {"enabled": True}, "configs": []}
    )
    assert openma_tools_match([confirmed], [desired])


def test_provider_permission_details_normalize_without_hiding_owned_switch_changes() -> None:
    manifest = ManagedToolManifest.from_definitions(
        "tank-event-analyst", DefinitionReader().read_config_json("skills/tank-event-analyst/tools.json")
    )
    builtin: dict[str, JsonValue] = {
        "type": "agent_toolset_20260401",
        "default_config": {"enabled": True, "permission_policy": {"type": "always_allow"}},
        "configs": [
            {
                "type": "browser",
                "name": "browser",
                "enabled": True,
                "permission_policy": {"type": "always_allow"},
            }
        ],
    }
    custom: dict[str, JsonValue] = {
        "type": "custom",
        "name": "synthetic_tool",
        "description": "Declared synthetic call",
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    }
    observed = [observe_openma_tool(custom), observe_openma_tool(builtin)]
    assert openma_tools_match(observed, manifest.tools)
    configs = builtin["configs"]
    assert isinstance(configs, list)
    config = configs[0]
    assert isinstance(config, dict)
    config["enabled"] = False
    assert not openma_tools_match([observe_openma_tool(custom), observe_openma_tool(builtin)], manifest.tools)


def test_duplicate_observed_tool_identity_is_not_resolved_by_array_position() -> None:
    manifest = ManagedToolManifest.from_definitions(
        "tank-event-analyst", DefinitionReader().read_config_json("skills/tank-event-analyst/tools.json")
    )
    one = observe_openma_tool(
        {"type": "custom", "name": "synthetic_tool", "description": "", "input_schema": {"type": "object"}}
    )
    with pytest.raises(OpenMAContractViolation, match="repeats an identity"):
        openma_tools_match([one, one], manifest.tools)

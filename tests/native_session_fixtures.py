"""Complete synthetic native sessions with declared resource bindings."""

import httpx
from pydantic import JsonValue

from tank.agents.native_sessions import NativeSessionCreate
from tank.util import clock_iso


def native_session_record(
    session_id: str,
    *,
    agent_id: str,
    environment_id: str,
    metadata: dict[str, str],
    model_id: str,
    agent_snapshot: dict[str, JsonValue] | None = None,
) -> dict[str, JsonValue]:
    at = clock_iso()
    agent: dict[str, JsonValue] = {
        "id": agent_id,
        "version": 1,
        "type": "agent",
        "name": "Synthetic native agent",
        "description": None,
        "mcp_servers": [],
        "multiagent": None,
        "model": {"id": model_id},
        "system": "Synthetic resolved instructions",
        "skills": [],
        "tools": [],
    }
    if agent_snapshot is not None:
        agent.update(agent_snapshot)
    tools = agent["tools"]
    if not isinstance(tools, list):
        raise ValueError("Synthetic native tool definitions must be an array")
    agent["tools"] = resolved_native_tools(tools)
    return {
        "id": session_id,
        "agent": agent,
        "archived_at": None,
        "budget": None,
        "created_at": at,
        "environment_id": environment_id,
        "metadata": {key: value for key, value in metadata.items()},
        "outcome_evaluations": [],
        "resources": [],
        "stats": {},
        "status": "idle",
        "title": None,
        "type": "session",
        "updated_at": at,
        "usage": {},
        "vault_ids": [],
    }


def created_native_session(
    request: httpx.Request,
    session_id: str,
    *,
    agent_snapshot: dict[str, JsonValue] | None = None,
) -> dict[str, JsonValue]:
    posted = NativeSessionCreate.model_validate_json(request.content)
    return native_session_record(
        session_id,
        agent_id=posted.agent,
        environment_id=posted.environment_id,
        metadata=posted.metadata.expected_fields(),
        model_id="gpt-6.1-sol",
        agent_snapshot=agent_snapshot,
    )


def resolved_native_tools(tools: list[JsonValue]) -> list[JsonValue]:
    """Apply pinned definition-resolution.ts defaults in the synthetic provider."""
    resolved: list[JsonValue] = []
    for raw in tools:
        if not isinstance(raw, dict):
            raise ValueError("Synthetic native tool definition must be an object")
        kind = raw["type"]
        if kind == "custom":
            resolved.append(dict(raw))
            continue
        if kind != "agent_toolset_20260401" and kind != "mcp_toolset":
            raise ValueError("Synthetic native tool type is unsupported")
        supplied_default = raw.get("default_config")
        if supplied_default is None:
            supplied_default = {}
        if not isinstance(supplied_default, dict):
            raise ValueError("Synthetic native default configuration must be an object")
        default: dict[str, JsonValue] = {
            "enabled": supplied_default.get("enabled", True),
            "permission_policy": supplied_default.get("permission_policy", {"type": "always_allow"}),
        }
        supplied_configs = raw.get("configs")
        if supplied_configs is None:
            supplied_configs = []
        if not isinstance(supplied_configs, list):
            raise ValueError("Synthetic native configurations must be an array")
        configs: list[JsonValue] = []
        for source in supplied_configs:
            if not isinstance(source, dict):
                raise ValueError("Synthetic native configuration must be an object")
            config: dict[str, JsonValue] = {
                **source,
                "enabled": source.get("enabled", default["enabled"]),
                "permission_policy": source.get("permission_policy", default["permission_policy"]),
            }
            if kind == "agent_toolset_20260401":
                config["type"] = source["name"]
            configs.append(config)
        resolved.append({**raw, "default_config": default, "configs": configs})
    return resolved

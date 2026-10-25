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

"""Native tool migration commits only after the actual session confirms its tools."""

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_session_fixtures import native_session_record
from pydantic import JsonValue

from tank.agents.context import CONTEXT_VERSION
from tank.agents.sessions import SessionService
from tank.contracts.json import decode_json
from tank.contracts.openma import OpenMAContractViolation
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
async def test_tool_update_requires_readback_and_recovers_lost_acceptance_without_republication(
    accepted: bool,
) -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {
        "session_id": "session",
        "model_context_version": CONTEXT_VERSION - 1,
        "runtime_status": "IDLE",
        "active_message_id": None,
        "message_log": [],
        "messages": [],
        "prompt_label": "declared-old-label",
    }
    await repo.put("ops.agent_sessions", "session", original)
    actual_tools: list[JsonValue] = []
    publications = 0
    lose_acceptance = accepted

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal actual_tools, publications, lose_acceptance
        assert request.url.path == "/v1/sessions/session"
        if request.method == "GET":
            return httpx.Response(
                200,
                json=native_session_record(
                    "session",
                    agent_id="agent",
                    environment_id="environment",
                    metadata={},
                    model_id="model",
                    agent_snapshot={"tools": actual_tools},
                ),
            )
        assert request.method == "POST"
        publications += 1
        value = decode_json(request.content)
        assert isinstance(value, dict)
        agent = value["agent"]
        assert isinstance(agent, dict)
        tools = agent["tools"]
        assert isinstance(tools, list)
        if accepted:
            actual_tools = tools
        if lose_acceptance:
            lose_acceptance = False
            raise httpx.ReadError("Synthetic lost response after acceptance", request=request)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
        )
        expected_error = httpx.ReadError if accepted else OpenMAContractViolation
        with pytest.raises(expected_error):
            await service.sync_tools("session")
        assert await repo.get("ops.agent_sessions", "session") == original
        assert publications == 1
        if accepted:
            result = await service.sync_tools("session")
            assert result.status == "UPDATED"
            assert publications == 1
            stored = await repo.get("ops.agent_sessions", "session")
            assert stored is not None
            assert stored["model_context_version"] == CONTEXT_VERSION
            assert isinstance(stored["tools_sha256"], str)
            assert stored["prompt_version_basis"] == "RESOLVED_SESSION_AGENT_SNAPSHOT"
            assert stored["prompt_label"] == "declared-old-label"


@pytest.mark.asyncio
async def test_local_owner_change_during_native_confirmation_cannot_commit_a_migration() -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {
        "session_id": "session",
        "model_context_version": CONTEXT_VERSION - 1,
        "runtime_status": "IDLE",
        "active_message_id": None,
        "message_log": [],
        "messages": [],
    }
    await repo.put("ops.agent_sessions", "session", original)
    actual_tools: list[JsonValue] = []
    changed: dict[str, JsonValue] = {**original, "runtime_status": "RUNNING"}

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal actual_tools
        if request.method == "POST":
            value = decode_json(request.content)
            assert isinstance(value, dict)
            agent = value["agent"]
            assert isinstance(agent, dict)
            tools = agent["tools"]
            assert isinstance(tools, list)
            actual_tools = tools
            await repo.put("ops.agent_sessions", "session", changed)
            return httpx.Response(200, json={})
        return httpx.Response(
            200,
            json=native_session_record(
                "session",
                agent_id="agent",
                environment_id="environment",
                metadata={},
                model_id="model",
                agent_snapshot={"tools": actual_tools},
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(ValueError, match="ownership changed"):
            await service.sync_tools("session")
    assert await repo.get("ops.agent_sessions", "session") == changed

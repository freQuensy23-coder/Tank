"""The native tool boundary cannot dispatch when its required owned schema cannot load."""

from typing import Literal

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from session_followup_fixtures import UnexpectedFollowupRefusals

from tank.agents.context import CONTEXT_VERSION
from tank.agents.sessions import SessionService
from tank.contracts.json import decode_json
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import canonical_json

PRIVATE = "FAKE_UNCONFIGURED_SCHEMA_INPUT_99281"


class SchemaSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)
    failure: Literal["io", "wrong_root", "duplicate", "invalid_definition"]

    def read_config_json(self, relative: str) -> JsonValue:
        if self.failure == "io":
            raise FileNotFoundError(PRIVATE)
        if self.failure == "wrong_root":
            return {"raw": PRIVATE}
        if self.failure == "invalid_definition":
            return [{"name": "web_search", "description": "Synthetic tool", "input_schema": [PRIVATE]}]
        definition: dict[str, JsonValue] = {
            "name": "web_search",
            "description": "Synthetic tool",
            "input_schema": {"type": "object"},
        }
        return [definition, definition]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["io", "wrong_root", "duplicate", "invalid_definition"])
async def test_required_tool_schema_failure_refuses_dispatch_without_payload_leak(
    failure: Literal["io", "wrong_root", "duplicate", "invalid_definition"],
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = MemoryRepository()
    await repo.put(
        "ops.agent_sessions",
        "session",
        {
            "session_id": "session",
            "status": "OPEN",
            "event_id": None,
            "item_id": None,
            "model_context_version": CONTEXT_VERSION,
            "message_log": [],
        },
    )
    called: list[str] = []
    posts: list[JsonValue] = []

    async def dispatch(
        tool: str, arguments: dict[str, JsonValue], context: dict[str, JsonValue]
    ) -> JsonValue:
        called.append(tool)
        raise AssertionError("Missing owned schema must not dispatch a tool")

    def native(request: httpx.Request) -> httpx.Response:
        posts.append(decode_json(request.content))
        return httpx.Response(200, json={})

    settings = SchemaSettings(failure=failure, openma_url="http://native", ops_progress_enabled=False)
    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as http:
        service = SessionService(
            repo,
            settings,
            dispatch,
            versions=declared_versions_fixture(),
            http=http,
            refusals=UnexpectedFollowupRefusals(),
        )
        await service.process_event(
            "session",
            {
                "id": "schema",
                "type": "agent.custom_tool_use",
                "name": "web_search",
                "input": {"query": "Public evidence"},
            },
        )
    captured = await repo.get("ops.tool_calls", "session:schema")
    assert captured is not None and captured["status"] in {"ERROR", "REFUSED"}
    assert not called and len(posts) == 1
    assert PRIVATE not in canonical_json(captured)
    assert PRIVATE not in canonical_json(posts)
    assert PRIVATE not in caplog.text

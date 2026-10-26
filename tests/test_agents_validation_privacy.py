"""Raw schema inputs and validator diagnostics cannot escape a real native tool response."""

from typing import Never

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue, field_validator
from session_followup_fixtures import UnexpectedFollowupRefusals
from test_agents_session_audit import seed, settings

from tank.agents.sessions import SessionService
from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.storage import MemoryRepository
from tank.util import canonical_json

_PRIVATE = "FAKE_UNCONFIGURED_PROVIDER_CREDENTIAL_81271"


class ProviderMeasurement(DomainModel):
    amount: int

    @field_validator("amount", mode="before")
    @classmethod
    def reject_provider_amount(cls, value: JsonValue) -> Never:
        raise ValueError(f"Private provider diagnostic: {value}")


async def test_schema_failure_never_enters_tool_capture_native_body_or_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = MemoryRepository()
    await seed(repo)
    native_posts: list[JsonValue] = []

    async def dispatch(
        tool: str, arguments: dict[str, JsonValue], context: dict[str, JsonValue]
    ) -> JsonValue:
        ProviderMeasurement.model_validate({"amount": _PRIVATE})
        raise AssertionError("Synthetic provider validator unexpectedly accepted input")

    def native(request: httpx.Request) -> httpx.Response:
        native_posts.append(decode_json(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as client:
        service = SessionService(
            repo,
            settings(),
            dispatch,
            http=client,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        await service.process_event(
            "s",
            {
                "id": "schema-privacy",
                "type": "agent.custom_tool_use",
                "name": "web_search",
                "input": {"query": "Public evidence"},
            },
        )
    capture = await repo.get("ops.tool_calls", "s:schema-privacy")
    assert capture is not None and capture["status"] == "REFUSED"
    assert capture["result"]["error"]["type"] == "ValidationError"
    assert capture["result"]["error"]["category"] == "VALIDATION"
    assert capture["is_error"] is True and capture["returned_at"]
    assert native_posts
    assert _PRIVATE not in canonical_json(capture) + canonical_json(native_posts) + caplog.text
    assert "Private provider diagnostic" not in canonical_json(capture)

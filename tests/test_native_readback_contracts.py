"""Actual native GET shapes and resource absence through the HTTP boundary."""

from typing import Literal

import httpx
import pytest
from native_session_fixtures import native_session_record
from pydantic import JsonValue, ValidationError

from tank.agents.native_readback import (
    NativeSessionMissing,
    NativeSessionReader,
    NativeSessionReference,
    NativeSessionState,
    observe_native_state,
)
from tank.contracts.json import decode_json


class HttpEndpoint:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http

    async def request(
        self,
        method: Literal["GET", "POST"],
        path: str,
        *,
        json: JsonValue = None,
        params: dict[str, str | int] | None = None,
        idem: str | None = None,
    ) -> dict[str, JsonValue]:
        assert method == "GET"
        assert json is None and params is None and idem is None
        response = await self.http.get("http://native/v1/" + path)
        response.raise_for_status()
        value = decode_json(response.content)
        assert isinstance(value, dict)
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["rescheduling", "running", "idle", "terminated"])
async def test_direct_native_state_keeps_the_actual_lifecycle(status: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/sessions/session"
        return httpx.Response(200, json={"id": "session", "status": status, "archived_at": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        observed = await NativeSessionReader(HttpEndpoint(http)).state("session")
    assert isinstance(observed, NativeSessionState)
    assert observed.status == status and observed.archived_at is None


@pytest.mark.asyncio
async def test_http404_is_missing_without_a_fabricated_terminated_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "synthetic-private-provider-message"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        observed = await NativeSessionReader(HttpEndpoint(http)).state("session")
    assert isinstance(observed, NativeSessionMissing)
    assert observed.model_dump(mode="json") == {"kind": "MISSING", "missing": True, "session_id": "session"}
    assert "status" not in NativeSessionMissing.model_fields


@pytest.mark.asyncio
async def test_other_http_failure_does_not_become_a_missing_resource() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await NativeSessionReader(HttpEndpoint(http)).state("session")


@pytest.mark.parametrize(
    "wire",
    [
        {},
        {"data": {"id": "session", "status": "idle", "archived_at": None}},
        {"id": "session", "status": "idle"},
        {"id": "session", "archived_at": None},
        {"id": "other", "status": "idle", "archived_at": None},
        {"id": "session", "status": "unknown", "archived_at": None},
    ],
)
def test_incomplete_wrapped_or_mismatched_lookup_cannot_claim_a_live_session(
    wire: dict[str, JsonValue],
) -> None:
    with pytest.raises((KeyError, ValueError)):
        observe_native_state(wire, NativeSessionReference(session_id="session"))


@pytest.mark.asyncio
async def test_lookup_encodes_a_resource_id_as_one_path_segment() -> None:
    identity = "session/with?delimiter"
    paths: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.raw_path)
        return httpx.Response(200, json={"id": identity, "status": "idle", "archived_at": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        observed = await NativeSessionReader(HttpEndpoint(http)).state(identity)
    assert observed.session_id == identity
    assert paths == [b"/v1/sessions/session%2Fwith%3Fdelimiter"]


def test_missing_lookup_identifier_is_not_replaced_by_an_empty_path() -> None:
    with pytest.raises(ValidationError):
        NativeSessionReference(session_id="")


@pytest.mark.asyncio
async def test_prompt_recovery_uses_the_exact_native_session_snapshot() -> None:
    paths: list[str] = []
    wire = native_session_record(
        "session",
        agent_id="observed-agent",
        environment_id="environment",
        metadata={},
        model_id="observed-model",
        agent_snapshot={"version": 9, "system": "Frozen observed session instructions"},
    )

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json=wire)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        found = await NativeSessionReader(HttpEndpoint(http)).prompt(
            "session", prompt_label="legacy-declaration"
        )
    assert not isinstance(found, NativeSessionMissing)
    descriptor = found.observation.provenance
    assert descriptor.agent_id == "observed-agent" and descriptor.agent_version == "9"
    assert descriptor.model_id == "observed-model"
    assert descriptor.prompt_label == "legacy-declaration"
    assert descriptor.prompt_version is not None
    assert paths == ["/v1/sessions/session"]


@pytest.mark.asyncio
async def test_partial_nested_tool_report_cannot_verify_prompt_provenance() -> None:
    wire = native_session_record(
        "session",
        agent_id="agent",
        environment_id="environment",
        metadata={},
        model_id="model",
    )
    agent = wire["agent"]
    assert isinstance(agent, dict)
    agent["tools"] = [{"type": "agent_toolset_20260401", "default_config": {"enabled": True}}]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=wire)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(ValueError, match="incomplete"):
            await NativeSessionReader(HttpEndpoint(http)).prompt("session", prompt_label=None)

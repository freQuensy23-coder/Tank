"""Accepted execution owners require recorded message and retry lineage."""

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue
from session_followup_fixtures import UnexpectedFollowupRefusals

from tank.agents.input_ownership import NativeInputOwner, UnprovedInputOwnership
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["current", "superseded", "tool"])
async def test_actual_service_resolves_recorded_current_superseded_and_tool_input_owners(kind: str) -> None:
    repo = MemoryRepository()
    message: dict[str, JsonValue] = {
        "message_id": "message",
        "payload": {"kind": "NEW_ITEM", "item_id": "item"},
        "retry_count": 1,
        "remote_event_id": "current",
        "superseded_native_input_ids": ["superseded"],
        "native_input_attempts": {"superseded": 0, "current": 1},
    }
    session: dict[str, JsonValue] = {"session_id": "session", "message_log": [message]}
    await repo.put(
        "ops.tool_calls",
        "session:callback",
        {
            "session_id": "session",
            "remote_event_id": "tool",
            "native_turn_message_id": "message",
            "native_turn_retry_count": 1,
        },
    )

    def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Ownership resolution must not call native HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        service = SessionService(
            repo,
            Settings(_env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        owner = await service.native_input_owner(session, kind)
    assert owner == NativeInputOwner(message_id="message", attempt=0 if kind == "superseded" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["superseded", "tool"])
async def test_historical_unknown_retry_attempt_cannot_be_invented_from_position_or_zero(kind: str) -> None:
    repo = MemoryRepository()
    message: dict[str, JsonValue] = {
        "message_id": "message",
        "payload": {"kind": "NEW_ITEM", "item_id": "item"},
        "retry_count": 1,
        "remote_event_id": "current",
        "superseded_native_input_ids": ["superseded"],
    }
    session: dict[str, JsonValue] = {"session_id": "session", "message_log": [message]}
    await repo.put(
        "ops.tool_calls",
        "session:callback",
        {"session_id": "session", "remote_event_id": "tool", "native_turn_message_id": "message"},
    )
    async with httpx.AsyncClient() as http:
        service = SessionService(
            repo,
            Settings(_env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(UnprovedInputOwnership, match="recorded retry attempt"):
            await service.native_input_owner(session, kind)
    assert message["retry_count"] == 1
    assert "native_input_attempts" not in message


@pytest.mark.asyncio
async def test_two_actual_messages_cannot_claim_one_native_input() -> None:
    repo = MemoryRepository()
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "message_log": [
            {
                "message_id": identity,
                "payload": {"kind": "NEW_ITEM", "item_id": "item"},
                "retry_count": 0,
                "remote_event_id": "shared-input",
            }
            for identity in ("first", "second")
        ],
    }
    async with httpx.AsyncClient() as http:
        service = SessionService(
            repo,
            Settings(_env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(ValueError, match="conflicting journal ownership"):
            await service.native_input_owner(session, "shared-input")

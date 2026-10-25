"""Active input ownership is enforced through actual message and terminal lifecycle paths."""

from pathlib import Path
from typing import Literal

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.agents.context_binding import ContextBinding
from tank.agents.session_ownership import put_owned_session
from tank.agents.sessions import SessionService
from tank.contracts.json import decode_json
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import canonical_json


class OwnershipSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def settings(tmp_path: Path) -> OwnershipSettings:
    return OwnershipSettings(
        data_dir=tmp_path,
        ops_progress_enabled=False,
        openma_url="http://native",
        openma_api_key="synthetic-owner-key",
        database_url="postgresql://fixture:synthetic-owner-db-key@database/fixture",
    )


@pytest.mark.parametrize("journal", ["absent", "empty", "old"])
def test_explicit_active_pointer_never_uses_a_stale_last_payload(
    journal: Literal["absent", "empty", "old"],
) -> None:
    record: dict[str, JsonValue] = {
        "session_id": "session",
        "active_message_id": "missing-owner",
        "item_id": "origin",
        "last_payload": {"kind": "NEW_ITEM", "item_id": "stale"},
    }
    if journal == "empty":
        record["message_log"] = []
    elif journal == "old":
        record["message_log"] = [{"message_id": "old", "payload": {"item_id": "stale"}}]
    with pytest.raises(ValueError, match="authoritative journal owner"):
        ContextBinding.from_session(record)


@pytest.mark.parametrize("entrypoint", ["message", "terminal"])
async def test_corrupt_owner_cannot_post_or_change_terminal_source_state(
    tmp_path: Path, entrypoint: Literal["message", "terminal"]
) -> None:
    repo = MemoryRepository()
    for item in ("origin", "stale"):
        await repo.put("ingest.source_items", item, {"item_id": item, "extraction_status": "PENDING"})
    record: dict[str, JsonValue] = {
        "session_id": "session",
        "event_id": None,
        "item_id": "origin",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "missing-owner",
        "message_log": [{"message_id": "old", "payload": {"kind": "NEW_ITEM", "item_id": "stale"}}],
        "last_payload": {"kind": "NEW_ITEM", "item_id": "stale"},
        "retry_count": 1,
    }
    await repo.put("ops.agent_sessions", "session", record)
    original = canonical_json(record)
    native: list[JsonValue] = []

    def handler(request: httpx.Request) -> httpx.Response:
        native.append(decode_json(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = SessionService(
            repo,
            settings(tmp_path),
            None,
            http=client,
            stream_http=client,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(ValueError, match="authoritative journal owner"):
            if entrypoint == "message":
                await service.send_message(
                    {"session_id": "session", "kind": "ITEM_ADDED", "item_id": "origin"}
                )
            else:
                await service.process_event("session", {"id": "failure", "type": "agent.refusal"})
    assert native == []
    assert canonical_json(await repo.get("ops.agent_sessions", "session")) == original
    for item in ("origin", "stale"):
        source = await repo.get("ingest.source_items", item)
        assert source is not None and source["extraction_status"] == "PENDING"


async def test_active_callback_terminal_failure_does_not_fail_the_opening_or_last_source(
    tmp_path: Path,
) -> None:
    repo = MemoryRepository()
    for item in ("origin", "stale"):
        await repo.put("ingest.source_items", item, {"item_id": item, "extraction_status": "PENDING"})
    await put_owned_session(
        repo,
        "session",
        {
            "session_id": "session",
            "event_id": None,
            "item_id": "origin",
            "status": "OPEN",
            "runtime_status": "RUNNING",
            "active_message_id": "callback",
            "message_log": [
                {
                    "message_id": "callback",
                    "payload": {"kind": "CONDITION_TRIGGERED"},
                    "body": {"type": "user.message", "content": []},
                    "turn_status": "RUNNING",
                    "retry_count": 1,
                }
            ],
            "last_payload": {"kind": "NEW_ITEM", "item_id": "stale"},
        },
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    ) as client:
        service = SessionService(
            repo,
            settings(tmp_path),
            None,
            http=client,
            stream_http=client,
            versions=declared_versions_fixture(),
        )
        await service.process_event("session", {"id": "failure", "type": "agent.refusal"})
    session = await repo.get("ops.agent_sessions", "session")
    assert session is not None and session["status"] == "FAILED" and session["active_message_id"] is None
    for item in ("origin", "stale"):
        source = await repo.get("ingest.source_items", item)
        assert source is not None and source["extraction_status"] == "PENDING"


async def test_persistence_guard_refuses_an_unrecorded_pointer_before_store_write() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", {"session_id": "session", "active_message_id": None})
    with pytest.raises(ValueError, match="authoritative journal owner"):
        await put_owned_session(repo, "session", {"session_id": "session", "active_message_id": "missing"})
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored == {"session_id": "session", "active_message_id": None}


async def test_send_writer_revalidates_owner_after_reloading_under_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "origin", {"item_id": "origin", "text": "Synthetic source"})
    initial: dict[str, JsonValue] = {
        "session_id": "session",
        "model_context_version": 2,
        "status": "OPEN",
        "event_id": None,
        "item_id": "origin",
        "active_message_id": None,
        "message_log": [],
    }
    await repo.put("ops.agent_sessions", "session", initial)
    original_get = repo.get
    reads = 0

    async def changed_owner(collection: str, key: str) -> dict[str, JsonValue] | None:
        nonlocal reads
        record = await original_get(collection, key)
        if collection == "ops.agent_sessions":
            reads += 1
            if reads == 2:
                assert record is not None
                record["active_message_id"] = "unrecorded"
        return record

    monkeypatch.setattr(repo, "get", changed_owner)
    posts: list[str] = []

    def native(request: httpx.Request) -> httpx.Response:
        posts.append(request.method)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(native)) as client:
        service = SessionService(
            repo,
            settings(tmp_path),
            None,
            http=client,
            stream_http=client,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(ValueError, match="authoritative journal owner"):
            await service.send_message({"session_id": "session", "kind": "ITEM_ADDED", "item_id": "origin"})
    assert reads >= 2 and posts == []
    assert await original_get("ops.agent_sessions", "session") == initial


async def test_acknowledged_execution_can_rebind_only_to_its_actual_journal_owner(tmp_path: Path) -> None:
    from tank.agents.context import ModelContext

    repo = MemoryRepository()
    await repo.put(
        "ops.agent_sessions",
        "session",
        {
            "session_id": "session",
            "status": "OPEN",
            "event_id": "event",
            "item_id": "origin",
            "active_message_id": "unrecorded",
            "last_payload": {"kind": "NEW_ITEM", "item_id": "stale"},
            "message_log": [
                {
                    "message_id": "actual-owner",
                    "remote_event_id": "native-input",
                    "retry_count": 0,
                    "payload": {"kind": "ITEM_ADDED", "item_id": "authoritative"},
                }
            ],
        },
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    ) as client:
        service = SessionService(
            repo,
            settings(tmp_path),
            None,
            http=client,
            stream_http=client,
            versions=declared_versions_fixture(),
        )
        await service.process_event(
            "session",
            {"id": "running", "type": "session.status_running", "execution_id": "native-input"},
        )
    record = await repo.get("ops.agent_sessions", "session")
    assert record is not None and record["active_message_id"] == "actual-owner"
    arguments: dict[str, JsonValue] = {}
    ModelContext(repo, settings(tmp_path), record).bind("add_observation", arguments)
    assert arguments["item_id"] == "authoritative" and arguments["item_id"] != "stale"

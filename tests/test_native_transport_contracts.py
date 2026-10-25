"""Actual native page/stream chronology and durable host-refusal admission."""

import json
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Literal, Self

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

from tank.agents.native_transport import (
    NativeFrameIdentity,
    NativeRetryRefusalJournal,
    NativeRetryRefusalLedger,
    decode_native_stream,
)
from tank.agents.sessions import SessionService, seen_key
from tank.contracts.json import DuplicateJsonKeyError, decode_json
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.settings import Settings
from tank.storage import MemoryRepository, Repository
from tank.time_precision import collection_clock
from tank.util import JsonInput, canonical_json


def transport_state() -> dict[str, JsonValue]:
    old: dict[str, JsonValue] = {
        "message_id": "old",
        "body": {"type": "user.message", "content": [{"type": "text", "text": "Unknown old input"}]},
        "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "old-question"},
        "turn_status": "RUNNING",
        "remote_event_id": "old-input",
        "native_input_attempts": {"old-input": 0},
    }
    fresh: dict[str, JsonValue] = {
        "message_id": "fresh",
        "body": {
            "type": "user.message",
            "content": [{"type": "text", "text": 'Fresh opaque text {"x":1,"x":2}'}],
        },
        "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "fresh-question"},
        "turn_status": "PENDING",
        "remote_event_id": "fresh-input",
        "retry_count": 0,
    }
    return {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "proposal_id": "synthetic-proposal",
        "active_message_id": "old",
        "message_log": [old, fresh],
        "messages": ["old", "fresh"],
        "last_event_seq": 0,
        "model_id": "synthetic-model",
        "agent_version_reason": "Synthetic retained version was not reported",
        "last_item_at_reason": "Synthetic scout has no source item",
        **collection_clock().fields("opened_at"),
    }


def failure(identity: str, execution: str, sequence: int | None) -> dict[str, JsonValue]:
    return {
        "id": identity,
        "type": "session.error",
        "execution_id": execution,
        **({"seq": sequence} if sequence is not None else {}),
    }


def settings() -> Settings:
    return Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["managed-page", "node-page", "stream"])
@pytest.mark.parametrize("same_event_id", [False, True])
async def test_refused_old_frame_does_not_starve_fresh_owned_input(
    transport: str, same_event_id: bool
) -> None:
    repo = MemoryRepository()
    original = transport_state()
    await repo.put("ops.agent_sessions", "session", original)
    sequenced = transport != "managed-page"
    old = failure("shared-failure" if same_event_id else "old-failure", "old-input", 1 if sequenced else None)
    fresh = failure(
        "shared-failure" if same_event_id else "fresh-failure", "fresh-input", 2 if sequenced else None
    )
    frames = [old, old, fresh]
    posts: list[tuple[str, JsonValue]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            posts.append((request.headers["idempotency-key"], decode_json(request.content)))
            return httpx.Response(200, json=acknowledge_native_input(request, "fresh-retry"))
        if transport == "stream":
            assert request.url.path.endswith("/events/stream")
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text="".join(f"id: {event['seq']}\ndata: {json.dumps(event)}\n\n" for event in frames),
            )
        assert request.url.path.endswith("/events")
        data: list[JsonValue] = [event for event in frames]
        page: dict[str, JsonValue] = {
            "data": data,
            **({"next_page": None} if transport == "managed-page" else {"has_more": False}),
        }
        return httpx.Response(200, json=page)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        if transport == "stream":
            await service.stream("session")
        else:
            await service.catch_up("session")
        assert not service.tasks
    row = await repo.get("ops.agent_sessions", "session")
    assert row is not None and row["status"] == "OPEN"
    ledger = NativeRetryRefusalLedger.from_record("session", row)
    assert len(ledger.entries) == 1
    receipt = next(iter(ledger.entries.values()))
    assert receipt.frame == NativeFrameIdentity.from_wire(old)
    assert receipt.reason == "RETRY_COUNT_NOT_RETAINED"
    assert receipt.controller == declared_versions_fixture()
    journal = row["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict) and isinstance(journal[1], dict)
    assert "retry_count" not in journal[0] and "terminal_failure" not in journal[0]
    assert journal[1]["retry_count"] == 1 and row["active_message_id"] == "fresh"
    assert len(posts) == 1 and posts[0][0] == "turn-retry:session:fresh"
    assert "Fresh opaque text" in canonical_json(posts[0][1])


class RefusalWriteFailure:
    def __init__(self, store: Repository, mode: Literal["BEFORE", "AFTER"]) -> None:
        self.store = store
        self.mode = mode
        self.failed = False
        self.written = False

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None:
        return await self.store.get(collection, key)

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        if "host_retry_refusals" in record and not self.failed and self.mode == "BEFORE":
            self.failed = True
            raise RuntimeError("Synthetic refusal persistence acknowledgement lost")
        stored = await self.store.put(collection, key, record, immutable=immutable)
        if "host_retry_refusals" in record:
            self.written = True
        return stored

    @asynccontextmanager
    async def lock(self, name: str) -> AsyncIterator[Self]:
        async with self.store.lock(name):
            yield self
        if self.mode == "AFTER" and self.written and not self.failed:
            self.failed = True
            raise RuntimeError("Synthetic refusal persistence acknowledgement lost")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["BEFORE", "AFTER"])
async def test_refusal_persistence_failure_never_advances_memory_and_restart_keeps_chronology(
    mode: Literal["BEFORE", "AFTER"],
) -> None:
    repo = MemoryRepository()
    original = transport_state()
    await repo.put("ops.agent_sessions", "session", original)
    old = failure("old-failure", "old-input", 1)
    fresh = failure("fresh-failure", "fresh-input", 2)
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            posts.append(request.headers["idempotency-key"])
            return httpx.Response(200, json=acknowledge_native_input(request, "fresh-retry"))
        return httpx.Response(200, json={"data": [old, old, fresh], "has_more": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        first = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        first.retry_refusals = NativeRetryRefusalJournal(
            RefusalWriteFailure(repo, mode), declared_versions_fixture(), collection_clock
        )
        with pytest.raises(RuntimeError, match="persistence acknowledgement"):
            await first.catch_up("session")
        assert seen_key(old) not in first.seen_events.get("session", set())
        after_failure = await repo.get("ops.agent_sessions", "session")
        assert after_failure is not None and posts == []
        if mode == "BEFORE":
            assert after_failure == original
        else:
            assert after_failure["last_event_seq"] == 1
            assert len(NativeRetryRefusalLedger.from_record("session", after_failure).entries) == 1
        restarted = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        await restarted.catch_up("session")
        snapshot = await repo.get("ops.agent_sessions", "session")
        await restarted.catch_up("session")
        assert await repo.get("ops.agent_sessions", "session") == snapshot
    assert posts == ["turn-retry:session:fresh"]


def test_native_stream_duplicate_keys_are_refused_before_ownership_projection() -> None:
    with pytest.raises(DuplicateJsonKeyError) as rejected:
        decode_native_stream(
            '{"id":"first-private-value","id":"second-private-value","type":"session.error"}', ""
        )
    assert "first-private-value" not in str(rejected.value)
    assert "second-private-value" not in str(rejected.value)


def test_native_stream_keeps_json_looking_tool_text_opaque() -> None:
    text = '{"id":"first","id":"second"}'
    raw = json.dumps({"id": "message", "type": "agent.message", "content": [{"type": "text", "text": text}]})
    frame = decode_native_stream(raw, "")
    assert frame.event["content"] == [{"type": "text", "text": text}]
    assert frame.sequence is None


@pytest.mark.asyncio
async def test_node_has_more_uses_actual_sequence_for_the_next_request() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", transport_state())
    old = failure("old-failure", "old-input", 17)
    fresh = failure("fresh-failure", "fresh-input", 41)
    cursors: list[str | None] = []
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            posts.append(request.headers["idempotency-key"])
            return httpx.Response(200, json=acknowledge_native_input(request, "fresh-retry"))
        assert "page" not in request.url.params
        cursor = request.url.params.get("after_seq")
        cursors.append(cursor)
        if cursor is None:
            return httpx.Response(200, json={"data": [old], "has_more": True})
        assert cursor == "17"
        return httpx.Response(200, json={"data": [fresh], "has_more": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        await service.catch_up("session")
    assert cursors == [None, "17"] and posts == ["turn-retry:session:fresh"]
    row = await repo.get("ops.agent_sessions", "session")
    assert row is not None and row["last_event_seq"] == 41


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "page",
    [
        {"data": []},
        {"data": [], "has_more": False, "next_page": None},
        {"data": [], "has_more": True},
        {
            "data": [{"id": "old-failure", "type": "session.error", "execution_id": "old-input"}],
            "has_more": True,
        },
        {"data": [], "has_more": 0},
        {"data": [], "next_page": ""},
    ],
)
async def test_invalid_native_paging_refuses_before_any_frame_effect(page: dict[str, JsonValue]) -> None:
    repo = MemoryRepository()
    original = transport_state()
    await repo.put("ops.agent_sessions", "session", original)
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(200, json=page)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        with pytest.raises(ValueError):
            await service.catch_up("session")
    assert methods == ["GET"]
    assert await repo.get("ops.agent_sessions", "session") == original


@pytest.mark.asyncio
async def test_actual_native_http_duplicate_ownership_cannot_commit_acceptance() -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "active_message_id": "message",
        "message_log": [
            {
                "message_id": "message",
                "body": {"type": "user.message", "content": []},
                "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "question"},
                "turn_status": "ACCEPTING",
                "retry_count": 0,
                "remote_event_id": None,
            }
        ],
        "messages": [],
    }
    await repo.put("ops.agent_sessions", "session", original)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.headers["idempotency-key"])
        return httpx.Response(
            200,
            content=b'{"data":[{"type":"user.message","id":"private-first","id":"private-second","content":[]}]}',
            headers={"content-type": "application/json"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        with pytest.raises(DuplicateJsonKeyError) as rejected:
            await service.post_message_intent("session", "message")
    assert "private-first" not in str(rejected.value) and "private-second" not in str(rejected.value)
    assert calls == ["message:message"]
    assert await repo.get("ops.agent_sessions", "session") == original


@pytest.mark.asyncio
async def test_native_stream_duplicate_frame_never_reaches_refusal_or_budget_state() -> None:
    repo = MemoryRepository()
    original = transport_state()
    await repo.put("ops.agent_sessions", "session", original)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET" and request.url.path.endswith("/events/stream")
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text='data: {"id":"one","id":"two","type":"session.error","execution_id":"old-input"}\n\n',
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        with pytest.raises(DuplicateJsonKeyError):
            await service.stream("session")
        assert service.seen_events == {}
    assert await repo.get("ops.agent_sessions", "session") == original


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["BEFORE", "AFTER"])
async def test_actual_postgres_refusal_is_atomic_and_restarts_without_starving_fresh_input(
    postgres_template: PostgresTemplate, mode: Literal["BEFORE", "AFTER"]
) -> None:
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.migrate()
        original = transport_state()
        await repo.put("ops.agent_sessions", "session", original)
        old = failure("old-failure", "old-input", 1)
        fresh = failure("fresh-failure", "fresh-input", 2)
        posts: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                posts.append(request.headers["idempotency-key"])
                return httpx.Response(200, json=acknowledge_native_input(request, "fresh-retry"))
            return httpx.Response(200, json={"data": [old, old, fresh], "has_more": False})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            first = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
            first.retry_refusals = NativeRetryRefusalJournal(
                RefusalWriteFailure(repo, mode), declared_versions_fixture(), collection_clock
            )
            with pytest.raises(RuntimeError, match="persistence acknowledgement"):
                await first.catch_up("session")
            assert seen_key(old) not in first.seen_events.get("session", set()) and posts == []
            after_failure = await repo.get("ops.agent_sessions", "session")
            assert after_failure is not None
            if mode == "BEFORE":
                assert after_failure == original
            else:
                assert after_failure["last_event_seq"] == 1
                assert len(NativeRetryRefusalLedger.from_record("session", after_failure).entries) == 1
            restarted = SessionService(
                repo, settings(), None, http=http, versions=declared_versions_fixture()
            )
            await restarted.catch_up("session")
            saved = await repo.get("ops.agent_sessions", "session")
            await restarted.catch_up("session")
            assert await repo.get("ops.agent_sessions", "session") == saved
        assert posts == ["turn-retry:session:fresh"]


@pytest.mark.asyncio
async def test_refusal_controller_provenance_does_not_relabel_unknown_producer_or_replay_receipt() -> None:
    repo = MemoryRepository()
    original = transport_state()
    original.update(config_version=None, code_version=None)
    await repo.put("ops.agent_sessions", "session", original)
    frame = failure("old-failure", "old-input", 17)
    first_versions = ResolvedVersionIdentity(
        config_version="controller-config-A", code_version="controller-code-A"
    )
    later_versions = ResolvedVersionIdentity(
        config_version="controller-config-B", code_version="controller-code-B"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"data": [frame], "has_more": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        first = SessionService(repo, settings(), None, http=http, versions=first_versions)
        await first.catch_up("session")
        saved = await repo.get("ops.agent_sessions", "session")
        assert saved is not None
        later = SessionService(repo, settings(), None, http=http, versions=later_versions)
        await later.catch_up("session")
    assert await repo.get("ops.agent_sessions", "session") == saved
    ledger = NativeRetryRefusalLedger.from_record("session", saved)
    assert next(iter(ledger.entries.values())).controller == first_versions
    assert saved["config_version"] is None and saved["code_version"] is None
    original_facts = {key: value for key, value in original.items() if key != "last_event_seq"}
    remaining = {
        key: value for key, value in saved.items() if key not in {"last_event_seq", "host_retry_refusals"}
    }
    assert canonical_json(original_facts) == canonical_json(remaining)


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", ["NODE", "MANAGED"])
async def test_repeating_native_cursor_refuses_instead_of_guessing_progress(protocol: str) -> None:
    repo = MemoryRepository()
    original = transport_state()
    await repo.put("ops.agent_sessions", "session", original)
    frame = failure("old-failure", "old-input", 17)
    reads = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal reads
        assert request.method == "GET"
        reads += 1
        if protocol == "NODE":
            return httpx.Response(200, json={"data": [frame], "has_more": True})
        return httpx.Response(200, json={"data": [], "next_page": "same-cursor"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings(), None, http=http, versions=declared_versions_fixture())
        with pytest.raises(ValueError, match="advance|repeated"):
            await service.catch_up("session")
    assert reads == 2
    row = await repo.get("ops.agent_sessions", "session")
    assert row is not None
    if protocol == "NODE":
        assert row["last_event_seq"] == 17
        assert len(NativeRetryRefusalLedger.from_record("session", row).entries) == 1
    else:
        assert row == original

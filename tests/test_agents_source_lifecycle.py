"""Historical lifecycle patches validate owned facts while retaining original capture data."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Self

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.source_items import FailedExtraction, SkippedExtraction
from tank.contracts.source_repository import SourceExtractionRepository
from tank.storage import MemoryRepository
from tank.util import canonical_json


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_historical_transition_preserves_unrelated_serialized_data_and_clears_old_rejection(
    failed: bool,
) -> None:
    store = MemoryRepository()
    original: dict[str, JsonValue] = {
        "item_id": "historical",
        "extraction_status": "PENDING",
        "extraction_decision": "NO_EVENT",
        "extraction_reason": "OLD_REJECTION",
        "raw_ref": "/frozen/retained.bin",
        "raw_sha256": "a" * 64,
        "retained_capture": {"n": 1, "flag": True, "text": "exact original"},
    }
    await store.put("ingest.source_items", "historical", original)
    repository = SourceExtractionRepository(store)
    outcome = (
        FailedExtraction(status="FAILED", reason="ACTUAL_FAILURE")
        if failed
        else SkippedExtraction(status="SKIPPED", decision=None, reason="ACTUAL_NATIVE_COMPLETION")
    )
    updated = await repository.transition("historical", frozenset({"PENDING"}), outcome)
    assert updated is not None
    assert updated.extraction_status == ("FAILED" if failed else "SKIPPED")
    assert updated.extraction_decision is None
    assert updated.extraction_reason == ("ACTUAL_FAILURE" if failed else "ACTUAL_NATIVE_COMPLETION")
    stored = await store.get("ingest.source_items", "historical")
    assert stored is not None and "source_id" not in stored and "author" not in stored
    owned = {"extraction_status", "extraction_decision", "extraction_reason"}
    assert canonical_json(
        {key: value for key, value in stored.items() if key not in owned}
    ) == canonical_json({key: value for key, value in original.items() if key not in owned})


@pytest.mark.asyncio
async def test_terminal_guard_and_missing_record_do_not_manufacture_updates() -> None:
    store = MemoryRepository()
    original: dict[str, JsonValue] = {"item_id": "done", "extraction_status": "DONE", "retained": True}
    await store.put("ingest.source_items", "done", original)
    repository = SourceExtractionRepository(store)
    outcome = FailedExtraction(status="FAILED", reason="LATE_FAILURE")
    state = await repository.transition("done", frozenset({"PENDING"}), outcome)
    assert state is not None and state.extraction_status == "DONE"
    assert await store.get("ingest.source_items", "done") == original
    assert await repository.transition("missing", frozenset({"PENDING"}), outcome) is None
    assert await store.get("ingest.source_items", "missing") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_identity", [False, True])
async def test_historical_transition_requires_owned_identity(missing_identity: bool) -> None:
    store = MemoryRepository()
    original: dict[str, JsonValue] = {"extraction_status": "PENDING"}
    if not missing_identity:
        original["item_id"] = "different"
    await store.put("ingest.source_items", "requested", original)
    repository = SourceExtractionRepository(store)
    with pytest.raises(KeyError if missing_identity else ValueError):
        await repository.transition(
            "requested", frozenset({"PENDING"}), FailedExtraction(status="FAILED", reason="ACTUAL_FAILURE")
        )
    assert await store.get("ingest.source_items", "requested") == original


@pytest.mark.asyncio
async def test_transition_reloads_under_capture_lock_and_preserves_concurrent_metadata() -> None:
    store = MemoryRepository()
    await store.put("ingest.source_items", "item", {"item_id": "item", "extraction_status": "PENDING"})
    repository = SourceExtractionRepository(store)
    async with store.lock("ingest-dedup"):
        task = asyncio.create_task(
            repository.transition(
                "item", frozenset({"PENDING"}), FailedExtraction(status="FAILED", reason="ACTUAL_FAILURE")
            )
        )
        await asyncio.sleep(0)
        await store.put(
            "ingest.source_items",
            "item",
            {"item_id": "item", "extraction_status": "PENDING", "new_capture": "preserved"},
        )
    async with asyncio.timeout(1):
        await task
    stored = await store.get("ingest.source_items", "item")
    assert stored is not None and stored["new_capture"] == "preserved"
    assert stored["extraction_status"] == "FAILED"


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_status", [False, True])
async def test_historical_projection_requires_declared_lifecycle(missing_status: bool) -> None:
    store = MemoryRepository()
    original: dict[str, JsonValue] = {"item_id": "item"}
    if not missing_status:
        original["extraction_status"] = "UNKNOWN"
    await store.put("ingest.source_items", "item", original)
    with pytest.raises(KeyError if missing_status else ValidationError):
        await SourceExtractionRepository(store).transition(
            "item", frozenset({"PENDING"}), FailedExtraction(status="FAILED", reason="ACTUAL_FAILURE")
        )
    assert await store.get("ingest.source_items", "item") == original


class MinimalLifecycleStore:
    def __init__(self, document: dict[str, JsonValue]) -> None:
        self.document = dict(document)

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None:
        assert collection == "ingest.source_items"
        return dict(self.document)

    async def put(self, collection: str, key: str, record: dict[str, JsonValue]) -> dict[str, JsonValue]:
        assert collection == "ingest.source_items"
        self.document = dict(record)
        return dict(record)

    @asynccontextmanager
    async def lock(self, key: str) -> AsyncIterator[Self]:
        assert key == "ingest-dedup"
        yield self

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        yield self


@pytest.mark.asyncio
async def test_authoritative_completion_needs_only_identity_and_minimal_persistence_port() -> None:
    original: dict[str, JsonValue] = {
        "item_id": "historical",
        "extraction_decision": "NO_EVENT",
        "extraction_reason": "OLD",
        "retained": {"n": 1, "flag": True},
        "raw_ref": "/frozen/original",
    }
    store = MinimalLifecycleStore(original)
    state = await SourceExtractionRepository(store).complete("historical")
    assert state is not None and state.extraction_status == "DONE"
    assert state.extraction_decision is None and state.extraction_reason is None
    owned = {"extraction_status", "extraction_decision", "extraction_reason"}
    assert canonical_json(
        {key: value for key, value in store.document.items() if key not in owned}
    ) == canonical_json({key: value for key, value in original.items() if key not in owned})
    assert "source_id" not in store.document and "author" not in store.document


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_identity", [False, True])
async def test_authoritative_completion_refuses_missing_or_mismatched_identity(
    missing_identity: bool,
) -> None:
    original: dict[str, JsonValue] = {"retained": "unchanged"}
    if not missing_identity:
        original["item_id"] = "different"
    store = MinimalLifecycleStore(original)
    with pytest.raises(KeyError if missing_identity else ValueError):
        await SourceExtractionRepository(store).complete("requested")
    assert store.document == original

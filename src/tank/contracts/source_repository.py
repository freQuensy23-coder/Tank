"""Typed native extraction transitions over the exact persisted lifecycle projection."""

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.source_items import ExtractionStatus, SourceExtractionFields, SourceExtractionOutcome


class SourceExtractionIdentity(DomainModel):
    item_id: str = Field(min_length=1)


class SourceExtractionState(SourceExtractionFields, SourceExtractionIdentity):
    pass


class SourceLifecycleStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(self, collection: str, key: str, record: dict[str, JsonValue]) -> dict[str, JsonValue]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...


def _identity(document: Mapping[str, JsonValue], item_id: str) -> SourceExtractionIdentity:
    identity = SourceExtractionIdentity.model_validate({"item_id": document["item_id"]})
    if identity.item_id != item_id:
        raise ValueError("Native source identity does not match its repository key")
    return identity


def _state(document: Mapping[str, JsonValue], item_id: str) -> SourceExtractionState:
    identity = _identity(document, item_id)
    return SourceExtractionState.model_validate(
        {
            "item_id": identity.item_id,
            "extraction_status": document["extraction_status"],
            "extraction_decision": document.get("extraction_decision"),
            "extraction_reason": document.get("extraction_reason"),
        }
    )


class SourceExtractionRepository:
    def __init__(self, store: SourceLifecycleStore) -> None:
        self._store = store

    async def get(self, item_id: str) -> SourceExtractionState | None:
        document = await self._store.get("ingest.source_items", item_id)
        return None if document is None else _state(document, item_id)

    async def transition(
        self,
        item_id: str,
        allowed: frozenset[ExtractionStatus],
        outcome: SourceExtractionOutcome,
    ) -> SourceExtractionState | None:
        async with self._store.lock("ingest-dedup"), self._store.transaction():
            document = await self._store.get("ingest.source_items", item_id)
            if document is None:
                return None
            state = _state(document, item_id)
            if state.extraction_status not in allowed:
                return state
            updated = state.with_extraction(outcome)
            await self._write(document, updated)
            return updated

    async def complete(self, item_id: str) -> SourceExtractionState | None:
        """Record authoritative accepted evidence without inventing a historical prior state."""
        async with self._store.lock("ingest-dedup"), self._store.transaction():
            document = await self._store.get("ingest.source_items", item_id)
            if document is None:
                return None
            identity = _identity(document, item_id)
            completed = SourceExtractionState(
                item_id=identity.item_id,
                extraction_status="DONE",
                extraction_decision=None,
                extraction_reason=None,
            )
            await self._write(document, completed)
            return completed

    async def _write(self, document: dict[str, JsonValue], state: SourceExtractionState) -> None:
        patch = decode_json(state.model_dump_json())
        if not isinstance(patch, dict):
            raise TypeError("Source lifecycle must serialize as an object")
        await self._store.put("ingest.source_items", state.item_id, {**document, **patch})

"""Typed domain persistence adapter over the inward document-store contract."""

import builtins
from collections.abc import AsyncIterator
from datetime import datetime

from pydantic import BaseModel, JsonValue

from tank.contracts.repository import Collection, DocumentStore


class Repository[T: BaseModel]:
    """Bind a domain schema once so callers cannot read unvalidated records."""

    def __init__(self, store: DocumentStore, collection: Collection[T]) -> None:
        self._store = store
        self._collection = collection

    async def get(self, key: str) -> T | None:
        document = await self._store.get(self._collection.name, key)
        if document is None:
            return None
        return self._collection.decode(document)

    async def put(self, key: str, entity: T, *, immutable: bool = False) -> T:
        document = await self._store.put(
            self._collection.name, key, self._collection.encode(entity), immutable=immutable
        )
        return self._collection.decode(document)

    async def list(
        self, *, filters: dict[str, JsonValue] | None = None, limit: int = 1000
    ) -> builtins.list[T]:
        documents = await self._store.list(self._collection.name, filters=filters, limit=limit)
        return [self._collection.decode(document) for document in documents]

    async def iter_all(
        self, *, filters: dict[str, JsonValue] | None = None, batch_size: int = 1000
    ) -> AsyncIterator[T]:
        async for document in self._store.iter_all(
            self._collection.name, filters=filters, batch_size=batch_size
        ):
            yield self._collection.decode(document)

    async def latest_before(
        self,
        time_field: str,
        cutoff: datetime,
        *,
        filters: dict[str, JsonValue] | None = None,
    ) -> T | None:
        document = await self._store.latest_before(self._collection.name, time_field, cutoff, filters=filters)
        return None if document is None else self._collection.decode(document)

    async def list_range(
        self,
        time_field: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        filters: dict[str, JsonValue] | None = None,
        limit: int = 1000,
        after: tuple[datetime, str] | None = None,
    ) -> builtins.list[T]:
        documents = await self._store.list_range(
            self._collection.name,
            time_field,
            start=start,
            end=end,
            filters=filters,
            limit=limit,
            after=after,
        )
        return [self._collection.decode(document) for document in documents]

    async def delete(self, key: str) -> None:
        await self._store.delete(self._collection.name, key)

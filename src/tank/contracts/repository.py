"""Typed repository ports and explicit codecs at the persistence boundary."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel, JsonValue

from tank.contracts.json import encode_json


@dataclass(frozen=True)
class Collection[T: BaseModel]:
    name: str
    schema: type[T]

    def decode(self, payload: Mapping[str, JsonValue]) -> T:
        return self.schema.model_validate_json(encode_json(dict(payload)), strict=True)

    def encode(self, entity: T) -> dict[str, JsonValue]:
        return entity.model_dump(mode="json")


class DocumentStore(Protocol):
    """Storage documents are JSON values; domain services use Repository instead."""

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

    async def put(
        self,
        collection: str,
        key: str,
        record: dict[str, JsonValue],
        *,
        immutable: bool = False,
    ) -> dict[str, JsonValue]: ...

    async def delete(self, collection: str, key: str) -> None: ...


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

    async def delete(self, key: str) -> None:
        await self._store.delete(self._collection.name, key)

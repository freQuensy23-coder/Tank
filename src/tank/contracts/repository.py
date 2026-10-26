"""Typed repository ports and explicit codecs at the persistence boundary."""

import builtins
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from pydantic import BaseModel, JsonValue

from tank.contracts.json import encode_json


@dataclass(frozen=True)
class Collection[T: BaseModel]:
    name: str
    schema: type[T]

    def decode(self, payload: Mapping[str, JsonValue]) -> T:
        return self.schema.model_validate_json(encode_json(dict(payload)), strict=True)

    def encode(self, entity: T, *, exclude_unset: bool = False) -> dict[str, JsonValue]:
        if not isinstance(entity, self.schema):
            raise TypeError(f"{self.name} requires {self.schema.__name__}")
        validated = self.schema.model_validate(entity, strict=True)
        payload = validated.model_dump(mode="json", warnings="error", exclude_unset=exclude_unset)
        self.decode(payload)
        return payload


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

    async def list(
        self,
        collection: str,
        *,
        filters: dict[str, JsonValue] | None = None,
        limit: int = 1000,
    ) -> builtins.list[dict[str, JsonValue]]: ...

    def iter_all(
        self,
        collection: str,
        *,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...

    async def latest_before(
        self,
        collection: str,
        time_field: str,
        cutoff: datetime,
        *,
        filters: dict[str, JsonValue] | None = None,
    ) -> dict[str, JsonValue] | None: ...

    async def list_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        filters: dict[str, JsonValue] | None = None,
        limit: int = 1000,
        after: tuple[datetime, str] | None = None,
    ) -> builtins.list[dict[str, JsonValue]]: ...

    async def delete(self, collection: str, key: str) -> None: ...

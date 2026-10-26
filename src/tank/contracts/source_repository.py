"""Inward source-extraction identities, state and persistence capability contract."""

from contextlib import AbstractAsyncContextManager
from typing import Protocol, Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.source_items import SourceExtractionFields


class SourceExtractionIdentity(DomainModel):
    item_id: str = Field(min_length=1)


class SourceExtractionState(SourceExtractionFields, SourceExtractionIdentity):
    pass


class SourceLifecycleStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(self, collection: str, key: str, record: dict[str, JsonValue]) -> dict[str, JsonValue]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...

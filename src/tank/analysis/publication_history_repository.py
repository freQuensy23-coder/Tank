"""Read exact publication identities and immutable rows without timestamp-order guesses."""

from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from pydantic import JsonValue

from tank.analysis.publication_history import (
    PublicationHistory,
    PublicationRevisionRef,
    publication_history_from_records,
    publication_revision_from_record,
)


class PublicationHistoryReader(Protocol):
    def iter_metadata(
        self,
        collection: str,
        fields: Sequence[str],
        *,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...


class PublicationHistoryRepository:
    def __init__(self, reader: PublicationHistoryReader) -> None:
        self.reader = reader

    async def history(self, event_id: str) -> PublicationHistory:
        metadata = [
            row
            async for row in self.reader.iter_metadata(
                "out.alerts",
                ("event_id", "alert_id", "version", "kind"),
                filters={"event_id": event_id},
            )
        ]
        return publication_history_from_records(event_id, metadata)

    async def read_record(self, reference: PublicationRevisionRef) -> dict[str, JsonValue]:
        selected = PublicationRevisionRef.model_validate(reference)
        record = await self.reader.get("out.alerts", selected.storage_key())
        if record is None:
            raise ValueError("Selected publication revision is missing its actual immutable row")
        if publication_revision_from_record(record) != selected:
            raise ValueError("Selected publication row disagrees with its scoped revision identity")
        return record

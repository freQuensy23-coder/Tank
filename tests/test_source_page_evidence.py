"""Web evidence preserves truthful typed page provenance without rewriting historical rows."""

from pathlib import Path
from typing import Protocol

import pytest
from pydantic import JsonValue, ValidationError
from pydantic_settings import SettingsConfigDict
from scheduler_support import operational_reaction_services
from source_support import fixture_versions

from tank.contracts.page_evidence import LivePageProvenance, RecordedBodyProvenance
from tank.contracts.source_items import SourceItemDocument, WebEvidenceItem, parse_source_item
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock


class WebCaptureWireWriter(Protocol):
    async def save_web(
        self,
        url: str,
        text: str,
        title: str,
        context: dict[str, JsonValue],
        *,
        raw_bytes: bytes,
        media_type: str,
    ) -> dict[str, JsonValue]: ...


async def historical_web_record(tmp_path: Path) -> dict[str, JsonValue]:
    class FixtureSettings(Settings):
        model_config = SettingsConfigDict(env_file=None)

    repo, settings = MemoryRepository(), FixtureSettings(data_dir=tmp_path, ops_progress_enabled=False)
    versions = fixture_versions()
    async with operational_reaction_services(repo, settings, versions=versions) as services:
        tools = EventToolService(
            repo,
            settings,
            services.truth,
            None,
            versions=versions,
            scheduler=services.scheduler,
        )
        writer: WebCaptureWireWriter = tools
        raw_record = await writer.save_web(
            "https://example.invalid/source",
            "Captured public document",
            "Document",
            {},
            raw_bytes=b"<article>Captured public document</article>",
            media_type="text/html",
        )
        return WebEvidenceItem.model_validate(raw_record).storage_record()


@pytest.mark.asyncio
async def test_historical_web_row_keeps_absent_page_provenance_absent(tmp_path: Path) -> None:
    record = await historical_web_record(tmp_path)
    assert "page_read_provenance" not in record
    item = parse_source_item(record)
    assert isinstance(item, WebEvidenceItem) and item.page_read_provenance is None
    assert item.storage_record() == record
    assert SourceItemDocument(item).model_dump(mode="json") == record


@pytest.mark.asyncio
async def test_live_page_provenance_roundtrips_its_actual_receipt(tmp_path: Path) -> None:
    record = await historical_web_record(tmp_path)
    stamp = collection_clock("2026-10-25T02:00:00Z")
    provenance = LivePageProvenance(mode="LIVE_RESPONSE", received_at=stamp)
    item = WebEvidenceItem.model_validate(
        {
            **record,
            "page_read_provenance": provenance.model_dump(mode="json"),
        }
    )
    assert isinstance(item.page_read_provenance, LivePageProvenance)
    assert item.page_read_provenance.received_at == stamp
    assert parse_source_item(item.storage_record()) == item


@pytest.mark.asyncio
async def test_recorded_page_provenance_must_match_the_actual_stored_raw_digest(tmp_path: Path) -> None:
    record = await historical_web_record(tmp_path)
    digest = record["raw_sha256"]
    assert isinstance(digest, str)
    provenance = RecordedBodyProvenance(
        mode="VERIFIED_RECORDED_BODY",
        input_sha256="a" * 64,
        record_id="captured-body-1",
        captured_body_sha256=digest,
    )
    item = WebEvidenceItem.model_validate(
        {
            **record,
            "page_read_provenance": provenance.model_dump(mode="json"),
        }
    )
    assert isinstance(item.page_read_provenance, RecordedBodyProvenance)
    assert item.raw_sha256 == digest
    assert parse_source_item(item.storage_record()) == item
    wrong = provenance.model_copy(update={"captured_body_sha256": "b" * 64})
    with pytest.raises(ValidationError, match="stored raw digest"):
        WebEvidenceItem.model_validate(
            {
                **record,
                "page_read_provenance": wrong.model_dump(mode="json"),
            }
        )

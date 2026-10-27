import json
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_sources import collector_http as collector_http
from test_storage import repo as repo

from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json
from tank.contracts.source_items import BinanceCatalogListing, PendingBinanceCatalog, pending_source_item
from tank.contracts.sources import SourceDefinition
from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.storage import StorageBackend
from tank.util import sha256


class HistoricalCatalogWitness(DomainModel):
    producer_commit: Literal["708d113442f29395655d9abf6ac0a29026f1f44c"]
    source: SourceDefinition
    checkpoint: dict[str, JsonValue]
    calls: list[str]


type Summary = Literal["ABSENT", "NULL", "EMPTY", "REPORTED"]


class CatalogSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def catalog(summary: Summary) -> bytes:
    article: dict[str, JsonValue] = {
        "code": "actual-code",
        "title": "Actual release",
        "releaseDate": 1791948608368,
    }
    if summary != "ABSENT":
        article["body"] = {"NULL": None, "EMPTY": "", "REPORTED": "<p>Actual catalog summary.</p>"}[summary]
    return json.dumps(
        {"success": True, "code": "000000", "data": {"catalogs": [{"total": 1, "articles": [article]}]}}
    ).encode()


@pytest.mark.parametrize("summary", ["ABSENT", "NULL", "EMPTY", "REPORTED"])
def test_catalog_admission_preserves_reported_summary_without_inventing_detail(summary: Summary) -> None:
    [envelope] = parsers.json_items(catalog(summary), "S09").discoveries
    listing = envelope.discovery
    assert isinstance(listing, BinanceCatalogListing)
    assert (
        listing.summary
        == {"ABSENT": None, "NULL": None, "EMPTY": "", "REPORTED": "Actual catalog summary."}[summary]
    )
    assert ("summary" in listing.model_fields_set) == (summary != "ABSENT")
    assert "text" not in listing.model_dump(mode="json")
    assert listing.publication.value == "2026-10-14T03:30:08.368000Z"


@pytest.mark.parametrize(
    "summary,historical",
    [("ABSENT", False), ("NULL", False), ("EMPTY", False), ("REPORTED", False), ("ABSENT", True)],
)
async def test_catalog_pending_restart_requires_detail_and_keeps_exact_listing_facts(
    repo: StorageBackend,
    tmp_path: Path,
    summary: Summary,
    historical: bool,
    collector_http: Callable[[httpx.AsyncClient], None],
) -> None:
    listing_payload = catalog(summary)
    detail_payload = json.dumps(
        {
            "data": {
                "publishDate": 1791948608368,
                "body": json.dumps(
                    {"node": "root", "child": [{"node": "text", "text": "Actual complete detail."}]}
                ),
            }
        }
    ).encode()
    fail = True
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.path == "/list":
            return httpx.Response(200, content=listing_payload)
        assert request.url.params["articleCode"] == "actual-code"
        return httpx.Response(404 if fail else 200, content=b"Unavailable" if fail else detail_payload)

    source = source_definition(
        {
            "source_id": "S09",
            "feeds": [{"kind": "json", "url": "https://www.binance.com/list?pageNo=1&pageSize=20"}],
        }
    )
    settings = CatalogSettings(data_dir=tmp_path, ops_progress_enabled=False)
    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            collector_http(http)
            first = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            first.sources = {"S09": source}
            try:
                if historical:
                    witness = HistoricalCatalogWitness.model_validate(
                        decode_unique_json(
                            (
                                Path(__file__).parent / "fixtures/source-binance-catalog/producer-708d.json"
                            ).read_bytes()
                        )
                    )
                    assert witness.source == source and witness.calls == [source.feeds[0].url]
                    await repo.put("ops.source_checkpoints", "S09:0", witness.checkpoint)
                else:
                    await first.poller.acquire(source, 0)
                checkpoint = await repo.get("ops.source_checkpoints", "S09:0")
                assert checkpoint is not None
                batches = checkpoint["pending_batches"]
                assert isinstance(batches, list) and len(batches) == 2
                batch, coverage_batch = batches
                assert isinstance(coverage_batch, dict) and coverage_batch["rows"] == []
                assert isinstance(batch, dict)
                rows = batch["rows"]
                assert isinstance(rows, list) and len(rows) == 1
                row = rows[0]
                assert isinstance(row, dict)
                if historical:
                    assert "pending_phase" not in row and row["text"] == ""
                else:
                    assert row["pending_phase"] == "BINANCE_CATALOG"
                    assert "text" not in row and "raw_ref" not in row
                    assert ("summary" in row) == (summary != "ABSENT")
                admitted = pending_source_item("S09", row)
                assert isinstance(admitted, PendingBinanceCatalog) == (not historical)
                assert admitted.storage_record() == row
                failed = await first.poller.drain(source, 0, force=True)
                assert failed.items == 0 and failed.errors
            finally:
                await first.close()
            after_failure = await repo.get("ops.source_checkpoints", "S09:0")
            assert after_failure is not None and after_failure["pending_batches"] == batches
            assert "discovery_checkpoint" not in after_failure
            assert await repo.list("ingest.source_items") == []
            assert list(tmp_path.rglob("*.zst")) == []
            fail = False
            restarted = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            restarted.sources = {"S09": source}
            try:
                complete = await restarted.poller.drain(source, 0, force=True)
                assert complete.items == 1 and complete.errors == []
                assert (await restarted.poller.drain(source, 0, force=True)).items == 0
            finally:
                await restarted.close()
        [item] = await repo.list("ingest.source_items")
        assert item["text"] == "Actual complete detail."
        assert item["published_at"] == "2026-10-14T03:30:08.368000Z"
        assert item["raw_sha256"] == sha256(detail_payload)
        reference = item["raw_ref"]
        assert isinstance(reference, str)
        assert zstandard.ZstdDecompressor().decompress(Path(reference).read_bytes()) == detail_payload
        assert "summary" not in item and "publication" not in item and "pending_phase" not in item
        final = await repo.get("ops.source_checkpoints", "S09:0")
        assert final is not None and final["pending_batches"] == []
        assert final["discovery_checkpoint"] == "actual-code"
        assert len(calls) == (2 if historical else 3)
    finally:
        await budgets.shutdown()

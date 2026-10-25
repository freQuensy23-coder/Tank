"""Capture and source coverage contracts exercise real validation and persistence decisions."""

import base64
from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue, ValidationError
from source_support import fixture_versions

from tank.sources.capture_fetch import (
    CaptureRepository,
    CaptureState,
    FetchRequest,
    FetchResult,
    capture_fetch,
)
from tank.sources.read_report import FeedReads, ReadCounter, ReadSnapshot, report
from tank.storage import MemoryRepository
from tank.util import sha256


def result(payload: bytes) -> FetchResult:
    return FetchResult(
        status_code=200,
        content_type="application/octet-stream",
        sha256=sha256(payload),
        fetched_at="2026-10-24T00:00:00Z",
        bytes=len(payload),
        source_id="S14",
        body_b64=base64.b64encode(payload).decode(),
    )


def test_capture_rejects_missing_and_corrupt_body() -> None:
    wire = result(b"evidence").model_dump(mode="json")
    del wire["status_code"]
    with pytest.raises(ValidationError):
        FetchResult.model_validate(wire)
    wire = result(b"evidence").model_dump(mode="json")
    wire["body_b64"] = base64.b64encode(b"changed!").decode()
    with pytest.raises(ValidationError, match="digest"):
        FetchResult.model_validate(wire)


def test_capture_binary_and_request_policy() -> None:
    assert result(b"\xff\x00").content() == b"\xff\x00"
    with pytest.raises(ValidationError):
        FetchRequest(method="GET", url="http://api.binance.com/api/v3/time", source_id="S14")
    with pytest.raises(ValidationError):
        FetchRequest.model_validate({"url": "https://api.binance.com/api/v3/time", "source_id": "S14"})


@pytest.mark.asyncio
async def test_capture_preserves_invalid_terminal_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    module = importlib.import_module("tank.sources.capture_fetch")
    repo = MemoryRepository()
    request_id = "CF-corrupt"
    wire = result(b"retained").model_dump(mode="json")
    wire["bytes"] = 999
    await repo.put(
        "mkt.capture_requests", request_id, {"request_id": request_id, "status": "COMPLETE", "result": wire}
    )

    async def completed(
        repo: CaptureRepository, request: FetchRequest, budget_s: float
    ) -> tuple[str, CaptureState]:
        return request_id, CaptureState(status="COMPLETE", reason=None, result=wire)

    monkeypatch.setattr(module, "_capture_request", completed)
    with pytest.raises(ValidationError):
        await capture_fetch(repo, "GET", "https://api.binance.com/api/v3/time")
    assert await repo.get("mkt.capture_requests", request_id) is not None


@pytest.mark.asyncio
async def test_capture_verified_response_releases_queue_record(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    module = importlib.import_module("tank.sources.capture_fetch")
    repo = MemoryRepository()
    request_id = "CF-valid"
    wire = result(b"\xff\x00").model_dump(mode="json")
    await repo.put(
        "mkt.capture_requests", request_id, {"request_id": request_id, "status": "COMPLETE", "result": wire}
    )

    async def completed(
        repo: CaptureRepository, request: FetchRequest, budget_s: float
    ) -> tuple[str, CaptureState]:
        return request_id, CaptureState(status="COMPLETE", reason=None, result=wire)

    monkeypatch.setattr(module, "_capture_request", completed)
    response = await capture_fetch(repo, "GET", "https://api.binance.com/api/v3/time")
    assert response.status_code == httpx.codes.OK
    assert response.content == b"\xff\x00"
    assert await repo.get("mkt.capture_requests", request_id) is None


def test_read_window_rejects_counter_resets_and_aggregates_details() -> None:
    def window(ok: int, failed: int) -> ReadSnapshot:
        return ReadSnapshot(
            taken_at="2026-10-24T00:00:00Z",
            feeds={
                "S01:0": FeedReads(
                    reads=ReadCounter(ok=ok, failed=failed),
                    detail_reads=ReadCounter(ok=2, failed=1),
                    status="DEGRADED",
                    error="publisher unavailable",
                )
            },
        )

    observed = report(window(20, 3), window(10, 1), ["S01", "S02"])
    assert observed.sources["S01"].ok == 10
    assert observed.sources["S01"].failed == 2
    assert observed.sources["S01"].degraded_feeds[0].error == "publisher unavailable"
    assert observed.sources["S02"].failed_rate is None
    assert observed.all_pass is False
    with pytest.raises(ValueError, match="counter reset"):
        report(window(9, 1), window(10, 1))


def test_source_configuration_requires_adapter_inputs_and_unique_identities() -> None:
    from tank.contracts.sources import SourceConfiguration, SourceFeed
    from tank.settings import Settings

    configuration = SourceConfiguration.model_validate(Settings().load_config("sources"))
    with pytest.raises(ValidationError, match="duplicate source IDs"):
        SourceConfiguration(
            version=configuration.version,
            contact_email=configuration.contact_email,
            sources=[configuration.sources[0], configuration.sources[0]],
        )
    with pytest.raises(ValidationError, match="article pattern"):
        SourceFeed(url="https://example.org/news", kind="html", interval_s=60)
    with pytest.raises(ValidationError, match="venue and market_type"):
        SourceFeed(url="https://example.org/instruments", kind="instruments", interval_s=60)
    with pytest.raises(ValidationError, match="POST and its request body"):
        SourceFeed(url="https://example.org/graphql", kind="graphql", interval_s=60)
    with pytest.raises(ValidationError):
        SourceFeed.model_validate({"url": "https://example.org/feed", "kind": "rss"})


def test_entry_error_variants_preserve_run_identity_and_validate_evidence() -> None:
    from tank.ops_entry_errors import error_key
    from tank.sources.errors import AccountEntryError, parse_entry_error

    run_error = parse_entry_error({"reason": "RUN_DEGRADED", "account": None})
    assert isinstance(run_error, AccountEntryError)
    assert run_error.identity().account == ""
    repeated = parse_entry_error({"reason": "RUN_DEGRADED", "account": None, "error": "retry 2"})
    assert error_key("S29", "https://api.apify.com/run", run_error) == error_key(
        "S29", "https://api.apify.com/run", repeated
    )
    account_error = parse_entry_error({"reason": "RUN_DEGRADED", "account": "alice"})
    assert error_key("S29", "https://api.apify.com/run", run_error) != error_key(
        "S29", "https://api.apify.com/run", account_error
    )
    with pytest.raises(ValidationError):
        parse_entry_error({"account": "alice"})
    with pytest.raises(ValidationError):
        parse_entry_error({"reason": "INVALID_PUBLISHER_ENTRY", "entry_index": -1})


def test_pending_source_requires_actual_durable_capture_and_preserves_unknown_publication(
    tmp_path: Path,
) -> None:
    import zstandard

    from tank.contracts.source_items import PendingSourceItem, RawSourceReference, discovered_item
    from tank.sources.raw_store import write_raw_item

    payload = b"frozen source evidence"
    observed = "2026-10-24T00:00:00Z"
    discovery = discovered_item(
        "S30",
        {
            "external_id": "release-1",
            "title": "Publisher release",
            "text": "Source facts",
            "url": "https://example.org/release",
            "published_at": None,
            "published_prec": "COLLECTION_ONLY",
            "published_prec_reason": "INCOMPLETE",
            "_payload": payload,
        },
    )
    pending = PendingSourceItem(
        discovery=discovery.discovery,
        item_id="ITM-fixture",
        feed="https://example.org/rss",
        observed_at=observed,
        discovery_config_version="cfg",
        discovery_code_version="code",
        raw=None,
    )
    with pytest.raises(ValueError, match="no captured raw evidence"):
        pending.captured()
    reference, digest = write_raw_item(tmp_path, "S30", pending.item_id, observed, payload)
    captured = pending.model_copy(
        update={"raw": RawSourceReference(raw_ref=reference, raw_sha256=digest)}
    ).captured()
    record = captured.storage_record()
    assert record["published_at"] is None and record["published_prec"] == "COLLECTION_ONLY"
    assert record["observed_at_prec"] == "COLLECTION_ONLY"
    assert "_payload" not in record
    assert zstandard.ZstdDecompressor().decompress(Path(reference).read_bytes()) == payload


def test_invalid_normalized_publisher_metadata_preserves_valid_sibling_and_identity() -> None:
    from tank.sources import parsers

    common = {
        "title": "Official release",
        "abstract": "Material facts",
        "html_url": "https://www.federalregister.gov/document",
        "publication_date": "2026-10-24",
    }
    rows = parsers.json_items(
        {
            "results": [
                {**common, "document_number": "bad", "agencies": [{"name": False}]},
                {**common, "document_number": "valid", "agencies": [{"name": "Treasury Department"}]},
            ]
        },
        "S03",
    )
    assert [row["external_id"] for row in rows] == ["valid"]
    assert len(rows.errors) == 1 and rows.errors[0]["entry_index"] == 0
    assert rows.errors[0]["entry"]["id"] == "bad"
    assert len(rows.errors[0]["entry_sha256"]) == 64


def test_source_discovery_cannot_replace_configured_identity_or_invent_missing_publication() -> None:
    from tank.contracts.source_items import discovered_item

    wire = {"external_id": "entry", "title": "Title", "text": "Facts", "url": "https://example.org/entry"}
    with pytest.raises(ValidationError):
        discovered_item("S30", wire)
    with pytest.raises(ValueError, match="conflicts"):
        discovered_item("S30", {**wire, "published_at": None, "source_id": "S29"})


@pytest.mark.asyncio
async def test_real_source_record_roundtrips_and_validates_processing_transitions(tmp_path: Path) -> None:
    from source_support import source_configuration
    from test_sources import RSS

    from tank.contracts.source_items import (
        ClassifiedRelevance,
        ClassifiedSource,
        CompletedExtraction,
        PendingExtraction,
        RoutedSource,
        SkippedExtraction,
        SourceRoute,
        SourceRoutingMessage,
        parse_feed_source_item,
    )
    from tank.settings import Settings
    from tank.sources import SourceService

    class FixtureSettings(Settings):
        def load_config(self, name: str) -> dict[str, JsonValue]:
            if name == "sources":
                return source_configuration(
                    {
                        "sources": [
                            {
                                "source_id": "S30",
                                "feeds": [{"kind": "rss", "url": "https://example.org/rss"}],
                            },
                        ]
                    }
                )
            return super().load_config(name)

    repo = MemoryRepository()
    configuration = FixtureSettings(data_dir=tmp_path, ops_progress_enabled=False)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=RSS))
    ) as http:
        source = SourceService(repo, configuration, http=http, versions=fixture_versions())
        assert (await source.poll_once("S30"))["items"] == 1
    [record] = await repo.list("ingest.source_items")
    item = parse_feed_source_item(record)
    assert item.storage_record() == record
    hinted = parse_feed_source_item(
        {
            **record,
            "tokens_mentioned": ["test:T"],
            "entities": [{"entity_id": "test:entity"}],
        }
    )
    assert hinted.identity_hints is not None
    assert hinted.storage_record()["tokens_mentioned"] == ["test:T"]
    from tank.contracts.repository import Repository as TypedRepository
    from tank.contracts.source_items import SOURCE_ITEMS, SourceItemDocument

    assert SourceItemDocument.model_validate(record).model_dump(mode="json") == record
    classified = item.classified(
        ClassifiedRelevance(
            decision="SEND",
            p_relevant=0.8,
            cutoff=0.5,
            model="synthetic-model",
            provider="synthetic-provider",
            requested_model="synthetic-request",
            semantic_model_id="synthetic-semantic",
            model_pin_evidence="Explicit synthetic test result",
            usage_id="synthetic-usage",
        )
    )
    assert isinstance(classified.processing, ClassifiedSource)
    route = SourceRoute(decision="ADDED", event_id="event-1", p_same=0.9)
    message = SourceRoutingMessage(kind="ITEM_ADDED", item_id=item.item_id, event_id="event-1")
    routed = classified.routed(route, message, {"event-1": 0.9})
    assert isinstance(routed.processing, RoutedSource)
    assert parse_feed_source_item(routed.storage_record()).storage_record() == routed.storage_record()
    assert routed.raw_ref == item.raw_ref and routed.observed_at == item.observed_at
    records = TypedRepository(repo, SOURCE_ITEMS)
    await records.put(item.item_id, SourceItemDocument(routed))
    reloaded = await records.get(item.item_id)
    assert reloaded is not None
    assert reloaded.model_dump(mode="json") == routed.storage_record()
    rejected = routed.with_extraction(
        SkippedExtraction(
            status="SKIPPED",
            decision="NO_EVENT",
            reason="The previous attempt found no event",
        )
    )
    await records.put(item.item_id, SourceItemDocument(rejected))
    prior = await records.get(item.item_id)
    assert prior is not None and prior.root.extraction_decision == "NO_EVENT"
    retried = rejected.with_extraction(PendingExtraction(status="PENDING"))
    assert retried.extraction_decision is None and retried.extraction_reason is None
    completed = rejected.with_extraction(CompletedExtraction(status="DONE"))
    await records.put(item.item_id, SourceItemDocument(completed))
    successful = await records.get(item.item_id)
    assert successful is not None
    assert successful.root.extraction_decision is None and successful.root.extraction_reason is None
    assert successful.root.raw_ref == item.raw_ref
    assert isinstance(completed.processing, RoutedSource)
    assert completed.processing.route_event_id == "event-1"
    with pytest.raises(ValidationError, match="item identity conflicts"):
        classified.routed(route, message.model_copy(update={"item_id": "different-item"}), {"event-1": 0.9})


@pytest.mark.asyncio
async def test_real_web_evidence_record_roundtrips_without_feed_source_assumptions(tmp_path: Path) -> None:
    from tank.contracts.source_items import CompletedExtraction, WebEvidenceItem, parse_source_item
    from tank.event_tools import EventToolService
    from tank.settings import Settings

    repo = MemoryRepository()
    service = EventToolService(repo, Settings(data_dir=tmp_path, ops_progress_enabled=False), None, None)
    record = await service.save_web(
        "https://example.org/evidence",
        "Verified public document text",
        "Public document",
        {},
        raw_bytes=b"<article>Verified public document text</article>",
        media_type="text/html",
    )
    item = parse_source_item(record)
    assert isinstance(item, WebEvidenceItem)
    assert item.storage_record() == record
    from tank.contracts.source_items import SourceItemDocument

    assert SourceItemDocument(item).model_dump(mode="json") == record
    assert item.raw_size_bytes == len(b"<article>Verified public document text</article>")
    assert item.with_extraction(CompletedExtraction(status="DONE")).raw_sha256 == item.raw_sha256


@pytest.mark.parametrize("identity", ["", " ", "event id"])
def test_route_and_message_context_cannot_use_empty_or_whitespace_identities(identity: str) -> None:
    from tank.contracts.source_items import SourceRoute, SourceRoutingMessage

    with pytest.raises(ValidationError):
        SourceRoute(decision="ADDED", event_id=identity, p_same=0.9)
    with pytest.raises(ValidationError):
        SourceRoutingMessage(kind="ITEM_ADDED", item_id="item-1", event_id=identity)
    with pytest.raises(ValidationError):
        SourceRoutingMessage(kind="NEW_ITEM", item_id="item-1", session_id=identity)


def test_publisher_authority_requires_declared_capture_feed() -> None:
    from source_support import source_definition

    from tank.sources.organization import resolve_author_organization

    source = source_definition(
        {
            "source_id": "S01",
            "name": "Declared publisher",
            "author_organization": "Configured issuer",
            "feeds": [],
        }
    )
    with pytest.raises(ValidationError, match="feed"):
        resolve_author_organization(source, {"url": "https://example.org/release"}, {})


@pytest.mark.asyncio
async def test_missing_provider_limits_prevents_source_service_start(tmp_path: Path) -> None:
    from tank.settings import Settings
    from tank.sources import SourceService

    class MissingProviderLimits(Settings):
        def load_config(self, name: str) -> dict[str, JsonValue]:
            configuration = super().load_config(name)
            if name == "api_limits":
                del configuration["limits"]
            return configuration

    repo = MemoryRepository()
    settings = MissingProviderLimits(data_dir=tmp_path, ops_progress_enabled=False)
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValidationError, match="limits"):
            SourceService(repo, settings, http=http, versions=fixture_versions())
    assert await repo.list("ops.jobs") == []
    assert await repo.list("ingest.source_items") == []


@pytest.mark.asyncio
async def test_feed_duplicate_of_web_evidence_counts_feeds_without_replacing_origin(tmp_path: Path) -> None:
    from test_sources import captured

    from tank.contracts.source_items import WebEvidenceItem, parse_source_item
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.sources import SourceService

    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, ops_progress_enabled=False)
    tools = EventToolService(repo, settings, None, None)
    original = await tools.save_web(
        "https://example.org/original",
        "Shared independently observed facts",
        "",
        {},
        raw_bytes=b"<article>Shared independently observed facts</article>",
        media_type="text/html",
    )
    original_raw = Path(original["raw_ref"])
    original_bytes = original_raw.read_bytes()
    async with httpx.AsyncClient() as http:
        service = SourceService(repo, settings, http=http, versions=fixture_versions())
        for external_id in ("feed-first", "feed-repeat"):
            incoming = captured(
                service,
                "S30",
                {
                    "external_id": external_id,
                    "title": "",
                    "text": "Shared independently observed facts",
                    "feed": "https://example.org/independent-feed",
                },
            )
            candidate_raw = Path(incoming["raw_ref"])
            result = await service.ingest(service.sources["S30"], incoming)
            assert result["item_id"] == original["item_id"]
            assert not candidate_raw.exists()
    stored = await repo.get("ingest.source_items", original["item_id"])
    assert stored is not None
    web = parse_source_item(stored)
    assert isinstance(web, WebEvidenceItem)
    assert web.observation_count == 2
    assert web.observed_feeds == ["web:page", "https://example.org/independent-feed"]
    assert web.feed == "web:page" and web.raw_ref == str(original_raw)
    assert web.raw_sha256 == original["raw_sha256"]
    assert original_raw.read_bytes() == original_bytes
    assert await repo.list("ops.jobs") == []
    assert len(await repo.list("ingest.source_items")) == 1


@pytest.mark.asyncio
async def test_duplicate_ingestion_cannot_delete_foreign_raw_evidence(tmp_path: Path) -> None:
    from test_sources import captured

    from tank.settings import Settings
    from tank.sources import SourceService

    repo = MemoryRepository()
    async with httpx.AsyncClient() as http:
        service = SourceService(
            repo,
            Settings(data_dir=tmp_path, ops_progress_enabled=False),
            http=http,
            versions=fixture_versions(),
        )
        first = captured(service, "S30", {"external_id": "first", "title": "Title", "text": "Same facts"})
        await service.ingest(service.sources["S30"], first)
        foreign = tmp_path / "unrelated-evidence"
        foreign.write_bytes(b"Preserve this independently owned evidence")
        candidate = captured(
            service,
            "S30",
            {
                "external_id": "second",
                "title": "Title",
                "text": "Same facts",
            },
        )
        candidate["raw_ref"] = str(foreign)
        with pytest.raises(ValueError, match="does not belong"):
            await service.ingest(service.sources["S30"], candidate)
        assert foreign.read_bytes() == b"Preserve this independently owned evidence"
        assert len(await repo.list("ingest.source_items")) == 1


@pytest.mark.asyncio
async def test_ingestion_cannot_invent_a_missing_capture_clock(tmp_path: Path) -> None:
    from test_sources import captured

    from tank.settings import Settings
    from tank.sources import SourceService

    repo = MemoryRepository()
    async with httpx.AsyncClient() as http:
        service = SourceService(
            repo,
            Settings(data_dir=tmp_path, ops_progress_enabled=False),
            http=http,
            versions=fixture_versions(),
        )
        incoming = captured(service, "S30", {"external_id": "entry", "title": "Title", "text": "Facts"})
        del incoming["observed_at"]
        with pytest.raises(ValidationError, match="observed_at"):
            await service.ingest(service.sources["S30"], incoming)
    assert await repo.list("ingest.source_items") == []
    assert await repo.list("ops.jobs") == []


def test_configured_source_intervals_preserve_canonical_json_and_reject_boolean() -> None:
    from tank.contracts.sources import SourceConfiguration, SourceDefinition
    from tank.settings import Settings
    from tank.util import canonical_json

    raw = Settings().load_config("sources")
    configured = SourceConfiguration.model_validate(raw)
    assert canonical_json(configured.model_dump(mode="json", exclude_unset=True)) == canonical_json(raw)
    first = configured.sources[0]
    invalid = first.model_dump(mode="json", exclude_unset=True)
    invalid["interval_s"] = True
    with pytest.raises(ValidationError, match="interval_s"):
        SourceDefinition.model_validate(invalid)


@pytest.mark.asyncio
async def test_source_repository_validation_logs_hide_complete_input_record(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    from agent_source_fixtures import source_item_fixture

    from tank.contracts.repository import Repository as TypedRepository
    from tank.contracts.source_items import SOURCE_ITEMS

    repo = MemoryRepository()
    marker = "synthetic-private-source-text-that-must-not-enter-errors"
    document = source_item_fixture("invalid-source", text=marker).storage_record()
    document["source_id"] = False
    await repo.put("ingest.source_items", "invalid-source", document)
    typed = TypedRepository(repo, SOURCE_ITEMS)
    with pytest.raises(ValidationError) as failure:
        await typed.get("invalid-source")
    logging.getLogger("tank.ingestion.validation-test").error(
        "Stored source schema rejected", exc_info=failure.value
    )
    assert marker not in str(failure.value)
    assert marker not in caplog.text
    assert "input_value" not in caplog.text
    assert "declared source identity" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("clock", ["2026-10-20T10:00:00", "1792480800000"])
async def test_ingestion_requires_an_aware_capture_clock(tmp_path: Path, clock: str) -> None:
    from test_sources import captured

    from tank.settings import Settings
    from tank.sources import SourceService

    repo = MemoryRepository()
    async with httpx.AsyncClient() as http:
        service = SourceService(
            repo,
            Settings(data_dir=tmp_path, ops_progress_enabled=False),
            http=http,
            versions=fixture_versions(),
        )
        incoming = captured(service, "S30", {"external_id": "clock-test", "title": "Title", "text": "Facts"})
        incoming["observed_at"] = clock
        with pytest.raises(ValidationError, match="observed_at"):
            await service.ingest(service.sources["S30"], incoming)
    assert await repo.list("ingest.source_items") == []
    assert await repo.list("ops.jobs") == []


def test_endpoint_clock_declarations_preserve_wire_and_reject_conflicting_aliases() -> None:
    from tank.contracts.sources import EndpointVerification
    from tank.util import canonical_json

    plain: dict[str, JsonValue] = {
        "checked_at": "2026-10-17T16:54:49.052127+00:00",
        "status": "VERIFIED",
        "basis": "Explicit synthetic endpoint verification",
    }
    assert EndpointVerification.model_validate(plain).model_dump(mode="json", exclude_unset=True) == plain
    declared = {**plain, "checked_at_prec": "EXACT", "checked_at_precision": "EXACT"}
    assert canonical_json(
        EndpointVerification.model_validate(declared).model_dump(mode="json", exclude_unset=True)
    ) == canonical_json(declared)
    with pytest.raises(ValidationError, match="Conflicting timestamp precision"):
        EndpointVerification.model_validate(
            {
                **declared,
                "checked_at_precision": "BUCKET(1m)",
            }
        )


def test_explicit_source_budget_numbers_keep_their_declared_json_form() -> None:
    from tank.contracts.sources import SourceApiLimitsConfiguration, SourceConfiguration, SourceDefinition
    from tank.settings import Settings
    from tank.util import canonical_json

    settings = Settings()
    raw_api_limits = settings.load_config("api_limits")
    budgets = SourceApiLimitsConfiguration.model_validate(raw_api_limits)
    assert canonical_json(budgets.model_dump(mode="json", exclude_unset=True)) == canonical_json(
        raw_api_limits
    )
    configured = settings.load_config_as("sources", SourceConfiguration)
    source = configured.sources[0].model_dump(mode="json", exclude_unset=True)
    limits = source["limits"]
    feeds = source["feeds"]
    assert isinstance(limits, dict) and isinstance(feeds, list) and isinstance(feeds[0], dict)
    limits["budget_fraction"] = 1
    feeds[0]["request_weight"] = 2
    feeds[0]["requests_per_minute"] = 60
    feeds[0]["interval_s"] = 0.5
    reparsed = SourceDefinition.model_validate(source)
    assert canonical_json(reparsed.model_dump(mode="json", exclude_unset=True)) == canonical_json(source)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_budget", [None, 6])
async def test_source_service_rejects_missing_or_excessive_known_provider_budget(
    tmp_path: Path, bad_budget: int | None
) -> None:
    from tank.settings import Settings
    from tank.sources import SourceService

    class InvalidProviderBudget(Settings):
        def load_config(self, name: str) -> dict[str, JsonValue]:
            configuration = super().load_config(name)
            if name == "api_limits":
                rules = configuration["limits"]
                assert isinstance(rules, list)
                sec = next(rule for rule in rules if isinstance(rule, dict) and rule["provider"] == "sec.gov")
                assert isinstance(sec, dict)
                if bad_budget is None:
                    del sec["tank_max_requests_per_s"]
                else:
                    sec["tank_max_requests_per_s"] = bad_budget
            return configuration

    repo = MemoryRepository()
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValidationError, match="tank_max_requests_per_s|half the documented"):
            SourceService(
                repo, InvalidProviderBudget(data_dir=tmp_path), http=http, versions=fixture_versions()
            )
    assert await repo.list("ops.jobs") == []
    assert await repo.list("ingest.source_items") == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider,missing_fields,assigned_field",
    [
        ("sec.gov", ("documented_requests_per_s",), "tank_max_requests_per_s"),
        ("binance.spot", ("documented_weight_per_min",), "tank_max_weight_per_min"),
        ("binance.usdm", ("documented_weight_per_min",), "tank_max_weight_per_min"),
        ("hyperliquid", ("documented_weight_per_min",), "tank_max_weight_per_min"),
        ("etherscan", ("documented_requests_per_s",), "tank_max_requests_per_s"),
        ("etherscan", ("documented_requests_per_day",), "tank_max_requests_per_day"),
        ("etherscan", ("tank_max_requests_per_day",), None),
        ("etherscan", ("documented_requests_per_day", "tank_max_requests_per_day"), None),
    ],
)
async def test_required_documented_provider_capacity_cannot_be_omitted(
    tmp_path: Path, provider: str, missing_fields: tuple[str, ...], assigned_field: str | None
) -> None:
    from tank.settings import Settings
    from tank.sources import SourceService

    class MissingDocumentedCapacity(Settings):
        def load_config(self, name: str) -> dict[str, JsonValue]:
            configuration = super().load_config(name)
            if name == "api_limits":
                rows = configuration["limits"]
                assert isinstance(rows, list)
                rule = next(row for row in rows if isinstance(row, dict) and row["provider"] == provider)
                assert isinstance(rule, dict)
                for field in missing_fields:
                    del rule[field]
                if assigned_field is not None:
                    rule[assigned_field] = 1_000_000
            return configuration

    repo = MemoryRepository()
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValidationError):
            SourceService(
                repo, MissingDocumentedCapacity(data_dir=tmp_path), http=http, versions=fixture_versions()
            )
    assert await repo.list("ops.jobs") == []
    assert await repo.list("ingest.source_items") == []


def test_coinbase_and_kraken_keep_uncaptured_documented_capacity_explicit() -> None:
    from tank.contracts.sources import SourceApiLimitsConfiguration, SourceUncapturedRpsBudget
    from tank.settings import Settings

    configuration = SourceApiLimitsConfiguration.model_validate(Settings().load_config("api_limits"))
    rules = [rule for rule in configuration.limits if isinstance(rule, SourceUncapturedRpsBudget)]
    assert {rule.provider for rule in rules} == {"coinbase", "kraken"}
    for rule in rules:
        assert rule.documented_requests_per_s is None
        assert "documented_requests_per_s" not in rule.model_dump(mode="json", exclude_unset=True)
        assert rule.tank_max_requests_per_s > 0 and rule.basis


@pytest.mark.parametrize("key_policy", [0, True])
def test_search_budget_does_not_coerce_boolean_access_policy(key_policy: int | bool) -> None:
    from tank.contracts.sources import SourceApiLimitsConfiguration
    from tank.settings import Settings

    configuration = Settings().load_config("api_limits")
    rules = configuration["limits"]
    assert isinstance(rules, list)
    search = next(rule for rule in rules if isinstance(rule, dict) and rule["provider"] == "duckduckgo")
    assert isinstance(search, dict)
    search["api_key_required"] = key_policy
    with pytest.raises(ValidationError):
        SourceApiLimitsConfiguration.model_validate(configuration)


@pytest.mark.asyncio
async def test_source_ingestion_persists_explicit_version_facet_not_raw_settings(tmp_path: Path) -> None:
    from test_sources import captured

    from tank.contracts.provenance import ResolvedVersionIdentity
    from tank.contracts.source_items import ArticleSourceItem, SourceItemDocument
    from tank.settings import Settings
    from tank.sources import SourceService

    settings = Settings(data_dir=tmp_path, ops_progress_enabled=False)
    with pytest.raises(ValueError, match="resolve_versions"):
        settings.require_versions()
    versions = ResolvedVersionIdentity(
        config_version="declared-source-config", code_version="declared-source-code"
    )
    repo = MemoryRepository()
    async with httpx.AsyncClient() as http:
        source = SourceService(repo, settings, versions=versions, http=http)
        wire = captured(source, "S30", {"external_id": "versioned", "title": "Title", "text": "Facts"})
        record = await source.ingest(source.sources["S30"], wire)
    item = SourceItemDocument.model_validate(record).root
    assert isinstance(item, ArticleSourceItem)
    assert item.config_version == versions.config_version
    assert item.code_version == versions.code_version
    assert item.discovery_config_version == versions.config_version
    assert item.discovery_code_version == versions.code_version

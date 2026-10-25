"""Pending checkpoint decoding retains malformed evidence and blocks coverage advancement."""

import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue, ValidationError
from source_support import fixture_request_budgets, fixture_versions, source_definition

from tank.contracts.source_checkpoints import (
    CompletedPendingRow,
    MaterializedSourceObservation,
    QuarantinedSourceBatch,
    SourcePendingBatch,
)
from tank.contracts.source_items import PendingSourceItem, RawSourceReference, discovered_item
from tank.ops_progress import DisabledSourceProgress
from tank.settings import Settings
from tank.sources import SourceService
from tank.sources.checkpoints import QuarantineArchiveRepository, decode_pending_checkpoint, pending_coverage
from tank.sources.ingestion import SourceIngestion
from tank.sources.raw_store import RawItemStorage, write_raw_item
from tank.storage import MemoryRepository, Repository
from tank.util import sha256

CAPTURED_AT = "2026-10-25T06:00:00Z"
DETECTED_AT = datetime(2026, 10, 25, 6, 1, tzinfo=UTC)


def pending_row(
    tmp_path: Path, *, item_id: str = "item_original", observed_at: str = CAPTURED_AT
) -> PendingSourceItem:
    versions = fixture_versions()
    raw_ref, raw_sha256 = write_raw_item(
        tmp_path, "S30", item_id, observed_at, b"actual retained publisher response"
    )
    material = discovered_item(
        "S30",
        {
            "external_id": "release-42",
            "url": "https://example.org/a",
            "title": "Actual publisher release",
            "text": "Retained publisher text",
            "published_at": None,
        },
    )
    return PendingSourceItem(
        discovery=material.discovery,
        item_id=item_id,
        feed="https://example.org/rss",
        observed_at=observed_at,
        discovery_config_version=versions.config_version,
        discovery_code_version=versions.code_version,
        raw=RawSourceReference(raw_ref=raw_ref, raw_sha256=raw_sha256),
    )


def checkpoint(row: dict[str, JsonValue]) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    batch: dict[str, JsonValue] = {
        "batch_id": "original-batch",
        "acquired_at": CAPTURED_AT,
        "acquired_at_precision": "COLLECTION_ONLY",
        "kind": "rss",
        "rows": [row],
        "coverage": {"etag": "new-uncommitted-head", "last_modified": None},
    }
    return {"source_id": "S30", "etag": "prior-head", "pending_batches": [batch]}, batch


def test_missing_item_identity_is_retained_with_an_unresolved_coverage_barrier(tmp_path: Path) -> None:
    original = pending_row(tmp_path)
    row = original.storage_record()
    row.pop("item_id")
    state, batch = checkpoint(row)
    before = deepcopy(state)
    assert original.raw is not None
    raw_file = Path(original.raw.raw_ref)
    raw_bytes = raw_file.read_bytes()
    decoded = decode_pending_checkpoint(
        source_definition({"source_id": "S30"}), "S30:0", state, now=DETECTED_AT, versions=fixture_versions()
    )
    [pending] = decoded.batches
    assert isinstance(pending, SourcePendingBatch)
    assert not pending.rows and not pending.may_commit_coverage
    [quarantine] = pending.quarantines
    assert quarantine.raw_batch == batch and quarantine.raw_sha256 == sha256(batch)
    [issue] = quarantine.issues
    assert issue.reason == "MISSING_PREASSIGNED_ITEM_ID"
    assert issue.declared_item_id is None and issue.item_declaration == "ABSENT"
    assert issue.declared_external_id == "release-42" and issue.recovery is None
    assert state == before and state["etag"] == "prior-head"
    assert raw_file.read_bytes() == raw_bytes


def test_quarantine_roundtrip_and_restart_do_not_duplicate_or_reclock_evidence(tmp_path: Path) -> None:
    row = pending_row(tmp_path).storage_record()
    row.pop("item_id")
    state, _ = checkpoint(row)
    source = source_definition({"source_id": "S30"})
    first = decode_pending_checkpoint(source, "S30:0", state, now=DETECTED_AT, versions=fixture_versions())
    persisted: dict[str, JsonValue] = {**state, "pending_batches": first.storage_records()}
    restarted = decode_pending_checkpoint(
        source, "S30:0", persisted, now=DETECTED_AT + timedelta(days=1), versions=fixture_versions()
    )
    assert first.changed and not restarted.changed
    assert restarted.batches == first.batches
    assert restarted.unresolved_quarantines == 1
    assert not restarted.pending_external_ids()


def test_missing_historical_batch_and_owner_identity_are_not_filled_from_evidence_id(tmp_path: Path) -> None:
    row = pending_row(tmp_path).storage_record()
    raw: dict[str, JsonValue] = {"rows": [row], "coverage": {}}
    state: dict[str, JsonValue] = {"pending_batches": [raw]}
    decoded = decode_pending_checkpoint(
        source_definition({"source_id": "S30"}), "S30:0", state, now=DETECTED_AT, versions=fixture_versions()
    )
    [batch] = decoded.batches
    assert isinstance(batch, QuarantinedSourceBatch) and not batch.may_commit_coverage
    [evidence] = batch.quarantines
    assert evidence.raw_batch == raw and evidence.evidence_id
    assert evidence.origin.operation_source_id == "S30"
    assert evidence.origin.declared_checkpoint_source_id is None
    assert evidence.origin.checkpoint_source_declaration == "ABSENT"
    assert evidence.origin.declared_batch_id is None and evidence.origin.batch_declaration == "ABSENT"
    assert evidence.issues[0].row_position is None
    assert evidence.issues[0].item_declaration == "NOT_APPLICABLE"


@pytest.mark.parametrize(
    "removed", ["discovery_config_version", "discovery_code_version", "observed_at_prec"]
)
def test_missing_capture_provenance_is_quarantined_instead_of_filled(tmp_path: Path, removed: str) -> None:
    row = pending_row(tmp_path).storage_record()
    row.pop(removed)
    state, raw = checkpoint(row)
    decoded = decode_pending_checkpoint(
        source_definition({"source_id": "S30"}), "S30:0", state, now=DETECTED_AT, versions=fixture_versions()
    )
    [batch] = decoded.batches
    assert isinstance(batch, SourcePendingBatch) and not batch.rows
    assert batch.quarantines[0].raw_batch == raw
    assert batch.quarantines[0].issues[0].reason == "INVALID_PENDING_ROW"
    assert not batch.may_commit_coverage


def test_numeric_zero_external_identity_remains_a_real_identity(tmp_path: Path) -> None:
    row = pending_row(tmp_path).storage_record()
    row["external_id"] = 0
    state, _ = checkpoint(row)
    decoded = decode_pending_checkpoint(
        source_definition({"source_id": "S30"}), "S30:0", state, now=DETECTED_AT, versions=fixture_versions()
    )
    [batch] = decoded.batches
    assert isinstance(batch, SourcePendingBatch)
    assert batch.rows[0].discovery.external_id == "0"
    assert not batch.quarantines


def test_conflicting_row_owner_is_retained_as_declared_in_quarantine(tmp_path: Path) -> None:
    row = pending_row(tmp_path).storage_record()
    row["source_id"] = "S01"
    state, raw = checkpoint(row)
    decoded = decode_pending_checkpoint(
        source_definition({"source_id": "S30"}), "S30:0", state, now=DETECTED_AT, versions=fixture_versions()
    )
    [batch] = decoded.batches
    assert isinstance(batch, SourcePendingBatch) and not batch.rows
    [evidence] = batch.quarantines
    assert evidence.raw_batch == raw
    assert evidence.issues[0].declared_source_id == "S01"
    assert evidence.issues[0].source_declaration == "DECLARED"


def test_quarantine_origin_cannot_relabel_preserved_historical_batch(tmp_path: Path) -> None:
    row = pending_row(tmp_path).storage_record()
    row.pop("item_id")
    state, _ = checkpoint(row)
    decoded = decode_pending_checkpoint(
        source_definition({"source_id": "S30"}), "S30:0", state, now=DETECTED_AT, versions=fixture_versions()
    )
    [batch] = decoded.batches
    [evidence] = batch.quarantines
    altered = evidence.model_copy(
        update={"origin": evidence.origin.model_copy(update={"declared_batch_id": "invented"})}
    )
    with pytest.raises(ValidationError, match="identity"):
        type(evidence).model_validate(altered)


async def assert_archive_and_checkpoint_rollback(repository: Repository, tmp_path: Path) -> None:
    row = pending_row(tmp_path).storage_record()
    row.pop("item_id")
    state, _ = checkpoint(row)
    source = source_definition({"source_id": "S30"})
    versions = fixture_versions()
    decoded = decode_pending_checkpoint(source, "S30:0", state, now=DETECTED_AT, versions=versions)
    archives = QuarantineArchiveRepository(repository, versions)
    await repository.put("ops.source_checkpoints", "S30:0", state)
    with pytest.raises(RuntimeError, match="synthetic after both writes"):
        async with repository.lock("source-checkpoint:S30:0"), repository.transaction():
            persisted = await archives.persist(decoded, now=DETECTED_AT)
            await repository.put(
                "ops.source_checkpoints",
                "S30:0",
                {**state, "pending_batches": persisted.storage_records()},
            )
            raise RuntimeError("synthetic after both writes")
    assert await repository.get("ops.source_checkpoints", "S30:0") == state
    assert not await archives.archives_for_source(source)


@pytest.mark.asyncio
async def test_new_actual_ingestion_recovers_barrier_and_keeps_original_archive(tmp_path: Path) -> None:
    original = pending_row(tmp_path).storage_record()
    original.pop("item_id")
    state, raw_batch = checkpoint(original)
    source = source_definition({"source_id": "S30"})
    versions = fixture_versions()
    decoded = decode_pending_checkpoint(source, "S30:0", state, now=DETECTED_AT, versions=versions)
    repository = MemoryRepository()
    archives = QuarantineArchiveRepository(repository, versions)
    persisted = await archives.persist(decoded, now=DETECTED_AT)
    assert not persisted.coverage_commit().completed

    fresh = pending_row(tmp_path, item_id="item_new_observation", observed_at="2026-10-25T06:01:00Z")
    captured = fresh.captured()
    ingestion = SourceIngestion(
        repository,
        versions,
        {},
        RawItemStorage(tmp_path),
        DisabledSourceProgress(),
    )
    stored = await ingestion.ingest(source, captured)
    linked_at = DETECTED_AT + timedelta(minutes=2)
    fresh_batch = SourcePendingBatch(
        source_id="S30",
        batch_id="actual-fresh-acquisition",
        acquired_at=fresh.observed_at,
        acquired_at_precision="COLLECTION_ONLY",
        kind="rss",
        rows=[fresh],
        coverage=pending_coverage({}),
    )
    with_fresh = type(persisted).model_validate(
        persisted.model_copy(update={"batches": [*persisted.batches, fresh_batch]})
    )
    completed = [
        CompletedPendingRow(
            original=fresh, observation=MaterializedSourceObservation(captured=captured, stored=stored)
        )
    ]
    earlier = type(persisted).model_validate(
        persisted.model_copy(update={"batches": [fresh_batch, *persisted.batches]})
    )
    assert earlier.link_completions(completed, now=linked_at).unresolved_quarantines == 1
    [old_batch] = persisted.batches
    assert isinstance(old_batch, SourcePendingBatch)
    same_batch = SourcePendingBatch.model_validate(old_batch.model_copy(update={"rows": [fresh]}))
    same_acquisition = type(persisted).model_validate(persisted.model_copy(update={"batches": [same_batch]}))
    assert same_acquisition.link_completions(completed, now=linked_at).unresolved_quarantines == 1
    recovered = with_fresh.link_completions(completed, now=linked_at).complete_rows(completed)
    assert not recovered.unresolved_quarantines
    plan = recovered.coverage_commit()
    assert len(plan.completed) == 2 and not plan.remaining
    [quarantine] = plan.completed[0].quarantines
    [issue] = quarantine.issues
    assert issue.recovery is not None
    assert issue.recovery.item_id == stored.item_id == "item_new_observation"
    assert issue.recovery.acquisition_batch_id == fresh_batch.batch_id
    assert issue.recovery.raw_ref == captured.capture.raw_ref
    assert issue.recovery.raw_sha256 == captured.capture.raw_sha256
    assert quarantine.raw_batch == raw_batch

    await archives.persist(recovered, now=linked_at)
    [archived] = await archives.archives_for_source(source)
    assert archived.evidence == quarantine and archived.evidence.raw_batch == raw_batch
    restored = await archives.persist(persisted, now=linked_at + timedelta(hours=1))
    assert not restored.unresolved_quarantines
    assert await archives.archives_for_source(source) == [archived]


class DeclaredClockSource(SourceService):
    clock: datetime = DETECTED_AT

    def now(self) -> datetime:
        return self.clock


async def assert_actual_quarantine_restart_and_fresh_capture(repository: Repository, tmp_path: Path) -> None:
    original = pending_row(tmp_path)
    assert original.raw is not None
    raw_path = Path(original.raw.raw_ref)
    raw_bytes = raw_path.read_bytes()
    malformed = original.storage_record()
    malformed.pop("item_id")
    state, raw_batch = checkpoint(malformed)
    await repository.put("ops.source_checkpoints", "S30:0", state)
    source = source_definition(
        {"source_id": "S30", "feeds": [{"kind": "rss", "url": "https://example.org/rss"}]}
    )
    publisher = b"""<rss version="2.0"><channel><title>Publisher</title><item>
    <guid>release-42</guid><title>Actual publisher release</title>
    <link>https://example.org/a</link><description>Newly observed publisher text</description>
    </item></channel></rss>"""
    requests: list[httpx.Request] = []
    publication_id = "unrelated-publisher-entry"

    def respond(request: httpx.Request) -> httpx.Response:
        assert "if-none-match" not in request.headers
        assert "if-modified-since" not in request.headers
        requests.append(request)
        return httpx.Response(
            200,
            content=publisher.replace(b"release-42", publication_id.encode()).replace(
                b"Newly observed publisher text", ("Publisher text for " + publication_id).encode()
            ),
            headers={"etag": "fresh-head"},
        )

    budgets = fixture_request_budgets()
    settings = Settings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = DeclaredClockSource(
            repository, settings, http=http, versions=fixture_versions(), budgets=budgets
        )
        service.sources["S30"] = source
        try:
            failed = await service.poller.drain(source, 0, force=True)
            assert failed.items == 0 and failed.errors == ["PENDING_CHECKPOINT_QUARANTINE"]
            durable = await repository.get("ops.source_checkpoints", "S30:0")
            assert durable is not None and durable["etag"] == "prior-head"
            decoded = await service.poller.pending(source, "S30:0", durable)
            assert decoded.unresolved_quarantines == 1
            assert decoded.coverage_commit().completed == []
            [archived] = await service.poller.archives.archives_for_source(source)
            assert archived.evidence.raw_batch == raw_batch
            assert archived.evidence.issues[0].declared_item_id is None
            assert raw_path.read_bytes() == raw_bytes
        finally:
            await service.close()
        restarted = DeclaredClockSource(
            repository, settings, http=http, versions=fixture_versions(), budgets=budgets
        )
        restarted.sources["S30"] = source
        try:
            assert (await restarted.poller.drain(source, 0, force=True)).items == 0
            [again] = await restarted.poller.archives.archives_for_source(source)
            assert again == archived and not requests
            restarted.clock = DETECTED_AT + timedelta(minutes=1)
            unrelated = await restarted.poll_once("S30")
            assert unrelated.items == 1 and unrelated.status == "DEGRADED"
            blocked = await repository.get("ops.source_checkpoints", "S30:0")
            assert blocked is not None and blocked["etag"] == "prior-head"
            assert "last_success_at" not in blocked
            blocked_pending = await restarted.poller.pending(source, "S30:0", blocked)
            assert blocked_pending.unresolved_quarantines == 1
            assert not blocked_pending.coverage_commit().completed
            assert all(
                not batch.rows for batch in blocked_pending.batches if isinstance(batch, SourcePendingBatch)
            )
            assert await restarted.poller.archives.archives_for_source(source) == [archived]
            publication_id = "release-42"
            restarted.clock = DETECTED_AT + timedelta(minutes=2)
            outcome = await restarted.poll_once("S30")
            assert outcome.status == "HEALTHY" and outcome.items == 1
            items = await repository.list("ingest.source_items")
            assert len(items) == 2
            [stored] = [item for item in items if item["external_id"] == "release-42"]
            [resolved] = await restarted.poller.archives.archives_for_source(source)
            recovery = resolved.evidence.issues[0].recovery
            assert recovery is not None and recovery.item_id == stored["item_id"]
            assert recovery.raw_ref == stored["raw_ref"]
            assert resolved.evidence.raw_batch == raw_batch
            assert resolved.evidence.origin == archived.evidence.origin
            assert raw_path.read_bytes() == raw_bytes
            final = await repository.get("ops.source_checkpoints", "S30:0")
            assert final is not None and final["pending_batches"] == []
            assert final["etag"] == "fresh-head"
            assert (await restarted.poller.drain(source, 0, force=True)).items == 0
            assert len(requests) == 2
            assert await restarted.poller.archives.archives_for_source(source) == [resolved]
        finally:
            await restarted.close()
    await budgets.shutdown()


@pytest.mark.asyncio
async def test_actual_quarantine_restart_and_fresh_capture(tmp_path: Path) -> None:
    await assert_actual_quarantine_restart_and_fresh_capture(MemoryRepository(), tmp_path)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_quarantine_restart_and_fresh_capture(
    tmp_path: Path, postgres_template: PostgresTemplate
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repository:
        await assert_actual_quarantine_restart_and_fresh_capture(repository, tmp_path)


@pytest.mark.asyncio
async def test_archive_and_checkpoint_writes_roll_back_together_on_actual_repository(tmp_path: Path) -> None:
    await assert_archive_and_checkpoint_rollback(MemoryRepository(), tmp_path)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_archive_and_checkpoint_writes_roll_back_together(
    tmp_path: Path, postgres_template: PostgresTemplate
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repository:
        await assert_archive_and_checkpoint_rollback(repository, tmp_path)

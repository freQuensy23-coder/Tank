"""Precision integration tests use only owned disposable databases; never migrate shared QA."""

import os
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
import pytest_asyncio
from foundation_helpers import present, text_field
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

from tank.contracts.jobs import JobEnvelope
from tank.models import Record
from tank.storage import MemoryRepository, PostgresRepository, _projection
from tank.time_precision import (
    Timestamp,
    chosen_boundary,
    collection_clock,
    offset_schedule,
    propagate_timestamp,
)
from tank.util import clock_iso, iso, parse_time, utcnow


@pytest_asyncio.fixture
async def disposable_pg(postgres_template: PostgresTemplate) -> AsyncIterator[PostgresRepository]:
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.fail("Requires isolated PostgreSQL16 TANK_TEST_DATABASE_URL with CREATE DATABASE")
    async with postgres_template.repository(url) as repository:
        yield repository


@pytest_asyncio.fixture
async def upgrade_pg(postgres_template: PostgresTemplate) -> AsyncIterator[PostgresRepository]:
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.fail("Requires isolated PostgreSQL16 TANK_TEST_DATABASE_URL with CREATE DATABASE")
    async with postgres_template.repository(url, migrated=False) as repository:
        yield repository


async def test_enqueue_explicit_anchors_and_unknown_legacy_clock_iso() -> None:
    repo = MemoryRepository()
    boundary = chosen_boundary(utcnow() + timedelta(days=1), precision="BUCKET(5m)")
    job = await repo.enqueue("REACTION_CAPTURE", {}, "bucket", run_after=boundary)
    assert job["run_after_prec"] == "BUCKET(5m)" and job["expires_at_prec"] == "BUCKET(5m)"
    assert parse_time(text_field(job, "expires_at")) == parse_time(text_field(job, "run_after")) + timedelta(
        hours=6
    )
    assert job["created_at_prec"] == "COLLECTION_ONLY"
    plain = await repo.enqueue(
        "CUSTOM",
        {},
        "legacy",
        run_after=boundary.value,
        expires_at=offset_schedule(boundary, timedelta(hours=1)).value,
    )
    assert plain["run_after_prec"] is None and plain["run_after_prec_reason"] == "INCOMPLETE"
    assert plain["expires_at_prec"] is None and plain["expires_at_prec_reason"] == "INCOMPLETE"
    assert job["finished_at"] is None and job["finished_at_prec_reason"] == "PENDING"


def test_projection_explicit_aliases_conflicts_and_publication_enum() -> None:
    source: Record = {
        "published_at": "2026-10-18T00:00:00Z",
        "published_prec": "BUCKET",
        "published_at_precision": "BUCKET(1d)",
        "published_at_bucket_size_s": 86400,
    }
    row = _projection("ingest.source_items", source)
    assert row["published_prec"] == "BUCKET" and row["published_at_bucket_size_s"] == 86400
    assert _projection("evt.events", {"known_to_us_at": clock_iso()})["known_to_us_at_prec"] is None
    usage_fields: Record = {**collection_clock("2026-10-18T00:00:00Z").fields("created_at")}
    usage = _projection("ops.llm_usage", usage_fields)
    assert usage["at_prec"] == "COLLECTION_ONLY"
    assert _projection("ops.llm_usage", {"created_at": "2026-10-18T00:00:00Z"})["at_prec"] is None
    with pytest.raises(ValueError, match="Conflicting"):
        _projection(
            "evt.events",
            {"public_at": clock_iso(), "public_at_prec": "EXACT", "public_at_precision": "BUCKET(1d)"},
        )


async def _sql_job(repo: PostgresRepository, key: str, *, created_at: str | None = None) -> Record:
    from psycopg.types.json import Jsonb

    run_after = iso(utcnow() - timedelta(seconds=1))
    document: Record = {
        "job_id": key,
        "type": "CUSTOM",
        "priority": 1,
        "idem_key": key,
        "payload": {},
        "status": "QUEUED",
        "attempts": 0,
        "max_attempts": 2,
        "run_after": run_after,
        "expires_at": None,
    }
    if created_at is not None:
        document["created_at"] = created_at
    async with repo.transaction():
        await repo._conn().execute(
            """INSERT INTO ops.jobs(storage_key,document,job_id,type,priority,idem_key,status,attempts,max_attempts,run_after,expires_at)
          VALUES(%s,%s,%s,'CUSTOM',1,%s,'QUEUED',0,2,%s,NULL)""",
            (key, Jsonb(document), key, key, parse_time(run_after)),
        )
    return present(await repo.get("ops.jobs", key))


@pytest.mark.integration
@pytest.mark.postgres
async def test_fresh_sql_lifecycle_clock_and_retry_precision(disposable_pg: PostgresRepository) -> None:
    repo = disposable_pg
    await repo.migrate()
    job = await _sql_job(repo, "database-clock")
    assert job["created_at_prec"] == "COLLECTION_ONLY"
    assert job["created_at_prec_reason"] is None
    claimed = present(await repo.claim_job())
    assert text_field(claimed, "job_id") == text_field(job, "job_id")
    await repo.fail_job(text_field(job, "job_id"), "retryable", retry_after=0)
    retried = present(await repo.get("ops.jobs", text_field(job, "job_id")))
    assert retried["run_after_prec"] == "COLLECTION_ONLY" and retried["finished_at"] is None
    assert (present(await repo.claim_job()))["attempts"] == 2
    await repo.recover_jobs()
    recovered = present(await repo.get("ops.jobs", text_field(job, "job_id")))
    assert recovered["recovery_at_prec"] == "COLLECTION_ONLY"
    assert recovered["finished_at_prec_reason"] == "PENDING"
    assert (present(await repo.claim_job()))["attempts"] == 2
    await repo.finish_job(text_field(job, "job_id"))
    terminal = present(await repo.get("ops.jobs", text_field(job, "job_id")))
    assert terminal["status"] == "DONE" and terminal["finished_at_prec"] == "COLLECTION_ONLY"
    async with repo.transaction():
        sqlrow = present(
            await (
                await repo._conn().execute(
                    "SELECT document,created_at_prec,run_after_prec,finished_at_prec FROM ops.jobs WHERE job_id=%s",
                    (text_field(job, "job_id"),),
                )
            ).fetchone()
        )
    saved_document = sqlrow["document"]
    assert isinstance(saved_document, dict)
    for field in ("created_at_prec", "run_after_prec", "finished_at_prec"):
        assert sqlrow[field] == saved_document[field]
    future = chosen_boundary(utcnow() + timedelta(days=1), precision="BUCKET(300s)")
    queued = await repo.enqueue("REACTION_CAPTURE", {}, "explicit-boundary", run_after=future)
    saved = present(await repo.get("ops.jobs", text_field(queued, "job_id")))
    assert saved["run_after_prec"] == "BUCKET(300s)" and saved["expires_at_prec"] == "BUCKET(300s)"
    legacy = await repo.enqueue("CUSTOM", {}, "plain-iso", run_after=future.value)
    assert (present(await repo.get("ops.jobs", text_field(legacy, "job_id"))))[
        "run_after_prec_reason"
    ] == "INCOMPLETE"
    direct = await _sql_job(repo, "direct-sql-terminal")
    async with repo.transaction():
        await repo._conn().execute(
            "UPDATE ops.jobs SET status='FAILED',document=jsonb_set(document,'{status}','\"FAILED\"') WHERE job_id=%s",
            (text_field(direct, "job_id"),),
        )
        direct_row = present(
            await (
                await repo._conn().execute(
                    "SELECT document,finished_at_prec,finished_at_prec_reason FROM ops.jobs WHERE job_id=%s",
                    (text_field(direct, "job_id"),),
                )
            ).fetchone()
        )
    direct_document = direct_row["document"]
    assert isinstance(direct_document, dict)
    assert direct_document["finished_at_prec"] == direct_row["finished_at_prec"] == "COLLECTION_ONLY"
    assert direct_row["finished_at_prec_reason"] is None


@pytest.mark.integration
@pytest.mark.postgres
async def test_upgrade_nullable_metadata_preserves_immutable_history(upgrade_pg: PostgresRepository) -> None:
    from psycopg.types.json import Jsonb

    repo = upgrade_pg
    await repo.migrate(target="025_capture_tier_transitions.sql")
    legacy = {
        "notice_id": "legacy-notice",
        "kind": "HEARTBEAT",
        "text": "frozen old notice",
        "created_at": "2026-01-01T00:00:00Z",
        "created_by": "scheduler",
    }
    async with repo.transaction():
        await repo._conn().execute(
            """INSERT INTO out.ops_notices(storage_key,document,immutable,notice_id,kind,text,created_at,created_by)
          VALUES('legacy-notice',%s,true,'legacy-notice','HEARTBEAT','frozen old notice','2026-01-01T00:00:00Z','scheduler')""",
            (Jsonb(legacy),),
        )
    before = await _sql_job(repo, "legacy-job", created_at="2026-01-01T00:00:00Z")
    await repo.migrate()
    await repo.migrate()
    assert await repo.get("out.ops_notices", "legacy-notice") == legacy
    assert await repo.get("ops.jobs", "legacy-job") == before
    async with repo.transaction():
        row = present(
            await (
                await repo._conn().execute(
                    "SELECT created_at_prec,created_at_prec_reason FROM out.ops_notices WHERE notice_id='legacy-notice'"
                )
            ).fetchone()
        )
    assert row["created_at_prec"] is None and row["created_at_prec_reason"] is None
    await repo.claim_job()
    changed = present(await repo.get("ops.jobs", "legacy-job"))
    assert changed["created_at_prec"] is None and changed["created_at_prec_reason"] == "INCOMPLETE"
    assert await repo.get("out.ops_notices", "legacy-notice") == legacy


async def _retry_replaces_bucket_clock(repo: MemoryRepository | PostgresRepository) -> None:
    boundary = Timestamp("2026-10-01T00:00:00Z", "BUCKET(60s)", None, "END")
    items: list[JsonValue] = [1, 2]
    job = await repo.enqueue(
        "CUSTOM", {"checkpoint": {"items": items}}, "retry-bucket-clock", run_after=boundary
    )
    JobEnvelope.model_validate(job)
    claimed = present(await repo.claim_job())
    assert claimed["run_after_bucket_anchor"] == "END"
    original_payload = claimed["payload"]
    original_created = propagate_timestamp(claimed, "created_at")
    original_expiry = propagate_timestamp(claimed, "expires_at")
    await repo.fail_job(text_field(claimed, "job_id"), "retryable", retry_after=0)
    retried = present(await repo.get("ops.jobs", text_field(claimed, "job_id")))
    assert retried["status"] == "QUEUED" and retried["run_after_prec"] == "COLLECTION_ONLY"
    assert "run_after_bucket_anchor" not in retried
    assert propagate_timestamp(retried, "created_at") == original_created
    assert propagate_timestamp(retried, "expires_at") == original_expiry
    assert retried["payload"] == original_payload
    JobEnvelope.model_validate(retried)
    next_claim = present(await repo.claim_job())
    assert next_claim["attempts"] == 2
    await repo.finish_job(text_field(next_claim, "job_id"))
    complete = JobEnvelope.model_validate(
        present(await repo.get("ops.jobs", text_field(next_claim, "job_id")))
    )
    assert complete.status == "DONE" and complete.finished_at_prec == "COLLECTION_ONLY"


async def _recovery_replaces_reported_clock_alias(repo: MemoryRepository | PostgresRepository) -> None:
    job = await repo.enqueue("CUSTOM", {"source": "retained"}, "recovery-clock-alias")
    await repo.claim_job()
    assert await repo.recover_jobs() == 1
    reclaimed = present(await repo.claim_job())
    assert reclaimed["recovery_count"] == 1
    original_created = propagate_timestamp(reclaimed, "created_at")
    original_schedule = propagate_timestamp(reclaimed, "run_after")
    prior_value = text_field(reclaimed, "recovery_at")
    reclaimed["recovery_at_prec"] = None
    reclaimed["recovery_at_prec_reason"] = "PRECISION_NOT_RETAINED_IN_CAPTURE"
    JobEnvelope.model_validate(reclaimed)
    reclaimed["recovery_at_precision"] = "UNKNOWN"
    assert propagate_timestamp(reclaimed, "recovery_at").precision is None
    await repo.put("ops.jobs", text_field(job, "job_id"), reclaimed)
    assert await repo.recover_jobs() == 1
    recovered = present(await repo.get("ops.jobs", text_field(job, "job_id")))
    assert "recovery_at_precision" not in recovered
    clock = propagate_timestamp(recovered, "recovery_at")
    assert clock.precision == "COLLECTION_ONLY" and clock.precision_reason is None
    assert clock.value is not None and parse_time(clock.value) >= parse_time(prior_value)
    assert recovered["recovery_count"] == 2 and recovered["payload"] == {"source": "retained"}
    assert propagate_timestamp(recovered, "created_at") == original_created
    assert propagate_timestamp(recovered, "run_after") == original_schedule
    JobEnvelope.model_validate(recovered)


async def test_memory_retry_replaces_complete_bucket_clock_family() -> None:
    await _retry_replaces_bucket_clock(MemoryRepository())


async def test_memory_recovery_replaces_reported_unknown_clock_alias() -> None:
    await _recovery_replaces_reported_clock_alias(MemoryRepository())


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_retry_replaces_complete_bucket_clock_family(
    disposable_pg: PostgresRepository,
) -> None:
    await _retry_replaces_bucket_clock(disposable_pg)


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_recovery_replaces_reported_unknown_clock_alias(
    disposable_pg: PostgresRepository,
) -> None:
    await _recovery_replaces_reported_clock_alias(disposable_pg)

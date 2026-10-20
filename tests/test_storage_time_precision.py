"""Precision integration tests use only owned disposable databases; never migrate shared QA."""

from __future__ import annotations

import os
from datetime import timedelta

import pytest
import pytest_asyncio

from tank.storage import MemoryRepository, PostgresRepository, _projection
from tank.time_precision import chosen_boundary, collection_clock, offset_schedule
from tank.util import iso, new_id, parse_time, utcnow


@pytest_asyncio.fixture
async def disposable_pg():
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires isolated PostgreSQL16 TANK_TEST_DATABASE_URL with CREATE DATABASE")
    name = "tank_precision_conformance_" + new_id("test").replace("-", "").lower()
    admin = await psycopg.AsyncConnection.connect(url, autocommit=True)
    repository = None
    created = False
    try:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        created = True
        parameters = conninfo_to_dict(url)
        parameters["dbname"] = name
        repository = PostgresRepository(make_conninfo(**parameters))
        await repository.open()
        yield repository
    finally:
        if repository is not None:
            await repository.close()
        if created:
            await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def test_enqueue_explicit_anchors_and_unknown_legacy_iso():
    repo = MemoryRepository()
    boundary = chosen_boundary(utcnow() + timedelta(days=1), precision="BUCKET(5m)")
    job = await repo.enqueue("REACTION_CAPTURE", {}, "bucket", run_after=boundary)
    assert job["run_after_prec"] == "BUCKET(5m)" and job["expires_at_prec"] == "BUCKET(5m)"
    assert parse_time(job["expires_at"]) == parse_time(job["run_after"]) + timedelta(hours=6)
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


def test_projection_explicit_aliases_conflicts_and_publication_enum():
    source = {
        "published_at": "2026-10-18T00:00:00Z",
        "published_prec": "BUCKET",
        "published_at_precision": "BUCKET(1d)",
        "published_at_bucket_size_s": 86400,
    }
    row = _projection("ingest.source_items", source)
    assert row["published_prec"] == "BUCKET" and row["published_at_bucket_size_s"] == 86400
    assert _projection("evt.events", {"known_to_us_at": iso()})["known_to_us_at_prec"] is None
    usage = _projection("ops.llm_usage", collection_clock("2026-10-18T00:00:00Z").fields("created_at"))
    assert usage["at_prec"] == "COLLECTION_ONLY"
    assert _projection("ops.llm_usage", {"created_at": "2026-10-18T00:00:00Z"})["at_prec"] is None
    with pytest.raises(ValueError, match="Conflicting"):
        _projection(
            "evt.events", {"public_at": iso(), "public_at_prec": "EXACT", "public_at_precision": "BUCKET(1d)"}
        )


async def _sql_job(repo, key, *, created_at=None):
    from psycopg.types.json import Jsonb

    document = {
        "job_id": key,
        "type": "CUSTOM",
        "priority": 1,
        "idem_key": key,
        "payload": {},
        "status": "QUEUED",
        "attempts": 0,
        "max_attempts": 2,
        "run_after": iso(utcnow() - timedelta(seconds=1)),
        "expires_at": None,
    }
    if created_at is not None:
        document["created_at"] = created_at
    async with repo.transaction():
        await repo._conn().execute(
            """INSERT INTO ops.jobs(storage_key,document,job_id,type,priority,idem_key,payload,status,attempts,max_attempts,run_after,expires_at)
          VALUES(%s,%s,%s,'CUSTOM',1,%s,%s,'QUEUED',0,2,%s,NULL)""",
            (key, Jsonb(document), key, key, Jsonb({}), parse_time(document["run_after"])),
        )
    return await repo.get("ops.jobs", key)


async def test_fresh_sql_lifecycle_clock_and_retry_precision(disposable_pg):
    repo = disposable_pg
    await repo.migrate()
    job = await _sql_job(repo, "database-clock")
    assert job["created_at_prec"] == "COLLECTION_ONLY"
    assert job["created_at_prec_reason"] is None
    claimed = await repo.claim_job()
    assert claimed["job_id"] == job["job_id"]
    await repo.fail_job(job["job_id"], "retryable", retry_after=0)
    retried = await repo.get("ops.jobs", job["job_id"])
    assert retried["run_after_prec"] == "COLLECTION_ONLY" and retried["finished_at"] is None
    assert (await repo.claim_job())["attempts"] == 2
    await repo.recover_jobs()
    recovered = await repo.get("ops.jobs", job["job_id"])
    assert recovered["recovery_at_prec"] == "COLLECTION_ONLY"
    assert recovered["finished_at_prec_reason"] == "PENDING"
    assert (await repo.claim_job())["attempts"] == 2
    await repo.finish_job(job["job_id"])
    terminal = await repo.get("ops.jobs", job["job_id"])
    assert terminal["status"] == "DONE" and terminal["finished_at_prec"] == "COLLECTION_ONLY"
    async with repo.transaction():
        sqlrow = await (
            await repo._conn().execute(
                "SELECT document,created_at_prec,run_after_prec,finished_at_prec FROM ops.jobs WHERE job_id=%s",
                (job["job_id"],),
            )
        ).fetchone()
    for field in ("created_at_prec", "run_after_prec", "finished_at_prec"):
        assert sqlrow[field] == sqlrow["document"][field]
    future = chosen_boundary(utcnow() + timedelta(days=1), precision="BUCKET(300s)")
    queued = await repo.enqueue("REACTION_CAPTURE", {}, "explicit-boundary", run_after=future)
    saved = await repo.get("ops.jobs", queued["job_id"])
    assert saved["run_after_prec"] == "BUCKET(300s)" and saved["expires_at_prec"] == "BUCKET(300s)"
    legacy = await repo.enqueue("CUSTOM", {}, "plain-iso", run_after=future.value)
    assert (await repo.get("ops.jobs", legacy["job_id"]))["run_after_prec_reason"] == "INCOMPLETE"
    direct = await _sql_job(repo, "direct-sql-terminal")
    async with repo.transaction():
        await repo._conn().execute(
            "UPDATE ops.jobs SET status='FAILED',document=jsonb_set(document,'{status}','\"FAILED\"') WHERE job_id=%s",
            (direct["job_id"],),
        )
        direct_row = await (
            await repo._conn().execute(
                "SELECT document,finished_at_prec,finished_at_prec_reason FROM ops.jobs WHERE job_id=%s",
                (direct["job_id"],),
            )
        ).fetchone()
    assert direct_row["document"]["finished_at_prec"] == direct_row["finished_at_prec"] == "COLLECTION_ONLY"
    assert direct_row["finished_at_prec_reason"] is None


async def test_upgrade_nullable_metadata_preserves_immutable_history(disposable_pg):
    from psycopg.types.json import Jsonb

    repo = disposable_pg
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
        row = await (
            await repo._conn().execute(
                "SELECT created_at_prec,created_at_prec_reason FROM out.ops_notices WHERE notice_id='legacy-notice'"
            )
        ).fetchone()
    assert row["created_at_prec"] is None and row["created_at_prec_reason"] is None
    await repo.claim_job()
    changed = await repo.get("ops.jobs", "legacy-job")
    assert changed["created_at_prec"] is None and changed["created_at_prec_reason"] == "INCOMPLETE"
    assert await repo.get("out.ops_notices", "legacy-notice") == legacy

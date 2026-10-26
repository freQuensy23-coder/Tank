import asyncio
import os

import psycopg
import pytest
from postgres_helpers import PostgresTemplate
from psycopg.conninfo import make_conninfo
from test_storage import repo as repo

from tank.contracts.reaction_recovery import ReactionRecoveryWitnessRequest
from tank.contracts.stored_documents import ImmutableDocument, NativeBackfillWitness
from tank.models import Record
from tank.storage import MemoryRepository, PostgresRepository, StorageBackend
from tank.util import new_id


def witness_request() -> ReactionRecoveryWitnessRequest:
    return ReactionRecoveryWitnessRequest(
        enqueue_idempotency_key=new_id("recovery"),
        original_job_id=new_id("original"),
        peer_job_id=new_id("peer"),
        source_backfill_id=new_id("backfill"),
        checkpoint_key=new_id("checkpoint"),
        change_marker_key=new_id("marker"),
        change_owner_key=new_id("owner"),
        recovery_link_key=new_id("link"),
    )


def marker(key: str) -> Record:
    return {
        "checkpoint_id": key,
        "at": "2026-10-17T12:34:56.123456Z",
        "at_prec": "COLLECTION_ONLY",
        "at_prec_reason": None,
        "state": {"values": [1]},
    }


async def test_recovery_witness_index_reports_all_actual_absences(repo: StorageBackend) -> None:
    request = witness_request()
    async with repo.reaction_recovery_witnesses(request) as witnesses:
        assert tuple(witnesses) == tuple((a.collection, a.storage_key) for a in request.addresses())
        assert len(witnesses) == 7
        assert all(value is None for value in witnesses.values())


async def test_recovery_document_witness_preserves_flag_and_isolation(repo: StorageBackend) -> None:
    request = witness_request()
    original = marker(request.change_marker_key)
    await repo.put("ops.scheduler_checkpoints", request.change_marker_key, original, immutable=True)
    async with repo.reaction_recovery_witnesses(request) as witnesses:
        value = witnesses[("ops.scheduler_checkpoints", request.change_marker_key)]
        assert isinstance(value, ImmutableDocument)
        assert value.immutable is True
        assert value.document == original
        state = value.document["state"]
        assert isinstance(state, dict)
        values = state["values"]
        assert isinstance(values, list)
        values.append("returned mutation")
    assert await repo.get("ops.scheduler_checkpoints", request.change_marker_key) == original


async def test_recovery_native_witness_is_actual_read_without_document_metadata(
    repo: StorageBackend,
) -> None:
    request = witness_request()
    source: Record = {
        "backfill_id": request.source_backfill_id,
        "event_id": "synthetic:event",
        "token_id": "synthetic:token",
        "status": "PENDING",
        "anchor": "2026-10-17T12:00:00Z",
        "anchor_prec": "EXACT",
        "anchor_prec_reason": None,
        "result": None,
    }
    await repo.put("mkt.backfill_runs", request.source_backfill_id, source)
    expected = await repo.get("mkt.backfill_runs", request.source_backfill_id)
    async with repo.reaction_recovery_witnesses(request) as witnesses:
        value = witnesses[("mkt.backfill_runs", request.source_backfill_id)]
        assert isinstance(value, NativeBackfillWitness)
        assert value.record == expected
        assert set(value.model_dump()) == {"record"}
        value.record["reason"] = "returned mutation"
    assert await repo.get("mkt.backfill_runs", request.source_backfill_id) == expected


async def test_recovery_body_enqueue_and_link_roll_back_together(repo: StorageBackend) -> None:
    request = witness_request()
    with pytest.raises(RuntimeError, match="abort recovery"):
        async with repo.reaction_recovery_witnesses(request):
            await repo.enqueue("REACTION_CAPTURE", {}, request.enqueue_idempotency_key)
            await repo.put(
                "ops.scheduler_checkpoints",
                request.recovery_link_key,
                marker(request.recovery_link_key),
                immutable=True,
            )
            raise RuntimeError("abort recovery")
    assert await repo.list("ops.jobs", filters={"idem_key": request.enqueue_idempotency_key}) == []
    assert await repo.get("ops.scheduler_checkpoints", request.recovery_link_key) is None
    async with repo.reaction_recovery_witnesses(request):
        first = await repo.enqueue("REACTION_CAPTURE", {}, request.enqueue_idempotency_key)
        second = await repo.enqueue("REACTION_CAPTURE", {}, request.enqueue_idempotency_key)
        assert first["job_id"] == second["job_id"]
    assert len(await repo.list("ops.jobs", filters={"idem_key": request.enqueue_idempotency_key})) == 1


async def test_memory_recovery_waits_for_enqueue_before_global_transaction() -> None:
    repository = MemoryRepository()
    await repository.open()
    request = witness_request()
    entered = asyncio.Event()

    async def read_witnesses() -> None:
        entered.set()
        async with repository.reaction_recovery_witnesses(request):
            pass

    try:
        async with repository.lock(f"queue:REACTION_CAPTURE:{request.enqueue_idempotency_key}"):
            task = asyncio.create_task(read_witnesses())
            try:
                await entered.wait()
                await repository.put("ops.scheduler_checkpoints", "unrelated", marker("unrelated"))
                assert not task.done()
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        async with repository.transaction():
            with pytest.raises(RuntimeError, match="before Memory transactions"):
                async with repository.reaction_recovery_witnesses(request):
                    pass
    finally:
        await repository.close()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("operation", ["UPDATE", "DELETE"])
async def test_native_recovery_witness_fences_physical_writers(
    postgres_template: PostgresTemplate, operation: str
) -> None:
    url = os.environ["TANK_TEST_DATABASE_URL"]
    async with postgres_template.repository(url) as repository:
        request = witness_request()
        await repository.put(
            "mkt.backfill_runs",
            request.source_backfill_id,
            {
                "backfill_id": request.source_backfill_id,
                "status": "PENDING",
                "anchor": "2026-10-17T12:00:00Z",
                "anchor_prec": "EXACT",
                "anchor_prec_reason": None,
                "result": None,
            },
        )
        connection = await psycopg.AsyncConnection.connect(repository.database_url, autocommit=True)
        monitor = await psycopg.AsyncConnection.connect(repository.database_url, autocommit=True)
        try:
            async with repository.reaction_recovery_witnesses(request):
                statement = (
                    "UPDATE mkt.backfill_runs SET reason='actual concurrent update' WHERE storage_key=%s"
                    if operation == "UPDATE"
                    else "DELETE FROM mkt.backfill_runs WHERE storage_key=%s"
                )
                writer = asyncio.create_task(connection.execute(statement, (request.source_backfill_id,)))
                try:
                    async with asyncio.timeout(5):
                        while True:
                            cursor = await monitor.execute(
                                "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s",
                                (connection.info.backend_pid,),
                            )
                            row = await cursor.fetchone()
                            if row is not None and row[0] == "Lock":
                                break
                            await asyncio.sleep(0.01)
                    assert not writer.done()
                    assert await repository.get("mkt.backfill_runs", request.source_backfill_id) is not None
                except BaseException:
                    writer.cancel()
                    await asyncio.gather(writer, return_exceptions=True)
                    raise
            await writer
            stored = await repository.get("mkt.backfill_runs", request.source_backfill_id)
            if operation == "DELETE":
                assert stored is None
            else:
                assert stored is not None and stored["reason"] == "actual concurrent update"
        finally:
            await connection.close()
            await monitor.close()


@pytest.mark.integration
@pytest.mark.postgres
async def test_absent_recovery_marker_fences_actual_document_insertion(
    postgres_template: PostgresTemplate,
) -> None:
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repository:
        request = witness_request()
        application = new_id("recovery-writer")
        writer_repository = PostgresRepository(
            make_conninfo(repository.database_url, application_name=application)
        )
        await writer_repository.open()
        monitor = await psycopg.AsyncConnection.connect(repository.database_url, autocommit=True)

        async def insert_marker() -> None:
            async with writer_repository.transaction():
                await writer_repository.put(
                    "ops.scheduler_checkpoints",
                    request.change_marker_key,
                    marker(request.change_marker_key),
                    immutable=True,
                )

        try:
            async with repository.reaction_recovery_witnesses(request) as witnesses:
                assert witnesses[("ops.scheduler_checkpoints", request.change_marker_key)] is None
                writer = asyncio.create_task(insert_marker())
                try:
                    async with asyncio.timeout(5):
                        while True:
                            cursor = await monitor.execute(
                                "SELECT wait_event_type FROM pg_stat_activity WHERE application_name=%s",
                                (application,),
                            )
                            rows = await cursor.fetchall()
                            if any(row[0] == "Lock" for row in rows):
                                break
                            await asyncio.sleep(0.01)
                    assert not writer.done()
                    assert (
                        await repository.get("ops.scheduler_checkpoints", request.change_marker_key) is None
                    )
                except BaseException:
                    writer.cancel()
                    await asyncio.gather(writer, return_exceptions=True)
                    raise
            await writer
            assert await repository.get("ops.scheduler_checkpoints", request.change_marker_key) == marker(
                request.change_marker_key
            )
        finally:
            await writer_repository.close()
            await monitor.close()

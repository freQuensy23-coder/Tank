import asyncio
import os
from contextlib import suppress

import httpx
import pytest
from test_x_diagnostics import diagnostics, multipart, notice_for, telegram_settings
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryDeferred, DeliveryError, DeliveryService
from tank.storage import MemoryRepository, PostgresRepository


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("interruption", ["cancel", "persistence"])
async def test_prior_document_receipt_commits_before_later_upload(
    postgres_template, wire_server, monkeypatch, interruption
):
    origin, state = wire_server
    blocked, release = asyncio.Event(), asyncio.Event()

    class BlockingTransport(LoopbackTelegramTransport):
        uploads = 0

        async def handle_async_request(self, request):
            if request.url.path.endswith("sendDocument"):
                self.uploads += 1
                if self.uploads == 2:
                    blocked.set()
                    await release.wait()
            return await super().handle_async_request(request)

    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = await notice_for(repo, diagnostics("independently committed body"))
        payload = {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        observer = await PostgresRepository(repo.database_url).open()
        original_put = repo.put

        async def put(collection, key, record, **kwargs):
            if (
                interruption == "persistence"
                and collection == "out.deliveries"
                and record.get("status") == "SENT"
                and record.get("filename") == notice["document_manifest"][1]["filename"]
            ):
                raise RuntimeError("later receipt persistence failed")
            return await original_put(collection, key, record, **kwargs)

        monkeypatch.setattr(repo, "put", put)
        monkeypatch.setattr("tank.delivery.DELIVERY_LOCK_WAIT_S", 0.05)
        producer = None
        try:
            async with httpx.AsyncClient(transport=BlockingTransport(origin)) as http:
                producer = asyncio.create_task(
                    DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
                )
                await asyncio.wait_for(blocked.wait(), 3)
                committed = await observer.list("out.deliveries", filters={"status": "SENT"})
                parent = [row for row in committed if row.get("method") != "sendDocument"]
                prior = [row for row in committed if row.get("method") == "sendDocument"]
                assert len(parent) == len(prior) == 1
                assert prior[0]["filename"] == notice["document_manifest"][0]["filename"]
                assert prior[0]["request_reply_to_message_ref"] == parent[0]["message_ref"]
                assert not producer.done()
                before = await observer.list("out.deliveries")
                with pytest.raises(DeliveryDeferred):
                    await DeliveryService(observer, telegram_settings(), http=http).deliver(payload)
                assert await observer.list("out.deliveries") == before
                assert len(state["telegram"]) == 2
                if interruption == "cancel":
                    producer.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await producer
                else:
                    release.set()
                    with pytest.raises(RuntimeError, match="later receipt persistence failed"):
                        await producer
            assert producer.done()
            assert await observer.list("out.deliveries", filters={"status": "SENT"}) == committed
            monkeypatch.setattr(repo, "put", original_put)
            await repo.close()
            await repo.open()
            async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
                async with observer.session_lock("documents:" + notice["notice_id"]):
                    pass
                await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
            final = await observer.list("out.deliveries", filters={"status": "SENT"})
            assert all(row in final for row in committed)
            documents = [row for row in final if row.get("method") == "sendDocument"]
            assert len(documents) == len(notice["document_manifest"])
            assert {row["filename"]: row["attempt"] for row in documents} == {
                item["filename"]: 2 if index == 1 else 1
                for index, item in enumerate(notice["document_manifest"])
            }
            uploads = [
                multipart(httpx.Request("POST", origin, headers=headers, content=body))["document"]
                for path, headers, body in state["telegram"]
                if path.endswith("sendDocument")
            ]
            assert sum(name == prior[0]["filename"] for name, _ in uploads) == 1
            assert sum(path.endswith("sendMessage") for path, _, _ in state["telegram"]) == 1
        finally:
            release.set()
            if producer is not None and not producer.done():
                producer.cancel()
                with suppress(asyncio.CancelledError):
                    await producer
            await observer.close()


async def test_memory_session_lock_is_reentrant_and_commits_outside_batch():
    repo = MemoryRepository()
    async with repo.session_lock("documents:notice:one"):
        async with repo.session_lock("documents:notice:one"):
            await repo.put("ops.scheduler_checkpoints", "one", {"state": {"committed": True}})
        with pytest.raises(RuntimeError):
            async with repo.transaction():
                async with repo.session_lock("documents:notice:two"):
                    pass
    assert await repo.get("ops.scheduler_checkpoints", "one") == {"state": {"committed": True}}
    assert not repo._locks


async def test_document_session_exclusion_preserves_five_attempts_and_later_progress():
    repo = MemoryRepository()
    notice = await notice_for(repo, diagnostics("bounded retry body"))
    first = notice["document_manifest"][0]["filename"]
    uploads = []

    def send(request):
        if request.url.path.endswith("sendDocument"):
            filename, _ = multipart(request)["document"]
            uploads.append(filename)
            if filename == first:
                return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 1}})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(uploads) + 1}})

    payload = {"notice_id": notice["notice_id"]}
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(repo, telegram_settings(), http=http)
        for _ in range(4):
            with pytest.raises(DeliveryDeferred):
                await service.deliver(payload)
        with pytest.raises(DeliveryError, match="exhausted five attempts"):
            await service.deliver(payload)
        sent = await repo.list("out.deliveries", filters={"status": "SENT"})
        prior_uploads = uploads.copy()
        with pytest.raises(DeliveryError, match="exhausted five attempts"):
            await service.deliver(payload)
    assert uploads == prior_uploads
    assert uploads.count(first) == 5
    assert {row["filename"] for row in sent if row.get("method") == "sendDocument"} == {
        item["filename"] for item in notice["document_manifest"][1:]
    }
    failed = await repo.list("out.deliveries", filters={"status": "FAILED", "filename": first})
    assert [row["attempt"] for row in failed] == [1, 2, 3, 4, 5]
    assert [row["terminal"] for row in failed] == [False, False, False, False, True]


@pytest.mark.integration
@pytest.mark.postgres
async def test_session_lock_reentrancy_transaction_refusal_and_legacy_exclusion(postgres_template):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        other = await PostgresRepository(repo.database_url).open()
        try:
            async with repo.transaction():
                with pytest.raises(RuntimeError, match="outside an application transaction"):
                    async with repo.session_lock("documents:legacy"):
                        pass
            async with repo.session_lock("documents:legacy"):
                async with repo.session_lock("documents:legacy"):
                    with pytest.raises(TimeoutError):
                        async with asyncio.timeout(0.05), other.lock("documents:legacy"):
                            pytest.fail("legacy transaction lock overlapped session owner")
            async with other.lock("documents:legacy"):
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.05), repo.session_lock("documents:legacy"):
                        pytest.fail("session lock overlapped legacy transaction owner")
            async with repo.session_lock("documents:legacy"):
                pass
        finally:
            await other.close()


@pytest.mark.integration
@pytest.mark.postgres
async def test_session_owners_commit_at_natural_pool_capacity(postgres_template):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        observer = await PostgresRepository(repo.database_url).open()
        capacity = repo._pool.max_size
        acquired = asyncio.Barrier(capacity)
        committed = asyncio.Barrier(capacity + 1)
        release = asyncio.Event()
        records = {
            str(index): {
                "checkpoint_id": str(index),
                "at": "2026-01-01T00:00:00Z",
                "state": {"committed": True},
            }
            for index in range(capacity)
        }

        async def owner(key, record):
            async with repo.session_lock("documents:capacity:" + key):
                await acquired.wait()
                await repo.put("ops.scheduler_checkpoints", key, record)
                async with repo.session_lock("documents:sibling:" + key):
                    assert await repo.get("ops.scheduler_checkpoints", key) == record
                    await committed.wait()
                    await release.wait()

        tasks = [asyncio.create_task(owner(key, record)) for key, record in records.items()]
        try:
            async with asyncio.timeout(3):
                await committed.wait()
                for key, record in records.items():
                    assert await observer.get("ops.scheduler_checkpoints", key) == record
            release.set()
            await asyncio.gather(*tasks)
            assert not repo._session_locks.get()
            assert repo._pool.get_stats()["pool_available"] == capacity
            for key in records:
                async with observer.lock("documents:capacity:" + key):
                    pass
                async with observer.lock("documents:sibling:" + key):
                    pass
            await repo.close()
            await repo.open()
            for key, record in records.items():
                assert await repo.get("ops.scheduler_checkpoints", key) == record
        finally:
            release.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await observer.close()


@pytest.mark.integration
@pytest.mark.postgres
async def test_inherited_session_context_does_not_share_owner_transaction(postgres_template):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        record = {
            "checkpoint_id": "isolation",
            "at": "2026-01-01T00:00:00Z",
            "state": {"committed": True},
        }

        async def independent_reader():
            async with repo.session_lock("documents:child"):
                return await repo.get("ops.scheduler_checkpoints", "isolation")

        async with repo.session_lock("documents:parent"):
            async with repo.transaction():
                await repo.put("ops.scheduler_checkpoints", "isolation", record)
                child = asyncio.create_task(independent_reader())
                assert await asyncio.wait_for(child, 3) is None
                with pytest.raises(ValueError, match="rollback savepoint"):
                    async with repo.transaction():
                        await repo.put(
                            "ops.scheduler_checkpoints",
                            "rolled-back",
                            {**record, "checkpoint_id": "rolled-back"},
                        )
                        raise ValueError("rollback savepoint")
                assert await repo.get("ops.scheduler_checkpoints", "rolled-back") is None
            assert await asyncio.wait_for(asyncio.create_task(independent_reader()), 3) == record
        assert await repo.get("ops.scheduler_checkpoints", "isolation") == record

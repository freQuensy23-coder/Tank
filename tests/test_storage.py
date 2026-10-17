import asyncio
import os
from datetime import timedelta

import pytest
import pytest_asyncio

from tank.storage import ImmutableRecordError, MemoryRepository, PostgresRepository
from tank.util import iso, utcnow, new_id


@pytest_asyncio.fixture(params=['memory', 'postgres'])
async def repo(request):
    if request.param == 'postgres':
        url = os.getenv('TANK_TEST_DATABASE_URL')
        if not url:
            pytest.skip('Real PostgreSQL requires TANK_TEST_DATABASE_URL pointing at an isolated test database')
        result = PostgresRepository(url)
        await result.open()
        await result.migrate()
        # A dedicated test database is required; never use the production database URL.
        async with result.transaction():
            await result._conn().execute('TRUNCATE ops.jobs, reg.tokens, evt.events, out.alerts')
    else:
        result = MemoryRepository()
        await result.open()
    yield result
    await result.close()


async def test_nested_transaction_rollback_and_exact_filters(repo):
    async with repo.transaction():
        await repo.put('reg.tokens', 'a', {'token_id': 'a', 'symbol': 'A', 'null': None})
        try:
            async with repo.transaction():
                await repo.put('reg.tokens', 'b', {'token_id': 'b'})
                raise ValueError('rollback nested')
        except ValueError:
            pass
        assert await repo.get('reg.tokens', 'a')
        assert await repo.get('reg.tokens', 'b') is None
    assert len(await repo.list('reg.tokens', filters={'null': None})) == 1
    assert await repo.list('reg.tokens', filters={'missing': None}) == []
    with pytest.raises(ValueError):
        async with repo.transaction():
            await repo.delete('reg.tokens', 'a')
            raise ValueError('rollback outer')
    assert await repo.get('reg.tokens', 'a')


async def test_append_only_and_immutable(repo):
    record = {'alert_id': 'a', 'version': 1, 'card_text': 'original'}
    await repo.put('out.alerts', 'a:1', record)
    assert await repo.put('out.alerts', 'a:1', record) == record
    with pytest.raises(ImmutableRecordError):
        await repo.put('out.alerts', 'a:1', {**record, 'card_text': 'changed'})
    with pytest.raises(ImmutableRecordError):
        await repo.delete('out.alerts', 'a:1')
    await repo.put('reg.tokens', 'sealed', {'token_id': 'sealed'}, immutable=True)
    with pytest.raises(ImmutableRecordError):
        await repo.delete('reg.tokens', 'sealed')


async def test_queue_namespaced_idempotency_concurrency_priority(repo):
    rows = await asyncio.gather(*[repo.enqueue('FILTER_ITEM', {'item_id': 'i'}, 'i') for _ in range(10)])
    assert len({r['job_id'] for r in rows}) == 1
    routed = await repo.enqueue('ROUTE_ITEM', {'item_id': 'i'}, 'i', priority=0)
    assert routed['job_id'] != rows[0]['job_id']
    claimed = await asyncio.gather(*[repo.claim_job() for _ in range(5)])
    jobs = [j for j in claimed if j]
    assert len(jobs) == 2
    assert len({j['job_id'] for j in jobs}) == 2
    await repo.finish_job(routed['job_id'])
    assert (await repo.enqueue('ROUTE_ITEM', {}, 'i'))['status'] == 'DONE'


async def test_queue_expiry_retry_recovery(repo):
    expired = await repo.enqueue('X', {}, 'expired', expires_at=iso(utcnow() - timedelta(seconds=1)))
    delayed = await repo.enqueue('X', {}, 'delayed', run_after=iso(utcnow() + timedelta(hours=1)))
    retry = await repo.enqueue('X', {}, 'retry', max_attempts=2)
    assert (await repo.claim_job())['job_id'] == retry['job_id']
    await repo.fail_job(retry['job_id'], 'temporary', retry_after=0)
    assert (await repo.claim_job())['attempts'] == 2
    await repo.recover_jobs()
    assert (await repo.get('ops.jobs', retry['job_id']))['status'] == 'FAILED'
    assert (await repo.get('ops.jobs', expired['job_id']))['status'] == 'EXPIRED'
    assert (await repo.get('ops.jobs', delayed['job_id']))['status'] == 'QUEUED'
    assert await repo.claim_job() is None


async def test_event_lock_serializes_increment(repo):
    await repo.put('evt.events', 'event', {'event_id': 'event', 'counter': 0})
    async def increment():
        async with repo.lock('event:event'):
            row = await repo.get('evt.events', 'event')
            await asyncio.sleep(0)
            row['counter'] += 1
            await repo.put('evt.events', 'event', row)
    await asyncio.gather(*[increment() for _ in range(10)])
    assert (await repo.get('evt.events', 'event'))['counter'] == 10


async def test_postgres_reopen_keeps_jobs_and_typed_columns(repo):
    if not isinstance(repo, PostgresRepository):
        pytest.skip('PostgreSQL restart/persistence requires the real PostgreSQL repository')
    job = await repo.enqueue('DELIVER', {'alert_id': 'a'}, new_id('idem'))
    await repo.claim_job()
    await repo.close()
    await repo.open()
    await repo.recover_jobs()
    assert (await repo.get('ops.jobs', job['job_id']))['status'] == 'QUEUED'
    async with repo.transaction():
        rows = await (await repo._conn().execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('reg','ingest','evt','mkt','ana','out','ops','scout','qa')")).fetchall()
    assert len(rows) >= 45

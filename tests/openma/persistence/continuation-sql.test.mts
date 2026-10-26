import assert from 'node:assert/strict';
import { requireDisposablePostgresDsn } from '../../../scripts/openma-persistence/database.mts';
import { randomUUID } from 'node:crypto';
import { test, type TestContext } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import postgres from 'postgres';
import { createPostgresSqlClient } from '../../../vendor/openma/packages/sql-client/src/adapters/postgres.ts';
import { loadSessionContinuationSnapshot } from '../../../vendor/openma/packages/session-runtime-sql/src/continuation.ts';
import { ensureSessionExecutionCoordinatorSchema, SqlSessionExecutionStore } from '../../../vendor/openma/packages/session-runtime-sql/src/coordination.ts';
import { ContinuationProtocolError } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';
import { SqlSessionEventStore } from '../../../vendor/openma/packages/session-event-store-sql/src/index.ts';
import type { Session, SentSessionEvent } from '../../../vendor/openma/packages/managed-agents-domain/src/sessions/index.ts';
import type { SqlClient } from '../../../vendor/openma/packages/sql-client/src/ports.ts';

interface Fixture {
  sql: SqlClient;
  store: SqlSessionExecutionStore;
  scope: { workspaceId: string; sessionId: string };
  session: Session;
}
async function fixture(t: TestContext): Promise<Fixture> {
  const dsn = requireDisposablePostgresDsn(process.env.TANK_TEST_DATABASE_URL);
  const schema = 'continuation_' + randomUUID().replaceAll('-', '');
  const connection = postgres(dsn);
  const configured = new URL(dsn);
  configured.searchParams.set('search_path', schema);
  configured.searchParams.set('application_name', schema);
  configured.searchParams.set('idle_timeout', '0.1');
  t.after(async (): Promise<void> => {
    try {
      const deadline = Date.now() + 3000;
      while (true) {
        const rows = await connection.unsafe<{ connections: number }[]>(
          'SELECT COUNT(*)::int AS connections FROM pg_stat_activity WHERE application_name=$1', [schema]);
        assert.ok(rows[0] !== undefined);
        if (rows[0].connections === 0) break;
        assert.ok(Date.now() < deadline, 'Owned factory connections did not drain');
        await delay(25);
      }
      await connection.unsafe('DROP SCHEMA IF EXISTS ' + schema + ' CASCADE');
    } finally { await connection.end(); }
  });
  await connection.unsafe('CREATE SCHEMA ' + schema);
  const sql = await createPostgresSqlClient(configured.toString());
  await sql.exec(`
    CREATE TABLE managed_sessions (workspace_id TEXT NOT NULL, id TEXT NOT NULL, document TEXT NOT NULL, revision BIGINT NOT NULL, agent_id TEXT, agent_version BIGINT, environment_id TEXT, deployment_id TEXT, status TEXT, created_at BIGINT, updated_at BIGINT, archived_at BIGINT, PRIMARY KEY(workspace_id,id));
    CREATE TABLE managed_session_initial_events (workspace_id TEXT, session_id TEXT, sequence BIGINT, document TEXT);
    CREATE TABLE managed_session_events (workspace_id TEXT NOT NULL, session_id TEXT NOT NULL, thread_id TEXT, id TEXT NOT NULL, type TEXT NOT NULL, document TEXT NOT NULL, processed_at BIGINT NOT NULL, PRIMARY KEY(workspace_id,session_id,id));
  `);
  await ensureSessionExecutionCoordinatorSchema(sql);
  const scope = { workspaceId: 'scope', sessionId: 'session' };
  const session: Session = {
    id: scope.sessionId,
    agent: { id: 'agent', name: 'Persistence qualification', description: null, mcpServers: [], model: { id: 'deterministic' }, multiagent: null, skills: [], system: null, tools: [], version: 1 },
    archivedAt: null, budget: null, createdAt: '2026-10-26T00:00:00Z', environmentId: 'fixture-environment', metadata: {}, outcomeEvaluations: [], resources: [], stats: {}, status: 'idle', title: null,
    updatedAt: '2026-10-26T00:00:00Z', usage: {}, vaultIds: [],
  };
  await sql.prepare('INSERT INTO managed_sessions(workspace_id,id,document,revision,status,created_at,updated_at) VALUES(?,?,?,1,?,?,?)')
    .bind(scope.workspaceId, scope.sessionId, JSON.stringify(session), session.status, Date.parse(session.createdAt), Date.parse(session.updatedAt)).run();
  const store = new SqlSessionExecutionStore(sql, { callbackContinuations: true, serializeSessionClaims: true });
  const admitted = await store.admit({ execution: { ...scope, id: 'execution', laneId: 'sthr_primary', admittedAt: '2026-10-26T00:00:00Z',
    events: [{ id: 'input', type: 'user.message', content: [{ type: 'text', text: 'original' }] }] }, policy: { maxAttempts: 3, timeoutMs: 3600000 } });
  assert.equal(admitted.type, 'admitted');
  return { sql, store, scope, session };
}

async function persisted(sql: SqlClient): Promise<unknown[]> {
  const rows = await sql.prepare('SELECT id,state,revision::text AS revision,attempt_count::text AS attempt_count,events_json,interrupt_requested_at_ms FROM managed_session_executions ORDER BY id').all<unknown>();
  assert.ok(rows.results !== undefined);
  return rows.results;
}

test('actual PG continuation load and claim preserve declared input, numeric strings and no-interrupt absence', async (t: TestContext): Promise<void> => {
  const f = await fixture(t);
  const snapshot = await loadSessionContinuationSnapshot(f.sql, f.scope);
  assert.equal(snapshot.revision, 1);
  assert.equal(snapshot.executions[0].revision, 1);
  assert.equal(snapshot.executions[0].attemptCount, 0);
  assert.equal(snapshot.executions[0].interruptRequestedAt, null);
  const claimed = await f.store.claim({ ...f.scope, ownerId: 'worker', attemptId: 'attempt', claimedAt: '2026-10-26T00:00:01Z', leaseTtlMs: 10000 });
  assert.equal(claimed.type, 'claimed');
  if (claimed.type !== 'claimed') throw new Error('Expected actual execution claim');
  assert.equal(claimed.execution.attemptCount, 1);
  assert.equal(claimed.execution.events[0].id, 'input');
});

for (const callbackContinuations of [true, false]) for (const document of ['null', '{}', '[]', '[{"id":"input","type":"user.custom_tool_result","customToolUseId":"call","isError":1}]',
  '[{"id":"input","type":"user.interrupt","id":"other"}]', '[{"id":"input","type":"user.message","content":[{"type":"text","text":"FAKE_SECRET_VALUE","text":"hidden"}]}]']) {
  test('actual PG refuses malformed continuation before claim or durable mutation (' + callbackContinuations + '): ' + document.slice(0, 22), async (t: TestContext): Promise<void> => {
    const f = await fixture(t);
    await f.sql.prepare('UPDATE managed_session_executions SET events_json=? WHERE id=?').bind(document, 'execution').run();
    const before = await persisted(f.sql);
    await assert.rejects(loadSessionContinuationSnapshot(f.sql, f.scope), ContinuationProtocolError);
    const store = new SqlSessionExecutionStore(f.sql, { callbackContinuations, serializeSessionClaims: true });
    await assert.rejects(store.claim({ ...f.scope, ownerId: 'worker', attemptId: 'attempt', claimedAt: '2026-10-26T00:00:01Z', leaseTtlMs: 10000 }),
      (error: unknown): boolean => {
        assert.ok(error instanceof ContinuationProtocolError);
        assert.equal(error.message, 'Invalid persisted continuation input');
        return true;
      });
    assert.deepEqual(await persisted(f.sql), before);
  });
}

test('actual PG refuses unsafe integer revision and ambiguous recovery without rewriting evidence', async (t: TestContext): Promise<void> => {
  const f = await fixture(t);
  await f.sql.prepare('UPDATE managed_session_executions SET revision=? WHERE id=?').bind('9007199254740993', 'execution').run();
  const before = await persisted(f.sql);
  await assert.rejects(loadSessionContinuationSnapshot(f.sql, f.scope), /Invalid persisted continuation integer/);
  assert.deepEqual(await persisted(f.sql), before);
  await f.sql.prepare('UPDATE managed_session_executions SET revision=1 WHERE id=?').bind('execution').run();
  await f.sql.prepare('INSERT INTO managed_session_image_recoveries(workspace_id,execution_id,after_event_id) VALUES(?,?,?)')
    .bind(f.scope.workspaceId, 'execution', '{"afterEventId":"first","afterEventId":"second","interruptRequestedAt":null}').run();
  const raw = await f.sql.prepare('SELECT after_event_id FROM managed_session_image_recoveries').all<unknown>();
  await assert.rejects(loadSessionContinuationSnapshot(f.sql, f.scope), ContinuationProtocolError);
  assert.deepEqual(await f.sql.prepare('SELECT after_event_id FROM managed_session_image_recoveries').all<unknown>(), raw);
});


test('actual PG admission validates constructed values before NaN can serialize to an allowed null', async (t: TestContext): Promise<void> => {
  const f = await fixture(t);
  const event: SentSessionEvent = { id: 'invalid', type: 'user.custom_tool_result', customToolUseId: 'call' };
  Object.defineProperty(event, 'isError', { value: NaN, enumerable: true });
  const before = await persisted(f.sql);
  await assert.rejects(f.store.admit({ execution: { ...f.scope, id: 'invalid', admittedAt: '2026-10-26T00:00:01Z', events: [event] }, policy: { maxAttempts: 3, timeoutMs: 3600000 } }), ContinuationProtocolError);
  assert.deepEqual(await persisted(f.sql), before);
  let calls = 0;
  const eventStore = new SqlSessionEventStore(f.sql, { executionOutbox: async (): Promise<boolean> => { calls++; return true; }, executionPolicy: { maxAttempts: 3, timeoutMs: 3600000 } });
  await assert.rejects(eventStore.append({ ...f.scope, expectedRevision: 1, events: [event], nextSession: f.session }), ContinuationProtocolError);
  assert.equal(calls, 0);
  assert.deepEqual(await persisted(f.sql), before);
  const journal = await f.sql.prepare('SELECT document FROM managed_session_events').all<unknown>();
  assert.ok(journal.results !== undefined);
  assert.equal(journal.results.length, 0);
});

test('actual PG event outbox captures validated input before an asynchronous policy lookup', async (t: TestContext): Promise<void> => {
  const f = await fixture(t);
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const event: SentSessionEvent = { id: 'new-input', type: 'user.message', processedAt: '2026-10-26T00:00:01Z', content: [{ type: 'text', text: 'captured original' }] };
  const eventStore = new SqlSessionEventStore(f.sql, { executionOutbox: async (): Promise<boolean> => { entered.resolve(); await release.promise; return true; }, executionPolicy: { maxAttempts: 3, timeoutMs: 3600000 } });
  const append = eventStore.append({ ...f.scope, expectedRevision: 1, events: [event], nextSession: { ...f.session, updatedAt: '2026-10-26T00:00:01Z' } });
  await entered.promise;
  event.content[0] = { type: 'text', text: 'later caller mutation' };
  release.resolve();
  assert.equal((await append).type, 'appended');
  const observed = await loadSessionContinuationSnapshot(f.sql, f.scope);
  const execution = observed.executions.find(row => row.id === 'new-input');
  assert.ok(execution !== undefined);
  const captured = execution.events[0];
  assert.equal(captured.type, 'user.message');
  if (captured.type !== 'user.message') throw new Error('Expected captured message');
  assert.deepEqual(captured.content, [{ type: 'text', text: 'captured original' }]);
  assert.equal(observed.revision, 2);
});

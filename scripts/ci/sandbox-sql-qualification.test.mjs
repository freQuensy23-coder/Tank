import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { createRequire } from 'node:module';
import { PostgresSqlClient } from '../../vendor/openma/packages/sql-client/src/adapters/postgres.ts';
import { createBetterSqlite3SqlClient } from '../../vendor/openma/packages/sql-client/src/index.ts';
import { ensureSessionExecutionCoordinatorSchema, SqlSessionExecutionCoordinator } from '../../vendor/openma/packages/session-runtime-sql/src/coordination.ts';
import { loadSessionContinuationSnapshot } from '../../vendor/openma/packages/session-runtime-sql/src/continuation.ts';
import { planSessionContinuation } from '../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';
import { SqlSessionEventStore } from '../../vendor/openma/packages/session-event-store-sql/src/index.ts';
import { SqlSessionRuntimeProjectionPersistence } from '../../vendor/openma/packages/managed-agents-adapters-sql/src/session-runtime-projection-sql-persistence.ts';

const at = seconds => new Date(Date.UTC(2026, 9, 24, 0, 0, seconds)).toISOString();
const scope = { workspaceId: 'qualification', sessionId: 'session' };
const session = {
  id: scope.sessionId, agent: { id: 'agent', name: 'Qualification', description: null, mcpServers: [], model: { id: 'deterministic' }, multiagent: null, skills: [], system: null, tools: [], version: 1 },
  archivedAt: null, budget: null, createdAt: at(0), environmentId: 'qualification', metadata: {}, outcomeEvaluations: [], resources: [], stats: {}, status: 'idle', title: null, updatedAt: at(0), usage: {}, vaultIds: [],
};
const coordinator = sql => new SqlSessionExecutionCoordinator(sql, { callbackContinuations: true, serializeSessionClaims: true });
const claim = (store, ownerId, seconds = 5) => store.claim({ ...scope, ownerId, attemptId: ownerId + '-attempt', claimedAt: at(seconds), leaseTtlMs: 10000 });
async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'tank-sql-qualification-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const path = join(directory, 'coordinator.sqlite');
  let open = () => createBetterSqlite3SqlClient(path);
  if (process.env.TANK_QUALIFICATION_PG_URL) {
    assert.equal(process.env.GITHUB_ACTIONS, 'true');
    const url = new URL(process.env.TANK_QUALIFICATION_PG_URL);
    assert.equal(url.hostname, '127.0.0.1');
    assert.equal(url.pathname, '/tank_test_qualification');
    const postgres = createRequire(new URL('../../vendor/openma/packages/node-database-postgres/package.json', import.meta.url))('postgres');
    const admin = postgres(url.toString());
    const database = 'tank_test_' + randomBytes(12).toString('hex');
    await admin.unsafe('CREATE DATABASE ' + database);
    await admin.end();
    url.pathname = '/' + database;
    const connections = [];
    t.after(async () => { await Promise.all(connections.map(connection => connection.end())); });
    open = async () => {
      const connection = postgres(url.toString(), { types: { bigint: { to: 20, from: [20], serialize: value => value.toString(), parse: value => Number(value) } } });
      connections.push(connection);
      return new PostgresSqlClient(connection);
    };
  }
  const sql = await open();
  await sql.exec(`
    CREATE TABLE managed_sessions (workspace_id TEXT NOT NULL, id TEXT NOT NULL, document TEXT NOT NULL, revision BIGINT NOT NULL, agent_id TEXT, agent_version BIGINT, environment_id TEXT, deployment_id TEXT, status TEXT, created_at BIGINT, updated_at BIGINT, archived_at BIGINT, PRIMARY KEY (workspace_id,id));
    CREATE TABLE managed_session_initial_events (workspace_id TEXT, session_id TEXT, sequence BIGINT, document TEXT);
    CREATE TABLE managed_session_events (workspace_id TEXT NOT NULL, session_id TEXT NOT NULL, thread_id TEXT, id TEXT NOT NULL, type TEXT NOT NULL, document TEXT NOT NULL, processed_at BIGINT NOT NULL, PRIMARY KEY (workspace_id,session_id,id));
  `);
  await ensureSessionExecutionCoordinatorSchema(sql);
  await sql.prepare('INSERT INTO managed_sessions (workspace_id,id,document,revision,status,created_at,updated_at) VALUES (?,?,?,1,?,?,?)')
    .bind(scope.workspaceId, session.id, JSON.stringify(session), 'idle', Date.parse(at(0)), Date.parse(at(0))).run();
  const events = new SqlSessionEventStore(sql, { executionOutbox: true });
  assert.equal((await events.append({ ...scope, expectedRevision: 1, events: [{ id: 'original', type: 'user.message', content: [{ type: 'text', text: 'original' }], processedAt: at(1) }], nextSession: session })).type, 'appended');
  const store = coordinator(sql);
  const owned = await claim(store, 'first', 2);
  assert.equal(owned.type, 'claimed');
  return { sql, open, store, events, owned, writer: new SqlSessionRuntimeProjectionPersistence(sql, { now: () => new Date(at(3)) }) };
}
async function failBeforeEffects(f, message = 'retained_configuration_changed', extraEvents = []) {
  const failureEvents = [
    { id: 'failed-error', type: 'session.error', error: { type: 'unknown_error', message, retryStatus: 'exhausted' }, processedAt: at(3) },
    ...extraEvents,
    { id: 'failed-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' }, processedAt: at(3) },
  ];
  assert.equal((await f.writer.project({ ...scope, expectedRevision: 2, executionFence: f.owned.fence, events: failureEvents, next: session })).type, 'projected');
  assert.equal((await f.store.settle({ fence: f.owned.fence, settledAt: at(4), outcome: 'failed', failure: message })).type, 'settled');
}

test(`actual ${process.env.TANK_QUALIFICATION_PG_URL ? 'PostgreSQL' : 'SQLite'} coordinator recovers once, fences a concurrent owner and preserves FIFO after durable reopen`, async t => {
  const f = await fixture(t);
  await failBeforeEffects(f);
  assert.equal((await f.events.append({ ...scope, expectedRevision: 3, events: [{ id: 'later', type: 'user.message', content: [{ type: 'text', text: 'later' }], processedAt: at(4) }], nextSession: session })).type, 'appended');
  const reopened = await f.open();
  const store = coordinator(reopened);
  const results = await Promise.all([claim(store, 'second'), claim(coordinator(f.sql), 'competitor')]);
  const winners = results.filter(result => result.type === 'claimed');
  assert.equal(winners.length, 1);
  const recovered = winners[0];
  assert.equal(recovered.execution.id, 'original');
  assert.equal(recovered.execution.attemptCount, 2);
  assert.equal((await claim(store, 'third', 6)).type, 'empty');
  const snapshot = await loadSessionContinuationSnapshot(reopened, scope);
  assert.equal(snapshot.executions.find(row => row.id === 'original').imageRecoveryAfterEventId, 'failed-idle');
  assert.equal(planSessionContinuation(snapshot, 'original').mode, 'model');
  assert.equal(planSessionContinuation(snapshot, 'later').mode, 'blocked');
  assert.deepEqual(snapshot.orderedEvents.map(row => row.event.id), ['original', 'failed-error', 'failed-idle', 'later']);
  assert.equal((await store.settle({ fence: f.owned.fence, settledAt: at(6), outcome: 'completed' })).type, 'lost');
  const output = { id: 'completed-original', type: 'session.status_idle', stopReason: { type: 'end_turn' }, processedAt: at(6) };
  const writer = new SqlSessionRuntimeProjectionPersistence(reopened, { now: () => new Date(at(6)) });
  const command = { ...scope, expectedRevision: 4, executionFence: recovered.fence, events: [output], next: session };
  assert.equal((await writer.project(command)).type, 'projected');
  assert.equal((await writer.project(command)).type, 'projected');
  assert.equal(planSessionContinuation(await loadSessionContinuationSnapshot(reopened, scope), 'original').mode, 'finalize');
  assert.equal((await store.settle({ fence: recovered.fence, settledAt: at(7), outcome: 'completed' })).type, 'settled');
  assert.equal((await claim(store, 'later-owner', 8)).execution.id, 'later');
  const observed = await loadSessionContinuationSnapshot(reopened, scope);
  assert.equal(observed.orderedEvents.filter(row => row.event.id === output.id).length, 1);
  assert.equal(observed.executions.find(row => row.id === 'original').state, 'completed');
});

for (const reason of ['unrelated', 'effects', 'archived', 'interrupted', 'deadline', 'attempt-limit']) {
  test(`actual SQL recovery refuses ${reason} without rewriting failed history`, async t => {
    const f = await fixture(t);
    await failBeforeEffects(f, reason === 'unrelated' ? 'provider unavailable' : undefined,
      reason === 'effects' ? [{ id: 'effect', type: 'agent.message', content: [{ type: 'text', text: 'already emitted' }], processedAt: at(3) }] : []);
    if (reason === 'archived') await f.sql.prepare('UPDATE managed_sessions SET archived_at = ? WHERE id = ?').bind(Date.parse(at(4)), scope.sessionId).run();
    if (reason === 'interrupted') await f.sql.prepare('UPDATE managed_session_executions SET interrupt_requested_at_ms = ? WHERE id = ?').bind(Date.parse(at(4)), 'original').run();
    if (reason === 'deadline') await f.sql.prepare('UPDATE managed_session_executions SET deadline_at_ms = ? WHERE id = ?').bind(Date.parse(at(4)), 'original').run();
    if (reason === 'attempt-limit') await f.sql.prepare('UPDATE managed_session_executions SET max_attempts = attempt_count WHERE id = ?').bind('original').run();
    const before = await loadSessionContinuationSnapshot(f.sql, scope);
    assert.equal((await claim(coordinator(await f.open()), 'replacement')).type, 'empty');
    const after = await loadSessionContinuationSnapshot(f.sql, scope);
    assert.deepEqual(after.orderedEvents, before.orderedEvents);
    assert.equal(after.executions[0].state, 'failed');
    assert.equal(after.executions[0].imageRecoveryAfterEventId, undefined);
  });
}

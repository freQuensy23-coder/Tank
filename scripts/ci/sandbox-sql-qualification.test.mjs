import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes, createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { Readable, Writable } from 'node:stream';
import { RetainedBoxOperations, pinnedBoxLiteProvider } from '../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts';
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

test('actual SQL coordinator recovers once, fences a concurrent owner and preserves FIFO after durable reopen', async t => {
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

async function revision(f) {
  return Number((await f.sql.prepare('SELECT revision FROM managed_sessions WHERE workspace_id = ? AND id = ?').bind(scope.workspaceId, scope.sessionId).first()).revision);
}
async function append(f, events) {
  assert.equal((await f.events.append({ ...scope, expectedRevision: await revision(f), events, nextSession: session })).type, 'appended');
}
async function project(f, owned, events, seconds) {
  const writer = new SqlSessionRuntimeProjectionPersistence(f.sql, { now: () => new Date(at(seconds)) });
  assert.equal((await writer.project({ ...scope, expectedRevision: await revision(f), executionFence: owned.fence, events, next: session })).type, 'projected');
}
const idle = (id, seconds, stopReason = { type: 'end_turn' }) => ({ id, type: 'session.status_idle', stopReason, processedAt: at(seconds) });
const errorEvent = (id, seconds, message) => ({ id, type: 'session.error', error: { type: 'unknown_error', message, retryStatus: 'exhausted' }, processedAt: at(seconds) });
const running = (id, seconds) => ({ id, type: 'session.status_running', processedAt: at(seconds) });
const findOriginal = f => f.store.find({ workspaceId: scope.workspaceId, executionId: 'original' });
async function recoveredAtCap(t) {
  const f = await fixture(t);
  await failBeforeEffects(f);
  await f.sql.prepare('UPDATE managed_session_executions SET max_attempts = 2 WHERE id = ?').bind('original').run();
  const recovered = await claim(f.store, 'recovered', 5);
  assert.equal(recovered.type, 'claimed');
  assert.equal(recovered.execution.attemptCount, 2);
  assert.equal(recovered.execution.maxAttempts, 2);
  return { f, recovered, deadlineAt: recovered.execution.deadlineAt };
}

test('actual SQL interrupted recovered execution stays cancelled and fresh input works', async t => {
  const { f, recovered, deadlineAt } = await recoveredAtCap(t);
  assert.equal((await f.store.requestInterrupt({ ...scope, requestedAt: at(6) })).type, 'requested');
  await append(f, [{ id: 'interrupt', type: 'user.interrupt', processedAt: at(6) }]);
  await project(f, recovered, [idle('cancelled-idle', 7, { type: 'interrupted' })], 7);
  assert.equal((await f.store.settle({ fence: recovered.fence, settledAt: at(7), outcome: 'cancelled' })).type, 'settled');
  const cancelled = await findOriginal(f);
  assert.equal(cancelled.state, 'cancelled');
  assert.equal(cancelled.deadlineAt, deadlineAt);
  assert.equal(cancelled.interruptRequestedAt, at(6));
  await append(f, [{ id: 'fresh', type: 'user.message', content: [{ type: 'text', text: 'valid fresh input' }], processedAt: at(8) }]);
  const fresh = await claim(coordinator(await f.open()), 'fresh-owner', 9);
  assert.equal(fresh.type, 'claimed');
  assert.equal(fresh.execution.id, 'fresh');
  const snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  assert.equal(planSessionContinuation(snapshot, 'original').mode, 'blocked');
  assert.equal(snapshot.executions.find(row => row.id === 'original').imageRecoveryInterruptRequestedAt, null);
  assert.equal(planSessionContinuation(snapshot, 'fresh').mode, 'model');
  await project(f, fresh, [idle('fresh-complete', 10)], 10);
  assert.equal((await f.store.settle({ fence: fresh.fence, settledAt: at(11), outcome: 'completed' })).type, 'settled');
  assert.equal((await claim(f.store, 'no-revival', 12)).type, 'empty');
});

test('actual SQL temporary preflight retries preserve original deadline and exactly-once result', async t => {
  const { f, recovered, deadlineAt } = await recoveredAtCap(t);
  await project(f, recovered, [running('retry-running', 6), errorEvent('retry-error', 6, 'retained_upgrade_transfer_failed:temporary'), idle('retry-idle', 6, { type: 'retries_exhausted' })], 6);
  const settled = await f.store.settle({ fence: recovered.fence, settledAt: at(7), outcome: 'failed', failure: 'retained_upgrade_transfer_failed:temporary' });
  assert.equal(settled.type, 'settled');
  assert.equal(settled.execution.state, 'failed');
  assert.equal(settled.execution.attemptCount, 1, 'no-effects preflight did not refund attempt');
  assert.equal(settled.execution.deadlineAt, deadlineAt);
  await append(f, [{ id: 'later', type: 'user.message', content: [{ type: 'text', text: 'later' }], processedAt: at(7) }]);
  const retried = await claim(coordinator(await f.open()), 'retry-after-restart', 8);
  assert.equal(retried.type, 'claimed');
  assert.equal(retried.execution.id, 'original');
  assert.equal(retried.execution.attemptCount, 2);
  assert.equal(retried.execution.deadlineAt, deadlineAt);
  const snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  assert.equal(snapshot.executions.find(row => row.id === 'original').imageRecoveryRetryAfterEventId, 'retry-idle');
  assert.equal(planSessionContinuation(snapshot, 'original').mode, 'model');
  assert.equal(planSessionContinuation(snapshot, 'later').mode, 'blocked');
  await project(f, retried, [{ id: 'one-effect', type: 'agent.message', content: [{ type: 'text', text: 'actual committed model result' }], processedAt: at(9) }, idle('retry-complete', 9)], 9);
  assert.equal((await f.store.settle({ fence: retried.fence, settledAt: at(10), outcome: 'completed' })).type, 'settled');
  assert.equal((await claim(f.store, 'next-owner', 11)).execution.id, 'later');
  assert.equal((await loadSessionContinuationSnapshot(f.sql, scope)).orderedEvents.filter(row => row.event.id === 'one-effect').length, 1);
});

test('actual SQL expired pre-model owner at attempt cap refunds and reclaims without deadline extension', async t => {
  const { f, recovered, deadlineAt } = await recoveredAtCap(t);
  await project(f, recovered, [running('owner-before-effects', 6)], 6);
  const replacement = await claim(coordinator(await f.open()), 'after-death', 16);
  assert.equal(replacement.type, 'claimed');
  assert.equal(replacement.execution.id, 'original');
  assert.equal(replacement.execution.attemptCount, 2);
  assert.equal(replacement.execution.deadlineAt, deadlineAt);
  assert.notEqual(replacement.fence.generation, recovered.fence.generation);
  assert.equal(planSessionContinuation(await loadSessionContinuationSnapshot(f.sql, scope), 'original').mode, 'model');
  assert.equal((await f.store.settle({ fence: recovered.fence, settledAt: at(17), outcome: 'completed' })).type, 'lost');
});

for (const reason of ['effects', 'interrupt', 'deadline']) {
  test(`actual SQL expired recovery never refunds or revives ${reason}`, async t => {
    const { f, recovered } = await recoveredAtCap(t);
    await project(f, recovered, reason === 'effects'
      ? [{ id: 'already-effected', type: 'agent.message', content: [{ type: 'text', text: 'effect exists' }], processedAt: at(6) }]
      : [running('no-effects', 6)], 6);
    if (reason === 'interrupt') assert.equal((await f.store.requestInterrupt({ ...scope, requestedAt: at(7) })).type, 'requested');
    if (reason === 'deadline') await f.sql.prepare('UPDATE managed_session_executions SET deadline_at_ms = ? WHERE id = ?').bind(Date.parse(at(14)), 'original').run();
    const before = await findOriginal(f);
    assert.equal((await claim(coordinator(await f.open()), 'never-revive', 16)).type, 'empty');
    const after = await findOriginal(f);
    assert.equal(after.attemptCount, 2, 'unsafe recovery attempt was refunded');
    assert.equal(after.deadlineAt, before.deadlineAt, 'deadline was extended');
    assert.equal(after.interruptRequestedAt, before.interruptRequestedAt);
    assert.equal(after.state, 'failed');
    assert.equal((await claim(f.store, 'still-no-revival', 17)).type, 'empty');
  });
}

test('actual SQL transfer timeout producer settles and retries original input after durable reopen', async t => {
  const { f, recovered, deadlineAt } = await recoveredAtCap(t);
  let killed = false;
  let completed;
  const operations = new RetainedBoxOperations(async operation => operation({
    async spawnDuplexProcess() {
      const child = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'pipe' });
      completed = new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', (code, signal) => resolve({ code, signal })); });
      t.after(() => child.kill('SIGKILL'));
      return { stdin: Writable.toWeb(child.stdin), stdout: Readable.toWeb(child.stdout), stderr: Readable.toWeb(child.stderr), exited: completed,
        async kill(signal) { killed = child.kill(signal); } };
    },
  }));
  let timeout;
  await assert.rejects(operations.exec('python3', [], undefined, { timeoutSecs: 0.03 }), error => { timeout = error; return error.message === 'retained_exec_timeout'; });
  assert.equal(killed, true);
  assert.equal((await completed).signal, 'SIGKILL');
  const retainedScope = { ...scope, environmentId: 'qualification', workId: 'tank-retained-session-v1' };
  const source = { provider: 'litebox', runtimeId: 'actual-source-boundary' };
  const sourceName = 'oma-' + createHash('sha256').update('litebox:' + scope.sessionId).digest('hex').slice(0, 32);
  const provider = pinnedBoxLiteProvider({
    module: {
      createBoxLiteProvider() { return { async resume() { return { async spawnDuplexProcess() { throw timeout; }, async suspend() {} }; }, async create() { assert.fail('timeout must precede replacement allocation'); } }; },
      createBoxLiteManagedRuntime() { return { sandbox: { async reap() { assert.fail('timeout must retain source'); } } }; },
    },
    client: { async getInfo(id) { return id === source.runtimeId ? { id, name: sourceName, state: { running: false } } : null; } },
    allocation: { image: 'candidate' }, leaseTtlMs: 30000, isCurrent: async () => true,
  });
  const signal = new AbortController().signal;
  const target = await provider.replacementIntent(source, {}, retainedScope, {}, signal);
  let diagnostic;
  await assert.rejects(provider.replace({ source, target }, retainedScope, {}, signal, async () => {}), error => {
    diagnostic = error.message;
    return diagnostic === 'retained_upgrade_transfer_failed:guest_timeout';
  });
  await project(f, recovered, [running('timeout-running', 6), errorEvent('timeout-error', 6, diagnostic), idle('timeout-idle', 6, { type: 'retries_exhausted' })], 6);
  const settled = await f.store.settle({ fence: recovered.fence, settledAt: at(7), outcome: 'failed', failure: diagnostic });
  assert.equal(settled.type, 'settled');
  assert.equal(settled.execution.attemptCount, 1);
  assert.equal(settled.execution.deadlineAt, deadlineAt);
  await append(f, [{ id: 'after-timeout', type: 'user.message', content: [{ type: 'text', text: 'later input' }], processedAt: at(7) }]);
  const reopened = coordinator(await f.open());
  const retried = await claim(reopened, 'restored-provider', 8);
  assert.equal(retried.type, 'claimed');
  assert.equal(retried.execution.id, 'original');
  assert.equal(retried.execution.deadlineAt, deadlineAt);
  let snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  assert.equal(planSessionContinuation(snapshot, 'original').mode, 'model');
  assert.equal(planSessionContinuation(snapshot, 'after-timeout').mode, 'blocked');
  await project(f, retried, [{ id: 'timeout-recovered-once', type: 'agent.message', content: [{ type: 'text', text: 'completed once' }], processedAt: at(9) }, idle('timeout-complete', 9)], 9);
  assert.equal((await reopened.settle({ fence: retried.fence, settledAt: at(10), outcome: 'completed' })).type, 'settled');
  assert.equal((await claim(reopened, 'ordered-later', 11)).execution.id, 'after-timeout');
  snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  assert.equal(snapshot.orderedEvents.filter(row => row.event.id === 'timeout-recovered-once').length, 1);
});

test('actual SQL expired-owner sweep preserves 105 recoverable sessions beyond one batch and refuses unsafe controls', { timeout: 120000 }, async t => {
  const f = await fixture(t);
  assert.equal((await f.store.settle({ fence: f.owned.fence, settledAt: at(3), outcome: 'completed' })).type, 'settled');
  const records = [];
  for (const kind of [...Array.from({ length: 105 }, (_, index) => 'eligible-' + index), 'effects', 'interrupt', 'deadline', 'archived']) {
    const ownScope = { workspaceId: scope.workspaceId, sessionId: 'batch-' + kind };
    const document = { ...session, id: ownScope.sessionId };
    const inputId = ownScope.sessionId + '-input';
    await f.sql.prepare('INSERT INTO managed_sessions (workspace_id,id,document,revision,status,created_at,updated_at) VALUES (?,?,?,1,?,?,?)').bind(ownScope.workspaceId, ownScope.sessionId, JSON.stringify(document), 'idle', Date.parse(at(0)), Date.parse(at(0))).run();
    assert.equal((await f.events.append({ ...ownScope, expectedRevision: 1, events: [{ id: inputId, type: 'user.message', content: [{ type: 'text', text: kind }], processedAt: at(1) }], nextSession: document })).type, 'appended');
    const first = await f.store.claim({ ...ownScope, ownerId: inputId + '-first', attemptId: inputId + '-first', claimedAt: at(2), leaseTtlMs: 10000 });
    assert.equal(first.type, 'claimed');
    assert.equal((await f.writer.project({ ...ownScope, expectedRevision: 2, executionFence: first.fence, events: [errorEvent(inputId + '-error', 3, 'retained_configuration_changed'), idle(inputId + '-idle', 3, { type: 'retries_exhausted' })], next: document })).type, 'projected');
    assert.equal((await f.store.settle({ fence: first.fence, settledAt: at(4), outcome: 'failed', failure: 'retained_configuration_changed' })).type, 'settled');
    await f.sql.prepare('UPDATE managed_session_executions SET max_attempts = 2 WHERE id = ?').bind(inputId).run();
    const recovery = await f.store.claim({ ...ownScope, ownerId: inputId + '-died', attemptId: inputId + '-died', claimedAt: at(5), leaseTtlMs: 10000 });
    assert.equal(recovery.type, 'claimed');
    const writer = new SqlSessionRuntimeProjectionPersistence(f.sql, { now: () => new Date(at(6)) });
    const effect = kind === 'effects' ? { id: inputId + '-effect', type: 'agent.message', content: [{ type: 'text', text: 'committed effect' }], processedAt: at(6) } : running(inputId + '-running', 6);
    assert.equal((await writer.project({ ...ownScope, expectedRevision: 3, executionFence: recovery.fence, events: [effect], next: document })).type, 'projected');
    if (kind === 'interrupt') assert.equal((await f.store.requestInterrupt({ ...ownScope, requestedAt: at(7) })).type, 'requested');
    if (kind === 'deadline') await f.sql.prepare('UPDATE managed_session_executions SET deadline_at_ms = ? WHERE id = ?').bind(Date.parse(at(14)), inputId).run();
    if (kind === 'archived') await f.sql.prepare('UPDATE managed_sessions SET archived_at = ? WHERE id = ?').bind(Date.parse(at(7)), ownScope.sessionId).run();
    records.push({ kind, ownScope, inputId, recovery });
  }
  const reopened = coordinator(await f.open());
  const first = records[0];
  const trigger = await reopened.claim({ ...first.ownScope, ownerId: 'sweep-trigger', attemptId: 'sweep-trigger', claimedAt: at(16), leaseTtlMs: 10000 });
  assert.equal(trigger.type, 'claimed');
  for (const [index, row] of records.entries()) {
    const saved = await reopened.find({ workspaceId: scope.workspaceId, executionId: row.inputId });
    if (row.kind.startsWith('eligible-')) {
      assert.notEqual(saved.failure, 'execution attempt limit exhausted', row.kind + ' was terminalized before adjudication');
      const owned = index === 0 ? trigger : await reopened.claim({ ...row.ownScope, ownerId: row.inputId + '-restored', attemptId: row.inputId + '-restored', claimedAt: at(17), leaseTtlMs: 10000 });
      assert.equal(owned.type, 'claimed', row.kind + ' lost recovery');
      assert.equal(owned.execution.id, row.inputId);
      assert.equal(owned.execution.attemptCount, 2);
      assert.equal(owned.execution.deadlineAt, row.recovery.execution.deadlineAt);
      assert.notEqual(owned.fence.generation, row.recovery.fence.generation);
      assert.equal(planSessionContinuation(await loadSessionContinuationSnapshot(f.sql, row.ownScope), row.inputId).mode, 'model');
      assert.equal((await reopened.settle({ fence: row.recovery.fence, settledAt: at(18), outcome: 'completed' })).type, 'lost');
    } else {
      assert.equal((await reopened.claim({ ...row.ownScope, ownerId: row.inputId + '-refused', attemptId: row.inputId + '-refused', claimedAt: at(17), leaseTtlMs: 10000 })).type, 'empty');
      assert.equal(saved.attemptCount, 2, row.kind + ' was refunded');
      assert.equal(saved.deadlineAt, row.kind === 'deadline' ? at(14) : row.recovery.execution.deadlineAt);
      const snapshot = await loadSessionContinuationSnapshot(f.sql, row.ownScope);
      assert.equal(snapshot.orderedEvents.filter(item => item.event.id === row.inputId + '-effect').length, row.kind === 'effects' ? 1 : 0);
    }
  }
  assert.equal(records.filter(row => row.kind.startsWith('eligible-')).length, 105);
});

async function failedSiblings(t, unrelated = false) {
  const f = await fixture(t);
  await project(f, f.owned, [
    { id: 'X', type: 'agent.custom_tool_use', name: 'qualification', input: { id: 'X' }, processedAt: at(3) },
    { id: 'Y', type: 'agent.custom_tool_use', name: 'qualification', input: { id: 'Y' }, processedAt: at(3) },
    idle('declare-both', 3, { type: 'requires_action', eventIds: ['X', 'Y'] }),
  ], 3);
  assert.equal((await f.store.settle({ fence: f.owned.fence, settledAt: at(4), outcome: 'completed' })).type, 'settled');
  const historical = new SqlSessionExecutionCoordinator(f.sql, { serializeSessionClaims: true });
  for (const [index, id] of ['RX', 'RY'].entries()) {
    const seconds = 5 + index * 4;
    await append(f, [{ id, type: 'user.custom_tool_result', customToolUseId: id === 'RX' ? 'X' : 'Y', content: [{ type: 'text', text: id + ' exact result' }], processedAt: at(seconds) }]);
    const owned = await claim(historical, id + '-old-owner', seconds + 1);
    assert.equal(owned.type, 'claimed');
    assert.equal(owned.execution.id, id);
    const message = unrelated && id === 'RY' ? 'provider unavailable' : 'retained_configuration_changed';
    await project(f, owned, [errorEvent(id + '-error', seconds + 2, message), idle(id + '-failed-idle', seconds + 2, { type: 'retries_exhausted' })], seconds + 2);
    assert.equal((await historical.settle({ fence: owned.fence, settledAt: at(seconds + 3), outcome: 'failed', failure: message })).type, 'settled');
  }
  return f;
}

test('actual SQL failed sibling RX RY consume both results then one model phase after reopen', async t => {
  const f = await failedSiblings(t);
  const store = coordinator(await f.open());
  const rx = await claim(store, 'RX-recovered', 13);
  assert.equal(rx.type, 'claimed');
  assert.equal(rx.execution.id, 'RX');
  let snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  const intake = planSessionContinuation(snapshot, 'RX');
  assert.equal(intake.mode, 'intake', 'recoverable RY must not cancel RX to noop');
  assert.equal(snapshot.executions.find(row => row.id === 'RY').state, 'queued');
  await project(f, rx, [running('RX-intake-start', 14), idle('RX-intake-committed', 14, { type: 'requires_action', eventIds: ['Y'] })], 14);
  assert.equal((await store.settle({ fence: rx.fence, settledAt: at(15), outcome: 'completed' })).type, 'settled');
  const ry = await claim(coordinator(await f.open()), 'RY-recovered', 16);
  assert.equal(ry.type, 'claimed');
  assert.equal(ry.execution.id, 'RY');
  snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  const resumed = planSessionContinuation(snapshot, 'RY');
  assert.equal(resumed.mode, 'model');
  assert.equal(resumed.historyEvents.filter(row => row.id === 'RX').length, 1);
  assert.equal(resumed.historyEvents.filter(row => row.id === 'RY').length, 1);
  await project(f, ry, [{ id: 'one-follow-on', type: 'agent.message', content: [{ type: 'text', text: 'both results consumed once' }], processedAt: at(17) }, idle('RY-final', 17)], 17);
  assert.equal((await store.settle({ fence: ry.fence, settledAt: at(18), outcome: 'completed' })).type, 'settled');
  assert.equal((await claim(store, 'no-duplicate-model', 19)).type, 'empty');
  snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  assert.deepEqual(snapshot.executions.filter(row => ['RX', 'RY'].includes(row.id)).map(row => [row.id, row.state]), [['RX', 'completed'], ['RY', 'completed']]);
  assert.equal(snapshot.orderedEvents.filter(row => row.event.id === 'one-follow-on').length, 1);
  assert.equal(snapshot.orderedEvents.filter(row => row.event.type === 'agent.custom_tool_use').length, 2, 'tool declaration duplicated');
});

test('actual SQL unrelated failed sibling is not classified as pending image recovery', async t => {
  const f = await failedSiblings(t, true);
  const result = await claim(coordinator(await f.open()), 'terminal-sibling-check', 13);
  if (result.type === 'claimed') assert.equal(result.execution.id, 'RX');
  const snapshot = await loadSessionContinuationSnapshot(f.sql, scope);
  const sibling = snapshot.executions.find(row => row.id === 'RY');
  assert.equal(sibling.state, 'failed');
  assert.equal(sibling.imageRecoveryAfterEventId, undefined);
  assert.equal(snapshot.orderedEvents.filter(row => row.event.id === 'RY-error' && row.event.error.message === 'provider unavailable').length, 1);
  if (result.type === 'claimed') assert.notEqual(planSessionContinuation(snapshot, result.execution.id).mode, 'model');
});


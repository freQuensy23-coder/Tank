import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const root = new URL('../../..', import.meta.url);
const source = new URL('vendor/openma/', root);
const load = path => import(new URL(path, source));
const require = createRequire(new URL('packages/node-database-postgres/package.json', source));
const postgres = require('postgres');
const { drizzle } = require('drizzle-orm/postgres-js');
const { migrate } = require('drizzle-orm/postgres-js/migrator');
const { PostgresSqlClient } = await load('packages/sql-client/src/adapters/postgres.ts');
const { migrateNodeDatabase } = await load('apps/main-node/src/database.ts');
const { SqlSessionStore } = await load('packages/session-store-sql/src/index.ts');
const { SqlSessionEventStore } = await load('packages/session-event-store-sql/src/index.ts');
const { SessionEventsApplicationService } = await load('packages/managed-agents-application/src/session-events/application.ts');
const { SqlSessionRuntimeProjectionPersistence } = await load('packages/managed-agents-adapters-sql/src/session-runtime-projection-sql-persistence.ts');
const { SqlSessionExecutionStore } = await load('packages/session-runtime-sql/src/coordination.ts');
const { loadSessionContinuationSnapshot } = await load('packages/session-runtime-sql/src/continuation.ts');
const { planSessionContinuation } = await load('packages/session-runtime-contract/src/continuation.ts');
const { ManagedNodeHarnessRuntime } = await load('packages/control-plane-node/src/lib/node-managed-harness-runtime.ts');
const { SqlRuntimeResourceFencePort, ensureRuntimeResourceFenceSchema } = await load('packages/runtime-resource-fence-sql/src/index.ts');
const { RetainedSessionLifecycle } = await load('packages/control-plane-node/src/lib/tank-retained/retained-session.ts');
let input = '';
for await (const chunk of process.stdin) input += chunk;
const { url } = JSON.parse(input);
const target = new URL(url);
assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(target.hostname));
assert.match(target.pathname, /^\/tank_test_[A-Za-z0-9_]+$/);
const connection = postgres(url, { onnotice: () => {}, types: {
  bigint: { to: 20, from: [20], serialize: value => String(value), parse: Number },
} });
const sql = new PostgresSqlClient(connection);
let now = new Date('2026-11-03T00:00:00.000Z');
const clock = { now: () => now };
const scope = { workspaceId: 'workspace', sessionId: 'session' };
const text = value => [{ type: 'text', text: value }];
const event = value => ({ ...value, processedAt: now.toISOString() });
const session = {
  id: 'session', agent: { id: 'agent', version: 1, model: { id: 'controlled' },
    name: 'fixture', description: null, system: null, tools: [], skills: [], mcpServers: [], multiagent: null },
  environmentId: 'environment', status: 'idle', resources: [], metadata: {}, stats: {}, usage: {},
  vaultIds: [], outcomeEvaluations: [], title: null, budget: null, archivedAt: null,
  createdAt: now.toISOString(), updatedAt: now.toISOString(),
};
let lifecycle;
let scenarioFailed = false;
try {
  const version = await sql.prepare('SHOW server_version_num').first();
  assert.equal(Math.floor(Number(version.server_version_num) / 10000), 16);
  await migrateNodeDatabase({ dialect: 'postgres', sql,
    applyMigrations: async migrationsFolder => migrate(drizzle(connection), { migrationsFolder }) });
  await ensureRuntimeResourceFenceSchema(sql);
  const sessions = new SqlSessionStore(sql, { seal: async () => assert.fail('no secrets') });
  await sessions.insert({ workspaceId: scope.workspaceId, session, initialEvents: [], resourceSecrets: [] });
  const events = new SqlSessionEventStore(sql, { executionOutbox: true });
  const projection = new SqlSessionRuntimeProjectionPersistence(sql, clock);
  let store = new SqlSessionExecutionStore(sql, { callbackContinuations: true });
  const current = () => sessions.findCurrent(scope);
  let sequence = 0;
  const intake = new SessionEventsApplicationService({ workspaceId: scope.workspaceId, store: events,
    sessions: { find: async () => (await current())?.session ?? null },
    execution: { find: async requested => {
      const stored = await sessions.findCurrent(requested);
      return stored && { ...stored, environment: { id: 'environment', config: { type: 'self_hosted' } } };
    } },
    stream: { subscribe: () => assert.fail('no stream') },
    dispatch: { sessionEventsAccepted: async () => {} }, clock,
    ids: { nextEventId: () => `input_${++sequence}`, nextOutcomeId: () => assert.fail('no outcome') },
  });
  const send = (key, content) => intake.sendSessionEvents({ sessionId: scope.sessionId,
    idempotencyKey: key, events: content });
  const claim = async ownerId => {
    const result = await store.claim({ ...scope, ownerId, attemptId: `${ownerId}-attempt`,
      claimedAt: now.toISOString(), leaseTtlMs: 30000 });
    assert.equal(result.type, 'claimed');
    return result;
  };
  const project = async (fence, batch, status = 'idle') => {
    const stored = await current();
    return projection.project({ ...scope, expectedRevision: stored.revision, executionFence: fence,
      events: batch, next: { ...stored.session, status, updatedAt: now.toISOString() } });
  };
  const accepted = await send('prompt', [{ type: 'user.message', content: text('Remember every rejected and corrected call') }]);
  assert.equal(accepted.type, 'accepted');
  assert.equal(accepted.events.length, 1);
  const promptId = accepted.events[0].id;
  assert.equal(typeof promptId, 'string');
  assert.ok(promptId.length > 0);
  const promptEvent = event({ id: promptId, type: 'user.message', content: text('Remember every rejected and corrected call') });
  assert.deepEqual(accepted.events, [promptEvent]);
  const original = await claim('before');
  assert.equal(original.execution.id, promptId);
  assert.deepEqual(original.execution.events, [promptEvent]);
  assert.deepEqual(original.fence, { ...scope, executionId: promptId, attemptId: 'before-attempt',
    ownerId: 'before', generation: 1, expiresAt: '2026-11-03T00:00:30.000Z' });
  const output = [
    event({ id: 'running', type: 'session.status_running' }),
    event({ id: 'model', type: 'span.model_request_start' }),
    event({ id: 'rejected', type: 'agent.custom_tool_use', name: 'record_event', input: { precision: 'MINUTE' } }),
    event({ id: 'rejected-result', type: 'agent.tool_result', toolUseId: 'rejected', isError: true, content: text('Use HOUR') }),
    event({ id: 'corrected', type: 'agent.custom_tool_use', name: 'record_event', input: { precision: 'HOUR' } }),
    event({ id: 'model-end', type: 'span.model_request_end', modelRequestStartId: 'model', isError: false,
      modelUsage: { inputTokens: 10, outputTokens: 5, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 } }),
    event({ id: 'waiting', type: 'session.status_idle', stopReason: { type: 'requires_action', eventIds: ['corrected'] } }),
  ];
  assert.equal((await project(original.fence, output)).type, 'projected');
  const persisted = async () => (await sql.prepare('SELECT * FROM managed_session_events WHERE workspace_id = ? AND session_id = ? ORDER BY id')
    .bind(scope.workspaceId, scope.sessionId).all()).results;
  const before = await persisted();
  const expectedRows = [
    { value: promptEvent, position: { revision: 2, index: 0 } },
    ...output.map((value, index) => ({ value, position: { revision: 3, index,
      execution_id: promptId, execution_metadata: { attemptId: 'before-attempt', generation: 1 } } })),
  ].map(({ value, position }) => ({ workspace_id: 'workspace', session_id: 'session', thread_id: null,
    id: value.id, type: value.type, document: { ...value, source_position: position },
    processed_at: Date.parse('2026-11-03T00:00:00.000Z') }));
  assert.deepEqual(before.map(row => ({ ...row, document: JSON.parse(row.document) })),
    [...expectedRows].sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
  const fences = new SqlRuntimeResourceFencePort(sql, clock);
  const disks = new Map();
  let allocation = 0;
  const provider = {
    async allocationIntent() { return { provider: 'controlled', runtimeId: 'intent' }; },
    async resolveIntent() { return null; },
    async create(ownedScope) {
      const id = `runtime-${++allocation}`;
      const disk = { scope: ownedScope, files: {}, packages: [], profile: {} };
      disks.set(id, disk);
      return { runtimeHandle: () => ({ provider: 'controlled', runtimeId: id }),
        suspend: async () => {}, disk };
    },
    async resume(handle) {
      const disk = disks.get(handle.runtimeId);
      assert.ok(disk);
      return { runtimeHandle: () => handle, suspend: async () => {}, disk };
    },
    async reap(handle, ownedScope) {
      assert.deepEqual(disks.get(handle.runtimeId).scope, ownedScope);
      disks.delete(handle.runtimeId);
    },
  };
  const identity = image => ({ version: 2, image: `sha256:${image.repeat(64)}`,
    compatibility: { scope: `sha256:${'d'.repeat(64)}` } });
  const options = { scope: { ...scope, environmentId: 'environment' }, provider, providerId: 'controlled',
    ownerId: 'controller', leaseTtlMs: 30000, fences };
  let renewals = 0;
  const authorized = async () => {
    assert.equal((await store.renew({ fence: original.fence, renewedAt: now.toISOString(), leaseTtlMs: 30000 })).type, 'renewed');
    renewals += 1;
  };
  lifecycle = new RetainedSessionLifecycle({ ...options, fingerprint: `sha256:${'a'.repeat(64)}`, identity: identity('a') });
  await lifecycle.begin(authorized, new AbortController().signal);
  await lifecycle.use(runtime => { runtime.disk.files.secret = 'not transferred'; runtime.disk.packages.push('not transferred'); runtime.disk.profile.cookie = 'not transferred'; });
  await lifecycle.idle(authorized);
  lifecycle = new RetainedSessionLifecycle({ ...options, fingerprint: `sha256:${'b'.repeat(64)}`, identity: identity('b') });
  await lifecycle.begin(authorized, new AbortController().signal);
  await lifecycle.use(runtime => assert.deepEqual(runtime.disk, { scope: lifecycle.scope, files: {}, packages: [], profile: {} }));
  assert.equal(disks.size, 1);
  assert.equal(allocation, 2);
  assert.deepEqual(await persisted(), before);
  await lifecycle.idle(authorized);
  const originalSettlement = await store.settle({ fence: original.fence, settledAt: now.toISOString(), outcome: 'completed' });
  const expectedOriginalExecution = { ...scope, id: promptId, laneId: 'sthr_primary',
    admittedAt: '2026-11-03T00:00:00.000Z', events: [promptEvent], state: 'completed',
    attempt: { id: 'before-attempt', ownerId: 'before', generation: 1,
      claimedAt: '2026-11-03T00:00:00.000Z', leaseExpiresAt: '2026-11-03T00:00:30.000Z' },
    attemptCount: 1, maxAttempts: 10, deadlineAt: '2026-11-03T01:00:00.000Z', interruptRequestedAt: null,
    settledAt: '2026-11-03T00:00:00.000Z', failure: null, revision: 3 + renewals };
  assert.deepEqual(originalSettlement, { type: 'settled', execution: expectedOriginalExecution });
  const callbackBody = [{ type: 'user.custom_tool_result', customToolUseId: 'corrected', content: text('Recorded'), isError: false }];
  const callback = await send('callback', callbackBody);
  assert.equal(callback.type, 'accepted');
  assert.equal(callback.events.length, 1);
  const callbackId = callback.events[0].id;
  assert.equal(typeof callbackId, 'string');
  assert.ok(callbackId.length > 0);
  assert.notEqual(callbackId, promptId);
  const callbackEvent = event({ id: callbackId, type: 'user.custom_tool_result',
    customToolUseId: 'corrected', content: text('Recorded'), isError: false });
  assert.deepEqual(callback.events, [callbackEvent]);
  expectedRows.push({ workspace_id: 'workspace', session_id: 'session', thread_id: null,
    id: callbackId, type: 'user.custom_tool_result', document: { ...callbackEvent,
      source_position: { revision: 4, index: 0 } }, processed_at: Date.parse('2026-11-03T00:00:00.000Z') });
  const afterCallback = await persisted();
  assert.deepEqual(afterCallback.map(row => ({ ...row, document: JSON.parse(row.document) })),
    [...expectedRows].sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
  assert.deepEqual((await send('callback', callbackBody)), callback);
  assert.deepEqual(await persisted(), afterCallback);
  assert.equal((await send('callback', [{ ...callbackBody[0], customToolUseId: 'rejected' }])).type, 'idempotency_conflict');
  assert.deepEqual(await persisted(), afterCallback);
  store = new SqlSessionExecutionStore(sql, { callbackContinuations: true });
  const reply = await claim('after');
  assert.equal(reply.execution.id, callbackId);
  assert.deepEqual(reply.execution.events, [callbackEvent]);
  assert.deepEqual(reply.fence, { ...scope, executionId: callbackId, attemptId: 'after-attempt',
    ownerId: 'after', generation: 1, expiresAt: '2026-11-03T00:00:30.000Z' });
  const snapshot = await loadSessionContinuationSnapshot(sql, scope);
  const plan = planSessionContinuation(snapshot, reply.execution.id);
  assert.deepEqual(plan, { mode: 'model', historyEvents: [promptEvent, ...output, callbackEvent],
    remaining: [], phaseInputIds: [callbackId], consumedInputIds: [callbackId], remainingInputIds: [],
    previousReceiptId: null });
  const runtime = new ManagedNodeHarnessRuntime({ initialEvents: [], events: plan.historyEvents,
    sandbox: {}, output: async () => assert.fail('projection must not execute'), clock,
    ids: { nextEventId: () => assert.fail('projection must retain original IDs') } });
  const messages = runtime.history.getMessages();
  assert.equal((await project(original.fence, [event({ id: 'stale', type: 'agent.message', content: text('forbidden') })])).type, 'execution_fence_lost');
  assert.deepEqual(await persisted(), afterCallback);
  assert.deepEqual(await store.settle({ fence: original.fence, settledAt: now.toISOString(), outcome: 'completed' }), originalSettlement);
  assert.deepEqual(await store.find({ workspaceId: 'workspace', executionId: promptId }), originalSettlement.execution);
  assert.deepEqual(await store.settle({ fence: original.fence, settledAt: '2026-11-03T00:00:00.001Z',
    outcome: 'failed', failure: 'stale settlement must not overwrite completion' }), { type: 'lost' });
  assert.deepEqual(await store.find({ workspaceId: 'workspace', executionId: promptId }), originalSettlement.execution);
  assert.deepEqual(await persisted(), afterCallback);
  const continuedOutput = [event({ id: 'continued', type: 'agent.message', content: text('continued') }),
    event({ id: 'done', type: 'session.status_idle', stopReason: { type: 'end_turn' } })];
  assert.equal((await project(reply.fence, continuedOutput)).type, 'projected');
  expectedRows.push(...continuedOutput.map((value, index) => ({ workspace_id: 'workspace', session_id: 'session',
    thread_id: null, id: value.id, type: value.type, document: { ...value, source_position: { revision: 5,
      index, execution_id: callbackId, execution_metadata: { attemptId: 'after-attempt', generation: 1 } } },
    processed_at: Date.parse('2026-11-03T00:00:00.000Z') })));
  assert.equal((await store.settle({ fence: reply.fence, settledAt: now.toISOString(), outcome: 'completed' })).type, 'settled');
  const reopened = new SqlSessionExecutionStore(sql, { callbackContinuations: true });
  const expectedReplyExecution = { ...scope, id: callbackId, laneId: 'sthr_primary',
    admittedAt: '2026-11-03T00:00:00.000Z', events: [callbackEvent], state: 'completed',
    attempt: { id: 'after-attempt', ownerId: 'after', generation: 1,
      claimedAt: '2026-11-03T00:00:00.000Z', leaseExpiresAt: '2026-11-03T00:00:30.000Z' },
    attemptCount: 1, maxAttempts: 10, deadlineAt: '2026-11-03T01:00:00.000Z', interruptRequestedAt: null,
    settledAt: '2026-11-03T00:00:00.000Z', failure: null, revision: 3 };
  assert.deepEqual(await reopened.find({ workspaceId: 'workspace', executionId: callbackId }), expectedReplyExecution);
  assert.deepEqual((await persisted()).filter(row => before.some(originalRow => originalRow.id === row.id)), before);
  const foreign = await send('foreign-callback', [{ ...callbackBody[0], customToolUseId: 'not-declared' }]);
  assert.equal(foreign.type, 'accepted');
  assert.equal(foreign.events.length, 1);
  const foreignId = foreign.events[0].id;
  assert.equal(typeof foreignId, 'string');
  assert.ok(foreignId.length > 0);
  assert.notEqual(foreignId, promptId);
  assert.notEqual(foreignId, callbackId);
  const foreignEvent = event({ id: foreignId, type: 'user.custom_tool_result',
    customToolUseId: 'not-declared', content: text('Recorded'), isError: false });
  assert.deepEqual(foreign.events, [foreignEvent]);
  expectedRows.push({ workspace_id: 'workspace', session_id: 'session', thread_id: null,
    id: foreignId, type: 'user.custom_tool_result', document: { ...foreignEvent,
      source_position: { revision: 6, index: 0 } }, processed_at: Date.parse('2026-11-03T00:00:00.000Z') });
  const expectedFailure = `Original execution ${foreignId} cannot safely continue: Result refers to an unknown or failed custom-tool group`;
  const refused = planSessionContinuation(await loadSessionContinuationSnapshot(sql, scope), foreignId);
  assert.deepEqual(refused, { mode: 'failure', failure: expectedFailure, historyEvents: [], remaining: [],
    phaseInputIds: [foreignId], consumedInputIds: [], remainingInputIds: [], previousReceiptId: null });
  const rejectedCallback = await claim('refusal');
  assert.equal(rejectedCallback.execution.id, foreignId);
  assert.deepEqual(rejectedCallback.execution.events, [foreignEvent]);
  assert.deepEqual(rejectedCallback.fence, { ...scope, executionId: foreignId, attemptId: 'refusal-attempt',
    ownerId: 'refusal', generation: 1, expiresAt: '2026-11-03T00:00:30.000Z' });
  const expectedFailedExecution = { ...scope, id: foreignId, laneId: 'sthr_primary',
    admittedAt: '2026-11-03T00:00:00.000Z', events: [foreignEvent], state: 'failed',
    attempt: { id: 'refusal-attempt', ownerId: 'refusal', generation: 1,
      claimedAt: '2026-11-03T00:00:00.000Z', leaseExpiresAt: '2026-11-03T00:00:30.000Z' },
    attemptCount: 1, maxAttempts: 10, deadlineAt: '2026-11-03T01:00:00.000Z', interruptRequestedAt: null,
    settledAt: '2026-11-03T00:00:00.000Z', failure: expectedFailure, revision: 3 };
  assert.deepEqual(await store.settle({ fence: rejectedCallback.fence, settledAt: now.toISOString(),
    outcome: 'failed', failure: refused.failure }), { type: 'settled', execution: expectedFailedExecution });
  assert.deepEqual(await reopened.find({ workspaceId: 'workspace', executionId: foreignId }), expectedFailedExecution);
  assert.deepEqual(await reopened.find({ workspaceId: 'workspace', executionId: callbackId }), expectedReplyExecution);
  assert.deepEqual(await reopened.find({ workspaceId: 'workspace', executionId: promptId }), expectedOriginalExecution);
  assert.deepEqual((await persisted()).map(row => ({ ...row, document: JSON.parse(row.document) })),
    [...expectedRows].sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
  assert.deepEqual((await persisted()).filter(row => afterCallback.some(originalRow => originalRow.id === row.id)), afterCallback);
  const resourceScope = { ...scope, environmentId: 'environment', workId: 'refusal' };
  const first = await fences.acquire({ scope: resourceScope, ownerId: 'first', ttlMs: 1000 });
  assert.equal(first.type, 'acquired');
  assert.equal((await fences.acquire({ scope: resourceScope, ownerId: 'competitor', ttlMs: 1000 })).type, 'conflict');
  now = new Date(now.getTime() + 1001);
  const next = await fences.acquire({ scope: resourceScope, ownerId: 'next', ttlMs: 1000 });
  assert.equal(next.type, 'acquired');
  assert.equal(next.fence.generation, first.fence.generation + 1);
  assert.deepEqual(await fences.publish({ fence: first.fence, workspaceCandidate: { id: 'stale', contentHash: 'stale' }, outputCandidate: null }), { type: 'lost' });
  assert.equal(await fences.isCurrent(next.fence), true);
  await fences.release({ fence: first.fence, reason: 'lease_lost' });
  assert.equal(await fences.isCurrent(next.fence), true);
  await fences.release({ fence: next.fence, reason: 'completed' });
  assert.deepEqual(messages, [
    { role: 'user', content: text('Remember every rejected and corrected call') },
    { role: 'assistant', content: [{ type: 'tool-call', toolCallId: 'rejected', toolName: 'record_event', input: { precision: 'MINUTE' } }] },
    { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'rejected', toolName: 'record_event', output: { type: 'error-text', value: 'Use HOUR' } }] },
    { role: 'assistant', content: [{ type: 'tool-call', toolCallId: 'corrected', toolName: 'record_event', input: { precision: 'HOUR' } }] },
    { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'corrected', toolName: 'record_event', output: { type: 'content', value: text('Recorded') } }] },
  ]);
} catch (error) {
  scenarioFailed = true;
  throw error;
} finally {
  let cleanupError;
  let cleanupTimer;
  try {
    if (lifecycle) await Promise.race([
      lifecycle.idle(async () => {}),
      new Promise((resolve, reject) => { cleanupTimer = setTimeout(() => reject(new Error('Lifecycle cleanup timed out')), 5000); }),
    ]);
  } catch (error) {
    cleanupError = error;
  } finally {
    clearTimeout(cleanupTimer);
    try {
      await connection.end({ timeout: 5 });
    } catch (error) {
      cleanupError ??= error;
    }
  }
  if (!scenarioFailed && cleanupError) throw cleanupError;
}
console.log(JSON.stringify({ qualification: 'postgres-source-only', shippingEntrypointExercised: false,
  nativeVmExercised: false, retainedHistory: true, callbackIdempotence: true,
  executionAndResourceFences: true, settlementAndReopen: true }));

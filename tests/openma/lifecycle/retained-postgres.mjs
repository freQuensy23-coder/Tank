import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

let stage = 'imports';
let failure;
let failureReported = false;
const reason = error => [
  'ERR_MODULE_NOT_FOUND', 'MODULE_NOT_FOUND', 'ERR_PACKAGE_PATH_NOT_EXPORTED',
  'ERR_UNKNOWN_FILE_EXTENSION', 'ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX',
].includes(error?.code) ? error.code : error?.code === 'ERR_ASSERTION' ? 'assertion' : 'UNKNOWN';
try {
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
const { SqlSessionExecutionStore, ensureSessionExecutionClaimLockSchema } = await load('packages/session-runtime-sql/src/coordination.ts');
const { loadSessionContinuationSnapshot } = await load('packages/session-runtime-sql/src/continuation.ts');
const { planSessionContinuation } = await load('packages/session-runtime-contract/src/continuation.ts');
const { ManagedNodeHarnessRuntime } = await load('packages/control-plane-node/src/lib/node-managed-harness-runtime.ts');
const { SqlRuntimeResourceFencePort, ensureRuntimeResourceFenceSchema } = await load('packages/runtime-resource-fence-sql/src/index.ts');
const { RetainedSessionLifecycle } = await load('packages/control-plane-node/src/lib/tank-retained/retained-session.ts');
stage = 'input';
let input = '';
for await (const chunk of process.stdin) input += chunk;
const { url } = JSON.parse(input);
const target = new URL(url);
assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(target.hostname));
assert.match(target.pathname, /^\/tank_test_[A-Za-z0-9_]+$/);
stage = 'connection';
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
  stage = 'migration';
  await migrateNodeDatabase({ dialect: 'postgres', sql,
    applyMigrations: async migrationsFolder => migrate(drizzle(connection), { migrationsFolder }) });
  await ensureSessionExecutionClaimLockSchema(sql, 'postgres');
  await ensureRuntimeResourceFenceSchema(sql);
  stage = 'fixture';
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
  stage = 'original-history';
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
  const persisted = async () => [...(await sql.prepare('SELECT * FROM managed_session_events WHERE workspace_id = ? AND session_id = ? ORDER BY id')
    .bind(scope.workspaceId, scope.sessionId).all()).results];
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
  stage = 'replacement';
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
  stage = 'callback';
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
  stage = 'continuation';
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
  stage = 'execution-fences';
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
  stage = 'settlement';
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
  stage = 'callback-refusal';
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
  stage = 'resource-fences';
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
  const beforeOrphanHistory = await persisted();
  const { SqlRuntimeOrphanPort } = await load('packages/runtime-resource-fence-sql/src/index.ts');
  const { NodeRetainedLifecycle } = await load('packages/control-plane-node/src/lib/tank-retained/node-lifecycle.ts');
  const { pinnedBoxLiteProvider } = await load('packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts');
  const providerModule = await load('packages/managed-runtime-boxlite/src/index.ts');
  for (const schema of [1, 2]) for (const mode of ['publication', 'fence-loss', 'post-allocation']) {
    const orphanScope = { workspaceId: 'orphan-workspace', environmentId: 'environment',
      sessionId: `orphan-${schema}-${mode}`, workId: 'tank-retained-session-v1' };
    const orphanFences = new SqlRuntimeResourceFencePort(sql, clock);
    const orphanQueue = new SqlRuntimeOrphanPort(sql);
    const nativeDisks = new Map(), effects = [];
    let lost = false, failedRemove = 0, failedPublication = 0;
    const inspect = id => nativeDisks.get(id) ?? [...nativeDisks.values()].find(disk => disk.name === id);
    const native = disk => ({
      id: disk.id, name: disk.name,
      async info() { return { id: disk.id, name: disk.name,
        state: { running: disk.running, status: disk.running ? 'Running' : 'Stopped' } }; },
      async start() { disk.running = true; effects.push(['start', disk.id]); },
      async stop() { disk.running = false; effects.push(['stop', disk.id]); },
      async exec(command, args) {
        effects.push(['exec', disk.id, command, args]);
        return { async stdin() { return { async write() {}, async close() {} }; },
          async stdout() { return { async next() { return null; } }; },
          async stderr() { return { async next() { return null; } }; },
          async wait() { return { exitCode: 0 }; }, async kill() {}, async id() { return 'controlled-exec'; } };
      },
      async copyIn() { assert.fail('no guest copy'); }, async copyOut() { assert.fail('no guest copy'); },
    });
    const client = {
      async getInfo(id) { const disk = inspect(id); return disk ? native(disk).info() : null; },
      async get(id) { const disk = inspect(id); return disk ? native(disk) : null; },
      async getOrCreate(allocation, name) {
        assert.equal(allocation.autoRemove, false); assert.equal(allocation.detach, true);
        const disk = { id: `controlled-native-${schema}-${mode}`, name, running: false,
          files: { '/workspace/marker': 'preserved' } };
        assert.equal(inspect(name), undefined); nativeDisks.set(disk.id, disk);
        effects.push(['allocate', disk.id]);
        if (mode !== 'publication') lost = true;
        if (mode === 'post-allocation') throw Error('injected post-allocation failure');
        return { created: true, box: native(disk) };
      },
      async remove(id) {
        effects.push(['remove', id]);
        if (failedRemove > 0) { failedRemove--; throw Error('injected cleanup failure'); }
        nativeDisks.delete(id);
      },
    };
    const isCurrent = orphanFences.isCurrent.bind(orphanFences);
    orphanFences.isCurrent = async fence => !lost && await isCurrent(fence);
    const publish = orphanFences.publish.bind(orphanFences);
    orphanFences.publish = async input => {
      if (mode === 'publication' && input.workspaceCandidate.metadata.state === 'retained') {
        failedPublication++;
        throw Error('injected retained publication failure');
      }
      return publish(input);
    };
    const orphanProvider = pinnedBoxLiteProvider({ module: providerModule, client,
      allocation: { image: 'controlled-no-vm' }, leaseTtlMs: 30000,
      isCurrent: fence => orphanFences.isCurrent(fence),
      unpublishedCleanupFailed: async (sandbox, scope, fence) => orphanQueue.enqueue({ scope,
        generation: fence.generation, ownerId: fence.ownerId, sandbox, reason: 'failed',
        error: Error('retained_initial_cleanup_failed') }) });
    const orphanOptions = { scope: orphanScope, fences: orphanFences, orphans: orphanQueue,
      provider: orphanProvider, providerId: 'litebox', ownerId: 'orphan-controller', leaseTtlMs: 30000,
      fingerprint: `sha256:${'c'.repeat(64)}`, identity: schema === 2 ? identity('c') : undefined };
    if (mode === 'publication') failedRemove = 1;
    await assert.rejects(new RetainedSessionLifecycle(orphanOptions).begin(async () => {},
      new AbortController().signal), /retained_initial_cleanup_failed/);
    assert.equal(failedPublication, mode === 'publication' ? 1 : 0);
    assert.equal(failedRemove, 0);
    assert.equal(effects.filter(effect => effect[0] === 'remove').length, mode === 'publication' ? 1 : 0);
    lost = false; orphanFences.publish = publish;
    const queued = await new SqlRuntimeOrphanPort(sql).list({ limit: 128 });
    assert.equal(queued.length, 1); assert.equal(queued[0].attempts, 0);
    assert.deepEqual(queued[0].scope, orphanScope);
    const diskId = queued[0].sandbox.runtimeId;
    assert.equal(queued[0].sandbox.provider, 'litebox'); assert.equal(nativeDisks.has(diskId), true);
    const readPublication = () => sql.prepare('SELECT publication_json, publication_generation, revision FROM runtime_resource_fences WHERE workspace_id = ? AND environment_id = ? AND session_id = ? AND work_id = ?')
      .bind(orphanScope.workspaceId, orphanScope.environmentId, orphanScope.sessionId, orphanScope.workId).first();
    const beforePublication = await readPublication();
    const metadata = JSON.parse(beforePublication.publication_json).workspaceCandidate.metadata;
    assert.equal(metadata.schema, schema); assert.equal(metadata.state, 'allocating');
    assert.equal(metadata.runtimeId, nativeDisks.get(diskId).name);
    assert.notEqual(metadata.runtimeId, diskId);
    const beforeDisk = structuredClone(nativeDisks.get(diskId)), beforeEffects = effects.length;
    const reconciler = new NodeRetainedLifecycle({ fences: new SqlRuntimeResourceFencePort(sql, clock),
      orphans: new SqlRuntimeOrphanPort(sql), providerModule, client, ownerId: 'orphan-reconciler',
      leaseTtlMs: 30000, fingerprint: async () => orphanOptions.fingerprint,
      assertExecutionCurrent: async () => assert.fail('cleanup must not execute a session') });
    await reconciler.reconcileOrphans();
    assert.deepEqual(await new SqlRuntimeOrphanPort(sql).list({ limit: 128 }), []);
    assert.equal(await sql.prepare('SELECT id FROM runtime_resource_orphans WHERE id = ?').bind(queued[0].id).first(), null);
    assert.deepEqual(await readPublication(), beforePublication);
    assert.deepEqual(nativeDisks.get(diskId), beforeDisk); assert.equal(nativeDisks.size, 1);
    assert.deepEqual(effects.slice(beforeEffects), []);
    await new RetainedSessionLifecycle(orphanOptions).archive(async () => {}, new AbortController().signal);
    assert.equal(nativeDisks.size, 0);
  }
  {
    const { createHash } = await import('node:crypto');
    const { stableDigest } = await load('packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts');
    const ownedScope = { workspaceId: 'orphan-workspace', environmentId: 'environment',
      sessionId: 'ordinary-lookup-loss', workId: 'tank-retained-session-v1' };
    const ownedFences = new SqlRuntimeResourceFencePort(sql, clock), queue = new SqlRuntimeOrphanPort(sql);
    const seed = await ownedFences.acquire({ scope: ownedScope, ownerId: 'seed', ttlMs: 30000 });
    assert.equal(seed.type, 'acquired');
    const handle = { provider: 'litebox', runtimeId: 'ordinary-orphan' };
    const manifest = JSON.stringify({ handle: { provider: 'litebox', runtimeId: 'published-sibling' },
      state: 'retained', archived: false, identity: identity('c'), retired: [] });
    assert.equal((await ownedFences.publish({ fence: seed.fence, workspaceCandidate: {
      id: 'ordinary-publication', contentHash: stableDigest({ scope: ownedScope, manifest }),
      metadata: { schema: 2, scope: JSON.stringify(ownedScope), manifest } }, outputCandidate: null })).type, 'published');
    await ownedFences.release({ fence: seed.fence, reason: 'completed' });
    await queue.enqueue({ scope: ownedScope, generation: seed.fence.generation, ownerId: 'seed',
      sandbox: handle, reason: 'failed', error: Error('retained_initial_cleanup_failed') });
    const queued = await queue.list({ limit: 128 });assert.equal(queued.length, 1);assert.equal(queued[0].attempts, 0);
    const disk = { id: handle.runtimeId,
      name: 'oma-' + createHash('sha256').update(`litebox:${ownedScope.sessionId}`).digest('hex').slice(0, 32),
      state: { running: false, status: 'Stopped' }, files: { marker: 'preserved' } };
    const nativeDisks = new Map([[disk.id, disk]]), effects = [], beforeDisk = structuredClone([...nativeDisks]);
    const readPublication = () => sql.prepare('SELECT publication_json, publication_generation, revision FROM runtime_resource_fences WHERE workspace_id = ? AND environment_id = ? AND session_id = ? AND work_id = ?')
      .bind(ownedScope.workspaceId, ownedScope.environmentId, ownedScope.sessionId, ownedScope.workId).first();
    const beforePublication = await readPublication();
    let lookups = 0, successor;
    const lookup = async id => {
      assert.equal(id, disk.id);lookups++;
      const previous = await sql.prepare('SELECT generation, owner_id, expires_at_ms FROM runtime_resource_fences WHERE workspace_id = ? AND environment_id = ? AND session_id = ? AND work_id = ?')
        .bind(ownedScope.workspaceId, ownedScope.environmentId, ownedScope.sessionId, ownedScope.workId).first();
      assert.equal(previous.owner_id, `ordinary-reconciler:orphan:${queued[0].id}`);
      assert.ok(Number(previous.expires_at_ms) > now.getTime());
      now = new Date(now.getTime() + 30001);
      successor = await new SqlRuntimeResourceFencePort(sql, clock).acquire({ scope: ownedScope, ownerId: 'lookup-successor', ttlMs: 30000 });
      assert.equal(successor.type, 'acquired');
      assert.equal(successor.fence.generation, Number(previous.generation) + 1);
      return nativeDisks.get(id);
    };
    const client = {
      async getInfo(id) { return await lookup(id) ?? null; },
      async get(id) { const value = await lookup(id);return value ? { id: value.id, name: value.name, info: async () => value } : null; },
      async remove(id) { effects.push(['remove', id]);nativeDisks.delete(id); },
      async getOrCreate() { assert.fail('cleanup must not allocate'); },
    };
    const reconciler = new NodeRetainedLifecycle({ fences: ownedFences, orphans: queue, providerModule, client,
      ownerId: 'ordinary-reconciler', leaseTtlMs: 30000, fingerprint: async () => assert.fail('no session fingerprint'),
      assertExecutionCurrent: async () => assert.fail('cleanup must not execute a session') });
    await reconciler.reconcileOrphans();
    assert.equal(lookups, 1);assert.deepEqual(effects, []);assert.deepEqual([...nativeDisks], beforeDisk);
    const remaining = await new SqlRuntimeOrphanPort(sql).list({ limit: 128 });
    assert.equal(remaining.length, 1);assert.deepEqual(remaining[0], { ...queued[0], attempts: 1, lastError: 'retained_orphan_cleanup_failed' });
    assert.equal(Number((await sql.prepare('SELECT attempts FROM runtime_resource_orphans WHERE id = ?').bind(queued[0].id).first()).attempts), 1);
    assert.deepEqual(await readPublication(), beforePublication);
    assert.equal(await new SqlRuntimeResourceFencePort(sql, clock).isCurrent(successor.fence), true);
    await ownedFences.release({ fence: successor.fence, reason: 'completed' });
    await queue.resolve({ id: queued[0].id });
    assert.deepEqual(await new SqlRuntimeOrphanPort(sql).list({ limit: 128 }), []);
  }
  for (const nativePath of ['get', 'info', 'owner-info']) for (const operation of ['begin', 'archive']) {
    const { createHash } = await import('node:crypto');
    const { stableDigest } = await load('packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts');
    const retirementScope = { workspaceId: 'retirement-workspace', environmentId: 'environment',
      sessionId: `retirement-${nativePath}-${operation}`, workId: 'tank-retained-session-v1' };
    const retirementFences = new SqlRuntimeResourceFencePort(sql, clock);
    const ordinaryName = 'oma-' + createHash('sha256').update(`litebox:${retirementScope.sessionId}`).digest('hex').slice(0, 32);
    const ownerName = 'oma-' + createHash('sha256').update(JSON.stringify(retirementScope)).digest('hex').slice(0, 32) + '-upgrade-' + 'a'.repeat(32);
    const handle = { provider: 'litebox', runtimeId: 'retirement-native',
      ...(nativePath === 'owner-info' ? { ownershipName: ownerName } : {}) };
    const disk = { id: handle.runtimeId, name: handle.ownershipName ?? ordinaryName,
      state: { running: false, status: 'Stopped' }, files: { marker: 'preserved' } };
    const nativeDisks = new Map([[disk.id, disk]]), effects = [];
    const seed = await retirementFences.acquire({ scope: retirementScope, ownerId: 'seed', ttlMs: 30000 });
    assert.equal(seed.type, 'acquired');
    const manifest = JSON.stringify({ handle, state: 'retained', archived: false, identity: identity('c'), retired: [] });
    assert.equal((await retirementFences.publish({ fence: seed.fence, workspaceCandidate: {
      id: 'retirement-publication', contentHash: stableDigest({ scope: retirementScope, manifest }),
      metadata: { schema: 2, scope: JSON.stringify(retirementScope), manifest } }, outputCandidate: null })).type, 'published');
    await retirementFences.release({ fence: seed.fence, reason: 'completed' });
    const readPublication = () => sql.prepare('SELECT publication_json, publication_generation, revision FROM runtime_resource_fences WHERE workspace_id = ? AND environment_id = ? AND session_id = ? AND work_id = ?')
      .bind(retirementScope.workspaceId, retirementScope.environmentId, retirementScope.sessionId, retirementScope.workId).first();
    const beforePublication = await readPublication(), beforeDisks = structuredClone([...nativeDisks]);
    let resourceFence, interruptions = 0;
    const execution = { current: true };
    const authorize = async () => { if (!execution.current) throw Error('execution_authority_lost'); };
    const interrupt = async boundary => {
      if (boundary !== nativePath) return;
      await Promise.resolve();
      assert.equal(await retirementFences.isCurrent(resourceFence), true);
      execution.current = false; interruptions++;
      assert.equal(await retirementFences.isCurrent(resourceFence), true);
    };
    const client = {
      async getInfo(id) { assert.equal(id, disk.id);await interrupt('owner-info');return nativeDisks.get(id) ?? null; },
      async get(id) {
        assert.equal(id, disk.id);await interrupt('get');
        return nativeDisks.has(id) ? { id, name: disk.name, async info() { await interrupt('info');return disk; } } : null;
      },
      async remove(id) { effects.push(['remove', id]);nativeDisks.delete(id); },
      async getOrCreate() { effects.push(['allocate']);assert.fail('rejected retirement must not allocate'); },
    };
    const retirementProvider = pinnedBoxLiteProvider({ module: providerModule, client,
      allocation: { image: 'controlled-no-vm' }, leaseTtlMs: 30000,
      isCurrent: async fence => { resourceFence = fence;return retirementFences.isCurrent(fence); } });
    const controller = new RetainedSessionLifecycle({ scope: retirementScope, fences: retirementFences,
      provider: retirementProvider, providerId: 'litebox', ownerId: 'retirement-controller', leaseTtlMs: 30000,
      fingerprint: `sha256:${'e'.repeat(64)}`, identity: identity('e') });
    const signal = new AbortController().signal;
    await assert.rejects(operation === 'begin' ? controller.begin(authorize, signal) : controller.archive(authorize, signal), /execution_authority_lost/);
    assert.equal(interruptions, 1);assert.deepEqual(effects, []);
    assert.deepEqual([...nativeDisks], beforeDisks);assert.deepEqual(await readPublication(), beforePublication);
    assert.equal(await retirementFences.isCurrent(resourceFence), false);
    const successor = await retirementFences.acquire({ scope: retirementScope, ownerId: 'successor', ttlMs: 30000 });
    assert.equal(successor.type, 'acquired');
    assert.equal(await retirementFences.isCurrent(successor.fence), true);
    await retirementFences.release({ fence: successor.fence, reason: 'completed' });
    execution.current = true;
    client.getInfo = async id => nativeDisks.get(id) ?? null;
    client.get = async id => nativeDisks.has(id) ? { id, name: disk.name, info: async () => disk } : null;
    await controller.archive(authorize, new AbortController().signal);
    assert.equal(nativeDisks.size, 0);assert.deepEqual(effects, [['remove', disk.id]]);
  }
  assert.deepEqual(await persisted(), beforeOrphanHistory);
  stage = 'model-history';
  assert.equal(messages[2]?.role, 'tool');
  assert.equal(messages[2]?.content?.[0]?.type, 'tool-result');
  assert.equal(messages[2]?.content?.[0]?.toolCallId, 'rejected');
  assert.equal(messages[2]?.content?.[0]?.toolName, 'record_event');
  stage = 'model-error-state';
  try {
    assert.equal(messages[2]?.content?.[0]?.output?.type, 'error-text');
  } catch (error) {
    if (error?.code === 'ERR_ASSERTION') failure = { stage, reason: 'rejected-error-state' };
    throw error;
  }
  stage = 'model-history';
  assert.deepEqual(messages, [
    { role: 'user', content: text('Remember every rejected and corrected call') },
    { role: 'assistant', content: [{ type: 'tool-call', toolCallId: 'rejected', toolName: 'record_event', input: { precision: 'MINUTE' } }] },
    { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'rejected', toolName: 'record_event', output: { type: 'error-text', value: 'Use HOUR' } }] },
    { role: 'assistant', content: [{ type: 'tool-call', toolCallId: 'corrected', toolName: 'record_event', input: { precision: 'HOUR' } }] },
    { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'corrected', toolName: 'record_event', output: { type: 'content', value: text('Recorded') } }] },
  ]);
} catch (error) {
  scenarioFailed = true;
  failure ??= { stage, reason: reason(error) };
  console.log('TANK_OPENMA_POSTGRES_FAILURE_V1 ' + JSON.stringify({ qualification: 'postgres-source-only', status: 'FAIL', ...failure }));
  failureReported = true;
  throw error;
} finally {
  stage = 'cleanup';
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
console.log('TANK_OPENMA_POSTGRES_SUCCESS_V1 ' + JSON.stringify({ qualification: 'postgres-source-only', shippingEntrypointExercised: false,
  nativeVmExercised: false, retainedHistory: true, callbackIdempotence: true,
  executionAndResourceFences: true, settlementAndReopen: true }));
} catch (error) {
  if (!failureReported) console.log('TANK_OPENMA_POSTGRES_FAILURE_V1 ' + JSON.stringify({ qualification: 'postgres-source-only', status: 'FAIL',
    ...(failure ?? { stage, reason: reason(error) }) }));
  throw error;
}

import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, test } from 'vitest';
import postgres from 'postgres';
import { createPostgresSqlClient } from '@open-managed-agents/sql-client/adapters/postgres';
import type { SqlClient } from '@open-managed-agents/sql-client';
import { SqlSessionEventStore } from '@open-managed-agents/session-event-store-sql';
import { SqlSessionRuntimeProjectionPersistence } from '@open-managed-agents/managed-agents-adapters-sql';
import type { Session, SentSessionEvent } from '@open-managed-agents/domain/sessions';
import type { ProjectSessionRuntimeState } from '@open-managed-agents/managed-agents-application';
import { ensureSessionExecutionCoordinatorSchema, SqlSessionExecutionCoordinator, type SqlSessionExecutionStore } from '@open-managed-agents/session-runtime-sql/coordination';
import { loadSessionContinuationSnapshot } from '@open-managed-agents/session-runtime-sql/continuation';
import { encodeTankContinuationProjectionDocument, planSessionContinuation } from '@open-managed-agents/session-runtime-contract/continuation';
import type { ClaimSessionExecutionResult, YieldSessionExecutionProgress } from '@open-managed-agents/session-runtime-contract/coordination';
import { requireDisposablePostgresDsn } from '../../../../../scripts/openma-persistence/database.mts';

const cleanup: Array<() => Promise<void>> = [];
const at = (seconds: number): string => new Date(Date.UTC(2026, 9, 26, 0, 0, seconds)).toISOString();
type ClaimedExecution = Extract<ClaimSessionExecutionResult, {type: 'claimed'}>;
interface Fixture {
  sql: SqlClient;
  events: SqlSessionEventStore;
  coordinator: SqlSessionExecutionStore;
  scope: {workspaceId: string; sessionId: string};
  session: Session;
}
interface WireRow {wire: string}
interface StoredState {sessions: string[]; events: string[]; executions: string[]}

afterEach(async (): Promise<void> => {
  while (cleanup.length > 0) {
    const operation = cleanup.pop();
    if (operation === undefined) throw new Error('Owned projection cleanup disappeared');
    await operation();
  }
});

async function fixture(): Promise<Fixture> {
  const dsn = requireDisposablePostgresDsn(process.env.TANK_TEST_DATABASE_URL, process.env.TANK_OPENMA_PERSISTENCE_DATABASE_HOST);
  const schema = 'projection_' + randomUUID().replaceAll('-', '');
  const admin = postgres(dsn);
  try { await admin.unsafe('CREATE SCHEMA ' + schema); }
  catch (error) { await admin.end(); throw error; }
  cleanup.push(async (): Promise<void> => {
    try {
      const deadline = Date.now() + 3000;
      while (true) {
        const rows = await admin.unsafe<{connections: number}[]>('SELECT COUNT(*)::int AS connections FROM pg_stat_activity WHERE application_name=$1', [schema]);
        assert.ok(rows[0] !== undefined);
        if (rows[0].connections === 0) break;
        assert.ok(Date.now() < deadline, 'Owned public factory connections did not drain');
        await delay(25);
      }
      await admin.unsafe('DROP SCHEMA ' + schema + ' CASCADE');
    } finally { await admin.end(); }
  });
  const configured = new URL(dsn);
  configured.searchParams.set('search_path', schema);
  configured.searchParams.set('application_name', schema);
  configured.searchParams.set('idle_timeout', '0.1');
  const sql = await createPostgresSqlClient(configured.toString());
  await sql.exec(`
    CREATE TABLE managed_sessions (workspace_id TEXT NOT NULL, id TEXT NOT NULL, document TEXT NOT NULL, revision BIGINT NOT NULL, agent_id TEXT, agent_version BIGINT, environment_id TEXT, deployment_id TEXT, status TEXT, created_at BIGINT, updated_at BIGINT, archived_at BIGINT, PRIMARY KEY(workspace_id,id));
    CREATE TABLE managed_session_initial_events (workspace_id TEXT, session_id TEXT, sequence BIGINT, document TEXT);
    CREATE TABLE managed_session_events (workspace_id TEXT NOT NULL, session_id TEXT NOT NULL, thread_id TEXT, id TEXT NOT NULL, type TEXT NOT NULL, document TEXT NOT NULL, processed_at BIGINT NOT NULL, PRIMARY KEY(workspace_id,session_id,id));
  `);
  await ensureSessionExecutionCoordinatorSchema(sql);
  const scope = {workspaceId: 'projection-scope', sessionId: 'session'};
  const session: Session = {
    id: scope.sessionId, agent: {id: 'agent', name: 'Projection proof', description: null, mcpServers: [], model: {id: 'deterministic'}, multiagent: null, skills: [], system: null, tools: [], version: 1},
    archivedAt: null, budget: null, createdAt: at(0), environmentId: 'fixture-environment', metadata: {}, outcomeEvaluations: [], resources: [], stats: {}, status: 'idle', title: null,
    updatedAt: at(0), usage: {}, vaultIds: [],
  };
  await sql.prepare('INSERT INTO managed_sessions(workspace_id,id,document,revision,status,created_at,updated_at) VALUES(?,?,?,1,?,?,?)')
    .bind(scope.workspaceId, scope.sessionId, JSON.stringify(session), session.status, Date.parse(session.createdAt), Date.parse(session.updatedAt)).run();
  return {sql, scope, session, events: new SqlSessionEventStore(sql, {executionOutbox: true}),
    coordinator: new SqlSessionExecutionCoordinator(sql, {callbackContinuations: true, serializeSessionClaims: true})};
}

async function revision(f: Fixture): Promise<number> {
  const row = await f.sql.prepare('SELECT revision::text AS revision FROM managed_sessions WHERE workspace_id=? AND id=?').bind(f.scope.workspaceId, f.scope.sessionId).first<{revision: string}>();
  assert.ok(row !== null && typeof row.revision === 'string');
  const value = Number(row.revision);
  assert.ok(Number.isSafeInteger(value) && value > 0);
  return value;
}
async function append(f: Fixture, events: SentSessionEvent[]): Promise<void> {
  const result = await f.events.append({...f.scope, expectedRevision: await revision(f), events, nextSession: f.session});
  assert.equal(result.type, 'appended');
}
async function claim(f: Fixture, attempt: string, seconds: number): Promise<ClaimedExecution> {
  const result = await f.coordinator.claim({...f.scope, ownerId: 'worker-' + attempt, attemptId: attempt, claimedAt: at(seconds), leaseTtlMs: 30000});
  if (result.type !== 'claimed') throw new Error('Expected the actual execution claim');
  return result;
}
async function wireRows(f: Fixture, query: string): Promise<string[]> {
  const result = await f.sql.prepare(query).all<WireRow>();
  assert.ok(result.results !== undefined);
  return result.results.map(row => { assert.equal(typeof row.wire, 'string'); return row.wire; });
}
async function stored(f: Fixture): Promise<StoredState> {
  return {
    sessions: await wireRows(f, 'SELECT row_to_json(s)::text AS wire FROM managed_sessions s ORDER BY workspace_id,id'),
    events: await wireRows(f, 'SELECT row_to_json(e)::text AS wire FROM managed_session_events e ORDER BY workspace_id,session_id,id'),
    executions: await wireRows(f, 'SELECT row_to_json(x)::text AS wire FROM managed_session_executions x ORDER BY workspace_id,id'),
  };
}
function user(id: string, seconds: number): SentSessionEvent {
  return {id, type: 'user.message', content: [{type: 'text', text: id}], processedAt: at(seconds)};
}

test('actual PostgreSQL generic projection retains the original default shape and replay', async (): Promise<void> => {
  const f = await fixture();
  await append(f, [user('original', 0)]);
  const owned = await claim(f, 'generic-attempt', 1);
  const writer = new SqlSessionRuntimeProjectionPersistence(f.sql, {now: () => new Date(at(2))});
  const command: ProjectSessionRuntimeState = {...f.scope, expectedRevision: await revision(f), executionFence: owned.fence,
    events: [{id: 'generic-output', type: 'session.status_running', processedAt: at(2)}], next: f.session};
  assert.equal((await writer.project(command)).type, 'projected');
  const beforeReplay = await stored(f);
  assert.equal((await writer.project(command)).type, 'projected');
  assert.deepEqual(await stored(f), beforeReplay);
  const entry = (await loadSessionContinuationSnapshot(f.sql, f.scope)).orderedEvents.find(value => value.event.id === 'generic-output');
  assert.ok(entry !== undefined);
  assert.equal(entry.executionId, owned.fence.executionId);
  assert.equal(Object.hasOwn(entry, 'executionMetadata'), false);
});

test('actual PostgreSQL Tank terminal metadata binds yield, exact replay and the next claim', async (): Promise<void> => {
  const f = await fixture();
  await append(f, [user('original', 0)]);
  const original = await claim(f, 'original-attempt', 1);
  const firstWriter = new SqlSessionRuntimeProjectionPersistence(f.sql, {now: () => new Date(at(2))}, encodeTankContinuationProjectionDocument);
  assert.equal((await firstWriter.project({...f.scope, expectedRevision: await revision(f), executionFence: original.fence, next: f.session, events: [
    {id: 'original-start', type: 'session.status_running', processedAt: at(2)},
    {id: 'tool-call', type: 'agent.custom_tool_use', name: 'fixture-tool', input: {}, processedAt: at(2)},
    {id: 'original-idle', type: 'session.status_idle', stopReason: {type: 'requires_action', eventIds: ['tool-call']}, processedAt: at(2)},
  ]})).type, 'projected');
  assert.equal((await f.coordinator.settle({fence: original.fence, settledAt: at(3), outcome: 'completed'})).type, 'settled');
  await append(f, [{id: 'callback', type: 'user.custom_tool_result', customToolUseId: 'tool-call', content: [{type: 'text', text: 'actual callback'}], processedAt: at(4)}, user('later', 4)]);
  const owned = await claim(f, 'callback-attempt', 5);
  const writer = new SqlSessionRuntimeProjectionPersistence(f.sql, {now: () => new Date(at(6))}, encodeTankContinuationProjectionDocument);
  const command: ProjectSessionRuntimeState = {...f.scope, expectedRevision: await revision(f), executionFence: owned.fence, next: f.session, events: [
    {id: 'callback-start', type: 'session.status_running', processedAt: at(6)},
    {id: 'callback-output', type: 'agent.message', content: [{type: 'text', text: 'callback consumed'}], processedAt: at(6)},
    {id: 'callback-idle', type: 'session.status_idle', stopReason: {type: 'end_turn'}, processedAt: at(6)},
  ]};
  assert.equal((await writer.project(command)).type, 'projected');
  const beforeProjectionReplay = await stored(f);
  assert.equal((await writer.project(command)).type, 'projected');
  assert.deepEqual(await stored(f), beforeProjectionReplay);
  const snapshot = await loadSessionContinuationSnapshot(f.sql, f.scope);
  const terminal = snapshot.orderedEvents.find(entry => entry.event.id === 'callback-idle');
  assert.ok(terminal !== undefined);
  assert.deepEqual(terminal.executionMetadata, {attemptId: owned.fence.attemptId, generation: owned.fence.generation});
  const plan = planSessionContinuation(snapshot, owned.execution.id);
  if (plan.mode === 'blocked' || plan.mode === 'failure' || plan.phaseStartEventId === undefined) throw new Error('Expected an actual terminal continuation plan');
  assert.equal(plan.mode, 'finalize');
  assert.deepEqual(plan.phaseInputIds, ['callback']);
  assert.deepEqual(plan.remainingInputIds, ['later']);
  const yieldInput: YieldSessionExecutionProgress = {fence: owned.fence, yieldedAt: at(7), previousReceiptId: plan.previousReceiptId,
    phaseStartEventId: plan.phaseStartEventId, terminalEventId: terminal.event.id, phaseInputIds: plan.phaseInputIds,
    consumedInputIds: plan.consumedInputIds, remainingInputIds: plan.remainingInputIds};
  const yielded = await f.coordinator.yieldProgress(yieldInput);
  assert.equal(yielded.type, 'yielded');
  if (yielded.type !== 'yielded') throw new Error('Expected an actual durable phase yield');
  assert.equal(yielded.replayed, false);
  const beforeYieldReplay = await stored(f);
  assert.deepEqual(await f.coordinator.yieldProgress(yieldInput), {...yielded, replayed: true});
  assert.deepEqual(await stored(f), beforeYieldReplay);
  const next = await claim(f, 'next-attempt', 8);
  assert.equal(next.execution.id, owned.execution.id);
  assert.notEqual(next.fence.generation, owned.fence.generation);
  const nextPlan = planSessionContinuation(await loadSessionContinuationSnapshot(f.sql, f.scope), next.execution.id);
  assert.equal(nextPlan.mode, 'model');
  assert.deepEqual(nextPlan.phaseInputIds, ['later']);
  const beforeRefusal = await stored(f);
  assert.equal((await writer.project({...command, expectedRevision: await revision(f), events: [{id: 'stale-output', type: 'session.status_running', processedAt: at(8)}]})).type, 'execution_fence_lost');
  await assert.rejects(f.coordinator.yieldProgress({...yieldInput, fence: next.fence, yieldedAt: at(9)}), /Conflicting phase-yield replay/);
  assert.deepEqual(await stored(f), beforeRefusal);
});

test('actual PostgreSQL Tank projection refuses missing or malformed fence before effects', async (): Promise<void> => {
  const f = await fixture();
  await append(f, [user('original', 0)]);
  const owned = await claim(f, 'refusal-attempt', 1);
  const writer = new SqlSessionRuntimeProjectionPersistence(f.sql, {now: () => new Date(at(2))}, encodeTankContinuationProjectionDocument);
  const command: ProjectSessionRuntimeState = {...f.scope, expectedRevision: await revision(f), next: f.session,
    events: [{id: 'unfenced-output', type: 'session.status_running', processedAt: at(2)}]};
  const before = await stored(f);
  await assert.rejects(writer.project(command), /requires an execution fence/);
  await assert.rejects(writer.project({...command, executionFence: {...owned.fence, generation: 0}}), /Invalid persisted continuation history source/);
  assert.deepEqual(await stored(f), before);
});

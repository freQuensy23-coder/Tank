import assert from 'node:assert/strict';
import test from 'node:test';
import { createBetterSqlite3SqlClient } from '/app/packages/sql-client/src/index.ts';
import { ensureSessionExecutionCoordinatorSchema, SqlSessionExecutionCoordinator } from '/app/packages/session-runtime-sql/src/coordination.ts';
import { encodeSessionEventDocument } from '/app/packages/session-runtime-contract/src/history.ts';
import { loadSessionContinuationSnapshot } from '/app/packages/session-runtime-sql/src/continuation.ts';
import { planSessionContinuation } from '/app/packages/session-runtime-contract/src/continuation.ts';
const at = n => new Date(Date.UTC(2026, 9, 20, 0, 0, n)).toISOString();
const user = id => ({ id, type: 'user.message', content: [{ type: 'text', text: id }] });
const reply = (id, call) => ({ id, type: 'user.custom_tool_result', customToolUseId: call, content: [{ type: 'text', text: 'captured result' }] });

async function setup(calls = ['call']) {
  const sql = await createBetterSqlite3SqlClient(':memory:');
  await ensureSessionExecutionCoordinatorSchema(sql);
  await sql.exec(`CREATE TABLE managed_sessions (workspace_id TEXT, id TEXT, revision INTEGER, archived_at INTEGER);
    CREATE TABLE managed_session_initial_events (workspace_id TEXT,session_id TEXT,document TEXT,sequence INTEGER);
    CREATE TABLE managed_session_events (workspace_id TEXT,session_id TEXT,document TEXT,processed_at INTEGER,id TEXT);`);
  const coordinator = new SqlSessionExecutionCoordinator(sql, { callbackContinuations: true, serializeSessionClaims: true });
  async function session(id) { await sql.prepare('INSERT INTO managed_sessions(workspace_id,id,revision) VALUES (?,?,?)').bind('w', id, 10).run(); }
  async function admit(id, sid, time, events) { return coordinator.admit({ execution: {
    id, workspaceId: 'w', sessionId: sid, admittedAt: at(time), events,
  } }); }
  await session('active'); await session('cold-1'); await session('cold-2');
  await admit('origin', 'active', 0, [user('original-input')]);
  await sql.prepare("UPDATE managed_session_executions SET state='completed' WHERE id='origin'").run();
  const history = calls.map(id => ({ id, type: 'agent.custom_tool_use', name: 'get_item', input: { item_id: id } }));
  history.push({ id: 'waiting', type: 'session.status_idle', stopReason: { type: 'requires_action', eventIds: calls } });
  for (const [index, event] of history.entries()) {
    await sql.prepare('INSERT INTO managed_session_events VALUES (?,?,?,?,?)').bind('w', 'active',
      encodeSessionEventDocument(event, { revision: index + 1, index: 0 }, 'origin'), index, event.id).run();
  }
  await admit('cold-first', 'cold-1', 1, [user('new-1')]);
  await admit('cold-second', 'cold-2', 2, [user('new-2')]);
  let attempt = 0;
  const claim = () => coordinator.claim({ ownerId: 'worker', attemptId: 'attempt-' + (++attempt), claimedAt: at(10 + attempt), leaseTtlMs: 30000 });
  return { sql, coordinator, admit, claim };
}

test('a tool result resumes active analysis ahead of older fresh sessions', async () => {
  const f = await setup();
  await f.admit('callback', 'active', 5, [reply('result', 'call')]);
  const selected = await f.claim();
  assert.equal(selected.type, 'claimed');
  assert.equal(selected.execution.id, 'callback');
  const plan = planSessionContinuation(await loadSessionContinuationSnapshot(f.sql, {workspaceId:'w', sessionId:'active'}), 'callback');
  assert.equal(plan.mode, 'model');
  await f.coordinator.settle({ fence: selected.fence, settledAt: at(12), outcome: 'completed' });
  assert.equal((await f.claim()).execution.id, 'cold-first');
  assert.equal((await f.claim()).execution.id, 'cold-second');
});

test('priority does not let a partial callback group invoke the model early', async () => {
  const f = await setup(['call-a', 'call-b']);
  await f.admit('callback-a', 'active', 5, [reply('result-a', 'call-a')]);
  assert.equal((await f.claim()).execution.id, 'callback-a');
  const plan = planSessionContinuation(await loadSessionContinuationSnapshot(f.sql, {workspaceId:'w', sessionId:'active'}), 'callback-a');
  assert.equal(plan.mode, 'intake');
});

test('ordinary new work preserves its existing FIFO order', async () => {
  const f = await setup();
  assert.equal((await f.claim()).execution.id, 'cold-first');
  assert.equal((await f.claim()).execution.id, 'cold-second');
});

import { NodeSessionExecutionWorker } from '/app/packages/control-plane-node/src/lib/node-session-execution-worker.ts';
test('shutdown drains the claimed turn and leaves queued work for the next controller', async () => {
  const sql = await createBetterSqlite3SqlClient(':memory:');
  await ensureSessionExecutionCoordinatorSchema(sql);
  const coordinator = new SqlSessionExecutionCoordinator(sql);
  for (const id of ['one','two']) await coordinator.admit({execution:{id,workspaceId:'w',sessionId:id,admittedAt:at(1),events:[user(id)]}});
  let started, release;
  const running = new Promise(resolve => { started = resolve; });
  const finish = new Promise(resolve => { release = resolve; });
  const seen = [];
  let attempt = 0;
  const worker = new NodeSessionExecutionWorker({coordinator,ownerId:'worker',maxConcurrent:1,
    clock:{now:()=>new Date(at(2))}, ids:{nextAttemptId:()=>String(++attempt)},
    context:{find:async scope=>({session:{id:scope.sessionId},environment:{id:'env'},revision:1})},
    runtime:{run:async ()=>{seen.push('run');started();await finish;},cancel:async ()=>{}},onError:error=>{throw error;}});
  await worker.poll();await running;
  worker.stop();release();await worker.waitForIdle();
  assert.equal(seen.length,1);
  assert.equal((await coordinator.find({workspaceId:'w',executionId:'two'})).state,'queued');
});

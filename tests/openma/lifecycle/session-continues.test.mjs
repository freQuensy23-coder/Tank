const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { fileURLToPath, pathToFileURL } from 'node:url';
import { registerHooks } from 'node:module';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import assert from 'node:assert/strict';

const openmaRoot = fileURLToPath(openmaSource);
const workspacePackages = new Map();
for (const group of ['packages', 'apps']) for (const name of readdirSync(join(openmaRoot, group))) {
  const manifest = join(openmaRoot, group, name, 'package.json');
  if (existsSync(manifest)) { const json = JSON.parse(readFileSync(manifest, 'utf8')); workspacePackages.set(json.name, { dir: join(openmaRoot, group, name), json }); }
}
const stubs = {
  '@boxlite-ai/boxlite': 'export class SimpleBox {}',
  nanoid: "import { randomBytes } from 'node:crypto'; export const nanoid = (size = 21) => randomBytes(size).toString('hex').slice(0, size); export const customAlphabet = () => nanoid;",
  '@earendil-works/pi-ai': 'export const getSupportedThinkingLevels = () => [];',
  '@earendil-works/pi-ai/providers/all': 'export const builtinProviders = {};',
  '@openma/common/protocol/managed': "export const sessionInputIdentityPrefix = 'session-input:';",
  croner: 'export class Cron {}',
  standardwebhooks: 'export class Webhook {}',
  fflate: 'export const zipSync = () => new Uint8Array();',
};
registerHooks({resolve(specifier, context, nextResolve) {
  if (Object.hasOwn(stubs, specifier)) return {url: 'data:text/javascript,' + encodeURIComponent(stubs[specifier]), shortCircuit: true};
  if (specifier.startsWith('@open-managed-agents/')) {
    const name = specifier.split('/').slice(0, 2).join('/'), found = workspacePackages.get(name);
    let target = found?.json.exports?.['.' + specifier.slice(name.length)];
    while (target && typeof target === 'object') target = target.import ?? target.default ?? target.types;
    if (target) return {url: pathToFileURL(join(found.dir, target)).href, shortCircuit: true};
  }
  return nextResolve(specifier, context);
}});
const load = path => import(new URL(path, openmaSource));
const { NodeRetainedLifecycle } = await load('packages/control-plane-node/src/lib/tank-retained/node-lifecycle.ts');
const { executableImageIdentity, stableDigest } = await load('packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts');
const { DefaultNodeManagedSessionRunner } = await load('packages/control-plane-node/src/lib/node-managed-session-runner.ts');
const { ApplicationBackedNodeManagedSessionRuntimeEngine, DefaultNodeManagedSessionRuntimeDriver } = await load('packages/control-plane-node/src/lib/node-managed-session-runtime.ts');
const { LiteBoxSandbox } = await load('packages/sandbox-adapter-litebox/src/litebox.ts');
const { SqlRuntimeOrphanPort, SqlRuntimeResourceFencePort, ensureRuntimeResourceFenceSchema } = await load('packages/runtime-resource-fence-sql/src/index.ts');
const { SqlSessionEventStore } = await load('packages/session-event-store-sql/src/index.ts');
const { SqlSessionRuntimeHistorySource } = await load('packages/session-runtime-sql/src/history.ts');
const { SqlSessionRuntimeProjectionPersistence } = await load('packages/managed-agents-adapters-sql/src/session-runtime-projection-sql-persistence.ts');
const { SessionRuntimeHistoryApplicationService } = await load('packages/managed-agents-application/src/session-execution/history-application.ts');
const { SessionRuntimeProjectionApplicationService } = await load('packages/managed-agents-application/src/session-execution/application.ts');
const { MemorySessionRealtimeHub } = await load('packages/session-realtime-memory/src/index.ts');
const { composeSystemPrompt } = await load('apps/agent/src/harness/platform-guidance.ts');
const { eventsToMessagesAsync } = await load('apps/agent/src/runtime/history.ts');

const workspaceId = 'workspace_A', sessionId = 'session_A', environmentId = 'production_A';
const scope = {workspaceId, environmentId, sessionId, workId: 'tank-retained-session-v1'};
const scopeKey = JSON.stringify([workspaceId, environmentId, sessionId, scope.workId]);
const reminder = 'The sandbox of this session was replaced with a fresh one after a sandbox environment update.';
const sha = value => createHash('sha256').update(value).digest('hex');
const boxName = 'oma-' + sha('litebox:' + sessionId).slice(0, 32);
const upgradeName = 'oma-' + sha(JSON.stringify(scope)).slice(0, 32) + '-upgrade-' + 'b'.repeat(32);

function sqlite(db) {
  const run = (text, params) => {
    const statement = db.prepare(text);
    if (statement.columns().length) { const results = statement.all(...params); return {results, success: true, meta: {changes: results.length}}; }
    const result = statement.run(...params);
    return {results: [], success: true, meta: {changes: Number(result.changes), last_row_id: Number(result.lastInsertRowid)}};
  };
  const prepared = (text, params = []) => ({
    text, params,
    bind: (...values) => prepared(text, values.map(value => value === undefined ? null : value)),
    run: async () => run(text, params),
    first: async () => run(text, params).results[0] ?? null,
    all: async () => ({results: run(text, params).results}),
  });
  return {
    prepare: text => prepared(text),
    exec: async text => { db.exec(text); },
    batch: async statements => {
      db.exec('BEGIN');
      try { const results = statements.map(statement => run(statement.text, statement.params)); db.exec('COMMIT'); return results; }
      catch (error) { db.exec('ROLLBACK'); throw error; }
    },
  };
}

async function sandboxImage(root, marker) {
  await mkdir(join(root, 'blobs', 'sha256'), {recursive: true});
  const blob = async (value, mediaType) => {
    const bytes = Buffer.isBuffer(value) ? value : Buffer.from(JSON.stringify(value));
    await writeFile(join(root, 'blobs', 'sha256', sha(bytes)), bytes);
    return {digest: 'sha256:' + sha(bytes), size: bytes.length, mediaType};
  };
  const architecture = process.arch === 'arm64' ? 'arm64' : 'amd64', layer = Buffer.from('rootfs-' + marker);
  const config = await blob({architecture, os: 'linux', rootfs: {type: 'layers', diff_ids: ['sha256:' + sha(layer)]}, config: {Env: ['IMAGE=' + marker]}}, 'application/vnd.oci.image.config.v1+json');
  const manifest = await blob({schemaVersion: 2, config, layers: [await blob(layer, 'application/vnd.oci.image.layer.v1.tar')]}, 'application/vnd.oci.image.manifest.v1+json');
  await writeFile(join(root, 'index.json'), JSON.stringify({schemaVersion: 2, manifests: [{...manifest, platform: {architecture, os: 'linux'}}]}));
  return root;
}

function boxHost() {
  const boxes = new Map(), host = {boxes, removed: [], dieInCreate: null};
  let next = 0;
  const named = name => [...boxes.values()].find(box => box.name === name) ?? null;
  const info = box => ({id: box.id, name: box.name, state: {status: box.running ? 'Running' : 'Stopped', running: box.running}});
  const stream = text => new Response(text).body;
  const runtime = box => ({
    runtimeHandle: () => ({provider: 'litebox', runtimeId: box.id}),
    async suspend() { box.running = false; },
    async spawnDuplexProcess({command, args}) {
      assert.equal(boxes.get(box.id), box);
      assert.equal(box.running, true);
      let stdout = '', code = 0;
      const script = command === 'sh' ? args[1] : '', write = /^printf %s (\S+) > (\S+)$/.exec(script), read = /^cat (\S+)$/.exec(script);
      if (write) box.files.set(write[2], write[1]);
      else if (read && box.files.has(read[1])) stdout = box.files.get(read[1]);
      else if (read) { stdout = 'cat: ' + read[1] + ': No such file or directory'; code = 1; }
      else assert.deepEqual([command, ...args], ['mkdir', '-p', '/workspace']);
      return {stdin: new WritableStream(), stdout: stream(stdout), stderr: stream(''), exited: Promise.resolve({code, signal: null}), async kill() {}};
    },
  });
  const nameOf = (options, context) => options.runtimeName?.(context) ?? 'oma-' + sha(options.providerId + ':' + context.sessionId).slice(0, 32);
  host.put = (name, image) => { const box = {id: 'box_' + (++next), name, image, running: false, files: new Map()}; boxes.set(box.id, box); return box; };
  host.module = {
    createBoxLiteProvider: options => ({
      async create(context) {
        const name = nameOf(options, context);
        if (host.dieInCreate === 'before') await new Promise(() => {});
        const box = named(name) ?? host.put(name, options.allocationOptions().rootfsPath);
        if (host.dieInCreate === 'after') await new Promise(() => {});
        box.running = true;
        return runtime(box);
      },
      async resume(handle, context) {
        const box = boxes.get(handle.runtimeId);
        if (!box) throw Error('BoxLite runtime not found: ' + handle.runtimeId);
        assert.equal(box.name, nameOf(options, context));
        box.running = true;
        return runtime(box);
      },
    }),
    createBoxLiteManagedRuntime: options => ({sandbox: {async reap({lease, scope: owner}) {
      const box = boxes.get(lease.runtimeId);
      if (!box) return;
      assert.equal(box.name, nameOf(options, {sessionId: owner.sessionId}));
      boxes.delete(box.id); host.removed.push(box.id);
    }}}),
  };
  const handle = box => ({id: box.id, name: box.name, info: () => info(box), async start() { box.running = true; }, async stop() { box.running = false; }});
  host.client = {
    async getInfo(key) { const box = boxes.get(key) ?? named(key); return box ? info(box) : null; },
    async get(id) { const box = boxes.get(id); return box ? handle(box) : null; },
    async remove(id) { if (boxes.delete(id)) host.removed.push(id); },
  };
  return host;
}

async function world(t) {
  const root = await mkdtemp(join(tmpdir(), 'tank-session-continues-'));
  const db = new DatabaseSync(':memory:'), sql = sqlite(db), migrations = join(openmaRoot, 'apps/main-node/migrations-sqlite');
  for (const name of readdirSync(migrations).filter(name => name.endsWith('.sql')).sort())
    for (const statement of readFileSync(join(migrations, name), 'utf8').split('--> statement-breakpoint')) if (statement.trim()) db.exec(statement);
  await ensureRuntimeResourceFenceSchema(sql);
  const requests = [];
  const model = createServer((request, response) => {
    let body = '';
    request.on('data', chunk => { body += chunk; }).on('end', () => {
      const received = JSON.parse(body), last = received.messages.at(-1).content.map(part => part.text).join('');
      requests.push(received);
      response.end(JSON.stringify(last.startsWith('sh: ') ? {run: last.slice(4)} : {text: 'answer to ' + last}));
    });
  });
  await new Promise(resolve => model.listen(0, '127.0.0.1', resolve));
  t.after(async () => { model.close(); db.close(); await rm(root, {recursive: true, force: true}); });
  const created = Date.parse('2026-11-01T00:00:00.000Z'), iso = new Date(created).toISOString();
  const session = {id: sessionId, agent: {id: 'agent_A', description: null, mcpServers: [], model: {id: 'model_A'}, multiagent: null, name: 'Agent',
    skills: [], system: 'You are the trading agent.', tools: [], version: 1}, archivedAt: null, budget: null, createdAt: iso, environmentId,
    metadata: {}, outcomeEvaluations: [], resources: [], stats: {}, status: 'idle', title: null, updatedAt: iso, usage: {}, vaultIds: []};
  db.prepare(`INSERT INTO managed_sessions (id, workspace_id, document, revision, agent_id, agent_version, environment_id, status, created_at, updated_at)
    VALUES (?, ?, ?, 1, ?, 1, ?, 'idle', ?, ?)`).run(sessionId, workspaceId, JSON.stringify(session), session.agent.id, environmentId, created, created);
  const state = {db, sql, root, requests, host: boxHost(), elapsedMs: 0, turn: 0, modelUrl: 'http://127.0.0.1:' + model.address().port,
    environment: {id: environmentId, archivedAt: null, config: {type: 'self_hosted'}, createdAt: iso, description: null, metadata: {}, name: 'tank', updatedAt: iso}};
  state.now = () => new Date(Date.now() + state.elapsedMs);
  state.tick = () => new Date(created + (++state.turn) * 1000);
  state.image = async marker => sandboxImage(join(root, 'image-' + marker), marker);
  state.publication = () => JSON.parse(db.prepare('SELECT publication_json FROM runtime_resource_fences WHERE scope_key = ?').get(scopeKey).publication_json).workspaceCandidate;
  state.publish = candidate => db.prepare('UPDATE runtime_resource_fences SET publication_json = ? WHERE scope_key = ?').run(JSON.stringify({outputCandidate: null, workspaceCandidate: candidate}), scopeKey);
  state.manifest = () => JSON.parse(state.publication().metadata.manifest);
  state.publishManifest = manifest => {
    const encoded = JSON.stringify(manifest);
    state.publish({id: 'retained-' + manifest.handle.runtimeId, contentHash: stableDigest({scope, manifest: encoded}),
      metadata: {schema: 2, scope: JSON.stringify(scope), manifest: encoded, provider: manifest.handle.provider, runtimeId: manifest.handle.runtimeId, state: manifest.state}});
  };
  state.messages = () => db.prepare(`SELECT type, document FROM managed_session_events WHERE session_id = ? AND type IN ('user.message', 'agent.message') ORDER BY rowid`)
    .all(sessionId).map(row => [row.type, JSON.stringify(JSON.parse(row.document)).match(/"text":"([^"]*)"/)[1]]);
  return state;
}

function node(state, rootfsPath) {
  const {sql, host} = state, ownerId = 'node_' + randomUUID();
  const executionCurrent = async fence => {
    const row = await sql.prepare(`SELECT 1 AS present FROM managed_session_executions WHERE workspace_id = ? AND id = ? AND session_id = ? AND state = 'running'
      AND attempt_id = ? AND owner_id = ? AND generation = ? AND lease_expires_at_ms > ?`)
      .bind(fence.workspaceId, fence.executionId, fence.sessionId, fence.attemptId, fence.ownerId, fence.generation, state.now().getTime()).first();
    if (!row) throw new Error('Retained Session execution fence lost');
  };
  const components = (context, allocation) => ({scope: [context.workspaceId, context.environment.id, context.sessionId],
    resources: context.session.resources, skills: context.session.agent.skills, environment: context.environment.config,
    runtimeResources: {cpus: allocation.cpus ?? null, memoryMib: allocation.memoryMib ?? null}});
  const lifecycle = new NodeRetainedLifecycle({
    fences: new SqlRuntimeResourceFencePort(sql, {now: state.now}), orphans: new SqlRuntimeOrphanPort(sql), providerModule: host.module, client: host.client,
    ownerId, leaseTtlMs: 30000, assertExecutionCurrent: executionCurrent,
    fingerprint: async (context, allocation) => stableDigest({...components(context, allocation), allocation, rootfsIndex: stableDigest(readFileSync(join(allocation.rootfsPath, 'index.json'), 'utf8'))}),
    identity: async (context, allocation) => ({version: 2, image: await executableImageIdentity(allocation.rootfsPath),
      compatibility: Object.fromEntries(Object.entries(components(context, allocation)).map(([name, value]) => [name, stableDigest(value)]))}),
  });
  const runner = new DefaultNodeManagedSessionRunner({
    harnessRetry: {attempts: 1, delayMs: () => 0},
    confirmedTools: {execute: async () => { throw new Error('unexpected confirmed tool execution'); }},
    outcomes: {evaluate: async () => { throw new Error('unexpected outcome evaluation'); }},
    buildSandbox: async () => new LiteBoxSandbox({image: 'tank-sandbox', rootfsPath, name: 'oma-' + sessionId, scratchRoot: state.root}),
    retainedLifecycle: {
      bind: input => lifecycle.bind({...input, sessionId: input.session.id}),
      idle: input => lifecycle.idle({...input, sessionId: input.session.id}),
      abandon: input => lifecycle.abandon({...input, sessionId: input.session.id}),
      archive: input => lifecycle.archive(input),
    },
    prepareSandbox: async ({sandbox}) => { await sandbox.prepareRetainedRuntime(); },
    buildModel: async () => ({}),
    buildTools: async () => ({}),
    buildHarness: () => ({run: async ctx => {
      const messages = await eventsToMessagesAsync(ctx.runtime.history.getEvents(), ctx.fileFetcher);
      const response = await fetch(state.modelUrl, {method: 'POST', body: JSON.stringify({system: ctx.systemPrompt, messages})});
      const reply = await response.json();
      ctx.runtime.broadcast({type: 'agent.message', content: [{type: 'text', text: reply.run ? await ctx.runtime.sandbox.exec(reply.run) : reply.text}]});
    }}),
    buildHarnessContext: async input => {
      const rawSystemPrompt = input.session.agent.system ?? '';
      const runtimeReplaced = lifecycle.takeRuntimeReplacedReminder({...input, sessionId: input.session.id});
      const platformReminders = runtimeReplaced ? [{source: 'sandbox-runtime-replaced', text: runtimeReplaced}] : [];
      return {agent: input.session.agent, userMessage: {type: 'user.message', content: []}, session_id: input.session.id, tenant_id: input.workspaceId,
        tools: input.tools, model: input.model, systemPrompt: composeSystemPrompt(rawSystemPrompt, platformReminders), rawSystemPrompt, platformReminders, env: {}, runtime: input.runtime};
    },
    clock: {now: state.tick},
    ids: {nextEventId: () => 'sevt_' + randomUUID()},
  });
  const history = new SqlSessionRuntimeHistorySource(sql), projection = new SqlSessionRuntimeProjectionPersistence(sql), events = new SqlSessionEventStore(sql, {executionOutbox: true});
  const driver = new DefaultNodeManagedSessionRuntimeDriver({
    engine: new ApplicationBackedNodeManagedSessionRuntimeEngine({runner,
      historyFor: workspace => new SessionRuntimeHistoryApplicationService({workspaceId: workspace, source: history})}),
    realtime: new MemorySessionRealtimeHub(),
    projectionFor: workspace => new SessionRuntimeProjectionApplicationService({workspaceId: workspace, persistence: projection}),
    clock: {now: state.tick},
    startRetry: {attempts: 1, delayMs: () => 0},
  });
  const execute = async event => {
    const now = state.now().getTime(), attemptId = 'attempt_' + randomUUID();
    const claimed = await sql.prepare(`UPDATE managed_session_executions SET state = 'running', attempt_id = ?, owner_id = ?, generation = generation + 1,
      attempt_count = attempt_count + 1, claimed_at_ms = ?, lease_expires_at_ms = ?, revision = revision + 1
      WHERE workspace_id = ? AND id = ? AND (state = 'queued' OR lease_expires_at_ms <= ?) RETURNING generation`)
      .bind(attemptId, ownerId, now, now + 30000, workspaceId, event.id, now).first();
    assert.ok(claimed, 'execution is claimable');
    const current = await projection.findCurrent({workspaceId, sessionId});
    await driver.accept({workspaceId, sessionId, session: current.session, environment: state.environment, events: [event],
      executionFence: {executionId: event.id, workspaceId, sessionId, attemptId, ownerId, generation: Number(claimed.generation), expiresAt: new Date(now + 30000).toISOString()}});
    await sql.prepare(`UPDATE managed_session_executions SET state = 'succeeded', settled_at_ms = ?, revision = revision + 1 WHERE workspace_id = ? AND id = ?`)
      .bind(state.now().getTime(), workspaceId, event.id).run();
    return state.requests.at(-1);
  };
  const send = async text => {
    const current = await projection.findCurrent({workspaceId, sessionId}), processedAt = state.tick().toISOString();
    const event = {id: 'sevt_user_' + state.turn, type: 'user.message', content: [{type: 'text', text}], processedAt};
    const appended = await events.append({workspaceId, sessionId, expectedRevision: current.revision, events: [event],
      nextSession: {...current.session, status: 'running', updatedAt: processedAt}});
    assert.equal(appended.type, 'appended');
    return event;
  };
  return {send, execute, turn: async text => execute(await send(text))};
}

const restart = (state, rootfsPath) => { state.elapsedMs += 31000; state.host.dieInCreate = null; return node(state, rootfsPath); };
const said = request => request.messages.map(message => [message.role, message.content.map(part => part.text).join('')]);
const earlier = [['user', 'remember the alpha plan'], ['assistant', 'answer to remember the alpha plan'],
  ['user', 'sh: printf %s alpha > /workspace/plan'], ['assistant', ''], ['user', 'sh: cat /workspace/plan'], ['assistant', 'alpha']];

async function conversation(t) {
  const state = await world(t), first = node(state, await state.image('v1'));
  await first.turn('remember the alpha plan');
  await first.turn('sh: printf %s alpha > /workspace/plan');
  const before = await first.turn('sh: cat /workspace/plan');
  assert.deepEqual(said(before), earlier.slice(0, 5));
  assert.equal(before.system.includes(reminder), false);
  assert.equal(state.manifest().state, 'retained');
  return state;
}

async function continues(state, current, {pending, reminded = true, gone = []} = {}) {
  const request = pending ? await current.execute(pending) : await current.turn('what was the plan?');
  assert.deepEqual(said(request), [...earlier, ['user', 'what was the plan?']]);
  assert.equal(request.system.startsWith('You are the trading agent.'), true);
  assert.equal(request.system.includes(reminder), reminded);
  assert.equal(state.manifest().state, 'retained');
  assert.deepEqual([...state.host.boxes.values()].map(box => box.name), [boxName]);
  for (const id of gone) assert.equal(state.host.removed.includes(id), true);
  const after = await current.turn('sh: cat /workspace/plan');
  assert.equal(after.system.includes(reminder), false);
  assert.deepEqual(said(after).slice(0, 8), [...earlier, ['user', 'what was the plan?'], ['assistant', 'answer to what was the plan?']]);
  assert.equal(state.messages().at(-1)[1].includes('No such file'), true);
}

test('a retained runtime of an older image is replaced and the model still receives the earlier conversation', async t => {
  const state = await conversation(t), old = [...state.host.boxes.keys()];
  await continues(state, restart(state, await state.image('v2')), {gone: old});
});

for (const [name, dieInCreate, leftover] of [['after its runtime was created', 'after', 1], ['before its runtime was created', 'before', 0]]) {
  test(`an allocation of an older image interrupted ${name} is replaced and the model still receives the earlier conversation`, async t => {
    const state = await conversation(t), interrupted = restart(state, await state.image('v2'));
    state.host.dieInCreate = dieInCreate;
    const pending = await interrupted.send('what was the plan?');
    void interrupted.execute(pending);
    await t.waitFor(() => assert.equal(state.manifest().state, 'allocating'));
    await t.waitFor(() => assert.equal(state.host.boxes.size, leftover));
    const old = [...state.host.boxes.keys()];
    await continues(state, restart(state, await state.image('v3')), {pending, gone: old});
  });
}

test('a replacement interrupted before its runtime was created continues after a restart with the same image', async t => {
  const state = await conversation(t), image = await state.image('v2'), interrupted = restart(state, image);
  state.host.dieInCreate = 'before';
  const pending = await interrupted.send('what was the plan?');
  void interrupted.execute(pending);
  await t.waitFor(() => assert.equal(state.manifest().state, 'allocating'));
  assert.equal(state.host.boxes.size, 0);
  await continues(state, restart(state, image), {pending, reminded: false});
});

test('an interrupted upgrade of an older image is replaced and the model still receives the earlier conversation', async t => {
  const state = await conversation(t), manifest = state.manifest(), target = state.host.put(upgradeName, 'interrupted-upgrade');
  const upgrade = {source: manifest.handle, target: {provider: 'litebox', runtimeId: target.id, ownershipName: upgradeName}, owner: 'upgrade_A',
    identity: {...manifest.identity, image: 'sha256:' + 'c'.repeat(64)}};
  state.publishManifest({...manifest, state: 'upgrading', upgrade});
  await continues(state, restart(state, await state.image('v2')), {gone: [manifest.handle.runtimeId, target.id]});
  assert.equal(state.manifest().upgrade, undefined);
});

for (const [name, damage] of [
  ['a broken integrity hash', candidate => ({...candidate, contentHash: 'sha256:' + '0'.repeat(64)})],
  ['an unknown state', candidate => { const manifest = {...JSON.parse(candidate.metadata.manifest), state: 'migrating'}, encoded = JSON.stringify(manifest);
    return {...candidate, contentHash: stableDigest({scope, manifest: encoded}), metadata: {...candidate.metadata, manifest: encoded, state: 'migrating'}}; }],
  ['an unreadable manifest', candidate => ({...candidate, metadata: {...candidate.metadata, manifest: '{'}})],
]) {
  test(`a runtime publication with ${name} is replaced and the model still receives the earlier conversation`, async t => {
    const state = await conversation(t), old = [...state.host.boxes.keys()];
    state.publish(damage(state.publication()));
    await continues(state, restart(state, await state.image('v2')), {gone: old});
  });
}

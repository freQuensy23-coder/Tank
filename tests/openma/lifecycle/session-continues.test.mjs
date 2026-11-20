const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { fileURLToPath, pathToFileURL } from 'node:url';
import { registerHooks } from 'node:module';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
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
  '@open-managed-agents/managed-runtime-sandbox': `const unexpected = () => { throw Error('unexpected managed runtime operation'); };
    export { unexpected as createPreinstalledRuntimeEnvironment, unexpected as createProviderManagedRuntime, unexpected as requireRuntimeEnvironmentArtifact };`,
  croner: 'export class Cron {}',
  standardwebhooks: 'export class Webhook {}',
  fflate: 'export const zipSync = () => new Uint8Array();',
  '@open-managed-agents/sql-client': 'export const withDbBoundary = (_, work) => work(); export const emitDbBoundaryLog = () => {};',
};
registerHooks({resolve(specifier, context, nextResolve) {
  if (specifier === 'effect') return {url: new URL('./worker-effects.mjs', import.meta.url).href, shortCircuit: true};
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
const { NodeSessionExecutionWorker } = await load('packages/control-plane-node/src/lib/node-session-execution-worker.ts');
const { createBoxLiteProvider } = await load('packages/managed-runtime-boxlite/src/boxlite.ts');
const { RetainedExecutionFenceLost } = await load('packages/control-plane-node/src/lib/tank-retained/idle-errors.ts');
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
  const boxes = new Map(), host = {boxes, removed: []};
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
        const box = named(name) ?? host.put(name, options.allocationOptions().rootfsPath);
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
  const handle = box => ({id: box.id, name: box.name, info: () => info(box),
    async start() { box.running = true; }, async stop() { box.running = false; },
    async exec(command) { assert.equal(command, '/bin/sync'); return {async wait() { return {exitCode: 0}; }}; },
  });
  host.client = {
    async getInfo(key) { const box = boxes.get(key) ?? named(key); return box ? info(box) : null; },
    async get(id) { const box = boxes.get(id); return box ? handle(box) : null; },
    async remove(id) { if (boxes.delete(id)) host.removed.push(id); },
  };
  return host;
}

async function world(t, workspace = workspaceId) {
  const root = await mkdtemp(join(tmpdir(), 'tank-session-continues-'));
  const db = new DatabaseSync(':memory:'), sql = sqlite(db), migrations = join(openmaRoot, 'apps/main-node/migrations-sqlite');
  for (const name of readdirSync(migrations).filter(name => name.endsWith('.sql')).sort())
    for (const statement of readFileSync(join(migrations, name), 'utf8').split('--> statement-breakpoint')) if (statement.trim()) db.exec(statement);
  await ensureRuntimeResourceFenceSchema(sql);
  const requests = [];
  const model = body => {
    const received = JSON.parse(body), last = received.messages.at(-1).content.map(part => part.text).join('');
    requests.push(received);
    return last.startsWith('sh: ') ? {run: last.slice(4)} : {text: 'answer to ' + last};
  };
  t.after(async () => { db.close(); await rm(root, {recursive: true, force: true}); });
  const created = Date.parse('2026-11-01T00:00:00.000Z'), iso = new Date(created).toISOString();
  const session = {id: sessionId, agent: {id: 'agent_A', description: null, mcpServers: [], model: {id: 'model_A'}, multiagent: null, name: 'Agent',
    skills: [], system: 'You are the trading agent.', tools: [], version: 1}, archivedAt: null, budget: null, createdAt: iso, environmentId,
    metadata: {}, outcomeEvaluations: [], resources: [], stats: {}, status: 'idle', title: null, updatedAt: iso, usage: {}, vaultIds: []};
  db.prepare(`INSERT INTO managed_sessions (id, workspace_id, document, revision, agent_id, agent_version, environment_id, status, created_at, updated_at)
    VALUES (?, ?, ?, 1, ?, 1, ?, 'idle', ?, ?)`).run(sessionId, workspace, JSON.stringify(session), session.agent.id, environmentId, created, created);
  const state = {db, sql, root, requests, host: boxHost(), elapsedMs: 0, turn: 0, model, workspaceId: workspace,
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

function node(state, rootfsPath, {idleTimeoutMs, phase = false, startAttempts = 1, prepareFailure = false} = {}) {
  const {sql, host, workspaceId} = state, ownerId = 'node_' + randomUUID();
  const executionCurrent = async fence => {
    const row = await sql.prepare(`SELECT 1 AS present FROM managed_session_executions WHERE workspace_id = ? AND id = ? AND session_id = ? AND state = 'running'
      AND attempt_id = ? AND owner_id = ? AND generation = ? AND lease_expires_at_ms > ?`)
      .bind(fence.workspaceId, fence.executionId, fence.sessionId, fence.attemptId, fence.ownerId, fence.generation, state.now().getTime()).first();
    if (!row) throw new RetainedExecutionFenceLost();
  };
  const components = (context, allocation) => ({scope: [context.workspaceId, context.environment.id, context.sessionId],
    resources: context.session.resources, skills: context.session.agent.skills, environment: context.environment.config,
    runtimeResources: {cpus: allocation.cpus ?? null, memoryMib: allocation.memoryMib ?? null}});
  const lifecycle = new NodeRetainedLifecycle({
    fences: new SqlRuntimeResourceFencePort(sql, {now: state.now}), orphans: new SqlRuntimeOrphanPort(sql), providerModule: host.module, client: host.client,
    ownerId, leaseTtlMs: 30000, nativeHome: state.root, idleTimeoutMs, assertExecutionCurrent: executionCurrent,
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
    prepareSandbox: async ({sandbox}) => {
      await sandbox.prepareRetainedRuntime();
      if (prepareFailure) throw Error('sandbox preparation failed');
    },
    buildModel: async () => ({}),
    buildTools: async () => ({}),
    buildHarness: () => ({run: async ctx => {
      const messages = await eventsToMessagesAsync(ctx.runtime.history.getEvents(), ctx.fileFetcher);
      const reply = state.model(JSON.stringify({system: ctx.systemPrompt, messages}));
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
    engine: new ApplicationBackedNodeManagedSessionRuntimeEngine({runner: phase ? {
      start: input => runner.start(input),
      accept: input => runner.accept({...input, continuationPlan: {mode: 'model', generation: input.executionFence.generation,
        phaseInputIds: [input.events[0].id], consumedInputIds: [input.events[0].id], remainingInputIds: ['later_input'], previousReceiptId: null}}),
    } : runner,
      historyFor: workspace => new SessionRuntimeHistoryApplicationService({workspaceId: workspace, source: history})}),
    realtime: new MemorySessionRealtimeHub(),
    projectionFor: workspace => new SessionRuntimeProjectionApplicationService({workspaceId: workspace, persistence: projection}),
    clock: {now: state.tick},
    startRetry: {attempts: startAttempts, delayMs: () => 0},
  });
  const claim = async (event, attemptId = 'attempt_' + randomUUID()) => {
    const now = state.now().getTime();
    const claimed = await sql.prepare(`UPDATE managed_session_executions SET state = 'running', attempt_id = ?, owner_id = ?, generation = generation + 1,
      attempt_count = attempt_count + 1, claimed_at_ms = ?, lease_expires_at_ms = ?, revision = revision + 1
      WHERE workspace_id = ? AND id = ? AND (state = 'queued' OR lease_expires_at_ms <= ?) RETURNING generation`)
      .bind(attemptId, ownerId, now, now + 30000, workspaceId, event.id, now).first();
    assert.ok(claimed, 'execution is claimable');
    return {executionId: event.id, workspaceId, sessionId, attemptId, ownerId, generation: Number(claimed.generation), expiresAt: new Date(now + 30000).toISOString()};
  };
  const context = async () => ({session: (await projection.findCurrent({workspaceId, sessionId})).session, environment: state.environment});
  const execute = async event => {
    await driver.accept({workspaceId, sessionId, ...await context(), events: [event], executionFence: await claim(event)});
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
  const cancel = input => { driver.cancel(input); runner.cancel(input); };
  return {send, execute, claim, context, driver, lifecycle, ownerId, cancel, turn: async text => execute(await send(text))};
}

async function supervisedTurn(t, scenario, {phase = false, modelFailure = false, prepareFailure = false, resumeFailure = false,
  loseOwnership = false, expireAfterTimeout = false, lateStopFails = false} = {}) {
  const workspaceId = 'workspace_' + randomUUID();
  const state = await world(t, workspaceId);
  const image = await state.image('idle');
  let resumeStarts = 0, workspaceChecks = 0;
  let resident, resumeFence;
  const stopFences = [];
  const resourceFence = () => state.db.prepare(`SELECT owner_id, generation FROM runtime_resource_fences WHERE workspace_id = ?`).get(workspaceId);
  if (resumeFailure) {
    await node(state, image).turn('seed retained guest');
    state.requests.length = 0;
    [resident] = state.host.boxes.values();
    assert.equal(resident.running, false);
    const provider = state.host.module.createBoxLiteProvider;
    state.host.module.createBoxLiteProvider = options => ({
      ...provider(options),
      resume: createBoxLiteProvider(options).resume,
    });
  }
  const local = node(state, image, {phase, prepareFailure, idleTimeoutMs: scenario.startsWith('hung') ? 250 : 1500, startAttempts: 3});
  const event = await local.send('run once');
  const next = [{id: 'same_session', sessionId}, {id: 'other_session', sessionId: 'session_B'}];
  const queued = [{id: event.id, sessionId}, ...next];
  const runs = [], settled = [], yielded = [], errors = [];
  const cancellations = [], recovered = [], proofs = [];
  let allowProof = false;
  let stops = 0, flushes = 0, observations = 0, observationsAtStop, release;
  const pending = new Promise(resolve => { release = resolve; });
  const originalGet = state.host.client.get, originalInfo = state.host.client.getInfo;
  state.host.client.getInfo = async key => {
    observations++;
    if (stops === 1 && scenario === 'verify throws' && observationsAtStop + 1 === observations) throw Error('inspection interrupted');
    if (stops === 1 && scenario === 'hung verification') await pending;
    const info = await originalInfo(key);
    if (loseOwnership && resourceFence()?.owner_id === `${stopFences[0]?.owner_id}:quiesce`) {
      if (!allowProof) return {...info, name: 'unrelated-resident'};
      proofs.push({fence: resourceFence(), info});
    }
    if (stops === 1 && scenario === 'incomplete proof' && observationsAtStop + 1 === observations) {
      return {...info, state: {status: 'Stopped'}};
    }
    return info;
  };
  state.host.client.get = async id => {
    const handle = await originalGet(id);
    return {...handle,
      async start() {
        assert.equal(id, resident?.id);
        resumeStarts++;
        resumeFence = resourceFence();
        await handle.start();
      },
      async exec(command, args) {
        if (command === '/bin/test') {
          assert.deepEqual(args, ['-d', '/workspace', '-a', '!', '-L', '/workspace']);
          assert.equal(id, resident?.id);
          assert.equal(resident.running, true);
          workspaceChecks++;
          if (scenario === 'lease lost') state.db.prepare(`UPDATE managed_session_executions SET lease_expires_at_ms = 0 WHERE id = ?`).run(event.id);
          const empty = async () => ({next: async () => null});
          return {stdout: empty, stderr: empty, wait: async () => ({exitCode: 1})};
        }
        flushes++;
        if (scenario === 'flush throws' && flushes === 1) throw Error('flush interrupted');
        return handle.exec(command);
      },
      async stop() {
        stops++;
        if (resumeFailure || loseOwnership) stopFences.push(resourceFence());
        observationsAtStop = observations;
        if (scenario === 'stop throws' && stops === 1) throw Error('stop interrupted');
        if (scenario === 'permanent throw') throw Error('stop unavailable');
        if (scenario === 'permanent nonack' || (scenario === 'ambiguous ack' && stops === 1)) return;
        if (scenario === 'hung stop') {
          if (loseOwnership && !expireAfterTimeout) state.db.prepare(`UPDATE managed_session_executions SET lease_expires_at_ms = 0 WHERE id = ?`).run(event.id);
          await pending;
          if (lateStopFails) throw Error('late stop rejected');
        }
        await handle.stop();
        if (scenario === 'stop throws after stopping' && stops === 1) throw Error('stop acknowledgement lost');
      },
    };
  };
  if (modelFailure) {
    const model = state.model;
    state.model = body => { model(body); throw Error('genuine model failure'); };
  }
  const worker = new NodeSessionExecutionWorker({
    coordinator: {
      async claim({attemptId, recoverFrom, ownerId}) {
        if (recoverFrom) {
          assert.equal(recoverFrom.executionId, event.id);
          if (loseOwnership) {
            if (ownerId !== local.ownerId) return {type: 'empty'};
            assert.equal(proofs.at(-1)?.info.state.running, false);
            const fence = await local.claim(event, attemptId);
            recovered.push(fence);
            return {type: 'claimed', fence, execution: {id: event.id, workspaceId, sessionId, events: [event], interruptRequestedAt: null}};
          }
          assert.equal(scenario, 'lease lost');
          if (ownerId === local.ownerId) assert.equal(resident.running, false);
          return {type: 'empty'};
        }
        const job = queued.shift();
        if (!job) return {type: 'empty'};
        const primary = job.id === event.id;
        const fence = primary ? await local.claim(event, attemptId) : {executionId: job.id, workspaceId, sessionId: job.sessionId,
          attemptId, ownerId: local.ownerId, generation: 1, expiresAt: new Date(state.now().getTime() + 30000).toISOString()};
        return {type: 'claimed', fence, execution: {...job, workspaceId, events: [event], interruptRequestedAt: null}};
      },
      async settle(result) { settled.push(result); return {type: 'settled'}; },
      async yieldProgress(result) { yielded.push(result); return {type: 'yielded'}; },
      async find() { return null; },
      async renew({fence}) {
        const row = state.db.prepare(`SELECT lease_expires_at_ms FROM managed_session_executions WHERE id = ?`).get(fence.executionId);
        return row?.lease_expires_at_ms === 0 ? {type: 'lost'} : {type: 'renewed', fence, interruptRequestedAt: null};
      },
    },
    context: {find: local.context},
    runtime: {
      async run({executionId, fence, ...input}) {
        runs.push(executionId);
        if (executionId === event.id && fence.generation === 1) return local.driver.accept({...input, executionFence: fence});
        assert.equal([...state.host.boxes.values()][0].running, false);
      },
      async cancel(input) { cancellations.push(input.reason); local.cancel(input); },
    },
    quiesceLostExecution: input => local.lifecycle.quiesceLostTurn(input),
    ownerId: local.ownerId, clock: {now: state.now}, ids: {nextAttemptId: randomUUID}, maxConcurrent: 1, onError: error => errors.push(error),
    ...(loseOwnership && {heartbeatIntervalMs: 5, pollIntervalMs: 10}),
  });
  t.after(() => { release(); worker.stop(); });
  await worker.poll();
  await worker.waitForIdle();
  const terminal = () => state.db.prepare(`SELECT id, type, document FROM managed_session_events
    WHERE rowid > (SELECT rowid FROM managed_session_events WHERE id = ?)
    AND type IN ('session.error', 'session.status_idle', 'session.status_rescheduled') ORDER BY rowid`).all(event.id);
  return {state, worker, event, next, runs, queued, settled, yielded, errors, terminal, release, resident, resumeFence, stopFences,
    cancellations, recovered, proofs, allowProof: () => { allowProof = true; }, expire: () => { state.elapsedMs += 30_000; },
    counts: () => ({stops, flushes, observations, observationsAtStop, resumeStarts, workspaceChecks})};
}

for (const [scenario, stops, flushes] of [
  ['flush throws', 1, 2], ['stop throws', 2, 2], ['stop throws after stopping', 1, 1],
  ['ambiguous ack', 2, 2], ['verify throws', 1, 1], ['incomplete proof', 1, 1],
]) test(`same-owner ${scenario} preserves the completed phase and releases the slot after proof`, async t => {
  const run = await supervisedTurn(t, scenario, {phase: true});
  assert.deepEqual(run.runs, [run.event.id, ...run.next.map(job => job.id)]);
  assert.equal(run.state.requests.length, 1);
  assert.equal(run.counts().stops, stops);
  assert.equal(run.counts().flushes, flushes);
  assert.deepEqual(run.errors, []);
  if (scenario === 'incomplete proof') assert.equal(run.counts().observations - run.counts().observationsAtStop, 3);
  const [terminal] = run.terminal();
  assert.deepEqual(run.terminal().map(row => row.type), ['session.status_idle']);
  assert.equal(run.yielded.length, 1);
  assert.equal(run.yielded[0].terminalEventId, terminal.id);
  assert.deepEqual(run.yielded[0].remainingInputIds, ['later_input']);
});

test('transient stop recovery preserves the original model failure through worker settlement', async t => {
  const run = await supervisedTurn(t, 'stop throws', {modelFailure: true});
  assert.equal(run.state.requests.length, 1);
  assert.equal(run.counts().stops, 2);
  assert.deepEqual(run.errors, []);
  assert.equal(run.settled[0].outcome, 'failed');
  assert.equal(run.settled[0].failure, 'genuine model failure');
  assert.deepEqual(run.terminal().map(row => row.type), ['session.error', 'session.status_idle']);
});

for (const [scenario, stops, message] of [
  ['permanent nonack', 3, 'retained_idle_unconfirmed'], ['permanent throw', 3, 'retained_idle_unconfirmed'], ['hung stop', 1, 'retained_idle_timeout'],
  ['hung verification', 1, 'retained_idle_timeout'],
]) test(`${scenario} logs once and leaves both same-session and unrelated work queued`, async t => {
  const run = await supervisedTurn(t, scenario, {modelFailure: scenario === 'permanent nonack'});
  assert.equal(run.state.requests.length, 1);
  assert.equal(run.counts().stops, stops);
  assert.deepEqual(run.errors.map(error => error.message), [message]);
  const before = run.counts();
  await run.worker.poll();
  run.release();
  await new Promise(resolve => setTimeout(resolve, 20));
  if (message === 'retained_idle_unconfirmed') run.expire();
  await run.worker.poll();
  assert.deepEqual(run.counts(), before);
  assert.deepEqual(run.runs, [run.event.id]);
  assert.deepEqual(run.queued, run.next);
  assert.deepEqual(run.settled, []);
  assert.deepEqual(run.yielded, []);
  assert.deepEqual(run.terminal(), []);
});

async function pollUntil(worker, predicate) {
  const signal = AbortSignal.timeout(2000);
  while (!predicate()) {
    await worker.poll();
    await delay(5, undefined, {signal});
  }
}

for (const [when, result] of [
  ['during', 'stopped'], ['during', 'pending'], ['after', 'stopped'], ['after', 'rejected'],
]) test(`lease loss ${when} idle timeout with native stop ${result} requires fenced stopped proof`, async t => {
  const expireAfterTimeout = when === 'after';
  const run = await supervisedTurn(t, 'hung stop', {loseOwnership: true, expireAfterTimeout, lateStopFails: result === 'rejected'});
  assert.deepEqual(run.cancellations, expireAfterTimeout ? [] : ['lease_lost']);
  assert.equal(run.errors[0]?.message, 'retained_idle_timeout');
  const before = run.counts();
  if (expireAfterTimeout) {
    await run.worker.poll();
    assert.deepEqual(run.errors.map(error => error.message), ['retained_idle_timeout']);
    assert.deepEqual(run.queued, run.next);
    run.expire();
  }
  await pollUntil(run.worker, () => run.errors.filter(error => error.message === 'retained_idle_pending').length >= 2);
  assert.deepEqual(run.counts(), before);
  assert.equal(before.stops, 1);
  assert.equal([...run.state.host.boxes.values()][0].running, true);
  assert.deepEqual(run.runs, [run.event.id]);
  assert.deepEqual(run.queued, run.next);
  assert.deepEqual(run.recovered, []);
  assert.deepEqual(run.settled, []);
  assert.deepEqual(run.terminal(), []);
  if (result === 'pending') return;

  run.release();
  await pollUntil(run.worker, () => run.errors.some(error => error.message === 'retained_recovery_resident_changed'));
  assert.equal([...run.state.host.boxes.values()][0].running, result === 'rejected');
  assert.deepEqual(run.recovered, []);
  assert.deepEqual(run.queued, run.next);
  run.allowProof();
  if (result === 'rejected') {
    await pollUntil(run.worker, () => run.errors.filter(error => error.message === 'retained_recovery_stop_unconfirmed').length >= 2);
    assert.equal(run.counts().stops, 1);
    assert.deepEqual(run.queued, run.next);
    assert.deepEqual(run.recovered, []);
    return;
  }
  await pollUntil(run.worker, () => run.settled.length === 1 + run.next.length);
  assert.deepEqual(run.runs, [run.event.id, run.event.id, ...run.next.map(job => job.id)]);
  assert.equal(run.recovered.length, 1);
  assert.equal(run.recovered[0].generation, 2);
  assert.deepEqual(run.settled.filter(result => result.fence.executionId === run.event.id).map(result => result.fence), run.recovered);
  assert.ok(run.proofs.at(-1).fence.generation > run.stopFences[0].generation);
  assert.equal(run.proofs.at(-1).fence.owner_id, `${run.stopFences[0].owner_id}:quiesce`);
  assert.equal(run.counts().stops, 1);
});

test('an unconfirmed stop during preparation failure bypasses startup retries and failure projection', async t => {
  const run = await supervisedTurn(t, 'permanent throw', {prepareFailure: true});
  assert.equal(run.state.requests.length, 0);
  assert.equal(run.counts().stops, 3);
  assert.deepEqual(run.errors.map(error => error.message), ['retained_idle_unconfirmed']);
  assert.deepEqual(run.runs, [run.event.id]);
  assert.deepEqual(run.queued, run.next);
  assert.deepEqual(run.settled, []);
  assert.deepEqual(run.terminal(), []);
});

for (const [scenario, stops, error] of [
  ['permanent throw', 3, 'retained_idle_unconfirmed'], ['hung stop', 1, 'retained_idle_timeout'],
]) test(`a failed BoxLite resume with ${scenario} quarantines the running guest`, async t => {
  const run = await supervisedTurn(t, scenario, {resumeFailure: true});
  assert.equal(run.resident.running, true);
  const counts = run.counts();
  assert.equal(counts.resumeStarts, 1);
  assert.equal(counts.workspaceChecks, 1);
  assert.equal(counts.stops, stops);
  assert.deepEqual(run.stopFences, Array(stops).fill(run.resumeFence));
  assert.deepEqual(run.errors.map(error => error.message), [error]);
  await run.worker.poll();
  assert.deepEqual(run.counts(), counts);
  assert.equal(run.state.requests.length, 0);
  assert.deepEqual(run.runs, [run.event.id]);
  assert.deepEqual(run.queued, run.next);
  assert.deepEqual(run.settled, []);
  assert.deepEqual(run.yielded, []);
  assert.deepEqual(run.terminal(), []);
  assert.deepEqual([...run.state.host.boxes.values()], [run.resident]);
});

test('a failed BoxLite resume retries only cleanup and fails the original input after stop proof', async t => {
  const run = await supervisedTurn(t, 'stop throws', {resumeFailure: true});
  assert.equal(run.resident.running, false);
  assert.equal(run.counts().resumeStarts, 1);
  assert.equal(run.counts().workspaceChecks, 1);
  assert.equal(run.counts().stops, 2);
  assert.deepEqual(run.stopFences, [run.resumeFence, run.resumeFence]);
  assert.equal(run.state.requests.length, 0);
  assert.deepEqual(run.errors, []);
  assert.deepEqual(run.runs, [run.event.id, ...run.next.map(job => job.id)]);
  assert.equal(run.settled[0].outcome, 'failed');
  assert.equal(run.settled[0].failure, 'retained_workspace_missing');
  assert.equal(run.terminal().filter(row => row.type === 'session.error').length, 1);
});

test('execution lease loss during a failed BoxLite resume leaves exact cleanup to fenced recovery', async t => {
  const run = await supervisedTurn(t, 'lease lost', {resumeFailure: true});
  assert.equal(run.resident.running, false);
  assert.equal(run.counts().resumeStarts, 1);
  assert.equal(run.counts().workspaceChecks, 1);
  assert.equal(run.counts().stops, 1);
  assert.equal(run.stopFences[0].owner_id, `${run.resumeFence.owner_id}:quiesce`);
  assert.equal(run.stopFences[0].generation, run.resumeFence.generation + 1);
  assert.deepEqual(run.errors, []);
  assert.deepEqual(run.runs, [run.event.id, ...run.next.map(job => job.id)]);
  assert.equal(run.settled.some(result => result.fence.executionId === run.event.id), false);
  assert.deepEqual(run.terminal(), []);
});

const restart = (state, rootfsPath) => { state.elapsedMs += 31000; return node(state, rootfsPath); };
async function seedInterruptedAllocation(state, image, hasResident) {
  const previous = state.manifest();
  await state.host.client.remove(previous.handle.runtimeId);
  const resident = hasResident ? state.host.put(boxName, image) : null;
  state.publishManifest({...previous, state: 'allocating', retired: [], handle: {provider: 'litebox', runtimeId: boxName},
    identity: {...previous.identity, image: await executableImageIdentity(image)}});
  return resident ? [resident.id] : [];
}
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

async function continues(state, current, {reminded = true, gone = []} = {}) {
  const request = await current.turn('what was the plan?');
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

for (const [name, hasResident] of [['after its runtime was created', true], ['before its runtime was created', false]]) {
  test(`an allocation of an older image interrupted ${name} is replaced and the model still receives the earlier conversation`, async t => {
    const state = await conversation(t), image = await state.image('v2');
    const old = await seedInterruptedAllocation(state, image, hasResident);
    await continues(state, restart(state, await state.image('v3')), {gone: old});
  });
}

test('a replacement interrupted before its runtime was created continues after a restart with the same image', async t => {
  const state = await conversation(t), image = await state.image('v2');
  await seedInterruptedAllocation(state, image, false);
  await continues(state, restart(state, image), {reminded: false});
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

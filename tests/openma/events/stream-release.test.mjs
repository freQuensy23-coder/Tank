import { fileURLToPath, pathToFileURL } from 'node:url';
import { registerHooks } from 'node:module';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import assert from 'node:assert/strict';

const openmaSource = new URL('../../../vendor/openma/', import.meta.url);
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
const { relayWithHeartbeat } = await load('packages/managed-agents-api/src/routes/sse-relay.ts');
const { DefaultNodeManagedSessionRuntimeDriver, NodeManagedSessionRuntimeAdapter } = await load('packages/control-plane-node/src/lib/node-managed-session-runtime.ts');
const { SqlReplicatedSessionEventStream } = await load('packages/managed-agents-adapters-sql/src/session-event-replicated-stream.ts');
const { EnvironmentAwareSessionEventStreamRouter } = await load('packages/managed-agents-adapters-runtime/src/session-lifecycle-router.ts');
const { MemorySessionRealtimeHub } = await load('packages/session-realtime-memory/src/index.ts');

const timeouts = () => process.getActiveResourcesInfo().filter(name => name === 'Timeout').length;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function settled(promise, ms = 500) {
  let timer;
  const expired = new Promise(resolve => { timer = setTimeout(() => resolve(false), ms); });
  try { return await Promise.race([promise.then(() => true), expired]); } finally { clearTimeout(timer); }
}
async function until(condition) {
  for (let i = 0; i < 200 && !condition(); i++) await pause(5);
  assert.ok(condition());
}

function neverEndingSource() {
  const probe = { nextCalls: 0, thenCalls: 0, returnCalls: 0 };
  const pending = new Promise(() => {});
  const source = {
    [Symbol.asyncIterator]: () => ({
      next: () => {
        probe.nextCalls++;
        return { then: (resolve, reject) => { probe.thenCalls++; return pending.then(resolve, reject); } };
      },
      return: async () => { probe.returnCalls++; return { done: true, value: undefined }; },
    }),
  };
  return { source, probe };
}

test('an idle SSE connection adds no continuation to the pending event per heartbeat', async () => {
  const { source, probe } = neverEndingSource();
  let beats = 0;
  let abort = () => {};
  const relay = relayWithHeartbeat({ onAbort: listener => { abort = listener; } }, source, 2, {
    heartbeat: async () => { beats++; },
    deliver: async () => true,
  });
  await until(() => beats >= 8);
  assert.equal(probe.nextCalls, 1);
  assert.equal(probe.thenCalls, 1);
  abort();
  await relay;
});

function countReactions() {
  const original = Promise.prototype.then;
  const counts = new WeakMap();
  const receivers = new Set();
  Promise.prototype.then = function (...args) {
    counts.set(this, (counts.get(this) ?? 0) + 1);
    receivers.add(new WeakRef(this));
    return original.apply(this, args);
  };
  return {
    max: () => Math.max(0, ...[...receivers].map(ref => counts.get(ref.deref()) ?? 0)),
    stop: () => { Promise.prototype.then = original; },
  };
}

function queuedSource() {
  const queue = [], waiting = [];
  const source = {
    [Symbol.asyncIterator]: () => ({
      next: () => queue.length ? Promise.resolve(queue.shift()) : new Promise(resolve => waiting.push(resolve)),
      return: async () => ({ done: true, value: undefined }),
    }),
  };
  const push = value => { const item = { done: false, value }; if (waiting.length) waiting.shift()(item); else queue.push(item); };
  return { source, push };
}

test('a connected idle SSE stream adds no reaction to any promise per heartbeat', async () => {
  const { source } = neverEndingSource();
  const registered = [];
  let beats = 0;
  const counter = countReactions();
  let relay;
  try {
    relay = relayWithHeartbeat({ onAbort: listener => { registered.push(listener); } }, source, 2, {
      heartbeat: async () => { beats++; },
      deliver: async () => true,
    });
    await until(() => beats >= 40);
  } finally { counter.stop(); }
  assert.equal(registered.length, 1);
  assert.ok(counter.max() <= 2, `a promise collected ${counter.max()} reactions over ${beats} heartbeats`);
  registered[0]();
  await relay;
});

test('a connected SSE stream keeps its abort and event reactions constant while delivering events', async () => {
  const { source, push } = queuedSource();
  const registered = [], delivered = [];
  let beats = 0;
  const counter = countReactions();
  let relay;
  try {
    relay = relayWithHeartbeat({ onAbort: listener => { registered.push(listener); } }, source, 3, {
      heartbeat: async () => { beats++; },
      deliver: async event => { delivered.push(event); return true; },
    });
    const events = 60;
    for (let i = 0; i < events; i++) {
      push(i);
      await until(() => delivered.length === i + 1);
      if (i % 20 === 0) { const seen = beats; await until(() => beats > seen); }
    }
  } finally { counter.stop(); }
  assert.deepEqual(delivered, Array.from({ length: 60 }, (_, i) => i));
  assert.equal(registered.length, 1);
  assert.ok(counter.max() <= 2, `a promise collected ${counter.max()} reactions over ${delivered.length} events`);
  registered[0]();
  await relay;
});

test('a dropped SSE connection ends the relay, returns the source and leaves no timer behind', async () => {
  const before = timeouts();
  const { source, probe } = neverEndingSource();
  let abort = () => {};
  const relay = relayWithHeartbeat({ onAbort: listener => { abort = listener; } }, source, 60_000, {
    heartbeat: async () => {},
    deliver: async () => true,
  });
  await pause(5);
  assert.equal(timeouts(), before + 1);
  abort();
  assert.equal(await settled(relay), true);
  assert.equal(probe.returnCalls, 1);
  assert.equal(timeouts(), before);
});

test('a delivery that asks to stop returns the source', async () => {
  const returned = [];
  const source = {
    [Symbol.asyncIterator]: () => {
      let sent = false;
      return {
        next: async () => sent ? { done: true, value: undefined } : (sent = true, { done: false, value: 'event' }),
        return: async () => { returned.push(true); return { done: true, value: undefined }; },
      };
    },
  };
  const delivered = [];
  await relayWithHeartbeat({ onAbort: () => {} }, source, 60_000, {
    heartbeat: async () => {},
    deliver: async event => { delivered.push(event); return false; },
  });
  assert.deepEqual(delivered, ['event']);
  assert.equal(returned.length, 1);
});

test('abandoned Session event streams release the realtime subscription while waiting for an event', async () => {
  const environmentId = 'env_A', workspaceId = 'workspace_A';
  const hub = new MemorySessionRealtimeHub();
  const driver = new DefaultNodeManagedSessionRuntimeDriver({
    engine: {}, realtime: hub, projectionFor: () => ({}), clock: { now: () => new Date() },
    startRetry: { attempts: 1, delayMs: () => 0 },
  });
  const client = { prepare: () => { const statement = { bind: () => statement, first: async () => null, all: async () => ({ results: [] }) }; return statement; } };
  const scheduled = [];
  const replicated = new SqlReplicatedSessionEventStream(client, new NodeManagedSessionRuntimeAdapter(driver), {
    pollIntervalMs: 300,
    schedule: () => { const timer = { stopped: false, stop() { timer.stopped = true; } }; scheduled.push(timer); return timer; },
  });
  const router = new EnvironmentAwareSessionEventStreamRouter({
    environments: { find: async () => ({ id: environmentId, config: { type: 'cloud', networking: { type: 'unrestricted' } } }) },
    runtime: replicated,
    selfHosted: { subscribe: () => { throw new Error('self-hosted stream used'); } },
  });
  const sessions = 12, iterators = [], pending = [];
  for (let i = 0; i < sessions; i++) {
    const iterator = router.subscribe({ workspaceId, sessionId: 'session_A', session: { environmentId }, deltaEventTypes: [] })[Symbol.asyncIterator]();
    iterators.push(iterator);
    pending.push(iterator.next());
  }
  const attached = () => [...(hub.workspaces.get(workspaceId)?.get('session_A') ?? [])].length;
  await until(() => attached() === sessions);
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0].stopped, false);
  const returns = iterators.map(iterator => iterator.return());
  assert.equal(await settled(Promise.all(returns)), true);
  assert.equal(await settled(Promise.all(pending)), true);
  assert.equal(attached(), 0);
  assert.equal(hub.workspaces.size, 0);
  assert.equal(scheduled[0].stopped, true);
});

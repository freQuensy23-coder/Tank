import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { BoxLiteFilesystemDurability } from '../../../vendor/openma/packages/managed-runtime-boxlite/src/guest-filesystem.ts';
import { pinnedBoxLiteProvider, RetainedBoxOperations } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts';
import { setup } from './fakes.mjs';

const identity = value => ({ version: 2, image: 'sha256:' + value.repeat(64), compatibility: { scope: 'sha256:' + 'c'.repeat(64) } });
const permitted = async () => {};
const signal = () => new AbortController().signal;
const turn = () => new Promise(resolve => setImmediate(resolve));
const publication = s => JSON.parse([...s.rows.values()][0].publication.workspaceCandidate.metadata.manifest);
const archive = Buffer.from('retained workspace bytes');
const hash = createHash('sha256').update(archive).digest('hex');
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
function fixture(t) {
  const s = setup(), boxes = new Map(), hooks = {};
  let next = 0;
  function runtime(box) {
    const durability = new BoxLiteFilesystemDurability(box);
    return {
      box, runtimeHandle: () => ({ provider: 'litebox', runtimeId: box.id }),
      flushFilesystem: assertCurrent => durability.flush(assertCurrent),
      suspend: input => durability.suspend(input.assertCurrent),
    };
  }
  const module = {
    BoxLiteFilesystemDurability,
    createBoxLiteManagedRuntime() { return { sandbox: { async reap({ lease }) { boxes.delete(lease.runtimeId); } } }; },
    createBoxLiteProvider(options) { return {
      async create(context) {
        const name = options.runtimeName?.(context) ?? 'oma-' + createHash('sha256').update(`litebox:${context.sessionId}`).digest('hex').slice(0, 32);
        const box = { id: 'native-' + ++next, name, dirty: '', durable: '', running: true, stops: 0,
          info() { return { id: this.id, name, state: { running: this.running } }; },
          async exec(command, args, env) {
            assert.equal(command, '/bin/sync'); assert.deepEqual(args, []); assert.deepEqual(env, []);
            return { wait: async () => {
              await hooks.flush?.(box);
              if (hooks.flushFailure) return { exitCode: 1 };
              box.durable = box.dirty; return { exitCode: 0 };
            }, async kill() {} };
          },
          async stop() { this.dirty = this.durable; this.running = false; this.stops++; },
        };
        boxes.set(box.id, box); return runtime(box);
      },
      async resume(handle) {
        const box = boxes.get(handle.runtimeId); assert.ok(box);
        box.running = true; return runtime(box);
      },
    }; },
  };
  const client = {
    async getInfo(id) { const box = boxes.get(id) ?? [...boxes.values()].find(box => box.name === id); return box?.info() ?? null; },
    async get(id) { return boxes.get(id) ?? null; },
  };
  const provider = pinnedBoxLiteProvider({ module, client, allocation: { image: 'new' }, leaseTtlMs: 30000, isCurrent: fence => s.fences.isCurrent(fence) });
  t.mock.method(RetainedBoxOperations.prototype, 'exec', async function(command, args) {
    return this.use(async value => {
      const mode = args[2];
      if (mode === 'import') { await hooks.import?.(); value.box.dirty = 'preserved user workspace'; }
      const stdout = mode === 'describe' ? JSON.stringify({ bytes: archive.length, sha256: hash }) : command === 'rm' ? '' : hash;
      return { stdout, stderr: '', exitCode: 0 };
    });
  });
  t.mock.method(RetainedBoxOperations.prototype, 'copyOut', async function(_guest, host) {
    await this.use(async () => { await hooks.copy?.(); await writeFile(host, archive); });
  });
  t.mock.method(RetainedBoxOperations.prototype, 'copyIn', async function(host) {
    await this.use(async () => assert.deepEqual(await readFile(host), archive));
  });
  return { ...s, boxes, hooks, controller: options => s.controller({ ...options, provider }) };
}
async function retain(s) {
  const first = s.controller({ identity: identity('a') });
  await first.begin(permitted, signal());
  await first.use(async runtime => { runtime.box.dirty = 'preserved user workspace'; });
  await first.idle(permitted);
}

for (const committed of [false, true]) test(`target flush precedes publication and survives lost acknowledgement (committed=${committed})`, async t => {
  const s = fixture(t); await retain(s);
  const original = publication(s).handle.runtimeId;
  const publish = s.fences.publish.bind(s.fences); let fail = true;
  s.fences.publish = async input => {
    const manifest = JSON.parse(input.workspaceCandidate.metadata.manifest);
    if (fail && manifest.state === 'retained' && manifest.identity.image === identity('b').image) {
      const box = s.boxes.get(manifest.handle.runtimeId);
      assert.equal(box.durable, 'preserved user workspace');
      fail = false; if (committed) await publish(input); throw Error('publication acknowledgement lost');
    }
    return publish(input);
  };
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /publication acknowledgement lost/);
  assert.equal(s.boxes.get(original).durable, 'preserved user workspace');
  const restarted = s.controller({ identity: identity('b') });
  await restarted.begin(permitted, signal());
  await restarted.use(async runtime => assert.equal(runtime.box.dirty, 'preserved user workspace'));
  await restarted.idle(permitted);
});

test('interrupted transfer keeps the original durable workspace for the next owner', async t => {
  const s = fixture(t); await retain(s);
  const original = publication(s).handle.runtimeId;
  s.hooks.copy = async () => { throw Error('retained_upgrade_transfer_failed:interrupted'); };
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /retained_upgrade_transfer_failed:interrupted/);
  assert.equal(publication(s).state, 'upgrading');
  assert.equal(publication(s).handle.runtimeId, original);
  assert.equal(s.boxes.get(original).dirty, 'preserved user workspace');
  s.hooks.copy = undefined;
  const retry = s.controller({ identity: identity('b') });
  await retry.begin(permitted, signal()); await retry.idle(permitted);
  assert.equal(s.boxes.get(publication(s).handle.runtimeId).durable, 'preserved user workspace');
});

test('a failed publication flush leaves the original journal authoritative', async t => {
  const s = fixture(t); await retain(s);
  const original = publication(s).handle.runtimeId;
  s.hooks.import = async () => { s.hooks.flushFailure = true; };
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /retained_upgrade_transfer_failed:guest_filesystem_flush/);
  assert.equal(publication(s).handle.runtimeId, original);
  assert.equal(publication(s).state, 'upgrading');
  assert.equal(s.boxes.get(original).durable, 'preserved user workspace');
});

test('a target flush timeout is a retryable preflight failure without publishing its incomplete target', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const s = fixture(t); await retain(s);
  const original = publication(s).handle.runtimeId, flushing = deferred(), release = deferred();
  s.hooks.import = async () => { s.hooks.flush = async () => { flushing.resolve(); await release.promise; }; };
  const pending = s.controller({ identity: identity('b') }).begin(permitted, signal());
  const rejected = assert.rejects(pending, /^Error: retained_upgrade_transfer_failed:guest_timeout$/);
  await flushing.promise; t.mock.timers.tick(30000); await rejected;
  assert.equal(publication(s).handle.runtimeId, original);
  assert.equal(publication(s).state, 'upgrading');
  release.resolve(); await turn();
  assert.equal(publication(s).handle.runtimeId, original);
  assert.equal(s.boxes.get(original).durable, 'preserved user workspace');
});

for (const reason of ['pending_input', 'resource_fence']) test(`${reason} changes during idle flush cannot stop or publish the runtime`, async t => {
  const s = fixture(t), controller = s.controller({ identity: identity('a') });
  let current = true, effects = 0;
  const execution = async () => { if (!current) throw Error('pending_input_changed'); };
  await controller.begin(execution, signal());
  await controller.use(async runtime => { runtime.box.dirty = 'single tool effect'; effects++; });
  const before = structuredClone(publication(s)), flushing = deferred(), release = deferred();
  s.hooks.flush = async () => { flushing.resolve(); await release.promise; };
  const pending = controller.idle(execution);
  const rejected = assert.rejects(pending, reason === 'pending_input' ? /pending_input_changed/ : /retained_fence_lost/);
  await flushing.promise;
  if (reason === 'pending_input') current = false; else [...s.rows.values()][0].active = false;
  release.resolve(); await rejected;
  const box = s.boxes.get(before.handle.runtimeId);
  assert.equal(box.stops, 0); assert.equal(box.durable, 'single tool effect'); assert.equal(effects, 1);
  assert.deepEqual(publication(s), before);
  await controller.abandon().catch(() => undefined);
});

test('same-image second append survives the next stopped fresh handle without replacement', async t => {
  const s = fixture(t); await retain(s);
  const handle = publication(s).handle.runtimeId;
  const second = s.controller({ identity: identity('a'), fingerprint: 'sha256:' + 'f'.repeat(64) });
  await second.begin(permitted, signal());
  await second.use(async runtime => { runtime.box.dirty += '\nB-second'; });
  await second.idle(permitted);
  const reopened = s.controller({ identity: identity('a') });
  await reopened.begin(permitted, signal());
  await reopened.use(async runtime => assert.equal(runtime.box.dirty, 'preserved user workspace\nB-second'));
  assert.equal(s.boxes.size, 1); assert.equal(publication(s).handle.runtimeId, handle);
  await reopened.idle(permitted);
});

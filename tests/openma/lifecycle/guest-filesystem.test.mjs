import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BoxLiteFilesystemDurability } from '../../../vendor/openma/packages/managed-runtime-boxlite/src/guest-filesystem.ts';

const turn = () => new Promise(resolve => setImmediate(resolve));
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
function fixture() {
  let running = true, dirty = 'acknowledged workspace write', durable = '', killed = 0;
  const calls = [], completed = deferred();
  const execution = {
    async wait() { await completed.promise; durable = dirty; return { exitCode: 0 }; },
    async kill() { killed++; },
  };
  const box = {
    info: () => ({ state: { running } }),
    async exec(...args) { calls.push(['exec', ...args]); return execution; },
    async stop() { calls.push(['stop']); dirty = durable; running = false; },
  };
  return { box, execution, calls, completed, barrier: new BoxLiteFilesystemDurability(box),
    get dirty() { return dirty; }, get durable() { return durable; }, get killed() { return killed; } };
}

test('state-preserving stop awaits the actual guest flush and preserves acknowledged writes', async () => {
  const s = fixture();
  const pending = s.barrier.suspend(async () => {});
  await turn();
  assert.equal(s.durable, '');
  assert.equal(s.calls.some(call => call[0] === 'stop'), false);
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_quiescing/);
  s.completed.resolve(); await pending;
  assert.equal(s.dirty, 'acknowledged workspace write');
  assert.deepEqual(s.calls, [['exec', '/bin/sync', [], [], false, null, 30, '/'], ['stop']]);
});

test('publication flush waits for durability without stopping the target', async () => {
  const s = fixture();
  const pending = s.barrier.flush(async () => {});
  await turn();
  assert.equal(s.durable, '');
  s.completed.resolve(); await pending;
  assert.equal(s.durable, 'acknowledged workspace write');
  assert.equal(s.calls.some(call => call[0] === 'stop'), false);
  s.barrier.assertUsable();
});

test('nonzero guest flush refuses a checkpoint and poisons subsequent operations', async () => {
  const s = fixture();
  s.execution.wait = async () => ({ exitCode: 5, errorMessage: 'writeback failed' });
  await assert.rejects(s.barrier.suspend(async () => {}), /retained_filesystem_flush_failed/);
  assert.equal(s.calls.some(call => call[0] === 'stop'), false);
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_requires_stop/);
  await assert.rejects(s.barrier.flush(async () => {}), /retained_runtime_requires_stop/);
  assert.equal(s.calls.length, 1);
});

for (const failure of ['lease', 'input', 'cancel']) test(`${failure} loss during flush prevents the later native stop`, async () => {
  const s = fixture(); let current = true;
  const pending = s.barrier.suspend(async () => { if (!current) throw Error(failure + '_lost'); });
  const rejected = assert.rejects(pending, new RegExp(failure + '_lost'));
  await turn(); current = false; s.completed.resolve(); await rejected;
  assert.equal(s.durable, 'acknowledged workspace write');
  assert.equal(s.calls.some(call => call[0] === 'stop'), false);
});

test('a failed flush can retry suspension while ordinary operations remain poisoned until stop', async () => {
  const s = fixture(), wait = s.execution.wait;
  s.execution.wait = async () => ({ exitCode: 5 });
  await assert.rejects(s.barrier.suspend(), /retained_filesystem_flush_failed/);
  s.execution.wait = wait;
  const pending = s.barrier.suspend().then(() => null, error => error);
  await turn();
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_requires_stop|retained_runtime_quiescing/);
  await assert.rejects(s.barrier.flush(), /retained_runtime_requires_stop|retained_runtime_quiescing/);
  await assert.rejects(s.barrier.suspend(), /retained_runtime_quiescing/);
  assert.equal(s.calls.filter(call => call[0] === 'exec').length, 2);
  s.completed.resolve();
  assert.equal(await pending, null);
  assert.equal(s.dirty, 'acknowledged workspace write');
  assert.equal(s.calls.filter(call => call[0] === 'stop').length, 1);
  s.barrier.assertUsable();
});

test('suspension recovery revalidates the fence before stopping', async () => {
  const s = fixture();
  s.execution.wait = async () => ({ exitCode: 5 });
  await assert.rejects(s.barrier.suspend(), /retained_filesystem_flush_failed/);
  s.execution.wait = async () => ({ exitCode: 0 });
  let checks = 0;
  await assert.rejects(s.barrier.suspend(async () => { if (++checks === 2) throw Error('lease_lost'); }), /lease_lost/);
  assert.equal(s.calls.some(call => call[0] === 'stop'), false);
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_requires_stop/);
  await s.barrier.suspend(async () => {});
  assert.equal(s.calls.filter(call => call[0] === 'stop').length, 1);
});

test('a transient native stop failure can be retried after another successful flush', async () => {
  const s = fixture(), stop = s.box.stop;
  s.completed.resolve();
  s.box.stop = async () => { throw Error('native_stop_transient'); };
  await assert.rejects(s.barrier.suspend(), /native_stop_transient/);
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_requires_stop/);
  s.box.stop = stop;
  await s.barrier.suspend();
  assert.equal(s.calls.filter(call => call[0] === 'exec').length, 2);
  assert.equal(s.dirty, 'acknowledged workspace write');
  s.barrier.assertUsable();
});

test('lost stop acknowledgement recovers without restarting or flushing the stopped guest', async () => {
  const s = fixture(), stop = s.box.stop;
  s.completed.resolve();
  s.box.stop = async () => { await stop(); throw Error('stop_ack_lost'); };
  await assert.rejects(s.barrier.suspend(), /stop_ack_lost/);
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_requires_stop/);
  let checks = 0;
  await s.barrier.suspend(async () => { checks++; });
  assert.equal(checks, 2);
  assert.equal(s.calls.filter(call => call[0] === 'exec').length, 1);
  assert.equal(s.calls.filter(call => call[0] === 'stop').length, 1);
  assert.equal(s.dirty, 'acknowledged workspace write');
  s.barrier.assertUsable();
});

test('a stopped guest is never restarted just to flush', async () => {
  const calls = [];
  const barrier = new BoxLiteFilesystemDurability({ info: () => ({ state: { running: false } }),
    async exec() { calls.push('exec'); }, async stop() { calls.push('stop'); } });
  await barrier.suspend(async () => {});
  assert.deepEqual(calls, []);
});

test('an unexpectedly stopped publication target cannot claim a successful flush', async () => {
  const barrier = new BoxLiteFilesystemDurability({ info: () => ({ state: { running: false } }),
    async exec() { assert.fail('must not restart'); }, async stop() { assert.fail('already stopped'); } });
  await assert.rejects(barrier.flush(), /retained_filesystem_flush_failed/);
  assert.throws(() => barrier.assertUsable(), /retained_runtime_requires_stop/);
});

for (const stage of ['spawn', 'wait']) test(`flush deadline bounds deferred ${stage} and kills only its late exact process`, async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const s = fixture(), spawned = deferred();
  if (stage === 'spawn') s.box.exec = async () => { s.calls.push(['exec']); return spawned.promise; };
  const rejected = assert.rejects(s.barrier.suspend(async () => {}), /^Error: retained_exec_timeout$/);
  await turn(); t.mock.timers.tick(30000); await rejected;
  assert.equal(s.calls.some(call => call[0] === 'stop'), false);
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_requires_stop/);
  if (stage === 'spawn') spawned.resolve(s.execution);
  await turn(); assert.equal(s.killed, 1);
  s.completed.resolve(); await turn();
  assert.equal(s.calls.some(call => call[0] === 'stop'), false);
  assert.throws(() => s.barrier.assertUsable(), /retained_runtime_requires_stop/);
});

test('an expired fence lookup cannot launch a late guest flush', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const s = fixture(), lookup = deferred();
  const rejected = assert.rejects(s.barrier.suspend(() => lookup.promise), /retained_exec_timeout/);
  await turn(); t.mock.timers.tick(30000); await rejected;
  lookup.resolve(); await turn();
  assert.deepEqual(s.calls, []);
});

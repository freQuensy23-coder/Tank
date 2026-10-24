import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setup } from './fakes.mjs';

const image = value => 'sha256:' + value.repeat(64);
const identity = value => ({ version: 2, image: image(value), compatibility: { scope: image('c'), network: image('d'), databasePolicy: image('e') } });
const permitted = async () => {};
const signal = () => new AbortController().signal;
const publication = s => JSON.parse([...s.rows.values()][0].publication.workspaceCandidate.metadata.manifest);
function fixture() {
  const s = setup(), hooks = {}, replacements = [];
  s.provider.validateLegacy = async (handle, scope) => {
    const disk = s.disks.get(handle.runtimeId);
    if (!disk || disk.scope !== JSON.stringify(scope)) throw Error('retained_legacy_provider_provenance_missing');
  };
  s.provider.replacementIntent = async () => ({ provider: 'litebox', runtimeId: 'replacement_' + replacements.length });
  s.provider.replace = async (journal, scope, fence, abort, assertCurrent) => {
    assert.equal(publication(s).state, 'upgrading');
    await assertCurrent();
    const original = s.disks.get(journal.source.runtimeId);
    assert.ok(original);
    original.running = false;
    const runtime = await s.provider.create(scope);
    const disk = s.disks.get(runtime.runtimeHandle().runtimeId);
    disk.files['/usr/bin/image-marker'] = 'image-B';
    replacements.push(disk.id);
    await hooks.transfer?.();
    await assertCurrent();
    for (const [name, value] of Object.entries(original.files)) if (name.startsWith('/workspace/')) disk.files[name] = value;
    disk.profile = structuredClone(original.profile);
    await hooks.validate?.();
    await assertCurrent();
    return runtime;
  };
  return { ...s, hooks, replacements };
}
async function retain(s) {
  const controller = s.controller({ identity: identity('a') });
  await controller.begin(permitted, signal());
  await controller.use(async runtime => {
    const { disk } = await runtime.exec('prepare');
    disk.files['/workspace/upload.bin'] = '\u0000upload';
    disk.files['/workspace/result.txt'] = 'ordered result';
    disk.files['/usr/bin/image-marker'] = 'image-A';
    disk.profile.cookie = 'durable-profile';
  });
  await controller.idle(permitted);
  return controller;
}

test('image-only replacement publishes new executable disk and preserves only persistent user state', async () => {
  const s = fixture(); await retain(s);
  const before = publication(s);
  const controller = s.controller({ identity: identity('b'), fingerprint: image('f') });
  await controller.begin(permitted, signal());
  await controller.use(async runtime => {
    const { disk } = await runtime.exec('continue');
    assert.equal(disk.files['/usr/bin/image-marker'], 'image-B');
    assert.equal(disk.files['/workspace/upload.bin'], '\u0000upload');
    assert.equal(disk.files['/workspace/result.txt'], 'ordered result');
    assert.equal(disk.profile.cookie, 'durable-profile');
  });
  assert.equal(publication(s).identity.image, image('b'));
  assert.equal(s.disks.has(before.handle.runtimeId), true);
  assert.notEqual(publication(s).handle.runtimeId, before.handle.runtimeId);
  assert.match(controller.upgradeNotice(), /Processes and in-memory state were restarted/);
  await controller.idle(permitted);
  const restarted = s.controller({ identity: identity('b') });
  await restarted.begin(permitted, signal());
  assert.equal(s.replacements.length, 1);
  assert.match(restarted.upgradeNotice(), /browser profile/);
  await restarted.idle(permitted);
  await s.controller({ identity: identity('b') }).archive(permitted, signal());
  assert.equal(s.disks.size, 0);
});

test('metadata-only fingerprint change with identical v2 image does not replace', async () => {
  const s = fixture(); await retain(s);
  const handle = publication(s).handle;
  const controller = s.controller({ identity: identity('a'), fingerprint: image('f') });
  await controller.begin(permitted, signal());
  assert.deepEqual(publication(s).handle, handle);
  assert.equal(s.replacements.length, 0);
  assert.equal(controller.upgradeNotice(), null);
  await controller.idle(permitted);
});

test('policy mismatch names protected components without resuming or deleting the original', async () => {
  const s = fixture(); await retain(s);
  const before = structuredClone(publication(s));
  const incompatible = identity('b'); incompatible.compatibility.network = image('f');
  await assert.rejects(s.controller({ identity: incompatible }).begin(permitted, signal()), /retained_configuration_changed:network/);
  assert.deepEqual(publication(s), before);
  assert.equal(s.calls.some(call => call[0] === 'fresh-get'), false);
  assert.equal(s.replacements.length, 0);
});

for (const committed of [false, true]) test(`replacement publication acknowledgement loss remains restart-recoverable (committed=${committed})`, async () => {
  const s = fixture(); await retain(s);
  const oldId = publication(s).handle.runtimeId;
  const publish = s.fences.publish.bind(s.fences); let fail = true;
  s.fences.publish = async input => {
    const manifest = JSON.parse(input.workspaceCandidate.metadata.manifest);
    if (fail && manifest.state === 'retained' && manifest.identity.image === image('b')) {
      fail = false;
      if (committed) await publish(input);
      throw Error('publication acknowledgement unavailable');
    }
    return publish(input);
  };
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /publication acknowledgement unavailable/);
  assert.equal(s.disks.has(oldId), true);
  const restarted = s.controller({ identity: identity('b') });
  await restarted.begin(permitted, signal());
  await restarted.use(async runtime => assert.equal((await runtime.exec('read')).disk.files['/workspace/result.txt'], 'ordered result'));
  assert.equal(s.replacements.length, committed ? 1 : 2);
  await restarted.idle(permitted);
});

test('interrupted transfer never publishes its incomplete runtime and a fresh owner retries from original', async () => {
  const s = fixture(); await retain(s);
  const oldId = publication(s).handle.runtimeId;
  s.hooks.transfer = async () => { throw Error('transfer interrupted'); };
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /transfer interrupted/);
  assert.equal(publication(s).state, 'upgrading');
  assert.equal(publication(s).handle.runtimeId, oldId);
  assert.equal(s.disks.get(oldId).files['/workspace/result.txt'], 'ordered result');
  s.hooks.transfer = undefined;
  const restarted = s.controller({ identity: identity('b') });
  await restarted.begin(permitted, signal()); await restarted.idle(permitted);
  assert.notEqual(publication(s).handle.runtimeId, s.replacements[0]);
});

test('lease loss during transfer cannot publish or destroy authoritative state', async () => {
  const s = fixture(); await retain(s);
  const oldId = publication(s).handle.runtimeId;
  s.hooks.transfer = async () => { [...s.rows.values()][0].active = false; };
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /retained_fence_lost/);
  assert.equal(publication(s).state, 'upgrading');
  assert.equal(publication(s).handle.runtimeId, oldId);
  assert.equal(s.disks.has(oldId), true);
  assert.equal(s.calls.some(call => call[0] === 'remove-owned'), false);
});

test('concurrent resume and archive cannot take ownership during replacement', async () => {
  const s = fixture(); await retain(s);
  let release, entered;
  const blocked = new Promise(resolve => { release = resolve; });
  const transferring = new Promise(resolve => { entered = resolve; });
  s.hooks.transfer = async () => { entered(); await blocked; };
  const first = s.controller({ identity: identity('b') });
  const pending = first.begin(permitted, signal()); await transferring;
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /retained_resource_busy/);
  await assert.rejects(s.controller({ identity: identity('b') }).archive(permitted, signal()), /retained_resource_busy/);
  release(); await pending; await first.idle(permitted);
  await s.controller({ identity: identity('b') }).archive(permitted, signal());
  await assert.rejects(s.controller({ identity: identity('b') }).begin(permitted, signal()), /retained_session_archived/);
});

test('legacy adoption requires exact original fingerprint and provider ownership proof', async () => {
  const s = fixture(), legacy = s.controller();
  await legacy.begin(permitted, signal()); await legacy.idle(permitted);
  await assert.rejects(s.controller({ identity: identity('b'), fingerprint: image('f') }).begin(permitted, signal()), /retained_legacy_provenance_missing/);
  assert.equal([...s.rows.values()][0].publication.workspaceCandidate.metadata.schema, 1);
  const validate = s.provider.validateLegacy;
  s.provider.validateLegacy = async () => { throw Error('retained_legacy_provider_provenance_missing'); };
  await assert.rejects(s.controller({ identity: identity('a') }).begin(permitted, signal()), /retained_legacy_provider_provenance_missing/);
  assert.equal([...s.rows.values()][0].publication.workspaceCandidate.metadata.schema, 1);
  s.provider.validateLegacy = validate;
  const adopted = s.controller({ identity: identity('a') });
  await adopted.begin(permitted, signal()); await adopted.idle(permitted);
  assert.equal([...s.rows.values()][0].publication.workspaceCandidate.metadata.schema, 2);
  assert.equal(s.replacements.length, 0);
});

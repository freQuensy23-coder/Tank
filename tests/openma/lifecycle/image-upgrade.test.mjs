import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { setup } from './fakes.mjs';
import { trustedLegacyBuild } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/legacy-build-provenance.ts';
import { resolveLegacyProvenance, retainedCompatibility } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/legacy-provenance.ts';
import { stableDigest } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts';
import { assertLegacyNativeConfiguration, readNativeLegacyConfiguration } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/legacy-native-provenance.ts';

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

function legacyInputs() {
  return {
    scope: ['test-workspace', 'test-environment', 'test-session'], resources: [], skills: [{ type: 'custom', skillId: 'test-skill', version: '1' }],
    environmentName: 'test-analysis', environment: { type: 'cloud', networking: { type: 'unrestricted' } },
    allocation: { image: 'test-analysis:amd64', rootfsPath: '/opt/test-oci', cpus: 2, memoryMib: 2048, name: 'oma-test-session',
      security: { jailerEnabled: false, seccompEnabled: false }, network: { mode: 'enabled' },
      volumes: [{ hostPath: '/owned/test-output', guestPath: '/mnt/session/outputs', readOnly: false }] },
    databasePolicy: { host: 'test-postgres', port: '5432', database: '/test', role: 'readonly' },
  };
}
function historicalFingerprint(inputs, runtimeIdentity = trustedLegacyBuild.runtimeIdentity) {
  return stableDigest({ ...inputs, runtimeIdentity, rootfsIndex: stableDigest(trustedLegacyBuild.rawIndex) });
}

for (const runtimeIdentity of [trustedLegacyBuild.runtimeIdentity, 'c4afefbdddba3e6f05634a3bb5a46db20d5636413f4f24909b2abcdfa8fa8654']) test(`reviewed legacy fingerprint ${runtimeIdentity} adopts the old image before fenced replacement`, async () => {
  const inputs = legacyInputs(), fingerprint = historicalFingerprint(inputs, runtimeIdentity);
  const resolved = resolveLegacyProvenance(inputs, fingerprint);
  assert.equal(resolved.identity.image, 'sha256:5069a7cd9fe3f987d7839d839ab3a9448ca8c9e5321b4d9a69c533ae887c7f90');
  assert.equal(resolved.provenance.build, '5dd1d3495f79bc203f9d0ea5a2320e148cf7a963b4709fe06dad0ac62430d296');
  const s = fixture();
  const legacy = s.controller({ fingerprint });
  await legacy.begin(permitted, signal()); await legacy.idle(permitted);
  const before = structuredClone([...s.rows.values()][0].publication.workspaceCandidate);
  s.hooks.transfer = async () => {
    const migrating = publication(s);
    assert.equal(migrating.identity.image, resolved.identity.image);
    assert.equal(migrating.legacy.fingerprint, fingerprint);
    assert.equal(migrating.legacy.publicationHash, before.contentHash);
    assert.equal(migrating.legacy.runtimeId, before.metadata.runtimeId);
  };
  const controller = s.controller({ fingerprint: image('f'), identity: { version: 2, image: image('b'), compatibility: retainedCompatibility(inputs) },
    resolveLegacy: saved => resolveLegacyProvenance(inputs, saved) });
  await controller.begin(permitted, signal());
  assert.equal(s.replacements.length, 1);
  assert.equal(publication(s).identity.image, image('b'));
  assert.equal(publication(s).upgradedFrom, resolved.identity.image);
  assert.equal(publication(s).legacy.digest, resolved.provenance.digest);
  assert.equal(publication(s).retired.some(handle => handle.runtimeId === before.metadata.runtimeId), true);
  await controller.idle(permitted);
});

for (const runtimeIdentity of [trustedLegacyBuild.runtimeIdentity, trustedLegacyBuild.runtimeAbi]) test(`altered protected inputs refuse legacy adoption for ${runtimeIdentity}`, () => {
  const inputs = legacyInputs(), original = historicalFingerprint(inputs, runtimeIdentity);
  assert.equal(resolveLegacyProvenance(inputs, historicalFingerprint(inputs, 'f'.repeat(64))), null);
  const mutations = [
    value => value.scope.push('foreign'), value => value.resources.push({ id: 'new-resource' }),
    value => value.skills[0].version = '2', value => value.environmentName = 'other',
    value => value.environment.networking.type = 'limited', value => value.allocation.cpus = 4,
    value => value.allocation.memoryMib = 4096, value => value.allocation.rootfsPath = '/foreign-oci',
    value => value.allocation.volumes[0].readOnly = true, value => value.allocation.network.mode = 'disabled',
    value => value.allocation.security.seccompEnabled = true, value => value.databasePolicy.role = 'writer',
  ];
  for (const mutate of mutations) {
    const changed = structuredClone(inputs); mutate(changed);
    assert.equal(resolveLegacyProvenance(changed, original), null);
  }
});

function nativeLegacyRecord() {
  return { id: 'test-native-id', name: 'test-native-owner', options: {
    cpus: 2, memory_mib: 2048, rootfs: { RootfsPath: '/opt/test-oci' },
    volumes: [{ host_path: '/owned/test-output', guest_path: '/mnt/session/outputs', read_only: false }],
    network: { Enabled: { allow_net: [] } }, env: [], secrets: [], ports: [], auto_remove: false, detach: true, working_dir: '/workspace',
    advanced: { security: { jailer_enabled: false, seccomp_enabled: false, uid: 65534, gid: 65534,
      new_pid_ns: true, new_net_ns: false, chroot_base: '/srv/boxlite', chroot_enabled: true,
      close_fds: true, sanitize_env: true, env_allowlist: ['RUST_LOG'],
      resource_limits: { max_open_files: null, max_file_size: 21474836480, max_processes: null, max_memory: null, max_cpu_time: null },
      sandbox_profile: null, network_enabled: true }, isolate_mounts: false, health_check: null,
      kernel: null, nested_virtualization: false, privileged: false },
  } };
}

test('legacy native provenance binds exact id, ownership, CPU, RAM, rootfs, mounts, network and security records', () => {
  const allocation = legacyInputs().allocation, record = nativeLegacyRecord();
  assert.doesNotThrow(() => assertLegacyNativeConfiguration(record, 'test-native-id', 'test-native-owner', allocation));
  const mutations = [
    value => value.id = 'foreign-id', value => value.name = 'foreign-owner', value => value.options.cpus = 4,
    value => value.options.memory_mib = 4096, value => value.options.rootfs.RootfsPath = '/foreign-image',
    value => value.options.volumes[0].read_only = true, value => value.options.network.Enabled.allow_net.push('foreign'),
    value => value.options.advanced.security.sanitize_env = false, value => value.options.advanced.privileged = true,
    value => value.options.env.push(['UNEXPECTED', 'value']), value => value.options.secrets.push({ name: 'unexpected' }),
  ];
  for (const mutate of mutations) {
    const changed = structuredClone(record); mutate(changed);
    assert.throws(() => assertLegacyNativeConfiguration(changed, 'test-native-id', 'test-native-owner', allocation), /retained_legacy_provider_provenance_missing/);
  }
  assert.throws(() => assertLegacyNativeConfiguration(null, 'test-native-id', 'test-native-owner', allocation), /runtime_ownership/);
});

test('native legacy lookup reads the exact durable configuration without creating or mutating the catalog', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tank-legacy-native-'));
  const previous = process.env.BOXLITE_HOME;
  try {
    process.env.BOXLITE_HOME = directory;
    await assert.rejects(readNativeLegacyConfiguration('test-native-id'), /native_configuration/);
    await mkdir(join(directory, 'db'));
    const database = new DatabaseSync(join(directory, 'db', 'boxlite.db'));
    try {
      database.exec('CREATE TABLE box_config (id TEXT PRIMARY KEY, name TEXT, json TEXT)');
      const record = nativeLegacyRecord();
      database.prepare('INSERT INTO box_config VALUES (?, ?, ?)').run(record.id, record.name, JSON.stringify(record));
      assert.deepEqual(await readNativeLegacyConfiguration(record.id), record);
      assert.equal(await readNativeLegacyConfiguration('foreign-native-id'), null);
      database.prepare('UPDATE box_config SET name = ? WHERE id = ?').run('foreign-owner', record.id);
      await assert.rejects(readNativeLegacyConfiguration(record.id), /native_configuration/);
      assert.equal(database.prepare('SELECT count(*) AS count FROM box_config').get().count, 1);
    } finally { database.close(); }
  } finally {
    if (previous === undefined) delete process.env.BOXLITE_HOME; else process.env.BOXLITE_HOME = previous;
    await rm(directory, { recursive: true, force: true });
  }
});



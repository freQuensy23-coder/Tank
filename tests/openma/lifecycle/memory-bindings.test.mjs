import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, rm, readFile, writeFile, symlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { setup } from './fakes.mjs';
import { retainedMemoryGeneration } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/memory-bindings.ts';
import { retainedCompatibility } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/legacy-provenance.ts';
import { stableDigest } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts';
import { ManagedMemorySnapshotMaterializer, ManagedMemoryBlobWorkspaceSynchronizer, ManagedMemoryWorkspaceReconciler } from '../../../vendor/openma/packages/managed-agents-adapters-runtime/src/managed-memory-snapshot.ts';

const digest = value => createHash('sha256').update(value).digest('hex');
async function fixture(access = 'read_write') {
  const directory = await mkdtemp(join(tmpdir(), 'tank-memory-bindings-'));
  const memoryRoot = join(directory, 'memory'), keys = new Set();
  await mkdir(memoryRoot);
  const blobs = {
    async getText(key) { try { return await readFile(join(memoryRoot, key), 'utf8'); } catch (error) { if (error.code === 'ENOENT') return null; throw error; } },
    async put(key, content) { await mkdir(dirname(join(memoryRoot, key)), { recursive: true }); await writeFile(join(memoryRoot, key), content); keys.add(key); return key; },
    async list(prefix) { return { keys: [...keys].filter(key => key.startsWith(prefix)), nextCursor: null }; },
    async delete(key) { keys.delete(key); await rm(join(memoryRoot, key), { force: true }); },
  };
  const remote = { id: 'mem-one', kind: 'memory', path: '/note.txt', content: 'baseline', contentSha256: digest('baseline') };
  const mutations = [];
  const memories = {
    async listMemories() { return { type: 'page', page: { items: [{ ...remote }], nextCursor: null } }; },
    async updateMemory(input) {
      assert.equal(input.memoryStoreId, 'store-A');
      assert.equal(input.memoryId, 'mem-one');
      assert.deepEqual(input.contentPrecondition, { expectedSha256: digest('baseline') });
      mutations.push(input);
      remote.content = input.content; remote.contentSha256 = digest(input.content);
      return { type: 'updated' };
    },
  };
  const s = setup(), scope = { ...s.options.scope, workId: 'tank-retained-session-v1' };
  const resources = [{ type: 'memory_store', id: 'resource-A', memoryStoreId: 'store-A', name: 'notes', access }];
  const materialize = new ManagedMemorySnapshotMaterializer(memories, blobs);
  const mount = await materialize.materialize({ ...scope, memoryStoreId: 'store-A', access: 'read_write', runtimeGeneration: 'runtime-original' });
  const allocation = { image: 'image-A', volumes: [{ hostPath: join(memoryRoot, mount.mountStoreId), guestPath: '/mnt/memory/notes', readOnly: false }] };
  const inputs = { scope: [scope.workspaceId, scope.environmentId, scope.sessionId], resources, skills: [], environmentName: 'analysis', environment: {}, allocation, databasePolicy: null };
  const identity = image => ({ version: 2, image: 'sha256:' + image.repeat(64), compatibility: retainedCompatibility(inputs) });
  const controller = s.controller({ identity: identity('a') });
  await controller.begin(async () => {}, new AbortController().signal); await controller.idle(async () => {});
  const candidate = [...s.rows.values()][0].publication.workspaceCandidate;
  const original = JSON.parse(candidate.metadata.manifest).handle;
  const name = 'oma-' + digest(`litebox:${scope.sessionId}`).slice(0, 32);
  const record = { id: original.runtimeId, name, options: { volumes: allocation.volumes.map(volume => ({ host_path: volume.hostPath, guest_path: volume.guestPath, read_only: volume.readOnly })) } };
  const input = { scope, candidate, resources, proposed: 'runtime-new-process', memoryRoot,
    inspect: async () => ({ id: original.runtimeId, name }), getText: key => blobs.getText(key), readConfiguration: async () => record };
  return { ...s, directory, memoryRoot, blobs, memories, mutations, remote, mount, input, record, allocation, identity,
    cleanup: () => rm(directory, { recursive: true, force: true }) };
}

test('fresh process recovers native writable-memory generation and image upgrade keeps fenced synchronization on its original data', async () => {
  const s = await fixture();
  try {
    await s.blobs.put(`${s.mount.mountStoreId}/note.txt`, 'unsynchronized local edit');
    await mkdir(join(s.directory, 'db'));
    const database = new DatabaseSync(join(s.directory, 'db', 'boxlite.db'));
    database.exec('CREATE TABLE box_config (id TEXT PRIMARY KEY, name TEXT, json TEXT)');
    database.prepare('INSERT INTO box_config VALUES (?, ?, ?)').run(s.record.id, s.record.name, JSON.stringify(s.record));
    database.close();
    const code = `
      import { readFile } from 'node:fs/promises';
      import { join } from 'node:path';
      import { retainedMemoryGeneration } from ${JSON.stringify(new URL('../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/memory-bindings.ts', import.meta.url).href)};
      import { ManagedMemorySnapshotMaterializer } from ${JSON.stringify(new URL('../../../vendor/openma/packages/managed-agents-adapters-runtime/src/managed-memory-snapshot.ts', import.meta.url).href)};
      const input = JSON.parse(process.argv[1]);
      const blobs = { getText: async key => { try { return await readFile(join(input.memoryRoot, key), 'utf8'); } catch (error) { if (error.code === 'ENOENT') return null; throw error; } }, put: async () => { throw Error('must not overwrite retained data'); } };
      const generation = await retainedMemoryGeneration({ ...input, inspect: async () => input.native, getText: blobs.getText });
      const mount = await new ManagedMemorySnapshotMaterializer({ listMemories: async () => { throw Error('must not project over retained data'); } }, blobs).materialize({ ...input.scope, memoryStoreId: 'store-A', access: 'read_write', runtimeGeneration: generation });
      console.log(JSON.stringify({ generation, mount, text: await blobs.getText(mount.mountStoreId + '/note.txt') }));
    `;
    const child = spawnSync(process.execPath, ['--experimental-transform-types', '--import', fileURLToPath(new URL('../../../scripts/node-typescript-loader.mjs', import.meta.url)), '--input-type=module', '-e', code,
      JSON.stringify({ ...s.input, native: { id: s.record.id, name: s.record.name } })], { encoding: 'utf8', timeout: 5000, env: { ...process.env, BOXLITE_HOME: s.directory } });
    assert.equal(child.status, 0, child.stderr);
    const recovered = JSON.parse(child.stdout);
    assert.equal(recovered.generation, 'runtime-original');
    assert.deepEqual(recovered.mount, s.mount);
    assert.equal(recovered.text, 'unsynchronized local edit');
    const restarted = s.controller({ identity: s.identity('b') });
    s.provider.replacementIntent = async () => ({ provider: 'litebox', runtimeId: 'upgrade-intent' });
    s.provider.replace = async (_journal, scope, _fence, _signal, authorize) => { await authorize(); return s.provider.create(scope); };
    await restarted.begin(async () => {}, new AbortController().signal);
    assert.equal(JSON.parse([...s.rows.values()][0].publication.workspaceCandidate.metadata.manifest).identity.image, s.identity('b').image);
    const synchronizer = new ManagedMemoryBlobWorkspaceSynchronizer(s.blobs, new ManagedMemoryWorkspaceReconciler(s.memories), s.memories);
    let authorized = true;
    const syncInput = { ...s.input.scope, runtimeGeneration: recovered.generation, memoryStoreId: 'store-A', authorize: async () => authorized };
    const result = await synchronizer.synchronize(syncInput);
    assert.equal(result.type, 'reconciled');
    assert.equal(result.updated, 1);
    assert.equal(s.remote.content, 'unsynchronized local edit');
    assert.equal(s.mutations.length, 1);
    await s.blobs.put(`${s.mount.mountStoreId}/note.txt`, 'late stale-owner edit');
    authorized = false;
    assert.equal((await synchronizer.synchronize(syncInput)).type, 'fence_lost');
    assert.equal(s.mutations.length, 1);
    await restarted.idle(async () => {});
  } finally { await s.cleanup(); }
});

test('nullable memory access retains writable generation without normalizing the protected resource', async () => {
  const s = await fixture(null);
  try {
    await s.blobs.put(`${s.mount.mountStoreId}/note.txt`, 'retained nullable-access edit');
    assert.equal(await retainedMemoryGeneration(s.input), 'runtime-original');
    assert.equal(s.input.resources[0].access, null);
    assert.equal(await s.blobs.getText(`${s.mount.mountStoreId}/note.txt`), 'retained nullable-access edit');
    s.input.resources[0].access = 'read_write';
    await assert.rejects(retainedMemoryGeneration(s.input), /retained_configuration_changed:resources/);
  } finally { await s.cleanup(); }
});

for (const changed of ['owner', 'store', 'workspace', 'access', 'guest', 'host', 'baseline', 'marker', 'symlink']) test(`retained writable-memory generation refuses unproven ${changed} binding without rewriting data`, async () => {
  const s = await fixture();
  try {
    const before = await s.blobs.getText(`${s.mount.mountStoreId}/note.txt`);
    if (changed === 'owner') s.record.name = 'foreign';
    if (changed === 'store') s.input.resources[0].memoryStoreId = 'store-other';
    if (changed === 'workspace') s.input.scope.workspaceId = 'workspace-other';
    if (changed === 'access') s.record.options.volumes[0].read_only = true;
    if (changed === 'guest') s.record.options.volumes[0].guest_path = '/mnt/memory/foreign';
    if (changed === 'host') s.record.options.volumes[0].host_path = join(s.memoryRoot, 'foreign');
    if (changed === 'baseline') await s.blobs.delete(s.mount.mountStoreId.replace(/\/data$/, '/manifest.json'));
    if (changed === 'marker') await s.blobs.put(`${s.mount.mountStoreId}/.openma-memory-store`, 'foreign marker');
    if (changed === 'symlink') {
      const real = s.allocation.volumes[0].hostPath, moved = real + '-moved';
      const { rename } = await import('node:fs/promises');
      await rename(real, moved); await symlink(moved, real);
    }
    await assert.rejects(retainedMemoryGeneration(s.input), changed === 'store' ? /retained_configuration_changed:resources/ : /retained_memory_binding_unproven/);
    assert.equal(await s.blobs.getText(`${s.mount.mountStoreId}/note.txt`), before);
  } finally { await s.cleanup(); }
});

test('physical host mounts remain protected independently of logical memory ownership', async () => {
  const s = await fixture();
  try {
    const original = s.identity('a');
    s.allocation.volumes[0].hostPath += '-foreign';
    assert.notEqual(s.identity('b').compatibility.mounts, original.compatibility.mounts);
    await assert.rejects(s.controller({ identity: s.identity('b') }).begin(async () => {}, new AbortController().signal), /retained_configuration_changed:mounts/);
    assert.equal(stableDigest(s.input.candidate), stableDigest([...s.rows.values()][0].publication.workspaceCandidate));
  } finally { await s.cleanup(); }
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { executableImageIdentity, assertCompatible, retainedSessionResources, stableDigest } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts';

async function layout(fn) {
  const root = await mkdtemp(join(tmpdir(), 'tank-image-identity-'));
  await mkdir(join(root, 'blobs', 'sha256'), { recursive: true });
  const blob = async (value, mediaType = 'application/vnd.oci.image.manifest.v1+json') => {
    const bytes = Buffer.isBuffer(value) ? value : Buffer.from(JSON.stringify(value));
    const digest = createHash('sha256').update(bytes).digest('hex');
    await writeFile(join(root, 'blobs', 'sha256', digest), bytes);
    return { digest: 'sha256:' + digest, size: bytes.length, mediaType };
  };
  const publish = async ({ created = 'old', env = ['MARKER=A'], architecture = 'amd64', annotation = 'old', layer = 'base-A', compression = null, mtime = 0 } = {}) => {
    const uncompressed = Buffer.from(layer);
    const bytes = compression === null ? uncompressed : gzipSync(uncompressed, { level: compression });
    if (compression !== null) bytes.writeUInt32LE(mtime, 4);
    const data = await blob(bytes, 'application/vnd.oci.image.layer.v1.tar' + (compression === null ? '' : '+gzip'));
    const diffId = 'sha256:' + createHash('sha256').update(uncompressed).digest('hex');
    const config = await blob({ architecture, os: 'linux', created, history: [{ created, created_by: 'builder-' + annotation }],
      rootfs: { type: 'layers', diff_ids: [diffId] }, config: { Env: env, Cmd: ['/bin/sh'] } }, 'application/vnd.oci.image.config.v1+json');
    const manifest = await blob({ schemaVersion: 2, config, layers: [data], annotations: { export: annotation } });
    await writeFile(join(root, 'index.json'), JSON.stringify({ schemaVersion: 2, manifests: [{ ...manifest, platform: { architecture, os: 'linux' } }], annotations: { exported: annotation } }, null, annotation === 'old' ? 0 : 2));
    return { data, config, manifest };
  };
  try { await fn({ root, blob, publish }); } finally { await rm(root, { recursive: true, force: true }); }
}

test('OCI timestamps, history, index formatting and export annotations do not replace executable identity', async () => {
  await layout(async ({ root, publish }) => {
    await publish(); const before = await executableImageIdentity(root, 'x64');
    await publish({ created: 'new', annotation: 'new' });
    assert.equal(await executableImageIdentity(root, 'x64'), before);
    await publish({ env: ['MARKER=B'] });
    assert.notEqual(await executableImageIdentity(root, 'x64'), before);
    await publish({ layer: 'base-B' });
    assert.notEqual(await executableImageIdentity(root, 'x64'), before);
  });
});

test('OCI recompression and gzip metadata preserve executable identity while compressed blob integrity remains mandatory', async () => {
  await layout(async ({ root, publish }) => {
    const plain = await publish();
    const expected = await executableImageIdentity(root, 'x64');
    const compressed = await publish({ compression: 1 });
    assert.notEqual(compressed.data.digest, plain.data.digest);
    assert.equal(compressed.config.digest, plain.config.digest);
    assert.equal(await executableImageIdentity(root, 'x64'), expected);
    const reexported = await publish({ compression: 9, mtime: 12345 });
    assert.notEqual(reexported.data.digest, compressed.data.digest);
    assert.equal(reexported.config.digest, compressed.config.digest);
    assert.equal(await executableImageIdentity(root, 'x64'), expected);
    await publish({ compression: 9, layer: 'base-B' });
    assert.notEqual(await executableImageIdentity(root, 'x64'), expected);
    await publish({ compression: 9, mtime: 12345 });
    await writeFile(join(root, 'blobs', 'sha256', reexported.data.digest.slice(7)), 'corrupt gzip');
    await assert.rejects(executableImageIdentity(root, 'x64'), /retained_image_blob_invalid/);
  });
});

test('OCI identity selects exact architecture and refuses ambiguous, missing and corrupted executable blobs', async () => {
  await layout(async ({ root, publish }) => {
    const first = await publish();
    const expected = await executableImageIdentity(root, 'x64');
    const second = await publish({ architecture: 'arm64', layer: 'arm64-base' });
    await writeFile(join(root, 'index.json'), JSON.stringify({ schemaVersion: 2, manifests: [
      { ...second.manifest, platform: { architecture: 'arm64', os: 'linux' } },
      { ...first.manifest, platform: { architecture: 'amd64', os: 'linux' } },
    ] }));
    assert.equal(await executableImageIdentity(root, 'x64'), expected);
    assert.notEqual(await executableImageIdentity(root, 'arm64'), expected);
    await writeFile(join(root, 'index.json'), JSON.stringify({ schemaVersion: 2, manifests: [first.manifest, first.manifest] }));
    await assert.rejects(executableImageIdentity(root, 'x64'), /retained_image_selection_ambiguous/);
    await publish();
    await writeFile(join(root, 'blobs', 'sha256', first.data.digest.slice(7)), 'corruption');
    await assert.rejects(executableImageIdentity(root, 'x64'), /retained_image_blob_invalid/);
  });
});

test('compatibility comparison reports every changed or missing component without exposing configuration', () => {
  const sha = value => 'sha256:' + value.repeat(64);
  const old = { version: 2, image: sha('a'), compatibility: { network: sha('a'), mounts: sha('b'), databasePolicy: sha('c') } };
  const next = { version: 2, image: sha('b'), compatibility: { mounts: sha('b'), databasePolicy: sha('d') } };
  assert.throws(() => assertCompatible(old, next), /^Error: retained_configuration_changed:databasePolicy,network$/);
  assert.doesNotThrow(() => assertCompatible(old, { ...old, image: sha('b') }));
});

test('missing optional identity components retain the original serialization digest', () => {
  assert.equal(stableDigest(undefined), 'sha256:' + createHash('sha256').update('undefined').digest('hex'));
  assert.equal(stableDigest({ b: 2, a: 1 }), stableDigest({ a: 1, b: 2 }));
});

test('reader file resources leave retained resource digests at their recorded values', () => {
  const repository = { type: 'github_repository', id: 'sesrsc_repo', url: 'https://github.com/acme/repo', mountPath: '/workspace/repo', checkout: { type: 'branch', name: 'main' } };
  const memory = { type: 'memory_store', id: 'sesrsc_memory', memoryStoreId: 'memstore_1', access: 'read_only', name: 'notes' };
  const file = { type: 'file', id: 'sesrsc_file', fileId: 'file_1', mountPath: '/workspace/uploads/20261108T000000Z/attachment-0/report.pdf' };
  const recorded = [
    { resources: [], component: 'sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945',
      fingerprint: 'sha256:07cb3c9cdec39790f579be3e9caa9572f8bc5e5f34625188ff97477ea52872ab' },
    { resources: [repository, memory], component: 'sha256:28b50270c9d75778440e4a85bb43fcbadefe15bbc921367c81001c661b11488b',
      fingerprint: 'sha256:94bd62cfee7aab92e9fef51f7ffd07221f5c59a2c2ac1aaa170618d56c948bfd' },
  ];
  for (const { resources, component, fingerprint } of recorded) {
    assert.equal(stableDigest(resources), component);
    assert.equal(stableDigest({ resources, skills: [] }), fingerprint);
    assert.equal(JSON.stringify(retainedSessionResources(resources)), JSON.stringify(resources));
    assert.equal(stableDigest(retainedSessionResources(resources)), component);
    assert.equal(stableDigest({ resources: retainedSessionResources(resources), skills: [] }), fingerprint);
    const withFiles = [file, ...resources, { ...file, id: 'sesrsc_file_2', fileId: 'file_2' }];
    assert.notEqual(stableDigest(withFiles), component);
    assert.equal(stableDigest(retainedSessionResources(withFiles)), component);
    assert.equal(stableDigest({ resources: retainedSessionResources(withFiles), skills: [] }), fingerprint);
  }
  assert.equal(retainedSessionResources(undefined), undefined);
});

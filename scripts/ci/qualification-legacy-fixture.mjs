import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { createReadStream } from 'node:fs';
import { lstat, readFile, realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { trustedLegacyBuild } from '../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/legacy-build-provenance.ts';

export const historicalController = Object.freeze({
  image: 'ghcr.io/frequensy23-coder/tank/tank-openma@sha256:f206f5766e470e2361a524e652b811c502515b807d39e0ec11089138ce2afb8c',
  sourceSha: 'ec4f1e1ff6785d1e4aa39f13efefef0cfa4390ff',
});

export async function verifyHistoricalLayout(root) {
  assert.equal(await realpath(root), root, 'historical fixture root must not traverse symlinks');

  const indexPath = join(root, 'index.json');
  const indexInfo = await lstat(indexPath);
  assert.ok(indexInfo.isFile() && !indexInfo.isSymbolicLink());
  const bytes = await readFile(indexPath);
  const indexSha256 = 'sha256:' + createHash('sha256').update(bytes).digest('hex');
  assert.equal(indexSha256, trustedLegacyBuild.indexRawSha256, 'published historical controller does not contain the recognized original OCI index');
  assert.equal(bytes.toString('utf8'), trustedLegacyBuild.rawIndex, 'historical OCI index differs from the recognized original');
  const descriptors = [];
  for (const descriptor of trustedLegacyBuild.descriptors) {
    assert.match(descriptor.digest, /^sha256:[a-f0-9]{64}$/);
    const path = join(root, 'blobs', 'sha256', descriptor.digest.slice(7));
    assert.equal(await realpath(path), path);
    const info = await lstat(path);
    assert.ok(info.isFile() && !info.isSymbolicLink());
    assert.equal(info.size, descriptor.bytes, 'historical OCI descriptor size changed');
    const hash = createHash('sha256');
    for await (const data of createReadStream(path)) hash.update(data);
    assert.equal('sha256:' + hash.digest('hex'), descriptor.digest, 'historical OCI descriptor bytes changed');
    descriptors.push({ digest: descriptor.digest, bytes: info.size });
  }
  const manifest = JSON.parse(await readFile(join(root, 'blobs', 'sha256', trustedLegacyBuild.manifest.slice(7)), 'utf8'));
  const executables = {};
  for (const layer of manifest.layers) {
    assert.ok(descriptors.some(row => row.digest === layer.digest && row.bytes === layer.size));
    assert.equal(layer.mediaType, 'application/vnd.oci.image.layer.v1.tar+gzip');
    for (const path of ['usr/local/bin/python3.12', 'opt/tank/browser-worker.py']) {
      const result = spawnSync('tar', ['--extract', '--gzip', '--to-stdout', '--file', join(root, 'blobs', 'sha256', layer.digest.slice(7)), path], { timeout: 120000, maxBuffer: 16 * 1024 ** 2 });
      if (result.status === 0) {
        assert.ok(result.stdout.length > 0);
        executables['/' + path] = createHash('sha256').update(result.stdout).digest('hex');
      } else assert.ok(result.status === 2 && result.stderr.toString().includes('Not found in archive'), 'historical executable extraction failed');
    }
  }
  assert.deepEqual(Object.keys(executables).sort(), ['/opt/tank/browser-worker.py', '/usr/local/bin/python3.12']);
  return { indexSha256, descriptors, executables, semanticImage: trustedLegacyBuild.image, controller: historicalController };
}

const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const fs = require('node:fs');
const { tmpdir } = require('node:os');
const { join, resolve } = require('node:path');
const { spawnSync } = require('node:child_process');
const { test } = require('node:test');
const { pathToFileURL } = require('node:url');

const entrypointPath = resolve(process.argv[2] || join(__dirname, '../vendor/openma/tank-deploy/openma_entrypoint.mjs'));
const entrypoint = import(pathToFileURL(entrypointPath).href);
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');

function fixture(architecture = 'amd64') {
  const shim = Buffer.alloc(64);
  Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1]).copy(shim);
  shim.writeUInt16LE(architecture === 'amd64' ? 62 : 183, 18);
  const provenance = Buffer.from(JSON.stringify({
    architecture, source: { version: '0.10.6' },
    qualification: { native: 'NOT_RUN', production_adoption: false },
  }));
  const binding = { shim_sha256: sha256(shim), provenance_sha256: sha256(provenance) };
  const receipt = architecture === 'amd64' ? {
    ...binding, schema: 'tank.boxlite-native-qualification.v1', status: 'PASS', architecture,
    checks: { kvm: 'PASS', state_persistence: 'PASS', dns: 'PASS', replay_isolation: 'PASS' },
  } : {
    ...binding, schema: 'tank.boxlite-qualified-replay-dns-artifact.v1', status: 'QUALIFIED_NATIVE_REPLAY_NETWORK',
    source: { version: '0.10.6' }, files: { 'boxlite-shim': binding.shim_sha256 },
    qualification: { status: 'PASS', scope: 'native factory replay network and read-only database component' },
  };
  return { shim, provenance, qualifications: { [architecture]: receipt } };
}

function probe(input, includeReceipt = true) {
  const root = fs.mkdtempSync(join(tmpdir(), 'tank-native-admission-'));
  try {
    fs.mkdirSync(join(root, 'tank-deploy'));
    fs.mkdirSync(join(root, 'native'));
    fs.copyFileSync(entrypointPath, join(root, 'tank-deploy/openma_entrypoint.mjs'));
    fs.writeFileSync(join(root, 'native/boxlite-shim'), input.shim);
    fs.writeFileSync(join(root, 'native/provenance.json'), input.provenance);
    if (includeReceipt) fs.writeFileSync(join(root, 'tank-native-qualification.json'), JSON.stringify(input.qualifications));
    const script = `
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
const read = fs.readFileSync;
fs.readFileSync = (path, ...args) => {
  if (String(path).startsWith('/run/secrets/')) throw Error('TEST_SECRET_READ_REACHED');
  return read(path, ...args);
};
syncBuiltinESMExports();
const { main } = await import(process.argv[1]);
await main();
`;
    return spawnSync(process.execPath, ['--input-type=module', '-e', script, pathToFileURL(join(root, 'tank-deploy/openma_entrypoint.mjs')).href], {
      env: { PATH: process.env.PATH, BOXLITE_RUNTIME_DIR: join(root, 'native') }, encoding: 'utf8', timeout: 5000,
    });
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

test('native admission binds each architecture to its authentic qualification scope and exact bytes', async () => {
  const { verifyNativeQualification } = await entrypoint;
  for (const architecture of ['amd64', 'arm64']) {
    const input = fixture(architecture);
    const before = Buffer.from(input.provenance);
    assert.equal(verifyNativeQualification(input.shim, input.provenance, input.qualifications, {
      platform: 'linux', arch: architecture === 'amd64' ? 'x64' : 'arm64',
    }), architecture);
    assert.deepEqual(input.provenance, before);
    assert.equal(JSON.parse(input.provenance).qualification.native, 'NOT_RUN');
    assert.equal(JSON.parse(input.provenance).qualification.production_adoption, false);
  }
});

test('native admission refuses missing, mismatched, incomplete and cross-architecture qualifications', async () => {
  const { verifyNativeQualification } = await entrypoint;
  const changes = [
    input => { input.qualifications = {}; },
    input => { input.qualifications.amd64.status = 'NOT_RUN'; },
    input => { input.qualifications.amd64.architecture = 'arm64'; },
    input => { input.qualifications.amd64.shim_sha256 = '0'.repeat(64); },
    input => { input.qualifications.amd64.provenance_sha256 = '0'.repeat(64); },
    input => { delete input.qualifications.amd64.checks.state_persistence; },
    input => { input.qualifications.amd64.checks.dns = 'FAIL'; },
    input => { input.qualifications.amd64.schema = 'tank.boxlite-qualified-replay-dns-artifact.v1'; },
    input => { input.shim[63] = 1; },
    input => { input.provenance = Buffer.concat([input.provenance, Buffer.from(' ')]); },
    input => { input.provenance = Buffer.from(JSON.stringify({ architecture: 'amd64', source: { version: '0.9.0' } })); input.qualifications.amd64.provenance_sha256 = sha256(input.provenance); },
  ];
  for (const change of changes) {
    const input = fixture(); change(input);
    assert.throws(() => verifyNativeQualification(input.shim, input.provenance, input.qualifications, { platform: 'linux', arch: 'x64' }), /native component qualification/);
  }
  for (const change of [
    input => { input.qualifications.arm64.qualification.status = 'NOT_RUN'; },
    input => { input.qualifications.arm64.files['boxlite-shim'] = '0'.repeat(64); },
    input => { input.qualifications.arm64.schema = 'tank.boxlite-native-qualification.v1'; },
  ]) {
    const input = fixture('arm64'); change(input);
    assert.throws(() => verifyNativeQualification(input.shim, input.provenance, input.qualifications, { platform: 'linux', arch: 'arm64' }), /native component qualification/);
  }
  assert.throws(() => verifyNativeQualification(fixture().shim, fixture().provenance, fixture().qualifications, { platform: 'linux', arch: 'arm64' }), /architecture differs/);
});

test('startup admits exact qualified bytes and rejects absent or invalid qualification before secret access', () => {
  const architecture = process.arch === 'arm64' ? 'arm64' : 'amd64';
  const accepted = probe(fixture(architecture));
  assert.equal(accepted.status, 1, accepted.stderr);
  assert.match(accepted.stderr, /TEST_SECRET_READ_REACHED/);
  const missing = probe(fixture(architecture), false);
  assert.equal(missing.status, 1, missing.stderr);
  assert.match(missing.stderr, /ENOENT.*tank-native-qualification.json/);
  assert.doesNotMatch(missing.stderr, /TEST_SECRET_READ_REACHED/);
  for (const change of [
    input => { input.qualifications = {}; },
    input => { input.qualifications[architecture].provenance_sha256 = '0'.repeat(64); },
    input => { input.qualifications[architecture].shim_sha256 = '0'.repeat(64); },
    input => { if (architecture === 'amd64') delete input.qualifications.amd64.checks.state_persistence; else input.qualifications.arm64.qualification.status = 'NOT_RUN'; },
  ]) {
    const input = fixture(architecture); change(input);
    const rejected = probe(input);
    assert.equal(rejected.status, 1, rejected.stderr);
    assert.match(rejected.stderr, /native component qualification/);
    assert.doesNotMatch(rejected.stderr, /TEST_SECRET_READ_REACHED/);
  }
});

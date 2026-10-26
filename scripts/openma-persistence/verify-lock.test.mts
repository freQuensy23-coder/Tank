import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test, type TestContext } from 'node:test';
import { pathToFileURL } from 'node:url';
import { requireDisposablePostgresDsn } from './database.mts';
import { DEFAULT_LOCK_PATHS, verifyPersistenceLocks, type PersistenceLockPaths } from './verify-lock.mts';

function inputs(t: TestContext): PersistenceLockPaths {
  const directory = mkdtempSync(join(tmpdir(), 'tank-persistence-lock-'));
  t.after((): void => { rmSync(directory, { recursive: true, force: true }); });
  const copied: PersistenceLockPaths = {
    manifest: pathToFileURL(join(directory, 'package.json')),
    lock: pathToFileURL(join(directory, 'package-lock.json')),
    upstream: pathToFileURL(join(directory, 'pnpm-lock.yaml')),
  };
  writeFileSync(copied.manifest, readFileSync(DEFAULT_LOCK_PATHS.manifest));
  writeFileSync(copied.lock, readFileSync(DEFAULT_LOCK_PATHS.lock));
  writeFileSync(copied.upstream, readFileSync(DEFAULT_LOCK_PATHS.upstream));
  return copied;
}
function changedSection(document: string, heading: string, original: string, changed: string): string {
  const start = document.indexOf('\n  ' + heading + ':\n');
  assert.ok(start >= 0);
  const field = document.indexOf(original, start);
  assert.ok(field > start);
  return document.slice(0, field) + changed + document.slice(field + original.length);
}

test('declared frozen closure matches the actual upstream importers, artifacts and transitive resolutions', (): void => {
  verifyPersistenceLocks(DEFAULT_LOCK_PATHS);
});
for (const kind of ['manifest', 'lock']) test('changed frozen ' + kind + ' refuses before dependency setup', (t: TestContext): void => {
  const copied = inputs(t);
  const target = kind === 'manifest' ? copied.manifest : copied.lock;
  writeFileSync(target, readFileSync(target, 'utf8') + ' ');
  assert.throws((): void => { verifyPersistenceLocks(copied); }, /input bytes changed/);
});
test('duplicate JSON facts cannot bypass sealed dependency inputs', (t: TestContext): void => {
  const copied = inputs(t);
  const document = readFileSync(copied.lock, 'utf8');
  writeFileSync(copied.lock, document.replace('"lockfileVersion": 3', '"lockfileVersion": 2, "lockfileVersion": 3'));
  assert.throws((): void => { verifyPersistenceLocks(copied); }, /input bytes changed/);
});
test('a drifted upstream importer cannot keep testing a stale native dependency version', (t: TestContext): void => {
  const copied = inputs(t);
  const upstream = readFileSync(copied.upstream, 'utf8');
  writeFileSync(copied.upstream, changedSection(upstream, 'packages/session-runtime-sql', '        version: 3.22.1', '        version: 3.22.0'));
  assert.throws((): void => { verifyPersistenceLocks(copied); }, /does not match/);
});
test('a changed upstream transitive resolution or archive integrity is refused', (t: TestContext): void => {
  const copied = inputs(t);
  const upstream = readFileSync(copied.upstream, 'utf8');
  writeFileSync(copied.upstream, changedSection(upstream, 'fast-check@3.23.2', '      pure-rand: 6.1.0', '      pure-rand: 6.0.0'));
  assert.throws((): void => { verifyPersistenceLocks(copied); }, /does not match/);
  writeFileSync(copied.upstream, upstream.replace('resolution: {integrity: sha512-GD3qdB0x1', 'resolution: {integrity: sha512-BAD3qdB0x1'));
  assert.throws((): void => { verifyPersistenceLocks(copied); }, /does not match/);
});
test('the explicit disposable database preserves allowed credentials without exposing them', (): void => {
  for (const host of ['127.0.0.1', 'localhost', 'pg']) {
    const value = 'postgresql://tank_ci:fake-disposable@' + host + ':5432/tank_test_persistence';
    assert.equal(requireDisposablePostgresDsn(value), value);
  }
});
for (const [index, value] of [undefined, '', 'FAKE_SECRET_VALUE', 'postgresql://tank_ci:fake@production.invalid/tank_test_persistence',
  'postgresql://tank_ci:fake@127.0.0.1/production', 'postgresql://postgres:fake@127.0.0.1/tank_test_persistence'].entries()) {
  test('invalid or absent disposable database is refused without input echo ' + index, (): void => {
    assert.throws((): void => { requireDisposablePostgresDsn(value); }, (error: unknown): boolean => {
      assert.ok(error instanceof Error);
      assert.ok(!error.message.includes('FAKE_SECRET_VALUE'));
      assert.ok(!error.message.includes('fake@'));
      return true;
    });
  });
}

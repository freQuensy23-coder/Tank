import assert from 'node:assert/strict';
import test from 'node:test';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { PROJECTION_MODULE, verifyHistoryModuleReport, verifyTankProjectionModuleReport } from '../../../scripts/openma-persistence/history-module.mts';
import { WorkspaceExports } from '../../../scripts/openma-persistence/workspace-exports.mts';

const repository = fileURLToPath(new URL('../../../', import.meta.url));
const module = resolve(repository, 'vendor/openma/packages/managed-agents-adapters-sql/test/session-event-source-position.test.ts');
function report(): string {
  return JSON.stringify({success: true, numTotalTests: 5, numPassedTests: 5, numFailedTests: 0, numPendingTests: 0,
    testResults: [{status: 'passed', name: module, assertionResults: [
      {fullName: 'snapshot', status: 'passed', duration: 1},
      {fullName: 'replay', status: 'passed', duration: 2},
      {fullName: 'legacy', status: 'passed', duration: 3},
      {fullName: 'acceptance false', status: 'passed', duration: 4},
      {fullName: 'acceptance true', status: 'passed', duration: 5},
    ]}],
  });
}
function projectionReport(): string {
  return JSON.stringify({success: true, numTotalTests: 3, numPassedTests: 3, numFailedTests: 0, numPendingTests: 0,
    testResults: [{status: 'passed', name: resolve(repository, PROJECTION_MODULE), assertionResults: [
      {fullName: 'default', status: 'passed', duration: 100},
      {fullName: 'Tank binding', status: 'passed', duration: 1500},
      {fullName: 'refusals', status: 'passed', duration: 300},
    ]}],
  });
}
test('actual PG report requires all owned integration cases with its declared deadline', () => {
  assert.deepEqual(verifyTankProjectionModuleReport(projectionReport(), resolve(repository, PROJECTION_MODULE)), {
    schema: 'tank.persistence.tank-projection.v1', cases: 3, passed: 3, failed: 0, skipped: 0, todo: 0, max_ms: 1500, p90_ms: 1500,
  });
  assert.throws(() => verifyTankProjectionModuleReport(projectionReport(), module), /incomplete or inconsistent/);
});
for (const [from, to] of [['"success":true', '"success":false'], ['"numPassedTests":3', '"numPassedTests":2'],
  ['"numPendingTests":0', '"numPendingTests":1'], ['"duration":1500', '"duration":10000']]) {
  test('failed, skipped or over-deadline PG report refuses ' + to, () => {
    assert.throws(() => verifyTankProjectionModuleReport(projectionReport().replace(from, to), resolve(repository, PROJECTION_MODULE)), /incomplete or inconsistent/);
  });
}
test('full module report verifies exact complete source and actual case metrics', () => {
  assert.deepEqual(verifyHistoryModuleReport(report(), module), {
    schema: 'tank.persistence.history-module.v1', cases: 5, passed: 5, failed: 0, skipped: 0, todo: 0, max_ms: 5, p90_ms: 5,
  });
});
for (const [from, to] of [
  ['"success":true', '"success":1'], ['"numPendingTests":0', '"numPendingTests":false'],
  ['"numPendingTests":0', '"numPendingTests":1'], ['"numTotalTests":5', '"numTotalTests":2'],
  ['"numPassedTests":5', '"numPassedTests":2'], ['"numFailedTests":0', '"numFailedTests":1'],
  ['"status":"passed"', '"status":"pending"'], ['"fullName":"replay"', '"fullName":"snapshot"'],
  ['"duration":3', '"duration":1000'], ['"duration":3', '"duration":10000'],
]) {
  test('partial, skipped or malformed module report refuses ' + to, () => {
    assert.throws(() => verifyHistoryModuleReport(report().replace(from, to), module), /incomplete or inconsistent/);
  });
}
test('a different source file cannot stand in for the protected module', () => {
  assert.throws(() => verifyHistoryModuleReport(report(), module + '.other'), /incomplete or inconsistent/);
});
test('an internally consistent three-case report cannot omit the expanded parameter cases', () => {
  const partial = JSON.stringify({success: true, numTotalTests: 3, numPassedTests: 3, numFailedTests: 0, numPendingTests: 0,
    testResults: [{status: 'passed', name: module, assertionResults: [
      {fullName: 'snapshot', status: 'passed', duration: 1},
      {fullName: 'replay', status: 'passed', duration: 2},
      {fullName: 'legacy', status: 'passed', duration: 3},
    ]}],
  });
  assert.throws(() => verifyHistoryModuleReport(partial, module), /incomplete or inconsistent/);
});
test('workspace resolver uses actual public barrel and wildcard exports without leaf substitutions', () => {
  const exports = new WorkspaceExports(repository);
  assert.equal(exports.resolve('@open-managed-agents/managed-agents-application'), resolve(repository, 'vendor/openma/packages/managed-agents-application/src/index.ts'));
  assert.equal(exports.resolve('@open-managed-agents/sql-client'), resolve(repository, 'vendor/openma/packages/sql-client/src/index.ts'));
  assert.equal(exports.resolve('@open-managed-agents/domain/sessions'), resolve(repository, 'vendor/openma/packages/managed-agents-domain/src/sessions/index.ts'));
  assert.equal(exports.resolve('@open-managed-agents/managed-agents-application/ports/sessions'), resolve(repository, 'vendor/openma/packages/managed-agents-application/src/ports/sessions.ts'));
  assert.equal(exports.resolve('@earendil-works/pi-ai/providers/all'), null);
  assert.equal(exports.resolve('effect'), null);
  assert.throws(() => exports.resolve('@open-managed-agents/sql-client/not-declared'), /public export/);
  assert.throws(() => exports.resolve('@open-managed-agents/managed-agents-application/ports/../../package.json'), /public export/);
  assert.throws(() => exports.resolve('@open-managed-agents/managed-agents-application/ports/../index'), /public export/);
  assert.throws(() => exports.resolve('@open-managed-agents/managed-agents-application/ports/%2e%2e/index'), /public export/);
});
test('strict compiler paths preserve declared public barrels and wildcard targets', () => {
  const paths = new WorkspaceExports(repository).typeScriptPaths();
  assert.deepEqual(paths['@open-managed-agents/managed-agents-application'], [resolve(repository, 'vendor/openma/packages/managed-agents-application/src/index.ts')]);
  assert.deepEqual(paths['@open-managed-agents/session-store'], [resolve(repository, 'vendor/openma/packages/session-store/src/index.ts')]);
  assert.deepEqual(paths['@open-managed-agents/sql-client'], [resolve(repository, 'vendor/openma/packages/sql-client/src/index.ts')]);
  assert.deepEqual(paths['@open-managed-agents/managed-agents-application/ports/*'], [resolve(repository, 'vendor/openma/packages/managed-agents-application/src/ports/*.ts')]);
  assert.equal(paths['@open-managed-agents/sql-client/not-declared'], undefined);
  assert.equal(paths['@earendil-works/pi-ai'], undefined);
});

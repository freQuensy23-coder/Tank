import assert from 'node:assert/strict';
import { test } from 'node:test';
import { requiredNativeCases, verifyAcceptance } from './check-sandbox-acceptance.mjs';
import { completedInputExecutions, settledNativeHistory } from './qualification-execution-state.mjs';
import { bindSharedNativeClient } from './qualification-shared-native.mjs';
import { validateQualificationExecutor } from './qualification-executor.mjs';

test('native executor requires the exact hosted owner branch dispatch', () => {
  const env = { GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'github-hosted', GITHUB_EVENT_NAME: 'workflow_dispatch', GITHUB_REPOSITORY: 'freQuensy23-coder/Tank', GITHUB_ACTOR: 'freQuensy23-coder', GITHUB_SHA: 'a'.repeat(40), GITHUB_REF: 'refs/heads/qualification' };
  assert.equal(validateQualificationExecutor(env), 'github-hosted');
  for (const changes of [{ GITHUB_EVENT_NAME: 'pull_request' }, { GITHUB_ACTOR: 'another-user' }, { GITHUB_REPOSITORY: 'fork/Tank' }, { RUNNER_ENVIRONMENT: 'self-hosted' }, { GITHUB_REF: 'refs/tags/untrusted' }, { GITHUB_SHA: 'invalid' }]) assert.throws(() => validateQualificationExecutor({ ...env, ...changes }));
});

test('native restart waits for exact durable inputs rather than public idle events', () => {
  const row = { id: 'input-a', session_id: 'session-a', state: 'running', settled_at_ms: null, failure: null };
  assert.equal(completedInputExecutions([row], ['input-a'], 'session-a'), false);
  assert.equal(completedInputExecutions([], ['input-a'], 'session-a'), false);
  assert.equal(completedInputExecutions([{ ...row, id: 'unrelated', state: 'completed', settled_at_ms: 123 }], ['input-a'], 'session-a'), false);
  const completed = { ...row, state: 'completed', settled_at_ms: 123 };
  assert.equal(completedInputExecutions([completed], ['input-a'], 'session-a'), true);
  assert.equal(completedInputExecutions([completed, { ...row, id: 'input-b', state: 'queued' }], ['input-a', 'input-b'], 'session-a'), false);
  for (const rows of [
    [{ ...completed, session_id: 'another-session' }],
    [{ ...completed, settled_at_ms: null }],
    [{ ...completed, failure: 'unexpected failure' }],
    [completed, completed],
    [{ ...row, state: 'failed' }],
    [{ ...row, state: 'cancelled' }],
  ]) assert.throws(() => completedInputExecutions(rows, ['input-a'], 'session-a'));
  assert.throws(() => completedInputExecutions([completed], [], 'session-a'));
  assert.throws(() => completedInputExecutions([completed], ['input-a', 'input-a'], 'session-a'));
});

test('native receipts are read after exact input settlement, not before it', async () => {
  const calls = [];
  let settled = false;
  const completed = () => { calls.push('settlement'); return settled; };
  const events = async () => { calls.push('events'); return ['A', 'B-first', 'B-second']; };
  const validate = rows => { calls.push('receipts'); return rows.length === 3; };
  assert.equal(await settledNativeHistory({ completed, events, validate }), null);
  assert.deepEqual(calls, ['settlement']);
  calls.length = 0;
  settled = true;
  assert.deepEqual(await settledNativeHistory({ completed, events, validate }), ['A', 'B-first', 'B-second']);
  assert.deepEqual(calls, ['settlement', 'events', 'receipts']);
  await assert.rejects(settledNativeHistory({ completed, events: async () => ['A'], validate }), error => error.fatal === true);
});

test('shared runtime composition shadows the SDK inherited read-only factory without opening another home', () => {
  let openings = 0;
  const native = {};
  class NativeSdk {}
  Object.defineProperty(NativeSdk, 'withDefaultConfig', { value() { openings++; return native; } });
  class Sdk extends NativeSdk {}
  assert.throws(() => { Sdk.withDefaultConfig = () => native; }, TypeError);
  const shared = bindSharedNativeClient(Sdk);
  assert.equal(shared.native, native);
  assert.equal(Sdk.withDefaultConfig(), native);
  assert.equal(Sdk.withDefaultConfig(), native);
  assert.equal(shared.bindings, 2);
  assert.equal(openings, 1);
  assert.equal(Object.getOwnPropertyDescriptor(NativeSdk, 'withDefaultConfig').writable, false);
});

const expected = { sourceSha: 'a'.repeat(40), runId: '123' };
const fixture = () => ({ schema: 'tank.native-managed-upgrade.v2', scope: 'candidate-native-upgrade', historicalImageQualification: 'not-exercised', ...expected, cases: requiredNativeCases.map(name => ({ case: name, status: 'PASS', receipt: name + '.json', receiptSha256: 'c'.repeat(64) })), unproved: [], fullyQualified: true });
test('qualification gate accepts only the complete exact-head native inventory', () => {
  assert.doesNotThrow(() => verifyAcceptance(fixture(), expected));
  for (const alter of [
    row => { row.schema = 'tank.native-managed-upgrade.v1'; },
    row => { delete row.scope; },
    row => { row.scope = 'historical-image-upgrade'; },
    row => { delete row.historicalImageQualification; },
    row => { row.historicalImageQualification = 'PASS'; },
    row => { row.sourceSha = 'b'.repeat(40); },
    row => { row.runId = '124'; },
    row => { row.cases.pop(); },
    row => { row.cases.push(row.cases[0]); },
    row => { row.cases.push({ ...row.cases[0], case: 'legacy-native-publication', receipt: 'legacy-native-publication.json' }); },
    row => { row.cases[0].status = 'SKIP'; },
    row => { row.cases[0].status = 'FAIL'; },
    row => { row.cases[0].status = 'NOT_RUN'; },
    row => { row.cases[0].receipt = '../wrong.json'; },
    row => { delete row.cases[0].receiptSha256; },
    row => { row.fullyQualified = false; },
    row => { row.unproved.push('lease-loss'); },
  ]) {
    const row = fixture(); alter(row);
    assert.throws(() => verifyAcceptance(row, expected));
  }
});

test('candidate qualification requires every upgrade and durability case independently', () => {
  const names = ['managed-api-A-to-B-1', 'managed-api-A-to-B-2', 'writable-memory-restart-A-to-B', 'metadata-only-native-rebuild', 'interrupted-native-transfer', 'interrupted-native-publication', 'lease-loss-concurrent-native-resume', 'native-archive-publication-race', 'security-mismatch-native-refusal'];
  assert.deepEqual([...requiredNativeCases].sort(), names.sort());
  for (const name of names) {
    const missing = fixture();
    missing.cases = missing.cases.filter(row => row.case !== name);
    assert.throws(() => verifyAcceptance(missing, expected), /missing or unexpected native evidence cases/);
    for (const status of ['FAIL', 'SKIP', 'NOT_RUN']) {
      const failed = fixture();
      failed.cases.find(row => row.case === name).status = status;
      assert.throws(() => verifyAcceptance(failed, expected), /native case failed or skipped/);
    }
  }
});

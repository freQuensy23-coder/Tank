import assert from 'node:assert/strict';
import { test } from 'node:test';
import { requiredNativeCases, verifyAcceptance } from './check-sandbox-acceptance.mjs';

const expected = { sourceSha: 'a'.repeat(40), runId: '123' };
const fixture = () => ({ schema: 'tank.native-managed-upgrade.v1', ...expected, cases: requiredNativeCases.map(name => ({ case: name, status: 'PASS' })), unproved: [], fullyQualified: true });
test('qualification gate accepts only the complete exact-head native inventory', () => {
  assert.doesNotThrow(() => verifyAcceptance(fixture(), expected));
  for (const alter of [
    row => { row.sourceSha = 'b'.repeat(40); },
    row => { row.runId = '124'; },
    row => { row.cases.pop(); },
    row => { row.cases.push(row.cases[0]); },
    row => { row.cases[0].status = 'SKIP'; },
    row => { row.cases[0].status = 'FAIL'; },
    row => { row.fullyQualified = false; },
    row => { row.unproved.push('lease-loss'); },
  ]) {
    const row = fixture(); alter(row);
    assert.throws(() => verifyAcceptance(row, expected));
  }
});

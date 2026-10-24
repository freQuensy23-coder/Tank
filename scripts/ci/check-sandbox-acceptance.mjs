import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';

export const requiredNativeCases = Object.freeze(['managed-api-A-to-B-1', 'managed-api-A-to-B-2', 'writable-memory-restart-A-to-B', 'legacy-native-publication', 'metadata-only-native-rebuild', 'interrupted-native-transfer', 'interrupted-native-publication', 'lease-loss-concurrent-native-resume', 'native-archive-publication-race', 'security-mismatch-native-refusal']);
export function verifyAcceptance(summary, expected) {
  assert.equal(summary.schema, 'tank.native-managed-upgrade.v1');
  assert.equal(summary.sourceSha, expected.sourceSha, 'evidence belongs to a different source head');
  assert.equal(summary.runId, expected.runId, 'evidence belongs to a different workflow run');
  assert.ok(Array.isArray(summary.cases));
  const names = summary.cases.map(row => row.case);
  assert.equal(new Set(names).size, names.length, 'duplicate native evidence case');
  assert.deepEqual([...names].sort(), [...requiredNativeCases].sort(), 'missing or unexpected native evidence cases');
  assert.ok(summary.cases.every(row => row.status === 'PASS'), 'native case failed or skipped');
  for (const row of summary.cases) {
    assert.equal(row.receipt, row.case + '.json', 'native receipt path does not match its exact case');
    assert.match(row.receiptSha256, /^[a-f0-9]{64}$/, 'missing immutable native receipt digest');
  }
  assert.deepEqual(summary.unproved, [], 'native obligations remain unproved');
  assert.equal(summary.fullyQualified, true, 'partial qualification is not acceptance');
}
async function main() {
  const directory = resolve(process.env.TANK_QUALIFICATION_EVIDENCE);
  const path = join(directory, 'native-summary.json');
  const stat = await lstat(path);
  assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size > 0 && stat.size < 1024 * 1024, 'invalid native summary file');
  const summary = JSON.parse(await readFile(path, 'utf8'));
  verifyAcceptance(summary, { sourceSha: process.env.GITHUB_SHA, runId: process.env.GITHUB_RUN_ID });
  for (const row of summary.cases) {
    const path = join(directory, row.receipt);
    const stat = await lstat(path);
    assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size > 0 && stat.size < 16 * 1024 ** 2, 'invalid case receipt file');
    const bytes = await readFile(path);
    assert.equal(createHash('sha256').update(bytes).digest('hex'), row.receiptSha256, 'native case receipt changed after summary');
    const receipt = JSON.parse(bytes);
    assert.equal(receipt.sourceSha, summary.sourceSha);
    assert.equal(receipt.runId, summary.runId);
    assert.equal(receipt.case, row.case);
    assert.equal(receipt.status, 'PASS');
  }
  console.log(JSON.stringify({ sourceSha: summary.sourceSha, runId: summary.runId, cases: summary.cases.length, status: 'PASS' }));
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();

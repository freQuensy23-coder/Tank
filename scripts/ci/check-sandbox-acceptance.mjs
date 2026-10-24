import assert from 'node:assert/strict';
import { lstat, readFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';

export const requiredNativeCases = Object.freeze(['managed-api-A-to-B-1', 'managed-api-A-to-B-2', 'legacy-native-publication', 'metadata-only-native-rebuild', 'interrupted-transfer-publication', 'lease-loss-concurrent-resume-archive', 'security-mismatch-native-refusal']);
export function verifyAcceptance(summary, expected) {
  assert.equal(summary.schema, 'tank.native-managed-upgrade.v1');
  assert.equal(summary.sourceSha, expected.sourceSha, 'evidence belongs to a different source head');
  assert.equal(summary.runId, expected.runId, 'evidence belongs to a different workflow run');
  assert.ok(Array.isArray(summary.cases));
  const names = summary.cases.map(row => row.case);
  assert.equal(new Set(names).size, names.length, 'duplicate native evidence case');
  assert.deepEqual([...names].sort(), [...requiredNativeCases].sort(), 'missing or unexpected native evidence cases');
  assert.ok(summary.cases.every(row => row.status === 'PASS'), 'native case failed or skipped');
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
  console.log(JSON.stringify({ sourceSha: summary.sourceSha, runId: summary.runId, cases: summary.cases.length, status: 'PASS' }));
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();

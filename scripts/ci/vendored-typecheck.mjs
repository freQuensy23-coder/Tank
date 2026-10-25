import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';

assert.equal(process.env.GITHUB_ACTIONS, 'true');
const root = resolve('vendor/openma');
const evidence = process.env.TANK_QUALIFICATION_EVIDENCE;
assert.ok(evidence);
await mkdir(evidence, { recursive: true });
const packages = ['control-plane-node', 'session-runtime-contract', 'session-runtime-sql', 'managed-runtime-boxlite', 'sandbox-adapter-litebox'];
const observations = [];
for (const name of packages) {
  const child = spawnSync('corepack', ['pnpm', '--filter', '@open-managed-agents/' + name, 'typecheck'], { cwd: root, encoding: 'utf8', timeout: 120000, maxBuffer: 16 * 1024 ** 2 });
  const log = (child.stdout ?? '') + (child.stderr ?? '');
  await writeFile(join(evidence, name + '-typecheck.txt'), log);
  console.log(log);
  observations.push({ package: name, exitCode: child.status, signal: child.signal, error: child.error?.message ?? null });
}
await writeFile(join(evidence, 'vendored-typecheck.json'), JSON.stringify({ schema: 'tank.vendored-typecheck.v1', sourceSha: process.env.GITHUB_SHA, runId: process.env.GITHUB_RUN_ID, observations }, null, 2) + '\n');
assert.ok(observations.every(row => row.exitCode === 0), 'Vendored composition typecheck failed; see package diagnostics');

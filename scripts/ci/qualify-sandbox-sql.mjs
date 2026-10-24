import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

assert.equal(process.env.GITHUB_ACTIONS, 'true');
const directory = process.env.TANK_QUALIFICATION_EVIDENCE;
assert.ok(directory);
await mkdir(directory, { recursive: true });
const observations = [];
const password = randomBytes(32).toString('hex');
const owner = 'tank-sql-' + process.env.GITHUB_RUN_ID + '-' + process.env.GITHUB_RUN_ATTEMPT;
function docker(args, options = {}) {
  const result = spawnSync('docker', args, { encoding: 'utf8', timeout: 120000, ...options });
  assert.equal(result.status, 0, String(result.stderr ?? '').replaceAll(password, '[TEST_SECRET]'));
  return result.stdout.trim();
}
async function suite(dialect, url) {
  const result = spawnSync(process.execPath, ['--import', './vendor/openma/apps/main-node/node_modules/tsx/dist/loader.mjs', '--test', '--test-reporter=junit', 'scripts/ci/sandbox-sql-qualification.test.mjs'], { encoding: 'utf8', timeout: 120000, maxBuffer: 8 * 1024 ** 2, env: { ...process.env, TANK_QUALIFICATION_PG_URL: url ?? '' } });
  const xml = String(result.stdout ?? '').replaceAll(password, '[TEST_SECRET]');
  await writeFile(join(directory, 'sql-' + dialect + '.xml'), xml);
  if (result.stderr) console.error(result.stderr.replaceAll(password, '[TEST_SECRET]'));
  const names = [...xml.matchAll(/<testcase\b[^>]*\bname="([^"]+)"/g)].map(match => match[1]);
  const rejected = /<(?:failure|error|skipped)\b/.test(xml);
  const valid = result.status === 0 && names.length === 7 && new Set(names).size === 7 && !rejected;
  observations.push({ dialect, exitCode: result.status, cases: names, valid, rejected, signal: result.signal });
  if (!valid) process.exitCode = 1;
  console.log(JSON.stringify(observations.at(-1)));
}
try {
  await suite('sqlite');
  docker(['run', '-d', '--name', owner, '--label', 'tank.qualification=' + owner, '--env', 'POSTGRES_PASSWORD', '--env', 'POSTGRES_DB=tank_test_qualification', '--publish', '127.0.0.1::5432', '--mount', 'type=tmpfs,destination=/var/lib/postgresql/data', 'postgres:16-bookworm'], { env: { ...process.env, POSTGRES_PASSWORD: password } });
  const deadline = performance.now() + 60000;
  let ready = false;
  while (performance.now() < deadline) {
    const check = spawnSync('docker', ['exec', owner, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'tank_test_qualification'], { encoding: 'utf8', timeout: 5000 });
    if (check.status === 0) { ready = true; break; }
    await new Promise(done => setTimeout(done, 250));
  }
  assert.ok(ready, 'fresh disposable PostgreSQL did not become ready');
  const port = docker(['port', owner, '5432/tcp']).split(':').at(-1);
  await suite('postgres', `postgresql://postgres:${password}@127.0.0.1:${port}/tank_test_qualification`);
} finally {
  const inspect = spawnSync('docker', ['inspect', '--format', '{{index .Config.Labels "tank.qualification"}}', owner], { encoding: 'utf8', timeout: 10000 });
  if (inspect.status === 0) {
    assert.equal(inspect.stdout.trim(), owner, 'refusing cleanup of an unowned SQL fixture');
    docker(['rm', '-f', '-v', owner]);
    const absent = spawnSync('docker', ['inspect', owner], { stdio: 'ignore', timeout: 10000 });
    assert.notEqual(absent.status, 0, 'owned SQL fixture survived cleanup');
  }
  await writeFile(join(directory, 'sql-qualification.json'), JSON.stringify({ schema: 'tank.sql-upgrade-qualification.v1', sourceSha: process.env.GITHUB_SHA, runId: process.env.GITHUB_RUN_ID, observations }, null, 2) + '\n');
}
assert.equal(observations.length, 2, 'both actual SQL dialects must execute');
assert.ok(observations.every(row => row.valid), 'actual SQL qualification failed; inspect JUnit');

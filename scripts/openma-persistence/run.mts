import { spawnSync } from 'node:child_process';
import { statSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { verifyTestReport } from '../typescript/test-report.mts';
import { requireDisposablePostgresDsn } from './database.mts';
import { DEFAULT_LOCK_PATHS, verifyPersistenceLocks } from './verify-lock.mts';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
if (process.versions.node !== '24.22.0' || process.argv.length !== 2) throw new Error('Persistence qualification requires Node24.22.0 and no selection arguments');
verifyPersistenceLocks(DEFAULT_LOCK_PATHS);
requireDisposablePostgresDsn(process.env.TANK_TEST_DATABASE_URL, process.env.TANK_OPENMA_PERSISTENCE_DATABASE_HOST);
const files = [
  'scripts/openma-persistence/verify-lock.test.mts',
  'tests/openma/persistence/continuation-input.test.mts',
  'tests/openma/persistence/runtime-json.test.mts',
  'tests/openma/persistence/continuation-sql.test.mts',
  'tests/openma/lifecycle/image-recovery.test.mjs',
];
for (const file of files) if (!statSync(resolve(root, file)).isFile()) throw new Error('Missing declared persistence test file');
const typecheck = spawnSync(process.execPath, ['scripts/typescript/node_modules/typescript/bin/tsc', '--project', 'scripts/openma-persistence/tsconfig.json'], { cwd: root, stdio: 'inherit', timeout: 60000 });
if (typecheck.error !== undefined || typecheck.status !== 0) throw new Error('Persistence strict type checking failed');
const tests = spawnSync(process.execPath, [
  '--experimental-transform-types', '--import', './scripts/openma-persistence/register.mts',
  '--import', './tests/openma/persistence/register-source.mts', '--test', '--test-reporter=tap', '--test-timeout=10000', ...files,
], { cwd: root, encoding: 'utf8', timeout: 60000 });
if (tests.error !== undefined || tests.status === null) throw new Error('Persistence test process did not start or finish');
process.stdout.write(tests.stdout);
process.stderr.write(tests.stderr);
if (tests.status !== 0) throw new Error('Persistence tests failed');
const summary = verifyTestReport(tests.stdout);
console.log(JSON.stringify({ schema: 'tank.persistence.qualification.v1', ...summary }));

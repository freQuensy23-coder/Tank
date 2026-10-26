import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { decodeRuntimeJson } from '../../vendor/openma/packages/session-runtime-contract/src/json.ts';
import { DEFAULT_LOCK_PATHS, verifyPersistenceLocks } from './verify-lock.mts';
import { WorkspaceExports } from './workspace-exports.mts';

const repository = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
export const HISTORY_MODULE = 'vendor/openma/packages/managed-agents-adapters-sql/test/session-event-source-position.test.ts';
const CONFIG = 'vendor/openma/packages/managed-agents-adapters-sql/vitest.config.ts';
const MODULE_SHA256 = 'b1480462425a147b125c302773e4b79f45fda67c8bf3e1b157ed8ae32c1a5488';
const CONFIG_SHA256 = '25ec2e1f7465adaa4f1d62ca7f43dfdd071f57f1bd08110128d40f538c89c2a1';
function invalidReport(): never { throw new Error('History module qualification is incomplete or inconsistent'); }
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function integer(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) return invalidReport();
  return value;
}
export interface HistoryModuleSummary {
  schema: 'tank.persistence.history-module.v1';
  cases: number;
  passed: number;
  failed: 0;
  skipped: 0;
  todo: 0;
  max_ms: number;
  p90_ms: number;
}
export function verifyHistoryModuleReport(document: string, expectedModule: string): HistoryModuleSummary {
  const report = decodeRuntimeJson(document);
  if (!object(report) || report.success !== true) return invalidReport();
  const total = integer(report.numTotalTests), passed = integer(report.numPassedTests);
  if (total !== 5 || passed !== total || integer(report.numFailedTests) !== 0 || integer(report.numPendingTests) !== 0) return invalidReport();
  if (!Array.isArray(report.testResults) || report.testResults.length !== 1) return invalidReport();
  const file: unknown = report.testResults[0];
  if (!object(file) || file.status !== 'passed' || typeof file.name !== 'string' || !isAbsolute(file.name) || resolve(file.name) !== resolve(expectedModule)) return invalidReport();
  if (!Array.isArray(file.assertionResults) || file.assertionResults.length !== total) return invalidReport();
  const names = new Set<string>(), durations: number[] = [];
  for (const value of file.assertionResults) {
    if (!object(value) || value.status !== 'passed' || typeof value.fullName !== 'string' || value.fullName.length === 0 || names.has(value.fullName)) return invalidReport();
    if (typeof value.duration !== 'number' || !Number.isFinite(value.duration) || value.duration < 0 || value.duration >= 10000) return invalidReport();
    names.add(value.fullName);
    durations.push(value.duration);
  }
  durations.sort((left, right) => left - right);
  const p90 = durations.at(Math.ceil(durations.length * .9) - 1);
  if (p90 === undefined || p90 >= 1000) return invalidReport();
  return {schema: 'tank.persistence.history-module.v1', cases: total, passed, failed: 0, skipped: 0, todo: 0, max_ms: Math.max(...durations), p90_ms: p90};
}
function checkedBytes(path: string, expected: string): void {
  if (createHash('sha256').update(readFileSync(path)).digest('hex') !== expected) throw new Error('Protected history qualification source differs from declared input');
}
function dependencyManifest(name: string, version: string): string {
  const path = resolve(repository, 'vendor/openma/node_modules', name, 'package.json');
  const value = decodeRuntimeJson(readFileSync(path, 'utf8'));
  if (!object(value) || value.name !== name || value.version !== version) throw new Error('History module dependency does not match the frozen root');
  return path;
}
export function runHistoryModule(): void {
  if (process.versions.node !== '24.22.0' || process.argv.length !== 2) throw new Error('History qualification requires Node24.22.0 without selection arguments');
  verifyPersistenceLocks(DEFAULT_LOCK_PATHS);
  checkedBytes(resolve(repository, HISTORY_MODULE), MODULE_SHA256);
  checkedBytes(resolve(repository, CONFIG), CONFIG_SHA256);
  const manifest = dependencyManifest('vitest', '4.1.4');
  dependencyManifest('better-sqlite3', '13.0.3');
  dependencyManifest('@earendil-works/pi-ai', '1.1.0');
  dependencyManifest('@openma/common', '0.9.1');
  const value = decodeRuntimeJson(readFileSync(manifest, 'utf8'));
  if (!object(value)) throw new Error('Vitest public binary is not declared');
  const entry = typeof value.bin === 'string' ? value.bin : object(value.bin) && typeof value.bin.vitest === 'string' ? value.bin.vitest : undefined;
  if (entry === undefined) throw new Error('Vitest public binary is not declared');
  const binary = resolve(dirname(manifest), entry);
  if (relative(dirname(manifest), binary).startsWith('..') || !statSync(binary).isFile()) throw new Error('Vitest public binary is outside its declared package');
  const temporary = mkdtempSync(resolve(tmpdir(), 'tank-history-module-'));
  try {
    const files = [
      'scripts/openma-persistence/history-module.mts', 'scripts/openma-persistence/history-module-config.ts',
      'scripts/openma-persistence/workspace-exports.mts',
      'vendor/openma/packages/session-runtime-contract/src/continuation.ts',
      'vendor/openma/packages/session-runtime-sql/src/continuation.ts',
      'vendor/openma/packages/session-runtime-sql/src/coordination.ts',
      'vendor/openma/packages/managed-agents-adapters-sql/src/session-runtime-projection-sql-persistence.ts',
    ];
    const config = resolve(temporary, 'strict.json');
    writeFileSync(config, JSON.stringify({extends: resolve(repository, 'scripts/openma-persistence/tsconfig.json'), compilerOptions: {paths: new WorkspaceExports(repository).typeScriptPaths()}, files: files.map(file => resolve(repository, file)), include: []}));
    const typecheck = spawnSync(process.execPath, [resolve(repository, 'scripts/typescript/node_modules/typescript/bin/tsc'), '--project', config], {cwd: repository, stdio: 'inherit', timeout: 60000});
    if (typecheck.error !== undefined || typecheck.status !== 0) throw new Error('History module strict qualification failed');
    const report = resolve(temporary, 'vitest-report.json');
    const result = spawnSync(process.execPath, [binary, 'run', '--config', resolve(repository, 'scripts/openma-persistence/history-module-config.ts'), '--testTimeout', '10000', '--reporter', 'json', '--outputFile', report, 'test/session-event-source-position.test.ts'], {
      cwd: resolve(repository, 'vendor/openma/packages/managed-agents-adapters-sql'), encoding: 'utf8', timeout: 60000,
      env: {...process.env, TANK_OPENMA_HISTORY_CACHE_DIR: resolve(temporary, 'cache')},
    });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    const document = readFileSync(report, 'utf8');
    process.stdout.write(document + '\n');
    if (result.error !== undefined || result.status !== 0) throw new Error('History module execution did not pass');
    console.log(JSON.stringify(verifyHistoryModuleReport(document, resolve(repository, HISTORY_MODULE))));
  } finally { rmSync(temporary, {recursive: true}); }
}
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) runHistoryModule();

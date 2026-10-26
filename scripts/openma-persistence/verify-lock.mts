import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { IMPORTERS, LOCK_SHA256, MANIFEST_SHA256, PACKAGES } from './manifest.mts';

export interface PersistenceLockPaths {
  manifest: URL;
  lock: URL;
  upstream: URL;
}
export const DEFAULT_LOCK_PATHS: PersistenceLockPaths = {
  manifest: new URL('./package.json', import.meta.url),
  lock: new URL('./package-lock.json', import.meta.url),
  upstream: new URL('../../vendor/openma/pnpm-lock.yaml', import.meta.url),
};
function requiredBlock(document: string, heading: string, indentation: number): string {
  const lines = document.split('\n');
  const prefix = ' '.repeat(indentation);
  const starts = lines.flatMap((line, index): number[] => line === prefix + heading + ':' ? [index] : []);
  if (starts.length !== 1) throw new Error('Missing or ambiguous persistence dependency lock section');
  const start = starts[0];
  if (start === undefined) throw new Error('Missing persistence dependency lock section');
  let end = start + 1;
  while (end < lines.length) {
    const line = lines[end];
    if (line === undefined) throw new Error('Missing persistence dependency lock line');
    if (line.length > 0 && !line.startsWith(' '.repeat(indentation + 1))) break;
    end++;
  }
  return lines.slice(start + 1, end).join('\n');
}
function requireLine(document: string, line: string): void {
  if (document.split('\n').filter(value => value === line).length !== 1) throw new Error('Persistence dependency lock does not match the declared closure');
}
function exactBytes(path: URL, expected: string): void {
  const actual = createHash('sha256').update(readFileSync(path)).digest('hex');
  if (actual !== expected) throw new Error('Persistence frozen dependency input bytes changed');
}
export function verifyPersistenceLocks(paths: PersistenceLockPaths): void {
  exactBytes(paths.manifest, MANIFEST_SHA256);
  exactBytes(paths.lock, LOCK_SHA256);
  const upstream = readFileSync(paths.upstream, 'utf8');
  const packages = requiredBlock(upstream, 'packages', 0);
  for (const pkg of PACKAGES) {
    const block = requiredBlock(packages, pkg.heading, 2);
    requireLine(block, '    resolution: {integrity: ' + pkg.integrity + '}');
  }
  const importers = requiredBlock(upstream, 'importers', 0);
  for (const importer of IMPORTERS) {
    const source = requiredBlock(importers, importer.name, 2);
    const dependencies = requiredBlock(source, importer.section, 4);
    const entry = requiredBlock(dependencies, importer.dependency, 6);
    requireLine(entry, '        specifier: ' + importer.specifier);
    requireLine(entry, '        version: ' + importer.version);
  }
  const snapshots = requiredBlock(upstream, 'snapshots', 0);
  const effect = requiredBlock(requiredBlock(snapshots, 'effect@3.22.1', 2), 'dependencies', 4);
  requireLine(effect, "      '@standard-schema/spec': 1.1.0");
  requireLine(effect, '      fast-check: 3.23.2');
  const random = requiredBlock(requiredBlock(snapshots, 'fast-check@3.23.2', 2), 'dependencies', 4);
  requireLine(random, '      pure-rand: 6.1.0');
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  verifyPersistenceLocks(DEFAULT_LOCK_PATHS);
  console.log('Verified the frozen five-package persistence closure against the upstream lock');
}

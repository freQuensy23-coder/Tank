import { globSync, readFileSync, realpathSync, statSync } from 'node:fs';
import { dirname, isAbsolute, relative, resolve } from 'node:path';
import { decodeRuntimeJson } from '../../vendor/openma/packages/session-runtime-contract/src/json.ts';

interface WorkspacePackage {
  readonly root: string;
  readonly exports: unknown;
}
function invalidExport(): never { throw new Error('Workspace public export is missing, ambiguous or unsupported'); }
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function conditionalTarget(value: unknown, conditions: readonly string[]): string {
  if (typeof value === 'string') return value;
  if (!object(value)) return invalidExport();
  for (const [condition, target] of Object.entries(value)) {
    if (conditions.includes(condition)) return conditionalTarget(target, conditions);
  }
  return invalidExport();
}
function runtimeTarget(value: unknown): string {
  return conditionalTarget(value, ['import', 'node', 'default']);
}
function declaredTarget(exports: unknown, subpath: string): string {
  if (typeof exports === 'string' || object(exports) && !Object.keys(exports).some(key => key.startsWith('.'))) {
    if (subpath !== '.') return invalidExport();
    return runtimeTarget(exports);
  }
  if (!object(exports)) return invalidExport();
  if (Object.hasOwn(exports, subpath)) return runtimeTarget(exports[subpath]);
  const matches: { key: string; prefix: string; suffix: string }[] = [];
  for (const key of Object.keys(exports)) {
    const star = key.indexOf('*');
    if (star < 0) continue;
    if (key.indexOf('*', star + 1) >= 0) return invalidExport();
    const prefix = key.slice(0, star), suffix = key.slice(star + 1);
    if (subpath.startsWith(prefix) && subpath.endsWith(suffix) && subpath.length >= prefix.length + suffix.length) matches.push({key, prefix, suffix});
  }
  matches.sort((left, right) => right.prefix.length - left.prefix.length || right.key.length - left.key.length);
  const match = matches[0];
  if (match === undefined) return invalidExport();
  const captured = subpath.slice(match.prefix.length, subpath.length - match.suffix.length);
  return runtimeTarget(exports[match.key]).replaceAll('*', captured);
}
export class WorkspaceExports {
  readonly #packages = new Map<string, WorkspacePackage>();

  constructor(repository: string) {
    const packages = resolve(repository, 'vendor/openma/packages');
    for (const manifest of globSync('*/package.json', {cwd: packages})) {
      const path = resolve(packages, manifest);
      const value = decodeRuntimeJson(readFileSync(path, 'utf8'));
      if (!object(value) || typeof value.name !== 'string' || value.name.length === 0 || this.#packages.has(value.name)) return invalidExport();
      this.#packages.set(value.name, {root: realpathSync(dirname(path)), exports: value.exports});
    }
  }

  typeScriptPaths(): Record<string, string[]> {
    const paths: Record<string, string[]> = {};
    for (const [name, entry] of this.#packages) {
      if (entry.exports === undefined) continue;
      const declarations = typeof entry.exports === 'string' || object(entry.exports) && !Object.keys(entry.exports).some(key => key.startsWith('.'))
        ? {'.': entry.exports} : entry.exports;
      if (!object(declarations)) return invalidExport();
      for (const [subpath, declaration] of Object.entries(declarations)) {
        if (subpath !== '.' && !subpath.startsWith('./')) return invalidExport();
        const target = conditionalTarget(declaration, ['types', 'import', 'node', 'default']);
        if (!target.startsWith('./') || (subpath.match(/\*/g)?.length ?? 0) > 1 || (target.match(/\*/g)?.length ?? 0) > 1) return invalidExport();
        const candidate = resolve(entry.root, target);
        const within = relative(entry.root, candidate);
        if (within === '..' || within.startsWith('../') || isAbsolute(within)) return invalidExport();
        paths[name + (subpath === '.' ? '' : subpath.slice(1))] = [candidate];
      }
    }
    return paths;
  }

  resolve(specifier: string): string | null {
    const parts = specifier.split('/');
    const count = specifier.startsWith('@') ? 2 : 1;
    if (parts.length < count) return null;
    const name = parts.slice(0, count).join('/');
    const entry = this.#packages.get(name);
    if (entry === undefined) return null;
    for (const segment of parts.slice(count)) {
      let decoded: string;
      try { decoded = decodeURIComponent(segment); } catch { return invalidExport(); }
      if (decoded === '.' || decoded === '..' || decoded.toLowerCase() === 'node_modules' || decoded.includes('/') || decoded.includes('\\')) return invalidExport();
    }
    const subpath = parts.length === count ? '.' : './' + parts.slice(count).join('/');
    const target = declaredTarget(entry.exports, subpath);
    if (!target.startsWith('./')) return invalidExport();
    const candidate = resolve(entry.root, target);
    const lexical = relative(entry.root, candidate);
    if (lexical === '..' || lexical.startsWith('../') || isAbsolute(lexical)) return invalidExport();
    try {
      const path = realpathSync(candidate);
      const within = relative(entry.root, path);
      if (within === '..' || within.startsWith('../') || isAbsolute(within) || !statSync(path).isFile()) return invalidExport();
      return path;
    } catch { return invalidExport(); }
  }
}

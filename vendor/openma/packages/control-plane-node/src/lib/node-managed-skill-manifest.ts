import { posix } from 'node:path';

export function skillArchiveManifest(files: Record<string, Uint8Array>): {
  path: string; directory: string; body: string;
} {
  const manifests: Array<{path: string; bytes: Uint8Array}> = [];
  const normalizedFiles = new Set<string>();
  for (const [rawPath, bytes] of Object.entries(files)) {
    const normalized = posix.normalize(rawPath.replaceAll('\\', '/'));
    if (rawPath.includes('\0') || normalized === '.' || normalized.startsWith('/')
        || normalized === '..' || normalized.startsWith('../') || /^[A-Za-z]:\//u.test(normalized)) {
      throw new Error('Unsafe skill archive path');
    }
    if (rawPath.endsWith('/')) continue;
    if (normalizedFiles.has(normalized)) throw new Error('Duplicate normalized skill archive path');
    normalizedFiles.add(normalized);
    if (posix.basename(normalized) === 'SKILL.md') manifests.push({path: normalized, bytes});
  }
  if (manifests.length !== 1) throw new Error('Skill archive requires exactly one SKILL.md');
  const manifest = manifests[0]!;
  const directory = posix.dirname(manifest.path);
  return {path: manifest.path, directory: directory === '.' ? '' : `${directory}/`,
    body: new TextDecoder('utf-8', {fatal: true}).decode(manifest.bytes)};
}

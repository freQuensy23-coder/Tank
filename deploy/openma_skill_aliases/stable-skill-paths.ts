import { createHash, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { posix } from 'node:path';
import { unzipSync, zipSync } from 'fflate';
import type { SandboxExecutor } from '@open-managed-agents/sandbox';
import { skillArchiveManifest } from './node-managed-skill-manifest.js';

export interface StableSkillPlan {
  binding: { name: string; skill_id: string; version: string; archive_sha256: string };
  canonicalRoot: string;
  files: Array<{ canonicalPath: string; aliasPath: string; bytes: Uint8Array }>;
}

function manifestName(body: string): string | undefined {
  const lines = body.replaceAll('\r\n', '\n').split('\n');
  if (lines[0] !== '---') return undefined;
  const end = lines.indexOf('---', 1);
  if (end < 0) return undefined;
  let name: string | undefined;
  for (const line of lines.slice(1, end)) {
    const match = /^name:\s*(.*)$/u.exec(line);
    if (!match) continue;
    let value = match[1]!.trim();
    if (value.startsWith('"') && value.endsWith('"')) {
      try { value = JSON.parse(value); } catch { throw new Error('Invalid skill manifest name'); }
    } else if (value.startsWith("'") && value.endsWith("'")) value = value.slice(1, -1).replaceAll("''", "'");
    name = value;
  }
  return name;
}

export function stableSkillPlan(input: {skillId: string; version: string; requestedVersion: string; name: string; archive: Uint8Array}): StableSkillPlan {
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/u.test(input.name) || input.name.length > 64
      || !input.skillId || input.skillId.length > 191 || !input.version || input.version.length > 191
      || !input.requestedVersion || input.requestedVersion.length > 191)
    throw new Error('Invalid stable skill binding');
  if (input.archive.byteLength > 128 * 1024 * 1024) throw new Error('Skill archive exceeds compiler bounds');
  const archive = unzipSync(input.archive), manifest = skillArchiveManifest(archive);
  if (manifestName(manifest.body) !== input.name) throw new Error('Skill metadata and manifest name differ');
  const aliasRoot = `/home/user/.skills/${input.name}/`;
  const canonicalRoot = `/workspace/.openma/skills/${encodeURIComponent(input.skillId)}/${encodeURIComponent(input.requestedVersion)}/`;
  const files: StableSkillPlan['files'] = [];
  let total = 0, count = 0;
  for (const [rawPath, bytes] of Object.entries(archive)) {
    if (rawPath.endsWith('/')) continue;
    const path = posix.normalize(rawPath.replaceAll('\\', '/'));
    total += bytes.byteLength; count++;
    if (count > 500 || total > 100 * 1024 * 1024 || bytes.byteLength > 32 * 1024 * 1024 || path.length > 4096)
      throw new Error('Skill files exceed compiler or guest path bounds');
    if (manifest.directory && !path.startsWith(manifest.directory)) throw new Error('Skill file is outside manifest directory');
    const relative = path.slice(manifest.directory.length);
    files.push({canonicalPath: canonicalRoot + path, aliasPath: aliasRoot + relative, bytes});
  }
  return {binding: {name: input.name, skill_id: input.skillId, version: input.version,
    archive_sha256: createHash('sha256').update(input.archive).digest('hex')}, canonicalRoot, files};
}

export async function materializeStableSkills(sandbox: SandboxExecutor, plans: StableSkillPlan[], assertSucceeded: (command: string, output: string) => void): Promise<void> {
  if (!plans.length) return;
  const names = new Set<string>();
  for (const plan of plans) {
    if (names.has(plan.binding.name)) throw new Error('Duplicate stable skill name');
    names.add(plan.binding.name);
  }
  const worker = await readFile(new URL('./node-managed-skill-aliases.py', import.meta.url), 'utf8');
  const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
  // One bounded receipt request at a time; no session-wide attachment count cap.
  const claims: StableSkillPlan['binding'][][] = [];
  let chunk: StableSkillPlan['binding'][] = [];
  for (const {binding} of plans) {
    if (Buffer.byteLength(JSON.stringify(binding)) > 2048) throw new Error('Skill binding exceeds receipt bounds');
    if (Buffer.byteLength(JSON.stringify([...chunk, binding])) > 48 * 1024) { claims.push(chunk); chunk = []; }
    chunk.push(binding);
  }
  if (chunk.length) claims.push(chunk);
  // Check every batch before claiming any. Both commands revalidate existing
  // receipts; an EEXIST race must match exactly before any file is copied.
  for (const operation of ['check', 'claim']) for (const bindings of claims) {
    const payload = Buffer.from(JSON.stringify(bindings)).toString('base64');
    const command = `python3 -c ${quote(worker)} ${operation} ${quote(payload)}`;
    const output = await sandbox.exec(command, 30_000);
    assertSucceeded(command, output);
    if (!/(?:^|\n)TANK_STABLE_SKILL_BINDINGS_OK\s*$/u.test(output)) throw new Error('Stable skill binding rejected');
  }
  // Keep the existing binary transfer and execution fence. Staging one bounded
  // ZIP avoids a mkdir + Python process for every canonical/alias file copy.
  const contentLimit = 48 * 1024 * 1024, fileLimit = 500;
  let entries: Record<string, Uint8Array> = {}, size = 0, count = 0;
  const scopes = new Map<string, { binding: StableSkillPlan['binding']; canonical_root: string }>();
  const flush = async () => {
    if (!count) return;
    const manifest = Buffer.from(JSON.stringify({ version: 1, scopes: [...scopes.values()] }));
    if (manifest.length > 2 * 1024 * 1024) throw new Error('Skill batch manifest exceeds bounds');
    entries['__tank_skill_batch__.json'] = manifest;
    const archive = zipSync(entries, { level: 0, mtime: new Date(1980, 0, 1) });
    const digest = createHash('sha256').update(archive).digest('hex');
    // The ordinary binary write API follows symlinks. First claim an
    // unpredictable, exclusive guest directory instead of a public digest path.
    const target = `/tmp/.openma-skills-${randomBytes(16).toString('hex')}/${digest}.zip`;
    const command = (operation: string) => `python3 -c ${quote(worker)} ${operation} ${quote(target)} ${quote(digest)}`;
    let staged = false;
    try {
      const prepare = command('stage'), prepared = await sandbox.exec(prepare, 30_000);
      assertSucceeded(prepare, prepared);
      if (!/(?:^|\n)TANK_STABLE_SKILL_STAGE_OK\s*$/u.test(prepared)) throw new Error('Stable skill staging rejected');
      staged = true;
      await sandbox.writeFileBytes!(target, archive);
      const materialize = command('materialize'), output = await sandbox.exec(materialize, 120_000);
      assertSucceeded(materialize, output);
      if (!/(?:^|\n)TANK_STABLE_SKILL_FILES_OK\s*$/u.test(output)) throw new Error('Stable skill materialization rejected');
    } catch (error) {
      // A poisoned/lost native fence may refuse cleanup. Preserve that failure;
      // no later session or broad temporary-directory sweep adopts this path.
      if (staged) await sandbox.exec(command('discard'), 30_000).catch(() => undefined);
      throw error;
    }
    entries = {}; size = 0; count = 0; scopes.clear();
  };
  for (const plan of plans) for (const file of plan.files) for (const target of [file.canonicalPath, file.aliasPath]) {
    if (count && (count >= fileLimit || size + file.bytes.byteLength > contentLimit)) await flush();
    const path = target.slice(1);
    if (!target.startsWith('/') || Object.hasOwn(entries, path)) throw new Error('Invalid or duplicate skill target');
    entries[path] = file.bytes; size += file.bytes.byteLength; count++;
    scopes.set(plan.canonicalRoot, { binding: plan.binding, canonical_root: plan.canonicalRoot });
  }
  await flush();
}

import { createHash } from 'node:crypto';
import { realpath } from 'node:fs/promises';
import { join, relative, resolve } from 'node:path';
import { readNativeLegacyConfiguration, type NativeLegacyConfiguration } from './legacy-native-provenance.js';
import { stableDigest, validateRuntimeIdentity } from './runtime-identity.js';
import type { Candidate, Handle, Scope } from './retained-session.js';

type MemoryResource = { type: string; memoryStoreId?: string; name?: string | null; access?: 'read_only' | 'read_write' | null };
type MemoryBindingInput = {
  scope: Scope;
  candidate: Candidate | null;
  resources: readonly MemoryResource[];
  proposed: string;
  memoryRoot: string | null;
  inspect(id: string): Promise<{ id: string; name?: string } | null>;
  getText(key: string): Promise<string | null>;
  readConfiguration?(id: string): Promise<NativeLegacyConfiguration | null>;
};
export async function retainedMemoryGeneration(input: MemoryBindingInput): Promise<string> {
  try { return await resolveMemoryGeneration(input); }
  catch (error) {
    if (error instanceof Error && ['retained_session_archived', 'retained_configuration_changed:resources'].includes(error.message)) throw error;
    throw Error('retained_memory_binding_unproven');
  }
}
async function resolveMemoryGeneration(input: MemoryBindingInput): Promise<string> {
  const writable = input.resources.filter(resource => resource.type === 'memory_store' && resource.access !== 'read_only');
  if (!writable.length || !input.candidate) return input.proposed;
  const fail = (): never => { throw Error('retained_memory_binding_unproven'); };
  const candidate = input.candidate, metadata = candidate.metadata;
  if (!metadata || metadata.scope !== JSON.stringify(input.scope)) return fail();
  let handle: Handle;
  if (metadata.schema === 2) {
    if (typeof metadata.manifest !== 'string' || candidate.contentHash !== stableDigest({ scope: input.scope, manifest: metadata.manifest })) return fail();
    const manifest = JSON.parse(metadata.manifest);
    validateRuntimeIdentity(manifest.identity);
    if (manifest.archived || manifest.state === 'archived') throw Error('retained_session_archived');
    if (manifest.identity.compatibility.resources !== stableDigest(input.resources)) throw Error('retained_configuration_changed:resources');
    if (!['retained', 'upgrading', 'allocating', 'aborted'].includes(manifest.state)) return fail();
    if (manifest.state === 'aborted') return input.proposed;
    handle = manifest.handle;
  } else if (metadata.schema === 1) {
    const identity = { scope: input.scope, provider: metadata.provider, runtimeId: metadata.runtimeId,
      fingerprint: metadata.fingerprint, state: metadata.state };
    if (candidate.contentHash !== 'sha256:' + createHash('sha256').update(JSON.stringify(identity)).digest('hex')) return fail();
    if (metadata.state === 'archived') throw Error('retained_session_archived');
    if (metadata.state === 'aborted') return input.proposed;
    if (!['retained', 'allocating'].includes(String(metadata.state))) return fail();
    handle = { provider: String(metadata.provider), runtimeId: String(metadata.runtimeId) };
  } else return fail();
  if (!handle || handle.provider !== 'litebox' || !handle.runtimeId || !input.memoryRoot) return fail();
  const baseName = 'oma-' + createHash('sha256').update(`litebox:${input.scope.sessionId}`).digest('hex').slice(0, 32);
  const upgradePrefix = 'oma-' + createHash('sha256').update(JSON.stringify(input.scope)).digest('hex').slice(0, 32) + '-upgrade-';
  const name = handle.ownershipName ?? baseName;
  if (name !== baseName && (!name.startsWith(upgradePrefix) || !/^[a-f0-9]{32}$/.test(name.slice(upgradePrefix.length)))) return fail();
  const info = await input.inspect(handle.runtimeId);
  if (!info || info.name !== name || (info.id !== handle.runtimeId && handle.runtimeId !== name)) return fail();
  const record = await (input.readConfiguration ?? readNativeLegacyConfiguration)(info.id);
  if (!record || record.id !== info.id || record.name !== name || !Array.isArray(record.options?.volumes)) return fail();
  const segment = (value: string) => {
    if (!value || value === '.' || value === '..' || value.includes('/')) return fail();
    return encodeURIComponent(value);
  };
  const root = resolve(input.memoryRoot);
  const prefix = join(root, '.openma-managed-memory-workspaces', segment(input.scope.workspaceId), segment(input.scope.sessionId));
  const canonicalRoot = await realpath(root);
  let generation: string | undefined;
  for (const resource of writable) {
    if (!resource.memoryStoreId) return fail();
    const matches = record.options.volumes.filter((volume: { guest_path?: string }) => volume.guest_path === `/mnt/memory/${resource.name ?? resource.memoryStoreId}`);
    if (matches.length !== 1 || matches[0].read_only !== false || typeof matches[0].host_path !== 'string') return fail();
    const host = matches[0].host_path as string;
    const parts = relative(prefix, host).split('/');
    if (parts.length !== 3 || parts[1] !== segment(resource.memoryStoreId) || parts[2] !== 'data') return fail();
    const value = decodeURIComponent(parts[0]!);
    if (segment(value) !== parts[0] || (generation !== undefined && generation !== value)
        || host !== join(prefix, segment(value), segment(resource.memoryStoreId), 'data')) return fail();
    if (await realpath(host) !== join(canonicalRoot, relative(root, host))) return fail();
    const workspace = relative(root, join(host, '..'));
    if (await realpath(join(host, '..', 'manifest.json')) !== join(canonicalRoot, workspace, 'manifest.json')) return fail();
    const raw = await input.getText(`${workspace}/manifest.json`);
    if (raw === null || await input.getText(`${workspace}/data/.openma-memory-store`) !== `openma-memory-store-v1\n${resource.memoryStoreId}`) return fail();
    const manifest = JSON.parse(raw);
    if (manifest.version !== 1 || !Array.isArray(manifest.memories) || manifest.memories.some((entry: { id?: unknown; path?: unknown; sha256?: unknown }) =>
      typeof entry.id !== 'string' || typeof entry.path !== 'string' || !entry.path.startsWith('/') || typeof entry.sha256 !== 'string')) return fail();
    generation = value;
  }
  return generation ?? fail();
}

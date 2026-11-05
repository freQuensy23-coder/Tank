import { createHash, randomUUID } from 'node:crypto';
import { assertCompatible, stableDigest, validateRuntimeIdentity, type RuntimeIdentity } from './runtime-identity.js';
type Manifest = { handle: Handle; state: string; archived: boolean; legacyFingerprint?: string; identity?: RuntimeIdentity; retired?: Handle[]; upgrade?: { source: Handle; target: Handle; owner: string; identity: RuntimeIdentity } };

export type Scope = { workspaceId: string; environmentId: string; sessionId: string; workId: string };
export type Handle = { provider: string; runtimeId: string; ownershipName?: string };
export type Fence = Scope & { ownerId: string; generation: number; token: string; expiresAt: string };
export type Candidate = { id: string; contentHash: string; metadata?: Record<string, string | number | boolean | null> };
export interface Fences {
  acquire(input: { scope: Scope; ownerId: string; ttlMs: number }): Promise<
    { type: 'acquired'; fence: Fence; publication: { workspaceCandidate: Candidate } | null }
    | { type: 'conflict'; expiresAt: string | null }>;
  isCurrent(fence: Fence): Promise<boolean>;
  renew(input: { fence: Fence; ttlMs: number }): Promise<{ type: 'renewed'; fence: Fence } | { type: 'lost' }>;
  publish(input: { fence: Fence; workspaceCandidate: Candidate; outputCandidate: null }): Promise<{ type: 'published'; revision: number } | { type: 'lost' }>;
  release(input: { fence: Fence; reason: 'completed' | 'failed' | 'lease_lost' }): Promise<void>;
}
export interface RetainedRuntime {
  runtimeHandle(): Handle;
  suspend(input: { kind: 'filesystem' }): Promise<unknown>;
}
export interface Provider<R extends RetainedRuntime> {
  allocationIntent(scope: Scope, fence: Fence, signal: AbortSignal): Promise<Handle>;
  resolveIntent(intent: Handle, scope: Scope, fence: Fence, signal: AbortSignal): Promise<Handle | null>;
  create(scope: Scope, fence: Fence, signal: AbortSignal): Promise<R>;
  resume(handle: Handle, scope: Scope, fence: Fence, signal: AbortSignal): Promise<R>;
  reap(handle: Handle, scope: Scope, fence: Fence, signal: AbortSignal, assertAuthorized?: () => Promise<void>): Promise<void>;
}
export interface Options<R extends RetainedRuntime> {
  scope: Omit<Scope, 'workId'>;
  fingerprint: string;
  identity?: RuntimeIdentity;
  providerId: string;
  ownerId: string;
  leaseTtlMs: number;
  fences: Fences;
  provider: Provider<R>;
  orphans?: { enqueue(input: { scope: Scope; generation: number; ownerId: string;
    sandbox: Handle; reason: 'failed'; error: unknown }): Promise<void> };
}
export class RetainedLifecycleError extends Error {
  constructor(code: string) { super(code); this.name = 'RetainedLifecycleError'; }
}
function fail(code: string): never { throw new RetainedLifecycleError(code); }
const digest = (value: unknown) => 'sha256:' + createHash('sha256').update(JSON.stringify(value)).digest('hex');

export function decodeSchema2Publication(candidate: Candidate, canonicalScope: Scope, providerId: string): Manifest {
  const metadata = candidate.metadata;
  if (metadata?.schema !== 2) fail('retained_manifest_invalid');
  if (metadata.scope !== JSON.stringify(canonicalScope) || typeof metadata.manifest !== 'string'
      || candidate.contentHash !== stableDigest({ scope: canonicalScope, manifest: metadata.manifest })) fail('retained_manifest_invalid');
  let manifest: Manifest;
  try { manifest = JSON.parse(metadata.manifest); } catch { fail('retained_manifest_invalid'); }
  if (!manifest || typeof manifest !== 'object') fail('retained_manifest_invalid');
  const validHandle = (handle: Handle) => handle && handle.provider === providerId
    && typeof handle.runtimeId === 'string' && handle.runtimeId.length > 0
    && (handle.ownershipName === undefined || (typeof handle.ownershipName === 'string' && handle.ownershipName.length > 0));
  if (!validHandle(manifest.handle) || !['allocating', 'aborted', 'retained', 'upgrading', 'archived'].includes(manifest.state)
      || manifest.archived !== (manifest.state === 'archived') || !Array.isArray(manifest.retired)
      || manifest.retired!.some(handle => !validHandle(handle))) fail('retained_manifest_invalid');
  validateRuntimeIdentity(manifest.identity);
  if (manifest.state === 'upgrading') {
    const upgrade = manifest.upgrade;
    if (!upgrade || !validHandle(upgrade.source) || !validHandle(upgrade.target) || typeof upgrade.owner !== 'string'
        || !upgrade.owner || JSON.stringify(upgrade.source) !== JSON.stringify(manifest.handle)
        || upgrade.target.runtimeId === upgrade.source.runtimeId) fail('retained_manifest_invalid');
    validateRuntimeIdentity(upgrade.identity);
    assertCompatible(manifest.identity!, upgrade.identity);
  } else if (manifest.upgrade) fail('retained_manifest_invalid');
  return manifest;
}

export class RetainedSessionLifecycle<R extends RetainedRuntime> {
  readonly scope: Scope;
  readonly options: Options<R>;
  readonly ownerId: string;
  private fence: Fence | null = null;
  private runtime: R | null = null;
  private handle: Handle | null = null;
  private heartbeat: ReturnType<typeof setInterval> | null = null;
  private heartbeatPending: Promise<void> | null = null;
  private lost = false;
  private phase: 'idle' | 'starting' | 'active' | 'quiescing' | 'stopped' | 'archived' = 'idle';
  private manifest: Manifest | null = null;
  private operations = new Set<Promise<unknown>>();

  constructor(options: Options<R>) {
    if (!/^sha256:[a-f0-9]{64}$/.test(options.fingerprint)) fail('retained_fingerprint_invalid');
    if (!Number.isSafeInteger(options.leaseTtlMs) || options.leaseTtlMs < 300) fail('retained_lease_invalid');
    for (const value of Object.values(options.scope)) if (!value || typeof value !== 'string') fail('retained_scope_invalid');
    this.options = options;
    if (options.identity) validateRuntimeIdentity(options.identity);
    this.ownerId = `${options.ownerId}:${randomUUID()}`;
    this.scope = { workspaceId: options.scope.workspaceId, environmentId: options.scope.environmentId,
      sessionId: options.scope.sessionId, workId: 'tank-retained-session-v1' };
  }
  private async current(): Promise<Fence> {
    const fence = this.fence;
    if (this.lost || !fence) fail('retained_fence_lost');
    if (!await this.options.fences.isCurrent(fence)) { this.lost = true; fail('retained_fence_lost'); }
    return fence;
  }
  private startHeartbeat(): void {
    this.heartbeat = setInterval(() => {
      if (this.heartbeatPending || !this.fence) return;
      this.heartbeatPending = (async () => {
        try {
          const result = await this.options.fences.renew({ fence: this.fence!, ttlMs: this.options.leaseTtlMs });
          if (result.type === 'lost') this.lost = true;
          else this.fence = result.fence;
        } catch { this.lost = true; }
      })().finally(() => { this.heartbeatPending = null; });
    }, Math.floor(this.options.leaseTtlMs / 3));
    this.heartbeat.unref?.();
  }
  private async release(reason: 'completed' | 'failed' | 'lease_lost', renewOnFailure = true): Promise<void> {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
    await this.heartbeatPending;
    const fence = this.fence;
    try {
      if (fence) await this.options.fences.release({ fence, reason });
      this.fence = null;
    } catch (error) {
      if (renewOnFailure && this.fence && !this.lost) this.startHeartbeat();
      throw error;
    }
  }
  private decode(candidate: Candidate, archiveOnly = false): Manifest {
    const metadata = candidate.metadata;
    if (metadata?.schema === 2) {
      const manifest = decodeSchema2Publication(candidate, this.scope, this.options.providerId);
      if (!archiveOnly && manifest.state !== 'archived') {
        if (!this.options.identity) fail('retained_identity_required');
        if (stableDigest(manifest.identity) !== stableDigest(this.options.identity)
            && !['retained', 'aborted'].includes(manifest.state)) fail('retained_configuration_changed');
        if (manifest.state === 'upgrading') fail('retained_upgrade_incomplete');
      }
      return manifest;
    }
    if (!metadata || metadata.schema !== 1 || metadata.scope !== JSON.stringify(this.scope)
        || metadata.provider !== this.options.providerId || typeof metadata.runtimeId !== 'string'
        || !metadata.runtimeId || !['allocating', 'aborted', 'retained', 'archived'].includes(String(metadata.state))) fail('retained_manifest_invalid');
    const identity = { scope: this.scope, provider: metadata.provider, runtimeId: metadata.runtimeId,
      fingerprint: metadata.fingerprint, state: metadata.state };
    if (candidate.contentHash !== digest(identity)) fail('retained_manifest_invalid');
    if (typeof metadata.fingerprint !== 'string' || !/^sha256:[a-f0-9]{64}$/.test(metadata.fingerprint)) fail('retained_manifest_invalid');
    if (!archiveOnly && metadata.state !== 'archived' && metadata.fingerprint !== this.options.fingerprint
        && (!this.options.identity || !['retained', 'aborted'].includes(String(metadata.state)))) fail('retained_configuration_changed');
    return { handle: { provider: String(metadata.provider), runtimeId: metadata.runtimeId as string }, state: String(metadata.state), archived: metadata.state === 'archived', legacyFingerprint: metadata.fingerprint };
  }
  private async publish(state: 'allocating' | 'aborted' | 'retained' | 'archived'): Promise<void> {
    if (this.manifest?.identity) {
      const manifest = { ...this.manifest, handle: this.handle!, state, archived: state === 'archived',
        ...(state === 'archived' ? { retired: [], upgrade: undefined } : {}) };
      const encoded = JSON.stringify(manifest);
      const result = await this.options.fences.publish({ fence: await this.current(), workspaceCandidate: {
        id: `retained-${manifest.handle.runtimeId}`, contentHash: stableDigest({ scope: this.scope, manifest: encoded }),
        metadata: { schema: 2, scope: JSON.stringify(this.scope), manifest: encoded,
          provider: manifest.handle.provider, runtimeId: manifest.handle.runtimeId, state }
      }, outputCandidate: null });
      if (result.type === 'lost') { this.lost = true; fail('retained_fence_lost'); }
      this.manifest = manifest;
      return;
    }
    const fence = await this.current();
    const handle = this.handle!;
    const identity = { scope: this.scope, provider: handle.provider, runtimeId: handle.runtimeId,
      fingerprint: this.options.fingerprint, state };
    const result = await this.options.fences.publish({ fence,
      workspaceCandidate: { id: `retained-${handle.runtimeId}`, contentHash: digest(identity),
        metadata: { schema: 1, scope: JSON.stringify(this.scope), provider: handle.provider,
          runtimeId: handle.runtimeId, fingerprint: this.options.fingerprint, state } }, outputCandidate: null });
    if (result.type === 'lost') { this.lost = true; fail('retained_fence_lost'); }
    this.manifest = { handle, state, archived: state === 'archived', legacyFingerprint: this.options.fingerprint };
  }
  async begin(assertExecutionCurrent: () => Promise<void>, signal: AbortSignal): Promise<void> {
    if (this.phase !== 'idle') fail('retained_already_active');
    this.phase = 'starting';
    let created = false, newIntent = false;
    try {
      signal.throwIfAborted();
      await assertExecutionCurrent();
      const acquired = await this.options.fences.acquire({ scope: this.scope, ownerId: this.ownerId, ttlMs: this.options.leaseTtlMs });
      if (acquired.type !== 'acquired') fail('retained_resource_busy');
      this.fence = acquired.fence; this.lost = false; this.startHeartbeat();
      let manifest = acquired.publication ? this.decode(acquired.publication.workspaceCandidate) : null;
      this.manifest = manifest;
      if (manifest?.archived) fail('retained_session_archived');
      if (manifest?.state === 'retained' && ((manifest.identity && stableDigest(manifest.identity) !== stableDigest(this.options.identity))
          || (manifest.legacyFingerprint && manifest.legacyFingerprint !== this.options.fingerprint))) {
        this.handle = manifest.handle;
        for (const handle of [...manifest.retired ?? [], manifest.handle]) {
          signal.throwIfAborted(); await assertExecutionCurrent();
          await this.options.provider.reap(handle, this.scope, await this.current(), signal, assertExecutionCurrent);
        }
        signal.throwIfAborted(); await assertExecutionCurrent();
        await this.publish('aborted');
        manifest = this.manifest;
      }
      if (manifest && manifest.state !== 'aborted') {
        this.handle = manifest.handle;
        if (manifest.state === 'allocating') {
          const resolved = await this.options.provider.resolveIntent(this.handle, this.scope, await this.current(), signal);
          if (!resolved) fail('retained_incomplete_allocation_missing');
          this.handle = resolved;
        }
        await assertExecutionCurrent();
        this.runtime = await this.options.provider.resume(this.handle, this.scope, await this.current(), signal);
        if (manifest.state === 'allocating') await this.publish('retained');
      } else {
        this.manifest = null;
        signal.throwIfAborted(); await assertExecutionCurrent();
        this.handle = await this.options.provider.allocationIntent(this.scope, await this.current(), signal);
        if (this.options.identity) this.manifest = { handle: this.handle, state: 'allocating', archived: false, identity: this.options.identity, retired: [] };
        await this.publish('allocating'); newIntent = true;
        this.runtime = await this.options.provider.create(this.scope, await this.current(), signal);
        this.handle = this.runtime.runtimeHandle(); created = true;
        if (this.handle.provider !== this.options.providerId || !this.handle.runtimeId) fail('retained_provider_identity_invalid');
        await this.publish('retained');
        created = false;
      }
      signal.throwIfAborted();
      await assertExecutionCurrent(); await this.current();
      this.phase = 'active';
    } catch (error) {
      let cleanupFailed = false;
      if (newIntent && !this.runtime && this.handle && this.fence && !this.lost) {
        try {
          const remaining = await this.options.provider.resolveIntent(this.handle, this.scope, await this.current(), new AbortController().signal);
          if (!remaining) await this.publish('aborted');
        } catch {   }
      }
      let publicationUnknown = created && this.lost;
      if (created && this.handle && this.fence && !this.lost) {
        try {
          const observed = await this.options.fences.acquire({ scope: this.scope, ownerId: this.ownerId, ttlMs: this.options.leaseTtlMs });
          if (observed.type !== 'acquired') { this.lost = true; publicationUnknown = true; }
          else {
            this.fence = observed.fence;
            if (observed.publication) {
              const manifest = this.decode(observed.publication.workspaceCandidate);
              if (manifest.state === 'retained') {
                if (manifest.handle.provider !== this.handle.provider || manifest.handle.runtimeId !== this.handle.runtimeId) fail('retained_manifest_conflict');
                created = false;
              } else if (manifest.state !== 'allocating') fail('retained_manifest_conflict');
            }
          }
        } catch { publicationUnknown = true; }
      }
      try {
        if (created && !publicationUnknown && this.handle && this.fence && !this.lost) {
          await this.options.provider.reap(this.handle, this.scope, this.fence, new AbortController().signal);
          await this.publish('aborted');
        } else if (this.runtime && this.fence && await this.options.fences.isCurrent(this.fence)) {
          await this.runtime.suspend({ kind: 'filesystem' });
        }
      } catch { cleanupFailed = true; }
      if ((cleanupFailed || publicationUnknown) && created && this.handle && this.fence) {
        cleanupFailed = true;
        await this.options.orphans?.enqueue({ scope: this.scope,
          generation: this.fence.generation, ownerId: this.fence.ownerId, sandbox: this.handle,
          reason: 'failed', error: new RetainedLifecycleError('retained_initial_cleanup_failed') }).catch(() => undefined);
      }
      this.runtime = null; this.handle = null; this.phase = 'idle';
      await this.release(this.lost ? 'lease_lost' : 'failed');
      if (cleanupFailed) fail('retained_initial_cleanup_failed');
      throw error;
    }
  }
  isStopped(): boolean { return this.phase === 'stopped'; }
  async use<T>(operation: (runtime: R) => Promise<T>): Promise<T> {
    if (this.phase !== 'active' || !this.runtime) fail('retained_not_active');
    const runtime = this.runtime;
    const task = (async () => { await this.current(); return operation(runtime); })();
    this.operations.add(task);
    try { return await task; } finally { this.operations.delete(task); }
  }
  async idle(assertExecutionCurrent: () => Promise<void>): Promise<void> {
    if (this.phase === 'idle') return;
    if (this.phase !== 'stopped') {
      if (this.phase !== 'active' || !this.runtime) fail('retained_not_active');
      this.phase = 'quiescing';
      try {
        await Promise.allSettled([...this.operations]);
        await assertExecutionCurrent(); await this.current();
        await this.runtime.suspend({ kind: 'filesystem' });
        this.runtime = null; this.phase = 'stopped';
      } catch (error) {
        this.phase = 'active';
        throw error;
      }
    }
    await assertExecutionCurrent();
    if (this.fence && await this.options.fences.isCurrent(this.fence)) {
      await this.publish('retained');
    }
    await this.release('completed');
    this.handle = null; this.phase = 'idle';
  }

  async abandon(): Promise<void> {
    if ((this.phase === 'idle' || this.phase === 'archived') && !this.fence) return;
    this.phase = 'quiescing';
    try {
      await Promise.allSettled([...this.operations]);
      if (this.runtime && this.fence && await this.options.fences.isCurrent(this.fence)) {
        await this.runtime.suspend({ kind: 'filesystem' });
      }
    } finally {
      this.runtime = null; this.handle = null; this.phase = 'idle';
      await this.release('lease_lost', false);
    }
  }
  async archive(assertArchiveAuthorized: () => Promise<void>, signal: AbortSignal): Promise<void> {
    if (this.phase !== 'idle') fail('retained_archive_while_active');
    this.phase = 'starting';
    try {
      signal.throwIfAborted(); await assertArchiveAuthorized();
      const acquired = await this.options.fences.acquire({ scope: this.scope, ownerId: this.ownerId, ttlMs: this.options.leaseTtlMs });
      if (acquired.type !== 'acquired') fail('retained_resource_busy');
      this.fence = acquired.fence; this.lost = false; this.startHeartbeat();
      if (acquired.publication) {
        const manifest = this.decode(acquired.publication.workspaceCandidate, true);
        this.manifest = manifest;
        this.handle = manifest.handle;
        if (!manifest.archived) {
          await assertArchiveAuthorized();
          if (manifest.state === 'allocating') this.handle = await this.options.provider.resolveIntent(this.handle, this.scope, await this.current(), signal);
          const handles = [...manifest.retired ?? [], ...(manifest.upgrade ? [manifest.upgrade.target] : []),
            ...(this.handle && manifest.state !== 'aborted' ? [this.handle] : [])];
          for (const handle of handles) {
            await assertArchiveAuthorized();
            await this.options.provider.reap(handle, this.scope, await this.current(), signal, assertArchiveAuthorized);
          }
          this.handle ??= manifest.handle;
          signal.throwIfAborted(); await assertArchiveAuthorized();
          await this.publish('archived');
        }
      }
      this.phase = 'archived'; this.handle = null;
      await this.release('completed');
    } catch (error) { this.phase = 'idle'; await this.release(this.lost ? 'lease_lost' : 'failed'); throw error; }
  }
}

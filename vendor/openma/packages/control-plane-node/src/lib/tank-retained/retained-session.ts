import { createHash, randomUUID } from 'node:crypto';

export type Scope = { workspaceId: string; environmentId: string; sessionId: string; workId: string };
export type Handle = { provider: string; runtimeId: string };
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
  reap(handle: Handle, scope: Scope, fence: Fence, signal: AbortSignal): Promise<void>;
}
export interface Options<R extends RetainedRuntime> {
  scope: Omit<Scope, 'workId'>;
  fingerprint: string;
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
  private operations = new Set<Promise<unknown>>();

  constructor(options: Options<R>) {
    if (!/^sha256:[a-f0-9]{64}$/.test(options.fingerprint)) fail('retained_fingerprint_invalid');
    if (!Number.isSafeInteger(options.leaseTtlMs) || options.leaseTtlMs < 300) fail('retained_lease_invalid');
    for (const value of Object.values(options.scope)) if (!value || typeof value !== 'string') fail('retained_scope_invalid');
    this.options = options;
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
  private decode(candidate: Candidate, archiveOnly = false): { handle: Handle; state: string; archived: boolean } {
    const metadata = candidate.metadata;
    if (!metadata || metadata.schema !== 1 || metadata.scope !== JSON.stringify(this.scope)
        || metadata.provider !== this.options.providerId || typeof metadata.runtimeId !== 'string'
        || !metadata.runtimeId || !['allocating', 'aborted', 'retained', 'archived'].includes(String(metadata.state))) fail('retained_manifest_invalid');
    const identity = { scope: this.scope, provider: metadata.provider, runtimeId: metadata.runtimeId,
      fingerprint: metadata.fingerprint, state: metadata.state };
    if (candidate.contentHash !== digest(identity)) fail('retained_manifest_invalid');
    if (!archiveOnly && metadata.state !== 'archived' && metadata.fingerprint !== this.options.fingerprint) fail('retained_configuration_changed');
    return { handle: { provider: String(metadata.provider), runtimeId: metadata.runtimeId as string }, state: String(metadata.state), archived: metadata.state === 'archived' };
  }
  private async publish(state: 'allocating' | 'aborted' | 'retained' | 'archived'): Promise<void> {
    const fence = await this.current();
    const handle = this.handle!;
    const identity = { scope: this.scope, provider: handle.provider, runtimeId: handle.runtimeId,
      fingerprint: this.options.fingerprint, state };
    const result = await this.options.fences.publish({ fence,
      workspaceCandidate: { id: `retained-${handle.runtimeId}`, contentHash: digest(identity),
        metadata: { schema: 1, scope: JSON.stringify(this.scope), provider: handle.provider,
          runtimeId: handle.runtimeId, fingerprint: this.options.fingerprint, state } }, outputCandidate: null });
    if (result.type === 'lost') { this.lost = true; fail('retained_fence_lost'); }
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
      const manifest = acquired.publication ? this.decode(acquired.publication.workspaceCandidate) : null;
      if (manifest?.archived) fail('retained_session_archived');
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
        this.handle = await this.options.provider.allocationIntent(this.scope, await this.current(), signal);
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
        this.handle = manifest.handle;
        if (!manifest.archived) {
          await assertArchiveAuthorized();
          if (manifest.state === 'allocating') this.handle = await this.options.provider.resolveIntent(this.handle, this.scope, await this.current(), signal);
          if (this.handle && manifest.state !== 'aborted') await this.options.provider.reap(this.handle, this.scope, await this.current(), signal);
          this.handle ??= manifest.handle;
          await this.publish('archived');
        }
      }
      this.phase = 'archived'; this.handle = null;
      await this.release('completed');
    } catch (error) { this.phase = 'idle'; await this.release(this.lost ? 'lease_lost' : 'failed'); throw error; }
  }
}

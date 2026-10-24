import { createHash } from 'node:crypto';
import { assertCompatible, stableDigest, type RuntimeIdentity } from './runtime-identity.js';
import { RetainedSessionLifecycle, type Fences, type Options, type Scope } from './retained-session.js';
import { RetainedBoxOperations, pinnedBoxLiteProvider, type Allocation, type BoxLiteRuntime, type PinnedBoxLiteModule } from './boxlite-provider.js';

type Box = RetainedBoxOperations & { stop(): Promise<void> };
export interface AttachableSandbox {
  setRetainedBoxFactory(factory: (allocation: Allocation) => Promise<Box>): void;
}
export interface Context {
  workspaceId: string; sessionId: string;
  session: { id: string; environmentId: string };
  environment: { id: string };
  sandbox: AttachableSandbox;
  executionFence: unknown;
  signal?: AbortSignal;
}
interface Lifetime { final: boolean; binds: number }
interface Entry {
  lifetime: Lifetime; context: Context; controller?: RetainedSessionLifecycle<BoxLiteRuntime>;
  allocation?: Allocation; beginning?: Promise<void>; finalizing?: Promise<void>; started: boolean;
}
export interface NodeRetainedOptions {
  fences: Fences;
  orphans?: Options<BoxLiteRuntime>['orphans'] & {
    list(input: { limit: number }): Promise<readonly { id: string; scope: Scope; sandbox: { provider: string; runtimeId: string } }[]>;
    failed(input: { id: string; error: unknown }): Promise<void>;
    resolve(input: { id: string }): Promise<void>;
  };
  providerModule: PinnedBoxLiteModule;
  client: unknown;
  ownerId: string;
  leaseTtlMs: number;
  fingerprint(context: Context, allocation: Allocation): Promise<string>;
  identity?(context: Context, allocation: Allocation): Promise<RuntimeIdentity>;
  assertExecutionCurrent(fence: unknown): Promise<void>;
}
const key = (scope: Omit<Scope, 'workId'>) => JSON.stringify([scope.workspaceId, scope.environmentId, scope.sessionId]);
const scopeOf = (context: Pick<Context, 'workspaceId' | 'sessionId' | 'session' | 'environment'>) => {
  if (context.session.id !== context.sessionId || context.session.environmentId !== context.environment.id) throw Error('retained_scope_mismatch');
  return { workspaceId: context.workspaceId, environmentId: context.environment.id, sessionId: context.sessionId };
};
export class NodeRetainedLifecycle {
  readonly options: NodeRetainedOptions;
  private entries = new Map<string, Entry>();
  private sandboxes = new WeakMap<object, Entry>();
  private archives = new Map<string, Promise<void>>();
  private lifetimes = new Map<string, Lifetime>();
  constructor(options: NodeRetainedOptions) { this.options = options; }
  async reconcileOrphans(): Promise<void> {
    const queue = this.options.orphans;
    if (!queue) return;
    for (const orphan of await queue.list({ limit: 128 })) {
      if (orphan.scope.workId !== 'tank-retained-session-v1' || orphan.sandbox.provider !== 'litebox') continue;
      const acquired = await this.options.fences.acquire({ scope: orphan.scope,
        ownerId: `${this.options.ownerId}:orphan:${orphan.id}`, ttlMs: this.options.leaseTtlMs });
      if (acquired.type !== 'acquired') continue;
      try {
        const candidate = acquired.publication?.workspaceCandidate;
        const published = candidate?.metadata;
        const protectedHandles: Array<{ provider: string; runtimeId: string }> = [];
        if (published?.schema === 2) {
          if (published.scope !== JSON.stringify(orphan.scope) || typeof published.manifest !== 'string'
              || candidate!.contentHash !== stableDigest({ scope: orphan.scope, manifest: published.manifest })) throw Error('retained_manifest_invalid');
          const manifest = JSON.parse(published.manifest);
          protectedHandles.push(manifest.handle, ...manifest.retired ?? [], ...(manifest.upgrade ? [manifest.upgrade.target] : []));
          for (const handle of [...protectedHandles]) {
            if (!handle || handle.provider !== 'litebox' || typeof handle.runtimeId !== 'string') throw Error('retained_manifest_invalid');
            const info = await (this.options.client as { getInfo(id: string): Promise<{ id: string } | null> }).getInfo(handle.runtimeId);
            if (info) protectedHandles.push({ provider: 'litebox', runtimeId: info.id });
          }
        }
        if (!await this.options.fences.isCurrent(acquired.fence)) throw Error('retained_fence_lost');
        const provider = pinnedBoxLiteProvider({ module: this.options.providerModule, client: this.options.client,
          allocation: { image: 'cleanup-only-no-allocation' }, leaseTtlMs: this.options.leaseTtlMs,
          isCurrent: fence => this.options.fences.isCurrent(fence) });
        const intent = published?.state === 'allocating' && typeof published.runtimeId === 'string'
          ? await provider.resolveIntent({ provider: 'litebox', runtimeId: published.runtimeId }, orphan.scope, acquired.fence, new AbortController().signal) : null;
        if (published?.runtimeId !== orphan.sandbox.runtimeId && intent?.runtimeId !== orphan.sandbox.runtimeId
            && !protectedHandles.some(handle => handle.runtimeId === orphan.sandbox.runtimeId)) {
          await provider.reap(orphan.sandbox, orphan.scope, acquired.fence, new AbortController().signal);
        }
        await queue.resolve({ id: orphan.id });
      } catch {
        await queue.failed({ id: orphan.id, error: new Error('retained_orphan_cleanup_failed') });
      } finally {
        await this.options.fences.release({ fence: acquired.fence, reason: 'completed' });
      }
    }
  }
  private controller(scope: Omit<Scope, 'workId'>, fingerprint: string, allocation: Allocation, identity?: RuntimeIdentity): RetainedSessionLifecycle<BoxLiteRuntime> {
    return new RetainedSessionLifecycle({ scope, fingerprint, identity, providerId: 'litebox', ownerId: this.options.ownerId,
      leaseTtlMs: this.options.leaseTtlMs, fences: this.options.fences,
      orphans: this.options.orphans,
      provider: pinnedBoxLiteProvider({ module: this.options.providerModule, client: this.options.client,
        allocation, leaseTtlMs: this.options.leaseTtlMs, isCurrent: fence => this.options.fences.isCurrent(fence),
        unpublishedCleanupFailed: async (sandbox, scope, fence) => {
          await this.options.orphans?.enqueue({ scope, generation: fence.generation, ownerId: fence.ownerId,
            sandbox, reason: 'failed', error: new Error('retained_initial_cleanup_failed') });
        } }) });
  }
  private assertOpen(lifetime: Lifetime): void {
    if (lifetime.final) throw Error('retained_session_archiving');
  }
  private releaseLifetime(scopeKey: string, lifetime: Lifetime): void {
    if (!lifetime.binds && !this.entries.has(scopeKey) && !this.archives.has(scopeKey)
      && this.lifetimes.get(scopeKey) === lifetime) this.lifetimes.delete(scopeKey);
  }
  async bind(context: Context): Promise<void> {
    const scope = scopeOf(context), scopeKey = key(scope);
    const lifetime = this.lifetimes.get(scopeKey) ?? { final: false, binds: 0 };
    this.lifetimes.set(scopeKey, lifetime);
    lifetime.binds++;
    try { await this.bindOpen(context, scope, lifetime); }
    finally { lifetime.binds--; this.releaseLifetime(scopeKey, lifetime); }
  }
  private async bindOpen(context: Context, scope: Omit<Scope, 'workId'>, lifetime: Lifetime): Promise<void> {
    this.assertOpen(lifetime);
    if (!context.executionFence) throw Error('retained_execution_fence_required');
    await this.options.assertExecutionCurrent(context.executionFence);
    this.assertOpen(lifetime);
    let entry = this.sandboxes.get(context.sandbox);
    if (entry) {
      this.assertOpen(entry.lifetime);
      if (key(scopeOf(entry.context)) !== key(scope)) throw Error('retained_sandbox_scope_changed');
      if (entry.beginning) throw Error('retained_previous_turn_not_quiescent');
      if (entry.started && entry.controller?.isStopped()) {
        await this.idleEntry(entry, () => this.options.assertExecutionCurrent(context.executionFence));
        this.assertOpen(lifetime);
      }
      if (entry.started) throw Error('retained_previous_turn_not_quiescent');
      if (entry.allocation) {
        const fingerprint = await this.options.fingerprint(context, entry.allocation);
        const identity = await this.options.identity?.(context, entry.allocation);
        this.assertOpen(lifetime);
        if (entry.controller?.options.identity && identity) {
          assertCompatible(entry.controller.options.identity, identity);
          if (stableDigest(entry.controller.options.identity) !== stableDigest(identity)) entry.controller = this.controller(scope, fingerprint, entry.allocation, identity);
        } else if (entry.controller && fingerprint !== entry.controller.options.fingerprint) throw Error('retained_configuration_changed');
      }
      entry.context = context;
      return;
    }
    entry = { lifetime, context, started: false };
    this.entries.set(key(scope), entry);
    this.sandboxes.set(context.sandbox, entry);
    const owned = entry;
    context.sandbox.setRetainedBoxFactory(async allocation => {
      this.assertOpen(lifetime);
      owned.allocation = allocation;
      const fingerprint = await this.options.fingerprint(owned.context, allocation);
      const identity = await this.options.identity?.(owned.context, allocation);
      this.assertOpen(lifetime);
      owned.controller = this.controller(scope, fingerprint, allocation, identity);
      const use = async <T>(operation: (runtime: BoxLiteRuntime) => Promise<T>): Promise<T> => {
        await this.ensureActive(owned);
        return owned.controller!.use(operation);
      };
      await this.ensureActive(owned);
      const operations = new RetainedBoxOperations(use);
      return Object.assign(operations, { stop: () => this.idleEntry(owned) });
    });
  }
  private async ensureActive(entry: Entry): Promise<void> {
    this.assertOpen(entry.lifetime);
    if (entry.started) return;
    entry.beginning ??= entry.controller!.begin(
      () => this.options.assertExecutionCurrent(entry.context.executionFence),
      entry.context.signal ?? new AbortController().signal,
    ).then(() => { entry.started = true; }).finally(() => { entry.beginning = undefined; });
    await entry.beginning;
  }
  private async idleEntry(entry: Entry, authorized?: () => Promise<void>): Promise<void> {
    await entry.beginning;
    if (entry.finalizing) return entry.finalizing;
    if (!entry.started) return;
    const finalizing: Promise<void> = entry.controller!.idle(authorized ?? (() => this.options.assertExecutionCurrent(entry.context.executionFence)))
      .then(() => { entry.started = false; })
      .finally(() => { if (entry.finalizing === finalizing) entry.finalizing = undefined; });
    entry.finalizing = finalizing;
    await finalizing;
  }
  async idle(context: Pick<Context, 'workspaceId' | 'sessionId' | 'session' | 'environment'>): Promise<void> {
    const entry = this.entries.get(key(scopeOf(context)));
    if (entry) await this.idleEntry(entry);
  }
  async abandon(context: Pick<Context, 'workspaceId' | 'sessionId' | 'session' | 'environment'>): Promise<void> {
    const entry = this.entries.get(key(scopeOf(context)));
    if (!entry) return;
    await entry.beginning;
    try { await entry.controller?.abandon(); } finally { entry.started = false; }
  }
  async archive(input: { workspaceId: string; sessionId: string; session: { id: string; environmentId: string }; reason: string }): Promise<void> {
    if (!['archived', 'deleted'].includes(input.reason)) throw Error('retained_final_stop_reason_required');
    const scope = { workspaceId: input.workspaceId, environmentId: input.session.environmentId, sessionId: input.sessionId };
    if (input.session.id !== scope.sessionId) throw Error('retained_scope_mismatch');
    const scopeKey = key(scope);
    const existing = this.archives.get(scopeKey);
    if (existing) return existing;
    const lifetime = this.lifetimes.get(scopeKey) ?? { final: false, binds: 0 };
    lifetime.final = true;
    this.lifetimes.set(scopeKey, lifetime);
    const entry = this.entries.get(scopeKey);
    const authorized = async () => { if (!['archived', 'deleted'].includes(input.reason)) throw Error('retained_final_stop_reason_required'); };
    const expectedCancellation = (error: unknown) => error instanceof Error && error.name === 'AbortError'
      && entry?.context.signal?.aborted && entry.context.signal.reason === error
      && [`session_${input.reason}`, 'session_stopped'].includes(error.message);
    const archive: Promise<void> = (async () => {
      let startupFailed = false, startupError: unknown;
      try { await entry?.beginning; } catch (error) { startupFailed = true; startupError = error; }
      if (entry) {
        try { await this.idleEntry(entry, authorized); }
        catch (error) {
          if (!expectedCancellation(error)) throw error;
          await this.idleEntry(entry, authorized);
        }
      }
      const fingerprint = 'sha256:' + createHash('sha256').update('archive-only').digest('hex');
      const controller = entry?.controller ?? this.controller(scope, fingerprint, { image: 'archive-only-no-allocation' });
      await controller.archive(authorized, new AbortController().signal);
      this.entries.delete(scopeKey);
      if (startupFailed && !expectedCancellation(startupError)) throw startupError;
    })().finally(() => {
      if (this.archives.get(scopeKey) === archive) this.archives.delete(scopeKey);
      this.releaseLifetime(scopeKey, lifetime);
    });
    this.archives.set(scopeKey, archive);
    await archive;
  }
  upgradeNotice(context: Pick<Context, 'workspaceId' | 'sessionId' | 'session' | 'environment'>): string | null {
    return this.entries.get(key(scopeOf(context)))?.controller?.upgradeNotice() ?? null;
  }
  async shutdown(): Promise<void> {
    for (const entry of this.entries.values()) await this.idleEntry(entry, async () => {});
  }
}

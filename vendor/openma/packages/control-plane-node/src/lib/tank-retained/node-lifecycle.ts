import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import type { SessionExecutionFence } from '@open-managed-agents/session-runtime-contract/coordination';
import type { RuntimeIdentity } from './runtime-identity.js';
import { RetainedSessionLifecycle, type Fences, type Options, type Scope } from './retained-session.js';
import { RetainedBoxOperations, pinnedBoxLiteProvider, type Allocation, type BoxLiteRuntime, type PinnedBoxLiteModule } from './boxlite-provider.js';
import { RetainedExecutionFenceLost, RetainedIdleUnconfirmed } from './idle-errors.js';

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
type RetainedScopeContext = Pick<Context, 'workspaceId' | 'sessionId' | 'session' | 'environment'>;
const sameExecutionFence = (left: unknown, right: SessionExecutionFence): boolean => {
  const fence = left as Partial<SessionExecutionFence> | null | undefined;
  return fence?.workspaceId === right.workspaceId && fence.sessionId === right.sessionId
    && fence.executionId === right.executionId && fence.attemptId === right.attemptId
    && fence.ownerId === right.ownerId && fence.generation === right.generation;
};
const transitions = new Map<string, Promise<void>>();
const residents = new Map<string, Entry>();
async function serialize<T>(scopeKey: string, operation: () => Promise<T>): Promise<T> {
  const task = (transitions.get(scopeKey) ?? Promise.resolve()).then(operation);
  const tail = task.then(() => {}, () => {});
  transitions.set(scopeKey, tail);
  try { return await task; }
  finally { if (transitions.get(scopeKey) === tail) transitions.delete(scopeKey); }
}
interface Entry {
  lifetime: Lifetime; context: Context; controller?: RetainedSessionLifecycle<BoxLiteRuntime>;
  allocation?: Allocation; started: boolean; open: boolean;
  idleFailure?: RetainedIdleUnconfirmed;
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
  nativeHome?: string;
  idleTimeoutMs?: number;
  identity?(context: Context, allocation: Allocation): Promise<RuntimeIdentity>;
  fingerprint(context: Context, allocation: Allocation): Promise<string>;
  assertExecutionCurrent(fence: unknown): Promise<void>;
}
export const runtimeReplacedReminder = 'The sandbox of this session was replaced with a fresh one after a sandbox environment update. '
  + 'Files under /workspace and elsewhere in the sandbox, installed packages, running processes and the browser profile '
  + 'from earlier turns are gone; files in /mnt/session/outputs and memory stores under /mnt/memory are kept, and '
  + 'this conversation and its history are intact. Recreate anything from the old sandbox that you still need.';
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
      await serialize(key(orphan.scope), async () => {
        if (residents.has(key(orphan.scope))) return;
        const acquired = await this.options.fences.acquire({ scope: orphan.scope,
          ownerId: `${this.options.ownerId}:orphan:${orphan.id}`, ttlMs: this.options.leaseTtlMs });
        if (acquired.type !== 'acquired') return;
        try {
          const published = acquired.publication?.workspaceCandidate.metadata;
          if (published?.schema === 2) throw Error('retained_schema2_orphan_cleanup_requires_archive');
          if (!await this.options.fences.isCurrent(acquired.fence)) throw Error('retained_fence_lost');
          const provider = pinnedBoxLiteProvider({ module: this.options.providerModule, client: this.options.client, nativeHome: this.options.nativeHome,
            allocation: { image: 'cleanup-only-no-allocation' }, leaseTtlMs: this.options.leaseTtlMs,
            isCurrent: fence => this.options.fences.isCurrent(fence) });
          const intent = published?.state === 'allocating' && typeof published.runtimeId === 'string'
            ? await provider.resolveIntent({ provider: 'litebox', runtimeId: published.runtimeId }, orphan.scope, acquired.fence, new AbortController().signal) : null;
          if (published?.runtimeId !== orphan.sandbox.runtimeId && intent?.runtimeId !== orphan.sandbox.runtimeId) {
            await provider.reap(orphan.sandbox, orphan.scope, acquired.fence, new AbortController().signal);
          }
          await queue.resolve({ id: orphan.id });
        } catch {
          await queue.failed({ id: orphan.id, error: new Error('retained_orphan_cleanup_failed') });
        } finally {
          await this.options.fences.release({ fence: acquired.fence, reason: 'completed' });
        }
      });
    }
  }
  private controller(scope: Omit<Scope, 'workId'>, fingerprint: string, allocation: Allocation, identity?: RuntimeIdentity): RetainedSessionLifecycle<BoxLiteRuntime> {
    return new RetainedSessionLifecycle({ scope, fingerprint, identity, providerId: 'litebox', ownerId: this.options.ownerId,
      leaseTtlMs: this.options.leaseTtlMs, fences: this.options.fences,
      orphans: this.options.orphans,
      provider: pinnedBoxLiteProvider({ module: this.options.providerModule, client: this.options.client, nativeHome: this.options.nativeHome,
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
  /** Resolves false when a warm sandbox's retained configuration changed: the caller must destroy it and prepare a new one. */
  async bind(context: Context): Promise<boolean> {
    const scope = scopeOf(context), scopeKey = key(scope);
    const lifetime = this.lifetimes.get(scopeKey) ?? { final: false, binds: 0 };
    this.lifetimes.set(scopeKey, lifetime);
    lifetime.binds++;
    try { return await serialize(scopeKey, () => this.bindOpen(context, scope, lifetime)); }
    finally { lifetime.binds--; this.releaseLifetime(scopeKey, lifetime); }
  }
  private async bindOpen(context: Context, scope: Omit<Scope, 'workId'>, lifetime: Lifetime): Promise<boolean> {
    this.assertOpen(lifetime);
    if (!context.executionFence) throw Error('retained_execution_fence_required');
    await this.options.assertExecutionCurrent(context.executionFence);
    this.assertOpen(lifetime);
    let entry = this.sandboxes.get(context.sandbox);
    const resident = residents.get(key(scope));
    if (resident && resident !== entry) throw Error('retained_previous_turn_not_quiescent');
    if (entry) {
      this.assertOpen(entry.lifetime);
      if (key(scopeOf(entry.context)) !== key(scope)) throw Error('retained_sandbox_scope_changed');
      if (this.entries.get(key(scope)) !== entry) throw Error('retained_sandbox_replaced');
      if (entry.controller?.needsQuiescence()) throw Error('retained_previous_turn_not_quiescent');
      if (entry.started && entry.controller?.isStopped()) {
        await this.idleEntry(entry, () => this.options.assertExecutionCurrent(context.executionFence));
        this.assertOpen(lifetime);
      }
      if (entry.started) throw Error('retained_previous_turn_not_quiescent');
      if (entry.allocation) {
        const fingerprint = await this.options.fingerprint(context, entry.allocation);
        await this.options.assertExecutionCurrent(context.executionFence);
        this.assertOpen(lifetime);
        if (entry.controller && fingerprint !== entry.controller.options.fingerprint) return false;
      }
      entry.context = context;
      entry.open = true;
      return true;
    }
    const previous = this.entries.get(key(scope));
    if (previous?.started || previous?.controller?.needsQuiescence()) throw Error('retained_previous_turn_not_quiescent');
    entry = { lifetime, context, started: false, open: true };
    this.entries.set(key(scope), entry);
    this.sandboxes.set(context.sandbox, entry);
    const owned = entry;
    const scopeKey = key(scope);
    context.sandbox.setRetainedBoxFactory(allocation => {
      const bound = owned.context;
      return serialize(scopeKey, async () => {
        this.assertOpen(lifetime);
        if (this.entries.get(scopeKey) !== owned || owned.context !== bound) throw Error('retained_sandbox_replaced');
        if (!owned.controller) {
          const fingerprint = await this.options.fingerprint(owned.context, allocation);
          const identity = await this.options.identity?.(owned.context, allocation);
          this.assertOpen(lifetime);
          owned.controller = this.controller(scope, fingerprint, allocation, identity);
          owned.allocation = allocation;
        }
        await this.ensureActive(owned);
        const use = async <T>(operation: (runtime: BoxLiteRuntime) => Promise<T>): Promise<T> => {
          const bound = owned.context;
          const { task } = await serialize(scopeKey, async () => {
            if (this.entries.get(scopeKey) !== owned || owned.context !== bound) throw Error('retained_sandbox_replaced');
            await this.ensureActive(owned);
            const task = owned.controller!.use(operation);
            void task.catch(() => undefined);
            return { task };
          });
          return task;
        };
        const operations = new RetainedBoxOperations(use);
        return Object.assign(operations, { stop: () => {
          const bound = owned.context;
          return serialize(scopeKey, async () => {
            if (this.entries.get(scopeKey) === owned && owned.context === bound) await this.idleEntry(owned);
          });
        } });
      });
    });
    return true;
  }
  private async ensureActive(entry: Entry): Promise<void> {
    this.assertOpen(entry.lifetime);
    if (!entry.open) throw Error('retained_turn_closed');
    const scopeKey = key(scopeOf(entry.context));
    const resident = residents.get(scopeKey);
    if ((resident && resident !== entry) || entry.controller?.needsQuiescence()) throw Error('retained_previous_turn_not_quiescent');
    const { executionFence } = entry.context;
    const assertExecution = () => this.options.assertExecutionCurrent(executionFence);
    const signal = entry.context.signal ?? new AbortController().signal;
    signal.throwIfAborted();
    await assertExecution();
    this.assertOpen(entry.lifetime);
    signal.throwIfAborted();
    if (entry.started) return;
    residents.set(scopeKey, entry);
    try {
      await entry.controller!.begin(assertExecution, signal);
      entry.started = true;
    } finally {
      if (!entry.started && !entry.controller!.needsQuiescence()) residents.delete(scopeKey);
    }
  }
  private async idleEntry(entry: Entry, authorized?: () => Promise<void>, signal?: AbortSignal): Promise<void> {
    entry.open = false;
    if (entry.idleFailure) throw entry.idleFailure;
    if (!entry.started) return;
    const timeoutMs = this.options.idleTimeoutMs ?? 35_000;
    const deadline = performance.now() + timeoutMs;
    const timeout = new RetainedIdleUnconfirmed('retained_idle_timeout');
    const elapsed = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const bounded = new Promise<never>((_, reject) => {
      timer = setTimeout(() => { elapsed.abort(timeout); reject(timeout); }, timeoutMs);
    });
    const check = () => {
      if (performance.now() >= deadline) throw timeout;
      if ((signal ?? entry.context.signal)?.reason === 'lease_lost') throw new RetainedExecutionFenceLost();
    };
    const assertCurrent = async () => {
      check();
      await (authorized ?? (() => this.options.assertExecutionCurrent(entry.context.executionFence)))();
      check();
    };
    const retryIdle = async () => {
      for (let attempt = 1; ; attempt++) {
        try {
          await assertCurrent();
          await entry.controller!.idle(assertCurrent);
          return;
        } catch (error) {
          check();
          if (error instanceof RetainedExecutionFenceLost) throw error;
          if (attempt >= 3) throw new RetainedIdleUnconfirmed('retained_idle_unconfirmed', error);
          await delay(attempt * 100, undefined, { signal: elapsed.signal });
        }
      }
    };
    try {
      await Promise.race([retryIdle(), bounded]);
    } catch (error) {
      if (error instanceof RetainedIdleUnconfirmed) entry.idleFailure = error;
      throw error;
    } finally {
      clearTimeout(timer);
    }
    entry.started = false;
    residents.delete(key(scopeOf(entry.context)));
  }
  takeRuntimeReplacedReminder(context: Pick<Context, 'workspaceId' | 'sessionId' | 'environment'>): string | null {
    const entry = this.entries.get(key({ workspaceId: context.workspaceId, environmentId: context.environment.id, sessionId: context.sessionId }));
    return entry?.controller?.takeRuntimeReplaced() ? runtimeReplacedReminder : null;
  }
  async idle(context: RetainedScopeContext & { executionFence?: unknown; signal?: AbortSignal }): Promise<void> {
    const scopeKey = key(scopeOf(context));
    const entry = this.entries.get(scopeKey);
    const bound = entry?.context;
    if (context.executionFence && bound?.executionFence !== context.executionFence) return;
    await serialize(scopeKey, async () => {
      if (entry && this.entries.get(scopeKey) === entry && entry.context === bound) await this.idleEntry(entry, undefined, context.signal);
    });
  }
  async abandon(context: RetainedScopeContext & { executionFence?: unknown }): Promise<void> {
    const scopeKey = key(scopeOf(context));
    const entry = this.entries.get(scopeKey);
    const bound = entry?.context;
    if (context.executionFence && bound?.executionFence !== context.executionFence) return;
    await serialize(scopeKey, async () => {
      if (!entry || this.entries.get(scopeKey) !== entry || entry.context !== bound) return;
      await this.abandonEntry(scopeKey, entry);
    });
  }
  private async abandonEntry(scopeKey: string, entry: Entry): Promise<void> {
    entry.open = false;
    if (entry.idleFailure) throw entry.idleFailure;
    try { await entry.controller?.abandon(); }
    finally {
      entry.started = false;
      if (residents.get(scopeKey) === entry && !entry.controller?.needsQuiescence()) residents.delete(scopeKey);
    }
  }
  async quiesceLostTurn(input: {
    fence: Readonly<SessionExecutionFence>;
    context: Pick<Context, 'session' | 'environment'> | null;
  }): Promise<void> {
    const { fence, context } = input;
    if (context === null) return;
    const scopeKey = key(scopeOf({ workspaceId: fence.workspaceId, sessionId: fence.sessionId,
      session: context.session, environment: context.environment }));
    await serialize(scopeKey, async () => {
      const entry = this.entries.get(scopeKey);
      const resident = residents.get(scopeKey);
      if (resident && resident !== entry) throw Error('retained_lost_turn_changed');
      if (!entry) return;
      if (!sameExecutionFence(entry.context.executionFence, fence)) throw Error('retained_lost_turn_changed');
      await this.abandonEntry(scopeKey, entry);
      await entry.controller?.quiesceLostTurn();
      if (residents.get(scopeKey) === entry) residents.delete(scopeKey);
    });
  }
  async archive(input: { workspaceId: string; sessionId: string; session: { id: string; environmentId: string }; reason: string }): Promise<void> {
    if (!['archived', 'deleted'].includes(input.reason)) throw Error('retained_final_stop_reason_required');
    const scope = { workspaceId: input.workspaceId, environmentId: input.session.environmentId, sessionId: input.sessionId };
    if (input.session.id !== scope.sessionId) throw Error('retained_scope_mismatch');
    const scopeKey = key(scope);
    const existing = this.archives.get(scopeKey);
    if (existing) return existing;
    const lifetime = this.lifetimes.get(scopeKey) ?? { final: false, binds: 0 };
    this.lifetimes.set(scopeKey, lifetime);
    const authorized = async () => { if (!['archived', 'deleted'].includes(input.reason)) throw Error('retained_final_stop_reason_required'); };
    const archive = serialize(scopeKey, async () => {
      const entry = this.entries.get(scopeKey);
      const resident = residents.get(scopeKey);
      if ((resident && resident !== entry) || entry?.controller?.needsQuiescence()) throw Error('retained_previous_turn_not_quiescent');
      lifetime.final = true;
      if (entry) await this.idleEntry(entry, authorized);
      const fingerprint = 'sha256:' + createHash('sha256').update('archive-only').digest('hex');
      const controller = entry?.controller ?? this.controller(scope, fingerprint, { image: 'archive-only-no-allocation' });
      await controller.archive(authorized, new AbortController().signal);
      this.entries.delete(scopeKey);
    }).finally(() => {
      if (this.archives.get(scopeKey) === archive) this.archives.delete(scopeKey);
      this.releaseLifetime(scopeKey, lifetime);
    });
    this.archives.set(scopeKey, archive);
    await archive;
  }
  async shutdown(): Promise<void> {
    for (const [scopeKey, entry] of this.entries) await serialize(scopeKey, async () => {
      if (this.entries.get(scopeKey) === entry) await this.idleEntry(entry);
    });
  }
}

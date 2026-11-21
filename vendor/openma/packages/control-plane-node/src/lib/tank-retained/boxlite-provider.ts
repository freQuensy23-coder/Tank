import { AsyncLocalStorage } from 'node:async_hooks';
import { boxLiteHome, withNativeLock } from './native-lock.js';
import { copyGuestFileIn, copyGuestFileOut, type TrackGuestWork } from './guest-file-io.js';
import { createHash } from 'node:crypto';
import type { Fence, Handle, Provider, RetainedRuntime, Scope, StopControl } from './retained-session.js';

type Process = {
  stdin: WritableStream<Uint8Array>; stdout: ReadableStream<Uint8Array>; stderr: ReadableStream<Uint8Array>;
  exited: Promise<{ code: number | null; signal: string | null }>; kill(signal?: string): Promise<void>;
};
export interface BoxLiteRuntime extends RetainedRuntime {
  readFileBytes(path: string): Promise<Uint8Array>;
  writeFileBytes(path: string, bytes: Uint8Array): Promise<string>;
  spawnDuplexProcess(spec: { command: string; args: string[]; env?: Record<string, string>; cwd?: string }): Promise<Process>;
}
export type Allocation = {
  image: string; rootfsPath?: string; cpus?: number; memoryMib?: number; name?: string;
  security?: { jailerEnabled: boolean; seccompEnabled: boolean };
  volumes?: Array<{ hostPath: string; guestPath: string; readOnly?: boolean }>;
  network?: { mode: 'enabled' | 'disabled'; allowNet?: string[] };
};
interface Acquisition {
  scope: Scope; fence: Fence; signal: AbortSignal;
  plan: { workspaceStrategy: 'retained_runtime'; outputStrategy: null; runtimeCheckpoint: null;
    driver: { type: 'ama_worker'; process: { command: string } } };
  workspace: { bindingId: string; mountPath: '/workspace' }; outputs: null; credentialEgress: null;
  environment: { type: 'base'; identity: string; artifact: { type: 'image'; reference: string } };
}
interface PublicProvider {
  create(context: { sessionId: string; workdir: string }, env: Record<string, string>, acquisition: Acquisition): Promise<BoxLiteRuntime>;
  resume(handle: Handle, context: { sessionId: string; workdir: string }, env: Record<string, string>, acquisition: Acquisition): Promise<BoxLiteRuntime>;
}
export interface PinnedBoxLiteModule {
  createBoxLiteProvider(options: { providerId: 'litebox'; image: string; runtimeName?: (context: { sessionId: string; workdir: string }) => string }): PublicProvider;
  createBoxLiteManagedRuntime(options: { providerId: 'litebox'; image: string; leaseTtlMs: number }): {
    sandbox: { reap(input: { scope: Scope; lease: Handle; reason: 'completed' }): Promise<void> };
  };
}
export interface NativeBoxLiteClient {
  getInfo(id: string): Promise<NativeBoxInfo | null>;
  get(id: string): Promise<NativeBox | null>;
  getOrCreate(options: object, name?: string | null): Promise<{ created: boolean; box: NativeBox }>;
  remove(id: string, force?: boolean | null): Promise<void>;
}
export const retainedBoxName = (scope: Scope): string =>
  'oma-' + createHash('sha256').update(`litebox:${scope.sessionId}`).digest('hex').slice(0, 32);
const upgradePrefix = (scope: Scope) =>
  'oma-' + createHash('sha256').update(JSON.stringify(scope)).digest('hex').slice(0, 32) + '-upgrade-';
export const retainedOwnershipName = (handle: Handle, scope: Scope): string => {
  if (!handle.ownershipName) return retainedBoxName(scope);
  const prefix = upgradePrefix(scope);
  if (!handle.ownershipName.startsWith(prefix) || !/^[a-f0-9]{32}$/.test(handle.ownershipName.slice(prefix.length))) {
    throw Error('retained_cleanup_ownership_mismatch');
  }
  return handle.ownershipName;
};

export type NativeBoxInfo = { id: string; name?: string; state: { status: string; running: boolean } };
export interface NativeBox {
  id: string; name: string | null; info(): NativeBoxInfo | Promise<NativeBoxInfo>;
  start(): Promise<void>; stop(): Promise<void>;
  exec(command: string, args?: string[] | null, env?: Array<[string, string]> | null,
    tty?: boolean | null, user?: string | null, timeoutSecs?: number | null, workingDir?: string | null): Promise<{
      id(): Promise<string>; stdin(): Promise<{ write(data: Uint8Array): Promise<void>; close(): Promise<void> }>;
      stdout(): Promise<{ next(): Promise<string | null> }>; stderr(): Promise<{ next(): Promise<string | null> }>;
      wait(): Promise<{ exitCode: number; errorMessage?: string }>; kill(): Promise<void>; signal?(signal: number): Promise<void>;
    }>;
  copyIn(host: string, guest: string, options?: unknown): Promise<void>;
  copyOut(guest: string, host: string, options?: unknown): Promise<void>;
}
export function publicBoxLiteClient(raw: unknown, assertMutation?: () => Promise<void>) {
  const sdk = raw as NativeBoxLiteClient;
  const adapt = async (box: NativeBox) => {
    let info = await box.info();
    return {
      id: box.id, name: box.name, info: () => info,
      async start() { await assertMutation?.(); await box.start(); info = await box.info(); },
      async stop() { await assertMutation?.(); await box.stop(); info = { ...info, state: { ...info.state, running: false, status: 'Stopped' } }; },
      async exec(...args: Parameters<NativeBox['exec']>) { await assertMutation?.(); return box.exec(...args); },
      copyIn: (...args: Parameters<NativeBox['copyIn']>) => box.copyIn(...args), copyOut: (...args: Parameters<NativeBox['copyOut']>) => box.copyOut(...args),
    };
  };
  return {
    getInfo: (id: string) => sdk.getInfo(id),
    async get(id: string) { const box = await sdk.get(id); return box ? adapt(box) : null; },
    async getOrCreate(options: object, name?: string | null) { await assertMutation?.(); const result = await sdk.getOrCreate(options, name); return { ...result, box: await adapt(result.box) }; },
    async remove(id: string, force?: boolean) { await assertMutation?.(); await sdk.remove(id, force); },
  };
}
type GuestWork = <T>(operation: () => Promise<T>, assertBeforeLaunch: () => void) => Promise<T>;
const guestWork = new WeakMap<BoxLiteRuntime, GuestWork>();
const unsafeRuntimes = new WeakSet<BoxLiteRuntime>();

export function pinnedBoxLiteProvider(input: {
  module: PinnedBoxLiteModule; client: unknown; allocation: Allocation; leaseTtlMs: number;
  nativeHome?: string; stopTimeoutMs?: number;
  isCurrent?(fence: Fence): Promise<boolean>;
  unpublishedCleanupFailed?(handle: Handle, scope: Scope, fence: Fence): Promise<void>;
}): Provider<BoxLiteRuntime> {
  const home = input.nativeHome ?? boxLiteHome();
  const mutation = new AsyncLocalStorage<() => Promise<void>>();
  const client = publicBoxLiteClient(input.client, () => {
    const assertCurrent = mutation.getStore();
    if (!assertCurrent) throw Error('retained_native_mutation_not_guarded');
    return assertCurrent();
  });
  const options = { providerId: 'litebox' as const, client, image: input.allocation.image,
    allocationOptions: () => ({ ...input.allocation }), leaseTtlMs: input.leaseTtlMs, outputStore: null };
  const provider = input.module.createBoxLiteProvider(options);
  const resource = input.module.createBoxLiteManagedRuntime(options);
  const inspect = async (identity: string) => {
    const client = input.client as { getInfo?(id: string): Promise<{ id: string; name: string; state: { running: boolean } } | null> };
    if (typeof client.getInfo !== 'function') throw Error('retained_sdk_inspection_required');
    return await client.getInfo(identity);
  };
  const current = async (fence: Fence) => {
    if (input.isCurrent && !await input.isCurrent(fence)) throw Error('retained_fence_lost');
  };
  const acquisition = (scope: Scope, fence: Fence, signal: AbortSignal): Acquisition => ({
    scope, fence, signal, plan: { workspaceStrategy: 'retained_runtime', outputStrategy: null, runtimeCheckpoint: null,
      driver: { type: 'ama_worker', process: { command: '/bin/true' } } },
    workspace: { bindingId: `tank-${scope.sessionId}`, mountPath: '/workspace' }, outputs: null, credentialEgress: null,
    environment: { type: 'base', identity: input.allocation.image,
      artifact: { type: 'image', reference: input.allocation.image } },
  });
  const lock = <T>(scope: Scope, mode: 'shared' | 'exclusive', work: () => Promise<T>) =>
    withNativeLock(home, retainedBoxName(scope), mode, work);
  const guarded = (fence: Fence, signal: AbortSignal, authorized?: () => Promise<void>) => async () => {
    signal.throwIfAborted();
    await authorized?.();
    await current(fence);
    await authorized?.();
    signal.throwIfAborted();
  };
  const attach = (runtime: BoxLiteRuntime, scope: Scope, assertCurrent: () => Promise<void>) => {
    guestWork.set(runtime, (work, assertBeforeLaunch) => lock(scope, 'shared', async () => {
      const assertLaunch = async () => {
        await assertCurrent();
        assertBeforeLaunch();
      };
      await assertLaunch();
      return mutation.run(assertLaunch, work);
    }));
    return runtime;
  };
  const publicProvider = (handle: Handle, scope: Scope) => handle.ownershipName
    ? input.module.createBoxLiteProvider({ ...options, runtimeName: () => retainedOwnershipName(handle, scope) }) : provider;
  const stopUnderLock = async (
    handle: Handle, scope: Scope, assertCurrent: () => Promise<void>,
    observe: <T>(observation: T | Promise<T>) => Promise<T> = async observation => observation,
    stop?: StopControl,
  ) => {
    const sdk = input.client as NativeBoxLiteClient;
    const name = retainedOwnershipName(handle, scope);
    const verify = (info: NativeBoxInfo | null) => {
      if (!info || info.id !== handle.runtimeId || info.name !== name) throw Error('retained_recovery_resident_changed');
      return info.state;
    };
    await assertCurrent();
    const state = verify(await observe(sdk.getInfo(handle.runtimeId)));
    if (state.running === true) {
      if (stop?.inspectOnly) throw Error('retained_recovery_stop_unconfirmed');
      await assertCurrent();
      const box = await observe(sdk.get(handle.runtimeId));
      if (!box || box.id !== handle.runtimeId || box.name !== name) throw Error('retained_recovery_resident_changed');
      await assertCurrent();
      if (verify(await observe(box.info())).running !== true) throw Error('retained_recovery_stop_unconfirmed');
      await assertCurrent();
      const sync = await box.exec('/bin/sync', [], [], false, null, 30, '/');
      const result = await observe(sync.wait());
      if (result.exitCode !== 0 || result.errorMessage) throw Error('retained_filesystem_flush_failed');
      await assertCurrent();
      stop?.onStopRequested?.(handle);
      await box.stop();
    }
    await assertCurrent();
    const stopped = verify(await observe(sdk.getInfo(handle.runtimeId)));
    if (stopped.running !== false || !/^(stopped|exited)$/i.test(stopped.status)) throw Error('retained_recovery_stop_unconfirmed');
    await assertCurrent();
  };
  const quiesceExact = async (handle: Handle, scope: Scope, assertCurrent: () => Promise<void>, intent?: Handle, stop?: StopControl) => {
    const timeout = Error('retained_recovery_stop_timeout');
    const timeoutMs = input.stopTimeoutMs ?? 35_000;
    const deadline = performance.now() + timeoutMs;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const bounded = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(timeout), timeoutMs); });
    const observe = <T>(observation: T | Promise<T>) => Promise.race([observation, bounded]);
    const check = async () => {
      if (performance.now() >= deadline) throw timeout;
      await observe(assertCurrent());
      if (performance.now() >= deadline) throw timeout;
    };
    try {
      const attempt = lock(scope, 'exclusive', async () => {
        if (!intent) return stopUnderLock(handle, scope, check, observe, stop);
        const name = retainedBoxName(scope);
        if (intent.provider !== 'litebox' || intent.runtimeId !== name || intent.ownershipName || handle.ownershipName) {
          throw Error('retained_allocation_intent_invalid');
        }
        await check();
        const info = await observe(inspect(name));
        if (!info && handle.runtimeId === name && !stop?.inspectOnly) return check();
        if (!info?.id || info.name !== name || (handle.runtimeId !== name && handle.runtimeId !== info.id)) {
          throw Error('retained_recovery_resident_changed');
        }
        await stopUnderLock({ provider: 'litebox', runtimeId: info.id }, scope, check, observe, stop);
      });
      await Promise.race([attempt, bounded]);
    } finally {
      clearTimeout(timer);
    }
  };
  return {
    quiesceExact,
    quiesceOwned: (handle, scope, assertCurrent, stop) =>
      lock(scope, 'exclusive', () => stopUnderLock(handle, scope, assertCurrent, undefined, stop)),
    async allocationIntent(scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      const name = retainedBoxName(scope);
      if (await inspect(name)) throw Error('retained_unpublished_runtime_exists');
      await current(fence);
      return { provider: 'litebox', runtimeId: name };
    },
    async resolveIntent(intent, scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      if (intent.provider !== 'litebox' || intent.runtimeId !== retainedBoxName(scope)) throw Error('retained_allocation_intent_invalid');
      const info = await inspect(intent.runtimeId);
      if (!info) return null;
      if (!info.id || info.name !== intent.runtimeId) throw Error('retained_cleanup_ownership_mismatch');
      return { provider: 'litebox', runtimeId: info.id };
    },
    async leftover(scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      const name = retainedBoxName(scope), info = await inspect(name);
      if (!info) return null;
      if (!info.id || info.name !== name) throw Error('retained_cleanup_ownership_mismatch');
      return { provider: 'litebox', runtimeId: info.id };
    },
    async create(scope, fence, signal, assertExecutionCurrent) {
      const assertCurrent = guarded(fence, signal, assertExecutionCurrent);
      return lock(scope, 'exclusive', () => mutation.run(assertCurrent, async () => {
        const name = retainedBoxName(scope);
        if (await inspect(name)) throw Error('retained_unpublished_runtime_exists');
        await assertCurrent();
        try {
          const runtime = await provider.create({ sessionId: scope.sessionId, workdir: '/workspace' }, {}, acquisition(scope, fence, signal));
          return attach(runtime, scope, assertCurrent);
        } catch (error) {
          const remaining = await inspect(name);
          if (remaining) {
            if (!remaining.id || remaining.name !== name) throw Error('retained_cleanup_ownership_mismatch');
            const handle = { provider: 'litebox', runtimeId: remaining.id };
            try {
              await assertCurrent();
              await resource.sandbox.reap({ scope, lease: handle, reason: 'completed' });
            } catch {
              await input.unpublishedCleanupFailed?.(handle, scope, fence).catch(() => undefined);
              throw Error('retained_initial_cleanup_failed');
            }
          }
          throw error;
        }
      }));
    },
    async resume(handle, scope, fence, signal, assertExecutionCurrent) {
      const assertCurrent = guarded(fence, signal, assertExecutionCurrent);
      return lock(scope, 'exclusive', () => mutation.run(assertCurrent, async () => {
        const info = await inspect(handle.runtimeId);
        if (!info) throw Error('retained_runtime_missing');
        if (info.id !== handle.runtimeId || info.name !== retainedOwnershipName(handle, scope)) throw Error('retained_cleanup_ownership_mismatch');
        const selectedProvider = publicProvider(handle, scope);
        const context = { sessionId: scope.sessionId, workdir: '/workspace' };
        await assertCurrent();
        if (info.state.running) {
          await stopUnderLock(handle, scope, assertCurrent);
        }
        const runtime = await selectedProvider.resume(handle, context, {}, acquisition(scope, fence, signal));
        return attach(runtime, scope, assertCurrent);
      }));
    },
    async reap(handle, scope, fence, signal, assertAuthorized) {
      const assertCurrent = guarded(fence, signal, assertAuthorized);
      await lock(scope, 'exclusive', () => mutation.run(assertCurrent, async () => {
        await assertCurrent();
        if (handle.ownershipName) {
          const name = retainedOwnershipName(handle, scope);
          const info = await inspect(handle.runtimeId);
          if (!info) return;
          if (info.name !== name || (handle.runtimeId !== name && info.id !== handle.runtimeId)) throw Error('retained_cleanup_ownership_mismatch');
          await current(fence);
          await client.remove(info.id, true);
        } else await resource.sandbox.reap({ scope, lease: handle, reason: 'completed' });
      }));
    },
  };
}
async function text(stream: ReadableStream<Uint8Array>): Promise<string> {
  return await new Response(stream).text();
}
export class RetainedBoxOperations {
  private readonly use: <T>(operation: (runtime: BoxLiteRuntime) => Promise<T>) => Promise<T>;
  constructor(use: <T>(operation: (runtime: BoxLiteRuntime) => Promise<T>) => Promise<T>) {
    this.use = operation => use(runtime => {
      if (unsafeRuntimes.has(runtime)) throw Error('retained_runtime_requires_stop');
      return operation(runtime);
    });
  }
  async exec(command: string, args: string[], env?: Record<string, string>, options?: { cwd?: string; user?: string; timeoutSecs?: number }) {
    return this.use(runtime => this.run(runtime, command, args, env, options, async (process, track) => {
      const completion = Promise.all([track(text(process.stdout)), track(text(process.stderr)), process.exited]);
      void completion.catch(() => undefined);
      const writer = process.stdin.getWriter();
      try { await writer.close().catch(() => undefined); } finally { writer.releaseLock(); }
      const [stdout, stderr, result] = await completion;
      return { stdout, stderr, exitCode: result.code ?? 1 };
    }));
  }
  private async run<T>(runtime: BoxLiteRuntime, command: string, args: string[], env: Record<string, string> | undefined,
    options: { cwd?: string; user?: string; timeoutSecs?: number } | undefined,
    exchange: (process: Process, track: TrackGuestWork) => Promise<T>): Promise<T> {
    if (options?.user) throw new Error('retained_exec_user_not_supported');
    if (unsafeRuntimes.has(runtime)) throw Error('retained_runtime_requires_stop');
    const timeoutMs = (options?.timeoutSecs ?? 120) * 1000;
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw Error('retained_exec_timeout_invalid');
    const deadline = performance.now() + timeoutMs;
    const timeout = Error('retained_exec_timeout');
    let process: Process | undefined;
    let draining: Promise<unknown> | undefined;
    let timedOut = false;
    const pending: Promise<unknown>[] = [];
    const track: TrackGuestWork = task => {
      pending.push(task);
      void task.catch(() => undefined);
      return task;
    };
    const killExactProcess = () => {
      if (!process || draining) return;
      const exact = process;
      draining = Promise.allSettled([
        Promise.resolve().then(() => exact.kill('SIGKILL')),
        exact.exited,
      ]);
    };
    const expire = () => {
      timedOut = true;
      unsafeRuntimes.add(runtime);
      killExactProcess();
    };
    const checkDeadline = () => {
      if (timedOut || performance.now() >= deadline) {
        expire();
        throw timeout;
      }
      if (unsafeRuntimes.has(runtime)) throw Error('retained_runtime_requires_stop');
    };
    const work = async () => {
      checkDeadline();
      try {
        process = await runtime.spawnDuplexProcess({ command, args, env, cwd: options?.cwd ?? '/workspace' });
        void process.exited.catch(() => undefined);
        checkDeadline();
        const result = await exchange(process, track);
        checkDeadline();
        return result;
      } catch (error) {
        unsafeRuntimes.add(runtime);
        killExactProcess();
        if (process) {
          if (!process.stdout.locked) track(text(process.stdout));
          if (!process.stderr.locked) track(text(process.stderr));
        }
        await Promise.allSettled([...pending, draining]);
        throw error;
      }
    };
    let timer: ReturnType<typeof setTimeout> | undefined;
    const bounded = new Promise<never>((_, reject) => {
      timer = setTimeout(() => { expire(); reject(timeout); }, timeoutMs);
    });
    const locked = guestWork.get(runtime);
    try {
      return await Promise.race([locked ? locked(work, checkDeadline) : work(), bounded]);
    } finally {
      clearTimeout(timer);
    }
  }
  async copyIn(host: string, guest: string): Promise<void> {
    await this.use(runtime =>
      copyGuestFileIn((args, exchange) => this.run(runtime, 'python3', args, undefined, { timeoutSecs: 120 }, exchange), host, guest));
  }
  async copyOut(guest: string, host: string): Promise<void> {
    await this.use(runtime =>
      copyGuestFileOut((args, exchange) => this.run(runtime, 'python3', args, undefined, { timeoutSecs: 120 }, exchange), guest, host));
  }
}

import { copyGuestFileIn, copyGuestFileOut, type TrackGuestWork } from './guest-file-io.js';
import { createHash } from 'node:crypto';
import type { Fence, Handle, Provider, RetainedRuntime, Scope } from './retained-session.js';

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
export function publicBoxLiteClient(raw: unknown) {
  const sdk = raw as NativeBoxLiteClient;
  const adapt = async (box: NativeBox) => {
    let info = await box.info();
    return {
      id: box.id, name: box.name, info: () => info,
      async start() { await box.start(); info = await box.info(); },
      async stop() { await box.stop(); info = { ...info, state: { ...info.state, running: false, status: 'Stopped' } }; },
      exec: (...args: Parameters<NativeBox['exec']>) => box.exec(...args),
      copyIn: (...args: Parameters<NativeBox['copyIn']>) => box.copyIn(...args), copyOut: (...args: Parameters<NativeBox['copyOut']>) => box.copyOut(...args),
    };
  };
  return {
    getInfo: (id: string) => sdk.getInfo(id),
    async get(id: string) { const box = await sdk.get(id); return box ? adapt(box) : null; },
    async getOrCreate(options: object, name?: string | null) { const result = await sdk.getOrCreate(options, name); return { ...result, box: await adapt(result.box) }; },
    remove: (id: string, force?: boolean) => sdk.remove(id, force),
  };
}
export function pinnedBoxLiteProvider(input: {
  module: PinnedBoxLiteModule; client: unknown; allocation: Allocation; leaseTtlMs: number;
  isCurrent?(fence: Fence): Promise<boolean>;
  unpublishedCleanupFailed?(handle: Handle, scope: Scope, fence: Fence): Promise<void>;
}): Provider<BoxLiteRuntime> {
  const client = publicBoxLiteClient(input.client);
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
  const expectedName = (scope: Scope) => 'oma-' + createHash('sha256').update(`litebox:${scope.sessionId}`).digest('hex').slice(0, 32);
  const upgradePrefix = (scope: Scope) => 'oma-' + createHash('sha256').update(JSON.stringify(scope)).digest('hex').slice(0, 32) + '-upgrade-';
  const ownershipName = (handle: Handle, scope: Scope): string => {
    if (!handle.ownershipName) return expectedName(scope);
    if (!handle.ownershipName.startsWith(upgradePrefix(scope)) || !/^[a-f0-9]{32}$/.test(handle.ownershipName.slice(upgradePrefix(scope).length))) throw Error('retained_cleanup_ownership_mismatch');
    return handle.ownershipName;
  };
  const publicProvider = (handle: Handle, scope: Scope) => handle.ownershipName
    ? input.module.createBoxLiteProvider({ ...options, runtimeName: () => ownershipName(handle, scope) }) : provider;
  const quiesceExact = async (handle: Handle, scope: Scope, assertCurrent: () => Promise<void>) => {
    const sdk = input.client as NativeBoxLiteClient;
    const name = ownershipName(handle, scope);
    const verify = (info: NativeBoxInfo | null) => {
      if (!info || info.id !== handle.runtimeId || info.name !== name) throw Error('retained_recovery_resident_changed');
      return info.state;
    };
    await assertCurrent();
    const state = verify(await sdk.getInfo(handle.runtimeId));
    if (state.running) {
      const box = await sdk.get(handle.runtimeId);
      if (!box || box.id !== handle.runtimeId || box.name !== name) throw Error('retained_recovery_resident_changed');
      if (!verify(await box.info()).running) throw Error('retained_recovery_stop_unconfirmed');
      await assertCurrent();
      const sync = await box.exec('/bin/sync', [], [], false, null, 30, '/');
      const result = await sync.wait();
      if (result.exitCode !== 0 || result.errorMessage) throw Error('retained_filesystem_flush_failed');
      await assertCurrent();
      await box.stop();
    }
    await assertCurrent();
    const stopped = verify(await sdk.getInfo(handle.runtimeId));
    if (stopped.running || !/^(stopped|exited)$/i.test(stopped.status)) throw Error('retained_recovery_stop_unconfirmed');
    await assertCurrent();
  };
  return {
    quiesceExact,
    async allocationIntent(scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      const name = expectedName(scope);
      if (await inspect(name)) throw Error('retained_unpublished_runtime_exists');
      await current(fence);
      return { provider: 'litebox', runtimeId: name };
    },
    async resolveIntent(intent, scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      if (intent.provider !== 'litebox' || intent.runtimeId !== expectedName(scope)) throw Error('retained_allocation_intent_invalid');
      const info = await inspect(intent.runtimeId);
      if (!info) return null;
      if (!info.id || info.name !== intent.runtimeId) throw Error('retained_cleanup_ownership_mismatch');
      return { provider: 'litebox', runtimeId: info.id };
    },
    async leftover(scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      const name = expectedName(scope), info = await inspect(name);
      if (!info) return null;
      if (!info.id || info.name !== name) throw Error('retained_cleanup_ownership_mismatch');
      return { provider: 'litebox', runtimeId: info.id };
    },
    async create(scope, fence, signal, assertExecutionCurrent) {
      const name = expectedName(scope);
      if (await inspect(name)) throw Error('retained_unpublished_runtime_exists');
      await assertExecutionCurrent?.();
      await current(fence);
      try {
        return await provider.create({ sessionId: scope.sessionId, workdir: '/workspace' }, {}, acquisition(scope, fence, signal));
      } catch (error) {
        const remaining = await inspect(name);
        if (remaining) {
          if (!remaining.id || remaining.name !== name) throw Error('retained_cleanup_ownership_mismatch');
          const handle = { provider: 'litebox', runtimeId: remaining.id };
          try {
            await assertExecutionCurrent?.();
            await current(fence);
            await resource.sandbox.reap({ scope, lease: handle, reason: 'completed' });
          } catch {
            await input.unpublishedCleanupFailed?.(handle, scope, fence).catch(() => undefined);
            throw Error('retained_initial_cleanup_failed');
          }
        }
        throw error;
      }
    },
    async resume(handle, scope, fence, signal, assertExecutionCurrent) {
      const assertCurrent = async () => { await assertExecutionCurrent?.(); await current(fence); };
      const info = await inspect(handle.runtimeId);
      if (!info) throw Error('retained_runtime_missing');
      if (info.id !== handle.runtimeId || info.name !== ownershipName(handle, scope)) throw Error('retained_cleanup_ownership_mismatch');
      const selectedProvider = publicProvider(handle, scope);
      const context = { sessionId: scope.sessionId, workdir: '/workspace' };
      await assertCurrent();
      if (info.state.running) {
        await quiesceExact(handle, scope, async () => { signal.throwIfAborted(); await assertCurrent(); });
      }
      return await selectedProvider.resume(handle, context, {}, acquisition(scope, fence, signal));
    },
    async reap(handle, scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      if (handle.ownershipName) {
        const name = ownershipName(handle, scope);
        const info = await inspect(handle.runtimeId);
        if (!info) return;
        if (info.name !== name || (handle.runtimeId !== name && info.id !== handle.runtimeId)) throw Error('retained_cleanup_ownership_mismatch');
        await current(fence);
        await client.remove(info.id, true);
      } else await resource.sandbox.reap({ scope, lease: handle, reason: 'completed' });
    },
  };
}
async function text(stream: ReadableStream<Uint8Array>): Promise<string> {
  return await new Response(stream).text();
}
export class RetainedBoxOperations {
  readonly use: <T>(operation: (runtime: BoxLiteRuntime) => Promise<T>) => Promise<T>;
  private unsafe = new WeakSet<BoxLiteRuntime>();
  constructor(use: <T>(operation: (runtime: BoxLiteRuntime) => Promise<T>) => Promise<T>) {
    this.use = operation => use(runtime => {
      if (this.unsafe.has(runtime)) throw Error('retained_runtime_requires_stop');
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
    if (this.unsafe.has(runtime)) throw Error('retained_runtime_requires_stop');
    const timeoutMs = (options?.timeoutSecs ?? 120) * 1000;
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw Error('retained_exec_timeout_invalid');
    const deadline = performance.now() + timeoutMs;
    let process: Process | undefined;
    let timedOut = false;
    let draining: Promise<unknown> | undefined;
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
      this.unsafe.add(runtime);
      killExactProcess();
    };
    const timer = setTimeout(expire, timeoutMs);
    try {
      process = await runtime.spawnDuplexProcess({ command, args, env, cwd: options?.cwd ?? '/workspace' });
      void process.exited.catch(() => undefined);
      if (timedOut || performance.now() >= deadline) {
        expire();
        throw Error('retained_exec_timeout');
      }
      const result = await exchange(process, track);
      if (timedOut || performance.now() >= deadline) {
        expire();
        throw Error('retained_exec_timeout');
      }
      return result;
    } catch (error) {
      this.unsafe.add(runtime);
      killExactProcess();
      if (process) {
        if (!process.stdout.locked) track(text(process.stdout));
        if (!process.stderr.locked) track(text(process.stderr));
      }
      await Promise.allSettled([...pending, draining]);
      if (timedOut) throw Error('retained_exec_timeout');
      throw error;
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

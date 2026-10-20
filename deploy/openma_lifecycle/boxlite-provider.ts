/** Bridge the pinned public BoxLite provider to LiteBoxSandbox's existing
 * instance interface. It keeps pi, tools, mounts and per-command env unchanged.
 * No SDK private field or native stopped-handle restart is used. */
import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import type { Fence, Handle, Provider, RetainedRuntime, Scope } from './retained-session.ts';

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
  createBoxLiteProvider(options: { providerId: 'litebox'; image: string }): PublicProvider;
  createBoxLiteManagedRuntime(options: { providerId: 'litebox'; image: string; leaseTtlMs: number }): {
    sandbox: { reap(input: { scope: Scope; lease: Handle; reason: 'completed' }): Promise<void> };
  };
}
/** OpenMA85d expects synchronous Box.info; SDK0.10.6 makes it async.
 * Resolve a public snapshot on every fresh acquisition and refresh after start/
 * stop. No private SDK fields or old stopped-handle restarts are used. */
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
    // The SDK supports rootfsPath; the provider forwards allocationOptions.
    // No env, keys, mutable secret snapshots or overridden lifecycle fields.
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
      // Allocation is provider-neutral; the existing Node pi runner executes
      // the turn, never this descriptor's external worker command.
      driver: { type: 'ama_worker', process: { command: '/bin/true' } } },
    workspace: { bindingId: `tank-${scope.sessionId}`, mountPath: '/workspace' }, outputs: null, credentialEgress: null,
    environment: { type: 'base', identity: input.allocation.image,
      artifact: { type: 'image', reference: input.allocation.image } },
  });
  const expectedName = (scope: Scope) => 'oma-' + createHash('sha256').update(`litebox:${scope.sessionId}`).digest('hex').slice(0, 32);
  const stopExactPublished = async (handle: Handle, scope: Scope, fence: Fence) => {
    await current(fence);
    const freshClient = client as unknown as { get(id: string): Promise<null | {
      id: string; info(): { id: string; name: string; state: { running: boolean } }; stop(): Promise<void>;
    }> };
    const box = await freshClient.get(handle.runtimeId);
    if (!box) return;
    const info = box.info();
    if (box.id !== handle.runtimeId || info.id !== handle.runtimeId || info.name !== expectedName(scope)) throw Error('retained_cleanup_ownership_mismatch');
    await current(fence);
    if (info.state.running) await box.stop();
  };
  return {
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
    async create(scope, fence, signal) {
      // Guard the exact stable-name contract of the hash-pinned OpenMA
      // provider. An orphaned disk without its canonical fingerprint must
      // never be silently adopted through getOrCreate.
      const name = expectedName(scope);
      if (await inspect(name)) throw Error('retained_unpublished_runtime_exists');
      await current(fence);
      try {
        return await provider.create({ sessionId: scope.sessionId, workdir: '/workspace' }, {}, acquisition(scope, fence, signal));
      } catch (error) {
        // Pinned provider compensates its own created=true failure, but suppresses
        // remove errors; getOrCreate may also throw after allocating natively.
        // Preflight proved this exact scoped name absent under our current fence.
        // Inspect it afresh, never adopt it, and use public ownership-checked reap.
        const remaining = await inspect(name);
        if (remaining) {
          if (!remaining.id || remaining.name !== name) throw Error('retained_cleanup_ownership_mismatch');
          const handle = { provider: 'litebox', runtimeId: remaining.id };
          try {
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
    async resume(handle, scope, fence, signal) {
      const info = await inspect(handle.runtimeId);
      if (!info) throw Error('retained_runtime_missing');
      const context = { sessionId: scope.sessionId, workdir: '/workspace' };
      await current(fence);
      try {
      if (info.state.running) {
        // A previous controller may have crashed mid-command. The public
        // provider validates ownership before we stop those orphan processes;
        // the second resume obtains a fresh native handle for the retained disk.
        const previous = await provider.resume(handle, context, {}, acquisition(scope, fence, signal));
        await current(fence);
        await previous.suspend({ kind: 'filesystem' });
        await current(fence);
      }
      return await provider.resume(handle, context, {}, acquisition(scope, fence, signal));
      } catch (error) {
        // Resume can start the guest, then fail in ensureWorkspace/abort before
        // returning a runtime. A fresh public SDK handle safely stops precisely
        // the published resource while retaining its disk and manifest.
        try { await stopExactPublished(handle, scope, fence); }
        catch { throw Error('retained_resume_cleanup_failed'); }
        throw error;
      }
    },
    async reap(handle, scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      await resource.sandbox.reap({ scope, lease: handle, reason: 'completed' });
    },
  };
}
async function text(stream: ReadableStream<Uint8Array>): Promise<string> {
  return await new Response(stream).text();
}
/** Matches the current adapter's SimpleBox-compatible methods. Environment
 * values are sent only on an exec, never to provider creation or publication. */
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
    if (options?.user) throw new Error('retained_exec_user_not_supported');
    return this.use(async runtime => {
      const timeoutMs = (options?.timeoutSecs ?? 120) * 1000;
      if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw Error('retained_exec_timeout_invalid');
      const deadline = performance.now() + timeoutMs;
      let process: Process | undefined, timedOut = false, killing = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const killExactProcess = () => {
        if (!process || killing) return;
        killing = true;
        const exact = process;
        // Consume asynchronous outcomes even when spawn or stdin returned only
        // after the caller's deadline. Never await an unconfirmed native kill.
        void exact.exited.catch(() => undefined);
        void Promise.resolve().then(() => exact.kill('SIGKILL')).catch(() => undefined);
        void exact.stdout.cancel().catch(() => undefined);
        void exact.stderr.cancel().catch(() => undefined);
      };
      let rejectDeadline: (error: Error) => void;
      const expire = () => {
        timedOut = true;
        // This runtime remains poisoned even if a late kill eventually succeeds.
        // Only safe VM stop and a fresh provider resume admit new guest I/O.
        this.unsafe.add(runtime);
        killExactProcess();
        const error = new Error('retained_exec_timeout');
        rejectDeadline(error);
        return error;
      };
      const expired = new Promise<never>((_, reject) => {
        rejectDeadline = reject;
        timer = setTimeout(expire, timeoutMs);
      });
      const execute = async () => {
        process = await runtime.spawnDuplexProcess({ command, args, env, cwd: options?.cwd ?? '/workspace' });
        if (timedOut || performance.now() >= deadline) throw expire();
        // Drain output while closing stdin; either native handshake may stall.
        const completion = Promise.all([text(process.stdout), text(process.stderr), process.exited]);
        void completion.catch(() => undefined);
        const writer = process.stdin.getWriter();
        try { await writer.close().catch(() => undefined); } finally { writer.releaseLock(); }
        if (timedOut || performance.now() >= deadline) throw expire();
        const [stdout, stderr, result] = await completion;
        if (timedOut || performance.now() >= deadline) throw expire();
        return { stdout, stderr, exitCode: result.code ?? 1 };
      };
      try { return await Promise.race([execute(), expired]); }
      finally { if (timer) clearTimeout(timer); }

    });
  }
  async copyIn(host: string, guest: string): Promise<void> {
    const bytes = await readFile(host);
    await this.use(runtime => runtime.writeFileBytes(guest, bytes));
  }
  async copyOut(guest: string, host: string): Promise<void> {
    const bytes = await this.use(runtime => runtime.readFileBytes(guest));
    await writeFile(host, bytes);
  }
}

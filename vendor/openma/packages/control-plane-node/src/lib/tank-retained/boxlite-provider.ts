import { copyGuestFileIn, copyGuestFileOut } from './guest-file-io.js';
import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { workspaceTransferProgram } from './workspace-transfer.js';
import { transferWorkspaceArchive } from './workspace-chunks.js';
import { assertLegacyNativeConfiguration, readNativeLegacyConfiguration, validateLegacyNativeArtifact, type NativeLegacyConfiguration } from './legacy-native-provenance.js';
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
  readLegacyConfiguration?(runtimeId: string): Promise<NativeLegacyConfiguration | null>;
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
  const decorate = (runtime: BoxLiteRuntime, name: string): BoxLiteRuntime => {
    const handle = runtime.runtimeHandle();
    runtime.runtimeHandle = () => ({ ...handle, ownershipName: name });
    return runtime;
  };
  const stopExactPublished = async (handle: Handle, scope: Scope, fence: Fence) => {
    await current(fence);
    const freshClient = client as unknown as { get(id: string): Promise<null | {
      id: string; info(): { id: string; name: string; state: { running: boolean } }; stop(): Promise<void>;
    }> };
    const box = await freshClient.get(handle.runtimeId);
    if (!box) return;
    const info = box.info();
    if (box.id !== handle.runtimeId || info.id !== handle.runtimeId || info.name !== ownershipName(handle, scope)) throw Error('retained_cleanup_ownership_mismatch');
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
      const name = expectedName(scope);
      if (await inspect(name)) throw Error('retained_unpublished_runtime_exists');
      await current(fence);
      try {
        return await provider.create({ sessionId: scope.sessionId, workdir: '/workspace' }, {}, acquisition(scope, fence, signal));
      } catch (error) {
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
    async validateLegacy(handle, scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      const info = await inspect(handle.runtimeId);
      if (!info || info.id !== handle.runtimeId || info.name !== expectedName(scope)) throw Error('retained_legacy_provider_provenance_missing:runtime_ownership');
      const configuration = await (input.readLegacyConfiguration ?? readNativeLegacyConfiguration)(handle.runtimeId);
      assertLegacyNativeConfiguration(configuration, handle.runtimeId, expectedName(scope), input.allocation);
      await validateLegacyNativeArtifact();
      await current(fence);
    },
    async replacementIntent(_source, _identity, scope, fence, signal) {
      signal.throwIfAborted(); await current(fence);
      const name = upgradePrefix(scope) + randomUUID().replaceAll('-', '');
      if (await inspect(name)) throw Error('retained_unpublished_runtime_exists');
      return { provider: 'litebox', runtimeId: name, ownershipName: name };
    },
    async replace(journal, scope, fence, signal, assertCurrent) {
      const name = ownershipName(journal.target, scope);
      if (!journal.target.ownershipName || journal.target.runtimeId !== name) throw Error('retained_upgrade_intent_invalid');
      await assertCurrent();
      if (await inspect(name)) throw Error('retained_upgrade_target_exists');
      const directory = await mkdtemp(join(tmpdir(), 'tank-runtime-upgrade-'));
      const guestArchive = '/tank-runtime-upgrade-' + randomUUID() + '.tar';
      const allowed = JSON.stringify((input.allocation.volumes ?? []).map(volume => volume.guestPath));
      let source: BoxLiteRuntime | undefined, replacement: BoxLiteRuntime | undefined;
      try {
        source = await this.resume(journal.source, scope, fence, signal);
        await assertCurrent();
        const original = new RetainedBoxOperations(async operation => { await assertCurrent(); return operation(source!); });
        const exported = await original.exec('python3', ['-c', workspaceTransferProgram, 'export', guestArchive, allowed, '/workspace'], undefined, { cwd: '/', timeoutSecs: 120 });
        if (exported.exitCode !== 0 || !/^[a-f0-9]{64}$/.test(exported.stdout.trim())) throw Error('retained_upgrade_transfer_failed:workspace_export');
        await assertCurrent();
        const selectedProvider = publicProvider(journal.target, scope);
        replacement = decorate(await selectedProvider.create({ sessionId: scope.sessionId, workdir: '/workspace' }, {}, acquisition(scope, fence, signal)), name);
        const info = await inspect(name);
        if (!info || info.id !== replacement.runtimeHandle().runtimeId || info.name !== name) throw Error('retained_upgrade_target_ownership_invalid');
        await assertCurrent();
        const target = new RetainedBoxOperations(async operation => { await assertCurrent(); return operation(replacement!); });
        await transferWorkspaceArchive({ source: original, target, archive: guestArchive, hostDirectory: directory, assertCurrent });
        const sourceRemoved = await original.exec('rm', ['-f', guestArchive], undefined, { cwd: '/' });
        if (sourceRemoved.exitCode !== 0) throw Error('retained_upgrade_transfer_failed:archive_cleanup');
        await assertCurrent();
        await source.suspend({ kind: 'filesystem' }); source = undefined;
        await assertCurrent();
        const imported = await target.exec('python3', ['-c', workspaceTransferProgram, 'import', guestArchive, allowed, '/workspace'], undefined, { cwd: '/', timeoutSecs: 120 });
        if (imported.exitCode !== 0 || imported.stdout.trim() !== exported.stdout.trim()) throw Error('retained_upgrade_transfer_failed:workspace_validation');
        const removed = await target.exec('rm', ['-f', guestArchive], undefined, { cwd: '/' });
        if (removed.exitCode !== 0) throw Error('retained_upgrade_transfer_failed:archive_cleanup');
        await assertCurrent();
        return replacement;
      } catch (error) {
        if (await input.isCurrent?.(fence)) {
          for (const runtime of [source, replacement]) if (runtime) {
            try {
              await assertCurrent();
              const cleanup = new RetainedBoxOperations(async operation => { await assertCurrent(); return operation(runtime); });
              await cleanup.exec('rm', ['-f', guestArchive, guestArchive + '.part'], undefined, { cwd: '/' });
            } catch {}
          }
          await Promise.allSettled([source?.suspend({ kind: 'filesystem' }), replacement?.suspend({ kind: 'filesystem' })]);
        }
        if (error instanceof Error && /retained_|lease|abort/i.test(error.message)) throw error;
        throw Error('retained_upgrade_transfer_failed:provider_io');
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    },
    async resume(handle, scope, fence, signal) {
      const info = await inspect(handle.runtimeId);
      if (!info) throw Error('retained_runtime_missing');
      const context = { sessionId: scope.sessionId, workdir: '/workspace' };
      const selectedProvider = publicProvider(handle, scope);
      if (handle.ownershipName && (info.id !== handle.runtimeId || info.name !== ownershipName(handle, scope))) throw Error('retained_cleanup_ownership_mismatch');
      await current(fence);
      try {
      if (info.state.running) {
        const previous = await selectedProvider.resume(handle, context, {}, acquisition(scope, fence, signal));
        await current(fence);
        await previous.suspend({ kind: 'filesystem' });
        await current(fence);
      }
      const resumed = await selectedProvider.resume(handle, context, {}, acquisition(scope, fence, signal));
      return handle.ownershipName ? decorate(resumed, handle.ownershipName) : resumed;
      } catch (error) {
        try { await stopExactPublished(handle, scope, fence); }
        catch { throw Error('retained_resume_cleanup_failed'); }
        throw error;
      }
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
    return this.run(command, args, env, options, async process => {
      const completion = Promise.all([text(process.stdout), text(process.stderr), process.exited]);
      void completion.catch(() => undefined);
      const writer = process.stdin.getWriter();
      try { await writer.close().catch(() => undefined); } finally { writer.releaseLock(); }
      const [stdout, stderr, result] = await completion;
      return { stdout, stderr, exitCode: result.code ?? 1 };
    });
  }
  private async run<T>(command: string, args: string[], env: Record<string, string> | undefined,
    options: { cwd?: string; user?: string; timeoutSecs?: number } | undefined,
    exchange: (process: Process) => Promise<T>): Promise<T> {
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
        void exact.exited.catch(() => undefined);
        void Promise.resolve().then(() => exact.kill('SIGKILL')).catch(() => undefined);
        void exact.stdout.cancel().catch(() => undefined);
        void exact.stderr.cancel().catch(() => undefined);
      };
      let rejectDeadline: (error: Error) => void;
      const expire = () => {
        timedOut = true;
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
        const result = await exchange(process);
        if (timedOut || performance.now() >= deadline) throw expire();
        return result;
      };
      try { return await Promise.race([execute(), expired]); }
      catch (error) {
        this.unsafe.add(runtime); killExactProcess(); throw error;
      }
      finally { if (timer) clearTimeout(timer); }

    });
  }
  async copyIn(host: string, guest: string): Promise<void> {
    await copyGuestFileIn((args, exchange) => this.run('python3', args, undefined, { timeoutSecs: 120 }, exchange), host, guest);
  }
  async copyOut(guest: string, host: string): Promise<void> {
    await copyGuestFileOut((args, exchange) => this.run('python3', args, undefined, { timeoutSecs: 120 }, exchange), guest, host);
  }
}

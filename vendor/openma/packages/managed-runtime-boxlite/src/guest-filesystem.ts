import { performance } from 'node:perf_hooks';
import timers from 'node:timers';

type GuestFlushExecution = {
  wait(): Promise<{ exitCode: number; errorMessage?: string }>;
  kill(): Promise<void>;
};
type GuestFlushBox = {
  info(): { state: { running: boolean } };
  exec(command: string, args: string[], env: Array<[string, string]>, tty: boolean,
    user: null, timeoutSecs: number, workingDir: string): Promise<GuestFlushExecution>;
  stop(): Promise<void>;
};

export class BoxLiteFilesystemDurability {
  readonly #box: GuestFlushBox;
  #unsafe = false;
  #busy = false;

  constructor(box: GuestFlushBox) { this.#box = box; }

  assertUsable(): void {
    if (this.#unsafe) throw Error('retained_runtime_requires_stop');
    if (this.#busy) throw Error('retained_runtime_quiescing');
  }

  async #flush(assertCurrent: () => Promise<void>, allowStopped: boolean): Promise<void> {
    const deadline = performance.now() + 30_000;
    let execution: GuestFlushExecution | undefined, expired = false, killing = false;
    const kill = () => {
      if (!execution || killing) return;
      killing = true;
      const exact = execution;
      void Promise.resolve().then(() => exact.kill()).catch(() => undefined);
    };
    let timer: ReturnType<typeof timers.setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = timers.setTimeout(() => {
        expired = true;
        kill();
        reject(Error('retained_exec_timeout'));
      }, 30_000);
    });
    const check = () => {
      if (expired || performance.now() >= deadline) {
        expired = true; kill(); throw Error('retained_exec_timeout');
      }
    };
    const flush = async () => {
      await assertCurrent(); check();
      if (!this.#box.info().state.running) {
        if (!allowStopped) throw Error('retained_filesystem_flush_failed');
        return;
      }
      execution = await this.#box.exec('/bin/sync', [], [], false, null, 30, '/');
      check();
      const result = await execution.wait(); check();
      if (result.exitCode !== 0 || result.errorMessage) throw Error('retained_filesystem_flush_failed');
      await assertCurrent(); check();
    };
    try { await Promise.race([flush(), timeout]); }
    catch (error) { kill(); throw error; }
    finally { if (timer) timers.clearTimeout(timer); }
  }

  async #quiesce(assertCurrent: () => Promise<void>, stop: boolean): Promise<void> {
    if (this.#busy) throw Error('retained_runtime_quiescing');
    if (!stop) this.assertUsable();
    this.#busy = true;
    try {
      await this.#flush(assertCurrent, stop);
      if (stop) {
        await assertCurrent();
        if (this.#box.info().state.running) await this.#box.stop();
        this.#unsafe = false;
      }
    } catch (error) {
      this.#unsafe = true;
      throw error;
    } finally { this.#busy = false; }
  }

  async flush(assertCurrent: () => Promise<void> = async () => {}): Promise<void> {
    await this.#quiesce(assertCurrent, false);
  }

  async suspend(assertCurrent: () => Promise<void> = async () => {}): Promise<void> {
    await this.#quiesce(assertCurrent, true);
  }
}

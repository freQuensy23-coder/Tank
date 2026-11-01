// BoxLite (aka Litebox) implementation of SandboxExecutor.
//
// BoxLite (https://github.com/boxlite-ai/boxlite, npm: @boxlite-ai/boxlite)
// is a local-first micro-VM sandbox — each box gets its own kernel via
// Firecracker. Hardware-level isolation, no daemon required, ships a
// Rust core with a Node.js binding. The selling point vs LocalSubprocess:
// real isolation against `rm -rf /` from a prompt-injected agent without
// needing the operator to run docker.
//
// Driver dep is a peer with peerDependenciesMeta.optional — caller installs:
//   pnpm add @boxlite-ai/boxlite
//
// SDK shape (as of @boxlite-ai/boxlite ^0.8):
//   const box = new SimpleBox({ image, env, volumes, ... })
//   await box.exec("sh", ["-c", command], env, { timeoutSecs })
//   await box.copyIn(hostPath, containerPath)
//   await box.copyOut(containerPath, hostPath)
//   await box.stop()
//
// Lazy-create: BoxLite's box is created on first exec(), so any
// configuration that needs to land BEFORE creation (volumes, image, env)
// must be collected via setEnvVars / mountMemoryStore / ctor opts before
// the first exec/readFile/writeFile call. We track this via `created` —
// once true, mutating ops that affect ctor opts throw.

import { verifyReplayEvidence } from "./tank-replay-evidence";
import type { ProcessHandle, SandboxExecutor, SandboxFactory } from "@open-managed-agents/sandbox";
import { promises as fs, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { randomBytes } from "node:crypto";
import { isIP } from "node:net";
import { Buffer } from "node:buffer";
import { getLogger } from "@open-managed-agents/observability";

const moduleLogger = getLogger("litebox-sandbox");

export interface LiteBoxSandboxOptions {
  /** Container image. Default: `node:22-bookworm` (includes git for Managed repository resources). */
  image?: string;
  rootfsPath?: string;
  tankNetwork?: { mode: "enabled" | "disabled"; allowNet?: string[] };
  replayEvidence?: { hostPath: string; guestPath: "/replay/evidence"; readOnly: true };
  /** Optional VM resource limits. */
  memoryMib?: number;
  cpus?: number;
  /** Default per-call timeout (ms). */
  defaultTimeoutMs?: number;
  /** Logger for debug/warn output. */
  logger?: { warn: (msg: string, ctx?: unknown) => void; log: (msg: string) => void };
  /** Optional name (must be unique). */
  name?: string;
  /** Memory blob root on the host — used by mountMemoryStore for the
   *  per-storeId source dir. Falls back to `process.env.MEMORY_BLOB_DIR`
   *  when omitted (back-compat for prior deploys). */
  memoryRoot?: string;
  /** Per-session outputs root on the host — used by mountSessionOutputs.
   *  Falls back to `process.env.FILES_BLOB_DIR` when omitted. */
  outputsRoot?: string;
  /** Host-owned lifecycle root for copy staging. The factory binds this to
   *  the session workdir so crash leftovers are reaped with the session. */
  scratchRoot?: string;
}

interface LiteBoxExecResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}
interface LiteBoxInstance {
  exec(
    cmd: string,
    args: string[],
    env?: Record<string, string>,
    options?: { cwd?: string; user?: string; timeoutSecs?: number },
  ): Promise<LiteBoxExecResult>;
  copyIn(hostPath: string, containerDest: string, options?: unknown): Promise<void>;
  copyOut(containerSrc: string, hostDest: string, options?: unknown): Promise<void>;
  stop(): Promise<void>;
}

interface RetainedBoxOptions {
  image: string; rootfsPath?: string; cpus?: number; memoryMib?: number; name?: string;
  security?: { jailerEnabled: boolean; seccompEnabled: boolean };
  volumes?: Array<{hostPath: string; guestPath: string; readOnly?: boolean}>;
  network?: {mode: "enabled" | "disabled"; allowNet?: string[]};
}
export class LiteBoxSandbox implements SandboxExecutor {
  private retainedBoxFactory: ((options: RetainedBoxOptions) => Promise<LiteBoxInstance>) | null = null;
  setRetainedBoxFactory(factory: (options: RetainedBoxOptions) => Promise<LiteBoxInstance>): void {
    if (this.boxPromise) throw new Error("Retained lifecycle must bind before first sandbox use");
    this.retainedBoxFactory = factory;
  }
  private envVars: Record<string, string> = {};
  private commandSecrets: Array<{ prefix: string; secrets: Record<string, string> }> = [];
  private volumes: Array<{ hostPath: string; guestPath: string; readOnly?: boolean }> = [];
  private boxPromise: Promise<LiteBoxInstance> | null = null;
  private logger: NonNullable<LiteBoxSandboxOptions["logger"]>;
  private tmpRoot: string;

  constructor(private opts: LiteBoxSandboxOptions = {}) {
    this.logger = opts.logger ?? {
      warn: (msg) => moduleLogger.warn({}, msg),
      log: (msg) => moduleLogger.info(msg),
    };
    // Per-instance host scratch dir for copyIn / copyOut staging. Cleaned
    // up on destroy.
    this.tmpRoot = opts.scratchRoot
      ? join(resolve(opts.scratchRoot), ".openma", `litebox-scratch-${Buffer.from(randomBytes(6)).toString("hex")}`)
      : join(tmpdir(), `oma-litebox-${Buffer.from(randomBytes(6)).toString("hex")}`);
    mkdirSync(this.tmpRoot, { recursive: true });
    if (opts.replayEvidence) this.volumes.push(opts.replayEvidence);
  }

  async exec(command: string, timeout?: number): Promise<string> {
    const box = await this.ensureBox();
    const env = this.buildEnv(command);
    const timeoutMs = timeout ?? this.opts.defaultTimeoutMs ?? 120_000;
    try {
      const r = await box.exec("sh", ["-c", command], env, {
        cwd: "/workspace",
        timeoutSecs: Math.max(1, Math.ceil(timeoutMs / 1000)),
      });
      const separator = r.stdout.length > 0 && !r.stdout.endsWith("\n") ? "\n" : "";
      const combined =
        (r.stdout + (r.stderr ? `${separator}${r.stderr}` : "")).replace(/\s+$/, "") +
        (r.exitCode !== 0 ? `\n[exit ${r.exitCode}]` : "");
      return combined;
    } catch (err) {
      return `[error: ${(err as Error).message}]`;
    }
  }

  async startProcess(_command: string): Promise<ProcessHandle | null> {
    // BoxLite's exec is blocking; backgrounding would need a separate API
    // (and a ProcessHandle bookkeeping pass). Returning null keeps the
    // harness's startProcess callers correct — they fall back to exec
    // with a longer timeout.
    return null;
  }

  async setEnvVars(envVars: Record<string, string>): Promise<void> {
    this.envVars = { ...this.envVars, ...envVars };
  }

  registerCommandSecrets(commandPrefix: string, secrets: Record<string, string>): void {
    this.commandSecrets.push({ prefix: commandPrefix, secrets });
  }

  async setOutboundContext(_opts?: { tenantId: string; sessionId: string }): Promise<void> {
    // BoxLite supports network — we route via env vars same as the
    // LocalSubprocess path. The CA cert lives on the host though, so
    // copy it into the box on first exec via ensureBox.
    const proxyUrl = process.env.OMA_VAULT_PROXY_URL;
    const caCertPath = process.env.OMA_VAULT_CA_CERT;
    if (!proxyUrl || !caCertPath) return;

    const inBoxCaPath = "/etc/ssl/oma-vault-ca.crt";
    await this.setEnvVars({
      HTTP_PROXY: proxyUrl,
      HTTPS_PROXY: proxyUrl,
      http_proxy: proxyUrl,
      https_proxy: proxyUrl,
      NODE_EXTRA_CA_CERTS: inBoxCaPath,
      SSL_CERT_FILE: inBoxCaPath,
      CURL_CA_BUNDLE: inBoxCaPath,
    });
    // Schedule the CA upload on first ensureBox call. We keep it as a
    // pending action because the box may not exist yet.
    this.pendingCaUpload = { hostPath: caCertPath, guestPath: inBoxCaPath };
  }

  async mountMemoryStore(opts: {
    storeName: string;
    storeId: string;
    readOnly: boolean;
  }): Promise<void> {
    if (this.boxPromise) {
      throw new Error(
        "LiteBoxSandbox.mountMemoryStore: box already created — mounts must " +
        "be configured before the first exec/readFile/writeFile call",
      );
    }
    const memoryRoot = this.opts.memoryRoot
      ? resolve(this.opts.memoryRoot)
      : process.env.MEMORY_BLOB_DIR
        ? resolve(process.env.MEMORY_BLOB_DIR)
        : null;
    if (!memoryRoot) {
      throw new Error(
        "LiteBoxSandbox.mountMemoryStore: memoryRoot not provided (constructor) " +
        "and MEMORY_BLOB_DIR env var not set — set one of them so mounts can " +
        "find the on-disk content",
      );
    }
    if (!this.opts.memoryRoot && process.env.MEMORY_BLOB_DIR) {
      this.logger.warn(
        "LiteBoxSandbox.mountMemoryStore falling back to MEMORY_BLOB_DIR env var; " +
        "wire ctx.memoryRoot through SandboxFactoryContext to remove the warning",
      );
    }
    const hostPath = join(memoryRoot, opts.storeId);
    mkdirSync(hostPath, { recursive: true });
    const guestPath = `/mnt/memory/${opts.storeName}`;
    this.volumes.push({ hostPath, guestPath, readOnly: opts.readOnly });
  }

  sessionOutputMountCapabilities(): { durability: "durable" } | null {
    return this.opts.outputsRoot || process.env.FILES_BLOB_DIR
      ? { durability: "durable" }
      : null;
  }

  async mountSessionOutputs(opts: {
    tenantId: string;
    sessionId: string;
  }): Promise<void> {
    if (this.boxPromise) {
      throw new Error(
        "LiteBoxSandbox.mountSessionOutputs: box already created — mounts must " +
        "be configured before the first exec/readFile/writeFile call",
      );
    }
    const outputsRoot = this.opts.outputsRoot
      ? resolve(this.opts.outputsRoot)
      : process.env.FILES_BLOB_DIR
        ? resolve(process.env.FILES_BLOB_DIR)
        : null;
    if (!outputsRoot) {
      throw new Error(
        "LiteBoxSandbox.mountSessionOutputs: outputsRoot not provided (constructor) " +
        "and FILES_BLOB_DIR env var not set — set one of them so mounts have " +
        "a host directory to bind",
      );
    }
    if (!this.opts.outputsRoot && process.env.FILES_BLOB_DIR) {
      this.logger.warn(
        "LiteBoxSandbox.mountSessionOutputs falling back to FILES_BLOB_DIR env var; " +
        "wire ctx.outputsRoot through SandboxFactoryContext to remove the warning",
      );
    }
    const hostPath = join(outputsRoot, opts.tenantId, opts.sessionId);
    mkdirSync(hostPath, { recursive: true });
    this.volumes.push({ hostPath, guestPath: "/mnt/session/outputs", readOnly: false });
  }

  async readFile(path: string): Promise<string> {
    const box = await this.ensureBox();
    const tmp = join(this.tmpRoot, `read-${Buffer.from(randomBytes(6)).toString("hex")}`);
    try {
      await box.copyOut(this.normalise(path), tmp);
      return await fs.readFile(tmp, "utf8");
    } finally {
      await removeBestEffort(tmp);
    }
  }

  async readFileBytes(path: string): Promise<Uint8Array> {
    const box = await this.ensureBox();
    const tmp = join(this.tmpRoot, `read-${Buffer.from(randomBytes(6)).toString("hex")}`);
    try {
      await box.copyOut(this.normalise(path), tmp);
      const buf = await fs.readFile(tmp);
      return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
    } finally {
      await removeBestEffort(tmp);
    }
  }

  async writeFile(path: string, content: string): Promise<string> {
    const box = await this.ensureBox();
    const tmp = join(this.tmpRoot, `write-${Buffer.from(randomBytes(6)).toString("hex")}`);
    const target = this.normalise(path);
    await fs.writeFile(tmp, content, "utf8");
    try {
      await this.copyIntoBox(box, tmp, target);
    } finally {
      await removeBestEffort(tmp);
    }
    return target;
  }

  async writeFileBytes(path: string, bytes: Uint8Array): Promise<string> {
    const box = await this.ensureBox();
    const tmp = join(this.tmpRoot, `write-${Buffer.from(randomBytes(6)).toString("hex")}`);
    const target = this.normalise(path);
    await fs.writeFile(tmp, bytes);
    try {
      await this.copyIntoBox(box, tmp, target);
    } finally {
      await removeBestEffort(tmp);
    }
    return target;
  }

  async destroy(): Promise<void> {
    if (this.boxPromise) {
      try {
        const box = await this.boxPromise;
        await box.stop();
      } catch (err) {
        this.logger.warn(`destroy stop failed: ${(err as Error).message}`);
      } finally {
        this.boxPromise = null;
      }
    }
    await removeBestEffort(this.tmpRoot, true);
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private pendingCaUpload: { hostPath: string; guestPath: string } | null = null;

  private async ensureBox(): Promise<LiteBoxInstance> {
    if (this.boxPromise) return this.boxPromise;
    this.boxPromise = (async () => {
      type LiteBoxModule = {
        SimpleBox: new (options: {
          image?: string;
          rootfsPath?: string;
          security?: { jailerEnabled: boolean; seccompEnabled: boolean };
          memoryMib?: number;
          cpus?: number;
          name?: string;
          volumes?: Array<{ hostPath: string; guestPath: string; readOnly?: boolean }>;
          network?: { mode: "enabled" | "disabled"; allowNet?: string[] };
        }) => LiteBoxInstance;
      };
      const mod = (await import(
        /* @vite-ignore */ "@boxlite-ai/boxlite" as string,
      ).catch((err) => {
        throw new Error(
          `LiteBoxSandbox: failed to load '@boxlite-ai/boxlite' — ` +
          `pnpm add @boxlite-ai/boxlite (cause: ${String(err)})`,
        );
      })) as LiteBoxModule;
      this.logger.log(`creating box (image=${this.opts.image ?? "node:22-bookworm"})`);
      const boxOptions: RetainedBoxOptions = {
        image: this.opts.image ?? "node:22-bookworm",
        rootfsPath: this.opts.rootfsPath,
        security: { jailerEnabled: false, seccompEnabled: false },
        memoryMib: this.opts.memoryMib,
        cpus: this.opts.cpus,
        name: this.opts.name,
        volumes: this.volumes,
        network: this.opts.tankNetwork ?? { mode: "enabled" },
      };
      const box = this.retainedBoxFactory
        ? await this.retainedBoxFactory(boxOptions) : new mod.SimpleBox(boxOptions);
      try {
      const workspace = await box.exec("mkdir", ["-p", "/workspace"]);
      if (workspace.exitCode !== 0) {
        throw new Error(
          `LiteBoxSandbox could not initialize /workspace: ${workspace.stderr || workspace.stdout}`,
        );
      }
      // Apply pending CA cert upload now that the box exists (a no-op exec
      // forces lazy creation per BoxLite's contract).
      if (this.pendingCaUpload) {
        const { hostPath, guestPath } = this.pendingCaUpload;
        try {
          await box.copyIn(hostPath, guestPath);
        } catch (err) {
          this.logger.warn(
            `vault CA cert upload failed (${(err as Error).message}); ` +
            `outbound TLS through oma-vault will fail with cert errors`,
          );
        }
      }
      return box;
      } catch (error) {
        try { await box.stop(); }
        catch (cleanupError) { this.logger.warn(`failed sandbox cleanup: ${(cleanupError as Error).message}`); }
        throw error;
      }
    })().catch((error) => {
      this.boxPromise = null;
      throw error;
    });
    return this.boxPromise;
  }

  /**
   * /workspace/foo → /workspace/foo (BoxLite has no opinion on workdir
   * by default; the harness assumes /workspace exists). We don't rewrite
   * paths the way LocalSubprocess does because BoxLite IS isolated, and
   * /workspace is just a regular dir inside the VM's rootfs.
   */
  private normalise(p: string): string {
    if (p.startsWith("/")) return p;
    return `/workspace/${p}`;
  }

  private buildEnv(command: string): Record<string, string> {
    const out: Record<string, string> = { ...this.envVars };
    for (const { prefix, secrets } of this.commandSecrets) {
      if (command.startsWith(prefix)) Object.assign(out, secrets);
    }
    return out;
  }

  private async copyIntoBox(
    box: LiteBoxInstance,
    hostPath: string,
    target: string,
  ): Promise<void> {
    const prepared = await box.exec("mkdir", ["-p", dirname(target)]);
    if (prepared.exitCode !== 0) {
      throw new Error(
        `LiteBoxSandbox could not create ${dirname(target)}: ${prepared.stderr || prepared.stdout}`,
      );
    }
    await box.copyIn(hostPath, target);
  }
}

async function removeBestEffort(path: string, recursive = false): Promise<void> {
  try {
    await fs.rm(path, { recursive, force: true });
  } catch {
    // Scratch cleanup must never mask the sandbox operation's outcome.
  }
}

// ── Factory (DIP entry point) ───────────────────────────────────────

export const sandboxFactory: SandboxFactory = async (ctx, env) => {
  const replay = env.TANK_SANDBOX_REPLAY === "1";
  const allowed = env.TANK_SANDBOX_ALLOWED_NET === "unrestricted" ? undefined : JSON.parse(env.TANK_SANDBOX_ALLOWED_NET ?? "[]") as string[];
  if (replay && (!allowed || allowed.some(host => host !== "postgres"))) throw new Error("Replay requires a database-only allowlist");
  if (replay && allowed?.includes("postgres")) {
    const address = env.TANK_SANDBOX_DATABASE_IPV4;
    const octets = address?.split(".").map(Number) ?? [];
    if (!address || isIP(address) !== 4 || !(octets[0] === 10
        || (octets[0] === 172 && octets[1]! >= 16 && octets[1]! <= 31)
        || (octets[0] === 192 && octets[1] === 168))) {
      throw new Error("Replay database must resolve to a private IPv4 service address");
    }
    allowed.splice(0, allowed.length, address);
  }
  const replayEvidence = replay ? await verifyReplayEvidence({
    root: env.TANK_REPLAY_EVIDENCE_ROOT,
    runId: env.TANK_REPLAY_RUN_ID,
    manifestSha256: env.TANK_REPLAY_EVIDENCE_SHA256,
    cutoff: env.TANK_REPLAY_CUTOFF,
  }) : undefined;
  const sandbox = new LiteBoxSandbox({
    replayEvidence,
    tankNetwork: replay && !allowed?.length ? { mode: "disabled" } : { mode: "enabled", ...(allowed ? { allowNet: allowed } : {}) },
    image: env.SANDBOX_IMAGE,
    rootfsPath: env.SANDBOX_ROOTFS_PATH,
    memoryMib: env.LITEBOX_MEMORY_MIB ? Number(env.LITEBOX_MEMORY_MIB) : undefined,
    cpus: env.LITEBOX_CPUS ? Number(env.LITEBOX_CPUS) : undefined,
    name: `oma-${ctx.sessionId}`,
    memoryRoot: ctx.memoryRoot,
    outputsRoot: ctx.outputsRoot,
    scratchRoot: ctx.workdir,
  });
  const vars: Record<string, string> = {};
  const names = replay ? ["TANK_DB_URL"] : ["TANK_DB_URL", "OPENROUTER_API_KEY", "TANK_JEV_PROVIDER", "ETHERSCAN_API_KEY"];
  for (const name of names) {
    const key = name === "TANK_DB_URL" ? (replay ? "TANK_SANDBOX_REPLAY_DB_URL" : "TANK_SANDBOX_DB_URL") : name === "TANK_JEV_PROVIDER" ? "TANK_SANDBOX_JEV_PROVIDER" : "TANK_SANDBOX_" + name;
    if (env[key]) vars[name] = env[key]!;
  }
  if (env.TANK_SANDBOX_ALLOWED_NET !== undefined && !vars.TANK_DB_URL) throw new Error("Tank sandbox read-only database URL is not configured");
  if (replayEvidence) {
    vars.TANK_REPLAY_EVIDENCE_ROOT = replayEvidence.guestPath;
    vars.TANK_REPLAY_RUN_ID = env.TANK_REPLAY_RUN_ID!;
  }
  await sandbox.setEnvVars(vars);
  return sandbox;
};

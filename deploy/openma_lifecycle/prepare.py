"""Apply retained lifecycle to a NEW workcopy of an already prepared OpenMA tree.

Never mutates its source checkout or starts a VM/service. Final deployment owner
must qualify the selected native runtime and run the resulting integration tests.
"""

import argparse
import hashlib
import json
import shutil
from pathlib import Path

BASE = Path(__file__).resolve().parent
PIN = "85d5361f535c7221de975838dc48444e468cc02e"


def refresh_composite_manifest(source, destination, identity):
    """Embed final hashes while retaining the exact prepared-base stage provenance."""
    source, destination = Path(source), Path(destination)
    raw = (source / "tank-overlay.json").read_bytes()
    base = json.loads(raw)
    composite = dict(base)
    composite["manifest_stage"] = "retained-lifecycle-composite"
    composite["base_stage_provenance"] = {
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "manifest": base,
    }
    patched = set(base.get("patched_file_hashes", {})) | {
        "packages/control-plane-node/src/lib/node-managed-session-runner.ts",
        "packages/control-plane-node/src/modules/node-managed.ts",
        "packages/control-plane-node/package.json",
        "pnpm-lock.yaml",
    }
    added = set(base.get("added_file_hashes", {})) | {
        "packages/control-plane-node/src/lib/tank-retained/" + name
        for name in [
            "retained-session.ts",
            "boxlite-provider.ts",
            "node-lifecycle.ts",
            "replay-attachment.ts",
        ]
    }
    for label, paths in [("patched_file_hashes", patched), ("added_file_hashes", added)]:
        composite[label] = {
            path: hashlib.sha256((destination / path).read_bytes()).hexdigest() for path in sorted(paths)
        }
    composite["dockerfile_sha256"] = hashlib.sha256(
        (destination / "apps/main-node/Dockerfile.tank").read_bytes()
    ).hexdigest()
    composite["retained_lifecycle"] = {
        "runtime_identity": identity,
        "native_vm_proof": "NOT_RUN",
        "production_adoption": False,
    }
    (destination / "tank-overlay.json").write_text(json.dumps(composite, indent=2) + "\n")
    return composite


def prepare(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if destination.exists():
        raise ValueError("Destination must be a new isolated workcopy")
    if source == destination or source in destination.parents:
        raise ValueError("Destination must be outside the source tree")
    expected = json.loads((BASE / "source-guards.json").read_text())
    for path, digest in expected.items():
        if hashlib.sha256((source / path).read_bytes()).hexdigest() != digest:
            raise ValueError("Pinned lifecycle primitive source changed: " + path)
    if not (source / "tank-deploy/boxlite-runtime/provenance.json").exists():
        raise ValueError("A prepared native runtime provenance manifest is required")
    original = {
        name: hashlib.sha256((source / name).read_bytes()).hexdigest()
        for name in [
            "packages/sandbox-adapter-litebox/src/litebox.ts",
            "packages/control-plane-node/src/lib/node-managed-session-runner.ts",
            "packages/control-plane-node/src/modules/node-managed.ts",
            "packages/sandbox-adapter-litebox/src/tank-replay-evidence.ts",
            "packages/sandbox-adapter-litebox/src/index.ts",
        ]
    }
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(
            ".git", "node_modules", ".env", ".env.*", "data", "secrets", "auth.json", "*.log"
        ),
    )

    def edit(relative, old, new):
        path = destination / relative
        text = path.read_text()
        if text.count(old) != 1:
            raise ValueError("Lifecycle patch context changed: " + relative)
        path.write_text(text.replace(old, new))

    target = destination / "packages/control-plane-node/src/lib/tank-retained"
    target.mkdir()
    for name in ["retained-session.ts", "boxlite-provider.ts", "node-lifecycle.ts", "replay-attachment.ts"]:
        (target / name).write_text(
            (BASE / name)
            .read_text()
            .replace("./retained-session.ts'", "./retained-session.js'")
            .replace("./boxlite-provider.ts'", "./boxlite-provider.js'")
        )
    identity = hashlib.sha256(
        json.dumps(
            {
                "openma": PIN,
                "sources": original,
                "dependency_lock": hashlib.sha256((source / "pnpm-lock.yaml").read_bytes()).hexdigest(),
                "dockerfile": hashlib.sha256(
                    (source / "apps/main-node/Dockerfile.tank").read_bytes()
                ).hexdigest(),
                "native_provenance": json.loads(
                    (source / "tank-deploy/boxlite-runtime/provenance.json").read_text()
                ),
                "native_sums": (source / "tank-deploy/boxlite-runtime/SHA256SUMS").read_text(),
                "assets": {
                    name: hashlib.sha256((BASE / name).read_bytes()).hexdigest()
                    for name in [
                        "retained-session.ts",
                        "boxlite-provider.ts",
                        "node-lifecycle.ts",
                        "replay-attachment.ts",
                        "prepare.py",
                        "source-guards.json",
                    ]
                },
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()

    lite = "packages/sandbox-adapter-litebox/src/litebox.ts"
    edit(
        lite,
        "export class LiteBoxSandbox implements SandboxExecutor {",
        """interface RetainedBoxOptions {
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
  }""",
    )
    edit(lite, "      const box = new mod.SimpleBox({", "      const boxOptions: RetainedBoxOptions = {")
    edit(
        lite,
        '        network: this.opts.tankNetwork ?? { mode: "enabled" },\n      });',
        """        network: this.opts.tankNetwork ?? { mode: "enabled" },
      };
      const box = this.retainedBoxFactory
        ? await this.retainedBoxFactory(boxOptions) : new mod.SimpleBox(boxOptions);""",
    )

    runner = "packages/control-plane-node/src/lib/node-managed-session-runner.ts"
    edit(
        runner,
        "  buildSandbox(input: ManagedRunnerContext & { signal?: AbortSignal }): Promise<SandboxExecutor>;",
        """  buildSandbox(input: ManagedRunnerContext & { signal?: AbortSignal }): Promise<SandboxExecutor>;
  retainedLifecycle?: {
    bind(input: ManagedRunnerContext & { sandbox: SandboxExecutor; executionFence?: SessionExecutionFence; signal?: AbortSignal }): Promise<void>;
    idle(input: ManagedRunnerContext): Promise<void>;
    abandon(input: ManagedRunnerContext): Promise<void>;
    archive(input: StopNodeManagedSessionRuntime): Promise<void>;
  };""",
    )
    edit(
        runner,
        "  private readonly sandboxRuntimes =",
        "  private readonly activeAccepts = new ScopedSessionMap<Promise<void>>();\n  private readonly sandboxRuntimes =",
    )
    edit(
        runner,
        "      if (reusable) return;",
        """      if (reusable) {
        await this.dependencies.retainedLifecycle?.bind({ ...contextForRetention(input), sandbox: existing!, executionFence: input.executionFence, signal });
        return;
      }""",
    )
    edit(
        runner,
        "      let workspace: ManagedWorkspaceState | undefined;\n      try {",
        """      let workspace: ManagedWorkspaceState | undefined;
      try {
        await this.dependencies.retainedLifecycle?.bind({ ...context, sandbox, executionFence: input.executionFence, signal });""",
    )
    edit(
        runner,
        "  async stop(input: StopNodeManagedSessionRuntime): Promise<void> {\n    await this.discardSandbox(input);\n  }",
        """  async stop(input: StopNodeManagedSessionRuntime): Promise<void> {
    this.cancel(input);
    await this.activeAccepts.get(input)?.catch(() => undefined);
    await this.dependencies.retainedLifecycle?.archive(input);
    await this.discardSandbox(input);
  }""",
    )
    edit(
        runner,
        "  async accept(input: NodeManagedSessionRunnerAcceptInput): Promise<void> {",
        """  async accept(input: NodeManagedSessionRunnerAcceptInput): Promise<void> {
    if (input.events.some(event => event.type === "user.interrupt")) return this.acceptTurn(input);
    if (this.activeAccepts.has(input)) throw new Error("Concurrent retained Session execution");
    const active = this.acceptTurn(input);
    this.activeAccepts.set(input, active);
    try { await active; } finally { this.activeAccepts.delete(input); }
  }
  private async acceptTurn(input: NodeManagedSessionRunnerAcceptInput): Promise<void> {""",
    )
    edit(
        runner,
        """        if (this.abortControllers.get(input) === abortController) {
          this.abortControllers.delete(input);
        }""",
        """        try {
          // This finally also runs when draining/publication fails. Ownership
          // loss never leaves the old retained resource heartbeat renewing.
          if (abortController.signal.reason === "lease_lost") await this.dependencies.retainedLifecycle?.abandon(input);
          else await this.dependencies.retainedLifecycle?.idle(input);
        } finally {
          if (this.abortControllers.get(input) === abortController) {
            this.abortControllers.delete(input);
          }
        }""",
    )
    edit(
        runner,
        "interface ManagedRunnerContext {",
        """function contextForRetention(input: StartNodeManagedSessionRuntime) {
  return { workspaceId: input.workspaceId, session: input.session, environment: input.environment };
}
interface ManagedRunnerContext {""",
    )

    managed = "packages/control-plane-node/src/modules/node-managed.ts"
    edit(
        managed,
        'import { rm } from "node:fs/promises";',
        """import { rm, readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { ensureRuntimeResourceFenceSchema, SqlRuntimeResourceFencePort, SqlRuntimeOrphanPort } from "@open-managed-agents/runtime-resource-fence-sql";
import * as retainedBoxLiteProvider from "@open-managed-agents/managed-runtime-boxlite";
import { verifyReplayEvidence } from "@open-managed-agents/sandbox-adapter-litebox";
import { validateReplayAttachment } from "../lib/tank-retained/replay-attachment.js";
import { NodeRetainedLifecycle, type AttachableSandbox } from "../lib/tank-retained/node-lifecycle.js";""",
    )
    edit(
        managed,
        '  if (config.workspace.strategy === "checkpoint_restore") {\n    await ensureSessionExecutionClaimLockSchema(sql, foundation.dialect);\n  }',
        "  await ensureSessionExecutionClaimLockSchema(sql, foundation.dialect);",
    )
    edit(
        managed,
        '    serializeSessionClaims: config.workspace.strategy === "checkpoint_restore",',
        "    serializeSessionClaims: true,",
    )
    assembly = """  await ensureRuntimeResourceFenceSchema(sql);
  const retainedSdk = await import(/* @vite-ignore */ "@boxlite-ai/boxlite" as string) as { JsBoxlite: { withDefaultConfig(): unknown } };
  const stableDigest = (value: unknown): string => {
    const normalize = (item: unknown): unknown => Array.isArray(item) ? item.map(normalize)
      : item !== null && typeof item === "object" ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)).map(([name, child]) => [name, normalize(child)])) : item;
    return "sha256:" + createHash("sha256").update(JSON.stringify(normalize(value))).digest("hex");
  };
  const retainedLifecycle = new NodeRetainedLifecycle({
    fences: new SqlRuntimeResourceFencePort(sql), orphans: new SqlRuntimeOrphanPort(sql), providerModule: retainedBoxLiteProvider,
    client: retainedSdk.JsBoxlite.withDefaultConfig(), ownerId: config.execution.ownerId, leaseTtlMs: 30_000,
    assertExecutionCurrent: async (candidate) => {
      if (!candidate || !await isManagedSessionExecutionFenceActive(candidate as Parameters<typeof isManagedSessionExecutionFenceActive>[0])) throw new Error("Retained Session execution fence lost");
    },
    fingerprint: async (context, allocation) => {
      const domain = context as typeof context & { session: { resources: unknown; agent: { skills: unknown }; metadata: Record<string, string> }; environment: { config: unknown; name: string } };
      await validateReplayAttachment(domain, allocation, verifyReplayEvidence, process.env.TANK_REPLAY_EVIDENCE_ROOT);
      const rootfsIndex = allocation.rootfsPath ? stableDigest(await readFile(join(allocation.rootfsPath, "index.json"), "utf8")) : null;
      const databaseUrl = domain.environment.name === "tank-replay" ? process.env.TANK_SANDBOX_REPLAY_DB_URL : process.env.TANK_SANDBOX_DB_URL;
      const database = databaseUrl ? new URL(databaseUrl) : null;
      return stableDigest({ runtimeIdentity: "IDENTITY_PLACEHOLDER", scope: [context.workspaceId, context.environment.id, context.sessionId],
        resources: domain.session.resources, skills: domain.session.agent.skills, environmentName: domain.environment.name, environment: domain.environment.config, allocation, rootfsIndex,
        databasePolicy: database ? { host: database.hostname, port: database.port, database: database.pathname, role: database.username } : null });
    },
  });
  await retainedLifecycle.reconcileOrphans();
  const managedRuntimeRunner = new DefaultNodeManagedSessionRunner({
    retainedLifecycle: {
      bind: input => {
        if (!("setRetainedBoxFactory" in input.sandbox)) throw new Error("Retained lifecycle requires the LiteBox adapter");
        return retainedLifecycle.bind({ ...input, sessionId: input.session.id, sandbox: input.sandbox as typeof input.sandbox & AttachableSandbox, executionFence: input.executionFence });
      },
      idle: input => retainedLifecycle.idle({ ...input, sessionId: input.session.id }),
      abandon: input => retainedLifecycle.abandon({ ...input, sessionId: input.session.id }),
      archive: input => retainedLifecycle.archive(input),
    },""".replace("IDENTITY_PLACEHOLDER", identity)
    edit(managed, "  const managedRuntimeRunner = new DefaultNodeManagedSessionRunner({", assembly)
    edit(
        managed,
        '  disposables.add("managed_platform", () => managedPlatform.stopAll());',
        """  disposables.add("managed_platform", () => managedPlatform.stopAll());
  disposables.add("tank_retained_sessions", async () => {
    managedSessionExecutionWorker.stop();
    await managedSessionExecutionWorker.waitForIdle();
    await retainedLifecycle.shutdown();
  });""",
    )

    package_path = destination / "packages/control-plane-node/package.json"
    package = json.loads(package_path.read_text())
    additions = ["managed-runtime-boxlite", "runtime-resource-fence-sql", "sandbox-adapter-litebox"]
    for name in additions:
        package["dependencies"]["@open-managed-agents/" + name] = "workspace:*"
    package["dependencies"]["@boxlite-ai/boxlite"] = "0.10.6"
    package["dependencies"] = dict(sorted(package["dependencies"].items()))
    package_path.write_text(json.dumps(package, indent=2) + "\n")
    lock = destination / "pnpm-lock.yaml"
    text = lock.read_text()
    head, section = text.split("\n  packages/control-plane-node:\n", 1)
    importer, tail = section.split("\n  packages/", 1)
    sdk_snapshot = "  '@boxlite-ai/boxlite@0.10.6(playwright-core@1.59.1)':"
    if text.count(sdk_snapshot) != 1:
        raise ValueError("Expected exactly one existing pinned SDK0.10.6 snapshot")
    if "      '@boxlite-ai/boxlite':" in importer:
        raise ValueError("Unexpected existing control-plane SDK dependency")
    importer = importer.replace(
        "    dependencies:\n",
        "    dependencies:\n      '@boxlite-ai/boxlite':\n"
        "        specifier: 0.10.6\n        version: 0.10.6(playwright-core@1.59.1)\n",
        1,
    )
    for name in additions:
        label = "@open-managed-agents/" + name
        if "      '" + label + "':" not in importer:
            importer = importer.replace(
                "    dependencies:\n",
                "    dependencies:\n      '"
                + label
                + "':\n        specifier: workspace:*\n        version: link:../"
                + name
                + "\n",
                1,
            )
    lock.write_text(head + "\n  packages/control-plane-node:\n" + importer + "\n  packages/" + tail)
    result = {
        "upstream_commit": PIN,
        "integration": "isolated-retained-session-candidate",
        "runtime_identity": identity,
        "source_sha256": original,
        "native_vm_proof": "NOT_RUN",
        "production_integration": "NOT_ADOPTED",
    }
    composite = refresh_composite_manifest(source, destination, identity)
    result["composite_manifest"] = {
        "path": "tank-overlay.json",
        "sha256": hashlib.sha256((destination / "tank-overlay.json").read_bytes()).hexdigest(),
        "base_stage_sha256": composite["base_stage_provenance"]["manifest_sha256"],
        "final_source_count": len(composite["patched_file_hashes"]) + len(composite["added_file_hashes"]),
    }
    (destination / "tank-retained-candidate.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.source, args.destination), indent=2))

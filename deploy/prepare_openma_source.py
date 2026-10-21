#!/usr/bin/env python3
"""Make an auditable pinned OpenMA source overlay; no live service/model calls."""

import argparse
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

PIN = "85d5361f535c7221de975838dc48444e468cc02e"
BASE = Path(__file__).resolve().parent


def prepare(source, destination, boxlite_runtime=None):
    source = Path(source).resolve()
    destination = Path(destination).resolve()
    commit = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if commit != PIN:
        raise ValueError("OpenMA checkout does not match pinned commit")
    hashes = json.loads((BASE / "openma-source-hashes.json").read_text())
    for relative, expected in hashes.items():
        if hashlib.sha256((source / relative).read_bytes()).hexdigest() != expected:
            raise ValueError("OpenMA source hash mismatch: " + relative)
    if boxlite_runtime is None:
        raise ValueError("A verified dynamic Linux BoxLite runtime bundle is required; see deploy/boxlite")
    boxlite_runtime = Path(boxlite_runtime).resolve()
    subprocess.run(
        [sys.executable, str(BASE / "prepare_boxlite_runtime.py"), str(boxlite_runtime), "--verify"],
        check=True,
        capture_output=True,
        text=True,
    )
    runtime_metadata = json.loads((boxlite_runtime / "provenance.json").read_text())
    runtime_version = runtime_metadata.get("boxlite_version") or runtime_metadata.get("source", {}).get(
        "version"
    )
    sdk_patch = None
    if runtime_version == "0.10.6":
        sdk_base = BASE / "boxlite/v0.10.6"
        sdk_patch = json.loads((sdk_base / "openma-sdk-patch.json").read_text())
        for relative, expected in sdk_patch["source_sha256"].items():
            if hashes.get(relative) != expected:
                raise ValueError("SDK dependency source guard mismatch: " + relative)
        if (
            hashlib.sha256((sdk_base / sdk_patch["lockfile_asset"]).read_bytes()).hexdigest()
            != sdk_patch["lockfile_sha256"]
        ):
            raise ValueError("SDK dependency lock asset hash mismatch")
    if destination.exists():
        raise ValueError("Destination must be new; do not overwrite a checkout")
    shutil.copytree(
        source, destination, ignore=shutil.ignore_patterns(".git", "node_modules", ".env", ".env.*", "data")
    )
    if sdk_patch is not None:
        workspace = destination / "pnpm-workspace.yaml"
        workspace.write_text(workspace.read_text() + sdk_patch["workspace_append"])
        shutil.copyfile(sdk_base / sdk_patch["lockfile_asset"], destination / "pnpm-lock.yaml")

    subprocess.run(
        [sys.executable, str(BASE / "openma_skills/prepare_overlay.py"), str(destination)],
        check=True,
    )

    def edit(relative, old, new):
        path = destination / relative
        data = path.read_text()
        if data.count(old) != 1:
            raise ValueError("Patch context mismatch: " + relative)
        path.write_text(data.replace(old, new))

    foundation = "packages/control-plane-node/src/modules/node-foundation.ts"
    edit(
        foundation,
        "    workdir: string,\n  ): Promise<import",
        "    workdir: string,\n    tankOverrides: Record<string, string> = {},\n  ): Promise<import",
    )
    edit(
        foundation,
        "      sandboxEnvironment,\n    );",
        "      { ...sandboxEnvironment, ...tankOverrides },\n    );",
    )
    edit(
        "packages/control-plane-node/src/registry.ts",
        "buildSandbox(sessionId: string, workdir: string): Promise<SandboxPort>;",
        "buildSandbox(sessionId: string, workdir: string, tankOverrides?: Record<string, string>): Promise<SandboxPort>;",
    )
    managed = "packages/control-plane-node/src/modules/node-managed.ts"
    edit(
        managed,
        "buildSandbox: async ({ session }) => buildSandbox(\n        session.id,\n        join(config.paths.sandboxWorkdir, session.id),\n      ),",
        """buildSandbox: async ({ session, environment }) => {
      const replay = environment.name === "tank-replay";
      const networking = environment.config.type === "cloud" ? environment.config.networking : undefined;
      if (replay && (networking?.type !== "limited" || (networking.allowedHosts ?? []).some(h => h !== "postgres"))) throw new Error("Tank replay requires database-only networking");
      return buildSandbox(session.id, join(config.paths.sandboxWorkdir, session.id), {
        TANK_SANDBOX_REPLAY: replay ? "1" : "0",
        TANK_SANDBOX_ALLOWED_NET: replay ? JSON.stringify(networking?.type === "limited" ? networking.allowedHosts ?? [] : []) : "unrestricted",
        TANK_REPLAY_RUN_ID: replay ? session.metadata?.tank_replay_run_id ?? "" : "",
        TANK_REPLAY_EVIDENCE_SHA256: replay ? session.metadata?.tank_replay_evidence_sha256 ?? "" : "",
        TANK_REPLAY_CUTOFF: replay ? session.metadata?.tank_replay_cutoff ?? "" : "",
      });
    },""",
    )
    lite = "packages/sandbox-adapter-litebox/src/litebox.ts"
    edit(
        lite,
        'import type { ProcessHandle, SandboxExecutor, SandboxFactory } from "@open-managed-agents/sandbox";',
        'import { verifyReplayEvidence } from "./tank-replay-evidence";\nimport type { ProcessHandle, SandboxExecutor, SandboxFactory } from "@open-managed-agents/sandbox";',
    )
    edit(
        lite,
        "  image?: string;\n  /** Optional VM resource limits. */",
        '  image?: string;\n  tankNetwork?: { mode: "enabled" | "disabled"; allowNet?: string[] };\n  replayEvidence?: { hostPath: string; guestPath: "/replay/evidence"; readOnly: true };\n  /** Optional VM resource limits. */',
    )
    edit(
        lite,
        '      network: { mode: "enabled" },',
        '      network: this.opts.tankNetwork ?? { mode: "enabled" },',
    )
    edit(
        lite,
        "    mkdirSync(this.tmpRoot, { recursive: true });",
        "    mkdirSync(this.tmpRoot, { recursive: true });\n    if (opts.replayEvidence) this.volumes.push(opts.replayEvidence);",
    )
    edit(
        lite,
        "  return new LiteBoxSandbox({\n    image: env.SANDBOX_IMAGE,",
        """  const replay = env.TANK_SANDBOX_REPLAY === "1";
  const allowed = env.TANK_SANDBOX_ALLOWED_NET === "unrestricted" ? undefined : JSON.parse(env.TANK_SANDBOX_ALLOWED_NET ?? "[]") as string[];
  if (replay && (!allowed || allowed.some(host => host !== "postgres"))) throw new Error("Replay requires a database-only allowlist");
  if (replay && allowed?.includes("postgres")) {
    if (!env.TANK_SANDBOX_DATABASE_IPV4) throw new Error("Replay database address is not resolved");
    allowed.push(env.TANK_SANDBOX_DATABASE_IPV4);
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
    image: env.SANDBOX_IMAGE,""",
    )
    edit(
        lite,
        "    scratchRoot: ctx.workdir,\n  });\n};",
        """    scratchRoot: ctx.workdir,
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
    // Current prefix hash/cutoff are verified native metadata, not static VM
    // configuration: the same run mount advances with ReplayClock. Scripts
    // read its current manifest, and retained allocations keep the same identity.
  }
  await sandbox.setEnvVars(vars);
  return sandbox;
};""",
    )
    edit(
        lite,
        '      const workspace = await box.exec("mkdir", ["-p", "/workspace"]);',
        '      try {\n      const workspace = await box.exec("mkdir", ["-p", "/workspace"]);',
    )
    edit(
        lite,
        "      return box;\n    })();\n    return this.boxPromise;",
        """      return box;
      } catch (error) {
        try { await box.stop(); }
        catch (cleanupError) { this.logger.warn(`failed sandbox cleanup: ${(cleanupError as Error).message}`); }
        throw error;
      }
    })().catch((error) => {
      this.boxPromise = null;
      throw error;
    });
    return this.boxPromise;""",
    )
    coordination = destination / "packages/session-runtime-sql/src/coordination.ts"
    sql = coordination.read_text()
    if sql.count("? IS NULL") != 6:
        raise ValueError("Coordination nullable SQL patch context changed")
    coordination.write_text(sql.replace("? IS NULL", "CAST(? AS VARCHAR(191)) IS NULL"))
    for relative, count in [
        ("packages/session-event-store-sql/src/index.ts", 3),
        ("packages/control-plane-node/src/lib/node-managed-workspace-checkpoints.ts", 1),
    ]:
        path = destination / relative
        sql = path.read_text()
        if sql.count("? IS NULL") != count:
            raise ValueError("Nullable guard SQL context changed: " + relative)
        path.write_text(sql.replace("? IS NULL", "CAST(? AS VARCHAR(191)) IS NULL"))
    for fix in json.loads((BASE / "openma-pg-time-fixes.json").read_text()):
        path = destination / fix["path"]
        data = path.read_text()
        if data.count(fix["old"]) != fix["count"]:
            raise ValueError("Timestamp patch context changed: " + fix["path"])
        path.write_text(data.replace(fix["old"], fix["new"]))
    shutil.copy(
        BASE / "openma-tank-network.test.ts",
        destination / "packages/sandbox-adapter-litebox/test/tank-network.test.ts",
    )
    for fix in json.loads((BASE / "openma-tool-config-fixes.json").read_text()):
        path = destination / fix["path"]
        data = path.read_text()
        if data.count(fix["old"]) != fix["count"]:
            raise ValueError("Tool config patch context changed: " + fix["path"])
        path.write_text(data.replace(fix["old"], fix["new"]))
    for fix in json.loads((BASE / "openma-opaque-input-fixes.json").read_text()):
        if hashes.get(fix["path"]) != fix["sha256"]:
            raise ValueError("Opaque payload patch source guard mismatch")
        path = destination / fix["path"]
        data = path.read_text()
        if data.count(fix["old"]) != fix["count"]:
            raise ValueError("Opaque payload patch context changed: " + fix["path"])
        path.write_text(data.replace(fix["old"], fix["new"]))
    for fix in json.loads((BASE / "openma_browser/overlay.json").read_text()):
        path = destination / fix["path"]
        data = path.read_text()
        if data.count(fix["old"]) != fix["count"]:
            raise ValueError("Sandbox browser patch context changed: " + fix["path"])
        path.write_text(data.replace(fix["old"], fix["new"]))
    for asset, relative in [
        ("sandbox-browser.ts", "apps/agent/src/harness/sandbox-browser.ts"),
        ("sandbox-browser.test.ts", "packages/browser-harness/test/tank-sandbox.test.ts"),
    ]:
        path = destination / relative
        if path.exists():
            raise ValueError("Unexpected existing sandbox browser file: " + relative)
        shutil.copy(BASE / "openma_browser" / asset, path)
    replay_helper = destination / "packages/sandbox-adapter-litebox/src/tank-replay-evidence.ts"
    if replay_helper.exists():
        raise ValueError("Unexpected existing replay evidence helper")
    shutil.copy(BASE / "openma_replay/replay-evidence.ts", replay_helper)
    edit(
        "packages/sandbox-adapter-litebox/src/index.ts",
        'export * from "./litebox";\n',
        'export * from "./litebox";\nexport { verifyReplayEvidence } from "./tank-replay-evidence";\n',
    )
    edit(
        "packages/sandbox-adapter-litebox/test/litebox.test.ts",
        '      image: "node:22-bookworm",\n      memoryMib: undefined,',
        '      image: "node:22-bookworm",\n      rootfsPath: undefined,\n      security: { jailerEnabled: false, seccompEnabled: false },\n      memoryMib: undefined,',
    )
    edit(lite, "  image?: string;\n  tankNetwork", "  image?: string;\n  rootfsPath?: string;\n  tankNetwork")
    edit(lite, "          image?: string;", "          image?: string;\n          rootfsPath?: string;")
    edit(
        lite,
        '        image: this.opts.image ?? "node:22-bookworm",',
        '        image: this.opts.image ?? "node:22-bookworm",\n        rootfsPath: this.opts.rootfsPath,',
    )
    edit(
        lite,
        "    image: env.SANDBOX_IMAGE,",
        "    image: env.SANDBOX_IMAGE,\n    rootfsPath: env.SANDBOX_ROOTFS_PATH,",
    )
    if sdk_patch is not None:
        edit(
            lite,
            "          rootfsPath?: string;",
            "          rootfsPath?: string;\n          security?: { jailerEnabled: boolean; seccompEnabled: boolean };",
        )
        edit(
            lite,
            "        rootfsPath: this.opts.rootfsPath,",
            "        rootfsPath: this.opts.rootfsPath,\n        security: { jailerEnabled: false, seccompEnabled: false },",
        )
    deploy = destination / "tank-deploy"
    deploy.mkdir()
    shutil.copy(BASE / "openma_entrypoint.mjs", deploy / "openma_entrypoint.mjs")
    shutil.copytree(boxlite_runtime, deploy / "boxlite-runtime")
    docker = (destination / "apps/main-node/Dockerfile").read_text()
    docker = docker.replace("FROM node:22-slim", "FROM node:24-bookworm-slim")
    docker = docker.replace(
        "COPY tsconfig.json ./", "COPY tsconfig.json ./\nCOPY tank-deploy /app/tank-deploy"
    )
    docker = docker.replace("AS runtime\n", "AS runtime\nENV COREPACK_HOME=/opt/corepack\n")
    docker = docker.replace(
        "&& corepack enable", "&& corepack enable && corepack prepare pnpm@11.0.8 --activate"
    )
    executables = " ".join(
        "/opt/tank-boxlite-runtime/" + name
        for name in sorted(runtime_metadata["files"])
        if name != "libkrunfw.so.5"
    )
    docker += f"\nCOPY --chown=node:node tank-deploy/boxlite-runtime /opt/tank-boxlite-runtime\nRUN chmod 755 {executables}\nENV BOXLITE_RUNTIME_DIR=/opt/tank-boxlite-runtime LD_LIBRARY_PATH=/opt/tank-boxlite-runtime\n"
    docker += "COPY --chown=node:node tank-overlay.json /app/tank-overlay.json\n"

    docker += "USER root\nRUN install -d -o node -g node /app/replay-evidence\nUSER node\n"

    (destination / "apps/main-node/Dockerfile.tank").write_text(docker)
    (destination / "tank-overlay.json").write_text(
        json.dumps(
            {
                "upstream_commit": PIN,
                "overlay": "tank8-replay-sealed" if sdk_patch else "tank5-startup",
                "boxlite_sdk_version": runtime_version,
                "boxlite_sdk_patch": sdk_patch,
                "boxlite_runtime": runtime_metadata,
                "sandbox_browser": {
                    "engine": "pinned Playwright default Chromium headless shell; no fallback",
                    "worker_sha256": hashlib.sha256(
                        (BASE / "openma_browser/browser-worker.py").read_bytes()
                    ).hexdigest(),
                    "cache_preparer_sha256": hashlib.sha256(
                        (BASE / "openma_browser/prepare-font-cache.py").read_bytes()
                    ).hexdigest(),
                    "image_manifest": "/opt/tank/browser-font-cache.json",
                    "rpc_budget_s": 75,
                    "rpc_response_receipt_budget_s": 80,
                    "sandbox_exec_budget_s": 90,
                },
                "entrypoint_sha256": hashlib.sha256(
                    (deploy / "openma_entrypoint.mjs").read_bytes()
                ).hexdigest(),
                "dockerfile_sha256": hashlib.sha256(docker.encode()).hexdigest(),
                "source_hashes": hashes,
                "patched_file_hashes": {
                    relative: hashlib.sha256((destination / relative).read_bytes()).hexdigest()
                    for relative in hashes
                },
                "added_file_hashes": {
                    relative: hashlib.sha256((destination / relative).read_bytes()).hexdigest()
                    for relative in [
                        "apps/agent/src/harness/sandbox-browser.ts",
                        "packages/browser-harness/test/tank-sandbox.test.ts",
                        "packages/control-plane-node/src/lib/node-managed-skill-manifest.ts",
                        "apps/main-node/test/tank-skill-archive.test.ts",
                        "apps/main-node/test/fixtures/tank-registered-skill.zip",
                        "packages/sandbox-adapter-litebox/src/tank-replay-evidence.ts",
                        "packages/sandbox-adapter-litebox/test/tank-network.test.ts",
                    ]
                },
                "changes": [
                    "PostgreSQL typed nullable guards and bigint epoch boundaries",
                    "opaque tool/application JSON preserved across runtime protocol envelope case mapping",
                    "canonical tool schemas/domain/mappers preserve browser and scheduling controls",
                    "browser tools execute through per-session SandboxPort and VM-local Unix-socket Playwright worker",
                    "network policy passed from selected environment to BoxLite",
                    "native skill descriptors resolve the validated unique manifest directory from compiled ZIPs in every exposure mode",
                    "sandbox Python3.12 image supplied with SANDBOX_IMAGE",
                    "curated read-only DB/API credentials only; replay excludes API keys",
                    "Node24 required by pinned pnpm11, upstream Dockerfile used Node22",
                ],
            },
            indent=2,
        )
        + "\n"
    )
    return destination


def prepare_deployment(source, destination, boxlite_runtime):
    """Canonical operator entrypoint; prepare() above is the guarded base stage."""
    spec = importlib.util.spec_from_file_location(
        "tank_complete_openma_preparation", BASE / "prepare_openma_deployment.py"
    )
    composer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(composer)
    return composer.prepare(source, destination, boxlite_runtime)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("destination")
    parser.add_argument("--boxlite-runtime", required=True, type=Path)
    a = parser.parse_args()
    result = prepare_deployment(a.source, a.destination, a.boxlite_runtime)
    print(json.dumps({"status": result["status"], "runtime_identity": result["runtime_identity"]}))

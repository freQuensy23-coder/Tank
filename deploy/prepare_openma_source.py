#!/usr/bin/env python3
"""Make an auditable pinned OpenMA source overlay; no live service/model calls."""

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

PIN = "85d5361f535c7221de975838dc48444e468cc02e"
BASE = Path(__file__).resolve().parent


def prepare(source, destination):
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
    if destination.exists():
        raise ValueError("Destination must be new; do not overwrite a checkout")
    shutil.copytree(
        source, destination, ignore=shutil.ignore_patterns(".git", "node_modules", ".env", ".env.*", "data")
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
      });
    },""",
    )
    lite = "packages/sandbox-adapter-litebox/src/litebox.ts"
    edit(
        lite,
        "  image?: string;\n  /** Optional VM resource limits. */",
        '  image?: string;\n  tankNetwork?: { mode: "enabled" | "disabled"; allowNet?: string[] };\n  /** Optional VM resource limits. */',
    )
    edit(
        lite,
        '      network: { mode: "enabled" },',
        '      network: this.opts.tankNetwork ?? { mode: "enabled" },',
    )
    edit(
        lite,
        "  return new LiteBoxSandbox({\n    image: env.SANDBOX_IMAGE,",
        """  const replay = env.TANK_SANDBOX_REPLAY === "1";
  const allowed = env.TANK_SANDBOX_ALLOWED_NET === "unrestricted" ? undefined : JSON.parse(env.TANK_SANDBOX_ALLOWED_NET ?? "[]") as string[];
  const sandbox = new LiteBoxSandbox({
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
  await sandbox.setEnvVars(vars);
  return sandbox;
};""",
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
    deploy = destination / "tank-deploy"
    deploy.mkdir()
    shutil.copy(BASE / "openma_entrypoint.mjs", deploy / "openma_entrypoint.mjs")
    docker = (destination / "apps/main-node/Dockerfile").read_text()
    docker = docker.replace("FROM node:22-slim", "FROM node:24-bookworm-slim")
    docker = docker.replace(
        "COPY tsconfig.json ./", "COPY tsconfig.json ./\nCOPY tank-deploy /app/tank-deploy"
    )
    docker = docker.replace("AS runtime\n", "AS runtime\nENV COREPACK_HOME=/opt/corepack\n")
    docker = docker.replace(
        "&& corepack enable", "&& corepack enable && corepack prepare pnpm@11.0.8 --activate"
    )
    # Upstream lock includes boxlite0.9.0 devDependency; full frozen install retains it.
    (destination / "apps/main-node/Dockerfile.tank").write_text(docker)
    (destination / "tank-overlay.json").write_text(
        json.dumps(
            {
                "upstream_commit": PIN,
                "overlay": "tank2-openrouter",
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
                    ]
                },
                "changes": [
                    "PostgreSQL typed nullable guards and bigint epoch boundaries",
                    "canonical tool schemas/domain/mappers preserve browser and scheduling controls",
                    "browser tools execute through per-session SandboxPort and VM-local Unix-socket Playwright worker",
                    "network policy passed from selected environment to BoxLite",
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("destination")
    a = parser.parse_args()
    print(prepare(a.source, a.destination))

import { afterEach, expect, it, vi } from "vitest";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
const roots: string[] = [];
async function replayEvidence() {
  const root = await mkdtemp(join(tmpdir(), "tank-replay-network-"));
  roots.push(root);
  const run = "RPL-network";
  await mkdir(join(root, run));
  const cutoff = "2026-10-18T12:00:00Z";
  const manifest = {
    schema: "tank.replay-evidence.v1",
    run_id: run,
    input_sha256: "a".repeat(64),
    cutoff,
    cutoff_prec: "EXACT",
    cutoff_prec_reason: null,
    guest_root: "/replay/evidence",
    files: [],
    identity_key: ["instrument_id", "trade_id"],
    writer: { library: "pyarrow", version: "test", compression: "zstd" },
    sealed_at: "2026-10-18T13:00:00Z",
    sealed_at_prec: "COLLECTION_ONLY",
    sealed_at_prec_reason: null,
  };
  const bytes = JSON.stringify(manifest) + "\n";
  await writeFile(join(root, run, "manifest.json"), bytes);
  return {
    TANK_REPLAY_EVIDENCE_ROOT: root,
    TANK_REPLAY_RUN_ID: run,
    TANK_REPLAY_CUTOFF: cutoff,
    TANK_REPLAY_EVIDENCE_SHA256: createHash("sha256")
      .update(bytes)
      .digest("hex"),
  };
}

const state = vi.hoisted(() => ({
  options: [] as any[],
  commands: [] as any[],
  failures: 0,
  stops: 0,
  owners: new Set<string>(),
}));
vi.mock("@boxlite-ai/boxlite", () => ({
  SimpleBox: class {
    constructor(private opts: any) {
      if (state.owners.has(opts.name)) throw Error("duplicate owned name");
      state.owners.add(opts.name);
      state.options.push(opts);
    }
    async exec(...args: any[]) {
      state.commands.push(args);
      if (state.failures > 0) {
        state.failures--;
        throw Error("synthetic initialization failure");
      }
      return { exitCode: 0, stdout: "ok", stderr: "" };
    }
    async stop() {
      state.stops++;
      state.owners.delete(this.opts.name);
    }
    async copyIn() {}
    async copyOut() {}
  },
}));
import { sandboxFactory } from "../src/litebox";
afterEach(async () => {
  for (const root of roots.splice(0))
    await rm(root, { recursive: true, force: true });
  state.options.length = 0;
  state.commands.length = 0;
  state.failures = 0;
  state.stops = 0;
  state.owners.clear();
});
it("analysis enables internet and injects only curated read-only credentials", async () => {
  const s = await sandboxFactory(
    { sessionId: "tank-test", workdir: "/tmp/tank-overlay-analysis" } as any,
    {
      LITEBOX_CPUS: "1",
      SANDBOX_IMAGE: "test-python312",
      TANK_SANDBOX_ALLOWED_NET: "unrestricted",
      TANK_SANDBOX_REPLAY: "0",
      TANK_SANDBOX_DB_URL: "postgresql://readonly@postgres/tank",
      TANK_SANDBOX_OPENROUTER_API_KEY: "synthetic-openrouter",
      TANK_SANDBOX_JEV_PROVIDER: "openrouter",
      TANK_SANDBOX_TYPESAFE_API_KEY: "wrong-provider-must-not-leak",
      API_KEY: "must-not-leak",
      TELEGRAM_TOKEN: "must-not-leak",
      CODEX_AUTH: "must-not-leak",
    },
  );
  await s.exec("true");
  expect(state.options[0].network).toEqual({ mode: "enabled" });
  const env = state.commands.at(-1)[2];
  expect(env).toEqual({
    TANK_DB_URL: "postgresql://readonly@postgres/tank",
    OPENROUTER_API_KEY: "synthetic-openrouter",
    TANK_JEV_PROVIDER: "openrouter",
  });
  expect(state.options[0].image).toBe("test-python312");
  expect(state.options[0].cpus).toBe(1);
  await s.destroy?.();
});
it("replay uses only the database IP and omits outside API secrets", async () => {
  const s = await sandboxFactory(
    {
      sessionId: "tank-replay-test",
      workdir: "/tmp/tank-overlay-replay",
    } as any,
    {
      ...(await replayEvidence()),
      TANK_SANDBOX_ALLOWED_NET: '["postgres"]',
      TANK_SANDBOX_DATABASE_IPV4: "172.29.0.2",
      TANK_SANDBOX_REPLAY: "1",
      TANK_SANDBOX_REPLAY_DB_URL: "postgresql://readonly@postgres/tank_replay",
      TANK_SANDBOX_TYPESAFE_API_KEY: "must-not-leak",
      TANK_SANDBOX_OPENROUTER_API_KEY: "must-not-leak",
      TANK_SANDBOX_JEV_PROVIDER: "openrouter",
      TANK_SANDBOX_ETHERSCAN_API_KEY: "must-not-leak",
    },
  );
  await s.exec("true");
  expect(state.options[0].network).toEqual({
    mode: "enabled",
    allowNet: ["172.29.0.2"],
  });
  expect(state.commands.at(-1)[2]).toEqual({
    TANK_DB_URL: "postgresql://readonly@postgres/tank_replay",
    TANK_REPLAY_EVIDENCE_ROOT: "/replay/evidence",
    TANK_REPLAY_RUN_ID: "RPL-network",
  });
  expect(state.options[0].volumes).toEqual([
    {
      hostPath: join(roots[0], "RPL-network"),
      guestPath: "/replay/evidence",
      readOnly: true,
    },
  ]);
  await s.destroy?.();
});
it("empty replay allowlist disables networking before the first guest command", async () => {
  const s = await sandboxFactory(
    { sessionId: "tank-replay-off", workdir: "/tmp/tank-overlay-off" } as any,
    {
      ...(await replayEvidence()),
      TANK_SANDBOX_ALLOWED_NET: "[]",
      TANK_SANDBOX_REPLAY: "1",
      TANK_SANDBOX_REPLAY_DB_URL: "postgresql://readonly@postgres/tank_replay",
    },
  );
  await s.exec("true");
  expect(state.options[0].network).toEqual({ mode: "disabled" });
  await s.destroy?.();
});
it("local verified OCI layout reaches the native SimpleBox loader independently of Docker cache", async () => {
  const s = await sandboxFactory(
    { sessionId: "tank-local-oci", workdir: "/tmp/tank-oci-test" } as any,
    {
      SANDBOX_ROOTFS_PATH: "/opt/tank-sandbox-oci",
      SANDBOX_IMAGE: "diagnostic-tag",
      TANK_SANDBOX_ALLOWED_NET: "unrestricted",
      TANK_SANDBOX_REPLAY: "0",
      TANK_SANDBOX_DB_URL: "postgresql://readonly@postgres/tank",
    },
  );
  await s.exec("true");
  expect(state.options[0].rootfsPath).toBe("/opt/tank-sandbox-oci");
  await s.destroy?.();
});

it("replay rejects unrestricted policy before allocation", async () => {
  const ctx = {
    sessionId: "tank-invalid",
    workdir: "/tmp/tank-invalid",
  } as any;
  await expect(
    sandboxFactory(ctx, {
      TANK_SANDBOX_REPLAY: "1",
      TANK_SANDBOX_ALLOWED_NET: "unrestricted",
    }),
  ).rejects.toThrow("database-only");
  expect(state.options).toHaveLength(0);
});
it("failed initialization stops only its owned box and retries the stable name cleanly", async () => {
  const s = await sandboxFactory(
    {
      sessionId: "tank-initialization-failure",
      workdir: "/tmp/tank-failure",
    } as any,
    {},
  );
  state.failures = 1;
  await expect(s.exec("true")).rejects.toThrow(
    "synthetic initialization failure",
  );
  expect(state.stops).toBe(1);
  expect(state.owners.size).toBe(0);
  await s.exec("true");
  expect(state.options).toHaveLength(2);
  expect(state.options[0].name).toBe(state.options[1].name);
  await s.destroy?.();
  expect(state.owners.size).toBe(0);
});

it("replay refuses unbound evidence before constructing any VM", async () => {
  await expect(
    sandboxFactory(
      { sessionId: "unbound", workdir: "/tmp/tank-unbound" } as any,
      {
        TANK_SANDBOX_REPLAY: "1",
        TANK_SANDBOX_ALLOWED_NET: "[]",
        TANK_SANDBOX_REPLAY_DB_URL:
          "postgresql://readonly@postgres/tank_replay",
      },
    ),
  ).rejects.toThrow("session-bound");
  expect(state.options).toHaveLength(0);
});

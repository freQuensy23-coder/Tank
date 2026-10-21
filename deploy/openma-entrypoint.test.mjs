import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { after, test } from "node:test";

let fixtureNumber = 0;
const initialEnv = { ...process.env };
const initialExit = process.exit;
const initialListeners = new Map(
  ["SIGTERM", "SIGINT"].map((signal) => [signal, process.listeners(signal)]),
);

after(() => {
  assert.deepEqual({ ...process.env }, initialEnv);
  assert.equal(process.exit, initialExit);
  for (const [signal, listeners] of initialListeners) {
    assert.deepEqual(process.listeners(signal), listeners);
  }
});

async function entrypointFixture(t, options = {}) {
  const originalEnv = { ...process.env };
  const originalListeners = new Map(
    ["SIGTERM", "SIGINT"].map((signal) => [
      signal,
      new Set(process.listeners(signal)),
    ]),
  );
  t.after(() => {
    for (const key of Object.keys(process.env)) {
      if (!(key in originalEnv)) delete process.env[key];
    }
    Object.assign(process.env, originalEnv);
    for (const [signal, retained] of originalListeners) {
      for (const listener of process.listeners(signal)) {
        if (!retained.has(listener)) process.removeListener(signal, listener);
      }
    }
  });

  for (const key of Object.keys(process.env)) {
    if (
      key.startsWith("TANK_SANDBOX_") ||
      ["CODEX_AUTH", "OPENROUTER_API_KEY"].includes(key)
    ) {
      delete process.env[key];
    }
  }
  process.env.BOXLITE_RUNTIME_DIR = "/synthetic-native";
  Object.assign(process.env, options.env);

  const architecture = process.arch === "x64" ? "amd64" : "arm64";
  const shim = Buffer.alloc(64);
  Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1]).copy(shim);
  shim.writeUInt16LE(
    options.wrongArchitecture ? 0 : process.arch === "x64" ? 62 : 183,
    18,
  );

  const secrets = {
    oma_db_password: "synthetic oma password",
    oma_api_key: "synthetic-service-key",
    oma_root_secret: "synthetic-root-secret",
    oma_auth_secret: "synthetic-auth-secret",
    tank_replay_agent_db_password: "synthetic replay password",
    tank_agent_db_password: "synthetic agent password",
    openrouter_api_key: "synthetic-openrouter-key",
    etherscan_api_key: "synthetic-etherscan-key",
    ...options.secrets,
  };
  const reads = [];
  const resolutions = [];
  const spawns = [];
  const kills = [];
  const exits = [];
  const child = new EventEmitter();
  child.kill = (signal) => kills.push(signal);

  t.mock.module("node:fs", {
    exports: {
      readFileSync(path, encoding) {
        reads.push(path);
        if (path === "/synthetic-native/boxlite-shim") return shim;
        if (path === "/synthetic-native/provenance.json") {
          return JSON.stringify({ architecture });
        }
        assert.equal(encoding, "utf8");
        const name = path.replace("/run/secrets/", "");
        assert.ok(Object.hasOwn(secrets, name), "Unexpected secret read");
        return secrets[name];
      },
    },
  });
  t.mock.module("node:dns/promises", {
    exports: {
      async lookup(host, query) {
        resolutions.push({ host, query });
        return { address: options.address ?? "172.29.0.2", family: 4 };
      },
    },
  });
  t.mock.module("node:child_process", {
    exports: {
      spawn(command, args, spawnOptions) {
        spawns.push({
          command,
          args,
          stdio: spawnOptions.stdio,
          env: { ...spawnOptions.env },
        });
        return child;
      },
    },
  });
  t.mock.method(process, "exit", (code) => exits.push(code));

  const moduleUrl = new URL("./openma_entrypoint.mjs", import.meta.url);
  moduleUrl.searchParams.set("fixture", String(++fixtureNumber));
  const { main } = await import(moduleUrl.href);
  return { main, reads, resolutions, spawns, kills, exits, child, secrets };
}

test("main selects service and sandbox credentials and starts the child", async (t) => {
  const f = await entrypointFixture(t, {
    env: {
      TANK_SANDBOX_DATABASE_NAME: "tank_check",
      TANK_SANDBOX_DATABASE_ROLE: "tank_audit_reader",
    },
  });

  await f.main();

  assert.deepEqual(f.resolutions, [
    { host: "postgres", query: { family: 4 } },
  ]);
  assert.equal(f.spawns.length, 1);
  const child = f.spawns[0];
  assert.deepEqual(
    { command: child.command, args: child.args, stdio: child.stdio },
    { command: "pnpm", args: ["start"], stdio: "inherit" },
  );
  assert.equal(
    child.env.DATABASE_URL,
    "postgresql://oma:synthetic%20oma%20password@postgres:5432/oma",
  );
  assert.equal(child.env.API_KEY, f.secrets.oma_api_key);
  assert.equal(child.env.PLATFORM_ROOT_SECRET, f.secrets.oma_root_secret);
  assert.equal(child.env.BETTER_AUTH_SECRET, f.secrets.oma_auth_secret);
  assert.deepEqual(
    Object.fromEntries(
      Object.entries(child.env).filter(([key]) =>
        key.startsWith("TANK_SANDBOX_"),
      ),
    ),
    {
      TANK_SANDBOX_DATABASE_NAME: "tank_check",
      TANK_SANDBOX_DATABASE_ROLE: "tank_audit_reader",
      TANK_SANDBOX_REPLAY_DB_URL:
        "postgresql://tank_replay_agent:synthetic%20replay%20password@172.29.0.2:5432/tank_replay",
      TANK_SANDBOX_DATABASE_IPV4: "172.29.0.2",
      TANK_SANDBOX_DB_URL:
        "postgresql://tank_audit_reader:synthetic%20agent%20password@postgres:5432/tank_check",
      TANK_SANDBOX_OPENROUTER_API_KEY: f.secrets.openrouter_api_key,
      TANK_SANDBOX_ETHERSCAN_API_KEY: f.secrets.etherscan_api_key,
      TANK_SANDBOX_JEV_PROVIDER: "openrouter",
    },
  );
  assert.equal(child.env.OPENROUTER_API_KEY, undefined);
  assert.equal(child.env.CODEX_AUTH, undefined);
  assert.deepEqual(
    f.reads.filter((path) => path.startsWith("/run/secrets/")).sort(),
    Object.keys(f.secrets).map((name) => "/run/secrets/" + name).sort(),
  );
});

test("main forwards shutdown signals and preserves the child's exit outcome", async (t) => {
  const f = await entrypointFixture(t);
  await f.main();

  process.emit("SIGTERM");
  process.emit("SIGINT");
  f.child.emit("exit", 7, null);
  f.child.emit("exit", null, "SIGTERM");
  f.child.emit("exit", null, null);

  assert.deepEqual(f.kills, ["SIGTERM", "SIGINT"]);
  assert.deepEqual(f.exits, [7, 1, 0]);
});

test("main refuses an incompatible native binary before reading secrets or spawning", async (t) => {
  const f = await entrypointFixture(t, { wrongArchitecture: true });

  await assert.rejects(f.main(), /shim architecture differs/);

  assert.deepEqual(f.reads, [
    "/synthetic-native/boxlite-shim",
    "/synthetic-native/provenance.json",
  ]);
  assert.deepEqual(f.resolutions, []);
  assert.deepEqual(f.spawns, []);
});

test("main refuses a public replay database route before spawning", async (t) => {
  const f = await entrypointFixture(t, { address: "8.8.8.8" });

  await assert.rejects(f.main(), /private IPv4 service address/);

  assert.ok(!f.reads.includes("/run/secrets/tank_replay_agent_db_password"));
  assert.deepEqual(f.spawns, []);
});

test("main refuses empty required secrets before resolving or spawning", async (t) => {
  const f = await entrypointFixture(t, {
    secrets: { oma_api_key: "  \n" },
  });

  await assert.rejects(f.main(), /Empty required secret: oma_api_key/);

  assert.deepEqual(f.resolutions, []);
  assert.deepEqual(f.spawns, []);
});

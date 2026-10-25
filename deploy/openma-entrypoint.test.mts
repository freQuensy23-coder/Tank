import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  main, replayDatabaseRoute, runEntrypoint, sandboxDatabaseRoute, verifyNativePlatform, verifyNativeQualification,
  type Architecture, type ChildPort, type EntrypointDependencies, type ForwardedSignal, type NativeSystem, type SecretName,
} from "../vendor/openma/tank-deploy/openma_entrypoint.mts";

interface SpawnInvocation { command: string; args: readonly [string, "src/index.ts"]; stdio: "inherit"; env: NodeJS.ProcessEnv; cwd: string | undefined }
interface ResolutionInvocation { host: "postgres"; family: 4 }
interface FixtureOptions {
  env?: NodeJS.ProcessEnv;
  secrets?: Partial<Record<SecretName, string>>;
  address?: string;
  wrongArchitecture?: boolean;
  unqualified?: boolean;
  system?: NativeSystem;
  provenance?: Buffer;
  qualifications?: Buffer;
}
class FixtureChild implements ChildPort {
  readonly kills: ForwardedSignal[] = [];
  private exited: ((code: number | null, signal: NodeJS.Signals | null) => void) | null = null;
  private failed: ((error: Error) => void) | null = null;
  kill(signal: ForwardedSignal): void { this.kills.push(signal); }
  onExit(listener: (code: number | null, signal: NodeJS.Signals | null) => void): void { this.exited = listener; }
  onError(listener: (error: Error) => void): void { this.failed = listener; }
  emitExit(code: number | null, signal: NodeJS.Signals | null): void {
    assert.notEqual(this.exited, null, "Entrypoint did not subscribe to child exit");
    if (this.exited !== null) this.exited(code, signal);
  }
  emitError(error: Error): void {
    assert.notEqual(this.failed, null, "Entrypoint did not subscribe to child errors");
    if (this.failed !== null) this.failed(error);
  }
}
interface Fixture {
  dependencies: EntrypointDependencies;
  reads: string[];
  resolutions: ResolutionInvocation[];
  spawns: SpawnInvocation[];
  exits: number[];
  errors: string[];
  signals: Map<ForwardedSignal, () => void>;
  secrets: Record<SecretName, string>;
  child: FixtureChild;
}
function shimBytes(target: Architecture): Buffer {
  const shim = Buffer.alloc(64);
  Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1]).copy(shim);
  shim.writeUInt16LE(target === "amd64" ? 62 : 183, 18);
  return shim;
}
function sha256(value: Buffer): string { return createHash("sha256").update(value).digest("hex"); }
function fixture(options: FixtureOptions = {}): Fixture {
  const system = options.system === undefined ? { platform: "linux", arch: "x64" } : options.system;
  const target = system.arch === "arm64" ? "arm64" : "amd64";
  const shim = shimBytes(target);
  if (options.wrongArchitecture === true) shim.writeUInt16LE(0, 18);
  const provenance = options.provenance === undefined
    ? Buffer.from(JSON.stringify({ architecture: target, boxlite_version: "0.10.6" }))
    : options.provenance;
  const hashes = { shim_sha256: sha256(shim), provenance_sha256: sha256(provenance) };
  const status = options.unqualified === true ? "FAIL" : "PASS";
  const qualifications = options.qualifications === undefined ? Buffer.from(JSON.stringify({
    amd64: {
      schema: "tank.boxlite-native-qualification.v1", status, architecture: "amd64",
      checks: { kvm: "PASS", state_persistence: "PASS", dns: "PASS", replay_isolation: "PASS" }, ...hashes,
    },
    arm64: {
      schema: "tank.boxlite-qualified-replay-dns-artifact.v1",
      status: options.unqualified === true ? "FAIL" : "QUALIFIED_NATIVE_REPLAY_NETWORK",
      qualification: { status }, source: { version: "0.10.6" }, files: { "boxlite-shim": hashes.shim_sha256 }, ...hashes,
    },
  })) : options.qualifications;
  const secrets: Record<SecretName, string> = {
    oma_db_password: "synthetic oma password", oma_api_key: "synthetic-service-key",
    oma_root_secret: "synthetic-root-secret", oma_auth_secret: "synthetic-auth-secret",
    tank_replay_agent_db_password: "synthetic replay password", tank_agent_db_password: "synthetic agent password",
    openrouter_api_key: "synthetic-openrouter-key", etherscan_api_key: "synthetic-etherscan-key", ...options.secrets,
  };
  const environment: NodeJS.ProcessEnv = { BOXLITE_RUNTIME_DIR: "/synthetic-native", ...options.env };
  for (const [name, value] of Object.entries(secrets)) environment[name.toUpperCase()] = value;
  const reads: string[] = [];
  const resolutions: ResolutionInvocation[] = [];
  const spawns: SpawnInvocation[] = [];
  const exits: number[] = [];
  const errors: string[] = [];
  const signals = new Map<ForwardedSignal, () => void>();
  const child = new FixtureChild();
  const dependencies: EntrypointDependencies = {
    environment, system,
    readBinary(path: string | URL): Buffer {
      if (path instanceof URL) {
        assert.ok(path.pathname.endsWith("/tank-native-qualification.json"));
        return qualifications;
      }
      reads.push(path);
      if (path === "/synthetic-native/boxlite-shim") return shim;
      if (path === "/synthetic-native/provenance.json") return provenance;
      throw Error("Unexpected file read");
    },
    async resolve(host: "postgres", query: { family: 4 }): Promise<{ address: string }> {
      resolutions.push({ host, family: query.family });
      return { address: options.address === undefined ? "172.29.0.2" : options.address };
    },
    launch(command: string, args: readonly [string, "src/index.ts"], options: { stdio: "inherit"; env: NodeJS.ProcessEnv; cwd?: string }): ChildPort {
      spawns.push({ command, args, stdio: options.stdio, env: { ...options.env }, cwd: options.cwd });
      return child;
    },
    onSignal(signal: ForwardedSignal, listener: () => void): void { signals.set(signal, listener); },
    exit(code: number): void { exits.push(code); },
    reportError(message: string): void { errors.push(message); },
  };
  return { dependencies, reads, resolutions, spawns, exits, errors, signals, secrets, child };
}

test("main starts the installed application without package-manager installs and preserves credentials and cwd", async (): Promise<void> => {
  const cwd = process.cwd();
  const f = fixture({ env: { TANK_SANDBOX_DATABASE_NAME: "tank_check", TANK_SANDBOX_DATABASE_ROLE: "tank_audit_reader" } });
  await main(f.dependencies);
  assert.deepEqual(f.resolutions, [{ host: "postgres", family: 4 }]);
  assert.equal(f.spawns.length, 1);
  const child = f.spawns[0];
  assert.deepEqual({ command: child.command, args: child.args, stdio: child.stdio }, {
    command: process.execPath,
    args: [fileURLToPath(new URL("../vendor/openma/apps/main-node/node_modules/tsx/dist/cli.mjs", import.meta.url)), "src/index.ts"],
    stdio: "inherit",
  });
  assert.equal(child.cwd, undefined);
  assert.equal(process.cwd(), cwd);
  assert.equal(child.env.DATABASE_URL, "postgresql://oma:synthetic%20oma%20password@postgres:5432/oma");
  assert.equal(child.env.API_KEY, f.secrets.oma_api_key);
  assert.equal(child.env.PLATFORM_ROOT_SECRET, f.secrets.oma_root_secret);
  assert.equal(child.env.BETTER_AUTH_SECRET, f.secrets.oma_auth_secret);
  assert.deepEqual(Object.fromEntries(Object.entries(child.env).filter(([key]: [string, string | undefined]): boolean => key.startsWith("TANK_SANDBOX_"))), {
    TANK_SANDBOX_DATABASE_NAME: "tank_check", TANK_SANDBOX_DATABASE_ROLE: "tank_audit_reader",
    TANK_SANDBOX_REPLAY_DB_URL: "postgresql://tank_replay_agent:synthetic%20replay%20password@172.29.0.2:5432/tank_replay",
    TANK_SANDBOX_DATABASE_IPV4: "172.29.0.2",
    TANK_SANDBOX_DB_URL: "postgresql://tank_audit_reader:synthetic%20agent%20password@postgres:5432/tank_check",
    TANK_SANDBOX_OPENROUTER_API_KEY: f.secrets.openrouter_api_key,
    TANK_SANDBOX_ETHERSCAN_API_KEY: f.secrets.etherscan_api_key,
    TANK_SANDBOX_JEV_PROVIDER: "openrouter",
  });
  for (const name of Object.keys(f.secrets)) assert.equal(child.env[name.toUpperCase()], undefined);
});

test("main forwards shutdown signals and preserves the child's exit outcome", async (): Promise<void> => {
  const f = fixture();
  await main(f.dependencies);
  for (const signal of ["SIGTERM", "SIGINT"] as const) {
    const listener = f.signals.get(signal);
    assert.notEqual(listener, undefined);
    if (listener !== undefined) listener();
  }
  f.child.emitExit(7, null);
  f.child.emitExit(null, "SIGTERM");
  f.child.emitExit(null, null);
  assert.deepEqual(f.child.kills, ["SIGTERM", "SIGINT"]);
  assert.deepEqual(f.exits, [7, 1, 1]);
  assert.deepEqual(f.errors, ["OpenMA child exited without a code or signal"]);
});

test("main refuses an incompatible native binary before resolving or spawning", async (): Promise<void> => {
  const f = fixture({ wrongArchitecture: true });
  await assert.rejects(main(f.dependencies), /shim architecture differs/);
  assert.deepEqual(f.reads, ["/synthetic-native/boxlite-shim", "/synthetic-native/provenance.json"]);
  assert.deepEqual(f.resolutions, []);
  assert.deepEqual(f.spawns, []);
});

test("main refuses an unqualified native component before resolving or spawning", async (): Promise<void> => {
  const f = fixture({ unqualified: true });
  await assert.rejects(main(f.dependencies), /qualification missing or invalid/);
  assert.deepEqual(f.resolutions, []);
  assert.deepEqual(f.spawns, []);
});

test("main refuses a public replay database route before spawning", async (): Promise<void> => {
  const f = fixture({ address: "8.8.8.8" });
  await assert.rejects(main(f.dependencies), /private IPv4 service address/);
  assert.deepEqual(f.spawns, []);
});

test("main refuses empty required secrets before resolving or spawning", async (): Promise<void> => {
  const f = fixture({ secrets: { oma_api_key: "  \n" } });
  await assert.rejects(main(f.dependencies), /Empty required secret: OMA_API_KEY/);
  assert.deepEqual(f.resolutions, []);
  assert.deepEqual(f.spawns, []);
});

test("child startup errors are terminal and never print the child's secret-bearing message", async (): Promise<void> => {
  const f = fixture();
  await main(f.dependencies);
  f.child.emitError(Error("synthetic-service-key must not appear"));
  assert.deepEqual(f.exits, [1]);
  assert.deepEqual(f.errors, ["OpenMA child process failed to start"]);
});

test("both retained provenance coordinate variants remain qualified", async (): Promise<void> => {
  for (const [arch, elf] of [["x64", "x86_64"], ["arm64", "aarch64"]] as const) {
    const f = fixture({
      system: { platform: "linux", arch },
      provenance: Buffer.from(JSON.stringify({ elf: { architecture: elf }, source: { version: "0.10.6" } })),
    });
    await main(f.dependencies);
    assert.equal(f.spawns.length, 1);
  }
});

test("conflicting or missing provenance coordinates refuse before service effects", async (): Promise<void> => {
  for (const provenance of [
    {}, { architecture: "amd64" }, { architecture: "amd64", boxlite_version: "0.10.6", source: { version: "wrong" } },
    { architecture: "amd64", elf: { architecture: "aarch64" }, source: { version: "0.10.6" } },
  ]) {
    const f = fixture({ provenance: Buffer.from(JSON.stringify(provenance)) });
    await assert.rejects(main(f.dependencies), /provenance/);
    assert.deepEqual(f.resolutions, []);
    assert.deepEqual(f.spawns, []);
  }
});

test("malformed qualification JSON is rejected without forwarding raw payload text", async (): Promise<void> => {
  const f = fixture({ qualifications: Buffer.from("synthetic-service-key-not-json") });
  await assert.rejects(main(f.dependencies), /BoxLite qualification JSON is invalid/);
  assert.deepEqual(f.spawns, []);
});

test("sandbox route preserves declared defaults and rejects invalid isolated identifiers", (): void => {
  const secret = (name: SecretName): string => {
    assert.equal(name, "tank_agent_db_password");
    return "synthetic:p@ss";
  };
  assert.equal(sandboxDatabaseRoute({}, secret), "postgresql://tank_agent:synthetic%3Ap%40ss@postgres:5432/tank");
  assert.equal(sandboxDatabaseRoute({ TANK_SANDBOX_DATABASE_NAME: "tank_check", TANK_SANDBOX_DATABASE_ROLE: "tank_reader" }, secret), "postgresql://tank_reader:synthetic%3Ap%40ss@postgres:5432/tank_check");
  for (const value of ["", "tank;DROP", "../tank", "A", "x".repeat(64)]) {
    assert.throws((): void => { sandboxDatabaseRoute({ TANK_SANDBOX_DATABASE_NAME: value }, secret); }, /SQL identifier/);
    assert.throws((): void => { sandboxDatabaseRoute({ TANK_SANDBOX_DATABASE_ROLE: value }, secret); }, /SQL identifier/);
  }
});

test("replay route resolves only the declared private IPv4 service", async (): Promise<void> => {
  const f = fixture();
  const route = await replayDatabaseRoute((_name: SecretName): string => "synthetic", f.dependencies.resolve);
  assert.equal(route.url, "postgresql://tank_replay_agent:synthetic@172.29.0.2:5432/tank_replay");
  for (const address of ["8.8.8.8", "0.0.0.0", "127.0.0.1", "::1"]) {
    await assert.rejects(replayDatabaseRoute((_name: SecretName): string => "synthetic", async (): Promise<{ address: string }> => ({ address })), /private IPv4/);
  }
});

test("native platform requires a supported Linux machine and declared provenance", (): void => {
  assert.equal(verifyNativePlatform(shimBytes("amd64"), { architecture: "amd64" }, { platform: "linux", arch: "x64" }), "amd64");
  assert.throws((): Architecture => verifyNativePlatform(shimBytes("amd64"), { architecture: "amd64" }, { platform: "darwin", arch: "x64" }), /requires Linux/);
  assert.throws((): Architecture => verifyNativePlatform(shimBytes("amd64"), {}, { platform: "linux", arch: "x64" }), /provenance architecture/);
});

test("qualification hashes bind the exact shim and provenance bytes", (): void => {
  const shim = shimBytes("amd64");
  const provenance = Buffer.from(JSON.stringify({ architecture: "amd64", boxlite_version: "0.10.6" }));
  assert.throws((): Architecture => verifyNativeQualification(shim, provenance, {
    amd64: { schema: "tank.boxlite-native-qualification.v1", status: "PASS", architecture: "amd64",
      checks: { kvm: "PASS", state_persistence: "PASS", dns: "PASS", replay_isolation: "PASS" },
      shim_sha256: "0".repeat(64), provenance_sha256: sha256(provenance) },
  }, { platform: "linux", arch: "x64" }), /qualification missing or invalid/);
});

test("the launch guard makes imports inert", async (): Promise<void> => {
  const f = fixture();
  await runEntrypoint(pathToFileURL("/not-the-current-program.mjs").href, f.dependencies);
  assert.deepEqual(f.reads, []);
  assert.deepEqual(f.spawns, []);
});

test("a normal zero child exit remains a successful process outcome", async (): Promise<void> => {
  const f = fixture();
  await main(f.dependencies);
  f.child.emitExit(0, null);
  assert.deepEqual(f.exits, [0]);
  assert.deepEqual(f.errors, []);
});

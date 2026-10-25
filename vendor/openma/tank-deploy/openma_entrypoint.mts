import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { lookup } from "node:dns/promises";
import { isIP } from "node:net";

export type Architecture = "amd64" | "arm64";
export type ForwardedSignal = "SIGTERM" | "SIGINT";
export interface NativeSystem { platform: string; arch: string }
export interface NativeCoordinates { architecture: Architecture; version: "0.10.6" }
export interface SandboxDatabaseVariables {
  TANK_SANDBOX_DATABASE_NAME?: string;
  TANK_SANDBOX_DATABASE_ROLE?: string;
}
export interface DatabaseResolution { address: string }
export interface ReplayDatabaseRoute { address: string; url: string }
export type DatabaseResolver = (host: "postgres", options: { family: 4 }) => Promise<DatabaseResolution>;
export type SecretName = "oma_db_password" | "oma_api_key" | "oma_root_secret" | "oma_auth_secret" |
  "tank_agent_db_password" | "tank_replay_agent_db_password" | "openrouter_api_key" | "etherscan_api_key";
export type SecretReader = (name: SecretName) => string;
export interface ChildPort {
  kill(signal: ForwardedSignal): void;
  onExit(listener: (code: number | null, signal: NodeJS.Signals | null) => void): void;
  onError(listener: (error: Error) => void): void;
}
export interface EntrypointDependencies {
  environment: NodeJS.ProcessEnv;
  system: NativeSystem;
  readBinary(path: string | URL): Buffer;
  resolve: DatabaseResolver;
  launch(command: string, args: readonly [string, "src/index.ts"], options: { stdio: "inherit"; env: NodeJS.ProcessEnv; cwd?: string }): ChildPort;
  onSignal(signal: ForwardedSignal, listener: () => void): void;
  exit(code: number): void;
  reportError(message: string): void;
}
interface NativeReceipt {
  architecture: Architecture;
  shimSha256: string;
  provenanceSha256: string;
}
type RawObject = { readonly [key: string]: unknown };
class StartupError extends Error {}

const SECRET_KEYS: Readonly<Record<SecretName, string>> = {
  oma_db_password: "OMA_DB_PASSWORD",
  oma_api_key: "OMA_API_KEY",
  oma_root_secret: "OMA_ROOT_SECRET",
  oma_auth_secret: "OMA_AUTH_SECRET",
  tank_agent_db_password: "TANK_AGENT_DB_PASSWORD",
  tank_replay_agent_db_password: "TANK_REPLAY_AGENT_DB_PASSWORD",
  openrouter_api_key: "OPENROUTER_API_KEY",
  etherscan_api_key: "ETHERSCAN_API_KEY",
};
const SIGNALS: readonly ForwardedSignal[] = ["SIGTERM", "SIGINT"];

function isRawObject(value: unknown): value is RawObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function object(value: unknown, reason: string): RawObject {
  if (!isRawObject(value)) throw new StartupError(reason);
  return value;
}

function jsonDocument(bytes: Buffer, reason: string): unknown {
  try {
    return JSON.parse(bytes.toString("utf8")) as unknown;
  } catch {
    throw new StartupError(reason);
  }
}

function architecture(value: unknown): Architecture {
  if (value !== "amd64" && value !== "arm64") throw new StartupError("BoxLite provenance architecture missing or invalid");
  return value;
}

function platformCoordinates(value: unknown): Architecture {
  const metadata = object(value, "BoxLite provenance must be a JSON object");
  if (metadata.architecture !== undefined) {
    const declared = architecture(metadata.architecture);
    if (metadata.elf !== undefined) {
      const elf = object(metadata.elf, "BoxLite ELF provenance is invalid");
      const expected = declared === "amd64" ? "x86_64" : "aarch64";
      if (elf.architecture !== expected) throw new StartupError("BoxLite provenance architectures disagree");
    }
    return declared;
  }
  const elf = object(metadata.elf, "BoxLite provenance architecture missing or invalid");
  if (elf.architecture === "x86_64") return "amd64";
  if (elf.architecture === "aarch64") return "arm64";
  throw new StartupError("BoxLite provenance architecture missing or invalid");
}

function provenanceCoordinates(value: unknown): NativeCoordinates {
  const metadata = object(value, "BoxLite provenance must be a JSON object");
  let version: string;
  if (metadata.boxlite_version !== undefined) {
    if (typeof metadata.boxlite_version !== "string" || metadata.boxlite_version.length === 0) {
      throw new StartupError("BoxLite provenance version missing or invalid");
    }
    version = metadata.boxlite_version;
    if (metadata.source !== undefined && object(metadata.source, "BoxLite source provenance is invalid").version !== version) {
      throw new StartupError("BoxLite provenance versions disagree");
    }
  } else {
    const source = object(metadata.source, "BoxLite provenance version missing or invalid");
    if (typeof source.version !== "string" || source.version.length === 0) {
      throw new StartupError("BoxLite provenance version missing or invalid");
    }
    version = source.version;
  }
  const declared = platformCoordinates(metadata);
  if (version !== "0.10.6") throw new StartupError("BoxLite native component qualification missing or invalid for " + declared);
  return { architecture: declared, version };
}

function hostArchitecture(system: NativeSystem): Architecture {
  if (system.platform !== "linux") throw new StartupError("OpenMA requires Linux amd64 or arm64");
  if (system.arch === "x64") return "amd64";
  if (system.arch === "arm64") return "arm64";
  throw new StartupError("OpenMA requires Linux amd64 or arm64");
}

function verifyPlatform(shim: Buffer, declared: Architecture, system: NativeSystem): Architecture {
  const target = hostArchitecture(system);
  const machine = target === "amd64" ? 62 : 183;
  if (shim.length < 64 || !shim.subarray(0, 6).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1])) || shim.readUInt16LE(18) !== machine) {
    throw new StartupError("BoxLite shim architecture differs from the OpenMA Linux container");
  }
  if (declared !== target) throw new StartupError("BoxLite provenance architecture differs from the OpenMA Linux container");
  return target;
}

export function verifyNativePlatform(
  shim: Buffer,
  metadata: unknown,
  system: NativeSystem = { platform: process.platform, arch: process.arch },
): Architecture {
  return verifyPlatform(shim, platformCoordinates(metadata), system);
}

function receipt(value: unknown, target: Architecture, shimHash: string): NativeReceipt {
  const reason = "BoxLite native component qualification missing or invalid for " + target;
  const qualifications = object(value, reason);
  const selected = object(qualifications[target], reason);
  if (target === "amd64") {
    const checks = object(selected.checks, reason);
    if (selected.schema !== "tank.boxlite-native-qualification.v1" || selected.status !== "PASS" ||
        selected.architecture !== target || checks.kvm !== "PASS" || checks.state_persistence !== "PASS" ||
        checks.dns !== "PASS" || checks.replay_isolation !== "PASS") throw new StartupError(reason);
  } else {
    const source = object(selected.source, reason);
    const qualification = object(selected.qualification, reason);
    const files = object(selected.files, reason);
    if (selected.schema !== "tank.boxlite-qualified-replay-dns-artifact.v1" ||
        selected.status !== "QUALIFIED_NATIVE_REPLAY_NETWORK" || qualification.status !== "PASS" ||
        source.version !== "0.10.6" || files["boxlite-shim"] !== shimHash) throw new StartupError(reason);
  }
  if (typeof selected.shim_sha256 !== "string" || !/^[0-9a-f]{64}$/.test(selected.shim_sha256) ||
      typeof selected.provenance_sha256 !== "string" || !/^[0-9a-f]{64}$/.test(selected.provenance_sha256)) throw new StartupError(reason);
  return { architecture: target, shimSha256: selected.shim_sha256, provenanceSha256: selected.provenance_sha256 };
}

export function verifyNativeQualification(
  shim: Buffer,
  provenance: Buffer,
  qualifications: unknown,
  system: NativeSystem = { platform: process.platform, arch: process.arch },
): Architecture {
  const coordinates = provenanceCoordinates(jsonDocument(provenance, "BoxLite provenance JSON is invalid"));
  const target = verifyPlatform(shim, coordinates.architecture, system);
  const shimHash = createHash("sha256").update(shim).digest("hex");
  const provenanceHash = createHash("sha256").update(provenance).digest("hex");
  const qualified = receipt(qualifications, target, shimHash);
  if (qualified.shimSha256 !== shimHash || qualified.provenanceSha256 !== provenanceHash) {
    throw new StartupError("BoxLite native component qualification missing or invalid for " + target);
  }
  return target;
}

function readSecret(environment: NodeJS.ProcessEnv, name: SecretName): string {
  const variable = SECRET_KEYS[name];
  const supplied = environment[variable];
  if (supplied === undefined || supplied.trim().length === 0) throw new StartupError("Empty required secret: " + variable);
  return supplied.trim();
}

export function sandboxDatabaseRoute(env: SandboxDatabaseVariables, secret: SecretReader): string {
  const database = env.TANK_SANDBOX_DATABASE_NAME === undefined ? "tank" : env.TANK_SANDBOX_DATABASE_NAME;
  const role = env.TANK_SANDBOX_DATABASE_ROLE === undefined ? "tank_agent" : env.TANK_SANDBOX_DATABASE_ROLE;
  for (const [name, value] of [["database", database], ["role", role]] as const) {
    if (typeof value !== "string" || !/^[a-z_][a-z0-9_]{0,62}$/.test(value)) {
      throw new StartupError("Invalid sandbox database " + name + "; use a lowercase SQL identifier");
    }
  }
  return "postgresql://" + role + ":" + encodeURIComponent(secret("tank_agent_db_password")) + "@postgres:5432/" + database;
}

export async function replayDatabaseRoute(secret: SecretReader, resolve: DatabaseResolver = lookup): Promise<ReplayDatabaseRoute> {
  const { address } = await resolve("postgres", { family: 4 });
  const octets = address.split(".").map((value: string): number => Number(value));
  if (isIP(address) !== 4 || !(octets[0] === 10 || (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) || (octets[0] === 192 && octets[1] === 168))) {
    throw new StartupError("Replay database must resolve to a private IPv4 service address");
  }
  return { address, url: "postgresql://tank_replay_agent:" + encodeURIComponent(secret("tank_replay_agent_db_password")) + "@" + address + ":5432/tank_replay" };
}

function productionDependencies(): EntrypointDependencies {
  return {
    environment: process.env,
    system: { platform: process.platform, arch: process.arch },
    readBinary(path: string | URL): Buffer { return readFileSync(path); },
    resolve: lookup,
    launch(command: string, args: readonly [string, "src/index.ts"], options: { stdio: "inherit"; env: NodeJS.ProcessEnv; cwd?: string }): ChildPort {
      const child = spawn(command, [...args], options);
      return {
        kill(signal: ForwardedSignal): void { child.kill(signal); },
        onExit(listener: (code: number | null, signal: NodeJS.Signals | null) => void): void { child.on("exit", listener); },
        onError(listener: (error: Error) => void): void { child.once("error", listener); },
      };
    },
    onSignal(signal: ForwardedSignal, listener: () => void): void { process.on(signal, listener); },
    exit(code: number): void { process.exit(code); },
    reportError(message: string): void { console.error(message); },
  };
}

export async function main(dependencies: EntrypointDependencies = productionDependencies()): Promise<void> {
  const env = dependencies.environment;
  const nativeRoot = env.BOXLITE_RUNTIME_DIR === undefined ? "/opt/tank-boxlite-runtime" : env.BOXLITE_RUNTIME_DIR;
  if (nativeRoot.length === 0) throw new StartupError("BoxLite runtime directory must not be empty");
  verifyNativeQualification(
    dependencies.readBinary(nativeRoot + "/boxlite-shim"),
    dependencies.readBinary(nativeRoot + "/provenance.json"),
    jsonDocument(dependencies.readBinary(new URL("../tank-native-qualification.json", import.meta.url)), "BoxLite qualification JSON is invalid"),
    dependencies.system,
  );
  const secret: SecretReader = (name: SecretName): string => readSecret(env, name);
  env.DATABASE_URL = "postgresql://oma:" + encodeURIComponent(secret("oma_db_password")) + "@postgres:5432/oma";
  env.API_KEY = secret("oma_api_key");
  env.PLATFORM_ROOT_SECRET = secret("oma_root_secret");
  env.BETTER_AUTH_SECRET = secret("oma_auth_secret");
  const replay = await replayDatabaseRoute(secret, dependencies.resolve);
  env.TANK_SANDBOX_REPLAY_DB_URL = replay.url;
  env.TANK_SANDBOX_DATABASE_IPV4 = replay.address;
  env.TANK_SANDBOX_DB_URL = sandboxDatabaseRoute(env, secret);
  env.TANK_SANDBOX_OPENROUTER_API_KEY = secret("openrouter_api_key");
  env.TANK_SANDBOX_ETHERSCAN_API_KEY = secret("etherscan_api_key");
  env.TANK_SANDBOX_JEV_PROVIDER = "openrouter";
  for (const variable of Object.values(SECRET_KEYS)) delete env[variable];
  const child = dependencies.launch(
    process.execPath,
    [fileURLToPath(new URL("../apps/main-node/node_modules/tsx/dist/cli.mjs", import.meta.url)), "src/index.ts"],
    { stdio: "inherit", env },
  );
  for (const signal of SIGNALS) dependencies.onSignal(signal, (): void => child.kill(signal));
  child.onExit((code: number | null, signal: NodeJS.Signals | null): void => {
    if (code !== null) dependencies.exit(code);
    else if (signal !== null) dependencies.exit(1);
    else {
      dependencies.reportError("OpenMA child exited without a code or signal");
      dependencies.exit(1);
    }
  });
  child.onError((_error: Error): void => {
    dependencies.reportError("OpenMA child process failed to start");
    dependencies.exit(1);
  });
}

export async function runEntrypoint(url: string, dependencies: EntrypointDependencies = productionDependencies()): Promise<void> {
  if (process.argv[1] === undefined || url !== pathToFileURL(process.argv[1]).href) return;
  try {
    await main(dependencies);
  } catch (error: unknown) {
    dependencies.reportError(error instanceof StartupError ? error.message : "OpenMA startup failed");
    dependencies.exit(1);
  }
}

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

export interface QualificationConfiguration {
  directory: string;
  rootfs: string;
  identity: string;
  imageId: string;
  memoryMib: 2048;
  previousProof: string | undefined;
  runtimeHome: string;
}
export interface GuestExecPort {
  exec(command: string, timeoutMs: number): Promise<string>;
}
export interface BrowserEvidencePort {
  content(): Promise<string>;
  screenshot(options: { type: "png" }): Promise<Uint8Array>;
}
export interface BrowserEvidence {
  domSha256: string;
  pngSha256: string;
  pngBytes: number;
  width: number;
  height: number;
}

export function hash(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

export const phaseWallSeconds = Object.freeze({ first: 1080, resume: 900, cleanup: 60 });
export const PREPARATION_TIMEOUT_EXIT = 86;

export function preparationWatchdog(expire: () => void, milliseconds: number = 240000): () => void {
  const timer = setTimeout(expire, milliseconds);
  return (): void => clearTimeout(timer);
}

export function configuration(env: NodeJS.ProcessEnv): QualificationConfiguration {
  if (env.TANK_NATIVE_LIFECYCLE_GO !== "AUTHORIZED_NATIVE_PROOF") throw Error("Native qualification requires explicit deployment-owner GO");
  const proof = env.TANK_NATIVE_PROOF_DIR;
  if (proof === undefined) throw Error("A dedicated new /proof/<run> directory is required");
  const directory = resolve(proof);
  if (!directory.startsWith("/proof/") || directory === "/proof") throw Error("A dedicated new /proof/<run> directory is required");
  const previousProof = env.TANK_NATIVE_REUSE_CACHE_PROOF_DIR;
  if (previousProof !== undefined && (!/^\/proof\/[a-zA-Z0-9_-]+$/.test(previousProof) || previousProof === directory)) throw Error("Cache reuse requires a distinct explicit owned /proof/<run> receipt directory");
  const runtimeHome = join(previousProof === undefined ? directory : previousProof, "boxlite");
  if (env.BOXLITE_HOME !== runtimeHome) throw Error("BOXLITE_HOME must be the dedicated proof directory/boxlite (or explicitly declared cleaned owned cache home)");
  const identity = env.TANK_NATIVE_EXPECT_IDENTITY;
  if (identity === undefined || !/^[a-f0-9]{64}$/.test(identity)) throw Error("Exact candidate runtime identity is required");
  const imageId = env.TANK_NATIVE_IMAGE_ID;
  if (imageId === undefined || !/^sha256:[a-f0-9]{64}$/.test(imageId)) throw Error("Host-inspected candidate image ID is required");
  if (env.TANK_NATIVE_MEMORY_MIB !== "2048") throw Error("Final native qualification requires explicit memory2048 in a fresh scope");
  const rootfs = resolve(env.TANK_NATIVE_ROOTFS === undefined ? "/opt/tank-sandbox-oci" : env.TANK_NATIVE_ROOTFS);
  if (!rootfs.startsWith("/opt/")) throw Error("Explicit readonly /opt OCI layout is required");
  return { directory, rootfs, identity, imageId, memoryMib: 2048, previousProof, runtimeHome };
}

export function quote(value: string): string {
  return "'" + value.replaceAll("'", "'\\''") + "'";
}

export async function mountedOutputNonce(sandbox: GuestExecPort, nonce: string, write: boolean): Promise<void> {
  const encoded = Buffer.from(nonce).toString("base64");
  const script = "from pathlib import Path; import base64; p=Path('/mnt/session/outputs/native-retained-proof'); " +
    (write ? `p.write_bytes(base64.b64decode('${encoded}')); ` : "") + "print(base64.b64encode(p.read_bytes()).decode())";
  const reply = (await sandbox.exec("python3 -c " + quote(script), 5000)).trim();
  assert.equal(reply, encoded, "mounted output nonce guest exec failed or returned different bytes");
}

export async function browserEvidence(page: BrowserEvidencePort, directory: string, name: string): Promise<BrowserEvidence> {
  const dom = await page.content();
  assert.ok(dom.includes("native-retained-browser-proof"), "loopback DOM marker missing");
  const png = Buffer.from(await page.screenshot({ type: "png" }));
  assert.ok(png.length > 24 && png.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])), "browser did not return PNG bytes");
  const dimensions = { width: png.readUInt32BE(16), height: png.readUInt32BE(20) };
  assert.ok(dimensions.width > 0 && dimensions.height > 0, "PNG dimensions missing");
  await writeFile(join(directory, `browser-${name}.html`), dom, { mode: 0o600 });
  await writeFile(join(directory, `browser-${name}.png`), png, { mode: 0o600 });
  return { domSha256: hash(dom), pngSha256: hash(png), pngBytes: png.length, ...dimensions };
}

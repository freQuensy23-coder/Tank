import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  browserEvidence, configuration, mountedOutputNonce, phaseWallSeconds, PREPARATION_TIMEOUT_EXIT,
  type BrowserEvidencePort,
} from "./qualification-contracts.mts";

const permitted = {
  TANK_NATIVE_LIFECYCLE_GO: "AUTHORIZED_NATIVE_PROOF", TANK_NATIVE_MEMORY_MIB: "2048",
  TANK_NATIVE_PROOF_DIR: "/proof/native-run", BOXLITE_HOME: "/proof/native-run/boxlite",
  TANK_NATIVE_EXPECT_IDENTITY: "a".repeat(64), TANK_NATIVE_IMAGE_ID: "sha256:" + "b".repeat(64),
};

test("native qualifier defaults to no native import/start and requires explicit GO", (): void => {
  assert.throws((): void => { configuration({}); }, /explicit deployment-owner GO/);
  const result = spawnSync(process.execPath, [new URL("./native-qualification.mjs", import.meta.url).pathname], { env: {}, encoding: "utf8", timeout: 1000 });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /explicit deployment-owner GO/);
  assert.doesNotMatch(result.stderr, /Cannot find|boxlite|native module/i);
});

test("native qualifier requires exact candidate, dedicated proof storage and explicit runtime home", (): void => {
  assert.throws((): void => { configuration({ ...permitted, TANK_NATIVE_MEMORY_MIB: "512" }); }, /explicit memory2048/);
  assert.equal(configuration(permitted).rootfs, "/opt/tank-sandbox-oci");
  assert.throws((): void => { configuration({ ...permitted, BOXLITE_HOME: "/app/data/boxlite" }); }, /dedicated proof directory/);
  assert.throws((): void => { configuration({ ...permitted, TANK_NATIVE_PROOF_DIR: "/app/data" }); }, /dedicated new/);
  assert.throws((): void => { configuration({ ...permitted, TANK_NATIVE_EXPECT_IDENTITY: "unknown" }); }, /Exact candidate/);
  assert.throws((): void => { configuration({ ...permitted, TANK_NATIVE_IMAGE_ID: "latest" }); }, /Host-inspected/);
});

test("diagnostic DOM/PNG capture saves exact artifacts and rejects absent evidence", async (): Promise<void> => {
  const directory = await mkdtemp(join(tmpdir(), "tank-probe-guard-"));
  const png = Buffer.alloc(25);
  Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]).copy(png);
  png.writeUInt32BE(12, 16);
  png.writeUInt32BE(9, 20);
  const page: BrowserEvidencePort = {
    content: async (): Promise<string> => "<h1>native-retained-browser-proof</h1>",
    screenshot: async (options: { type: "png" }): Promise<Uint8Array> => {
      assert.deepEqual(options, { type: "png" });
      return png;
    },
  };
  try {
    const result = await browserEvidence(page, directory, "first");
    assert.equal(result.width, 12);
    assert.equal(result.height, 9);
    assert.deepEqual(await readFile(join(directory, "browser-first.png")), png);
    assert.match(await readFile(join(directory, "browser-first.html"), "utf8"), /native-retained-browser-proof/);
    await assert.rejects(browserEvidence({
      content: async (): Promise<string> => "<h1>wrong site</h1>",
      screenshot: async (): Promise<Uint8Array> => assert.fail("DOM check must precede PNG capture"),
    }, directory, "resume"), /DOM marker missing/);
    await assert.rejects(browserEvidence({
      content: async (): Promise<string> => "native-retained-browser-proof",
      screenshot: async (): Promise<Uint8Array> => Buffer.from("not png"),
    }, directory, "resume"), /PNG bytes/);
  } finally {
    await rm(directory, { recursive: true });
  }
});

test("outer orchestration contains cold preparation and all unchanged browser RPC budgets", (): void => {
  assert.ok(phaseWallSeconds.first >= 240 + 7 * 90 + 90);
  assert.ok(phaseWallSeconds.resume >= 240 + 6 * 90 + 90);
});

test("separate preparation watchdog exits an unresolved phase before browser work", (): void => {
  const moduleUrl = new URL("./native-qualification.mjs", import.meta.url).href;
  const code = `import {preparationWatchdog,PREPARATION_TIMEOUT_EXIT} from ${JSON.stringify(moduleUrl)};preparationWatchdog(process.exit.bind(process,PREPARATION_TIMEOUT_EXIT),10);await Promise.withResolvers().promise;console.log('browser_must_not_run');`;
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", code], { encoding: "utf8", timeout: 1000 });
  assert.equal(result.status, PREPARATION_TIMEOUT_EXIT);
  assert.equal(result.stdout, "");
  assert.equal(result.error, undefined);
});

test("mounted output nonce uses bounded guest exec and never mounted-path file transfer", async (): Promise<void> => {
  const python = process.env.TANK_FILE_IO_TEST_PYTHON;
  assert.ok(python, "explicit Python3.13.13 test interpreter required");
  const directory = await mkdtemp(join(tmpdir(), "tank-probe-nonce-"));
  const target = join(directory, "nonce");
  const nonce = "quote' dollar$ newline\n";
  const calls: { command: string; budget: number }[] = [];
  await symlink(python, join(directory, "python3"));
  const port = {
    writeFileBytes: (): never => { throw Error("unsupported mounted transfer"); },
    readFileBytes: (): never => { throw Error("unsupported mounted transfer"); },
    exec: async (command: string, budget: number): Promise<string> => {
      calls.push({ command, budget });
      const result = spawnSync("sh", ["-c", command.replaceAll("/mnt/session/outputs/native-retained-proof", target)], {
        encoding: "utf8", timeout: budget, env: { PATH: directory + ":/usr/bin:/bin" },
      });
      if (result.error !== undefined) throw result.error;
      assert.equal(result.status, 0, result.stderr);
      return result.stdout;
    },
  };
  try {
    await mountedOutputNonce(port, nonce, true);
    await mountedOutputNonce(port, nonce, false);
    assert.equal(await readFile(target, "utf8"), nonce);
    assert.equal(calls.length, 2);
    assert.ok(calls.every((call: { command: string; budget: number }): boolean => call.budget === 5000));
    assert.ok(calls.every((call: { command: string; budget: number }): boolean => call.command.startsWith("python3 -c ") && call.command.includes("/mnt/session/outputs/native-retained-proof")));
    assert.match(calls[0]!.command, /write_bytes/);
    assert.doesNotMatch(calls[1]!.command, /write_bytes/);
    assert.ok(calls.every((call: { command: string; budget: number }): boolean => !call.command.includes(nonce)));
    await assert.rejects(mountedOutputNonce({ exec: async (): Promise<string> => "[error: failure]" }, nonce, true), /guest exec failed/);
  } finally {
    await rm(directory, { recursive: true });
  }
});

test("public cache reuse is explicit and keeps a distinct proof/session directory", (): void => {
  const next = { ...permitted, TANK_NATIVE_PROOF_DIR: "/proof/native-browser-v7", TANK_NATIVE_REUSE_CACHE_PROOF_DIR: "/proof/native-browser-v6", BOXLITE_HOME: "/proof/native-browser-v6/boxlite" };
  assert.equal(configuration(next).runtimeHome, "/proof/native-browser-v6/boxlite");
  assert.equal(configuration(next).directory, "/proof/native-browser-v7");
  assert.throws((): void => { configuration({ ...next, TANK_NATIVE_REUSE_CACHE_PROOF_DIR: "/app/data" }); }, /explicit owned/);
  assert.throws((): void => { configuration({ ...next, TANK_NATIVE_REUSE_CACHE_PROOF_DIR: "/proof/native-browser-v7" }); }, /distinct/);
  assert.throws((): void => { configuration({ ...next, TANK_NATIVE_REUSE_CACHE_PROOF_DIR: undefined }); }, /dedicated proof directory/);
});

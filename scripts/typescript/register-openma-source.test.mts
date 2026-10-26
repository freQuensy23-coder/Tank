import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const hook = fileURLToPath(new URL("./register-openma-source.mts", import.meta.url));
const source = process.env.TANK_AUDIT_OPENMA_ROOT === undefined
  ? new URL("../../vendor/openma/", import.meta.url)
  : pathToFileURL(resolve(process.env.TANK_AUDIT_OPENMA_ROOT) + "/");

test("the local source loader admits the actual continuation JSON and history runtime chain", () => {
  const continuation = new URL("packages/session-runtime-contract/src/continuation.ts", source);
  const history = new URL("packages/session-runtime-contract/src/history.ts", source);
  const json = new URL("packages/session-runtime-contract/src/json.ts", source);
  const result = spawnSync(process.execPath, [
    "--import", hook, "--experimental-transform-types", "--input-type=module", "--eval",
    `import assert from "node:assert/strict";
const continuation = await import(${JSON.stringify(continuation.href)});
const history = await import(${JSON.stringify(history.href)});
const json = await import(${JSON.stringify(json.href)});
assert.equal(typeof continuation.planSessionContinuation, "function");
const event = { id: "actual-chain", type: "user.interrupt" };
const encoded = history.encodeSessionEventDocument(event, { revision: 1, index: 0 }, "execution");
assert.equal(json.decodeRuntimeJson(encoded).id, event.id);
assert.throws(() => json.decodeRuntimeJson('{"id":"first","id":"second"}'), json.RuntimeJsonDocumentError);
console.log("actual_continuation_json_history_loaded");`,
  ], { cwd: root, encoding: "utf8", timeout: 5000 });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.trim(), "actual_continuation_json_history_loaded");
});

test("nonmatching, explicitly extended and absent source imports retain real loader refusal", () => {
  const absent = new URL("packages/session-runtime-contract/src/absent-source-file", source);
  const explicit = new URL("packages/session-runtime-contract/src/json.mjs", source);
  const result = spawnSync(process.execPath, [
    "--import", hook, "--input-type=module", "--eval",
    `import assert from "node:assert/strict";
for (const specifier of ["./scripts/typescript/test-report", ${JSON.stringify(absent.href)}, ${JSON.stringify(explicit.href)}]) {
  await assert.rejects(import(specifier), { code: "ERR_MODULE_NOT_FOUND" });
}
console.log("real_loader_refusals_preserved");`,
  ], { cwd: root, encoding: "utf8", timeout: 5000 });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.trim(), "real_loader_refusals_preserved");
});

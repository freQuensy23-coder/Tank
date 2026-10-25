import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { inventory } from "./inventory.mts";

function fixture(t: TestContext): string {
  const root = mkdtempSync(join(tmpdir(), "tank-source-inventory-"));
  for (const area of ["src", "tests", "scripts", "deploy", "capture"]) mkdirSync(join(root, area));
  t.after((): void => rmSync(root, { recursive: true, force: true }));
  return root;
}

test("JavaScript inventory inspects functions without executing source", (t: TestContext): void => {
  const root = fixture(t);
  writeFileSync(join(root, "src/boundary.mjs"), "function parse(payload) { return payload.value ?? {}; }\nthrow Error('must not execute');");
  const report = inventory(root);
  assert.equal(report.files, 1);
  assert.equal(report.counts.untyped_parameter, 1);
  assert.equal(report.counts.untyped_return, 1);
  assert.equal(report.counts.fallback, 1);
});

test("JavaScript inventory preserves explicit immutable fixtures and rejects active syntax errors", (t: TestContext): void => {
  const root = fixture(t);
  mkdirSync(join(root, "tests/fixtures/openma"), { recursive: true });
  writeFileSync(join(root, "tests/fixtures/openma/runtime-codec.ts"), "export function reference(value) { return value; }");
  const reference = inventory(root);
  assert.equal(reference.files, 0);
  assert.equal(reference.reference_programs["tests/fixtures/openma/runtime-codec.ts"]?.findings.length, 2);
  mkdirSync(join(root, "capture/testdata"), { recursive: true });
  writeFileSync(join(root, "capture/testdata/generator.mjs"), "function generate(input) { return input; }");
  assert.equal(inventory(root).files, 1);
  writeFileSync(join(root, "src/broken.mts"), "function broken(");
  assert.throws((): void => { inventory(root); }, /broken.mts/);
});

test("JavaScript inventory exposes embedded active Python source", (t: TestContext): void => {
  const root = fixture(t);
  writeFileSync(join(root, "deploy/probe.mjs"), "const PROBE = String.raw`import json\ndef decode(payload):\n    return payload\n`;");
  const report = inventory(root);
  assert.equal(report.embedded.length, 1);
  assert.equal(report.embedded[0]?.symbol, "PROBE");
  assert.equal(report.embedded[0]?.source, "import json\ndef decode(payload):\n    return payload\n");
});

test("Explicit vendor paths expose Tank source without executing it", (t: TestContext): void => {
  const root = fixture(t);
  mkdirSync(join(root, "vendor/openma"), { recursive: true });
  writeFileSync(join(root, "vendor/openma/tank.mts"), "function worker(payload) { return payload.value ?? {}; }\nthrow Error('must not execute');");
  const report = inventory(root, ["vendor/openma/tank.mts"]);
  assert.equal(report.modules["vendor/openma/tank.mts"]?.untyped_parameter, 1);
  assert.equal(report.modules["vendor/openma/tank.mts"]?.fallback, 1);
  assert.equal(inventory(root).modules["vendor/openma/tank.mts"], undefined);
});

test("Explicit inventory paths cannot escape the declared repository", (t: TestContext): void => {
  const root = fixture(t);
  assert.throws((): void => { inventory(root, ["../outside.mts"]); }, /repository/);
});

test("Encoded Python worker literals retain decoded source and their physical container", (t: TestContext): void => {
  const root = fixture(t);
  const worker = "import json\n\ndef decode(payload):\n    return payload\n";
  writeFileSync(join(root, "deploy/worker.mts"), "export const WORKER = " + JSON.stringify(worker) + ";");
  const report = inventory(root);
  assert.equal(report.embedded.length, 1);
  assert.equal(report.embedded[0]?.encoding, "decoded-literal");
  assert.equal(report.embedded[0]?.line, 1);
  assert.equal(report.embedded[0]?.source, worker);
});

test("Inventory refuses a source symlink that leaves the repository", (t: TestContext): void => {
  const root = fixture(t);
  const outside = mkdtempSync(join(tmpdir(), "tank-inventory-outside-"));
  t.after((): void => rmSync(outside, { recursive: true, force: true }));
  const program = join(outside, "outside.mts");
  writeFileSync(program, "throw Error('must not read outside the repository');");
  symlinkSync(program, join(root, "src/linked.mts"));
  assert.throws((): void => { inventory(root); }, /repository/);
});

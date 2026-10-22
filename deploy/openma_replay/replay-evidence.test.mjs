import { test } from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { verifyReplayEvidence } from "./replay-evidence.ts";
const sha = (value) => createHash("sha256").update(value).digest("hex");
async function fixture(fn, withFile = true) {
  const base = await fs.mkdtemp(join(tmpdir(), "tank-sealed-test-"));
  const root = join(base, "test-run");
  await fs.mkdir(root);
  const bytes = Buffer.from(
    "public test bytes; filesystem contract only, not print validity",
  );
  const hash = sha(bytes);
  const cutoff = "2026-10-18T12:00:00Z";
  const file = {
    relative_path: hash + ".jsonl",
    sha256: hash,
    byte_length: bytes.length,
    row_count: 1,
    kind: "NORMALIZED_TRADES",
    instrument_id: "test",
    source_ref: "captured:one",
    source_sha256: sha("original"),
    source_refs: [
      {
        source_ref: "captured:one",
        source_sha256: sha("original"),
        instrument_id: "test",
        origin_kind: "NORMALIZED_TRADES",
      },
    ],
    guest_path: "/replay/evidence/" + hash + ".jsonl",
    projection_cutoff: cutoff,
    projection_cutoff_prec: "EXACT",
    projection_cutoff_prec_reason: null,
    coverage: [],
  };
  const manifest = {
    schema: "tank.replay-evidence.v1",
    run_id: "test-run",
    input_sha256: sha("input"),
    cutoff,
    cutoff_prec: "EXACT",
    cutoff_prec_reason: null,
    guest_root: "/replay/evidence",
    files: withFile ? [file] : [],
    identity_key: ["instrument_id", "trade_id"],
    format: "jsonl",
    sealed_at: "2026-10-18T13:00:00Z",
    sealed_at_prec: "COLLECTION_ONLY",
    sealed_at_prec_reason: null,
  };
  if (withFile) await fs.writeFile(join(root, file.relative_path), bytes);
  async function seal() {
    const b = Buffer.from(JSON.stringify(manifest) + "\n");
    await fs.writeFile(join(root, "manifest.json"), b);
    return { root: base, runId: "test-run", manifestSha256: sha(b), cutoff };
  }
  try {
    await fn({ root, manifest, file, seal, options: await seal() });
  } finally {
    await fs.rm(base, { recursive: true, force: true });
  }
}
test("binds exact session/run bytes and returns only readonly guest path", () =>
  fixture(async (x) => {
    assert.deepEqual(await verifyReplayEvidence(x.options), {
      hostPath: x.root,
      guestPath: "/replay/evidence",
      readOnly: true,
    });
  }));
test("empty sealed projections remain explicit and valid", () =>
  fixture(async (x) => {
    await verifyReplayEvidence(x.options);
  }, false));
test("run identity and exact manifest hash are independently enforced", () =>
  fixture(async (x) => {
    await assert.rejects(
      verifyReplayEvidence({ ...x.options, runId: "another-run" }),
      /run identity|ENOENT/,
    );
    await assert.rejects(
      verifyReplayEvidence({ ...x.options, manifestSha256: sha("other") }),
      /manifest hash/,
    );
  }));
test("unlisted future/evaluator files refuse entire mount", () =>
  fixture(async (x) => {
    await fs.writeFile(join(x.root, "expected-outcome.json"), "{}");
    await assert.rejects(verifyReplayEvidence(x.options), /unlisted/);
  }));
test("mutation of captured projection refuses mount", () =>
  fixture(async (x) => {
    await fs.writeFile(join(x.root, x.file.relative_path), "changed");
    await assert.rejects(verifyReplayEvidence(x.options), /bytes/);
  }));
test("symlink evidence refuses mount", () =>
  fixture(async (x) => {
    await fs.unlink(join(x.root, x.file.relative_path));
    await fs.symlink("/etc/hosts", join(x.root, x.file.relative_path));
    await assert.rejects(verifyReplayEvidence(x.options), /regular/);
  }));
test("cutoff provenance mismatch refuses mount", () =>
  fixture(async (x) => {
    x.file.projection_cutoff = "2026-10-19T00:00:00Z";
    await assert.rejects(
      verifyReplayEvidence(await x.seal()),
      /cutoff provenance/,
    );
  }));
test("unknown legacy cutoff is retained with reason, never stamped exact", () =>
  fixture(async (x) => {
    x.manifest.cutoff_prec = null;
    x.manifest.cutoff_prec_reason = "INCOMPLETE";
    x.file.projection_cutoff_prec = null;
    x.file.projection_cutoff_prec_reason = "INCOMPLETE";
    await verifyReplayEvidence(await x.seal());
    assert.equal(x.manifest.cutoff_prec, null);
  }));
test("extra manifest payload refuses evaluator metadata", () =>
  fixture(async (x) => {
    x.manifest.expected_outcome = "BUY";
    await assert.rejects(
      verifyReplayEvidence(await x.seal()),
      /unexpected manifest field/,
    );
  }));

test("run metadata cannot choose a path outside controller root", () =>
  fixture(async (x) => {
    await assert.rejects(
      verifyReplayEvidence({ ...x.options, runId: "../test-run" }),
      /session-bound/,
    );
  }));
test("native selected cutoff must match sealed cutoff", () =>
  fixture(async (x) => {
    await assert.rejects(
      verifyReplayEvidence({ ...x.options, cutoff: "2026-10-19T00:00:00Z" }),
      /projection root\/cutoff/,
    );
  }));
test("controller may contain other runs but guest mount is only selected child", () =>
  fixture(async (x) => {
    await fs.mkdir(join(x.options.root, "later-run"));
    await fs.writeFile(
      join(x.options.root, "later-run", "expected-outcome.json"),
      "{}",
    );
    assert.equal((await verifyReplayEvidence(x.options)).hostPath, x.root);
  }));
test("run child itself cannot be a symlink", () =>
  fixture(async (x) => {
    await fs.rename(x.root, join(x.options.root, "real-child"));
    await fs.symlink("real-child", x.root);
    await assert.rejects(
      verifyReplayEvidence(x.options),
      /real sealed directory/,
    );
  }));

test("collection-only cutoff provenance is not promoted or rejected", () =>
  fixture(async (x) => {
    x.manifest.cutoff_prec = "COLLECTION_ONLY";
    x.file.projection_cutoff_prec = "COLLECTION_ONLY";
    await verifyReplayEvidence(await x.seal());
    assert.equal(x.manifest.cutoff_prec, "COLLECTION_ONLY");
  }));

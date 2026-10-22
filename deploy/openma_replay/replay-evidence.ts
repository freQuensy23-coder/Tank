/** Verify one caller-sealed replay projection before exposing its read-only VM mount. */
import { createHash } from "node:crypto";
import { createReadStream, promises as fs } from "node:fs";
import { join, resolve, relative } from "node:path";

const SHA = /^[a-f0-9]{64}$/;
const TOP_FIELDS = new Set([
  "schema",
  "run_id",
  "input_sha256",
  "cutoff",
  "cutoff_prec",
  "cutoff_prec_reason",
  "guest_root",
  "files",
  "identity_key",
  "format",
  "sealed_at",
  "sealed_at_prec",
  "sealed_at_prec_reason",
]);
const FILE_FIELDS = new Set([
  "relative_path",
  "sha256",
  "byte_length",
  "kind",
  "instrument_id",
  "row_count",
  "source_ref",
  "source_sha256",
  "source_refs",
  "guest_path",
  "projection_cutoff",
  "projection_cutoff_prec",
  "projection_cutoff_prec_reason",
  "coverage",
]);
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function text(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}
function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error("Replay evidence: " + message);
}
function precision(value: unknown, reason: unknown) {
  if (value === null) {
    assert(text(reason), "unknown cutoff precision requires its reason");
    return;
  }
  assert(
    value === "EXACT" ||
      value === "COLLECTION_ONLY" ||
      (typeof value === "string" &&
        /^BUCKET\((?:[1-9][0-9]*(?:\.[0-9]+)?|0\.[0-9]*[1-9][0-9]*)(?:us|ms|s|m|h|d|w)\)$/.test(
          value,
        )),
    "cutoff precision must be declared or explicitly unknown",
  );
  assert(
    reason === null,
    "known cutoff precision cannot carry an unknown reason",
  );
}
function closed(value: Record<string, unknown>, allowed: Set<string>) {
  assert(
    Object.keys(value).every((key) => allowed.has(key)),
    "unexpected manifest field",
  );
}
async function regular(path: string) {
  const stat = await fs.lstat(path);
  assert(
    stat.isFile() && !stat.isSymbolicLink(),
    "only regular sealed files are permitted",
  );
  return stat;
}
async function digest(path: string) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

export async function verifyReplayEvidence(options: {
  root?: string;
  runId?: string;
  manifestSha256?: string;
  cutoff?: string;
}) {
  assert(
    text(options.root) &&
      text(options.runId) &&
      /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(options.runId) &&
      text(options.cutoff) &&
      typeof options.manifestSha256 === "string" &&
      SHA.test(options.manifestSha256),
    "selected root and session-bound run/hash are required",
  );
  const base = resolve(options.root);
  const baseStat = await fs.lstat(base);
  assert(
    baseStat.isDirectory() && !baseStat.isSymbolicLink(),
    "controller evidence root must be a real directory",
  );
  const root = join(base, options.runId);
  const rootStat = await fs.lstat(root);
  assert(
    rootStat.isDirectory() && !rootStat.isSymbolicLink(),
    "selected root must be a real sealed directory",
  );
  assert(
    relative(await fs.realpath(base), await fs.realpath(root)) ===
      options.runId,
    "run directory escapes selected evidence root",
  );
  const manifestPath = join(root, "manifest.json");
  await regular(manifestPath);
  const bytes = await fs.readFile(manifestPath);
  assert(
    createHash("sha256").update(bytes).digest("hex") === options.manifestSha256,
    "session manifest hash mismatch",
  );
  const manifest: unknown = JSON.parse(new TextDecoder().decode(bytes));
  assert(object(manifest), "manifest must be an object");
  closed(manifest, TOP_FIELDS);
  assert(
    manifest.schema === "tank.replay-evidence.v1" &&
      manifest.run_id === options.runId,
    "session run identity mismatch",
  );
  assert(
    manifest.guest_root === "/replay/evidence" &&
      text(manifest.cutoff) &&
      manifest.cutoff === options.cutoff,
    "invalid projection root/cutoff",
  );
  precision(manifest.cutoff_prec, manifest.cutoff_prec_reason);
  assert(
    manifest.sealed_at_prec === "COLLECTION_ONLY" &&
      manifest.sealed_at_prec_reason === null &&
      text(manifest.sealed_at),
    "sealing collection provenance is required",
  );
  assert(manifest.format === "jsonl", "unsupported projection format");
  assert(Array.isArray(manifest.files), "files must be an explicit list");
  assert(
    SHA.test(String(manifest.input_sha256)),
    "input provenance hash is required",
  );
  assert(
    JSON.stringify(manifest.identity_key) ===
      JSON.stringify(["instrument_id", "trade_id"]),
    "trade identity mismatch",
  );
  const names = new Set(["manifest.json"]);
  for (const entry of manifest.files) {
    assert(object(entry), "file entry must be an object");
    closed(entry, FILE_FIELDS);
    assert(
      typeof entry.sha256 === "string" && SHA.test(entry.sha256),
      "file hash is required",
    );
    assert(
      entry.relative_path === entry.sha256 + ".jsonl" &&
        !names.has(String(entry.relative_path)),
      "file must have a unique content-addressed flat path",
    );
    assert(
      entry.guest_path === "/replay/evidence/" + entry.relative_path,
      "guest relocation mismatch",
    );
    assert(
      entry.kind === "NORMALIZED_TRADES" && text(entry.instrument_id),
      "only normalized trade projections are permitted",
    );
    assert(
      Number.isSafeInteger(entry.byte_length) &&
        Number(entry.byte_length) > 0 &&
        Number.isSafeInteger(entry.row_count) &&
        Number(entry.row_count) >= 0,
      "invalid file dimensions",
    );
    assert(
      text(entry.source_ref) &&
        typeof entry.source_sha256 === "string" &&
        SHA.test(entry.source_sha256),
      "original source provenance is required",
    );
    assert(
      entry.projection_cutoff === manifest.cutoff &&
        entry.projection_cutoff_prec === manifest.cutoff_prec &&
        entry.projection_cutoff_prec_reason === manifest.cutoff_prec_reason,
      "projection cutoff provenance mismatch",
    );
    assert(
      Array.isArray(entry.coverage) && Array.isArray(entry.source_refs),
      "explicit coverage/source provenance is required",
    );
    for (const source of entry.source_refs) {
      assert(
        object(source) &&
          Object.keys(source).every((key) =>
            [
              "source_ref",
              "source_sha256",
              "instrument_id",
              "origin_kind",
            ].includes(key),
          ),
        "unexpected source provenance field",
      );
      assert(
        source.instrument_id === entry.instrument_id &&
          text(source.source_ref) &&
          typeof source.source_sha256 === "string" &&
          SHA.test(source.source_sha256),
        "source provenance mismatch",
      );
    }
    const path = join(root, entry.relative_path as string);
    assert(
      (await regular(path)).size === entry.byte_length &&
        (await digest(path)) === entry.sha256,
      "sealed file bytes do not match manifest",
    );
    names.add(entry.relative_path as string);
  }
  const actual = await fs.readdir(root);
  assert(
    actual.length === names.size && actual.every((name) => names.has(name)),
    "unlisted files or directories in selected evidence root",
  );
  return {
    hostPath: root,
    guestPath: "/replay/evidence" as const,
    readOnly: true as const,
  };
}

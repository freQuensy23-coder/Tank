import { Hono } from "hono";
import type { Env } from "@open-managed-agents/shared";
import { generateFileId, fileR2Key, guessSessionOutputMime, sessionOutputsPrefix } from "@open-managed-agents/shared";
import {
  toFileRecord,
  FileNotFoundError,
  decodeOutputId,
  unifiedPageErrorMessage,
  unifiedPageHttpBody,
} from "@open-managed-agents/files-store";
import type { Services } from "@open-managed-agents/services";
import { checkUploadFreq, checkUploadSize } from "../quotas";
import { createR2SessionOutputSource } from "../lib/r2-session-output-source";

const app = new Hono<{
  Bindings: Env;
  Variables: { tenant_id: string; services: Services };
}>();

// ─── Session-outputs synthesis ──────────────────────────────────────
//
// Files the agent writes to /mnt/session/outputs/ inside the sandbox land
// in R2 under `t/<tenant>/session-outputs/<session>/<filename>` with no
// D1 row (the mount is bytes-only — listing scans the R2 prefix directly).
// To make these reachable through the standard AMA Files API, we synthesize
// file rows on the fly:
//
//   - LIST /v1/files?scope_id=<sessionId> includes both real D1-backed
//     files AND R2 objects under the session-outputs prefix. Pages walk
//     D1 (created_at, id) then R2 filenames. `next_cursor` resumes across
//     that boundary; `before_id` accepts either a file id or that cursor.
//   - GET /v1/files/:id and /content recognize ids matching `out:<sessionId>:
//     <base64url(filename)>` and read R2 directly with no D1 round-trip.
//
// Wire id format is opaque to the SDK; format is stable and self-describing
// so we never need a backing index. base64url encoding so filenames with
// special chars (spaces, slashes — though slashes shouldn't reach here)
// don't break URL routing. The codec lives in files-store so Node and
// Cloudflare mint the same ids.

interface ApiFileRecord {
  id: string;
  type: "file";
  filename: string;
  media_type: string;
  size_bytes: number;
  created_at: string;
  scope?: { type: "session"; id: string };
  downloadable?: boolean;
}

// POST /v1/files — upload file (multipart form or JSON body)
app.post("/", async (c) => {
  const t = c.get("tenant_id");
  // Cheap upfront rejects so a flood of oversized / over-frequent uploads
  // doesn't even read the body. Both gates soft-pass when unconfigured.
  const sizeCheck = checkUploadSize(c.env, c.req.raw);
  if (sizeCheck) return sizeCheck;
  const freqCheck = await checkUploadFreq(c.env, t);
  if (freqCheck) return freqCheck;

  const bucket = c.var.services.filesBlob;
  if (!bucket) return c.json({ error: "FILES_BUCKET binding not configured" }, 500);

  let filename: string;
  let mediaType: string;
  let body: ArrayBuffer;
  let scopeId: string | undefined;
  let downloadable = false;

  const contentType = c.req.header("content-type") || "";

  if (contentType.includes("multipart/form-data")) {
    const formData = await c.req.formData();
    const file = formData.get("file") as File | null;
    if (!file) {
      return c.json({ error: "file field is required in multipart upload" }, 400);
    }
    filename = file.name;
    mediaType = file.type || "application/octet-stream";
    body = await file.arrayBuffer();
    const sc = formData.get("scope_id");
    if (typeof sc === "string") scopeId = sc;
    const d = formData.get("downloadable");
    if (typeof d === "string") downloadable = d === "true" || d === "1";
  } else {
    // JSON body upload — content is base64-encoded for binary, raw text for text/*
    const json = await c.req.json<{
      filename: string;
      content: string;
      media_type?: string;
      scope_id?: string;
      encoding?: "base64" | "utf8";
      downloadable?: boolean;
    }>();

    if (!json.filename || json.content === undefined || json.content === null) {
      return c.json({ error: "filename and content are required" }, 400);
    }
    filename = json.filename;
    mediaType = json.media_type || "application/octet-stream";
    scopeId = json.scope_id;
    downloadable = json.downloadable === true;

    const encoding = json.encoding || (mediaType.startsWith("text/") ? "utf8" : "base64");
    if (encoding === "base64") {
      const bin = atob(json.content);
      const bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      body = bytes.buffer;
    } else {
      body = new TextEncoder().encode(json.content).buffer as ArrayBuffer;
    }
  }

  const id = generateFileId();
  const r2Key = fileR2Key(t, id);
  // R2 PUT first, then metadata insert — same failure semantics as the KV era
  // (orphan R2 object on metadata failure, never the reverse).
  await bucket.put(r2Key, body, { httpMetadata: { contentType: mediaType } });

  const row = await c.var.services.files.create({
    id,
    tenantId: t,
    sessionId: scopeId,
    filename,
    mediaType,
    sizeBytes: body.byteLength,
    r2Key,
    downloadable,
  });

  return c.json(toFileRecord(row), 201);
});

// GET /v1/files — list files (cursor-paginated, optional scope_id filter)
app.get("/", async (c) => {
  const t = c.get("tenant_id");
  const scopeId = c.req.query("scope_id");
  const limitParam = c.req.query("limit");
  const beforeId = c.req.query("before_id");
  const afterId = c.req.query("after_id");
  const cursor = c.req.query("cursor");
  const order = c.req.query("order") === "asc" ? "asc" : "desc";

  let requested = limitParam ? parseInt(limitParam, 10) : 100;
  if (isNaN(requested) || requested < 1) requested = 100;
  if (requested > 1000) requested = 1000;

  // Session scope merges the files table with the R2 session-outputs
  // prefix. `cursor` (also accepted as a `fcur1.` before_id) resumes
  // across that boundary. Unscoped lists stay on the files table only.
  if (scopeId) {
    const bucket = c.env.FILES_BUCKET;
    const page = await c.var.services.files.listUnifiedPage({
      tenantId: t,
      scopeId,
      limit: requested,
      order,
      cursor,
      beforeId,
      afterId,
      outputs: bucket ? createR2SessionOutputSource(bucket) : null,
    });
    if (!page.ok) return c.json({ error: unifiedPageErrorMessage(page.error) }, 400);
    return c.json(unifiedPageHttpBody(page));
  }

  // Ask for one extra row so we can derive `has_more` without a count query.
  const rows = await c.var.services.files.list({
    tenantId: t,
    sessionId: scopeId,
    beforeId,
    afterId,
    order,
    limit: requested + 1,
  });

  const slice = rows.slice(0, requested);
  const data: ApiFileRecord[] = slice.map(toFileRecord) as ApiFileRecord[];
  const hasMore = rows.length > requested;

  return c.json({
    data,
    has_more: hasMore,
    first_id: data[0]?.id,
    last_id: data[data.length - 1]?.id,
  });
});

// GET /v1/files/:id — get file metadata
app.get("/:id", async (c) => {
  const id = c.req.param("id");
  const t = c.get("tenant_id");

  // Synthesized session-output id: derive metadata from R2 directly,
  // no D1 round-trip needed.
  const decoded = decodeOutputId(id);
  if (decoded) {
    const bucket = c.env.FILES_BUCKET;
    if (!bucket) return c.json({ error: "FILES_BUCKET binding not configured" }, 500);
    const r2Key = `${sessionOutputsPrefix(t, decoded.sessionId)}${decoded.filename}`;
    const head = await bucket.head(r2Key);
    if (!head) return c.json({ error: "File not found" }, 404);
    const record: ApiFileRecord = {
      id,
      type: "file",
      filename: decoded.filename,
      media_type: head.httpMetadata?.contentType || guessSessionOutputMime(decoded.filename),
      size_bytes: head.size,
      created_at: head.uploaded.toISOString(),
      scope: { type: "session", id: decoded.sessionId },
      downloadable: true,
    };
    return c.json(record);
  }

  const row = await c.var.services.files.get({
    tenantId: t,
    fileId: id,
  });
  if (!row) return c.json({ error: "File not found" }, 404);
  return c.json(toFileRecord(row));
});

// GET /v1/files/:id/content — download file content (streamed from R2).
// Gated by `downloadable` flag, mirroring Anthropic's split: user-uploaded
// files are opaque, model/sandbox-emitted artefacts are downloadable.
app.get("/:id/content", async (c) => {
  const id = c.req.param("id");
  const t = c.get("tenant_id");
  const r2 = c.env.FILES_BUCKET;
  const bucket = c.var.services.filesBlob;
  if (!r2 || !bucket) return c.json({ error: "FILES_BUCKET binding not configured" }, 500);

  // Synthesized session-output id: stream R2 directly.
  const decoded = decodeOutputId(id);
  if (decoded) {
    const r2Key = `${sessionOutputsPrefix(t, decoded.sessionId)}${decoded.filename}`;
    const obj = await r2.get(r2Key);
    if (!obj) return c.json({ error: "File content not found" }, 404);
    return new Response(obj.body, {
      headers: {
        "Content-Type": obj.httpMetadata?.contentType || guessSessionOutputMime(decoded.filename),
      },
    });
  }

  const row = await c.var.services.files.get({
    tenantId: t,
    fileId: id,
  });
  if (!row) return c.json({ error: "File not found" }, 404);
  if (!row.downloadable) {
    return c.json({ error: "This file is not downloadable" }, 403);
  }

  const obj = await bucket.get(row.r2_key);
  if (!obj) return c.json({ error: "File content not found" }, 404);

  return new Response(obj.body, {
    headers: { "Content-Type": row.media_type },
  });
});

// DELETE /v1/files/:id — delete metadata + R2 object
app.delete("/:id", async (c) => {
  const bucket = c.var.services.filesBlob;
  try {
    const deleted = await c.var.services.files.delete({
      tenantId: c.get("tenant_id"),
      fileId: c.req.param("id"),
    });
    if (bucket) await bucket.delete(deleted.r2_key);
    return c.json({ type: "file_deleted", id: deleted.id });
  } catch (err) {
    if (err instanceof FileNotFoundError) {
      return c.json({ error: "File not found" }, 404);
    }
    throw err;
  }
});

export default app;

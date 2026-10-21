// Scoped file listing across the `files` table and the session-outputs
// blob prefix.
//
// Order (stable, same on every page):
//   1. D1/SQL rows for the session, by (created_at, id) in the requested
//      direction. `desc` is the API default.
//   2. Session-output objects, by filename ascending. Blob listings are
//      lexicographic; filename order is the order R2 can resume with
//      `startAfter` without rereading keys that were already returned.
//
// The cursor embeds that position. It does not point at a row that must
// still exist, so deleting the last item of a page does not skip or
// repeat the rest. A raw file id passed as `before_id` / `after_id` is
// resolved once; an `out:` id carries its filename, so it still resumes
// after the object is deleted.
//
// Concurrent writes (keyset):
//   - An insert that sorts after the cursor (not yet visited) is returned
//     exactly once on a later page.
//   - An insert that sorts before the cursor (already visited, including
//     a new D1 row after the cursor has entered the output phase) is not
//     returned. Seeing it requires a fresh list from the start. Pulling
//     it into a later page would duplicate or reshuffle the sequence.
//   - A delete of a not-yet-returned item omits that item and still
//     returns its neighbors.

import { guessSessionOutputMime } from "@open-managed-agents/shared";
import { decodeOutputId, encodeOutputId } from "./output-id";
import type { FileKeyset, FileRepo } from "./ports";
import type { FileRow } from "./types";

export const UNIFIED_FILE_CURSOR_PREFIX = "fcur1.";
const MAX_CURSOR_CHARS = 4096;

export interface SessionOutputObject {
  filename: string;
  sizeBytes: number;
  uploadedAtMs: number;
  mediaType?: string;
}

export interface SessionOutputPageSource {
  /**
   * Filenames strictly greater than `startAfterFilename`, ascending,
   * at most `limit` objects. Fewer than `limit` means the source is
   * exhausted. Order must match JavaScript `<` for BMP filenames, which
   * is also R2's lexicographic key order for those names.
   */
  listAfter(input: {
    tenantId: string;
    sessionId: string;
    limit: number;
    startAfterFilename?: string;
  }): Promise<SessionOutputObject[]>;
}

export interface UnifiedFileItem {
  source: "d1" | "r2";
  id: string;
  filename: string;
  mediaType: string;
  sizeBytes: number;
  createdAt: string;
  createdAtMs: number;
  sessionId: string | null;
  downloadable: boolean;
}

export interface ListedFile {
  id: string;
  type: "file";
  filename: string;
  media_type: string;
  size_bytes: number;
  created_at: string;
  scope_id?: string;
  scope?: { type: "session"; id: string };
  downloadable: boolean;
}

export type UnifiedPageError =
  | "invalid_cursor"
  | "anchor_not_found"
  | "cursor_scope_mismatch"
  | "cursor_order_mismatch"
  | "conflicting_cursors";

export type UnifiedPageResult =
  | { ok: true; items: UnifiedFileItem[]; hasMore: boolean; nextCursor?: string }
  | { ok: false; error: UnifiedPageError };

export interface UnifiedListQuery {
  tenantId: string;
  scopeId: string;
  limit: number;
  order: "asc" | "desc";
  cursor?: string;
  beforeId?: string;
  afterId?: string;
}

interface DecodedCursor {
  order: "asc" | "desc";
  scopeId: string;
  phase: "d1" | "r2";
  createdAtMs?: number;
  id?: string;
  afterFilename?: string;
}

type ForwardStart =
  | { phase: "start" }
  | { phase: "d1"; createdAtMs: number; id: string }
  | { phase: "r2"; afterFilename?: string };

const ERROR_MESSAGES: Record<UnifiedPageError, string> = {
  invalid_cursor: "Invalid pagination cursor",
  anchor_not_found: "Pagination anchor was not found in this scope",
  cursor_scope_mismatch: "Pagination cursor does not match scope_id",
  cursor_order_mismatch: "Pagination cursor does not match order",
  conflicting_cursors: "Pagination accepts only one of cursor, before_id, or after_id",
};

export function unifiedPageErrorMessage(error: UnifiedPageError): string {
  return ERROR_MESSAGES[error];
}

export function toListedFile(item: UnifiedFileItem): ListedFile {
  const base: ListedFile = {
    id: item.id,
    type: "file",
    filename: item.filename,
    media_type: item.mediaType,
    size_bytes: item.sizeBytes,
    created_at: item.createdAt,
    downloadable: item.downloadable,
  };
  if (!item.sessionId) return base;
  if (item.source === "r2") {
    return {
      ...base,
      scope_id: item.sessionId,
      scope: { type: "session", id: item.sessionId },
    };
  }
  return { ...base, scope_id: item.sessionId };
}

export function unifiedPageHttpBody(page: Extract<UnifiedPageResult, { ok: true }>): {
  data: ListedFile[];
  has_more: boolean;
  first_id?: string;
  last_id?: string;
  next_cursor?: string;
} {
  const data = page.items.map(toListedFile);
  return {
    data,
    has_more: page.hasMore,
    ...(data[0] ? { first_id: data[0].id } : {}),
    ...(data.length > 0 ? { last_id: data[data.length - 1]!.id } : {}),
    ...(page.nextCursor ? { next_cursor: page.nextCursor } : {}),
  };
}

export function compareFilename(a: string, b: string): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

export async function listUnifiedSessionFiles(
  repo: FileRepo,
  query: UnifiedListQuery,
  outputs: SessionOutputPageSource | null,
): Promise<UnifiedPageResult> {
  const limit = normalizeLimit(query.limit);
  const direction = resolveDirection(query);
  if (!direction.ok) return direction;

  if (direction.mode === "forward") {
    const start = await resolveForwardStart(repo, query, direction.token);
    if (!start.ok) return start;
    const page = await readForward(repo, outputs, query, start.start, limit);
    return finishForward(page, query);
  }

  const anchor = await resolveAnchor(repo, query, direction.anchorId);
  if (!anchor.ok) return anchor;
  return readBackward(repo, outputs, query, anchor.anchor, limit);
}

function normalizeLimit(limit: number): number {
  if (!Number.isFinite(limit) || limit < 1) return 1;
  return Math.floor(limit);
}

function present(value: string | undefined): value is string {
  return value !== undefined && value !== "";
}

function resolveDirection(query: UnifiedListQuery):
  | { ok: true; mode: "forward"; token?: string }
  | { ok: true; mode: "backward"; anchorId: string }
  | { ok: false; error: UnifiedPageError } {
  const hasCursor = present(query.cursor);
  const hasBefore = present(query.beforeId);
  const hasAfter = present(query.afterId);
  if ((hasCursor && hasBefore) || (hasCursor && hasAfter) || (hasBefore && hasAfter)) {
    return { ok: false, error: "conflicting_cursors" };
  }
  if (hasAfter) return { ok: true, mode: "backward", anchorId: query.afterId! };
  if (hasCursor) return { ok: true, mode: "forward", token: query.cursor };
  if (hasBefore) return { ok: true, mode: "forward", token: query.beforeId };
  return { ok: true, mode: "forward" };
}

async function resolveForwardStart(
  repo: FileRepo,
  query: UnifiedListQuery,
  token: string | undefined,
): Promise<{ ok: true; start: ForwardStart } | { ok: false; error: UnifiedPageError }> {
  if (!present(token)) return { ok: true, start: { phase: "start" } };
  if (token.startsWith(UNIFIED_FILE_CURSOR_PREFIX)) {
    const decoded = decodeCursor(token);
    if (!decoded) return { ok: false, error: "invalid_cursor" };
    if (decoded.scopeId !== query.scopeId) return { ok: false, error: "cursor_scope_mismatch" };
    if (decoded.order !== query.order) return { ok: false, error: "cursor_order_mismatch" };
    if (decoded.phase === "d1") {
      return {
        ok: true,
        start: { phase: "d1", createdAtMs: decoded.createdAtMs!, id: decoded.id! },
      };
    }
    return { ok: true, start: { phase: "r2", afterFilename: decoded.afterFilename } };
  }
  const anchor = await resolveAnchor(repo, query, token);
  if (!anchor.ok) return anchor;
  if (anchor.anchor.phase === "d1") {
    return {
      ok: true,
      start: { phase: "d1", createdAtMs: anchor.anchor.createdAtMs, id: anchor.anchor.id },
    };
  }
  return { ok: true, start: { phase: "r2", afterFilename: anchor.anchor.filename } };
}

type Anchor =
  | { phase: "d1"; createdAtMs: number; id: string }
  | { phase: "r2"; filename: string };

async function resolveAnchor(
  repo: FileRepo,
  query: UnifiedListQuery,
  id: string,
): Promise<{ ok: true; anchor: Anchor } | { ok: false; error: UnifiedPageError }> {
  if (id.startsWith(UNIFIED_FILE_CURSOR_PREFIX)) {
    return { ok: false, error: "invalid_cursor" };
  }
  const output = decodeOutputId(id);
  if (output) {
    if (output.sessionId !== query.scopeId) return { ok: false, error: "cursor_scope_mismatch" };
    return { ok: true, anchor: { phase: "r2", filename: output.filename } };
  }
  const row = await repo.get(query.tenantId, id);
  if (!row || row.session_id !== query.scopeId) return { ok: false, error: "anchor_not_found" };
  const createdAtMs = Date.parse(row.created_at);
  if (!Number.isFinite(createdAtMs)) return { ok: false, error: "anchor_not_found" };
  return { ok: true, anchor: { phase: "d1", createdAtMs, id: row.id } };
}

async function readForward(
  repo: FileRepo,
  outputs: SessionOutputPageSource | null,
  query: UnifiedListQuery,
  start: ForwardStart,
  limit: number,
): Promise<{ items: UnifiedFileItem[]; hasMore: boolean }> {
  const items: UnifiedFileItem[] = [];
  if (start.phase !== "r2") {
    const after: FileKeyset | undefined = start.phase === "d1"
      ? { createdAtMs: start.createdAtMs, id: start.id }
      : undefined;
    const rows = await repo.listKeyset(query.tenantId, {
      sessionId: query.scopeId,
      order: query.order,
      limit: limit + 1,
      after,
    });
    for (const row of rows.slice(0, limit)) items.push(d1Item(row));
    if (rows.length > limit) return { items, hasMore: true };
    const need = limit - items.length;
    if (!outputs) return { items, hasMore: false };
    if (need === 0) {
      const peek = await outputs.listAfter({
        tenantId: query.tenantId,
        sessionId: query.scopeId,
        limit: 1,
      });
      return { items, hasMore: peek.length > 0 };
    }
    const objects = await outputs.listAfter({
      tenantId: query.tenantId,
      sessionId: query.scopeId,
      limit: need + 1,
    });
    for (const object of objects.slice(0, need)) items.push(r2Item(query.scopeId, object));
    return { items, hasMore: objects.length > need };
  }

  if (!outputs) return { items, hasMore: false };
  const objects = await outputs.listAfter({
    tenantId: query.tenantId,
    sessionId: query.scopeId,
    limit: limit + 1,
    startAfterFilename: start.afterFilename,
  });
  for (const object of objects.slice(0, limit)) items.push(r2Item(query.scopeId, object));
  return { items, hasMore: objects.length > limit };
}

function finishForward(
  page: { items: UnifiedFileItem[]; hasMore: boolean },
  query: UnifiedListQuery,
): UnifiedPageResult {
  if (page.items.length === 0) return { ok: true, items: [], hasMore: false };
  const last = page.items[page.items.length - 1]!;
  return {
    ok: true,
    items: page.items,
    hasMore: page.hasMore,
    ...(page.hasMore ? { nextCursor: encodeCursor(cursorAfter(last, query)) } : {}),
  };
}

async function readBackward(
  repo: FileRepo,
  outputs: SessionOutputPageSource | null,
  query: UnifiedListQuery,
  anchor: Anchor,
  limit: number,
): Promise<UnifiedPageResult> {
  if (anchor.phase === "d1") {
    const rows = await repo.listKeyset(query.tenantId, {
      sessionId: query.scopeId,
      order: opposite(query.order),
      limit: limit + 1,
      after: { createdAtMs: anchor.createdAtMs, id: anchor.id },
    });
    const items = rows.slice(0, limit).reverse().map(d1Item);
    return { ok: true, items, hasMore: rows.length > limit };
  }

  const before = await outputsBefore(outputs, query, anchor.filename, limit);
  if (before.items.length === limit) {
    const hasMore = before.hasMore || await hasAnyD1(repo, query);
    return { ok: true, items: before.items, hasMore };
  }
  const need = limit - before.items.length;
  const tail = await d1Tail(repo, query, need);
  return {
    ok: true,
    items: [...tail.items, ...before.items],
    hasMore: tail.hasMore || before.hasMore,
  };
}

async function outputsBefore(
  outputs: SessionOutputPageSource | null,
  query: UnifiedListQuery,
  filename: string,
  limit: number,
): Promise<{ items: UnifiedFileItem[]; hasMore: boolean }> {
  if (!outputs || limit < 1) return { items: [], hasMore: false };
  const cap = limit + 1;
  let window: SessionOutputObject[] = [];
  let startAfter: string | undefined;
  for (let guard = 0; guard < 10_000; guard++) {
    const batch = await outputs.listAfter({
      tenantId: query.tenantId,
      sessionId: query.scopeId,
      limit: 1000,
      startAfterFilename: startAfter,
    });
    if (batch.length === 0) break;
    const lastName = batch[batch.length - 1]!.filename;
    if (startAfter !== undefined && !(lastName > startAfter)) break;
    let reachedAnchor = false;
    for (const object of batch) {
      if (!(object.filename < filename)) {
        reachedAnchor = true;
        break;
      }
      window.push(object);
      if (window.length > cap) window.shift();
    }
    if (reachedAnchor || batch.length < 1000) break;
    startAfter = lastName;
  }
  const page = window.length > limit ? window.slice(window.length - limit) : window;
  return {
    items: page.map((object) => r2Item(query.scopeId, object)),
    hasMore: window.length > limit,
  };
}

async function d1Tail(
  repo: FileRepo,
  query: UnifiedListQuery,
  need: number,
): Promise<{ items: UnifiedFileItem[]; hasMore: boolean }> {
  if (need < 1) return { items: [], hasMore: false };
  const rows = await repo.listKeyset(query.tenantId, {
    sessionId: query.scopeId,
    // Walk from the end of the D1 segment back toward the start.
    order: opposite(query.order),
    limit: need + 1,
  });
  return {
    items: rows.slice(0, need).reverse().map(d1Item),
    hasMore: rows.length > need,
  };
}

async function hasAnyD1(repo: FileRepo, query: UnifiedListQuery): Promise<boolean> {
  const rows = await repo.listKeyset(query.tenantId, {
    sessionId: query.scopeId,
    order: query.order,
    limit: 1,
  });
  return rows.length > 0;
}

function opposite(order: "asc" | "desc"): "asc" | "desc" {
  return order === "desc" ? "asc" : "desc";
}

function d1Item(row: FileRow): UnifiedFileItem {
  const createdAtMs = Date.parse(row.created_at);
  return {
    source: "d1",
    id: row.id,
    filename: row.filename,
    mediaType: row.media_type,
    sizeBytes: row.size_bytes,
    createdAt: row.created_at,
    createdAtMs: Number.isFinite(createdAtMs) ? createdAtMs : 0,
    sessionId: row.session_id,
    downloadable: row.downloadable,
  };
}

function r2Item(sessionId: string, object: SessionOutputObject): UnifiedFileItem {
  const createdAtMs = Number.isFinite(object.uploadedAtMs) ? object.uploadedAtMs : 0;
  return {
    source: "r2",
    id: encodeOutputId(sessionId, object.filename),
    filename: object.filename,
    mediaType: object.mediaType || guessSessionOutputMime(object.filename),
    sizeBytes: object.sizeBytes,
    createdAt: new Date(createdAtMs).toISOString(),
    createdAtMs,
    sessionId,
    downloadable: true,
  };
}

function cursorAfter(item: UnifiedFileItem, query: UnifiedListQuery): DecodedCursor {
  if (item.source === "d1") {
    return {
      order: query.order,
      scopeId: query.scopeId,
      phase: "d1",
      createdAtMs: item.createdAtMs,
      id: item.id,
    };
  }
  return {
    order: query.order,
    scopeId: query.scopeId,
    phase: "r2",
    afterFilename: item.filename,
  };
}

function encodeCursor(cursor: DecodedCursor): string {
  const body: Record<string, unknown> = {
    v: 1,
    o: cursor.order,
    s: cursor.scopeId,
    p: cursor.phase,
  };
  if (cursor.phase === "d1") {
    body.t = cursor.createdAtMs;
    body.i = cursor.id;
  } else if (cursor.afterFilename !== undefined) {
    body.f = cursor.afterFilename;
  }
  const bytes = new TextEncoder().encode(JSON.stringify(body));
  return UNIFIED_FILE_CURSOR_PREFIX + encodeBase64Url(bytes);
}

function decodeCursor(token: string): DecodedCursor | null {
  if (!token.startsWith(UNIFIED_FILE_CURSOR_PREFIX) || token.length > MAX_CURSOR_CHARS) return null;
  try {
    const bytes = decodeBase64Url(token.slice(UNIFIED_FILE_CURSOR_PREFIX.length));
    const parsed = JSON.parse(new TextDecoder().decode(bytes)) as {
      v?: unknown;
      o?: unknown;
      s?: unknown;
      p?: unknown;
      t?: unknown;
      i?: unknown;
      f?: unknown;
    };
    if (parsed.v !== 1 || (parsed.o !== "asc" && parsed.o !== "desc")) return null;
    if (typeof parsed.s !== "string" || parsed.s.length === 0) return null;
    if (parsed.p === "d1") {
      if (typeof parsed.t !== "number" || !Number.isFinite(parsed.t)) return null;
      if (typeof parsed.i !== "string" || parsed.i.length === 0) return null;
      return { order: parsed.o, scopeId: parsed.s, phase: "d1", createdAtMs: parsed.t, id: parsed.i };
    }
    if (parsed.p === "r2") {
      if (parsed.f !== undefined && (typeof parsed.f !== "string" || parsed.f.length === 0)) return null;
      return {
        order: parsed.o,
        scopeId: parsed.s,
        phase: "r2",
        ...(typeof parsed.f === "string" ? { afterFilename: parsed.f } : {}),
      };
    }
    return null;
  } catch {
    return null;
  }
}

function encodeBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function decodeBase64Url(value: string): Uint8Array {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/")
    + "===".slice((value.length + 3) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

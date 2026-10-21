import { expect, it } from "vitest";
import { encodeOutputId } from "./output-id";
import type { UnifiedPageHarness } from "./unified-page.harness";
import type { UnifiedPageResult } from "./unified-page";


export interface UnifiedPageScenarioOptions {
  /** When > 0, also walk a filename run long enough to cross a 1000-key list cap. */
  bulkOutputCount?: number;
}

export function registerUnifiedFilePageScenarios(
  createHarness: () => Promise<UnifiedPageHarness>,
  options: UnifiedPageScenarioOptions = {},
): void {
  it("walks an empty scope as one empty page", async () => {
    const harness = await createHarness();
    const page = ok(await harness.list({ limit: 2 }));
    expect(page.items).toEqual([]);
    expect(page.hasMore).toBe(false);
    expect(page.nextCursor).toBeUndefined();
  });

  it("does not emit an empty page at the D1/R2 boundary", async () => {
    const harness = await createHarness();
    await harness.insertFile({ id: "file-new", createdAtMs: 400 });
    await harness.insertFile({ id: "file-old", createdAtMs: 100 });
    await harness.insertOutput({ filename: "a.txt" });
    await harness.insertOutput({ filename: "b.txt" });

    const exact = await walk(harness, 2);
    expect(exact).toEqual([
      "file-new",
      "file-old",
      encodeOutputId(harness.sessionId, "a.txt"),
      encodeOutputId(harness.sessionId, "b.txt"),
    ]);

    const split = await walk(harness, 3);
    expect(split).toEqual(exact);

    const past = ok(await harness.list({
      limit: 2,
      beforeId: encodeOutputId(harness.sessionId, "b.txt"),
    }));
    expect(past.items).toEqual([]);
    expect(past.hasMore).toBe(false);
  });

  it("keeps a stable order when created_at ties and when order=asc", async () => {
    const harness = await createHarness();
    await harness.insertFile({ id: "file-b", createdAtMs: 100 });
    await harness.insertFile({ id: "file-a", createdAtMs: 100 });
    await harness.insertOutput({ filename: "m.txt" });
    await harness.insertOutput({ filename: "a.txt" });

    const desc = await walk(harness, 1, "desc");
    expect(desc).toEqual([
      "file-b",
      "file-a",
      encodeOutputId(harness.sessionId, "a.txt"),
      encodeOutputId(harness.sessionId, "m.txt"),
    ]);
    expect(await walk(harness, 1, "desc")).toEqual(desc);

    const asc = await walk(harness, 2, "asc");
    expect(asc).toEqual([
      "file-a",
      "file-b",
      encodeOutputId(harness.sessionId, "a.txt"),
      encodeOutputId(harness.sessionId, "m.txt"),
    ]);
  });

  it("resumes from a raw id the same way as next_cursor", async () => {
    const harness = await createHarness();
    await harness.insertFile({ id: "file-new", createdAtMs: 300 });
    await harness.insertFile({ id: "file-old", createdAtMs: 200 });
    await harness.insertOutput({ filename: "a.txt" });
    await harness.insertOutput({ filename: "b.txt" });

    const first = ok(await harness.list({ limit: 1 }));
    const byCursor = ok(await harness.list({ limit: 2, cursor: first.nextCursor }));
    const byId = ok(await harness.list({ limit: 2, beforeId: first.items[0]!.id }));
    expect(byId.items.map((item) => item.id)).toEqual(byCursor.items.map((item) => item.id));
    expect(byId.items.map((item) => item.id)).toEqual([
      "file-old",
      encodeOutputId(harness.sessionId, "a.txt"),
    ]);

    const mid = ok(await harness.list({ limit: 1, cursor: byCursor.nextCursor }));
    expect(mid.items.map((item) => item.filename)).toEqual(["b.txt"]);
    const byOutputId = ok(await harness.list({
      limit: 1,
      beforeId: encodeOutputId(harness.sessionId, "a.txt"),
    }));
    expect(byOutputId.items.map((item) => item.id)).toEqual(mid.items.map((item) => item.id));
  });

  it("pages backward across the D1/R2 boundary without a hole", async () => {
    const harness = await createHarness();
    await harness.insertFile({ id: "file-new", createdAtMs: 300 });
    await harness.insertFile({ id: "file-mid", createdAtMs: 200 });
    await harness.insertFile({ id: "file-old", createdAtMs: 100 });
    await harness.insertOutput({ filename: "a.txt" });
    await harness.insertOutput({ filename: "b.txt" });
    await harness.insertOutput({ filename: "c.txt" });

    const back = ok(await harness.list({
      limit: 2,
      afterId: encodeOutputId(harness.sessionId, "c.txt"),
    }));
    expect(back.items.map((item) => item.filename)).toEqual(["a.txt", "b.txt"]);
    expect(back.hasMore).toBe(true);

    const across = ok(await harness.list({
      limit: 2,
      afterId: encodeOutputId(harness.sessionId, "a.txt"),
    }));
    expect(across.items.map((item) => item.id)).toEqual(["file-mid", "file-old"]);
    expect(across.hasMore).toBe(true);

    const head = ok(await harness.list({ limit: 5, afterId: "file-new" }));
    expect(head.items).toEqual([]);
    expect(head.hasMore).toBe(false);
  });

  it("rejects conflicting, foreign, and corrupt cursors", async () => {
    const harness = await createHarness();
    await harness.insertFile({ id: "file-new", createdAtMs: 300 });
    await harness.insertOutput({ filename: "a.txt" });
    const first = ok(await harness.list({ limit: 1 }));

    expect((await harness.list({
      limit: 1,
      cursor: first.nextCursor,
      beforeId: "file-new",
    })).ok).toBe(false);
    expect(await harness.list({ beforeId: "file-new", afterId: "file-new" })).toMatchObject({
      ok: false,
      error: "conflicting_cursors",
    });
    expect(await harness.list({ cursor: "fcur1.%%%%" })).toMatchObject({
      ok: false,
      error: "invalid_cursor",
    });
    expect(await harness.list({ beforeId: "file-missing" })).toMatchObject({
      ok: false,
      error: "anchor_not_found",
    });
    expect(await harness.list({
      scopeId: "other-session",
      cursor: first.nextCursor,
    })).toMatchObject({ ok: false, error: "cursor_scope_mismatch" });
    expect(await harness.list({
      order: "asc",
      cursor: first.nextCursor,
    })).toMatchObject({ ok: false, error: "cursor_order_mismatch" });
  });

  it("hides other tenants and sessions", async () => {
    const harness = await createHarness();
    await harness.insertFile({ id: "file-mine", createdAtMs: 300 });
    await harness.insertFile({
      id: "file-other-tenant",
      createdAtMs: 400,
      tenantId: harness.otherTenantId,
    });
    await harness.insertFile({
      id: "file-other-session",
      createdAtMs: 500,
      sessionId: harness.otherSessionId,
    });
    await harness.insertOutput({ filename: "mine.txt" });
    await harness.insertOutput({
      filename: "other.txt",
      tenantId: harness.otherTenantId,
    });
    await harness.insertOutput({
      filename: "elsewhere.txt",
      sessionId: harness.otherSessionId,
    });
    expect(await walk(harness, 10)).toEqual([
      "file-mine",
      encodeOutputId(harness.sessionId, "mine.txt"),
    ]);
  });

  it("does not duplicate or skip when rows are written between pages", async () => {
    const harness = await createHarness();
    await harness.insertFile({ id: "file-a", createdAtMs: 300 });
    await harness.insertFile({ id: "file-b", createdAtMs: 200 });
    await harness.insertOutput({ filename: "m.txt" });
    await harness.insertOutput({ filename: "p.txt" });

    const first = ok(await harness.list({ limit: 1 }));
    expect(first.items.map((item) => item.id)).toEqual(["file-a"]);

    await harness.insertFile({ id: "file-c", createdAtMs: 250 });
    await harness.insertFile({ id: "file-z", createdAtMs: 400 });
    await harness.deleteFile("file-b");
    await harness.insertOutput({ filename: "a.txt" });
    await harness.insertOutput({ filename: "z.txt" });

    const second = ok(await harness.list({ limit: 1, cursor: first.nextCursor }));
    expect(second.items.map((item) => item.id)).toEqual(["file-c"]);

    const third = ok(await harness.list({ limit: 1, cursor: second.nextCursor }));
    expect(third.items.map((item) => item.filename)).toEqual(["a.txt"]);

    await harness.insertOutput({ filename: "0.txt" });
    await harness.insertOutput({ filename: "n.txt" });
    await harness.deleteFile("file-a");

    const rest: string[] = [];
    let cursor = third.nextCursor;
    for (let i = 0; i < 10; i++) {
      const page = ok(await harness.list({ limit: 1, cursor }));
      if (page.items.length === 0) break;
      rest.push(page.items[0]!.filename);
      if (!page.hasMore) break;
      cursor = page.nextCursor;
    }
    expect(rest).toEqual(["m.txt", "n.txt", "p.txt", "z.txt"]);

    const seen = ["file-a", "file-c", ...rest.map((name) => encodeOutputId(harness.sessionId, name))];
    expect(new Set(seen).size).toBe(seen.length);
    expect(seen).not.toContain("file-z");
    expect(seen).not.toContain("file-b");
    expect(seen).not.toContain(encodeOutputId(harness.sessionId, "0.txt"));

    const still = ok(await harness.list({ limit: 1, cursor: first.nextCursor }));
    expect(still.items.map((item) => item.id)).toEqual(["file-c"]);
    expect(await harness.list({ limit: 1, beforeId: "file-a" })).toMatchObject({
      ok: false,
      error: "anchor_not_found",
    });
  });

  const bulk = options.bulkOutputCount ?? 0;
  if (bulk > 1000) {
    it(`walks ${bulk} session outputs past a single storage list page`, async () => {
      const harness = await createHarness();
      const names: string[] = [];
      const batchSize = 40;
      for (let start = 0; start < bulk; start += batchSize) {
        const writes: Array<Promise<void>> = [];
        for (let i = start; i < Math.min(bulk, start + batchSize); i++) {
          const filename = `f${String(i).padStart(4, "0")}.txt`;
          names.push(filename);
          writes.push(harness.insertOutput({ filename, bytes: "x" }));
        }
        await Promise.all(writes);
      }
      names.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
      const ids = await walk(harness, 1000);
      expect(ids).toEqual(names.map((name) => encodeOutputId(harness.sessionId, name)));
      expect(ids).toHaveLength(bulk);
    }, 120_000);
  }
}

function ok(result: UnifiedPageResult): Extract<UnifiedPageResult, { ok: true }> {
  if (!result.ok) throw new Error(result.error);
  return result;
}

async function walk(
  harness: UnifiedPageHarness,
  limit: number,
  order: "asc" | "desc" = "desc",
): Promise<string[]> {
  const ids: string[] = [];
  let cursor: string | undefined;
  for (let i = 0; i < 20_000; i++) {
    const page = ok(await harness.list({ limit, order, cursor }));
    if (page.items.length === 0) {
      expect(i).toBe(0);
      expect(page.hasMore).toBe(false);
      return ids;
    }
    expect(page.items.length).toBeLessThanOrEqual(limit);
    ids.push(...page.items.map((item) => item.id));
    if (!page.hasMore) {
      expect(page.nextCursor).toBeUndefined();
      expect(new Set(ids).size).toBe(ids.length);
      return ids;
    }
    expect(page.items.length).toBe(limit);
    expect(page.nextCursor).toEqual(expect.any(String));
    cursor = page.nextCursor;
  }
  throw new Error("pagination did not terminate");
}

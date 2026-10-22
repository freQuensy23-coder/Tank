import { describe, expect, it } from "vitest";
import { memoryObjectPrefix, memoryObjectPath } from "../src/lib/s3-memory-poller.js";

describe("S3 memory poller key namespace", () => {
  it("lists only this deployment's store keys and projects logical paths", () => {
    const prefix = memoryObjectPrefix("store1", "prod/openma/memory/");
    expect(prefix).toBe("prod/openma/memory/store1/");
    expect(memoryObjectPath("prod/openma/memory/store1/notes/a.md", prefix)).toBe("/notes/a.md");
    expect(memoryObjectPath("prod/benchmark/notes/a.md", prefix)).toBeNull();
  });

  it("preserves existing unprefixed buckets", () => {
    expect(memoryObjectPrefix("store1")).toBe("store1/");
    expect(memoryObjectPath("store1/a.md", "store1/")).toBe("/a.md");
  });
});

import { describe } from "vitest";
import { FileService } from "./service";
import { InMemoryFileRepo } from "./test-fakes";
import type { UnifiedPageHarness } from "./unified-page.harness";
import { registerUnifiedFilePageScenarios } from "./unified-page.scenarios";
import {
  compareFilename,
  type SessionOutputObject,
  type SessionOutputPageSource,
} from "./unified-page";

describe("unified session file page (in-memory)", () => {
  registerUnifiedFilePageScenarios(async () => createMemoryHarness());
});

function createMemoryHarness(): UnifiedPageHarness {
  const tenantId = `tenant_${crypto.randomUUID()}`;
  const sessionId = `sess_${crypto.randomUUID()}`;
  const otherTenantId = `tenant_${crypto.randomUUID()}`;
  const otherSessionId = `sess_${crypto.randomUUID()}`;
  const repo = new InMemoryFileRepo();
  const service = new FileService({ repo });
  const outputs = new MemoryOutputs();
  return {
    tenantId,
    sessionId,
    otherTenantId,
    otherSessionId,
    async insertFile(input) {
      const owner = input.tenantId ?? tenantId;
      const session = input.sessionId ?? sessionId;
      await repo.insert({
        id: input.id,
        tenantId: owner,
        sessionId: session,
        scope: "session",
        filename: input.filename ?? `${input.id}.txt`,
        mediaType: "text/plain",
        sizeBytes: 1,
        downloadable: true,
        r2Key: `t/${owner}/files/${input.id}`,
        createdAt: input.createdAtMs,
      });
    },
    async insertOutput(input) {
      outputs.put({
        tenantId: input.tenantId ?? tenantId,
        sessionId: input.sessionId ?? sessionId,
        filename: input.filename,
        sizeBytes: (input.bytes ?? "x").length,
        uploadedAtMs: 1_000,
      });
    },
    async deleteFile(id) {
      await repo.delete(tenantId, id);
    },
    async deleteOutput(filename, session) {
      outputs.delete(tenantId, session ?? sessionId, filename);
    },
    list(input) {
      return service.listUnifiedPage({
        tenantId: input.tenantId ?? tenantId,
        scopeId: input.scopeId ?? sessionId,
        outputs,
        limit: input.limit,
        order: input.order,
        cursor: input.cursor,
        beforeId: input.beforeId,
        afterId: input.afterId,
      });
    },
  };
}

class MemoryOutputs implements SessionOutputPageSource {
  private readonly objects: SessionOutputObject[] = [];
  private readonly keys: string[] = [];

  put(input: SessionOutputObject & { tenantId: string; sessionId: string }): void {
    const key = `${input.tenantId}\0${input.sessionId}\0${input.filename}`;
    const existing = this.keys.indexOf(key);
    const object = {
      filename: input.filename,
      sizeBytes: input.sizeBytes,
      uploadedAtMs: input.uploadedAtMs,
      mediaType: input.mediaType,
    };
    if (existing >= 0) {
      this.objects[existing] = object;
      return;
    }
    this.keys.push(key);
    this.objects.push(object);
  }

  delete(tenantId: string, sessionId: string, filename: string): void {
    const key = `${tenantId}\0${sessionId}\0${filename}`;
    const index = this.keys.indexOf(key);
    if (index < 0) return;
    this.keys.splice(index, 1);
    this.objects.splice(index, 1);
  }

  async listAfter(input: {
    tenantId: string;
    sessionId: string;
    limit: number;
    startAfterFilename?: string;
  }): Promise<SessionOutputObject[]> {
    const prefix = `${input.tenantId}\0${input.sessionId}\0`;
    const matched: SessionOutputObject[] = [];
    for (let i = 0; i < this.keys.length; i++) {
      if (!this.keys[i]!.startsWith(prefix)) continue;
      const object = this.objects[i]!;
      if (input.startAfterFilename !== undefined && compareFilename(object.filename, input.startAfterFilename) <= 0) {
        continue;
      }
      matched.push(object);
    }
    matched.sort((a, b) => compareFilename(a.filename, b.filename));
    return matched.slice(0, input.limit);
  }
}

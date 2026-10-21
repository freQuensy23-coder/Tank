import type { UnifiedPageResult } from "./unified-page";

export interface UnifiedPageHarness {
  tenantId: string;
  sessionId: string;
  otherTenantId: string;
  otherSessionId: string;
  insertFile(input: {
    id: string;
    createdAtMs: number;
    filename?: string;
    sessionId?: string;
    tenantId?: string;
  }): Promise<void>;
  insertOutput(input: {
    filename: string;
    sessionId?: string;
    tenantId?: string;
    bytes?: string;
  }): Promise<void>;
  deleteFile(id: string): Promise<void>;
  deleteOutput(filename: string, sessionId?: string): Promise<void>;
  list(input: {
    scopeId?: string;
    tenantId?: string;
    limit?: number;
    order?: "asc" | "desc";
    cursor?: string;
    beforeId?: string;
    afterId?: string;
  }): Promise<UnifiedPageResult>;
}

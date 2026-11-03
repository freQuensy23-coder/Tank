import { Hono, type Context } from "hono";
import { z } from "zod";
import { resolveApplicationPort, type ApplicationPortSource } from "../application-port-source";
import { MANAGED_AGENTS_BETA, requireBeta } from "../beta";
import { invalidRequest, notFound, apiError } from "../errors";
import type { SessionsApplicationPort } from "../ports/sessions";

export interface PublicSessionExecution {
  execution_id: string;
  state: "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED" | "UNKNOWN";
  settled_outcome: "COMPLETED" | "FAILED" | "CANCELLED" | null;
  custom_tool_uses?: {
    declaration_id: string;
    event: {
      id: string; type: "agent.custom_tool_use"; execution_id: string;
      name: string; input: Record<string, unknown>; processed_at: string;
      session_thread_id?: string | null;
    };
  }[];
}
export interface ManagedSessionExecutionsExtension {
  workspaceId(context: Context): string;
  store: { list(workspaceId: string, sessionId: string, executionIds: string[]): Promise<PublicSessionExecution[]> };
}
const idsSchema = z.array(z.string().min(1).max(191)).min(1).max(64).refine(ids => new Set(ids).size === ids.length);
const rowSchema = z.object({
  execution_id: z.string().min(1).max(191),
  state: z.enum(["QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"]),
  settled_outcome: z.enum(["COMPLETED", "FAILED", "CANCELLED"]).nullable(),
  custom_tool_uses: z.array(z.object({
    declaration_id: z.string().min(1),
    event: z.object({
      id: z.string().min(1), type: z.literal("agent.custom_tool_use"), execution_id: z.string().min(1),
      name: z.string(), input: z.record(z.string(), z.unknown()), processed_at: z.string(),
      session_thread_id: z.string().nullable().optional(),
    }).strict(),
  }).strict()).optional(),
}).strict();
export function buildManagedSessionExecutionRoutes(sessions: ApplicationPortSource<SessionsApplicationPort>, extension: ManagedSessionExecutionsExtension): Hono {
  const app = new Hono();
  app.use("*", requireBeta(MANAGED_AGENTS_BETA));
  app.get("/:sessionId/executions", async c => {
    if (c.req.query().ids !== undefined || Object.keys(c.req.query()).some(key => key !== "ids[]")) return c.json(invalidRequest("Only ids[] is supported"), 400);
    const parsed = idsSchema.safeParse(c.req.queries("ids[]"));
    if (!parsed.success) return c.json(invalidRequest("Request 1 to 64 unique execution IDs of at most 191 characters"), 400);
    const sessionId = c.req.param("sessionId");
    const session = await resolveApplicationPort(sessions, c).retrieveSession({ sessionId });
    if (session.type !== "found") return c.json(notFound("Session was not found"), 404);
    const rows = await extension.store.list(extension.workspaceId(c), sessionId, parsed.data);
    const response = z.object({ data: z.array(rowSchema) }).strict().safeParse({ data: rows });
    if (!response.success || rows.length !== parsed.data.length || rows.some((row, index) => row.execution_id !== parsed.data[index])) return c.json(apiError("Invalid execution status projection"), 500);
    return c.json(response.data, 200);
  });
  return app;
}

import type { SessionExecutionCoordinatorPort } from "@open-managed-agents/session-runtime-contract/coordination";
import { declaredCustomToolUses, type ContinuationSnapshot } from "@open-managed-agents/session-runtime-contract/continuation";

export function sessionExecutionStatusStore(
  coordinator: Pick<SessionExecutionCoordinatorPort, "find">,
  snapshotSource?: (scope: { workspaceId: string; sessionId: string }) => Promise<ContinuationSnapshot>,
) {
  return {
    async list(workspaceId: string, sessionId: string, executionIds: string[]) {
      if (!executionIds.length || executionIds.length > 64 || new Set(executionIds).size !== executionIds.length || executionIds.some(id => !id || id.length > 191)) throw new Error("Invalid bounded execution status request");
      const snapshot = snapshotSource ? await snapshotSource({ workspaceId, sessionId }) : undefined;
      return Promise.all(executionIds.map(async executionId => {
        const row = await coordinator.find({ workspaceId, executionId });
        if (!row || row.sessionId !== sessionId || row.workspaceId !== workspaceId) return { execution_id: executionId, state: "UNKNOWN" as const, settled_outcome: null };
        const state = row.state.toUpperCase() as "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";
        const terminal = state === "COMPLETED" || state === "FAILED" || state === "CANCELLED";
        if (terminal && row.settledAt === null) return { execution_id: executionId, state: "UNKNOWN" as const, settled_outcome: null };
        const source = snapshot?.executions.find(item => item.id === executionId);
        const uses = snapshot && source?.revision === row.revision && source.state === row.state
          ? declaredCustomToolUses(snapshot, executionId) : [];
        return {
          execution_id: executionId, state, settled_outcome: terminal ? state as "COMPLETED" | "FAILED" | "CANCELLED" : null,
          ...(snapshot === undefined ? {} : { custom_tool_uses: uses.map(({ declarationId, event }) => ({
            declaration_id: declarationId,
            event: {
              id: event.id, type: event.type, execution_id: executionId,
              name: event.name, input: event.input, processed_at: event.processedAt,
              ...(event.sessionThreadId === undefined ? {} : { session_thread_id: event.sessionThreadId }),
            },
          })) }),
        };
      }));
    },
  };
}

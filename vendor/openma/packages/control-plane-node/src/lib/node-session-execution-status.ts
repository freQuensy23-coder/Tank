import type { SessionExecutionCoordinatorPort } from "@open-managed-agents/session-runtime-contract/coordination";

export function sessionExecutionStatusStore(coordinator: Pick<SessionExecutionCoordinatorPort, "find">) {
  return {
    async list(workspaceId: string, sessionId: string, executionIds: string[]) {
      if (!executionIds.length || executionIds.length > 64 || new Set(executionIds).size !== executionIds.length || executionIds.some(id => !id || id.length > 191)) throw new Error("Invalid bounded execution status request");
      return Promise.all(executionIds.map(async executionId => {
        const row = await coordinator.find({ workspaceId, executionId });
        if (!row || row.sessionId !== sessionId || row.workspaceId !== workspaceId) return { execution_id: executionId, state: "UNKNOWN" as const, settled_outcome: null };
        const state = row.state.toUpperCase() as "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";
        const terminal = state === "COMPLETED" || state === "FAILED" || state === "CANCELLED";
        if (terminal && row.settledAt === null) return { execution_id: executionId, state: "UNKNOWN" as const, settled_outcome: null };
        return { execution_id: executionId, state, settled_outcome: terminal ? state as "COMPLETED" | "FAILED" | "CANCELLED" : null };
      }));
    },
  };
}

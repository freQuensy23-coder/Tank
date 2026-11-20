import { ExecutionRecoveryRefusal } from "./execution-recovery-refusal.js";
import type { SessionExecutionCoordinatorPort } from "@open-managed-agents/session-runtime-contract/coordination";

export function sessionExecutionStatusStore(coordinator: Pick<SessionExecutionCoordinatorPort, "find" | "findReadyToolCalls">) {
  return {
    async list(workspaceId: string, sessionId: string, executionIds: string[], includeToolReadiness = false) {
      if (!executionIds.length || executionIds.length > 64 || new Set(executionIds).size !== executionIds.length || executionIds.some(id => !id || id.length > 191)) throw new Error("Invalid bounded execution status request");
      const rows = await Promise.all(executionIds.map(async executionId => {
        const row = await coordinator.find({ workspaceId, executionId });
        if (!row || row.sessionId !== sessionId || row.workspaceId !== workspaceId) return { execution_id: executionId, state: "UNKNOWN" as const, settled_outcome: null };
        const state = row.state.toUpperCase() as "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";
        const terminal = state === "COMPLETED" || state === "FAILED" || state === "CANCELLED";
        if (terminal && row.settledAt === null) return { execution_id: executionId, state: "UNKNOWN" as const, settled_outcome: null };
        return { execution_id: executionId, state, ...((state === "FAILED" || state === "CANCELLED") && row.failure === ExecutionRecoveryRefusal.code && { recovery_refused: true as const }), settled_outcome: terminal ? state as "COMPLETED" | "FAILED" | "CANCELLED" : null };
      }));
      if (!includeToolReadiness) return rows;
      const candidates = rows.filter(row => row.state === "QUEUED" || row.state === "RUNNING" || row.state === "COMPLETED").map(row => row.execution_id);
      const ready = candidates.length ? await coordinator.findReadyToolCalls?.({ workspaceId, sessionId, executionIds: candidates }) : undefined;
      return rows.map(row => ({ ...row, ready_tool_call_ids: ready?.get(row.execution_id) ?? [] }));
    },
  };
}

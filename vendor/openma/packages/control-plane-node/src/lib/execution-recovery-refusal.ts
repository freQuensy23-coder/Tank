import { executionRecoveryRefusalCode } from "@open-managed-agents/session-runtime-contract/continuation";

export class ExecutionRecoveryRefusal extends Error {
  static readonly code = executionRecoveryRefusalCode;
}

export function executionFailure(error: unknown): string {
  if (error instanceof ExecutionRecoveryRefusal) return ExecutionRecoveryRefusal.code;
  const message = error instanceof Error ? error.message : String(error);
  return message === ExecutionRecoveryRefusal.code ? "Runtime error: " + message : message;
}

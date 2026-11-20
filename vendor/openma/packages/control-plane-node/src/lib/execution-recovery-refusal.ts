export class ExecutionRecoveryRefusal extends Error {
  static readonly code = "tank:execution-recovery-refused:v1";
}

export function executionFailure(error: unknown): string {
  if (error instanceof ExecutionRecoveryRefusal) return ExecutionRecoveryRefusal.code;
  const message = error instanceof Error ? error.message : String(error);
  return message === ExecutionRecoveryRefusal.code ? "Runtime error: " + message : message;
}

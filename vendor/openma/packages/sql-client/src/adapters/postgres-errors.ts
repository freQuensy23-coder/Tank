import { ConnectionLost, Deadlock, LockWaitTimeout, dbErrorFields, isTaggedDbError } from "../db-errors";

/**
 * `40001` (serialization_failure) is not `40P01` (deadlock_detected).
 * PostgreSQL has already aborted the transaction, and the client must run
 * it again. Deadlock is the existing tag for that recovery, so callers do
 * not grow a second retry path.
 *
 * node-postgres stores the SQLSTATE on `error.code`. It reports a dropped
 * socket as `Connection terminated` / `Connection terminated unexpectedly`
 * with no SQLSTATE. postgres.js, which this adapter calls, uses
 * `CONNECTION_CLOSED` / `CONNECTION_ENDED` / `CONNECTION_DESTROYED`.
 */
const deadlockCodes = new Set(["40P01", "40001"]);
const lockWaitCodes = new Set(["55P03"]);
const connectionCodes = new Set([
  "57P01",
  "57P02",
  "57P03",
  "08000",
  "08003",
  "08006",
  "08001",
  "08004",
  "CONNECTION_CLOSED",
  "CONNECTION_ENDED",
  "CONNECTION_DESTROYED",
]);

export function translatePostgresError(error: unknown, hints?: { sql?: string }): unknown {
  if (isTaggedDbError(error)) return error;
  const kind = postgresKind(error);
  if (kind === undefined) return error;
  const fields = dbErrorFields(error, hints);
  if (kind === "deadlock") return new Deadlock(fields);
  if (kind === "lockWait") return new LockWaitTimeout(fields);
  return new ConnectionLost(fields);
}

function postgresKind(error: unknown): "deadlock" | "lockWait" | "connection" | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const current = error as { code?: unknown; message?: unknown; cause?: unknown };
  const code = typeof current.code === "string" ? current.code : undefined;
  const message = typeof current.message === "string" ? current.message : undefined;
  if (code !== undefined && deadlockCodes.has(code)) return "deadlock";
  if (code !== undefined && lockWaitCodes.has(code)) return "lockWait";
  if (
    (code !== undefined && connectionCodes.has(code)) ||
    message === "Connection terminated" ||
    message === "Connection terminated unexpectedly" ||
    message === "Client was closed and is not queryable"
  ) return "connection";
  return current.cause === undefined ? undefined : postgresKind(current.cause);
}

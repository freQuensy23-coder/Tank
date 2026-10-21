import { ConnectionLost, Deadlock, LockWaitTimeout, dbErrorFields, isTaggedDbError } from "../db-errors";

/**
 * mysql2 puts the server errno on `error.errno` and the symbol on `error.code`.
 * Walk `cause` so a pool wrapper does not hide the driver error.
 */
export function translateMysql2Error(error: unknown, hints?: { sql?: string }): unknown {
  if (isTaggedDbError(error)) return error;
  const kind = mysqlKind(error);
  if (kind === undefined) return error;
  const fields = dbErrorFields(error, hints);
  if (kind === "deadlock") return new Deadlock(fields);
  if (kind === "lockWait") return new LockWaitTimeout(fields);
  return new ConnectionLost(fields);
}

function mysqlKind(error: unknown): "deadlock" | "lockWait" | "connection" | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const current = error as { errno?: unknown; code?: unknown; cause?: unknown };
  const errno = typeof current.errno === "number" ? current.errno : undefined;
  const code = typeof current.code === "string" ? current.code : undefined;
  if (errno === 1213 || code === "ER_LOCK_DEADLOCK") return "deadlock";
  if (errno === 1205 || code === "ER_LOCK_WAIT_TIMEOUT") return "lockWait";
  if (
    errno === 2006 || errno === 2013 || errno === 1053 ||
    code === "PROTOCOL_CONNECTION_LOST" ||
    code === "ECONNRESET" ||
    code === "ECONNREFUSED" ||
    code === "EPIPE" ||
    code === "ETIMEDOUT" ||
    code === "CR_SERVER_LOST" ||
    code === "CR_SERVER_GONE_ERROR" ||
    code === "PROTOCOL_ENQUEUE_AFTER_QUIT" ||
    code === "PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR"
  ) return "connection";
  return current.cause === undefined ? undefined : mysqlKind(current.cause);
}

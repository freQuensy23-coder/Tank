import { LockWaitTimeout, dbErrorFields, isTaggedDbError } from "../db-errors";

/**
 * better-sqlite3 sets `code` to the SQLite symbol (`SQLITE_BUSY`,
 * `SQLITE_BUSY_SNAPSHOT`, …) and sometimes `errcode` to the extended
 * integer. The primary result is the low 8 bits: 5 busy, 6 locked.
 * Both mean "the statement can be run again", so they become LockWaitTimeout.
 */
export function translateBetterSqlite3Error(error: unknown, hints?: { sql?: string }): unknown {
  if (isTaggedDbError(error)) return error;
  if (!sqliteBusyOrLocked(error)) return error;
  return new LockWaitTimeout(dbErrorFields(error, hints));
}

function sqliteBusyOrLocked(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const current = error as { code?: unknown; errcode?: unknown; cause?: unknown };
  const code = typeof current.code === "string" ? current.code : undefined;
  const errcode = typeof current.errcode === "number" ? current.errcode : undefined;
  if (
    code === "SQLITE_BUSY" ||
    (code !== undefined && code.startsWith("SQLITE_BUSY_")) ||
    code === "SQLITE_LOCKED" ||
    (code !== undefined && code.startsWith("SQLITE_LOCKED_")) ||
    (errcode !== undefined && ((errcode & 0xff) === 5 || (errcode & 0xff) === 6))
  ) return true;
  return current.cause === undefined ? false : sqliteBusyOrLocked(current.cause);
}

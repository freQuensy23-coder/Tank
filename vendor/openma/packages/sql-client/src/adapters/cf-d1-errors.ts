import { ConnectionLost, LockWaitTimeout, dbErrorFields, isTaggedDbError } from "../db-errors";

/**
 * Phrases are the D1_ERROR texts whose documented action is to retry, from
 * https://developers.cloudflare.com/d1/observability/debug-d1/ (updated
 * 2026-08-11). Older wrangler put the detail on `error.cause.message`.
 * Overloaded is a queue wait, so it is LockWaitTimeout. Disconnect and
 * reset drop the session, so they are ConnectionLost.
 */
const connectionPhrases = [
  "Network connection lost.",
  "Replica disconnected from primary.",
  "Cannot resolve D1 DB due to transient issue on remote node.",
  "Can't read from request stream because client disconnected.",
  "D1 DB reset because its code was updated.",
  "Internal error while starting up D1 DB storage caused object to be reset.",
  "Internal error in D1 DB storage caused object to be reset.",
];

const overloadedPhrases = [
  "D1 DB is overloaded. Requests queued for too long.",
  "D1 DB is overloaded. Too many requests queued.",
];

export function translateCfD1Error(error: unknown, hints?: { sql?: string }): unknown {
  if (isTaggedDbError(error)) return error;
  const text = d1Messages(error);
  if (overloadedPhrases.some((phrase) => text.includes(phrase))) {
    return new LockWaitTimeout(dbErrorFields(error, hints));
  }
  if (connectionPhrases.some((phrase) => text.includes(phrase))) {
    return new ConnectionLost(dbErrorFields(error, hints));
  }
  return error;
}

function d1Messages(error: unknown): string {
  const parts: string[] = [];
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    const record = current as { message?: unknown; cause?: unknown };
    if (typeof record.message === "string") parts.push(record.message);
    current = record.cause;
  }
  return parts.join("\n");
}

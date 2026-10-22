// Opaque ids for session-output objects that live only in the blob
// prefix (R2 / the node outputs directory) and have no `files` row.
// Wire format is stable: `out:<sessionId>:<base64url(filename)>`.

const OUTPUT_ID_PREFIX = "out:";

export function encodeOutputId(sessionId: string, filename: string): string {
  const b64 = btoa(filename).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `${OUTPUT_ID_PREFIX}${sessionId}:${b64}`;
}

export function decodeOutputId(
  id: string,
): { sessionId: string; filename: string } | null {
  if (!id.startsWith(OUTPUT_ID_PREFIX)) return null;
  const rest = id.slice(OUTPUT_ID_PREFIX.length);
  const sep = rest.indexOf(":");
  if (sep < 0) return null;
  const sessionId = rest.slice(0, sep);
  const b64 = rest.slice(sep + 1);
  if (!sessionId || !b64) return null;
  try {
    const padded = b64.replace(/-/g, "+").replace(/_/g, "/")
      + "===".slice((b64.length + 3) % 4);
    return { sessionId, filename: atob(padded) };
  } catch {
    return null;
  }
}

/** Single path segment. Rejects traversal and nested keys on filesystem adapters. */
export function isSafeOutputFilename(filename: string): boolean {
  return filename.length > 0
    && filename !== "."
    && filename !== ".."
    && !filename.includes("/")
    && !filename.includes("\\")
    && !filename.includes("\0");
}

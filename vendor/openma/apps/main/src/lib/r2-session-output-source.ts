import { sessionOutputsPrefix } from "@open-managed-agents/shared";
import type {
  SessionOutputObject,
  SessionOutputPageSource,
} from "@open-managed-agents/files-store";

const R2_LIST_MAX = 1000;

/**
 * Session-output page source over R2. One `list` call returns at most
 * 1000 keys; this loops with the continuation cursor until `limit` keys
 * have been collected or the prefix is exhausted. Resume is `startAfter`
 * on the object key, which is exclusive and stable if objects are
 * inserted or deleted between calls.
 */
export function createR2SessionOutputSource(bucket: R2Bucket): SessionOutputPageSource {
  return {
    async listAfter(input) {
      const prefix = sessionOutputsPrefix(input.tenantId, input.sessionId);
      const collected: SessionOutputObject[] = [];
      let cursor: string | undefined;
      const bound = input.startAfterFilename;
      while (collected.length < input.limit) {
        const page = await bucket.list({
          prefix,
          limit: Math.min(R2_LIST_MAX, input.limit - collected.length),
          ...(cursor ? { cursor } : {}),
          ...(!cursor && bound !== undefined ? { startAfter: prefix + bound } : {}),
          include: ["httpMetadata"],
        });
        for (const object of page.objects) {
          if (!object.key.startsWith(prefix)) continue;
          const filename = object.key.slice(prefix.length);
          if (!filename || filename.endsWith("/")) continue;
          if (bound !== undefined && !(filename > bound)) continue;
          collected.push({
            filename,
            sizeBytes: object.size,
            uploadedAtMs: object.uploaded.getTime(),
            mediaType: object.httpMetadata?.contentType,
          });
          if (collected.length >= input.limit) break;
        }
        if (collected.length >= input.limit) break;
        if (!page.truncated || !page.cursor || page.cursor === cursor) break;
        cursor = page.cursor;
      }
      return collected;
    },
  };
}

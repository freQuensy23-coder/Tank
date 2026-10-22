// S3-compatible BlobStore for the wider blob-store port (file/skill/hub
// payloads, scoped session-file copies — anything that uses the
// non-memory-specific BlobStore in apps/main).
//
// Same general shape as CfR2BlobStore / InMemoryBlobStore. Keeps the
// `@aws-sdk/client-s3` import lazy so consumers that don't construct an
// S3BlobStore don't pay the bundle cost.
//
// Conditional PUT mapping:
//   ifNoneMatch "*"  → IfNoneMatch: "*" (atomic on AWS S3 / R2 / MinIO
//                       since 2024; older endpoints fall back to a
//                       race-prone HEAD pre-check)
//   ifMatch <etag>   → IfMatch: etag (always supported)

import type {
  BlobBody,
  BlobMetadata,
  BlobObject,
  BlobPutOptions,
  BlobStore,
} from "../ports";

export interface S3BlobStoreOptions {
  endpoint: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  region?: string;
  forcePathStyle?: boolean;
  requestChecksumCalculation?: "WHEN_REQUIRED" | "WHEN_SUPPORTED";
  prefix?: string;
  /** How create-only (`ifNoneMatch: "*"`) PUTs are expressed. Aliyun OSS's S3
   * layer rejects If-None-Match with NotImplemented but honours its native
   * `x-oss-forbid-overwrite` header (409 FileAlreadyExists when present). */
  conditionalCreate?: "if-none-match" | "oss-forbid-overwrite";
}

interface S3HeadOutput {
  ETag?: string;
  ContentLength?: number;
  ContentType?: string;
  ContentEncoding?: string;
  ContentDisposition?: string;
  ContentLanguage?: string;
  CacheControl?: string;
  Metadata?: Record<string, string>;
}
interface S3GetOutput extends S3HeadOutput {
  Body?: {
    transformToWebStream(): ReadableStream<Uint8Array>;
  };
}
interface S3PutOutput {
  ETag?: string;
}
type S3CommandCtor<I, O> = new (input: I) => { output?: O };
interface S3Client {
  send<O>(command: { output?: O }): Promise<O>;
  destroy(): void;
}

export class S3BlobStore implements BlobStore {
  readonly bucket: string;
  readonly prefix: string;
  private clientPromise: Promise<{
    client: S3Client;
    HeadObjectCommand: S3CommandCtor<unknown, S3HeadOutput>;
    GetObjectCommand: S3CommandCtor<unknown, S3GetOutput>;
    PutObjectCommand: S3CommandCtor<unknown, S3PutOutput>;
    DeleteObjectCommand: S3CommandCtor<unknown, unknown>;
  }> | null = null;

  constructor(private opts: S3BlobStoreOptions) {
    this.bucket = opts.bucket;
    this.prefix = opts.prefix ?? "";
  }

  private async ensureClient() {
    if (!this.clientPromise) {
      this.clientPromise = (async () => {
        const sdk = (await import(
          /* @vite-ignore */ "@aws-sdk/client-s3" as string
        )) as {
          S3Client: new (cfg: unknown) => S3Client;
          HeadObjectCommand: S3CommandCtor<unknown, S3HeadOutput>;
          GetObjectCommand: S3CommandCtor<unknown, S3GetOutput>;
          PutObjectCommand: S3CommandCtor<unknown, S3PutOutput>;
          DeleteObjectCommand: S3CommandCtor<unknown, unknown>;
        };
        const client = new sdk.S3Client({
          endpoint: this.opts.endpoint,
          region: this.opts.region ?? "us-east-1",
          forcePathStyle: this.opts.forcePathStyle ?? true,
          ...(this.opts.requestChecksumCalculation ? { requestChecksumCalculation: this.opts.requestChecksumCalculation } : {}),
          credentials: {
            accessKeyId: this.opts.accessKeyId,
            secretAccessKey: this.opts.secretAccessKey,
          },
        });
        return {
          client,
          HeadObjectCommand: sdk.HeadObjectCommand,
          GetObjectCommand: sdk.GetObjectCommand,
          PutObjectCommand: sdk.PutObjectCommand,
          DeleteObjectCommand: sdk.DeleteObjectCommand,
        };
      })();
    }
    return this.clientPromise;
  }

  async head(key: string): Promise<BlobMetadata | null> {
    const { client, HeadObjectCommand } = await this.ensureClient();
    try {
      const out = await client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: this.fullKey(key) }),
      );
      return toMetadata(out);
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
  }

  async get(key: string): Promise<BlobObject | null> {
    const { client, GetObjectCommand } = await this.ensureClient();
    try {
      const out = await client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: this.fullKey(key) }),
      );
      const meta = toMetadata(out);
      // Expose the SDK's actual stream, not a ReadableStream whose start()
      // eagerly drains the entire object before a caller can enforce limits.
      const body = out.Body?.transformToWebStream() ?? new ReadableStream<Uint8Array>({
        start(controller) { controller.close(); },
      });
      let bytesPromise: Promise<Uint8Array> | undefined;
      const bytes = () => bytesPromise ??= new Response(body).arrayBuffer().then(
        (buffer) => new Uint8Array(buffer),
      );
      return {
        ...meta,
        body,
        text: async () => new TextDecoder("utf-8").decode(await bytes()),
        arrayBuffer: async () => {
          const b = await bytes();
          return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer;
        },
        bytes,
      };
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
  }

  async put(
    key: string,
    body: BlobBody,
    opts?: BlobPutOptions,
  ): Promise<BlobMetadata | null> {
    const { client, PutObjectCommand } = await this.ensureClient();
    const fullKey = this.fullKey(key);
    const putInput: Record<string, unknown> = {
      Bucket: this.bucket,
      Key: fullKey,
      Body: bodyToS3Payload(body),
    };
    const http = opts?.httpMetadata;
    if (http?.contentType) putInput.ContentType = http.contentType;
    if (http?.contentEncoding) putInput.ContentEncoding = http.contentEncoding;
    if (http?.contentDisposition) putInput.ContentDisposition = http.contentDisposition;
    if (http?.contentLanguage) putInput.ContentLanguage = http.contentLanguage;
    if (http?.cacheControl) putInput.CacheControl = http.cacheControl;
    if (opts?.customMetadata) putInput.Metadata = opts.customMetadata;
    const ossCreateOnly = opts?.precondition?.type === "ifNoneMatch" && this.opts.conditionalCreate === "oss-forbid-overwrite";
    if (opts?.precondition?.type === "ifNoneMatch" && !ossCreateOnly) putInput.IfNoneMatch = "*";
    else if (opts?.precondition?.type === "ifMatch") putInput.IfMatch = opts.precondition.etag;

    const command = new PutObjectCommand(putInput);
    if (ossCreateOnly) addOssForbidOverwrite(command);
    let put: S3PutOutput;
    try {
      put = await client.send(command);
    } catch (err) {
      if (isPreconditionFailed(err) || (ossCreateOnly && isOssAlreadyExists(err))) return null;
      throw err;
    }
    return {
      etag: stripQuotes(put.ETag ?? ""),
      size: estimateBodySize(body),
      httpMetadata: http,
      customMetadata: opts?.customMetadata,
    };
  }

  async delete(key: string): Promise<void> {
    const { client, DeleteObjectCommand } = await this.ensureClient();
    await client.send(
      new DeleteObjectCommand({ Bucket: this.bucket, Key: this.fullKey(key) }),
    );
  }

  fullKey(key: string): string {
    const stripped = key.replace(/^\/+/, "");
    return this.prefix ? `${this.prefix}${stripped}` : stripped;
  }
}

function toMetadata(out: S3HeadOutput): BlobMetadata {
  return {
    etag: stripQuotes(out.ETag ?? ""),
    size: out.ContentLength ?? 0,
    httpMetadata: {
      contentType: out.ContentType,
      contentEncoding: out.ContentEncoding,
      contentDisposition: out.ContentDisposition,
      contentLanguage: out.ContentLanguage,
      cacheControl: out.CacheControl,
    },
    customMetadata: out.Metadata,
  };
}

function bodyToS3Payload(body: BlobBody): unknown {
  // S3 SDK accepts string | Buffer | Uint8Array | ReadableStream — the BlobBody
  // shapes all map directly without conversion.
  return body;
}

function estimateBodySize(body: BlobBody): number {
  if (typeof body === "string") return Buffer.byteLength(body, "utf8");
  if (body instanceof ArrayBuffer) return body.byteLength;
  if (ArrayBuffer.isView(body)) return body.byteLength;
  return 0; // streaming uploads — caller can re-HEAD if size is needed
}

function stripQuotes(etag: string): string {
  if (etag.startsWith('"') && etag.endsWith('"')) return etag.slice(1, -1);
  return etag;
}

function isNotFound(err: unknown): boolean {
  const e = err as { name?: string; $metadata?: { httpStatusCode?: number } };
  return (
    e?.name === "NoSuchKey" ||
    e?.name === "NotFound" ||
    e?.$metadata?.httpStatusCode === 404
  );
}

/** Added in the build step, before SigV4 signing, so the header is signed. */
function addOssForbidOverwrite(command: unknown): void {
  const stack = (command as { middlewareStack?: { add(mw: unknown, options: unknown): void } }).middlewareStack;
  if (!stack) throw new Error("S3 client does not expose a middleware stack for OSS create-only writes");
  stack.add((next: (args: unknown) => Promise<unknown>) => async (args: unknown) => {
    const request = (args as { request?: { headers?: Record<string, string> } }).request;
    if (request?.headers) request.headers["x-oss-forbid-overwrite"] = "true";
    return next(args);
  }, { step: "build", name: "openmaOssForbidOverwrite" });
}

function isOssAlreadyExists(err: unknown): boolean {
  const e = err as { name?: string; Code?: string; $metadata?: { httpStatusCode?: number } } | null;
  return e?.name === "FileAlreadyExists" || e?.Code === "FileAlreadyExists" || e?.$metadata?.httpStatusCode === 409;
}

function isPreconditionFailed(err: unknown): boolean {
  const e = err as {
    name?: string;
    $metadata?: { httpStatusCode?: number };
  };
  return (
    e?.name === "PreconditionFailed" ||
    e?.$metadata?.httpStatusCode === 412
  );
}

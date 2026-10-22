import { describe, expect, it, vi } from "vitest";
import { S3BlobStore } from "../src/adapters/s3";

function fakeStore(body: ReadableStream<Uint8Array>, size: number) {
  const transformed = vi.fn(() => body);
  const oldEagerRead = vi.fn(async () => { throw new Error("eager object buffering"); });
  const store = new S3BlobStore({ endpoint: "http://localhost:9000", bucket: "test",
    accessKeyId: "test", secretAccessKey: "test" });
  (store as unknown as { clientPromise: Promise<unknown> }).clientPromise = Promise.resolve({
    client: { send: async () => ({ ContentLength: size, Body: {
      transformToWebStream: transformed,
      transformToByteArray: oldEagerRead,
    } }) },
    GetObjectCommand: class { constructor(_input: unknown) {} },
  });
  return { store, transformed, oldEagerRead };
}

describe("S3 BlobStore GET", () => {
  it("streams GET bodies without eager whole-object buffering and supports the convenience reader", async () => {
    const payload = new TextEncoder().encode("streamed");
    const { store, transformed, oldEagerRead } = fakeStore(new ReadableStream({
      start(controller) { controller.enqueue(payload); controller.close(); },
    }), payload.byteLength);
    const blob = await store.get("session/archive");
    expect(blob?.size).toBe(payload.byteLength);
    expect(oldEagerRead).not.toHaveBeenCalled();
    expect(transformed).toHaveBeenCalledOnce();
    expect(await blob?.text()).toBe("streamed");
    expect(oldEagerRead).not.toHaveBeenCalled();
  });
});

function fakePutStore(options: Partial<ConstructorParameters<typeof S3BlobStore>[0]>, send: (command: FakeCommand) => Promise<unknown>) {
  const store = new S3BlobStore({ endpoint: "http://localhost:9000", bucket: "test",
    accessKeyId: "test", secretAccessKey: "test", ...options });
  (store as unknown as { clientPromise: Promise<unknown> }).clientPromise = Promise.resolve({
    client: { send },
    PutObjectCommand: FakeCommand,
  });
  return store;
}
class FakeCommand {
  readonly middleware: Array<(next: (args: unknown) => Promise<unknown>) => (args: unknown) => Promise<unknown>> = [];
  readonly middlewareStack = { add: (mw: FakeCommand["middleware"][number]) => { this.middleware.push(mw); } };
  constructor(readonly input: Record<string, unknown>) {}
  async headers(): Promise<Record<string, string>> {
    const args = { request: { headers: {} as Record<string, string> } };
    let handler = async (value: unknown) => value;
    for (const mw of [...this.middleware].reverse()) handler = mw(handler) as typeof handler;
    await handler(args);
    return args.request.headers;
  }
}

describe("S3 BlobStore create-only PUT", () => {
  it("uses IfNoneMatch by default", async () => {
    let sent: FakeCommand | undefined;
    const store = fakePutStore({}, async (command) => { sent = command; return { ETag: "\"e\"" }; });
    await store.put("k", "v", { precondition: { type: "ifNoneMatch", value: "*" } });
    expect(sent!.input.IfNoneMatch).toBe("*");
  });

  it("maps create-only to Aliyun OSS x-oss-forbid-overwrite, which rejects If-None-Match", async () => {
    let sent: FakeCommand | undefined;
    const store = fakePutStore({ conditionalCreate: "oss-forbid-overwrite" }, async (command) => {
      sent = command; return { ETag: "\"e\"" };
    });
    expect(await store.put("k", "v", { precondition: { type: "ifNoneMatch", value: "*" } })).not.toBeNull();
    expect(sent!.input.IfNoneMatch).toBeUndefined();
    expect(await sent!.headers()).toMatchObject({ "x-oss-forbid-overwrite": "true" });
    // Unconditional writes must not forbid overwrite.
    await store.put("k2", "v");
    expect(await sent!.headers()).not.toHaveProperty("x-oss-forbid-overwrite");
  });

  it("returns null when OSS reports the object already exists", async () => {
    const store = fakePutStore({ conditionalCreate: "oss-forbid-overwrite" }, async () => {
      throw Object.assign(new Error("exists"), { name: "FileAlreadyExists", $metadata: { httpStatusCode: 409 } });
    });
    expect(await store.put("k", "v", { precondition: { type: "ifNoneMatch", value: "*" } })).toBeNull();
  });
});

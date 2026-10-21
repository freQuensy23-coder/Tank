/**
 * Local OAuth token endpoint plus MCP server for credential-validation tests.
 * Not a production service. It does not write request bodies or tokens to logs.
 */
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import {
  OAUTH_MCP_TEST_PORT,
  type OauthMcpTestControl,
  type OauthMcpTestMeta,
} from "./oauth-mcp-local-endpoint.ts";

interface State {
  accessToken: string;
  refreshToken: string;
  clientId: string | null;
  clientSecret: string | null;
  mcp: "match" | "unavailable" | "forbidden" | "redirect";
  token: "rotate" | "invalid_grant" | "unavailable";
  resource: string | null;
  scope: string | null;
  issuedCount: number;
  mcpRequests: number;
  tokenRequests: number;
  sinkSawAuthorization: boolean;
  basicSecretMatched: boolean;
  postSecretMatched: boolean;
  resourceMatched: boolean;
  scopeMatched: boolean;
  clientIdMatched: boolean;
}

export interface OauthMcpTestServer {
  origin: string;
  close(): Promise<void>;
}

export async function startOauthMcpTestServer(
  options: { port?: number } = {},
): Promise<OauthMcpTestServer> {
  const state = emptyState();
  const server = createServer((request, response) => {
    void handle(request, response, state, originOf(server)).catch(() => {
      if (response.headersSent) {
        response.end();
        return;
      }
      sendText(response, 500, "error");
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port ?? 0, "127.0.0.1", () => resolve());
  });
  server.unref();
  return {
    origin: originOf(server),
    close: () =>
      new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

export async function reserveClosedPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  const port = address !== null && typeof address !== "string" ? address.port : 0;
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
  return port;
}

export { OAUTH_MCP_TEST_PORT };

function originOf(server: ReturnType<typeof createServer>): string {
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("oauth test server is not listening");
  }
  return `http://127.0.0.1:${address.port}`;
}

function emptyState(): State {
  return {
    accessToken: "unset-access-token",
    refreshToken: "unset-refresh-token",
    clientId: null,
    clientSecret: null,
    mcp: "match",
    token: "rotate",
    resource: null,
    scope: null,
    issuedCount: 0,
    mcpRequests: 0,
    tokenRequests: 0,
    sinkSawAuthorization: false,
    basicSecretMatched: false,
    postSecretMatched: false,
    resourceMatched: false,
    scopeMatched: false,
    clientIdMatched: false,
  };
}

async function handle(
  request: IncomingMessage,
  response: ServerResponse,
  state: State,
  origin: string,
): Promise<void> {
  const url = new URL(request.url ?? "/", origin);
  if (request.method === "GET" && url.pathname === "/__meta") {
    sendJson(response, 200, metaOf(state));
    return;
  }
  if (request.method === "POST" && url.pathname === "/__reset") {
    const control = JSON.parse(await readBody(request)) as OauthMcpTestControl;
    applyControl(state, control);
    sendJson(response, 200, { ok: true });
    return;
  }
  if (request.method === "POST" && url.pathname === "/__invalidate-access") {
    state.accessToken = "invalidated-access";
    sendJson(response, 200, { ok: true });
    return;
  }
  if (request.method === "POST" && url.pathname === "/__closed-port") {
    sendJson(response, 200, { port: await reserveClosedPort() });
    return;
  }
  if (url.pathname === "/sink") {
    const authorization = request.headers.authorization ?? "";
    state.sinkSawAuthorization = authorization.length > 0;
    sendText(response, 204, "");
    return;
  }
  if (request.method === "POST" && url.pathname === "/oauth/token") {
    await handleToken(request, response, state);
    return;
  }
  if (request.method === "POST" && url.pathname === "/mcp") {
    await handleMcp(request, response, state, origin);
    return;
  }
  sendText(response, 404, "not found");
}

function applyControl(state: State, control: OauthMcpTestControl): void {
  const fresh = emptyState();
  fresh.accessToken = control.accessToken;
  fresh.refreshToken = control.refreshToken;
  fresh.clientId = control.clientId ?? null;
  fresh.clientSecret = control.clientSecret ?? null;
  fresh.mcp = control.mcp ?? "match";
  fresh.token = control.token ?? "rotate";
  fresh.resource = control.resource ?? null;
  fresh.scope = control.scope ?? null;
  Object.assign(state, fresh);
}

function metaOf(state: State): OauthMcpTestMeta {
  return {
    issuedCount: state.issuedCount,
    mcpRequests: state.mcpRequests,
    tokenRequests: state.tokenRequests,
    sinkSawAuthorization: state.sinkSawAuthorization,
    basicSecretMatched: state.basicSecretMatched,
    postSecretMatched: state.postSecretMatched,
    resourceMatched: state.resourceMatched,
    scopeMatched: state.scopeMatched,
    clientIdMatched: state.clientIdMatched,
  };
}

async function handleMcp(
  request: IncomingMessage,
  response: ServerResponse,
  state: State,
  origin: string,
): Promise<void> {
  state.mcpRequests += 1;
  const bearer = bearerOf(request.headers.authorization);
  if (state.mcp === "redirect") {
    response.writeHead(302, { location: `${origin}/sink` });
    response.end();
    return;
  }
  if (state.mcp === "unavailable") {
    sendJson(response, 503, { error: "unavailable", access_token: bearer });
    return;
  }
  if (state.mcp === "forbidden") {
    sendJson(response, 403, { error: "invalid_token", access_token: bearer });
    return;
  }
  if (bearer !== state.accessToken) {
    sendJson(response, 401, { error: "invalid_token", access_token: bearer });
    return;
  }
  await readBody(request);
  sendJson(response, 200, {
    jsonrpc: "2.0",
    id: "credential-validation",
    result: {
      protocolVersion: "2025-06-18",
      serverInfo: { name: "oauth-mcp-test", version: "0" },
      access_token: state.accessToken,
    },
  });
}

async function handleToken(
  request: IncomingMessage,
  response: ServerResponse,
  state: State,
): Promise<void> {
  state.tokenRequests += 1;
  const raw = await readBody(request);
  const params = paramsOf(request.headers["content-type"], raw);
  const submittedRefresh = params.get("refresh_token") ?? "";
  const submittedSecret = params.get("client_secret");
  const submittedClient = params.get("client_id");
  if (state.clientId !== null && submittedClient === state.clientId) {
    state.clientIdMatched = true;
  }
  if (state.resource !== null && params.get("resource") === state.resource) {
    state.resourceMatched = true;
  }
  if (state.scope !== null && params.get("scope") === state.scope) {
    state.scopeMatched = true;
  }
  const basic = decodeBasic(request.headers.authorization);
  if (
    basic !== null &&
    state.clientSecret !== null &&
    basic.secret === state.clientSecret &&
    (state.clientId === null || basic.id === state.clientId)
  ) {
    state.basicSecretMatched = true;
  }
  if (
    submittedSecret !== null &&
    state.clientSecret !== null &&
    submittedSecret === state.clientSecret
  ) {
    state.postSecretMatched = true;
  }
  const echoedSecret = submittedSecret ?? basic?.secret ?? "";
  if (state.token === "unavailable") {
    sendJson(response, 503, {
      error: "temporarily_unavailable",
      refresh_token: submittedRefresh,
      client_secret: echoedSecret,
    });
    return;
  }
  if (state.token === "invalid_grant" || submittedRefresh !== state.refreshToken) {
    sendJson(response, 400, {
      error: "invalid_grant",
      refresh_token: submittedRefresh,
      client_secret: echoedSecret,
    });
    return;
  }
  state.issuedCount += 1;
  state.accessToken = `fresh-access-${state.issuedCount}`;
  state.refreshToken = `fresh-refresh-${state.issuedCount}`;
  sendJson(response, 200, {
    access_token: state.accessToken,
    refresh_token: state.refreshToken,
    expires_in: 3600,
    token_type: "Bearer",
    client_secret: echoedSecret,
  });
}

function paramsOf(contentType: string | undefined, raw: string): URLSearchParams {
  if ((contentType ?? "").includes("application/json")) {
    const parsed = JSON.parse(raw) as Record<string, string>;
    return new URLSearchParams(parsed);
  }
  return new URLSearchParams(raw);
}

function bearerOf(header: string | undefined): string {
  if (header === undefined || !header.startsWith("Bearer ")) return "";
  return header.slice("Bearer ".length);
}

function decodeBasic(
  header: string | undefined,
): { id: string; secret: string } | null {
  if (header === undefined) return null;
  const match = /^Basic\s+(\S+)$/.exec(header);
  if (match === null) return null;
  try {
    const decoded = Buffer.from(match[1] ?? "", "base64").toString("utf8");
    const separator = decoded.indexOf(":");
    if (separator < 0) return null;
    return {
      id: decodeURIComponent(decoded.slice(0, separator)),
      secret: decodeURIComponent(decoded.slice(separator + 1)),
    };
  } catch {
    return null;
  }
}

function readBody(request: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    request.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > 1_000_000) {
        reject(new Error("body too large"));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    request.on("error", () => reject(new Error("request read failed")));
  });
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    "cache-control": "no-store",
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
  });
  response.end(payload);
}

function sendText(response: ServerResponse, status: number, body: string): void {
  response.writeHead(status, {
    "cache-control": "no-store",
    "content-type": "text/plain",
    "content-length": Buffer.byteLength(body),
  });
  response.end(body);
}

import { expect } from "vitest";

import {
  invalidateOauthMcpAccessToken,
  readOauthMcpTestMeta,
  reserveOauthMcpClosedPort,
  resetOauthMcpTestServer,
  type OauthMcpTestControl,
} from "./oauth-mcp-local-endpoint.ts";

const MARKERS = [
  "stored-access-",
  "stored-refresh-",
  "stored-client-",
  "fresh-access-",
  "fresh-refresh-",
];

const BETA = {
  "anthropic-beta": "managed-agents-2026-04-01",
  "content-type": "application/json",
  "x-api-key": "test-key",
};

interface ValidationResponse {
  has_refresh_token: boolean;
  mcp_probe: {
    http_response: { body: string; status_code: number } | null;
    method: string;
  } | null;
  refresh: {
    http_response: { body: string; status_code: number } | null;
    status: string;
  } | null;
  status: string;
  type: string;
}

export async function runMcpOauthValidationScenarios(options: {
  origin: string;
  request(path: string, init?: RequestInit): Promise<Response>;
}): Promise<void> {
  const vault = await json<{ id: string }>(
    await options.request("/v1/vaults", {
      method: "POST",
      headers: BETA,
      body: JSON.stringify({ display_name: "oauth-validation" }),
    }),
    201,
  );

  await expectValidWithoutRefresh(options, vault.id);
  await expectRefreshPersistsAndScrubs(options, vault.id);
  await expectBasicClientAuth(options, vault.id);
  await expectRevokedRefresh(options, vault.id);
  await expectMissingRefreshToken(options, vault.id);
  await expectMcpUnreachable(options, vault.id);
  await expectTokenUnreachable(options, vault.id);
  await expectMcpUnavailable(options, vault.id);
  await expectTokenUnavailable(options, vault.id);
  await expectRedirectIsNotFollowed(options, vault.id);
}

async function expectValidWithoutRefresh(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const access = token("stored-access-");
  await reset(options.origin, { accessToken: access, refreshToken: token("stored-refresh-") });
  const credentialId = await createCredential(options, vaultId, {
    access,
    refresh: token("stored-refresh-"),
    secret: token("stored-client-"),
    auth: "client_secret_post",
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation.status).toBe("valid");
  expect(validation.mcp_probe).toBeNull();
  expect(validation.refresh).toBeNull();
  expect(validation.has_refresh_token).toBe(true);
  const meta = await readOauthMcpTestMeta(options.origin);
  expect(meta.tokenRequests).toBe(0);
  expect(meta.mcpRequests).toBe(1);
  assertNoMarkers(validation);
}

async function expectRefreshPersistsAndScrubs(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const refresh = token("stored-refresh-");
  const secret = token("stored-client-");
  await reset(options.origin, {
    accessToken: "server-current-access",
    refreshToken: refresh,
    clientId: "oauth-client",
    clientSecret: secret,
    resource: "https://mcp.example.test",
    scope: "tools.read",
  });
  const credentialId = await createCredential(options, vaultId, {
    access: token("stored-access-"),
    refresh,
    secret,
    auth: "client_secret_post",
    resource: "https://mcp.example.test",
    scope: "tools.read",
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation).toMatchObject({
    status: "valid",
    type: "vault_credential_validation",
    has_refresh_token: true,
    mcp_probe: null,
    refresh: { http_response: null, status: "succeeded" },
  });
  assertNoMarkers(validation);
  const after = await readOauthMcpTestMeta(options.origin);
  expect(after).toMatchObject({
    issuedCount: 1,
    tokenRequests: 1,
    postSecretMatched: true,
    clientIdMatched: true,
    resourceMatched: true,
    scopeMatched: true,
  });
  const retrieved = await json<{ auth: { expires_at?: string } }>(
    await options.request(`/v1/vaults/${vaultId}/credentials/${credentialId}`, {
      headers: BETA,
    }),
    200,
  );
  expect(Date.parse(retrieved.auth.expires_at ?? "")).toBeGreaterThan(Date.now());
  assertNoMarkers(retrieved);

  await invalidateOauthMcpAccessToken(options.origin);
  const again = await validate(options, vaultId, credentialId);
  expect(again.status).toBe("valid");
  expect(again.refresh?.status).toBe("succeeded");
  assertNoMarkers(again);
  expect(await readOauthMcpTestMeta(options.origin)).toMatchObject({
    issuedCount: 2,
    tokenRequests: 2,
  });
}

async function expectBasicClientAuth(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const secret = token("stored-client-");
  const refresh = token("stored-refresh-");
  await reset(options.origin, {
    accessToken: "server-current-access",
    refreshToken: refresh,
    clientId: "oauth-client",
    clientSecret: secret,
  });
  const credentialId = await createCredential(options, vaultId, {
    access: token("stored-access-"),
    refresh,
    secret,
    auth: "client_secret_basic",
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation.status).toBe("valid");
  assertNoMarkers(validation);
  expect(await readOauthMcpTestMeta(options.origin)).toMatchObject({
    basicSecretMatched: true,
    postSecretMatched: false,
  });
}

async function expectRevokedRefresh(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const refresh = token("stored-refresh-");
  const secret = token("stored-client-");
  await reset(options.origin, {
    accessToken: "server-current-access",
    refreshToken: refresh,
    clientSecret: secret,
    token: "invalid_grant",
  });
  const credentialId = await createCredential(options, vaultId, {
    access: token("stored-access-"),
    refresh,
    secret,
    auth: "client_secret_post",
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation.status).toBe("invalid");
  expect(validation.mcp_probe?.http_response?.status_code).toBe(401);
  expect(validation.refresh).toMatchObject({ status: "failed" });
  expect(validation.refresh?.http_response?.status_code).toBe(400);
  expect(validation.mcp_probe?.http_response?.body.includes("[redacted]")).toBe(true);
  expect(validation.refresh?.http_response?.body.includes("[redacted]")).toBe(true);
  assertNoMarkers(validation);
}

async function expectMissingRefreshToken(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const access = token("stored-access-");
  await reset(options.origin, {
    accessToken: "server-current-access",
    refreshToken: token("stored-refresh-"),
  });
  const credentialId = await createCredential(options, vaultId, { access });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation).toMatchObject({
    status: "invalid",
    has_refresh_token: false,
    refresh: { status: "no_refresh_token", http_response: null },
  });
  expect(validation.mcp_probe?.http_response?.status_code).toBe(401);
  assertNoMarkers(validation);
}

async function expectMcpUnreachable(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const closed = await reserveOauthMcpClosedPort(options.origin);
  const credentialId = await createCredential(options, vaultId, {
    access: token("stored-access-"),
    refresh: token("stored-refresh-"),
    secret: token("stored-client-"),
    auth: "client_secret_post",
    mcpUrl: `http://127.0.0.1:${closed}/mcp`,
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation).toMatchObject({
    status: "unknown",
    mcp_probe: { method: "initialize", http_response: null },
    refresh: null,
  });
  assertNoMarkers(validation);
}

async function expectTokenUnreachable(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const refresh = token("stored-refresh-");
  const closed = await reserveOauthMcpClosedPort(options.origin);
  await reset(options.origin, {
    accessToken: "server-current-access",
    refreshToken: refresh,
  });
  const credentialId = await createCredential(options, vaultId, {
    access: token("stored-access-"),
    refresh,
    secret: token("stored-client-"),
    auth: "client_secret_post",
    tokenUrl: `http://127.0.0.1:${closed}/oauth/token`,
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation.status).toBe("unknown");
  expect(validation.mcp_probe?.http_response?.status_code).toBe(401);
  expect(validation.refresh).toEqual({ http_response: null, status: "connect_error" });
  assertNoMarkers(validation);
}

async function expectMcpUnavailable(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const access = token("stored-access-");
  await reset(options.origin, {
    accessToken: access,
    refreshToken: token("stored-refresh-"),
    mcp: "unavailable",
  });
  const credentialId = await createCredential(options, vaultId, {
    access,
    refresh: token("stored-refresh-"),
    secret: token("stored-client-"),
    auth: "client_secret_post",
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation.status).toBe("unknown");
  expect(validation.refresh).toBeNull();
  expect(validation.mcp_probe?.http_response?.status_code).toBe(503);
  expect(validation.mcp_probe?.http_response?.body.includes("[redacted]")).toBe(true);
  assertNoMarkers(validation);
  expect((await readOauthMcpTestMeta(options.origin)).tokenRequests).toBe(0);
}

async function expectTokenUnavailable(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const refresh = token("stored-refresh-");
  const secret = token("stored-client-");
  await reset(options.origin, {
    accessToken: "server-current-access",
    refreshToken: refresh,
    clientSecret: secret,
    token: "unavailable",
  });
  const credentialId = await createCredential(options, vaultId, {
    access: token("stored-access-"),
    refresh,
    secret,
    auth: "client_secret_post",
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation.status).toBe("unknown");
  expect(validation.refresh?.status).toBe("failed");
  expect(validation.refresh?.http_response?.status_code).toBe(503);
  expect(validation.refresh?.http_response?.body.includes("[redacted]")).toBe(true);
  assertNoMarkers(validation);
}

async function expectRedirectIsNotFollowed(
  options: Scenario,
  vaultId: string,
): Promise<void> {
  const access = token("stored-access-");
  await reset(options.origin, {
    accessToken: access,
    refreshToken: token("stored-refresh-"),
    mcp: "redirect",
  });
  const credentialId = await createCredential(options, vaultId, {
    access,
    refresh: token("stored-refresh-"),
    secret: token("stored-client-"),
    auth: "client_secret_post",
  });
  const validation = await validate(options, vaultId, credentialId);
  expect(validation.status).toBe("unknown");
  expect(validation.refresh).toBeNull();
  expect([0, 301, 302, 303, 307, 308]).toContain(
    validation.mcp_probe?.http_response?.status_code,
  );
  expect((await readOauthMcpTestMeta(options.origin)).sinkSawAuthorization).toBe(false);
  assertNoMarkers(validation);
}

interface Scenario {
  origin: string;
  request(path: string, init?: RequestInit): Promise<Response>;
}

async function reset(origin: string, control: OauthMcpTestControl): Promise<void> {
  await resetOauthMcpTestServer(origin, control);
}

function token(prefix: string): string {
  return `${prefix}${crypto.randomUUID()}`;
}

async function createCredential(
  options: Scenario,
  vaultId: string,
  input: {
    access: string;
    refresh?: string;
    secret?: string;
    auth?: "client_secret_post" | "client_secret_basic";
    mcpUrl?: string;
    tokenUrl?: string;
    resource?: string;
    scope?: string;
  },
): Promise<string> {
  const created = await json<{ id: string }>(
    await options.request(`/v1/vaults/${vaultId}/credentials`, {
      method: "POST",
      headers: BETA,
      body: JSON.stringify({
        display_name: "mcp-oauth",
        auth: {
          type: "mcp_oauth",
          access_token: input.access,
          mcp_server_url: input.mcpUrl ?? `${options.origin}/mcp`,
          ...(input.refresh === undefined
            ? {}
            : {
                refresh: {
                  client_id: "oauth-client",
                  refresh_token: input.refresh,
                  token_endpoint: input.tokenUrl ?? `${options.origin}/oauth/token`,
                  token_endpoint_auth:
                    input.auth === "client_secret_basic"
                      ? { type: "client_secret_basic", client_secret: input.secret }
                      : { type: "client_secret_post", client_secret: input.secret },
                  ...(input.resource === undefined ? {} : { resource: input.resource }),
                  ...(input.scope === undefined ? {} : { scope: input.scope }),
                },
              }),
        },
      }),
    }),
    201,
  );
  return created.id;
}

async function validate(
  options: Scenario,
  vaultId: string,
  credentialId: string,
): Promise<ValidationResponse> {
  let leaked = false;
  const methods = ["log", "info", "warn", "error", "debug"] as const;
  const originals = methods.map((method) => console[method]);
  for (const method of methods) {
    console[method] = ((...args: unknown[]) => {
      const text = args.map((arg) => {
        if (typeof arg === "string") return arg;
        try {
          return JSON.stringify(arg);
        } catch {
          return "";
        }
      }).join("\n");
      if (MARKERS.some((marker) => text.includes(marker))) leaked = true;
    }) as (typeof console)[typeof method];
  }
  try {
    return await json<ValidationResponse>(
      await options.request(
        `/v1/vaults/${vaultId}/credentials/${credentialId}/mcp_oauth_validate`,
        { method: "POST", headers: BETA },
      ),
      200,
    );
  } finally {
    for (let index = 0; index < methods.length; index += 1) {
      console[methods[index]!] = originals[index]!;
    }
    expect(leaked).toBe(false);
  }
}

async function json<T>(response: Response, expected: number): Promise<T> {
  const text = await response.text();
  if (response.status !== expected) {
    throw new Error(`HTTP ${response.status}, expected ${expected}, ${text.length} bytes`);
  }
  return JSON.parse(text) as T;
}

function assertNoMarkers(value: unknown): void {
  const text = JSON.stringify(value);
  for (const marker of MARKERS) {
    expect(text.includes(marker), marker).toBe(false);
  }
}

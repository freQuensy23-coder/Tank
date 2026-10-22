import { startOauthMcpTestServer, type OauthMcpTestServer } from "../fixtures/oauth-mcp-local-server";
import { OAUTH_MCP_TEST_PORT } from "../fixtures/oauth-mcp-local-endpoint";

let server: OauthMcpTestServer | undefined;

export async function setup(): Promise<void> {
  server = await startOauthMcpTestServer({ port: OAUTH_MCP_TEST_PORT });
}

export async function teardown(): Promise<void> {
  await server?.close();
}

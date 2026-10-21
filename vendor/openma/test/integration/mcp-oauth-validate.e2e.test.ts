import { exports } from "cloudflare:workers";
import { describe, it } from "vitest";

import { OAUTH_MCP_TEST_ORIGIN } from "../fixtures/oauth-mcp-local-endpoint";
import { runMcpOauthValidationScenarios } from "../fixtures/oauth-mcp-validate-scenarios";

describe("Cloudflare MCP OAuth credential validation", () => {
  it("distinguishes valid, revoked, and indeterminate credentials against a local OAuth server", async () => {
    await runMcpOauthValidationScenarios({
      origin: OAUTH_MCP_TEST_ORIGIN,
      request: (path, init) =>
        exports.default.fetch(new Request(`http://localhost${path}`, init)),
    });
  }, 60_000);
});

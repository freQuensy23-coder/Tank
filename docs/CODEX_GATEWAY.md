# Codex subscription gateway

Tank serves `POST /v1/responses` on `http://tank:8790` in its existing Python
asyncio process. OpenMA's `gpt-6.1-sol` Model Card uses `api: openai-responses`,
`base_url: http://tank:8790/v1`, and the Docker secret `codex_gateway_key` as its
Bearer key. The gateway compares that key in constant time before reading the
request body or the OAuth file. No host port is published.

The configured `TANK_CODEX_AUTH_PATH` points to the operator-provisioned
`/codex-auth/auth.json` in the writable `codex-auth` volume. The application does
not search the host's Codex configuration, launch `codex login`, fall back to an
API key, or initiate login. OpenMA and its sandboxes have no mount for this volume.
Startup and staged reload preflight read only this configured file and validate
its shape; neither sends a model/authentication request. Missing or invalid
credentials prevent startup. A valid access token without a refresh token works
until refresh is needed, then returns `codex_login_required`.

## Protocol evidence

Verified against official OpenAI documentation and public source; none of the
checks used real credentials or authenticated endpoints.

* [Sign in with ChatGPT through a gateway](https://learn.chatgpt.com/docs/enterprise/sign-in-with-chatgpt-through-a-gateway)
  documents `https://chatgpt.com/backend-api/codex`, the ChatGPT Bearer credential,
  `ChatGPT-Account-ID`, and separating the gateway credential from provider auth.
  This implementation reverses the documented client credential handoff:
  OpenMA supplies only Tank's internal key; Tank loads and adds the OAuth headers.
* Public [openai/codex](https://github.com/openai/codex/tree/ce0323f2f2d3b25e82806cd555237da3db4e1897),
  pinned at `ce0323f2f2d3b25e82806cd555237da3db4e1897` for this inspection:
  [auth storage](https://github.com/openai/codex/blob/ce0323f2f2d3b25e82806cd555237da3db4e1897/codex-rs/login/src/auth/storage.rs)
  defines `tokens.access_token`, `refresh_token`, `id_token`, `account_id`,
  `last_refresh`, and `auth_mode`;
  [token data](https://github.com/openai/codex/blob/ce0323f2f2d3b25e82806cd555237da3db4e1897/codex-rs/login/src/token_data.rs)
  defines JWT expiry and the ID token account claim;
  [auth manager](https://github.com/openai/codex/blob/ce0323f2f2d3b25e82806cd555237da3db4e1897/codex-rs/login/src/auth/manager.rs)
  defines the fixed refresh URL `https://auth.openai.com/oauth/token`, public CLI
  client ID `app_EMoamEEZ73f0CkXaXp7hrann`, five-minute proactive window,
  eight-day fallback schedule, and saving optional rotated tokens;
  [OAuth client](https://github.com/openai/codex/blob/ce0323f2f2d3b25e82806cd555237da3db4e1897/codex-rs/login/src/oauth/client.rs)
  defines the JSON refresh grant (`grant_type`, `client_id`, `refresh_token`,
  `refresh_reason`, including `proactive_expiry` and `unauthorized_recovery`);
  [Responses endpoint](https://github.com/openai/codex/blob/ce0323f2f2d3b25e82806cd555237da3db4e1897/codex-rs/codex-api/src/endpoint/responses.rs)
  sends streaming `/responses` with SSE acceptance;
  [rate-limit headers](https://github.com/openai/codex/blob/ce0323f2f2d3b25e82806cd555237da3db4e1897/codex-rs/codex-api/src/rate_limits.rs)
  define the provider's usage/reset headers.
* OpenMA is pinned separately at `85d5361f535c7221de975838dc48444e468cc02e`.
  Its locked `@earendil-works/pi-ai@1.1.0` package was inspected from the public npm
  tarball. `dist/api/openai-responses.js` builds generic Responses requests;
  `dist/api/openai-codex-responses.js` builds subscription requests. The generic
  provider removes unsupported API-platform parameters only when it sees ChatGPT
  auth, which OpenMA deliberately does not receive. Tank therefore moves initial
  system/developer instructions from input to top-level `instructions`, sends
  `store: false` and `stream: true`, and removes `max_output_tokens`, `temperature`,
  `prompt_cache_retention` and `prompt_cache_options`. Tools, reasoning effort,
  encrypted reasoning, service tier, usage, function calls and remaining Responses
  data pass through. No spending, token or daily quota is imposed by the gateway.

These sources establish protocol compatibility, not account entitlement to a model.
The initial offline suite did not establish live access. Authorized live probes on
18 October 2026 subsequently verified `gpt-6.1-sol`, SSE, a synthetic tool call and
result, and nonstream JSON through the actual gateway; see
[`verification/live-codex-gateway-2026-10-18.json`](verification/live-codex-gateway-2026-10-18.json).
The provider omitted Content-Type on its SSE responses, so nonstream collection
uses body framing while preserving actual JSON objects. OAuth refresh and the
production container deployment were not exercised.

## Refresh and isolation

Provider destinations are fixed module constants and cannot be changed by request
headers, body parameters or environment proxy settings. Redirects are never
followed. Requests are built explicitly so HTTPX's response cookie jar cannot
send cookies to another upstream request. Incoming cookies, authorization,
account, routing and arbitrary headers are never forwarded. The internal key
never leaves Tank.

Every model call reloads the auth file, observing an operator's replacement.
Refreshes serialize in one asyncio lock and recheck current credentials. A 401
triggers at most one refresh/retry; concurrent 401s reuse the replacement. A wave
of simultaneous failed refreshes shares the same failure for one second to avoid
replaying a rejected refresh token immediately. This is refresh-failure
coalescing, not a model usage limit. A request disconnect does not cancel an
in-flight refresh: its shielded task can persist rotated tokens and is drained
before HTTP client shutdown.

The refresh response is validated before replacement. Optional absent refresh and
ID tokens retain their previous values. Tank reloads and merges the file before
writeback, preserving unrelated fields. If its token fields changed during the
exchange, the operator's replacement wins. Writes use an adjacent mode-0600 temp
file, file fsync, atomic replace, and directory fsync. The volume must have a
single active Tank gateway writer; atomic replacement protects partial reads,
but there is no distributed lock for multiple containers or an external writer
racing at the exact replacement instant.

The HTTP boundary returns static local errors and does not print provider
exceptions, credentials, request bodies or auth documents. Known internal keys,
access tokens, refresh tokens, ID tokens, old rotated credentials, and their JSON
and URL-escaped forms are redacted from provider response bodies and allowed
response header values. The streaming redactor retains incomplete secret prefixes
across arbitrary chunk boundaries. An explicit response-header allowlist excludes
cookies, authorization and arbitrary upstream headers.

## Streaming and usage limits

SSE bytes are forwarded as they arrive with awaited writes for backpressure.
Downstream disconnect cancels the upstream model stream; all paths close the
upstream response. Existing `response.failed`/`error` events pass through, including
usage-limit reset data. A broken upstream stream adds a static SSE error instead
of pretending it completed. Nonstream callers collect the terminal Responses
object, and a stream without a terminal response fails explicitly.

Provider HTTP status, error JSON, `Retry-After`, recognized `x-ratelimit-*` and
Codex primary/secondary usage/reset headers are retained. A provider 429 remains a
429; Tank invents no retry/reset time and does not automatically retry it. It
never converts usage limits into a local budget.

## Offline validation

Run:

```sh
.venv/bin/pytest tests/test_codex_gateway.py -q
.venv/bin/ruff check src/tank/codex_gateway.py tests/test_codex_gateway.py
```

The suite starts the actual aiohttp gateway on ephemeral localhost ports, supplies
only temporary `FAKE-auth.json` files and fake internal keys, and replaces every
provider/auth HTTP exchange with HTTPX MockTransport. It covers internal auth
before credential access; header and cookie isolation; request translation;
provider error and 429/reset preservation; expired-token and concurrent-401
singleflight refresh; account fallback; optional token retention; external login
replacement; malformed/expired refresh responses; atomic-write failure; mode-0600
persistence; redirect refusal; split-secret and rotated-token redaction; SSE failure,
backpressure/cancellation; nonstream completion/truncation; and lifecycle startup
without provider traffic. Live model requests, live refresh, operator auth files,
subscription entitlement and production networking are intentionally not tested.

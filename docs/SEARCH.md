# DuckDuckGo search

`web_search` uses `ddgs==9.16.0`, without an API key or charge. DuckDuckGo has no
official search API. Reading its results page and the possibility of blocking
are accepted in PRD D9. No other search engine is used.

`tank.search.DuckDuckGoSearchService(user_agent=...)` calls the pinned DDGS
`text(query, backend="duckduckgo", max_results=10)` implementation in a worker
thread. Source ingestion, session streams and other asyncio tasks remain free
while DDGS blocks. Results retain the library's `title`, `href` and `body`
fields. EventToolService saves each result's complete JSON bytes as durable
evidence and uses those fields for title, URL and readable snippet.

## Timing, errors and accounting

The default limiter is a process-owned object shared by every service instance,
including instances created after SIGHUP configuration changes. It serializes
requests and leaves at least two seconds between actual HTTP request starts,
including retries and calls from concurrent sessions. Cancellation drains a
submitted blocking request before the shared slot is released. A request
cancelled while waiting for the slot makes no attempt.

Rate limits, blocks, challenges and timeouts receive three retries: wait 5 seconds
after the first failure, 15 after the second, and 45 after the third. The shared
two-second limiter applies again before each retry. Additional concurrent work
can extend these waits. Every actual attempt invokes the caller's `on_attempt`
accounting callback; EventToolService appends an immutable `ops.llm_usage` row
with `call=WEB_SEARCH`, `web_searches=1`, `cost_usd=0`, and `billing_basis=FREE`.
Token fields are zero. No paid search cost or API credential is involved.

After the fourth retryable failure, or the first nonretryable failure, the agent
receives a structured error. `SearchError.as_dict()` identifies DuckDuckGo,
the error code and attempt count, with `verification=UNCONFIRMED` and
`certainty=MEDIUM`. A failed search produces no evidence and cannot upgrade a
fact. Error messages omit queries and vendor exception details.

## Why the backend argument needs enforcement

The installed 9.16.0 source was inspected, rather than assuming the backend
argument prevents fallback:

* `ddgs.DDGS._get_engines` calls itself with `auto` when no engine instance was
  selected. Its automatic registry contains other engines.
* `BaseSearchEngine.request` returns `None` for every non-200 status. DDGS then
  reports generic no-results, losing the difference between a block and an
  ordinary empty search.
* DDGS's default HTTP client chooses a random browser identity; its constructor
  can also read `DDGS_PROXY` from the environment.

The adapter overrides engine selection with a single instance of the pinned
DuckDuckGo engine. It does not consult the registry or call the upstream
fallback. It inherits the real engine's payload builder, HTML parser and result
normalizers. DDGS still aggregates and returns the results through `text`.
The upstream engine's `provider="bing"` attribute describes its syndicated
result source; it does not authorize a request to Bing. The endpoint guard
permits only DuckDuckGo.

The HTTP transport accepts exactly one POST to
`https://html.duckduckgo.com/html/` per attempt. It rejects other endpoints,
methods and additional hidden requests. HTTP redirects are not followed;
ambient proxies and HTTP client retries are disabled. The declared source
User-Agent is sent directly. No random browser identity, proxy rotation,
challenge solving, engine substitution or credential lookup occurs.

HTTP 429 is a rate-limit error. HTTP 202, 401, 403, 418, 503, redirects, and
recognized challenge pages are block errors. HTTP timeouts retain their typed
error even though DDGS wraps engine exceptions. Other HTTP failures, parser
errors and empty results return an error without fallback. The runtime rejects
any installed DDGS version other than 9.16.0; upgrading requires a fresh source
audit and parser/transport tests.

Audited package file SHA-256 values:

| File in installed `ddgs` package | SHA-256 |
|---|---|
| `ddgs.py` | `1cd33c4c5dd81ab865d6257327c4f079fae0cbe2e37ec8c7a9c575df771f27b1` |
| `base.py` | `429013e3fac08b17e9e832062de7d925e0b4e543dafdd3a3a59f3520e237405e` |
| `engines/duckduckgo.py` | `91ff4d7443e229cab28c25732df8e046f5730f8d21fe5ee22421d3536bdf71bb` |
| `http_client.py` | `f47f9b973dcfd7ee195f34e95bd78f76b8b7a9bced0f14095c66a906a4f3c5de` |
| `exceptions.py` | `0b5b9f886c0adcda572916f70e5b1aa32d27ff19d29df9d80ea9bab5afb4c6fb` |

## Verification

`uv run pytest -q tests/test_search.py` exercises the actual pinned engine/parser
against local HTML, with an HTTP MockTransport and controlled clocks. It covers
the exact request, raw results, missing-registry fallback trap, rate limits,
blocks, challenges, redirects, timeouts, retry timing, attempt accounting,
concurrent calls, cross-generation spacing, cancellation, version drift and
event-loop responsiveness. The tests never need credentials or external network.

One credential-free public smoke search ran from the host at
`2026-10-17T18:15:55.731360Z`, using the same adapter, one HTTP attempt and
query `site:sec.gov crypto`. `config/sources.yaml` declared `user_agent: null`,
so the public Settings default was used:
`TankV0/0.1 (operator contact not configured)`. The search succeeded with 10
results. No fallback, redirects, proxies, secrets, `.env` reads or model calls
were used. This proves access for that host and time, not ongoing availability
or access from other deployments.

A second single-attempt smoke ran from the rebuilt `tank-v0:local` image at
`2026-10-17T18:21:23.077673Z` on Docker's bridge network, with a read-only root
filesystem, ephemeral `/tmp`, no bind mounts and no runtime secrets or operator
environment supplied. The same query and User-Agent succeeded with 10 results.
The imported search module's SHA-256 matched the workspace source:
`84cc2b691a2f056f95efddc145a5c7a07299f3487263e751496387fe2c6ee24e`.
The image had previously passed a network-disabled DDGS version/import check.
If a deployment later becomes blocked, retain that error and resolve deployment
availability without bypassing DuckDuckGo's block.

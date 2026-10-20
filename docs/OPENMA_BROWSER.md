# OpenMA browser inside each Tank session VM

The pinned upstream commit is `85d5361f535c7221de975838dc48444e468cc02e`.
Tank Technical Design §3.2 requires OpenMA's built-in browser to run in the
session sandbox. The upstream canonical configuration accepted browser opt-in,
but Node's two `buildTools` call sites supplied no `env.browser`. Its existing
Node browser adapter would launch Chromium in the OpenMA container.

`deploy/openma_browser/overlay.json` supplies count-checked source edits;
`source-hashes.json` supplies the original upstream SHA-256 guards consumed by
`deploy/prepare_openma_source.py`. The new adapter is copied into the agent
harness package, whose existing dependencies already provide BrowserHarness
and SandboxPort. No additional JavaScript dependency or lockfile change is
required. Both the normal and confirmed-tool execution paths bind the exact
session's SandboxPort to `createSandboxBrowserHarness`.

## Execution and state

The TypeScript adapter implements the actual upstream `BrowserHarness`,
`BrowserSession` and `BrowserPage` contracts. Upstream `buildBrowserTools` still
provides its seven original tools: navigate, screenshot, click, type, get text,
evaluate and close. The adapter encodes JSON requests as base64, quotes the
fixed command arguments and calls only `SandboxPort.exec`. Page content, URLs,
selectors and typed text are never interpolated as shell syntax.

The worker's command-line reply is one anchored `__TANK_BROWSER_RPC_V1__ `
JSON line. SandboxPort combines stdout and stderr, and the real BoxLite guest
runtime emits diagnostic warnings on stderr. The adapter therefore extracts
exactly one framed reply, validates its version and shape, and refuses native
command-exit/error markers. Missing, duplicate, malformed and oversized replies
fail explicitly; it does not filter particular warning strings or accept an
unframed legacy response.

Inside that sandbox, `/opt/tank/browser-worker.py` starts a Python Playwright
daemon lazily. Its Unix-domain socket and startup lock live under `/tmp`; it
never opens a TCP or external CDP listener. Playwright and Chromium run inside
the same VM as the other session commands. Requests are serialized, preserving
the page between individual tool calls. Startup is locked so concurrent
commands cannot open two Chromium instances against one profile.

One 75-second monotonic operation budget covers the startup lock, daemon
and Playwright initialization, socket connection, request write, action queue
and browser action. The daemon receives the original absolute deadline.
The client permits five additional seconds only to receive cleanup/error
responses after operation expiry; one absolute response deadline bounds all
chunks. No operation, retry or new launch receives that grace. Total client
receipt time is at most 80 seconds, within the unchanged 90-second SandboxPort
cap and with ten seconds left for execution/transport overhead. Real KVM measurements on
18 October found a healthy cold Playwright driver taking 17.76 seconds, so the
earlier separate 15-second startup cutoff was removed. A startup child that
exits early reports its exit code instead of waiting for the full budget.
The command-line client supplies that same absolute deadline to the daemon.
It includes time waiting for the action lock, so expired queued actions never
execute. Native launch receives only the remaining time. An action that exceeds
the budget, or any failed native launch, recycles the daemon and its exact owned
Chromium/driver descendant tree before the next request can proceed. Linux
subreaper adoption and stable pidfds cover detached and double-forked helpers;
no process-name or UID sweep is used. Fatal cleanup keeps the action lock until
the error is flushed and the worker exits. A new request reopens the persistent
profile through normal daemon startup. This closes the daemon deadline and
cleanup gap in local tests. The earlier baseline shell passed the real KVM
browser contract; the combined 4f image failed first-open qualification both
cold and warm. The newer response-grace diagnostic patch has no KVM readiness
claim and must be packaged with a newly bound manifest if later qualified.

The selected engine is Playwright 1.64.0's default bundled Chromium headless
shell (Chromium 156.0.8078.4). The adapter omits an explicit channel, matching
Playwright's supported headless default. There is one fixed engine selection
and no fallback chain. The full `channel="chromium"` mode did not become ready
within the same budgets in the tested nested KVM environment.

The sandbox image runs `prepare-font-cache.py` after copying the final worker
and installing the pinned Playwright browser. It uses that exact default shell
to generate genuine format-12 public font caches with a temporary local-only
profile, closes the browser, and deletes the temporary profile. Original
Bookworm format-8 cache bytes remain unchanged; newly created older-format
aliases to format 12 are removed. The build manifest binds the cache hashes,
font inputs, browser version and final worker SHA. The image contract requires
that manifest and verifies its cache and worker hashes before browser actions.
The tested producer generated all 18 byte-identical caches used in the real
KVM comparison. No font files are removed or renamed.

The Chromium persistent profile is
`/workspace/.tank-browser/<sha256(sessionId)[:32]>/profile`. Downloads remain
under that session directory. The profile is created from empty state and
receives no operator X profile, X storage state, Codex sign-in or host browser
configuration. Each production session already has its own litebox VM; the
hashed directory also separates identities if a SandboxPort is reused in a
test. Browser requests inherit the VM's network policy, so replay cannot gain
network access through the OpenMA host.

`disposeBrowserTools` and aggregate `disposeTools` close and flush the browser
context at the end of a turn, before OpenMA's workspace checkpoint runs.
Confirmed tools dispose their toolset in `finally`. Browser cleanup errors
propagate through runtime finalization, preventing publication of a checkpoint
while Chromium might still be writing the profile. Cookies and localStorage
survive reopening; live DOM state belongs to the active browser context.
The profile participates in upstream `/workspace` checkpoints and restore,
including the upstream archive size limit. Crash recovery has the guarantees
of the most recently published workspace checkpoint; uncheckpointed browser
changes are not made durable by this adapter.

## Image

`deploy/sandbox.Dockerfile` uses Python 3.12, includes the specified analysis
libraries and PostgreSQL client, installs `playwright==1.64.0`, installs its
matching Chromium with Linux system dependencies, and copies the worker.
`PLAYWRIGHT_BROWSERS_PATH=/opt/playwright` locates image-owned browser binaries.
Chromium runs headless inside the microVM using Playwright's own process
transport. It requires no host Playwright installation.

## Offline evidence

These checks were recorded on 17–18 October 2026 without application credentials:

| Check | Result |
| --- | --- |
| Full pinned browser-harness suite after overlay | 28 passed, including 10 Tank tests |
| New TypeScript adapter compiled against installed upstream types | Passed |
| Actual Chromium through a local fake SandboxPort executor | Passed |
| Fixture actions, evaluate, content and PNG bytes | Passed |
| Same-session page continuity during an active context | Passed |
| Cookie/localStorage retention after close/reopen | Passed |
| Profile copied into a replacement fake VM directory | Passed |
| Other session sees no first-session cookie/localStorage | Passed |
| Aggregate agent toolset and confirmed-tool cleanup | Passed |
| Slow startup, shared response/lock deadlines, startup-child exit and response-only grace | 7 regression tests passed |
| Linux real Chromium hung action, expired queue, exact cleanup and profile recovery | Passed; 16 owned processes, zero new live orphans, unrelated process alive |
| Linux detached/double-fork startup timeout and exited-parent cleanup | Both passed |
| Injected native launch timeout/error before outer deadline | Both passed; queued mutation refused, exact Linux subprocess cleanup and new-daemon recovery |
| Python 3.12 sandbox image, `--network none`, local fixture only | Passed |
| Real litebox KVM boot and guest Python execution | Passed: Python 3.12.15, aarch64 |
| Full Chromium channel in a fresh real KVM guest | Native 180-second launch timeout; no page |
| Default Chromium headless-shell in a real KVM guest | Did not become ready within the 75/90-second limits |
| Full Chromium, production UID/groups/default Docker seccomp, `/workspace` profile, explicit 1 CPU / 1536 MiB diagnostic allocation (0.9 runtime) | Driver started in 18.20 seconds; no browser readiness within 75/90 seconds; owned VM stopped |
| Matched 0.10.6 dynamic runtime, CPU1/2048 MiB, actual TS adapter/current framed worker | Boot and worker hash verified; open timed out; owned VM stopped |
| Same 0.10.6 tuple, official software graphics flags only | No readiness/DOM/PNG within original budget; owned VM stopped |
| Same 0.10.6 tuple, matched direct guest-loopback CDP transport only | Chromium never reached CDP readiness before driver start; no DOM/PNG; owned VM stopped |
| Default headless shell on 0.10.6, CPU1/2048, validated format-12 caches | Full 21-RPC real TS/VM contract passed: DOM/actions/PNG/continuity/isolation/profile persistence/real guest stderr framing; owned VM and record removed |
| Combined default shell, daemon deadline cleanup and generated-cache image (4f) | Docker contracts passed; actual KVM cold and warm first open timed out, no DOM/PNG or fatal subtest reached |

The image check imports every required analysis library, asserts Python 3.12
and Playwright 1.64.0, navigates a local HTTP fixture, fills/clicks/extracts text,
validates a real PNG, and checks session isolation and profile persistence.
Reproduce it against the built sandbox image:

```sh
docker run --rm --network none \
  -v "$PWD/deploy/openma_browser/image-contract.py:/tmp/image-contract.py:ro" \
  tank-analysis:python3.12-local python3 /tmp/image-contract.py
```

The local adapter contract uses Node 24 or newer, a Python environment with
Playwright 1.64.0, and its installed matching browser cache. It deliberately
executes Chromium as an isolated temporary subprocess to test the transport;
it does not certify VM isolation. Supply those two paths explicitly:

```sh
TANK_BROWSER_TEST_PYTHON=/path/to/python \
PLAYWRIGHT_BROWSERS_PATH=/path/to/installed/playwright-browsers \
node --test deploy/openma_browser/local-contract.test.mjs
```

This fake executor passes a minimal environment, uses only a loopback fixture,
creates empty profiles, and cleans up its processes and directories. The
production adapter has no local subprocess or host-browser fallback.

Playwright's [persistent-context API](https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch-persistent-context)
and [page API](https://playwright.dev/python/docs/api/class-page) describe the
browser operations used by the VM worker. The actual pinned upstream source
was checked for the BrowserHarness and tool result contracts, including PNG
base64 image output to the model.

The latest diagnostic used the framed OCI with a fresh credential-free host
container and kept guest networking disabled. Guest memory initially had about
1.46 GB available, RLIMIT_NOFILE was 1024, and RLIMIT_NPROC was 4096 (hard 8192).
No process-limit error or child exit was observed before the bounded stop. These
measurements do not establish the cause of Chromium initialization failure.
The separate BoxLite initialization helper resolved `debian:bookworm-slim` to
arm64 manifest `sha256:a1b86db52ce3daef089e45aabe36dfec4091f82464c25c1fdcf03de197cbe82a`;
it is a separate runtime input from Tank's supplied sandbox OCI.

The dated 0.10.6 browser controls and exact artifact/log hashes are in
`docs/verification/browser-runtime-0.10.6-controls-2026-10-18`. The failed controls and accepted-cache evidence remain preserved. The final
supported default-shell control passed in a real VM; its baseline worker did
not yet include the daemon deadline patch. Exact baseline and coherent cold/warm
proofs are frozen in `docs/verification/browser-coherent-2026-10-18`. Warm guest
verification improved to 18.09 seconds from 177.37 seconds cold, but first open
still timed out after 80.61 seconds (cold84.00). All18 prepared caches were
opened and recursively hash-verified unchanged, with no font-file reads.
Cold extraction alone does not explain the failure. The newer response-only
grace fixes an independently reproduced diagnostics race, not slow KVM launch.
No production adoption or further VM repeat is implied. There is no transport
or engine fallback.

To exercise the Linux deadline regressions, mount the current worker over
`/opt/tank/browser-worker.py` in the Python/Playwright sandbox image and run
`deadline-contract.py`, `startup-contract.py` (also `--exited-parent`), and
`launch-failure-contract.py` separately. `response-contract.py` exercises real
Unix sockets with synthetic daemon cleanup timing: structured failure during
grace succeeds, while cleanup beyond grace and partial reply chunks stay bounded. The real Chromium deadline fixture
reduces its local test budget to 2.5 seconds; run it serially to avoid competing
browser startup in that deliberately short test window. Production stays at
75 seconds per operation, five seconds for response-only cleanup receipt,
and 90 seconds per sandbox execution.

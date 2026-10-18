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

Inside that sandbox, `/opt/tank/browser-worker.py` starts a Python Playwright
daemon lazily. Its Unix-domain socket and startup lock live under `/tmp`; it
never opens a TCP or external CDP listener. Playwright and Chromium run inside
the same VM as the other session commands. Requests are serialized, preserving
the page between individual tool calls. Startup is locked so concurrent
commands cannot open two Chromium instances against one profile.

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

All of these checks ran on 17 October 2026 without application credentials:

| Check | Result |
| --- | --- |
| Full pinned browser-harness suite after overlay | 24 passed, including 6 Tank tests |
| New TypeScript adapter compiled against installed upstream types | Passed |
| Actual Chromium through a local fake SandboxPort executor | Passed |
| Fixture actions, evaluate, content and PNG bytes | Passed |
| Same-session page continuity during an active context | Passed |
| Cookie/localStorage retention after close/reopen | Passed |
| Profile copied into a replacement fake VM directory | Passed |
| Other session sees no first-session cookie/localStorage | Passed |
| Aggregate agent toolset and confirmed-tool cleanup | Passed |
| Python 3.12 sandbox image, `--network none`, local fixture only | Passed |
| Real litebox microVM/KVM end-to-end execution | Unverified: local Docker daemon has no `/dev/kvm` |

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

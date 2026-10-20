# Owning asynchronous harness output failures

The synchronous `broadcast` and `broadcastProducedEvent` methods enqueue
asynchronous persistence but return before the harness finishes. A persistence
failure can therefore reject the queue long before the runner reaches its
awaited `drain()`. Node reports that detached rejection as unhandled, and the
existing process failure net correctly exits for unknown unhandled errors.

The repair appends one exact upstream-source replacement to `overlay.json`.
Each `enqueue()` immediately observes rejection on its output promise while
retaining and returning the **original** promise. The stored queue remains
rejected. `drain()` and awaited stream callers still receive the original
error; later queued frames do not pass the failed write. The driver execution
fence, realtime publication gate, runner finalization, worker settlement and
global process failure handler are unchanged.

In the actual F08 incident, root's recovery receipt establishes that archival
cancelled the session's executions about 51 ms before the fatal rejection.
The fence refusal was valid. The error ownership defect promoted that session
boundary into a controller exit. The earlier event-acceptance revision conflict
was an HTTP error caught by Hono; the available evidence does not establish it
as the cause of the fence loss.

The existing execution overlay preparer applies the new row with the same
full-input SHA, exact-source and full-output SHA checks as every other row.
The canonical composer already enumerates those rows, so this previously
unmodified upstream file becomes tracked in the final patched-file map and
runtime identity. No special source normalization or fallback is needed.

Run the permanent regression with an existing dependency image and an exact
prepared source tree (the tree is read-only):

```sh
python3 deploy/openma_execution/run-output-lifetime-tests.py \
  --prepared-source /absolute/prepared-source \
  --image LOCAL_NODE24_DEPENDENCY_IMAGE \
  --output /tmp/NEW_OWNED_OUTPUT_DIRECTORY
```

The test runs two ephemeral Docker Desktop containers with networking disabled,
1 GiB/2 CPU, and no native device, HOME, credentials, SDK initialization, service
or model. It mounts the actual driver and process rejection net. Only the
engine, projection, sandbox and realtime dependencies are mocks. The negative
control must exit 1 with the same fence-loss/unhandled-rejection stack. The
patched child must remain alive, reject at delayed drain with the exact original
error, block later stale frames, execute another session, preserve generic
recording errors and awaited stream errors, and retain ordinary output order.

Python source-guard controls are in
`tests/test_openma_output_lifetime_overlay.py`. Actual before/after receipts,
source hashes, archived old overlay and type-check commands are in
`docs/verification/output-lifetime-2026-10-19`. Strict module type checking
reports the identical existing `definition-resolution.ts:84` TS2322 before and
after this change, with zero new diagnostics; it is not a full-project clean
TypeScript claim. Native, model, API and production qualification remain
separate from this offline lifetime regression.

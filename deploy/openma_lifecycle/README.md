# Retained-session lifecycle overlay and historical qualification records

The lifecycle is a required stage of the canonical deployment composer and is
included in the running OpenMA image. The historical candidate status below is
not its current deployment status. This stage preserves the native session and
history across turns, stops the guest at the safe idle boundary, resumes through
a fresh SDK handle, fences competing ownership, and cleans only its exact owned
resource. Removing it would remove those required behaviors; its size alone is
not a reason to replace it with an unqualified shortcut.

Later, scoped native evidence supersedes the original offline-only statement:

- [Mounted file API r16c](../../docs/verification/native-file-api-r16c-2026-10-19/verification.json)
  exercises the actual factory/provider guest file transport.
- [Replay network r18](../../docs/verification/native-replay-network-r18-2026-10-19/verification.json)
  verifies the native replay DNS/network/read-only database boundary.
- [Primary replay factory](../../docs/verification/primary-replay-native-probe-2026-10-20/report.json)
  verifies the registered primary replay environment with exact native cleanup.
- [Selected live-source E2E](../../docs/verification/normal-source-e2e-r56-2026-10-20/world/independent-result.json)
  records real RSS, Jev, native Managed analysis/scout sessions, market analysis,
  and Telegram delivery. It does not qualify every lifecycle crash/restart case.

These are ARM64 results for their recorded image/runtime identities. They do not
certify a rebuilt image, x86 KVM, every controller crash/restart sequence, or the
complete 27-fixture acceptance. Earlier failed native attempts and the original
build-time `NOT_RUN` metadata remain unchanged. The sections below retain the
historical preparation record, including its then-pending checks.

For current operator preparation use the canonical `prepare_openma_source.py`
command and [complete composition recipe](../OPENMA_PREPARATION.md). The direct
`prepare.py` example below documents the lower-level retained stage and historical
workcopy; it does not include the later execution-status and SKILL composition.

Historical candidate status: prepared and verified offline; **not adopted at that
point**. No VM was
started for these checks. Native mounted-factory boot qualification remains a
separate prerequisite owned by the deployment team.

The candidate keeps the pinned OpenMA session and history, current Node pi runner,
custom tools, sandbox mounts, and per-exec secret injection. It uses the pinned
public `createBoxLiteProvider` / `createBoxLiteManagedRuntime` APIs with
`autoRemove=false`, fresh SDK handles on resume, and the pinned SQL resource
fence/publication and orphan primitives. It adds no service or object storage.

`prepare.py` copies an already prepared OpenMA checkout to a **new** isolated
workcopy. It checks hashes of provider/fence sources and exact patch contexts;
it never edits the source, starts Docker, or deploys the result. Example:

```sh
python3 deploy/openma_lifecycle/prepare.py \
  /tmp/tank-openma-replay-prefix-final4 /tmp/tank-openma-retained-sdk-direct-v7-final
```

The current workcopy is `/tmp/tank-openma-retained-sdk-direct-v7-final` and describes its
source/native identity in `tank-retained-candidate.json`. After the deployment
owner changes the qualified runtime, regenerate into another new destination.
The fingerprint includes that runtime provenance, native checksums, dependency
lock, Dockerfile, patched sources, lifecycle modules, allocation/image/rootfs
identity, mounts/network, scoped role/environment/skills/resources policy, and
nonsecret DB target/role. Changed fingerprints are rejected on resume; authorized
final cleanup still removes the exact previously published resource.

## Integration hooks

- `LiteBoxSandbox.setRetainedBoxFactory` supplies a compatible underlying box.
  The existing adapter still performs its initialization and handles its mounts,
  environment, memory/output synchronization, and scratch cleanup.
- The Node runner binds the lifecycle before cold preparation and warm reuse.
  Finalization waits for tool/browser disposal, synchronization, stream drain,
  and artifact publication, then stops the VM while retaining its disk. No
  inactivity timer or invented session cap determines that boundary.
- A session-stable SQL resource scope publishes an allocating intent (scope,
  fingerprint, exact stable native name), only after an absent-name preflight,
  before native allocation. It publishes the returned native ID before caller
  guest operations. A controller crash after allocation resolves only its bound
  intent through a fresh public lookup; it never adopts an unbound disk. Turn-level execution fencing and session claim
  serialization prevent overlapping turns. Idle releases resource ownership;
  the next turn resumes through a fresh native SDK handle.
- Shutdown stops polling and drains active turns before retaining their disks.
  Native final archive/deletion cancels and drains its active turn before cold,
  ownership-checked exact-resource reap. Lost execution finalization stops only
  its current resource, skips publication, and always stops renewing its resource
  lease; a failed release acknowledgement can expire safely. No SQL unarchive/history mutation is
  used. The native session policy remains responsible for archive eligibility.
- Failed initial allocation compensation inspects only the exact stable owned
  name that was absent before this fenced attempt. It never adopts an
  unpublished rootfs. Failed published resume stops via a fresh ownership-checked
  SDK handle and preserves its disk. Durable orphan reconciliation never reaps
  a currently published resource. Unknown SQL publication outcomes preserve the
  disk instead of assuming it is unpublished.
- Successful native stop immediately invalidates the in-memory runtime. SQL
  publication/release retries keep an explicit stopped state, reject guest
  commands, and cannot double-stop or reuse the invalid native handle. A new
  valid native turn can finish that known-stopped SQL transition before resuming.
- One original execution deadline starts before native spawn and covers its
  acknowledgement, stdin closure, output draining, and exit. A timed-out command
  poisons its runtime before returning at that deadline. Kill acknowledgement may still be pending, so all subsequent guest
  I/O is refused until safe VM stop and a fresh resume; no timeout is extended.
  A process returned by spawn after expiry is killed exactly once without any
  stdin handshake, its late exit rejection is consumed, and its streams are
  cancelled when possible. Deferred spawn/stdin/kill/exit tests prove this path.
- The public SDK facade supports synchronous 0.9 and asynchronous 0.10.6
  `Box.info()` contracts, exposing the pinned provider's synchronous acquisition
  snapshot and refreshing it after public start/stop. The 0.10.6 declaration
  hashes match the SDK installed in the deployment owner's actual control VM.

## Evidence and limits

`verification.json` records source hashes and checks. The 43 local/Node24 tests
exercise fencing, exact cleanup, configuration mismatch, command draining,
per-exec environment rotation, binary copies, fresh-handle resume, and partial
publication/release failures. `pinned-provider.integration.mjs` uses the **actual
pinned provider, patched sandbox adapter, SQL fence, and persistent SQLite**.
Only its native client/disk contents are synthetic. It specifically injects
provider initialization failure, suppressed remove failure, post-allocation
throw, failed published resume, initial publication committed before lost
acknowledgement, idle/release acknowledgement failures, and lost-fence orphan
receipts. A real child process exits at native allocation before handle return;
its persistent SQLite intent is recovered by a replacement controller without
duplicate allocation. The native disk in this process-exit test is still
synthetic. A public injected fence clock models lease expiry; no session/history
SQL is edited. `pinned-runner.integration.mjs` exercises the actual patched
runner's lost-execution, publication-failure, and concurrent interrupt hooks.

Strict types for the four modules and their assignments to actual pinned public
contracts pass, including strict assignment from the official 0.10.6 SDK
to the facade and from the facade to the actual pinned provider client. The complete control-plane package has six existing
browser/schedule-versus-Anthropic-contract diagnostics in both baseline and
candidate; `typecheck-baseline.json` records identical diagnostics and zero
additional candidate errors. This is not a clean full-package typecheck claim.

Before adoption, prove on the finally qualified mounted native runtime: real
workspace/rootfs/browser-profile persistence and same native session/history
across idle, graceful controller restart, and crashed-controller recovery;
VM/RAM release at safe finalization; per-exec credential rotation after resume;
configuration drift rejection; exclusive competing-controller ownership; native
final archive exact disk cleanup; and fault/retry behavior using real persistent
volumes and the deployed database. Regenerate and re-run checks after runtime or
boot-overlay changes. These offline checks do not certify real VM durability,
full service restart behavior, live model integration, or production adoption.

One conservative limit remains explicit: a hard crash after publishing an
allocating intent but before native allocation leaves an intent with no disk;
restart refuses it with `retained_incomplete_allocation_missing` rather than
creating an empty replacement. A catchable allocation failure whose compensation
is confirmed missing is recorded as aborted and can retry. Native final cleanup
can archive a missing incomplete intent without booting a VM. This limit needs
review against the required restart contract before production acceptance.

The version 1 evidence is preserved verbatim as `verification.v1.json`,
`offline-check.v1.log`, and `typecheck-baseline.v1.json`. The current evidence
supersedes its command-timeout assessment: v1 started the deadline after spawn
and stdin closure; v2 bounds all execution phases with the original deadline.
Both versions remain isolated and are not production acceptance records.

Version 3 regenerates the corrected deadline lifecycle against the deployment
owner's `tank7-headless-shell` base with SDK0.10.6 and the recorded dynamic d44
shim. Its explicit native security flags are preserved in typed allocation and
fingerprint; no new native security layer is claimed. The offline dependency
image is still the recorded existing Node24 image; native lifecycle qualification
of the newly built image remains pending. Version2 evidence is preserved.

Version4 evidence uses the versioned `v3b` workcopy, admits the adapter's
existing optional `name` field, and directly strictly typechecks the dynamically
loaded adapter alongside the lifecycle/SDK contracts. Earlier control-plane
checks did not include that adapter file directly. `native-qualification.mjs` and
`NATIVE_QUALIFICATION.md` are prepared for the deployment owner's bounded actual
native run; the two no-start guard tests pass, but its native phases have not run.
Prior evidence remains preserved.

Version5 rebases the production candidate onto current-prefix final3. It validates
latest replay Session.metadata at every warm attachment using the adapter public
verifier, then requires the exact existing readonly run mount. Mutable hash/cutoff
stay outside static allocation identity. Actual-filesystem/provider/SQL tests
cover prefix advance, stale/tampered/swapped inputs and controller replacement.
Direct adapter/helper strict checks pass; full-package diagnostics remain the
identical six baseline errors. See `PREFIX_ALIGNMENT.md`. Final native proof must
use CPU1/memory2048 in a fresh isolated scope; no native run is claimed.

Version6 includes the deployment owner Dockerfile-only replay-directory ownership
fix and a final composite manifest. Embedded `tank-overlay.json` now hashes all43
final patched/added sources (including retained modules, runner and dependency
metadata), preserving the untouched base-stage manifest and its SHA under
`base_stage_provenance`. The resulting helper-bound runtime identity changes;
lifecycle modules and worker policy do not. Offline integrations/types pass again;
full-package diagnostics remain the same six. The separate native qualifier has
five no-start/artifact/watchdog tests and a real240s preparation watchdog, with
1080/900/60s outer phase bounds. Native execution remains NOT_RUN.

Version7 repairs the actual645a service initialization failure: control-plane-node
now directly declares SDK0.10.6 and its exact existing lock snapshot. Baseline
package/lock SHAs and existing snapshot match are guarded. No hoist, NODE_PATH,
private package path or hand-created dependency link is used. The prior module
import check did not invoke createManagedNodeRuntime and was insufficient for
startup. The new focused SDK check uses clean native ESM resolution in the actual
caller package and then initializes the real control plane on fresh diagnostic
SQLite; it is prepared for the correctly installed built image. Actual frozen
installation, new-image initialization and isolatedPostgreSQL server/API proof
remain pending deployment owner execution. No new VM/model run is claimed.

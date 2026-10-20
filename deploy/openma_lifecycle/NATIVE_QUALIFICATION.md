# Native qualification command and historical preparation record

`native-qualification.mjs` starts the SDK only with the explicit
`AUTHORIZED_NATIVE_PROOF` invocation flag. Its historical offline-only status was
superseded by the scoped evidence linked in README.md. A new image/architecture
remains unqualified until this actual command and the separate integration checks
run on it. Use the newly built retained image with its matching OCI and adequate
disk/RAM, never the browser-only base image.

This is an actual bridge/provider qualification, not an OpenMA API history test.
It uses the built image's production `NodeRetainedLifecycle`, patched
`LiteBoxSandbox`, pinned public provider, real SQL resource fence/SQLite, SDK0.10.6,
and production VM-local browser harness/worker. No model or external endpoint is
called. The only injected environment key is an explicitly fake probe value.

The orchestrator runs bounded first/resume child processes (1080/900 seconds)
and exact scoped cleanup in a final child (60 seconds). There is at most one
native VM/disk; stop/resume keeps its ID and disk. A new JS process means a fresh
SDK/runtime/controller and native handle. All guest operations retain their
existing command/browser deadlines. If a phase times out, only its child PID is
killed; the final cleanup resolves the dedicated scope's public manifest and
reaps that exact resource. It may wait up to 35 seconds for the dead controller's
existing 30-second resource lease; it never edits SQL to force expiry. The cleanup
child remains bounded by 60 seconds. Preserve the proof volume if cleanup fails.

The proof checks CPU1/memory2048 and actual `autoDelete=0`, rootfs and workspace
bytes, the actual read/write output mount, per-exec fake key rotation, and an
actual Chromium persistent cookie plus localStorage on a VM-local loopback HTTP
site. The browser context closes before idle to flush its real profile. It records
the active VM's owned PID/start-time/RSS and asserts that original process exited
(or is reaped/zombie with released memory), retained info is stopped, and native
runtime metrics report no running VM. It refuses fingerprint mismatch and a
foreign scoped owner without booting/mutating the retained resource. A competing
controller cannot acquire its active resource. Final cleanup is idempotent and
uses the public exact ownership check; idle does not archive/recreate anything.

The script verifies the embedded lifecycle identity, actual installed SDK version,
and actual guest worker hash against `/app/tank-overlay.json`. It records the
host-supplied immutable image ID; the host must inspect/verify that ID itself.
The proof runs in a dedicated `/proof/<run>/boxlite` home and rejects the active
service's runtime home. It never reads operator credential files.

## Exact invocation template

The deployment owner supplies the built image tag/ID, KVM group ID and coherent
readonly OCI layout. The following is a template; replace paths/IDs explicitly.
The new named volume must be absent (inspect it first and choose a new name if it exists). Initialization only changes ownership of
that new volume's root; it starts no VM.

```sh
task_image='BUILT_RETAINED_CANDIDATE_TAG'
task_image_id=$(docker image inspect --format '{{.Id}}' "$task_image")
task_runtime_identity=$(docker run --rm --network none --entrypoint node "$task_image_id" \
  -e 'process.stdout.write(JSON.parse(require("node:fs").readFileSync("/app/tank-overlay.json","utf8")).retained_lifecycle.runtime_identity)')
task_proof_volume='tank-native-lifecycle-UNIQUE_RUN'
task_container='tank-native-lifecycle-UNIQUE_RUN'
task_kvm_gid=$(stat -c %g /dev/kvm)
task_oci='/ABSOLUTE/QUALIFIED/COHERENT/OCI_LAYOUT'
task_assets="$PWD/deploy/openma_lifecycle"

docker volume create "$task_proof_volume"
docker run --rm --network none --user 0 \
  --mount "type=volume,src=$task_proof_volume,dst=/proof" \
  --entrypoint sh "$task_image_id" -c 'chown 1000:1000 /proof'

docker run --name "$task_container" --network none \
  --user 1000:1000 --group-add "$task_kvm_gid" --device /dev/kvm:/dev/kvm \
  --cpus 2 --memory 4g \
  --mount "type=volume,src=$task_proof_volume,dst=/proof" \
  --mount "type=bind,src=$task_oci,dst=/opt/tank-sandbox-oci,readonly" \
  --mount "type=bind,src=$task_assets,dst=/audit,readonly" \
  -e TANK_NATIVE_MEMORY_MIB=2048 \
  -e TANK_NATIVE_LIFECYCLE_GO=AUTHORIZED_NATIVE_PROOF \
  -e TANK_NATIVE_PROOF_DIR=/proof/native-lifecycle \
  -e BOXLITE_HOME=/proof/native-lifecycle/boxlite \
  -e TANK_NATIVE_ROOTFS=/opt/tank-sandbox-oci \
  -e "TANK_NATIVE_EXPECT_IDENTITY=$task_runtime_identity" \
  -e "TANK_NATIVE_IMAGE_ID=$task_image_id" \
  --entrypoint /app/apps/main-node/node_modules/.bin/tsx \
  "$task_image_id" /audit/native-qualification.mjs
```

Use the deployment host's Docker context/OCI/assets paths if they differ from
this local template. Check the current immutable image ID before invoking. Do
not mount secrets, the active OpenMA data volume, its database or a Docker socket.
Do not add capabilities, privilege, relaxed Docker seccomp/AppArmor, native
security changes or timeout overrides to make this test pass.

The container is retained for evidence extraction. Export only `result.json`,
`run.json`, `phase-first.json`, `phase-resume.json`, `phase-cleanup.json` and the
three phase logs, `browser-first.html/png`, `browser-resume.html/png`, any
`browser-*-failure.json`, and only the `browser-*-observer/`,
`browser-*-phase.json`, `browser-*-observer.log` files beneath the dedicated
`outputs/native_qualification/<run-session>/` from `/proof/native-lifecycle/` to a new host directory such as
`docs/verification/native-lifecycle-<run>/`, using `docker cp`. Record the exact
image ID and worker/OCI identity alongside them. Do not export the native home or
raw resource fence database. Remove the dedicated test container/volume only
after reviewing cleanup and preserving the evidence; this template performs no
automatic broad cleanup or unrelated service changes.

## Separate service proof still required

A PASS certifies only the actual standalone native bridge flow tested here. It
does not certify native OpenMA session/history continuity or the service's
shutdown/restart composition. The final acceptance proof must use one actual
OpenMA API session, retain its exact session ID and preexisting append-only event
IDs/messages, perform real safe idle and graceful/crashed controller restart,
then continue that same session and inspect its workspace/browser profile. It
must also verify real configured PostgreSQL fencing and terminal archive policy.
No SQL unarchive, replacement session, synthetic calibration event or idle archive
can substitute for that proof.

The final service/qualifier tuple is explicitly CPU1/memory2048. Historical512MiB
evidence remains historical. The current script refuses a512MiB qualification
and always creates a fresh isolated proof scope/home; it never restores an old
512MiB resource as a2048MiB candidate. Check actual current headroom before GO.

The same single native allocation also performs first open, VM-local loopback DOM
and PNG capture before idle, then again after fresh-controller resume. The
read-only `native-startup-observer.py` is a formatting-only copy of the browser owner's
prepared observer, with its SHA recorded. It samples process identities/timing,
RSS, pressure, sockets and profile metadata in this credential-free guest. It
does not launch or configure browsers, alter worker transport, flags or budgets.
Ready/phase/timeline files are written through the existing output mount so a
browser timeout still leaves host-readable telemetry. Observer startup has a
bounded readiness check; outer child-phase deadlines are 1080/900/60 seconds.
A failure is a recorded failure, never a fallback launch or retry. The native
qualifier and observer remain prepared and NOT_RUN.

The former180-second aggregate guard was insufficient for the observed177.37s
cold preparation followed by a complete first browser operation. Only the outer
diagnostic orchestration allowance is now first1080s/resume900s/cleanup60s. The
first path has seven browser RPCs (open, goto, DOM, PNG, write profile, read profile,
close); resume has six. Each retains its existing90s SandboxPort budget and the
worker retains75s operation/80s receipt/30s guest-ready budgets. The outer bound
contains a240s cold-preparation allowance, all RPC ceilings, setup/readiness and
finalization margin. Preparation elapsed time and host progress checkpoints are
recorded separately. Hitting the outer guard is labeled OUTER_DIAGNOSTIC_TIMEOUT,
with the last stage, never classified by itself as a browser failure. No retries,
fallbacks or prewarming are added: the fresh first profile/home remains cold and
resume uses that same retained cache/profile.

The240s preparation allowance is enforced by a separate real watchdog around
the first native allocation/file preparation (and resume/file verification).
On expiry it writes PREPARATION_TIMEOUT with elapsed time, exits only that phase
child with code86, and never starts browser work. The parent always proceeds to
exact scoped cleanup, preserving timeout receipts and evidence. It performs no
retry. This diagnostic watchdog does not modify a product/native operation
timeout. Export `progress-*.json` and any `preparation-*-timeout.json` as well.

## Revised post-v6 diagnostic invocation — NOT RUN, requires new GO

The single v6 attempt failed before browser/observer startup because official SDK
copyIn/copyOut does not operate beneath native mounts. Its exact failed helper and
actual receipts remain under `docs/verification/native-browser-retained-2026-10-18-v6/`.
This is not a new browser-startup failure. The revised diagnostic uses normal
guest exec with base64 bytes and a5s budget only for the mounted output nonce;
root/workspace file APIs are unchanged. No product code/image/runtime identity
changes are required: `/audit` diagnostic assets are explicitly readonly-mounted
and carry their separately recorded helper hash.

For the next reviewed invocation, keep the exact image/OCI/KVM/resource/security
arguments above and make only these explicit ownership changes:

```sh
task_proof_volume='tank-native-browser-retained-20261018-v6'
task_container='tank-native-browser-retained-20261018-v7'
# Existing volume is already owned by node; do not reinitialize or delete it.
# In docker run, use this fresh proof directory and explicitly selected cache:
# -e TANK_NATIVE_PROOF_DIR=/proof/native-browser-v7
# -e TANK_NATIVE_REUSE_CACHE_PROOF_DIR=/proof/native-browser-v6
# -e BOXLITE_HOME=/proof/native-browser-v6/boxlite
```

The helper requires the prior exact-cleanup receipt and identical image/runtime/
OCI/resource provenance, then verifies the reused native catalog is empty. It
creates a new random session/scope and requires the first observer profile stat
to be absent before opening Chromium. The public OCI cache is warm; the VM disk
and browser profile are fresh. Existing first/resume/cleanup bounds and all
product budgets remain unchanged. A new3s dual-disk monitor with8GiB floor and
new actual-result directory are required. No second VM attempt is authorized by
this document.

## Prepared v8 staging correction — NOT RUN, requires new GO

The single v7 attempt also stopped before observer/browser startup: official SDK
refused observer copyIn beneath its standard `/tmp` tmpfs. Its exact7a1d helper
and receipts remain in the separate actual v7 directory. The root-owned correction
changes exactly two observer staging strings to `/workspace/native-startup-observer.py`,
a rootfs path whose parent was verified by preceding actual v7 file operations.
Official SDK source declares standard mounts `/proc`, `/dev`, `/dev/pts`,
`/dev/shm`, `/sys`, `/tmp`, `/etc/hostname`, `/etc/hosts`, `/etc/resolv.conf`;
this qualifier adds `/mnt/session/outputs`. The remaining SDK file transfer paths
are individual files under `/root`, `/workspace`, `/opt/tank`, outside all mounts.
All mounted-path operations use guest exec/normal mounted host filesystem access.

The next reviewed command uses container/proofdir v8 with the same volume/runtime
HOME v6 and `previousProof` v6 receipt, preserving the existing configuration
contract. Host preflight additionally requires the latest v7 exact-cleanup receipt,
v7 containerPid0 and no active volume users. It creates another fresh random
scope/disk/profile. Product budgets, image/runtime/OCI identity and security
remain unchanged. No next native run is authorized by this document.

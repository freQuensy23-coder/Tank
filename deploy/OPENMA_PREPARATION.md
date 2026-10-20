# Complete OpenMA source preparation

Use the canonical `prepare_openma_source.py` command for the normal deployment.
Its operator CLI delegates to `prepare_openma_deployment.py`, the reusable
complete composer. The existing internal Python `prepare()` function remains
the guarded base stage used by the composer; invoking it alone is insufficient
for the retained-session and causal execution-completion contracts.

The public composer applies the pinned base overlay (including the browser
SandboxPort, chronological replay-prefix verifier and SKILL manifest path), then
retained lifecycle with the direct SDK dependency, then execution-status API,
public execution correlation, required stable skill paths, and bounded guest
file I/O for mounted evidence and outputs, then the private-IP replay network policy.
The native runtime must match the strict-DNS source/build contract or the exact historical qualification; source preparation does not claim new hardware acceptance.
Existing upstream SHA, exact-context, SDK lock and
execution-overlay guards are unchanged. An incompatible stage fails closed;
there is no automatic guard rewriting or fallback to an older overlay.

Required inputs are the official OpenMA checkout at
`85d5361f535c7221de975838dc48444e468cc02e` and the complete architecture-matched, verified
BoxLite0.10.6 dynamic runtime bundle, including its original `provenance.json`
and `SHA256SUMS`. The verifier accepts the exact historical qualified ARM artifact or an operator-built bundle whose pinned source, official per-platform files, strict-DNS patch and successful build/offline-test receipts all match the public build contract. A version label alone is insufficient. The dynamic runtime requires the Bookworm system loader.
For its source/toolchain pins, build/export recipe, and rebuild qualification
boundary, see [the versioned native recipe](boxlite/v0.10.6/README.md). A rebuilt shim has its own identity and keeps native qualification `NOT_RUN`; source reproducibility does not imply a byte-identical binary or inherited KVM approval. Normal source preparation accepts a verified local build without any developer-private artifact registration. It does not perform hardware acceptance or deploy it.

The build/export contracts support Linux arm64 and amd64 and the OCI preparer
can enforce the same `--architecture`. The currently registered exact native
replay-DNS bundle is ARM64 only. No amd64 KVM/browser/retained-session proof has been recorded. A pinned locally built strict-DNS bundle can be prepared on either architecture, with `NOT_RUN` kept explicit. Running the compiler or format tests cannot grant hardware qualification. The clean-host path and public native command are in [INSTALL_LINUX.md](boxlite/v0.10.6/INSTALL_LINUX.md). Preserve the working primary artifact while obtaining the matching isolated native and system acceptance evidence.

Run with Python in the Tank image; the host needs only Docker and Git through
the documented container pattern:

```sh
docker run --rm --user "$(id -u):$(id -g)" \
  --entrypoint /opt/tank/venv/bin/python --network none \
  -v "$PWD/deploy:/app/deploy:ro" -v /srv/tank-sources:/sources \
  -v /absolute/qualified-runtime-bundle:/runtime:ro \
  tank-v0:persistent-no-x /app/deploy/prepare_openma_source.py \
  /sources/pinned /sources/prepared --boxlite-runtime /runtime
docker run --rm --user "$(id -u):$(id -g)" \
  --entrypoint /opt/tank/venv/bin/python --network none \
  -v "$PWD/deploy:/app/deploy:ro" -v /srv/tank-sources:/sources:ro \
  tank-v0:persistent-no-x /app/deploy/prepare_openma_deployment.py verify /sources/prepared
```

The destination must be new and outside the source checkout. Both intermediate
stages live in a temporary sibling directory. A failed stage is removed without
publishing a partial destination or modifying the source. Final rename occurs
only after source, metadata, embedded identity and native bundle verification.
The composer starts no container, service, guest or model itself.

Set `OPENMA_SOURCE_PATH` to this final directory and use the existing Compose
Dockerfile/lock installation. `tank-deployment-composition.json` records stage
provenance, preparation asset hashes and every final patched/added source hash.
`tank-overlay.json` contains the final composite maps and the same runtime
identity embedded in `node-managed.ts`; `tank-retained-candidate.json` is updated
to match. The identity hashes normalized final sources (only its own embedded
value becomes an explicit marker), stage metadata and qualified native
provenance. This avoids a circular hash while binding the SKILL, browser,
replay-prefix, lifecycle and execution code to the runtime fingerprint.

This complete preparation has a different identity from historical manually
composed images even when functional source bytes match. Compare those sources
with the single embedded runtime identity normalized; preserve both metadata
receipts. Preparation reports `NOT_RUN` for native qualification and does not
adopt the output or restore disks from a different runtime identity. Image
hash verification, actual API initialization, native/browser/retention proofs
and operator adoption remain separate checks. The mounted-file I/O stage passed
the actual factory API checks on the r16c image; that historical result does not
qualify a newly composed image. The b645 native artifact passed the actual r18
replay DNS/connection/DB sequence, with unrestricted positives before and after.
Its external qualification is fixed in `boxlite/v0.10.6/replay-dns-qualified-artifact.json`;
raw build provenance and historical d44 metadata remain unchanged. Normal
preparation rejects d44 and 0.9 fallbacks. The execution and continuation stages normalize only the single generated runtime-identity literal after runtime-contract verification. Their remaining reviewed before/after bytes retain exact guards, so an architecture rebuild needs no source edits.
Successful preparation is still not a Managed-session or full-system pass.

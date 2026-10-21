# Docker builds

Run builds on `openclaw-dp`, from the checkout being built. Buildx must be
installed for the Docker CLI in use; check `docker buildx version` first.
No host Python, Node or Go installation is needed.

Use the deployment host architecture throughout. This server is x86-64:

```sh
task_arch=amd64                 # arm64 on an ARM64 KVM host
```

These commands use the builder's normal resources, without CPU quotas or memory
limits. Keep the same builder between builds so its layers and package caches
survive. Start cold Python/native builds on this shared root disk only with at
least **12 GB available**, or use a builder whose storage is on a separate disk.
Small warm builds can proceed separately; they do not establish capacity for
cold builds. Check the filesystem holding builder data and export output before
starting.

## Tank and capture

```sh
docker buildx build --progress=plain --platform "linux/$task_arch" --load \
  -f Dockerfile -t tank-v0:local .

docker buildx build --progress=plain --platform "linux/$task_arch" --load \
  --build-arg "GIT_SHA=$(git rev-parse HEAD)" \
  -f capture/Dockerfile -t tank-capture:local ./capture
```

Build capture from its own context. Its revision argument records the source
commit in captured rows; supply the actual revision being built.

The main image installs frozen Python dependencies in a separate stage. A
scratch stage selects only its locked Playwright package; the runtime mounts
that installer temporarily at its final package path to install Chromium and
system dependencies. The full venv and project metadata are copied afterwards.
This makes browser installation depend on the selected installer bytes rather
than unrelated project metadata, with no second version pin or duplicate
Playwright/Node payload in the runtime image.

Source edits rerun the project installation; deployment-only edits rerun the
deployment copy. Neither should reinstall dependencies or Chromium. The uv cache
mount retains downloaded wheels and build backends without baking that cache
into the runtime image. Installed files use copy mode and do not depend on that
mount at runtime.

Capture keeps module downloads and compilation in separate Go cache mounts.
A source or revision change reuses downloaded modules and unchanged compiled
packages. Go compiles for the requested target architecture on the builder's
native architecture.

## Sandbox OCI

Export a single architecture for the actual KVM host. Use a new verification
output directory; `prepare_sandbox.py` rejects an existing destination.

```sh
task_artifacts=/srv/tank-artifacts
mkdir -p "$task_artifacts"
docker buildx build --progress=plain --platform "linux/$task_arch" \
  --provenance=false -f deploy/sandbox.Dockerfile \
  --output "type=oci,dest=$task_artifacts/tank-analysis-$task_arch.oci.tar" .

docker run --rm --network none --user "$(id -u):$(id -g)" \
  --entrypoint /opt/tank/venv/bin/python \
  -v "$PWD/deploy:/app/deploy:ro" -v "$task_artifacts:/artifacts" \
  tank-v0:local /app/deploy/prepare_sandbox.py \
  "/artifacts/tank-analysis-$task_arch.oci.tar" \
  "/artifacts/tank-analysis-$task_arch-oci" \
  --boxlite-version 0.10.6 --architecture "$task_arch"
```

`--provenance=false` keeps the export compatible with the single-image native
rootfs verifier. Compose mounts the verified OCI layout through
`TANK_SANDBOX_OCI_PATH`. A Docker image tag alone is insufficient for BoxLite.
The sandbox, native BoxLite bundle and host must have the same architecture;
ARM qualification does not qualify an amd64 deployment. Follow the guarded
[native build procedure](../deploy/boxlite/v0.10.6/INSTALL_LINUX.md) when rebuilding
BoxLite. Keep its source pins and verification receipts.

The sandbox browser installation precedes the scientific Python installation,
so changing analysis packages keeps the pinned browser layer. Worker changes
rerun only the worker copy and font-cache preparation. The sandbox-specific
ignore file restricts its context to those browser implementation assets.

## Prepared OpenMA

Prepare and verify a fresh checkout with the public
[OpenMA composer](../deploy/OPENMA_PREPARATION.md). Set `OPENMA_SOURCE_PATH` to
that prepared directory, then build its generated recipe:

```sh
docker buildx build --progress=plain --platform "linux/$task_arch" --load \
  --build-arg SKIP_CONSOLE=1 \
  -f "${OPENMA_SOURCE_PATH:?Set the verified prepared checkout}/apps/main-node/Dockerfile.tank" \
  -t tank-openma:local "$OPENMA_SOURCE_PATH"
```

Keep `SKIP_CONSOLE=1` consistent with Compose. The generated recipe extracts the
final root and workspace package manifests before copying package source. One
frozen pnpm installation uses that manifest tree; sources are copied afterwards.
The runtime copies the stable dependency layer separately from the updated
sources, so source edits retain installed dependencies. Entrypoint/native-asset
changes enter the runtime stage. The pnpm store stays on the build-stage
filesystem so shared package files retain hardlinks. The same install command
removes the unused store afterwards, keeping it out of dependency and runtime
images. Dependency changes repeat the frozen install. Regenerate
through the composer so the generated Dockerfile, checked hashes and runtime
identity match.

## Cache and verification

Repeat the same command to check warm builds; `--progress=plain` shows `CACHED`
steps. Avoid `--no-cache` and `--pull` in an ordinary warm-build comparison.
Use them deliberately when refreshing base images or checking a clean rebuild.
Cache mounts and image-layer cache are distinct: an external layer-cache export
does not automatically export package-cache mount contents. Normal local builds
reuse both through the persistent builder.

Use `docker buildx du` to inspect builder cache usage. Avoid global prune commands
on this shared server; they discard other work and make later builds cold.
The main and sandbox context allowlists reduce available inputs; actual BuildKit
transfer bytes must be measured because it already transfers files lazily.

## Pull-request CI

[Docker images](../.github/workflows/docker-images.yml) runs on pull requests and
manual dispatch. Tank, capture and sandbox build on separate hosted Ubuntu
runners, with a separate GitHub layer-cache scope for each image. Real containers
then run without external network or operator credentials: Tank checks its
frozen packages, PostgreSQL clients, UID/permissions, CLI and both Chromium
variants; capture checks its embedded schema/static executable, stamped revision,
startup and failed health check against a deliberately closed local database
port; sandbox checks analysis libraries, font caches and actual browser-worker
RPC, including profile isolation and persistence. Scratch exports test the
actual main/sandbox context filters with synthetic excluded inputs.

The separate OpenMA job prepares the pinned official upstream and builds the
actual generated `deps` stage with `SKIP_CONSOLE=1`. It checks the installed
pinned pnpm/tsx/esbuild environment offline. Native BoxLite/runtime stages are
outside this job; no native or full-system qualification is implied.

GitHub cache exports retain image layers; package-cache mounts are local to the
builder and are not automatically carried between hosted runners. Persistent
local builders reuse both. CI publishes no images and performs no deployment.
Building or exporting an image does not establish its application or KVM
qualification; use the corresponding smoke and native acceptance results.

# Docker builds

CI (`.github/workflows/ci.yml`, job `image`) builds every production image on each push to `main` and pushes it to GHCR as `ghcr.io/frequensy23-coder/tank/<image>:<commit sha>`. The deploy host only pulls them ([OPERATIONS.md](OPERATIONS.md#deployment)); it never builds.

| Image | Context | Dockerfile |
|---|---|---|
| `tank` | `.` | `Dockerfile` |
| `tank-capture` | `.` | `capture/Dockerfile` |
| `tank-openma` | `vendor/openma`, plus build context `sandbox` | `vendor/openma/Dockerfile` (`SKIP_CONSOLE=1`) |
| `tank-postgres` | `deploy/postgres` | `deploy/postgres/Dockerfile` |
| `tank-clickhouse` | `deploy/clickhouse` | `deploy/clickhouse/Dockerfile` |

Each image gets the label `org.tank.content`: a hash of the git blob IDs of the paths listed for it in the matrix. The deploy recreates PostgreSQL, ClickHouse and OpenMA only when this label changes. Add a path to the list when an image starts using it. `GIT_SHA` is the full commit SHA; Tank and capture record it as `config_version`.

A local build needs only Docker with buildx, for example:

```sh
docker buildx build --load -f Dockerfile -t tank:local .
docker buildx build --load -f capture/Dockerfile -t tank-capture:local .
```

## Tank and capture

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

The `tank-openma` build first builds `deploy/sandbox.Dockerfile` as an OCI layout and passes it as build context `sandbox`. The OpenMA Dockerfile copies it to `/opt/tank-sandbox-oci`, and BoxLite boots each session from it (`SANDBOX_ROOTFS_PATH`). A Docker image tag alone is insufficient for BoxLite. The sandbox, native BoxLite bundle and host must have the same architecture; ARM qualification does not qualify an amd64 deployment. Follow the guarded [native build procedure](../deploy/boxlite/v0.10.6/INSTALL_LINUX.md) when rebuilding BoxLite. Keep its source pins and verification receipts.

The sandbox browser installation precedes the scientific Python installation,
so changing analysis packages keeps the pinned browser layer. Worker changes
rerun only the worker copy and font-cache preparation. The sandbox-specific
ignore file restricts its context to those browser implementation assets.

## Vendored OpenMA

OpenMA and Tank's compatibility changes are checked in under `vendor/openma` and built by its static `Dockerfile`.

The static recipe uses Node24, pnpm11.0.8 and the checked-in frozen workspace
lockfile. Its dependency layer precedes application source copies. The pnpm
store stays on the build-stage filesystem during installation so shared package
files retain hardlinks, then the same command removes the unused store.
Dependency changes repeat the frozen install. The native bundle, license notices
and provenance live inside the same build context. No external source checkout,
source generator or patch command is part of a normal build.

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

# syntax=docker/dockerfile:1.7
#
# Self-host OMA — Node-side single-process image.
#
# Build:
#   docker build -f apps/main-node/Dockerfile -t openma/main-node:dev .
# Run (standalone):
#   docker run --rm -p 8787:8787 \
#     -v $(pwd)/data:/app/data \
#     -e ANTHROPIC_API_KEY=sk-... \
#     openma/main-node:dev
# Run (compose): see docker-compose.yml at repo root.
#
# What's intentionally NOT here yet (Phase G+ work):
#   - Compiled JS bundle. We run via tsx in the container too — same toolchain
#     as `pnpm --filter main-node start`. Adds ~30MB tsx + slows cold start
#     by ~150ms vs a pre-bundled dist; trivial relative to LLM round-trips.
#   - Distroless / scratch base. node:22-slim is ~150MB, fine for a self-host
#     audience. Switch to distroless when packaging matters.

# ─── Stage 1: deps (and native compile) ────────────────────────────────────
FROM node:24-bookworm-slim AS package-manifests
WORKDIR /source
COPY packages packages
RUN mkdir -p /manifests && find packages -mindepth 2 -maxdepth 2 -type f -name package.json -exec cp --parents --target-directory=/manifests {} +

FROM node:24-bookworm-slim AS dependencies

# better-sqlite3 loads its bundled prebuilt addon (pnpm-workspace.yaml
# disables its source build). python3 + build-essential stay for native
# modules a deployment adds on top.
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 build-essential \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Workspace metadata. Copy first so a code-only change doesn't bust the
# pnpm install cache.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .pnpm-approve-builds.json ./
COPY apps/main-node/package.json apps/main-node/
COPY apps/main-fly/package.json apps/main-fly/
COPY apps/main/package.json apps/main/
COPY apps/agent/package.json apps/agent/
COPY apps/integrations/package.json apps/integrations/
COPY apps/console/package.json apps/console/
COPY apps/docs/package.json apps/docs/
COPY --from=package-manifests /manifests/packages packages

# Install workspace deps. --frozen-lockfile fails fast on lockfile drift.
# Build approval list at .pnpm-approve-builds.json covers better-sqlite3 +
# esbuild + workerd + the anthropic tokeniser.
RUN corepack enable && corepack prepare pnpm@11.0.8 --activate && pnpm install --frozen-lockfile --store-dir=/pnpm/store && rm -rf /pnpm/store

FROM dependencies AS deps

# Now bring in the source code (cache-busts only on src changes, not deps).
COPY packages packages
COPY apps/main-node apps/main-node
COPY apps/main-fly apps/main-fly
COPY apps/main apps/main
COPY apps/agent apps/agent
COPY apps/console apps/console
COPY tsconfig.json ./

# Build console SPA. Vite produces /app/apps/console/dist; the runtime
# stage sets CONSOLE_DIR to that path and main-node's serveStatic
# middleware picks it up. Skip with --build-arg SKIP_CONSOLE=1 if you
# want a smaller API-only image (e.g. for SDK-only deploys).
ARG SKIP_CONSOLE=0
RUN if [ "$SKIP_CONSOLE" != "1" ]; then \
      pnpm --filter managed-agents-console build; \
    else \
      mkdir -p apps/console/dist; \
    fi

FROM node:24-bookworm-slim AS boxlite-runtime
ARG TARGETARCH
WORKDIR /opt/tank-boxlite-runtime
COPY tank-native/boxlite-runtime-v0.10.6-tank-replay-dns-${TARGETARCH}.tar.gz /tmp/boxlite-runtime.tar.gz
RUN test "$(dpkg --print-architecture)" = "$TARGETARCH" \
  && case "$TARGETARCH" in \
       amd64) tank_archive_sha=3ea02ae7ca0563b2f8266e07310c41b4f99db1aaf463a25f3d0eaac7a348377f ;; \
       arm64) tank_archive_sha=2372c189fc7e6b90aa26fa56941b5c4d38439ae31ad0664ecd473d0dc6c3bcbe ;; \
       *) exit 1 ;; \
     esac \
  && echo "$tank_archive_sha  /tmp/boxlite-runtime.tar.gz" | sha256sum --check - \
  && tar -xzf /tmp/boxlite-runtime.tar.gz -C /opt/tank-boxlite-runtime \
  && sha256sum --check SHA256SUMS \
  && rm /tmp/boxlite-runtime.tar.gz
COPY tank-native/licenses /usr/share/licenses/tank-boxlite-runtime

# ─── Stage 2: runtime ──────────────────────────────────────────────────────
FROM node:24-bookworm-slim AS runtime
ENV COREPACK_HOME=/opt/corepack

# Tools the agent's bash subprocess relies on. node:22-slim ships almost
# nothing — `curl`, `git`, `ca-certificates` are the bare minimum for the
# harness's bash + read/write/web_fetch tools to do anything useful.
# `python3` is here so build-tooling that shells out to `python` (mostly
# memory store demos + agent-written scripts) doesn't immediately die.
# tsx + better-sqlite3 native module also need basic runtime libs only.
# corepack is needed at runtime so `pnpm start` resolves; alternatively we
# could invoke tsx directly via node_modules/.bin but corepack matches the
# dev workflow exactly.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       curl ca-certificates git python3 \
  && rm -rf /var/lib/apt/lists/* \
  && corepack enable && corepack prepare pnpm@11.0.8 --activate
WORKDIR /app

# Copy the whole monorepo from deps (includes node_modules, source, schemas
# and the freshly-built apps/console/dist).
# An optimisation pass would `pnpm deploy --prod` to a slim subtree; for the
# PoC this is the simplest correct copy.
COPY --from=dependencies /app /app
COPY --from=deps /app/packages /app/packages
COPY --from=deps /app/apps /app/apps
COPY --from=deps /app/tsconfig.json /app/tsconfig.json
COPY tank-deploy/openma_entrypoint.mjs /app/tank-deploy/openma_entrypoint.mjs
COPY tank-native-qualification.json /app/tank-native-qualification.json

# /app/data holds the sqlite db + per-session sandbox workdirs. Mount a
# volume here in production so state survives container restarts.
RUN mkdir -p /app/data && chown -R node:node /app/data

ENV NODE_ENV=production
ENV PORT=8787
ENV HOST=0.0.0.0
ENV DATABASE_PATH=/app/data/oma.db
ENV SANDBOX_WORKDIR=/app/data/sandboxes
# Console SPA is at /app/apps/console/dist (built in the deps stage).
# main-node's serveStatic middleware mounts it at "/*" with index.html
# fallback. Unset CONSOLE_DIR (or build with --build-arg SKIP_CONSOLE=1)
# to ship an API-only image.
ENV CONSOLE_DIR=/app/apps/console/dist

EXPOSE 8787
USER node
WORKDIR /app/apps/main-node

# tsx is the entry. SIGTERM is handled by main-node/src/index.ts for
# graceful shutdown.
CMD ["pnpm", "start"]

COPY --from=boxlite-runtime --chown=node:node /opt/tank-boxlite-runtime /opt/tank-boxlite-runtime
COPY --from=boxlite-runtime /usr/share/licenses/tank-boxlite-runtime /usr/share/licenses/tank-boxlite-runtime
COPY LICENSE NOTICE /usr/share/licenses/openma/
RUN chmod 755 /opt/tank-boxlite-runtime/boxlite-guest /opt/tank-boxlite-runtime/boxlite-shim /opt/tank-boxlite-runtime/bwrap /opt/tank-boxlite-runtime/debugfs /opt/tank-boxlite-runtime/guest-mke2fs /opt/tank-boxlite-runtime/guest-resize2fs /opt/tank-boxlite-runtime/mke2fs
ENV BOXLITE_RUNTIME_DIR=/opt/tank-boxlite-runtime LD_LIBRARY_PATH=/opt/tank-boxlite-runtime
COPY --chown=node:node tank-overlay.json /app/tank-overlay.json
USER root
RUN install -d -o node -g node /app/replay-evidence
COPY --link --from=sandbox / /opt/tank-sandbox-oci/
USER node
ENV BOXLITE_HOME=/app/data/boxlite \
    TANK_REPLAY_EVIDENCE_ROOT=/app/replay-evidence \
    LITEBOX_CPUS=2 \
    LITEBOX_MEMORY_MIB=2048 \
    SANDBOX_PROVIDER=litebox \
    SANDBOX_IMAGE=tank-analysis:server-amd64 \
    SANDBOX_ROOTFS_PATH=/opt/tank-sandbox-oci \
    MEMORY_BLOB_DIR=/app/data/memory-blobs \
    SESSION_OUTPUTS_DIR=/app/data/session-outputs \
    FILES_BLOB_DIR=/app/data/files \
    AUTH_DISABLED=0
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=30s CMD ["node", "-e", "fetch('http://localhost:8787/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]
ENTRYPOINT ["node", "/app/tank-deploy/openma_entrypoint.mjs"]
CMD []

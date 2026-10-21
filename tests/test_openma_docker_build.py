"""Build renderer contracts use the actual pinned upstream Dockerfile as input."""

import hashlib
import importlib.util
import json
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def module(relative):
    spec = importlib.util.spec_from_file_location("tank_docker_test_module", ROOT / relative)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


# openma-ai/open-managed-agents@85d5361f535c7221de975838dc48444e468cc02e,
# apps/main-node/Dockerfile (Apache-2.0).
UPSTREAM_DOCKERFILE = r"""# syntax=docker/dockerfile:1.7
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
FROM node:22-slim AS deps

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
COPY packages packages

# Install workspace deps. --frozen-lockfile fails fast on lockfile drift.
# Build approval list at .pnpm-approve-builds.json covers better-sqlite3 +
# esbuild + workerd + the anthropic tokeniser.
RUN corepack enable \
  && pnpm install --frozen-lockfile

# Now bring in the source code (cache-busts only on src changes, not deps).
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

# ─── Stage 2: runtime ──────────────────────────────────────────────────────
FROM node:22-slim AS runtime

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
  && corepack enable
WORKDIR /app

# Copy the whole monorepo from deps (includes node_modules, source, schemas
# and the freshly-built apps/console/dist).
# An optimisation pass would `pnpm deploy --prod` to a slim subtree; for the
# PoC this is the simplest correct copy.
COPY --from=deps /app /app

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
"""


def test_dependency_layer_is_independent_of_package_source_and_runtime_assets():
    docker = module("deploy/prepare_openma_source.py").prepare_dockerfile(UPSTREAM_DOCKERFILE)
    manifests, stages = docker.split("FROM node:24-bookworm-slim AS dependencies\n", 1)
    dependencies, stages = stages.split("FROM dependencies AS deps\n", 1)
    source, runtime = stages.split("FROM node:24-bookworm-slim AS runtime\n", 1)
    assert "COPY packages packages" in manifests
    assert "COPY --from=package-manifests /manifests/packages packages" in dependencies
    assert "COPY packages packages" not in dependencies
    assert dependencies.count("pnpm install --frozen-lockfile --store-dir=/pnpm/store") == 1
    assert "pnpm fetch" not in docker and "--offline" not in docker
    assert "COPY packages packages" in source
    assert "COPY apps/main-node apps/main-node" in source
    assert "COPY --from=dependencies /app /app" in runtime
    assert "COPY --from=deps /app /app" not in runtime
    for path in ("packages", "apps", "tsconfig.json"):
        assert f"COPY --from=deps /app/{path} /app/{path}" in runtime
    assert "COPY tank-deploy" not in manifests + dependencies + source
    assert "COPY tank-deploy/openma_entrypoint.mjs /app/tank-deploy/openma_entrypoint.mjs" in runtime
    assert "COPY tank-deploy /app/tank-deploy" not in docker
    assert "--mount=type=cache" not in docker
    assert "pnpm install --frozen-lockfile --store-dir=/pnpm/store && rm -rf /pnpm/store" in dependencies
    assert 'CMD ["pnpm", "start"]' in runtime and "USER node" in runtime


def test_actual_manifest_extractor_keeps_source_edits_out_of_dependency_input(tmp_path):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node executable required for actual Docker manifest extractor")
    docker = module("deploy/prepare_openma_source.py").prepare_dockerfile(UPSTREAM_DOCKERFILE)
    command = next(line for line in docker.splitlines() if line.startswith("RUN node -e "))
    code = shlex.split(command.removeprefix("RUN "))[2]
    destination = tmp_path / "manifests"
    code = code.replace('"/manifests"', json.dumps(str(destination)))
    for name in ("first", "second"):
        package = tmp_path / "packages" / name
        package.mkdir(parents=True)
        (package / "package.json").write_text(json.dumps({"name": name, "private": True}))
        (package / "index.ts").write_text("export const value = 1;\n")
    subprocess.run([node, "--eval", code], cwd=tmp_path, check=True)
    before = {str(p.relative_to(destination)): p.read_bytes() for p in destination.rglob("*") if p.is_file()}
    assert set(before) == {"packages/first/package.json", "packages/second/package.json"}
    (tmp_path / "packages/first/index.ts").write_text("export const value = 2;\n")
    subprocess.run([node, "--eval", code], cwd=tmp_path, check=True)
    after = {str(p.relative_to(destination)): p.read_bytes() for p in destination.rglob("*") if p.is_file()}
    assert before == after
    for name in ("first", "second"):
        assert (destination / "packages" / name / "package.json").read_bytes() == (
            tmp_path / "packages" / name / "package.json"
        ).read_bytes()


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("FROM node:22-slim", "FROM node:other"),
        ("AS runtime\n", "AS renamed\n"),
        ("COPY packages packages", "COPY packages /different"),
        ("pnpm install --frozen-lockfile", "pnpm install"),
        ("COPY --from=deps /app /app", "COPY --from=deps /different /app"),
    ],
)
def test_changed_upstream_build_contract_is_rejected(before, after):
    helper = module("deploy/prepare_openma_source.py")
    with pytest.raises(ValueError, match="Dockerfile patch context mismatch"):
        helper.prepare_dockerfile(UPSTREAM_DOCKERFILE.replace(before, after, 1))


def test_dependency_context_rejects_wrong_real_git_commit_without_publishing(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "README").write_text("A real disposable Git checkout with the wrong pin.\n")
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "add", "README"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    before = hashlib.sha256((source / "README").read_bytes()).hexdigest()
    destination = tmp_path / "output"
    script = module("scripts/ci/openma-docker-check.py")
    with pytest.raises(ValueError, match="does not match pinned commit"):
        script.prepare(source, destination)
    assert not destination.exists()
    assert hashlib.sha256((source / "README").read_bytes()).hexdigest() == before


def test_dependency_context_preserves_existing_destination(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    marker = destination / "keep"
    marker.write_bytes(b"existing operator files")
    script = module("scripts/ci/openma-docker-check.py")
    with pytest.raises(ValueError, match="Destination must be new"):
        script.prepare(source, destination)
    assert marker.read_bytes() == b"existing operator files"

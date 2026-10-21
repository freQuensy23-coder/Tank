# syntax=docker/dockerfile:1.7
FROM postgres:16-bookworm AS pgclient
FROM ghcr.io/astral-sh/uv:0.9.5 AS uv
FROM python:3.12-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git libpq5 liblz4-1 libzstd1 libreadline8 libxml2 libssl3 libicu72 zlib1g tini && rm -rf /var/lib/apt/lists/*
COPY --from=pgclient /usr/lib/postgresql/16/bin/pg_dump /usr/local/bin/pg_dump
COPY --from=pgclient /usr/lib/postgresql/16/bin/pg_restore /usr/local/bin/pg_restore
COPY --from=pgclient /usr/lib/postgresql/16/bin/psql /usr/local/bin/psql
WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/opt/tank/venv \
    UV_LINK_MODE=copy \
    PATH=/opt/tank/venv/bin:$PATH \
    PLAYWRIGHT_BROWSERS_PATH=/opt/playwright \
    PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock ./
# Keep downloaded wheels/build backends outside image layers and reusable on rebuilds.
RUN --mount=type=cache,id=tank-uv,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
RUN /opt/tank/venv/bin/python -m playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*
# Source edits must not repeat account and writable-directory preparation.
RUN useradd --create-home --uid 1000 tank \
    && mkdir -p /data/replay/evidence /browser /codex-auth /backup \
    && chown -R tank:tank /data /browser /codex-auth /backup
COPY src ./src
RUN --mount=type=cache,id=tank-uv,target=/root/.cache/uv \
    uv sync --frozen --no-dev
COPY deploy /app/deploy
USER tank
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/tank/venv/bin/python", "/app/deploy/tank_entrypoint.py"]
CMD ["tank", "serve"]

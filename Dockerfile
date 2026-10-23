# syntax=docker/dockerfile:1.7
FROM postgres:16-bookworm AS pgclient
FROM ghcr.io/astral-sh/uv:0.11.7 AS uv
FROM python:3.13.13-slim-bookworm AS runtime-base
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

FROM runtime-base AS dependencies
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,id=tank-uv,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

FROM scratch AS browser-installer
COPY --from=dependencies /opt/tank/venv/lib/python3.13/site-packages/playwright /playwright

FROM runtime-base AS runtime
RUN --mount=type=bind,from=browser-installer,target=/opt/tank/venv/lib/python3.13/site-packages \
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/opt/tank/venv/lib/python3.13/site-packages \
    /usr/local/bin/python -m playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*
COPY --from=dependencies /opt/tank/venv /opt/tank/venv
COPY pyproject.toml uv.lock ./
RUN useradd --create-home --uid 1000 tank \
    && mkdir -p /data/replay/evidence /codex-auth /backup \
    && chown -R tank:tank /data /codex-auth /backup
COPY src ./src
RUN --mount=type=cache,id=tank-uv,target=/root/.cache/uv \
    uv sync --frozen --no-dev
COPY config /app/config
ARG GIT_SHA
ENV TANK_BUILD_SHA=${GIT_SHA} \
    TANK_CONFIG_DIR=/app/config \
    TANK_DATA_DIR=/data \
    TANK_OPENMA_URL=http://openma:8787 \
    TANK_CODEX_GATEWAY_HOST=0.0.0.0 \
    TANK_CODEX_GATEWAY_PORT=8790 \
    TANK_CODEX_AUTH_PATH=/codex-auth/auth.json \
    TANK_BACKUP_DIR=/backup \
    TANK_CLICKHOUSE_URL=http://tank@clickhouse:8123
USER tank
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["tank", "serve"]

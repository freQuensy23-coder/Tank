# syntax=docker/dockerfile:1.7
FROM clickhouse/clickhouse-server:25.8.33.6@sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77 AS native-clickhouse
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
    PYTHONUNBUFFERED=1

FROM runtime-base AS dependencies
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,id=tank-uv,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

FROM runtime-base AS runtime
COPY --from=dependencies /opt/tank/venv /opt/tank/venv
COPY --from=native-clickhouse /usr/bin/clickhouse /opt/tank/venv/bin/clickhouse
RUN case "$(dpkg --print-architecture)" in \
      amd64) expected=6a96266c0f00897225f9bb2ab0f965111298da75d7ec1b67465c6c9d5cf5ae81 ;; \
      arm64) expected=6a23992b3035628dd1fc0fc981453db82b2f35fcf0f5c588293ce7c4918841fd ;; \
      *) exit 1 ;; \
    esac \
    && printf '%s  %s\n' "$expected" /opt/tank/venv/bin/clickhouse | sha256sum --check \
    && /opt/tank/venv/bin/clickhouse --version | grep -E ' version 25[.]8[.]33[.]6([[:space:]]|$)'
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

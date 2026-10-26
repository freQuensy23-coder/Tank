# syntax=docker/dockerfile:1.7
FROM python:3.13.13-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git postgresql-client nodejs npm bash && rm -rf /var/lib/apt/lists/*

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN --mount=type=cache,id=tank-sandbox-pip,target=/root/.cache/pip,sharing=locked \
    python -m pip install playwright==1.64.0 \
    && python -m playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,id=tank-sandbox-pip,target=/root/.cache/pip,sharing=locked \
    python -m pip install pandas numpy scipy statsmodels duckdb 'psycopg[binary]' httpx websockets

RUN mkdir -p /workspace /home/user/.skills
WORKDIR /workspace

COPY deploy/openma_browser/browser-worker.py /opt/tank/browser-worker.py
COPY deploy/openma_browser/prepare-font-cache.py /opt/tank/prepare-browser-font-cache.py
RUN python /opt/tank/prepare-browser-font-cache.py

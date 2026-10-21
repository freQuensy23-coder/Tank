# syntax=docker/dockerfile:1.7
# Build and export a verified local OCI layout; no credentials or registry publication.
FROM python:3.12-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git postgresql-client nodejs npm bash && rm -rf /var/lib/apt/lists/*

# The pinned browser layer survives changes to the scientific Python packages.
# Keep both bundled Chromium variants for all existing sandbox consumers.
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN --mount=type=cache,id=tank-sandbox-pip,target=/root/.cache/pip,sharing=locked \
    python -m pip install playwright==1.64.0 \
    && python -m playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,id=tank-sandbox-pip,target=/root/.cache/pip,sharing=locked \
    python -m pip install pandas numpy scipy statsmodels pyarrow duckdb 'psycopg[binary]' httpx websockets

# Node is needed by OpenMA pi harness tools; image remains a Python3.12 environment.
RUN mkdir -p /workspace /home/user/.skills
WORKDIR /workspace

COPY deploy/openma_browser/browser-worker.py /opt/tank/browser-worker.py
COPY deploy/openma_browser/prepare-font-cache.py /opt/tank/prepare-browser-font-cache.py
RUN python /opt/tank/prepare-browser-font-cache.py

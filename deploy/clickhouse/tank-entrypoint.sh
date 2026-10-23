#!/bin/bash
set -euo pipefail
export CLICKHOUSE_PASSWORD="${TANK_DB_PASSWORD:?}"
exec /entrypoint.sh "$@"

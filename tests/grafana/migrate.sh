#!/usr/bin/env bash
set -euo pipefail
pg=("$engine" exec -i -u postgres "$project-postgres-1" psql -XqAt -v ON_ERROR_STOP=1 -d tank)
for _ in {1..60}; do
    if "${pg[@]}" -c 'SELECT 1' >/dev/null 2>&1; then break; fi
    sleep 1
done
if [[ $("${pg[@]}" -c "SELECT to_regclass('ops.schema_migrations') IS NOT NULL") == t ]]; then exit 0; fi
{
    printf 'SET ROLE tank; CREATE SCHEMA IF NOT EXISTS ops; CREATE TABLE ops.schema_migrations (version text primary key, applied_at timestamptz NOT NULL DEFAULT now());\n'
    for file in "$root"/src/tank/migrations/*.sql; do
        cat "$file"
        printf "\nINSERT INTO ops.schema_migrations(version) VALUES ('%s');\n" "${file##*/}"
    done
    cat "$root/src/tank/agent_grants.sql"
} | "${pg[@]}" -1 -f - >/dev/null

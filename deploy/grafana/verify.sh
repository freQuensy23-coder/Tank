#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root=$PWD
project="tank-grafana-check-$(openssl rand -hex 8)"
case $(docker context inspect --format '{{.Endpoints.docker.Host}}') in
    unix://*) ;;
    *) printf 'Use a local Docker socket.\n' >&2; exit 1 ;;
esac
[[ ${DOCKER_HOST:-unix://} == unix://* ]]
compose=(docker compose --env-file /dev/null --project-directory "$root" -p "$project" -f deploy/grafana/verify.compose.yaml -f compose.grafana.yaml)
for key in POSTGRES_PASSWORD TANK_DB_PASSWORD OMA_DB_PASSWORD TANK_AGENT_DB_PASSWORD TANK_REPLAY_AGENT_DB_PASSWORD GRAFANA_ADMIN_PASSWORD GRAFANA_SECRET_KEY GRAFANA_STORE_PASSWORD GRAFANA_READER_PASSWORD; do
    printf -v "$key" '%s' "$(openssl rand -hex 32)"
    export "$key"
done
export GRAFANA_PORT=0 GRAFANA_BIND_ADDRESS=127.0.0.1
umask 077
tmp=$(mktemp -d)
cleanup() {
    if (( $? != 0 )); then
        "${compose[@]}" logs --no-color | grep -Ei 'permission denied|no such file|fatal:|pg_hba.conf|level=error' | perl -pe 's/[0-9a-f]{64}/[redacted]/gi' | cut -c1-350 >&2 || true
    fi
    rm -rf "$tmp"
    "${compose[@]}" down -v
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${compose[@]}" up -d --wait postgres
pg=("${compose[@]}" exec -T -u postgres postgres psql -XqAt -v ON_ERROR_STOP=1 -d tank)
{ printf 'SET ROLE tank;\n'; for f in src/tank/migrations/*.sql; do printf '\\i /test-migrations/%s\n' "${f##*/}"; done; } | "${pg[@]}" -1 -f -
"${compose[@]}" exec -T -u postgres -e GRAFANA_STORE_PASSWORD -e GRAFANA_READER_PASSWORD postgres psql -Xq -d tank -f /etc/tank-grafana/bootstrap.sql
"${pg[@]}" -f /etc/tank-grafana/install-views.sql
"${pg[@]}" -f /etc/tank-grafana/install-views.sql
"${pg[@]}" -d postgres -c 'CREATE DATABASE tank_test_grafana TEMPLATE tank'
for fixture in tests/grafana/*.sql; do
    "${pg[@]}" -d tank_test_grafana -f - < "$fixture"
    printf 'PostgreSQL fixture passed: %s\n' "$fixture"
done
"${compose[@]}" up -d --wait --wait-timeout 120 grafana
resolve_url() {
    local address
    address=$("${compose[@]}" port grafana 3000)
    [[ $address =~ ^127\.0\.0\.1:[0-9]+$ ]]
    base="http://$address"
}
resolve_url
printf 'machine 127.0.0.1 login admin password %s\n' "$GRAFANA_ADMIN_PASSWORD" > "$tmp/netrc"
http=(curl --noproxy '*' -fsS --max-time 20 --netrc-file "$tmp/netrc")
"${http[@]}" "$base/api/health" | jq -ec 'select(.database == "ok") | {database,version}'
status=$(curl --noproxy '*' -sS -o /dev/null -w '%{http_code}' "$base/api/datasources/uid/tank-postgres")
[[ $status == 401 ]]
printf 'Anonymous datasource access: HTTP %s\n' "$status"
"${http[@]}" "$base/api/datasources/uid/tank-postgres" | jq -ec 'select(.uid == "tank-postgres" and .user == "tank_grafana_reader" and .readOnly and .secureJsonFields.password) | {uid,user,readOnly}'
"${http[@]}" "$base/api/datasources/uid/tank-postgres/health" | jq -ec 'select(.status == "OK")'
query() {
    jq -nc --arg sql "$1" '{from:"now-1h",to:"now",queries:[{refId:"A",datasource:{type:"grafana-postgresql-datasource",uid:"tank-postgres"},format:"table",rawSql:$sql}]}' |
        curl --noproxy '*' -sS --max-time 20 --netrc-file "$tmp/netrc" -H 'Content-Type: application/json' --data-binary @- "$base/api/ds/query"
}
query "SELECT current_user, current_schema, current_setting('transaction_read_only')" | jq -ec '.results.A.frames[0].data.values | select(. == [["tank_grafana_reader"],["tank_metrics"],["on"]])'
for view in queue_now queue_terminal source_pulls source_poll_history agent_runs agent_model_turns agent_first_verdict agent_watches agent_milestones; do
    query "SELECT count(*) FROM tank_metrics.$view" | jq -e '.results.A | (.error == null) and (.frames[0].data.values == [[0]])' >/dev/null
    printf 'Reader SELECT tank_metrics.%s: OK\n' "$view"
done
for sql in 'SELECT * FROM ops.jobs LIMIT 0' 'SELECT * FROM ops.source_poll_counts LIMIT 0' 'SELECT * FROM grafana."user" LIMIT 0'; do
    query "$sql" | jq -e '.results.A.error | contains("permission denied")' >/dev/null
    printf 'Reader denied: %s\n' "$sql"
done
[[ $("${pg[@]}" -c "SELECT has_table_privilege('tank_grafana_reader','tank_metrics.queue_now','INSERT, UPDATE, DELETE') OR has_schema_privilege('tank_grafana_reader','tank_metrics','CREATE') OR has_database_privilege('tank_grafana_reader','tank','TEMP')") == f ]]
for file in deploy/grafana/dashboards/*.json; do
    uid=$(jq -er '.uid' "$file")
    "${http[@]}" "$base/api/dashboards/uid/$uid" > "$tmp/dashboard"
    jq -ec 'select(.meta.provisioned and .meta.folderUid == "tank") | .dashboard.uid' "$tmp/dashboard"
    jq -c '.dashboard.panels[] | .targets[]? | {from:"now-1h",to:"now",queries:[. + {intervalMs:60000,maxDataPoints:60}]}' "$tmp/dashboard" > "$tmp/requests"
    [[ -s $tmp/requests ]]
    while IFS= read -r request; do
        "${http[@]}" -H 'Content-Type: application/json' --data-binary "$request" "$base/api/ds/query" | jq -e '.results | length > 0 and all(.[]; .error == null and (.frames | type == "array"))' >/dev/null
    done < "$tmp/requests"
    printf 'Panel queries OK: %s (%s)\n' "$uid" "$(wc -l < "$tmp/requests")"
done
"${pg[@]}" -c "INSERT INTO ops.source_poll_counts (bucket_start,source_mode,feed_kind,successful,failed) VALUES
    ('2026-01-01 00:00Z','REST','rss',3,0), ('2026-01-01 00:00Z','PAGE','html',1,2),
    ('2026-01-01 00:10Z','REST','rss',0,4), ('2026-01-01 00:15Z','REST','rss',91,99)"
"${http[@]}" "$base/api/dashboards/uid/tank-pipeline" > "$tmp/pipeline"
for panel in 101 102; do
    jq -ce --argjson id "$panel" '.dashboard.panels[] | select(.id == $id) | {from:"1767225600000",to:"1767226620000",queries:.targets}' "$tmp/pipeline" |
        "${http[@]}" -H 'Content-Type: application/json' --data-binary @- "$base/api/ds/query" > "$tmp/result"
    jq -ec '.results.A.frames[0] | . as $frame | [.schema.fields | to_entries[] |
        {name:.value.name,type:(.value.labels.source_type // ""),values:$frame.data.values[.key]}] | sort_by(.name,.type)' "$tmp/result" > "$tmp/fields"
    jq -e 'map(select(.name == "Time" or .name == "time") | .values) == [[1767225600000,1767226200000]]' "$tmp/fields" >/dev/null
    if [[ $panel == 101 ]]; then
        jq -e 'map(select(.name == "Failed" or .name == "Successful") | .values) == [[2,4],[4,0]]' "$tmp/fields" >/dev/null
    else
        jq -e 'map(select(.name == "Failed" or .name == "Successful") | [.type,.values]) == [["PAGE / html",[2,null]],["REST / rss",[0,4]],["PAGE / html",[1,null]],["REST / rss",[3,0]]]' "$tmp/fields" >/dev/null
    fi
    printf 'Seeded source panel %s: observed counts, gaps and range boundaries OK\n' "$panel"
done
store_check="SELECT count(*) FROM grafana.data_source WHERE uid='tank-postgres'"
[[ $("${pg[@]}" -c "$store_check") == 1 ]]
"${http[@]}" -X PUT -H 'Content-Type: application/json' --data '{"name":"Persistence check","email":"admin@localhost","login":"admin"}' "$base/api/user" >/dev/null
"${compose[@]}" up -d --wait --force-recreate --no-deps grafana
resolve_url
[[ $("${pg[@]}" -c "$store_check") == 1 ]]
"${http[@]}" "$base/api/user" | jq -ec 'select(.login == "admin" and .name == "Persistence check") | {login,name}'
printf 'Grafana container replaced; admin and provisioned datasource retained in PostgreSQL.\n'

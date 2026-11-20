#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ $# == 0 ]]; then
    for scenario in fresh existing; do
        env -i HOME="$HOME" PATH="$PATH" TMPDIR="${TMPDIR:-/tmp}" DOCKER_CONTEXT="$(docker context show)" DOCKER_HOST="${DOCKER_HOST:-}" bash "$PWD/deploy/grafana/verify.sh" "$scenario"
    done
    exit
fi
scenario=$1
[[ $scenario == fresh || $scenario == existing ]]
[[ $(docker context inspect --format '{{.Endpoints.docker.Host}}') == unix://* && ${DOCKER_HOST:-unix://} == unix://* ]]
export root=$PWD tests=$PWD/tests/grafana
project="tank-grafana-check-$(openssl rand -hex 8)"
engine=$(command -v docker)
image="ghcr.io/frequensy23-coder/tank/tank-postgres:$project"
umask 077
tmp=$(mktemp -d)
export project engine image scratch=$tmp TANK_VERSION=$project
host=$tmp/host
mkdir "$host" "$tmp/bin" "$tmp/backup"
cleanup() {
    local status=$? label="label=com.docker.compose.project=$project"
    "$engine" ps -aq --filter "$label" | xargs -r "$engine" rm -f >/dev/null || status=1
    "$engine" volume ls -q --filter "$label" | xargs -r "$engine" volume rm >/dev/null || status=1
    "$engine" network ls -q --filter "$label" | xargs -r "$engine" network rm >/dev/null || status=1
    for tag in "$image" "$image-legacy"; do
        if "$engine" image inspect "$tag" >/dev/null 2>&1; then "$engine" image rm "$tag" >/dev/null || status=1; fi
    done
    rm -rf -- "$tmp"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python=python3
if [[ -x .venv/bin/python ]]; then python=.venv/bin/python; fi
"$engine" build -q --label org.tank.content="$project" -t "$image" deploy/postgres >/dev/null
a=$(git rev-parse HEAD)
export legacy=92c24e88a86dbe583ae3cee2f56aaf88e875ce29 FIXTURE_REPO=$tmp/source
sha=$a
git clone -q --shared --no-checkout . "$FIXTURE_REPO"
git -C "$FIXTURE_REPO" sparse-checkout set deploy/grafana
git -C "$FIXTURE_REPO" checkout -q -b fixture-main "$a"
dashboard=deploy/grafana/dashboards/tank-queue.json
jq '.tags += ["fixture-release-b"]' "$FIXTURE_REPO/$dashboard" > "$tmp/b.json"
mv "$tmp/b.json" "$FIXTURE_REPO/$dashboard"
git -C "$FIXTURE_REPO" -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm 'Distinct enabled release B' -- "$dashboard"
b=$(git -C "$FIXTURE_REPO" rev-parse HEAD)
[[ $a != "$b" ]]
stage() {
    local runner
    runner=$(mktemp -d "$tmp/runner.XXXXXXXX")
    RELEASE_SHA="$sha" ENGINE="$engine" IMAGE="$image" PROJECT="$project" "$python" tests/grafana/stage_release.py "$host" "$runner"
}
[[ $(stage) == "$sha" ]]
stack=$host/releases/$sha/stack
for key in POSTGRES_PASSWORD TANK_DB_PASSWORD OMA_DB_PASSWORD TANK_AGENT_DB_PASSWORD TANK_REPLAY_AGENT_DB_PASSWORD OMA_API_KEY OMA_ROOT_SECRET OMA_AUTH_SECRET OPENROUTER_API_KEY TANK_FOUNDRY_API_KEY TELEGRAM_BOT_TOKEN ETHERSCAN_API_KEY CODEX_GATEWAY_KEY APIFY_API_TOKEN; do
    printf '%s=%s\n' "$key" "$(openssl rand -hex 20)" >> "$host/.env"
done
printf 'GRAFANA_PORT=0\nTANK_BACKUP_PATH=%s\nKVM_GID=1\nTANK_SANDBOX_CPUS=1\nTANK_SANDBOX_MEMORY_MIB=128\n' "$tmp/backup" >> "$host/.env"

base_compose=("$engine" compose -p "$project" --env-file "$host/.env" -f "$stack/compose.yaml")
compose=("${base_compose[@]}" -f "$stack/compose.grafana.yaml")
pg=("$engine" exec -i -u postgres "$project-postgres-1" psql -XqAt -v ON_ERROR_STOP=1 -d tank)
role_hashes() {
    "${pg[@]}" -c "SELECT rolname,md5(rolpassword) FROM pg_authid WHERE rolcanlogin AND ($1) ORDER BY rolname"
}
base_roles="rolname IN ('postgres','tank','oma','tank_agent','tank_replay_agent')"
if [[ $scenario == existing ]]; then
    mkdir "$tmp/legacy"
    git archive "$legacy:deploy/postgres" | tar -x -C "$tmp/legacy"
    git show "$legacy:compose.yaml" > "$host/compose.yaml"
    git cat-file -e "$legacy:compose.grafana.yaml"
    printf '%s\n' "$legacy" > "$host/deployed"
    "$engine" build -q --label org.tank.content="$project-legacy" -t "$image-legacy" "$tmp/legacy" >/dev/null
    TANK_VERSION="$project-legacy" "$engine" compose -p "$project" --env-file "$host/.env" -f "$host/compose.yaml" up -d --no-deps --wait postgres
    legacy_id=$("${base_compose[@]}" ps -q postgres)
    printf "GRAFANA_READER_PASSWORD='%s\$fixture'\n" "$(openssl rand -hex 20)" >> "$host/.env"
    bash "$tests/migrate.sh"
    "${pg[@]}" -c 'CREATE TABLE ops.rollout_sentinel (value text); INSERT INTO ops.rollout_sentinel VALUES (gen_random_uuid()::text)'
    sentinel=$("${pg[@]}" -c 'SELECT value FROM ops.rollout_sentinel')
    role_hashes "$base_roles" > "$tmp/base-roles"
    [[ $(wc -l < "$tmp/base-roles") == 5 ]]
fi
cp "$host/.env" "$tmp/env.before"
install -m 755 "$tests/fixture-docker.sh" "$tmp/bin/docker"
ln -s /bin/true "$tmp/bin/nsenter"
printf '#!/bin/sh\nif [ "$1" = apify_worker.py ]; then printf "fixture fixture\\n"; else exec "%s" "$@"; fi\n' "$(command -v python3)" > "$tmp/bin/python3"
chmod 755 "$tmp/bin/python3"
rollout() {
    rm -f "$tmp/capture"
    timeout 240 env PATH="$tmp/bin:$PATH" "$host/deploy.sh" "$sha"
    [[ $(<"$host/deployed") == "$sha" ]]
}
fail_rollout() {
    local target=$1 recovery=$2
    touch "$tmp/fail-ready"
    rm -f "$tmp/capture"
    if timeout 240 env PATH="$tmp/bin:$PATH" "$host/deploy.sh" "$target" > "$tmp/failure.log"; then exit 1; else [[ $? == 1 ]]; fi
    grep -Fx "rolled back to $recovery" "$tmp/failure.log"
    [[ ! -e $tmp/fail-ready && $(<"$host/deployed") == "$recovery" ]]
}
if [[ $scenario == existing ]]; then
    fail_rollout "$a" "$legacy"
    [[ -z $("$engine" ps -aq --filter "label=com.docker.compose.project=$project" --filter label=com.docker.compose.service=grafana) ]]
    [[ $("$engine" inspect -f '{{index .Config.Labels "org.tank.content"}}' "$project-postgres-1") == "$project-legacy" ]]
fi
rollout
cmp -n "$(wc -c < "$tmp/env.before")" "$tmp/env.before" "$host/.env"
[[ $(stat -c %a "$host/.env") == 600 ]]
cp "$host/.env" "$tmp/env.first"
role_hashes 'true' > "$tmp/all-roles"
[[ $(grep -c '^tank_grafana_' "$tmp/all-roles") == 2 ]]
if [[ $scenario == existing ]]; then
    [[ $("${compose[@]}" ps -q postgres) != "$legacy_id" ]]
    role_hashes "$base_roles" | cmp "$tmp/base-roles" -
    [[ $("${pg[@]}" -c 'SELECT value FROM ops.rollout_sentinel') == "$sentinel" ]]
fi
GRAFANA_ADMIN_PASSWORD=$(cut -d= -f2- < <(grep '^GRAFANA_ADMIN_PASSWORD=' "$host/.env"))
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
for file in "$stack"/deploy/grafana/dashboards/*.json; do
    uid=$(jq -er '.uid' "$file")
    "${http[@]}" "$base/api/dashboards/uid/$uid" > "$tmp/dashboard"
    jq -ec 'select(.meta.provisioned and .meta.folderUid == "tank") | .dashboard.uid' "$tmp/dashboard"
    jq -e --slurpfile source "$file" '([.dashboard.panels[].id] | length == (unique | length)) and
        ([.dashboard.panels[] | {id,targets}] == [$source[0].panels[] | {id,targets}])' "$tmp/dashboard" >/dev/null
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
old_id=$("${compose[@]}" ps -q grafana)
"${pg[@]}" -d postgres -c 'CREATE DATABASE tank_test_grafana'
"$engine" exec -u postgres "$project-postgres-1" pg_dump -d tank | "${pg[@]}" -d tank_test_grafana >/dev/null
for fixture in tests/grafana/*.sql; do
    "${pg[@]}" -d tank_test_grafana -f - < "$fixture"
    printf 'PostgreSQL fixture passed: %s\n' "$fixture"
done
sha=$b
[[ $(stage) == "$sha" ]]
rollout
[[ $("${compose[@]}" ps -q grafana) != "$old_id" ]]
cmp -s "$tmp/env.first" "$host/.env"
role_hashes 'true' | cmp "$tmp/all-roles" -
if [[ $scenario == existing ]]; then [[ $("${pg[@]}" -c 'SELECT value FROM ops.rollout_sentinel') == "$sentinel" ]]; fi
resolve_url
[[ $("${pg[@]}" -c "$store_check") == 1 ]]
"${http[@]}" "$base/api/user" | jq -ec 'select(.login == "admin" and .name == "Persistence check") | {login,name}'
"${pg[@]}" -f - < "$tests/seeds/minute-checks.sql"
"${http[@]}" "$base/api/dashboards/uid/tank-pipeline" |
    jq -ce '[.dashboard.panels[] | select(.id == 34 and .type == "timeseries")] |
        if length == 1 and (.[0].targets | length > 0) then
        {from:"1767225600000",to:"1767226320000",queries:.[0].targets} else error("missing minute-check target") end' > "$tmp/minute-request"
"${http[@]}" -H 'Content-Type: application/json' --data-binary "@$tmp/minute-request" "$base/api/ds/query" |
    jq -ec '.results.A | select(.error == null and (.frames | length == 1)) | .frames[0] |
        .data.values as $values | [.schema.fields | to_entries[] | {key:.value.name,value:$values[.key]}] | from_entries |
        select((.Time // .time) == [1767225600000,1767226200000]) | del(.Time,.time) |
        select(. == {DONE:[2,0],FAILED:[1,0],EXPIRED:[1,1],QUEUED:[1,0],RUNNING:[0,1]})'
printf 'Grafana container replaced; admin and provisioned datasource retained in PostgreSQL.\n'
assert_b() {
    resolve_url
    "${http[@]}" "$base/api/dashboards/uid/tank-queue" | jq -e '.dashboard.tags | index("fixture-release-b") != null' >/dev/null
    "${http[@]}" "$base/api/user" | jq -e '.login == "admin" and .name == "Persistence check"' >/dev/null
    cmp -s "$tmp/env.first" "$host/.env"
    role_hashes 'true' | cmp "$tmp/all-roles" -
    [[ $("${pg[@]}" -c "$store_check") == 1 ]]
}
assert_b
if [[ $scenario == existing ]]; then
    fail_rollout "$a" "$b"
    assert_b
    sha=$legacy
    [[ $(stage) == "$legacy" && ! -e $host/releases/$legacy/stack/compose.grafana.yaml ]]
    cmp "$host/compose.yaml" "$host/releases/$legacy/stack/compose.yaml"
    rollout
    [[ -z $("$engine" ps -aq --filter "label=com.docker.compose.project=$project" --filter label=com.docker.compose.service=grafana) ]]
    [[ $("$engine" inspect -f '{{index .Config.Labels "org.tank.content"}}' "$project-postgres-1") == "$project-legacy" ]]
    [[ $("${pg[@]}" -c 'SELECT value FROM ops.rollout_sentinel') == "$sentinel" ]]
    sha=$b
    rollout
    assert_b
fi
printf 'PASS %s A=%s B=%s legacy=%s\n' "$scenario" "$a" "$b" "$legacy"

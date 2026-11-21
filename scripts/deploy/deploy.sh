#!/bin/bash
# The one way Tank reaches production, run by .github/workflows/deploy.yml as `deploy.sh <git sha>`
# in the host directory holding compose.yaml, .env and this script. It starts the CI-built images
# of that commit, proves within 8 minutes that they work, and otherwise returns to the previous
# commit. tank-capture is replaced make-before-break so no minute of prints is lost.
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
SHA=${1:?usage: deploy.sh <git sha>}
[[ "$SHA" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid release SHA"; exit 1; }
LIMIT=${LIMIT:-480}
exec 9>.deploy.lock
flock -n 9 || { echo "another deploy is running"; exit 1; }

grafana_enabled() {
  [ -f "releases/$1/stack/deploy/grafana/production-version" ]
}

machine=''
external_host=''

shared_nginx() {
  [ -n "$external_host" ] && [ -f "releases/$1/stack/deploy/grafana/nginx/compose.yaml" ]
}

dedicated_https() {
  [ "$machine" = oracle-vps ] && [ -z "$external_host" ] && [ -f "releases/$1/stack/deploy/grafana/https/compose.yaml" ]
}

preflight_release() {
  local v=$1 stack="releases/$1/stack" marker="releases/$1/stack/deploy/grafana/production-version"
  if [ -d "$stack/deploy/grafana/nginx" ]; then
    [ -f "$stack/deploy/grafana/nginx/compose.yaml" ] && grafana_enabled "$v" || {
      echo 'Incomplete shared nginx release'; return 1;
    }
  fi
  if [ -f "$stack/deploy/grafana/https/compose.yaml" ]; then
    grafana_enabled "$v" && [ -f "$stack/deploy/grafana/https/Caddyfile" ] || {
      echo 'Incomplete Grafana HTTPS release'; return 1;
    }
  fi
  if [ -e "$marker" ] || [ -L "$marker" ]; then
    [ ! -L "$marker" ] && grafana_enabled "$v" && cmp -s "$marker" <(printf '1\n') || {
      echo 'Unsupported Grafana production version'; return 1;
    }
    [ -f "$stack/compose.grafana.yaml" ] || { echo 'Missing Grafana Compose overlay'; return 1; }
  fi
}

compose() {
  local v=$1 stack="releases/$1/stack" files=()
  shift
  if [ -d "$stack" ]; then
    files=(-f "$stack/compose.yaml")
    if grafana_enabled "$v"; then files+=(-f "$stack/compose.grafana.yaml"); fi
    if shared_nginx "$v"; then files+=(-f "$stack/deploy/grafana/nginx/compose.yaml"); fi
    if dedicated_https "$v"; then files+=(-f "$stack/deploy/grafana/https/compose.yaml"); fi
  elif [ "$v" = "$previous" ]; then
    files=(-f compose.yaml)
  else
    echo "missing staged release $v" >&2; return 1
  fi
  TANK_VERSION=$v docker compose --env-file "$PWD/.env" "${files[@]}" "$@"
}

remove_service() {
  local id
  id=$(docker ps -aq --filter label=com.docker.compose.project=tank --filter "label=com.docker.compose.service=$1") || return 1
  if [ -n "$id" ]; then docker rm -f "$id" >/dev/null || return 1; fi
}

grafana_ready() {
  local address
  address=$(compose "$1" port grafana 3000) || return 1
  if shared_nginx "$1" && [[ $address != 127.0.0.2:983 ]]; then return 1; fi
  python3 grafana.py ready "$address" "releases/$1/stack/deploy/grafana/dashboards"
}

https_ready() {
  local host=${external_host:-grafana.84-235-183-96.sslip.io} path expected status format='%{http_code}'
  if [[ -n $external_host ]]; then format+=':%header{x-tank-route}'; fi
  for path in /login /api/dashboards/uid/tank-queue /api/dashboards/uid/tank-pipeline; do
    expected=401; [ "$path" != /login ] || expected=200
    if [[ -n $external_host ]]; then expected+=:grafana; fi
    status=$(curl -q --noproxy '*' --resolve "$host:443:127.0.0.1" --max-time 4 -s -o /dev/null -w "$format" "https://$host$path") || return 1
    [ "$status" = "$expected" ] || return 1
  done
}

start_grafana() {
  local v=$1 migrated=no
  for _ in $(seq 1 60); do
    if [ "$(docker exec -u postgres tank-postgres-1 psql -XAt -d tank -c \
      "SELECT 1 FROM ops.schema_migrations WHERE version='013_source_poll_counts.sql'" 2>/dev/null)" = 1 ]; then
      migrated=yes; break
    fi
    sleep 2
  done
  [ "$migrated" = yes ] || { echo 'Tank metrics migration did not finish'; return 1; }
  python3 grafana.py install "releases/$v/stack/deploy/grafana" tank-postgres-1 || return 1
  compose "$v" up -d --no-deps --wait --wait-timeout 120 grafana || return 1
  grafana_ready "$v" || return 1
  if dedicated_https "$v"; then
    compose "$v" up -d --no-deps grafana-edge || return 1
    for _ in $(seq 1 30); do
      https_ready && return 0
      sleep 2
    done
    echo 'Grafana TLS login or anonymous denial failed'; return 1
  fi
}
content() { docker image inspect -f '{{index .Config.Labels "org.tank.content"}}' "$1"; }

retained_image() {
  local v=$1 svc=$2 container running ref id desired label target tagged
  container=$(compose "$v" ps -a -q "$svc") || return 1
  [ -n "$container" ] || return 0
  running=$(docker inspect -f '{{.Config.Image}} {{.Image}}' "$container") || return 1
  read -r ref id <<< "$running"
  [[ ${ref##*:} =~ ^[a-f0-9]{40}$ && ${ref##*:} != "$v" ]] || return 0
  desired=$(compose "$v" config --format json "$svc" | python3 -c 'import json,sys; print(json.load(sys.stdin)["services"][sys.argv[1]]["image"])' "$svc") || return 1
  [[ $desired == "${ref%:*}:$v" ]] || return 0
  label=$(content "$id") || return 1
  target=$(content "$desired") || return 1
  [[ $label =~ ^[a-f0-9]{64}$ && $label == "$target" ]] || return 0
  tagged=$(docker image ls --no-trunc -q "$ref") || return 1
  if [ -z "$tagged" ]; then docker tag "$id" "$ref" || return 1; fi
  tagged=$(docker image inspect -f '{{.Id}}' "$ref") || return 1
  if [[ $tagged == "$id" ]]; then printf '%s\n' "$ref"; fi
}

start_backend() {
  local v=$1 svc=$2 ref files=()
  ref=$(retained_image "$v" "$svc") || return 1
  if [ -n "$ref" ]; then files=(-f -); fi
  compose "$v" "${files[@]}" up -d --pull never --no-deps "$svc" <<< "services: {$svc: {image: '$ref'}}"
}

healthy() {
  for _ in $(seq 1 60); do
    [ "$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$1" 2>/dev/null)" = healthy ] && return 0
    [ "$(docker inspect -f '{{if .State.Health}}none{{else}}{{.State.Status}}{{end}}' "$1" 2>/dev/null)" = running ] && return 0
    sleep 3
  done
  return 1
}

roll() {
  local v=$1 svc old_cap new_cap pid ready handoff wait services backup=no
  services=$(compose "$v" config --services) || return 1
  if grep -qxF tank-backup <<< "$services"; then
    backup=yes
  else
    remove_service tank-backup || return 1
  fi
  if ! dedicated_https "$v"; then
    remove_service grafana-edge || return 1
  fi
  if ! grafana_enabled "$v"; then
    remove_service grafana || return 1
  fi
  echo "pull $v $(date -u +%T)"
  compose "$v" pull -q || return 1
  for svc in postgres clickhouse openma; do
    start_backend "$v" "$svc" || return 1
    healthy "tank-$svc-1" || { echo "$svc not healthy"; return 1; }
  done

  old_cap=$(docker ps -q --filter label=com.docker.compose.project=tank --filter label=com.docker.compose.service=tank-capture)
  echo "tank-capture standby next to ${old_cap:-none} $(date -u +%T)"
  compose "$v" up -d --no-deps --no-recreate --scale tank-capture=$(( $(echo "$old_cap" | grep -c .) + 1 )) tank-capture || return 1
  new_cap=$(docker ps -q --filter label=com.docker.compose.project=tank --filter label=com.docker.compose.service=tank-capture | grep -v -x -F "${old_cap:-none}" | head -1)
  pid=$(docker inspect -f '{{.State.Pid}}' "$new_cap")
  ready=no
  for _ in $(seq 1 24); do
    nsenter -t "$pid" -n curl -sf -m 3 http://127.0.0.1:8791/readyz >/dev/null 2>&1 && { ready=yes; break; }
    sleep 5
  done
  echo "tank-capture ready=$ready $(date -u +%T)"
  if [ -n "$old_cap" ]; then
    if [ "$ready" = yes ]; then
      handoff=$(( ($(date +%s) / 60 + 2) * 60 + 20 ))
      wait=$(( handoff - $(date +%s) ))
      [ "$wait" -gt 0 ] && sleep "$wait"
    fi
    docker stop -t 30 $old_cap >/dev/null && docker rm $old_cap >/dev/null || return 1
    echo "old tank-capture stopped $(date -u +%T)"
  fi
  healthy "$new_cap" || { echo "tank-capture not healthy"; return 1; }
  echo "tank recreate $(date -u +%T)"
  compose "$v" up -d --no-deps tank || return 1
  healthy tank-tank-1 || { echo "tank not running"; return 1; }
  if grafana_enabled "$v"; then start_grafana "$v" || return 1; fi
  if [ "$backup" = yes ]; then
    compose "$v" exec -T -u postgres postgres provision-tank-backup || return 1
    compose "$v" up -d --no-deps --wait --wait-timeout 30 tank-backup || return 1
  fi
}

verify() {
  local since=$1 fail=0 cap lag n stale
  bad() { echo "FAIL $*"; fail=1; }
  chq() { docker exec -i tank-clickhouse-1 sh -c 'clickhouse-client --user tank --password "$TANK_DB_PASSWORD"'; }
  psqlq() { docker exec -i -u postgres tank-postgres-1 psql -d tank -Atq; }
  cap=$(docker ps -q --filter label=com.docker.compose.project=tank --filter label=com.docker.compose.service=tank-capture)
  [ "$(echo "$cap" | grep -c .)" = 1 ] || bad "expected one tank-capture container, found: $(echo $cap)"
  cap=$(echo "$cap" | head -1)
  [ "$(docker inspect -f '{{.State.Status}}' tank-tank-1 2>/dev/null)" = running ] || bad "tank not running"
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$cap" 2>/dev/null)" = healthy ] || bad "tank-capture not healthy"
  lag=$(echo "SELECT ifNull(toInt64(dateDiff('second', max(recorded_at), now64(6))), 9999) FROM mkt.klines_1m FINAL WHERE open_time > now() - INTERVAL 5 MINUTE" | chq)
  [ "${lag:-9999}" -le 90 ] || bad "no kline recorded for ${lag}s"
  n=$(docker logs --since "$since" "$cap" 2>&1 | grep -c "market flush failed" || true)
  [ "$n" = 0 ] || bad "$n market flush failures"
  stale=$(echo "select string_agg(s, ' ') from unnest(array['S09','S10','S11','S14']) s where not exists (select 1 from ops.source_checkpoints c where c.source_id=s and c.updated_at > timestamptz '$(docker inspect -f '{{.State.StartedAt}}' tank-tank-1)');" | psqlq)
  [ -z "$stale" ] || bad "exchange sources not polled since core start: $stale"
  stale=$(echo "SELECT arrayStringConcat(arrayFilter(i -> NOT has((SELECT groupArray(instrument_id) FROM mkt.oi_obs FINAL WHERE ts > now() - INTERVAL 10 MINUTE), i) OR NOT has((SELECT groupArray(instrument_id) FROM mkt.funding_obs FINAL WHERE as_of > now() - INTERVAL 10 MINUTE), i), ['binance:perp_linear:BTCUSDT','okx:perp_linear:BTC-USDT-SWAP','bybit:perp_linear:BTCUSDT','hyperliquid:perp_linear:BTC']), ' ')" | chq)
  [ -z "$stale" ] || bad "OI or funding older than 10 min: $stale"
  if grafana_enabled "$SHA"; then grafana_ready "$SHA" || bad "Grafana not ready"; fi
  if shared_nginx "$SHA" || dedicated_https "$SHA"; then https_ready || bad "Grafana HTTPS not ready"; fi
  return $fail
}

previous=$(cat deployed 2>/dev/null || true)
[[ -z "$previous" || "$previous" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid previous release SHA"; exit 1; }
tank_started=no
worker_previous=""
worker_current=""
recover() {
  local status=$?
  trap - EXIT INT TERM
  if [ "$status" -ne 0 ] && [ -n "$worker_previous" ] && [ "$worker_previous" != "$worker_current" ]; then
    python3 apify_worker.py restore "$worker_previous" || { echo "X worker restore failed"; status=1; }
  fi
  if [ "$status" -ne 0 ] && [ "$tank_started" = yes ] && [ -n "$previous" ]; then
    echo "rollback to $previous $(date -u +%T)"
    if roll "$previous"; then
      printf '%s\n' "$previous" > deployed.next && mv deployed.next deployed
      echo "rolled back to $previous"
      if shared_nginx "$previous" && ! https_ready; then echo 'Grafana restored locally; shared HTTPS is unavailable'; fi
    else
      echo "Tank rollback failed"
      status=1
    fi
  elif [ "$status" -ne 0 ] && [ "$tank_started" = yes ]; then
    remove_service tank-backup || status=1
    if grafana_enabled "$SHA"; then
      remove_service grafana-edge || status=1
      remove_service grafana || status=1
    fi
  fi
  exit "$status"
}
trap recover EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
preflight_release "$SHA"
if [ -n "$previous" ]; then preflight_release "$previous"; fi
machine=$(hostname -s)
site=/etc/nginx/sites-available/zzzz-tank-grafana
if [[ -f $site ]]; then
  external_host=$(awk '$1 == "server_name" { sub(/;$/, "", $2); print $2; exit }' "$site")
  [[ $external_host =~ ^grafana\.([0-9]{1,3}-){3}[0-9]{1,3}\.sslip\.io$ ]] || { echo 'Invalid Nexus Grafana site'; exit 1; }
  export GRAFANA_EXTERNAL_HOST=$external_host
elif [[ $machine != oracle-vps && ( -f releases/$SHA/stack/deploy/grafana/nginx/compose.yaml ||
    -f releases/$previous/stack/deploy/grafana/nginx/compose.yaml ) ]]; then
  echo 'Provision the Nexus Grafana site before deploying alongside nginx'; exit 1
fi
if [ -n "$previous" ] && grafana_enabled "$previous"; then
  python3 grafana.py require-credentials
elif grafana_enabled "$SHA"; then
  python3 grafana.py prepare
else
  python3 grafana.py check-env
fi
compose "$SHA" config -q >/dev/null 2>&1 || { echo 'Invalid candidate Compose configuration'; exit 1; }
if [ -n "$previous" ]; then
  compose "$previous" config -q >/dev/null 2>&1 || { echo 'Invalid recovery Compose configuration'; exit 1; }
fi
worker_source="releases/$SHA/x-reader"
if [ -d "releases/$SHA/stack" ]; then worker_source="releases/$SHA/stack/apify/x-reader"; fi
if [ -d "$worker_source" ]; then
  worker=$(python3 apify_worker.py publish "$worker_source" "$SHA") || { echo "X worker publication failed; Tank unchanged"; exit 1; }
  read -r worker_previous worker_current <<< "$worker"
fi
start=$(date +%s)
since=$(date -u +%FT%TZ)
echo "deploy $SHA at $since; previous ${previous:-none}"
out=""
tank_started=yes
if roll "$SHA"; then
  while [ $(( $(date +%s) - start )) -lt "$LIMIT" ]; do
    if out=$(verify "$since" 2>&1); then
      printf '%s\n' "$SHA" > deployed.next && mv deployed.next deployed
      echo "VERIFIED $SHA after $(( $(date +%s) - start ))s"
      docker image ls --format '{{.Repository}}:{{.Tag}}' | grep '^ghcr.io/frequensy23-coder/tank/' \
        | grep -v -E ":(${SHA}|${previous:-none})\$" | xargs -r docker rmi >/dev/null 2>&1 || true
      exit 0
    fi
    sleep 15
  done
  echo "NOT VERIFIED in ${LIMIT}s:"; echo "$out"
else
  echo "roll of $SHA failed"
fi
exit 1

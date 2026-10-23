#!/bin/bash
# The one way Tank reaches production, run by .github/workflows/deploy.yml as `deploy.sh <git sha>`
# in the host directory holding compose.yaml, .env and this script. It starts the CI-built images
# of that commit, proves within 8 minutes that they work, and otherwise returns to the previous
# commit. PostgreSQL, ClickHouse and OpenMA restart only when their image content changed;
# tank-capture is replaced make-before-break so no minute of prints is lost.
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
SHA=${1:?usage: deploy.sh <git sha>}
LIMIT=${LIMIT:-480}
exec 9>.deploy.lock
flock -n 9 || { echo "another deploy is running"; exit 1; }

compose() { TANK_VERSION=$1 docker compose -f compose.yaml "${@:2}"; }
content() { docker inspect -f '{{index .Config.Labels "org.tank.content"}}' "$1" 2>/dev/null || true; }
healthy() {
  for _ in $(seq 1 60); do
    [ "$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$1" 2>/dev/null)" = healthy ] && return 0
    [ "$(docker inspect -f '{{if .State.Health}}none{{else}}{{.State.Status}}{{end}}' "$1" 2>/dev/null)" = running ] && return 0
    sleep 3
  done
  return 1
}

roll() {
  local v=$1 svc old_cap new_cap pid ready handoff wait
  echo "pull $v $(date -u +%T)"
  compose "$v" pull -q
  for svc in postgres clickhouse openma; do
    if [ "$(content "tank-$svc-1")" = "$(content "ghcr.io/frequensy23-coder/tank/tank-$svc:$v")" ]; then
      echo "$svc unchanged"
    else
      echo "$svc recreate $(date -u +%T)"
      compose "$v" up -d --no-deps "$svc"
      healthy "tank-$svc-1" || { echo "$svc not healthy"; return 1; }
    fi
  done
  old_cap=$(docker ps -q --filter label=com.docker.compose.project=tank --filter label=com.docker.compose.service=tank-capture)
  echo "tank-capture standby next to ${old_cap:-none} $(date -u +%T)"
  compose "$v" up -d --no-deps --no-recreate --scale tank-capture=$(( $(echo "$old_cap" | grep -c .) + 1 )) tank-capture
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
    docker stop -t 30 $old_cap >/dev/null && docker rm $old_cap >/dev/null
    echo "old tank-capture stopped $(date -u +%T)"
  fi
  healthy "$new_cap" || { echo "tank-capture not healthy"; return 1; }
  echo "tank recreate $(date -u +%T)"
  compose "$v" up -d --no-deps tank
  healthy tank-tank-1 || { echo "tank not running"; return 1; }
  echo "$v" > deployed
}

verify() {
  local since=$1 fail=0 cap lag n stale holes age
  bad() { echo "FAIL $*"; fail=1; }
  chq() { docker exec -i tank-clickhouse-1 sh -c 'clickhouse-client --user tank --password "$TANK_DB_PASSWORD"'; }
  psqlq() { docker exec -i -u postgres tank-postgres-1 psql -d tank -Atq; }
  cap=$(docker ps -q --filter label=com.docker.compose.project=tank --filter label=com.docker.compose.service=tank-capture)
  [ "$(echo "$cap" | grep -c .)" = 1 ] || bad "expected one tank-capture container, found: $(echo $cap)"
  cap=$(echo "$cap" | head -1)
  [ "$(docker inspect -f '{{.State.Status}}' tank-tank-1 2>/dev/null)" = running ] || bad "tank not running"
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$cap" 2>/dev/null)" = healthy ] || bad "tank-capture not healthy"
  lag=$(echo "SELECT ifNull(toInt64(dateDiff('second', max(recorded_at), now64(6))), 9999) FROM mkt.flow_bars_1m FINAL WHERE minute_start > now() - INTERVAL 5 MINUTE" | chq)
  [ "${lag:-9999}" -le 90 ] || bad "no bar recorded for ${lag}s"
  n=$(docker logs --since "$since" "$cap" 2>&1 | grep -c "market flush failed" || true)
  [ "$n" = 0 ] || bad "$n market flush failures"
  stale=$(echo "select string_agg(s, ' ') from unnest(array['S09','S10','S11','S14']) s where not exists (select 1 from ops.source_checkpoints c where c.source_id=s and c.updated_at > timestamptz '$(docker inspect -f '{{.State.StartedAt}}' tank-tank-1)');" | psqlq)
  [ -z "$stale" ] || bad "exchange sources not polled since core start: $stale"
  age=$(echo "SELECT dateDiff('second', max(ts), now64(6)) FROM mkt.trades WHERE instrument_id = 'binance:perp_linear:BTCUSDT' AND ts > now() - INTERVAL 1 HOUR" | chq 2>/dev/null)
  [ "${age:-9999}" -le 60 ] 2>/dev/null || bad "newest BTC print in mkt.trades ${age:-none}s old"
  holes=$(echo "SELECT arrayStringConcat(groupArray(concat(i, ' ', formatDateTime(m, '%H:%i'))), ', ') FROM (SELECT arrayJoin(['binance:spot:BTCUSDT','coinbase:spot:BTC-USD','kraken:spot:XBT/USD','okx:spot:BTC-USDT','bybit:spot:BTCUSDT','binance:perp_linear:BTCUSDT','okx:perp_linear:BTC-USDT-SWAP','bybit:perp_linear:BTCUSDT','hyperliquid:perp_linear:BTC']) AS i, arrayJoin(arrayMap(x -> toStartOfMinute(parseDateTime64BestEffort('$since', 6, 'UTC')) - INTERVAL 5 MINUTE + toIntervalMinute(x), range(toUInt32(dateDiff('minute', toStartOfMinute(parseDateTime64BestEffort('$since', 6, 'UTC')) - INTERVAL 5 MINUTE, toStartOfMinute(now()) - INTERVAL 2 MINUTE)) + 1))) AS m) w LEFT ANTI JOIN (SELECT instrument_id AS i, minute_start AS m FROM mkt.flow_bars_1m FINAL WHERE complete AND minute_start >= parseDateTime64BestEffort('$since', 6, 'UTC') - INTERVAL 6 MINUTE) b USING (i, m)" | chq)
  [ -z "$holes" ] || bad "incomplete BTC minutes: $holes"
  stale=$(echo "SELECT arrayStringConcat(arrayFilter(i -> NOT has((SELECT groupArray(instrument_id) FROM mkt.oi_obs FINAL WHERE ts > now() - INTERVAL 10 MINUTE), i) OR NOT has((SELECT groupArray(instrument_id) FROM mkt.funding_obs FINAL WHERE as_of > now() - INTERVAL 10 MINUTE), i), ['binance:perp_linear:BTCUSDT','okx:perp_linear:BTC-USDT-SWAP','bybit:perp_linear:BTCUSDT','hyperliquid:perp_linear:BTC']), ' ')" | chq)
  [ -z "$stale" ] || bad "OI or funding older than 10 min: $stale"
  return $fail
}

previous=$(cat deployed 2>/dev/null || true)
start=$(date +%s)
since=$(date -u +%FT%TZ)
echo "deploy $SHA at $since; previous ${previous:-none}"
out=""
if roll "$SHA"; then
  while [ $(( $(date +%s) - start )) -lt "$LIMIT" ]; do
    if out=$(verify "$since" 2>&1); then
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
[ -n "$previous" ] || exit 1
echo "rollback to $previous $(date -u +%T)"
roll "$previous" && echo "rolled back to $previous after $(( $(date +%s) - start ))s"
exit 1

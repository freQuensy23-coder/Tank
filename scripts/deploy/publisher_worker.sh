#!/bin/bash
set -euo pipefail
[ "$#" -eq 6 ] || { echo "publisher worker requires six explicit arguments" >&2; exit 1; }
image=$1
orchestrator=$2
selected=$3
transaction=$4
action=$5
host_root=$(readlink -e "$6")
[[ "$orchestrator" =~ ^[0-9a-f]{40}$ && "$selected" =~ ^[0-9a-f]{40}$ && "$transaction" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid publisher identity" >&2; exit 1; }
[[ "$image" =~ ^ghcr\.io/[a-z0-9._-]+/[a-z0-9._-]+/tank:[0-9a-f]{40}$ && "$image" = *:"$orchestrator" ]] || { echo "invalid publisher image" >&2; exit 1; }
case "$action" in
  prepare|activate|commit|finish|rollback|rollback_finish|previous_release|recovery_release) ;;
  *) echo "invalid publisher action" >&2; exit 1 ;;
esac
[[ "$host_root" = /* && "$host_root" != *:* && "$host_root" != *$'\n'* ]] || { echo "invalid publisher host directory" >&2; exit 1; }
state="$host_root/apify-state"
[ ! -L "$state" ] || { echo "publisher state cannot be symbolic" >&2; exit 1; }
if [ ! -e "$state" ]; then
  install -d -m 700 "$state"
fi
[ -d "$state" ] && [ "$(stat -c '%u:%a' "$state")" = "$EUID:700" ] || { echo "publisher state must be private and owned" >&2; exit 1; }
umask 077
lock="$state/.worker.lock"
[ ! -L "$lock" ] || { echo "publisher lock cannot be symbolic" >&2; exit 1; }
if [ -e "$lock" ]; then
  [ -f "$lock" ] && [ "$(stat -c '%u:%a' "$lock")" = "$EUID:600" ] || { echo "publisher lock must be private and owned" >&2; exit 1; }
fi
exec 8>"$lock"
flock -n 8 || { echo "another publisher worker command is active" >&2; exit 1; }
lease="$state/.worker-active"
cid=""
nonce=""
client=""
work=""

check_container() {
  local actual
  actual=$(docker inspect --format '{{index .Config.Labels "org.tank.publisher.role"}}|{{index .Config.Labels "org.tank.publisher.owner"}}|{{index .Config.Labels "org.tank.publisher.image"}}|{{index .Config.Labels "org.tank.publisher.root"}}' "$1" 8>&-) || return 1
  [ "$actual" = "worker-v1|$2|$3|$host_root" ]
}

drain_container() {
  check_container "$1" "$2" "$3" || { echo "publisher container ownership is unverified" >&2; return 1; }
  docker rm --force "$1" >/dev/null 8>&- || { echo "publisher container drain failed" >&2; return 1; }
}

if [ -e "$lease" ] || [ -L "$lease" ]; then
  [ ! -L "$lease" ] && [ -f "$lease" ] && [ "$(stat -c '%u:%a' "$lease")" = "$EUID:600" ] || { echo "publisher lease must be private and owned" >&2; exit 1; }
  mapfile -t prior < "$lease"
  [ "${#prior[@]}" -eq 4 ] && [ "${prior[0]}" = tank-publisher-worker-v1 ] || { echo "invalid publisher lease shape" >&2; exit 1; }
  [[ "${prior[1]}" =~ ^[0-9a-f]{64}$ && "${prior[2]}" =~ ^\.publisher-run\.[A-Za-z0-9]+$ && "${prior[3]}" =~ ^ghcr\.io/[a-z0-9._-]+/[a-z0-9._-]+/tank:[0-9a-f]{40}$ ]] || { echo "invalid publisher lease identity" >&2; exit 1; }
  drain_container "${prior[1]}" "${prior[2]}" "${prior[3]}" || exit 125
  rm -- "$lease"
fi

cleanup() {
  local result=$?
  trap - EXIT INT TERM HUP
  if [ -n "$cid" ]; then
    drain_container "$cid" "$nonce" "$image" || exit 125
  fi
  if [ -n "$client" ]; then
    wait "$client" 2>/dev/null || true
  fi
  if [ -n "$cid" ]; then
    rm -f -- "$lease"
  fi
  if [ -n "$work" ]; then
    rm -f -- "$work/cid" "$work/lease" "$work/stdout" "$work/stderr"
    rmdir -- "$work"
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
work=$(mktemp -d "$state/.publisher-run.XXXXXXXX")
nonce=${work##*/}
docker create --pull never --cidfile "$work/cid" \
  --label org.tank.publisher.role=worker-v1 \
  --label "org.tank.publisher.owner=$nonce" \
  --label "org.tank.publisher.image=$image" \
  --label "org.tank.publisher.root=$host_root" \
  --user 0:0 --network host --read-only \
  --volume "$host_root:/host:ro" --volume "$state:/host/apify-state:rw" \
  --env PYTHONDONTWRITEBYTECODE=1 --workdir /host \
  --entrypoint /opt/tank/venv/bin/python "$image" \
  -m tank.deployment.cli "$action" \
  --orchestrator-sha "$orchestrator" --selected-release-sha "$selected" \
  --transaction-release-sha "$transaction" --runtime-root /app \
  --package "/host/releases/$selected" --state /host/apify-state \
  --credential-file /host/.env --tank-marker /host/deployed --build-timeout-s 600 \
  >/dev/null 8>&-
mapfile -t created < "$work/cid"
[ "${#created[@]}" -eq 1 ] && [[ "${created[0]}" =~ ^[0-9a-f]{64}$ ]] || { echo "publisher create did not return a single container identity" >&2; exit 1; }
cid=${created[0]}
check_container "$cid" "$nonce" "$image" || { echo "publisher create ownership is unverified" >&2; exit 125; }
printf '%s\n' tank-publisher-worker-v1 "$cid" "$nonce" "$image" > "$work/lease"
mv -- "$work/lease" "$lease"
docker start --attach "$cid" >"$work/stdout" 2>"$work/stderr" 8>&- &
client=$!
if wait "$client"; then
  client_status=0
else
  client_status=1
fi
client=""
cat -- "$work/stdout"
cat -- "$work/stderr" >&2
[ "$client_status" -eq 0 ] || exit 1
container_status=$(docker inspect --format '{{.State.Status}}|{{.State.ExitCode}}' "$cid" 8>&-)
[ "$container_status" = 'exited|0' ] || { echo "publisher process did not finish successfully" >&2; exit 1; }

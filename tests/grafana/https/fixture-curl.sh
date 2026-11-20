#!/usr/bin/env bash
set -euo pipefail
[[ $project =~ ^tank-grafana-check-[0-9a-f]{16}$ ]]
host=grafana.84-235-183-96.sslip.io
if [[ ${!#} == "https://$host/"* ]]; then
    id=$("$engine" ps -q --filter "label=com.docker.compose.project=$project" --filter label=com.docker.compose.service=grafana-edge)
    binding=$("$engine" port "$id" 443/tcp)
    [[ $binding =~ ^127\.0\.0\.1:([0-9]+)$ ]]
    url=${!#}
    exec /usr/bin/curl "${@:1:$#-1}" --cacert "$scratch/pebble.minica.pem" "https://localhost:${BASH_REMATCH[1]}/${url#https://$host/}"
fi
exec /usr/bin/curl "$@"

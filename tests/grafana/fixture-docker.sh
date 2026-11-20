#!/usr/bin/env bash
set -euo pipefail
[[ $project =~ ^tank-grafana-check-[0-9a-f]{16}$ ]]
last=${!#}
case $1 in
    compose)
        shift
        if [[ " $* " == *' -p tank-settings -f - config --format json '* ]]; then exec "$engine" compose "$@"; fi
        if [[ $TANK_VERSION == "$legacy" ]]; then export TANK_VERSION=$project-legacy; else export TANK_VERSION=$project; fi
        case " $* " in
            *' config -q '*) exec "$engine" compose -p "$project" "$@" ;;
            *' pull -q '*) exit 0 ;;
            *' up '*)
                case $last in
                    grafana)
                        if [[ -n ${STAGE_PORT:-} ]]; then
                            args=(); for arg in "$@"; do
                                if [[ $arg == up ]]; then args+=(-f "$scratch/staging/grafana.yaml"); fi
                                args+=("$arg")
                            done
                            exec "$engine" compose -p "$project" "${args[@]}"
                        fi
                        exec "$engine" compose -p "$project" "$@" ;;
                    postgres) exec "$engine" compose -p "$project" "$@" ;;
                    clickhouse|openma) exit 0 ;;
                    tank-capture) touch "$scratch/capture"; exit 0 ;;
                    tank) bash "$tests/migrate.sh"; exit 0 ;;
                esac ;;
            *' port grafana 3000 '*)
                if [[ -e $scratch/fail-ready ]]; then rm "$scratch/fail-ready" "$scratch/capture"; exit 1; fi
                exec "$engine" compose -p "$project" "$@" ;;
        esac ;;
    inspect)
        case $last in
            tank-postgres-1) exec "$engine" inspect "${@:2:$#-2}" "$project-postgres-1" ;;
            ghcr.io/frequensy23-coder/tank/tank-postgres:"$legacy") exec "$engine" inspect "${@:2:$#-2}" "$image-legacy" ;;
            ghcr.io/frequensy23-coder/tank/tank-postgres:*) exec "$engine" inspect "${@:2:$#-2}" "$image" ;;
            capture-fixture|tank-clickhouse-1|tank-openma-1|tank-tank-1|ghcr.io/*)
                case $3 in
                    *org.tank.content*) printf 'fixture\n' ;;
                    *StartedAt*) date -u +%FT%TZ ;;
                    *Health*) printf 'healthy\n' ;;
                    *State.Status*) printf 'running\n' ;;
                    *State.Pid*) printf '1\n' ;;
                    *) exit 1 ;;
                esac
                exit 0 ;;
        esac ;;
    ps)
        case $last in
            *service=tank-capture) if [[ -e $scratch/capture ]]; then printf 'capture-fixture\n'; fi; exit 0 ;;
            *service=grafana) exec "$engine" ps -aq --filter "label=com.docker.compose.project=$project" --filter label=com.docker.compose.service=grafana ;;
        esac ;;
    exec)
        if [[ " $* " == *' tank-clickhouse-1 '* ]]; then
            sql=$(cat); if [[ $sql == *'mkt.klines_1m'* ]]; then printf '2\n'; fi; exit 0
        fi
        if [[ " $* " == *' tank-postgres-1 '* ]]; then
            args=(); for arg in "$@"; do
                if [[ $arg == tank-postgres-1 ]]; then arg="$project-postgres-1"; fi
                args+=("$arg")
            done
            if [[ $last == -Atq ]]; then
                sql=$(cat); [[ $sql == *ops.source_checkpoints* ]] || exit 1; exit 0
            fi
            exec "$engine" "${args[@]}"
        fi ;;
    logs) [[ $last == capture-fixture ]]; exit ;;
    image) [[ $2 == ls ]]; exit ;;
    rm) [[ $2 == -f && $("$engine" inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$last") == "$project" ]] && exec "$engine" rm -f "$last" ;;
esac
printf 'Unexpected Docker call: %s %s\n' "$1" "$last" >&2
exit 1

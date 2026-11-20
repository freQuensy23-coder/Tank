start_stage_https() {
    stage_name=grafana.127-0-0-1.sslip.io
    [[ $(getent ahostsv4 "$stage_name" | cut -d' ' -f1 | sort -u) == 127.0.0.1 ]]
    mkdir "$tmp/staging"
    git archive "$a:deploy/grafana/staging" | tar -x -C "$tmp/staging"
    edge=("$engine" compose -p "$project" -f "$tmp/staging/compose.yaml")
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=pebble \
        -addext 'subjectAltName=DNS:pebble,IP:127.0.0.1' -keyout "$tmp/transport.key" -out "$tmp/transport.pem" 2>/dev/null
    "${edge[@]}" up -d pebble edge
    local binding
    binding=$("${edge[@]}" port edge 443)
    [[ $binding =~ ^127\.0\.0\.1:([0-9]+)$ ]]
    export STAGE_PORT=${BASH_REMATCH[1]}
    stage_url="https://$stage_name:$STAGE_PORT"
    "$engine" run --rm --network "container:$("${edge[@]}" ps -q pebble)" \
        -v "$tmp/transport.pem:/etc/ssl/certs/ca-certificates.crt:ro" --entrypoint wget \
        "$("${edge[@]}" images -q edge)" -qO- https://127.0.0.1:15000/roots/0 > "$tmp/root.pem"
    stage_curl=(curl --noproxy '*' --resolve "$stage_name:$STAGE_PORT:127.0.0.1" --cacert "$tmp/root.pem" --max-time 5 -sS)
    for _ in {1..45}; do
        if "${stage_curl[@]}" -o /dev/null "$stage_url/login" 2>/dev/null; then break; fi
        sleep 1
    done
    stage_unavailable
    if curl --noproxy '*' --resolve "$stage_name:$STAGE_PORT:127.0.0.1" --max-time 5 -s -o /dev/null "$stage_url/login"; then
        return 1
    else
        [[ $? == 60 ]]
    fi
    stage_serial > "$tmp/first-serial"
    local db="$tmp/browser-home/.local/share/pki/nssdb"
    mkdir -p "$db" "$tmp/browser-home/.pki"
    ln -s "$db" "$tmp/browser-home/.pki/nssdb"
    certutil -N -d "sql:$db" --empty-password
    certutil -A -d "sql:$db" -n staging-only -t 'C,,' -i "$tmp/root.pem"
}

stage_unavailable() {
    [[ $("${stage_curl[@]}" -w '%{http_code}' -o /dev/null "$stage_url/api/dashboards/uid/tank-queue") == 502 ]]
}

stage_serial() {
    openssl s_client -connect "127.0.0.1:$STAGE_PORT" -servername "$stage_name" -verify_hostname "$stage_name" \
        -CAfile "$tmp/root.pem" -verify_return_error </dev/null 2>/dev/null | openssl x509 -noout -serial
}

stage_viewer() {
    openssl rand -hex 32 > "$tmp/viewer-password"
    jq -nc --rawfile pw "$tmp/viewer-password" '{name:"Staging Viewer",login:"stage-viewer",email:"viewer@localhost",password:($pw|rtrimstr("\n"))}' |
        "${http[@]}" -H 'Content-Type: application/json' --data-binary @- "$base/api/admin/users" | jq -e '.id > 1' >/dev/null
}

stage_browser() {
    timeout 100 env HOME="$tmp/browser-home" XDG_CONFIG_HOME="$tmp/browser-home/.config" XDG_DATA_HOME="$tmp/browser-home/.local/share" \
        python3 "$tests/https/browser.py" "$tmp" "$STAGE_PORT"
}

stage_renewal() {
    local serial
    for _ in {1..65}; do
        serial=$(stage_serial)
        if [[ -n $serial && $serial != "$(<"$tmp/first-serial")" ]]; then
            stage_browser
            printf 'Verified automatically renewed DNS certificate and existing Viewer session.\n'
            return
        fi
        sleep 2
    done
    return 1
}

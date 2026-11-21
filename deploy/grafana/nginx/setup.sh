#!/bin/bash
set -euo pipefail
host=${1:?expected the Nexus Grafana hostname}
[[ $host =~ ^grafana\.([0-9]{1,3}-){3}[0-9]{1,3}\.sslip\.io$ ]] || exit 1
site=/etc/nginx/sites-enabled/zzzz-tank-grafana
candidate=$(mktemp /etc/nginx/tank-grafana.XXXXXXXX)
trap 'rm -f "$candidate"' EXIT
sed "s/@HOST@/$host/g" "$(dirname "$0")/tank-grafana.conf" > "$candidate"
chmod 644 "$candidate"
if [[ -L $site || -e $site ]] && ! { [[ -f $site && ! -L $site ]] && cmp -s "$candidate" "$site"; }; then
  echo 'Refusing to replace a different nginx site' >&2; exit 1
fi
systemctl is-active --quiet certbot.timer
systemctl is-enabled --quiet certbot.timer
certbot certonly --non-interactive --agree-tos --keep-until-expiring --webroot -w /var/www/html \
  --cert-name "$host" -d "$host" >/dev/null 2>&1 || { echo 'Certbot issuance failed' >&2; exit 1; }
created=no
if [[ ! -e $site ]]; then ln "$candidate" "$site"; created=yes; fi
if nginx -t >/dev/null 2>&1 && systemctl reload nginx; then exit 0; fi
if [[ $created = yes ]]; then rm "$site"; nginx -t >/dev/null 2>&1 && systemctl reload nginx; fi
echo 'Grafana site activation failed' >&2; exit 1

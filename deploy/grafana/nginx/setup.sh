#!/bin/bash
set -euo pipefail
host=${1:?expected the Nexus Grafana hostname}
[[ $host =~ ^grafana\.([0-9]{1,3}-){3}[0-9]{1,3}\.sslip\.io$ ]] || exit 1
site=/etc/nginx/sites-available/zzzz-tank-grafana
enabled=/etc/nginx/sites-enabled/zzzz-tank-grafana
candidate=$(mktemp /etc/nginx/tank-grafana.XXXXXXXX)
trap 'rm -f "$candidate"' EXIT
sed "s/@HOST@/$host/g" "$(dirname "$0")/tank-grafana.conf" > "$candidate"
chmod 644 "$candidate"
if [[ -L $site || -e $site ]] && ! { [[ -f $site && ! -L $site ]] && cmp -s "$candidate" "$site"; }; then
  echo 'Refusing to replace a different nginx site' >&2; exit 1
fi
systemctl is-active --quiet certbot.timer
systemctl is-enabled --quiet certbot.timer
if [[ -e $enabled || -L $enabled ]]; then
  [[ $(readlink "$enabled") == "$site" && -f $site ]] || { echo 'Unowned nginx site'; exit 1; }
  nginx -t >/dev/null 2>&1 && systemctl reload nginx
  exit $?
fi
if [[ ! -f $site && ( -e /etc/letsencrypt/renewal/$host.conf || -e /etc/letsencrypt/live/$host ) ]]; then
  echo 'Refusing to adopt an existing certificate'; exit 1
fi
nginx -T 2>/dev/null | awk -v host="$host" 'index(tolower($0), host) || /server_name[[:space:]]+[^;]*[~*$]/ || /server_name[^;]*[[:space:]]\./ || /server_name[[:space:]][^;]*$/ { found=1 } END { exit found }' || {
  echo 'Invalid nginx configuration or hostname already in use'; exit 1;
}
if [[ ! -e $site ]]; then ln "$candidate" "$site"; fi
certbot certonly --non-interactive --agree-tos --keep-until-expiring --webroot -w /var/www/html \
  --cert-name "$host" -d "$host" >/dev/null 2>&1 || { echo 'Certbot issuance failed' >&2; exit 1; }
ln -s "$site" "$enabled"
if nginx -t >/dev/null 2>&1 && systemctl reload nginx; then exit 0; fi
rm "$enabled"
nginx -t >/dev/null 2>&1 && systemctl reload nginx
echo 'Grafana site activation failed; the system certificate was retained' >&2; exit 1

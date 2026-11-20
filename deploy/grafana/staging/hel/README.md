# HEL Viewer preview (staging only)

This recipe is specific to the authorized HEL host, its shared nginx and the unused dedicated `172.29.137.0/28` bridge. It is not an overlay for the Tank production stack. The November 16 UTC data are generated metric rows, **not** historical activity. This fixture uses its own PostgreSQL and a Grafana settings volume, not the production store schema.

Prepare a **new, private** stage directory from one committed checkout. On an existing stage retain the original credentials and volumes; do not run initialization again.

```bash
set -eu -o pipefail
umask 077
sha=$(git rev-parse HEAD)
stage=$HOME/.local/share/tank-grafana-hel
mkdir -m 700 "$stage"
git archive "$sha:deploy/grafana/staging/hel" | tar -xp -C "$stage"
mkdir "$stage/source"
git archive "$sha" deploy/grafana/dashboards deploy/grafana/provisioning/datasources | tar -xp -C "$stage/source"
cd "$stage"
for key in DB_PASSWORD ADMIN_PASSWORD SECRET_KEY GRAFANA_READER_PASSWORD; do
  printf '%s=%s\n' "$key" "$(openssl rand -hex 32)"
done > .env
docker compose up -d
```

1. On HEL only, confirm the named host in `http.conf` resolves to this stand and the two site names below do not already exist. Keep the existing default vhosts. Install `http.conf` as `sites-enabled/zz-tank-grafana-hel.conf`, create `/var/www/tank-grafana-hel` and run `nginx -t` before reloading nginx.
2. Obtain a public certificate for that name using `certbot certonly --webroot -w /var/www/tank-grafana-hel -d grafana-hel.31-77-97-14.sslip.io --config-dir /var/lib/tank-grafana-hel-acme/config --work-dir /var/lib/tank-grafana-hel-acme/work --logs-dir /var/lib/tank-grafana-hel-acme/logs`. Reuse a valid existing stage certificate. Install `https.conf` as `sites-enabled/zz-tank-grafana-hel-tls.conf`, test and reload nginx. No container port or CA management endpoint is published.
3. Use the private stage admin credential over the verified HTTPS origin to create a named Viewer; keep credentials off the command line and out of the repository. Open `/` for the fixture window or choose the same range on `/d/tank-queue/`. Anonymous protected APIs must return 401; `/api/admin/settings` must return 403 to the Viewer.
4. Schedule expiry **before** certificate expiration. Remove only the two matching stage sites, then successfully test and reload nginx **before** removing the stack and bridge (`docker compose down --volumes` from the stage directory). Do not leave a proxy pointing to a released container IP. Do not assume the shared Certbot timer covers this separate state; a longer-lived stand needs dedicated renewal and a successful nginx reload.

# Tank Grafana

## Production

Run **Deploy** on `main`. No Grafana password, separate database, manual SQL or post-deploy command is required. The action ships the selected commit's Compose files, SQL, provisioning and dashboards. The PostgreSQL image admits the two Grafana logins only from `172.30.60.11` on `tank-net` using SCRAM. The first rollout replaces the Postgres container to load that rule; it preserves `pg-data`.

Tank applies its migrations first. Deploy waits for `013_source_poll_counts.sql`, bootstraps the roles and schemas, refreshes the metric views in a transaction, then starts the pinned Grafana image. Before marking a release deployed it verifies the configured PostgreSQL reader's datasource health and both provisioned dashboards in the **Tank** folder through authenticated HTTP. Those API endpoints must deny anonymous access.

### Browser access

The host-local upstream is `http://127.0.0.1:3000`, **not** an Internet URL. The dashboard paths are `/d/tank-queue/` and `/d/tank-pipeline/`. Port 3000 is published on loopback only. There is no public dashboard, anonymous login or published database port.

After the reviewed release is deployed, open `https://grafana.84-235-183-96.sslip.io/d/tank-queue/` or `/d/tank-pipeline/` on the same origin and sign in with a production Grafana account. Give dashboard readers a named Viewer; staging accounts and URLs are not production access.

The selected HTTPS overlay runs staging’s pinned Caddy image on a separate network shared only with Grafana. It publishes TCP 443 only. The hostname must resolve to the production VM, inbound TCP 443 must reach Docker through the Oracle network and host firewall, and Caddy needs outbound DNS and HTTPS to Let's Encrypt. It obtains and renews the certificate by TLS-ALPN-01; keep the `tank_grafana-tls` volume private and persistent. Deploy checks the trusted hostname, login page and anonymous API denial locally through Caddy, without sending credentials. Check the same origin from outside after Deploy; a local check cannot verify the cloud firewall.

### Hosts with an existing nginx

When the deployment target already serves other sites on TCP 80 and 443, set `GRAFANA_EXTERNAL_HOST=grafana.192-0-2-1.sslip.io` in its protected `/srv/tank-deploy/.env`, replacing the documentation address with the target’s dashed IPv4 address. Use the hostname only, not a URL, port or wildcard. Keep the override through rollbacks. It selects shared ingress; it **does not** change the workflow’s SSH destination. Leave it unset on a dedicated-edge host.

Deploy stages the named nginx site from the release. Grafana still publishes only on IPv4 loopback. No Caddy service, Docker port 80/443, new nginx daemon or replacement default site is required. The exact DNS name must point directly to this host; Let’s Encrypt must reach TCP 80 for HTTP-01 and browsers must reach TCP 443. Keep nginx, Certbot, the renewal scheduler and outbound DNS/HTTPS available. Do not use Certbot standalone mode or stop nginx to issue a certificate.

Before exposing an existing Grafana store, rotate any compromised credentials, revoke affected sessions, and synchronize the protected settings. Changing only the initial-password setting does not change an existing account. Verify browser access at `https://<GRAFANA_EXTERNAL_HOST>/d/tank-queue/` and `/d/tank-pipeline/` as a named Viewer. Deploy’s HTTPS checks validate the certificate and anonymous API denial without credentials. A loopback check does not prove public reachability.

On a shared host the selected release must include the nginx assets for public access. Selecting an older release without them removes the managed route before replacing its upstream; even a historical dedicated-edge release stays loopback-only. Rollback does not stop the shared nginx or edit other sites. Restore public access by selecting a shared-ingress-capable release through Deploy.

### Credentials and state

Under the deployment lock, Deploy keeps existing Grafana settings in `/srv/tank-deploy/.env` and generates only absent `GRAFANA_ADMIN_PASSWORD`, `GRAFANA_SECRET_KEY`, `GRAFANA_STORE_PASSWORD` and `GRAFANA_READER_PASSWORD` (32 independent random bytes per generated value). The SSH transport identity must be authorized for noninteractive `sudo` on the deployment host. Root stages and runs the release; the regular `.env` remains root-owned and mode 0600. Deploy validates the candidate and recovery configurations before X worker publication. Changes preserve its prior contents and are installed atomically with mode 0600. Subsequent deploys do not rewrite a complete file. No secret is sent back to Actions. Host administrators and Docker administrators remain trusted.

The infrastructure owner can recover the initial `admin` credential from `GRAFANA_ADMIN_PASSWORD` in the protected file and issue named Viewer accounts privately. Keep an access-controlled backup of this file **with** `pg-data`; do not regenerate the encryption key on recovery. The initial admin credential is also used by the local readiness check: if deliberately rotating it in Grafana, update the protected setting in the same maintenance window. Never send the `.env` file, put credentials in URLs or share the admin login with dashboard readers.

`tank.grafana` is owned by `tank_grafana_store` and persists dashboards, users, preferences and encrypted datasource settings. `tank_grafana_reader` gets only the named `tank_metrics` views, a read-only transaction default, four connections and a 15-second statement timeout. Bootstrap removes `PUBLIC` temporary-table creation in `tank`, preserving it for Tank, its existing agent and the Grafana store. Unmarked existing roles or schemas with the same names are not adopted.

### Rollback

A failed rollout returns to the previous recorded Tank release. For a Grafana-enabled release it uses that release's files, reinstalls its views and checks Grafana. Only `deploy/grafana/production-version` with version `1` enables the production overlay. A release without the marker (even one with a local `compose.grafana.yaml`) removes the Grafana container and reinstates its original Postgres HBA via the previous image. A release without the HTTPS overlay first removes the public proxy, including on rollback to loopback-only Grafana. Neither path drops database objects, deletes `pg-data`, or changes passwords. To deliberately select a prior release, use the same Deploy action with its `main` commit SHA. Future Grafana major upgrades or incompatible metric view changes need an explicit database backup and compatibility plan; selecting an older image is not a database downgrade.

## HEL HTTPS staging

The [HEL recipe](staging/hel/README.md) serves both dashboards to named Viewers over publicly trusted HTTPS without SSH. It is a temporary, isolated **synthetic** preview. The production Deploy workflow never installs its nginx site, fixture or staging credentials.

## Disposable HTTPS verification (local only)

Run `bash deploy/grafana/verify.sh staging` from a committed checkout on a local Linux Docker daemon with the Tank test subnet free. In addition to the tools below, put `google-chrome`, NSS `certutil` and a Python 3 with Playwright and PyYAML on `PATH`. No staging secret or remote host is required.

The staging target is `grafana.127-0-0-1.sslip.io` on a Docker-assigned **loopback** port. The stand loads `deploy/grafana/staging` from the tested commit; the production workflow does not stage it. Caddy accepts TLS 1.2/1.3, requests a DNS-name certificate via TLS-ALPN-01 from the stand's Pebble CA, and renews it automatically. The 90-second fixture lifetime makes renewal observable. Both the primary and retry CA are local. Do not install the generated root in your system trust store.

The check stages the selected Deploy release in a disposable project, runs real PostgreSQL and Grafana with synthetic data, and opens Queue and Pipeline in a fresh Chrome profile as a named Viewer. It checks denied anonymous and admin requests, secure cookies, replacement, failed-release rollback, removal of Grafana on a legacy release, restoration and a changed, trusted leaf certificate. Unrelated Tank services and SSH are simulated. Exiting removes the stand; a passing run is not a public deployment or a publicly trusted certificate.

## Local verification

Use `deploy/grafana/verify.sh fresh-browser` with the local browser prerequisites above to verify a Viewer through the release edge without issuing a certificate. The fixture substitutes localhost, a loopback port and the pinned Pebble image’s existing test certificate; production ACME is not exercised.

Run `deploy/grafana/verify.sh` on a local Docker daemon with the Tank subnet free. It requires Bash, Docker Compose, `curl`, `jq` and OpenSSL. It uses disposable credentials and PostgreSQL, tests dashboard targets and reader permissions, replaces Grafana to check persistence, and removes only its test project. Do not point it at a production stack.

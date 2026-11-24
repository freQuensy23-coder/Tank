# Tank Grafana

## Production

Run **Deploy** on `main`. No Grafana password, separate database, manual SQL or post-deploy command is required. The action ships the selected commit's Compose files, SQL, provisioning and dashboards. The PostgreSQL image admits the two Grafana logins only from `172.30.60.11` on `tank-net` using SCRAM. The first rollout replaces the Postgres container to load that rule; it preserves `pg-data`.

Tank applies its migrations first. Deploy waits for `013_source_poll_counts.sql`, bootstraps the roles and schemas, refreshes the metric views in a transaction, then starts the pinned Grafana image. Before marking a release deployed it verifies the configured PostgreSQL reader's datasource health and both provisioned dashboards in the **Tank** folder through authenticated HTTP. Those API endpoints must deny anonymous access.

### Browser access

The host-local upstream is `http://127.0.0.1:3000`, **not** an Internet URL. The dashboard paths are `/d/tank-queue/` and `/d/tank-pipeline/`. Port 3000 is published on loopback only. There is no public dashboard, anonymous login or published database port.

After the reviewed release is deployed, open `https://grafana.84-235-183-96.sslip.io/d/tank-queue/` or `/d/tank-pipeline/` on the same origin and sign in with a production Grafana account. Give dashboard readers a named Viewer; staging accounts and URLs are not production access.

The selected HTTPS overlay runs staging’s pinned Caddy image on a separate network shared only with Grafana. It publishes TCP 443 only. The hostname must resolve to the production VM, inbound TCP 443 must reach Docker through the Oracle network and host firewall, and Caddy needs outbound DNS and HTTPS to Let's Encrypt. It obtains and renews the certificate by TLS-ALPN-01; keep the `tank_grafana-tls` volume private and persistent. Deploy checks the trusted hostname, login page and anonymous API denial locally through Caddy, without sending credentials. Check the same origin from outside after Deploy; a local check cannot verify the cloud firewall.

### Nexus shared nginx

Nexus already serves IPv4/IPv6 HTTP and HTTPS through nginx. Its default HTTP site serves `/.well-known/acme-challenge/` from `/var/www/html`. System Certbot uses webroot, an enabled `certbot.timer`, and an existing deploy hook that tests and reloads nginx. The nginx Certbot plugin is not required.

At host setup, the infrastructure owner runs `bash deploy/grafana/nginx/setup.sh <hostname>` as root from reviewed IaC, not during an app release. Use the exact `grafana.<dashed-IPv4>.sslip.io` name for Nexus. Confirm DNS points here, TCP 80/443 reach the existing listeners, no other site/lineage owns that name, the new file sorts after the existing defaults and `127.0.0.2:983` is reserved exclusively for Tank (keep 983 below the host’s unprivileged port range, including while Tank is stopped). Keep the system ACME account, timer and reload hook in place. First setup expects the inspected Nexus configuration (literal server names only); it refuses a conflicting name or existing system lineage, then issues before enabling the site. Repeat setup does not request a certificate. The retained file in `sites-available` identifies the site even if its enabled link is removed. Retry setup after correcting an activation failure. If interrupted between issuance and writing the site file, setup refuses adoption; the owner must verify and retire the orphaned certificate before retrying. If a renewal reload fails, repair the nginx configuration, test and reload nginx; do not reissue merely to reload.

Deploy takes the hostname from that site and publishes Grafana only at the reserved loopback address, with the HTTPS root URL, secure cookies and anonymous access disabled. It does not edit nginx or invoke Certbot. An older release without the shared overlay uses its original `127.0.0.1` binding (or no Grafana); nginx then returns 502, not the historical app. Restoring a capable release reconnects the same route. Deploy checks that the loopback TLS probe was proxied to the selected Grafana binding. Neither direction starts Caddy on Nexus, even if the enabled link is missing. If the retained site definition itself is lost and either the candidate or recovery release needs it, Deploy refuses before changing services: it cannot reconstruct the previous HTTPS origin. Restore the reviewed site definition first. A historical-to-historical deployment needs neither file. The dedicated host is unchanged.

To decommission, remove the `zzzz-tank-grafana` enabled link, test and reload nginx, remove its `sites-available` file, then retire only this certificate with `certbot delete --cert-name <hostname>` if no longer needed. Do not remove the shared timer, hook, account or webroot. Before public access, rotate any compromised Grafana credentials and revoke affected sessions; changing the initial password setting does not reset an existing account.

### Credentials and state

Under the deployment lock, Deploy keeps existing Grafana settings in `/srv/tank-deploy/.env` and generates only absent `GRAFANA_ADMIN_PASSWORD`, `GRAFANA_SECRET_KEY`, `GRAFANA_STORE_PASSWORD` and `GRAFANA_READER_PASSWORD` (32 independent random bytes per generated value). The SSH transport identity must be authorized for noninteractive `sudo` on the deployment host. Root stages and runs the release; the regular `.env` remains root-owned and mode 0600. Deploy validates the candidate and recovery configurations before X worker publication. Changes preserve its prior contents and are installed atomically with mode 0600. Subsequent deploys do not rewrite a complete file. No secret is sent back to Actions. Host administrators and Docker administrators remain trusted.

The infrastructure owner can recover the initial `admin` credential from `GRAFANA_ADMIN_PASSWORD` in the protected file and issue named Viewer accounts privately. Keep an access-controlled backup of this file **with** `pg-data`; do not regenerate the encryption key on recovery. The initial admin credential is also used by the local readiness check: if deliberately rotating it in Grafana, update the protected setting in the same maintenance window. Never send the `.env` file, put credentials in URLs or share the admin login with dashboard readers.

`tank.grafana` is owned by `tank_grafana_store` and persists dashboards, users, preferences and encrypted datasource settings. `tank_grafana_reader` gets only the named `tank_metrics` views, a read-only transaction default, four connections and a 15-second statement timeout. Bootstrap removes `PUBLIC` temporary-table creation in `tank`, preserving it for Tank, its existing agent and the Grafana store. Unmarked existing roles or schemas with the same names are not adopted.

### Rollback

A failed rollout returns to the previous recorded Tank release. For a Grafana-enabled release it uses that release's files, reinstalls its views and checks Grafana. Only `deploy/grafana/production-version` with version `1` enables the production overlay. A release without the marker (even one with a local `compose.grafana.yaml`) removes the Grafana container and reinstates its original Postgres HBA via the previous image. A release without the HTTPS overlay first removes the public proxy, including on rollback to loopback-only Grafana. Neither path drops database objects, deletes `pg-data`, or changes passwords. To deliberately select a prior release, use the same Deploy action with its `main` commit SHA. Future Grafana major upgrades or incompatible metric view changes need an explicit database backup and compatibility plan; selecting an older image is not a database downgrade.

## Minute-condition panel

The panel narrows the scan with the existing `(type, idem_key)` index over UTC cutoff keys, then applies the exact half-open timestamp range. The key range starts one minute early so PostgreSQL's accepted `24:00` spelling of midnight is not lost. The original two-column metrics view remains compatible with older releases and readers.

## HEL HTTPS staging

The [HEL recipe](staging/hel/README.md) serves both dashboards to named Viewers over publicly trusted HTTPS without SSH. It is a temporary, isolated **synthetic** preview. The production Deploy workflow never installs its nginx site, fixture or staging credentials.

## Disposable HTTPS verification (local only)

Run `bash deploy/grafana/verify.sh staging` from a committed checkout on a local Linux Docker daemon with the Tank test subnet free. In addition to the tools below, put `google-chrome`, NSS `certutil` and a Python 3 with Playwright and PyYAML on `PATH`. No staging secret or remote host is required.

The staging target is `grafana.127-0-0-1.sslip.io` on a Docker-assigned **loopback** port. The stand loads `deploy/grafana/staging` from the tested commit; the production workflow does not stage it. Caddy accepts TLS 1.2/1.3, requests a DNS-name certificate via TLS-ALPN-01 from the stand's Pebble CA, and renews it automatically. The 90-second fixture lifetime makes renewal observable. Both the primary and retry CA are local. Do not install the generated root in your system trust store.

The check stages the selected Deploy release in a disposable project, runs real PostgreSQL and Grafana with synthetic data, and opens Queue and Pipeline in a fresh Chrome profile as a named Viewer. It checks denied anonymous and admin requests, secure cookies, replacement, failed-release rollback, removal of Grafana on a legacy release, restoration and a changed, trusted leaf certificate. Unrelated Tank services and SSH are simulated. Exiting removes the stand; a passing run is not a public deployment or a publicly trusted certificate.

## Local verification

Use `deploy/grafana/verify.sh fresh-browser` with the local browser prerequisites above to verify a Viewer through the release edge without issuing a certificate. The fixture substitutes localhost, a loopback port and the pinned Pebble image’s existing test certificate; production ACME is not exercised.

Run `deploy/grafana/verify.sh` on a local Docker daemon with the Tank subnet free. It requires Bash, Docker Compose, `curl`, `jq` and OpenSSL. It uses disposable credentials and PostgreSQL, tests dashboard targets and reader permissions, replaces Grafana to check persistence, and removes only its test project. Do not point it at a production stack.

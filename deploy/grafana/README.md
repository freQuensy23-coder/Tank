# Local Grafana for Tank

Run `deploy/grafana/verify.sh` from the checkout to test the integration without a running Tank stack. It requires Bash, Docker Engine with Compose, `curl`, `jq` and OpenSSL. Use a local Docker daemon with `172.30.60.0/24` available. If the subnet is in use, use a different local daemon; do not remove the existing network.

The harness uses a uniquely named project, synthetic passwords, an unpublished PostgreSQL 16 on tmpfs, Tank's init script and migrations, and the same Grafana overlay as below. It verifies authenticated HTTP, both dashboards' provisioned queries on empty operational tables, reader access and denials, and PostgreSQL persistence across Grafana container replacement. It removes its containers, volumes and networks on exit. It does not check metric accuracy on production data.

## Use with an existing local Tank stack

`compose.grafana.yaml` is an explicit overlay, not part of the default deployment. It adds Grafana; it does **not** add a second PostgreSQL instance or publish a database port. These commands are for a **disposable local development stack**, not a production rollout. Use the same local project name, Compose environment and `pg-data` as that stack. Tank must already have migrated the `tank` database. Applying the overlay recreates the local `postgres` container to mount the HBA rules; schedule that interruption. Address `172.30.60.11` on `tank-net` must be free.

From the repository root, generate four independent secrets once. Keep them outside `deploy/grafana` (that directory is mounted read-only into PostgreSQL), out of git, logs and shell tracing. Back up the secret key with your local database; changing it can prevent Grafana from decrypting stored credentials.

```bash
(umask 077; set -C; for key in GRAFANA_ADMIN_PASSWORD GRAFANA_SECRET_KEY GRAFANA_STORE_PASSWORD GRAFANA_READER_PASSWORD; do
  printf '%s=%s\n' "$key" "$(openssl rand -hex 32)"
done > .env.grafana)
```

For subsequent starts, load that same file, **do not regenerate it**. With the existing local Tank configuration available to Compose:

```bash
set -a; source .env.grafana; set +a
local_tank=(docker compose -p "${LOCAL_TANK_PROJECT:?Set to your existing local Tank project name}" -f compose.yaml -f compose.grafana.yaml)
"${local_tank[@]}" up -d --no-deps --wait postgres
"${local_tank[@]}" exec -T -u postgres -e GRAFANA_STORE_PASSWORD -e GRAFANA_READER_PASSWORD postgres \
  psql -X -d tank -f /etc/tank-grafana/bootstrap.sql
"${local_tank[@]}" exec -T -u postgres postgres psql -X -d tank -f /etc/tank-grafana/install-views.sql
"${local_tank[@]}" up -d --no-deps --wait grafana
```

`bootstrap.sql` is one-time and transactional. It deliberately fails on role/schema collisions instead of taking over an existing principal. Review its database ACL change: PostgreSQL grants `TEMPORARY` to `PUBLIC` by default. The bootstrap removes that implicit grant for `tank`, preserving temp-table access explicitly for Tank, its existing agent and the Grafana storage login. Check custom local roles before applying. Re-run **only** `install-views.sql` after a metrics SQL update. Keep the underlying table schemas inaccessible to the reader: grant `SELECT` only on each reviewed view, never on all current/future tables.

Open `http://127.0.0.1:3000/` and log in as `admin` using `GRAFANA_ADMIN_PASSWORD`. This sets the initial password only. For dashboard consumers, use named Viewer accounts rather than sharing the admin login. `GRAFANA_BIND_ADDRESS` and `GRAFANA_PORT` override the default host binding. HTTP is unencrypted; keep the listener on loopback and use a tunnel or TLS-terminating reverse proxy for remote access. `/api/health` is public; dashboards and the datasource API require authentication.

| Interface | Contract |
|---|---|
| Grafana state | `tank.grafana`, owned by `tank_grafana_store`. Users, preferences, dashboards and datasource configuration persist in `pg-data`. |
| Datasource | UID `tank-postgres`, type `grafana-postgresql-datasource`, database `tank`, user `tank_grafana_reader`. Read-only transaction default, four connections, 15-second statement timeout. Object grants enforce the data boundary. |
| Metrics | `tank_metrics`, owned by `tank`. `install-views.sql` installs `queue-views.sql` and `pipeline-views.sql` with named view grants. Neither Grafana login receives Tank's application/agent role. |
| Dashboard files | `deploy/grafana/dashboards/*.json`, provisioned in the **Tank** folder. Edit JSON in git; UI edits are not the source of truth. |
| Database authentication | Two dedicated logins, SCRAM only from Grafana's fixed IP into `tank`; other connections use the existing Tank HBA rules. |

Stop only Grafana with `"${local_tank[@]}" stop grafana`. Keep the PostgreSQL volume and `.env.grafana` for subsequent use. Never use `down -v` against a stack with data you intend to keep.

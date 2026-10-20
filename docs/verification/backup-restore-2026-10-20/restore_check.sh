#!/bin/bash
# Backup -> restore check (tank-ops, 2026-10-20). Restores a `tank backup` snapshot into a
# separate PostgreSQL 16 container with its own volume (never production) and compares
# every table with production by row count and a hash of the sorted row text.
# Disk on openclaw-dp is ~98% full, so the row data of mkt.klines_1m_* and mkt.oi_obs_*
# daily partitions is not materialized; it is streamed from the dump and hashed instead.
set -euo pipefail
BACKUP=${1:?backup directory}
OUT=${2:?evidence directory}
IMAGE=sha256:0ea6700a3b4f0ae6ce746519073558aed4d88a79d8d07622a9a644946c7319c4
NAME=tank-ops-restore-pg
VOL=tank-ops-restore-pg-20261020
PROD=tank-postgres-1
mkdir -p "$OUT"
log() { echo "$(date -u +%FT%TZ) $*"; }
# Row count and md5 of the sorted COPY text rows (order-independent table content).
rowhash() { /srv/tank/.venv/bin/python -c "import hashlib,sys; rows=sorted(sys.stdin.buffer.read().splitlines(True)); print(len(rows), hashlib.md5(b\"\".join(rows)).hexdigest())"; }
# Only the data lines of the COPY block in pg_restore SQL output.
copy_rows() { awk '/^COPY /{on=1;next} /^\\\.$/{on=0} on'; }
rp() { docker exec -u postgres "$NAME" psql -X -v ON_ERROR_STOP=1 -At "$@"; }
pp() { docker exec -u postgres "$PROD" psql -X -v ON_ERROR_STOP=1 -At "$@"; }

log "start backup=$BACKUP"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker volume rm "$VOL" >/dev/null 2>&1 || true
docker volume create "$VOL" >/dev/null
docker run -d --name "$NAME" --network none --shm-size 512m --cpus 2 --memory 2g \
  -e POSTGRES_HOST_AUTH_METHOD=trust -v "$VOL":/var/lib/postgresql/data -v "$BACKUP":/b:ro \
  "$IMAGE" >/dev/null
for i in $(seq 60); do docker exec -u postgres "$NAME" pg_isready -q && break; sleep 2; done
docker exec -u postgres "$NAME" pg_isready
rp -d postgres -c "CREATE ROLE tank NOLOGIN; CREATE ROLE oma NOLOGIN; CREATE ROLE tank_agent NOLOGIN; CREATE ROLE tank_replay_agent NOLOGIN"
rp -d postgres -c "CREATE DATABASE tank OWNER tank"
rp -d postgres -c "CREATE DATABASE oma OWNER oma"

log "verify backup manifest hashes"
( cd "$BACKUP" && /srv/tank/.venv/bin/python - <<PY
import hashlib, json
m = json.load(open("manifest.json"))
bad = []
for name, digest in m["files"].items():
    h = hashlib.sha256()
    with open(name, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    if h.hexdigest() != digest:
        bad.append(name)
print(json.dumps({"files": len(m["files"]), "mismatched": bad}))
assert not bad
PY
) > "$OUT/manifest-check.json"
cat "$OUT/manifest-check.json"

log "restore oma (full)"
docker exec -u postgres "$NAME" pg_restore --exit-on-error -d oma /b/oma.dump
log "restore tank (all but klines_1m/oi_obs daily partition data)"
docker exec -u postgres "$NAME" sh -c "pg_restore -l /b/tank.dump > /tmp/full.list && grep -Ev \"TABLE DATA mkt (klines_1m|oi_obs)_[0-9]{8} \" /tmp/full.list > /tmp/restore.list && wc -l /tmp/full.list /tmp/restore.list"
docker exec -u postgres "$NAME" pg_restore --exit-on-error -j 2 -L /tmp/restore.list -d tank /b/tank.dump
log "restored; sizes"
rp -d postgres -c "select datname, pg_database_size(datname) from pg_database where datname in (\$\$tank\$\$,\$\$oma\$\$)" | tee "$OUT/restored-sizes.txt"

TABLES="select n.nspname||chr(46)||c.relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.relkind=\$\$r\$\$ and n.nspname not in (\$\$pg_catalog\$\$,\$\$information_schema\$\$) and n.nspname not like \$\$pg_toast%\$\$ order by 1"
echo -e "db\ttable\tmode\tprod_rows\trestored_rows\tprod_md5\trestored_md5\tmatch" > "$OUT/tables.tsv"
for db in oma tank; do
  pp -d "$db" -c "$TABLES" > /tmp/tank-ops-prod-tables.txt
  rp -d "$db" -c "$TABLES" > /tmp/tank-ops-restored-tables.txt
  if ! diff -q /tmp/tank-ops-prod-tables.txt /tmp/tank-ops-restored-tables.txt >/dev/null; then
    echo "TABLE SET DIFFERS for $db" | tee -a "$OUT/table-set-diff.txt"
    diff /tmp/tank-ops-prod-tables.txt /tmp/tank-ops-restored-tables.txt | tee -a "$OUT/table-set-diff.txt" || true
  fi
  while read -r t; do
    if [[ "$db" == tank && "$t" =~ ^mkt\.(klines_1m|oi_obs)_[0-9]{8}$ ]]; then
      mode=stream
      read -r rc r < <(docker exec -u postgres "$NAME" pg_restore -a -n mkt -t "${t#mkt.}" -f - /b/tank.dump | copy_rows | rowhash)
    else
      mode=restored
      read -r rc r < <(docker exec -u postgres "$NAME" psql -X -At -d "$db" -c "COPY $t TO STDOUT" | rowhash)
    fi
    read -r pc p < <(docker exec -u postgres "$PROD" psql -X -At -d "$db" -c "COPY $t TO STDOUT" | rowhash)
    m=$([[ "$pc" == "$rc" && "$p" == "$r" ]] && echo yes || echo NO)
    echo -e "$db\t$t\t$mode\t$pc\t$rc\t$p\t$r\t$m" >> "$OUT/tables.tsv"
  done < /tmp/tank-ops-prod-tables.txt
done
awk -F"\t" "NR>1{n++; if(\$8==\"yes\")ok++; else bad++; rows+=\$4} END{printf \"{\\\"tables\\\":%d,\\\"match\\\":%d,\\\"mismatch\\\":%d,\\\"prod_rows\\\":%d}\\n\", n, ok, bad, rows}" "$OUT/tables.tsv" | tee "$OUT/summary.json"
log "done"

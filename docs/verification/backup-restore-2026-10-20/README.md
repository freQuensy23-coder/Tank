# Backup → restore, new full cycle (2026-10-20, tank-ops)

Production `tank` and `oma` were backed up the way HLD §5/§8 and `deploy/backup.sh` say, then restored into a separate PostgreSQL 16 container with its own Docker volume. Production was never written to.

## Backup

- Command: `docker compose -p tank … run --rm --no-deps tank /app/deploy/backup.sh`, which runs `tank backup --force` in image `sha256:1c828c59…` (r84). Log: [run1/backup.log](run1/backup.log).
- Result: `/var/backups/tank/tank-2026-10-20` (volume `backup`) holds `tank.dump` (630 MB), `oma.dump` (3.8 MB), `bundles/` (12 files), `manifest.json`, `COMPLETE`. Created 16:52:38Z, took 59 s.
- `tank.scheduler.verify_backup` re-checks the snapshot: ok, 14 files ([run1/tank-verify_backup.json](run1/tank-verify_backup.json)). Every manifest SHA-256 matches the file ([run1/manifest-check.json](run1/manifest-check.json)).

## Restore ([restore_check.sh](restore_check.sh), log [run1/restore.log](run1/restore.log))

- Separate container `tank-ops-restore-pg` (postgres:16, same image digest as production, `--network none`) with its own volume `tank-ops-restore-pg-20261020`. The roles were recreated and `pg_restore --exit-on-error` ran: `oma` in full, and `tank` in full except the row data of the daily partitions of `mkt.klines_1m` and `mkt.oi_obs`.
- Why those two were not materialized: those partitions take ~7.5 GB, and the shared host disk had 3.7–6.6 GB free (98–99 %). They are checked by streaming instead: `pg_restore -a -t <partition>` sends the COPY rows from the dump straight into the hash, without writing them to disk.
- Check per table, all 393 tables of both databases: row count plus MD5 of the sorted COPY text, on both production and the restore. Result [run1/summary.json](run1/summary.json): **393/393 match, 4,920,781 rows** ([run1/tables.tsv](run1/tables.tsv)).
  - 227 tables (tank 130, oma 97) were restored and compared;
  - 166 partitions (4,618,921 rows) were streamed from the dump and compared.
  - The table sets are identical (no `table-set-diff.txt`).
- Files: `bundles/` was copied from the backup into a separate volume `tank-ops-restore-files-20261020`. All 12 files match the manifest SHA-256, and each file name is the SHA-256 of its content ([run1/bundles-restored.json](run1/bundles-restored.json), [verify_bundles.py](verify_bundles.py)).
- Afterwards the restore container and both volumes were deleted to free disk. The backup itself is kept.

## Schedule and retention against the spec

- Schedule: `Scheduler.tick` launches `maintenance` on each tick. Before `backup_hour_utc` (02 UTC) it defers; after that it makes one `tank-YYYY-MM-DD` snapshot a day (HLD §5 scheduler "nightly backup", §8 "Nightly pg_dump + bundles/"). That matches the spec.
- Retention runs only after a verified backup (`Scheduler.retention`). The values come from `config/thresholds.yaml`, and they match HLD §7.1/§7.2 and TDD §12:
  - market base tier 90 d;
  - jobs 30 d;
  - hot raw trades/books 30 d;
  - archive cache 180 d.
  The manual `tank backup` and `deploy/backup.sh` prune nothing without `--prune`.
- Open points, raised with the lead:
  1. The spec says nothing on how long backup snapshots are kept. They grow by about 0.8 GB a day.
  2. `/var/backups/tank` is on the production disk, while HLD §8 says "an external disk on the host".

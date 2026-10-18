# PostgreSQL 16 recovery verification

The canonical `tank backup --force` command was exercised against four isolated PostgreSQL 16.10 databases: Tank/OpenMA fixture sources and fresh restore targets. `--prune` was omitted, so this manual verification performed no retention. All four databases were dropped after verification. No production database, external account or user backup disk was used.

Executed command:

```sh
PATH=/tmp/tank-pg16/bin:$PATH .venv/bin/python docs/evidence/recovery_probe.py
```

The probe creates a temporary local backup directory, migrates the Tank fixture database, and writes a source item, event, pending job and immutable evidence bundle. It creates a separate OpenMA fixture table containing one session transcript. Fixture files cover compressed source evidence, raw web evidence, a pinned replay input packet and a frozen bundle; configuration and session logs are included too.

The command calls the same backup implementation used by the scheduler and `deploy/backup.sh`. Both custom-format dumps were produced by real `pg_dump` 16.10. The probe relocates the complete backup and deletes the original live fixture directory, then runs real `pg_restore --exit-on-error --no-owner --dbname <isolated-target> <dump>` for both databases.

Both restore commands exited 0. Restored Tank rows retained the source reference, event, queued job and immutable bundle. The OpenMA transcript matched exactly. All four evidence files resolved from their original references on the relocated backup, and the complete relative-path checksum manifest verified. Machine-readable results and dump hashes are in [RECOVERY_VERIFICATION.json](RECOVERY_VERIFICATION.json).

The OpenMA fixture proves preservation of a separate database and session record, rather than interoperability with a running OpenMA server. Recovery of encrypted real Model Cards additionally requires separately protected OpenMA root secrets. The script deliberately uses only the isolated local test PostgreSQL server and creates uniquely named fixture databases.

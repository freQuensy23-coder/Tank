# W11: tables outside HLD §7.1 and their retention (non-market part, tank-ops, 2026-10-20)

HLD §7.1 lists the tables of the `ops` and `qa` schemas and their retention: jobs 30 days, LLM usage permanent; `qa` holds reviews, fixtures and replay_runs. This note covers every non-market table outside that list, and the decision taken for each.

| Table | Decision | Why | Where |
|---|---|---|---|
| `qa.outcomes` | dropped | The outcome evaluator is descriptive (TDD §11.5); `review show` computes the realized move on demand from frozen evidence | review.py; migration 036 |
| `ops.source_dedup` | dropped | It duplicated the dedup key of `ingest.source_items` (TDD §2.9: unique source_id + external_id, index content_hash) | tank-sources 4313f11; migration 040 |
| `ops.routing_reservations` | dropped | Leftover: nothing created reservations any more | tank-sessions, tank-events, tank-calibration removed the readers; migration 040 |
| `ops.source_escalations` | kept, 30 days after expiry | FR-D-02 escalations with expiry (HLD §2 rule 9) are control records like jobs | `_prune_expired_escalations`, run with job retention |
| `ops.source_checkpoints` | kept, bounded | Conditional-GET and pending-batch state of each feed (HLD §3); one row per feed, overwritten | — |
| `ops.scheduler_checkpoints` | kept, bounded | A handful of scheduler markers (raw retention, partitions, scout hooks, reaction changes), overwritten | — |
| `qa.reviews` METRIC_SEMANTICS rows | deleted (4,705) | Leftovers of the removed semantic self-test; a ReviewRecord subject is an alert_id or event_id (TDD §2.8). Lead ruling | migration 038 |
| `ops.schema_migrations` | kept | Migration bookkeeping | — |
| Backup snapshots | newest 3 kept | The spec gives no count. Lead ruling: 3 while the backup shares the production disk | `tank.backup.prune_backup_snapshots`, `backup_snapshots_kept` |

The market tables are tank-market's: `mkt.*`, `ops.capture_tier_transitions`, `ops.market_migration_exports`, and the `qa.reviews` ARCHIVE_RETRY rows (their migration 039). `mkt.capture_gaps` now has the market retention (storage.py).

## Production check (read-only, 2026-10-20 ~20:15Z)

- `ops.schema_migrations`: 036, 038 and 040 applied at 19:35:09Z (tank-platform, deploy r90).
- `to_regclass` returns null for `qa.outcomes`, `ops.source_dedup` and `ops.routing_reservations`.
- `qa.reviews` by subject: ARCHIVE_RETRY 3,646 (waiting for tank-market's 039); METRIC_SEMANTICS 0.
- The `append_only` trigger on `qa.reviews` is enabled again (`O`).
- Before applying, migration 038 was rehearsed on a full restored copy of production, where it removed exactly 4,705 rows ([../backup-restore-2026-10-20/run2-full](../backup-restore-2026-10-20/run2-full/)). 036 and 040 were checked on a test database: the tables are gone, and all 58 `TABLE_COLUMNS` collections stay queryable.

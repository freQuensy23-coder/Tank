-- W11: neither table is in HLD §7.1 and no code reads them any more.
-- ops.source_dedup duplicated the dedup key of ingest.source_items (TDD §2.9: unique
-- (source_id, external_id), index content_hash); tank-sources dedupes on source_items.
-- ops.routing_reservations was a leftover control table: nothing creates reservations
-- (tank-sessions, tank-events removed the readers). No CASCADE: an unexpected
-- dependency aborts the migration.
DROP TABLE IF EXISTS ops.source_dedup;
DROP TABLE IF EXISTS ops.routing_reservations;

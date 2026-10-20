-- Preserve original SQL rows in verified permanent bundles before dropping their
-- obsolete copies. Bounded stages fit PostgreSQL's default transaction lock budget.
DO $$ BEGIN
    IF current_setting('tank.legacy_archive_export_verified', true) IS DISTINCT FROM '032_remove_verified_legacy_archive.sql' THEN
        RAISE EXCEPTION '032 must use guarded Repository.migrate verified legacy archive export';
    END IF;
    DROP TABLE IF EXISTS archive.ops_budget_state;
    DROP TABLE IF EXISTS archive.funding_obs;
    DROP TABLE IF EXISTS archive.supply_obs;
    DROP TABLE IF EXISTS archive.supply;
    DROP TABLE IF EXISTS archive.unlocks;
    DROP TABLE IF EXISTS archive.hot_tokens;
    DROP TABLE IF EXISTS archive.capture_state;
    DROP TABLE IF EXISTS archive.capture_gaps;
    DROP TABLE IF EXISTS archive.raw_archives;
    DROP TABLE IF EXISTS archive.backfill_runs;
    -- No CASCADE: unexpected dependencies abort this version atomically.
END $$;

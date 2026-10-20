-- Preserve original SQL rows in verified permanent bundles before dropping their
-- obsolete copies. Bounded stages fit PostgreSQL's default transaction lock budget.
DO $$ BEGIN
    IF current_setting('tank.legacy_archive_export_verified', true) IS DISTINCT FROM '033_remove_legacy_price_flow.sql' THEN
        RAISE EXCEPTION '033 must use guarded Repository.migrate verified legacy archive export';
    END IF;
    DROP TABLE IF EXISTS archive.flow_bars_1m;
    DROP TABLE IF EXISTS archive.klines_1m;
    -- No CASCADE: unexpected dependencies abort this version atomically.
END $$;

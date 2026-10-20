-- Preserve original SQL rows in verified permanent bundles before dropping their
-- obsolete copies. Bounded stages fit PostgreSQL's default transaction lock budget.
DO $$ BEGIN
    IF current_setting('tank.legacy_archive_export_verified', true) IS DISTINCT FROM '034_remove_legacy_derivatives.sql' THEN
        RAISE EXCEPTION '034 must use guarded Repository.migrate verified legacy archive export';
    END IF;
    DROP TABLE IF EXISTS archive.oi_obs;
    DROP TABLE IF EXISTS archive.liq_obs;
    -- No CASCADE: unexpected dependencies abort this version atomically.
END $$;

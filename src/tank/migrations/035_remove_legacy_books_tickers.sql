-- Preserve original SQL rows in verified permanent bundles before dropping their
-- obsolete copies. Bounded stages fit PostgreSQL's default transaction lock budget.
DO $$ BEGIN
    IF current_setting('tank.legacy_archive_export_verified', true) IS DISTINCT FROM '035_remove_legacy_books_tickers.sql' THEN
        RAISE EXCEPTION '035 must use guarded Repository.migrate verified legacy archive export';
    END IF;
    DROP TABLE IF EXISTS archive.book_summaries;
    DROP TABLE IF EXISTS archive.tickers_24h;
    DROP SCHEMA IF EXISTS archive;
    -- No CASCADE: unexpected dependencies abort this version atomically.
END $$;
